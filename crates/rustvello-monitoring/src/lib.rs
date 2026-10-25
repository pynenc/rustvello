//! Web-based monitoring dashboard for Rustvello.
//!
//! Provides a browser-based UI for inspecting invocations, runners,
//! workflows, and task execution timelines. Built with Axum + Askama + HTMX.

pub mod data;
pub mod family_tree;
pub mod health;
pub mod histogram;
pub mod log_explorer;
pub mod navigation;
pub mod query;
pub mod routes;
pub mod server;
pub mod state;
pub mod svg;
pub mod util;
pub mod view;

use std::net::SocketAddr;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::TaskId;

use crate::state::AppState;

/// Environment variable that puts the monitor in read-only mode (`1`, `true`,
/// `yes` or `on`).
pub const READ_ONLY_ENV: &str = "RUSTVELLO_MONITORING_READ_ONLY";

/// Configuration for the monitoring server.
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Address to bind the HTTP server to.
    pub bind: SocketAddr,
    /// Log level filter (e.g. "info", "debug").
    pub log_level: String,
    /// Refuse purge, rerun and cancel and hide their buttons. Also enabled by
    /// [`READ_ONLY_ENV`].
    pub read_only: bool,
    /// Probe every backend before serving and fail to start when one refuses
    /// the monitor's credentials (the error names the backend and its message).
    pub check_backends: bool,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 8000)),
            log_level: "info".to_owned(),
            read_only: false,
            check_backends: true,
        }
    }
}

/// Options for [`serve_with_options`]: what [`MonitorConfig`] carries besides
/// the bind address.
#[derive(Debug, Clone)]
pub struct MonitorOptions {
    /// See [`MonitorConfig::read_only`].
    pub read_only: bool,
    /// See [`MonitorConfig::check_backends`].
    pub check_backends: bool,
}

impl Default for MonitorOptions {
    fn default() -> Self {
        Self {
            read_only: false,
            check_backends: true,
        }
    }
}

impl MonitorOptions {
    /// Defaults, with read-only mode taken from [`READ_ONLY_ENV`].
    pub fn from_env() -> Self {
        Self {
            read_only: read_only_from_env(),
            ..Self::default()
        }
    }
}

/// Whether [`READ_ONLY_ENV`] asks for read-only mode.
pub fn read_only_from_env() -> bool {
    std::env::var(READ_ONLY_ENV).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// A named application instance for multi-app monitoring.
#[derive(Clone)]
pub struct AppInstance {
    pub app_id: String,
    pub config: AppConfig,
    pub broker: Arc<dyn Broker>,
    pub orchestrator: Arc<dyn InvocationControlBackend>,
    pub state_backend: Arc<dyn StateBackend>,
    /// Trigger evidence for full stores; absent only for broker-only apps.
    pub trigger_store: Option<Arc<dyn TriggerStore>>,
    pub client_data_store: Arc<ClientDataStoreManager>,
    pub task_ids: Vec<TaskId>,
}

/// Start the monitoring web server.
///
/// # Arguments
/// * `apps` — Map of app_id → `AppInstance` to monitor
/// * `selected_app` — Initial active app ID
/// * `monitor_config` — Server configuration
pub async fn start_monitor(
    apps: std::collections::HashMap<String, AppInstance>,
    selected_app: &str,
    monitor_config: MonitorConfig,
) -> RustvelloResult<()> {
    let listener = tokio::net::TcpListener::bind(monitor_config.bind)
        .await
        .map_err(|e| rustvello_core::error::RustvelloError::Internal {
            message: format!("bind: {e}"),
        })?;
    let options = MonitorOptions {
        read_only: monitor_config.read_only || read_only_from_env(),
        check_backends: monitor_config.check_backends,
    };
    serve_with_options(apps, selected_app, listener, options).await
}

/// Serve the monitoring web server on an already bound listener.
///
/// Binding first lets a caller learn the actual address (for example after
/// binding port 0) and report bind errors before the server task starts.
pub async fn serve_on(
    apps: std::collections::HashMap<String, AppInstance>,
    selected_app: &str,
    listener: tokio::net::TcpListener,
) -> RustvelloResult<()> {
    serve_with_options(apps, selected_app, listener, MonitorOptions::from_env()).await
}

/// Probe the selected app's backends once; `Err` names every failing component.
///
/// Used before a monitor starts serving so that a backend refusing the
/// monitor's credentials stops the process with the backend's error text
/// instead of an empty dashboard.
pub async fn check_backends(
    apps: &std::collections::HashMap<String, AppInstance>,
    selected_app: &str,
) -> RustvelloResult<health::HealthReport> {
    let app = apps.get(selected_app).ok_or_else(|| {
        rustvello_core::error::RustvelloError::Configuration {
            message: format!("selected app '{selected_app}' not found in apps list"),
        }
    })?;
    let report = health::health_report(app).await;
    if report.is_ok() {
        for problem in &report.problems {
            tracing::warn!("monitoring {selected_app}: {problem}");
        }
        Ok(report)
    } else {
        Err(rustvello_core::error::RustvelloError::Configuration {
            message: format!(
                "backend check failed for app '{selected_app}': {}. Check that the credentials have read and write access to the storage of this app (docs: storage-and-permissions)",
                report.problems.join("; ")
            ),
        })
    }
}

/// [`serve_on`] with explicit [`MonitorOptions`].
pub async fn serve_with_options(
    apps: std::collections::HashMap<String, AppInstance>,
    selected_app: &str,
    listener: tokio::net::TcpListener,
    options: MonitorOptions,
) -> RustvelloResult<()> {
    if options.check_backends {
        check_backends(&apps, selected_app).await?;
    }
    let state = AppState::new(apps, selected_app)?.with_read_only(options.read_only);
    let app = server::build_router(state);
    if let Ok(address) = listener.local_addr() {
        tracing::info!(
            "Monitoring server listening on {} (read_only={}, embedded static assets: {})",
            address,
            options.read_only,
            server::embedded_asset_count()
        );
    }
    axum::serve(listener, app).await.map_err(|e| {
        rustvello_core::error::RustvelloError::Internal {
            message: format!("serve: {e}"),
        }
    })?;
    Ok(())
}
