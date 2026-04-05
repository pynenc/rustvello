//! Web-based monitoring dashboard for Rustvello.
//!
//! Provides a browser-based UI for inspecting invocations, runners,
//! workflows, and task execution timelines. Built with Axum + Askama + HTMX.

pub mod data;
pub mod family_tree;
pub mod log_explorer;
pub mod routes;
pub mod server;
pub mod state;
pub mod svg;
pub mod util;

use std::net::SocketAddr;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::TaskId;

use crate::state::AppState;

/// Configuration for the monitoring server.
#[derive(Debug, Clone)]
pub struct MonitorConfig {
    /// Address to bind the HTTP server to.
    pub bind: SocketAddr,
    /// Log level filter (e.g. "info", "debug").
    pub log_level: String,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 8000)),
            log_level: "info".to_owned(),
        }
    }
}

/// A named application instance for multi-app monitoring.
#[derive(Clone)]
pub struct AppInstance {
    pub app_id: String,
    pub config: AppConfig,
    pub broker: Arc<dyn Broker>,
    pub orchestrator: Arc<dyn Orchestrator>,
    pub state_backend: Arc<dyn StateBackend>,
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
    let state = AppState::new(apps, selected_app)?;
    let app = server::build_router(state);
    let listener = tokio::net::TcpListener::bind(monitor_config.bind)
        .await
        .map_err(|e| rustvello_core::error::RustvelloError::Internal {
            message: format!("bind: {e}"),
        })?;
    tracing::info!("Monitoring server listening on {}", monitor_config.bind);
    axum::serve(listener, app).await.map_err(|e| {
        rustvello_core::error::RustvelloError::Internal {
            message: format!("serve: {e}"),
        }
    })?;
    Ok(())
}
