//! Example: start a monitoring server with seeded test data.
//!
//! Run with:
//!   cargo run -p monitoring-server-example
//!
//! Then open <http://127.0.0.1:8000> in your browser to explore the dashboard.
//! The server stays running until you press Ctrl-C.

use std::collections::HashMap;
use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_core::error::RustvelloResult;
use rustvello_mem::broker::MemBroker;
use rustvello_mem::client_data_store::MemClientDataStore;
use rustvello_mem::orchestrator::MemOrchestrator;
use rustvello_mem::state_backend::MemStateBackend;
use rustvello_monitoring::{start_monitor, AppInstance, MonitorConfig};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::{AppConfig, ClientDataStoreConfig};
use rustvello_proto::identifiers::TaskId;

/// Seed invocations in various statuses so the dashboard has data to display.
async fn seed_data(app: &RustvelloApp) -> RustvelloResult<()> {
    // Submit a few invocations via the low-level API
    let task_id = TaskId::new("example", "process_order");
    for i in 0..5 {
        let mut args = SerializedArguments::new();
        args.insert("order_id", format!("ORD-{i:04}"));
        let inv_id = app.submit(&task_id, args).await?;
        tracing::info!("Submitted invocation: {inv_id}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Build a RustvelloApp with in-memory backends
    let config = AppConfig::new("demo-app");
    let broker: Arc<dyn rustvello_core::broker::Broker> = Arc::new(MemBroker::new());
    let orchestrator: Arc<dyn rustvello_core::orchestrator::Orchestrator> =
        Arc::new(MemOrchestrator::new());
    let state_backend: Arc<dyn rustvello_core::state_backend::StateBackend> =
        Arc::new(MemStateBackend::new());
    let cds: Arc<dyn ClientDataStore> = Arc::new(MemClientDataStore::new());
    let client_data_store = Arc::new(ClientDataStoreManager::new(
        cds,
        ClientDataStoreConfig::default(),
    ));

    let mut app = RustvelloApp::with_backends(
        config.clone(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&client_data_store),
    );

    // Register a simple task so submit works
    app.register_task(
        TaskId::new("example", "process_order"),
        rustvello_proto::config::TaskConfig::default(),
        Arc::new(|args_json: String| {
            let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| {
                rustvello_core::error::RustvelloError::Serialization {
                    message: e.to_string(),
                }
            })?;
            Ok(format!("processed: {}", args))
        }),
    )?;

    // Seed some test data
    seed_data(&app).await?;

    // Build the AppInstance for monitoring
    let task_ids: Vec<TaskId> = app.task_registry.task_ids().into_iter().cloned().collect();
    let instance = AppInstance {
        app_id: config.app_id.clone(),
        config: config.clone(),
        broker,
        orchestrator,
        state_backend,
        client_data_store,
        task_ids,
    };

    let mut apps = HashMap::new();
    apps.insert(instance.app_id.clone(), instance);

    // Optionally run a TaskRunner in the background to process invocations
    let runner = app.into_runner();
    let runner_handle = tokio::spawn(async move {
        tracing::info!("Runner started — processing invocations");
        if let Err(e) = runner.run().await {
            tracing::error!("Runner error: {e}");
        }
    });

    // Start the monitoring server (blocks until shutdown)
    let monitor_config = MonitorConfig {
        bind: ([127, 0, 0, 1], 8000).into(),
        log_level: "info".to_owned(),
    };

    tracing::info!("Starting monitoring server at http://127.0.0.1:8000");
    tracing::info!("Press Ctrl-C to stop");

    tokio::select! {
        result = start_monitor(apps, "demo-app", monitor_config) => {
            if let Err(e) = result {
                tracing::error!("Monitor error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Shutting down...");
        }
    }

    runner_handle.abort();
    Ok(())
}
