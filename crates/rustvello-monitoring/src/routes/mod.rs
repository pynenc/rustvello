//! Route modules for the monitoring dashboard.

pub mod atomic_service;
pub mod broker;
pub mod calls;
pub mod client_data_store;
pub mod family_tree;
pub mod home;
pub mod invocations;
pub mod log_explorer;
pub mod orchestrator;
pub mod runners;
pub mod state_backend;
pub mod tasks;
pub mod workflows;

use axum::Router;

use crate::state::AppState;

/// Compose all sub-routers into the main application router.
pub fn router() -> Router<AppState> {
    Router::new()
        .merge(home::router())
        .nest("/broker", broker::router())
        .nest("/orchestrator", orchestrator::router())
        .nest("/atomic-service", atomic_service::router())
        .nest("/state-backend", state_backend::router())
        .nest("/client-data-store", client_data_store::router())
        .nest("/tasks", tasks::router())
        .nest("/runners", runners::router())
        .nest("/invocations", invocations::router())
        .nest("/calls", calls::router())
        .nest("/workflows", workflows::router())
        .nest("/log-explorer", log_explorer::router())
}
