//! Client data store monitoring views.

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(Template)]
#[template(path = "client_data_store/overview.html")]
#[allow(dead_code)]
struct ClientDataStoreOverviewTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    backend_name: &'static str,
    usage_stats: Vec<(&'static str, String)>,
    config_disabled: bool,
    config_min_size: usize,
    config_max_size: usize,
    config_cache_size: usize,
    config_warn_threshold: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(overview))
        .route("/purge", axum::routing::post(purge))
}

async fn overview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let backend_name = app.client_data_store.backend_name();
    let usage_stats = app.client_data_store.usage_stats().await;
    let config = app.client_data_store.config();
    let warn_threshold = if config.warn_threshold >= 1_048_576 {
        format!("{} MB", config.warn_threshold / 1_048_576)
    } else if config.warn_threshold >= 1024 {
        format!("{} KB", config.warn_threshold / 1024)
    } else {
        format!("{} B", config.warn_threshold)
    };
    Ok(HtmlTemplate(ClientDataStoreOverviewTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "client_data_store",
        backend_name,
        usage_stats,
        config_disabled: config.disabled,
        config_min_size: config.min_size_to_cache,
        config_max_size: config.max_size_to_cache,
        config_cache_size: config.local_cache_size,
        config_warn_threshold: warn_threshold,
    }))
}

async fn purge(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    if let Err(e) = app.client_data_store.purge().await {
        tracing::error!(error = %e, "client data store purge failed");
    }
    Ok(axum::response::Redirect::to("/client-data-store"))
}
