//! State backend monitoring views.

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(Template)]
#[template(path = "state_backend/overview.html")]
#[allow(dead_code)]
struct StateBackendOverviewTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    backend_name: &'static str,
    usage_stats: Vec<(&'static str, String)>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(overview))
        .route("/purge", axum::routing::post(purge))
}

async fn overview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let backend_name = app.state_backend.backend_name();
    let usage_stats = app.state_backend.usage_stats().await;
    Ok(HtmlTemplate(StateBackendOverviewTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "state_backend",
        backend_name,
        usage_stats,
    }))
}

async fn purge(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    if let Err(e) = app.state_backend.purge().await {
        tracing::error!(error = %e, "state backend purge failed");
    }
    Ok(axum::response::Redirect::to("/state-backend"))
}
