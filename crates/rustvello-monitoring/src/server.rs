//! Axum router construction and server setup.

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Json, Redirect};
use axum::Router;
use tower_http::services::ServeDir;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::routes;
use crate::state::AppState;
use crate::util::csrf;
use crate::util::view_helpers::AppResult;

/// Build the complete Axum router with all sub-routers and middleware.
pub fn build_router(state: AppState) -> Router {
    let static_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static");

    Router::new()
        .route("/health", axum::routing::get(health_check))
        .route("/switch-app/{app_id}", axum::routing::get(switch_app))
        .nest_service("/static", ServeDir::new(&static_dir))
        .merge(routes::router())
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(middleware::from_fn(csrf::validate_origin))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}

async fn health_check() -> AppResult<impl IntoResponse> {
    Ok(Json(serde_json::json!({"status": "ok"})))
}

async fn switch_app(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    match state.switch_app(&app_id) {
        Ok(()) => Ok(Redirect::to("/").into_response()),
        Err(e) => Ok((StatusCode::NOT_FOUND, e.to_string()).into_response()),
    }
}
