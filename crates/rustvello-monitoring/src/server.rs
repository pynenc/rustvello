//! Axum router construction and server setup.

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Json, Redirect};
use axum::Router;
use std::path::{Path as FsPath, PathBuf};
use tower_http::services::ServeDir;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::routes;
use crate::state::AppState;
use crate::util::csrf;
use crate::util::view_helpers::AppResult;

/// Build the complete Axum router with all sub-routers and middleware.
pub fn build_router(state: AppState) -> Router {
    let static_dir = monitoring_static_dir();

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

fn monitoring_static_dir() -> PathBuf {
    if let Ok(path) = std::env::var("RUSTVELLO_MONITORING_STATIC_DIR") {
        let candidate = PathBuf::from(path);
        if candidate.is_dir() {
            return candidate;
        }
    }

    let cwd = std::env::current_dir().ok();
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(FsPath::to_path_buf));
    let manifest_dir = Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")));

    resolve_monitoring_static_dir(cwd.as_deref(), exe_dir.as_deref(), manifest_dir.as_deref())
}

fn resolve_monitoring_static_dir(
    cwd: Option<&FsPath>,
    exe_dir: Option<&FsPath>,
    manifest_dir: Option<&FsPath>,
) -> PathBuf {
    let mut candidates = Vec::new();

    if let Some(cwd) = cwd {
        add_static_dir_candidates(&mut candidates, cwd);
    }
    if let Some(exe_dir) = exe_dir {
        add_static_dir_candidates(&mut candidates, exe_dir);
    }
    if let Some(manifest_dir) = manifest_dir {
        candidates.push(manifest_dir.join("static"));
    }

    candidates
        .into_iter()
        .find(|candidate| candidate.is_dir())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("static"))
}

fn add_static_dir_candidates(candidates: &mut Vec<PathBuf>, base: &FsPath) {
    for ancestor in base.ancestors() {
        candidates.push(ancestor.join("crates/rustvello-monitoring/static"));
        candidates.push(ancestor.join("rustvello/crates/rustvello-monitoring/static"));
        candidates.push(ancestor.join("repos/rustvello/crates/rustvello-monitoring/static"));
    }
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

#[cfg(test)]
mod tests {
    use super::resolve_monitoring_static_dir;

    #[test]
    fn static_dir_resolves_after_workspace_repo_move() {
        let temp = tempfile::tempdir().expect("temp workspace");
        let moved_static_dir = temp
            .path()
            .join("repos/rustvello/crates/rustvello-monitoring/static");
        std::fs::create_dir_all(&moved_static_dir).expect("static dir");

        let old_manifest_dir = temp.path().join("rustvello/crates/rustvello-monitoring");
        let resolved = resolve_monitoring_static_dir(
            Some(&temp.path().join("repos/rustvello")),
            None,
            Some(&old_manifest_dir),
        );

        assert_eq!(resolved, moved_static_dir);
    }
}
