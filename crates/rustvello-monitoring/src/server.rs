//! Axum router construction and server setup.

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Json, Redirect, Response};
use axum::Router;
use include_dir::{include_dir, Dir};
use std::path::PathBuf;
use tower_http::services::ServeDir;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::health::health_report;
use crate::routes;
use crate::state::AppState;
use crate::util::csrf;
use crate::util::view_helpers::AppResult;

/// The dashboard's CSS, JavaScript and logo, compiled into the binary so the
/// wheel and the CLI serve them without a source checkout.
static STATIC_ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/static");

/// Environment variable that serves `/static` from a directory on disk instead
/// of the embedded copy (front-end development without a rebuild).
pub const STATIC_DIR_ENV: &str = "RUSTVELLO_MONITORING_STATIC_DIR";

/// Build the complete Axum router with all sub-routers and middleware.
pub fn build_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/health", axum::routing::get(health_check))
        .route("/switch-app/{app_id}", axum::routing::get(switch_app));
    let router = match static_dir_override() {
        Some(dir) => router.nest_service("/static", ServeDir::new(&dir)),
        None => router.route("/static/{*path}", axum::routing::get(embedded_static)),
    };

    router
        .merge(routes::router())
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            reject_writes_when_read_only,
        ))
        .layer(middleware::from_fn(csrf::validate_origin))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}

/// Directory named by [`STATIC_DIR_ENV`], when it exists. A value that is not
/// a directory is logged and ignored, so a stale variable cannot break the UI.
fn static_dir_override() -> Option<PathBuf> {
    let value = std::env::var(STATIC_DIR_ENV).ok()?;
    let candidate = PathBuf::from(&value);
    if candidate.is_dir() {
        tracing::info!(
            "monitoring static assets served from {} ({STATIC_DIR_ENV})",
            candidate.display()
        );
        Some(candidate)
    } else {
        tracing::warn!(
            "{STATIC_DIR_ENV}={value} is not a directory; serving the embedded static assets"
        );
        None
    }
}

/// Embedded file for a `/static/<path>` request, with its content type.
pub(crate) fn embedded_asset(path: &str) -> Option<(&'static [u8], String)> {
    let file = STATIC_ASSETS.get_file(path.trim_start_matches('/'))?;
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    Some((file.contents(), mime.essence_str().to_owned()))
}

/// Number of files compiled into the binary (tests and the startup log).
pub(crate) fn embedded_asset_count() -> usize {
    fn count(dir: &Dir<'_>) -> usize {
        dir.files().count() + dir.dirs().map(count).sum::<usize>()
    }
    count(&STATIC_ASSETS)
}

async fn embedded_static(Path(path): Path<String>) -> Response {
    match embedded_asset(&path) {
        Some((bytes, mime)) => (
            [
                (header::CONTENT_TYPE, mime),
                (header::CACHE_CONTROL, "public, max-age=3600".to_owned()),
            ],
            bytes,
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "static asset not found").into_response(),
    }
}

/// In read-only mode every request that could change data is refused.
async fn reject_writes_when_read_only(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if state.read_only()
        && !matches!(
            *request.method(),
            Method::GET | Method::HEAD | Method::OPTIONS
        )
    {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"))],
            "monitor is read-only: purge, rerun and cancel are disabled",
        )
            .into_response();
    }
    next.run(request).await
}

/// `/health`: 200 with the per-component report when every backend answers,
/// 503 with the same body otherwise. No runners is reported, not an error.
async fn health_check(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = state
        .active_app()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
    let report = health_report(&app).await;
    let status = if report.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok((status, Json(report)))
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
    use super::{embedded_asset, embedded_asset_count};

    #[test]
    fn dashboard_assets_are_embedded() {
        assert!(
            embedded_asset_count() >= 7,
            "css, js and logo are compiled in"
        );
        let (css, mime) = embedded_asset("css/rustvello.css").expect("stylesheet");
        assert_eq!(mime, "text/css");
        assert!(std::str::from_utf8(css).expect("utf8").contains("--nav-bg"));
        let (_, mime) = embedded_asset("js/monitoring.js").expect("script");
        assert!(mime.contains("javascript"), "{mime}");
        let (_, mime) = embedded_asset("/logo.png").expect("leading slash tolerated");
        assert_eq!(mime, "image/png");
    }

    #[test]
    fn unknown_or_escaping_paths_are_not_served() {
        assert!(embedded_asset("css/missing.css").is_none());
        assert!(embedded_asset("../Cargo.toml").is_none());
    }
}
