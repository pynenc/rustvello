//! View helper functions.

use askama::Template;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};

use super::escape::xml_escape;
use crate::state::AppState;
use crate::AppInstance;

/// Result type for route handlers that use `get_active_app`.
pub type AppResult<T> = Result<T, axum::response::Response>;

/// Wrapper that converts any Askama `Template` into an Axum `IntoResponse`.
pub struct HtmlTemplate<T: Template>(pub T);

impl<T: Template> IntoResponse for HtmlTemplate<T> {
    fn into_response(self) -> axum::response::Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => {
                tracing::error!(error = %err, "template rendering failed");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error").into_response()
            }
        }
    }
}

/// Render an error page as an HTML response with the given status code.
pub fn render_error(status: StatusCode, message: &str) -> axum::response::Response {
    let escaped = xml_escape(message);
    (
        status,
        axum::response::Html(format!(
            r#"<div class="alert alert-danger" role="alert">
                <h4 class="alert-heading">Error {}</h4>
                <p>{}</p>
            </div>"#,
            status.as_u16(),
            escaped
        )),
    )
        .into_response()
}

/// Get the active app instance, returning an error response on failure.
pub fn get_active_app(state: &AppState) -> Result<AppInstance, axum::response::Response> {
    state.active_app().map_err(|e| {
        tracing::error!(error = %e, "failed to get active app");
        render_error(StatusCode::INTERNAL_SERVER_ERROR, "No active application")
    })
}
