//! CSRF protection middleware via Origin/Referer validation.
//!
//! For POST requests, validates that the `Origin` or `Referer` header matches
//! the request's `Host` header. Rejects cross-origin POSTs with 403 Forbidden.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Middleware that validates Origin/Referer on non-GET/HEAD/OPTIONS requests.
pub async fn validate_origin(request: Request<Body>, next: Next) -> Response {
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return next.run(request).await;
    }

    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let origin_ok = if let Some(origin) = request
        .headers()
        .get("origin")
        .and_then(|v| v.to_str().ok())
    {
        origin_matches_host(origin, host)
    } else if let Some(referer) = request
        .headers()
        .get("referer")
        .and_then(|v| v.to_str().ok())
    {
        referer_matches_host(referer, host)
    } else {
        // No Origin or Referer — reject the request. Browser form submissions
        // always include at least one of these headers.
        false
    };

    if origin_ok {
        next.run(request).await
    } else {
        (StatusCode::FORBIDDEN, "CSRF validation failed").into_response()
    }
}

/// Check if the Origin header value matches the expected host.
fn origin_matches_host(origin: &str, host: &str) -> bool {
    // Origin format: "scheme://host[:port]" or "null"
    if origin == "null" {
        return false;
    }
    origin
        .split("://")
        .nth(1)
        .is_some_and(|origin_host| origin_host == host)
}

/// Check if the Referer header's host matches the expected host.
fn referer_matches_host(referer: &str, host: &str) -> bool {
    // Referer format: "scheme://host[:port]/path..."
    referer
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .is_some_and(|referer_host| referer_host == host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_matches() {
        assert!(origin_matches_host(
            "http://localhost:8000",
            "localhost:8000"
        ));
        assert!(origin_matches_host("https://example.com", "example.com"));
        assert!(!origin_matches_host("https://evil.com", "example.com"));
        assert!(!origin_matches_host("null", "localhost:8000"));
    }

    #[test]
    fn referer_matches() {
        assert!(referer_matches_host(
            "http://localhost:8000/broker/purge",
            "localhost:8000"
        ));
        assert!(!referer_matches_host(
            "http://evil.com/broker/purge",
            "localhost:8000"
        ));
    }
}
