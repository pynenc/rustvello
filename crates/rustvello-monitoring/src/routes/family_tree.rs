//! Family tree visualization endpoint.
//!
//! The main handler is nested under `/invocations/{id}/family-tree`
//! in the invocations router. This module provides the standalone
//! router for when family_tree needs its own prefix.

use axum::Router;

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    // Family tree routes are nested under /invocations/{id}/family-tree
    // in the invocations router. This is a no-op router for the module system.
    Router::new()
}
