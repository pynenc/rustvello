//! Discoverable machine-facing monitoring API metadata.

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::query::{DEFAULT_PAGE_SIZE, PAGE_SIZES};
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult};

pub fn router() -> Router<AppState> {
    Router::new().route("/capabilities", axum::routing::get(capabilities))
}

async fn capabilities(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    Ok(axum::response::Json(serde_json::json!({
        "schema_version": 1,
        "app_id": app.app_id,
        "applications": state.app_ids().unwrap_or_default(),
        "pagination": {
            "default_page_size": DEFAULT_PAGE_SIZE,
            "page_sizes": PAGE_SIZES,
            "maximum_page_size": 200,
        },
        "investigation": {
            "invocation": "/invocations/{invocation_id}/investigation",
            "invocation_record": "/invocations/{invocation_id}/api",
            "invocation_history": "/invocations/{invocation_id}/history",
            "cli": "rustvello investigate <invocation-id> --app-id <app-id> --db-path <sqlite-db> --format json",
        },
        "lists": {
            "invocations": "/invocations?page={page}&limit={limit}",
            "tasks": "/tasks?page={page}&limit={limit}",
            "workflows": "/workflows?page={page}&limit={limit}",
            "workflow_runs": "/workflows/{workflow_type}?page={page}&limit={limit}",
            "runners": "/runners",
            "events": "/events?page={page}&limit={limit}",
            "atomic_service": "/atomic-service?page={page}&limit={limit}",
        },
        "timeline": {
            "path": "/invocations/timeline",
            "filters": [
                "time_range", "start_date", "end_date", "task_id",
                "workflow_type", "workflow_id", "inv_ids", "runner_ids",
                "selected", "limit", "histogram_status"
            ],
            "timestamps": "RFC3339; positive offsets must be percent-encoded",
            "selection_target_fill": 0.82,
        },
        "guarantees": guarantees(&app),
        "notes": [
            "Use the investigation endpoint first when an invocation id is known.",
            "Registration runner and execution worker can differ.",
            "HTML list endpoints are bounded but are intended for humans; investigation endpoints return JSON."
        ]
    })))
}

/// Declared guarantees of the active app's backend plus the full matrix.
fn guarantees(app: &crate::AppInstance) -> serde_json::Value {
    let profile = app.orchestrator.guarantee_profile();
    let publication = app.orchestrator.runtime_publication().map(|p| p.domain());
    let atomic_publication = publication.is_some_and(|domain| {
        app.broker.publication_domain().as_ref() == Some(&domain)
            && app.state_backend.publication_domain().as_ref() == Some(&domain)
    });
    serde_json::json!({
        "schema_version": 1,
        "documentation": "docs/guarantees.md",
        "active": {
            "profile": profile,
            "declared": profile.and_then(rustvello_core::guarantees::backend_guarantees),
            "effective_atomic_publication": atomic_publication,
            "atomic_trigger_claims": app.trigger_store.as_ref().map(|store| store.atomic_trigger_claims()),
        },
        "matrix": rustvello_core::guarantees::guarantee_matrix(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertised_page_sizes_match_public_contract() {
        assert_eq!(PAGE_SIZES, [25, 50, 100, 200]);
        assert!(PAGE_SIZES.contains(&DEFAULT_PAGE_SIZE));
    }
}
