//! Discoverable machine-facing monitoring API: capabilities, health, runners,
//! invocations by status, queue depths and the effective configuration.
//!
//! Every route here returns JSON. Agents and scripts read these instead of the
//! HTML pages; `/api/capabilities` lists them.

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::Router;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatus;

use crate::health::health_report;
use crate::query::{DEFAULT_PAGE_SIZE, PAGE_SIZES};
use crate::routes::runners::collect_runner_rows;
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult};

/// Largest `limit` accepted by `/api/invocations`.
const MAX_API_LIMIT: usize = 500;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/capabilities", axum::routing::get(capabilities))
        .route("/health", axum::routing::get(health))
        .route("/runners", axum::routing::get(runners))
        .route("/invocations", axum::routing::get(invocations))
        .route("/queues", axum::routing::get(queues))
        .route("/config", axum::routing::get(config))
}

async fn capabilities(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    Ok(axum::response::Json(serde_json::json!({
        "schema_version": 2,
        "app_id": app.app_id,
        "applications": state.app_ids().unwrap_or_default(),
        "read_only": state.read_only(),
        "pagination": {
            "default_page_size": DEFAULT_PAGE_SIZE,
            "page_sizes": PAGE_SIZES,
            "maximum_page_size": 200,
        },
        "health": {
            "path": "/health",
            "api": "/api/health",
            "status_codes": "200 when every backend answers, 503 otherwise; the body is the same per-component report",
            "cli": "python -m rustvello.doctor module:app --json",
        },
        "json": {
            "runners": "/api/runners",
            "invocations": "/api/invocations?status={status}&task_id={task_id}&limit={limit}&offset={offset}",
            "invocation_statuses": ["Registered", "Pending", "Running", "Success", "Failed", "Retry", "Paused", "Scheduled", "ConcurrencyControlled"],
            "queues": "/api/queues",
            "config": "/api/config",
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
            "HTML list endpoints are bounded but are intended for humans; /api/* and the investigation endpoints return JSON."
        ]
    })))
}

/// Same report and status codes as `/health`, under the `/api` prefix.
async fn health(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let report = health_report(&app).await;
    let status = if report.is_ok() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok((status, Json(report)))
}

/// Active runners with class, language, host, pid and heartbeat age.
async fn runners(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let rows = collect_runner_rows(&app).await;
    Ok(Json(serde_json::json!({
        "app_id": app.app_id,
        "dead_after_seconds": app.config.runner_dead_after_seconds,
        "heartbeat_interval_seconds": app.config.heartbeat_interval_seconds,
        "count": rows.len(),
        "runners": rows,
    })))
}

#[derive(serde::Deserialize, Default)]
pub struct InvocationsQuery {
    /// Status name, for example `Failed`; omitted means every status.
    pub status: Option<String>,
    /// Canonical task id, for example `python::orders.process`.
    pub task_id: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Invocations by status as compact JSON records (newest first as the
/// orchestrator stores them). Bounded by `limit` (default page size, at most
/// `MAX_API_LIMIT`).
async fn invocations(
    State(state): State<AppState>,
    Query(query): Query<InvocationsQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let statuses: Option<Vec<InvocationStatus>> = match query.status.as_deref() {
        None | Some("") => None,
        Some(raw) => match raw.parse::<InvocationStatus>() {
            Ok(status) => Some(vec![status]),
            Err(_) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("unknown status {raw:?}")})),
                )
                    .into_response())
            }
        },
    };
    let task_id =
        match query.task_id.as_deref() {
            None | Some("") => None,
            Some(raw) => match raw.parse::<TaskId>() {
                Ok(task_id) => Some(task_id),
                Err(error) => return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": format!("invalid task_id {raw:?}: {error}")})),
                )
                    .into_response()),
            },
        };
    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_API_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let ids = match app
        .orchestrator
        .get_invocation_ids_paginated(task_id.as_ref(), statuses.as_deref(), limit, offset)
        .await
    {
        Ok(ids) => ids,
        Err(error) => {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": error.to_string()})),
            )
                .into_response())
        }
    };
    let mut records = Vec::with_capacity(ids.len());
    for id in &ids {
        let status = app
            .orchestrator
            .get_invocation_status(id)
            .await
            .ok()
            .map(|record| format!("{:?}", record.status));
        let mut record = serde_json::json!({
            "invocation_id": id.to_string(),
            "status": status,
            "investigation": format!("/invocations/{id}/investigation"),
        });
        if let Ok(inv) = app.state_backend.get_invocation(id).await {
            record["task_id"] = serde_json::Value::String(inv.task_id.to_string());
            record["created_at"] = serde_json::Value::String(inv.created_at.to_rfc3339());
            record["updated_at"] = serde_json::Value::String(inv.updated_at.to_rfc3339());
            record["parent_invocation_id"] = inv
                .parent_invocation_id
                .as_ref()
                .map_or(serde_json::Value::Null, |p| {
                    serde_json::Value::String(p.to_string())
                });
            if record["status"].is_null() {
                record["status"] = serde_json::Value::String(format!("{:?}", inv.status));
            }
        }
        records.push(record);
    }
    Ok(Json(serde_json::json!({
        "app_id": app.app_id,
        "status": query.status,
        "task_id": query.task_id,
        "limit": limit,
        "offset": offset,
        "count": records.len(),
        "invocations": records,
    }))
    .into_response())
}

/// Queued invocations per declared broker queue.
async fn queues(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let mut depths = serde_json::Map::new();
    for queue in &app.config.broker_queues {
        match app
            .broker
            .count_invocations_in_queues(std::slice::from_ref(queue), None)
            .await
        {
            Ok(depth) => {
                depths.insert(queue.clone(), serde_json::json!(depth));
            }
            Err(error) => {
                return Ok((
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": error.to_string(), "queue": queue})),
                )
                    .into_response())
            }
        }
    }
    let total = app.broker.count_invocations(None).await.unwrap_or(0);
    Ok(Json(serde_json::json!({
        "app_id": app.app_id,
        "queues": depths,
        "total": total,
        "runner_queues": app.config.runner_queues,
    }))
    .into_response())
}

/// The effective `AppConfig` of the active app plus the backend
/// implementations, so a deployment can be confirmed from a script.
async fn config(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    Ok(Json(serde_json::json!({
        "app_id": app.app_id,
        "read_only": state.read_only(),
        "implementations": {
            "broker": app.broker.backend_name(),
            "orchestrator": app.orchestrator.backend_name(),
            "state_backend": app.state_backend.backend_name(),
            "client_data_store": app.client_data_store.backend_name(),
        },
        "tasks": app.task_ids.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "config": app.config,
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
