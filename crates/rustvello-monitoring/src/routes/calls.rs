//! Call detail views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(serde::Deserialize, Default)]
pub struct CallQuery {
    pub call_id_key: Option<String>,
}

#[derive(Template)]
#[template(path = "calls/detail.html")]
#[allow(dead_code)]
struct CallDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    call_id: String,
    task_id: String,
    invocation_ids: Vec<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(detail_by_query))
        .route("/{call_id_key}", axum::routing::get(detail_by_path))
}

async fn detail_by_query(
    State(state): State<AppState>,
    Query(query): Query<CallQuery>,
) -> AppResult<impl IntoResponse> {
    let call_id_key = query.call_id_key.unwrap_or_default();
    render_call_detail(state, &call_id_key).await
}

async fn detail_by_path(
    State(state): State<AppState>,
    Path(call_id_key): Path<String>,
) -> AppResult<impl IntoResponse> {
    render_call_detail(state, &call_id_key).await
}

async fn render_call_detail(state: AppState, call_id_key: &str) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;

    let mut found_task_id = String::new();
    let mut found_invocations = Vec::new();

    // Fast path: try exact CallId parse → direct lookup (O(1))
    if let Ok(exact_call_id) = call_id_key.parse::<rustvello_proto::identifiers::CallId>() {
        if let Ok(call_invs) = app
            .orchestrator
            .get_invocations_by_call(&exact_call_id)
            .await
        {
            if !call_invs.is_empty() {
                found_task_id = exact_call_id.task_id.to_string();
                found_invocations = call_invs.into_iter().map(|id| id.to_string()).collect();
            }
        }
    }

    // Slow path: substring scan (only if exact lookup missed)
    if found_invocations.is_empty() {
        const MAX_SCAN: usize = 5000;
        let mut scanned = 0usize;
        'outer: for tid in &app.task_ids {
            let inv_ids = app
                .orchestrator
                .get_invocations_by_task(tid)
                .await
                .unwrap_or_default();
            for inv_id in &inv_ids {
                scanned += 1;
                if scanned > MAX_SCAN {
                    break 'outer;
                }
                if let Ok(inv) = app.state_backend.get_invocation(inv_id).await {
                    if inv.call_id.to_string().contains(call_id_key) {
                        found_task_id = tid.to_string();
                        let call_invocations = app
                            .orchestrator
                            .get_invocations_by_call(&inv.call_id)
                            .await
                            .unwrap_or_default();
                        found_invocations = call_invocations
                            .into_iter()
                            .map(|id| id.to_string())
                            .collect();
                        break 'outer;
                    }
                }
            }
        }
    }

    Ok(HtmlTemplate(CallDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "invocations",
        call_id: call_id_key.to_owned(),
        task_id: found_task_id,
        invocation_ids: found_invocations,
    }))
}
