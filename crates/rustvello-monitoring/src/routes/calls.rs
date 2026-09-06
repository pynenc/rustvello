//! Call detail views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope};
use crate::query::{load_invocation_rows, PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::{InvocationRowView, PaginationView};

#[derive(serde::Deserialize, Default)]
pub struct CallQuery {
    pub call_id_key: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
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
    invocations: Vec<InvocationRowView>,
    invocation_count: usize,
    timeline_url: String,
    pagination: PaginationView,
    pagination_path: String,
    pagination_query: String,
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
    render_call_detail(state, &call_id_key, query.page, query.limit).await
}

async fn detail_by_path(
    State(state): State<AppState>,
    Path(call_id_key): Path<String>,
    Query(query): Query<CallQuery>,
) -> AppResult<impl IntoResponse> {
    render_call_detail(state, &call_id_key, query.page, query.limit).await
}

async fn render_call_detail(
    state: AppState,
    call_id_key: &str,
    page: Option<usize>,
    limit: Option<usize>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(page, limit);

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
                found_invocations = call_invs;
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
                        found_invocations = call_invocations;
                        break 'outer;
                    }
                }
            }
        }
    }

    let invocation_count = found_invocations.len();
    let page_ids = found_invocations
        .into_iter()
        .skip(page_request.offset())
        .take(page_request.limit)
        .collect::<Vec<_>>();
    let scope = MonitoringScope {
        task_id: (!found_task_id.is_empty()).then_some(found_task_id.clone()),
        invocation_ids: page_ids.iter().map(ToString::to_string).collect(),
        ..MonitoringScope::default()
    };
    let invocations = load_invocation_rows(&app, page_ids, scope.clone()).await;
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(scope)
        .href();
    Ok(HtmlTemplate(CallDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "invocations",
        call_id: call_id_key.to_owned(),
        task_id: found_task_id,
        invocations,
        invocation_count,
        timeline_url,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(invocation_count),
            page_request.offset() + page_request.limit < invocation_count,
        ),
        pagination_path: format!("/calls/{call_id_key}"),
        pagination_query: format!("limit={}", page_request.limit),
    }))
}
