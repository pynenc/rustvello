//! Task listing and detail views.

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
struct TaskDetailQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(serde::Deserialize, Default)]
struct TaskListQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Template)]
#[template(path = "tasks/list.html")]
#[allow(dead_code)]
struct TaskListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    tasks: Vec<TaskInfo>,
    total_tasks: usize,
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
}

struct TaskInfo {
    task_id: String,
    language: String,
    invocation_count: usize,
}

#[derive(Template)]
#[template(path = "tasks/detail.html")]
#[allow(dead_code)]
struct TaskDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    task_id: String,
    language: String,
    module: String,
    function: String,
    invocations: Vec<InvocationRowView>,
    invocation_count: usize,
    timeline_url: String,
    invocations_url: String,
    pagination: PaginationView,
    pagination_path: String,
    pagination_query: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/refresh", axum::routing::get(list_refresh))
        .route("/{task_id}", axum::routing::get(detail))
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<TaskListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(query.page, query.limit);
    let total_tasks = app.task_ids.len();
    let tasks = collect_task_rows(&app, page_request).await;
    Ok(HtmlTemplate(TaskListTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "tasks",
        tasks,
        total_tasks,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(total_tasks),
            page_request.offset() + page_request.limit < total_tasks,
        ),
        pagination_path: "/tasks",
        pagination_query: format!("limit={}", page_request.limit),
    }))
}

#[derive(Template)]
#[template(path = "tasks/partials/list_content.html")]
struct TaskListContentPartial {
    tasks: Vec<TaskInfo>,
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
}

async fn list_refresh(
    State(state): State<AppState>,
    Query(query): Query<TaskListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(query.page, query.limit);
    let total_tasks = app.task_ids.len();
    let tasks = collect_task_rows(&app, page_request).await;
    Ok(HtmlTemplate(TaskListContentPartial {
        tasks,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(total_tasks),
            page_request.offset() + page_request.limit < total_tasks,
        ),
        pagination_path: "/tasks",
        pagination_query: format!("limit={}", page_request.limit),
    }))
}

async fn collect_task_rows(app: &crate::AppInstance, page_request: PageRequest) -> Vec<TaskInfo> {
    let mut tasks = Vec::new();
    for tid in app
        .task_ids
        .iter()
        .skip(page_request.offset())
        .take(page_request.limit)
    {
        let count = app
            .orchestrator
            .count_invocations(Some(tid), None)
            .await
            .unwrap_or(0);
        tasks.push(TaskInfo {
            task_id: tid.to_string(),
            language: tid.language().to_string(),
            invocation_count: count,
        });
    }
    tasks
}

async fn detail(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(query): Query<TaskDetailQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(query.page, query.limit);
    // Find matching task
    let tid = app.task_ids.iter().find(|t| t.to_string() == task_id);
    let (language, module, function, invocation_ids, invocation_count) = if let Some(tid) = tid {
        let language = tid.language().to_string();
        let invocation_count = app
            .orchestrator
            .count_invocations(Some(tid), None)
            .await
            .unwrap_or(0);
        let invocation_ids = app
            .orchestrator
            .get_invocation_ids_paginated(
                Some(tid),
                None,
                page_request.limit,
                page_request.offset(),
            )
            .await
            .unwrap_or_default();
        (
            language,
            tid.module().to_owned(),
            tid.name().to_owned(),
            invocation_ids,
            invocation_count,
        )
    } else {
        (String::new(), String::new(), String::new(), Vec::new(), 0)
    };
    let scope = MonitoringScope {
        task_id: Some(task_id.clone()),
        ..MonitoringScope::default()
    };
    let invocations = load_invocation_rows(&app, invocation_ids, scope.clone()).await;
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(scope.clone())
        .href();
    let invocations_url = MonitoringLink::new(MonitoringDestination::InvocationList)
        .with_scope(scope)
        .with_limit(page_request.limit)
        .href();

    Ok(HtmlTemplate(TaskDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "tasks",
        task_id: task_id.clone(),
        language,
        module,
        function,
        invocations,
        invocation_count,
        timeline_url,
        invocations_url,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(invocation_count),
            page_request.offset() + page_request.limit < invocation_count,
        ),
        pagination_path: format!("/tasks/{task_id}"),
        pagination_query: format!("limit={}", page_request.limit),
    }))
}
