//! Task listing and detail views.

use askama::Template;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(Template)]
#[template(path = "tasks/list.html")]
#[allow(dead_code)]
struct TaskListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    tasks: Vec<TaskInfo>,
}

struct TaskInfo {
    task_id: String,
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
    invocation_ids: Vec<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/refresh", axum::routing::get(list_refresh))
        .route("/{task_id}", axum::routing::get(detail))
}

async fn list(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let mut tasks = Vec::new();
    for tid in &app.task_ids {
        let count = app
            .orchestrator
            .get_invocations_by_task(tid)
            .await
            .map(|ids| ids.len())
            .unwrap_or(0);
        tasks.push(TaskInfo {
            task_id: tid.to_string(),
            invocation_count: count,
        });
    }
    Ok(HtmlTemplate(TaskListTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "tasks",
        tasks,
    }))
}

#[derive(Template)]
#[template(path = "tasks/partials/list_content.html")]
struct TaskListContentPartial {
    tasks: Vec<TaskInfo>,
}

async fn list_refresh(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let mut tasks = Vec::new();
    for tid in &app.task_ids {
        let count = app
            .orchestrator
            .get_invocations_by_task(tid)
            .await
            .map(|ids| ids.len())
            .unwrap_or(0);
        tasks.push(TaskInfo {
            task_id: tid.to_string(),
            invocation_count: count,
        });
    }
    Ok(HtmlTemplate(TaskListContentPartial { tasks }))
}

async fn detail(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    // Find matching task
    let tid = app.task_ids.iter().find(|t| t.to_string() == task_id);
    let invocation_ids = if let Some(tid) = tid {
        app.orchestrator
            .get_invocations_by_task(tid)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|id| id.to_string())
            .collect()
    } else {
        Vec::new()
    };

    Ok(HtmlTemplate(TaskDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "tasks",
        task_id,
        invocation_ids,
    }))
}
