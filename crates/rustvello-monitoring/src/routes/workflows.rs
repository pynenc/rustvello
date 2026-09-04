//! Workflow monitoring views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Redirect};
use axum::Router;

use crate::histogram::{
    build_histogram, parse_categories, serialize_categories, HistogramCategory, HistogramEntry,
    HistogramPanel,
};
use crate::state::AppState;
use crate::util::escape::xml_escape;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(Template)]
#[template(path = "workflows/list.html")]
#[allow(dead_code)]
struct WorkflowListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    workflow_types: Vec<WorkflowTypeRow>,
    workflow_runs: Vec<WorkflowRunInfo>,
}

struct WorkflowTypeRow {
    workflow_type: String,
    run_count: usize,
}

struct WorkflowRunInfo {
    invocation_id: String,
    short_id: String,
    task_id: String,
    status: String,
    status_class: String,
    child_count: usize,
}

#[derive(Template)]
#[template(path = "workflows/detail.html")]
#[allow(dead_code)]
struct WorkflowDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    workflow_type: String,
    runs: Vec<WorkflowRunRow>,
    workflow_histograms: Vec<WorkflowHistogramView>,
    histogram_selection_capped: bool,
    histogram_status: String,
}

struct WorkflowRunRow {
    workflow_id: String,
    short_id: String,
    member_count: usize,
    histogram_selected: bool,
}

struct WorkflowHistogramView {
    workflow_id: String,
    short_id: String,
    histogram: HistogramPanel,
}

#[derive(serde::Deserialize, Default)]
struct WorkflowDetailQuery {
    histogram_workflow: Option<String>,
    histogram_status: Option<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/refresh", axum::routing::get(list_refresh))
        .route("/runs", axum::routing::get(all_runs))
        .route("/children/{invocation_id}", axum::routing::get(children))
        .route("/{workflow_type}", axum::routing::get(detail))
        .route(
            "/{workflow_type}/refresh",
            axum::routing::get(detail_refresh),
        )
        .route(
            "/{workflow_type}/{workflow_id}",
            axum::routing::get(workflow_run_detail),
        )
}

async fn list(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let (workflow_types, workflow_runs) = collect_workflow_data(&app).await;

    Ok(HtmlTemplate(WorkflowListTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "workflows",
        workflow_types,
        workflow_runs,
    }))
}

#[derive(Template)]
#[template(path = "workflows/partials/list_content.html")]
struct WorkflowListContentPartial {
    workflow_types: Vec<WorkflowTypeRow>,
    workflow_runs: Vec<WorkflowRunInfo>,
}

async fn list_refresh(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let (workflow_types, workflow_runs) = collect_workflow_data(&app).await;
    Ok(HtmlTemplate(WorkflowListContentPartial {
        workflow_types,
        workflow_runs,
    }))
}

/// Collect workflow types with run counts and all workflow runs (invocations with children).
async fn collect_workflow_data(
    app: &crate::AppInstance,
) -> (Vec<WorkflowTypeRow>, Vec<WorkflowRunInfo>) {
    use rustvello_proto::status::InvocationStatus;

    let mut workflow_types = Vec::new();
    let mut workflow_runs = Vec::new();

    // Hard cap to prevent DoS from unbounded iteration.
    const MAX_WORKFLOW_RUNS: usize = 500;

    for tid in &app.task_ids {
        let inv_ids = app
            .orchestrator
            .get_invocations_by_task(tid)
            .await
            .unwrap_or_default();

        let mut run_count = 0usize;
        for inv_id in &inv_ids {
            let children = app
                .state_backend
                .get_child_invocations(inv_id)
                .await
                .unwrap_or_default();
            if !children.is_empty() {
                run_count += 1;
                if workflow_runs.len() < MAX_WORKFLOW_RUNS {
                    let status = app
                        .orchestrator
                        .get_invocation_status(inv_id)
                        .await
                        .map(|r| r.status)
                        .unwrap_or(InvocationStatus::Registered);
                    let badge = status_colors::badge_class(&status);
                    let full_id = inv_id.to_string();
                    let short = crate::util::formatting::truncate_id(&full_id);
                    workflow_runs.push(WorkflowRunInfo {
                        invocation_id: full_id,
                        short_id: short,
                        task_id: tid.to_string(),
                        status: format!("{status:?}"),
                        status_class: badge.to_owned(),
                        child_count: children.len(),
                    });
                }
            }
        }

        workflow_types.push(WorkflowTypeRow {
            workflow_type: tid.to_string(),
            run_count,
        });
    }

    (workflow_types, workflow_runs)
}

async fn all_runs(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let (_, workflow_runs) = collect_workflow_data(&app).await;
    let count = workflow_runs.len();
    let mut html =
        format!("<h5>All Workflow Runs <span class=\"badge bg-primary\">{count}</span></h5>");
    if workflow_runs.is_empty() {
        html.push_str("<p class=\"text-muted\">No workflow runs found.</p>");
    } else {
        html.push_str("<table class=\"table table-hover\"><thead><tr><th>Invocation</th><th>Task</th><th>Status</th><th>Children</th><th>Actions</th></tr></thead><tbody>");
        for run in &workflow_runs {
            html.push_str(&format!(
                "<tr><td><a href=\"/invocations/{}\">{}</a></td><td>{}</td><td><span class=\"badge {}\">{}</span></td><td>{}</td><td><a href=\"/invocations/{}\" class=\"btn btn-sm btn-outline-primary\">View</a></td></tr>",
                xml_escape(&run.invocation_id), xml_escape(&run.short_id), xml_escape(&run.task_id), xml_escape(&run.status_class), xml_escape(&run.status), run.child_count, xml_escape(&run.invocation_id)
            ));
        }
        html.push_str("</tbody></table>");
    }
    Ok(axum::response::Html(html))
}

async fn detail(
    State(state): State<AppState>,
    Path(workflow_type): Path<String>,
    Query(query): Query<WorkflowDetailQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let mut runs = collect_workflow_runs(&app, &workflow_type).await;
    let selected = select_workflow_histograms(&runs, query.histogram_workflow.as_deref());
    for run in &mut runs {
        run.histogram_selected = selected.contains(&run.workflow_id);
    }
    let categories = parse_categories(query.histogram_status.as_deref());
    let workflow_histograms =
        build_workflow_histograms(&app, &workflow_type, &runs, &selected, &categories).await;
    let histogram_status = serialize_categories(&categories);
    Ok(HtmlTemplate(WorkflowDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "workflows",
        workflow_type,
        runs,
        workflow_histograms,
        histogram_selection_capped: selected.len() > 10,
        histogram_status,
    }))
}

#[derive(Template)]
#[template(path = "workflows/partials/detail_content.html")]
#[allow(dead_code)]
struct WorkflowDetailContentPartial {
    workflow_type: String,
    runs: Vec<WorkflowRunRow>,
    workflow_histograms: Vec<WorkflowHistogramView>,
    histogram_selection_capped: bool,
    histogram_status: String,
}

async fn detail_refresh(
    State(state): State<AppState>,
    Path(workflow_type): Path<String>,
    Query(query): Query<WorkflowDetailQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let mut runs = collect_workflow_runs(&app, &workflow_type).await;
    let selected = select_workflow_histograms(&runs, query.histogram_workflow.as_deref());
    for run in &mut runs {
        run.histogram_selected = selected.contains(&run.workflow_id);
    }
    let categories = parse_categories(query.histogram_status.as_deref());
    let workflow_histograms =
        build_workflow_histograms(&app, &workflow_type, &runs, &selected, &categories).await;
    let histogram_status = serialize_categories(&categories);
    Ok(HtmlTemplate(WorkflowDetailContentPartial {
        workflow_type,
        runs,
        workflow_histograms,
        histogram_selection_capped: selected.len() > 10,
        histogram_status,
    }))
}

/// Collect individual workflow runs for a given workflow/task type.
async fn collect_workflow_runs(
    app: &crate::AppInstance,
    workflow_type: &str,
) -> Vec<WorkflowRunRow> {
    let tid: rustvello_proto::identifiers::TaskId = workflow_type
        .parse()
        .unwrap_or_else(|_| rustvello_proto::identifiers::TaskId::new(workflow_type, ""));
    let identities = app
        .state_backend
        .get_workflow_runs(&tid)
        .await
        .unwrap_or_default();
    let mut root_ids: Vec<_> = identities
        .into_iter()
        .map(|identity| identity.workflow_id)
        .collect();
    if root_ids.is_empty() {
        root_ids = app
            .orchestrator
            .get_invocations_by_task(&tid)
            .await
            .unwrap_or_default();
        let mut discovered = Vec::new();
        for invocation_id in root_ids {
            if !app
                .state_backend
                .get_child_invocations(&invocation_id)
                .await
                .unwrap_or_default()
                .is_empty()
            {
                discovered.push(invocation_id);
            }
        }
        root_ids = discovered;
    }

    let mut dated_runs = Vec::new();
    for inv_id in root_ids {
        let members = app
            .state_backend
            .get_workflow_invocations(&inv_id)
            .await
            .unwrap_or_default();
        let created_at = app
            .state_backend
            .get_invocation(&inv_id)
            .await
            .ok()
            .map_or_else(chrono::Utc::now, |invocation| invocation.created_at);
        let full_id = inv_id.to_string();
        let short = crate::util::formatting::truncate_id(&full_id);
        dated_runs.push((
            created_at,
            WorkflowRunRow {
                workflow_id: full_id,
                short_id: short,
                member_count: members.len(),
                histogram_selected: false,
            },
        ));
    }
    dated_runs.sort_by_key(|(created_at, _)| std::cmp::Reverse(*created_at));
    dated_runs.into_iter().map(|(_, run)| run).collect()
}

fn select_workflow_histograms(
    runs: &[WorkflowRunRow],
    requested: Option<&str>,
) -> std::collections::BTreeSet<String> {
    let available: std::collections::BTreeSet<&str> =
        runs.iter().map(|run| run.workflow_id.as_str()).collect();
    requested.map_or_else(
        || {
            runs.iter()
                .take(3)
                .map(|run| run.workflow_id.clone())
                .collect()
        },
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|id| available.contains(id))
                .map(str::to_owned)
                .collect()
        },
    )
}

async fn build_workflow_histograms(
    app: &crate::AppInstance,
    workflow_type: &str,
    runs: &[WorkflowRunRow],
    selected: &std::collections::BTreeSet<String>,
    categories: &std::collections::BTreeSet<HistogramCategory>,
) -> Vec<WorkflowHistogramView> {
    let mut models = Vec::new();
    for run in runs
        .iter()
        .filter(|run| selected.contains(&run.workflow_id))
        .take(10)
    {
        let workflow_id =
            rustvello_proto::identifiers::InvocationId::from_string(run.workflow_id.clone());
        let mut invocation_ids = app
            .state_backend
            .get_workflow_invocations(&workflow_id)
            .await
            .unwrap_or_default();
        if !invocation_ids.contains(&workflow_id) {
            invocation_ids.push(workflow_id);
        }
        let mut entries = Vec::new();
        let mut latest = std::collections::HashMap::new();
        for invocation_id in invocation_ids {
            let Ok(invocation) = app.state_backend.get_invocation(&invocation_id).await else {
                continue;
            };
            let history = app
                .state_backend
                .get_history(&invocation_id)
                .await
                .unwrap_or_default();
            let task_id = invocation.task_id.to_string();
            for item in &history {
                let entry = HistogramEntry::from_history(item, &task_id);
                let latest_item = latest
                    .entry(entry.invocation_id.clone())
                    .or_insert((entry.timestamp, entry.status));
                if entry.timestamp > latest_item.0 {
                    *latest_item = (entry.timestamp, entry.status);
                }
                entries.push(entry);
            }
        }
        let Some(start) = entries.iter().map(|entry| entry.timestamp).min() else {
            continue;
        };
        let has_active = latest.values().any(|(_, status)| !status.is_terminal());
        let mut end = if has_active {
            chrono::Utc::now()
        } else {
            entries
                .iter()
                .map(|entry| entry.timestamp)
                .max()
                .unwrap_or(start)
        };
        if end <= start {
            end = start + chrono::Duration::seconds(1);
        }
        let data = build_histogram(&entries, start, end, categories.clone(), None);
        let common_params = vec![
            ("workflow_id".to_owned(), run.workflow_id.clone()),
            ("workflow_type".to_owned(), workflow_type.to_owned()),
        ];
        models.push((
            run.workflow_id.clone(),
            run.short_id.clone(),
            data,
            common_params,
        ));
    }
    let shared_max = models
        .iter()
        .map(|(_, _, data, _)| data.max_count)
        .max()
        .unwrap_or_default();
    models
        .into_iter()
        .map(
            |(workflow_id, short_id, data, common_params)| WorkflowHistogramView {
                workflow_id,
                short_id,
                histogram: HistogramPanel::from_data_with_y_axis(
                    &data,
                    &common_params,
                    "/invocations",
                    true,
                    Some(shared_max),
                ),
            },
        )
        .collect()
}

/// Redirect a workflow run (identified by its root invocation_id) to the invocations detail page.
async fn workflow_run_detail(
    Path((_workflow_type, workflow_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let sanitized_id: String = workflow_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();
    Redirect::to(&format!("/invocations/{sanitized_id}"))
}

/// HTMX partial: return the child invocations of a workflow root as an inline table.
async fn children(
    State(state): State<AppState>,
    Path(invocation_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    use rustvello_proto::identifiers::InvocationId;
    use rustvello_proto::status::InvocationStatus;

    let app = get_active_app(&state)?;
    let inv_id = InvocationId::from_string(invocation_id);

    let child_ids = app
        .state_backend
        .get_child_invocations(&inv_id)
        .await
        .unwrap_or_default();

    if child_ids.is_empty() {
        return Ok(axum::response::Html(
            "<tr><td colspan=\"5\" class=\"text-muted small ps-4\">No child invocations.</td></tr>"
                .to_owned(),
        ));
    }

    let mut html = String::new();
    for child_id in &child_ids {
        let full_id = child_id.to_string();
        let short = crate::util::formatting::truncate_id(&full_id);

        // Get status
        let status = app
            .orchestrator
            .get_invocation_status(child_id)
            .await
            .map(|r| r.status)
            .unwrap_or(InvocationStatus::Registered);
        let badge = status_colors::badge_class(&status);

        // Get task_id from the invocation DTO
        let task_id = app
            .state_backend
            .get_invocation(child_id)
            .await
            .ok()
            .map_or_else(|| "unknown".to_owned(), |dto| dto.task_id.to_string());

        // Check if this child has its own children (sub-workflow)
        let grandchildren = app
            .state_backend
            .get_child_invocations(child_id)
            .await
            .unwrap_or_default();
        let has_children = !grandchildren.is_empty();

        let esc_full_id = xml_escape(&full_id);
        let esc_short = xml_escape(&short);
        let esc_task_id = xml_escape(&task_id);
        let esc_badge = xml_escape(badge);
        let esc_status = xml_escape(&format!("{status:?}"));
        let child_count = if has_children {
            grandchildren.len().to_string()
        } else {
            "—".to_owned()
        };
        html.push_str(&format!(
            "<tr class=\"table-active\"><td class=\"ps-4\">\
             <a href=\"/invocations/{esc_full_id}\" class=\"text-decoration-none\">\
             <code class=\"text-secondary\" title=\"{esc_full_id}\">&nbsp;↳ {esc_short}</code></a></td>\
             <td><a href=\"/tasks/{esc_task_id}\" class=\"text-decoration-none\">{esc_task_id}</a></td>\
             <td><span class=\"badge {esc_badge}\">{esc_status}</span></td>\
             <td>{child_count}</td>\
             <td><a href=\"/invocations/{esc_full_id}\" class=\"btn btn-sm btn-outline-secondary\">Detail</a></td></tr>",
        ));
    }

    Ok(axum::response::Html(html))
}
