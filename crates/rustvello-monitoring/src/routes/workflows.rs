//! Workflow monitoring views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Redirect};
use axum::Router;

use crate::histogram::{
    build_histogram, parse_categories, serialize_categories, HistogramCategory, HistogramEntry,
    HistogramPanel,
};
use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope};
use crate::query::{PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::escape::xml_escape;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::PaginationView;

const MAX_COMPARISON_RUNS: usize = 10;
const MAX_COMPARISON_MEMBERS: usize = 2_000;

#[derive(Template)]
#[template(path = "workflows/list.html")]
#[allow(dead_code)]
struct WorkflowListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    workflow_types: Vec<WorkflowTypeRow>,
    workflow_runs: Vec<WorkflowRunInfo>,
    total_workflow_runs: usize,
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
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
    member_count: usize,
    invocations_url: String,
    timeline_url: String,
}

#[derive(serde::Deserialize, Default)]
struct WorkflowListQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

#[derive(Template)]
#[template(path = "workflows/detail.html")]
#[allow(dead_code)]
struct WorkflowDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    workflow_type: String,
    workflow_task_id: String,
    runs: Vec<WorkflowRunRow>,
    total_runs: usize,
    pagination: PaginationView,
    pagination_path: String,
    pagination_query: String,
    selected_workflow_ids: String,
    workflow_histograms: Vec<WorkflowHistogramView>,
    histogram_selection_capped: bool,
    histogram_status: String,
    limit: usize,
}

#[derive(Clone)]
struct WorkflowRunRow {
    workflow_id: String,
    short_id: String,
    member_count: usize,
    worker_count: usize,
    duration_ms: i64,
    duration: String,
    histogram_selected: bool,
    selection_url: String,
    invocations_url: String,
    timeline_url: String,
    root_invocation_url: String,
    started: String,
}

struct WorkflowHistogramView {
    workflow_id: String,
    histogram: HistogramPanel,
    duration: String,
    member_count: usize,
    timeline_url: String,
    truncated: bool,
}

#[derive(serde::Deserialize, Default)]
struct WorkflowDetailQuery {
    histogram_workflow: Option<String>,
    histogram_status: Option<String>,
    page: Option<usize>,
    limit: Option<usize>,
}

fn workflow_run_urls(
    workflow_type: &str,
    workflow_id: &str,
    limit: usize,
) -> (String, String, String) {
    let invocations_url = MonitoringLink::new(MonitoringDestination::InvocationList)
        .with_scope(MonitoringScope::default().with_workflow(workflow_type, workflow_id))
        .with_limit(limit)
        .href();
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(MonitoringScope::default().with_workflow(workflow_type, workflow_id))
        .href();
    let root_invocation_url = MonitoringLink::new(MonitoringDestination::InvocationDetail(
        workflow_id.to_owned(),
    ))
    .href();
    (invocations_url, timeline_url, root_invocation_url)
}

fn workflow_selection_url(
    workflow_type: &str,
    selected: &std::collections::BTreeSet<String>,
    toggled_workflow_id: &str,
    histogram_status: &str,
    limit: usize,
    page: usize,
) -> String {
    let mut next = selected.clone();
    if !next.remove(toggled_workflow_id) {
        next.insert(toggled_workflow_id.to_owned());
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair(
        "histogram_workflow",
        &next.into_iter().collect::<Vec<_>>().join(","),
    );
    if !histogram_status.is_empty() {
        serializer.append_pair("histogram_status", histogram_status);
    }
    serializer.append_pair("limit", &limit.to_string());
    serializer.append_pair("page", &page.to_string());
    format!("/workflows/{workflow_type}?{}", serializer.finish())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/refresh", axum::routing::get(list_refresh))
        .route("/runs", axum::routing::get(all_runs))
        .route("/children/{invocation_id}", axum::routing::get(children))
        .route("/{workflow_type}", axum::routing::get(detail))
        .route(
            "/{workflow_type}/{workflow_id}",
            axum::routing::get(workflow_run_detail),
        )
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<WorkflowListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(query.page, query.limit);
    let (workflow_types, workflow_runs, total_workflow_runs) =
        collect_workflow_data(&app, page_request).await;

    Ok(HtmlTemplate(WorkflowListTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "workflows",
        workflow_types,
        workflow_runs,
        total_workflow_runs,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(total_workflow_runs),
            page_request.offset() + page_request.limit < total_workflow_runs,
        ),
        pagination_path: "/workflows",
        pagination_query: format!("limit={}", page_request.limit),
    }))
}

#[derive(Template)]
#[template(path = "workflows/partials/list_content.html")]
struct WorkflowListContentPartial {
    workflow_types: Vec<WorkflowTypeRow>,
    workflow_runs: Vec<WorkflowRunInfo>,
    total_workflow_runs: usize,
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
}

async fn list_refresh(
    State(state): State<AppState>,
    Query(query): Query<WorkflowListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page_request = PageRequest::new(query.page, query.limit);
    let (workflow_types, workflow_runs, total_workflow_runs) =
        collect_workflow_data(&app, page_request).await;
    Ok(HtmlTemplate(WorkflowListContentPartial {
        workflow_types,
        workflow_runs,
        total_workflow_runs,
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(total_workflow_runs),
            page_request.offset() + page_request.limit < total_workflow_runs,
        ),
        pagination_path: "/workflows",
        pagination_query: format!("limit={}", page_request.limit),
    }))
}

/// Collect workflow types with run counts and all workflow runs (invocations with children).
async fn collect_workflow_data(
    app: &crate::AppInstance,
    page_request: PageRequest,
) -> (Vec<WorkflowTypeRow>, Vec<WorkflowRunInfo>, usize) {
    use rustvello_proto::status::InvocationStatus;

    let mut workflow_types = Vec::new();
    let mut workflow_runs = Vec::new();
    let mut total_runs = 0usize;
    let mut skip = page_request.offset();
    let mut remaining = page_request.limit;
    let workflow_task_ids = app
        .state_backend
        .get_all_workflow_types()
        .await
        .unwrap_or_default();
    for tid in workflow_task_ids {
        let run_count = app
            .state_backend
            .count_workflow_runs(&tid)
            .await
            .unwrap_or(0);
        total_runs = total_runs.saturating_add(run_count);
        workflow_types.push(WorkflowTypeRow {
            workflow_type: tid.to_string(),
            run_count,
        });
        if remaining == 0 || skip >= run_count {
            skip = skip.saturating_sub(run_count);
            continue;
        }
        let identities = app
            .state_backend
            .get_workflow_runs_paginated(&tid, remaining, skip)
            .await
            .unwrap_or_default();
        skip = 0;
        for identity in identities {
            let inv_id = identity.workflow_id;
            let (members, status) = tokio::join!(
                app.state_backend
                    .get_workflow_invocations_page(&inv_id, 0, 0),
                app.orchestrator.get_invocation_status(&inv_id)
            );
            let (_, member_count) = members.unwrap_or_default();
            let status = status
                .map(|record| record.status)
                .unwrap_or(InvocationStatus::Registered);
            let full_id = inv_id.to_string();
            let (invocations_url, timeline_url, _) =
                workflow_run_urls(&tid.to_string(), &full_id, page_request.limit);
            workflow_runs.push(WorkflowRunInfo {
                short_id: crate::util::formatting::truncate_id(&full_id),
                invocation_id: full_id,
                task_id: tid.to_string(),
                status: format!("{status:?}"),
                status_class: status_colors::badge_class(&status).to_owned(),
                member_count,
                invocations_url,
                timeline_url,
            });
            remaining = remaining.saturating_sub(1);
        }
    }

    (workflow_types, workflow_runs, total_runs)
}

async fn all_runs(
    State(state): State<AppState>,
    Query(query): Query<WorkflowListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let (_, workflow_runs, count) =
        collect_workflow_data(&app, PageRequest::new(query.page, query.limit)).await;
    let mut html =
        format!("<h5>All Workflow Runs <span class=\"badge bg-primary\">{count}</span></h5>");
    if workflow_runs.is_empty() {
        html.push_str("<p class=\"text-muted\">No workflow runs found.</p>");
    } else {
        html.push_str("<table class=\"table table-hover\"><thead><tr><th>Invocation</th><th>Task</th><th>Status</th><th>Members</th><th>Actions</th></tr></thead><tbody>");
        for run in &workflow_runs {
            html.push_str(&format!(
                "<tr><td><a href=\"/invocations/{}\">{}</a></td><td>{}</td><td><span class=\"badge {}\">{}</span></td><td>{}</td><td><a href=\"/invocations/{}\" class=\"btn btn-sm btn-outline-primary\">View</a></td></tr>",
                xml_escape(&run.invocation_id), xml_escape(&run.short_id), xml_escape(&run.task_id), xml_escape(&run.status_class), xml_escape(&run.status), run.member_count, xml_escape(&run.invocation_id)
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
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(25).clamp(10, 100);
    let categories = parse_categories(query.histogram_status.as_deref());
    let histogram_status = serialize_categories(&categories);
    let workflow_task_id = workflow_type
        .parse::<rustvello_proto::identifiers::TaskId>()
        .unwrap_or_else(|_| rustvello_proto::identifiers::TaskId::new(&workflow_type, ""));
    let total_runs = app
        .state_backend
        .count_workflow_runs(&workflow_task_id)
        .await
        .unwrap_or(0);
    let total_pages = total_runs.div_ceil(limit).max(1);
    let current_page = page.min(total_pages);
    let page_identities = app
        .state_backend
        .get_workflow_runs_paginated(
            &workflow_task_id,
            limit,
            (current_page - 1).saturating_mul(limit),
        )
        .await
        .unwrap_or_default();
    let page_ids = page_identities
        .into_iter()
        .map(|identity| identity.workflow_id.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let requested_ids = query
        .histogram_workflow
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .take(MAX_COMPARISON_RUNS)
        .map(str::to_owned)
        .collect::<std::collections::BTreeSet<_>>();
    let mut roots = page_ids.clone();
    roots.extend(requested_ids.iter().cloned());
    let mut all_runs = collect_workflow_runs(&app, &workflow_type, roots, limit).await;
    let selected = select_workflow_histograms(&all_runs, query.histogram_workflow.as_deref());
    for run in &mut all_runs {
        run.histogram_selected = selected.contains(&run.workflow_id);
    }
    let selected_runs = all_runs
        .iter()
        .filter(|run| run.histogram_selected)
        .cloned()
        .collect::<Vec<_>>();
    for run in &mut all_runs {
        run.selection_url = workflow_selection_url(
            &workflow_type,
            &selected,
            &run.workflow_id,
            &histogram_status,
            limit,
            current_page,
        );
    }
    let runs = all_runs
        .into_iter()
        .filter(|run| page_ids.contains(&run.workflow_id))
        .collect::<Vec<_>>();
    let workflow_histograms =
        build_workflow_histograms(&state, &app, &selected_runs, &selected, &categories).await;
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair(
        "histogram_workflow",
        &selected.iter().cloned().collect::<Vec<_>>().join(","),
    );
    serializer.append_pair("histogram_status", &histogram_status);
    serializer.append_pair("limit", &limit.to_string());
    let pagination_query = serializer.finish();
    Ok(HtmlTemplate(WorkflowDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "workflows",
        workflow_task_id: workflow_type.clone(),
        workflow_type: workflow_type.clone(),
        runs,
        total_runs,
        pagination: PaginationView::new(
            PageRequest::new(Some(current_page), Some(limit)),
            TotalCount::Exact(total_runs),
            current_page < total_pages,
        ),
        pagination_path: format!("/workflows/{workflow_type}"),
        pagination_query,
        selected_workflow_ids: selected.iter().cloned().collect::<Vec<_>>().join(","),
        workflow_histograms,
        histogram_selection_capped: query
            .histogram_workflow
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .filter(|id| !id.trim().is_empty())
            .count()
            > MAX_COMPARISON_RUNS,
        histogram_status,
        limit,
    }))
}

/// Collect individual workflow runs for a given workflow/task type.
async fn collect_workflow_runs(
    app: &crate::AppInstance,
    workflow_type: &str,
    root_ids: std::collections::BTreeSet<String>,
    limit: usize,
) -> Vec<WorkflowRunRow> {
    let root_ids = root_ids
        .into_iter()
        .map(rustvello_proto::identifiers::InvocationId::from_string)
        .collect::<Vec<_>>();

    let mut dated_runs = Vec::new();
    for inv_id in root_ids {
        let Ok(root) = app.state_backend.get_invocation(&inv_id).await else {
            continue;
        };
        if root.task_id.to_string() != workflow_type {
            continue;
        }
        let (_, member_count) = app
            .state_backend
            .get_workflow_invocations_page(&inv_id, 0, 0)
            .await
            .unwrap_or_default();
        let created_at = root.created_at;
        // DTO updated_at is not a status-transition clock on every backend.
        // Read only the root history; unselected member histories remain untouched.
        let root_history = app
            .state_backend
            .get_history(&inv_id)
            .await
            .unwrap_or_default();
        let completed_at = root_history
            .iter()
            .max_by_key(|entry| entry.status_record.timestamp)
            .map_or(root.updated_at, |entry| {
                if entry.status_record.status.is_terminal() {
                    entry.status_record.timestamp
                } else {
                    chrono::Utc::now()
                }
            });
        let duration_ms = (completed_at - created_at).num_milliseconds().max(0);
        let full_id = inv_id.to_string();
        let short = crate::util::formatting::truncate_id(&full_id);
        let (invocations_url, timeline_url, root_invocation_url) =
            workflow_run_urls(workflow_type, &full_id, limit);
        dated_runs.push((
            created_at,
            WorkflowRunRow {
                workflow_id: full_id,
                short_id: short,
                member_count: member_count.max(1),
                worker_count: 0,
                duration_ms,
                duration: crate::util::formatting::format_duration_secs(
                    duration_ms as f64 / 1_000.0,
                ),
                histogram_selected: false,
                selection_url: String::new(),
                invocations_url,
                timeline_url,
                root_invocation_url,
                started: created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
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
            let mut candidates = runs.iter().collect::<Vec<_>>();
            candidates.sort_by(|left, right| {
                right
                    .duration_ms
                    .cmp(&left.duration_ms)
                    .then_with(|| right.worker_count.cmp(&left.worker_count))
                    .then_with(|| right.member_count.cmp(&left.member_count))
            });
            candidates
                .into_iter()
                .take(3)
                .map(|run| run.workflow_id.clone())
                .collect()
        },
        |value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|id| available.contains(id))
                .take(MAX_COMPARISON_RUNS)
                .map(str::to_owned)
                .collect()
        },
    )
}

async fn load_workflow_history(
    app: &crate::AppInstance,
    id: &str,
) -> crate::state::WorkflowHistorySnapshot {
    let workflow_id = rustvello_proto::identifiers::InvocationId::from_string(id);
    let Ok((mut ids, total)) = app
        .state_backend
        .get_workflow_invocations_page(&workflow_id, MAX_COMPARISON_MEMBERS, 0)
        .await
    else {
        return crate::state::WorkflowHistorySnapshot {
            entries: Vec::new(),
            truncated: true,
        };
    };
    if !ids.contains(&workflow_id) {
        ids.insert(0, workflow_id);
    }
    let mut entries = Vec::new();
    let mut truncated = total > MAX_COMPARISON_MEMBERS;
    for id in ids {
        let Ok(invocation) = app.state_backend.get_invocation(&id).await else {
            truncated = true;
            continue;
        };
        let Ok(history) = app.state_backend.get_history(&id).await else {
            truncated = true;
            continue;
        };
        // Stop at an invocation boundary so a missing terminal event cannot
        // turn a completed invocation into an apparently active one.
        if entries.len() + history.len() > 20_000 {
            truncated = true;
            break;
        }
        entries.extend(
            history
                .iter()
                .map(|item| HistogramEntry::from_history(item, &invocation.task_id.to_string())),
        );
    }
    crate::state::WorkflowHistorySnapshot { entries, truncated }
}

async fn build_workflow_histograms(
    state: &AppState,
    app: &crate::AppInstance,
    runs: &[WorkflowRunRow],
    selected: &std::collections::BTreeSet<String>,
    categories: &std::collections::BTreeSet<HistogramCategory>,
) -> Vec<WorkflowHistogramView> {
    let mut models = Vec::new();
    for run in runs
        .iter()
        .filter(|run| selected.contains(&run.workflow_id))
        .take(MAX_COMPARISON_RUNS)
    {
        let snapshot = if let Some(snapshot) = state.workflow_history(&app.app_id, &run.workflow_id)
        {
            snapshot
        } else {
            let snapshot = std::sync::Arc::new(load_workflow_history(app, &run.workflow_id).await);
            state.cache_workflow_history(
                &app.app_id,
                &run.workflow_id,
                std::sync::Arc::clone(&snapshot),
            );
            snapshot
        };
        let entries = snapshot.entries.clone();
        let mut latest = std::collections::HashMap::new();
        for entry in &entries {
            let item = latest
                .entry(entry.invocation_id.clone())
                .or_insert((entry.timestamp, entry.status));
            if entry.timestamp >= item.0 {
                *item = (entry.timestamp, entry.status);
            }
        }
        let start = entries
            .iter()
            .map(|entry| entry.timestamp)
            .min()
            .unwrap_or_else(chrono::Utc::now);
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
        let mut summary = run.clone();
        summary.duration = crate::util::formatting::format_duration_secs(
            (end - start).num_milliseconds() as f64 / 1_000.0,
        );
        models.push((
            run.workflow_id.clone(),
            run.short_id.clone(),
            start,
            end,
            entries,
            snapshot.truncated,
            summary,
        ));
    }
    let shared_duration = models
        .iter()
        .map(|(_, _, start, end, _, _, _)| (*end - *start).num_milliseconds())
        .max()
        .unwrap_or(1)
        .max(1);
    let comparison_start = chrono::DateTime::from_timestamp(0, 0)
        .expect("the Unix epoch is representable as a UTC timestamp");
    let comparison_end = comparison_start + chrono::Duration::milliseconds(shared_duration);
    let models = models
        .into_iter()
        .map(
            |(workflow_id, short_id, start, _end, mut entries, truncated, run)| {
                for entry in &mut entries {
                    entry.timestamp = comparison_start + (entry.timestamp - start);
                }
                (
                    workflow_id,
                    short_id,
                    build_histogram(
                        &entries,
                        comparison_start,
                        comparison_end,
                        categories.clone(),
                        None,
                    ),
                    truncated,
                    run,
                )
            },
        )
        .collect::<Vec<_>>();
    let shared_max = models
        .iter()
        .map(|(_, _, data, _, _)| data.max_count)
        .max()
        .unwrap_or_default();
    let shared_workers = models
        .iter()
        .map(|(_, _, data, _, _)| data.peak_worker_count())
        .max()
        .unwrap_or(1);
    models
        .into_iter()
        .map(
            |(_workflow_id, _short_id, data, truncated, run)| WorkflowHistogramView {
                workflow_id: _workflow_id,
                histogram: HistogramPanel::from_data_with_options(
                    &data,
                    &[],
                    "",
                    false,
                    crate::histogram::HistogramPanelOptions {
                        y_axis_max: Some(shared_max),
                        worker_axis_max: Some(shared_workers),
                        relative_time: true,
                        ..Default::default()
                    },
                )
                .with_form_id("workflow-selection-form"),
                duration: run.duration,
                member_count: run.member_count,
                timeline_url: run.timeline_url,
                truncated,
            },
        )
        .collect()
}

/// Redirect a workflow run to the workflow detail page with that run selected.
async fn workflow_run_detail(
    State(state): State<AppState>,
    Path((workflow_type, workflow_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut page = 1;
    if let (Ok(app), Ok(task_id)) = (state.active_app(), workflow_type.parse()) {
        let id = rustvello_proto::identifiers::InvocationId::from_string(workflow_id.clone());
        if let Ok(Some(offset)) = app
            .state_backend
            .get_workflow_run_offset(&task_id, &id)
            .await
        {
            page = offset / 25 + 1;
        }
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("histogram_workflow", &workflow_id);
    serializer.append_pair("page", &page.to_string());
    Redirect::to(&format!(
        "/workflows/{workflow_type}?{}",
        serializer.finish()
    ))
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
