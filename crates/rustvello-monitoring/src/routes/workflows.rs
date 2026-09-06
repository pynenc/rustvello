//! Workflow monitoring views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Redirect};
use axum::Router;

use crate::histogram::{
    build_histogram, parse_categories, serialize_categories, HistogramCategory, HistogramEntry,
    HistogramPanel,
};
use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope, TimeWindow};
use crate::query::{PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::escape::xml_escape;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::PaginationView;

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
    child_count: usize,
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
    page: Option<usize>,
    limit: Option<usize>,
}

fn workflow_run_urls(
    workflow_type: &str,
    workflow_id: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
    limit: usize,
) -> (String, String, String) {
    let scope = MonitoringScope::default()
        .with_workflow(workflow_type, workflow_id)
        .with_time(TimeWindow::fit_default(start, end));
    let invocations_url = MonitoringLink::new(MonitoringDestination::InvocationList)
        .with_scope(scope.clone())
        .with_limit(limit)
        .href();
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(scope)
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
    if !next.is_empty() {
        serializer.append_pair(
            "histogram_workflow",
            &next.into_iter().collect::<Vec<_>>().join(","),
        );
    }
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
            let (children, status) = tokio::join!(
                app.state_backend.get_child_invocations(&inv_id),
                app.orchestrator.get_invocation_status(&inv_id)
            );
            let children = children.unwrap_or_default();
            let status = status
                .map(|record| record.status)
                .unwrap_or(InvocationStatus::Registered);
            let full_id = inv_id.to_string();
            workflow_runs.push(WorkflowRunInfo {
                short_id: crate::util::formatting::truncate_id(&full_id),
                invocation_id: full_id,
                task_id: tid.to_string(),
                status: format!("{status:?}"),
                status_class: status_colors::badge_class(&status).to_owned(),
                child_count: children.len(),
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
        .filter(|run| page_ids.contains(&run.workflow_id) || run.histogram_selected)
        .collect::<Vec<_>>();
    let workflow_histograms =
        build_workflow_histograms(&app, &selected_runs, &selected, &categories).await;
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    if let Some(selected) = query
        .histogram_workflow
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        serializer.append_pair("histogram_workflow", selected);
    }
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
        histogram_selection_capped: selected.len() > 10,
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
    let mut root_ids = root_ids
        .into_iter()
        .map(rustvello_proto::identifiers::InvocationId::from_string)
        .collect::<Vec<_>>();
    if root_ids.is_empty() {
        let tid: rustvello_proto::identifiers::TaskId = workflow_type
            .parse()
            .unwrap_or_else(|_| rustvello_proto::identifiers::TaskId::new(workflow_type, ""));
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
        let mut members = app
            .state_backend
            .get_workflow_invocations(&inv_id)
            .await
            .unwrap_or_default();
        if !members.contains(&inv_id) {
            members.push(inv_id.clone());
        }
        let created_at = app
            .state_backend
            .get_invocation(&inv_id)
            .await
            .ok()
            .map_or_else(chrono::Utc::now, |invocation| invocation.created_at);
        let mut worker_ids = std::collections::HashSet::new();
        let mut first_seen = created_at;
        let mut completed_at = created_at;
        for member_id in &members {
            let history = app
                .state_backend
                .get_history(member_id)
                .await
                .unwrap_or_default();
            for entry in history {
                let timestamp = entry
                    .history_timestamp
                    .unwrap_or(entry.status_record.timestamp);
                first_seen = first_seen.min(timestamp);
                completed_at = completed_at.max(timestamp);
                if let Some(runner_id) = entry.runner_id.or(entry.status_record.runner_id) {
                    worker_ids.insert(runner_id.to_string());
                }
            }
        }
        let duration_ms = (completed_at - created_at).num_milliseconds().max(0);
        let full_id = inv_id.to_string();
        let short = crate::util::formatting::truncate_id(&full_id);
        let (invocations_url, timeline_url, root_invocation_url) = workflow_run_urls(
            workflow_type,
            &full_id,
            first_seen,
            completed_at.max(first_seen + chrono::Duration::milliseconds(1)),
            limit,
        );
        dated_runs.push((
            created_at,
            WorkflowRunRow {
                workflow_id: full_id,
                short_id: short,
                member_count: members.len(),
                worker_count: worker_ids.len(),
                duration_ms,
                duration: crate::util::formatting::format_duration_secs(
                    duration_ms as f64 / 1_000.0,
                ),
                histogram_selected: false,
                selection_url: String::new(),
                invocations_url,
                timeline_url,
                root_invocation_url,
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
                .map(str::to_owned)
                .collect()
        },
    )
}

async fn build_workflow_histograms(
    app: &crate::AppInstance,
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
        models.push((
            run.workflow_id.clone(),
            run.short_id.clone(),
            start,
            end,
            entries,
        ));
    }
    let shared_duration = models
        .iter()
        .map(|(_, _, start, end, _)| (*end - *start).num_milliseconds())
        .max()
        .unwrap_or(1)
        .max(1);
    let comparison_start = chrono::DateTime::from_timestamp(0, 0)
        .expect("the Unix epoch is representable as a UTC timestamp");
    let comparison_end = comparison_start + chrono::Duration::milliseconds(shared_duration);
    let models = models
        .into_iter()
        .map(|(workflow_id, short_id, start, _end, mut entries)| {
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
            )
        })
        .collect::<Vec<_>>();
    let shared_max = models
        .iter()
        .map(|(_, _, data)| data.max_count)
        .max()
        .unwrap_or_default();
    models
        .into_iter()
        .map(|(_workflow_id, short_id, data)| WorkflowHistogramView {
            workflow_id: _workflow_id,
            short_id,
            histogram: HistogramPanel::from_data_comparison(&data, Some(shared_max)),
        })
        .collect()
}

/// Redirect a workflow run to the workflow detail page with that run selected.
async fn workflow_run_detail(
    Path((workflow_type, workflow_id)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("histogram_workflow", &workflow_id);
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
