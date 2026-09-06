//! Invocation views: list, timeline, detail.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;
use futures_util::{stream, StreamExt};

use crate::histogram::{
    build_histogram, parse_categories, HistogramEntry, HistogramPanel, HistogramPanelOptions,
};
use crate::navigation::{
    parse_datetime, MonitoringDestination, MonitoringLink, MonitoringScope, TimeWindow,
};
use crate::query::{load_invocation_rows, PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::{FilterSummaryItem, InvocationRowView, PaginationView};

use rustvello_proto::status::InvocationStatus;

// ---------------------------------------------------------------------------
// Query / template structs
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct InvocationListQuery {
    pub status: Option<String>,
    pub status_mode: Option<String>,
    pub task_id: Option<String>,
    pub workflow_type: Option<String>,
    pub workflow_id: Option<String>,
    pub time_range: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub inv_ids: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

/// Current filter values echoed back to the template.
struct CurrentFilters {
    statuses: Vec<String>,
    status_mode: String,
    task_id: String,
    workflow_type: String,
    workflow_id: String,
    start_date: String,
    end_date: String,
    inv_ids: String,
    limit: usize,
}

#[derive(Template)]
#[template(path = "invocations/list.html")]
#[allow(dead_code)]
struct InvocationListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    invocations: Vec<InvocationRowView>,
    all_statuses: Vec<String>,
    all_task_ids: Vec<String>,
    all_workflow_types: Vec<String>,
    current_filters: CurrentFilters,
    filter_summary: Vec<FilterSummaryItem>,
    pagination: PaginationView,
    pagination_path: &'static str,
    timeline_url: String,
    status_query: String,
    pagination_query: String,
}

#[derive(Template)]
#[template(path = "invocations/detail.html")]
#[allow(dead_code)]
struct InvocationDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    invocation_id: String,
    task_id: String,
    call_id: String,
    status: String,
    status_class: String,
    num_retries: usize,
    parent_invocation_id: Option<String>,
    workflow_type: Option<String>,
    workflow_id: Option<String>,
    is_workflow_defining: bool,
    created_at: Option<String>,
    completed_at: Option<String>,
    duration: Option<String>,
    history: Vec<HistoryEntry>,
    result: Option<String>,
    error: Option<String>,
    arguments: Vec<(String, String)>,
    timeline_url: String,
    workflow_timeline_url: Option<String>,
}

struct HistoryEntry {
    status: String,
    status_class: String,
    timestamp: String,
    message: Option<String>,
    runner_id: Option<String>,
    runner_cls: Option<String>,
    runner_language: Option<String>,
    hostname: Option<String>,
    pid: Option<String>,
    thread_id: Option<String>,
    parent_runner_cls: Option<String>,
    parent_runner_id: Option<String>,
}

#[derive(serde::Deserialize, Default)]
pub struct TimelineQuery {
    pub time_range: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub task_id: Option<String>,
    pub workflow_type: Option<String>,
    pub workflow_id: Option<String>,
    pub limit: Option<String>,
    pub selected: Option<String>,
    pub inv_ids: Option<String>,
    pub runner_ids: Option<String>,
    pub histogram_status: Option<String>,
}

fn parse_timeline_datetime(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_datetime(value)
}

fn timeline_input_value(value: chrono::DateTime<chrono::Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
}

fn list_scope(query: &InvocationListQuery, invocation_ids: Vec<String>) -> MonitoringScope {
    let time = (query.time_range.as_deref() == Some("custom"))
        .then(|| {
            parse_datetime(query.start_date.as_deref()?)
                .zip(parse_datetime(query.end_date.as_deref()?))
                .map(|(start, end)| TimeWindow::new(start, end))
        })
        .flatten();
    MonitoringScope {
        task_id: query.task_id.clone(),
        workflow_type: query.workflow_type.clone(),
        workflow_id: query.workflow_id.clone(),
        invocation_ids,
        statuses: query
            .status
            .as_deref()
            .unwrap_or_default()
            .split(',')
            .filter(|status| !status.is_empty())
            .map(str::to_owned)
            .collect(),
        status_mode: query.status_mode.clone(),
        time,
        ..MonitoringScope::default()
    }
}

fn invocation_timeline_url(invocation_id: &str, query: &InvocationListQuery) -> String {
    MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(list_scope(query, vec![invocation_id.to_owned()]))
        .with_selected_invocation(invocation_id)
        .href()
}

fn invocation_list_timeline_url(query: &InvocationListQuery, invocation_ids: &[String]) -> String {
    let mut invocation_ids = parse_invocation_scope(query.inv_ids.as_deref())
        .map_or_else(|| invocation_ids.to_vec(), |ids| ids.into_iter().collect());
    invocation_ids.sort();
    MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(list_scope(query, invocation_ids))
        .href()
}

fn invocation_filter_url(query: &InvocationListQuery, removed: &[&str]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    let pairs = [
        ("status", query.status.as_deref()),
        ("status_mode", query.status_mode.as_deref()),
        ("task_id", query.task_id.as_deref()),
        ("workflow_type", query.workflow_type.as_deref()),
        ("workflow_id", query.workflow_id.as_deref()),
        ("time_range", query.time_range.as_deref()),
        ("start_date", query.start_date.as_deref()),
        ("end_date", query.end_date.as_deref()),
        ("inv_ids", query.inv_ids.as_deref()),
    ];
    for (key, value) in pairs {
        if !removed.contains(&key) {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                serializer.append_pair(key, value);
            }
        }
    }
    if !removed.contains(&"limit") {
        if let Some(value) = query.limit {
            serializer.append_pair("limit", &value.to_string());
        }
    }
    let query = serializer.finish();
    if query.is_empty() {
        "/invocations".to_owned()
    } else {
        format!("/invocations?{query}")
    }
}

fn invocation_filter_summary(query: &InvocationListQuery, limit: usize) -> Vec<FilterSummaryItem> {
    let mut summary = Vec::new();
    for (label, value, removed) in [
        (
            "Status",
            query.status.as_deref(),
            vec!["status", "status_mode"],
        ),
        ("Task", query.task_id.as_deref(), vec!["task_id"]),
        (
            "Workflow type",
            query.workflow_type.as_deref(),
            vec!["workflow_type"],
        ),
        (
            "Workflow",
            query.workflow_id.as_deref(),
            vec!["workflow_id"],
        ),
        (
            "From",
            query.start_date.as_deref(),
            vec!["start_date", "time_range"],
        ),
        (
            "To",
            query.end_date.as_deref(),
            vec!["end_date", "time_range"],
        ),
        (
            "Invocation scope",
            query.inv_ids.as_deref(),
            vec!["inv_ids"],
        ),
    ] {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            summary.push(FilterSummaryItem {
                label: label.to_owned(),
                value: value.to_owned(),
                remove_url: invocation_filter_url(query, &removed),
                removable: true,
            });
        }
    }
    summary.push(FilterSummaryItem {
        label: "Limit".to_owned(),
        value: limit.to_string(),
        remove_url: invocation_filter_url(query, &["limit"]),
        removable: false,
    });
    summary
}

/// Current filter values for the timeline sidebar.
struct TimelineFilters {
    time_range: String,
    start_date: String,
    end_date: String,
    task_id: String,
    workflow_type: String,
    workflow_id: String,
    limit: String,
    inv_ids: String,
    runner_ids: String,
}

#[derive(Template)]
#[template(path = "invocations/timeline.html")]
#[allow(dead_code)]
struct InvocationTimelineTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    svg_content: String,
    histogram: HistogramPanel,
    all_task_ids: Vec<String>,
    all_workflow_types: Vec<String>,
    current_filters: TimelineFilters,
    filter_summary: Vec<FilterSummaryItem>,
    start_datetime: String,
    end_datetime: String,
    selected_invocation: String,
}

fn timeline_filter_url(query: &TimelineQuery, removed: &[&str]) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    let pairs = [
        ("time_range", query.time_range.as_deref()),
        ("start_date", query.start_date.as_deref()),
        ("end_date", query.end_date.as_deref()),
        ("task_id", query.task_id.as_deref()),
        ("workflow_type", query.workflow_type.as_deref()),
        ("workflow_id", query.workflow_id.as_deref()),
        ("limit", query.limit.as_deref()),
        ("inv_ids", query.inv_ids.as_deref()),
        ("runner_ids", query.runner_ids.as_deref()),
    ];
    for (key, value) in pairs {
        if !removed.contains(&key) {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                serializer.append_pair(key, value);
            }
        }
    }
    let query = serializer.finish();
    if query.is_empty() {
        "/invocations/timeline".to_owned()
    } else {
        format!("/invocations/timeline?{query}")
    }
}

fn timeline_filter_summary(
    query: &TimelineQuery,
    start: &str,
    end: &str,
) -> Vec<FilterSummaryItem> {
    let mut summary = Vec::new();
    let time_value = if query.time_range.as_deref() == Some("custom") {
        format!("{start} - {end}")
    } else {
        query.time_range.as_deref().unwrap_or("5m").to_owned()
    };
    summary.push(FilterSummaryItem {
        label: "Time".to_owned(),
        value: time_value,
        remove_url: timeline_filter_url(query, &["time_range", "start_date", "end_date"]),
        removable: true,
    });
    if let Some(value) = query.task_id.as_deref().filter(|value| !value.is_empty()) {
        summary.push(FilterSummaryItem {
            label: "Task".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["task_id"]),
            removable: true,
        });
    }
    if let Some(value) = query
        .workflow_type
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        summary.push(FilterSummaryItem {
            label: "Workflow type".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["workflow_type"]),
            removable: true,
        });
    }
    if let Some(value) = query
        .workflow_id
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        summary.push(FilterSummaryItem {
            label: "Workflow".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["workflow_id"]),
            removable: true,
        });
    }
    if let Some(value) = query.limit.as_deref().filter(|value| !value.is_empty()) {
        summary.push(FilterSummaryItem {
            label: "Limit".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["limit"]),
            removable: false,
        });
    }
    if let Some(value) = query.inv_ids.as_deref().filter(|value| !value.is_empty()) {
        summary.push(FilterSummaryItem {
            label: "Invocation scope".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["inv_ids"]),
            removable: true,
        });
    }
    if let Some(value) = query
        .runner_ids
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        summary.push(FilterSummaryItem {
            label: "Runner scope".to_owned(),
            value: value.to_owned(),
            remove_url: timeline_filter_url(query, &["runner_ids"]),
            removable: true,
        });
    }
    summary
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/timeline", axum::routing::get(timeline))
        .route("/table", axum::routing::get(table_partial))
        .route(
            "/{inv_id}/family-tree",
            axum::routing::get(family_tree::family_tree_handler),
        )
        .route("/{inv_id}/history", axum::routing::get(history_json))
        .route("/{inv_id}/api", axum::routing::get(api_json))
        .route(
            "/{inv_id}/investigation",
            axum::routing::get(invocation_investigation_json),
        )
        .route("/{inv_id}/rerun", axum::routing::post(rerun))
        .route("/{inv_id}", axum::routing::get(detail))
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<InvocationListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let page = query.page.unwrap_or(1);
    let limit = query.limit.unwrap_or(50).min(200);
    let history_status_mode = query.status_mode.as_deref() == Some("history");
    let requested_ids = parse_invocation_scope(query.inv_ids.as_deref());
    let time_window = if query.time_range.as_deref() == Some("custom") {
        query
            .start_date
            .as_deref()
            .and_then(parse_timeline_datetime)
            .zip(query.end_date.as_deref().and_then(parse_timeline_datetime))
    } else {
        None
    };

    let mut invocations = Vec::new();
    let selected_status_values = query
        .status
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .filter_map(|status| status.parse::<InvocationStatus>().ok())
        .collect::<Vec<_>>();
    let has_workflow_filter = query
        .workflow_type
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        || query
            .workflow_id
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    let has_requested_status = query
        .status
        .as_deref()
        .is_some_and(|value| !value.is_empty());
    // Hard cap: collect at most `max_collect` invocations to prevent DoS
    // from unbounded iteration over all tasks × all invocations.
    let max_collect = page.saturating_mul(limit);

    // Collect invocations from all tasks if no filter
    let task_ids = if let Some(tid_str) = &query.task_id {
        if tid_str.is_empty() {
            app.task_ids.clone()
        } else {
            app.task_ids
                .iter()
                .filter(|t| t.to_string() == *tid_str)
                .cloned()
                .collect::<Vec<_>>()
        }
    } else {
        app.task_ids.clone()
    };
    let valid_task_filter = query
        .task_id
        .as_deref()
        .is_none_or(|value| value.is_empty() || task_ids.len() == 1);
    let backend_paginated = requested_ids.is_none()
        && time_window.is_none()
        && !has_workflow_filter
        && !history_status_mode
        && valid_task_filter
        && (!has_requested_status || !selected_status_values.is_empty());
    let mut exact_total = None;

    if backend_paginated {
        let task_filter = (task_ids.len() == 1).then_some(&task_ids[0]);
        let statuses =
            (!selected_status_values.is_empty()).then_some(selected_status_values.as_slice());
        exact_total = app
            .orchestrator
            .count_invocations(task_filter, statuses)
            .await
            .ok();
        let invocation_ids = app
            .orchestrator
            .get_invocation_ids_paginated(task_filter, statuses, limit, (page - 1) * limit)
            .await
            .unwrap_or_default();
        invocations =
            load_invocation_rows(&app, invocation_ids, list_scope(&query, Vec::new())).await;
    }

    'outer: for tid in if backend_paginated {
        &[]
    } else {
        task_ids.as_slice()
    } {
        let ids = app
            .orchestrator
            .get_invocations_by_task(tid)
            .await
            .unwrap_or_default();
        for inv_id in ids {
            if requested_ids
                .as_ref()
                .is_some_and(|ids| !ids.contains(&inv_id.to_string()))
            {
                continue;
            }
            let inv = app.state_backend.get_invocation(&inv_id).await.ok();
            // Use latest status from history if available, fall back to orchestrator
            let history_records = app
                .state_backend
                .get_history(&inv_id)
                .await
                .unwrap_or_default();
            if requested_ids.is_none()
                && time_window.is_some_and(|(start, end)| {
                    !history_records.iter().any(|entry| {
                        let timestamp = entry
                            .history_timestamp
                            .unwrap_or(entry.status_record.timestamp);
                        start <= timestamp && timestamp < end
                    })
                })
            {
                continue;
            }
            if query.workflow_type.as_deref().is_some_and(|workflow_type| {
                !workflow_type.is_empty()
                    && inv.as_ref().is_none_or(|invocation| {
                        invocation.workflow.as_ref().is_none_or(|workflow| {
                            workflow.workflow_type.to_string() != workflow_type
                        })
                    })
            }) {
                continue;
            }
            if query.workflow_id.as_deref().is_some_and(|workflow_id| {
                !workflow_id.is_empty()
                    && inv.as_ref().is_none_or(|invocation| {
                        invocation.workflow.as_ref().is_none_or(|workflow| {
                            !workflow.workflow_id.to_string().contains(workflow_id)
                        })
                    })
            }) {
                continue;
            }
            let status = if let Some(last) = history_records.last() {
                last.status_record.status
            } else {
                app.orchestrator
                    .get_invocation_status(&inv_id)
                    .await
                    .map(|r| r.status)
                    .unwrap_or(InvocationStatus::Registered)
            };

            let selected_statuses: Vec<&str> = query
                .status
                .as_deref()
                .unwrap_or_default()
                .split(',')
                .filter(|value| !value.is_empty())
                .collect();
            if !selected_statuses.is_empty() {
                let matches = if history_status_mode {
                    history_records.iter().any(|entry| {
                        let history_status = format!("{:?}", entry.status_record.status);
                        selected_statuses
                            .iter()
                            .any(|candidate| *candidate == history_status)
                    })
                } else {
                    let status_str = format!("{status:?}");
                    selected_statuses.iter().any(|s| *s == status_str)
                };
                if !matches {
                    continue;
                }
            }

            let full_id = inv_id.to_string();
            let short = crate::util::formatting::truncate_id(&full_id);
            let call_id = inv
                .as_ref()
                .map(|i| i.call_id.to_string())
                .unwrap_or_default();
            let is_workflow_defining = inv
                .as_ref()
                .is_some_and(rustvello_proto::invocation::InvocationDTO::is_workflow_defining);
            let badge = status_colors::badge_class(&status);

            let num_retries = history_records
                .iter()
                .filter(|h| h.status_record.status == InvocationStatus::Retry)
                .count();

            invocations.push(InvocationRowView {
                timeline_url: invocation_timeline_url(&full_id, &query),
                detail_url: MonitoringLink::new(MonitoringDestination::InvocationDetail(
                    full_id.clone(),
                ))
                .href(),
                invocation_id: full_id,
                short_id: short,
                task_id: tid.to_string(),
                call_id,
                status: format!("{status:?}"),
                status_class: badge.to_owned(),
                num_retries,
                is_workflow_defining,
            });

            if invocations.len() >= max_collect {
                break 'outer;
            }
        }
    }

    let total_count = exact_total.unwrap_or(invocations.len());

    // Paginate
    let start = (page - 1) * limit;
    let paginated: Vec<InvocationRowView> = if backend_paginated {
        invocations
    } else {
        invocations.into_iter().skip(start).take(limit).collect()
    };
    let timeline_invocation_ids = paginated
        .iter()
        .map(|invocation| invocation.invocation_id.clone())
        .collect::<Vec<_>>();
    let timeline_url = invocation_list_timeline_url(&query, &timeline_invocation_ids);
    let filter_summary = invocation_filter_summary(&query, limit);
    let total_pages = if limit > 0 {
        total_count.div_ceil(limit)
    } else {
        1
    };

    // Collect all known statuses for the filter dropdown
    let all_statuses: Vec<String> = rustvello_proto::status::ALL_STATUSES
        .iter()
        .map(|s| format!("{s:?}"))
        .collect();

    // Collect all known task IDs
    let all_task_ids: Vec<String> = app
        .task_ids
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let all_workflow_types: Vec<String> = app
        .state_backend
        .get_all_workflow_types()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|workflow_type| workflow_type.to_string())
        .collect();

    let status_raw = query.status.unwrap_or_default();
    let status_query = status_raw.clone();
    let statuses_vec: Vec<String> = status_raw
        .split(',')
        .filter(|s| !s.is_empty())
        .map(std::borrow::ToOwned::to_owned)
        .collect();
    let invocation_scope = query.inv_ids.clone().unwrap_or_default();
    let mut pagination_serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in [
        ("status", Some(status_query.as_str())),
        ("status_mode", query.status_mode.as_deref()),
        ("task_id", query.task_id.as_deref()),
        ("workflow_type", query.workflow_type.as_deref()),
        ("workflow_id", query.workflow_id.as_deref()),
        ("time_range", query.time_range.as_deref()),
        ("start_date", query.start_date.as_deref()),
        ("end_date", query.end_date.as_deref()),
        (
            "inv_ids",
            (!invocation_scope.is_empty()).then_some(invocation_scope.as_str()),
        ),
    ] {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            pagination_serializer.append_pair(key, value);
        }
    }
    pagination_serializer.append_pair("limit", &limit.to_string());
    let pagination_query = pagination_serializer.finish();

    Ok(HtmlTemplate(InvocationListTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "invocations",
        invocations: paginated,
        all_statuses,
        all_task_ids,
        all_workflow_types,
        current_filters: CurrentFilters {
            statuses: statuses_vec,
            status_mode: if history_status_mode {
                "history".to_owned()
            } else {
                "current".to_owned()
            },
            task_id: query.task_id.unwrap_or_default(),
            workflow_type: query.workflow_type.unwrap_or_default(),
            workflow_id: query.workflow_id.unwrap_or_default(),
            start_date: query.start_date.unwrap_or_default(),
            end_date: query.end_date.unwrap_or_default(),
            inv_ids: query.inv_ids.unwrap_or_default(),
            limit,
        },
        filter_summary,
        pagination: PaginationView::new(
            PageRequest::new(Some(page), Some(limit)),
            TotalCount::Exact(total_count),
            page < total_pages,
        ),
        pagination_path: "/invocations",
        timeline_url,
        status_query,
        pagination_query,
    }))
}

async fn detail(
    State(state): State<AppState>,
    Path(inv_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let inv_id_typed = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());

    let inv = app.state_backend.get_invocation(&inv_id_typed).await.ok();
    let task_id = inv
        .as_ref()
        .map(|i| i.task_id.to_string())
        .unwrap_or_default();
    let call_id = inv
        .as_ref()
        .map(|i| i.call_id.to_string())
        .unwrap_or_default();
    let parent_invocation_id = inv
        .as_ref()
        .and_then(|i| i.parent_invocation_id.as_ref())
        .map(std::string::ToString::to_string);
    let workflow_type = inv
        .as_ref()
        .and_then(|i| i.workflow.as_ref())
        .map(|wf| wf.workflow_type.to_string());
    let workflow_id = inv
        .as_ref()
        .and_then(|i| i.workflow.as_ref())
        .map(|wf| wf.workflow_id.to_string());
    let is_workflow_defining = inv
        .as_ref()
        .is_some_and(rustvello_proto::invocation::InvocationDTO::is_workflow_defining);
    let history_records = app
        .state_backend
        .get_history(&inv_id_typed)
        .await
        .unwrap_or_default();
    let history_window = history_records
        .iter()
        .map(|entry| {
            entry
                .history_timestamp
                .unwrap_or(entry.status_record.timestamp)
        })
        .min()
        .zip(
            history_records
                .iter()
                .map(|entry| {
                    entry
                        .history_timestamp
                        .unwrap_or(entry.status_record.timestamp)
                })
                .max(),
        )
        .map(|(start, end)| TimeWindow::fit_default(start, end));
    let timeline_scope = MonitoringScope::default().with_invocation(inv_id.clone());
    let timeline_scope = history_window.map_or(timeline_scope.clone(), |time| {
        timeline_scope.with_time(time)
    });
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(timeline_scope.clone())
        .with_selected_invocation(inv_id.clone())
        .href();
    let workflow_timeline_url =
        workflow_type
            .as_ref()
            .zip(workflow_id.as_ref())
            .map(|(workflow_type, workflow_id)| {
                let mut scope = MonitoringScope::default()
                    .with_workflow(workflow_type.clone(), workflow_id.clone());
                if let Some(time) = history_window {
                    scope = scope.with_time(time);
                }
                MonitoringLink::new(MonitoringDestination::Timeline)
                    .with_scope(scope)
                    .href()
            });

    // Use latest status from history if available, fall back to orchestrator
    let status = if let Some(last) = history_records.last() {
        last.status_record.status
    } else {
        app.orchestrator
            .get_invocation_status(&inv_id_typed)
            .await
            .map(|r| r.status)
            .unwrap_or(InvocationStatus::Registered)
    };
    let badge = status_colors::badge_class(&status);

    let num_retries = history_records
        .iter()
        .filter(|h| h.status_record.status == InvocationStatus::Retry)
        .count();

    // Batch-fetch runner contexts for all unique runner IDs
    let mut runner_contexts = std::collections::HashMap::new();
    for h in &history_records {
        let rid = h.runner_id.as_ref().or(h.status_record.runner_id.as_ref());
        if let Some(r) = rid {
            let key = r.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = runner_contexts.entry(key) {
                if let Ok(Some(ctx)) = app.state_backend.get_runner_context(e.key()).await {
                    e.insert(ctx);
                }
            }
        }
    }

    let history: Vec<HistoryEntry> = history_records
        .iter()
        .map(|h| {
            let st = format!("{:?}", h.status_record.status);
            let cls = status_colors::badge_class(&h.status_record.status).to_owned();
            // Use history.runner_id first, fall back to status_record.runner_id
            let rid = h
                .runner_id
                .as_ref()
                .or(h.status_record.runner_id.as_ref())
                .map(std::string::ToString::to_string);
            let ctx = rid.as_ref().and_then(|r| runner_contexts.get(r));
            HistoryEntry {
                status: st,
                status_class: cls,
                timestamp: h.status_record.timestamp.to_rfc3339(),
                message: h.message.clone(),
                runner_id: rid,
                runner_cls: ctx.map(|c| c.runner_cls.clone()),
                runner_language: ctx.map(|c| c.runner_language.to_string()),
                hostname: ctx.map(|c| c.hostname.clone()),
                pid: ctx.map(|c| c.pid.to_string()),
                thread_id: ctx.map(|c| c.thread_id.to_string()),
                parent_runner_cls: ctx.and_then(|c| c.parent_runner_cls.clone()),
                parent_runner_id: ctx.and_then(|c| c.parent_runner_id.clone()),
            }
        })
        .collect();

    // Timestamps & duration from history
    let created_at = history.first().map(|h| h.timestamp.clone());
    let completed_at = history
        .last()
        .filter(|h| h.status == "Success" || h.status == "Failed")
        .map(|h| h.timestamp.clone());
    let duration = if let (Some(c), Some(d)) = (&created_at, &completed_at) {
        chrono::DateTime::parse_from_rfc3339(c)
            .ok()
            .zip(chrono::DateTime::parse_from_rfc3339(d).ok())
            .map(|(start, end)| {
                let dur = end - start;
                let ms = dur.num_milliseconds();
                if ms < 1000 {
                    format!("{ms}ms")
                } else {
                    format!("{:.2}s", ms as f64 / 1000.0)
                }
            })
    } else {
        None
    };

    let result = app
        .state_backend
        .get_result(&inv_id_typed)
        .await
        .ok()
        .flatten();
    let error = app
        .state_backend
        .get_error(&inv_id_typed)
        .await
        .ok()
        .flatten()
        .map(|e| e.to_string());

    // Arguments from call
    let arguments: Vec<(String, String)> = if let Some(inv_data) = &inv {
        app.state_backend
            .get_call(&inv_data.call_id)
            .await
            .ok()
            .map(|call| {
                call.serialized_arguments
                    .0
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    Ok(HtmlTemplate(InvocationDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "invocations",
        invocation_id: inv_id,
        task_id,
        call_id,
        status: format!("{status:?}"),
        status_class: badge.to_owned(),
        num_retries,
        parent_invocation_id,
        workflow_type: workflow_type.clone(),
        workflow_id,
        is_workflow_defining,
        created_at,
        completed_at,
        duration,
        history,
        result,
        error,
        arguments,
        timeline_url,
        workflow_timeline_url,
    }))
}

async fn timeline(
    State(state): State<AppState>,
    Query(query): Query<TimelineQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;

    let mut time_range = query.time_range.as_deref().unwrap_or("5m").to_owned();

    // Parse time range
    let now = chrono::Utc::now();
    let (mut start_dt, mut end_dt) = if time_range == "custom" {
        let start = query
            .start_date
            .as_deref()
            .and_then(parse_timeline_datetime)
            .unwrap_or_else(|| now - chrono::Duration::minutes(5));
        let end = query
            .end_date
            .as_deref()
            .and_then(parse_timeline_datetime)
            .unwrap_or(now);
        (start, end)
    } else {
        let duration = match time_range.as_str() {
            "1m" => chrono::Duration::minutes(1),
            "15m" => chrono::Duration::minutes(15),
            "1h" => chrono::Duration::hours(1),
            "3h" => chrono::Duration::hours(3),
            "12h" => chrono::Duration::hours(12),
            "1d" => chrono::Duration::days(1),
            "3d" => chrono::Duration::days(3),
            "1w" => chrono::Duration::weeks(1),
            _ => chrono::Duration::minutes(5), // default "5m"
        };
        (now - duration, now)
    };

    // When a specific invocation is selected (e.g. navigating from another page),
    // auto-compute time bounds centered on that invocation's history so it's visible.
    if let Some(ref selected_id) = query.selected {
        if !selected_id.is_empty() && time_range != "custom" {
            let inv_id = rustvello_core::prelude::InvocationId::from(selected_id.as_str());
            let history = app
                .state_backend
                .get_history(&inv_id)
                .await
                .unwrap_or_default();
            if !history.is_empty() {
                let timestamps: Vec<_> =
                    history.iter().map(|h| h.status_record.timestamp).collect();
                if let (Some(&min_t), Some(&max_t)) =
                    (timestamps.iter().min(), timestamps.iter().max())
                {
                    let window = TimeWindow::fit_default(min_t, max_t);
                    (start_dt, end_dt) = (window.start, window.end);
                    time_range = "custom".to_owned();
                }
            }
        }
    }

    // A list-level timeline link scopes the view to every invocation visible
    // on that page. Focus on their combined history when no explicit time
    // range was supplied, including for historical invocations.
    if time_range != "custom" {
        if let Some(invocation_ids) = parse_invocation_scope(query.inv_ids.as_deref()) {
            let mut min_t: Option<chrono::DateTime<chrono::Utc>> = None;
            let mut max_t: Option<chrono::DateTime<chrono::Utc>> = None;
            for invocation_id in invocation_ids {
                let invocation_id =
                    rustvello_core::prelude::InvocationId::from(invocation_id.as_str());
                for entry in app
                    .state_backend
                    .get_history(&invocation_id)
                    .await
                    .unwrap_or_default()
                {
                    let timestamp = entry
                        .history_timestamp
                        .unwrap_or(entry.status_record.timestamp);
                    min_t = Some(min_t.map_or(timestamp, |current| current.min(timestamp)));
                    max_t = Some(max_t.map_or(timestamp, |current| current.max(timestamp)));
                }
            }
            if let (Some(start), Some(end)) = (min_t, max_t) {
                let window = TimeWindow::fit_default(start, end);
                (start_dt, end_dt) = (window.start, window.end);
                time_range = "custom".to_owned();
            }
        }
    }

    if time_range != "custom" {
        if let Some(workflow_id) = query.workflow_id.as_deref().filter(|id| !id.is_empty()) {
            let workflow_root =
                rustvello_proto::identifiers::InvocationId::from_string(workflow_id.to_owned());
            let mut workflow_invocations = app
                .state_backend
                .get_workflow_invocations(&workflow_root)
                .await
                .unwrap_or_default();
            if !workflow_invocations.contains(&workflow_root) {
                workflow_invocations.push(workflow_root);
            }
            let mut min_t: Option<chrono::DateTime<chrono::Utc>> = None;
            let mut max_t: Option<chrono::DateTime<chrono::Utc>> = None;
            for invocation_id in workflow_invocations {
                for entry in app
                    .state_backend
                    .get_history(&invocation_id)
                    .await
                    .unwrap_or_default()
                {
                    let timestamp = entry
                        .history_timestamp
                        .unwrap_or(entry.status_record.timestamp);
                    min_t = Some(min_t.map_or(timestamp, |current| current.min(timestamp)));
                    max_t = Some(max_t.map_or(timestamp, |current| current.max(timestamp)));
                }
            }
            if let (Some(start), Some(end)) = (min_t, max_t) {
                let window = TimeWindow::fit_default(start, end);
                (start_dt, end_dt) = (window.start, window.end);
                time_range = "custom".to_owned();
            }
        }
    }

    let limit: Option<usize> = query
        .limit
        .as_deref()
        .and_then(|s| if s.is_empty() { None } else { s.parse().ok() })
        .map(|value: usize| value.min(50_000));

    // Use the backend's bounded history query to identify active invocations
    // before loading complete histories for the small set that will render.
    // This avoids the previous task-by-task full-history scan and keeps the
    // timeline responsive when old invocations greatly outnumber visible ones.
    let task_filter = query.task_id.as_deref().filter(|value| !value.is_empty());
    let workflow_type_filter = query
        .workflow_type
        .as_deref()
        .filter(|value| !value.is_empty());
    let workflow_id_filter = query
        .workflow_id
        .as_deref()
        .filter(|value| !value.is_empty());

    let requested_inv_ids = parse_invocation_scope(query.inv_ids.as_deref());
    let requested_runner_ids = parse_invocation_scope(query.runner_ids.as_deref());
    let mut candidate_ids = Vec::new();
    let mut candidate_seen = std::collections::HashSet::new();

    if let Some(requested_ids) = &requested_inv_ids {
        // An explicit scope is authoritative. Do not discard a selected
        // invocation merely because a backend's range index is stale or the
        // requested window contains only part of its history.
        for inv_id in requested_ids {
            if candidate_seen.insert(inv_id.clone()) {
                candidate_ids.push(rustvello_core::prelude::InvocationId::from(inv_id.as_str()));
            }
        }
    } else {
        match app
            .state_backend
            .get_history_in_timerange(start_dt, end_dt, 0, 0)
            .await
        {
            Ok(ranged_history) => {
                for entry in ranged_history {
                    let inv_id = entry.invocation_id.to_string();
                    if candidate_seen.insert(inv_id) {
                        candidate_ids.push(entry.invocation_id);
                    }
                }
            }
            Err(error) => {
                // Keep the dashboard usable with older or partially migrated
                // backend indexes. The fallback is intentionally limited to
                // the registered task set and still applies the filters below.
                tracing::warn!(%error, "timeline range query failed; falling back to task index");
                let fallback_task_ids = task_filter.map_or_else(
                    || app.task_ids.clone(),
                    |task_id| {
                        app.task_ids
                            .iter()
                            .filter(|candidate| candidate.to_string() == task_id)
                            .cloned()
                            .collect()
                    },
                );
                for task_id in fallback_task_ids {
                    if let Ok(invocations) =
                        app.orchestrator.get_invocations_by_task(&task_id).await
                    {
                        for inv_id in invocations {
                            if candidate_seen.insert(inv_id.to_string()) {
                                candidate_ids.push(inv_id);
                            }
                        }
                    }
                }
            }
        }
    }

    // Build the SVG from complete histories so an invocation that started
    // just before the visible window still renders its correct transitions.
    let mut builder = crate::svg::TimelineDataBuilder::new(crate::svg::TimelineConfig::default());
    builder.set_time_bounds(start_dt, end_dt);

    let mut inv_count = 0usize;
    let mut runner_ids_seen = std::collections::HashSet::new();
    let mut history_batches = Vec::new();
    let backend = &app.state_backend;
    let mut loaded_candidates = stream::iter(candidate_ids.into_iter().map(|inv_id| async move {
        let (history, invocation) = tokio::join!(
            backend.get_history(&inv_id),
            backend.get_invocation(&inv_id)
        );
        (history.unwrap_or_default(), invocation.ok())
    }))
    .buffered(32);

    while let Some((history, invocation)) = loaded_candidates.next().await {
        if limit.is_some_and(|maximum| inv_count >= maximum) {
            break;
        }
        if history.is_empty() {
            continue;
        }
        let Some(invocation) = invocation else {
            continue;
        };
        if requested_runner_ids.as_ref().is_some_and(|runner_ids| {
            !history.iter().any(|entry| {
                entry
                    .runner_id
                    .as_ref()
                    .or(entry.status_record.runner_id.as_ref())
                    .is_some_and(|runner_id| runner_ids.contains(&runner_id.to_string()))
            })
        }) {
            continue;
        }
        if task_filter.is_some_and(|value| invocation.task_id.to_string() != value) {
            continue;
        }
        if workflow_type_filter.is_some_and(|value| {
            invocation
                .workflow
                .as_ref()
                .is_none_or(|workflow| workflow.workflow_type.to_string() != value)
        }) {
            continue;
        }
        if workflow_id_filter.is_some_and(|value| {
            invocation
                .workflow
                .as_ref()
                .is_none_or(|workflow| !workflow.workflow_id.to_string().contains(value))
        }) {
            continue;
        }

        for entry in &history {
            if let Some(ref rid) = entry.runner_id {
                runner_ids_seen.insert(rid.to_string());
            }
            if let Some(ref rid) = entry.status_record.runner_id {
                runner_ids_seen.insert(rid.to_string());
            }
        }
        let task_id = invocation.task_id.to_string();
        history_batches.push((history.clone(), task_id.clone()));
        builder.add_history_batch_for_task(history, &task_id);
        inv_count += 1;
    }

    let atomic_service_executions = app
        .orchestrator
        .get_atomic_service_timeline()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|execution| execution.end >= start_dt && execution.start <= end_dt)
        .collect::<Vec<_>>();
    runner_ids_seen.extend(
        atomic_service_executions
            .iter()
            .map(|execution| execution.runner_id.clone()),
    );

    // Fetch runner contexts for enriched labels
    let mut runner_contexts = std::collections::HashMap::new();
    let mut loaded_contexts = stream::iter(runner_ids_seen.into_iter().map(|rid| async move {
        let context = backend.get_runner_context(&rid).await.ok().flatten();
        (rid, context)
    }))
    .buffer_unordered(32);
    while let Some((rid, context)) = loaded_contexts.next().await {
        if let Some(ctx) = context {
            runner_contexts.insert(rid.clone(), ctx);
        }
    }
    builder.set_runner_contexts(runner_contexts.clone());
    builder.set_atomic_service_executions(atomic_service_executions);

    let mut histogram_entries = Vec::new();
    for (history, task_id) in &history_batches {
        histogram_entries.extend(history.iter().map(|entry| {
            HistogramEntry::from_history_with_runner_contexts(entry, task_id, &runner_contexts)
        }));
    }

    let data = builder.build();
    let svg_content = crate::svg::TimelineSvgRenderer::render(&data);
    let selected_categories = parse_categories(query.histogram_status.as_deref());
    let histogram_data = build_histogram(
        &histogram_entries,
        data.bounds.start,
        data.bounds.end,
        selected_categories,
        None,
    );
    let mut histogram_params = Vec::new();
    for (key, value) in [
        ("task_id", query.task_id.as_deref()),
        ("workflow_type", query.workflow_type.as_deref()),
        ("workflow_id", query.workflow_id.as_deref()),
    ] {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            histogram_params.push((key.to_owned(), value.to_owned()));
        }
    }
    let histogram = HistogramPanel::from_data_with_options(
        &histogram_data,
        &histogram_params,
        "/invocations",
        false,
        HistogramPanelOptions {
            plot_left: Some(data.bounds.left_margin),
            plot_right: Some(data.bounds.left_margin + data.bounds.drawable_width),
            ..HistogramPanelOptions::default()
        },
    );

    let all_task_ids: Vec<String> = app
        .task_ids
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    let all_workflow_types: Vec<String> = app
        .state_backend
        .get_all_workflow_types()
        .await
        .unwrap_or_default()
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

    let start_datetime = start_dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let end_datetime = end_dt.format("%Y-%m-%d %H:%M:%S").to_string();
    let filter_summary = timeline_filter_summary(&query, &start_datetime, &end_datetime);

    Ok(HtmlTemplate(InvocationTimelineTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "timeline",
        svg_content,
        histogram,
        all_task_ids,
        all_workflow_types,
        filter_summary,
        current_filters: TimelineFilters {
            time_range: time_range.to_owned(),
            start_date: timeline_input_value(start_dt),
            end_date: timeline_input_value(end_dt),
            task_id: query.task_id.unwrap_or_default(),
            workflow_type: query.workflow_type.unwrap_or_default(),
            workflow_id: query.workflow_id.unwrap_or_default(),
            limit: query.limit.unwrap_or_else(|| "500".to_owned()),
            inv_ids: query.inv_ids.unwrap_or_default(),
            runner_ids: query.runner_ids.unwrap_or_default(),
        },
        start_datetime,
        end_datetime,
        selected_invocation: query.selected.unwrap_or_default(),
    }))
}

fn parse_invocation_scope(value: Option<&str>) -> Option<std::collections::HashSet<String>> {
    let ids: std::collections::HashSet<String> = value
        .unwrap_or_default()
        .split([',', ' ', '\n', '\t'])
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .collect();
    (!ids.is_empty()).then_some(ids)
}

async fn table_partial(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let _ = app; // Stub — HTMX partial table refresh
    Ok(axum::response::Html(
        "<tr><td>Loading...</td></tr>".to_owned(),
    ))
}

async fn history_json(
    State(state): State<AppState>,
    Path(inv_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let inv_id_typed = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());
    let history = app
        .state_backend
        .get_history(&inv_id_typed)
        .await
        .unwrap_or_default();

    // Batch-fetch runner contexts for all unique runner IDs in the history
    let mut runner_contexts = std::collections::HashMap::new();
    for h in &history {
        let rid = h.runner_id.as_ref().or(h.status_record.runner_id.as_ref());
        if let Some(r) = rid {
            let key = r.to_string();
            if let std::collections::hash_map::Entry::Vacant(e) = runner_contexts.entry(key) {
                if let Ok(Some(ctx)) = app.state_backend.get_runner_context(e.key()).await {
                    e.insert(ctx);
                }
            }
        }
    }

    let entries: Vec<serde_json::Value> = history
        .iter()
        .map(|h| {
            let rid = h
                .runner_id
                .as_ref()
                .or(h.status_record.runner_id.as_ref())
                .map(std::string::ToString::to_string);
            let runner_info = rid
                .as_ref()
                .and_then(|r| runner_contexts.get(r))
                .map(|ctx| {
                    serde_json::json!({
                        "runner_cls": ctx.runner_cls,
                        "runner_language": ctx.runner_language,
                        "executor_kind": ctx.executor_kind,
                        "hostname": ctx.hostname,
                        "pid": ctx.pid,
                        "thread_id": ctx.thread_id,
                        "parent_runner_cls": ctx.parent_runner_cls,
                        "parent_runner_id": ctx.parent_runner_id,
                    })
                });
            serde_json::json!({
                "status": format!("{:?}", h.status_record.status),
                "timestamp": h.status_record.timestamp.to_rfc3339(),
                "message": h.message,
                "runner_id": rid,
                "runner_info": runner_info,
            })
        })
        .collect();
    Ok(axum::response::Json(entries))
}

async fn api_json(
    State(state): State<AppState>,
    Path(inv_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let inv_id_typed = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());
    Ok(
        match app.state_backend.get_invocation(&inv_id_typed).await {
            Ok(inv) => {
                // Use latest status from history if available
                let history = app
                    .state_backend
                    .get_history(&inv_id_typed)
                    .await
                    .unwrap_or_default();
                let status = if let Some(last) = history.last() {
                    format!("{:?}", last.status_record.status)
                } else {
                    format!("{:?}", inv.status)
                };
                let task_language = inv.task_id.language().to_string();
                axum::response::Json(serde_json::json!({
                    "invocation_id": inv.invocation_id.to_string(),
                    "task_id": inv.task_id.to_string(),
                    "task_language": task_language,
                    "call_id": inv.call_id.to_string(),
                    "status": status,
                    "created_at": inv.created_at.to_rfc3339(),
                    "updated_at": inv.updated_at.to_rfc3339(),
                    "parent_invocation_id": inv.parent_invocation_id.as_ref().map(std::string::ToString::to_string),
                    "is_workflow_defining": inv.is_workflow_defining(),
                    "workflow": inv.workflow.as_ref().map(|w| serde_json::json!({
                        "workflow_type": w.workflow_type.to_string(),
                        "workflow_id": w.workflow_id.to_string(),
                        "parent_id": w.parent_id.as_ref().map(std::string::ToString::to_string),
                        "depth": w.depth,
                    })),
                }))
                .into_response()
            }
            Err(e) => {
                tracing::debug!(error = %e, invocation_id = %inv_id, "invocation not found");
                (
                    axum::http::StatusCode::NOT_FOUND,
                    axum::response::Json(serde_json::json!({"error": "Invocation not found"})),
                )
                    .into_response()
            }
        },
    )
}

/// A single, agent-friendly provenance view for diagnosing an invocation.
///
/// This intentionally composes existing backend information into one bounded
/// JSON response so automated debugging does not have to scrape the HTML
/// timeline or reconcile runner IDs by hand.
async fn invocation_investigation_json(
    State(state): State<AppState>,
    Path(inv_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let invocation_id = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());
    let invocation = match app.state_backend.get_invocation(&invocation_id).await {
        Ok(invocation) => invocation,
        Err(_) => {
            return Ok((
                axum::http::StatusCode::NOT_FOUND,
                axum::response::Json(serde_json::json!({"error": "Invocation not found"})),
            )
                .into_response());
        }
    };
    let history = app
        .state_backend
        .get_history(&invocation_id)
        .await
        .unwrap_or_default();
    let mut contexts = std::collections::HashMap::new();
    for entry in &history {
        if let Some(runner_id) = entry
            .runner_id
            .as_ref()
            .or(entry.status_record.runner_id.as_ref())
        {
            let runner_id = runner_id.to_string();
            if let std::collections::hash_map::Entry::Vacant(entry) =
                contexts.entry(runner_id.clone())
            {
                if let Ok(Some(context)) = app.state_backend.get_runner_context(&runner_id).await {
                    entry.insert(context);
                }
            }
        }
    }

    let registered = history
        .iter()
        .find(|entry| entry.status_record.status == InvocationStatus::Registered);
    let registration_runner_id = registered.and_then(|entry| {
        entry
            .runner_id
            .as_ref()
            .or(entry.status_record.runner_id.as_ref())
            .map(std::string::ToString::to_string)
    });
    let registration_time = registered.map(|entry| entry.status_record.timestamp);
    let atomic_execution = if let (Some(runner_id), Some(timestamp)) =
        (registration_runner_id.as_ref(), registration_time)
    {
        app.orchestrator
            .get_atomic_service_timeline()
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|execution| {
                execution.runner_id == *runner_id
                    && execution.start <= timestamp
                    && timestamp <= execution.end
            })
    } else {
        None
    };

    let trigger_runs =
        if let (Some(store), Some(timestamp)) = (&app.trigger_store, registration_time) {
            store
            .get_trigger_runs(&rustvello_proto::trigger::TriggerRunQuery {
                start: Some(timestamp - chrono::Duration::milliseconds(5)),
                end: Some(timestamp + chrono::Duration::milliseconds(5)),
                limit: Some(50),
                ..rustvello_proto::trigger::TriggerRunQuery::default()
            })
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|run| {
                run.triggered_invocation_id
                    .as_ref()
                    .is_some_and(|triggered| triggered == &invocation_id)
            })
            .map(|run| {
                serde_json::json!({
                    "task_id": run.task_id.to_string(),
                    "claimed_at": run.claimed_at.to_rfc3339(),
                    "triggered_invocation_id": run.triggered_invocation_id.map(|id| id.to_string()),
                })
            })
            .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

    let history = history
        .iter()
        .map(|entry| {
            let runner_id = entry
                .runner_id
                .as_ref()
                .or(entry.status_record.runner_id.as_ref())
                .map(std::string::ToString::to_string);
            let context = runner_id.as_ref().and_then(|id| contexts.get(id));
            serde_json::json!({
                "status": format!("{:?}", entry.status_record.status),
                "timestamp": entry.status_record.timestamp.to_rfc3339(),
                "message": entry.message,
                "runner_id": runner_id,
                "runner": context.map(|context| serde_json::json!({
                    "class": context.runner_cls,
                    "language": context.runner_language.to_string(),
                    "executor": context.executor_kind.to_string(),
                    "hostname": context.hostname,
                    "pid": context.pid,
                    "thread_id": context.thread_id,
                    "parent_runner_id": context.parent_runner_id,
                    "parent_runner_class": context.parent_runner_cls,
                })),
                "registered_by_invocation_id": entry.registered_by_inv_id.as_ref().map(std::string::ToString::to_string),
            })
        })
        .collect::<Vec<_>>();
    let mut timeline_scope = MonitoringScope::default().with_invocation(inv_id.clone());
    if let Some(timestamp) = registration_time {
        timeline_scope = timeline_scope.with_time(TimeWindow::fit_default(timestamp, timestamp));
    }
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(timeline_scope)
        .with_selected_invocation(inv_id.clone())
        .href();

    Ok(axum::response::Json(serde_json::json!({
        "invocation": {
            "id": invocation.invocation_id.to_string(),
            "task_id": invocation.task_id.to_string(),
            "call_id": invocation.call_id.to_string(),
            "parent_invocation_id": invocation.parent_invocation_id.as_ref().map(std::string::ToString::to_string),
            "workflow": invocation.workflow.as_ref().map(|workflow| serde_json::json!({
                "workflow_id": workflow.workflow_id.to_string(),
                "workflow_type": workflow.workflow_type.to_string(),
                "parent_id": workflow.parent_id.as_ref().map(std::string::ToString::to_string),
            })),
        },
        "registration": {
            "timestamp": registration_time.map(|time| time.to_rfc3339()),
            "runner_id": registration_runner_id,
            "runner": registration_runner_id.as_ref().and_then(|id| contexts.get(id)).map(|context| serde_json::json!({
                "class": context.runner_cls,
                "language": context.runner_language.to_string(),
                "executor": context.executor_kind.to_string(),
                "hostname": context.hostname,
                "pid": context.pid,
            })),
            "atomic_service_execution": atomic_execution.as_ref().map(|execution| serde_json::json!({
                "runner_id": execution.runner_id,
                "start": execution.start.to_rfc3339(),
                "end": execution.end.to_rfc3339(),
                "duration_seconds": execution.duration_secs(),
            })),
            "trigger_runs": trigger_runs,
        },
        "history": history,
        "integrity": {
            "has_registered_event": registered.is_some(),
            "registration_runner_known": registration_runner_id.as_ref().is_some_and(|id| contexts.contains_key(id)),
            "registration_in_atomic_service_window": atomic_execution.is_some(),
        },
        "links": {
            "detail": format!("/invocations/{inv_id}"),
            "timeline": timeline_url,
            "history": format!("/invocations/{inv_id}/history"),
        },
    }))
    .into_response())
}

async fn rerun(
    State(state): State<AppState>,
    Path(inv_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;

    // Validate and sanitize the redirect parameter (S-W04)
    let sanitized_inv_id: String = inv_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .collect();

    let inv_id_typed = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());
    Ok(
        match app.state_backend.get_invocation(&inv_id_typed).await {
            Ok(inv) => {
                let call = app.state_backend.get_call(&inv.call_id).await;
                if let Ok(_call) = call {
                    // Monitoring does not own the task registry, so it cannot
                    // reconstruct per-task queue configuration. Preserve the
                    // task identity at minimum; the broker uses its default
                    // queue for this generic rerun path.
                    if let Err(e) = app
                        .broker
                        .route_invocation_for_task(&inv.invocation_id, &inv.task_id)
                        .await
                    {
                        tracing::error!(error = %e, invocation_id = %inv.invocation_id, "rerun route failed");
                    }
                }
                axum::response::Redirect::to(&format!("/invocations/{sanitized_inv_id}"))
            }
            Err(_) => {
                tracing::warn!(invocation_id = %inv_id, "rerun: invocation not found");
                axum::response::Redirect::to(&format!("/invocations/{sanitized_inv_id}"))
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{parse_invocation_scope, parse_timeline_datetime};
    use chrono::{Datelike, Timelike};

    #[test]
    fn parse_timeline_datetime_accepts_rfc3339_offsets() {
        let parsed = parse_timeline_datetime("2026-08-31T08:04:25.701+00:00").unwrap();
        assert_eq!(parsed.year(), 2026);
        assert_eq!(parsed.hour(), 8);
        assert_eq!(parsed.nanosecond(), 701_000_000);
    }

    #[test]
    fn parse_timeline_datetime_accepts_url_decoded_positive_offset() {
        let parsed = parse_timeline_datetime("2026-08-31T08:04:25.701 00:00").unwrap();
        assert_eq!(parsed.to_rfc3339(), "2026-08-31T08:04:25.701+00:00");
    }

    #[test]
    fn parse_invocation_scope_accepts_comma_and_whitespace_separators() {
        let parsed = parse_invocation_scope(Some("first, second\nfirst\tthird")).unwrap();
        assert_eq!(parsed.len(), 3);
        assert!(parsed.contains("first"));
        assert!(parsed.contains("second"));
        assert!(parsed.contains("third"));
        assert!(parse_invocation_scope(Some("  , \n\t")).is_none());
    }
}

// Family tree sub-module
mod family_tree {
    use axum::extract::{Path, Query, State};
    use axum::response::IntoResponse;

    use crate::state::AppState;
    use crate::util::view_helpers::AppResult;

    #[derive(serde::Deserialize, Default)]
    pub struct FamilyTreeQuery {
        pub expand: Option<String>,
    }

    pub async fn family_tree_handler(
        State(state): State<AppState>,
        Path(inv_id): Path<String>,
        Query(query): Query<FamilyTreeQuery>,
    ) -> AppResult<impl IntoResponse> {
        let app = crate::util::view_helpers::get_active_app(&state)?;
        let inv_id_typed = rustvello_proto::identifiers::InvocationId::from_string(inv_id.as_str());

        let expand_ids: Vec<String> = query
            .expand
            .map(|s| s.split(',').map(str::to_owned).collect())
            .unwrap_or_default();

        let tree = crate::family_tree::build_family_tree(
            &inv_id_typed,
            &app.orchestrator,
            &app.state_backend,
            &expand_ids,
        )
        .await;

        let svg = match tree {
            Some(root) => crate::family_tree::render_family_tree_svg(&root, Some(&inv_id)),
            None => "<svg><text x=\"10\" y=\"20\" font-size=\"14\" fill=\"#999\">No family tree data available</text></svg>".to_owned(),
        };

        Ok(axum::response::Html(svg))
    }
}
