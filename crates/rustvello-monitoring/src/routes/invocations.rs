//! Invocation views: list, timeline, detail.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

use rustvello_proto::status::InvocationStatus;

// ---------------------------------------------------------------------------
// Query / template structs
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize, Default)]
pub struct InvocationListQuery {
    pub status: Option<String>,
    pub task_id: Option<String>,
    pub workflow_type: Option<String>,
    pub workflow_id: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

#[allow(dead_code)]
struct InvocationRow {
    invocation_id: String,
    short_id: String,
    task_id: String,
    call_id: String,
    status: String,
    status_class: String,
    num_retries: usize,
    is_workflow_defining: bool,
}

/// Current filter values echoed back to the template.
struct CurrentFilters {
    statuses: Vec<String>,
    task_id: String,
    workflow_type: String,
    workflow_id: String,
    limit: usize,
}

struct Pagination {
    page: usize,
    limit: usize,
    total_count: usize,
    total_pages: usize,
    has_prev: bool,
    has_next: bool,
}

#[derive(Template)]
#[template(path = "invocations/list.html")]
#[allow(dead_code)]
struct InvocationListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    invocations: Vec<InvocationRow>,
    all_statuses: Vec<String>,
    all_task_ids: Vec<String>,
    all_workflow_types: Vec<String>,
    current_filters: CurrentFilters,
    pagination: Pagination,
    status_query: String,
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
}

struct HistoryEntry {
    status: String,
    status_class: String,
    timestamp: String,
    message: Option<String>,
    runner_id: Option<String>,
    runner_cls: Option<String>,
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
}

fn parse_timeline_datetime(value: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // Query-string decoding turns an unescaped `+00:00` offset into a space.
    // Normalize that form so links from event and trigger pages remain valid.
    let normalized = value.replace(' ', "+");
    chrono::DateTime::parse_from_rfc3339(&normalized)
        .map(|datetime| datetime.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.3f")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S"))
                .map(|datetime| datetime.and_utc())
        })
        .ok()
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
}

#[derive(Template)]
#[template(path = "invocations/timeline.html")]
#[allow(dead_code)]
struct InvocationTimelineTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    svg_content: String,
    all_task_ids: Vec<String>,
    all_workflow_types: Vec<String>,
    current_filters: TimelineFilters,
    start_datetime: String,
    end_datetime: String,
    selected_invocation: String,
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

    let mut invocations = Vec::new();

    // Hard cap: collect at most `max_collect` invocations to prevent DoS
    // from unbounded iteration over all tasks × all invocations.
    let max_collect = page * limit;

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

    'outer: for tid in &task_ids {
        let ids = app
            .orchestrator
            .get_invocations_by_task(tid)
            .await
            .unwrap_or_default();
        for inv_id in ids {
            let inv = app.state_backend.get_invocation(&inv_id).await.ok();
            // Use latest status from history if available, fall back to orchestrator
            let history_records = app
                .state_backend
                .get_history(&inv_id)
                .await
                .unwrap_or_default();
            let status = if let Some(last) = history_records.last() {
                last.status_record.status
            } else {
                app.orchestrator
                    .get_invocation_status(&inv_id)
                    .await
                    .map(|r| r.status)
                    .unwrap_or(InvocationStatus::Registered)
            };

            if let Some(status_filter) = &query.status {
                if !status_filter.is_empty() {
                    let selected: Vec<&str> = status_filter.split(',').collect();
                    let status_str = format!("{status:?}");
                    if !selected.iter().any(|s| *s == status_str) {
                        continue;
                    }
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

            // Count retries from history
            let history = app
                .state_backend
                .get_history(&inv_id)
                .await
                .unwrap_or_default();
            let num_retries = history
                .iter()
                .filter(|h| h.status_record.status == InvocationStatus::Retry)
                .count();

            invocations.push(InvocationRow {
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

    let total_count = invocations.len();

    // Paginate
    let start = (page - 1) * limit;
    let paginated: Vec<InvocationRow> = invocations.into_iter().skip(start).take(limit).collect();
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

    // Workflow types: not yet stored per-invocation, so empty for now
    let all_workflow_types: Vec<String> = Vec::new();

    let status_raw = query.status.unwrap_or_default();
    let status_query = status_raw.clone();
    let statuses_vec: Vec<String> = status_raw
        .split(',')
        .filter(|s| !s.is_empty())
        .map(std::borrow::ToOwned::to_owned)
        .collect();

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
            task_id: query.task_id.unwrap_or_default(),
            workflow_type: query.workflow_type.unwrap_or_default(),
            workflow_id: query.workflow_id.unwrap_or_default(),
            limit,
        },
        pagination: Pagination {
            page,
            limit,
            total_count,
            total_pages,
            has_prev: page > 1,
            has_next: page < total_pages,
        },
        status_query,
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
        workflow_type,
        workflow_id,
        is_workflow_defining,
        created_at,
        completed_at,
        duration,
        history,
        result,
        error,
        arguments,
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
    let mut auto_zoom_start: Option<String> = None;
    let mut auto_zoom_end: Option<String> = None;
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
                    let span = (max_t - min_t).num_milliseconds().max(1);
                    let padding =
                        chrono::Duration::milliseconds((span as f64 * 0.5).max(100.0) as i64);
                    start_dt = min_t - padding;
                    end_dt = max_t + padding;
                    time_range = "custom".to_owned();
                    auto_zoom_start = Some(start_dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string());
                    auto_zoom_end = Some(end_dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string());
                }
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
    for inv_id in &candidate_ids {
        if limit.is_some_and(|maximum| inv_count >= maximum) {
            break;
        }

        let history = app
            .state_backend
            .get_history(inv_id)
            .await
            .unwrap_or_default();
        if history.is_empty() {
            continue;
        }

        let invocation = app.state_backend.get_invocation(inv_id).await.ok();
        let Some(invocation) = invocation else {
            continue;
        };
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
        builder.add_history_batch_for_task(history, &task_id);
        inv_count += 1;
    }

    // Fetch runner contexts for enriched labels
    let mut runner_contexts = std::collections::HashMap::new();
    for rid in &runner_ids_seen {
        if let Ok(Some(ctx)) = app.state_backend.get_runner_context(rid).await {
            runner_contexts.insert(rid.clone(), ctx);
        }
    }
    builder.set_runner_contexts(runner_contexts);

    let data = builder.build();
    let svg_content = crate::svg::TimelineSvgRenderer::render(&data);

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

    Ok(HtmlTemplate(InvocationTimelineTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "timeline",
        svg_content,
        all_task_ids,
        all_workflow_types,
        current_filters: TimelineFilters {
            time_range: time_range.to_owned(),
            start_date: auto_zoom_start.or(query.start_date).unwrap_or_default(),
            end_date: auto_zoom_end.or(query.end_date).unwrap_or_default(),
            task_id: query.task_id.unwrap_or_default(),
            workflow_type: query.workflow_type.unwrap_or_default(),
            workflow_id: query.workflow_id.unwrap_or_default(),
            limit: query.limit.unwrap_or_else(|| "500".to_owned()),
            inv_ids: query.inv_ids.unwrap_or_default(),
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
                // Task language info
                let task_language = if inv.task_id.is_foreign() {
                    inv.task_id.language().to_owned()
                } else {
                    String::new()
                };
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
                    if let Err(e) = app.broker.route_invocation(&inv.invocation_id).await {
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
