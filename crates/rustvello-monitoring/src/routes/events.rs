//! Event monitoring list and detail views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Json};
use axum::Router;

use rustvello_proto::trigger::{EventQuery, TriggerRunQuery};

use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope, TimeWindow};
use crate::query::{PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::{FilterSummaryItem, PaginationView, TemporalExtent, TemporalPosition};

#[derive(serde::Deserialize, Default)]
pub struct EventsQuery {
    event_code: Option<String>,
    triggered: Option<String>,
    matched: Option<String>,
    page: Option<usize>,
    limit: Option<usize>,
}

struct EventRow {
    event_id: String,
    event_code: String,
    timestamp: String,
    matched: bool,
    triggered: bool,
    matched_count: usize,
    triggered_count: usize,
    emitted_by_invocation_id: Option<String>,
    timeline_url: String,
    left_percent: f64,
    width_percent: f64,
}

struct TriggerRunRow {
    trigger_run_id: String,
    task_id: String,
    claimed_at: String,
    executed_at: Option<String>,
    triggered_invocation_id: Option<String>,
    participant_count: usize,
    timeline_url: String,
}

#[derive(Default)]
struct EventStats {
    rendered: usize,
    matched: usize,
    triggered: usize,
    external: usize,
}

#[derive(Template)]
#[template(path = "events/list.html")]
struct EventsListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    events: Vec<EventRow>,
    event_code: String,
    triggered_filter: String,
    matched_filter: String,
    filter_summary: Vec<FilterSummaryItem>,
    stats: EventStats,
    monitoring_available: bool,
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
}

#[derive(Template)]
#[template(path = "events/detail.html")]
struct EventDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    found: bool,
    event_id: String,
    event_code: String,
    timestamp: String,
    payload: String,
    matched_condition_ids: Vec<String>,
    valid_condition_ids: Vec<String>,
    triggered_invocation_ids: Vec<String>,
    emitted_by_invocation_id: Option<String>,
    emitted_by_task_id: Option<String>,
    emitted_by_runner_id: Option<String>,
    trigger_runs: Vec<TriggerRunRow>,
    timeline_url: String,
    api_url: String,
    trace_url: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/{event_id}/api", axum::routing::get(detail_api))
        .route(
            "/{event_id}/trigger-runs",
            axum::routing::get(trigger_runs_api),
        )
        .route("/{event_id}/trace", axum::routing::get(trace_api))
        .route("/{event_id}", axum::routing::get(detail))
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let event_code = query.event_code.unwrap_or_default();
    let triggered_only = query
        .triggered
        .as_deref()
        .is_none_or(|value| value == "only");
    let matched_only = query.matched.as_deref() == Some("only");
    let page_request = PageRequest::new(query.page, query.limit);
    let mut monitoring_available = false;
    let mut events = Vec::new();
    let mut has_next = false;
    let mut observed_total = TotalCount::Exact(0);
    let fetch_limit = page_request
        .offset()
        .saturating_add(page_request.limit)
        .saturating_add(1);
    if let Some(store) = &app.trigger_store {
        let result = store
            .get_events(&EventQuery {
                event_code: (!event_code.is_empty()).then_some(event_code.clone()),
                matched: matched_only.then_some(true),
                triggered: triggered_only.then_some(true),
                offset: Some(page_request.offset()),
                limit: Some(fetch_limit),
                ..EventQuery::default()
            })
            .await;
        if let Ok(records) = result {
            monitoring_available = true;
            let filtered_records = records.into_iter().collect::<Vec<_>>();
            let mut page_records = filtered_records
                .into_iter()
                .take(page_request.limit.saturating_add(1))
                .collect::<Vec<_>>();
            has_next = page_records.len() > page_request.limit;
            observed_total = if has_next {
                TotalCount::AtLeast(page_request.offset() + page_request.limit + 1)
            } else {
                TotalCount::Exact(page_request.offset() + page_records.len())
            };
            page_records.truncate(page_request.limit);
            let page_range = crate::view::page_time_range(
                page_records
                    .iter()
                    .map(|event| TemporalExtent::new(event.timestamp, event.timestamp)),
            );
            events = page_records
                .into_iter()
                .map(|event| {
                    let matched = event.is_matched();
                    let triggered = event.is_triggered();
                    let timeline_url = timeline_url_for_event_context(
                        event.timestamp,
                        event
                            .emitted_by_invocation_id
                            .as_ref()
                            .into_iter()
                            .chain(event.triggered_invocation_ids.iter())
                            .map(ToString::to_string)
                            .collect(),
                    );
                    let position = page_range.map_or(
                        TemporalPosition {
                            left_percent: 0.0,
                            width_percent: 0.0,
                        },
                        |range| {
                            TemporalExtent::new(event.timestamp, event.timestamp)
                                .position_within(range)
                        },
                    );
                    EventRow {
                        event_id: event.event_id,
                        event_code: event.event_code,
                        timestamp: event.timestamp.to_rfc3339(),
                        matched,
                        triggered,
                        matched_count: event.matched_condition_ids.len(),
                        triggered_count: event.triggered_invocation_ids.len(),
                        emitted_by_invocation_id: event
                            .emitted_by_invocation_id
                            .map(|id| id.to_string()),
                        timeline_url,
                        left_percent: position.left_percent,
                        width_percent: position.width_percent,
                    }
                })
                .collect();
        }
    }

    let mut pagination_query = url::form_urlencoded::Serializer::new(String::new());
    if !event_code.is_empty() {
        pagination_query.append_pair("event_code", &event_code);
    }
    pagination_query.append_pair("triggered", if triggered_only { "only" } else { "all" });
    pagination_query.append_pair("matched", if matched_only { "only" } else { "all" });
    pagination_query.append_pair("limit", &page_request.limit.to_string());

    let mut filter_summary = Vec::new();
    if !event_code.is_empty() {
        filter_summary.push(FilterSummaryItem {
            label: "Event code".to_owned(),
            value: event_code.clone(),
            remove_url: event_filter_url("", triggered_only, matched_only, page_request.limit),
            removable: true,
        });
    }
    filter_summary.push(FilterSummaryItem {
        label: "Triggered".to_owned(),
        value: if triggered_only { "only" } else { "all" }.to_owned(),
        remove_url: event_filter_url(&event_code, false, matched_only, page_request.limit),
        removable: true,
    });
    if matched_only {
        filter_summary.push(FilterSummaryItem {
            label: "Matched".to_owned(),
            value: "only".to_owned(),
            remove_url: event_filter_url(&event_code, triggered_only, false, page_request.limit),
            removable: true,
        });
    }
    filter_summary.push(FilterSummaryItem {
        label: "Page size".to_owned(),
        value: page_request.limit.to_string(),
        remove_url: event_filter_url(&event_code, triggered_only, matched_only, 50),
        removable: false,
    });
    let stats = EventStats {
        rendered: events.len(),
        matched: events.iter().filter(|event| event.matched).count(),
        triggered: events.iter().filter(|event| event.triggered).count(),
        external: events
            .iter()
            .filter(|event| event.emitted_by_invocation_id.is_none())
            .count(),
    };

    Ok(HtmlTemplate(EventsListTemplate {
        app_id: app.app_id,
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "events",
        events,
        event_code,
        triggered_filter: if triggered_only {
            "only".to_owned()
        } else {
            "all".to_owned()
        },
        matched_filter: if matched_only {
            "only".to_owned()
        } else {
            "all".to_owned()
        },
        filter_summary,
        stats,
        monitoring_available,
        pagination: PaginationView::new(page_request, observed_total, has_next),
        pagination_path: "/events",
        pagination_query: pagination_query.finish(),
    }))
}

fn event_filter_url(
    event_code: &str,
    triggered_only: bool,
    matched_only: bool,
    limit: usize,
) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    if !event_code.is_empty() {
        serializer.append_pair("event_code", event_code);
    }
    serializer.append_pair("triggered", if triggered_only { "only" } else { "all" });
    serializer.append_pair("matched", if matched_only { "only" } else { "all" });
    serializer.append_pair("limit", &limit.to_string());
    format!("/events?{}", serializer.finish())
}

async fn detail_api(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let app = get_active_app(&state)?;
    let Some(store) = &app.trigger_store else {
        return Ok(Json(serde_json::json!({
            "event": null,
            "trigger_runs": [],
            "links": {},
        })));
    };
    let event = store.get_event(&event_id).await.ok().flatten();
    let runs = store
        .get_trigger_runs(&TriggerRunQuery {
            event_id: Some(event_id.clone()),
            ..TriggerRunQuery::default()
        })
        .await
        .unwrap_or_default();
    let timeline = event.as_ref().map(|event| {
        timeline_url_for_event_context(
            event.timestamp,
            event
                .emitted_by_invocation_id
                .as_ref()
                .into_iter()
                .chain(event.triggered_invocation_ids.iter())
                .chain(
                    runs.iter()
                        .filter_map(|run| run.triggered_invocation_id.as_ref()),
                )
                .chain(
                    runs.iter()
                        .flat_map(|run| run.source_invocation_ids().into_iter()),
                )
                .map(ToString::to_string)
                .collect(),
        )
    });
    Ok(Json(serde_json::json!({
        "event": event,
        "trigger_runs": runs,
        "links": {
            "detail": format!("/events/{event_id}"),
            "timeline": timeline,
            "trigger_runs": format!("/events/{event_id}/trigger-runs"),
            "trace": format!("/events/{event_id}/trace"),
        },
    })))
}

async fn trigger_runs_api(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let app = get_active_app(&state)?;
    let runs = if let Some(store) = &app.trigger_store {
        store
            .get_trigger_runs(&TriggerRunQuery {
                event_id: Some(event_id.clone()),
                ..TriggerRunQuery::default()
            })
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Ok(Json(serde_json::json!({ "trigger_runs": runs })))
}

async fn trace_api(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let app = get_active_app(&state)?;
    let event = if let Some(store) = &app.trigger_store {
        store.get_event(&event_id).await.ok().flatten()
    } else {
        None
    };
    let runs = if let Some(store) = &app.trigger_store {
        store
            .get_trigger_runs(&TriggerRunQuery {
                event_id: Some(event_id.clone()),
                ..TriggerRunQuery::default()
            })
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let mut source_invocation_ids = Vec::new();
    let mut generated_invocation_ids = Vec::new();
    if let Some(event) = &event {
        if let Some(id) = &event.emitted_by_invocation_id {
            source_invocation_ids.push(id.to_string());
        }
        generated_invocation_ids.extend(
            event
                .triggered_invocation_ids
                .iter()
                .map(ToString::to_string),
        );
    }
    for run in &runs {
        for source_id in run.source_invocation_ids() {
            push_unique(&mut source_invocation_ids, source_id.to_string());
        }
        if let Some(invocation_id) = &run.triggered_invocation_id {
            push_unique(&mut generated_invocation_ids, invocation_id.to_string());
        }
    }
    Ok(Json(serde_json::json!({
        "focus_kind": "event",
        "focus_id": event_id,
        "event": event,
        "trigger_runs": runs,
        "source_invocation_ids": source_invocation_ids,
        "generated_invocation_ids": generated_invocation_ids,
    })))
}

async fn detail(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let event = if let Some(store) = &app.trigger_store {
        store.get_event(&event_id).await.ok().flatten()
    } else {
        None
    };
    let runs = if let Some(store) = &app.trigger_store {
        store
            .get_trigger_runs(&TriggerRunQuery {
                event_id: Some(event_id.clone()),
                ..TriggerRunQuery::default()
            })
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let trigger_runs = runs
        .into_iter()
        .map(|run| TriggerRunRow {
            timeline_url: timeline_url_for_event_context(
                event
                    .as_ref()
                    .map_or(run.claimed_at, |event| event.timestamp),
                run.triggered_invocation_id
                    .as_ref()
                    .into_iter()
                    .chain(run.source_invocation_ids().into_iter())
                    .map(ToString::to_string)
                    .collect(),
            ),
            trigger_run_id: run.trigger_run_id.to_string(),
            task_id: run.task_id.to_string(),
            claimed_at: run.claimed_at.to_rfc3339(),
            executed_at: run.executed_at.map(|dt| dt.to_rfc3339()),
            triggered_invocation_id: run.triggered_invocation_id.map(|id| id.to_string()),
            participant_count: run.participants.len(),
        })
        .collect();
    let timeline_url = event.as_ref().map_or_else(
        || MonitoringLink::new(MonitoringDestination::Timeline).href(),
        |event| {
            timeline_url_for_event_context(
                event.timestamp,
                event
                    .emitted_by_invocation_id
                    .as_ref()
                    .into_iter()
                    .chain(event.triggered_invocation_ids.iter())
                    .map(ToString::to_string)
                    .collect(),
            )
        },
    );

    Ok(HtmlTemplate(EventDetailTemplate {
        app_id: app.app_id,
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "events",
        found: event.is_some(),
        event_id: event_id.clone(),
        event_code: event
            .as_ref()
            .map_or_else(String::new, |e| e.event_code.clone()),
        timestamp: event
            .as_ref()
            .map_or_else(String::new, |e| e.timestamp.to_rfc3339()),
        payload: event
            .as_ref()
            .and_then(|e| serde_json::to_string_pretty(&e.payload).ok())
            .unwrap_or_default(),
        matched_condition_ids: event
            .as_ref()
            .map(|e| {
                e.matched_condition_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        valid_condition_ids: event
            .as_ref()
            .map(|e| e.valid_condition_ids.clone())
            .unwrap_or_default(),
        triggered_invocation_ids: event
            .as_ref()
            .map(|e| {
                e.triggered_invocation_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        emitted_by_invocation_id: event
            .as_ref()
            .and_then(|e| e.emitted_by_invocation_id.as_ref().map(ToString::to_string)),
        emitted_by_task_id: event
            .as_ref()
            .and_then(|e| e.emitted_by_task_id.as_ref().map(ToString::to_string)),
        emitted_by_runner_id: event
            .as_ref()
            .and_then(|e| e.emitted_by_runner_id.as_ref().map(ToString::to_string)),
        trigger_runs,
        timeline_url,
        api_url: format!("/events/{event_id}/api"),
        trace_url: format!("/events/{event_id}/trace"),
    }))
}

fn timeline_url_for_event_context(
    timestamp: chrono::DateTime<chrono::Utc>,
    invocation_ids: Vec<String>,
) -> String {
    let mut unique_invocation_ids = Vec::new();
    for invocation_id in invocation_ids {
        push_unique(&mut unique_invocation_ids, invocation_id);
    }

    let mut scope = MonitoringScope::default();
    if unique_invocation_ids.is_empty() {
        scope = scope.with_time(TimeWindow::fit_default(timestamp, timestamp));
    } else {
        scope.invocation_ids = unique_invocation_ids.clone();
    }

    let mut link = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(scope)
        .with_limit(500);
    if let Some(selected) = unique_invocation_ids.first() {
        link = link.with_selected_invocation(selected.clone());
    }
    link.href()
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}
