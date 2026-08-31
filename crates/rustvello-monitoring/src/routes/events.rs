//! Event monitoring list and detail views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;

use rustvello_proto::trigger::{EventQuery, TriggerRunQuery};

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

#[derive(serde::Deserialize, Default)]
pub struct EventsQuery {
    event_code: Option<String>,
    limit: Option<usize>,
}

struct EventRow {
    event_id: String,
    event_code: String,
    timestamp: String,
    matched: bool,
    triggered: bool,
    emitted_by_invocation_id: Option<String>,
}

struct TriggerRunRow {
    trigger_run_id: String,
    task_id: String,
    claimed_at: String,
    triggered_invocation_id: Option<String>,
}

#[derive(Template)]
#[template(path = "events/list.html")]
struct EventsListTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    events: Vec<EventRow>,
    event_code: String,
    monitoring_available: bool,
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
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(list))
        .route("/{event_id}", axum::routing::get(detail))
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<EventsQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let event_code = query.event_code.unwrap_or_default();
    let mut monitoring_available = false;
    let mut events = Vec::new();
    if let Some(store) = &app.trigger_store {
        let result = store
            .get_events(&EventQuery {
                event_code: (!event_code.is_empty()).then_some(event_code.clone()),
                limit: Some(query.limit.unwrap_or(100).min(500)),
                ..EventQuery::default()
            })
            .await;
        if let Ok(records) = result {
            monitoring_available = true;
            events = records
                .into_iter()
                .map(|event| {
                    let matched = event.is_matched();
                    let triggered = event.is_triggered();
                    EventRow {
                        event_id: event.event_id,
                        event_code: event.event_code,
                        timestamp: event.timestamp.to_rfc3339(),
                        matched,
                        triggered,
                        emitted_by_invocation_id: event
                            .emitted_by_invocation_id
                            .map(|id| id.to_string()),
                    }
                })
                .collect();
        }
    }

    Ok(HtmlTemplate(EventsListTemplate {
        app_id: app.app_id,
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "events",
        events,
        event_code,
        monitoring_available,
    }))
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
            trigger_run_id: run.trigger_run_id.to_string(),
            task_id: run.task_id.to_string(),
            claimed_at: run.claimed_at.to_rfc3339(),
            triggered_invocation_id: run.triggered_invocation_id.map(|id| id.to_string()),
        })
        .collect();
    let timeline_url = event.as_ref().map_or_else(
        || "/invocations/timeline".into(),
        |event| {
            let start = (event.timestamp - chrono::Duration::seconds(5)).to_rfc3339();
            let end = (event.timestamp + chrono::Duration::seconds(5)).to_rfc3339();
            format!("/invocations/timeline?time_range=custom&start_date={start}&end_date={end}")
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
    }))
}
