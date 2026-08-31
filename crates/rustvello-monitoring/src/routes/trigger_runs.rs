//! Trigger-run monitoring detail view.

use askama::Template;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::Router;

use rustvello_proto::trigger::TriggerRunId;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

struct ParticipantRow {
    context_type: String,
    condition_id: String,
    valid_condition_id: String,
    event_id: Option<String>,
    source_invocation_id: Option<String>,
    context_summary: String,
}

#[derive(Template)]
#[template(path = "trigger_runs/detail.html")]
struct TriggerRunDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    found: bool,
    trigger_run_id: String,
    trigger_id: String,
    task_id: String,
    logic: String,
    arguments: String,
    claimed_at: String,
    executed_at: Option<String>,
    triggered_invocation_id: Option<String>,
    participants: Vec<ParticipantRow>,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/{run_id}", axum::routing::get(detail))
}

async fn detail(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let run = if let Some(store) = &app.trigger_store {
        store
            .get_trigger_run(&TriggerRunId::from(run_id.clone()))
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    let participants = run
        .as_ref()
        .map(|record| {
            record
                .participants
                .iter()
                .map(|participant| ParticipantRow {
                    context_type: participant.context_type.clone(),
                    condition_id: participant.condition_id.to_string(),
                    valid_condition_id: participant.valid_condition_id.clone(),
                    event_id: participant.event_id.clone(),
                    source_invocation_id: participant
                        .source_invocation_id
                        .as_ref()
                        .map(ToString::to_string),
                    context_summary: participant.context_summary.clone(),
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(HtmlTemplate(TriggerRunDetailTemplate {
        app_id: app.app_id,
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "events",
        found: run.is_some(),
        trigger_run_id: run_id,
        trigger_id: run
            .as_ref()
            .map_or_else(String::new, |r| r.trigger_id.to_string()),
        task_id: run
            .as_ref()
            .map_or_else(String::new, |r| r.task_id.to_string()),
        logic: run
            .as_ref()
            .map_or_else(String::new, |r| r.logic.to_string()),
        arguments: run
            .as_ref()
            .and_then(|r| serde_json::to_string_pretty(&r.arguments).ok())
            .unwrap_or_default(),
        claimed_at: run
            .as_ref()
            .map_or_else(String::new, |r| r.claimed_at.to_rfc3339()),
        executed_at: run
            .as_ref()
            .and_then(|r| r.executed_at.map(|value| value.to_rfc3339())),
        triggered_invocation_id: run
            .as_ref()
            .and_then(|r| r.triggered_invocation_id.as_ref().map(ToString::to_string)),
        participants,
    }))
}
