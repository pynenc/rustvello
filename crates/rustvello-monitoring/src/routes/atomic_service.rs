//! Atomic service timeline monitoring views.

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

struct TimelineRow {
    runner_id: String,
    short_id: String,
    start: String,
    end: String,
    duration_secs: String,
}

#[derive(Template)]
#[template(path = "atomic_service/timeline.html")]
#[allow(dead_code)]
struct AtomicServiceTimelineTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    service_interval_minutes: f64,
    spread_margin_minutes: f64,
    check_interval_minutes: f64,
    total_executions: usize,
    avg_duration_secs: String,
    max_duration_secs: String,
    rows: Vec<TimelineRow>,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/", axum::routing::get(timeline))
}

async fn timeline(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let config = &app.config;

    let executions = app
        .orchestrator
        .get_atomic_service_timeline()
        .await
        .unwrap_or_default();

    let rows: Vec<TimelineRow> = executions
        .iter()
        .map(|e| {
            let short_id = crate::util::formatting::truncate_id(&e.runner_id);
            let dur = e.duration_secs();
            TimelineRow {
                runner_id: e.runner_id.clone(),
                short_id,
                start: e.start.format("%H:%M:%S UTC").to_string(),
                end: e.end.format("%H:%M:%S UTC").to_string(),
                duration_secs: format!("{dur:.2}"),
            }
        })
        .collect();

    let total = rows.len();
    let avg = if total > 0 {
        executions
            .iter()
            .map(rustvello_core::orchestrator::AtomicServiceExecution::duration_secs)
            .sum::<f64>()
            / total as f64
    } else {
        0.0
    };
    let max = executions
        .iter()
        .map(rustvello_core::orchestrator::AtomicServiceExecution::duration_secs)
        .fold(0.0_f64, f64::max);

    Ok(HtmlTemplate(AtomicServiceTimelineTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "atomic_service",
        service_interval_minutes: config.atomic_service_interval_minutes,
        spread_margin_minutes: config.atomic_service_spread_margin_minutes,
        check_interval_minutes: config.atomic_service_check_interval_minutes,
        total_executions: total,
        avg_duration_secs: format!("{avg:.2}"),
        max_duration_secs: format!("{max:.2}"),
        rows,
    }))
}
