//! Atomic service timeline monitoring views.

use askama::Template;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::Router;

use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope, TimeWindow};
use crate::query::{PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, render_error, AppResult, HtmlTemplate};
use crate::view::{page_time_range, PaginationView, TemporalExtent};

struct TimelineRow {
    runner_id: String,
    short_id: String,
    start: String,
    end: String,
    duration_secs: String,
    left_percent: f64,
    width_percent: f64,
    overlaps: bool,
    timeline_url: String,
    detail_url: String,
}

#[derive(serde::Deserialize)]
struct AtomicExecutionQuery {
    runner_id: String,
    start: String,
    end: String,
}

#[derive(serde::Deserialize, Default)]
struct AtomicListQuery {
    page: Option<usize>,
    limit: Option<usize>,
}

struct AtomicTriggerRunRow {
    task_id: String,
    claimed_at: String,
    invocation_id: Option<String>,
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
    pagination: PaginationView,
    pagination_path: &'static str,
    pagination_query: String,
}

#[derive(Template)]
#[template(path = "atomic_service/detail.html")]
#[allow(dead_code)]
struct AtomicServiceDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    runner_id: String,
    start: String,
    end: String,
    duration_secs: String,
    timeline_url: String,
    trigger_runs: Vec<AtomicTriggerRunRow>,
    trigger_store_available: bool,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(timeline))
        .route("/execution", axum::routing::get(execution_detail))
}

fn execution_urls(
    runner_id: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> (String, String) {
    let detail_url = MonitoringLink::new(MonitoringDestination::AtomicExecution {
        runner_id: runner_id.to_owned(),
        start,
        end,
    })
    .href();
    let timeline_url = MonitoringLink::new(MonitoringDestination::Timeline)
        .with_scope(MonitoringScope::default().with_time(TimeWindow::fit_default(start, end)))
        .href();
    (detail_url, timeline_url)
}

async fn timeline(
    State(state): State<AppState>,
    Query(query): Query<AtomicListQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let config = &app.config;

    let executions = app
        .orchestrator
        .get_atomic_service_timeline()
        .await
        .unwrap_or_default();

    let page_request = PageRequest::new(query.page, query.limit);
    let total = executions.len();
    let page_executions = executions
        .iter()
        .skip(page_request.offset())
        .take(page_request.limit)
        .collect::<Vec<_>>();
    let page_range = page_time_range(
        page_executions
            .iter()
            .map(|execution| TemporalExtent::new(execution.start, execution.end)),
    );
    let rows: Vec<TimelineRow> = page_executions
        .iter()
        .enumerate()
        .map(|(index, e)| {
            let short_id = crate::util::formatting::truncate_id(&e.runner_id);
            let dur = e.duration_secs();
            let position = page_range.map_or(
                crate::view::TemporalPosition {
                    left_percent: 0.0,
                    width_percent: 0.0,
                },
                |range| TemporalExtent::new(e.start, e.end).position_within(range),
            );
            let overlaps = page_executions
                .iter()
                .enumerate()
                .any(|(other_index, other)| {
                    other_index != index && e.start < other.end && other.start < e.end
                });
            let (detail_url, timeline_url) = execution_urls(&e.runner_id, e.start, e.end);
            TimelineRow {
                runner_id: e.runner_id.clone(),
                short_id,
                start: e.start.format("%H:%M:%S UTC").to_string(),
                end: e.end.format("%H:%M:%S UTC").to_string(),
                duration_secs: format!("{dur:.2}"),
                left_percent: position.left_percent,
                width_percent: position.width_percent,
                overlaps,
                timeline_url,
                detail_url,
            }
        })
        .collect();

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
        pagination: PaginationView::new(
            page_request,
            TotalCount::Exact(total),
            page_request.offset() + page_request.limit < total,
        ),
        pagination_path: "/atomic-service",
        pagination_query: format!("limit={}", page_request.limit),
    }))
}

async fn execution_detail(
    State(state): State<AppState>,
    Query(query): Query<AtomicExecutionQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let start = chrono::DateTime::parse_from_rfc3339(&query.start)
        .map_err(|error| {
            render_error(
                axum::http::StatusCode::BAD_REQUEST,
                &format!("invalid atomic execution start: {error}"),
            )
        })?
        .with_timezone(&chrono::Utc);
    let end = chrono::DateTime::parse_from_rfc3339(&query.end)
        .map_err(|error| {
            render_error(
                axum::http::StatusCode::BAD_REQUEST,
                &format!("invalid atomic execution end: {error}"),
            )
        })?
        .with_timezone(&chrono::Utc);
    let (_, timeline_url) = execution_urls(&query.runner_id, start, end);
    let trigger_store_available = app.trigger_store.is_some();
    let trigger_runs = if let Some(store) = &app.trigger_store {
        store
            .get_trigger_runs(&rustvello_proto::trigger::TriggerRunQuery {
                start: Some(start),
                end: Some(end),
                limit: Some(250),
                ..rustvello_proto::trigger::TriggerRunQuery::default()
            })
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|run| AtomicTriggerRunRow {
                task_id: run.task_id.to_string(),
                claimed_at: run
                    .claimed_at
                    .format("%Y-%m-%d %H:%M:%S%.3f UTC")
                    .to_string(),
                invocation_id: run.triggered_invocation_id.map(|id| id.to_string()),
            })
            .collect()
    } else {
        Vec::new()
    };

    Ok(HtmlTemplate(AtomicServiceDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "atomic_service",
        runner_id: query.runner_id,
        start: start.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        end: end.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        duration_secs: format!("{:.3}", (end - start).num_milliseconds() as f64 / 1_000.0),
        timeline_url,
        trigger_runs,
        trigger_store_available,
    }))
}
