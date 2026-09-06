//! Runner monitoring views.

use askama::Template;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Router;
use chrono::Utc;

use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope};
use crate::query::{load_invocation_rows, PageRequest, TotalCount};
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::view::{InvocationRowView, PaginationView};

#[derive(serde::Deserialize, Default)]
pub struct RunnerDetailQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Template)]
#[template(path = "runners/overview.html")]
#[allow(dead_code)]
struct RunnersOverviewTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    total_runners: usize,
    atomic_eligible: usize,
    runners_with_history: usize,
    heartbeat_timeout_minutes: f64,
    atomic_check_interval_minutes: f64,
    heartbeat_interval_seconds: u64,
    runners: Vec<RunnerRow>,
    runner_implementation: String,
}

#[allow(dead_code)]
struct RunnerRow {
    runner_id: String,
    short_id: String,
    runner_cls: String,
    runner_language: String,
    executor_kind: String,
    hostname: String,
    pid: String,
    started_at: String,
    last_heartbeat_secs_ago: u64,
    is_active: bool,
    is_atomic_eligible: bool,
    has_execution_history: bool,
    last_service_start: String,
    last_service_duration_secs: String,
    parent_runner_id: Option<String>,
    parent_runner_cls: Option<String>,
}

#[derive(Template)]
#[template(path = "runners/detail.html")]
#[allow(dead_code)]
struct RunnerDetailTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    runner_id: String,
    runner_cls: String,
    runner_language: String,
    executor_kind: String,
    hostname: String,
    pid: String,
    thread_id: String,
    started_at: String,
    // Parent context
    parent_runner_id: Option<String>,
    parent_runner_cls: Option<String>,
    parent_hostname: Option<String>,
    parent_pid: Option<String>,
    parent_thread_id: Option<String>,
    // Heartbeat status
    last_heartbeat_secs_ago: u64,
    is_active: bool,
    is_atomic_eligible: bool,
    has_execution_history: bool,
    last_service_start: String,
    last_service_duration_secs: String,
    // Workers (child runners)
    workers: Vec<RunnerWorkerRow>,
    // Invocations
    invocation_count: usize,
    invocations: Vec<InvocationRowView>,
    // Pagination
    pagination: PaginationView,
    pagination_path: String,
    pagination_query: String,
    timeline_url: String,
}

struct RunnerWorkerRow {
    runner_id: String,
    short_id: String,
    runner_cls: String,
    runner_language: String,
    executor_kind: String,
    hostname: String,
    pid: String,
    is_active: bool,
    last_heartbeat_secs_ago: u64,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(overview))
        .route("/refresh", axum::routing::get(refresh))
        .route("/{runner_id}", axum::routing::get(detail))
}

async fn collect_runner_rows(app: &crate::AppInstance) -> Vec<RunnerRow> {
    let timeout = app.config.runner_dead_after_seconds;
    let active_runners = app
        .orchestrator
        .get_active_runners(timeout, None)
        .await
        .unwrap_or_default();

    let mut rows = Vec::new();
    for runner in &active_runners {
        let ctx = app
            .state_backend
            .get_runner_context(runner.runner_id.as_ref())
            .await
            .ok()
            .flatten();

        let hb_secs_ago = (Utc::now() - runner.last_heartbeat).num_seconds().max(0) as u64;
        let is_active = hb_secs_ago <= timeout;

        let has_execution_history = runner.last_service_start.is_some();

        let last_service_start = runner
            .last_service_start
            .map_or_else(|| "—".to_owned(), |t| t.format("%H:%M:%S UTC").to_string());

        let last_service_duration_secs = match (runner.last_service_start, runner.last_service_end)
        {
            (Some(s), Some(e)) => {
                let d = (e - s).num_milliseconds() as f64 / 1000.0;
                format!("{d:.2}s")
            }
            _ => "—".to_owned(),
        };

        let short_id = crate::util::formatting::truncate_id(runner.runner_id.as_ref());
        rows.push(RunnerRow {
            runner_id: runner.runner_id.to_string(),
            short_id,
            runner_cls: ctx
                .as_ref()
                .map_or_else(|| "Unknown".to_owned(), |c| c.runner_cls.clone()),
            runner_language: ctx
                .as_ref()
                .map_or_else(|| "unknown".to_owned(), |c| c.runner_language.to_string()),
            executor_kind: ctx
                .as_ref()
                .map_or_else(|| "unknown".to_owned(), |c| c.executor_kind.to_string()),
            hostname: ctx
                .as_ref()
                .map_or_else(|| "—".to_owned(), |c| c.hostname.clone()),
            pid: ctx
                .as_ref()
                .map_or_else(|| "—".to_owned(), |c| c.pid.to_string()),
            started_at: ctx.as_ref().map_or_else(
                || "—".to_owned(),
                |c| c.started_at.format("%Y-%m-%d %H:%M:%S").to_string(),
            ),
            last_heartbeat_secs_ago: hb_secs_ago,
            is_active,
            is_atomic_eligible: runner.can_run_atomic_service,
            has_execution_history,
            last_service_start,
            last_service_duration_secs,
            parent_runner_id: ctx.as_ref().and_then(|c| c.parent_runner_id.clone()),
            parent_runner_cls: ctx.as_ref().and_then(|c| c.parent_runner_cls.clone()),
        });
    }
    rows.sort_by(|a, b| a.runner_id.cmp(&b.runner_id));
    rows
}

async fn overview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let config = &app.config;
    let runners = collect_runner_rows(&app).await;
    let total_runners = runners.len();
    let atomic_eligible = runners.iter().filter(|r| r.is_atomic_eligible).count();
    let runners_with_history = runners.iter().filter(|r| r.has_execution_history).count();
    // Determine runner implementation from the first parent runner (non-child)
    let runner_implementation = runners
        .iter()
        .find(|r| r.parent_runner_id.is_none())
        .or(runners.first())
        .map_or_else(|| "Unknown".to_owned(), |r| r.runner_cls.clone());
    Ok(HtmlTemplate(RunnersOverviewTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "runners",
        total_runners,
        atomic_eligible,
        runners_with_history,
        heartbeat_timeout_minutes: config.runner_dead_after_seconds as f64 / 60.0,
        atomic_check_interval_minutes: config.atomic_service_check_interval_minutes,
        heartbeat_interval_seconds: config.heartbeat_interval_seconds,
        runners,
        runner_implementation,
    }))
}

#[derive(Template)]
#[template(path = "runners/partials/runners_table.html")]
struct RunnersTablePartial {
    runners: Vec<RunnerRow>,
}

async fn refresh(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let runners = collect_runner_rows(&app).await;
    Ok(HtmlTemplate(RunnersTablePartial { runners }))
}

async fn detail(
    State(state): State<AppState>,
    Path(runner_id): Path<String>,
    Query(query): Query<RunnerDetailQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let timeout = app.config.runner_dead_after_seconds;
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(50).min(200);

    let ctx = app
        .state_backend
        .get_runner_context(&runner_id)
        .await
        .ok()
        .flatten();

    // Parent context (if this runner has a parent)
    let parent_ctx = if let Some(pid) = ctx.as_ref().and_then(|c| c.parent_runner_id.as_ref()) {
        app.state_backend
            .get_runner_context(pid)
            .await
            .ok()
            .flatten()
    } else {
        None
    };

    // Find this runner in active runners for heartbeat/atomic info
    let active_runners = app
        .orchestrator
        .get_active_runners(timeout, None)
        .await
        .unwrap_or_default();
    let active_runner = active_runners
        .iter()
        .find(|r| r.runner_id.to_string() == runner_id);

    let now = Utc::now();
    let last_heartbeat_secs_ago =
        active_runner.map_or(0, |r| (now - r.last_heartbeat).num_seconds().max(0) as u64);
    let is_active = active_runner
        .is_some_and(|r| (now - r.last_heartbeat).num_seconds().max(0) as u64 <= timeout);
    let is_atomic_eligible = active_runner.is_some_and(|r| r.can_run_atomic_service);
    let has_execution_history = active_runner.is_some_and(|r| r.last_service_start.is_some());
    let last_service_start = active_runner
        .and_then(|r| r.last_service_start)
        .map_or_else(|| "—".to_owned(), |t| t.format("%H:%M:%S UTC").to_string());
    let last_service_duration_secs =
        match active_runner.and_then(|r| r.last_service_start.zip(r.last_service_end)) {
            Some((s, e)) => {
                let d = (e - s).num_milliseconds() as f64 / 1000.0;
                format!("{d:.2}s")
            }
            None => "—".to_owned(),
        };

    // Collect child workers from state backend (not filtered by heartbeat)
    let child_contexts = app
        .state_backend
        .get_runner_contexts_by_parent(&runner_id)
        .await
        .unwrap_or_default();
    let mut workers: Vec<RunnerWorkerRow> = child_contexts
        .iter()
        .map(|cctx| {
            // Check if worker is still active by looking up in active_runners
            let active = active_runners
                .iter()
                .find(|r| r.runner_id.to_string() == cctx.runner_id);
            let (is_active, hb_secs) = if let Some(ar) = active {
                let secs = (Utc::now() - ar.last_heartbeat).num_seconds().max(0) as u64;
                (secs <= timeout, secs)
            } else {
                (false, 0)
            };
            RunnerWorkerRow {
                runner_id: cctx.runner_id.clone(),
                short_id: crate::util::formatting::truncate_id(&cctx.runner_id),
                runner_cls: cctx.runner_cls.clone(),
                runner_language: cctx.runner_language.to_string(),
                executor_kind: cctx.executor_kind.to_string(),
                hostname: cctx.hostname.clone(),
                pid: cctx.pid.to_string(),
                is_active,
                last_heartbeat_secs_ago: hb_secs,
            }
        })
        .collect();
    workers.sort_by(|a, b| a.runner_id.cmp(&b.runner_id));

    // Collect invocations processed by this runner (and its workers) using state backend index
    let mut all_runner_ids = vec![runner_id.clone()];
    for w in &workers {
        all_runner_ids.push(w.runner_id.clone());
    }

    // Count total invocations across this runner and its workers
    let mut total_count = 0usize;
    for rid in &all_runner_ids {
        total_count += app
            .state_backend
            .count_invocations_by_runner(rid)
            .await
            .unwrap_or(0);
    }

    let total_pages = if total_count == 0 {
        1
    } else {
        total_count.div_ceil(limit)
    };
    let offset = (page - 1) * limit;

    // Gather paginated invocation IDs across all runner IDs
    let mut invocation_ids = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut remaining = limit;
    let mut skip = offset;

    for rid in &all_runner_ids {
        if remaining == 0 {
            break;
        }
        let count = app
            .state_backend
            .count_invocations_by_runner(rid)
            .await
            .unwrap_or(0);
        if skip >= count {
            skip -= count;
            continue;
        }
        let inv_ids = app
            .state_backend
            .get_invocation_ids_by_runner(rid, remaining, skip)
            .await
            .unwrap_or_default();
        skip = 0;
        for inv_id in inv_ids {
            if remaining == 0 {
                break;
            }
            if !seen.insert(inv_id.to_string()) {
                continue;
            }
            invocation_ids.push(inv_id);
            remaining -= 1;
        }
    }
    let runner_scope = MonitoringScope {
        runner_ids: all_runner_ids.clone(),
        ..MonitoringScope::default()
    };
    let invocations = load_invocation_rows(&app, invocation_ids, runner_scope).await;

    Ok(HtmlTemplate(RunnerDetailTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "runners",
        runner_id: runner_id.clone(),
        runner_cls: ctx
            .as_ref()
            .map_or_else(|| "Unknown".to_string(), |c| c.runner_cls.clone()),
        runner_language: ctx
            .as_ref()
            .map_or_else(|| "unknown".to_string(), |c| c.runner_language.to_string()),
        executor_kind: ctx
            .as_ref()
            .map_or_else(|| "unknown".to_string(), |c| c.executor_kind.to_string()),
        hostname: ctx
            .as_ref()
            .map_or_else(|| "—".to_string(), |c| c.hostname.clone()),
        pid: ctx
            .as_ref()
            .map_or_else(|| "—".to_string(), |c| c.pid.to_string()),
        thread_id: ctx
            .as_ref()
            .map_or_else(|| "—".to_string(), |c| c.thread_id.to_string()),
        started_at: ctx.as_ref().map_or_else(
            || "—".to_string(),
            |c| c.started_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        ),
        parent_runner_id: ctx.as_ref().and_then(|c| c.parent_runner_id.clone()),
        parent_runner_cls: ctx.as_ref().and_then(|c| c.parent_runner_cls.clone()),
        parent_hostname: parent_ctx.as_ref().map(|c| c.hostname.clone()),
        parent_pid: parent_ctx.as_ref().map(|c| c.pid.to_string()),
        parent_thread_id: parent_ctx.as_ref().map(|c| c.thread_id.to_string()),
        last_heartbeat_secs_ago,
        is_active,
        is_atomic_eligible,
        has_execution_history,
        last_service_start,
        last_service_duration_secs,
        workers,
        invocation_count: total_count,
        invocations,
        pagination: PaginationView::new(
            PageRequest::new(Some(page), Some(limit)),
            TotalCount::Exact(total_count),
            page < total_pages,
        ),
        pagination_path: format!("/runners/{runner_id}"),
        pagination_query: format!("limit={limit}"),
        timeline_url: MonitoringLink::new(MonitoringDestination::Timeline)
            .with_scope(MonitoringScope {
                runner_ids: vec![runner_id.clone()],
                ..MonitoringScope::default()
            })
            .href(),
    }))
}
