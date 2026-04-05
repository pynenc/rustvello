//! Orchestrator monitoring views.

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::formatting::cron_to_human;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

use rustvello_proto::status::InvocationStatus;

/// A blocking invocation row for the orchestrator stats.
struct BlockingRow {
    invocation_id: String,
    short_id: String,
    task_id: String,
    status: String,
    status_color: String,
}

#[derive(Template)]
#[template(path = "orchestrator/overview.html")]
#[allow(dead_code)]
struct OrchestratorOverviewTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    status_counts: Vec<(String, usize, String)>,
    total_invocations: usize,
    blocking_invocations: Vec<BlockingRow>,
    blocking_control: bool,
    max_pending_seconds: u64,
    runner_dead_after_seconds: u64,
    auto_purge_hours: f64,
    atomic_svc_interval: f64,
    atomic_svc_spread_margin: f64,
    atomic_svc_check_interval: f64,
    recover_pending_cron: String,
    recover_pending_human: String,
    recover_running_cron: String,
    recover_running_human: String,
    runner_dead_after_minutes: f64,
    active_runner_count: usize,
    backend_name: &'static str,
    usage_stats: Vec<(&'static str, String)>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(overview))
        .route("/refresh", axum::routing::get(refresh))
}

async fn overview(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let status_counts = collect_status_counts(&app).await;
    let total_invocations: usize = status_counts.iter().map(|(_, c, _)| c).sum();
    let blocking_invocations = collect_blocking(&app).await;
    let config = &app.config;
    let active_runner_count = app
        .orchestrator
        .get_active_runner_ids(config.runner_dead_after_seconds)
        .await
        .map(|ids| ids.len())
        .unwrap_or(0);
    let backend_name = app.orchestrator.backend_name();
    let usage_stats = app.orchestrator.usage_stats().await;
    Ok(HtmlTemplate(OrchestratorOverviewTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "orchestrator",
        status_counts,
        total_invocations,
        blocking_invocations,
        blocking_control: config.blocking_control,
        max_pending_seconds: config.max_pending_seconds,
        runner_dead_after_seconds: config.runner_dead_after_seconds,
        auto_purge_hours: config.auto_final_invocation_purge_hours,
        atomic_svc_interval: config.atomic_service_interval_minutes,
        atomic_svc_spread_margin: config.atomic_service_spread_margin_minutes,
        atomic_svc_check_interval: config.atomic_service_check_interval_minutes,
        recover_pending_cron: config.recover_pending_cron.clone(),
        recover_pending_human: cron_to_human(&config.recover_pending_cron),
        recover_running_cron: config.recover_running_cron.clone(),
        recover_running_human: cron_to_human(&config.recover_running_cron),
        runner_dead_after_minutes: config.runner_dead_after_seconds as f64 / 60.0,
        active_runner_count,
        backend_name,
        usage_stats,
    }))
}

#[derive(Template)]
#[template(path = "orchestrator/partials/stats.html")]
struct OrchestratorStatsPartial {
    status_counts: Vec<(String, usize, String)>,
    total_invocations: usize,
    blocking_invocations: Vec<BlockingRow>,
}

async fn refresh(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let status_counts = collect_status_counts(&app).await;
    let total_invocations: usize = status_counts.iter().map(|(_, c, _)| c).sum();
    let blocking_invocations = collect_blocking(&app).await;
    Ok(HtmlTemplate(OrchestratorStatsPartial {
        status_counts,
        total_invocations,
        blocking_invocations,
    }))
}

async fn collect_status_counts(app: &crate::AppInstance) -> Vec<(String, usize, String)> {
    let statuses = [
        InvocationStatus::Registered,
        InvocationStatus::ConcurrencyControlled,
        InvocationStatus::ConcurrencyControlledFinal,
        InvocationStatus::Rerouted,
        InvocationStatus::Pending,
        InvocationStatus::PendingRecovery,
        InvocationStatus::Running,
        InvocationStatus::RunningRecovery,
        InvocationStatus::Paused,
        InvocationStatus::Resumed,
        InvocationStatus::Killed,
        InvocationStatus::Success,
        InvocationStatus::Failed,
        InvocationStatus::Retry,
    ];

    let mut counts = Vec::with_capacity(statuses.len());
    for status in &statuses {
        let count = app
            .orchestrator
            .get_invocations_by_status(*status, None)
            .await
            .map(|ids| ids.len())
            .unwrap_or(0);
        let color = status_colors::hex_color(status);
        counts.push((format!("{status:?}"), count, color.to_owned()));
    }
    counts
}

async fn collect_blocking(app: &crate::AppInstance) -> Vec<BlockingRow> {
    let blocking_ids = app
        .orchestrator
        .get_blocking_invocations(10)
        .await
        .unwrap_or_default();

    let mut rows = Vec::new();
    for inv_id in blocking_ids {
        if let Ok(record) = app.orchestrator.get_invocation_status(&inv_id).await {
            let id_str = inv_id.to_string();
            let short = crate::util::formatting::short_id(&id_str);
            let status = record.status;
            let color = status_colors::hex_color(&status);
            // Try to find the task_id from state backend
            let task_id = app
                .state_backend
                .get_invocation(&inv_id)
                .await
                .map_or_else(|_| "—".to_owned(), |dto| dto.task_id.to_string());
            rows.push(BlockingRow {
                invocation_id: id_str,
                short_id: short,
                task_id,
                status: format!("{status:?}"),
                status_color: color.to_owned(),
            });
        }
    }
    rows
}
