//! Dashboard home page.

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::state::AppState;
use crate::util::status_colors;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};

use rustvello_proto::status::InvocationStatus;

/// A component card for the dashboard architecture section.
struct ComponentCard {
    label: &'static str,
    type_name: String,
    icon: &'static str,
    href: &'static str,
}

#[derive(Template)]
#[template(path = "index.html")]
#[allow(dead_code)]
struct IndexTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    invocation_counts: Vec<(String, usize, String)>,
    task_count: usize,
    broker_pending: usize,
    runner_count: usize,
    total_invocations: usize,
    components: Vec<ComponentCard>,
    dev_mode: bool,
    logging_level: String,
    log_format: String,
    arg_print_mode: String,
    compact_logs: bool,
    broker_type: String,
    orchestrator_type: String,
    state_backend_type: String,
    scheduler_enabled: bool,
    blocking_control: bool,
    auto_purge_hours: f64,
    atomic_svc_interval: f64,
    runner_dead_after_minutes: f64,
    max_pending_seconds: u64,
}

pub fn router() -> Router<AppState> {
    Router::new().route("/", axum::routing::get(index))
}

async fn index(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let app_id = app.app_id.clone();
    let app_ids = state.app_ids().unwrap_or_default();
    let task_count = app.task_ids.len();

    let statuses = [
        InvocationStatus::Running,
        InvocationStatus::Pending,
        InvocationStatus::Success,
        InvocationStatus::Failed,
        InvocationStatus::Retry,
        InvocationStatus::ConcurrencyControlled,
    ];

    let mut invocation_counts = Vec::new();
    let mut total_invocations: usize = 0;
    let mut runner_count: usize = 0;
    for status in &statuses {
        let ids = app
            .orchestrator
            .get_invocations_by_status(*status, None)
            .await
            .unwrap_or_default();
        let count = ids.len();
        if *status == InvocationStatus::Running {
            // Count unique runners from running invocations
            let mut runner_set = std::collections::HashSet::new();
            for inv_id in &ids {
                if let Ok(record) = app.orchestrator.get_invocation_status(inv_id).await {
                    if let Some(ref rid) = record.runner_id {
                        runner_set.insert(rid.to_string());
                    }
                }
            }
            runner_count = runner_set.len();
        }
        total_invocations += count;
        let color = status_colors::hex_color(status);
        invocation_counts.push((format!("{status:?}"), count, color.to_owned()));
    }

    let broker_pending = app.broker.count_invocations(None).await.unwrap_or(0);
    let config = &app.config;

    let components = vec![
        ComponentCard {
            label: "Broker",
            type_name: "Message queue".to_owned(),
            icon: "queue",
            href: "/broker",
        },
        ComponentCard {
            label: "Orchestrator",
            type_name: "Task coordination".to_owned(),
            icon: "settings",
            href: "/orchestrator",
        },
        ComponentCard {
            label: "Runners",
            type_name: "Task execution".to_owned(),
            icon: "devices",
            href: "/runners",
        },
        ComponentCard {
            label: "State Backend",
            type_name: "Persistent state".to_owned(),
            icon: "storage",
            href: "/state-backend",
        },
        ComponentCard {
            label: "Data Store",
            type_name: "Client cache".to_owned(),
            icon: "cached",
            href: "/client-data-store",
        },
        ComponentCard {
            label: "Tasks",
            type_name: "Registered tasks".to_owned(),
            icon: "task",
            href: "/tasks",
        },
    ];

    Ok(HtmlTemplate(IndexTemplate {
        app_id,
        app_ids,
        nav_path: "dashboard",
        invocation_counts,
        task_count,
        broker_pending,
        runner_count,
        total_invocations,
        components,
        dev_mode: config.dev_mode_force_sync,
        logging_level: config.logging_level.clone(),
        log_format: format!("{:?}", config.log_format),
        arg_print_mode: format!("{:?}", config.argument_print_mode),
        compact_logs: config.compact_log_context,
        broker_type: "Active".to_owned(),
        orchestrator_type: "Active".to_owned(),
        state_backend_type: "Active".to_owned(),
        scheduler_enabled: config.enable_scheduler,
        blocking_control: config.blocking_control,
        auto_purge_hours: config.auto_final_invocation_purge_hours,
        atomic_svc_interval: config.atomic_service_interval_minutes,
        runner_dead_after_minutes: config.runner_dead_after_seconds as f64 / 60.0,
        max_pending_seconds: config.max_pending_seconds,
    }))
}
