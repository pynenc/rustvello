//! Per-component health report shared by `/health`, the Python `App.health()`
//! helper and the `rustvello.doctor` command.
//!
//! Every check is a read the component needs anyway (a count, a lookup), so a
//! backend that rejects the monitor's credentials fails here with the backend's
//! own error text instead of a silent "0 runners" dashboard.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::AppInstance;

/// Health of one backend component.
#[derive(Debug, Clone, Serialize)]
pub struct ComponentHealth {
    /// `"ok"` or `"error"`.
    pub status: &'static str,
    /// Backend implementation name, for example `MongoDB 3.6` or `SQLite`.
    pub implementation: String,
    /// The backend's own error text when the check failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ComponentHealth {
    fn ok(implementation: impl Into<String>) -> Self {
        Self {
            status: "ok",
            implementation: implementation.into(),
            error: None,
        }
    }

    fn error(implementation: impl Into<String>, error: impl std::fmt::Display) -> Self {
        Self {
            status: "error",
            implementation: implementation.into(),
            error: Some(error.to_string()),
        }
    }

    fn from_result<T>(
        implementation: impl Into<String>,
        result: Result<T, rustvello_core::error::RustvelloError>,
    ) -> Self {
        match result {
            Ok(_) => Self::ok(implementation),
            Err(error) => Self::error(implementation, error),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }
}

/// Active runners as seen through orchestrator heartbeats.
#[derive(Debug, Clone, Serialize)]
pub struct RunnerSummary {
    /// Runners with a heartbeat younger than `dead_after_seconds`.
    pub active: usize,
    /// Heartbeat age after which a runner counts as dead.
    pub dead_after_seconds: u64,
}

/// The whole report; `status` is `"ok"` only when every component answered.
#[derive(Debug, Clone, Serialize)]
pub struct HealthReport {
    pub status: &'static str,
    pub app_id: String,
    pub components: BTreeMap<&'static str, ComponentHealth>,
    pub runners: RunnerSummary,
    /// Queued invocations per declared broker queue (empty when the broker failed).
    pub queues: BTreeMap<String, usize>,
    /// Human-readable problems: component errors first, then warnings such as
    /// "no active runners".
    pub problems: Vec<String>,
}

impl HealthReport {
    /// Every component answered. Having no runners is a warning, not an error.
    pub fn is_ok(&self) -> bool {
        self.status == "ok"
    }

    /// `is_ok()` and at least one runner is alive.
    pub fn is_serving(&self) -> bool {
        self.is_ok() && self.runners.active > 0
    }
}

/// Probe every component of `app` with one read each.
pub async fn health_report(app: &AppInstance) -> HealthReport {
    let mut components = BTreeMap::new();
    let mut problems = Vec::new();
    let mut queues = BTreeMap::new();

    let broker_name = app.broker.backend_name();
    let mut broker_ok = true;
    for queue in &app.config.broker_queues {
        match app
            .broker
            .count_invocations_in_queues(std::slice::from_ref(queue), None)
            .await
        {
            Ok(depth) => {
                queues.insert(queue.clone(), depth);
            }
            Err(error) => {
                broker_ok = false;
                components.insert("broker", ComponentHealth::error(broker_name, &error));
                problems.push(format!("broker: {error}"));
                break;
            }
        }
    }
    if broker_ok {
        // Apps without declared queues still have the global count.
        match app.broker.count_invocations(None).await {
            Ok(_) => {
                components.insert("broker", ComponentHealth::ok(broker_name));
            }
            Err(error) => {
                components.insert("broker", ComponentHealth::error(broker_name, &error));
                problems.push(format!("broker: {error}"));
            }
        }
    }

    let dead_after_seconds = app.config.runner_dead_after_seconds;
    let orchestrator_name = app.orchestrator.backend_name();
    let mut active = 0;
    match app
        .orchestrator
        .get_active_runners(dead_after_seconds, None)
        .await
    {
        Ok(runners) => {
            active = runners.len();
            components.insert("orchestrator", ComponentHealth::ok(orchestrator_name));
        }
        Err(error) => {
            components.insert(
                "orchestrator",
                ComponentHealth::error(orchestrator_name, &error),
            );
            problems.push(format!("orchestrator: {error}"));
        }
    }

    let state_backend_name = app.state_backend.backend_name();
    let state_backend = ComponentHealth::from_result(
        state_backend_name,
        app.state_backend.get_app_info(&app.app_id).await,
    );
    if let Some(error) = &state_backend.error {
        problems.push(format!("state_backend: {error}"));
    }
    components.insert("state_backend", state_backend);

    let data_store_name = app.client_data_store.backend_name();
    // `retrieve` of an unknown key is a NotFound-style error on most backends;
    // `usage_stats` is the read every store answers.
    app.client_data_store.usage_stats().await;
    components.insert("client_data_store", ComponentHealth::ok(data_store_name));

    if let Some(store) = &app.trigger_store {
        let trigger =
            ComponentHealth::from_result("Trigger store", store.get_all_conditions().await);
        if let Some(error) = &trigger.error {
            problems.push(format!("trigger_store: {error}"));
        }
        components.insert("trigger_store", trigger);
    }

    let all_ok = components.values().all(ComponentHealth::is_ok);
    if all_ok && active == 0 {
        problems.push(format!(
            "no active runners (no heartbeat in the last {dead_after_seconds}s)"
        ));
    }

    HealthReport {
        status: if all_ok { "ok" } else { "error" },
        app_id: app.app_id.clone(),
        components,
        runners: RunnerSummary {
            active,
            dead_after_seconds,
        },
        queues,
        problems,
    }
}
