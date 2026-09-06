//! Trigger evaluation and atomic service scheduling use cases.

use chrono::Utc;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::ActiveRunnerInfo;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) async fn run_trigger_iteration(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.trigger_loop_iteration(runner_id, &task_catalog.all_routing(app_config))
            .await
    }

    pub(crate) async fn run_atomic_services(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
        service_interval_minutes: f64,
        spread_margin_minutes: f64,
        runner_timeout_seconds: f64,
    ) -> RustvelloResult<Option<Vec<InvocationId>>> {
        self.check_atomic_services(
            runner_id,
            service_interval_minutes,
            spread_margin_minutes,
            runner_timeout_seconds,
            &task_catalog.all_routing(app_config),
        )
        .await
    }

    /// Execute one trigger evaluation loop iteration.
    ///
    /// Returns the list of invocation IDs created by fired triggers.
    pub async fn trigger_loop_iteration(
        &self,
        runner_id: &RunnerId,
        routes: &std::collections::HashMap<rustvello_proto::identifiers::TaskId, (String, f64)>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let tm = match self.backends.trigger_manager {
            Some(ref tm) => tm,
            None => return Ok(Vec::new()),
        };

        let _ = tm.evaluate_cron_conditions().await?;
        let to_invoke = tm.evaluate_trigger_runs().await?;

        let mut created_ids = Vec::new();
        for execution in &to_invoke {
            let trigger_def = &execution.trigger;
            let args = json_value_to_serialized_args(&execution.arguments);
            let call_dto = CallDTO::new(trigger_def.task_id.clone(), args);
            let inv_id = InvocationId::new();

            let inv_dto = InvocationDTO::new(
                inv_id.clone(),
                trigger_def.task_id.clone(),
                call_dto.call_id.clone(),
            );
            self.backends
                .state_backend
                .upsert_invocation(&inv_dto, &call_dto)
                .await?;

            let record = self
                .backends
                .invocation_control
                .register_invocation_with_id(&inv_id, &call_dto, Some(runner_id))
                .await?;

            let history = InvocationHistory::new(inv_id.clone(), record.clone(), None)
                .with_runner(runner_id.clone());
            if let Err(e) = self.backends.state_backend.add_history(&history).await {
                tracing::warn!("trigger_loop_iteration: failed to record history: {e}");
            }

            let (queue_name, priority) = routes.get(&trigger_def.task_id).ok_or_else(|| {
                RustvelloError::TaskNotRegistered {
                    task_id: trigger_def.task_id.clone(),
                }
            })?;
            self.backends
                .broker
                .route_invocation_with_options(
                    &inv_id,
                    Some(&trigger_def.task_id),
                    queue_name,
                    *priority,
                )
                .await?;

            if let Err(error) = tm.complete_trigger_run(&execution.run_id, &inv_id).await {
                tracing::debug!(%error, trigger_run_id = %execution.run_id, "trigger-run completion unavailable");
            }

            created_ids.push(inv_id);
        }

        Ok(created_ids)
    }

    /// Execute one atomic service check: coordination, trigger loop, recording.
    pub async fn check_atomic_services(
        &self,
        runner_id: &RunnerId,
        service_interval_minutes: f64,
        spread_margin_minutes: f64,
        runner_timeout_seconds: f64,
        routes: &std::collections::HashMap<rustvello_proto::identifiers::TaskId, (String, f64)>,
    ) -> RustvelloResult<Option<Vec<InvocationId>>> {
        self.backends
            .invocation_control
            .register_heartbeat(runner_id, true)
            .await?;

        let active_runners = self
            .backends
            .invocation_control
            .get_active_runners(runner_timeout_seconds as u64, Some(true))
            .await?;

        let now = Utc::now().timestamp() as f64
            + Utc::now().timestamp_subsec_nanos() as f64 / 1_000_000_000.0;

        if !can_run_atomic_service(
            runner_id,
            &active_runners,
            now,
            service_interval_minutes,
            spread_margin_minutes,
        ) {
            return Ok(None);
        }

        let start = Utc::now();
        let created_ids = self.trigger_loop_iteration(runner_id, routes).await?;
        let end = Utc::now();

        self.backends
            .invocation_control
            .record_atomic_service_execution(runner_id, start, end)
            .await?;

        Ok(Some(created_ids))
    }
}

fn can_run_atomic_service(
    runner_id: &RunnerId,
    active_runners: &[ActiveRunnerInfo],
    current_time: f64,
    service_interval_minutes: f64,
    spread_margin_minutes: f64,
) -> bool {
    if active_runners.is_empty() {
        return false;
    }

    let total_runners = active_runners.len();
    if total_runners == 1 {
        return true;
    }

    let runner_position = active_runners
        .iter()
        .position(|r| r.runner_id == *runner_id);
    let runner_position = match runner_position {
        Some(pos) => pos,
        None => return false,
    };

    let service_interval = service_interval_minutes * 60.0;
    let spread_margin = spread_margin_minutes * 60.0;
    let time_slot_size = service_interval / total_runners as f64;

    let start_time = runner_position as f64 * time_slot_size;
    let mut end_time = start_time + time_slot_size - spread_margin;
    if end_time <= start_time {
        end_time = start_time + (time_slot_size / 2.0);
    }

    let time_in_cycle = current_time % service_interval;
    start_time <= time_in_cycle && time_in_cycle < end_time
}

fn json_value_to_serialized_args(value: &serde_json::Value) -> SerializedArguments {
    let mut args = SerializedArguments::new();
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let v_str = serde_json::to_string(v).unwrap_or_else(|_| v.to_string());
            args.insert(k.clone(), v_str);
        }
    }
    args
}
