//! Trigger evaluation and atomic service scheduling use cases.

use chrono::Utc;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::failpoints;
use rustvello_core::orchestrator::ActiveRunnerInfo;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::InvocationStatus;
use rustvello_proto::trigger::TriggerRunRecord;

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

/// Pending trigger runs published per iteration; the rest wait for the next one.
const PENDING_TRIGGER_RUN_BATCH: usize = 1000;

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
    /// 1. Claim every firing into the trigger outbox (claim, run record and
    ///    condition clear commit together on transactional stores).
    /// 2. Publish every pending run, including runs a crashed process claimed
    ///    but never published, under the run-derived invocation id.
    /// 3. Attach the invocation to its run, which removes it from the outbox.
    ///
    /// A crash between any two steps is repaired by the next iteration, and
    /// the deterministic invocation id makes re-publication idempotent: each
    /// firing yields exactly one logical invocation.
    ///
    /// Returns the invocation IDs published by this iteration.
    pub async fn trigger_loop_iteration(
        &self,
        runner_id: &RunnerId,
        routes: &std::collections::HashMap<rustvello_proto::identifiers::TaskId, (String, f64)>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let tm = match self.backends.trigger_manager {
            Some(ref tm) => tm,
            None => return Ok(Vec::new()),
        };
        // Fail closed on a mixed deployment before claiming anything.
        let publication = self.publication()?;

        let _ = tm.evaluate_cron_conditions().await?;
        for execution in tm.evaluate_trigger_runs().await? {
            failpoints::boundary("trigger.claimed", execution.run_id.as_str()).await?;
        }

        let mut created_ids = Vec::new();
        for run in tm.pending_trigger_runs(PENDING_TRIGGER_RUN_BATCH).await? {
            let Some(invocation_id) = run.planned_invocation_id.clone() else {
                continue;
            };
            let Some((queue_name, priority)) = routes.get(&run.task_id) else {
                // Another runner that registers the task publishes it.
                tracing::debug!(
                    trigger_run_id = %run.trigger_run_id,
                    task_id = %run.task_id,
                    "trigger run stays pending: task is not registered on this runner"
                );
                continue;
            };
            let route = rustvello_core::publication::PublicationRoute {
                queue: queue_name.clone(),
                priority: *priority,
            };
            self.publish_trigger_run(&run, &invocation_id, runner_id, route, publication.as_ref())
                .await?;
            failpoints::boundary("trigger.published", invocation_id.as_str()).await?;
            tm.complete_trigger_run(&run.trigger_run_id, &invocation_id)
                .await?;
            failpoints::boundary("trigger.completed", invocation_id.as_str()).await?;
            created_ids.push(invocation_id);
        }

        Ok(created_ids)
    }

    /// Publish one claimed trigger run; a no-op when an earlier attempt did.
    async fn publish_trigger_run(
        &self,
        run: &TriggerRunRecord,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        route: rustvello_core::publication::PublicationRoute,
        publication: Option<&std::sync::Arc<dyn rustvello_core::publication::RuntimePublication>>,
    ) -> RustvelloResult<()> {
        let call_dto = CallDTO::new(
            run.task_id.clone(),
            json_value_to_serialized_args(&run.arguments),
        );
        let existing = match self
            .backends
            .invocation_control
            .get_invocation_status(invocation_id)
            .await
        {
            Ok(record) => Some(record.status),
            Err(RustvelloError::InvocationNotFound { .. }) => None,
            Err(error) => return Err(error),
        };

        if let Some(publication) = publication {
            // One transaction: an existing invocation is completely published.
            if existing.is_none() {
                let inv_dto = InvocationDTO::new(
                    invocation_id.clone(),
                    run.task_id.clone(),
                    call_dto.call_id.clone(),
                );
                publication
                    .submit(rustvello_core::publication::SubmissionPublication {
                        invocation: inv_dto,
                        call: call_dto,
                        runner_id: runner_id.clone(),
                        runner_context: None,
                        workflow_root: false,
                        cc_arguments: None,
                        route,
                    })
                    .await?;
            }
            return Ok(());
        }

        // Fallback: ordered writes, each idempotent for this invocation id.
        // Once a worker has moved the invocation past Registered it is
        // published; while it is Registered the remaining writes are repeated.
        // A repeated route can queue a second delivery of the same id, which
        // retrieval drops because the status can no longer become Pending.
        match existing {
            Some(status) if status != InvocationStatus::Registered => return Ok(()),
            Some(_) => {}
            None => {
                self.backends
                    .invocation_control
                    .register_invocation_with_id(invocation_id, &call_dto, Some(runner_id))
                    .await?;
                failpoints::boundary("trigger.fallback.registered", invocation_id.as_str()).await?;
            }
        }
        let inv_dto = InvocationDTO::new(
            invocation_id.clone(),
            run.task_id.clone(),
            call_dto.call_id.clone(),
        );
        self.backends
            .state_backend
            .upsert_invocation(&inv_dto, &call_dto)
            .await?;
        failpoints::boundary("trigger.fallback.stored", invocation_id.as_str()).await?;
        let registered_history = self
            .backends
            .state_backend
            .get_history(invocation_id)
            .await?
            .iter()
            .any(|h| h.status_record.status == InvocationStatus::Registered);
        if !registered_history {
            let record = self
                .backends
                .invocation_control
                .get_invocation_status(invocation_id)
                .await?;
            let history = InvocationHistory::new(invocation_id.clone(), record, None)
                .with_runner(runner_id.clone());
            self.backends.state_backend.add_history(&history).await?;
        }
        failpoints::boundary("trigger.fallback.history", invocation_id.as_str()).await?;
        self.backends
            .broker
            .route_invocation_with_options(
                invocation_id,
                Some(&run.task_id),
                &route.queue,
                route.priority,
            )
            .await?;
        failpoints::boundary("trigger.fallback.routed", invocation_id.as_str()).await?;
        Ok(())
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
