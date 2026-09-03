//! Extended composites — Phase 6 coordination operations.
//!
//! Adds `route_call`, `reroute_invocations`, `trigger_loop_iteration`,
//! and `check_atomic_services` to `OrchestratorCoordinator`.

use std::collections::HashMap;

use chrono::Utc;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::ActiveRunnerInfo;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{CallId, InvocationId, RunnerId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use super::OrchestratorCoordinator;

/// Result of a `route_call` composite operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteCallResult {
    /// A new invocation was created and routed.
    New(InvocationId),
    /// An existing REGISTERED invocation was reused (same call_id).
    Reused(InvocationId),
    /// An existing REGISTERED invocation was found with a different call_id.
    /// The caller decides: reuse or raise `on_diff_non_key_args_raise`.
    ReusedDifferentCall {
        invocation_id: InvocationId,
        existing_call_id: CallId,
    },
}

impl OrchestratorCoordinator {
    // -----------------------------------------------------------------------
    // 6.1 — route_call() composite
    // -----------------------------------------------------------------------

    /// Route a call: check registration CC, create or reuse an invocation, route.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.route_call()`:
    /// 1. If `registration_cc == Unlimited`: always create a new invocation.
    /// 2. Else: query existing REGISTERED invocations with matching CC args.
    ///    - No match → create new.
    ///    - Match with same `call_id` → reuse (return existing).
    ///    - Match with different `call_id` → return `ReusedDifferentCall`
    ///      so the caller can decide (raise error or reuse).
    /// 3. For new invocations: register + persist + index CC + route.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_call(
        &self,
        new_invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        registration_cc: ConcurrencyControlType,
        index_cc: bool,
        runner_id: &RunnerId,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<RouteCallResult> {
        // Fast path: no registration CC → always create new
        if registration_cc == ConcurrencyControlType::Unlimited {
            return self
                .create_and_route_invocation(
                    new_invocation_id,
                    call_dto,
                    cc_args,
                    index_cc,
                    runner_id,
                    queue_name,
                    priority,
                )
                .await
                .map(RouteCallResult::New);
        }

        // Check for an existing REGISTERED invocation matching the CC key
        let existing = self
            .orchestrator
            .get_existing_invocations(&call_dto.task_id, cc_args, &[InvocationStatus::Registered])
            .await?;

        if let Some(existing_inv_id) = existing.into_iter().next() {
            // Found an existing invocation — check if same call
            let existing_inv = self.state_backend.get_invocation(&existing_inv_id).await?;

            if existing_inv.call_id == call_dto.call_id {
                return Ok(RouteCallResult::Reused(existing_inv_id));
            }
            return Ok(RouteCallResult::ReusedDifferentCall {
                invocation_id: existing_inv_id,
                existing_call_id: existing_inv.call_id,
            });
        }

        // No existing match — create new invocation
        self.create_and_route_invocation(
            new_invocation_id,
            call_dto,
            cc_args,
            index_cc,
            runner_id,
            queue_name,
            priority,
        )
        .await
        .map(RouteCallResult::New)
    }

    /// Internal: Create, register, persist, index CC, and route an invocation.
    #[allow(clippy::too_many_arguments)]
    async fn create_and_route_invocation(
        &self,
        invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        index_cc: bool,
        runner_id: &RunnerId,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<InvocationId> {
        let inv_dto = InvocationDTO::new(
            invocation_id.clone(),
            call_dto.task_id.clone(),
            call_dto.call_id.clone(),
        );

        // 1. Persist in state backend
        self.state_backend
            .upsert_invocation(&inv_dto, call_dto)
            .await?;

        // 2. Register with orchestrator (sets REGISTERED status)
        let record = self
            .orchestrator
            .register_invocation_with_id(invocation_id, call_dto, Some(runner_id))
            .await?;

        // 3. Record history
        let history = InvocationHistory::new(invocation_id.clone(), record.clone(), None)
            .with_runner(runner_id.clone());
        self.state_backend.add_history(&history).await?;

        // 4. Trigger notification
        if let Some(ref tm) = self.trigger_manager {
            let ctx = rustvello_proto::trigger::StatusContext {
                invocation_id: invocation_id.clone(),
                task_id: call_dto.task_id.clone(),
                status: record.status,
                arguments: call_dto.serialized_arguments.0.clone(),
            };
            tm.report_status_change(&ctx).await?;
        }

        // 5. Index for CC if needed
        if index_cc {
            self.orchestrator
                .index_for_concurrency_control(invocation_id, &call_dto.task_id, cc_args)
                .await?;
        }

        // 6. Route through broker
        self.broker
            .route_invocation_with_options(
                invocation_id,
                Some(&call_dto.task_id),
                queue_name,
                priority,
            )
            .await?;

        Ok(invocation_id.clone())
    }

    // -----------------------------------------------------------------------
    // 6.2 — reroute_invocations() composite
    // -----------------------------------------------------------------------

    /// Reroute a set of invocations: transition to Rerouted, then re-enqueue.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.reroute_invocations()`:
    /// For each invocation: set status REROUTED → route through broker.
    /// Invalid status transitions are silently skipped (race-safe).
    pub async fn reroute_invocations(
        &self,
        invocation_ids: &[InvocationId],
        runner_id: &RunnerId,
        routes: &HashMap<InvocationId, (String, f64)>,
    ) -> RustvelloResult<()> {
        for inv_id in invocation_ids {
            match self
                .orchestrator
                .set_invocation_status(inv_id, InvocationStatus::Rerouted, Some(runner_id))
                .await
            {
                Ok(record) => {
                    // Record history
                    let history = InvocationHistory::new(inv_id.clone(), record.clone(), None)
                        .with_runner(runner_id.clone());
                    let _ = self.state_backend.add_history(&history).await;

                    // Trigger notification
                    if let Some(ref tm) = self.trigger_manager {
                        let (task_id, arguments) = self.get_trigger_context(inv_id).await;
                        let ctx = rustvello_proto::trigger::StatusContext {
                            invocation_id: inv_id.clone(),
                            task_id,
                            status: InvocationStatus::Rerouted,
                            arguments,
                        };
                        let _ = tm.report_status_change(&ctx).await;
                    }

                    // Re-enqueue in broker — propagate errors since a
                    // failed re-enqueue leaves the invocation permanently stuck.
                    let invocation = self.state_backend.get_invocation(inv_id).await?;
                    let (queue_name, priority) =
                        routes.get(inv_id).ok_or_else(|| RustvelloError::Internal {
                            message: format!("missing routing for invocation {inv_id}"),
                        })?;
                    self.broker
                        .route_invocation_with_options(
                            inv_id,
                            Some(&invocation.task_id),
                            queue_name,
                            *priority,
                        )
                        .await?
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {
                    // Race: invocation was already transitioned — skip
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // 6.4 — trigger_loop_iteration() composite
    // -----------------------------------------------------------------------

    /// Execute one trigger evaluation loop iteration.
    ///
    /// Mirrors pynenc's `BaseTrigger.trigger_loop_iteration()`:
    /// 1. Evaluate cron conditions (time-based).
    /// 2. Process valid conditions → match triggers → fire.
    /// 3. For each fired trigger: register a new invocation + route.
    ///
    /// Returns the list of invocation IDs created by triggers.
    pub async fn trigger_loop_iteration(
        &self,
        runner_id: &RunnerId,
        routes: &HashMap<rustvello_proto::identifiers::TaskId, (String, f64)>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let tm = match self.trigger_manager {
            Some(ref tm) => tm,
            None => return Ok(Vec::new()),
        };

        // 1. Evaluate cron conditions
        let _ = tm.evaluate_cron_conditions().await?;

        // 2. Evaluate all pending valid conditions → determine triggers to fire
        let to_invoke = tm.evaluate_trigger_runs().await?;

        // 3. For each fired trigger: create invocation + route
        let mut created_ids = Vec::new();
        for execution in &to_invoke {
            let trigger_def = &execution.trigger;
            let args_value = &execution.arguments;
            // Convert JSON argument_template → SerializedArguments
            let args = json_value_to_serialized_args(args_value);
            let call_dto = CallDTO::new(trigger_def.task_id.clone(), args);
            let inv_id = InvocationId::new();

            let inv_dto = InvocationDTO::new(
                inv_id.clone(),
                trigger_def.task_id.clone(),
                call_dto.call_id.clone(),
            );

            // Register: upsert + register + history + trigger + route
            self.state_backend
                .upsert_invocation(&inv_dto, &call_dto)
                .await?;

            let record = self
                .orchestrator
                .register_invocation_with_id(&inv_id, &call_dto, Some(runner_id))
                .await?;

            let history = InvocationHistory::new(inv_id.clone(), record.clone(), None)
                .with_runner(runner_id.clone());
            if let Err(e) = self.state_backend.add_history(&history).await {
                tracing::warn!("trigger_loop_iteration: failed to record history: {e}");
            }

            let (queue_name, priority) = routes.get(&trigger_def.task_id).ok_or_else(|| {
                RustvelloError::TaskNotRegistered {
                    task_id: trigger_def.task_id.clone(),
                }
            })?;
            self.broker
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

    // -----------------------------------------------------------------------
    // 6.3 — check_atomic_services() composite
    // -----------------------------------------------------------------------

    /// Execute one atomic service check: coordination + trigger loop + recording.
    ///
    /// Mirrors pynenc's `BaseRunner._check_atomic_services()` flow:
    /// 1. Register heartbeat for this runner (with `can_run_atomic_service=true`).
    /// 2. Get active runners eligible for atomic services.
    /// 3. Check distributed coordination algorithm (time-slot allocation).
    /// 4. If authorized: run `trigger_loop_iteration`, record execution.
    /// 5. Return `None` if not authorized, `Some(created_ids)` if ran.
    pub async fn check_atomic_services(
        &self,
        runner_id: &RunnerId,
        service_interval_minutes: f64,
        spread_margin_minutes: f64,
        runner_timeout_seconds: f64,
        routes: &HashMap<rustvello_proto::identifiers::TaskId, (String, f64)>,
    ) -> RustvelloResult<Option<Vec<InvocationId>>> {
        // 1. Register heartbeat
        self.orchestrator
            .register_heartbeat(runner_id, true)
            .await?;

        // 2. Get active runners eligible for atomic services
        let active_runners = self
            .orchestrator
            .get_active_runners(runner_timeout_seconds as u64, Some(true))
            .await?;

        // 3. Check coordination algorithm
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

        // 4. Run trigger loop
        let start = Utc::now();
        let created_ids = self.trigger_loop_iteration(runner_id, routes).await?;
        let end = Utc::now();

        // 5. Record execution
        self.orchestrator
            .record_atomic_service_execution(runner_id, start, end)
            .await?;

        Ok(Some(created_ids))
    }
}

// ---------------------------------------------------------------------------
// Distributed coordination algorithm
// ---------------------------------------------------------------------------

/// Determine if a runner should execute atomic global services now.
///
/// Port of pynenc's `can_run_atomic_service()` from `atomic_service.py`.
/// Divides the service interval into equal time-slots among active runners
/// (ordered by creation time). Each runner gets an exclusive window.
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

    // Single-runner optimization
    if total_runners == 1 {
        return true;
    }

    // Find this runner's position (runners are sorted by creation_time)
    let runner_position = active_runners
        .iter()
        .position(|r| r.runner_id == *runner_id);
    let runner_position = match runner_position {
        Some(pos) => pos,
        None => return false,
    };

    // Calculate time-slot boundaries
    let service_interval = service_interval_minutes * 60.0;
    let spread_margin = spread_margin_minutes * 60.0;
    let time_slot_size = service_interval / total_runners as f64;

    let start_time = runner_position as f64 * time_slot_size;
    let mut end_time = start_time + time_slot_size - spread_margin;

    // Ensure the window is valid
    if end_time <= start_time {
        end_time = start_time + (time_slot_size / 2.0);
    }

    // Check if current time falls within this runner's slot
    let time_in_cycle = current_time % service_interval;
    start_time <= time_in_cycle && time_in_cycle < end_time
}

/// Convert a JSON value (typically an object) to `SerializedArguments`.
///
/// If the value is an object, each key-value pair becomes a serialized argument.
/// Non-string values are serialized as JSON strings.
fn json_value_to_serialized_args(value: &serde_json::Value) -> SerializedArguments {
    let mut args = SerializedArguments::new();
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let v_str = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            args.insert(k.clone(), v_str);
        }
    }
    args
}
