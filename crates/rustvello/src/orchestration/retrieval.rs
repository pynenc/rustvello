//! Invocation retrieval — `get_invocations_to_run` and supporting helpers.
//!
//! Extracted from the main orchestration module to keep file sizes under
//! the 500-line limit.

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatus;

use super::OrchestratorCoordinator;

impl OrchestratorCoordinator {
    // -----------------------------------------------------------------------
    // get_invocations_to_run — mirrors pynenc's BaseOrchestrator method
    // -----------------------------------------------------------------------

    /// Retrieve invocations ready to run, handling blocking priority and CC.
    ///
    /// 1. Prioritise blocking invocations (those with waiters)
    /// 2. Fill remaining slots from broker queue
    /// 3. For each candidate: check CC, set PENDING or reject/reroute
    /// 4. Reroute CC-denied invocations at the end
    ///
    /// `config_for_task` resolves the effective task config for CC decisions.
    /// Returns `None` when the task is unknown (CC is skipped).
    pub async fn get_invocations_to_run(
        &self,
        max_num_invocations: usize,
        runner_id: &RunnerId,
        queue_names: &[String],
        config_for_task: &dyn Fn(&TaskId) -> Option<TaskConfig>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut result = Vec::new();
        let mut reroute_ids = Vec::new();

        // --- Phase 1: blocking-priority invocations ---
        if let Ok(blocking) = self
            .orchestrator
            .get_blocking_invocations(max_num_invocations)
            .await
        {
            for inv_id in &blocking {
                if result.len() >= max_num_invocations {
                    break;
                }
                let status_rec = match self.orchestrator.get_invocation_status(inv_id).await {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if !status_rec
                    .status
                    .can_transition_to(InvocationStatus::Pending)
                {
                    continue;
                }
                if !self
                    .check_cc_for_invocation(inv_id, config_for_task)
                    .await?
                {
                    continue;
                }
                if self
                    .try_transition_to_pending(inv_id, runner_id, &mut result)
                    .await?
                {
                    continue;
                }
            }
        }

        // --- Phase 2: broker FIFO with CC ---
        let max_retries = max_num_invocations * 2 + 8;
        let mut attempts = 0;
        while result.len() < max_num_invocations && attempts < max_retries {
            attempts += 1;

            let mut candidate = None;
            for queue_name in queue_names {
                if let Some(invocation_id) = self
                    .broker
                    .retrieve_invocation_from_queue(queue_name, None)
                    .await?
                {
                    candidate = Some(invocation_id);
                    break;
                }
            }
            let Some(inv_id) = candidate else { break };

            if result.contains(&inv_id) {
                continue;
            }

            let status_rec = match self.orchestrator.get_invocation_status(&inv_id).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            if !status_rec.status.is_available_for_run() {
                continue;
            }

            // CC check
            let cc_ok = self
                .check_cc_for_invocation(&inv_id, config_for_task)
                .await?;
            if !cc_ok {
                let inv_dto = match self.state_backend.get_invocation(&inv_id).await {
                    Ok(dto) => dto,
                    Err(_) => continue,
                };
                let reroute = config_for_task(&inv_dto.task_id).is_some_and(|c| c.reroute_on_cc);

                if reroute {
                    match self
                        .orchestrator
                        .set_invocation_status(
                            &inv_id,
                            InvocationStatus::ConcurrencyControlled,
                            Some(runner_id),
                        )
                        .await
                    {
                        Ok(_) => reroute_ids.push(inv_id),
                        Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                        Err(e) => return Err(e),
                    }
                } else {
                    match self
                        .orchestrator
                        .set_invocation_status(
                            &inv_id,
                            InvocationStatus::ConcurrencyControlledFinal,
                            Some(runner_id),
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                        Err(e) => return Err(e),
                    }
                }
                continue;
            }

            self.try_transition_to_pending(&inv_id, runner_id, &mut result)
                .await?;
        }

        // --- Phase 3: reroute CC-denied invocations ---
        for inv_id in &reroute_ids {
            match self
                .orchestrator
                .set_invocation_status(inv_id, InvocationStatus::Rerouted, Some(runner_id))
                .await
            {
                Ok(_) => {
                    if let Ok(invocation) = self.state_backend.get_invocation(inv_id).await {
                        if let Some(config) = config_for_task(&invocation.task_id) {
                            let _ = self
                                .broker
                                .route_invocation_with_options(
                                    inv_id,
                                    Some(&invocation.task_id),
                                    &config.queue,
                                    config.priority,
                                )
                                .await;
                        }
                    }
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                Err(e) => return Err(e),
            }
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Private helpers (used by get_invocations_to_run)
    // -----------------------------------------------------------------------

    /// Transition an invocation to PENDING, record history + trigger, push to result.
    ///
    /// Returns `true` on success, `false` on InvalidStatusTransition (race).
    async fn try_transition_to_pending(
        &self,
        inv_id: &InvocationId,
        runner_id: &RunnerId,
        result: &mut Vec<InvocationId>,
    ) -> RustvelloResult<bool> {
        match self
            .orchestrator
            .set_invocation_status(inv_id, InvocationStatus::Pending, Some(runner_id))
            .await
        {
            Ok(record) => {
                let inv_dto = self.state_backend.get_invocation(inv_id).await.ok();
                let task_id = inv_dto.as_ref().map(|d| &d.task_id);
                let history = InvocationHistory::new(inv_id.clone(), record, None)
                    .with_runner(runner_id.clone());
                let _ = self.state_backend.add_history(&history).await;
                if let (Some(ref tm), Some(tid)) = (&self.trigger_manager, task_id) {
                    let args = self.get_invocation_arguments(inv_id).await;
                    let ctx = rustvello_proto::trigger::StatusContext {
                        invocation_id: inv_id.clone(),
                        task_id: tid.clone(),
                        status: InvocationStatus::Pending,
                        arguments: args,
                    };
                    let _ = tm.report_status_change(&ctx).await;
                }
                result.push(inv_id.clone());
                Ok(true)
            }
            Err(RustvelloError::InvalidStatusTransition { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Check concurrency control for a single candidate invocation.
    ///
    /// Returns `true` if the invocation is cleared to proceed.
    async fn check_cc_for_invocation(
        &self,
        invocation_id: &InvocationId,
        config_for_task: &dyn Fn(&TaskId) -> Option<TaskConfig>,
    ) -> RustvelloResult<bool> {
        use crate::runner::executor_common::compute_cc_args;
        use rustvello_proto::status::ConcurrencyControlType;

        let inv_dto = match self.state_backend.get_invocation(invocation_id).await {
            Ok(dto) => dto,
            Err(_) => return Ok(true),
        };

        let config = match config_for_task(&inv_dto.task_id) {
            Some(c) => c,
            None => return Ok(true), // unknown task → skip CC
        };

        if config.concurrency_control == ConcurrencyControlType::Unlimited {
            return Ok(true);
        }

        let call_dto = match self.state_backend.get_call(&inv_dto.call_id).await {
            Ok(c) => c,
            Err(_) => return Ok(true),
        };

        let cc_args = compute_cc_args(&config, &call_dto.serialized_arguments);

        self.orchestrator
            .check_running_concurrency(&inv_dto.task_id, &config, cc_args.as_ref())
            .await
    }
}
