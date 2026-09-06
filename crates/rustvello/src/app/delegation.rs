use rustvello_core::error::RustvelloResult;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus, InvocationStatusRecord};

use super::RustvelloApp;
use crate::orchestration::RouteCallResult;

impl RustvelloApp {
    fn task_routing(&self, task_id: &TaskId) -> RustvelloResult<(String, f64)> {
        let task = self.task_catalog.get(task_id).ok_or_else(|| {
            rustvello_core::error::RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            }
        })?;
        let config = self.resolve_task_config(task_id, task.config());
        Ok((config.queue, config.priority))
    }
}

// ---------------------------------------------------------------------------
// Composite operations — thin delegation to Orchestrator
//
// Each method delegates to self.orchestrator, which bundles multiple
// subsystem calls into a single Rust operation. Method names:
//   _with_context = explicit task_id + arguments
//   plain name    = auto-resolves context from state backend
// ---------------------------------------------------------------------------

impl RustvelloApp {
    /// Atomic status transition with auto-resolved trigger context.
    pub async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: &RunnerId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        self.orchestrator
            .set_invocation_status(invocation_id, status, runner_id)
            .await
    }

    /// Mark one invocation as waiting for another invocation.
    pub async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        self.orchestrator.set_waiting_for(waiter, waited_on).await
    }

    /// Atomic status transition with explicit trigger context.
    pub async fn set_invocation_status_with_context(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: std::collections::BTreeMap<String, String>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        self.orchestrator
            .set_invocation_status_with_context(
                invocation_id,
                status,
                runner_id,
                task_id,
                arguments,
            )
            .await
    }

    /// Register invocations with all side-effects.
    pub async fn register_invocations(
        &self,
        invocations: &[(InvocationDTO, CallDTO)],
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        let routes: Vec<(String, f64)> = invocations
            .iter()
            .map(|(_, call)| self.task_routing(&call.task_id))
            .collect::<RustvelloResult<_>>()?;
        self.orchestrator
            .register_invocations(invocations, runner_id, &routes)
            .await
    }

    /// Store result and transition to Success (auto context).
    pub async fn set_invocation_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .set_invocation_result(invocation_id, result, runner_id)
            .await
    }

    /// Store result and transition to Success (explicit context).
    pub async fn set_invocation_result_with_context(
        &self,
        invocation_id: &InvocationId,
        result: &str,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: std::collections::BTreeMap<String, String>,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .set_invocation_result_with_context(
                invocation_id,
                result,
                runner_id,
                task_id,
                arguments,
            )
            .await
    }

    /// Store exception and transition to Failed (auto context).
    pub async fn set_invocation_exception(
        &self,
        invocation_id: &InvocationId,
        error_type: &str,
        error_message: &str,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .set_invocation_exception(invocation_id, error_type, error_message, runner_id)
            .await
    }

    /// Store exception and transition to Failed (explicit context).
    pub async fn set_invocation_exception_with_context(
        &self,
        invocation_id: &InvocationId,
        error_type: &str,
        error_message: &str,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: std::collections::BTreeMap<String, String>,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .set_invocation_exception_with_context(
                invocation_id,
                error_type,
                error_message,
                runner_id,
                task_id,
                arguments,
            )
            .await
    }

    /// Set retry (auto context).
    pub async fn set_invocation_retry(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .retry_invocation(&self.config, &self.task_catalog, invocation_id, runner_id)
            .await
    }

    /// Set retry (explicit context).
    pub async fn set_invocation_retry_with_context(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: std::collections::BTreeMap<String, String>,
    ) -> RustvelloResult<()> {
        let (queue_name, priority) = self.task_routing(task_id)?;
        self.orchestrator
            .set_invocation_retry_with_context(
                invocation_id,
                runner_id,
                task_id,
                arguments,
                &queue_name,
                priority,
            )
            .await
    }

    /// Retrieve invocations ready to run, handling blocking priority and CC.
    ///
    /// Builds a config resolver closure from the task registry and delegates
    /// to the orchestrator, which handles the blocking priority + broker + CC logic.
    pub async fn get_invocations_to_run(
        &self,
        max_num_invocations: usize,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let config_for_task = |task_id: &TaskId| -> Option<TaskConfig> {
            self.task_catalog
                .get(task_id)
                .map(|t| self.resolve_task_config(task_id, t.config()))
        };
        let queue_names = crate::orchestration::queue_names_for_retrieval(&self.config);
        self.orchestrator
            .get_invocations_to_run(
                max_num_invocations,
                runner_id,
                &queue_names,
                &config_for_task,
            )
            .await
    }

    // -----------------------------------------------------------------------
    // Phase 6 composites
    // -----------------------------------------------------------------------

    /// Route a call: check registration CC, create or reuse invocation, route.
    pub async fn route_call(
        &self,
        new_invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        registration_cc: ConcurrencyControlType,
        index_cc: bool,
        runner_id: &RunnerId,
    ) -> RustvelloResult<RouteCallResult> {
        self.orchestrator
            .route_catalog_call(
                &self.config,
                &self.task_catalog,
                new_invocation_id,
                call_dto,
                cc_args,
                registration_cc,
                index_cc,
                runner_id,
            )
            .await
    }

    /// Reroute a set of invocations (transition to Rerouted + re-enqueue).
    pub async fn reroute_invocations(
        &self,
        invocation_ids: &[InvocationId],
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .reroute_catalog_invocations(
                &self.config,
                &self.task_catalog,
                invocation_ids,
                runner_id,
            )
            .await
    }

    /// Execute one trigger evaluation loop iteration.
    pub async fn trigger_loop_iteration(
        &self,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.orchestrator
            .run_trigger_iteration(&self.config, &self.task_catalog, runner_id)
            .await
    }

    /// Execute one atomic service check: coordination + triggers + recording.
    ///
    /// Returns `None` if this runner is not authorized to run now,
    /// `Some(created_ids)` if it ran the trigger loop.
    pub async fn check_atomic_services(
        &self,
        runner_id: &RunnerId,
        service_interval_minutes: f64,
        spread_margin_minutes: f64,
        runner_timeout_seconds: f64,
    ) -> RustvelloResult<Option<Vec<InvocationId>>> {
        self.orchestrator
            .run_atomic_services(
                &self.config,
                &self.task_catalog,
                runner_id,
                service_interval_minutes,
                spread_margin_minutes,
                runner_timeout_seconds,
            )
            .await
    }
}
