use rustvello_core::error::RustvelloResult;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus, InvocationStatusRecord};

use super::RustvelloApp;
use crate::orchestration::RouteCallResult;

// ---------------------------------------------------------------------------
// Composite operations — thin delegation to OrchestratorCoordinator
//
// Each method delegates to self.coordinator, which bundles multiple
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
        self.coordinator
            .set_invocation_status(invocation_id, status, runner_id)
            .await
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
        self.coordinator
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
        self.coordinator
            .register_invocations(invocations, runner_id)
            .await
    }

    /// Store result and transition to Success (auto context).
    pub async fn set_invocation_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        self.coordinator
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
        self.coordinator
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
        self.coordinator
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
        self.coordinator
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
        self.coordinator
            .set_invocation_retry(invocation_id, runner_id)
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
        self.coordinator
            .set_invocation_retry_with_context(invocation_id, runner_id, task_id, arguments)
            .await
    }

    /// Retrieve invocations ready to run, handling blocking priority and CC.
    ///
    /// Builds a config resolver closure from the task registry and delegates
    /// to the coordinator, which handles the blocking priority + broker + CC logic.
    pub async fn get_invocations_to_run(
        &self,
        max_num_invocations: usize,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let config_for_task = |task_id: &TaskId| -> Option<TaskConfig> {
            self.task_registry
                .get_dyn(task_id)
                .map(|t| self.resolve_task_config(task_id, t.config()))
        };
        self.coordinator
            .get_invocations_to_run(max_num_invocations, runner_id, &config_for_task)
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
        self.coordinator
            .route_call(
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
        self.coordinator
            .reroute_invocations(invocation_ids, runner_id)
            .await
    }

    /// Execute one trigger evaluation loop iteration.
    pub async fn trigger_loop_iteration(
        &self,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.coordinator.trigger_loop_iteration(runner_id).await
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
        self.coordinator
            .check_atomic_services(
                runner_id,
                service_interval_minutes,
                spread_margin_minutes,
                runner_timeout_seconds,
            )
            .await
    }
}
