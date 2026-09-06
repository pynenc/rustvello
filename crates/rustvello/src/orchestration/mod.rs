//! Concrete orchestration service for cross-backend invocation use cases.
//!
//! Mirrors pynenc's `BaseOrchestrator` coordination methods: each method
//! bundles multiple subsystem calls (status transition + history + trigger +
//! waiters + auto-purge) into a single Rust operation, eliminating FFI
//! round-trips when called from language bindings.
//!
//! The orchestrator owns operation ordering across invocation control, state,
//! broker, trigger, and payload ports. It does not execute user task code.

mod backends;
mod dispatch;
mod maintenance;
mod retrieval;
mod routing;
mod submission;
mod triggers;

pub(crate) use dispatch::queue_names_for_retrieval;
pub use routing::RouteCallResult;

use std::collections::BTreeMap;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::{RustvelloResult, TaskError};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::call::CallDTO;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::task_catalog::TaskCatalog;

/// Concrete owner of cross-backend invocation use cases.
///
/// Created by [`crate::app::RustvelloApp`] or directly via [`Self::new`]
/// for the `from_backends()` FFI path.  All methods are `&self` — the
/// service is logically immutable once built.
#[derive(Clone)]
pub struct Orchestrator {
    backends: backends::RuntimeBackends,
    stored_runner_cache: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>,
    auto_purge_delay_secs: u64,
}

/// Named ports transferred from the application composition root to a runner.
pub(crate) struct RunnerPorts {
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) invocation_control: Arc<dyn InvocationControlBackend>,
    pub(crate) state_backend: Arc<dyn StateBackend>,
    pub(crate) trigger_manager: Option<TriggerManager>,
}

/// Convert a purge duration in fractional hours to whole seconds.
///
/// Silently clamps invalid values (NaN, negative) to 0 (disables auto-purge)
/// and logs a warning so misconfiguration is visible in logs without panicking.
/// Values exceeding ~`u64::MAX` seconds are clamped to `u64::MAX`.
fn hours_to_purge_secs(hours: f64) -> u64 {
    if !hours.is_finite() || hours < 0.0 {
        tracing::warn!(
            auto_purge_hours = hours,
            "auto_final_invocation_purge_hours is not a positive finite number; \
             auto-purge disabled (effective value: 0 hours)"
        );
        return 0;
    }
    let secs = hours * 3600.0;
    if secs >= u64::MAX as f64 {
        tracing::warn!(
            auto_purge_hours = hours,
            "auto_final_invocation_purge_hours is too large; clamping to u64::MAX seconds"
        );
        return u64::MAX;
    }
    secs as u64
}

impl Orchestrator {
    /// Create an orchestrator from shared backend references.
    pub fn new(
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        broker: Arc<dyn Broker>,
        client_data_store: Arc<ClientDataStoreManager>,
        trigger_manager: Option<TriggerManager>,
        auto_purge_hours: f64,
    ) -> Self {
        Self {
            backends: backends::RuntimeBackends::new(
                orchestrator,
                state_backend,
                broker,
                client_data_store,
                trigger_manager,
            ),
            stored_runner_cache: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashSet::new()),
            ),
            auto_purge_delay_secs: hours_to_purge_secs(auto_purge_hours),
        }
    }

    pub(crate) fn for_runner(
        invocation_control: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        broker: Arc<dyn Broker>,
        trigger_manager: Option<TriggerManager>,
        auto_purge_hours: f64,
    ) -> Self {
        Self {
            backends: backends::RuntimeBackends::for_runner(
                invocation_control,
                state_backend,
                broker,
                trigger_manager,
            ),
            stored_runner_cache: Arc::new(
                tokio::sync::Mutex::new(std::collections::HashSet::new()),
            ),
            auto_purge_delay_secs: hours_to_purge_secs(auto_purge_hours),
        }
    }

    pub(crate) fn broker(&self) -> Arc<dyn Broker> {
        Arc::clone(&self.backends.broker)
    }

    pub(crate) fn invocation_control(&self) -> Arc<dyn InvocationControlBackend> {
        Arc::clone(&self.backends.invocation_control)
    }

    pub(crate) fn state_backend(&self) -> Arc<dyn StateBackend> {
        Arc::clone(&self.backends.state_backend)
    }

    pub(crate) fn client_data_store(&self) -> Arc<ClientDataStoreManager> {
        Arc::clone(&self.backends.client_data_store)
    }

    pub(crate) fn trigger_manager(&self) -> Option<&TriggerManager> {
        self.backends.trigger_manager.as_ref()
    }

    pub(crate) fn set_trigger_manager(&mut self, manager: TriggerManager) {
        self.backends.trigger_manager = Some(manager);
    }

    pub(crate) async fn purge(&self) -> RustvelloResult<()> {
        self.backends.invocation_control.purge().await?;
        self.backends.broker.purge(None).await?;
        self.backends.state_backend.purge().await?;
        if let Some(trigger_manager) = &self.backends.trigger_manager {
            trigger_manager.store().purge().await?;
        }
        Ok(())
    }

    pub(crate) fn into_runner_ports(self) -> RunnerPorts {
        RunnerPorts {
            broker: self.backends.broker,
            invocation_control: self.backends.invocation_control,
            state_backend: self.backends.state_backend,
            trigger_manager: self.backends.trigger_manager,
        }
    }

    pub async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        self.backends
            .invocation_control
            .set_waiting_for(waiter, waited_on)
            .await
    }

    pub(crate) async fn release_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        self.backends
            .invocation_control
            .remove_from_concurrency_index(invocation_id)
            .await
    }

    pub(crate) async fn retry_invocation(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        let invocation = self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await?;
        let (queue, priority) = task_catalog
            .routing_for(app_config, &invocation.task_id)
            .ok_or_else(
                || rustvello_core::error::RustvelloError::TaskNotRegistered {
                    task_id: invocation.task_id,
                },
            )?;
        self.set_invocation_retry(invocation_id, runner_id, &queue, priority)
            .await
    }

    // -----------------------------------------------------------------------
    // Status transition — mirrors pynenc's BaseOrchestrator.set_invocation_status
    // -----------------------------------------------------------------------

    /// Atomic status transition with all side-effects, auto-resolving trigger context.
    ///
    /// Looks up task_id and arguments from the state backend for trigger
    /// reporting.  Prefer [`Self::set_invocation_status_with_context`] when
    /// the caller already has this data.
    pub async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: &RunnerId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let (task_id, arguments) = if self.backends.trigger_manager.is_some() {
            self.get_trigger_context(invocation_id).await
        } else {
            (TaskId::new("_", "_"), BTreeMap::new())
        };
        self.set_invocation_status_with_context(
            invocation_id,
            status,
            runner_id,
            &task_id,
            arguments,
        )
        .await
    }

    /// Atomic status transition with all side-effects and explicit trigger context.
    ///
    /// 1. Atomic status transition (validates state machine)
    /// 2. If terminal: release waiters + schedule auto-purge
    /// 3. Record history in state backend
    /// 4. Notify trigger system (if configured)
    pub async fn set_invocation_status_with_context(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: BTreeMap<String, String>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        // 1. Atomic status transition
        let record = self
            .backends
            .invocation_control
            .set_invocation_status(invocation_id, status, Some(runner_id))
            .await?;

        // 2. Terminal side-effects
        if status.is_terminal() {
            self.backends
                .invocation_control
                .release_waiters(invocation_id)
                .await?;
            if self.auto_purge_delay_secs > 0 {
                self.backends
                    .invocation_control
                    .schedule_auto_purge(invocation_id)
                    .await?;
            }
        }

        // 3. Record history
        let history = InvocationHistory::new(invocation_id.clone(), record.clone(), None)
            .with_runner(runner_id.clone());
        self.backends.state_backend.add_history(&history).await?;

        // 4. Trigger notification
        if let Some(ref tm) = self.backends.trigger_manager {
            let ctx = rustvello_proto::trigger::StatusContext {
                invocation_id: invocation_id.clone(),
                task_id: task_id.clone(),
                status,
                arguments,
            };
            tm.report_status_change(&ctx).await?;
        }

        Ok(record)
    }

    // -----------------------------------------------------------------------
    // Registration — mirrors pynenc's BaseOrchestrator.register_new_invocations
    // -----------------------------------------------------------------------

    /// Register invocations with all side-effects.
    ///
    /// 1. Upsert each invocation + call in state backend
    /// 2. Register with orchestrator (sets Registered status)
    /// 3. Record history for each
    /// 4. Notify trigger system (if configured)
    /// 5. Route all through broker
    pub async fn register_invocations(
        &self,
        invocations: &[(InvocationDTO, CallDTO)],
        runner_id: &RunnerId,
        routes: &[(String, f64)],
    ) -> RustvelloResult<()> {
        if invocations.len() != routes.len() {
            return Err(rustvello_core::error::RustvelloError::Internal {
                message: "invocation and routing counts differ".to_owned(),
            });
        }
        for (inv_dto, call_dto) in invocations {
            self.backends
                .state_backend
                .upsert_invocation(inv_dto, call_dto)
                .await?;

            let record = self
                .backends
                .invocation_control
                .register_invocation_with_id(&inv_dto.invocation_id, call_dto, Some(runner_id))
                .await?;

            let history =
                InvocationHistory::new(inv_dto.invocation_id.clone(), record.clone(), None)
                    .with_runner(runner_id.clone());
            self.backends.state_backend.add_history(&history).await?;

            if let Some(ref tm) = self.backends.trigger_manager {
                let ctx = rustvello_proto::trigger::StatusContext {
                    invocation_id: inv_dto.invocation_id.clone(),
                    task_id: inv_dto.task_id.clone(),
                    status: record.status,
                    arguments: call_dto.serialized_arguments.0.clone(),
                };
                tm.report_status_change(&ctx).await?;
            }
        }

        for ((invocation, _), (queue_name, priority)) in invocations.iter().zip(routes) {
            self.backends
                .broker
                .route_invocation_with_options(
                    &invocation.invocation_id,
                    Some(&invocation.task_id),
                    queue_name,
                    *priority,
                )
                .await?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Result — mirrors pynenc's BaseOrchestrator.set_invocation_result
    // -----------------------------------------------------------------------

    /// Store result and transition to Success, auto-resolving trigger context.
    pub async fn set_invocation_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        let (task_id, arguments) = if self.backends.trigger_manager.is_some() {
            self.get_trigger_context(invocation_id).await
        } else {
            (TaskId::new("_", "_"), BTreeMap::new())
        };
        self.set_invocation_result_with_context(
            invocation_id,
            result,
            runner_id,
            &task_id,
            arguments,
        )
        .await
    }

    /// Store a successful result and transition to Success with explicit context.
    ///
    /// 1. Store result in state backend
    /// 2. Set status to Success (includes release_waiters, auto_purge, history, trigger)
    /// 3. Notify trigger system of result (if configured)
    pub async fn set_invocation_result_with_context(
        &self,
        invocation_id: &InvocationId,
        result: &str,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: BTreeMap<String, String>,
    ) -> RustvelloResult<()> {
        self.backends
            .state_backend
            .store_result(invocation_id, result)
            .await?;

        self.set_invocation_status_with_context(
            invocation_id,
            InvocationStatus::Success,
            runner_id,
            task_id,
            arguments.clone(),
        )
        .await?;

        // Trigger result notification
        if let Some(ref tm) = self.backends.trigger_manager {
            let result_value: serde_json::Value =
                serde_json::from_str(result).unwrap_or_else(|e| {
                    tracing::warn!(
                        invocation_id = %invocation_id,
                        "Failed to parse result as JSON for trigger: {e}; wrapping as string"
                    );
                    serde_json::Value::String(result.to_owned())
                });
            let ctx = rustvello_proto::trigger::ResultContext {
                invocation_id: invocation_id.clone(),
                task_id: task_id.clone(),
                result: result_value,
                arguments,
            };
            tm.report_result(&ctx).await?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Exception — mirrors pynenc's BaseOrchestrator.set_invocation_exception
    // -----------------------------------------------------------------------

    /// Store exception and transition to Failed, auto-resolving trigger context.
    pub async fn set_invocation_exception(
        &self,
        invocation_id: &InvocationId,
        error_type: &str,
        error_message: &str,
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        let (task_id, arguments) = if self.backends.trigger_manager.is_some() {
            self.get_trigger_context(invocation_id).await
        } else {
            (TaskId::new("_", "_"), BTreeMap::new())
        };
        self.set_invocation_exception_with_context(
            invocation_id,
            error_type,
            error_message,
            runner_id,
            &task_id,
            arguments,
        )
        .await
    }

    /// Store an exception and transition to Failed with explicit context.
    ///
    /// 1. Store error in state backend
    /// 2. Set status to Failed (includes release_waiters, auto_purge, history, trigger)
    /// 3. Notify trigger system of failure (if configured)
    pub async fn set_invocation_exception_with_context(
        &self,
        invocation_id: &InvocationId,
        error_type: &str,
        error_message: &str,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: BTreeMap<String, String>,
    ) -> RustvelloResult<()> {
        let task_error = TaskError {
            error_type: error_type.to_owned(),
            message: error_message.to_owned(),
            traceback: None,
        };
        self.backends
            .state_backend
            .store_error(invocation_id, &task_error)
            .await?;

        self.set_invocation_status_with_context(
            invocation_id,
            InvocationStatus::Failed,
            runner_id,
            task_id,
            arguments.clone(),
        )
        .await?;

        if let Some(ref tm) = self.backends.trigger_manager {
            let ctx = rustvello_proto::trigger::ExceptionContext {
                invocation_id: invocation_id.clone(),
                task_id: task_id.clone(),
                error_type: error_type.to_owned(),
                error_message: error_message.to_owned(),
                arguments,
            };
            tm.report_failure(&ctx).await?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Retry — mirrors pynenc's BaseOrchestrator.set_invocation_retry
    // -----------------------------------------------------------------------

    /// Set an invocation for retry, auto-resolving trigger context.
    pub async fn set_invocation_retry(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        let task_id = self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await?
            .task_id;
        let arguments = self.get_invocation_arguments(invocation_id).await;
        self.set_invocation_retry_with_context(
            invocation_id,
            runner_id,
            &task_id,
            arguments,
            queue_name,
            priority,
        )
        .await
    }

    /// Set an invocation for retry with explicit context.
    ///
    /// 1. Set status to Retry (via `set_invocation_status_with_context`)
    /// 2. Increment retry counter
    /// 3. Re-route through broker
    pub async fn set_invocation_retry_with_context(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: BTreeMap<String, String>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        self.set_invocation_status_with_context(
            invocation_id,
            InvocationStatus::Retry,
            runner_id,
            task_id,
            arguments,
        )
        .await?;

        self.backends
            .invocation_control
            .increment_invocation_retries(invocation_id)
            .await?;

        self.backends
            .broker
            .route_invocation_with_options(invocation_id, Some(task_id), queue_name, priority)
            .await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Shared helpers (used by composites in submodules)
    // -----------------------------------------------------------------------

    /// Look up serialized arguments for an invocation (for trigger context).
    pub(crate) async fn get_invocation_arguments(
        &self,
        invocation_id: &InvocationId,
    ) -> BTreeMap<String, String> {
        let inv_dto = match self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await
        {
            Ok(dto) => dto,
            Err(_) => return BTreeMap::new(),
        };
        match self.backends.state_backend.get_call(&inv_dto.call_id).await {
            Ok(call) => call.serialized_arguments.0,
            Err(_) => BTreeMap::new(),
        }
    }

    /// Look up task_id and arguments for trigger context from state backend.
    pub async fn get_trigger_context(
        &self,
        invocation_id: &InvocationId,
    ) -> (TaskId, BTreeMap<String, String>) {
        let inv_dto = match self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await
        {
            Ok(dto) => dto,
            Err(_) => return (TaskId::new("unknown", "unknown"), BTreeMap::new()),
        };
        let args = match self.backends.state_backend.get_call(&inv_dto.call_id).await {
            Ok(call) => call.serialized_arguments.0,
            Err(_) => BTreeMap::new(),
        };
        (inv_dto.task_id, args)
    }
}
