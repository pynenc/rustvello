use std::sync::Arc;

use rustvello_core::call::Call;
use rustvello_core::context::{get_or_create_runner_context, with_invocation_context};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::invocation::{Invocation, InvocationHandle, SyncInvocation};
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_core::task::Task;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{
    ConcurrencyControlType, InvocationStatus, InvocationStatusRecord, ALL_STATUSES,
};

use super::RustvelloApp;

impl RustvelloApp {
    /// Resolve the workflow identity for a new invocation.
    ///
    /// If called from within a running task (i.e. `with_invocation_context()` returns
    /// `Some`), the new invocation inherits the parent's workflow unless
    /// `force_new_workflow` is set—in which case a sub-workflow is created.
    /// When called from top-level code (no context), a new root workflow is started.
    fn resolve_workflow(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        force_new_workflow: bool,
    ) -> (Option<InvocationId>, WorkflowIdentity) {
        // Use borrow-based accessor to avoid cloning the entire context
        let parent_info = with_invocation_context(|ctx| {
            (
                ctx.invocation_id.clone(),
                ctx.workflow.workflow_id.clone(),
                ctx.workflow.workflow_type.clone(),
                ctx.workflow.depth,
            )
        });

        match parent_info {
            Some((parent_inv_id, wf_id, wf_type, depth)) if !force_new_workflow => {
                let workflow =
                    WorkflowIdentity::child(wf_id, wf_type, parent_inv_id.clone(), depth + 1);
                (Some(parent_inv_id), workflow)
            }
            Some((parent_inv_id, wf_id, _, _)) => {
                let workflow =
                    WorkflowIdentity::sub_workflow(invocation_id.clone(), task_id.clone(), wf_id);
                (Some(parent_inv_id), workflow)
            }
            None => {
                let workflow = WorkflowIdentity::root(invocation_id.clone(), task_id.clone());
                (None, workflow)
            }
        }
    }

    /// Ensure the runner context for the given runtime context is persisted
    /// in the state backend, so monitoring can look it up. Uses an in-memory
    /// cache to avoid redundant writes (mirrors pynenc's `_runner_context_cache`).
    async fn ensure_runner_context_stored(
        &self,
        ctx: &rustvello_core::context::RunnerContext,
    ) -> RustvelloResult<()> {
        let rid = ctx.runner_id.to_string();
        {
            let cache = self.stored_runner_cache.lock().await;
            if cache.contains(&rid) {
                return Ok(());
            }
        }
        let stored = StoredRunnerContext::from_runtime(ctx);
        self.coordinator
            .state_backend
            .store_runner_context(&stored)
            .await?;
        {
            let mut cache = self.stored_runner_cache.lock().await;
            cache.insert(rid);
        }
        Ok(())
    }

    /// Submit a task for distributed execution.
    ///
    /// Creates a call from the task and arguments, registers an invocation
    /// with the orchestrator, stores it in the state backend, and routes
    /// it through the broker.
    pub async fn submit(
        &self,
        task_id: &TaskId,
        args: SerializedArguments,
    ) -> RustvelloResult<InvocationId> {
        // Verify task is registered and get config in one lookup
        let task = self.task_registry.get_dyn(task_id).ok_or_else(|| {
            RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            }
        })?;
        let force_new = self.resolve_force_new_workflow(task_id, task.config());

        // Create the call DTO
        let call = CallDTO::new(task_id.clone(), args);

        // Register invocation with orchestrator
        let invocation_id = self
            .coordinator
            .orchestrator
            .register_invocation(&call)
            .await?;

        // Resolve workflow identity
        let (parent_id, workflow) = self.resolve_workflow(&invocation_id, task_id, force_new);

        // Store in state backend
        let inv_dto = InvocationDTO::with_workflow(
            invocation_id.clone(),
            task_id.clone(),
            call.call_id.clone(),
            parent_id,
            workflow,
        );
        self.coordinator
            .state_backend
            .upsert_invocation(&inv_dto, &call)
            .await?;

        // Record the initial Registered history entry.
        // Always capture the caller's runner identity — never None.
        // Inside a runner task: returns the worker's runner_id.
        // Outside (scripts/CLI): returns an ExternalRunner-style hostname-pid id.
        let caller_ctx = get_or_create_runner_context();
        self.ensure_runner_context_stored(&caller_ctx).await?;
        let caller_runner_id = caller_ctx.runner_id.clone();
        let history = InvocationHistory::new(
            invocation_id.clone(),
            InvocationStatusRecord::new(
                InvocationStatus::Registered,
                Some(caller_runner_id.clone()),
            ),
            None,
        )
        .with_runner(caller_runner_id);
        self.coordinator.state_backend.add_history(&history).await?;

        // Route to broker — runner will transition Registered → Pending → Running
        self.coordinator
            .broker
            .route_invocation_for_task(&invocation_id, task_id)
            .await?;

        Ok(invocation_id)
    }

    /// Submit a task with registration concurrency control.
    ///
    /// Checks for existing non-terminal invocations matching the given CC
    /// key arguments before creating a new one. If a matching invocation
    /// already exists, returns its ID (dedup). Otherwise, delegates to
    /// `submit()` to create and route a new invocation.
    ///
    /// `key_args` controls the CC scope:
    /// - `Some(args)`: arg-level CC — dedup by the CC key hash of these args
    /// - `None`: task-level CC — dedup across all invocations for this task
    ///
    /// Mirrors pynenc's `BaseOrchestrator.route_call()` registration CC logic.
    pub async fn submit_with_cc(
        &self,
        task_id: &TaskId,
        args: SerializedArguments,
        key_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<InvocationId> {
        // Look up config to check registration_concurrency
        let task = self.task_registry.get_dyn(task_id).ok_or_else(|| {
            RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            }
        })?;
        let config = self.resolve_task_config(task_id, task.config());

        if config.registration_concurrency != ConcurrencyControlType::Unlimited {
            // Collect non-terminal statuses for the dedup check
            let non_terminal: Vec<InvocationStatus> = ALL_STATUSES
                .iter()
                .copied()
                .filter(|s| !s.is_terminal())
                .collect();

            let existing = self
                .coordinator
                .orchestrator
                .get_existing_invocations(task_id, key_args, &non_terminal)
                .await?;

            if let Some(inv_id) = existing.first() {
                return Ok(inv_id.clone());
            }
        }

        self.submit(task_id, args).await
    }

    /// Execute a task synchronously (dev mode).
    ///
    /// Bypasses the broker/runner — executes immediately in the current thread.
    pub async fn submit_sync(
        &self,
        task_id: &TaskId,
        args: SerializedArguments,
    ) -> RustvelloResult<String> {
        let task_def =
            self.task_registry
                .get(task_id)
                .ok_or_else(|| RustvelloError::TaskNotRegistered {
                    task_id: task_id.clone(),
                })?;

        let args_json =
            serde_json::to_string(&args.0).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;

        (task_def.func)(args_json)
    }

    /// Get the current status of an invocation.
    pub async fn get_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatus> {
        let record = self
            .coordinator
            .orchestrator
            .get_invocation_status(invocation_id)
            .await?;
        Ok(record.status)
    }

    /// Get the result of a completed invocation.
    pub async fn get_result(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Option<String>> {
        self.coordinator
            .state_backend
            .get_result(invocation_id)
            .await
    }

    /// Submit a typed task for distributed execution, returning a typed handle.
    ///
    /// Creates a [`Call`], registers the invocation, stores it in the state
    /// backend, and routes it through the broker. Returns an
    /// [`InvocationHandle`] that provides typed result access.
    pub async fn submit_call<T: Task>(
        &self,
        task: &T,
        params: T::Params,
    ) -> RustvelloResult<InvocationHandle<T::Result>> {
        let task_id = task.task_id();

        // Verify task is registered
        if !self.task_registry.contains(task_id) {
            return Err(RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            });
        }

        // Create typed call and convert to DTO
        let call = Call::new(task, params);
        let call_dto = call.to_dto()?;

        // Register invocation with orchestrator
        let invocation_id = self
            .coordinator
            .orchestrator
            .register_invocation(&call_dto)
            .await?;

        // Resolve workflow identity — only need force_new_workflow from config
        let force_new = self.resolve_force_new_workflow(task_id, task.config());
        let (parent_id, workflow) = self.resolve_workflow(&invocation_id, task_id, force_new);

        // Store in state backend
        let inv_dto = InvocationDTO::with_workflow(
            invocation_id.clone(),
            task_id.clone(),
            call_dto.call_id.clone(),
            parent_id,
            workflow,
        );
        self.coordinator
            .state_backend
            .upsert_invocation(&inv_dto, &call_dto)
            .await?;

        // Record the initial Registered history entry.
        // Always capture the caller's runner identity — never None.
        let caller_ctx = get_or_create_runner_context();
        self.ensure_runner_context_stored(&caller_ctx).await?;
        let caller_runner_id = caller_ctx.runner_id.clone();
        let history = InvocationHistory::new(
            invocation_id.clone(),
            InvocationStatusRecord::new(
                InvocationStatus::Registered,
                Some(caller_runner_id.clone()),
            ),
            None,
        )
        .with_runner(caller_runner_id);
        self.coordinator.state_backend.add_history(&history).await?;

        // Route to broker — runner will transition Registered → Pending → Running
        self.coordinator
            .broker
            .route_invocation_for_task(&invocation_id, task_id)
            .await?;

        Ok(InvocationHandle::new(
            invocation_id,
            Arc::clone(&self.coordinator.orchestrator),
            Arc::clone(&self.coordinator.state_backend),
        ))
    }

    /// Execute a typed task synchronously (dev mode).
    ///
    /// Bypasses the broker/runner — executes immediately in the current thread.
    /// Returns the typed result directly.
    pub fn execute_sync<T: Task>(&self, task: &T, params: T::Params) -> RustvelloResult<T::Result> {
        task.run(params)
    }

    /// Unified call routing — automatically selects sync or distributed execution.
    ///
    /// Checks `config.dev_mode_force_sync`:
    /// - `true` → executes immediately with retry loop, returns `Invocation::Sync`
    /// - `false` → routes through broker, returns `Invocation::Distributed`
    ///
    /// This is the primary API for task submission. Matches pynenc's
    /// `Task._call()` pattern.
    pub async fn call<T: Task>(
        &self,
        task: &T,
        params: T::Params,
    ) -> RustvelloResult<Invocation<T::Result>>
    where
        T::Params: Clone,
    {
        let task_id = task.task_id();

        // Verify task is registered
        if !self.task_registry.contains(task_id) {
            return Err(RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            });
        }

        if self.config.dev_mode_force_sync {
            Ok(Invocation::Sync(Self::run_sync_with_retries(task, params)))
        } else {
            // Distributed path: delegate to submit_call
            let handle = self.submit_call(task, params).await?;
            Ok(Invocation::Distributed(handle))
        }
    }

    /// Execute a task synchronously with retry logic.
    ///
    /// Mirrors pynenc's `ConcurrentInvocation` retry behaviour.
    fn run_sync_with_retries<T: Task>(task: &T, params: T::Params) -> SyncInvocation<T::Result>
    where
        T::Params: Clone,
    {
        let invocation_id = InvocationId::new();
        let max_retries = task.config().max_retries;

        let mut last_err = None;
        for attempt in 0..=max_retries {
            match task.run(params.clone()) {
                Ok(result) => {
                    return SyncInvocation::success(invocation_id, result);
                }
                Err(e) => {
                    if attempt < max_retries {
                        tracing::warn!(
                            "Sync invocation:{} status:failed (attempt {}/{}): {}",
                            invocation_id,
                            attempt + 1,
                            max_retries,
                            e
                        );
                    }
                    last_err = Some(e);
                }
            }
        }

        SyncInvocation::failed(
            invocation_id,
            last_err.unwrap_or_else(|| RustvelloError::Internal {
                message: "retry loop exited without result".into(),
            }),
        )
    }
}
