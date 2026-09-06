use rustvello_core::call::{params_to_serialized_arguments, Call};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::invocation::{Invocation, InvocationHandle, SyncInvocation};
use rustvello_core::task::{ForeignTask, Task};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::RustvelloApp;

impl RustvelloApp {
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
        self.orchestrator
            .submit(
                &self.config,
                &self.task_catalog,
                CallDTO::new(task_id.clone(), args),
            )
            .await
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
        _key_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<InvocationId> {
        self.orchestrator
            .submit_with_registration_control(&self.config, &self.task_catalog, task_id, args)
            .await
    }

    /// Execute a task synchronously (dev mode).
    ///
    /// Bypasses the broker/runner — executes immediately in the current thread.
    pub async fn submit_sync(
        &self,
        task_id: &TaskId,
        args: SerializedArguments,
    ) -> RustvelloResult<String> {
        let task_def = self.task_catalog.registry().get(task_id).ok_or_else(|| {
            RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            }
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
            .orchestrator
            .invocation_control()
            .get_invocation_status(invocation_id)
            .await?;
        Ok(record.status)
    }

    /// Get the result of a completed invocation.
    pub async fn get_result(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Option<String>> {
        self.orchestrator
            .state_backend()
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

        if !self.task_catalog.contains(task_id) {
            return Err(RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            });
        }

        let invocation_id = self
            .orchestrator
            .submit(
                &self.config,
                &self.task_catalog,
                Call::new(task, params).to_dto()?,
            )
            .await?;

        Ok(InvocationHandle::new(
            invocation_id,
            self.orchestrator.invocation_control(),
            self.orchestrator.state_backend(),
        ))
    }

    /// Submit a typed foreign task for distributed execution.
    ///
    /// The task is registered and routed exactly like any other task, but only
    /// a runner whose language matches the task ID can execute it.
    pub async fn submit_foreign_call<F: ForeignTask>(
        &self,
        task: &F,
        params: F::Params,
    ) -> RustvelloResult<InvocationHandle<F::Result>> {
        let task_id = task.task_id();
        if !self.task_catalog.contains(&task_id) {
            return Err(RustvelloError::TaskNotRegistered { task_id });
        }

        let args = params_to_serialized_arguments(&params)?;
        let invocation_id = self.submit(&task_id, args).await?;

        Ok(InvocationHandle::new(
            invocation_id,
            self.orchestrator.invocation_control(),
            self.orchestrator.state_backend(),
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
        if !self.task_catalog.contains(task_id) {
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
