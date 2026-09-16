use rustvello_core::context::{get_or_create_runner_context, with_invocation_context};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::observability::{
    capture_w3c_trace_context, is_valid_w3c_trace_context, TaskAttemptContext, TaskLifecycleEvent,
    TraceContextCarrier, WorkerTelemetryContext,
};
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{
    ConcurrencyControlType, InvocationStatus, InvocationStatusRecord, ALL_STATUSES,
};

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

impl Orchestrator {
    pub(crate) async fn submit_with_registration_control(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        task_id: &TaskId,
        args: SerializedArguments,
    ) -> RustvelloResult<InvocationId> {
        let task = task_catalog
            .get(task_id)
            .ok_or_else(|| RustvelloError::TaskNotRegistered {
                task_id: task_id.clone(),
            })?;
        let config = task_catalog.resolve_config(app_config, task_id, task.config());

        if config.registration_concurrency != ConcurrencyControlType::Unlimited {
            let non_terminal: Vec<_> = ALL_STATUSES
                .iter()
                .copied()
                .filter(|status| !status.is_terminal())
                .collect();
            let requested_key = crate::task_config::concurrency_arguments(
                config.registration_concurrency,
                &config.key_arguments,
                &args,
            );

            for invocation_id in self
                .backends
                .invocation_control
                .get_existing_invocations(task_id, None, &non_terminal)
                .await?
            {
                if config.registration_concurrency == ConcurrencyControlType::Task {
                    return Ok(invocation_id);
                }
                let invocation = self
                    .backends
                    .state_backend
                    .get_invocation(&invocation_id)
                    .await?;
                let call = self
                    .backends
                    .state_backend
                    .get_call(&invocation.call_id)
                    .await?;
                let existing_key = crate::task_config::concurrency_arguments(
                    config.registration_concurrency,
                    &config.key_arguments,
                    &call.serialized_arguments,
                );
                if existing_key == requested_key {
                    return Ok(invocation_id);
                }
            }
        }

        self.submit(
            app_config,
            task_catalog,
            CallDTO::new(task_id.clone(), args),
        )
        .await
    }

    /// Submit one registered task through the complete persistence and routing use case.
    pub(crate) async fn submit(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        call: CallDTO,
    ) -> RustvelloResult<InvocationId> {
        self.submit_with_trace_context(app_config, task_catalog, call, None)
            .await
    }

    pub(crate) async fn submit_with_trace_context(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        call: CallDTO,
        explicit_trace_context: Option<TraceContextCarrier>,
    ) -> RustvelloResult<InvocationId> {
        self.submit_with_id(app_config, task_catalog, call, explicit_trace_context, None)
            .await
    }

    pub(crate) async fn submit_with_id(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        call: CallDTO,
        explicit_trace_context: Option<TraceContextCarrier>,
        requested_id: Option<InvocationId>,
    ) -> RustvelloResult<InvocationId> {
        let publication = self.publication()?;
        if requested_id.is_some() {
            self.require_crash_consistent_publication()?;
        }
        let task =
            task_catalog
                .get(&call.task_id)
                .ok_or_else(|| RustvelloError::TaskNotRegistered {
                    task_id: call.task_id.clone(),
                })?;
        let task_config = task_catalog.resolve_config(app_config, &call.task_id, task.config());

        let invocation_id = requested_id.unwrap_or_default();
        let (parent_id, workflow) =
            Self::resolve_workflow(&invocation_id, &call.task_id, task_config.is_workflow_task);
        let trace_context = Self::resolve_trace_context(explicit_trace_context)?;
        let invocation = Self::invocation_dto(
            invocation_id.clone(),
            call.task_id.clone(),
            call.call_id.clone(),
            parent_id,
            workflow.clone(),
        )
        .with_trace_context(trace_context.clone());

        let mut caller = get_or_create_runner_context();
        if caller.app_id.as_ref() != app_config.app_id {
            caller = rustvello_core::context::RunnerContext::external();
            caller.app_id = app_config.app_id.clone().into();
        }
        let created = if let Some(publication) = publication {
            publication
                .submit(rustvello_core::publication::SubmissionPublication {
                    invocation: invocation.clone(),
                    call: call.clone(),
                    runner_id: caller.runner_id.clone(),
                    runner_context: Some(StoredRunnerContext::from_runtime(&caller)),
                    workflow_root: task_config.is_workflow_task,
                    cc_arguments: None,
                    route: rustvello_core::publication::PublicationRoute {
                        queue: task_config.queue.clone(),
                        priority: task_config.priority,
                    },
                })
                .await?
        } else {
            self.backends
                .invocation_control
                .register_invocation_with_id(&invocation_id, &call, Some(&caller.runner_id))
                .await?;

            self.backends
                .state_backend
                .upsert_invocation(&invocation, &call)
                .await?;
            if task_config.is_workflow_task {
                if let Some(workflow) = workflow.as_ref() {
                    self.backends
                        .state_backend
                        .store_workflow_run(workflow)
                        .await?;
                }
            }

            self.ensure_runner_context_stored(&caller).await?;
            let runner_id = caller.runner_id.clone();
            self.backends
                .state_backend
                .add_history(
                    &InvocationHistory::new(
                        invocation_id.clone(),
                        InvocationStatusRecord::new(
                            InvocationStatus::Registered,
                            Some(runner_id.clone()),
                        ),
                        None,
                    )
                    .with_runner(runner_id),
                )
                .await?;

            self.backends
                .broker
                .route_invocation_with_options(
                    &invocation_id,
                    Some(&call.task_id),
                    &task_config.queue,
                    task_config.priority,
                )
                .await?;

            true
        };
        if !created {
            return Ok(invocation_id);
        }

        let telemetry_context = TaskAttemptContext::new(
            app_config.app_id.clone(),
            call.task_id,
            invocation_id.clone(),
            0,
            task_config.queue,
            invocation.parent_invocation_id,
            invocation.workflow,
            WorkerTelemetryContext::from(&caller),
            trace_context,
        );
        self.event_emitter
            .on_task_lifecycle(&TaskLifecycleEvent::submitted(telemetry_context));
        Ok(invocation_id)
    }

    fn resolve_trace_context(
        explicit: Option<TraceContextCarrier>,
    ) -> RustvelloResult<TraceContextCarrier> {
        let inherited = with_invocation_context(|context| context.trace_context.clone());
        let active = capture_w3c_trace_context();
        let carrier = explicit
            .or_else(|| (!active.is_empty()).then_some(active))
            .or(inherited)
            .unwrap_or_default();
        if !is_valid_w3c_trace_context(&carrier) {
            return Err(RustvelloError::Configuration {
                message: "invalid W3C traceparent or tracestate".to_string(),
            });
        }
        Ok(carrier)
    }

    fn resolve_workflow(
        invocation_id: &InvocationId,
        task_id: &TaskId,
        is_workflow_task: bool,
    ) -> (Option<InvocationId>, Option<WorkflowIdentity>) {
        let parent_info =
            with_invocation_context(|ctx| (ctx.invocation_id.clone(), ctx.workflow.clone()));

        match (parent_info, is_workflow_task) {
            (Some((parent_id, Some(parent_workflow))), false) => (
                Some(parent_id.clone()),
                Some(WorkflowIdentity::child(
                    parent_workflow.workflow_id,
                    parent_workflow.workflow_type,
                    parent_id,
                    parent_workflow.depth + 1,
                )),
            ),
            (Some((parent_id, Some(parent_workflow))), true) => (
                Some(parent_id),
                Some(WorkflowIdentity::sub_workflow(
                    invocation_id.clone(),
                    task_id.clone(),
                    parent_workflow.workflow_id,
                )),
            ),
            (Some((parent_id, None)), false) => (Some(parent_id), None),
            (Some((parent_id, None)), true) => (
                Some(parent_id),
                Some(WorkflowIdentity::root(
                    invocation_id.clone(),
                    task_id.clone(),
                )),
            ),
            (None, false) => (None, None),
            (None, true) => (
                None,
                Some(WorkflowIdentity::root(
                    invocation_id.clone(),
                    task_id.clone(),
                )),
            ),
        }
    }

    fn invocation_dto(
        invocation_id: InvocationId,
        task_id: TaskId,
        call_id: rustvello_proto::identifiers::CallId,
        parent_id: Option<InvocationId>,
        workflow: Option<WorkflowIdentity>,
    ) -> InvocationDTO {
        match workflow {
            Some(workflow) => {
                InvocationDTO::with_workflow(invocation_id, task_id, call_id, parent_id, workflow)
            }
            None => {
                let mut invocation = InvocationDTO::new(invocation_id, task_id, call_id);
                invocation.parent_invocation_id = parent_id;
                invocation
            }
        }
    }

    async fn ensure_runner_context_stored(
        &self,
        context: &rustvello_core::context::RunnerContext,
    ) -> RustvelloResult<()> {
        let runner_id = context.runner_id.to_string();
        {
            let cache = self.stored_runner_cache.lock().await;
            if cache.contains(&runner_id) {
                return Ok(());
            }
        }

        if self
            .backends
            .state_backend
            .get_runner_context(&runner_id)
            .await?
            .is_some()
        {
            self.stored_runner_cache.lock().await.insert(runner_id);
            return Ok(());
        }

        self.backends
            .state_backend
            .store_runner_context(&StoredRunnerContext::from_runtime(context))
            .await?;
        self.stored_runner_cache.lock().await.insert(runner_id);
        Ok(())
    }
}
