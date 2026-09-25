//! Shared execution logic for all runner types.
//!
//! Every runner follows the same lifecycle for executing an invocation:
//! claim → running → load data → middleware before → **execute** → middleware after → result handling.
//! The only variation is the actual task execution mechanism (async, spawn_blocking, rayon pool).
//! This module extracts that shared flow into [`execute_invocation_common`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rustvello_core::context::{InvocationContext, RunnerContext};
use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::middleware::TaskMiddleware;
use rustvello_core::observability::{
    EventEmitter, LastResult, TaskAttemptContext, TaskLifecycleEvent, WorkerState,
    WorkerTelemetryContext,
};
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use crate::orchestration::Orchestrator as InvocationOrchestrator;
use crate::runner::attempt::{supervise, AttemptLimits, Interruption};
use crate::runner::executor::TaskExecutor;
use crate::task_catalog::TaskCatalog;

/// Extract a human-readable message from a panic payload.
///
/// Panics can carry `&str`, `String`, or an opaque `Box<dyn Any>`.
/// This function tries the first two and falls back to `"unknown panic"`.
pub(crate) fn unwrap_panic(panic: Box<dyn std::any::Any + Send>) -> RustvelloError {
    let msg = match panic.downcast_ref::<&str>() {
        Some(s) => (*s).to_string(),
        None => match panic.downcast_ref::<String>() {
            Some(s) => s.clone(),
            None => "unknown panic".to_string(),
        },
    };
    RustvelloError::Internal {
        message: format!("task panicked: {msg}"),
    }
}

/// Shared dependencies for task execution, passed by reference from each runner.
pub(crate) struct ExecutionDeps {
    pub lifecycle: InvocationOrchestrator,
    pub state_backend: Arc<dyn StateBackend>,
    pub emitter: Arc<dyn EventEmitter>,
    pub middlewares: Vec<Arc<dyn TaskMiddleware>>,
    pub task_catalog: Arc<TaskCatalog>,
    pub app_config: AppConfig,
    pub worker_states: Option<Arc<std::sync::Mutex<HashMap<RunnerId, WorkerState>>>>,
}

/// Execute an invocation using the shared lifecycle.
///
/// The executor runs task code while this function owns the distributed
/// invocation lifecycle around it.
pub(crate) async fn execute_invocation_common(
    deps: &ExecutionDeps,
    invocation_id: &InvocationId,
    worker_runner_id: &RunnerId,
    runner_label: &str,
    worker_ctx: &RunnerContext,
    executor: &dyn TaskExecutor,
) -> RustvelloResult<()> {
    // --- 1. Claim ownership ---
    match deps
        .lifecycle
        .set_invocation_status(invocation_id, InvocationStatus::Pending, worker_runner_id)
        .await
    {
        Ok(_) => {}
        Err(RustvelloError::InvalidStatusTransition {
            from_status,
            to_status,
            ..
        }) => {
            tracing::debug!(
                "Already claimed (race): from_status:{} to_status:{} skipped",
                from_status,
                to_status
            );
            deps.lifecycle
                .release_nontransactional_concurrency_slot(invocation_id)
                .await?;
            return Ok(());
        }
        Err(RustvelloError::OwnershipViolation { .. }) => {
            tracing::warn!("Already owned by another runner");
            deps.lifecycle
                .release_nontransactional_concurrency_slot(invocation_id)
                .await?;
            return Ok(());
        }
        Err(e) => {
            let _ = deps
                .lifecycle
                .release_nontransactional_concurrency_slot(invocation_id)
                .await;
            return Err(e);
        }
    }

    // --- 2. Transition to Running ---
    deps.lifecycle
        .set_invocation_status(invocation_id, InvocationStatus::Running, worker_runner_id)
        .await?;

    // --- 3. Load invocation data ---
    let inv_dto = deps.state_backend.get_invocation(invocation_id).await?;
    let call_dto = deps.state_backend.get_call(&inv_dto.call_id).await?;

    tracing::Span::current().record("task_id", tracing::field::display(&inv_dto.task_id));

    let task = deps.task_catalog.get(&inv_dto.task_id).ok_or_else(|| {
        RustvelloError::TaskNotRegistered {
            task_id: inv_dto.task_id.clone(),
        }
    })?;
    let resolved_task_config =
        deps.task_catalog
            .resolve_config(&deps.app_config, &inv_dto.task_id, task.config());

    tracing::debug!(
        runner = runner_label,
        executor = %executor.kind(),
        task_id = %inv_dto.task_id,
        "dispatching task to local executor"
    );

    // --- 4. Build invocation context ---
    let retry_history = deps.state_backend.get_history(invocation_id).await?;
    let num_retries = retry_history
        .iter()
        .filter(|h| h.status_record.status == InvocationStatus::Retry)
        .count() as u32;

    let identity = deps
        .lifecycle
        .begin_execution(
            invocation_id,
            worker_runner_id,
            num_retries,
            &inv_dto.trace_context,
        )
        .await?;
    let inv_ctx = InvocationContext {
        invocation_id: invocation_id.clone(),
        task_id: inv_dto.task_id.clone(),
        workflow: inv_dto.workflow.clone(),
        is_workflow_defining: task.config().is_workflow_task,
        state_backend: Some(Arc::clone(&deps.state_backend)),
        parent_invocation_id: inv_dto.parent_invocation_id.clone(),
        num_retries,
        trace_context: identity.execution_trace_context.clone(),
    };
    let run_ctx = worker_ctx.clone();
    let mut telemetry_context = TaskAttemptContext::new(
        Arc::clone(&worker_ctx.app_id),
        inv_dto.task_id.clone(),
        invocation_id.clone(),
        identity.attempt,
        resolved_task_config.queue.clone(),
        inv_dto.parent_invocation_id.clone(),
        inv_dto.workflow.clone(),
        WorkerTelemetryContext::from(worker_ctx),
        inv_dto.trace_context.clone(),
    );
    telemetry_context.execution_trace_context = identity.execution_trace_context;
    telemetry_context.previous_attempt_trace_context = identity.previous_attempt_trace_context;

    // --- 5. Pre-execution bookkeeping ---
    deps.emitter
        .on_task_lifecycle(&TaskLifecycleEvent::started(telemetry_context.clone()));

    if let Some(ref ws) = deps.worker_states {
        if let Ok(mut ws) = ws.lock() {
            if let Some(state) = ws.get_mut(worker_runner_id) {
                state.current_invocation = Some(invocation_id.clone());
                state.current_task = Some(inv_dto.task_id.clone());
                state.started_at = Some(Instant::now());
            }
        }
    }

    let exec_start = Instant::now();

    for mw in &deps.middlewares {
        mw.before(invocation_id, &inv_dto.task_id).await?;
    }

    // --- 6. Execute the task (runner-specific) under deadline and cancellation ---
    let limits = AttemptLimits::new(
        resolved_task_config.timeout(),
        deps.app_config.cancellation_check_interval_seconds,
    );
    let (exec_result, interruption) = supervise(
        executor.execute(
            Arc::clone(&task),
            call_dto.serialized_arguments.clone(),
            inv_ctx,
            run_ctx,
        ),
        &limits,
        &deps.lifecycle,
        invocation_id,
    )
    .await;

    // --- 7. Post-execution middleware ---
    for mw in deps.middlewares.iter().rev() {
        if let Err(e) = mw
            .after(invocation_id, &inv_dto.task_id, &exec_result)
            .await
        {
            tracing::warn!("After-middleware failed: {}", e);
        }
    }

    // --- 8. Handle result ---
    if interruption == Some(Interruption::Cancelled) {
        finish_cancelled(
            deps,
            invocation_id,
            worker_runner_id,
            &inv_dto.task_id,
            Some(telemetry_context),
            exec_start.elapsed(),
        )
        .await;
        return Ok(());
    }
    match exec_result {
        Ok(result) => {
            let written = deps
                .lifecycle
                .set_invocation_result_with_context(
                    invocation_id,
                    &result,
                    worker_runner_id,
                    &inv_dto.task_id,
                    call_dto.serialized_arguments.0.clone(),
                )
                .await;
            if discard_if_cancelled(deps, invocation_id, written).await? {
                finish_cancelled(
                    deps,
                    invocation_id,
                    worker_runner_id,
                    &inv_dto.task_id,
                    Some(telemetry_context),
                    exec_start.elapsed(),
                )
                .await;
                return Ok(());
            }

            // Remove from CC index now that invocation is complete
            if let Err(e) = deps
                .lifecycle
                .release_nontransactional_concurrency_slot(invocation_id)
                .await
            {
                tracing::warn!("Failed to remove from CC index: {}", e);
            }

            tracing::info!("Invocation completed status:success");

            let exec_duration = exec_start.elapsed();
            deps.emitter
                .on_task_lifecycle(&TaskLifecycleEvent::succeeded(
                    telemetry_context,
                    exec_duration,
                ));

            if let Some(ref ws) = deps.worker_states {
                if let Ok(mut ws) = ws.lock() {
                    if let Some(state) = ws.get_mut(worker_runner_id) {
                        state.current_invocation = None;
                        state.current_task = None;
                        state.started_at = None;
                        state.last_result = Some(LastResult::Success {
                            task_id: inv_dto.task_id.clone(),
                            duration: exec_duration,
                        });
                        state.invocations_completed += 1;
                    }
                }
            }
        }
        Err(err) => {
            let task_error = match &err {
                RustvelloError::TaskExecution {
                    error_type,
                    message,
                    traceback,
                } => TaskError {
                    error_type: error_type.clone(),
                    message: message.clone(),
                    traceback: traceback.clone(),
                },
                _ => TaskError {
                    error_type: "TaskExecutionError".to_string(),
                    message: err.to_string(),
                    traceback: None,
                },
            };

            let retry_count = num_retries;
            let max_retries = task.config().max_retries;
            let retry_for_errors = &task.config().retry_for_errors;
            let timed_out = matches!(interruption, Some(Interruption::TimedOut(_)));
            let should_retry = retry_count < max_retries
                && (!timed_out || resolved_task_config.retry_on_timeout)
                && (retry_for_errors.is_empty()
                    || retry_for_errors
                        .iter()
                        .any(|e| task_error.error_type.contains(e.as_str())));

            let exec_duration = exec_start.elapsed();
            deps.emitter.on_task_lifecycle(&TaskLifecycleEvent::failed(
                telemetry_context.clone(),
                task_error.error_type.clone(),
                exec_duration,
            ));

            if should_retry {
                let next_attempt = telemetry_context.attempt.checked_add(1).ok_or_else(|| {
                    RustvelloError::state_backend("execution attempt counter exhausted")
                })?;
                deps.lifecycle
                    .release_nontransactional_concurrency_slot(invocation_id)
                    .await?;
                let delay = resolved_task_config.retry_delay(retry_count, rand::random::<f64>());
                let written = deps
                    .lifecycle
                    .set_invocation_retry_after_with_context(
                        invocation_id,
                        worker_runner_id,
                        &inv_dto.task_id,
                        call_dto.serialized_arguments.0.clone(),
                        &resolved_task_config.queue,
                        resolved_task_config.priority,
                        delay,
                    )
                    .await;
                if discard_if_cancelled(deps, invocation_id, written).await? {
                    finish_cancelled(
                        deps,
                        invocation_id,
                        worker_runner_id,
                        &inv_dto.task_id,
                        None,
                        exec_duration,
                    )
                    .await;
                    return Ok(());
                }

                tracing::warn!(
                    delay_ms = delay.as_millis() as u64,
                    "Failed status:retry {}/{}",
                    retry_count + 1,
                    max_retries
                );
                deps.emitter
                    .on_task_lifecycle(&TaskLifecycleEvent::retry_scheduled(
                        telemetry_context,
                        next_attempt,
                    ));
            } else {
                let written = deps
                    .lifecycle
                    .set_invocation_exception_with_context(
                        invocation_id,
                        &task_error.error_type,
                        &task_error.message,
                        worker_runner_id,
                        &inv_dto.task_id,
                        call_dto.serialized_arguments.0.clone(),
                    )
                    .await;
                if discard_if_cancelled(deps, invocation_id, written).await? {
                    finish_cancelled(
                        deps,
                        invocation_id,
                        worker_runner_id,
                        &inv_dto.task_id,
                        None,
                        exec_duration,
                    )
                    .await;
                    return Ok(());
                }

                tracing::error!("Invocation status:failed permanently: {}", err);

                // Remove from CC index now that invocation is terminal
                if let Err(e) = deps
                    .lifecycle
                    .release_nontransactional_concurrency_slot(invocation_id)
                    .await
                {
                    tracing::warn!("Failed to remove from CC index: {}", e);
                }

                if let Some(ref ws) = deps.worker_states {
                    if let Ok(mut ws) = ws.lock() {
                        if let Some(state) = ws.get_mut(worker_runner_id) {
                            state.current_invocation = None;
                            state.current_task = None;
                            state.started_at = None;
                            state.last_result = Some(LastResult::Failed {
                                task_id: inv_dto.task_id.clone(),
                                error: err.to_string(),
                            });
                            state.invocations_completed += 1;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// `Ok(true)` when a lifecycle write failed only because a user cancelled the
/// invocation meanwhile: the attempt's outcome is discarded, not an error.
async fn discard_if_cancelled(
    deps: &ExecutionDeps,
    invocation_id: &InvocationId,
    written: RustvelloResult<()>,
) -> RustvelloResult<bool> {
    match written {
        Ok(()) => Ok(false),
        Err(
            error @ (RustvelloError::InvalidStatusTransition { .. }
            | RustvelloError::OwnershipViolation { .. }
            | RustvelloError::StatusRaceCondition { .. }),
        ) => {
            if deps.lifecycle.is_cancelled(invocation_id).await {
                tracing::info!(%invocation_id, "invocation was cancelled; late attempt outcome discarded");
                Ok(true)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

/// Bookkeeping for an attempt whose invocation was cancelled by a user.
async fn finish_cancelled(
    deps: &ExecutionDeps,
    invocation_id: &InvocationId,
    worker_runner_id: &RunnerId,
    task_id: &rustvello_proto::identifiers::TaskId,
    telemetry_context: Option<TaskAttemptContext>,
    duration: std::time::Duration,
) {
    if let Err(e) = deps
        .lifecycle
        .release_nontransactional_concurrency_slot(invocation_id)
        .await
    {
        tracing::warn!("Failed to remove from CC index: {}", e);
    }
    if let Some(context) = telemetry_context {
        deps.emitter.on_task_lifecycle(&TaskLifecycleEvent::failed(
            context,
            "InvocationCancelled",
            duration,
        ));
    }
    if let Some(ref ws) = deps.worker_states {
        if let Ok(mut ws) = ws.lock() {
            if let Some(state) = ws.get_mut(worker_runner_id) {
                state.current_invocation = None;
                state.current_task = None;
                state.started_at = None;
                state.last_result = Some(LastResult::Failed {
                    task_id: task_id.clone(),
                    error: "invocation cancelled".to_owned(),
                });
                state.invocations_completed += 1;
            }
        }
    }
}
