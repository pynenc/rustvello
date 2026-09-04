//! Shared execution logic for all runner types.
//!
//! Every runner follows the same lifecycle for executing an invocation:
//! claim → running → load data → middleware before → **execute** → middleware after → result handling.
//! The only variation is the actual task execution mechanism (async, spawn_blocking, rayon pool).
//! This module extracts that shared flow into [`execute_invocation_common`].

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rustvello_core::broker::Broker;
use rustvello_core::context::{InvocationContext, RunnerContext};
use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::middleware::TaskMiddleware;
use rustvello_core::observability::{EventEmitter, LastResult, WorkerState};
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{DynTask, TaskRegistry};
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::{AppConfig, QueueSelectionStrategy};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus, InvocationStatusRecord};
use rustvello_proto::trigger::{ExceptionContext, ResultContext, StatusContext};

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
    pub orchestrator: Arc<dyn Orchestrator>,
    pub state_backend: Arc<dyn StateBackend>,
    pub broker: Arc<dyn Broker>,
    pub emitter: Arc<dyn EventEmitter>,
    pub middlewares: Vec<Arc<dyn TaskMiddleware>>,
    pub task_registry: Arc<TaskRegistry>,
    pub trigger_manager: Option<Arc<TriggerManager>>,
    pub worker_states: Option<Arc<std::sync::Mutex<HashMap<RunnerId, WorkerState>>>>,
}

/// Maximum number of blocking invocations to consider per dispatch cycle.
const MAX_BLOCKING_CANDIDATES: usize = 8;

/// Maximum number of broker retrievals to attempt before giving up when
/// all candidates fail CC checks.
const MAX_CC_RETRIES: usize = 8;
static NEXT_QUEUE_INDEX: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn queue_names_for_retrieval(config: &AppConfig) -> Vec<String> {
    use rand::seq::SliceRandom;

    let mut queues = if config.runner_queues.is_empty() {
        config.broker_queues.clone()
    } else {
        config.runner_queues.clone()
    };
    match config.queue_selection_strategy {
        QueueSelectionStrategy::Ordered => {}
        QueueSelectionStrategy::RoundRobin if !queues.is_empty() => {
            let start = NEXT_QUEUE_INDEX.fetch_add(1, Ordering::Relaxed) % queues.len();
            queues.rotate_left(start);
        }
        QueueSelectionStrategy::Random => queues.shuffle(&mut rand::thread_rng()),
        _ => {}
    }
    queues
}

/// Retrieve the next invocation to execute using orchestrator-mediated dispatch.
///
/// When `state_backend` and `task_registry` are provided, performs CC checks
/// on each candidate invocation before returning it. Invocations that fail
/// CC checks are rerouted (if `reroute_on_cc` is set) or permanently rejected.
pub(crate) async fn retrieve_next_invocation_with_cc(
    orchestrator: &dyn Orchestrator,
    broker: &dyn Broker,
    state_backend: Option<&dyn StateBackend>,
    task_registry: Option<&TaskRegistry>,
    config: &AppConfig,
) -> RustvelloResult<Option<InvocationId>> {
    // Step 1: Check for blocking-priority invocations (those with waiters).
    match orchestrator
        .get_blocking_invocations(MAX_BLOCKING_CANDIDATES)
        .await
    {
        Ok(blocking) if !blocking.is_empty() => {
            for inv_id in &blocking {
                if check_cc_for_candidate(
                    orchestrator,
                    broker,
                    state_backend,
                    task_registry,
                    inv_id,
                )
                .await?
                {
                    tracing::debug!("Prioritizing blocking invocation {} (has waiters)", inv_id);
                    return Ok(Some(inv_id.clone()));
                }
            }
        }
        Ok(_) => {} // No blocking invocations, fall through to broker
        Err(e) => {
            tracing::warn!(
                "get_blocking_invocations failed, falling back to broker: {}",
                e
            );
        }
    }

    // Step 2: Fall back to broker FIFO with CC filtering
    for _ in 0..MAX_CC_RETRIES {
        let mut candidate = None;
        for queue_name in queue_names_for_retrieval(config) {
            if let Some(invocation_id) = broker
                .retrieve_invocation_from_queue(&queue_name, None)
                .await?
            {
                candidate = Some(invocation_id);
                break;
            }
        }
        match candidate {
            Some(inv_id) => {
                if check_cc_for_candidate(
                    orchestrator,
                    broker,
                    state_backend,
                    task_registry,
                    &inv_id,
                )
                .await?
                {
                    return Ok(Some(inv_id));
                }
                // CC check failed, the candidate was rerouted/rejected;
                // try the next one from the broker.
            }
            None => return Ok(None),
        }
    }

    Ok(None)
}

/// Check concurrency control for a candidate invocation.
///
/// Returns `true` if the invocation is clear to execute.
/// Returns `false` if it was denied and has been rerouted or rejected.
///
/// When `state_backend`/`task_registry` are `None`, skips the CC check
/// (backwards-compatible: always returns true).
async fn check_cc_for_candidate(
    orchestrator: &dyn Orchestrator,
    broker: &dyn Broker,
    state_backend: Option<&dyn StateBackend>,
    task_registry: Option<&TaskRegistry>,
    invocation_id: &InvocationId,
) -> RustvelloResult<bool> {
    let (Some(sb), Some(tr)) = (state_backend, task_registry) else {
        return Ok(true); // No CC deps available, skip check
    };

    // Load the invocation to get its task config and CC args
    let inv_dto = match sb.get_invocation(invocation_id).await {
        Ok(dto) => dto,
        Err(_) => return Ok(true), // Can't load → let execute_invocation_common handle it
    };

    let task = match tr.get_dyn(&inv_dto.task_id) {
        Some(t) => t,
        None => return Ok(true), // Unknown task → let execute_invocation_common handle it
    };

    let config = task.config();
    if config.concurrency_control == ConcurrencyControlType::Unlimited {
        return Ok(true); // No CC restrictions
    }

    // Compute CC args from the call data
    let call_dto = match sb.get_call(&inv_dto.call_id).await {
        Ok(c) => c,
        Err(_) => return Ok(true),
    };

    let cc_args = compute_cc_args(config, &call_dto.serialized_arguments);

    // Reserve a slot before claiming the invocation. Backends implement this
    // as one atomic check-and-index operation.
    if orchestrator
        .try_acquire_concurrency_slot(invocation_id, &inv_dto.task_id, config, cc_args.as_ref())
        .await?
    {
        return Ok(true); // CC check passed
    }

    // CC check failed — handle based on task config
    tracing::debug!(
        "Concurrency control denied invocation {} for task {}",
        invocation_id,
        inv_dto.task_id
    );

    if config.reroute_on_cc {
        // Mark as ConcurrencyControlled and reroute back to broker
        match orchestrator
            .set_invocation_status(invocation_id, InvocationStatus::ConcurrencyControlled, None)
            .await
        {
            Ok(_) => {
                // Transition to Rerouted, then re-enqueue
                orchestrator
                    .set_invocation_status(invocation_id, InvocationStatus::Rerouted, None)
                    .await?;
                broker
                    .route_invocation_with_options(
                        invocation_id,
                        Some(&inv_dto.task_id),
                        &config.queue,
                        config.priority,
                    )
                    .await?;
                tracing::info!(
                    "Rerouted CC-denied invocation {} back to broker",
                    invocation_id
                );
            }
            Err(RustvelloError::InvalidStatusTransition { .. }) => {
                // Already transitioned by another worker — skip
            }
            Err(e) => return Err(e),
        }
    } else {
        // Permanently reject
        match orchestrator
            .set_invocation_status(
                invocation_id,
                InvocationStatus::ConcurrencyControlledFinal,
                None,
            )
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "Permanently rejected CC-denied invocation {}",
                    invocation_id
                );
            }
            Err(RustvelloError::InvalidStatusTransition { .. }) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(false)
}

/// Compute the CC arguments for an invocation based on its task config.
pub(crate) fn compute_cc_args(
    config: &rustvello_proto::config::TaskConfig,
    args: &SerializedArguments,
) -> Option<SerializedArguments> {
    crate::task_config::concurrency_arguments(
        config.concurrency_control,
        &config.key_arguments,
        args,
    )
}

/// Execute an invocation using the shared lifecycle.
///
/// The `execute_task` closure receives the task, serialized arguments,
/// invocation context, and runner context, and should run the actual task
/// using whatever mechanism the runner provides (direct async, spawn_blocking,
/// rayon pool, etc.).
pub(crate) async fn execute_invocation_common<F, Fut>(
    deps: &ExecutionDeps,
    invocation_id: &InvocationId,
    worker_runner_id: &RunnerId,
    runner_label: &str,
    worker_ctx: &RunnerContext,
    execute_task: F,
) -> RustvelloResult<()>
where
    F: FnOnce(Arc<dyn DynTask>, SerializedArguments, InvocationContext, RunnerContext) -> Fut,
    Fut: Future<Output = RustvelloResult<String>> + Send,
{
    // --- 1. Claim ownership ---
    match deps
        .orchestrator
        .set_invocation_status(
            invocation_id,
            InvocationStatus::Pending,
            Some(worker_runner_id),
        )
        .await
    {
        Ok(_) => {
            deps.state_backend
                .add_history(
                    &InvocationHistory::new(
                        invocation_id.clone(),
                        InvocationStatusRecord::new(
                            InvocationStatus::Pending,
                            Some(worker_runner_id.clone()),
                        ),
                        Some(format!("{runner_label} claimed invocation")),
                    )
                    .with_runner(worker_runner_id.clone()),
                )
                .await?;
        }
        Err(RustvelloError::InvalidStatusTransition {
            from_status,
            to_status,
            ..
        }) => {
            tracing::warn!(
                "Already claimed (race): from_status:{} to_status:{} skipped",
                from_status,
                to_status
            );
            deps.orchestrator
                .remove_from_concurrency_index(invocation_id)
                .await?;
            return Ok(());
        }
        Err(RustvelloError::OwnershipViolation { .. }) => {
            tracing::warn!("Already owned by another runner");
            deps.orchestrator
                .remove_from_concurrency_index(invocation_id)
                .await?;
            return Ok(());
        }
        Err(e) => {
            let _ = deps
                .orchestrator
                .remove_from_concurrency_index(invocation_id)
                .await;
            return Err(e);
        }
    }

    // --- 2. Transition to Running ---
    deps.orchestrator
        .set_invocation_status(
            invocation_id,
            InvocationStatus::Running,
            Some(worker_runner_id),
        )
        .await?;

    deps.state_backend
        .add_history(
            &InvocationHistory::new(
                invocation_id.clone(),
                InvocationStatusRecord::new(
                    InvocationStatus::Running,
                    Some(worker_runner_id.clone()),
                ),
                Some(format!("{runner_label} executing")),
            )
            .with_runner(worker_runner_id.clone()),
        )
        .await?;

    // --- 3. Load invocation data ---
    let inv_dto = deps.state_backend.get_invocation(invocation_id).await?;
    let call_dto = deps.state_backend.get_call(&inv_dto.call_id).await?;

    tracing::Span::current().record("task_id", tracing::field::display(&inv_dto.task_id));

    let task = deps
        .task_registry
        .get_dyn(&inv_dto.task_id)
        .ok_or_else(|| RustvelloError::TaskNotRegistered {
            task_id: inv_dto.task_id.clone(),
        })?;

    // --- 4. Build invocation context ---
    let retry_history = deps.state_backend.get_history(invocation_id).await?;
    let num_retries = retry_history
        .iter()
        .filter(|h| h.status_record.status == InvocationStatus::Retry)
        .count() as u32;

    let inv_ctx = InvocationContext {
        invocation_id: invocation_id.clone(),
        task_id: inv_dto.task_id.clone(),
        workflow: inv_dto.workflow.clone(),
        is_workflow_defining: task.config().is_workflow_task,
        state_backend: Some(Arc::clone(&deps.state_backend)),
        parent_invocation_id: inv_dto.parent_invocation_id.clone(),
        num_retries,
    };
    let run_ctx = worker_ctx.clone();

    // --- 5. Pre-execution bookkeeping ---
    deps.emitter
        .on_task_started(&inv_dto.task_id, invocation_id);

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

    // --- 6. Execute the task (runner-specific) ---
    let exec_result = execute_task(
        Arc::clone(&task),
        call_dto.serialized_arguments.clone(),
        inv_ctx,
        run_ctx,
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
    match exec_result {
        Ok(result) => {
            deps.state_backend
                .store_result(invocation_id, &result)
                .await?;

            deps.orchestrator
                .set_invocation_status(
                    invocation_id,
                    InvocationStatus::Success,
                    Some(worker_runner_id),
                )
                .await?;

            deps.state_backend
                .add_history(
                    &InvocationHistory::new(
                        invocation_id.clone(),
                        InvocationStatusRecord::new(
                            InvocationStatus::Success,
                            Some(worker_runner_id.clone()),
                        ),
                        None,
                    )
                    .with_runner(worker_runner_id.clone()),
                )
                .await?;

            deps.orchestrator.release_waiters(invocation_id).await?;

            // Remove from CC index now that invocation is complete
            if let Err(e) = deps
                .orchestrator
                .remove_from_concurrency_index(invocation_id)
                .await
            {
                tracing::warn!("Failed to remove from CC index: {}", e);
            }

            if let Some(ref tm) = deps.trigger_manager {
                let result_ctx = ResultContext {
                    invocation_id: invocation_id.clone(),
                    task_id: inv_dto.task_id.clone(),
                    result: serde_json::Value::String(result.clone()),
                    arguments: std::collections::BTreeMap::new(),
                };
                if let Err(e) = tm.report_result(&result_ctx).await {
                    tracing::warn!("Trigger report_result failed: {}", e);
                }
                let status_ctx = StatusContext {
                    invocation_id: invocation_id.clone(),
                    task_id: inv_dto.task_id.clone(),
                    status: InvocationStatus::Success,
                    arguments: std::collections::BTreeMap::new(),
                };
                if let Err(e) = tm.report_status_change(&status_ctx).await {
                    tracing::warn!("Trigger report_status_change failed: {}", e);
                }
            }

            tracing::info!("Invocation completed status:success");

            let exec_duration = exec_start.elapsed();
            deps.emitter
                .on_task_succeeded(&inv_dto.task_id, invocation_id, exec_duration);

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
            let should_retry = retry_count < max_retries
                && (retry_for_errors.is_empty()
                    || retry_for_errors
                        .iter()
                        .any(|e| task_error.error_type.contains(e.as_str())));

            if should_retry {
                deps.orchestrator
                    .remove_from_concurrency_index(invocation_id)
                    .await?;
                deps.orchestrator
                    .set_invocation_status(
                        invocation_id,
                        InvocationStatus::Retry,
                        Some(worker_runner_id),
                    )
                    .await?;

                deps.broker
                    .route_invocation_with_options(
                        invocation_id,
                        Some(&inv_dto.task_id),
                        &task.config().queue,
                        task.config().priority,
                    )
                    .await?;

                deps.state_backend
                    .add_history(
                        &InvocationHistory::new(
                            invocation_id.clone(),
                            InvocationStatusRecord::new(
                                InvocationStatus::Retry,
                                Some(worker_runner_id.clone()),
                            ),
                            Some(format!(
                                "Retry {}/{}: {}",
                                retry_count + 1,
                                max_retries,
                                err
                            )),
                        )
                        .with_runner(worker_runner_id.clone()),
                    )
                    .await?;

                tracing::warn!("Failed status:retry {}/{}", retry_count + 1, max_retries);
                deps.emitter
                    .on_task_retried(&inv_dto.task_id, invocation_id, retry_count + 1);
            } else {
                deps.state_backend
                    .store_error(invocation_id, &task_error)
                    .await?;

                deps.orchestrator
                    .set_invocation_status(
                        invocation_id,
                        InvocationStatus::Failed,
                        Some(worker_runner_id),
                    )
                    .await?;

                deps.state_backend
                    .add_history(
                        &InvocationHistory::new(
                            invocation_id.clone(),
                            InvocationStatusRecord::new(
                                InvocationStatus::Failed,
                                Some(worker_runner_id.clone()),
                            ),
                            Some(format!("Failed: {}", err)),
                        )
                        .with_runner(worker_runner_id.clone()),
                    )
                    .await?;

                if let Some(ref tm) = deps.trigger_manager {
                    let exc_ctx = ExceptionContext {
                        invocation_id: invocation_id.clone(),
                        task_id: inv_dto.task_id.clone(),
                        error_type: task_error.error_type.clone(),
                        error_message: task_error.message.clone(),
                        arguments: std::collections::BTreeMap::new(),
                    };
                    if let Err(e) = tm.report_failure(&exc_ctx).await {
                        tracing::warn!("Trigger report_failure failed: {}", e);
                    }
                    let status_ctx = StatusContext {
                        invocation_id: invocation_id.clone(),
                        task_id: inv_dto.task_id.clone(),
                        status: InvocationStatus::Failed,
                        arguments: std::collections::BTreeMap::new(),
                    };
                    if let Err(e) = tm.report_status_change(&status_ctx).await {
                        tracing::warn!("Trigger report_status_change failed: {}", e);
                    }
                }

                tracing::error!("Invocation status:failed permanently: {}", err);

                // Remove from CC index now that invocation is terminal
                if let Err(e) = deps
                    .orchestrator
                    .remove_from_concurrency_index(invocation_id)
                    .await
                {
                    tracing::warn!("Failed to remove from CC index: {}", e);
                }

                let exec_duration = exec_start.elapsed();
                deps.emitter.on_task_failed(
                    &inv_dto.task_id,
                    invocation_id,
                    &err.to_string(),
                    exec_duration,
                );

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
