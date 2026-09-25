//! Native execution of asynchronous task bodies on the runner's Tokio runtime.

use std::sync::Arc;

use tokio::task::{AbortHandle, JoinHandle};
use tracing::Instrument;

use rustvello_core::context::{InvocationContext, RunnerContext, INVOCATION_CTX, RUNNER_CTX};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::observability::in_w3c_trace_context;
use rustvello_core::task::DynTask;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::InvocationId;

/// Error type recorded when an async task body is cancelled before finishing.
pub(crate) const TASK_CANCELLED: &str = "TaskCancelled";

/// Aborts the spawned task body if the awaiting executor future is dropped.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Await an async task body as its own Tokio task.
///
/// The body holds no blocking thread; the caller's permit (one worker slot)
/// bounds concurrency. The invocation and runner contexts are task-locals and
/// the W3C context is attached on every poll, so all of them survive `.await`
/// points without touching the thread-local fallbacks that other bodies polled
/// on the same thread would observe. The current tracing span is carried in.
///
/// Cancellation safety: a panic becomes a task error; an aborted body becomes a
/// `TaskCancelled` task error, so the invocation still reaches a terminal or
/// retry state; and dropping this future aborts the body instead of leaving it
/// running detached from its invocation.
pub(crate) async fn execute_native_async(
    task: Arc<dyn DynTask>,
    args: SerializedArguments,
    invocation_context: InvocationContext,
    runner_context: RunnerContext,
) -> RustvelloResult<String> {
    let invocation_id = invocation_context.invocation_id.clone();
    let body = in_w3c_trace_context(&invocation_context.trace_context, task.execute_async(args));
    let body = INVOCATION_CTX
        .scope(invocation_context, RUNNER_CTX.scope(runner_context, body))
        .instrument(tracing::Span::current());
    let handle = tokio::spawn(body);
    let _abort_on_drop = AbortOnDrop(handle.abort_handle());
    join_task_body(handle, &invocation_id).await
}

/// Map the outcome of a spawned body onto the invocation's task result.
///
/// Whoever aborts the body (runtime shutdown today, a deadline or cancel
/// request later) gets a `TaskCancelled` task error, never a hang.
async fn join_task_body(
    handle: JoinHandle<RustvelloResult<String>>,
    invocation_id: &InvocationId,
) -> RustvelloResult<String> {
    match handle.await {
        Ok(result) => result,
        Err(error) if error.is_panic() => Err(crate::runner::executor_common::unwrap_panic(
            error.into_panic(),
        )),
        Err(error) => Err(RustvelloError::TaskExecution {
            error_type: TASK_CANCELLED.to_owned(),
            message: format!(
                "async task body for invocation {invocation_id} was cancelled: {error}"
            ),
            traceback: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn aborted_body_is_a_cancelled_task_error() {
        let handle = tokio::spawn(std::future::pending::<RustvelloResult<String>>());
        handle.abort();
        let error = join_task_body(handle, &InvocationId::new())
            .await
            .unwrap_err();
        assert!(
            matches!(error, RustvelloError::TaskExecution { ref error_type, .. } if error_type == TASK_CANCELLED),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn panicking_body_is_a_task_error() {
        let handle = tokio::spawn(async {
            tokio::task::yield_now().await;
            panic!("boom");
        });
        let error = join_task_body(handle, &InvocationId::new())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("task panicked: boom"), "{error}");
    }

    #[tokio::test]
    async fn dropping_the_executor_future_aborts_the_body() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct SetOnDrop(Arc<AtomicBool>);
        impl Drop for SetOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let guard = SetOnDrop(Arc::clone(&dropped));
        let body = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<RustvelloResult<String>>().await
        });
        let waiter = {
            let abort = AbortOnDrop(body.abort_handle());
            async move {
                let _abort = abort;
                join_task_body(body, &InvocationId::new()).await
            }
        };
        // Poll once, then drop the waiting future as a cancelled worker would.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), waiter)
                .await
                .is_err()
        );
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::SeqCst));
    }
}
