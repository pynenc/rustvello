use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use rustvello_core::context::{
    clear_thread_invocation_context, clear_thread_runner_context, set_thread_invocation_context,
    set_thread_runner_context, InvocationContext, RunnerContext, INVOCATION_CTX, RUNNER_CTX,
};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::task::DynTask;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::ExecutorKind;

use super::TaskExecutor;

#[derive(Clone)]
pub(crate) struct TokioExecutor {
    blocking_permits: Arc<Semaphore>,
}

impl TokioExecutor {
    pub(crate) fn new(max_blocking: usize) -> Self {
        Self {
            blocking_permits: Arc::new(Semaphore::new(max_blocking.max(1))),
        }
    }

    fn should_spawn_blocking(&self, task: &dyn DynTask) -> bool {
        task.config().blocking
    }
}

#[async_trait]
impl TaskExecutor for TokioExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tokio
    }

    async fn execute(
        &self,
        task: Arc<dyn DynTask>,
        args: SerializedArguments,
        invocation_context: InvocationContext,
        runner_context: RunnerContext,
    ) -> RustvelloResult<String> {
        if self.should_spawn_blocking(task.as_ref()) {
            let permit = Arc::clone(&self.blocking_permits)
                .acquire_owned()
                .await
                .map_err(|error| RustvelloError::Internal {
                    message: format!("blocking executor closed: {error}"),
                })?;
            let thread_runner = runner_context.clone();
            let thread_invocation = invocation_context.clone();
            return INVOCATION_CTX
                .scope(
                    invocation_context,
                    RUNNER_CTX.scope(runner_context, async move {
                        tokio::task::spawn_blocking(move || {
                            let _permit = permit;
                            set_thread_runner_context(thread_runner);
                            set_thread_invocation_context(thread_invocation);
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    task.execute(&args)
                                }));
                            clear_thread_invocation_context();
                            clear_thread_runner_context();
                            result.unwrap_or_else(|panic| {
                                Err(crate::runner::executor_common::unwrap_panic(panic))
                            })
                        })
                        .await
                        .map_err(|error| RustvelloError::Internal {
                            message: format!("spawn_blocking join: {error}"),
                        })?
                    }),
                )
                .await;
        }

        let thread_runner = runner_context.clone();
        let thread_invocation = invocation_context.clone();
        INVOCATION_CTX
            .scope(
                invocation_context,
                RUNNER_CTX.scope(runner_context, async move {
                    set_thread_runner_context(thread_runner);
                    set_thread_invocation_context(thread_invocation);
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        task.execute(&args)
                    }));
                    clear_thread_invocation_context();
                    clear_thread_runner_context();
                    result.unwrap_or_else(|panic| {
                        Err(crate::runner::executor_common::unwrap_panic(panic))
                    })
                }),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use rustvello_core::task::{TaskDefinition, TaskRegistry};
    use rustvello_proto::config::TaskConfig;
    use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_execution_respects_its_own_permit_limit() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let task_id = TaskId::new("executor", "bounded_blocking");
        let mut registry = TaskRegistry::new();
        let mut config = TaskConfig::default();
        config.blocking = true;
        registry
            .register(TaskDefinition::new(
                task_id.clone(),
                config,
                Arc::new({
                    let active = Arc::clone(&active);
                    let peak = Arc::clone(&peak);
                    move |_| {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(30));
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok("null".to_owned())
                    }
                }),
            ))
            .unwrap();
        let task = registry.get_dyn(&task_id).unwrap();
        let executor = TokioExecutor::new(2);
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..6 {
            let executor = executor.clone();
            let task = Arc::clone(&task);
            tasks.spawn(async move {
                executor
                    .execute(
                        task,
                        SerializedArguments::new(),
                        invocation_context(),
                        runner_context(),
                    )
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    fn invocation_context() -> InvocationContext {
        InvocationContext {
            invocation_id: InvocationId::new(),
            task_id: TaskId::new("executor", "bounded_blocking"),
            workflow: None,
            is_workflow_defining: false,
            state_backend: None,
            parent_invocation_id: None,
            num_retries: 0,
        }
    }

    fn runner_context() -> RunnerContext {
        RunnerContext::new(
            RunnerId::new(),
            Arc::from("executor-test"),
            "TokioExecutorTest",
        )
    }
}
