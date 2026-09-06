use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use rustvello_core::context::{
    clear_thread_invocation_context, clear_thread_runner_context, set_thread_invocation_context,
    set_thread_runner_context, InvocationContext, RunnerContext,
};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::task::DynTask;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::ExecutorKind;

use super::TaskExecutor;

#[derive(Clone)]
pub(crate) struct RayonExecutor {
    pool: Arc<rayon::ThreadPool>,
    permits: Arc<Semaphore>,
}

impl RayonExecutor {
    pub(crate) fn new(num_threads: usize) -> RustvelloResult<Self> {
        let num_threads = num_threads.max(1);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(|index| format!("rustvello-rayon-{index}"))
            .build()
            .map_err(|error| RustvelloError::Internal {
                message: format!("failed to build rayon pool: {error}"),
            })?;
        Ok(Self {
            pool: Arc::new(pool),
            permits: Arc::new(Semaphore::new(num_threads)),
        })
    }
}

#[async_trait]
impl TaskExecutor for RayonExecutor {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Rayon
    }

    async fn execute(
        &self,
        task: Arc<dyn DynTask>,
        args: SerializedArguments,
        invocation_context: InvocationContext,
        runner_context: RunnerContext,
    ) -> RustvelloResult<String> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|error| RustvelloError::Internal {
                message: format!("rayon executor closed: {error}"),
            })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pool.spawn(move || {
            let _permit = permit;
            set_thread_runner_context(runner_context);
            set_thread_invocation_context(invocation_context);
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task.execute(&args)));
            clear_thread_invocation_context();
            clear_thread_runner_context();
            let _ =
                sender.send(result.unwrap_or_else(|panic| {
                    Err(crate::runner::executor_common::unwrap_panic(panic))
                }));
        });
        receiver.await.map_err(|error| RustvelloError::Internal {
            message: format!("rayon executor response dropped: {error}"),
        })?
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

    #[tokio::test]
    async fn rayon_execution_respects_pool_capacity() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let task_id = TaskId::new("executor", "bounded_rayon");
        let mut registry = TaskRegistry::new();
        registry
            .register(TaskDefinition::new(
                task_id.clone(),
                TaskConfig::default(),
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
        let executor = RayonExecutor::new(2).unwrap();
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
            task_id: TaskId::new("executor", "bounded_rayon"),
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
            "RayonExecutorTest",
        )
    }
}
