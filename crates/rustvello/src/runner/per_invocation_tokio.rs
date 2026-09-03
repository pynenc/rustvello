use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello_core::broker::Broker;
use rustvello_core::context::{
    clear_thread_invocation_context, clear_thread_runner_context, set_thread_invocation_context,
    set_thread_runner_context, RunnerContext, INVOCATION_CTX, RUNNER_CTX,
};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::middleware::TaskMiddleware;
use rustvello_core::observability::{
    CompositeEmitter, EventEmitter, EventLevel, NoopEmitter, WorkerState,
};
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskRegistry;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};

use tokio::sync::{watch, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::executor_common::{
    execute_invocation_common, retrieve_next_invocation_with_cc, ExecutionDeps,
};
use super::PrevEmitterWrapper;

/// A runner that spawns a new tokio task per invocation.
///
/// Unlike [`PersistentTokioRunner`], this runner does not maintain persistent
/// worker tasks. Instead, it listens for work on the broker and spawns a fresh
/// tokio task for each invocation. A [`Semaphore`] limits concurrency.
///
/// Each spawned task gets a unique `RunnerId` (UUID) and a child `RunnerContext`
/// with the main runner as parent — same hierarchical identity model.
///
/// Best for: workloads where tasks are infrequent or long-running, and the
/// overhead of per-invocation task creation is negligible.
///
/// **Limitation:** This runner does not fire triggers after task completion.
/// Use [`PersistentTokioRunner`] if trigger evaluation is needed.
pub struct PerInvocationTokioRunner {
    runner_id: RunnerId,
    app_id: Arc<str>,
    config: AppConfig,
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn Orchestrator>,
    state_backend: Arc<dyn StateBackend>,
    task_registry: Arc<TaskRegistry>,
    middlewares: Vec<Arc<dyn TaskMiddleware>>,
    emitter: Arc<dyn EventEmitter>,
    /// Tracks active task runner_ids → WorkerState.
    active_tasks: Arc<std::sync::Mutex<HashMap<RunnerId, WorkerState>>>,
    shutdown_tx: Arc<watch::Sender<bool>>,
    max_concurrent: usize,
}

impl Clone for PerInvocationTokioRunner {
    fn clone(&self) -> Self {
        Self {
            runner_id: self.runner_id.clone(),
            app_id: Arc::clone(&self.app_id),
            config: self.config.clone(),
            broker: Arc::clone(&self.broker),
            orchestrator: Arc::clone(&self.orchestrator),
            state_backend: Arc::clone(&self.state_backend),
            task_registry: Arc::clone(&self.task_registry),
            middlewares: self.middlewares.clone(),
            emitter: Arc::clone(&self.emitter),
            active_tasks: Arc::clone(&self.active_tasks),
            shutdown_tx: Arc::clone(&self.shutdown_tx),
            max_concurrent: self.max_concurrent,
        }
    }
}

impl std::fmt::Debug for PerInvocationTokioRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerInvocationTokioRunner")
            .field("runner_id", &self.runner_id)
            .field("app_id", &self.app_id)
            .field("max_concurrent", &self.max_concurrent)
            .finish_non_exhaustive()
    }
}

impl PerInvocationTokioRunner {
    pub fn new(
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn Orchestrator>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let max_concurrent = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        Self {
            runner_id: RunnerId::new(),
            app_id: Arc::from(app_id),
            config,
            broker,
            orchestrator,
            state_backend,
            task_registry,
            middlewares: Vec::new(),
            emitter: Arc::new(NoopEmitter),
            active_tasks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown_tx: Arc::new(shutdown_tx),
            max_concurrent,
        }
    }

    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n.max(1);
        self
    }

    pub fn with_middleware(mut self, middleware: impl TaskMiddleware + 'static) -> Self {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    pub fn with_event_emitter(
        mut self,
        level: EventLevel,
        emitter: impl EventEmitter + 'static,
    ) -> Self {
        let mut composite = CompositeEmitter::new();
        let prev = std::mem::replace(&mut self.emitter, Arc::new(NoopEmitter));
        composite.add_sink(EventLevel::DistributedTracing, PrevEmitterWrapper(prev));
        composite.add_sink(level, emitter);
        self.emitter = Arc::new(composite);
        self
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    async fn wait_for_shutdown(&self) {
        let mut rx = self.shutdown_tx.subscribe();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
    }

    pub async fn with_graceful_shutdown<F>(self, signal: F) -> RustvelloResult<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let shutdown_tx = Arc::clone(&self.shutdown_tx);
        tokio::pin!(signal);
        let run_future = self.run();
        tokio::pin!(run_future);
        tokio::select! {
            result = &mut run_future => result,
            _ = &mut signal => {
                tracing::info!("Shutdown signal received, draining...");
                let _ = shutdown_tx.send(true);
                run_future.await
            }
        }
    }

    /// Execute a single invocation with a unique worker identity.
    async fn execute_invocation(
        &self,
        invocation_id: &InvocationId,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
    ) -> RustvelloResult<()> {
        let inv_span = tracing::info_span!(
            "invocation",
            invocation_id = %invocation_id,
            task_id = tracing::field::Empty,
        );
        self.execute_invocation_inner(invocation_id, worker_runner_id, worker_ctx)
            .instrument(inv_span)
            .await
    }

    async fn execute_invocation_inner(
        &self,
        invocation_id: &InvocationId,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
    ) -> RustvelloResult<()> {
        let deps = ExecutionDeps {
            orchestrator: Arc::clone(&self.orchestrator),
            state_backend: Arc::clone(&self.state_backend),
            broker: Arc::clone(&self.broker),
            emitter: Arc::clone(&self.emitter),
            middlewares: self.middlewares.clone(),
            task_registry: Arc::clone(&self.task_registry),
            trigger_manager: None,
            worker_states: None,
        };

        execute_invocation_common(
            &deps,
            invocation_id,
            worker_runner_id,
            "PerInvocation worker",
            worker_ctx,
            |task, args, inv_ctx, run_ctx| async move {
                let task_config = task.config();
                if task_config.blocking {
                    let task_clone = task;
                    let thread_ctx = run_ctx.clone();
                    let thread_inv_ctx = inv_ctx.clone();
                    INVOCATION_CTX
                        .scope(
                            inv_ctx,
                            RUNNER_CTX.scope(run_ctx, async {
                                tokio::task::spawn_blocking(move || {
                                    set_thread_runner_context(thread_ctx);
                                    set_thread_invocation_context(thread_inv_ctx);
                                    let result = std::panic::catch_unwind(
                                        std::panic::AssertUnwindSafe(|| task_clone.execute(&args)),
                                    );
                                    clear_thread_invocation_context();
                                    clear_thread_runner_context();
                                    match result {
                                        Ok(r) => r,
                                        Err(panic) => {
                                            Err(super::executor_common::unwrap_panic(panic))
                                        }
                                    }
                                })
                                .await
                                .map_err(|e| {
                                    RustvelloError::Internal {
                                        message: format!("spawn_blocking join: {e}"),
                                    }
                                })?
                            }),
                        )
                        .await
                } else {
                    let thread_ctx = run_ctx.clone();
                    let thread_inv = inv_ctx.clone();
                    INVOCATION_CTX
                        .scope(
                            inv_ctx,
                            RUNNER_CTX.scope(run_ctx, async {
                                set_thread_runner_context(thread_ctx);
                                set_thread_invocation_context(thread_inv);
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        task.execute(&args)
                                    }));
                                clear_thread_invocation_context();
                                clear_thread_runner_context();
                                match result {
                                    Ok(r) => r,
                                    Err(panic) => Err(super::executor_common::unwrap_panic(panic)),
                                }
                            }),
                        )
                        .await
                }
            },
        )
        .await
    }
}

#[async_trait::async_trait]
impl Runner for PerInvocationTokioRunner {
    fn runner_id(&self) -> &RunnerId {
        &self.runner_id
    }

    fn runner_cls(&self) -> &str {
        "PerInvocationTokioRunner"
    }

    fn max_parallel_slots(&self) -> usize {
        self.max_concurrent
    }

    fn active_worker_ids(&self) -> Vec<RunnerId> {
        self.active_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .cloned()
            .collect()
    }

    async fn run(&self) -> RustvelloResult<()> {
        let runner_span = tracing::info_span!(
            "runner",
            runner_id = %self.runner_id,
            cls = "PITR",
            app_id = %self.app_id,
        );

        async {
            tracing::info!(
                "PerInvocationTokioRunner starting (max_concurrent={}, app_id={}, pid={})",
                self.max_concurrent,
                self.app_id,
                std::process::id()
            );
            self.emitter.on_worker_started(&self.runner_id);

            let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current(
                self.runner_id.to_string(),
                "PerInvocationTokioRunner",
            );
            if let Err(e) = self.state_backend.store_runner_context(&runner_ctx).await {
                tracing::warn!("Failed to store runner context: {}", e);
            }

            self.heartbeat().await.ok();

            let main_ctx = RunnerContext::new(
                self.runner_id.clone(),
                Arc::clone(&self.app_id),
                "PerInvocationTokioRunner",
            );
            let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
            let mut handles = tokio::task::JoinSet::new();

            // Bridge shutdown_tx → CancellationToken for wait_for_work
            let cancel = CancellationToken::new();
            {
                let cancel_clone = cancel.clone();
                let mut rx = self.shutdown_tx.subscribe();
                tokio::spawn(async move {
                    if !*rx.borrow() {
                        let _ = rx.changed().await;
                    }
                    cancel_clone.cancel();
                });
            }

            let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_seconds);
            let mut last_heartbeat = Instant::now();

            while !self.is_shutdown() {
                // Periodic heartbeat
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    self.heartbeat().await.ok();
                    last_heartbeat = Instant::now();
                }

                // Wait for a semaphore permit (limits concurrency)
                let permit = tokio::select! {
                    p = Arc::<tokio::sync::Semaphore>::clone(&semaphore).acquire_owned() => {
                        match p {
                            Ok(permit) => permit,
                            Err(_) => break, // Semaphore closed
                        }
                    }
                    _ = self.wait_for_shutdown() => break,
                };

                // Try to get work (blocking-priority first, then broker FIFO)
                let inv_id = match retrieve_next_invocation_with_cc(
                    &*self.orchestrator,
                    &*self.broker,
                    Some(&*self.state_backend),
                    Some(&*self.task_registry),
                    &self.config,
                )
                .await?
                {
                    Some(id) => id,
                    None => {
                        drop(permit);
                        if !self.broker.wait_for_work(&cancel).await {
                            break;
                        }
                        continue;
                    }
                };

                // Spawn a tokio task for this invocation
                let worker_runner_id = RunnerId::new();
                let worker_ctx = main_ctx.new_child(worker_runner_id.clone());
                let runner = self.clone();
                let w_id = worker_runner_id.clone();

                // Store worker context in state backend for monitoring
                let worker_sb_ctx =
                    runner_ctx.new_child(worker_runner_id.to_string(), "PerInvocationWorker");
                if let Err(e) = self
                    .state_backend
                    .store_runner_context(&worker_sb_ctx)
                    .await
                {
                    tracing::warn!(
                        "Failed to store worker context for worker:{}: {}",
                        worker_runner_id,
                        e
                    );
                }
                if let Ok(mut tasks) = self.active_tasks.lock() {
                    tasks.insert(
                        worker_runner_id.clone(),
                        WorkerState::new(worker_runner_id.clone()),
                    );
                }

                let worker_span = tracing::info_span!(
                    "worker",
                    worker_id = %w_id,
                );
                handles.spawn(
                    async move {
                        let result = runner.execute_invocation(&inv_id, &w_id, &worker_ctx).await;
                        // Unregister active task
                        if let Ok(mut tasks) = runner.active_tasks.lock() {
                            tasks.remove(&w_id);
                        }
                        drop(permit); // Release semaphore
                        result
                    }
                    .instrument(worker_span),
                );

                // Reap completed tasks
                while let Some(result) = handles.try_join_next() {
                    match result {
                        Ok(Err(e)) => tracing::error!("Task error: {}", e),
                        Err(e) => tracing::error!("Task panicked: {}", e),
                        Ok(Ok(())) => {}
                    }
                }
            }

            // Wait for in-flight tasks to complete
            while let Some(result) = handles.join_next().await {
                match result {
                    Ok(Err(e)) => tracing::error!("Task error: {}", e),
                    Err(e) => tracing::error!("Task panicked: {}", e),
                    Ok(Ok(())) => {}
                }
            }

            tracing::info!("PerInvocationTokioRunner shutting down");
            self.emitter.on_worker_shutdown(&self.runner_id);
            Ok(())
        }
        .instrument(runner_span)
        .await
    }

    async fn run_one(&self) -> RustvelloResult<bool> {
        let main_ctx = RunnerContext::new(
            self.runner_id.clone(),
            Arc::clone(&self.app_id),
            "PerInvocationTokioRunner",
        );
        let worker_runner_id = RunnerId::new();
        let worker_ctx = main_ctx.new_child(worker_runner_id.clone());

        // Store main + worker contexts for monitoring
        let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current(
            self.runner_id.to_string(),
            "PerInvocationTokioRunner",
        );
        if let Err(e) = self.state_backend.store_runner_context(&runner_ctx).await {
            tracing::warn!("Failed to store runner context: {}", e);
        }
        let worker_sb_ctx =
            runner_ctx.new_child(worker_runner_id.to_string(), "PerInvocationWorker");
        if let Err(e) = self
            .state_backend
            .store_runner_context(&worker_sb_ctx)
            .await
        {
            tracing::warn!(
                "Failed to store worker context for worker:{}: {}",
                worker_runner_id,
                e
            );
        }

        match retrieve_next_invocation_with_cc(
            &*self.orchestrator,
            &*self.broker,
            Some(&*self.state_backend),
            Some(&*self.task_registry),
            &self.config,
        )
        .await?
        {
            Some(inv_id) => {
                self.execute_invocation(&inv_id, &worker_runner_id, &worker_ctx)
                    .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn shutdown(&self) -> RustvelloResult<()> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    async fn heartbeat(&self) -> RustvelloResult<()> {
        self.orchestrator
            .register_heartbeat(&self.runner_id, true)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
mod tests {
    use super::*;
    use rustvello_core::runner::Runner;
    use rustvello_core::task::TaskDefinition;
    use rustvello_proto::call::SerializedArguments;
    use rustvello_proto::config::TaskConfig;
    use rustvello_proto::identifiers::TaskId;
    use rustvello_proto::invocation::InvocationDTO;
    use rustvello_proto::status::InvocationStatus;

    fn make_runner() -> (
        PerInvocationTokioRunner,
        Arc<dyn Orchestrator>,
        Arc<dyn StateBackend>,
        Arc<dyn Broker>,
    ) {
        let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
        let orchestrator: Arc<dyn Orchestrator> =
            Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
        let state_backend: Arc<dyn StateBackend> =
            Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

        let mut registry = TaskRegistry::new();
        registry
            .register(TaskDefinition::new(
                TaskId::new("test", "double"),
                TaskConfig::default(),
                Arc::new(|args_json: String| {
                    let args: std::collections::BTreeMap<String, String> =
                        serde_json::from_str(&args_json).map_err(|e| {
                            RustvelloError::Serialization {
                                message: e.to_string(),
                            }
                        })?;
                    let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                    serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })
                }),
            ))
            .unwrap();

        let runner = PerInvocationTokioRunner::new(
            "test-app".to_string(),
            AppConfig::default(),
            Arc::clone(&broker),
            Arc::clone(&orchestrator),
            Arc::clone(&state_backend),
            Arc::new(registry),
        );

        (runner, orchestrator, state_backend, broker)
    }

    #[tokio::test]
    async fn test_run_one_no_work() {
        let (runner, _, _, _) = make_runner();
        let did_work = runner.run_one().await.unwrap();
        assert!(!did_work);
    }

    #[tokio::test]
    async fn test_full_invocation_cycle() {
        let (runner, orchestrator, state_backend, broker) = make_runner();

        let task_id = TaskId::new("test", "double");
        let mut args = SerializedArguments::new();
        args.insert("x", "21");
        let call = rustvello_proto::call::CallDTO::new(task_id.clone(), args);

        let inv_id = orchestrator.register_invocation(&call).await.unwrap();
        let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
        state_backend
            .upsert_invocation(&inv_dto, &call)
            .await
            .unwrap();
        broker.route_invocation(&inv_id).await.unwrap();

        let did_work = runner.run_one().await.unwrap();
        assert!(did_work);

        let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
        assert_eq!(status.status, InvocationStatus::Success);

        let result = state_backend.get_result(&inv_id).await.unwrap();
        assert_eq!(result, Some("42".to_string()));
    }

    #[test]
    fn test_runner_cls() {
        let (runner, _, _, _) = make_runner();
        assert_eq!(runner.runner_cls(), "PerInvocationTokioRunner");
    }

    #[test]
    fn test_max_parallel_slots() {
        let (runner, _, _, _) = make_runner();
        let runner = runner.with_max_concurrent(16);
        assert_eq!(runner.max_parallel_slots(), 16);
    }
}
