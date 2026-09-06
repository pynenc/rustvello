use std::collections::HashMap;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::context::RunnerContext;
use rustvello_core::error::RustvelloResult;
use rustvello_core::middleware::TaskMiddleware;
use rustvello_core::observability::{
    CompositeEmitter, EventEmitter, EventLevel, NoopEmitter, WorkerState,
};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskRegistry;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{RunnerId, TaskLanguage};

use tracing::Instrument;

use super::control_plane::RunnerControlPlane;
use super::executor::TokioExecutor;
use super::PrevEmitterWrapper;
use crate::task_catalog::TaskCatalog;

mod execution;
mod worker;

/// A persistent worker pool runner using tokio tasks.
///
/// Spawns N persistent worker tasks that each independently poll the broker
/// for work. Each worker has a unique UUID identity (child of the main runner)
/// and its own WorkerState, following pynenc's hierarchical runner pattern.
///
/// # Worker Identity
///
/// The main runner has a `runner_id`. Each worker gets a unique `RunnerId`
/// (UUID) and a child `RunnerContext` with the main runner as parent.
/// All status transitions use the worker's runner_id, enabling per-worker
/// monitoring and attribution.
///
/// # Shutdown
///
/// `shutdown()` is safe to call from any thread or task via a cloned handle.
pub struct PersistentTokioRunner {
    /// Main runner identity (parent of all workers).
    runner_id: RunnerId,
    control_plane: RunnerControlPlane,
    executor: TokioExecutor,
    pub(crate) middlewares: Vec<Arc<dyn TaskMiddleware>>,
    pub(crate) emitter: Arc<dyn EventEmitter>,
    /// Per-worker state: maps worker RunnerId → WorkerState.
    pub(crate) worker_states: Arc<std::sync::Mutex<HashMap<RunnerId, WorkerState>>>,
    pub(crate) idle_sleep_ms: u64,
    pub(crate) num_workers: usize,
}

impl Clone for PersistentTokioRunner {
    fn clone(&self) -> Self {
        Self {
            runner_id: self.runner_id.clone(),
            control_plane: self.control_plane.clone(),
            executor: self.executor.clone(),
            middlewares: self.middlewares.clone(),
            emitter: Arc::clone(&self.emitter),
            worker_states: Arc::clone(&self.worker_states),
            idle_sleep_ms: self.idle_sleep_ms,
            num_workers: self.num_workers,
        }
    }
}

impl std::fmt::Debug for PersistentTokioRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentTokioRunner")
            .field("runner_id", &self.runner_id)
            .field("app_id", &self.control_plane.app_id)
            .field("num_workers", &self.num_workers)
            .finish_non_exhaustive()
    }
}

impl PersistentTokioRunner {
    pub fn new(
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        Self::new_for_language(
            TaskLanguage::Rust,
            app_id,
            config,
            broker,
            orchestrator,
            state_backend,
            task_registry,
            trigger_manager,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_for_language(
        runner_language: TaskLanguage,
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        Self::from_control_plane(RunnerControlPlane::new(
            runner_language,
            app_id,
            config,
            broker,
            orchestrator,
            state_backend,
            task_registry,
            trigger_manager,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_catalog(
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_catalog: Arc<TaskCatalog>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        Self::from_control_plane(RunnerControlPlane::with_catalog(
            TaskLanguage::Rust,
            app_id,
            config,
            broker,
            orchestrator,
            state_backend,
            task_catalog,
            trigger_manager,
        ))
    }

    fn from_control_plane(control_plane: RunnerControlPlane) -> Self {
        let num_workers = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        Self {
            runner_id: RunnerId::new(),
            control_plane,
            executor: TokioExecutor::new(num_workers),
            middlewares: Vec::new(),
            emitter: Arc::new(NoopEmitter),
            worker_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            idle_sleep_ms: 100,
            num_workers,
        }
    }

    pub fn new_python(
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        Self::new_for_language(
            TaskLanguage::Python,
            app_id,
            config,
            broker,
            orchestrator,
            state_backend,
            task_registry,
            trigger_manager,
        )
    }

    pub fn with_idle_sleep(mut self, ms: u64) -> Self {
        self.idle_sleep_ms = ms;
        self
    }

    pub fn with_num_workers(mut self, n: usize) -> Self {
        self.num_workers = n.max(1);
        self.executor = TokioExecutor::new(self.num_workers);
        self
    }

    pub fn num_workers(&self) -> usize {
        self.num_workers
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

    /// Get a snapshot of the current worker state for a specific worker.
    pub fn worker_state(&self) -> HashMap<RunnerId, WorkerState> {
        self.worker_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub async fn with_graceful_shutdown<F>(self, signal: F) -> RustvelloResult<()>
    where
        F: std::future::Future<Output = ()> + Send,
    {
        let control_plane = self.control_plane.clone();
        tokio::pin!(signal);
        let run_future = self.run();
        tokio::pin!(run_future);
        tokio::select! {
            result = &mut run_future => result,
            _ = &mut signal => {
                tracing::info!("Shutdown signal received, draining...");
                control_plane.request_shutdown();
                run_future.await
            }
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.control_plane.is_shutdown()
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        self.control_plane.wait_for_shutdown().await;
    }
}

#[async_trait::async_trait]
impl Runner for PersistentTokioRunner {
    fn runner_id(&self) -> &RunnerId {
        &self.runner_id
    }

    fn runner_cls(&self) -> &str {
        "PersistentTokioRunner"
    }

    fn max_parallel_slots(&self) -> usize {
        self.num_workers
    }

    fn active_worker_ids(&self) -> Vec<RunnerId> {
        self.worker_states
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
            cls = "PTR",
            app_id = %self.control_plane.app_id,
        );
        self.run_impl().instrument(runner_span).await
    }

    /// Run one invocation using the main runner_id (for backward compatibility).
    async fn run_one(&self) -> RustvelloResult<bool> {
        let ctx = RunnerContext::new_with_runtime(
            self.runner_id.clone(),
            Arc::clone(&self.control_plane.app_id),
            "PersistentTokioRunner",
            self.control_plane.runner_language,
            self.control_plane.executor_kind,
        );

        let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current_with_runtime(
            self.runner_id.to_string(),
            "PersistentTokioRunner",
            self.control_plane.runner_language,
            self.control_plane.executor_kind,
        );
        if let Err(e) = self
            .control_plane
            .state_backend
            .store_runner_context(&runner_ctx)
            .await
        {
            tracing::warn!("Failed to store runner context: {}", e);
        }
        let worker_runner_id = RunnerId::new();
        let worker_sb_ctx =
            runner_ctx.new_child(worker_runner_id.to_string(), "PersistentTokioWorker");
        if let Err(e) = self
            .control_plane
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
        let worker_ctx = ctx.new_child(worker_runner_id.clone());

        {
            let mut states = self
                .worker_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            states.insert(
                worker_runner_id.clone(),
                WorkerState::new(worker_runner_id.clone()),
            );
        }

        if let Err(e) = self.control_plane.heartbeat(&self.runner_id, true).await {
            tracing::warn!("run_one: main runner heartbeat failed: {}", e);
        }
        if let Err(e) = self.control_plane.heartbeat(&worker_runner_id, false).await {
            tracing::warn!("run_one: worker heartbeat failed: {}", e);
        }

        match self.control_plane.claim_next().await? {
            Some(inv_id) => {
                let result = self
                    .execute_invocation(&inv_id, &worker_runner_id, &worker_ctx)
                    .await;
                if let Ok(mut states) = self.worker_states.lock() {
                    states.remove(&worker_runner_id);
                }
                result?;
                Ok(true)
            }
            None => {
                if let Ok(mut states) = self.worker_states.lock() {
                    states.remove(&worker_runner_id);
                }
                Ok(false)
            }
        }
    }

    async fn shutdown(&self) -> RustvelloResult<()> {
        self.control_plane.request_shutdown();
        Ok(())
    }

    async fn heartbeat(&self) -> RustvelloResult<()> {
        self.control_plane.heartbeat(&self.runner_id, true).await?;
        tracing::trace!("runner:{} heartbeat", self.runner_id);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity, clippy::needless_borrows_for_generic_args)]
mod tests;
