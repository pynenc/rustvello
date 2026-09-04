use std::collections::HashMap;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::context::RunnerContext;
use rustvello_core::error::RustvelloResult;
use rustvello_core::middleware::TaskMiddleware;
use rustvello_core::observability::{
    CompositeEmitter, EventEmitter, EventLevel, NoopEmitter, WorkerState,
};
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskRegistry;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::RunnerId;

use tokio::sync::watch;
use tracing::Instrument;

use super::executor_common::retrieve_next_invocation_with_cc;
use super::PrevEmitterWrapper;

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
    pub(crate) app_id: Arc<str>,
    pub(crate) config: AppConfig,
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) orchestrator: Arc<dyn Orchestrator>,
    pub(crate) state_backend: Arc<dyn StateBackend>,
    pub(crate) task_registry: Arc<TaskRegistry>,
    pub(crate) trigger_manager: Option<Arc<TriggerManager>>,
    pub(crate) middlewares: Vec<Arc<dyn TaskMiddleware>>,
    pub(crate) emitter: Arc<dyn EventEmitter>,
    /// Per-worker state: maps worker RunnerId → WorkerState.
    pub(crate) worker_states: Arc<std::sync::Mutex<HashMap<RunnerId, WorkerState>>>,
    pub(crate) shutdown_tx: Arc<watch::Sender<bool>>,
    pub(crate) idle_sleep_ms: u64,
    pub(crate) num_workers: usize,
}

impl Clone for PersistentTokioRunner {
    fn clone(&self) -> Self {
        Self {
            runner_id: self.runner_id.clone(),
            app_id: Arc::clone(&self.app_id),
            config: self.config.clone(),
            broker: Arc::clone(&self.broker),
            orchestrator: Arc::clone(&self.orchestrator),
            state_backend: Arc::clone(&self.state_backend),
            task_registry: Arc::clone(&self.task_registry),
            trigger_manager: self.trigger_manager.clone(),
            middlewares: self.middlewares.clone(),
            emitter: Arc::clone(&self.emitter),
            worker_states: Arc::clone(&self.worker_states),
            shutdown_tx: Arc::clone(&self.shutdown_tx),
            idle_sleep_ms: self.idle_sleep_ms,
            num_workers: self.num_workers,
        }
    }
}

impl std::fmt::Debug for PersistentTokioRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentTokioRunner")
            .field("runner_id", &self.runner_id)
            .field("app_id", &self.app_id)
            .field("num_workers", &self.num_workers)
            .finish_non_exhaustive()
    }
}

impl PersistentTokioRunner {
    pub fn new(
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn Orchestrator>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        let runner_id = RunnerId::new();
        let num_workers = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        Self {
            runner_id,
            app_id: Arc::from(app_id),
            config,
            broker,
            orchestrator,
            state_backend,
            task_registry,
            trigger_manager: trigger_manager.map(Arc::new),
            middlewares: Vec::new(),
            emitter: Arc::new(NoopEmitter),
            worker_states: Arc::new(std::sync::Mutex::new(HashMap::new())),
            shutdown_tx: Arc::new(shutdown_tx),
            idle_sleep_ms: 100,
            num_workers,
        }
    }

    pub fn with_idle_sleep(mut self, ms: u64) -> Self {
        self.idle_sleep_ms = ms;
        self
    }

    pub fn with_num_workers(mut self, n: usize) -> Self {
        self.num_workers = n.max(1);
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

    pub(crate) fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        let mut rx = self.shutdown_tx.subscribe();
        if *rx.borrow() {
            return;
        }
        let _ = rx.changed().await;
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
            app_id = %self.app_id,
        );
        self.run_impl().instrument(runner_span).await
    }

    /// Run one invocation using the main runner_id (for backward compatibility).
    async fn run_one(&self) -> RustvelloResult<bool> {
        let ctx = RunnerContext::new(
            self.runner_id.clone(),
            Arc::clone(&self.app_id),
            "PersistentTokioRunner",
        );

        let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current(
            self.runner_id.to_string(),
            "PersistentTokioRunner",
        );
        if let Err(e) = self.state_backend.store_runner_context(&runner_ctx).await {
            tracing::warn!("Failed to store runner context: {}", e);
        }
        let worker_runner_id = RunnerId::new();
        let worker_sb_ctx =
            runner_ctx.new_child(worker_runner_id.to_string(), "PersistentTokioWorker");
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

        if let Err(e) = self
            .orchestrator
            .register_heartbeat(&self.runner_id, true)
            .await
        {
            tracing::warn!("run_one: main runner heartbeat failed: {}", e);
        }
        if let Err(e) = self
            .orchestrator
            .register_heartbeat(&worker_runner_id, false)
            .await
        {
            tracing::warn!("run_one: worker heartbeat failed: {}", e);
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
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    async fn heartbeat(&self) -> RustvelloResult<()> {
        self.orchestrator
            .register_heartbeat(&self.runner_id, true)
            .await?;
        tracing::trace!("runner:{} heartbeat", self.runner_id);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity, clippy::needless_borrows_for_generic_args)]
mod tests;
