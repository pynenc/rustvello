use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskRegistry;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{ExecutorKind, InvocationId, RunnerId, TaskLanguage};

use rustvello_core::error::RustvelloResult;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::task_catalog::TaskCatalog;

/// Shared runner process dependencies and immutable language identity.
///
/// Executors own local task-code execution. This control plane owns the ports
/// and policy required for dispatch, heartbeat, recovery, and shutdown loops.
#[derive(Clone)]
pub(crate) struct RunnerControlPlane {
    pub(crate) runner_language: TaskLanguage,
    pub(crate) executor_kind: ExecutorKind,
    pub(crate) app_id: Arc<str>,
    pub(crate) config: AppConfig,
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) orchestrator: Arc<dyn InvocationControlBackend>,
    pub(crate) state_backend: Arc<dyn StateBackend>,
    pub(crate) task_catalog: Arc<TaskCatalog>,
    pub(crate) trigger_manager: Option<Arc<TriggerManager>>,
    shutdown_tx: Arc<watch::Sender<bool>>,
}

impl RunnerControlPlane {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        language: TaskLanguage,
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_registry: Arc<TaskRegistry>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            runner_language: language,
            executor_kind: default_executor_for(language),
            app_id: Arc::from(app_id),
            config,
            broker,
            orchestrator,
            state_backend,
            task_catalog: Arc::new(TaskCatalog::from_registry((*task_registry).clone())),
            trigger_manager: trigger_manager.map(Arc::new),
            shutdown_tx: Arc::new(shutdown_tx),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_catalog(
        language: TaskLanguage,
        app_id: String,
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        task_catalog: Arc<TaskCatalog>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            runner_language: language,
            executor_kind: default_executor_for(language),
            app_id: Arc::from(app_id),
            config,
            broker,
            orchestrator,
            state_backend,
            task_catalog,
            trigger_manager: trigger_manager.map(Arc::new),
            shutdown_tx: Arc::new(shutdown_tx),
        }
    }

    #[cfg(feature = "rayon")]
    pub(crate) fn with_executor_kind(mut self, executor_kind: ExecutorKind) -> Self {
        self.executor_kind = executor_kind;
        self
    }

    pub(crate) fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        *self.shutdown_tx.borrow()
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        let mut receiver = self.shutdown_tx.subscribe();
        if !*receiver.borrow() {
            let _ = receiver.changed().await;
        }
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        let token = CancellationToken::new();
        let cancelled = token.clone();
        let mut receiver = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            if !*receiver.borrow() {
                let _ = receiver.changed().await;
            }
            cancelled.cancel();
        });
        token
    }

    pub(crate) async fn claim_next(&self) -> RustvelloResult<Option<InvocationId>> {
        super::dispatcher::claim_next(self).await
    }

    pub(crate) fn task_registry(&self) -> &TaskRegistry {
        self.task_catalog.registry()
    }

    pub(crate) async fn heartbeat(
        &self,
        runner_id: &RunnerId,
        can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .register_heartbeat(runner_id, can_run_atomic_service)
            .await
    }

    pub(crate) async fn recover_stale_invocations(
        &self,
        runner_id: &RunnerId,
    ) -> RustvelloResult<u32> {
        self.lifecycle()
            .recover_stale_invocations(&self.config, &self.task_catalog, runner_id)
            .await
    }

    pub(crate) async fn evaluate_triggers(
        &self,
        runner_id: &RunnerId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.lifecycle()
            .run_trigger_iteration(&self.config, &self.task_catalog, runner_id)
            .await
    }

    pub(crate) fn lifecycle(&self) -> crate::orchestration::Orchestrator {
        crate::orchestration::Orchestrator::for_runner(
            Arc::clone(&self.orchestrator),
            Arc::clone(&self.state_backend),
            Arc::clone(&self.broker),
            self.trigger_manager.as_deref().cloned(),
            self.config.auto_final_invocation_purge_hours,
        )
    }
}

const fn default_executor_for(language: TaskLanguage) -> ExecutorKind {
    match language {
        TaskLanguage::Rust => ExecutorKind::Tokio,
        TaskLanguage::Python => ExecutorKind::Python,
    }
}
