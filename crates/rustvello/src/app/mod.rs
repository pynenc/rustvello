mod delegation;
mod submission;

use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{DynTask, ForeignTask, Task, TaskFn, TaskRegistry};
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::TaskId;

use crate::orchestration::Orchestrator;
use crate::task_catalog::TaskCatalog;
use crate::task_config::TaskConfigOverride;

// ---------------------------------------------------------------------------
// TaskEntry — compile-time task registration via inventory
// ---------------------------------------------------------------------------

/// An auto-discovered task entry submitted by `#[rustvello::task]`.
///
/// Collected at link-time by the `inventory` crate. The builder's
/// `auto_discover_tasks()` method iterates all submitted entries
/// and registers them with the application's task registry.
///
/// This is the Rust equivalent of pynenc's entry_points-based plugin
/// discovery — but resolved at compile time with zero runtime cost.
///
/// The entry stores a plain function pointer (`register_fn`) that
/// creates and registers the task. Function pointers are const-constructible,
/// so they can be placed in a static context by `inventory::submit!`.
pub struct TaskEntry {
    pub register_fn: fn(&mut TaskRegistry) -> RustvelloResult<()>,
}

inventory::collect!(TaskEntry);

/// The central Rustvello application.
///
/// Owns all subsystems (broker, orchestrator, state backend, task registry)
/// and coordinates task registration, invocation, and execution.
///
/// Mirrors pynenc's `Pynenc` class.
pub struct RustvelloApp {
    pub config: AppConfig,
    task_catalog: TaskCatalog,
    pub(crate) orchestrator: Orchestrator,
}

impl std::fmt::Debug for RustvelloApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustvelloApp")
            .field("config", &self.config)
            .field("tasks", &self.task_catalog.registry().task_ids().len())
            .finish_non_exhaustive()
    }
}

impl RustvelloApp {
    /// Create a new app with the given config and default in-memory backends.
    #[cfg(feature = "mem")]
    pub fn new(config: AppConfig) -> Self {
        use rustvello_core::client_data_store::ClientDataStore;
        use rustvello_proto::config::ClientDataStoreConfig;

        let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
        let invocation_control: Arc<dyn InvocationControlBackend> =
            Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
        let state_backend: Arc<dyn StateBackend> =
            Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
        let mem_cds: Arc<dyn ClientDataStore> =
            Arc::new(rustvello_mem::client_data_store::MemClientDataStore::new());
        let client_data_store = Arc::new(ClientDataStoreManager::new(
            mem_cds,
            ClientDataStoreConfig::default(),
        ));
        let orchestrator = Orchestrator::new(
            invocation_control,
            state_backend,
            broker,
            client_data_store,
            None,
            config.auto_final_invocation_purge_hours,
        );
        Self {
            config,
            task_catalog: TaskCatalog::new(),
            orchestrator,
        }
    }

    /// Create a new app with custom backend implementations.
    pub fn with_backends(
        config: AppConfig,
        broker: Arc<dyn Broker>,
        invocation_control: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        client_data_store: Arc<ClientDataStoreManager>,
    ) -> Self {
        Self::with_backends_and_triggers(
            config,
            broker,
            invocation_control,
            state_backend,
            client_data_store,
            None,
        )
    }

    /// Create a new app with custom backend implementations including trigger manager.
    pub fn with_backends_and_triggers(
        config: AppConfig,
        broker: Arc<dyn Broker>,
        invocation_control: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        client_data_store: Arc<ClientDataStoreManager>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let orchestrator = Orchestrator::new(
            invocation_control,
            state_backend,
            broker,
            client_data_store,
            trigger_manager,
            config.auto_final_invocation_purge_hours,
        );
        Self {
            config,
            task_catalog: TaskCatalog::new(),
            orchestrator,
        }
    }

    /// Register a task with this application.
    pub fn register_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
        func: TaskFn,
    ) -> RustvelloResult<()> {
        self.task_catalog.register_task(task_id, config, func)
    }

    /// Register a task implemented by another language runtime.
    ///
    /// The task can be submitted by this app and routed through the shared
    /// backend, but it is intentionally not executable by this runner.
    pub fn register_foreign_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
    ) -> RustvelloResult<()> {
        self.task_catalog.register_foreign_task(task_id, config)
    }

    // -----------------------------------------------------------------------
    // Typed task API — registration & config resolution
    // -----------------------------------------------------------------------

    /// Register a typed task implementing the [`Task`] trait.
    ///
    /// Resolves the final `TaskConfig` by merging:
    /// 1. Macro defaults from `task.config()`
    /// 2. Global task defaults (TOML [task_defaults] or env `RUSTVELLO__TASK__KEY`)
    /// 3. Per-task overrides (TOML [tasks.<name>] or env `RUSTVELLO__TASK__<NAME>__KEY`)
    ///
    /// This is the preferred way to register tasks.
    pub fn register<T: Task>(&mut self, task: T) -> RustvelloResult<()> {
        self.task_catalog.register(task)
    }

    /// Register a typed task proxy implemented by another runtime.
    pub fn register_foreign<F: ForeignTask>(&mut self, task: F) -> RustvelloResult<()> {
        self.task_catalog.register_foreign(task)
    }

    /// Set per-task config overrides (called from builder or tests).
    pub fn set_task_config_overrides(
        &mut self,
        overrides: std::collections::HashMap<String, TaskConfigOverride>,
        defaults: TaskConfigOverride,
    ) {
        self.task_catalog.set_config_overrides(overrides, defaults);
    }

    /// Resolve the effective config for a task, applying overrides.
    pub fn resolve_task_config(&self, task_id: &TaskId, base: &TaskConfig) -> TaskConfig {
        self.task_catalog
            .resolve_config(&self.config, task_id, base)
    }

    /// Get a type-erased task from the registry by ID.
    pub fn get_task(&self, task_id: &TaskId) -> Option<Arc<dyn DynTask>> {
        self.task_catalog.get(task_id)
    }

    /// Read-only access to registered task definitions.
    pub fn task_registry(&self) -> &TaskRegistry {
        self.task_catalog.registry()
    }

    /// Mutable registry access for compile-time discovery and task modules.
    pub fn task_registry_mut(&mut self) -> &mut TaskRegistry {
        self.task_catalog.registry_mut()
    }

    // -----------------------------------------------------------------------
    // Backend accessors
    // -----------------------------------------------------------------------

    /// Get shared references to backends (for runner construction).
    pub fn broker(&self) -> Arc<dyn Broker> {
        self.orchestrator.broker()
    }

    pub fn orchestrator(&self) -> Arc<dyn InvocationControlBackend> {
        self.orchestrator.invocation_control()
    }

    pub fn state_backend(&self) -> Arc<dyn StateBackend> {
        self.orchestrator.state_backend()
    }

    pub fn client_data_store(&self) -> Arc<ClientDataStoreManager> {
        self.orchestrator.client_data_store()
    }

    /// Get a reference to the trigger manager (if configured).
    pub fn trigger_manager(&self) -> Option<&TriggerManager> {
        self.orchestrator.trigger_manager()
    }

    /// Set the trigger manager.
    pub fn set_trigger_manager(&mut self, manager: TriggerManager) {
        self.orchestrator.set_trigger_manager(manager);
    }

    /// Purge all data from all backends (orchestrator, broker, state backend).
    ///
    /// Equivalent to pynenc's `Pynenc.purge()`.
    pub async fn purge(&self) -> RustvelloResult<()> {
        self.orchestrator.purge().await
    }

    /// Consume the app and return a `TaskRunner` ready to process invocations.
    pub fn into_runner(self) -> crate::runner::TaskRunner {
        let crate::orchestration::RunnerPorts {
            broker,
            invocation_control,
            state_backend,
            trigger_manager,
        } = self.orchestrator.into_runner_ports();
        crate::runner::TaskRunner::new_with_catalog(
            self.config.app_id.clone(),
            self.config,
            broker,
            invocation_control,
            state_backend,
            Arc::new(self.task_catalog),
            trigger_manager,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_core::error::RustvelloError;
    use rustvello_proto::call::SerializedArguments;
    use rustvello_proto::status::InvocationStatus;

    fn make_app() -> RustvelloApp {
        let mut app = RustvelloApp::new(AppConfig::new("test-app"));
        app.register_task(
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
        )
        .unwrap();
        app
    }

    #[tokio::test]
    async fn test_submit_and_status() {
        let app = make_app();
        let mut args = SerializedArguments::new();
        args.insert("x", "21");

        let inv_id = app
            .submit(&TaskId::new("test", "double"), args)
            .await
            .unwrap();

        let status = app.get_status(&inv_id).await.unwrap();
        assert_eq!(status, InvocationStatus::Registered);
    }

    #[tokio::test]
    async fn test_submit_unregistered_task() {
        let app = make_app();
        let args = SerializedArguments::new();

        let result = app.submit(&TaskId::new("nonexistent", "task"), args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_submit_sync() {
        let app = make_app();
        let mut args = SerializedArguments::new();
        args.insert("x", "21");

        let result = app
            .submit_sync(&TaskId::new("test", "double"), args)
            .await
            .unwrap();
        assert_eq!(result, "42");
    }

    #[tokio::test]
    async fn test_submit_sync_unregistered() {
        let app = make_app();
        let args = SerializedArguments::new();
        let result = app.submit_sync(&TaskId::new("no", "such"), args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_result() {
        let app = make_app();
        let mut args = SerializedArguments::new();
        args.insert("x", "21");

        let inv_id = app
            .submit(&TaskId::new("test", "double"), args)
            .await
            .unwrap();

        // Result not yet available (task not executed)
        let result = app.get_result(&inv_id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_backend_accessors() {
        let app = make_app();
        // Just verify they return without panicking
        let _broker = app.broker();
        let _orch = app.orchestrator();
        let _sb = app.state_backend();
    }
}
