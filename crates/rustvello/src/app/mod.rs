mod delegation;
mod submission;

use std::collections::HashMap;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{DynTask, Task, TaskDefinition, TaskFn, TaskRegistry};
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::TaskId;

use crate::orchestration::OrchestratorCoordinator;
use crate::task_config::{apply_task_env_overrides, TaskConfigOverride};

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
    pub task_registry: TaskRegistry,
    pub(crate) coordinator: OrchestratorCoordinator,
    /// Per-task config overrides from TOML [tasks.<name>] sections + env vars.
    /// Key is the task function name (e.g. "add", "process_order").
    task_config_overrides: HashMap<String, TaskConfigOverride>,
    /// Global task defaults from TOML [task_defaults] or env RUSTVELLO__TASK__KEY.
    task_defaults_override: TaskConfigOverride,
    /// Cache of resolved per-task env overrides, populated on first access.
    /// Prevents repeated `std::env::var` calls on every submit.
    env_override_cache: std::sync::Mutex<HashMap<String, Arc<TaskConfigOverride>>>,
    /// Cache of runner IDs whose `StoredRunnerContext` has already been persisted.
    /// Prevents redundant `store_runner_context` calls on every submit.
    /// Mirrors pynenc's `_runner_context_cache` in `BaseStateBackend`.
    stored_runner_cache: tokio::sync::Mutex<std::collections::HashSet<String>>,
}

impl std::fmt::Debug for RustvelloApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustvelloApp")
            .field("config", &self.config)
            .field("tasks", &self.task_registry.task_ids().len())
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
        let orchestrator: Arc<dyn Orchestrator> =
            Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
        let state_backend: Arc<dyn StateBackend> =
            Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
        let mem_cds: Arc<dyn ClientDataStore> =
            Arc::new(rustvello_mem::client_data_store::MemClientDataStore::new());
        let client_data_store = Arc::new(ClientDataStoreManager::new(
            mem_cds,
            ClientDataStoreConfig::default(),
        ));
        let coordinator = OrchestratorCoordinator::new(
            orchestrator,
            state_backend,
            broker,
            client_data_store,
            None,
            config.auto_final_invocation_purge_hours,
        );
        Self {
            config,
            task_registry: TaskRegistry::new(),
            coordinator,
            task_config_overrides: HashMap::new(),
            task_defaults_override: TaskConfigOverride::default(),
            env_override_cache: std::sync::Mutex::new(HashMap::new()),
            stored_runner_cache: tokio::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Create a new app with custom backend implementations.
    pub fn with_backends(
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn Orchestrator>,
        state_backend: Arc<dyn StateBackend>,
        client_data_store: Arc<ClientDataStoreManager>,
    ) -> Self {
        Self::with_backends_and_triggers(
            config,
            broker,
            orchestrator,
            state_backend,
            client_data_store,
            None,
        )
    }

    /// Create a new app with custom backend implementations including trigger manager.
    pub fn with_backends_and_triggers(
        config: AppConfig,
        broker: Arc<dyn Broker>,
        orchestrator: Arc<dyn Orchestrator>,
        state_backend: Arc<dyn StateBackend>,
        client_data_store: Arc<ClientDataStoreManager>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let coordinator = OrchestratorCoordinator::new(
            orchestrator,
            state_backend,
            broker,
            client_data_store,
            trigger_manager,
            config.auto_final_invocation_purge_hours,
        );
        Self {
            config,
            task_registry: TaskRegistry::new(),
            coordinator,
            task_config_overrides: HashMap::new(),
            task_defaults_override: TaskConfigOverride::default(),
            env_override_cache: std::sync::Mutex::new(HashMap::new()),
            stored_runner_cache: tokio::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Register a task with this application.
    pub fn register_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
        func: TaskFn,
    ) -> RustvelloResult<()> {
        let definition = TaskDefinition::new(task_id, config, func);
        self.task_registry.register(definition)
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
        self.task_registry.register_typed(task)
    }

    /// Set per-task config overrides (called from builder or tests).
    pub fn set_task_config_overrides(
        &mut self,
        overrides: HashMap<String, TaskConfigOverride>,
        defaults: TaskConfigOverride,
    ) {
        self.task_config_overrides = overrides;
        self.task_defaults_override = defaults;
    }

    /// Resolve the effective config for a task, applying overrides.
    pub fn resolve_task_config(&self, task_id: &TaskId, base: &TaskConfig) -> TaskConfig {
        let mut config = base.clone();

        // Layer 1: global task defaults
        self.task_defaults_override.apply_to(&mut config);

        // Layer 2: per-task overrides (by task function name)
        if let Some(per_task) = self.task_config_overrides.get(task_id.name()) {
            per_task.apply_to(&mut config);
        }

        // Layer 3: per-task env vars — cached to avoid repeated std::env::var calls
        let env_override = self.get_or_load_env_override(task_id.name());
        env_override.apply_to(&mut config);

        config
    }

    /// Resolve just the `force_new_workflow` flag without cloning the full config.
    ///
    /// Priority: env > per-task override > global defaults > base config
    /// (matching `resolve_task_config` layering order).
    fn resolve_force_new_workflow(&self, task_id: &TaskId, base: &TaskConfig) -> bool {
        // Highest priority: env override
        let env_override = self.get_or_load_env_override(task_id.name());
        if let Some(v) = env_override.force_new_workflow {
            return v;
        }
        // Then per-task config override
        if let Some(per_task) = self.task_config_overrides.get(task_id.name()) {
            if let Some(v) = per_task.force_new_workflow {
                return v;
            }
        }
        // Then global defaults
        if let Some(v) = self.task_defaults_override.force_new_workflow {
            return v;
        }
        base.force_new_workflow
    }

    /// Get or lazily load the env-based config override for a task name.
    fn get_or_load_env_override(&self, task_name: &str) -> Arc<TaskConfigOverride> {
        let mut cache = self
            .env_override_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cached) = cache.get(task_name) {
            return Arc::clone(cached);
        }
        let env_prefix = format!("RUSTVELLO__TASK__{}__", task_name.to_uppercase());
        let mut config = TaskConfig::default();
        let base = TaskConfig::default();
        apply_task_env_overrides(&env_prefix, &mut config);

        // Build an override from only the fields that changed
        let env_override = Arc::new(TaskConfigOverride {
            max_retries: (config.max_retries != base.max_retries).then_some(config.max_retries),
            concurrency_control: (config.concurrency_control != base.concurrency_control)
                .then_some(config.concurrency_control),
            running_concurrency: (config.running_concurrency != base.running_concurrency)
                .then_some(config.running_concurrency),
            registration_concurrency: None,
            cache_results: (config.cache_results != base.cache_results)
                .then_some(config.cache_results),
            key_arguments: None,
            retry_for_errors: None,
            disable_cache_args: None,
            on_diff_non_key_args_raise: None,
            parallel_batch_size: None,
            force_new_workflow: (config.force_new_workflow != base.force_new_workflow)
                .then_some(config.force_new_workflow),
            reroute_on_cc: (config.reroute_on_cc != base.reroute_on_cc)
                .then_some(config.reroute_on_cc),
            blocking: (config.blocking != base.blocking).then_some(config.blocking),
        });
        cache.insert(task_name.to_owned(), Arc::clone(&env_override));
        env_override
    }

    /// Get a type-erased task from the registry by ID.
    pub fn get_task(&self, task_id: &TaskId) -> Option<Arc<dyn DynTask>> {
        self.task_registry.get_dyn(task_id)
    }

    // -----------------------------------------------------------------------
    // Backend accessors
    // -----------------------------------------------------------------------

    /// Get shared references to backends (for runner construction).
    pub fn broker(&self) -> Arc<dyn Broker> {
        Arc::clone(&self.coordinator.broker)
    }

    pub fn orchestrator(&self) -> Arc<dyn Orchestrator> {
        Arc::clone(&self.coordinator.orchestrator)
    }

    pub fn state_backend(&self) -> Arc<dyn StateBackend> {
        Arc::clone(&self.coordinator.state_backend)
    }

    pub fn client_data_store(&self) -> Arc<ClientDataStoreManager> {
        Arc::clone(&self.coordinator.client_data_store)
    }

    /// Get a reference to the trigger manager (if configured).
    pub fn trigger_manager(&self) -> Option<&TriggerManager> {
        self.coordinator.trigger_manager.as_ref()
    }

    /// Set the trigger manager.
    pub fn set_trigger_manager(&mut self, manager: TriggerManager) {
        self.coordinator.trigger_manager = Some(manager);
    }

    /// Purge all data from all backends (orchestrator, broker, state backend).
    ///
    /// Equivalent to pynenc's `Pynenc.purge()`.
    pub async fn purge(&self) -> RustvelloResult<()> {
        self.coordinator.orchestrator.purge().await?;
        self.coordinator.broker.purge(None).await?;
        self.coordinator.state_backend.purge().await?;
        Ok(())
    }

    /// Consume the app and return a `TaskRunner` ready to process invocations.
    pub fn into_runner(self) -> crate::runner::TaskRunner {
        crate::runner::TaskRunner::new(
            self.config.app_id.clone(),
            self.config,
            self.coordinator.broker,
            self.coordinator.orchestrator,
            self.coordinator.state_backend,
            Arc::new(self.task_registry),
            self.coordinator.trigger_manager,
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
