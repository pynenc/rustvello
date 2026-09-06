//! Application-level task registration and configuration resolution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustvello_core::error::RustvelloResult;
use rustvello_core::task::{DynTask, ForeignTask, Task, TaskDefinition, TaskFn, TaskRegistry};
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::TaskId;

use crate::task_config::{apply_task_env_overrides, TaskConfigOverride};

/// Owns task definitions and resolves their effective runtime configuration.
///
/// The catalog deliberately knows nothing about brokers, invocation state, or
/// runners. Foreign declarations live here because they are task metadata;
/// whether a task is executable is decided by the language-specific registry
/// installed in a runner.
pub struct TaskCatalog {
    registry: TaskRegistry,
    task_config_overrides: HashMap<String, TaskConfigOverride>,
    task_defaults_override: TaskConfigOverride,
    env_override_cache: Mutex<HashMap<String, Arc<TaskConfigOverride>>>,
}

impl Clone for TaskCatalog {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            task_config_overrides: self.task_config_overrides.clone(),
            task_defaults_override: self.task_defaults_override.clone(),
            env_override_cache: Mutex::new(
                self.env_override_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone(),
            ),
        }
    }
}

impl Default for TaskCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TaskCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskCatalog")
            .field("tasks", &self.registry.task_ids().len())
            .finish_non_exhaustive()
    }
}

impl TaskCatalog {
    pub fn new() -> Self {
        Self {
            registry: TaskRegistry::new(),
            task_config_overrides: HashMap::new(),
            task_defaults_override: TaskConfigOverride::default(),
            env_override_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn from_registry(registry: TaskRegistry) -> Self {
        Self {
            registry,
            task_config_overrides: HashMap::new(),
            task_defaults_override: TaskConfigOverride::default(),
            env_override_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    pub fn registry_mut(&mut self) -> &mut TaskRegistry {
        &mut self.registry
    }

    pub fn into_registry(self) -> TaskRegistry {
        self.registry
    }

    pub fn register_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
        func: TaskFn,
    ) -> RustvelloResult<()> {
        self.registry
            .register(TaskDefinition::new(task_id, config, func))
    }

    pub fn register_foreign_task(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
    ) -> RustvelloResult<()> {
        self.registry.register_task_proxy(task_id, config)
    }

    pub fn register<T: Task>(&mut self, task: T) -> RustvelloResult<()> {
        self.registry.register_typed(task)
    }

    pub fn register_foreign<F: ForeignTask>(&mut self, task: F) -> RustvelloResult<()> {
        self.registry.register_foreign(task)
    }

    pub fn get(&self, task_id: &TaskId) -> Option<Arc<dyn DynTask>> {
        self.registry.get_dyn(task_id)
    }

    pub fn contains(&self, task_id: &TaskId) -> bool {
        self.registry.contains(task_id)
    }

    pub fn set_config_overrides(
        &mut self,
        overrides: HashMap<String, TaskConfigOverride>,
        defaults: TaskConfigOverride,
    ) {
        self.task_config_overrides = overrides;
        self.task_defaults_override = defaults;
        self.env_override_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    pub fn resolve_config(
        &self,
        app_config: &AppConfig,
        task_id: &TaskId,
        base: &TaskConfig,
    ) -> TaskConfig {
        let mut config = base.clone();
        self.task_defaults_override.apply_to(&mut config);

        if let Some(per_task) = self.task_config_overrides.get(task_id.name()) {
            per_task.apply_to(&mut config);
        }

        self.get_or_load_env_override(task_id.name())
            .apply_to(&mut config);

        config.priority = app_config
            .priority_rules
            .iter()
            .filter(|rule| {
                glob::Pattern::new(&rule.task_id)
                    .is_ok_and(|pattern| pattern.matches(&task_id.to_string()))
            })
            .map(|rule| rule.priority)
            .max_by(f64::total_cmp)
            .unwrap_or(config.priority);

        config
    }

    pub fn routing_for(&self, app_config: &AppConfig, task_id: &TaskId) -> Option<(String, f64)> {
        self.get(task_id).map(|task| {
            let config = self.resolve_config(app_config, task_id, task.config());
            (config.queue, config.priority)
        })
    }

    pub fn all_routing(&self, app_config: &AppConfig) -> HashMap<TaskId, (String, f64)> {
        self.registry
            .task_ids()
            .into_iter()
            .filter_map(|task_id| {
                self.routing_for(app_config, task_id)
                    .map(|route| (task_id.clone(), route))
            })
            .collect()
    }

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
        let env_override = Arc::new(TaskConfigOverride {
            queue: (config.queue != base.queue).then_some(config.queue),
            priority: (config.priority != base.priority).then_some(config.priority),
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
            is_workflow_task: (config.is_workflow_task != base.is_workflow_task)
                .then_some(config.is_workflow_task),
            reroute_on_cc: (config.reroute_on_cc != base.reroute_on_cc)
                .then_some(config.reroute_on_cc),
            blocking: (config.blocking != base.blocking).then_some(config.blocking),
        });
        cache.insert(task_name.to_owned(), Arc::clone(&env_override));
        env_override
    }
}
