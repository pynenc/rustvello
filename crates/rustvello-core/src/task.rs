use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::TaskId;

use crate::error::{RustvelloError, RustvelloResult};

// ---------------------------------------------------------------------------
// Typed Task trait
// ---------------------------------------------------------------------------

/// A distributable task with typed parameters and results.
///
/// This is the Rust equivalent of pynenc's `Task` class. Each task
/// definition implements this trait, providing:
/// - A unique identity ([`TaskId`])
/// - Configuration (retries, concurrency, etc.)
/// - Typed execution (`Params` → `Result` via serde)
///
/// Tasks are typically created via the `#[rustvello::task]` proc-macro, but
/// can also be implemented manually for testing or advanced use cases.
///
/// # Example (manual implementation)
///
/// ```rust
/// use rustvello_core::task::Task;
/// use rustvello_proto::config::TaskConfig;
/// use rustvello_proto::identifiers::TaskId;
/// use rustvello_core::error::RustvelloResult;
///
/// struct AddTask {
///     task_id: TaskId,
///     config: TaskConfig,
/// }
///
/// impl Task for AddTask {
///     type Params = (i32, i32);
///     type Result = i32;
///
///     fn task_id(&self) -> &TaskId {
///         &self.task_id
///     }
///
///     fn config(&self) -> &TaskConfig {
///         &self.config
///     }
///
///     fn run(&self, params: Self::Params) -> RustvelloResult<Self::Result> {
///         Ok(params.0 + params.1)
///     }
/// }
/// ```
pub trait Task: Send + Sync + 'static {
    /// The input parameters type (must be serializable).
    type Params: Serialize + DeserializeOwned + Send + Sync + 'static;
    /// The return type (must be serializable).
    type Result: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Unique identifier for this task.
    fn task_id(&self) -> &TaskId;

    /// Per-task configuration.
    fn config(&self) -> &TaskConfig;

    /// Execute the task with the given parameters.
    fn run(&self, params: Self::Params) -> RustvelloResult<Self::Result>;
}

// ---------------------------------------------------------------------------
// Type-erased DynTask for heterogeneous registry storage
// ---------------------------------------------------------------------------

/// Type-erased task interface for the [`TaskRegistry`].
///
/// Every `T: Task` automatically implements `DynTask` via a blanket impl.
/// The registry stores `Arc<dyn DynTask>`, which handles serialization
/// and deserialization internally.
pub trait DynTask: Send + Sync {
    /// The task's unique identifier.
    fn task_id(&self) -> &TaskId;

    /// The task's configuration.
    fn config(&self) -> &TaskConfig;

    /// Execute with [`SerializedArguments`], returns serialized JSON result.
    fn execute(&self, args: &SerializedArguments) -> RustvelloResult<String>;
}

/// Reconstruct a single JSON string from per-key [`SerializedArguments`].
///
/// - If only `__args__` is present, returns its raw value (non-struct params).
/// - Otherwise, builds a JSON object from the key-value pairs with proper
///   key escaping and value validation to prevent structural injection.
pub fn serialized_args_to_json(
    args: &SerializedArguments,
) -> RustvelloResult<std::borrow::Cow<'_, str>> {
    use std::borrow::Cow;
    if args.0.len() == 1 && args.0.contains_key("__args__") {
        // Non-struct params (primitives, tuples) stored under __args__
        return Ok(Cow::Borrowed(&args.0["__args__"]));
    }
    // Struct params: build a JSON object string directly
    use std::fmt::Write;
    let mut buf = String::with_capacity(args.0.len() * 32 + 2);
    buf.push('{');
    for (i, (k, v)) in args.0.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        // Escape keys to prevent JSON injection from arbitrary input.
        let escaped_key =
            serde_json::to_string(k.as_str()).map_err(|e| RustvelloError::Serialization {
                message: format!("failed to escape JSON key: {e}"),
            })?;
        // Validate that the value is valid JSON to prevent structural injection
        serde_json::from_str::<serde_json::Value>(v).map_err(|e| {
            RustvelloError::Serialization {
                message: format!("invalid JSON value for key {k}: {e}"),
            }
        })?;
        write!(buf, "{}:{}", escaped_key, v).map_err(|e| RustvelloError::Serialization {
            message: format!("failed to build JSON: {e}"),
        })?;
    }
    buf.push('}');
    Ok(Cow::Owned(buf))
}

impl<T: Task> DynTask for T {
    #[inline]
    fn task_id(&self) -> &TaskId {
        Task::task_id(self)
    }

    #[inline]
    fn config(&self) -> &TaskConfig {
        Task::config(self)
    }

    fn execute(&self, args: &SerializedArguments) -> RustvelloResult<String> {
        let json_str = serialized_args_to_json(args)?;
        let params: T::Params =
            serde_json::from_str(&json_str).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;
        let result = self.run(params)?;
        serde_json::to_string(&result).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })
    }
}

impl fmt::Debug for dyn DynTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynTask")
            .field("task_id", &self.task_id())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Cross-language safety marker + ForeignTask trait
// ---------------------------------------------------------------------------

/// Marker trait for types that can safely cross language boundaries.
///
/// Types implementing this trait must serialize to/from JSON using only
/// universally supported primitives: bool, number, string, array, object, null.
/// This excludes language-specific types (Python objects, Rust enums with data, etc.).
///
/// Used as a bound on [`ForeignTask`] params and results to provide
/// compile-time enforcement that cross-language calls use compatible types.
pub trait CrossLanguageSafe: Serialize + DeserializeOwned {}

// Blanket implementations for common JSON-safe types
impl CrossLanguageSafe for String {}
impl CrossLanguageSafe for bool {}
impl CrossLanguageSafe for i32 {}
impl CrossLanguageSafe for i64 {}
impl CrossLanguageSafe for u32 {}
impl CrossLanguageSafe for u64 {}
impl CrossLanguageSafe for f32 {}
impl CrossLanguageSafe for f64 {}
impl<T: CrossLanguageSafe> CrossLanguageSafe for Vec<T> {}
impl<T: CrossLanguageSafe> CrossLanguageSafe for Option<T> {}
impl<K: CrossLanguageSafe + Ord, V: CrossLanguageSafe> CrossLanguageSafe
    for std::collections::BTreeMap<K, V>
{
}
impl<K: CrossLanguageSafe + Eq + std::hash::Hash, V: CrossLanguageSafe> CrossLanguageSafe
    for std::collections::HashMap<K, V>
{
}

/// A typed task proxy for a task implemented by another runtime.
///
/// Unlike [`Task`], a `ForeignTask` has no `run()` method because execution
/// happens in the foreign language worker. The Rust side only creates
/// invocations that the foreign worker picks up from its language queue.
///
/// The `CrossLanguageSafe` bound on `Params` and `Result` ensures that
/// only JSON-compatible types are used for cross-language serialization.
///
/// Implement this trait when a proxy needs custom behavior or metadata. For
/// ordinary cross-language calls, [`ForeignTaskProxy`] avoids the boilerplate.
///
/// # Example
///
/// ```rust
/// use rustvello_core::task::{ForeignTask, CrossLanguageSafe};
/// use rustvello_proto::config::TaskConfig;
/// use rustvello_proto::identifiers::TaskId;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct TrainModelParams {
///     dataset_path: String,
///     epochs: u32,
/// }
/// impl CrossLanguageSafe for TrainModelParams {}
///
/// struct TrainModel {
///     task_id: TaskId,
/// }
///
/// impl ForeignTask for TrainModel {
///     type Params = TrainModelParams;
///     type Result = String;
///
///     fn task_id(&self) -> TaskId {
///         self.task_id.clone()
///     }
/// }
/// ```
pub trait ForeignTask: Send + Sync + 'static {
    /// The input parameters type (must be cross-language safe).
    type Params: CrossLanguageSafe + Send + Sync + 'static;
    /// The return type (must be cross-language safe).
    type Result: CrossLanguageSafe + Send + Sync + 'static;

    /// Unique language-qualified identifier for this task.
    fn task_id(&self) -> TaskId;

    /// Per-task configuration (optional override).
    fn config(&self) -> TaskConfig {
        TaskConfig::default()
    }
}

/// A reusable typed proxy for a task executed by another language worker.
///
/// The proxy carries the canonical task identity and its Rust-visible input
/// and output types. Register it on the application just like a local task;
/// matching runner-language routing ensures it can only execute remotely.
pub struct ForeignTaskProxy<P, R> {
    task_id: TaskId,
    config: TaskConfig,
    _types: PhantomData<fn(P) -> R>,
}

impl<P, R> Clone for ForeignTaskProxy<P, R> {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id.clone(),
            config: self.config.clone(),
            _types: PhantomData,
        }
    }
}

impl<P, R> fmt::Debug for ForeignTaskProxy<P, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForeignTaskProxy")
            .field("task_id", &self.task_id)
            .field("config", &self.config)
            .finish()
    }
}

impl<P, R> ForeignTaskProxy<P, R> {
    /// Create a proxy with the default task configuration.
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            config: TaskConfig::default(),
            _types: PhantomData,
        }
    }

    /// Override the task configuration used when registering this proxy.
    pub fn with_config(mut self, config: TaskConfig) -> Self {
        self.config = config;
        self
    }
}

impl<P, R> ForeignTask for ForeignTaskProxy<P, R>
where
    P: CrossLanguageSafe + Send + Sync + 'static,
    R: CrossLanguageSafe + Send + Sync + 'static,
{
    type Params = P;
    type Result = R;

    fn task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    fn config(&self) -> TaskConfig {
        self.config.clone()
    }
}

/// Runtime registry entry for a task implemented by another worker language.
struct TaskProxy {
    task_id: TaskId,
    config: TaskConfig,
}

impl TaskProxy {
    fn new(task_id: TaskId, config: TaskConfig) -> Self {
        Self { task_id, config }
    }
}

impl DynTask for TaskProxy {
    fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    fn config(&self) -> &TaskConfig {
        &self.config
    }

    fn execute(&self, _args: &SerializedArguments) -> RustvelloResult<String> {
        Err(RustvelloError::Configuration {
            message: format!(
                "task proxy {} cannot execute in this worker; it must be processed by a {} worker",
                self.task_id,
                self.task_id.language(),
            ),
        })
    }
}

// ---------------------------------------------------------------------------
// Legacy untyped TaskFn/TaskDefinition (preserved for backward compatibility)
// ---------------------------------------------------------------------------

/// A function that can be executed as a task (untyped, legacy).
///
/// In Rust, tasks are registered as boxed closures or function pointers.
/// The input and output are serialized JSON strings to allow heterogeneous
/// task types in the same registry.
///
/// **Prefer using the typed [`Task`] trait for new code.**
pub type TaskFn = Arc<dyn Fn(String) -> RustvelloResult<String> + Send + Sync>;

/// A registered task definition with its metadata and executable function (legacy).
///
/// **Prefer using the typed [`Task`] trait for new code.** This type exists
/// for backward compatibility with code that uses `TaskFn` closures.
pub struct TaskDefinition {
    pub task_id: TaskId,
    pub config: TaskConfig,
    pub func: TaskFn,
}

impl TaskDefinition {
    pub fn new(task_id: TaskId, config: TaskConfig, func: TaskFn) -> Self {
        Self {
            task_id,
            config,
            func,
        }
    }
}

impl fmt::Debug for TaskDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskDefinition")
            .field("task_id", &self.task_id)
            .field("config", &self.config)
            .finish()
    }
}

/// Adapter: wraps a legacy [`TaskDefinition`] as a [`DynTask`].
struct LegacyTaskAdapter {
    definition: Arc<TaskDefinition>,
}

impl DynTask for LegacyTaskAdapter {
    fn task_id(&self) -> &TaskId {
        &self.definition.task_id
    }

    fn config(&self) -> &TaskConfig {
        &self.definition.config
    }

    fn execute(&self, args: &SerializedArguments) -> RustvelloResult<String> {
        // Legacy tasks expect the BTreeMap<String, String> serialized as JSON
        let args_json =
            serde_json::to_string(&args.0).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;
        (self.definition.func)(args_json)
    }
}

// ---------------------------------------------------------------------------
// TaskRegistry — stores both typed and legacy tasks
// ---------------------------------------------------------------------------

/// Registry holding all known task definitions for this application.
///
/// Tasks must be registered before they can be invoked. Supports both
/// typed tasks (via [`Task`] trait) and legacy closure-based tasks.
#[derive(Default, Clone)]
pub struct TaskRegistry {
    tasks: HashMap<TaskId, Arc<dyn DynTask>>,
    /// Legacy index for backward-compatible `get_legacy()` access.
    legacy_tasks: HashMap<TaskId, Arc<TaskDefinition>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a typed task. Returns error if the task ID is already registered.
    pub fn register_typed<T: Task>(&mut self, task: T) -> RustvelloResult<()> {
        let task_id = task.task_id().clone();
        if self.tasks.contains_key(&task_id) {
            return Err(RustvelloError::Configuration {
                message: format!("task already registered: {}", task_id),
            });
        }
        self.tasks.insert(task_id, Arc::new(task));
        Ok(())
    }

    /// Register a typed proxy for a task implemented by another runtime.
    pub fn register_foreign<F: ForeignTask>(&mut self, task: F) -> RustvelloResult<()> {
        self.register_task_proxy(task.task_id(), task.config())
    }

    /// Register a legacy task definition. Returns error if already registered.
    pub fn register(&mut self, definition: TaskDefinition) -> RustvelloResult<()> {
        let task_id = definition.task_id.clone();
        if self.tasks.contains_key(&task_id) {
            return Err(RustvelloError::Configuration {
                message: format!("task already registered: {}", task_id),
            });
        }
        let def = Arc::new(definition);
        let adapter = LegacyTaskAdapter {
            definition: Arc::clone(&def),
        };
        self.tasks.insert(task_id.clone(), Arc::new(adapter));
        self.legacy_tasks.insert(task_id, def);
        Ok(())
    }

    /// Register a language-qualified task proxy from a dynamic API boundary.
    ///
    /// The stub keeps routing metadata in this registry while making accidental
    /// local execution fail loudly.
    pub fn register_task_proxy(
        &mut self,
        task_id: TaskId,
        config: TaskConfig,
    ) -> RustvelloResult<()> {
        if self.tasks.contains_key(&task_id) {
            return Err(RustvelloError::Configuration {
                message: format!("task already registered: {}", task_id),
            });
        }
        self.tasks
            .insert(task_id.clone(), Arc::new(TaskProxy::new(task_id, config)));
        Ok(())
    }

    /// Get a type-erased task by ID.
    pub fn get_dyn(&self, task_id: &TaskId) -> Option<Arc<dyn DynTask>> {
        self.tasks.get(task_id).cloned()
    }

    /// Get a legacy task definition by ID (backward compatibility).
    pub fn get(&self, task_id: &TaskId) -> Option<Arc<TaskDefinition>> {
        self.legacy_tasks.get(task_id).cloned()
    }

    /// Check if a task is registered.
    pub fn contains(&self, task_id: &TaskId) -> bool {
        self.tasks.contains_key(task_id)
    }

    /// List all registered task IDs.
    pub fn task_ids(&self) -> Vec<&TaskId> {
        self.tasks.keys().collect()
    }

    /// Number of registered tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("tasks", &self.tasks.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// TaskModule — grouping of task registrations
// ---------------------------------------------------------------------------

/// A module that registers one or more tasks with a [`TaskRegistry`].
///
/// Inspired by pynenc's plugin system — each module groups related tasks
/// and registers them at application startup.
///
/// # Example
///
/// ```rust
/// use rustvello_core::task::{TaskModule, TaskRegistry, TaskDefinition};
/// use rustvello_proto::config::TaskConfig;
/// use rustvello_proto::identifiers::TaskId;
/// use rustvello_core::error::RustvelloResult;
/// use std::sync::Arc;
///
/// struct MathTasks;
///
/// impl TaskModule for MathTasks {
///     fn name(&self) -> &str { "math" }
///
///     fn register(&self, registry: &mut TaskRegistry) -> RustvelloResult<()> {
///         registry.register(TaskDefinition::new(
///             TaskId::new("math", "add"),
///             TaskConfig::default(),
///             Arc::new(|_| Ok("0".to_string())),
///         ))
///     }
/// }
/// ```
pub trait TaskModule: Send + Sync {
    /// Human-readable name for this module (for logging/diagnostics).
    fn name(&self) -> &str;

    /// Register all tasks provided by this module.
    fn register(&self, registry: &mut TaskRegistry) -> RustvelloResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_proto::identifiers::TaskLanguage;

    fn dummy_fn() -> TaskFn {
        Arc::new(|_| Ok("ok".to_string()))
    }

    #[test]
    fn registry_new_is_empty() {
        let reg = TaskRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn register_and_get() {
        let mut reg = TaskRegistry::new();
        let tid = TaskId::new("mod", "func");
        reg.register(TaskDefinition::new(
            tid.clone(),
            TaskConfig::default(),
            dummy_fn(),
        ))
        .unwrap();

        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());
        assert!(reg.contains(&tid));
        assert!(reg.get(&tid).is_some());
        assert_eq!(reg.get(&tid).unwrap().task_id, tid);
    }

    #[test]
    fn register_duplicate_errors() {
        let mut reg = TaskRegistry::new();
        let tid = TaskId::new("mod", "func");
        reg.register(TaskDefinition::new(
            tid.clone(),
            TaskConfig::default(),
            dummy_fn(),
        ))
        .unwrap();
        let result = reg.register(TaskDefinition::new(tid, TaskConfig::default(), dummy_fn()));
        assert!(result.is_err());
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let reg = TaskRegistry::new();
        let tid = TaskId::new("no", "such");
        assert!(!reg.contains(&tid));
        assert!(reg.get(&tid).is_none());
    }

    #[test]
    fn task_ids_lists_all() {
        let mut reg = TaskRegistry::new();
        let t1 = TaskId::new("mod", "a");
        let t2 = TaskId::new("mod", "b");
        reg.register(TaskDefinition::new(
            t1.clone(),
            TaskConfig::default(),
            dummy_fn(),
        ))
        .unwrap();
        reg.register(TaskDefinition::new(
            t2.clone(),
            TaskConfig::default(),
            dummy_fn(),
        ))
        .unwrap();

        let ids = reg.task_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&&t1));
        assert!(ids.contains(&&t2));
    }

    #[test]
    fn task_definition_debug() {
        let def = TaskDefinition::new(
            TaskId::new("mod", "func"),
            TaskConfig::default(),
            dummy_fn(),
        );
        let debug_str = format!("{:?}", def);
        assert!(debug_str.contains("mod"));
        assert!(debug_str.contains("func"));
    }

    // -- Cross-language tests --

    #[derive(serde::Serialize, serde::Deserialize)]
    struct TestParams {
        value: String,
    }
    impl CrossLanguageSafe for TestParams {}

    fn test_foreign_task() -> ForeignTaskProxy<TestParams, String> {
        ForeignTaskProxy::new(TaskId::for_language(
            TaskLanguage::Python,
            "analytics.tasks",
            "train_model",
        ))
    }

    #[test]
    fn register_foreign_task() {
        let mut reg = TaskRegistry::new();
        reg.register_foreign(test_foreign_task()).unwrap();

        let tid = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");
        assert!(reg.contains(&tid));
        assert_eq!(reg.len(), 1);

        let dyn_task = reg.get_dyn(&tid).unwrap();
        assert_eq!(dyn_task.task_id(), &tid);
        assert_eq!(dyn_task.task_id().language(), TaskLanguage::Python);
    }

    #[test]
    fn foreign_task_execute_returns_error() {
        let mut reg = TaskRegistry::new();
        reg.register_foreign(test_foreign_task()).unwrap();

        let tid = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");
        let dyn_task = reg.get_dyn(&tid).unwrap();

        let args = SerializedArguments::default();
        let result = dyn_task.execute(&args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("task proxy"));
        assert!(err_msg.contains("python"));
    }

    #[test]
    fn register_foreign_duplicate_errors() {
        let mut reg = TaskRegistry::new();
        reg.register_foreign(test_foreign_task()).unwrap();
        let result = reg.register_foreign(test_foreign_task());
        assert!(result.is_err());
    }

    #[test]
    fn cross_language_safe_primitives() {
        // Verify the marker trait compiles for common types
        fn assert_cls<T: CrossLanguageSafe>() {}
        assert_cls::<String>();
        assert_cls::<bool>();
        assert_cls::<i32>();
        assert_cls::<i64>();
        assert_cls::<u32>();
        assert_cls::<u64>();
        assert_cls::<f32>();
        assert_cls::<f64>();
        assert_cls::<Vec<String>>();
        assert_cls::<Option<i64>>();
        assert_cls::<std::collections::BTreeMap<String, i64>>();
        assert_cls::<std::collections::HashMap<String, String>>();
    }
}
