//! Typed call representation bridging task parameters to serialized form.
//!
//! A [`Call`] represents a specific invocation of a [`Task`] with concrete
//! parameters. It lazily computes the serialized form and deterministic
//! [`CallId`] exactly like pynenc's `Call` class.
//!
//! The call hierarchy mirrors pynenc:
//! - [`Call`] holds typed params + task reference → computes [`CallDTO`]
//! - [`CallId`] = `TaskId` + SHA256(serialized args) → deterministic identity
//! - [`CallDTO`] is the serialized form suitable for persistence

use std::collections::BTreeMap;
use std::marker::PhantomData;

use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{CallId, TaskId};
use rustvello_proto::status::ConcurrencyControlType;

use crate::error::{RustvelloError, RustvelloResult};
use crate::task::Task;

/// Convert typed params into per-key serialized arguments.
pub fn params_to_serialized_arguments<P: serde::Serialize>(
    params: &P,
) -> RustvelloResult<SerializedArguments> {
    let value = serde_json::to_value(params).map_err(|e| RustvelloError::Serialization {
        message: e.to_string(),
    })?;

    let mut args = SerializedArguments::new();
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let v_str =
                    serde_json::to_string(&v).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                args.insert(k, v_str);
            }
        }
        other => {
            let v_str =
                serde_json::to_string(&other).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            args.insert("__args__", v_str);
        }
    }
    Ok(args)
}

/// A typed task call with concrete parameters.
///
/// Mirrors pynenc's `Call` class. Holds a reference to the task and the
/// typed parameters. Lazily serializes the parameters and computes the
/// deterministic [`CallId`] on demand.
///
/// # Example
///
/// ```rust
/// use rustvello_core::call::Call;
/// use rustvello_core::task::Task;
/// use rustvello_proto::config::TaskConfig;
/// use rustvello_proto::identifiers::TaskId;
/// use rustvello_core::error::RustvelloResult;
///
/// struct DoubleTask { task_id: TaskId, config: TaskConfig }
/// impl DoubleTask {
///     fn new() -> Self {
///         Self { task_id: TaskId::new("example", "double"), config: TaskConfig::default() }
///     }
/// }
/// impl Task for DoubleTask {
///     type Params = i32;
///     type Result = i32;
///     fn task_id(&self) -> &TaskId { &self.task_id }
///     fn config(&self) -> &TaskConfig { &self.config }
///     fn run(&self, x: i32) -> RustvelloResult<i32> { Ok(x * 2) }
/// }
///
/// let task = DoubleTask::new();
/// let call = Call::new(&task, 21);
/// let dto = call.to_dto().unwrap();
/// assert_eq!(dto.task_id, TaskId::new("example", "double"));
/// ```
pub struct Call<'a, T: Task> {
    task: &'a T,
    params: T::Params,
    _marker: PhantomData<T::Result>,
}

impl<'a, T: Task> Call<'a, T> {
    /// Create a new call with the given task and parameters.
    pub fn new(task: &'a T, params: T::Params) -> Self {
        Self {
            task,
            params,
            _marker: PhantomData,
        }
    }

    /// Get a reference to the task.
    pub fn task(&self) -> &T {
        self.task
    }

    /// Get a reference to the parameters.
    pub fn params(&self) -> &T::Params {
        &self.params
    }

    /// Consume the call and return the parameters.
    pub fn into_params(self) -> T::Params {
        self.params
    }

    /// Serialize the parameters to a JSON string.
    pub fn serialize_params(&self) -> RustvelloResult<String> {
        serde_json::to_string(&self.params).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })
    }

    /// Compute the serialized arguments as a [`SerializedArguments`].
    ///
    /// For struct-like params, each field becomes a key-value pair.
    /// For other types (primitives, tuples), the entire value is stored
    /// under a single `"__args__"` key.
    pub fn serialized_arguments(&self) -> RustvelloResult<SerializedArguments> {
        params_to_serialized_arguments(&self.params)
    }

    /// Compute the deterministic [`CallId`] for this call.
    pub fn call_id(&self) -> RustvelloResult<CallId> {
        let args = self.serialized_arguments()?;
        let args_id = args.compute_args_id();
        Ok(CallId::new(self.task.task_id().clone(), args_id))
    }

    /// Convert to a [`CallDTO`] suitable for persistence.
    pub fn to_dto(&self) -> RustvelloResult<CallDTO> {
        let args = self.serialized_arguments()?;
        Ok(CallDTO::new(self.task.task_id().clone(), args))
    }

    /// Returns the serialized arguments relevant for concurrency checking.
    ///
    /// Mirrors pynenc's `Call.serialized_args_for_concurrency_check`.
    /// The result depends on the task's concurrency control configuration:
    /// - `Unlimited` → `None` (no concurrency check needed)
    /// - `Task` → `Some(empty)` (task-level only, no args)
    /// - `Argument` → `Some(all args)` or `Some(key_arguments subset)` if key_arguments is set
    /// - `None` → `Some(all args)` (strictest: full dedup)
    pub fn serialized_args_for_concurrency_check(
        &self,
    ) -> RustvelloResult<Option<SerializedArguments>> {
        let config = self.task.config();
        match config.concurrency_control {
            ConcurrencyControlType::Unlimited => Ok(None),
            ConcurrencyControlType::Task => Ok(Some(SerializedArguments::new())),
            ConcurrencyControlType::Argument => {
                let all_args = self.serialized_arguments()?;
                if config.key_arguments.is_empty() {
                    Ok(Some(all_args))
                } else {
                    let mut filtered = SerializedArguments::new();
                    for key in &config.key_arguments {
                        if let Some(val) = all_args.0.get(key) {
                            filtered.insert(key, val.clone());
                        }
                    }
                    Ok(Some(filtered))
                }
            }
            ConcurrencyControlType::None => {
                let all_args = self.serialized_arguments()?;
                Ok(Some(all_args))
            }
            // #[non_exhaustive] requires a fallback — treat unknown variants
            // conservatively by applying full-argument dedup.
            _ => {
                let all_args = self.serialized_arguments()?;
                Ok(Some(all_args))
            }
        }
    }
}

/// Create a [`CallDTO`] from raw parts (for use in runners when executing).
pub fn call_dto_from_parts(task_id: TaskId, serialized_args: BTreeMap<String, String>) -> CallDTO {
    let mut args = SerializedArguments::new();
    for (k, v) in serialized_args {
        args.insert(k, v);
    }
    CallDTO::new(task_id, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RustvelloResult;
    use rustvello_proto::config::TaskConfig;
    use serde::{Deserialize, Serialize};

    // -- Helper task for tests --

    struct AddTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl AddTask {
        fn new() -> Self {
            Self {
                task_id: TaskId::new("test", "add"),
                config: TaskConfig::default(),
            }
        }
    }
    impl Task for AddTask {
        type Params = AddParams;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, p: AddParams) -> RustvelloResult<i32> {
            Ok(p.x + p.y)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AddParams {
        x: i32,
        y: i32,
    }

    struct DoubleTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl DoubleTask {
        fn new() -> Self {
            Self {
                task_id: TaskId::new("test", "double"),
                config: TaskConfig::default(),
            }
        }
    }
    impl Task for DoubleTask {
        type Params = i32;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, x: i32) -> RustvelloResult<i32> {
            Ok(x * 2)
        }
    }

    #[test]
    fn call_serialized_arguments_struct() {
        let task = AddTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        let args = call.serialized_arguments().unwrap();
        // Struct params become individual keys
        assert!(args.0.contains_key("x"));
        assert!(args.0.contains_key("y"));
        assert_eq!(args.0["x"], "1");
        assert_eq!(args.0["y"], "2");
    }

    #[test]
    fn call_serialized_arguments_primitive() {
        let task = DoubleTask::new();
        let call = Call::new(&task, 42);
        let args = call.serialized_arguments().unwrap();
        // Non-struct params go under __args__
        assert!(args.0.contains_key("__args__"));
        assert_eq!(args.0["__args__"], "42");
    }

    #[test]
    fn call_id_deterministic() {
        let task1 = AddTask::new();
        let call1 = Call::new(&task1, AddParams { x: 1, y: 2 });
        let task2 = AddTask::new();
        let call2 = Call::new(&task2, AddParams { x: 1, y: 2 });
        assert_eq!(call1.call_id().unwrap(), call2.call_id().unwrap());
    }

    #[test]
    fn call_id_different_args() {
        let task1 = AddTask::new();
        let call1 = Call::new(&task1, AddParams { x: 1, y: 2 });
        let task2 = AddTask::new();
        let call2 = Call::new(&task2, AddParams { x: 3, y: 4 });
        assert_ne!(call1.call_id().unwrap(), call2.call_id().unwrap());
    }

    #[test]
    fn call_to_dto() {
        let task = AddTask::new();
        let call = Call::new(&task, AddParams { x: 10, y: 20 });
        let dto = call.to_dto().unwrap();
        assert_eq!(dto.task_id, TaskId::new("test", "add"));
        assert_eq!(dto.serialized_arguments.0["x"], "10");
        assert_eq!(dto.serialized_arguments.0["y"], "20");
    }

    #[test]
    fn call_dto_from_parts_works() {
        let mut map = BTreeMap::new();
        map.insert("a".to_string(), "1".to_string());
        let dto = call_dto_from_parts(TaskId::new("m", "f"), map);
        assert_eq!(dto.task_id, TaskId::new("m", "f"));
        assert_eq!(dto.serialized_arguments.0["a"], "1");
    }

    // -- Concurrency control args tests --

    struct TaskCCTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl TaskCCTask {
        fn new() -> Self {
            let mut config = TaskConfig::default();
            config.concurrency_control = ConcurrencyControlType::Task;
            Self {
                task_id: TaskId::new("test", "cc_task"),
                config,
            }
        }
    }
    impl Task for TaskCCTask {
        type Params = AddParams;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, p: AddParams) -> RustvelloResult<i32> {
            Ok(p.x + p.y)
        }
    }

    struct ArgCCTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl ArgCCTask {
        fn new() -> Self {
            let mut config = TaskConfig::default();
            config.concurrency_control = ConcurrencyControlType::Argument;
            Self {
                task_id: TaskId::new("test", "cc_arg"),
                config,
            }
        }
    }
    impl Task for ArgCCTask {
        type Params = AddParams;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, p: AddParams) -> RustvelloResult<i32> {
            Ok(p.x + p.y)
        }
    }

    struct KeyCCTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl KeyCCTask {
        fn new() -> Self {
            let mut config = TaskConfig::default();
            config.concurrency_control = ConcurrencyControlType::Argument;
            config.key_arguments = vec!["x".to_string()];
            Self {
                task_id: TaskId::new("test", "cc_key"),
                config,
            }
        }
    }
    impl Task for KeyCCTask {
        type Params = AddParams;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, p: AddParams) -> RustvelloResult<i32> {
            Ok(p.x + p.y)
        }
    }

    struct NoneCCTask {
        task_id: TaskId,
        config: TaskConfig,
    }
    impl NoneCCTask {
        fn new() -> Self {
            let mut config = TaskConfig::default();
            config.concurrency_control = ConcurrencyControlType::None;
            Self {
                task_id: TaskId::new("test", "cc_none"),
                config,
            }
        }
    }
    impl Task for NoneCCTask {
        type Params = AddParams;
        type Result = i32;
        fn task_id(&self) -> &TaskId {
            &self.task_id
        }
        fn config(&self) -> &TaskConfig {
            &self.config
        }
        fn run(&self, p: AddParams) -> RustvelloResult<i32> {
            Ok(p.x + p.y)
        }
    }

    #[test]
    fn cc_args_unlimited_returns_none() {
        let task = AddTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        assert!(call
            .serialized_args_for_concurrency_check()
            .unwrap()
            .is_none());
    }

    #[test]
    fn cc_args_task_returns_empty() {
        let task = TaskCCTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        let args = call
            .serialized_args_for_concurrency_check()
            .unwrap()
            .unwrap();
        assert!(args.0.is_empty());
    }

    #[test]
    fn cc_args_argument_returns_all() {
        let task = ArgCCTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        let args = call
            .serialized_args_for_concurrency_check()
            .unwrap()
            .unwrap();
        assert_eq!(args.0.len(), 2);
        assert_eq!(args.0["x"], "1");
        assert_eq!(args.0["y"], "2");
    }

    #[test]
    fn cc_args_argument_with_key_args_returns_subset() {
        let task = KeyCCTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        let args = call
            .serialized_args_for_concurrency_check()
            .unwrap()
            .unwrap();
        assert_eq!(args.0.len(), 1);
        assert_eq!(args.0["x"], "1");
        assert!(!args.0.contains_key("y"));
    }

    #[test]
    fn cc_args_none_returns_all() {
        let task = NoneCCTask::new();
        let call = Call::new(&task, AddParams { x: 1, y: 2 });
        let args = call
            .serialized_args_for_concurrency_check()
            .unwrap()
            .unwrap();
        assert_eq!(args.0.len(), 2);
    }
}
