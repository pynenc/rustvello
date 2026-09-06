use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Runtime that owns and executes a task.
///
/// This is intentionally closed: adding another language means adding a new
/// Rust-backed binding and extending this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskLanguage {
    #[serde(alias = "Rust", alias = "unknown")]
    Rust,
    #[serde(alias = "Python")]
    Python,
}

impl TaskLanguage {
    /// Every runtime implemented by this Rustvello release.
    pub const ALL: [Self; 2] = [Self::Rust, Self::Python];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::Python => "python",
        }
    }
}

impl std::fmt::Display for TaskLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for TaskLanguage {
    type Err = ParseTaskIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rust" | "Rust" => Ok(Self::Rust),
            "python" | "Python" => Ok(Self::Python),
            other => Err(ParseTaskIdError(format!(
                "unsupported task language {other:?}; supported languages are rust and python"
            ))),
        }
    }
}

/// Local runtime used by a runner process to execute task code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorKind {
    Tokio,
    TokioBlocking,
    Rayon,
    Python,
}

impl ExecutorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tokio => "tokio",
            Self::TokioBlocking => "tokio_blocking",
            Self::Rayon => "rayon",
            Self::Python => "python",
        }
    }
}

impl Default for ExecutorKind {
    fn default() -> Self {
        Self::Tokio
    }
}

impl std::fmt::Display for ExecutorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ExecutorKind {
    type Err = ParseTaskIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "tokio" => Ok(Self::Tokio),
            "tokio_blocking" | "tokio-blocking" => Ok(Self::TokioBlocking),
            "rayon" => Ok(Self::Rayon),
            "python" => Ok(Self::Python),
            other => Err(ParseTaskIdError(format!(
                "unsupported executor kind {other:?}"
            ))),
        }
    }
}

/// Uniquely identifies a task definition (language + module path + function name).
///
/// # Format
///
/// The canonical string representation is `language::module.name`.
///
/// The `name` field must not contain dots to ensure lossless round-tripping
/// through `Display`/`FromStr`.
///
/// # Representation
///
/// Internally, the module and name are packed into a single `Arc<str>`.
/// Cloning a `TaskId` is a single atomic increment with zero heap allocation.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TaskId {
    language: TaskLanguage,
    /// Packed string: `module ++ name` (concatenated, no separator).
    packed: Arc<str>,
    /// Byte offset where `module` ends (and `name` begins).
    module_end: u16,
}

impl TaskId {
    /// Build a packed `TaskId` from its three components (no validation).
    ///
    /// This is the single internal path that all constructors and
    /// deserialization go through.
    fn from_parts(language: TaskLanguage, module: &str, name: &str) -> Self {
        let module_end = module.len();
        assert!(
            module_end <= u16::MAX as usize,
            "TaskId module exceeds u16::MAX bytes ({module_end} bytes)"
        );
        let mut buf = String::with_capacity(module_end + name.len());
        buf.push_str(module);
        buf.push_str(name);
        Self {
            language,
            packed: Arc::from(buf.as_str()),
            module_end: module_end as u16,
        }
    }

    /// Create a Rust task ID.
    ///
    /// # Panics
    /// Panics if the module or name is empty, if the module contains `::`,
    /// or if the name contains `.` or `::`.
    ///
    /// Use [`try_new`](Self::try_new) for user-supplied input at API boundaries.
    pub fn new(module: impl Into<String>, name: impl Into<String>) -> Self {
        Self::for_language(TaskLanguage::Rust, module, name)
    }

    /// Fallible constructor for user-supplied input at API boundaries.
    ///
    /// Returns `Err` when the module or name cannot be represented losslessly.
    pub fn try_new(
        module: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, ParseTaskIdError> {
        Self::try_for_language(TaskLanguage::Rust, module, name)
    }

    /// Create a task ID for a specific language.
    pub fn for_language(
        language: TaskLanguage,
        module: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self::try_for_language(language, module, name).unwrap_or_else(|error| panic!("{error}"))
    }

    /// Fallible constructor for user-supplied task ID components.
    pub fn try_for_language(
        language: TaskLanguage,
        module: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, ParseTaskIdError> {
        let name = name.into();
        if name.is_empty() {
            return Err(ParseTaskIdError("TaskId name must not be empty".to_owned()));
        }
        if name.contains('.') || name.contains("::") {
            return Err(ParseTaskIdError(format!(
                "TaskId name must not contain '.' or '::': {name:?}"
            )));
        }
        let module = module.into();
        if module.is_empty() {
            return Err(ParseTaskIdError(
                "TaskId module must not be empty".to_owned(),
            ));
        }
        if module.contains("::") {
            return Err(ParseTaskIdError(format!(
                "TaskId module must not contain '::': {module:?}"
            )));
        }
        Ok(Self::from_parts(language, &module, &name))
    }

    /// Get the language that owns and executes this task.
    pub const fn language(&self) -> TaskLanguage {
        self.language
    }

    /// Get the module path.
    pub fn module(&self) -> &str {
        &self.packed[..self.module_end as usize]
    }

    /// Get the task function name.
    pub fn name(&self) -> &str {
        &self.packed[self.module_end as usize..]
    }

    /// Language-qualified config key for this task (dots replaced with underscores).
    pub fn config_key(&self) -> String {
        format!(
            "{}_{}_{}",
            self.language,
            self.module().replace('.', "_"),
            self.name()
        )
    }
}

impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskId")
            .field("language", &self.language())
            .field("module", &self.module())
            .field("name", &self.name())
            .finish()
    }
}

impl Serialize for TaskId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("TaskId", 3)?;
        s.serialize_field("language", &self.language)?;
        s.serialize_field("module", self.module())?;
        s.serialize_field("name", self.name())?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for TaskId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            #[serde(default)]
            language: Option<TaskLanguage>,
            module: String,
            name: String,
        }
        let f = Fields::deserialize(deserializer)?;
        TaskId::try_for_language(f.language.unwrap_or(TaskLanguage::Rust), f.module, f.name)
            .map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}::{}.{}", self.language, self.module(), self.name())
    }
}

/// Error returned when parsing a `TaskId` from a string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTaskIdError(String);

impl fmt::Display for ParseTaskIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseTaskIdError {}

impl FromStr for TaskId {
    type Err = ParseTaskIdError;

    /// Parse a `TaskId` from its canonical string representation.
    ///
    /// The canonical format is `language::module.name`. Legacy unqualified
    /// `module.name` values are accepted as Rust tasks and normalize on display.
    ///
    /// Returns an error if the string is empty, contains no `.` separator
    /// (in the module.name portion), or has empty module or name components.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ParseTaskIdError("empty task ID string".to_owned()));
        }

        let (language, rest) = if let Some(lang_end) = s.find("::") {
            let lang = &s[..lang_end];
            if lang.is_empty() {
                return Err(ParseTaskIdError("empty language before '::'".to_owned()));
            }
            (lang.parse()?, &s[lang_end + 2..])
        } else {
            (TaskLanguage::Rust, s)
        };

        let dot_pos = rest
            .rfind('.')
            .ok_or_else(|| ParseTaskIdError(format!("no '.' separator in task ID: {:?}", s)))?;

        let module = &rest[..dot_pos];
        let name = &rest[dot_pos + 1..];

        if module.is_empty() {
            return Err(ParseTaskIdError(format!(
                "empty module in task ID: {:?}",
                s
            )));
        }
        if name.is_empty() {
            return Err(ParseTaskIdError(format!("empty name in task ID: {:?}", s)));
        }

        TaskId::try_for_language(language, module, name)
    }
}

/// Uniquely identifies a specific call (task + serialized arguments).
///
/// The `args_id` is a SHA-256 hash of the sorted serialized arguments,
/// making it deterministic for the same inputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CallId {
    pub task_id: TaskId,
    pub args_id: Arc<str>,
}

impl CallId {
    pub fn new(task_id: TaskId, args_id: impl Into<Arc<str>>) -> Self {
        let args_id: Arc<str> = args_id.into();
        debug_assert!(
            !args_id.contains(':'),
            "CallId args_id must not contain ':' (breaks Display/FromStr round-trip): {args_id:?}"
        );
        Self { task_id, args_id }
    }
}

impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.task_id, self.args_id)
    }
}

/// Error returned when parsing a `CallId` from a string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseCallIdError {
    /// A formatting / structure issue with the call ID string itself.
    Format(String),
    /// The embedded task ID portion could not be parsed.
    TaskId(ParseTaskIdError),
}

impl fmt::Display for ParseCallIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(msg) => f.write_str(msg),
            Self::TaskId(e) => write!(f, "invalid task_id in call ID: {e}"),
        }
    }
}

impl std::error::Error for ParseCallIdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TaskId(e) => Some(e),
            Self::Format(_) => None,
        }
    }
}

impl FromStr for CallId {
    type Err = ParseCallIdError;

    /// Parse a `CallId` from `task_id:args_id` format.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Find the last ':' that separates task_id from args_id
        let colon_pos = s.rfind(':').ok_or_else(|| {
            ParseCallIdError::Format(format!("no ':' separator in call ID: {:?}", s))
        })?;
        let task_str = &s[..colon_pos];
        let args_id = &s[colon_pos + 1..];
        if args_id.is_empty() {
            return Err(ParseCallIdError::Format(format!(
                "empty args_id in call ID: {:?}",
                s
            )));
        }
        let task_id = task_str
            .parse::<TaskId>()
            .map_err(ParseCallIdError::TaskId)?;
        Ok(CallId::new(task_id, args_id))
    }
}

/// Uniquely identifies a single invocation instance (UUID).
///
/// Each time a call is routed for execution, a new invocation is created.
/// Multiple invocations can exist for the same call (retries, concurrent runs).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InvocationId(Arc<str>);

impl InvocationId {
    pub fn new() -> Self {
        Self(Arc::from(uuid::Uuid::new_v4().to_string()))
    }

    pub fn from_string(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Create an InvocationId from a string, validating UUID format.
    pub fn try_from_string(id: impl Into<Arc<str>>) -> Result<Self, String> {
        let s: Arc<str> = id.into();
        uuid::Uuid::parse_str(&s).map_err(|e| format!("invalid UUID for InvocationId: {e}"))?;
        Ok(Self(s))
    }

    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for InvocationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for InvocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for InvocationId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for InvocationId {
    fn from(s: String) -> Self {
        Self(Arc::from(s))
    }
}

impl From<&str> for InvocationId {
    fn from(s: &str) -> Self {
        Self(Arc::from(s))
    }
}

/// Identifies a runner instance processing invocations.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RunnerId(Arc<str>);

impl RunnerId {
    pub fn new() -> Self {
        Self(Arc::from(uuid::Uuid::new_v4().to_string()))
    }

    pub fn from_string(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    /// Create a RunnerId from a string, validating UUID format.
    pub fn try_from_string(id: impl Into<Arc<str>>) -> Result<Self, String> {
        let s: Arc<str> = id.into();
        uuid::Uuid::parse_str(&s).map_err(|e| format!("invalid UUID for RunnerId: {e}"))?;
        Ok(Self(s))
    }

    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for RunnerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RunnerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for RunnerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for RunnerId {
    fn from(s: String) -> Self {
        Self(Arc::from(s))
    }
}

impl From<&str> for RunnerId {
    fn from(s: &str) -> Self {
        Self(Arc::from(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_display() {
        let tid = TaskId::new("myapp.tasks", "process_order");
        assert_eq!(tid.to_string(), "rust::myapp.tasks.process_order");
    }

    #[test]
    fn python_task_id_display() {
        let tid = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");
        assert_eq!(tid.to_string(), "python::analytics.tasks.train_model");
    }

    #[test]
    fn task_id_has_language() {
        let rust = TaskId::new("mod", "func");
        assert_eq!(rust.language(), TaskLanguage::Rust);

        let python = TaskId::for_language(TaskLanguage::Python, "mod", "func");
        assert_eq!(python.language(), TaskLanguage::Python);
    }

    #[test]
    fn task_ids_in_different_languages_are_not_equal() {
        let rust = TaskId::new("mod", "func");
        let python = TaskId::for_language(TaskLanguage::Python, "mod", "func");
        assert_ne!(rust, python);
    }

    #[test]
    fn config_keys_are_language_qualified() {
        let rust = TaskId::new("analytics.tasks", "train_model");
        let python = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");

        assert_eq!(rust.config_key(), "rust_analytics_tasks_train_model");
        assert_eq!(python.config_key(), "python_analytics_tasks_train_model");
    }

    #[test]
    fn call_id_display() {
        let cid = CallId::new(TaskId::new("myapp.tasks", "process_order"), "abc123");
        assert_eq!(cid.to_string(), "rust::myapp.tasks.process_order:abc123");
    }

    #[test]
    fn invocation_id_uniqueness() {
        let id1 = InvocationId::new();
        let id2 = InvocationId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn invocation_id_from_string() {
        let id = InvocationId::from_string("my-custom-id");
        assert_eq!(&*id.0, "my-custom-id");
        assert_eq!(id.to_string(), "my-custom-id");
    }

    #[test]
    fn invocation_id_default() {
        let id = InvocationId::default();
        assert!(!id.as_str().is_empty());
    }

    #[test]
    fn runner_id_basics() {
        let r1 = RunnerId::new();
        let r2 = RunnerId::new();
        assert_ne!(r1, r2);
        assert!(!r1.to_string().is_empty());
    }

    #[test]
    fn runner_id_from_string() {
        let r = RunnerId::from_string("worker-1");
        assert_eq!(&*r.0, "worker-1");
        assert_eq!(r.to_string(), "worker-1");
    }

    #[test]
    fn runner_id_default() {
        let r = RunnerId::default();
        assert!(!r.as_str().is_empty());
    }

    #[test]
    fn serde_round_trip_task_id() {
        let tid = TaskId::new("myapp", "process");
        let json = serde_json::to_string(&tid).unwrap();
        let back: TaskId = serde_json::from_str(&json).unwrap();
        assert_eq!(tid, back);
    }

    #[test]
    fn serde_round_trip_invocation_id() {
        let id = InvocationId::from_string("abc-123");
        let json = serde_json::to_string(&id).unwrap();
        let back: InvocationId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn serde_round_trip_call_id() {
        let cid = CallId::new(TaskId::new("m", "f"), "hash123");
        let json = serde_json::to_string(&cid).unwrap();
        let back: CallId = serde_json::from_str(&json).unwrap();
        assert_eq!(cid, back);
    }

    #[test]
    fn task_id_accessors() {
        let tid = TaskId::new("myapp.tasks", "process_order");
        assert_eq!(tid.module(), "myapp.tasks");
        assert_eq!(tid.name(), "process_order");
    }

    #[test]
    fn serde_backward_compat_missing_language() {
        // Old JSON without language is interpreted as Rust and normalized.
        let json = r#"{"module":"myapp","name":"process"}"#;
        let tid: TaskId = serde_json::from_str(json).unwrap();
        assert_eq!(tid.language(), TaskLanguage::Rust);
        assert_eq!(tid.module(), "myapp");
        assert_eq!(tid.name(), "process");
        assert_eq!(tid.to_string(), "rust::myapp.process");
    }

    #[test]
    fn serde_round_trip_python_task_id() {
        let tid = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");
        let json = serde_json::to_string(&tid).unwrap();
        let back: TaskId = serde_json::from_str(&json).unwrap();
        assert_eq!(tid, back);
        assert_eq!(back.language(), TaskLanguage::Python);
    }

    #[test]
    fn invocation_id_as_str() {
        let id = InvocationId::from_string("test-id");
        assert_eq!(id.as_str(), "test-id");
    }

    #[test]
    fn invocation_id_try_from_string_valid() {
        let uuid_str = uuid::Uuid::new_v4().to_string();
        let result = InvocationId::try_from_string(uuid_str.clone());
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_str(), uuid_str);
    }

    #[test]
    fn invocation_id_try_from_string_invalid() {
        let result = InvocationId::try_from_string("not-a-uuid");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid UUID"));
    }

    #[test]
    fn runner_id_as_str() {
        let r = RunnerId::from_string("worker-1");
        assert_eq!(r.as_str(), "worker-1");
    }

    // --- FromStr / parse_task_id tests ---

    #[test]
    fn parse_legacy_unqualified_task_id_as_rust() {
        let tid: TaskId = "myapp.tasks.process_order".parse().unwrap();
        assert_eq!(tid.module(), "myapp.tasks");
        assert_eq!(tid.name(), "process_order");
        assert_eq!(tid.language(), TaskLanguage::Rust);
        assert_eq!(tid.to_string(), "rust::myapp.tasks.process_order");
    }

    #[test]
    fn parse_language_qualified_task_id() {
        let tid: TaskId = "python::analytics.tasks.train_model".parse().unwrap();
        assert_eq!(tid.language(), TaskLanguage::Python);
        assert_eq!(tid.module(), "analytics.tasks");
        assert_eq!(tid.name(), "train_model");
    }

    #[test]
    fn parse_simple_task_id() {
        let tid: TaskId = "mod.func".parse().unwrap();
        assert_eq!(tid.module(), "mod");
        assert_eq!(tid.name(), "func");
    }

    #[test]
    fn parse_empty_string_fails() {
        let result = "".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty"));
    }

    #[test]
    fn parse_no_dot_fails() {
        let result = "nodot".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no '.'"));
    }

    #[test]
    fn parse_trailing_dot_fails() {
        let result = "module.".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty name"));
    }

    #[test]
    fn parse_leading_dot_fails() {
        let result = ".name".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty module"));
    }

    #[test]
    fn parse_empty_language_fails() {
        let result = "::mod.func".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty language"));
    }

    #[test]
    fn parse_qualified_id_without_function_fails() {
        let result = "rust::nodot".parse::<TaskId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no '.'"));
    }

    #[test]
    fn display_parse_round_trip_rust() {
        let original = TaskId::new("myapp.tasks", "process_order");
        let serialized = original.to_string();
        let parsed: TaskId = serialized.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn display_parse_round_trip_python() {
        let original = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train_model");
        let serialized = original.to_string();
        let parsed: TaskId = serialized.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn display_parse_round_trip_simple() {
        let original = TaskId::new("mod", "func");
        let serialized = original.to_string();
        let parsed: TaskId = serialized.parse().unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn display_parse_round_trip_dotted_module() {
        let original = TaskId::new("a.b.c", "func");
        let serialized = original.to_string();
        let parsed: TaskId = serialized.parse().unwrap();
        assert_eq!(original, parsed);
    }
}
