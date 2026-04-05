use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::config::ArgumentPrintMode;
use crate::identifiers::{CallId, TaskId};

/// Serialized arguments for a task call.
///
/// Arguments are stored as a sorted map of key-value pairs where
/// values are JSON-serialized strings. Sorting ensures deterministic
/// hashing for deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedArguments(pub BTreeMap<String, String>);

impl SerializedArguments {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.0.insert(key.into(), value.into());
    }

    /// Compute a deterministic hash of the serialized arguments.
    /// Used to generate the `args_id` component of `CallId`.
    /// Returns `"no_args"` for empty argument maps (matches pynenc convention).
    pub fn compute_args_id(&self) -> String {
        if self.0.is_empty() {
            return "no_args".to_string();
        }
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for (k, v) in &self.0 {
            // Use JSON-escaped keys and values to prevent delimiter collisions
            let ek = serde_json::to_string(k).unwrap_or_else(|_| k.clone());
            let ev = serde_json::to_string(v).unwrap_or_else(|_| v.clone());
            hasher.update(ek.as_bytes());
            hasher.update(b"=");
            hasher.update(ev.as_bytes());
            hasher.update(b";");
        }
        format!("{:x}", hasher.finalize())
    }
}

impl Default for SerializedArguments {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializedArguments {
    /// Convert arguments into individual `(key, value)` pairs for per-pair
    /// concurrency-control indexing.
    ///
    /// When the argument map is present but empty, a sentinel `("", "")` pair
    /// is returned so that `Some(empty_args)` is distinguishable from `None`
    /// (which means CC is disabled for this invocation).
    ///
    /// Backend implementations should always call this method instead of
    /// manually iterating `self.0` to ensure the sentinel convention is
    /// applied consistently.
    pub fn cc_arg_pairs(&self) -> Vec<(String, String)> {
        if self.0.is_empty() {
            vec![(String::new(), String::new())]
        } else {
            self.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        }
    }
}

impl SerializedArguments {
    /// Compute a concurrency control key from optional arguments.
    ///
    /// Returns an empty string when `args` is `None` or empty,
    /// otherwise returns the deterministic args hash.
    pub fn cc_key(args: Option<&Self>) -> String {
        match args {
            None => String::new(),
            Some(a) if a.0.is_empty() => String::new(),
            Some(a) => a.compute_args_id(),
        }
    }

    /// Format arguments for display using the given print mode.
    pub fn display(&self, mode: ArgumentPrintMode, truncate_length: usize) -> String {
        if self.0.is_empty() {
            return "<no_args>".to_string();
        }
        match mode {
            ArgumentPrintMode::Hidden => "<arguments hidden>".to_string(),
            ArgumentPrintMode::Keys => {
                let keys: Vec<&str> = self.0.keys().map(std::string::String::as_str).collect();
                format!("{{{}}}", keys.join(", "))
            }
            ArgumentPrintMode::Full => {
                let pairs: Vec<String> = self.0.iter().map(|(k, v)| format!("{k}: {v}")).collect();
                format!("{{{}}}", pairs.join(", "))
            }
            ArgumentPrintMode::Truncated => {
                let pairs: Vec<String> = self
                    .0
                    .iter()
                    .map(|(k, v)| {
                        if v.len() > truncate_length {
                            // Safe UTF-8 truncation: find the last char boundary
                            let end = v
                                .char_indices()
                                .nth(truncate_length)
                                .map_or(v.len(), |(i, _)| i);
                            format!("{k}: {}...", &v[..end])
                        } else {
                            format!("{k}: {v}")
                        }
                    })
                    .collect();
                format!("{{{}}}", pairs.join(", "))
            }
        }
    }
}

/// A call represents a task with specific arguments, ready to be invoked.
///
/// This is the DTO form suitable for persistence and wire transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallDTO {
    pub call_id: CallId,
    pub task_id: TaskId,
    pub serialized_arguments: SerializedArguments,
}

impl CallDTO {
    pub fn new(task_id: TaskId, args: SerializedArguments) -> Self {
        let args_id = args.compute_args_id();
        let call_id = CallId::new(task_id.clone(), args_id);
        Self {
            call_id,
            task_id,
            serialized_arguments: args,
        }
    }
}

impl std::fmt::Display for CallDTO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys: Vec<&str> = self
            .serialized_arguments
            .0
            .keys()
            .map(std::string::String::as_str)
            .collect();
        write!(
            f,
            "Call(task={}, arguments=[{}])",
            self.task_id,
            keys.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_id_is_deterministic() {
        let mut args1 = SerializedArguments::new();
        args1.insert("x", "42");
        args1.insert("y", "hello");

        let mut args2 = SerializedArguments::new();
        args2.insert("y", "hello");
        args2.insert("x", "42");

        // BTreeMap ensures sorted order, so same args_id regardless of insert order
        assert_eq!(args1.compute_args_id(), args2.compute_args_id());
    }

    #[test]
    fn different_args_different_id() {
        let mut args1 = SerializedArguments::new();
        args1.insert("x", "42");

        let mut args2 = SerializedArguments::new();
        args2.insert("x", "43");

        assert_ne!(args1.compute_args_id(), args2.compute_args_id());
    }

    #[test]
    fn empty_args_id() {
        let args = SerializedArguments::new();
        let id = args.compute_args_id();
        assert!(!id.is_empty());
        // Empty args should be deterministic
        let args2 = SerializedArguments::default();
        assert_eq!(id, args2.compute_args_id());
    }

    #[test]
    fn call_dto_new() {
        let task_id = TaskId::new("mod", "func");
        let mut args = SerializedArguments::new();
        args.insert("a", "1");
        let call = CallDTO::new(task_id.clone(), args.clone());

        assert_eq!(call.task_id, task_id);
        assert_eq!(call.call_id.task_id, task_id);
        assert_eq!(&*call.call_id.args_id, args.compute_args_id());
        assert_eq!(call.serialized_arguments, args);
    }

    #[test]
    fn serde_round_trip_call_dto() {
        let task_id = TaskId::new("mod", "func");
        let mut args = SerializedArguments::new();
        args.insert("key", "val");
        let call = CallDTO::new(task_id, args);

        let json = serde_json::to_string(&call).unwrap();
        let back: CallDTO = serde_json::from_str(&json).unwrap();
        assert_eq!(back.call_id, call.call_id);
        assert_eq!(back.task_id, call.task_id);
        assert_eq!(back.serialized_arguments, call.serialized_arguments);
    }

    #[test]
    fn args_id_no_delimiter_collision() {
        // {"a": "b;c=d"} must differ from {"a": "b", "c": "d"}
        let mut args1 = SerializedArguments::new();
        args1.insert("a", "b;c=d");

        let mut args2 = SerializedArguments::new();
        args2.insert("a", "b");
        args2.insert("c", "d");

        assert_ne!(args1.compute_args_id(), args2.compute_args_id());
    }

    #[test]
    fn truncated_display_safe_on_multibyte_utf8() {
        let mut args = SerializedArguments::new();
        // Each Japanese char is 3 bytes in UTF-8
        args.insert("x", "日本語テスト");
        // Truncate at 2 chars — should not panic
        let result = args.display(ArgumentPrintMode::Truncated, 2);
        assert!(result.contains("日本..."));
    }
}
