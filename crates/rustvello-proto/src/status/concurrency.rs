use serde::{Deserialize, Serialize};
use std::fmt;

/// Concurrency control strategy for a task.
///
/// Determines how concurrent invocations of the same task are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConcurrencyControlType {
    /// No concurrency restrictions
    Unlimited,
    /// Only one invocation per (task_id, key_args) at a time
    Task,
    /// Only one invocation per exact argument set
    Argument,
    /// No concurrent invocations allowed at all
    None,
}

impl Default for ConcurrencyControlType {
    fn default() -> Self {
        Self::Unlimited
    }
}

impl fmt::Display for ConcurrencyControlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unlimited => f.write_str("Unlimited"),
            Self::Task => f.write_str("Task"),
            Self::Argument => f.write_str("Argument"),
            Self::None => f.write_str("None"),
        }
    }
}

impl std::str::FromStr for ConcurrencyControlType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "UNLIMITED" | "DISABLED" => Ok(Self::Unlimited),
            "TASK" => Ok(Self::Task),
            "ARGUMENT" | "ARGUMENTS" | "KEYS" => Ok(Self::Argument),
            "NONE" => Ok(Self::None),
            _ => Err(format!("unknown ConcurrencyControlType: {s:?}")),
        }
    }
}
