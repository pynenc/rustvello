//! Trigger system data types — conditions, contexts, and definitions.
//!
//! Mirrors pynenc's `trigger/` subsystem with Rust-idiomatic enums
//! for polymorphic dispatch instead of class hierarchies.

use serde::{Deserialize, Serialize};
use std::fmt;

mod condition;
mod context;
mod definition;
pub(crate) mod filter;
#[cfg(test)]
mod tests;

pub use condition::*;
pub use context::*;
pub use definition::*;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

/// SHA-256 hash-based identifier for a trigger condition.
///
/// Format matches pynenc's human-readable condition IDs exactly:
/// - Status: `condition#{task_id}#{sorted_statuses}#{filter_id}`
/// - Cron: `cron_{expression}`
/// - Event: `event#{event_code}#{filter_id}`
/// - Result: `{status_cid}_result_{result_filter_id}`
/// - Exception: `{status_cid}_exception_{types|any}`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConditionId(pub(crate) String);

impl ConditionId {
    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConditionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ConditionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for ConditionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ConditionId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// SHA-256 hash-based identifier for a trigger definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TriggerDefinitionId(String);

impl TriggerDefinitionId {
    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TriggerDefinitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TriggerDefinitionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for TriggerDefinitionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TriggerDefinitionId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Unique identifier for a trigger execution run (dedup key).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TriggerRunId(String);

impl TriggerRunId {
    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TriggerRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TriggerRunId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for TriggerRunId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TriggerRunId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

// ---------------------------------------------------------------------------
// Trigger logic
// ---------------------------------------------------------------------------

/// Logic for combining multiple conditions in a trigger definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TriggerLogic {
    /// All conditions must be satisfied.
    And,
    /// Any condition is sufficient.
    Or,
}

impl fmt::Display for TriggerLogic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}
