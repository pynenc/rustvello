//! Condition context types for trigger evaluation.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use crate::identifiers::TaskId;
use crate::status::InvocationStatus;

/// Context data for a cron evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronContext {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub last_execution: Option<chrono::DateTime<chrono::Utc>>,
}

/// Context for an invocation status change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusContext {
    pub invocation_id: crate::identifiers::InvocationId,
    pub task_id: TaskId,
    pub status: InvocationStatus,
    /// Serialized invocation arguments (kwargs as key→JSON-string).
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
}

/// Context for a custom application event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventContext {
    pub event_id: String,
    pub event_code: String,
    pub payload: serde_json::Value,
}

/// Context for a successful task result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultContext {
    pub invocation_id: crate::identifiers::InvocationId,
    pub task_id: TaskId,
    /// The task result as a JSON value.
    pub result: serde_json::Value,
    /// Serialized invocation arguments (kwargs as key→JSON-string).
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
}

/// Context for a task failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionContext {
    pub invocation_id: crate::identifiers::InvocationId,
    pub task_id: TaskId,
    pub error_type: String,
    pub error_message: String,
    /// Serialized invocation arguments (kwargs as key→JSON-string).
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
}

/// Polymorphic condition context — matches condition variants 1:1.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConditionContext {
    Cron(CronContext),
    Status(StatusContext),
    Event(EventContext),
    Result(ResultContext),
    Exception(ExceptionContext),
}

impl std::fmt::Display for ConditionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cron(c) => write!(f, "CronCtx({})", c.timestamp),
            Self::Status(c) => write!(f, "StatusCtx({}, {})", c.invocation_id, c.status),
            Self::Event(c) => write!(f, "EventCtx({})", c.event_code),
            Self::Result(c) => write!(f, "ResultCtx({})", c.invocation_id),
            Self::Exception(c) => write!(f, "ExceptionCtx({})", c.invocation_id),
        }
    }
}

impl ConditionContext {
    /// Compute a deterministic context ID for dedup.
    pub fn context_id(&self) -> String {
        let mut hasher = Sha256::new();
        match self {
            Self::Cron(c) => {
                hasher.update(b"cron_ctx:");
                hasher.update(c.timestamp.to_rfc3339().as_bytes());
            }
            Self::Status(c) => {
                hasher.update(b"status_ctx:");
                hasher.update(c.invocation_id.as_str().as_bytes());
                hasher.update(b":");
                hasher.update(c.status.to_string().as_bytes());
            }
            Self::Event(c) => {
                hasher.update(b"event_ctx:");
                hasher.update(c.event_id.as_bytes());
            }
            Self::Result(c) => {
                hasher.update(b"result_ctx:");
                hasher.update(c.invocation_id.as_str().as_bytes());
            }
            Self::Exception(c) => {
                hasher.update(b"exception_ctx:");
                hasher.update(c.invocation_id.as_str().as_bytes());
            }
        }
        format!("{:x}", hasher.finalize())
    }
}
