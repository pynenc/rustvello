use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::{InvocationStatus, StatusMachineError};
use std::fmt;

/// Classification of infrastructure failures.
///
/// Used to distinguish retriable (Connection, Timeout) from non-retriable
/// (Query, DataCorruption) infrastructure errors without string parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InfraErrorKind {
    /// Network or pool connection failure (retriable).
    Connection,
    /// Operation timed out (retriable).
    Timeout,
    /// Query or command failed (not retriable).
    Query,
    /// Stored data is corrupt or unparsable.
    DataCorruption,
    /// Uncategorised infrastructure error.
    Other,
}

impl fmt::Display for InfraErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connection => write!(f, "connection"),
            Self::Timeout => write!(f, "timeout"),
            Self::Query => write!(f, "query"),
            Self::DataCorruption => write!(f, "data_corruption"),
            Self::Other => write!(f, "other"),
        }
    }
}

impl InfraErrorKind {
    /// Returns `true` for errors that are typically transient and worth retrying.
    pub fn is_retriable(self) -> bool {
        matches!(self, Self::Connection | Self::Timeout)
    }
}

/// Root error type — mirrors pynenc's exception hierarchy.
///
/// Each variant maps 1:1 to a pynenc exception class (see 302 §4).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RustvelloError {
    // --- Retry errors (mirror RetryError) ---
    /// Task explicitly requested a retry
    #[error("retry requested: {reason}")]
    Retry { reason: String },

    /// Concurrency control triggered a retry
    #[error("concurrency retry: task {task_id} — {reason}")]
    ConcurrencyRetry { task_id: TaskId, reason: String },

    // --- Serialization (mirror SerializationError) ---
    /// Serialization/deserialization error
    #[error("serialization error: {message}")]
    Serialization { message: String },

    // --- Task errors (mirror TaskError hierarchy) ---
    /// Task definition not found in any registry
    #[error("task not found: {task_id}")]
    TaskNotFound { task_id: TaskId },

    /// Task exists but is not registered in this app instance
    #[error("task not registered: {task_id}")]
    TaskNotRegistered { task_id: TaskId },

    /// Dependency cycle detected in task graph
    #[error("cycle detected: {task_id} — {message}")]
    CycleDetected { task_id: TaskId, message: String },

    /// Runner cannot execute this task type
    #[error("runner not executable: {task_id} — {message}")]
    RunnerNotExecutable { task_id: TaskId, message: String },

    /// Task class/type not found during deserialization
    #[error("task class not found: {task_id}")]
    TaskClassNotFound { task_id: TaskId },

    // --- Invocation errors (mirror InvocationError hierarchy) ---
    /// Invocation not found
    #[error("invocation not found: {invocation_id}")]
    InvocationNotFound { invocation_id: InvocationId },

    /// Invalid status transition attempted
    #[error("invalid status transition: {from_status} -> {to_status}")]
    InvalidStatusTransition {
        invocation_id: InvocationId,
        from_status: InvocationStatus,
        to_status: InvocationStatus,
        allowed_statuses: Vec<InvocationStatus>,
    },

    /// Runner ownership violation during status transition
    #[error("ownership violation: {from_status} -> {to_status}, owner={current_owner}, requester={attempted_owner}")]
    OwnershipViolation {
        invocation_id: InvocationId,
        from_status: InvocationStatus,
        to_status: InvocationStatus,
        current_owner: String,
        attempted_owner: String,
        reason: String,
    },

    /// Status changed between read and write (optimistic locking failure)
    #[error("status race condition on {invocation_id}")]
    StatusRaceCondition {
        invocation_id: InvocationId,
        previous_status: InvocationStatus,
        expected_status: InvocationStatus,
        actual_status: InvocationStatus,
    },

    // --- Task execution errors ---
    /// Task function raised an error during execution.
    /// Carries the original exception type name for retry matching.
    #[error("task execution error ({error_type}): {message}")]
    TaskExecution {
        error_type: String,
        message: String,
        traceback: Option<String>,
    },

    // --- Infrastructure errors ---
    /// Unified infrastructure error with structured classification.
    #[error("infrastructure error ({kind}): {message}")]
    Infrastructure {
        kind: InfraErrorKind,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    // --- Config errors ---
    /// Configuration error
    #[error("configuration error: {message}")]
    Configuration { message: String },

    // --- Workflow errors ---
    /// A workflow operation was requested outside a runner-managed invocation.
    #[error("workflow context is unavailable outside a running invocation")]
    WorkflowContextUnavailable,

    /// A workflow operation was requested by an invocation with no workflow.
    #[error("invocation {invocation_id} is not a workflow member")]
    WorkflowMembershipRequired { invocation_id: InvocationId },

    /// A root-only workflow operation was requested by an ordinary member.
    #[error("invocation {invocation_id} is not the defining root of workflow {workflow_id}")]
    WorkflowRootRequired {
        invocation_id: InvocationId,
        workflow_id: InvocationId,
    },

    // --- Internal (no pynenc equivalent — for Rust-only bugs) ---
    /// Generic internal error
    #[error("internal error: {message}")]
    Internal { message: String },

    /// Backend does not support this operation.
    #[error("{backend} backend does not support {method}")]
    NotSupported { backend: String, method: String },
}

impl RustvelloError {
    /// State backend query/storage error (not retriable).
    pub fn state_backend(message: impl Into<String>) -> Self {
        Self::Infrastructure {
            kind: InfraErrorKind::Query,
            message: message.into(),
            source: None,
        }
    }

    /// Broker messaging error (not retriable).
    pub fn broker_err(message: impl Into<String>) -> Self {
        Self::Infrastructure {
            kind: InfraErrorKind::Query,
            message: message.into(),
            source: None,
        }
    }

    /// Runner infrastructure error.
    pub fn runner_err(message: impl Into<String>) -> Self {
        Self::Infrastructure {
            kind: InfraErrorKind::Other,
            message: message.into(),
            source: None,
        }
    }

    /// Connection failure (retriable).
    pub fn connection_err(message: impl Into<String>) -> Self {
        Self::Infrastructure {
            kind: InfraErrorKind::Connection,
            message: message.into(),
            source: None,
        }
    }
}

/// Convenience type alias for Results in Rustvello.
pub type RustvelloResult<T> = Result<T, RustvelloError>;

/// Convert a mutex lock-poison error into [`RustvelloError::Internal`].
///
/// Use this in backend implementations to avoid duplicating lock-error
/// conversion logic.
pub fn lock_err(e: impl std::fmt::Display) -> RustvelloError {
    RustvelloError::Internal {
        message: format!("lock poisoned: {}", e),
    }
}

/// Error information stored with a failed invocation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskError {
    pub error_type: String,
    pub message: String,
    pub traceback: Option<String>,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type, self.message)
    }
}

/// Convert a [`StatusMachineError`] into a [`RustvelloError`].
///
/// `fallback_from_status` is used when the transition error's `from` field
/// is `None` (i.e. the current status was not captured in the error itself).
pub fn status_machine_error_to_rustvello(
    e: StatusMachineError,
    invocation_id: &InvocationId,
    fallback_from_status: InvocationStatus,
) -> RustvelloError {
    match e {
        StatusMachineError::Transition(t) => RustvelloError::InvalidStatusTransition {
            invocation_id: invocation_id.clone(),
            from_status: t.from.unwrap_or(fallback_from_status),
            to_status: t.to,
            allowed_statuses: t.allowed,
        },
        StatusMachineError::Ownership(o) => RustvelloError::OwnershipViolation {
            invocation_id: invocation_id.clone(),
            from_status: o.from_status,
            to_status: o.to_status,
            current_owner: o.current_owner.unwrap_or_default(),
            attempted_owner: o.attempted_owner.unwrap_or_default(),
            reason: o.reason,
        },
        _ => RustvelloError::Internal {
            message: format!("unexpected status machine error: {e}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let e = RustvelloError::InvalidStatusTransition {
            invocation_id: InvocationId::from_string("inv-1"),
            from_status: InvocationStatus::Registered,
            to_status: InvocationStatus::Running,
            allowed_statuses: vec![InvocationStatus::Pending],
        };
        assert!(e.to_string().contains("REGISTERED"));
        assert!(e.to_string().contains("RUNNING"));

        let e = RustvelloError::InvocationNotFound {
            invocation_id: InvocationId::from_string("inv-1"),
        };
        assert!(e.to_string().contains("inv-1"));

        let e = RustvelloError::TaskNotRegistered {
            task_id: TaskId::new("mod", "func"),
        };
        assert!(e.to_string().contains("mod.func"));

        let e = RustvelloError::Serialization {
            message: "bad json".to_string(),
        };
        assert!(e.to_string().contains("bad json"));

        let e = RustvelloError::state_backend("disk full".to_string());
        assert!(e.to_string().contains("disk full"));

        let e = RustvelloError::Configuration {
            message: "bad config".to_string(),
        };
        assert!(e.to_string().contains("bad config"));

        let e = RustvelloError::broker_err("queue full".to_string());
        assert!(e.to_string().contains("queue full"));

        let e = RustvelloError::runner_err("timeout waiting for invocation inv-2".to_string());
        assert!(e.to_string().contains("inv-2"));

        let e = RustvelloError::Internal {
            message: "unexpected".to_string(),
        };
        assert!(e.to_string().contains("unexpected"));
    }

    #[test]
    fn task_error_display() {
        let te = TaskError {
            error_type: "ValueError".to_string(),
            message: "negative number".to_string(),
            traceback: Some("line 1".to_string()),
        };
        assert_eq!(te.to_string(), "ValueError: negative number");
    }

    #[test]
    fn task_error_serde() {
        let te = TaskError {
            error_type: "RuntimeError".to_string(),
            message: "oops".to_string(),
            traceback: None,
        };
        let json = serde_json::to_string(&te).unwrap();
        let back: TaskError = serde_json::from_str(&json).unwrap();
        assert_eq!(back.error_type, "RuntimeError");
        assert_eq!(back.message, "oops");
        assert!(back.traceback.is_none());
    }

    #[test]
    fn ownership_violation_display() {
        let e = RustvelloError::OwnershipViolation {
            invocation_id: InvocationId::from_string("inv-1"),
            from_status: InvocationStatus::Running,
            to_status: InvocationStatus::Success,
            current_owner: "runner-a".to_string(),
            attempted_owner: "runner-b".to_string(),
            reason: "status requires ownership".to_string(),
        };
        let s = e.to_string();
        assert!(s.contains("runner-a"));
        assert!(s.contains("runner-b"));
    }

    #[test]
    fn retry_display() {
        let e = RustvelloError::Retry {
            reason: "transient network error".to_string(),
        };
        let s = e.to_string();
        assert!(s.contains("retry"));
        assert!(s.contains("transient network error"));
    }

    #[test]
    fn concurrency_retry_display() {
        let e = RustvelloError::ConcurrencyRetry {
            task_id: TaskId::new("mod", "my_task"),
            reason: "task-level lock held".to_string(),
        };
        let s = e.to_string();
        assert!(s.contains("mod.my_task"));
        assert!(s.contains("task-level lock held"));
    }

    #[test]
    fn lock_err_converts_to_internal() {
        let e = lock_err("PoisonError { .. }");
        match e {
            RustvelloError::Internal { message } => assert!(message.contains("lock poisoned")),
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn status_race_condition_display() {
        let e = RustvelloError::StatusRaceCondition {
            invocation_id: InvocationId::from_string("inv-1"),
            previous_status: InvocationStatus::Pending,
            expected_status: InvocationStatus::Running,
            actual_status: InvocationStatus::Failed,
        };
        let s = e.to_string();
        assert!(s.contains("inv-1"));
    }

    #[test]
    fn runner_error_display() {
        let e = RustvelloError::runner_err("process exited with code 1".to_string());
        assert!(e.to_string().contains("process exited"));
    }

    #[test]
    fn task_error_variants_display() {
        let tid = TaskId::new("mymod", "myfunc");

        let e = RustvelloError::TaskNotFound {
            task_id: tid.clone(),
        };
        assert!(e.to_string().contains("mymod.myfunc"));

        let e = RustvelloError::CycleDetected {
            task_id: tid.clone(),
            message: "A -> B -> A".to_string(),
        };
        assert!(e.to_string().contains("cycle"));

        let e = RustvelloError::TaskClassNotFound {
            task_id: tid.clone(),
        };
        assert!(e.to_string().contains("class not found"));
    }
}
