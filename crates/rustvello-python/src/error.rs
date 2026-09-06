use pyo3::create_exception;
use pyo3::prelude::*;
use rustvello_core::error::RustvelloError as CoreError;

// ─── Exception hierarchy ────────────────────────────────────────────
//
// Mirrors pynenc's exception tree (see 302 §4).
// The first argument to create_exception! is the Python module name.

// Base
create_exception!(rustvello, RustvelloError, pyo3::exceptions::PyException);

// Retry
create_exception!(rustvello, RetryError, RustvelloError);
create_exception!(rustvello, ConcurrencyRetryError, RetryError);

// Serialization
create_exception!(rustvello, SerializationError, RustvelloError);

// Task hierarchy
create_exception!(rustvello, TaskError, RustvelloError);
create_exception!(rustvello, TaskNotFoundError, TaskError);
create_exception!(rustvello, TaskNotRegisteredError, TaskError);
create_exception!(rustvello, CycleDetectedError, TaskError);
create_exception!(rustvello, RunnerNotExecutableError, TaskError);
create_exception!(rustvello, TaskClassNotFoundError, TaskError);

// Invocation hierarchy
create_exception!(rustvello, InvocationError, RustvelloError);
create_exception!(rustvello, InvocationNotFoundError, InvocationError);

// Status hierarchy (under Invocation)
create_exception!(rustvello, InvocationStatusError, InvocationError);
create_exception!(rustvello, StatusTransitionError, InvocationStatusError);
create_exception!(rustvello, StatusOwnershipError, InvocationStatusError);
create_exception!(rustvello, StatusRaceConditionError, InvocationStatusError);

// Infrastructure
create_exception!(rustvello, StateBackendError, RustvelloError);
create_exception!(rustvello, BrokerError, RustvelloError);
create_exception!(rustvello, RunnerError, RustvelloError);
create_exception!(rustvello, ConfigurationError, RustvelloError);

// Internal
create_exception!(rustvello, InternalError, RustvelloError);

// ─── Error conversion ───────────────────────────────────────────────

/// Convert a [`CoreError`] to the matching typed Python exception.
///
/// Uses [`Python::with_gil`] internally so callers can pass this as
/// `.map_err(to_py_err)` without threading a `Python<'_>` token.
pub fn to_py_err(e: CoreError) -> PyErr {
    Python::with_gil(|py| to_py_err_impl(py, e))
}

fn to_py_err_impl(py: Python<'_>, e: CoreError) -> PyErr {
    match e {
        // ── Retry ───────────────────────────────────────────────
        CoreError::Retry { reason } => {
            let err = RetryError::new_err(format!("retry requested: {reason}"));
            let _ = err.value_bound(py).setattr("reason", &*reason);
            err
        }
        CoreError::ConcurrencyRetry { task_id, reason } => {
            let err =
                ConcurrencyRetryError::new_err(format!("concurrency retry: {task_id} — {reason}"));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("task_id", task_id.to_string());
                let _ = val.setattr("reason", &*reason);
            }
            err
        }

        // ── Serialization ───────────────────────────────────────
        CoreError::Serialization { message } => SerializationError::new_err(message),

        // ── Task ────────────────────────────────────────────────
        CoreError::TaskNotFound { task_id } => {
            let err = TaskNotFoundError::new_err(format!("task not found: {task_id}"));
            let _ = err.value_bound(py).setattr("task_id", task_id.to_string());
            err
        }
        CoreError::TaskNotRegistered { task_id } => {
            let err = TaskNotRegisteredError::new_err(format!("task not registered: {task_id}"));
            let _ = err.value_bound(py).setattr("task_id", task_id.to_string());
            err
        }
        CoreError::CycleDetected { task_id, message } => {
            let err = CycleDetectedError::new_err(format!("cycle detected: {task_id} — {message}"));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("task_id", task_id.to_string());
                let _ = val.setattr("message", &*message);
            }
            err
        }
        CoreError::RunnerNotExecutable { task_id, message } => {
            let err = RunnerNotExecutableError::new_err(format!(
                "runner not executable: {task_id} — {message}"
            ));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("task_id", task_id.to_string());
                let _ = val.setattr("message", &*message);
            }
            err
        }
        CoreError::TaskClassNotFound { task_id } => {
            let err = TaskClassNotFoundError::new_err(format!("task class not found: {task_id}"));
            let _ = err.value_bound(py).setattr("task_id", task_id.to_string());
            err
        }

        // ── Invocation ──────────────────────────────────────────
        CoreError::InvocationNotFound { invocation_id } => {
            let err =
                InvocationNotFoundError::new_err(format!("invocation not found: {invocation_id}"));
            let _ = err
                .value_bound(py)
                .setattr("invocation_id", invocation_id.to_string());
            err
        }

        // ── Status ──────────────────────────────────────────────
        CoreError::InvalidStatusTransition {
            invocation_id,
            from_status,
            to_status,
            allowed_statuses,
        } => {
            let err = StatusTransitionError::new_err(format!(
                "invalid status transition: {from_status} → {to_status}"
            ));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("invocation_id", invocation_id.to_string());
                let _ = val.setattr("from_status", from_status.to_string());
                let _ = val.setattr("to_status", to_status.to_string());
                let _ = val.setattr(
                    "allowed_statuses",
                    allowed_statuses
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>(),
                );
            }
            err
        }
        CoreError::OwnershipViolation {
            invocation_id,
            from_status,
            to_status,
            current_owner,
            attempted_owner,
            reason,
        } => {
            let err = StatusOwnershipError::new_err(format!(
                "ownership violation: {from_status} → {to_status}, \
                 owner={current_owner}, requester={attempted_owner}"
            ));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("invocation_id", invocation_id.to_string());
                let _ = val.setattr("from_status", from_status.to_string());
                let _ = val.setattr("to_status", to_status.to_string());
                let _ = val.setattr("current_owner", &*current_owner);
                let _ = val.setattr("attempted_owner", &*attempted_owner);
                let _ = val.setattr("reason", &*reason);
            }
            err
        }
        CoreError::StatusRaceCondition {
            invocation_id,
            previous_status,
            expected_status,
            actual_status,
        } => {
            let err = StatusRaceConditionError::new_err(format!(
                "status race condition on {invocation_id}"
            ));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("invocation_id", invocation_id.to_string());
                let _ = val.setattr("previous_status", previous_status.to_string());
                let _ = val.setattr("expected_status", expected_status.to_string());
                let _ = val.setattr("actual_status", actual_status.to_string());
            }
            err
        }

        // ── Infrastructure ──────────────────────────────────────
        CoreError::TaskExecution {
            error_type,
            message,
            traceback,
        } => {
            let err = RunnerError::new_err(format!("{error_type}: {message}"));
            {
                let val = err.value_bound(py);
                let _ = val.setattr("error_type", &*error_type);
                let _ = val.setattr("message", &*message);
                let _ = val.setattr("traceback", traceback.as_deref());
            }
            err
        }
        CoreError::Infrastructure { kind, message, .. } => {
            use rustvello_core::error::InfraErrorKind;
            match kind {
                InfraErrorKind::Other => RunnerError::new_err(message),
                _ => StateBackendError::new_err(message),
            }
        }

        // ── Config ──────────────────────────────────────────────
        CoreError::Configuration { message } => ConfigurationError::new_err(message),

        // ── Internal ────────────────────────────────────────────
        CoreError::Internal { message } => InternalError::new_err(message),

        // Non-exhaustive catch-all for future variants
        _ => RustvelloError::new_err(e.to_string()),
    }
}

/// Register all exception classes in the given Python module.
pub fn register_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Base
    m.add("RustvelloError", py.get_type_bound::<RustvelloError>())?;

    // Retry
    m.add("RetryError", py.get_type_bound::<RetryError>())?;
    m.add(
        "ConcurrencyRetryError",
        py.get_type_bound::<ConcurrencyRetryError>(),
    )?;

    // Serialization
    m.add(
        "SerializationError",
        py.get_type_bound::<SerializationError>(),
    )?;

    // Task
    m.add("TaskError", py.get_type_bound::<TaskError>())?;
    m.add(
        "TaskNotFoundError",
        py.get_type_bound::<TaskNotFoundError>(),
    )?;
    m.add(
        "TaskNotRegisteredError",
        py.get_type_bound::<TaskNotRegisteredError>(),
    )?;
    m.add(
        "CycleDetectedError",
        py.get_type_bound::<CycleDetectedError>(),
    )?;
    m.add(
        "RunnerNotExecutableError",
        py.get_type_bound::<RunnerNotExecutableError>(),
    )?;
    m.add(
        "TaskClassNotFoundError",
        py.get_type_bound::<TaskClassNotFoundError>(),
    )?;

    // Invocation
    m.add("InvocationError", py.get_type_bound::<InvocationError>())?;
    m.add(
        "InvocationNotFoundError",
        py.get_type_bound::<InvocationNotFoundError>(),
    )?;

    // Status
    m.add(
        "InvocationStatusError",
        py.get_type_bound::<InvocationStatusError>(),
    )?;
    m.add(
        "StatusTransitionError",
        py.get_type_bound::<StatusTransitionError>(),
    )?;
    m.add(
        "StatusOwnershipError",
        py.get_type_bound::<StatusOwnershipError>(),
    )?;
    m.add(
        "StatusRaceConditionError",
        py.get_type_bound::<StatusRaceConditionError>(),
    )?;

    // Infrastructure
    m.add(
        "StateBackendError",
        py.get_type_bound::<StateBackendError>(),
    )?;
    m.add("BrokerError", py.get_type_bound::<BrokerError>())?;
    m.add("RunnerError", py.get_type_bound::<RunnerError>())?;
    m.add(
        "ConfigurationError",
        py.get_type_bound::<ConfigurationError>(),
    )?;

    // Internal
    m.add("InternalError", py.get_type_bound::<InternalError>())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use rustvello_proto::identifiers::InvocationId;
    use rustvello_proto::status::InvocationStatus;

    // ── Type mapping ────────────────────────────────────────────────

    #[test]
    fn retry_maps_to_retry_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::Retry {
                reason: "transient".into(),
            });
            assert!(err.is_instance_of::<RetryError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn concurrency_retry_maps_to_concurrency_retry_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::ConcurrencyRetry {
                task_id: "mod.task".parse().unwrap(),
                reason: "locked".into(),
            });
            assert!(err.is_instance_of::<ConcurrencyRetryError>(py));
            assert!(err.is_instance_of::<RetryError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn serialization_maps_to_serialization_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::Serialization {
                message: "bad json".into(),
            });
            assert!(err.is_instance_of::<SerializationError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn task_not_found_maps_to_task_not_found_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::TaskNotFound {
                task_id: "mod.func".parse().unwrap(),
            });
            assert!(err.is_instance_of::<TaskNotFoundError>(py));
            assert!(err.is_instance_of::<TaskError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn task_not_registered_maps_to_task_not_registered_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::TaskNotRegistered {
                task_id: "mod.my_task".parse().unwrap(),
            });
            assert!(err.is_instance_of::<TaskNotRegisteredError>(py));
            assert!(err.is_instance_of::<TaskError>(py));
        });
    }

    #[test]
    fn invocation_not_found_maps_to_invocation_not_found_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::InvocationNotFound {
                invocation_id: InvocationId::from_string("abc-123"),
            });
            assert!(err.is_instance_of::<InvocationNotFoundError>(py));
            assert!(err.is_instance_of::<InvocationError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn status_transition_maps_to_status_transition_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::InvalidStatusTransition {
                invocation_id: InvocationId::from_string("inv-1"),
                from_status: InvocationStatus::Registered,
                to_status: InvocationStatus::Running,
                allowed_statuses: vec![InvocationStatus::Pending],
            });
            assert!(err.is_instance_of::<StatusTransitionError>(py));
            assert!(err.is_instance_of::<InvocationStatusError>(py));
            assert!(err.is_instance_of::<InvocationError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn ownership_violation_maps_to_status_ownership_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::OwnershipViolation {
                invocation_id: InvocationId::from_string("inv-2"),
                from_status: InvocationStatus::Pending,
                to_status: InvocationStatus::Running,
                current_owner: "runner-a".into(),
                attempted_owner: "runner-b".into(),
                reason: "already owned".into(),
            });
            assert!(err.is_instance_of::<StatusOwnershipError>(py));
            assert!(err.is_instance_of::<InvocationStatusError>(py));
        });
    }

    #[test]
    fn race_condition_maps_to_status_race_condition_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::StatusRaceCondition {
                invocation_id: InvocationId::from_string("inv-3"),
                previous_status: InvocationStatus::Registered,
                expected_status: InvocationStatus::Pending,
                actual_status: InvocationStatus::Running,
            });
            assert!(err.is_instance_of::<StatusRaceConditionError>(py));
            assert!(err.is_instance_of::<InvocationStatusError>(py));
        });
    }

    #[test]
    fn state_backend_maps_to_state_backend_error() {
        Python::with_gil(|py| {
            use rustvello_core::error::InfraErrorKind;
            let err = to_py_err(CoreError::Infrastructure {
                kind: InfraErrorKind::Query,
                message: "disk full".into(),
                source: None,
            });
            assert!(err.is_instance_of::<StateBackendError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn broker_maps_to_broker_error() {
        Python::with_gil(|py| {
            use rustvello_core::error::InfraErrorKind;
            let err = to_py_err(CoreError::Infrastructure {
                kind: InfraErrorKind::Connection,
                message: "connection refused".into(),
                source: None,
            });
            // Connection errors map to StateBackendError (non-Other kind)
            assert!(err.is_instance_of::<StateBackendError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn runner_maps_to_runner_error() {
        Python::with_gil(|py| {
            use rustvello_core::error::InfraErrorKind;
            let err = to_py_err(CoreError::Infrastructure {
                kind: InfraErrorKind::Other,
                message: "task failed".into(),
                source: None,
            });
            assert!(err.is_instance_of::<RunnerError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn configuration_maps_to_configuration_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::Configuration {
                message: "missing field".into(),
            });
            assert!(err.is_instance_of::<ConfigurationError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    #[test]
    fn internal_maps_to_internal_error() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::Internal {
                message: "lock contention".into(),
            });
            assert!(err.is_instance_of::<InternalError>(py));
            assert!(err.is_instance_of::<RustvelloError>(py));
        });
    }

    // ── Structured attributes ───────────────────────────────────────

    #[test]
    fn status_transition_carries_structured_attrs() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::InvalidStatusTransition {
                invocation_id: InvocationId::from_string("inv-1"),
                from_status: InvocationStatus::Registered,
                to_status: InvocationStatus::Running,
                allowed_statuses: vec![InvocationStatus::Pending],
            });
            let val = err.value_bound(py);
            let inv_id: String = val.getattr("invocation_id").unwrap().extract().unwrap();
            assert_eq!(inv_id, "inv-1");
            let from: String = val.getattr("from_status").unwrap().extract().unwrap();
            assert!(from.contains("REGISTERED"));
            let to: String = val.getattr("to_status").unwrap().extract().unwrap();
            assert!(to.contains("RUNNING"));
            let allowed: Vec<String> = val.getattr("allowed_statuses").unwrap().extract().unwrap();
            assert_eq!(allowed.len(), 1);
            assert!(allowed[0].contains("PENDING"));
        });
    }

    #[test]
    fn ownership_violation_carries_structured_attrs() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::OwnershipViolation {
                invocation_id: InvocationId::from_string("inv-2"),
                from_status: InvocationStatus::Pending,
                to_status: InvocationStatus::Running,
                current_owner: "runner-a".into(),
                attempted_owner: "runner-b".into(),
                reason: "already owned".into(),
            });
            let val = err.value_bound(py);
            let owner: String = val.getattr("current_owner").unwrap().extract().unwrap();
            assert_eq!(owner, "runner-a");
            let requester: String = val.getattr("attempted_owner").unwrap().extract().unwrap();
            assert_eq!(requester, "runner-b");
            let reason: String = val.getattr("reason").unwrap().extract().unwrap();
            assert_eq!(reason, "already owned");
        });
    }

    #[test]
    fn race_condition_carries_structured_attrs() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::StatusRaceCondition {
                invocation_id: InvocationId::from_string("inv-3"),
                previous_status: InvocationStatus::Registered,
                expected_status: InvocationStatus::Pending,
                actual_status: InvocationStatus::Running,
            });
            let val = err.value_bound(py);
            let prev: String = val.getattr("previous_status").unwrap().extract().unwrap();
            assert!(prev.contains("REGISTERED"));
            let actual: String = val.getattr("actual_status").unwrap().extract().unwrap();
            assert!(actual.contains("RUNNING"));
        });
    }

    #[test]
    fn concurrency_retry_carries_structured_attrs() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::ConcurrencyRetry {
                task_id: "mod.task".parse().unwrap(),
                reason: "locked".into(),
            });
            let val = err.value_bound(py);
            let tid: String = val.getattr("task_id").unwrap().extract().unwrap();
            assert_eq!(tid, "mod.task");
            let reason: String = val.getattr("reason").unwrap().extract().unwrap();
            assert_eq!(reason, "locked");
        });
    }

    #[test]
    fn invocation_not_found_carries_invocation_id() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::InvocationNotFound {
                invocation_id: InvocationId::from_string("abc-123"),
            });
            let val = err.value_bound(py);
            let inv_id: String = val.getattr("invocation_id").unwrap().extract().unwrap();
            assert_eq!(inv_id, "abc-123");
        });
    }

    #[test]
    fn task_not_found_carries_task_id() {
        Python::with_gil(|py| {
            let err = to_py_err(CoreError::TaskNotFound {
                task_id: "mod.func".parse().unwrap(),
            });
            let val = err.value_bound(py);
            let tid: String = val.getattr("task_id").unwrap().extract().unwrap();
            assert_eq!(tid, "rust::mod.func");
        });
    }
}
