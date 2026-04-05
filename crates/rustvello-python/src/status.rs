use pyo3::prelude::*;

use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

/// Convert a status name (any case, e.g. "SUCCESS" or "success") to
/// the Rust serde JSON representation (PascalCase, e.g. "Success").
///
/// This lets Python always ask Rust for the canonical serde form instead
/// of reimplementing the mapping.
#[pyfunction]
pub fn status_to_serde(name: &str) -> PyResult<String> {
    let status: InvocationStatus = name
        .parse()
        .map_err(pyo3::exceptions::PyValueError::new_err)?;
    // Serialize to a JSON string like `"Success"`, then strip the quotes
    let json = serde_json::to_string(&status)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    // serde_json::to_string wraps in quotes: "\"Success\""
    Ok(json.trim_matches('"').to_string())
}

/// Convert a serde-format status (PascalCase, e.g. "ConcurrencyControlled")
/// back to the UPPER_SNAKE_CASE name Python uses (e.g. "CONCURRENCY_CONTROLLED").
#[pyfunction]
pub fn status_from_serde(serde_name: &str) -> PyResult<String> {
    let json_str = format!("\"{serde_name}\"");
    let status: InvocationStatus = serde_json::from_str(&json_str)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    Ok(status.to_string()) // Display = UPPER_SNAKE_CASE
}

/// Python wrapper for InvocationStatus.
#[pyclass(name = "InvocationStatus")]
#[derive(Clone)]
pub struct PyInvocationStatus {
    pub inner: InvocationStatus,
}

#[pymethods]
impl PyInvocationStatus {
    #[staticmethod]
    fn registered() -> Self {
        Self {
            inner: InvocationStatus::Registered,
        }
    }

    #[staticmethod]
    fn pending() -> Self {
        Self {
            inner: InvocationStatus::Pending,
        }
    }

    #[staticmethod]
    fn running() -> Self {
        Self {
            inner: InvocationStatus::Running,
        }
    }

    #[staticmethod]
    fn success() -> Self {
        Self {
            inner: InvocationStatus::Success,
        }
    }

    #[staticmethod]
    fn failed() -> Self {
        Self {
            inner: InvocationStatus::Failed,
        }
    }

    #[staticmethod]
    fn retry() -> Self {
        Self {
            inner: InvocationStatus::Retry,
        }
    }

    #[staticmethod]
    fn concurrency_controlled() -> Self {
        Self {
            inner: InvocationStatus::ConcurrencyControlled,
        }
    }

    #[staticmethod]
    fn concurrency_controlled_final() -> Self {
        Self {
            inner: InvocationStatus::ConcurrencyControlledFinal,
        }
    }

    #[staticmethod]
    fn rerouted() -> Self {
        Self {
            inner: InvocationStatus::Rerouted,
        }
    }

    #[staticmethod]
    fn pending_recovery() -> Self {
        Self {
            inner: InvocationStatus::PendingRecovery,
        }
    }

    #[staticmethod]
    fn running_recovery() -> Self {
        Self {
            inner: InvocationStatus::RunningRecovery,
        }
    }

    fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("InvocationStatus.{}", self.inner)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut h);
        h.finish()
    }
}

/// Python wrapper for ConcurrencyControlType.
#[pyclass(name = "ConcurrencyControlType")]
#[derive(Clone)]
pub struct PyConcurrencyControlType {
    pub inner: ConcurrencyControlType,
}

#[pymethods]
impl PyConcurrencyControlType {
    #[staticmethod]
    fn unlimited() -> Self {
        Self {
            inner: ConcurrencyControlType::Unlimited,
        }
    }

    #[staticmethod]
    fn task() -> Self {
        Self {
            inner: ConcurrencyControlType::Task,
        }
    }

    #[staticmethod]
    fn argument() -> Self {
        Self {
            inner: ConcurrencyControlType::Argument,
        }
    }

    #[staticmethod]
    fn none() -> Self {
        Self {
            inner: ConcurrencyControlType::None,
        }
    }

    fn __str__(&self) -> String {
        format!("{:?}", self.inner)
    }

    fn __repr__(&self) -> String {
        format!("ConcurrencyControlType.{:?}", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── PyInvocationStatus constructors ──────────────────────────

    #[test]
    fn all_status_constructors_have_correct_inner() {
        assert_eq!(
            PyInvocationStatus::registered().inner,
            InvocationStatus::Registered
        );
        assert_eq!(
            PyInvocationStatus::pending().inner,
            InvocationStatus::Pending
        );
        assert_eq!(
            PyInvocationStatus::running().inner,
            InvocationStatus::Running
        );
        assert_eq!(
            PyInvocationStatus::success().inner,
            InvocationStatus::Success
        );
        assert_eq!(PyInvocationStatus::failed().inner, InvocationStatus::Failed);
        assert_eq!(PyInvocationStatus::retry().inner, InvocationStatus::Retry);
        assert_eq!(
            PyInvocationStatus::concurrency_controlled().inner,
            InvocationStatus::ConcurrencyControlled
        );
        assert_eq!(
            PyInvocationStatus::concurrency_controlled_final().inner,
            InvocationStatus::ConcurrencyControlledFinal
        );
        assert_eq!(
            PyInvocationStatus::rerouted().inner,
            InvocationStatus::Rerouted
        );
        assert_eq!(
            PyInvocationStatus::pending_recovery().inner,
            InvocationStatus::PendingRecovery
        );
        assert_eq!(
            PyInvocationStatus::running_recovery().inner,
            InvocationStatus::RunningRecovery
        );
    }

    // ── is_terminal ──────────────────────────────────────────────

    #[test]
    fn terminal_statuses() {
        assert!(PyInvocationStatus::success().is_terminal());
        assert!(PyInvocationStatus::failed().is_terminal());
        assert!(PyInvocationStatus::concurrency_controlled_final().is_terminal());
    }

    #[test]
    fn non_terminal_statuses() {
        assert!(!PyInvocationStatus::registered().is_terminal());
        assert!(!PyInvocationStatus::pending().is_terminal());
        assert!(!PyInvocationStatus::running().is_terminal());
        assert!(!PyInvocationStatus::retry().is_terminal());
        assert!(!PyInvocationStatus::concurrency_controlled().is_terminal());
        assert!(!PyInvocationStatus::rerouted().is_terminal());
        assert!(!PyInvocationStatus::pending_recovery().is_terminal());
        assert!(!PyInvocationStatus::running_recovery().is_terminal());
    }

    // ── __str__ / __repr__ ───────────────────────────────────────

    #[test]
    fn str_roundtrip() {
        let cases = [
            (PyInvocationStatus::registered(), "REGISTERED"),
            (PyInvocationStatus::pending(), "PENDING"),
            (PyInvocationStatus::running(), "RUNNING"),
            (PyInvocationStatus::success(), "SUCCESS"),
            (PyInvocationStatus::failed(), "FAILED"),
            (PyInvocationStatus::retry(), "RETRY"),
            (PyInvocationStatus::rerouted(), "REROUTED"),
        ];
        for (status, expected) in cases {
            assert_eq!(status.__str__(), expected);
        }
    }

    #[test]
    fn repr_contains_status_name() {
        let s = PyInvocationStatus::success();
        let repr = s.__repr__();
        assert!(repr.starts_with("InvocationStatus."));
        assert!(repr.contains("SUCCESS"));
    }

    // ── __eq__ / __hash__ ────────────────────────────────────────

    #[test]
    fn equality() {
        let a = PyInvocationStatus::running();
        let b = PyInvocationStatus::running();
        let c = PyInvocationStatus::failed();
        assert!(a.__eq__(&b));
        assert!(!a.__eq__(&c));
    }

    #[test]
    fn hash_consistency() {
        let a = PyInvocationStatus::pending();
        let b = PyInvocationStatus::pending();
        assert_eq!(a.__hash__(), b.__hash__());
    }

    // ── PyConcurrencyControlType ─────────────────────────────────

    #[test]
    fn concurrency_control_constructors() {
        assert_eq!(
            PyConcurrencyControlType::unlimited().inner,
            ConcurrencyControlType::Unlimited
        );
        assert_eq!(
            PyConcurrencyControlType::task().inner,
            ConcurrencyControlType::Task
        );
        assert_eq!(
            PyConcurrencyControlType::argument().inner,
            ConcurrencyControlType::Argument
        );
        assert_eq!(
            PyConcurrencyControlType::none().inner,
            ConcurrencyControlType::None
        );
    }

    #[test]
    fn concurrency_control_str() {
        let cc = PyConcurrencyControlType::unlimited();
        assert_eq!(cc.__str__(), "Unlimited");
    }
}
