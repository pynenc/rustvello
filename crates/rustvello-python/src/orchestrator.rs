use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_mem::orchestrator::MemOrchestrator;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

pub fn parse_status(s: &str) -> PyResult<InvocationStatus> {
    s.parse::<InvocationStatus>()
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

/// Parse a Python `ConcurrencyControlType` name to the Rust enum variant.
pub fn parse_cc_type(s: &str) -> PyResult<ConcurrencyControlType> {
    s.parse::<ConcurrencyControlType>()
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

/// Rust in-memory orchestrator exposed to Python.
///
/// Wraps `MemOrchestrator` — manages invocation lifecycle with atomic
/// status transitions in process memory.
#[pyclass(name = "RustMemOrchestrator")]
pub struct PyMemOrchestrator {
    pub(crate) inner: Arc<MemOrchestrator>,
}

#[pymethods]
impl PyMemOrchestrator {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(MemOrchestrator::new()),
        }
    }
}

impl_py_orchestrator!(PyMemOrchestrator);

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use rustvello_core::orchestrator::OrchestratorStatus;
    use rustvello_proto::call::{CallDTO, SerializedArguments};
    use rustvello_proto::identifiers::TaskId;

    /// Helper: register an invocation directly on the inner MemOrchestrator and
    /// return the generated invocation ID string.
    fn register_invocation(orch: &PyMemOrchestrator, module: &str, name: &str) -> String {
        let task_id = TaskId::try_new(module, name).unwrap();
        let call = CallDTO::new(task_id, SerializedArguments::new());
        let rt = crate::runtime::shared_runtime().unwrap();
        rt.block_on(orch.inner.register_invocation(&call))
            .unwrap()
            .as_str()
            .to_string()
    }

    #[test]
    fn set_and_get_status() {
        Python::with_gil(|py| {
            let orch = PyMemOrchestrator::new();
            let inv = register_invocation(&orch, "mod", "func");

            // Newly registered → REGISTERED
            let (status, _, _) = orch.get_invocation_status(py, &inv).unwrap();
            assert_eq!(status, "REGISTERED");

            // Transition to Pending (acquires_ownership → needs runner_id)
            let (status, rid, _) = orch
                .set_invocation_status(py, &inv, "PENDING", Some("runner-1"))
                .unwrap();
            assert_eq!(status, "PENDING");
            assert_eq!(rid.as_deref(), Some("runner-1"));

            // Read back
            let (status, _, _) = orch.get_invocation_status(py, &inv).unwrap();
            assert_eq!(status, "PENDING");
        });
    }

    #[test]
    fn get_invocations_by_status() {
        Python::with_gil(|py| {
            let orch = PyMemOrchestrator::new();
            let inv1 = register_invocation(&orch, "mod", "func");
            let inv2 = register_invocation(&orch, "mod", "func");

            // Pending acquires_ownership → needs runner_id
            orch.set_invocation_status(py, &inv1, "PENDING", Some("runner-1"))
                .unwrap();

            let pending = orch
                .get_invocations_by_status(py, "PENDING", None, None)
                .unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0], inv1);

            let registered = orch
                .get_invocations_by_status(py, "REGISTERED", None, None)
                .unwrap();
            assert_eq!(registered.len(), 1);
            assert_eq!(registered[0], inv2);
        });
    }

    #[test]
    fn waiting_for_and_release() {
        Python::with_gil(|py| {
            let orch = PyMemOrchestrator::new();
            let inv1 = register_invocation(&orch, "mod", "f1");
            let inv2 = register_invocation(&orch, "mod", "f2");

            orch.set_waiting_for(py, &inv1, &inv2).unwrap();

            let released = orch.release_waiters(py, &inv2).unwrap();
            assert!(released.contains(&inv1));
        });
    }

    #[test]
    fn invalid_status_string() {
        Python::with_gil(|py| {
            let orch = PyMemOrchestrator::new();
            let inv = register_invocation(&orch, "mod", "func");
            let result = orch.set_invocation_status(py, &inv, "NONEXISTENT", None);
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(err.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        });
    }

    #[test]
    fn status_with_runner_id() {
        Python::with_gil(|py| {
            let orch = PyMemOrchestrator::new();
            let inv = register_invocation(&orch, "mod", "func");

            // Pending acquires_ownership with runner-1
            orch.set_invocation_status(py, &inv, "PENDING", Some("runner-1"))
                .unwrap();
            // Running requires_ownership — must use same runner-1
            let status = orch
                .set_invocation_status(py, &inv, "RUNNING", Some("runner-1"))
                .unwrap();
            assert_eq!(status.0, "RUNNING");
        });
    }
}
