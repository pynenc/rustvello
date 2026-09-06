//! PyO3 wrappers for explicit workflow-root operations.

use pyo3::prelude::*;

use rustvello_core::workflow::WorkflowRoot;

use crate::error::to_py_err;

/// Root-scoped deterministic operations for a running workflow invocation.
///
/// Obtain this with `workflow_root()` from inside a Python function registered
/// through `App.workflow`. Ordinary tasks and child workflow members cannot
/// construct this handle.
#[pyclass(name = "WorkflowRoot")]
pub struct PyWorkflowRoot {
    inner: WorkflowRoot,
}

#[pymethods]
impl PyWorkflowRoot {
    /// Return the next deterministic random value in `[0, 1)`.
    fn random(&mut self) -> PyResult<f64> {
        crate::runtime::shared_runtime()?
            .block_on(self.inner.random_async())
            .map_err(to_py_err)
    }

    /// Return the next deterministic UTC timestamp as an RFC 3339 string.
    fn utc_now(&mut self) -> PyResult<String> {
        crate::runtime::shared_runtime()?
            .block_on(self.inner.utc_now_async())
            .map(|dt| dt.to_rfc3339())
            .map_err(to_py_err)
    }

    /// Return the next deterministic UUID string.
    fn uuid(&mut self) -> PyResult<String> {
        crate::runtime::shared_runtime()?
            .block_on(self.inner.uuid_async())
            .map_err(to_py_err)
    }

    fn __repr__(&self) -> &'static str {
        "WorkflowRoot(...)"
    }
}

/// Resolve the current explicit workflow root.
#[pyfunction]
pub fn workflow_root() -> PyResult<PyWorkflowRoot> {
    WorkflowRoot::current()
        .map(|inner| PyWorkflowRoot { inner })
        .map_err(to_py_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_root_requires_running_context() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let Err(err) = workflow_root() else {
                panic!("workflow_root should fail outside an invocation context");
            };
            assert!(err.value_bound(py).to_string().contains("workflow"));
        });
    }
}
