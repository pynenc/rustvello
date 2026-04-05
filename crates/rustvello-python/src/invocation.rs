use pyo3::prelude::*;

use crate::identifiers::PyInvocationId;
use crate::status::PyInvocationStatus;

/// Python wrapper for an invocation result.
#[pyclass(name = "InvocationResult")]
pub struct PyInvocationResult {
    pub invocation_id: rustvello_proto::identifiers::InvocationId,
    pub status: rustvello_proto::status::InvocationStatus,
    pub result: Option<String>,
    pub error: Option<String>,
}

#[pymethods]
impl PyInvocationResult {
    #[getter]
    fn invocation_id(&self) -> PyInvocationId {
        PyInvocationId {
            inner: self.invocation_id.clone(),
        }
    }

    #[getter]
    fn status(&self) -> PyInvocationStatus {
        PyInvocationStatus { inner: self.status }
    }

    #[getter]
    fn result(&self) -> Option<&str> {
        self.result.as_deref()
    }

    #[getter]
    fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    fn __repr__(&self) -> String {
        format!(
            "InvocationResult(id='{}', status={})",
            self.invocation_id, self.status
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_proto::identifiers::InvocationId;
    use rustvello_proto::status::InvocationStatus;

    fn make_result(
        result: Option<&str>,
        error: Option<&str>,
        status: InvocationStatus,
    ) -> PyInvocationResult {
        PyInvocationResult {
            invocation_id: InvocationId::new(),
            status,
            result: result.map(String::from),
            error: error.map(String::from),
        }
    }

    #[test]
    fn getters_return_correct_values() {
        let inv = make_result(Some("42"), None, InvocationStatus::Success);
        assert_eq!(inv.result(), Some("42"));
        assert_eq!(inv.error(), None);
        assert_eq!(inv.status().inner, InvocationStatus::Success);
    }

    #[test]
    fn error_getter() {
        let inv = make_result(None, Some("boom"), InvocationStatus::Failed);
        assert_eq!(inv.result(), None);
        assert_eq!(inv.error(), Some("boom"));
    }

    #[test]
    fn invocation_id_getter_roundtrips() {
        let id = InvocationId::new();
        let inv = PyInvocationResult {
            invocation_id: id.clone(),
            status: InvocationStatus::Running,
            result: None,
            error: None,
        };
        assert_eq!(inv.invocation_id().inner, id);
    }

    #[test]
    fn repr_contains_id_and_status() {
        let inv = make_result(None, None, InvocationStatus::Pending);
        let repr = inv.__repr__();
        assert!(repr.starts_with("InvocationResult("));
        assert!(repr.contains("PENDING"));
    }
}
