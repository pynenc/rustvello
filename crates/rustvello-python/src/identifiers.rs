use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Python wrapper for TaskId
#[pyclass(name = "TaskId")]
#[derive(Clone)]
pub struct PyTaskId {
    pub inner: rustvello_proto::identifiers::TaskId,
}

#[pymethods]
impl PyTaskId {
    #[new]
    fn new(module: String, name: String) -> PyResult<Self> {
        let inner = rustvello_proto::identifiers::TaskId::try_new(module, name)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner })
    }

    #[getter]
    fn module(&self) -> &str {
        self.inner.module()
    }

    #[getter]
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("TaskId('{}', '{}')", self.inner.module(), self.inner.name())
    }
}

/// Python wrapper for InvocationId
#[pyclass(name = "InvocationId")]
#[derive(Clone)]
pub struct PyInvocationId {
    pub inner: rustvello_proto::identifiers::InvocationId,
}

#[pymethods]
impl PyInvocationId {
    #[new]
    fn new() -> Self {
        Self {
            inner: rustvello_proto::identifiers::InvocationId::new(),
        }
    }

    #[staticmethod]
    fn from_string(id: String) -> PyResult<Self> {
        uuid::Uuid::parse_str(&id).map_err(|e| {
            PyValueError::new_err(format!("invalid invocation_id (expected UUID): {e}"))
        })?;
        Ok(Self {
            inner: rustvello_proto::identifiers::InvocationId::from_string(id),
        })
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("InvocationId::from('{}')", self.inner.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn task_id_valid() {
        Python::with_gil(|_py| {
            let tid = PyTaskId::new("my_module".into(), "my_func".into()).unwrap();
            assert_eq!(tid.module(), "my_module");
            assert_eq!(tid.name(), "my_func");
            assert!(tid.__str__().contains("my_module"));
            assert!(tid.__str__().contains("my_func"));
        });
    }

    #[test]
    fn task_id_repr() {
        Python::with_gil(|_py| {
            let tid = PyTaskId::new("mod".into(), "fn".into()).unwrap();
            let repr = tid.__repr__();
            assert!(repr.starts_with("TaskId("));
            assert!(repr.contains("mod"));
            assert!(repr.contains("fn"));
        });
    }

    #[test]
    fn invocation_id_new_generates_unique() {
        let a = PyInvocationId::new();
        let b = PyInvocationId::new();
        assert_ne!(a.__str__(), b.__str__());
    }

    #[test]
    fn invocation_id_from_valid_uuid() {
        Python::with_gil(|_py| {
            let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
            let id = PyInvocationId::from_string(uuid_str.into()).unwrap();
            assert_eq!(id.__str__(), uuid_str);
        });
    }

    #[test]
    fn invocation_id_from_invalid_uuid() {
        Python::with_gil(|_py| {
            let result = PyInvocationId::from_string("not-a-uuid".into());
            assert!(result.is_err());
        });
    }

    #[test]
    fn invocation_id_repr() {
        let id = PyInvocationId::new();
        let repr = id.__repr__();
        assert!(repr.starts_with("InvocationId::from('"));
    }
}
