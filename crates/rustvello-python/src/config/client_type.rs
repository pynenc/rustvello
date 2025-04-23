use pyo3::prelude::*;

use rustvello::prelude::ClientType as RustClientType;

#[pyclass(eq, eq_int, name = "ClientType")]
#[derive(Clone, PartialEq)]
pub enum PyClientType {
    Poet,
    Mock,
}

impl From<RustClientType> for PyClientType {
    fn from(muse: RustClientType) -> Self {
        match muse {
            RustClientType::Poet => PyClientType::Poet,
            RustClientType::Mock => PyClientType::Mock,
        }
    }
}

impl From<PyClientType> for RustClientType {
    fn from(py_client_type: PyClientType) -> Self {
        match py_client_type {
            PyClientType::Poet => RustClientType::Poet,
            PyClientType::Mock => RustClientType::Mock,
        }
    }
}
