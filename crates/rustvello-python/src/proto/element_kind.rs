// crates/rustvello-python/src/proto/element_kind.rs

use pyo3::prelude::*;

use super::PyElementKindRegistration;
use rustvello_proto::ElementKindRegistration as RustElementKindRegistration;

#[pymethods]
impl PyElementKindRegistration {
    #[new]
    #[pyo3(signature = (code, name, description, parent_code=None))]
    pub fn __init__(
        code: String,
        name: String,
        description: String,
        parent_code: Option<String>,
    ) -> PyResult<Self> {
        let ekr =
            RustElementKindRegistration::new(&code, parent_code.as_deref(), &name, &description);
        Ok(Self::from(ekr))
    }
}
