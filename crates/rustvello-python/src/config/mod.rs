// crates/rustvello-python/src/config.rs

mod client_type;
#[cfg(feature = "pymethods")]
mod general;

use pyo3::prelude::pyclass;

pub use client_type::PyClientType;
use rustvello::prelude::Config as RustConfig;

#[pyclass]
#[repr(transparent)]
pub struct PyConfig {
    pub inner: RustConfig,
}

impl From<RustConfig> for PyConfig {
    fn from(config: RustConfig) -> Self {
        PyConfig { inner: config }
    }
}
