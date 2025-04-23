// py-rustvello/src/lib.rs

#![allow(clippy::nonstandard_macro_braces)]
#![allow(clippy::transmute_undefined_repr)]
#![allow(non_local_definitions)]
#![allow(clippy::too_many_arguments)]

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use rustvello_python::config::{PyClientType, PyConfig};
use rustvello_python::proto::PyTimestampResolution;
use rustvello_python::proto::{
    PyElementKindRegistration, PyMetricDefinition, PyMetricPayload, PyMetricQuery,
};
// use rustvello_python::element_kind_registration::PyElementKindRegistration;
// use rustvello_python::metric_definition::PyMetricDefinition;
use rustvello_python::exceptions;
use rustvello_python::muse::PyMuse;

// #[pymodule]
// fn proto(_py: Python, m: &PyModule) -> PyResult<()> {
//     m.add_class::<PyMuse>()?;
//     Ok(())
// }

#[pymodule]
// #[pyo3(name = "rustvello")]
fn rustvello(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<PyMuse>()?;
    m.add_class::<PyConfig>()?;
    m.add_class::<PyClientType>()?;
    m.add_class::<PyTimestampResolution>()?;
    m.add_class::<PyElementKindRegistration>()?;
    m.add_class::<PyMetricDefinition>()?;
    m.add_class::<PyMetricPayload>()?;
    m.add_class::<PyMetricQuery>()?;

    #[pyfunction]
    fn get_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
    m.add_function(wrap_pyfunction!(get_version, m)?)?;

    // Add submodules
    // m.add_submodule(proto::module(m)?)?;

    // Exceptions - Errors
    m.add("MuseError", py.get_type_bound::<exceptions::MuseError>())
        .unwrap();
    m.add(
        "ConfigurationError",
        py.get_type_bound::<exceptions::ConfigurationError>(),
    )
    .unwrap();
    m.add(
        "MuseInitializationTimeoutError",
        py.get_type_bound::<exceptions::MuseInitializationTimeoutError>(),
    )
    .unwrap();
    m.add(
        "ClientError",
        py.get_type_bound::<exceptions::ClientError>(),
    )
    .unwrap();
    m.add(
        "RecordingError",
        py.get_type_bound::<exceptions::RecordingError>(),
    )
    .unwrap();
    m.add(
        "ReplayingError",
        py.get_type_bound::<exceptions::ReplayingError>(),
    )
    .unwrap();
    m.add(
        "InvalidFileExtensionError",
        py.get_type_bound::<exceptions::InvalidFileExtensionError>(),
    )
    .unwrap();
    m.add(
        "InvalidElementKindCodeError",
        py.get_type_bound::<exceptions::InvalidElementKindCodeError>(),
    )
    .unwrap();
    m.add(
        "NotAvailableRemoteElementIdError",
        py.get_type_bound::<exceptions::NotAvailableRemoteElementIdError>(),
    )
    .unwrap();
    m.add(
        "InvalidMetricCodeError",
        py.get_type_bound::<exceptions::InvalidMetricCodeError>(),
    )
    .unwrap();
    m.add(
        "DurationConversionError",
        py.get_type_bound::<exceptions::DurationConversionError>(),
    )
    .unwrap();

    // Build info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
