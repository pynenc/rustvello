// crates/rustvello-python/src/proto/timestamp_resolution.rs

use pyo3::prelude::*;
use pyo3::types::PyDelta;

use rustvello_proto::TimestampResolution as RustTimestampResolution;

#[pyclass(eq, eq_int, name = "TimestampResolution")]
#[derive(Clone, PartialEq)]
pub enum PyTimestampResolution {
    Years,
    Months,
    Weeks,
    Days,
    Hours,
    Minutes,
    Seconds,
    Milliseconds,
    Microseconds,
}

#[pymethods]
impl PyTimestampResolution {
    fn to_timedelta(&self, py: Python) -> PyResult<PyObject> {
        let rust_resolution: RustTimestampResolution = self.clone().into();
        let duration = rust_resolution.to_duration();

        let total_seconds = duration.as_secs();
        let microseconds = duration.as_micros() % 1_000_000;

        // Create a &PyDelta reference
        let delta_ref = PyDelta::new_bound(
            py,
            0,                    // days
            total_seconds as i32, // seconds
            microseconds as i32,  // microseconds
            true,                 // normalize
        )?;

        // Convert &PyDelta to PyObject
        // This PyObject is a Python datetime.timedelta object
        Ok(delta_ref.to_object(py))
    }
}

impl From<RustTimestampResolution> for PyTimestampResolution {
    fn from(ts: RustTimestampResolution) -> Self {
        match ts {
            RustTimestampResolution::Years => PyTimestampResolution::Years,
            RustTimestampResolution::Months => PyTimestampResolution::Months,
            RustTimestampResolution::Weeks => PyTimestampResolution::Weeks,
            RustTimestampResolution::Days => PyTimestampResolution::Days,
            RustTimestampResolution::Hours => PyTimestampResolution::Hours,
            RustTimestampResolution::Minutes => PyTimestampResolution::Minutes,
            RustTimestampResolution::Seconds => PyTimestampResolution::Seconds,
            RustTimestampResolution::Milliseconds => PyTimestampResolution::Milliseconds,
            RustTimestampResolution::Microseconds => PyTimestampResolution::Microseconds,
        }
    }
}

impl From<PyTimestampResolution> for RustTimestampResolution {
    fn from(py_res: PyTimestampResolution) -> Self {
        match py_res {
            PyTimestampResolution::Years => RustTimestampResolution::Years,
            PyTimestampResolution::Months => RustTimestampResolution::Months,
            PyTimestampResolution::Weeks => RustTimestampResolution::Weeks,
            PyTimestampResolution::Days => RustTimestampResolution::Days,
            PyTimestampResolution::Hours => RustTimestampResolution::Hours,
            PyTimestampResolution::Minutes => RustTimestampResolution::Minutes,
            PyTimestampResolution::Seconds => RustTimestampResolution::Seconds,
            PyTimestampResolution::Milliseconds => RustTimestampResolution::Milliseconds,
            PyTimestampResolution::Microseconds => RustTimestampResolution::Microseconds,
        }
    }
}
