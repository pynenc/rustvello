// crates/rustvello-python/src/config/general.rs

use chrono::Duration;
use pyo3::prelude::*;

use super::{PyClientType, PyConfig};
use crate::error::PyMusesErr;
use crate::proto::PyElementKindRegistration;
use crate::proto::PyMetricDefinition;
use crate::proto::PyTimestampResolution;
use rustvello::prelude::Config as RustConfig;

#[pymethods]
impl PyConfig {
    #[new]
    #[pyo3(signature = (endpoints, client_type, default_resolution, element_kinds, metric_definitions, max_reg_elem_retries, max_endpoint_retries, recording_enabled, recording_path=None, recording_flush_interval=None, initialization_interval=None, cluster_monitor_interval=None))]
    pub fn __init__(
        endpoints: Vec<String>,
        client_type: PyClientType,
        default_resolution: PyTimestampResolution,
        element_kinds: Vec<PyElementKindRegistration>,
        metric_definitions: Vec<PyMetricDefinition>,
        max_reg_elem_retries: usize,
        max_endpoint_retries: Option<usize>,
        recording_enabled: bool,
        recording_path: Option<String>,
        recording_flush_interval: Option<Duration>,
        initialization_interval: Option<Duration>,
        cluster_monitor_interval: Option<Duration>,
    ) -> PyResult<Self> {
        let recording_flush_d = match recording_flush_interval {
            Some(d) => Some(d.to_std().map_err(PyMusesErr::from)?),
            None => None,
        };
        let initialization_d = match initialization_interval {
            Some(d) => Some(d.to_std().map_err(PyMusesErr::from)?),
            None => None,
        };
        let cluster_monitor_d = match cluster_monitor_interval {
            Some(d) => Some(d.to_std().map_err(PyMusesErr::from)?),
            None => None,
        };
        let muse = RustConfig::new(
            endpoints,
            client_type.into(),
            recording_enabled,
            recording_path,
            recording_flush_d,
            default_resolution.into(),
            element_kinds.into_iter().map(|p| p.inner).collect(),
            metric_definitions.into_iter().map(|p| p.inner).collect(),
            initialization_d,
            cluster_monitor_d,
            max_reg_elem_retries,
            max_endpoint_retries,
        )
        .map_err(PyMusesErr::from)?;
        Ok(Self::from(muse))
    }
}
