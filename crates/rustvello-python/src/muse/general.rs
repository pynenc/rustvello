// crates/rustvello-python/src/muse/general.rs

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::proto::PyTimestampResolution;
use num_traits::cast::AsPrimitive;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::Mutex;
use tokio::time::Duration;

use super::PyMuse;
use crate::config::PyConfig;
use crate::error::PyMusesErr;
use crate::proto::{PyMetricPayload, PyMetricQuery};
use rustvello::Muse as RustMuse;
use rustvello_proto::types::*;

#[pymethods]
impl PyMuse {
    #[new]
    pub fn __init__(config: &PyConfig) -> PyResult<Self> {
        let muse = RustMuse::new(&config.inner).map_err(PyMusesErr::from)?;
        let is_initialized = muse.is_initialized.clone();
        Ok(Self {
            muse: Arc::new(Mutex::new(muse)),
            is_initialized,
        })
    }

    #[pyo3(signature = (timeout=None))]
    pub fn initialize<'p>(
        &self,
        py: Python<'p>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let muse = self.muse.clone();

        future_into_py(py, async move {
            let timeout = timeout.map(Duration::from_secs_f64);
            let mut muse_guard = muse.lock().await;
            muse_guard
                .initialize(timeout)
                .await
                .map_err(PyMusesErr::from)?;
            Ok(())
        })
    }

    #[getter]
    pub fn is_initialized(&self) -> PyResult<bool> {
        Ok(self.is_initialized.load(Ordering::SeqCst))
    }

    #[getter]
    pub fn finest_resolution(&self) -> PyTimestampResolution {
        let muse = self.muse.clone();
        // Use a blocking mutex lock instead of creating an async context
        let muse_guard = muse.blocking_lock();
        let finest_resolution = muse_guard.get_finest_resolution();
        finest_resolution.into()
    }

    #[pyo3(signature = (kind_code, name, metadata, parent_id=None))]
    pub fn register_element<'p>(
        &self,
        kind_code: &str,
        name: String,
        metadata: HashMap<String, String>,
        parent_id: Option<&str>,
        py: Python<'p>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let muse = self.muse.clone();
        let kind_code = kind_code.to_string();
        let name = name.clone();
        let metadata = metadata.clone();
        let parent_id = parent_id
            .map(LocalElementId::parse_str)
            .transpose()
            .map_err(PyMusesErr::from)?;
        future_into_py(py, async move {
            let muse_guard = muse.lock().await;
            let local_elem_id = muse_guard
                .register_element(&kind_code, name, metadata, parent_id)
                .await
                .map_err(PyMusesErr::from)?;
            Ok(local_elem_id.to_string())
        })
    }

    pub fn get_remote_element_id(&self, local_elem_id: &str) -> PyResult<Option<u64>> {
        let muse = self.muse.clone();
        let local_elem_uuid = LocalElementId::parse_str(local_elem_id).map_err(PyMusesErr::from)?;

        // Use a blocking mutex lock instead of creating an async context
        let muse_guard = muse.blocking_lock();
        let remote_elem_id = muse_guard.get_remote_element_id(&local_elem_uuid);
        Ok(remote_elem_id.map(|id| id.as_()))
    }

    pub fn send_metric<'p>(
        &self,
        local_elem_id: &str,
        metric_code: &str,
        value: f32,
        py: Python<'p>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let muse = self.muse.clone();
        let metric_code = metric_code.to_string();
        let local_elem_uuid = LocalElementId::parse_str(local_elem_id).map_err(PyMusesErr::from)?;
        future_into_py(py, async move {
            let muse_guard = muse.lock().await;
            muse_guard
                .send_metric(local_elem_uuid, &metric_code, value)
                .await
                .map_err(PyMusesErr::from)?;
            Ok(())
        })
    }

    pub fn get_metrics<'p>(
        &self,
        query: PyMetricQuery,
        py: Python<'p>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let muse = self.muse.clone();
        future_into_py(py, async move {
            let muse_guard = muse.lock().await;
            let metrics = muse_guard
                .get_metrics(&query.inner)
                .await
                .map_err(PyMusesErr::from)?;

            // Convert MetricPayload to PyMetricPayload
            let py_metrics: Vec<PyMetricPayload> =
                metrics.into_iter().map(PyMetricPayload::from).collect();

            Ok(py_metrics)
        })
    }

    pub fn replay<'p>(&self, replay_path: &str, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let muse = self.muse.clone();
        let replay_path = PathBuf::from(replay_path);
        future_into_py(py, async move {
            let muse_guard = muse.lock().await;
            muse_guard
                .replay(&replay_path)
                .await
                .map_err(PyMusesErr::from)?;
            Ok(())
        })
    }
}
