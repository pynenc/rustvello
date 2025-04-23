// crates/rustvello-python/src/proto/metric.rs

use pyo3::prelude::*;

use super::PyMetricDefinition;
use super::PyMetricPayload;
use super::PyMetricQuery;
use rustvello_proto::MetricDefinition as RustMetricDefinition;
use rustvello_proto::MetricPayload as RustMetricPayload;
use rustvello_proto::MetricQuery as RustMetricQuery;

#[pymethods]
impl PyMetricDefinition {
    #[new]
    pub fn __init__(code: String, name: String, description: String) -> PyResult<Self> {
        let ekr = RustMetricDefinition::new(&code, &name, &description);
        Ok(Self::from(ekr))
    }
}

#[pymethods]
impl PyMetricPayload {
    #[new]
    pub fn __init__(
        time: i64,
        element_id: u64,
        metric_ids: Vec<u32>,
        values: Vec<Option<f32>>,
    ) -> PyResult<Self> {
        let ekr = RustMetricPayload::new(time, element_id, metric_ids, values);
        Ok(Self::from(ekr))
    }

    #[getter]
    pub fn time(&self) -> i64 {
        self.inner.time
    }

    #[getter]
    pub fn element_id(&self) -> u64 {
        self.inner.element_id
    }

    #[getter]
    pub fn metric_ids(&self) -> Vec<u32> {
        self.inner.metric_ids.clone()
    }

    #[getter]
    pub fn values(&self) -> Vec<Option<f32>> {
        self.inner.values.clone()
    }
}

#[pymethods]
impl PyMetricQuery {
    #[new]
    #[pyo3(signature = (start_time=None, end_time=None, element_id=None, parent_id=None, metric_id=None))]
    pub fn __init__(
        start_time: Option<i64>,
        end_time: Option<i64>,
        element_id: Option<u64>,
        parent_id: Option<u64>,
        metric_id: Option<u32>,
    ) -> PyResult<Self> {
        let ekr = RustMetricQuery::new(start_time, end_time, element_id, parent_id, metric_id);
        Ok(Self::from(ekr))
    }
}
