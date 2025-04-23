// crates/rustvello-core/src/buffer/metric_buffer.rs

use std::collections::HashMap;

use tokio::sync::Mutex;

use rustvello_proto::{LocalElementId, MetricValue};

pub struct MetricBuffer {
    buffer: Mutex<HashMap<LocalElementId, HashMap<String, MetricValue>>>,
}

impl Default for MetricBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricBuffer {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(HashMap::new()),
        }
    }

    /// Adds a metric to the buffer.
    pub async fn add_metric(
        &self,
        local_elem_id: LocalElementId,
        metric_code: String,
        value: MetricValue,
    ) {
        let mut buffer = self.buffer.lock().await;
        buffer
            .entry(local_elem_id)
            .or_insert_with(HashMap::new)
            .insert(metric_code, value);
    }

    /// Retrieves and clears all buffered metrics.
    pub async fn get_and_clear(&self) -> HashMap<LocalElementId, HashMap<String, MetricValue>> {
        let mut buffer = self.buffer.lock().await;
        let data = buffer.clone();
        buffer.clear();
        data
    }
}
