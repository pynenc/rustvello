// crates/rustvello-client/src/mock_client.rs

use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use uuid::Uuid;

use rustvello_core::{MuseError, MuseResult, Transport};
use rustvello_proto::prelude::*;

static NEXT_ELEMENT_ID: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

/// Get a new unique ElementId as a `u64`
pub fn get_new_element_id() -> u64 {
    NEXT_ELEMENT_ID.fetch_add(1, Ordering::SeqCst)
}

pub struct MockClient {
    metrics: Arc<Mutex<Vec<MetricDefinition>>>,
    sent_metrics: Arc<Mutex<Vec<MetricPayload>>>,
    finest_resolution: Arc<Mutex<TimestampResolution>>,
    node_state: NodeState,
}

impl Default for MockClient {
    fn default() -> Self {
        Self::new(TimestampResolution::default())
    }
}

impl MockClient {
    pub fn new(default_resolution: TimestampResolution) -> Self {
        let node_id = Uuid::new_v4();
        let node_info = NodeInfo {
            start_date: Utc::now().timestamp(),
            node_addr: "127.0.0.1:0".parse().unwrap(), // Mock address
        };
        let cluster_id = Uuid::new_v4();
        let available_nodes = {
            let mut map = HashMap::new();
            map.insert(node_id, node_info);
            map
        };
        let node_state = NodeState {
            node_id,
            node_info,
            available_nodes,
            main_node_id: Some(node_id),
            current_status: NodeStatus::Leader,
            cluster_id: Some(cluster_id),
        };

        MockClient {
            metrics: Arc::new(Mutex::new(Vec::new())),
            sent_metrics: Arc::new(Mutex::new(Vec::new())),
            finest_resolution: Arc::new(Mutex::new(default_resolution)),
            node_state,
        }
    }
}

#[async_trait]
impl Transport for MockClient {
    async fn health_check(&self) -> MuseResult<()> {
        log::info!("MockClient: health_check called");
        Ok(())
    }

    async fn get_node_state(&self) -> MuseResult<NodeState> {
        log::info!("MockClient: get_node_state called");
        Ok(self.node_state.clone())
    }

    async fn get_finest_resolution(&self) -> MuseResult<TimestampResolution> {
        log::info!("MockClient: get_finest_resolution called");
        Ok(*self.finest_resolution.lock().await)
    }

    async fn get_node_elem_ranges(
        &self,
        _ini: Option<u64>,
        _end: Option<u64>,
    ) -> MuseResult<Vec<NodeElementRange>> {
        log::info!(
            "MockClient: get_node_elem_ranges called with {:?}..{:?}",
            _ini,
            _end
        );

        let current_max_elem_id = NEXT_ELEMENT_ID.load(Ordering::SeqCst);

        // Calculate the range end, rounded up to the next multiple of 100
        let range_end = ((current_max_elem_id + 99) / 100) * 100;

        let node_element_range = NodeElementRange {
            node_id: self.node_state.node_id,
            range: OrdRangeInc(RangeInclusive::new(0, range_end)),
        };

        Ok(vec![node_element_range])
    }

    async fn register_element_kinds(
        &self,
        element_kinds: &[ElementKindRegistration],
    ) -> MuseResult<()> {
        log::info!(
            "MockClient: register_element_kinds called with {:?}",
            element_kinds
        );
        Ok(())
    }

    async fn register_elements(
        &self,
        elements: &[ElementRegistration],
    ) -> MuseResult<Vec<Result<ElementId, MuseError>>> {
        log::info!("MockClient: register_elements called with {:?}", elements);
        let results = elements.iter().map(|_| Ok(get_new_element_id())).collect();
        Ok(results)
    }

    async fn register_metrics(&self, payload: &[MetricDefinition]) -> MuseResult<()> {
        log::info!("MockClient: register_metrics called with {:?}", payload);
        let mut metrics = self.metrics.lock().await;
        metrics.extend(payload.iter().cloned());
        Ok(())
    }

    async fn get_metric_order(&self) -> MuseResult<Vec<MetricDefinition>> {
        log::info!("MockClient: get_metric_order called");
        let metrics = self.metrics.lock().await;
        Ok(metrics.clone())
    }

    async fn get_metrics(
        &self,
        query: &MetricQuery,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<Vec<MetricPayload>> {
        log::info!(
            "MockClient: get_metrics from {:?} called with query: {:?}",
            node_addr,
            query
        );
        if query.parent_id.is_some() {
            return Err(MuseError::Client(
                "parent_id not implemented in MockClient".to_string(),
            ));
        }
        let mut results = Vec::new();
        for payload in self.sent_metrics.lock().await.iter() {
            // Filter by time range
            if let Some(start_time) = query.start_time {
                if payload.time < start_time {
                    continue;
                }
            }
            if let Some(end_time) = query.end_time {
                if payload.time > end_time {
                    continue;
                }
            }
            // Filter by element_id
            if let Some(query_element_id) = query.element_id {
                if payload.element_id != query_element_id {
                    continue;
                }
            }
            // Filter by metric_id if specified
            if let Some(query_metric_id) = query.metric_id {
                // Check if any metric_id in the payload matches `query_metric_id`
                if !payload.metric_ids.contains(&query_metric_id) {
                    continue;
                }
            }
            results.push(payload.clone());
        }
        Ok(results)
    }

    async fn send_metrics(
        &self,
        payload: Vec<MetricPayload>,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<()> {
        log::info!(
            "MockClient: send_metrics to {:?} called with {:?}",
            node_addr,
            payload
        );
        let mut sent_metrics = self.sent_metrics.lock().await;
        sent_metrics.extend(payload);
        Ok(())
    }
}
