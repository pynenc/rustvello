// crates/rustvello-core/src/transport.rs

use std::net::SocketAddr;

use async_trait::async_trait;

pub use crate::errors::MuseResult;
pub use rustvello_proto::{
    types::*, ElementKindRegistration, ElementRegistration, MetricDefinition, MetricPayload,
    MetricQuery, NodeElementRange, NodeState, TimestampResolution,
};

#[async_trait]
pub trait Transport {
    async fn health_check(&self) -> MuseResult<()>;
    async fn get_node_state(&self) -> MuseResult<NodeState>;
    async fn get_finest_resolution(&self) -> MuseResult<TimestampResolution>;
    async fn register_element_kinds(
        &self,
        element_kinds: &[ElementKindRegistration],
    ) -> MuseResult<()>;
    async fn register_elements(
        &self,
        elements: &[ElementRegistration],
    ) -> MuseResult<Vec<MuseResult<ElementId>>>;
    async fn get_node_elem_ranges(
        &self,
        ini: Option<u64>,
        end: Option<u64>,
    ) -> MuseResult<Vec<NodeElementRange>>;
    async fn register_metrics(&self, payload: &[MetricDefinition]) -> MuseResult<()>;
    async fn get_metric_order(&self) -> MuseResult<Vec<MetricDefinition>>;
    async fn get_metrics(
        &self,
        query: &MetricQuery,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<Vec<MetricPayload>>;
    async fn send_metrics(
        &self,
        payload: Vec<MetricPayload>,
        node_addr: Option<SocketAddr>,
    ) -> MuseResult<()>;
}
