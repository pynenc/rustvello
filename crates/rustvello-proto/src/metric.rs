// crates/rustvello-proto/src/metric.rs

use serde::{Deserialize, Serialize};

use crate::types::*;
use crate::utils::deterministic_u32_from_str;

pub fn metric_id_from_code(code: &str) -> u32 {
    deterministic_u32_from_str(code)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MetricDefinition {
    pub id: MetricId,
    pub code: String,
    pub name: String,
    pub description: String,
}

impl MetricDefinition {
    pub fn new(code: &str, name: &str, description: &str) -> Self {
        Self {
            id: metric_id_from_code(code),
            code: code.to_string(),
            name: name.to_string(),
            description: description.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MetricPayload {
    pub time: Timestamp,
    pub element_id: ElementId,
    pub metric_ids: Vec<MetricId>,
    pub values: Vec<Option<MetricValue>>,
}

impl MetricPayload {
    pub fn new(
        time: Timestamp,
        element_id: ElementId,
        metric_ids: Vec<MetricId>,
        values: Vec<Option<MetricValue>>,
    ) -> Self {
        Self {
            time,
            element_id,
            metric_ids,
            values,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
pub struct MetricQuery {
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub element_id: Option<u64>,
    pub parent_id: Option<u64>,
    pub metric_id: Option<u32>,
}

impl MetricQuery {
    pub fn new(
        start_time: Option<i64>,
        end_time: Option<i64>,
        element_id: Option<u64>,
        parent_id: Option<u64>,
        metric_id: Option<u32>,
    ) -> Self {
        Self {
            start_time,
            end_time,
            element_id,
            parent_id,
            metric_id,
        }
    }
}
