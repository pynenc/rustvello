use std::sync::Arc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatusRecord;

use crate::connection::MongoPool;

mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;
#[cfg(test)]
mod tests;

pub(crate) const STATUS_COL: &str = "orch_status";
pub(crate) const WAITERS_COL: &str = "orch_waiters";
pub(crate) const CC_COL: &str = "orch_concurrency";
pub(crate) const HEARTBEAT_COL: &str = "orch_heartbeat";
pub(crate) const ATOMIC_TIMELINE_COL: &str = "orch_atomic_service_timeline";
pub(crate) const AUTO_PURGE_COL: &str = "orch_auto_purge";

/// Build the per-pair `_id` for the `orch_concurrency` collection.
pub(crate) fn cc_pair_mongo_key(task_id: &TaskId, arg_key: &str, arg_value: &str) -> String {
    format!("{}\x1f{}\x1f{}", task_id, arg_key, arg_value)
}

pub(crate) fn serialize_record(record: &InvocationStatusRecord) -> RustvelloResult<String> {
    serde_json::to_string(record).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}

pub(crate) fn deserialize_record(s: &str) -> RustvelloResult<InvocationStatusRecord> {
    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}

/// MongoDB-backed orchestrator for distributed invocation lifecycle management.
#[non_exhaustive]
pub struct Mongo3Orchestrator {
    pub(crate) pool: Arc<MongoPool>,
}

impl Mongo3Orchestrator {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}
