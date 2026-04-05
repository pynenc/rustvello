mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatusRecord;

use crate::connection::MongoPool;

/// Build a mongo CC document _id for a single arg pair.
fn cc_pair_mongo_key(task_id: &TaskId, arg_key: &str, arg_value: &str) -> String {
    format!("{}\x1f{}\x1f{}", task_id, arg_key, arg_value)
}

const STATUS_COL: &str = "orch_status";
const WAITERS_COL: &str = "orch_waiters";
const CC_COL: &str = "orch_concurrency";
const HEARTBEAT_COL: &str = "orch_heartbeat";

/// MongoDB-backed orchestrator for distributed invocation lifecycle management.
#[non_exhaustive]
pub struct MongoOrchestrator {
    pub(crate) pool: Arc<MongoPool>,
}

impl MongoOrchestrator {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}

fn serialize_record(record: &InvocationStatusRecord) -> RustvelloResult<String> {
    serde_json::to_string(record).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}

fn deserialize_record(s: &str) -> RustvelloResult<InvocationStatusRecord> {
    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}
