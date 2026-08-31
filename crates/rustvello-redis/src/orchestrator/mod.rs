mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::status::InvocationStatusRecord;

use crate::connection::RedisPool;

fn prefixed_key(prefix: &str, suffix: &str) -> String {
    let mut s = String::with_capacity(prefix.len() + suffix.len());
    s.push_str(prefix);
    s.push_str(suffix);
    s
}

/// Build a Redis key for a single CC arg pair.
fn cc_pair_redis_key(cc_prefix: &str, task_id: &str, arg_key: &str, arg_value: &str) -> String {
    format!("{cc_prefix}{task_id}\x1f{arg_key}\x1f{arg_value}")
}

/// Redis-backed orchestrator for distributed invocation lifecycle management.
///
/// Uses Redis hashes for status records, sets for indexes, and atomic
/// operations for concurrency safety.
#[non_exhaustive]
pub struct RedisOrchestrator {
    pub(crate) pool: Arc<RedisPool>,
    pub(crate) status_prefix: String,
    pub(crate) task_inv_prefix: String,
    pub(crate) call_inv_prefix: String,
    pub(crate) waiters_prefix: String,
    pub(crate) cc_prefix: String,
    pub(crate) cc_rev_prefix: String,
    pub(crate) heartbeat_prefix: String,
    pub(crate) retries_prefix: String,
    pub(crate) auto_purge_prefix: String,
    pub(crate) atomic_timeline_key: String,
    pub(crate) atomic_timeline_sequence_key: String,
}

impl RedisOrchestrator {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let p = pool.prefix();
        Self {
            status_prefix: format!("{p}orch:status:"),
            task_inv_prefix: format!("{p}orch:task_inv:"),
            call_inv_prefix: format!("{p}orch:call_inv:"),
            waiters_prefix: format!("{p}orch:waiters:"),
            cc_prefix: format!("{p}orch:cc:"),
            cc_rev_prefix: format!("{p}orch:cc_rev:"),
            heartbeat_prefix: format!("{p}orch:heartbeat:"),
            retries_prefix: format!("{p}orch:retries:"),
            auto_purge_prefix: format!("{p}orch:auto_purge:"),
            atomic_timeline_key: format!("{p}orch:atomic_timeline"),
            atomic_timeline_sequence_key: format!("{p}orch:atomic_timeline_sequence"),
            pool,
        }
    }

    fn status_key(&self, inv_id: &rustvello_proto::identifiers::InvocationId) -> String {
        prefixed_key(&self.status_prefix, inv_id.as_str())
    }
}

fn serialize_status_record(record: &InvocationStatusRecord) -> RustvelloResult<String> {
    serde_json::to_string(record).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}

fn deserialize_status_record(s: &str) -> RustvelloResult<InvocationStatusRecord> {
    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
        message: format!("status record: {}", e),
    })
}
