mod blocking;
mod concurrency;
mod query;
mod recovery;
mod status;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use chrono::{DateTime, Utc};

use rustvello_core::orchestrator::AtomicServiceExecution;
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::InvocationStatusRecord;

/// In-memory orchestrator implementation.
///
/// Manages invocation lifecycle with atomic status transitions.
/// All state is held in process memory.
pub struct OrchestratorState {
    pub invocations: HashMap<Arc<str>, InvocationDTO>,
    pub status_records: HashMap<Arc<str>, InvocationStatusRecord>,
    /// task_id string -> set of invocation_id strings
    pub(crate) task_invocations: HashMap<String, HashSet<Arc<str>>>,
    /// call_id string -> set of invocation_id strings
    pub(crate) call_invocations: HashMap<String, HashSet<Arc<str>>>,
    /// waiter_id -> waited_on_id
    pub(crate) waiting_for: HashMap<Arc<str>, Arc<str>>,
    /// waited_on_id -> set of waiter_ids
    pub(crate) waiters: HashMap<Arc<str>, HashSet<Arc<str>>>,
    /// Concurrency control index: (task_id, arg_key, arg_value) -> set of invocation_ids
    /// Each argument pair is indexed individually (matching pynenc's per-pair design).
    pub(crate) cc_index: HashMap<(String, String, String), HashSet<Arc<str>>>,
    /// Reverse lookup: invocation_id -> list of (task_id, arg_key, arg_value) tuples
    pub(crate) cc_reverse: HashMap<Arc<str>, Vec<(String, String, String)>>,
    /// Retry counters: invocation_id -> retry count
    pub(crate) retries: HashMap<Arc<str>, u32>,
    /// Runner heartbeats: runner_id -> last heartbeat time
    pub(crate) heartbeats: HashMap<Arc<str>, DateTime<Utc>>,
    /// Runner creation times: runner_id -> first seen time
    pub(crate) runner_created: HashMap<Arc<str>, DateTime<Utc>>,
    /// Atomic service eligibility per runner
    pub(crate) runner_atomic_eligible: HashMap<Arc<str>, bool>,
    /// Last atomic service execution start time per runner
    pub(crate) runner_last_service_start: HashMap<Arc<str>, DateTime<Utc>>,
    /// Last atomic service execution end time per runner
    pub(crate) runner_last_service_end: HashMap<Arc<str>, DateTime<Utc>>,
    /// Full atomic service execution timeline (up to 200 most recent)
    pub(crate) atomic_timeline: Vec<AtomicServiceExecution>,
    /// Auto-purge schedule: invocation_id -> scheduled_at timestamp
    pub(crate) auto_purge_queue: HashMap<Arc<str>, DateTime<Utc>>,
}

pub struct MemOrchestrator {
    /// Internal state. Use query/mutation methods instead of direct access.
    pub(crate) state: Mutex<OrchestratorState>,
}

impl MemOrchestrator {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(OrchestratorState {
                invocations: HashMap::with_capacity(64),
                status_records: HashMap::with_capacity(64),
                task_invocations: HashMap::with_capacity(8),
                call_invocations: HashMap::with_capacity(64),
                waiting_for: HashMap::new(),
                waiters: HashMap::new(),
                cc_index: HashMap::with_capacity(64),
                cc_reverse: HashMap::with_capacity(64),
                retries: HashMap::new(),
                heartbeats: HashMap::with_capacity(4),
                runner_created: HashMap::with_capacity(4),
                runner_atomic_eligible: HashMap::with_capacity(4),
                runner_last_service_start: HashMap::with_capacity(4),
                runner_last_service_end: HashMap::with_capacity(4),
                atomic_timeline: Vec::new(),
                auto_purge_queue: HashMap::new(),
            }),
        }
    }
}

impl Default for MemOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

impl MemOrchestrator {
    /// Backdate the status record timestamp for a given invocation.
    ///
    /// Used in integration tests to simulate stale invocations for recovery
    /// testing. Not intended for use in production code.
    pub async fn backdate_status_for_testing(
        &self,
        inv_id: &InvocationId,
        offset: chrono::Duration,
    ) {
        let mut state = self.state.lock().await;
        if let Some(record) = state.status_records.get_mut(inv_id.as_str()) {
            record.timestamp = chrono::Utc::now() - offset;
        }
    }
}
