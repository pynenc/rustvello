use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{CallId, InvocationId, RunnerId, TaskId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::error::RustvelloResult;

/// A recorded execution of the atomic global service by a specific runner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicServiceExecution {
    pub runner_id: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl AtomicServiceExecution {
    pub fn duration_secs(&self) -> f64 {
        (self.end - self.start).num_milliseconds() as f64 / 1000.0
    }
}

/// Information about an active runner, including heartbeat and atomic service metadata.
///
/// All timestamps use wall-clock `DateTime<Utc>` so the struct can cross
/// FFI boundaries and be serialized/persisted by any backend.
#[derive(Debug, Clone)]
pub struct ActiveRunnerInfo {
    pub runner_id: RunnerId,
    pub creation_time: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub can_run_atomic_service: bool,
    /// Wall-clock time of the last atomic service execution start.
    pub last_service_start: Option<DateTime<Utc>>,
    /// Wall-clock time of the last atomic service execution end.
    pub last_service_end: Option<DateTime<Utc>>,
}

/// Orchestrator interface — manages the invocation lifecycle.
///
/// Mirrors pynenc's `BaseOrchestrator`. Responsible for:
/// - Creating and registering invocations
/// - Managing status transitions (atomic state machine)
/// - Routing calls to the broker
/// - Concurrency control
/// - Blocking control (waiting for results)
///
/// This is a composite trait combining five sub-traits:
/// - [`OrchestratorStatus`] — registration, status, retries, cleanup
/// - [`OrchestratorConcurrency`] — concurrency control indexing
/// - [`OrchestratorBlocking`] — waiting/release for blocking invocations
/// - [`OrchestratorQuery`] — listing, filtering, pagination
/// - [`OrchestratorRecovery`] — heartbeats, stale detection, atomic service
///
/// Implementations should implement the sub-traits directly.
/// This supertrait is auto-implemented via a blanket impl.
pub trait Orchestrator:
    OrchestratorStatus
    + OrchestratorConcurrency
    + OrchestratorBlocking
    + OrchestratorQuery
    + OrchestratorRecovery
{
}

impl<
        T: OrchestratorStatus
            + OrchestratorConcurrency
            + OrchestratorBlocking
            + OrchestratorQuery
            + OrchestratorRecovery,
    > Orchestrator for T
{
}

// ===========================================================================
// OrchestratorStatus — registration, status transitions, retries, cleanup
// ===========================================================================

/// Invocation lifecycle and status management.
///
/// Handles registration, status transitions, retry tracking, and cleanup
/// (purge / auto-purge).
#[async_trait]
pub trait OrchestratorStatus: Send + Sync {
    // --- Invocation registration ---

    /// Register a new invocation for the given call.
    /// Sets initial status to `Registered`.
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId>;

    /// Register an invocation with a pre-existing ID and call.
    ///
    /// Used when the invocation ID is generated externally (e.g., by a
    /// language framework such as pynenc). Sets initial status to
    /// `Registered` and indexes by task and call for later queries.
    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord>;

    // --- Retry tracking ---

    /// Increment the retry counter for an invocation. Returns the new count.
    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32>;

    /// Get the current retry count for an invocation.
    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32>;

    /// Remove an invocation from all indexes (status, task, call, retries, CC).
    /// Used during auto-purge of terminal invocations.
    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()>;

    // --- Status management ---

    /// Get the current status of an invocation.
    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord>;

    /// Atomically transition an invocation to a new status.
    /// Validates the transition against the state machine.
    /// `runner_id` is required for Running/RunningRecovery transitions.
    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord>;

    // --- Introspection ---

    /// Human-readable name of the orchestrator backend (e.g. "In-Memory", "SQLite").
    fn backend_name(&self) -> &'static str {
        "Unknown"
    }

    /// Key-value statistics about this orchestrator's current state.
    ///
    /// Each entry is a `(label, value)` pair for display. Implementations
    /// should return counts, sizes, or any relevant runtime metrics.
    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }

    // --- Cleanup ---

    /// Remove all orchestrator data (invocations, statuses, CC index, etc.).
    ///
    /// Used in tests and monitoring dashboard "purge" action.
    /// Mirrors pynenc's `BaseOrchestrator.purge`.
    async fn purge(&self) -> RustvelloResult<()>;

    // --- Auto-purge ---

    /// Mark an invocation for future auto-purge.
    ///
    /// Stores `(invocation_id, now)` — the timestamp when the invocation
    /// became eligible for purging. The actual purge decision happens in
    /// `run_auto_purge`, which uses the *current* max-age configuration.
    async fn schedule_auto_purge(&self, invocation_id: &InvocationId) -> RustvelloResult<()>;

    /// Execute scheduled auto-purges older than `max_age_secs`.
    ///
    /// Purges invocations whose schedule timestamp is ≤ `now - max_age_secs`.
    /// Returns the IDs of purged invocations.
    async fn run_auto_purge(&self, max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>>;
}

// ===========================================================================
// OrchestratorConcurrency — concurrency control indexing and slot acquisition
// ===========================================================================

/// Concurrency control operations.
///
/// Manages the concurrency index that tracks which invocations are currently
/// using concurrency-controlled slots.
#[async_trait]
pub trait OrchestratorConcurrency: Send + Sync {
    /// Check if a new invocation is authorized to run given concurrency constraints.
    ///
    /// Returns `true` if the invocation can proceed, `false` if it should be
    /// held back or rejected. Mirrors pynenc's
    /// `is_candidate_to_run_by_concurrency_control` (checks **Pending + Running**
    /// invocations). Pynenc also has a stricter `is_authorize_to_run_by_concurrency_control`
    /// (Running only, used as a safety-net in `DistributedInvocation.run()`); the Rust
    /// runner replaces that with ownership claims + CC indexing before execution.
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool>;

    /// Index an invocation's arguments for concurrency control tracking.
    ///
    /// Called after creating a new invocation so future CC checks can find it.
    async fn index_for_concurrency_control(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<()>;

    /// Remove an invocation from the concurrency control index.
    ///
    /// Called when an invocation reaches a terminal state.
    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()>;

    /// Atomically check concurrency and index if under the limit.
    ///
    /// Returns `true` if the slot was acquired (invocation indexed),
    /// `false` if the task is already at its concurrency limit.
    ///
    /// The default implementation is a compatibility fallback. Production
    /// backends must override it with one atomic check-and-index operation;
    /// runner correctness relies on the index representing reserved slots.
    #[instrument(skip(self, task_config, cc_args), fields(%invocation_id, %task_id))]
    async fn try_acquire_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if self
            .check_running_concurrency(task_id, task_config, cc_args)
            .await?
        {
            self.index_for_concurrency_control(invocation_id, task_id, cc_args)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// ===========================================================================
// OrchestratorBlocking — waiting and release for blocking invocations
// ===========================================================================

/// Blocking control operations.
///
/// Manages the waiting graph: which invocations are waiting on other
/// invocations to complete before they can proceed.
#[async_trait]
pub trait OrchestratorBlocking: Send + Sync {
    /// Mark that `waiter` is waiting for `waited_on` to complete.
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()>;

    /// Get invocations that are waiting on the given invocation.
    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>>;

    /// Release all invocations waiting on the given completed invocation.
    async fn release_waiters(&self, completed: &InvocationId)
        -> RustvelloResult<Vec<InvocationId>>;
}

// ===========================================================================
// OrchestratorQuery — listing, filtering, pagination
// ===========================================================================

/// Query operations for finding and filtering invocations.
#[async_trait]
pub trait OrchestratorQuery: OrchestratorStatus {
    /// Get all invocation IDs for a given task.
    async fn get_invocations_by_task(&self, task_id: &TaskId)
        -> RustvelloResult<Vec<InvocationId>>;

    /// Get all invocation IDs for a given call.
    async fn get_invocations_by_call(&self, call_id: &CallId)
        -> RustvelloResult<Vec<InvocationId>>;

    /// Get invocations with a specific status, optionally filtered by task.
    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Count invocations, optionally filtered by task and/or statuses.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.count_invocations`.
    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize>;

    /// Get paginated invocation IDs, optionally filtered by task and statuses.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.get_invocation_ids_paginated`.
    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Filter a set of invocation IDs by status.
    ///
    /// Returns only the invocation IDs whose current status matches any status in the filter.
    /// Mirrors pynenc's `BaseOrchestrator.filter_by_status`.
    #[instrument(skip(self, invocation_ids, statuses), fields(count = invocation_ids.len()))]
    async fn filter_by_status(
        &self,
        invocation_ids: &[InvocationId],
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        // Default: check each invocation individually
        let mut result = Vec::new();
        for inv_id in invocation_ids {
            if let Ok(record) = self.get_invocation_status(inv_id).await {
                if statuses.contains(&record.status) {
                    result.push(inv_id.clone());
                }
            }
        }
        Ok(result)
    }

    /// Get invocations that are blocking other invocations but are not blocked themselves.
    ///
    /// Returns up to `max_num` invocations that have waiters but are themselves
    /// in an available-for-run status.
    /// Mirrors pynenc's `BaseBlockingControl.get_blocking_invocations`.
    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>>;

    /// Find existing invocations for a task, filtered by concurrency-control
    /// key arguments and statuses.
    ///
    /// Used by `route_call()` / `submit_with_cc()` to implement registration
    /// concurrency control — deduplicating invocations with the same key args
    /// that are still in a non-terminal status.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.get_existing_invocations`.
    ///
    /// - `cc_args`: When `Some`, only return invocations whose CC key matches.
    ///   When `None`, return all invocations for the task (task-level CC).
    /// - `statuses`: Only return invocations in one of these statuses.
    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>>;
}

// ===========================================================================
// OrchestratorRecovery — heartbeats, stale detection, runner info
// ===========================================================================

/// Recovery and heartbeat operations for distributed runner management.
#[async_trait]
pub trait OrchestratorRecovery: Send + Sync {
    /// Register a heartbeat for a runner, indicating it is still alive.
    ///
    /// `can_run_atomic_service` marks whether this runner is eligible to run
    /// shared atomic services (recovery, triggers). Parent/main runners
    /// typically pass `true`; child workers pass `false`.
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        can_run_atomic_service: bool,
    ) -> RustvelloResult<()>;

    /// Get invocations stuck in Pending beyond the configured threshold.
    ///
    /// Returns invocation IDs that have been in Pending status for longer
    /// than `max_pending_seconds`.
    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Get Running invocations owned by runners that haven't sent a
    /// heartbeat within `runner_dead_after_seconds`.
    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Get the IDs of all runners that have registered a heartbeat within the
    /// given timeout window.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.get_active_runners`.
    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>>;

    /// Get detailed info for active runners, optionally filtered by
    /// `can_run_atomic_service` eligibility.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.get_active_runners`.
    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>>;

    // --- Atomic service coordination ---

    /// Record that this runner executed the atomic global service.
    ///
    /// Called by the runner after recovery + trigger evaluation completes.
    async fn record_atomic_service_execution(
        &self,
        runner_id: &RunnerId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> RustvelloResult<()>;

    /// Get the timeline of atomic service executions (most recent first).
    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>>;
}
