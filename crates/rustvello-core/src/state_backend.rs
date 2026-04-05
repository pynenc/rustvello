use async_trait::async_trait;

use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};

use crate::context::RunnerContext;

use crate::error::{RustvelloResult, TaskError};

/// Runner execution context — metadata about the environment running an invocation.
///
/// Mirrors pynenc's `RunnerContext`: captures runner type, PID, hostname,
/// thread ID, and optional parent context for hierarchical runner relationships
/// (e.g., PersistentProcessRunner → PPRWorker).
///
/// Named `StoredRunnerContext` to distinguish from `context::RunnerContext` (the
/// runtime task-local context used during invocation execution).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredRunnerContext {
    /// Class/type name of the runner (e.g. "TaskRunner", "PPRWorker").
    pub runner_cls: String,
    /// Unique identifier for this runner instance.
    pub runner_id: String,
    pub pid: u32,
    pub hostname: String,
    pub thread_id: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Parent runner ID, if this is a child worker.
    pub parent_runner_id: Option<String>,
    /// Parent runner class name.
    pub parent_runner_cls: Option<String>,
}

impl StoredRunnerContext {
    pub fn current(runner_id: impl Into<String>, runner_cls: impl Into<String>) -> Self {
        let thread_id = format!("{:?}", std::thread::current().id());
        let thread_num: u64 = thread_id
            .trim_start_matches("ThreadId(")
            .trim_end_matches(')')
            .parse()
            .unwrap_or(0);
        Self {
            runner_cls: runner_cls.into(),
            runner_id: runner_id.into(),
            pid: std::process::id(),
            hostname: hostname::get().map_or_else(
                |_| "unknown".to_string(),
                |h| h.to_string_lossy().into_owned(),
            ),
            thread_id: thread_num,
            started_at: chrono::Utc::now(),
            parent_runner_id: None,
            parent_runner_cls: None,
        }
    }

    /// Create a child runner context with this context as parent.
    pub fn new_child(
        &self,
        child_runner_id: impl Into<String>,
        child_runner_cls: impl Into<String>,
    ) -> Self {
        let mut child = Self::current(child_runner_id, child_runner_cls);
        child.parent_runner_id = Some(self.runner_id.clone());
        child.parent_runner_cls = Some(self.runner_cls.clone());
        child
    }

    /// Convert a runtime `RunnerContext` into a `StoredRunnerContext`.
    ///
    /// Maps the runtime context (used in task-local/thread-local storage) to
    /// the persistent format stored in the state backend for monitoring.
    pub fn from_runtime(ctx: &RunnerContext) -> Self {
        Self {
            runner_cls: ctx.runner_cls.as_ref().to_string(),
            runner_id: ctx.runner_id.to_string(),
            pid: ctx.pid,
            hostname: ctx.hostname.clone(),
            thread_id: ctx.thread_id,
            started_at: chrono::Utc::now(),
            parent_runner_id: ctx.parent_ctx.as_ref().map(|p| p.runner_id.to_string()),
            parent_runner_cls: ctx
                .parent_ctx
                .as_ref()
                .map(|p| p.runner_cls.as_ref().to_string()),
        }
    }
}

/// State backend interface — persistence of invocations and results.
///
/// Mirrors pynenc's `BaseStateBackend`. This is a composite trait combining three sub-traits:
/// - [`StateBackendCore`] — invocation/result/history storage, cleanup, introspection
/// - [`StateBackendQuery`] — workflow queries, workflow discovery, workflow data
/// - [`StateBackendRunner`] — runner context, runner analytics, time-range queries
///
/// Implementations should implement the sub-traits directly.
/// This supertrait is auto-implemented via a blanket impl.
pub trait StateBackend: StateBackendCore + StateBackendQuery + StateBackendRunner {}

impl<T: StateBackendCore + StateBackendQuery + StateBackendRunner> StateBackend for T {}

// ===========================================================================
// Sub-trait: StateBackendCore
// ===========================================================================

/// Core state persistence — invocation storage, results, history, and cleanup.
///
/// All methods in this sub-trait are required (no defaults).
#[async_trait]
pub trait StateBackendCore: Send + Sync {
    // --- Invocation storage ---

    /// Store or update an invocation and its associated call.
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()>;

    /// Retrieve an invocation by ID.
    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO>;

    /// Retrieve a call by ID.
    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO>;

    // --- Result storage ---

    /// Store the result of a successful invocation.
    async fn store_result(&self, invocation_id: &InvocationId, result: &str)
        -> RustvelloResult<()>;

    /// Retrieve the result of a completed invocation.
    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>>;

    /// Store error information for a failed invocation.
    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()>;

    /// Retrieve error information for a failed invocation.
    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>>;

    // --- History ---

    /// Record a status change in the audit log.
    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()>;

    /// Get the full status history for an invocation.
    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>>;

    // --- Cleanup ---

    /// Purge all stored data.
    async fn purge(&self) -> RustvelloResult<()>;

    /// Human-readable name of this backend implementation.
    fn backend_name(&self) -> &'static str {
        "Unknown"
    }

    /// Key-value statistics about this backend's current state.
    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

// ===========================================================================
// Sub-trait: StateBackendQuery
// ===========================================================================

/// Workflow queries, discovery, and data.
#[async_trait]
pub trait StateBackendQuery: Send + Sync {
    // --- Workflow queries ---

    /// Get all invocation IDs that belong to a workflow.
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Get direct child invocations of a parent invocation.
    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>>;

    // --- Workflow discovery ---

    /// Store a workflow run for tracking and monitoring.
    async fn store_workflow_run(
        &self,
        workflow: &rustvello_proto::invocation::WorkflowIdentity,
    ) -> RustvelloResult<()>;

    /// Retrieve all distinct workflow types (task IDs that have started workflows).
    async fn get_all_workflow_types(
        &self,
    ) -> RustvelloResult<Vec<rustvello_proto::identifiers::TaskId>>;

    /// Retrieve workflow run identities for a specific workflow type.
    async fn get_workflow_runs(
        &self,
        workflow_type: &rustvello_proto::identifiers::TaskId,
    ) -> RustvelloResult<Vec<rustvello_proto::invocation::WorkflowIdentity>>;

    // --- Workflow data (key-value store scoped to a workflow) ---

    /// Set a value in the workflow's key-value data store.
    async fn set_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
        value: &str,
    ) -> RustvelloResult<()>;

    /// Get a value from the workflow's key-value data store.
    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>>;

    // --- App info storage ---

    /// Store application info as opaque JSON.
    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()>;

    /// Get application info by ID.
    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>>;

    /// Get all stored application infos as (app_id, info_json) pairs.
    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>>;

    // --- Workflow sub-invocations ---

    /// Record a sub-invocation belonging to a workflow.
    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()>;

    /// Get all sub-invocations for a workflow.
    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>>;

    // --- Aggregated workflow queries ---

    /// Get all workflow runs across all types.
    ///
    /// Default implementation calls `get_all_workflow_types()` then
    /// `get_workflow_runs()` for each. Backends can override for efficiency.
    async fn get_all_workflow_runs(
        &self,
    ) -> RustvelloResult<Vec<rustvello_proto::invocation::WorkflowIdentity>> {
        let types = self.get_all_workflow_types().await?;
        let mut all = Vec::new();
        for t in &types {
            let runs = self.get_workflow_runs(t).await?;
            all.extend(runs);
        }
        Ok(all)
    }
}

// ===========================================================================
// Sub-trait: StateBackendRunner
// ===========================================================================

/// Runner context storage, analytics, and time-range queries.
#[async_trait]
pub trait StateBackendRunner: Send + Sync {
    // --- Runner context ---

    /// Store a runner's execution context for monitoring.
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()>;

    /// Retrieve a runner's execution context.
    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>>;

    /// Get all runner contexts whose `parent_runner_id` matches the given runner.
    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>>;

    /// Get invocation IDs that were processed by a specific runner (by runner_id in history).
    ///
    /// Returns paginated results. `limit` of 0 means no limit.
    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Count invocations that were processed by a specific runner.
    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize>;

    // --- Time-range queries ---

    /// Get history entries within a time range, for monitoring log explorers.
    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>>;

    // --- Matching runner contexts ---

    /// Get runner contexts whose runner_id contains the given partial string.
    ///
    /// Used for prefix/partial matching on runner IDs.
    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>>;
}
