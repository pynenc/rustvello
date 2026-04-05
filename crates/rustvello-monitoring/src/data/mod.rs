//! Data access layer for monitoring queries.

pub mod backend;

use async_trait::async_trait;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::InvocationStatus;

use rustvello_core::error::TaskError;

/// Abstraction over monitoring-specific queries against rustvello backends.
#[async_trait]
pub trait MonitorDataSource: Send + Sync {
    /// Get an invocation by ID.
    async fn get_invocation(&self, id: &InvocationId) -> RustvelloResult<InvocationDTO>;

    /// Get the current status of an invocation.
    async fn get_invocation_status(&self, id: &InvocationId) -> RustvelloResult<InvocationStatus>;

    /// Get all invocation IDs with a specific status.
    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
    ) -> RustvelloResult<Vec<InvocationId>>;

    /// Get all invocation IDs for a given task.
    async fn get_invocations_by_task(&self, task_id: &TaskId)
        -> RustvelloResult<Vec<InvocationId>>;

    /// Get the full status history for an invocation.
    async fn get_history(&self, id: &InvocationId) -> RustvelloResult<Vec<InvocationHistory>>;

    /// Get the result of a completed invocation.
    async fn get_result(&self, id: &InvocationId) -> RustvelloResult<Option<String>>;

    /// Get error info for a failed invocation.
    async fn get_error(&self, id: &InvocationId) -> RustvelloResult<Option<TaskError>>;

    /// Get child invocations of a parent invocation.
    async fn get_child_invocations(&self, id: &InvocationId) -> RustvelloResult<Vec<InvocationId>>;

    /// Get a call by ID.
    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO>;

    /// Count pending invocations in the broker.
    async fn count_broker_pending(&self) -> RustvelloResult<usize>;

    /// List registered task IDs.
    fn get_registered_task_ids(&self) -> &[TaskId];
}
