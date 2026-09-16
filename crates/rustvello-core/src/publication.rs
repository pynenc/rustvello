//! Optional, co-located transaction port. Mixed backends must not claim this capability.

use std::sync::Arc;

use async_trait::async_trait;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::error::{RustvelloResult, TaskError};
use crate::state_backend::StoredRunnerContext;

/// An identity shared only by ports backed by the same transaction manager.
pub type PublicationDomain = Arc<str>;

#[derive(Clone)]
pub struct PublicationRoute {
    pub queue: String,
    pub priority: f64,
}

#[derive(Clone)]
pub struct SubmissionPublication {
    pub invocation: InvocationDTO,
    pub call: CallDTO,
    pub runner_id: RunnerId,
    pub runner_context: Option<StoredRunnerContext>,
    pub workflow_root: bool,
    pub cc_arguments: Option<SerializedArguments>,
    pub route: PublicationRoute,
}

#[derive(Clone)]
pub enum PublicationChange {
    Status(InvocationStatus),
    Retry(PublicationRoute),
    Reroute(PublicationRoute),
    ConcurrencyReroute(PublicationRoute),
    Recover {
        status: InvocationStatus,
        stale_after_seconds: u64,
        route: PublicationRoute,
    },
    Success(String),
    Failure(TaskError),
}

#[async_trait]
pub trait RuntimePublication: Send + Sync {
    fn domain(&self) -> PublicationDomain;

    async fn begin_execution(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        retries: u32,
        incoming: &rustvello_proto::invocation::TraceContextCarrier,
    ) -> RustvelloResult<rustvello_proto::invocation::ExecutionAttemptIdentity>;

    /// Commit the entire submission, or verify an identical existing submission.
    /// Returns false on a replay; never resets an existing invocation's state.
    async fn submit(&self, submission: SubmissionPublication) -> RustvelloResult<bool>;

    /// Commit status, history, retry count, payload and queue effects together.
    /// A raced/non-stale recovery returns None without mutation.
    async fn change(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        change: PublicationChange,
        auto_purge: bool,
    ) -> RustvelloResult<Option<InvocationStatusRecord>>;
}
