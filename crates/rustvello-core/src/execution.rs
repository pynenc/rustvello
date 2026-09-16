//! Durable execution identity on app-scoped backends.

use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::invocation::{ExecutionAttemptIdentity, TraceContextCarrier};

use crate::error::{RustvelloError, RustvelloResult};
use crate::observability::allocate_execution_trace_context;
use crate::state_backend::StateBackend;

pub const IDENTITY_KEY: &str = "rustvello.execution.identity.v1";

/// Read the last persisted execution identity, independently of exporter state.
pub async fn get_execution_identity(
    backend: &dyn StateBackend,
    invocation_id: &InvocationId,
) -> RustvelloResult<Option<ExecutionAttemptIdentity>> {
    backend
        .get_workflow_data(invocation_id, IDENTITY_KEY)
        .await?
        .map(|value| {
            serde_json::from_str(&value).map_err(|error| RustvelloError::Serialization {
                message: error.to_string(),
            })
        })
        .transpose()
}

/// Allocate and persist before dispatch. The caller must hold invocation ownership.
/// Retains only the current and preceding identities, including recovered executions.
/// The retry count is a lower bound; each recovered execution also advances identity.
pub async fn begin_execution(
    backend: &dyn StateBackend,
    invocation_id: &InvocationId,
    retry_count: u32,
    incoming: &TraceContextCarrier,
) -> RustvelloResult<ExecutionAttemptIdentity> {
    let previous = get_execution_identity(backend, invocation_id).await?;
    let identity = next_execution_identity(previous, retry_count, incoming)?;
    let encoded =
        serde_json::to_string(&identity).map_err(|error| RustvelloError::Serialization {
            message: error.to_string(),
        })?;
    backend
        .set_workflow_data(invocation_id, IDENTITY_KEY, &encoded)
        .await?;
    Ok(identity)
}

/// Pure identity allocation shared by transactional and generic backends.
pub fn next_execution_identity(
    previous: Option<ExecutionAttemptIdentity>,
    retry_count: u32,
    incoming: &TraceContextCarrier,
) -> RustvelloResult<ExecutionAttemptIdentity> {
    let attempt = match previous.as_ref() {
        Some(identity) => {
            retry_count.max(identity.attempt.checked_add(1).ok_or_else(|| {
                RustvelloError::state_backend("execution attempt counter exhausted")
            })?)
        }
        None => retry_count,
    };
    // Root retries share the first execution's trace even without an incoming parent.
    let parent = if incoming.is_empty() {
        previous
            .as_ref()
            .map_or(incoming, |identity| &identity.execution_trace_context)
    } else {
        incoming
    };
    let identity = ExecutionAttemptIdentity {
        attempt,
        execution_trace_context: allocate_execution_trace_context(parent),
        previous_attempt_trace_context: previous
            .map(|identity| identity.execution_trace_context)
            .unwrap_or_default(),
    };
    Ok(identity)
}
