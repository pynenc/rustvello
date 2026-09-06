use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use rustvello_proto::config::{MAX_PRIORITY, MIN_PRIORITY};
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};

use crate::error::RustvelloError;
use crate::error::RustvelloResult;

pub const DEFAULT_QUEUE: &str = "default";

/// Validate backend-independent queue and priority values.
pub fn validate_routing(queue_name: &str, priority: f64) -> RustvelloResult<()> {
    if queue_name.is_empty()
        || !queue_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(RustvelloError::Configuration {
            message: format!("invalid queue name {queue_name:?}; expected [A-Za-z0-9_.-]+"),
        });
    }
    if !priority.is_finite() || !(MIN_PRIORITY..=MAX_PRIORITY).contains(&priority) {
        return Err(RustvelloError::Configuration {
            message: format!(
                "priority must be a finite float between {MIN_PRIORITY} and {MAX_PRIORITY}"
            ),
        });
    }
    Ok(())
}

/// Message broker interface for routing invocations to runners.
///
/// Mirrors pynenc's `BaseBroker`. The broker is a queue that accepts
/// invocations from the orchestrator and delivers them to runners.
///
/// ## Cross-language routing
///
/// Every task carries a [`TaskLanguage`] in its [`TaskId`]. Workers only
/// retrieve invocations for their own language via
/// [`retrieve_invocation_for_language`].
#[async_trait]
pub trait Broker: Send + Sync {
    /// Queue an invocation with independent task, logical queue, and priority routing.
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()>;

    /// Retrieve from one logical queue, optionally filtered by task.
    async fn retrieve_invocation_from_queue(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>>;

    /// Retrieve from one logical queue for a language worker.
    async fn retrieve_invocation_for_language_from_queue(
        &self,
        language: TaskLanguage,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>>;

    /// Count invocations in the selected logical queues.
    async fn count_invocations_in_queues(
        &self,
        queue_names: &[String],
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize>;

    /// Queue an invocation for processing by a runner.
    ///
    /// When the task ID is unknown at the call site this selects the global
    /// queue. Callers with a task ID must use
    /// [`route_invocation_for_task`] to preserve routing identity.
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()>;

    /// Queue an invocation for processing, with the task ID for per-task routing.
    ///
    /// Backends must preserve the task identity so task-filtered retrieval
    /// remains correct. A global-only queue is not a complete implementation.
    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()>;

    /// Queue multiple invocations at once (batch optimization).
    #[instrument(skip(self, ids), fields(count = ids.len()))]
    async fn route_invocations(&self, ids: &[InvocationId]) -> RustvelloResult<()> {
        for id in ids {
            self.route_invocation(id).await?;
        }
        Ok(())
    }

    /// Retrieve the next invocation to process.
    /// Returns `None` if the queue is empty.
    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>>;

    /// Retrieve the next invocation for a specific language worker.
    ///
    /// Returns invocations routed to the given language partition. Worker paths
    /// must never scan, consume, or requeue another language's work.
    ///
    /// The legacy [`route_invocation`] method has no task identity and is treated
    /// as Rust work. Mixed-language application paths must route with a `TaskId`.
    async fn retrieve_invocation_for_language(
        &self,
        language: TaskLanguage,
    ) -> RustvelloResult<Option<InvocationId>>;

    /// Retrieve up to `max` invocations at once (batch optimization).
    ///
    /// Default implementation calls [`retrieve_invocation`] in a loop.
    /// Backends should override for a single lock acquisition.
    #[instrument(skip(self))]
    async fn retrieve_invocations(
        &self,
        max: usize,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let capped = max.min(10_000);
        let mut results = Vec::with_capacity(capped);
        for _ in 0..capped {
            match self.retrieve_invocation(task_id).await? {
                Some(id) => results.push(id),
                None => break,
            }
        }
        Ok(results)
    }

    /// Block until work is available or cancellation is requested.
    ///
    /// Returns `true` if work may be available, `false` if cancelled.
    /// Default implementation sleeps for 100ms. Backends with notification
    /// support (e.g. `MemBroker`) should override with zero-cost waiting.
    async fn wait_for_work(&self, cancel: &CancellationToken) -> bool {
        tokio::select! {
            _ = cancel.cancelled() => false,
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => true,
        }
    }

    /// Count queued invocations, optionally filtered by task.
    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize>;

    /// Remove all queued invocations.
    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()>;
}
