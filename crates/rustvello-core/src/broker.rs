use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::error::RustvelloResult;

/// Message broker interface for routing invocations to runners.
///
/// Mirrors pynenc's `BaseBroker`. The broker is a queue that accepts
/// invocations from the orchestrator and delivers them to runners.
///
/// ## Cross-language routing
///
/// When a task has a non-empty `language` field in its [`TaskId`],
/// the broker routes it to a language-specific queue. Workers only
/// retrieve invocations for their own language via
/// [`retrieve_invocation_for_language`].
#[async_trait]
pub trait Broker: Send + Sync {
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
    /// Returns invocations routed to the given language queue.
    ///
    /// **Note:** Implementations that check the global queue first (e.g. `MemBroker`)
    /// may "steal" invocations intended for all workers before language-agnostic
    /// workers get a chance. In mixed-language deployments, prefer routing
    /// language-specific tasks to per-task queues via [`route_invocation_for_task`]
    /// and use [`retrieve_invocation`] with `None` only for language-agnostic work.
    ///
    /// Backends must preserve language filtering while allowing global queue
    /// items to be consumed by any language worker.
    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
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
