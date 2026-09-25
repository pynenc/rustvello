//! Durable retry backoff and user cancellation use cases.
//!
//! A delayed retry is committed as a Retry status plus a queued entry whose
//! not-before time lives in the broker's storage, never as an in-memory
//! sleep: a worker that dies during the backoff loses nothing, and the entry
//! is delivered once when due. Cancellation is a terminal status any client
//! may set; workers still running the attempt notice it cooperatively.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::publication::{PublicationChange, PublicationRoute};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::Orchestrator;

/// Result of a cancellation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CancelOutcome {
    /// The invocation was not finished and is now `Cancelled`.
    Cancelled,
    /// The invocation had already reached this terminal status; nothing changed.
    AlreadyFinal(InvocationStatus),
}

impl CancelOutcome {
    /// Whether this request moved the invocation to `Cancelled`.
    pub fn was_cancelled(self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

static DELAY_UNSUPPORTED_WARNED: AtomicBool = AtomicBool::new(false);

fn warn_delay_unsupported(invocation_id: &InvocationId, delay: Duration) {
    if !DELAY_UNSUPPORTED_WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            %invocation_id,
            delay_ms = delay.as_millis() as u64,
            "broker has no durable delayed delivery; retry backoff is ignored and \
             retries are routed immediately (warned once per process)"
        );
    }
}

impl Orchestrator {
    /// Whether retries with a backoff delay are stored durably by these backends.
    pub fn supports_durable_retry_delay(&self) -> RustvelloResult<bool> {
        Ok(match self.publication()? {
            Some(publication) => publication.supports_delayed_retry(),
            None => self.backends.broker.supports_delayed_delivery(),
        })
    }

    /// Set an invocation for retry that must not run before `delay` elapses.
    ///
    /// With a zero delay this is exactly [`Self::set_invocation_retry_with_context`].
    /// The not-before time is committed with the Retry status (transactional
    /// publication) or with the queued broker entry. Backends without durable
    /// delayed delivery degrade to an immediate retry and log a warning once.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_invocation_retry_after_with_context(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        task_id: &TaskId,
        arguments: BTreeMap<String, String>,
        queue_name: &str,
        priority: f64,
        delay: Duration,
    ) -> RustvelloResult<()> {
        if delay.is_zero() || !self.supports_durable_retry_delay()? {
            if !delay.is_zero() {
                warn_delay_unsupported(invocation_id, delay);
            }
            return self
                .set_invocation_retry_with_context(
                    invocation_id,
                    runner_id,
                    task_id,
                    arguments,
                    queue_name,
                    priority,
                )
                .await;
        }
        if let Some(publication) = self.publication()? {
            publication
                .change(
                    invocation_id,
                    runner_id,
                    PublicationChange::DelayedRetry {
                        route: PublicationRoute {
                            queue: queue_name.into(),
                            priority,
                        },
                        delay,
                    },
                    false,
                )
                .await?;
            return self
                .report_published_status(
                    invocation_id,
                    runner_id,
                    InvocationStatus::Retry,
                    task_id,
                    arguments,
                )
                .await;
        }
        self.set_invocation_status_with_context(
            invocation_id,
            InvocationStatus::Retry,
            runner_id,
            task_id,
            arguments,
        )
        .await?;
        self.backends
            .invocation_control
            .increment_invocation_retries(invocation_id)
            .await?;
        self.backends
            .broker
            .route_invocation_after(invocation_id, Some(task_id), queue_name, priority, delay)
            .await
    }

    /// Cancel an invocation that has not finished yet.
    ///
    /// Semantics:
    /// - already terminal (Success, Failed, Cancelled, ...): nothing changes and
    ///   [`CancelOutcome::AlreadyFinal`] reports the status;
    /// - queued, waiting for a retry, or concurrency-controlled: it becomes
    ///   `Cancelled` and will never run (its queued entry is dropped or
    ///   ignored when dequeued);
    /// - running: it becomes `Cancelled` at once; the worker notices within
    ///   `cancellation_check_interval_seconds`, stops awaiting the attempt
    ///   (async bodies are aborted, sync bodies are abandoned) and discards
    ///   any late result. Side effects already performed are not undone.
    ///
    /// Waiters are released and triggers see the `CANCELLED` status.
    pub async fn cancel_invocation(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
    ) -> RustvelloResult<CancelOutcome> {
        let current = self
            .backends
            .invocation_control
            .get_invocation_status(invocation_id)
            .await?
            .status;
        if current.is_terminal() {
            return Ok(CancelOutcome::AlreadyFinal(current));
        }
        match self
            .set_invocation_status(invocation_id, InvocationStatus::Cancelled, runner_id)
            .await
        {
            Ok(_) => {}
            // Finished between the read and the write: report what won.
            Err(RustvelloError::InvalidStatusTransition { .. })
            | Err(RustvelloError::StatusRaceCondition { .. }) => {
                let now = self
                    .backends
                    .invocation_control
                    .get_invocation_status(invocation_id)
                    .await?
                    .status;
                return Ok(if now == InvocationStatus::Cancelled {
                    CancelOutcome::Cancelled
                } else {
                    CancelOutcome::AlreadyFinal(now)
                });
            }
            Err(error) => return Err(error),
        }
        if let Err(error) = self
            .release_nontransactional_concurrency_slot(invocation_id)
            .await
        {
            tracing::warn!(%invocation_id, "failed to release concurrency slot after cancel: {error}");
        }
        tracing::info!(%invocation_id, from = %current, "invocation cancelled");
        Ok(CancelOutcome::Cancelled)
    }

    /// Whether the invocation has been cancelled (used by running workers).
    pub(crate) async fn is_cancelled(&self, invocation_id: &InvocationId) -> bool {
        matches!(
            self.backends
                .invocation_control
                .get_invocation_status(invocation_id)
                .await,
            Ok(record) if record.status == InvocationStatus::Cancelled
        )
    }
}
