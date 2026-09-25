//! Execution deadline and cooperative cancellation around one task attempt.
//!
//! The attempt future is raced against the task's deadline and a periodic
//! check of the invocation status. When either wins, the attempt future is
//! dropped:
//!
//! - async task bodies are aborted at their next `.await` point;
//! - subprocess executors kill the worker process (`kill_on_drop`);
//! - sync bodies on a blocking/Rayon thread cannot be stopped safely. The
//!   thread runs to completion in the background (still holding its
//!   executor permit, so concurrency stays bounded) and its result is
//!   discarded. Sync bodies that never yield (not `blocking`) are only
//!   checked after they return; a result past the deadline is discarded.
//!
//! The caller then raises the attempt's [`AttemptSignal`], so code that cannot
//! be preempted can still stop: Python coroutines are cancelled on their
//! worker loop, and sync bodies may poll the signal.
//!
//! [`AttemptSignal`]: rustvello_core::context::AttemptSignal

use std::future::Future;
use std::time::{Duration, Instant};

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::config::TASK_TIMEOUT_ERROR;
use rustvello_proto::identifiers::InvocationId;

use crate::orchestration::Orchestrator;

/// Why an attempt stopped before producing its own result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interruption {
    /// The execution deadline expired.
    TimedOut(Duration),
    /// A user cancelled the invocation while it was running.
    Cancelled,
}

/// Limits applied to one attempt.
pub(crate) struct AttemptLimits {
    pub timeout: Option<Duration>,
    pub cancel_check: Option<Duration>,
}

impl AttemptLimits {
    pub(crate) fn new(timeout: Option<Duration>, cancel_check_seconds: f64) -> Self {
        let cancel_check = (cancel_check_seconds.is_finite() && cancel_check_seconds > 0.0)
            .then(|| Duration::from_secs_f64(cancel_check_seconds.max(0.01)));
        Self {
            timeout,
            cancel_check,
        }
    }
}

/// Error recorded for an attempt that exceeded its deadline.
pub(crate) fn timeout_error(timeout: Duration) -> RustvelloError {
    RustvelloError::TaskExecution {
        error_type: TASK_TIMEOUT_ERROR.to_owned(),
        message: format!(
            "attempt exceeded its execution deadline of {} ms",
            timeout.as_millis()
        ),
        traceback: None,
    }
}

/// Run `attempt` under `limits`.
///
/// Returns the attempt's own result, or a synthetic error plus the reason
/// when the attempt was interrupted (the attempt future is dropped then).
pub(crate) async fn supervise<F>(
    attempt: F,
    limits: &AttemptLimits,
    lifecycle: &Orchestrator,
    invocation_id: &InvocationId,
) -> (RustvelloResult<String>, Option<Interruption>)
where
    F: Future<Output = RustvelloResult<String>>,
{
    if limits.timeout.is_none() && limits.cancel_check.is_none() {
        return (attempt.await, None);
    }
    let started = Instant::now();
    let deadline = limits
        .timeout
        .map(|timeout| tokio::time::Instant::now() + timeout);
    let mut ticker = limits.cancel_check.map(|every| {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker
    });
    tokio::pin!(attempt);
    loop {
        tokio::select! {
            biased;
            result = &mut attempt => {
                // A body that blocked the runtime past its deadline cannot be
                // preempted; its late result is discarded all the same.
                if let Some(timeout) = limits.timeout {
                    if started.elapsed() > timeout {
                        return (Err(timeout_error(timeout)), Some(Interruption::TimedOut(timeout)));
                    }
                }
                return (result, None);
            }
            () = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => {
                let timeout = limits.timeout.unwrap_or_default();
                tracing::warn!(%invocation_id, timeout_ms = timeout.as_millis() as u64, "attempt timed out; abandoning it");
                return (Err(timeout_error(timeout)), Some(Interruption::TimedOut(timeout)));
            }
            _ = async {
                match ticker.as_mut() {
                    Some(ticker) => ticker.tick().await,
                    None => std::future::pending().await,
                }
            } => {
                if lifecycle.is_cancelled(invocation_id).await {
                    tracing::info!(%invocation_id, "invocation cancelled while running; abandoning attempt");
                    return (
                        Err(RustvelloError::InvocationCancelled {
                            invocation_id: invocation_id.clone(),
                        }),
                        Some(Interruption::Cancelled),
                    );
                }
            }
        }
    }
}
