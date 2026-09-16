//! Bounded shutdown of the existing persistent runner.

use std::future::Future;
use std::time::Duration;

use rustvello_core::error::RustvelloResult;
use rustvello_core::runner::Runner;

use super::PersistentTokioRunner;

/// Whether runner shutdown drained its workers before the caller's budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// The runner finished and all of its worker futures were joined.
    Drained,
    /// The drain budget expired; abortion of unfinished worker futures was requested.
    DeadlineElapsed,
}

impl PersistentTokioRunner {
    /// Run until `signal`, then stop admission and drain for at most `budget`.
    ///
    /// A ready signal is checked before starting the runner. An absolute run
    /// deadline can be supplied as `tokio::time::sleep_until(deadline)`.
    /// On expiry, dropping the existing run future aborts its worker JoinSet;
    /// no second scheduler or replacement task execution is introduced.
    ///
    /// This bounds the caller's asynchronous wait, not synchronous task code.
    /// Tasks that can block must set `TaskConfig::blocking`; Tokio must remain
    /// able to poll timers. Already running blocking tasks and backend calls
    /// can outlive this method, and process/runtime shutdown may still wait for
    /// them. Interrupted committed Pending/Running claims use stale recovery;
    /// `DeadlineElapsed` does not certify rollback or cancellation of effects.
    pub async fn with_bounded_shutdown<F>(
        self,
        signal: F,
        budget: Duration,
    ) -> RustvelloResult<ShutdownOutcome>
    where
        F: Future<Output = ()> + Send,
    {
        tokio::pin!(signal);
        let run = self.run();
        tokio::pin!(run);
        tokio::select! {
            biased;
            _ = &mut signal => self.shutdown().await?,
            result = &mut run => return result.map(|()| ShutdownOutcome::Drained),
        }
        match tokio::time::timeout(budget, &mut run).await {
            Ok(result) => result.map(|()| ShutdownOutcome::Drained),
            Err(_) => Ok(ShutdownOutcome::DeadlineElapsed),
        }
    }
}
