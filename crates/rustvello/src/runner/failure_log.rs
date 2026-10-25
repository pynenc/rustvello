//! Rate-limited ERROR logging for background work that repeats (heartbeats,
//! runner context writes).
//!
//! A backend that refuses every heartbeat used to produce one WARN line per
//! interval, easy to miss under INFO noise. `FailureLog` logs the first failure
//! at ERROR, then at most one ERROR per minute carrying the number of failures
//! since the last line, and one INFO line when the work succeeds again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) struct FailureLog {
    what: &'static str,
    interval: Duration,
    /// Failures since the last line was logged.
    pending: AtomicU64,
    /// Failures since the last success.
    total: AtomicU64,
    last_logged: Mutex<Option<Instant>>,
}

impl FailureLog {
    pub(crate) fn new(what: &'static str) -> Self {
        Self::with_interval(what, Duration::from_secs(60))
    }

    pub(crate) fn with_interval(what: &'static str, interval: Duration) -> Self {
        Self {
            what,
            interval,
            pending: AtomicU64::new(0),
            total: AtomicU64::new(0),
            last_logged: Mutex::new(None),
        }
    }

    /// Record a failure; returns whether a line was logged.
    pub(crate) fn failed(&self, error: &dyn std::fmt::Display) -> bool {
        let pending = self.pending.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.total.fetch_add(1, Ordering::Relaxed) + 1;
        let mut last = self
            .last_logged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let due = last.is_none_or(|at| at.elapsed() >= self.interval);
        if !due {
            return false;
        }
        *last = Some(Instant::now());
        self.pending.store(0, Ordering::Relaxed);
        if total == 1 {
            tracing::error!("{} failed: {}", self.what, error);
        } else {
            tracing::error!(
                "{} still failing ({} failures in the last {}s, {} since the last success): {}",
                self.what,
                pending,
                self.interval.as_secs(),
                total,
                error
            );
        }
        true
    }

    /// Record a success; logs the recovery once after failures.
    pub(crate) fn succeeded(&self) {
        let total = self.total.swap(0, Ordering::Relaxed);
        if total > 0 {
            self.pending.store(0, Ordering::Relaxed);
            *self
                .last_logged
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            tracing::info!("{} recovered after {} failures", self.what, total);
        }
    }

    /// Record the outcome of one attempt.
    pub(crate) fn record<T, E: std::fmt::Display>(&self, result: &Result<T, E>) {
        match result {
            Ok(_) => self.succeeded(),
            Err(error) => {
                self.failed(error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FailureLog;
    use std::time::Duration;

    #[test]
    fn first_failure_logs_then_rate_limits_until_recovery() {
        let log = FailureLog::with_interval("heartbeat", Duration::from_secs(3600));
        assert!(log.failed(&"down"));
        assert!(!log.failed(&"down"));
        assert!(!log.failed(&"down"));
        log.succeeded();
        // After a recovery the next failure is logged again at once.
        assert!(log.failed(&"down"));
    }

    #[test]
    fn interval_elapsed_logs_again_with_counts() {
        let log = FailureLog::with_interval("heartbeat", Duration::from_millis(0));
        assert!(log.failed(&"down"));
        assert!(log.failed(&"down"));
        assert_eq!(log.total.load(std::sync::atomic::Ordering::Relaxed), 2);
    }
}
