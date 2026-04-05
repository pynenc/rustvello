//! Observability infrastructure for the Rustvello distributed task system.
//!
//! Defines the [`EventEmitter`] trait, [`CompositeEmitter`], and
//! [`EventLevel`] for configurable event propagation to monitoring backends.

use std::time::Duration;

use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

// ---------------------------------------------------------------------------
// EventLevel — granularity of observable events
// ---------------------------------------------------------------------------

/// Granularity level for observable events.
///
/// Events at a lower level are implicitly included when a higher level is
/// enabled. For example, a sink configured at `TaskLifecycle` receives both
/// Level 0 (Worker Health) and Level 1 (Task Lifecycle) events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum EventLevel {
    /// Level 0: Worker start/stop/heartbeat events.
    WorkerHealth = 0,
    /// Level 1: Per-invocation lifecycle events (submitted, started, succeeded, failed).
    TaskLifecycle = 1,
    /// Level 2: Queue depth, enqueue/dequeue, concurrency control metrics.
    QueueConcurrency = 2,
    /// Level 3: Full distributed tracing with context propagation.
    DistributedTracing = 3,
}

// ---------------------------------------------------------------------------
// EventEmitter trait — the central abstraction
// ---------------------------------------------------------------------------

/// Observable events emitted during task lifecycle.
///
/// Each method corresponds to one event in the event taxonomy.
/// The default implementation is a no-op, allowing the compiler to
/// inline and eliminate calls when no monitoring feature is enabled.
///
/// # Levels
///
/// | Method | Level |
/// |---|---|
/// | `on_worker_started`, `on_worker_shutdown` | 0 (WorkerHealth) |
/// | `on_task_submitted` .. `on_task_retried` | 1 (TaskLifecycle) |
/// | `on_queue_depth`, `on_cc_rejected` | 2 (QueueConcurrency) |
pub trait EventEmitter: Send + Sync {
    // -- Level 0: Worker Health --

    /// A runner process has started.
    fn on_worker_started(&self, _runner_id: &RunnerId) {}

    /// A runner process is shutting down.
    fn on_worker_shutdown(&self, _runner_id: &RunnerId) {}

    // -- Level 1: Task Lifecycle --

    /// A task invocation was submitted to the system.
    fn on_task_submitted(&self, _task_id: &TaskId, _inv_id: &InvocationId) {}

    /// A runner began executing a task invocation.
    fn on_task_started(&self, _task_id: &TaskId, _inv_id: &InvocationId) {}

    /// A task invocation completed successfully.
    fn on_task_succeeded(&self, _task_id: &TaskId, _inv_id: &InvocationId, _duration: Duration) {}

    /// A task invocation failed (final failure, retries exhausted).
    fn on_task_failed(
        &self,
        _task_id: &TaskId,
        _inv_id: &InvocationId,
        _error: &str,
        _duration: Duration,
    ) {
    }

    /// A task invocation is being retried.
    fn on_task_retried(&self, _task_id: &TaskId, _inv_id: &InvocationId, _attempt: u32) {}

    // -- Level 2: Queue & Concurrency --

    /// Current queue depth snapshot.
    fn on_queue_depth(&self, _queue: &str, _depth: usize) {}

    /// Concurrency control rejected an invocation registration.
    fn on_cc_rejected(&self, _task_id: &TaskId) {}

    /// A concurrency slot was successfully acquired.
    fn on_cc_slot_acquired(&self, _task_id: &TaskId) {}

    /// A concurrency slot was released (invocation reached terminal state).
    fn on_cc_slot_released(&self, _task_id: &TaskId) {}
}

// ---------------------------------------------------------------------------
// NoopEmitter — zero-cost default
// ---------------------------------------------------------------------------

/// A no-op event emitter that discards all events.
///
/// Used as the default when no monitoring feature is enabled.
pub struct NoopEmitter;

impl EventEmitter for NoopEmitter {}

// ---------------------------------------------------------------------------
// CompositeEmitter — fan-out to multiple sinks
// ---------------------------------------------------------------------------

/// Fans out events to multiple sinks, each with an associated [`EventLevel`]
/// filter. A sink only receives events at or below its configured level.
pub struct CompositeEmitter {
    sinks: Vec<(EventLevel, Box<dyn EventEmitter>)>,
}

impl CompositeEmitter {
    /// Create a new composite emitter with no sinks.
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    /// Add a sink that receives events up to (and including) the given level.
    pub fn add_sink(&mut self, level: EventLevel, sink: impl EventEmitter + 'static) {
        self.sinks.push((level, Box::new(sink)));
    }

    /// Helper: iterate sinks that accept the given event level.
    fn for_level(&self, event_level: EventLevel, f: impl Fn(&dyn EventEmitter)) {
        for (max_level, sink) in &self.sinks {
            if *max_level >= event_level {
                f(sink.as_ref());
            }
        }
    }
}

impl Default for CompositeEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl EventEmitter for CompositeEmitter {
    fn on_worker_started(&self, runner_id: &RunnerId) {
        self.for_level(EventLevel::WorkerHealth, |s| s.on_worker_started(runner_id));
    }

    fn on_worker_shutdown(&self, runner_id: &RunnerId) {
        self.for_level(EventLevel::WorkerHealth, |s| {
            s.on_worker_shutdown(runner_id)
        });
    }

    fn on_task_submitted(&self, task_id: &TaskId, inv_id: &InvocationId) {
        self.for_level(EventLevel::TaskLifecycle, |s| {
            s.on_task_submitted(task_id, inv_id);
        });
    }

    fn on_task_started(&self, task_id: &TaskId, inv_id: &InvocationId) {
        self.for_level(EventLevel::TaskLifecycle, |s| {
            s.on_task_started(task_id, inv_id);
        });
    }

    fn on_task_succeeded(&self, task_id: &TaskId, inv_id: &InvocationId, duration: Duration) {
        self.for_level(EventLevel::TaskLifecycle, |s| {
            s.on_task_succeeded(task_id, inv_id, duration);
        });
    }

    fn on_task_failed(
        &self,
        task_id: &TaskId,
        inv_id: &InvocationId,
        error: &str,
        duration: Duration,
    ) {
        self.for_level(EventLevel::TaskLifecycle, |s| {
            s.on_task_failed(task_id, inv_id, error, duration);
        });
    }

    fn on_task_retried(&self, task_id: &TaskId, inv_id: &InvocationId, attempt: u32) {
        self.for_level(EventLevel::TaskLifecycle, |s| {
            s.on_task_retried(task_id, inv_id, attempt);
        });
    }

    fn on_queue_depth(&self, queue: &str, depth: usize) {
        self.for_level(EventLevel::QueueConcurrency, |s| {
            s.on_queue_depth(queue, depth);
        });
    }

    fn on_cc_rejected(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |s| {
            s.on_cc_rejected(task_id);
        });
    }

    fn on_cc_slot_acquired(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |s| {
            s.on_cc_slot_acquired(task_id);
        });
    }

    fn on_cc_slot_released(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |s| {
            s.on_cc_slot_released(task_id);
        });
    }
}

// ---------------------------------------------------------------------------
// WorkerState — lightweight in-memory registry
// ---------------------------------------------------------------------------

/// Tracks what a worker is currently doing (Phase 3C).
#[derive(Debug, Clone)]
pub struct WorkerState {
    pub runner_id: RunnerId,
    pub current_invocation: Option<InvocationId>,
    pub current_task: Option<TaskId>,
    pub started_at: Option<std::time::Instant>,
    pub last_result: Option<LastResult>,
    pub invocations_completed: u64,
}

/// The outcome of the most recently completed invocation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum LastResult {
    Success { task_id: TaskId, duration: Duration },
    Failed { task_id: TaskId, error: String },
}

impl WorkerState {
    /// Create a new idle worker state.
    pub fn new(runner_id: RunnerId) -> Self {
        Self {
            runner_id,
            current_invocation: None,
            current_task: None,
            started_at: None,
            last_result: None,
            invocations_completed: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    struct CountingSink {
        started: Arc<AtomicU32>,
        submitted: Arc<AtomicU32>,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                started: Arc::new(AtomicU32::new(0)),
                submitted: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl EventEmitter for CountingSink {
        fn on_worker_started(&self, _: &RunnerId) {
            self.started.fetch_add(1, Ordering::Relaxed);
        }
        fn on_task_submitted(&self, _: &TaskId, _: &InvocationId) {
            self.submitted.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn noop_emitter_compiles() {
        let emitter = NoopEmitter;
        let rid = RunnerId::new();
        emitter.on_worker_started(&rid);
        emitter.on_worker_shutdown(&rid);
    }

    #[test]
    fn composite_filters_by_level() {
        let mut composite = CompositeEmitter::new();

        // This sink only sees WorkerHealth events (level 0)
        let health_sink = CountingSink::new();
        let health_started = Arc::clone(&health_sink.started);
        let health_submitted = Arc::clone(&health_sink.submitted);
        composite.add_sink(EventLevel::WorkerHealth, health_sink);

        // This sink sees TaskLifecycle (level 0 + 1)
        let task_sink = CountingSink::new();
        let task_started = Arc::clone(&task_sink.started);
        let task_submitted = Arc::clone(&task_sink.submitted);
        composite.add_sink(EventLevel::TaskLifecycle, task_sink);

        let rid = RunnerId::new();
        let tid = TaskId::new("mod", "task");
        let iid = InvocationId::new();

        composite.on_worker_started(&rid);
        composite.on_task_submitted(&tid, &iid);

        // Both sinks should see worker_started (level 0)
        assert_eq!(health_started.load(Ordering::Relaxed), 1);
        assert_eq!(task_started.load(Ordering::Relaxed), 1);

        // Only the task sink should see task_submitted (level 1)
        assert_eq!(health_submitted.load(Ordering::Relaxed), 0);
        assert_eq!(task_submitted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn worker_state_new() {
        let rid = RunnerId::new();
        let ws = WorkerState::new(rid.clone());
        assert_eq!(ws.runner_id, rid);
        assert!(ws.current_invocation.is_none());
        assert_eq!(ws.invocations_completed, 0);
    }
}
