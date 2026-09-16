//! Context-bearing observability hooks for Rustvello lifecycle boundaries.
//!
//! Rustvello emits native domain events. Exporters map these events to OpenTelemetry
//! or another backend without making the task engine depend on an exporter SDK.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags};
use opentelemetry::Context;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};
use rustvello_proto::identifiers::{ExecutorKind, InvocationId, RunnerId, TaskId, TaskLanguage};
use rustvello_proto::invocation::WorkflowIdentity;
use serde::{Deserialize, Serialize};

use crate::context::RunnerContext;

/// Version of Rustvello's native lifecycle context.
pub const LIFECYCLE_CONTEXT_VERSION: u16 = 1;

/// Granularity level for observable events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum EventLevel {
    WorkerHealth = 0,
    TaskLifecycle = 1,
    QueueConcurrency = 2,
    DistributedTracing = 3,
}

pub use rustvello_proto::invocation::TraceContextCarrier;

struct CarrierMap(HashMap<String, String>);

impl Injector for CarrierMap {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

impl Extractor for CarrierMap {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Capture the active OpenTelemetry context as W3C `traceparent`/`tracestate`.
pub fn capture_w3c_trace_context() -> TraceContextCarrier {
    let mut carrier = CarrierMap(HashMap::with_capacity(2));
    TraceContextPropagator::new().inject_context(&Context::current(), &mut carrier);
    TraceContextCarrier {
        traceparent: carrier.0.remove("traceparent"),
        tracestate: carrier
            .0
            .remove("tracestate")
            .filter(|value| !value.is_empty()),
    }
}

/// Extract a persisted W3C carrier through the OpenTelemetry propagator.
pub fn extract_w3c_trace_context(carrier: &TraceContextCarrier) -> Context {
    let mut values = HashMap::with_capacity(2);
    if let Some(traceparent) = &carrier.traceparent {
        values.insert("traceparent".to_string(), traceparent.clone());
    }
    if let Some(tracestate) = &carrier.tracestate {
        values.insert("tracestate".to_string(), tracestate.clone());
    }
    TraceContextPropagator::new().extract_with_context(&Context::new(), &CarrierMap(values))
}

/// Allocate an execution identity without creating or exporting an SDK span.
pub fn allocate_execution_trace_context(parent: &TraceContextCarrier) -> TraceContextCarrier {
    let extracted = extract_w3c_trace_context(parent);
    let span = extracted.span();
    let parent = span.span_context();
    let generator = RandomIdGenerator::default();
    let context = SpanContext::new(
        if parent.is_valid() {
            parent.trace_id()
        } else {
            generator.new_trace_id()
        },
        generator.new_span_id(),
        if parent.is_valid() {
            parent.trace_flags()
        } else {
            TraceFlags::SAMPLED
        },
        false,
        parent.trace_state().clone(),
    );
    let mut carrier = CarrierMap(HashMap::with_capacity(2));
    TraceContextPropagator::new().inject_context(
        &Context::new().with_remote_span_context(context),
        &mut carrier,
    );
    TraceContextCarrier {
        traceparent: carrier.0.remove("traceparent"),
        tracestate: carrier
            .0
            .remove("tracestate")
            .filter(|value| !value.is_empty()),
    }
}

/// Return whether the carrier is empty or contains a valid remote span context.
pub fn is_valid_w3c_trace_context(carrier: &TraceContextCarrier) -> bool {
    carrier.is_empty()
        || extract_w3c_trace_context(carrier)
            .span()
            .span_context()
            .is_valid()
}

/// Stable worker/resource facts available at runner lifecycle boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerTelemetryContext {
    pub context_version: u16,
    pub app_id: Arc<str>,
    pub runner_id: RunnerId,
    pub parent_runner_id: Option<RunnerId>,
    pub runner_cls: Arc<str>,
    pub runner_language: TaskLanguage,
    pub executor_kind: ExecutorKind,
    pub hostname: String,
    pub process_id: u32,
    pub thread_id: u64,
}

impl From<&RunnerContext> for WorkerTelemetryContext {
    fn from(context: &RunnerContext) -> Self {
        Self {
            context_version: LIFECYCLE_CONTEXT_VERSION,
            app_id: Arc::clone(&context.app_id),
            runner_id: context.runner_id.clone(),
            parent_runner_id: context
                .parent_ctx
                .as_ref()
                .map(|parent| parent.runner_id.clone()),
            runner_cls: Arc::clone(&context.runner_cls),
            runner_language: context.runner_language,
            executor_kind: context.executor_kind,
            hostname: context.hostname.clone(),
            process_id: context.pid,
            thread_id: context.thread_id,
        }
    }
}

/// Correlation facts for one task attempt.
///
/// `attempt` is zero-based. The pair `(invocation_id, attempt)` is the native
/// attempt identity. Execution events carry a span identity allocated by the
/// runtime; integrations must preserve it when projecting the execution span.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAttemptContext {
    pub context_version: u16,
    pub app_id: Arc<str>,
    pub task_id: TaskId,
    pub invocation_id: InvocationId,
    pub attempt: u32,
    pub queue: Arc<str>,
    pub parent_invocation_id: Option<InvocationId>,
    pub workflow: Option<WorkflowIdentity>,
    pub worker: WorkerTelemetryContext,
    #[serde(default)]
    pub trace_context: TraceContextCarrier,
    /// Identity allocated and persisted before task code executes; empty on submission.
    #[serde(default)]
    pub execution_trace_context: TraceContextCarrier,
    /// Link to the last execution, never the parent of this attempt.
    #[serde(default)]
    pub previous_attempt_trace_context: TraceContextCarrier,
}

impl TaskAttemptContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app_id: impl Into<Arc<str>>,
        task_id: TaskId,
        invocation_id: InvocationId,
        attempt: u32,
        queue: impl Into<Arc<str>>,
        parent_invocation_id: Option<InvocationId>,
        workflow: Option<WorkflowIdentity>,
        worker: WorkerTelemetryContext,
        trace_context: TraceContextCarrier,
    ) -> Self {
        Self {
            context_version: LIFECYCLE_CONTEXT_VERSION,
            app_id: app_id.into(),
            task_id,
            invocation_id,
            attempt,
            queue: queue.into(),
            parent_invocation_id,
            workflow,
            worker,
            trace_context,
            execution_trace_context: TraceContextCarrier::default(),
            previous_attempt_trace_context: TraceContextCarrier::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkerLifecycleKind {
    Started,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLifecycleEvent {
    pub context: WorkerTelemetryContext,
    pub kind: WorkerLifecycleKind,
    pub event_time: DateTime<Utc>,
}

impl WorkerLifecycleEvent {
    pub fn started(context: WorkerTelemetryContext) -> Self {
        Self {
            context,
            kind: WorkerLifecycleKind::Started,
            event_time: Utc::now(),
        }
    }

    pub fn stopped(context: WorkerTelemetryContext) -> Self {
        Self {
            context,
            kind: WorkerLifecycleKind::Stopped,
            event_time: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaskLifecycleKind {
    Submitted,
    Started,
    Succeeded {
        duration: Duration,
    },
    Failed {
        error_type: String,
        duration: Duration,
    },
    RetryScheduled {
        next_attempt: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLifecycleEvent {
    pub context: TaskAttemptContext,
    pub kind: TaskLifecycleKind,
    pub event_time: DateTime<Utc>,
}

impl TaskLifecycleEvent {
    fn new(context: TaskAttemptContext, kind: TaskLifecycleKind) -> Self {
        Self {
            context,
            kind,
            event_time: Utc::now(),
        }
    }

    pub fn submitted(context: TaskAttemptContext) -> Self {
        Self::new(context, TaskLifecycleKind::Submitted)
    }

    pub fn started(context: TaskAttemptContext) -> Self {
        Self::new(context, TaskLifecycleKind::Started)
    }

    pub fn succeeded(context: TaskAttemptContext, duration: Duration) -> Self {
        Self::new(context, TaskLifecycleKind::Succeeded { duration })
    }

    pub fn failed(
        context: TaskAttemptContext,
        error_type: impl Into<String>,
        duration: Duration,
    ) -> Self {
        Self::new(
            context,
            TaskLifecycleKind::Failed {
                error_type: error_type.into(),
                duration,
            },
        )
    }

    pub fn retry_scheduled(context: TaskAttemptContext, next_attempt: u32) -> Self {
        Self::new(context, TaskLifecycleKind::RetryScheduled { next_attempt })
    }
}

/// Receives Rustvello lifecycle events.
///
/// Implementations called on task paths must do bounded, non-blocking work and
/// must not panic. An exporter should enqueue here and perform network I/O on its
/// own worker. The callbacks intentionally return no exporter result: telemetry
/// failure cannot change task success.
pub trait EventEmitter: Send + Sync {
    fn on_worker_lifecycle(&self, _event: &WorkerLifecycleEvent) {}

    fn on_task_lifecycle(&self, _event: &TaskLifecycleEvent) {}

    fn on_queue_depth(&self, _queue: &str, _depth: usize) {}

    fn on_cc_rejected(&self, _task_id: &TaskId) {}

    fn on_cc_slot_acquired(&self, _task_id: &TaskId) {}

    fn on_cc_slot_released(&self, _task_id: &TaskId) {}
}

pub struct NoopEmitter;

impl EventEmitter for NoopEmitter {}

/// Fans out events to sinks filtered by [`EventLevel`].
pub struct CompositeEmitter {
    sinks: Vec<(EventLevel, Arc<dyn EventEmitter>)>,
}

impl CompositeEmitter {
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    pub fn add_sink(&mut self, level: EventLevel, sink: impl EventEmitter + 'static) {
        self.add_shared_sink(level, Arc::new(sink));
    }

    pub fn add_shared_sink(&mut self, level: EventLevel, sink: Arc<dyn EventEmitter>) {
        self.sinks.push((level, sink));
    }

    fn for_level(&self, event_level: EventLevel, f: impl Fn(&dyn EventEmitter)) {
        for (max_level, sink) in &self.sinks {
            if *max_level >= event_level
                && std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(sink.as_ref())))
                    .is_err()
            {
                tracing::warn!(?event_level, "telemetry sink panicked; lifecycle continues");
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
    fn on_worker_lifecycle(&self, event: &WorkerLifecycleEvent) {
        self.for_level(EventLevel::WorkerHealth, |sink| {
            sink.on_worker_lifecycle(event)
        });
    }

    fn on_task_lifecycle(&self, event: &TaskLifecycleEvent) {
        self.for_level(EventLevel::TaskLifecycle, |sink| {
            sink.on_task_lifecycle(event)
        });
    }

    fn on_queue_depth(&self, queue: &str, depth: usize) {
        self.for_level(EventLevel::QueueConcurrency, |sink| {
            sink.on_queue_depth(queue, depth)
        });
    }

    fn on_cc_rejected(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |sink| {
            sink.on_cc_rejected(task_id)
        });
    }

    fn on_cc_slot_acquired(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |sink| {
            sink.on_cc_slot_acquired(task_id)
        });
    }

    fn on_cc_slot_released(&self, task_id: &TaskId) {
        self.for_level(EventLevel::QueueConcurrency, |sink| {
            sink.on_cc_slot_released(task_id)
        });
    }
}

/// Owned event passed from task paths to asynchronous exporters.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LifecycleEvent {
    Worker(WorkerLifecycleEvent),
    Task(Box<TaskLifecycleEvent>),
}

/// Transport-neutral batch exporter for native lifecycle events.
///
/// Transport adapters, including `rustvello-otel`, own signal-specific mappings.
/// This interface only owns bounded delivery and therefore does not define
/// attributes.
pub trait LifecycleExporter: Send + 'static {
    fn export(&mut self, events: &[LifecycleEvent]) -> Result<(), String>;

    fn shutdown(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// Capacity and batching limits for [`BoundedAsyncEmitter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncExportConfig {
    pub queue_capacity: usize,
    pub control_capacity: usize,
    pub max_batch_size: usize,
    pub scheduled_delay: Duration,
}

impl Default for AsyncExportConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 2_048,
            control_capacity: 16,
            max_batch_size: 512,
            scheduled_delay: Duration::from_secs(5),
        }
    }
}

/// Snapshot of bounded exporter delivery and callback overhead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AsyncExportStats {
    pub accepted: u64,
    pub dropped: u64,
    pub exported: u64,
    pub export_failed: u64,
    pub rejected_after_shutdown: u64,
    pub enqueue_nanos_total: u64,
    pub enqueue_nanos_max: u64,
}

#[derive(Default)]
struct ExportCounters {
    accepted: AtomicU64,
    dropped: AtomicU64,
    exported: AtomicU64,
    export_failed: AtomicU64,
    rejected_after_shutdown: AtomicU64,
    enqueue_nanos_total: AtomicU64,
    enqueue_nanos_max: AtomicU64,
}

struct FlushCommand {
    target: u64,
    response: mpsc::SyncSender<AsyncExportStats>,
}

struct ExportWorkerChannels {
    events: mpsc::Receiver<LifecycleEvent>,
    flushes: mpsc::Receiver<FlushCommand>,
    shutdowns: mpsc::Receiver<u64>,
}

struct ExportWorkerShared {
    counters: Arc<ExportCounters>,
    state: Arc<AtomicU8>,
    shutdown_error: Arc<Mutex<Option<String>>>,
}

const EXPORT_OPEN: u8 = 0;
const EXPORT_CLOSING: u8 = 1;
const EXPORT_CLOSED: u8 = 2;

struct AsyncExportInner {
    events: mpsc::SyncSender<LifecycleEvent>,
    flushes: mpsc::SyncSender<FlushCommand>,
    shutdowns: mpsc::SyncSender<u64>,
    counters: Arc<ExportCounters>,
    state: Arc<AtomicU8>,
    in_flight_enqueues: AtomicU64,
    shutdown_admitted: AtomicBool,
    shutdown_error: Arc<Mutex<Option<String>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

/// Non-blocking lifecycle emitter backed by a bounded queue and worker thread.
///
/// Full queues drop the new event and increment `dropped`; task execution never
/// waits for an exporter. `flush` and `shutdown` use a separate control channel,
/// report failed exports, and are bounded by the caller's timeout.
#[derive(Clone)]
pub struct BoundedAsyncEmitter {
    inner: Arc<AsyncExportInner>,
}

impl BoundedAsyncEmitter {
    pub fn new(config: AsyncExportConfig, exporter: impl LifecycleExporter) -> Self {
        assert!(config.queue_capacity > 0, "queue_capacity must be positive");
        assert!(
            config.control_capacity > 0,
            "control_capacity must be positive"
        );
        assert!(config.max_batch_size > 0, "max_batch_size must be positive");
        assert!(
            !config.scheduled_delay.is_zero(),
            "scheduled_delay must be positive"
        );
        let (event_tx, event_rx) = mpsc::sync_channel(config.queue_capacity);
        let (flush_tx, flush_rx) = mpsc::sync_channel(config.control_capacity);
        let (shutdown_tx, shutdown_rx) = mpsc::sync_channel(1);
        let counters = Arc::new(ExportCounters::default());
        let worker_counters = Arc::clone(&counters);
        let state = Arc::new(AtomicU8::new(EXPORT_OPEN));
        let worker_state = Arc::clone(&state);
        let shutdown_error = Arc::new(Mutex::new(None));
        let worker_shutdown_error = Arc::clone(&shutdown_error);
        let worker = std::thread::Builder::new()
            .name("rustvello-telemetry-export".to_string())
            .spawn(move || {
                run_export_worker(
                    ExportWorkerChannels {
                        events: event_rx,
                        flushes: flush_rx,
                        shutdowns: shutdown_rx,
                    },
                    exporter,
                    config,
                    ExportWorkerShared {
                        counters: worker_counters,
                        state: worker_state,
                        shutdown_error: worker_shutdown_error,
                    },
                );
            })
            .expect("failed to spawn telemetry exporter thread");
        Self {
            inner: Arc::new(AsyncExportInner {
                events: event_tx,
                flushes: flush_tx,
                shutdowns: shutdown_tx,
                counters,
                state,
                in_flight_enqueues: AtomicU64::new(0),
                shutdown_admitted: AtomicBool::new(false),
                shutdown_error,
                worker: Mutex::new(Some(worker)),
            }),
        }
    }

    pub fn stats(&self) -> AsyncExportStats {
        stats(&self.inner.counters)
    }

    pub fn flush(&self, timeout: Duration) -> Result<AsyncExportStats, &'static str> {
        let started = Instant::now();
        if self.inner.state.load(Ordering::Acquire) != EXPORT_OPEN {
            return Err("telemetry exporter is shutting down");
        }
        while self.inner.in_flight_enqueues.load(Ordering::Acquire) != 0 {
            if started.elapsed() >= timeout {
                return Err("telemetry flush timed out");
            }
            std::thread::yield_now();
        }
        let (response, receiver) = mpsc::sync_channel(1);
        let target = self.inner.counters.accepted.load(Ordering::Acquire);
        self.inner
            .flushes
            .try_send(FlushCommand { target, response })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => "telemetry control queue is full",
                mpsc::TrySendError::Disconnected(_) => "export worker stopped",
            })?;
        receiver
            .recv_timeout(timeout.saturating_sub(started.elapsed()))
            .map_err(|_| "telemetry flush timed out")
    }

    pub fn shutdown(&self, timeout: Duration) -> Result<AsyncExportStats, String> {
        let started = Instant::now();
        let _ = self.inner.state.compare_exchange(
            EXPORT_OPEN,
            EXPORT_CLOSING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        while self.inner.in_flight_enqueues.load(Ordering::Acquire) != 0 {
            if started.elapsed() >= timeout {
                return Err("telemetry shutdown timed out".to_string());
            }
            std::thread::yield_now();
        }
        if self
            .inner
            .shutdown_admitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let target = self.inner.counters.accepted.load(Ordering::Acquire);
            if self.inner.shutdowns.try_send(target).is_err() {
                *self.inner.shutdown_error.lock().expect("shutdown lock") =
                    Some("export worker stopped".to_string());
                self.inner.state.store(EXPORT_CLOSED, Ordering::Release);
            }
        }
        while self.inner.state.load(Ordering::Acquire) != EXPORT_CLOSED {
            if started.elapsed() >= timeout {
                return Err("telemetry shutdown timed out".to_string());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        // CLOSED describes exporter shutdown, not necessarily thread teardown:
        // user-defined exporter destructors can still block after that signal.
        loop {
            let mut worker = self.inner.worker.lock().expect("worker lock");
            match worker.as_ref() {
                None => break,
                Some(handle) if handle.is_finished() => {
                    worker
                        .take()
                        .expect("finished worker")
                        .join()
                        .map_err(|_| "telemetry worker panicked".to_string())?;
                    break;
                }
                Some(_) => {}
            }
            drop(worker);
            if started.elapsed() >= timeout {
                return Err("telemetry thread shutdown timed out".to_string());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if let Some(error) = self
            .inner
            .shutdown_error
            .lock()
            .expect("shutdown lock")
            .clone()
        {
            Err(error)
        } else {
            Ok(stats(&self.inner.counters))
        }
    }

    fn enqueue(&self, event: LifecycleEvent) {
        let started = Instant::now();
        self.inner.in_flight_enqueues.fetch_add(1, Ordering::AcqRel);
        if self.inner.state.load(Ordering::Acquire) != EXPORT_OPEN {
            self.inner
                .counters
                .rejected_after_shutdown
                .fetch_add(1, Ordering::Relaxed);
            self.inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            match self.inner.events.try_send(event) {
                Ok(()) => {
                    self.inner.counters.accepted.fetch_add(1, Ordering::Release);
                }
                Err(mpsc::TrySendError::Full(_)) | Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        self.inner
            .in_flight_enqueues
            .fetch_sub(1, Ordering::Release);
        let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.inner
            .counters
            .enqueue_nanos_total
            .fetch_add(elapsed, Ordering::Relaxed);
        self.inner
            .counters
            .enqueue_nanos_max
            .fetch_max(elapsed, Ordering::Relaxed);
    }
}

impl EventEmitter for BoundedAsyncEmitter {
    fn on_worker_lifecycle(&self, event: &WorkerLifecycleEvent) {
        self.enqueue(LifecycleEvent::Worker(event.clone()));
    }

    fn on_task_lifecycle(&self, event: &TaskLifecycleEvent) {
        self.enqueue(LifecycleEvent::Task(Box::new(event.clone())));
    }
}

fn stats(counters: &ExportCounters) -> AsyncExportStats {
    AsyncExportStats {
        accepted: counters.accepted.load(Ordering::Acquire),
        dropped: counters.dropped.load(Ordering::Relaxed),
        exported: counters.exported.load(Ordering::Acquire),
        export_failed: counters.export_failed.load(Ordering::Acquire),
        rejected_after_shutdown: counters.rejected_after_shutdown.load(Ordering::Relaxed),
        enqueue_nanos_total: counters.enqueue_nanos_total.load(Ordering::Relaxed),
        enqueue_nanos_max: counters.enqueue_nanos_max.load(Ordering::Relaxed),
    }
}

fn run_export_worker(
    channels: ExportWorkerChannels,
    mut exporter: impl LifecycleExporter,
    config: AsyncExportConfig,
    shared: ExportWorkerShared,
) {
    let mut processed = 0_u64;
    let mut pending_flush = None;
    let mut pending_shutdown = None;
    loop {
        if pending_shutdown.is_none() {
            pending_shutdown = channels.shutdowns.try_recv().ok();
        }
        if pending_flush.is_none() && pending_shutdown.is_none() {
            pending_flush = channels.flushes.try_recv().ok();
        }

        let mut batch = Vec::with_capacity(config.max_batch_size);
        let command_poll = config.scheduled_delay.min(Duration::from_millis(10));
        match channels.events.recv_timeout(command_poll) {
            Ok(event) => batch.push(event),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        while batch.len() < config.max_batch_size {
            match channels.events.try_recv() {
                Ok(event) => batch.push(event),
                Err(_) => break,
            }
        }
        if !batch.is_empty() {
            let count = batch.len() as u64;
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| exporter.export(&batch)));
            match result {
                Ok(Ok(())) => {
                    shared.counters.exported.fetch_add(count, Ordering::Release);
                }
                Ok(Err(_)) | Err(_) => {
                    shared
                        .counters
                        .export_failed
                        .fetch_add(count, Ordering::Release);
                }
            }
            processed += count;
        }

        let snapshot = stats(&shared.counters);
        if pending_flush
            .as_ref()
            .is_some_and(|command| processed >= command.target)
        {
            let command = pending_flush.take().expect("flush exists");
            let _ = command.response.send(snapshot);
        }
        if pending_shutdown.is_some_and(|target| processed >= target) {
            finish_exporter(&mut exporter, &shared);
            return;
        }
    }
    // Dropping the last emitter disconnects the channel after admitted events
    // drain. Give the adapter a chance to account for unfinished attempts too.
    finish_exporter(&mut exporter, &shared);
}

fn finish_exporter(exporter: &mut impl LifecycleExporter, shared: &ExportWorkerShared) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| exporter.shutdown()));
    let error = match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => Some("telemetry exporter panicked during shutdown".to_string()),
    };
    *shared.shutdown_error.lock().expect("shutdown lock") = error;
    shared.state.store(EXPORT_CLOSED, Ordering::Release);
}

/// Tracks what a worker is currently doing.
#[derive(Debug, Clone)]
pub struct WorkerState {
    pub runner_id: RunnerId,
    pub current_invocation: Option<InvocationId>,
    pub current_task: Option<TaskId>,
    pub started_at: Option<std::time::Instant>,
    pub last_result: Option<LastResult>,
    pub invocations_completed: u64,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum LastResult {
    Success { task_id: TaskId, duration: Duration },
    Failed { task_id: TaskId, error: String },
}

impl WorkerState {
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
    use std::sync::mpsc::{channel, Receiver, Sender};

    #[test]
    fn execution_identity_preserves_parent_trace_flags_and_state() {
        for flags in ["00", "01"] {
            let incoming = TraceContextCarrier {
                traceparent: Some(format!(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-{flags}"
                )),
                tracestate: Some("ih=execution".into()),
            };
            let execution = allocate_execution_trace_context(&incoming);
            assert_ne!(incoming, execution);
            let parent = extract_w3c_trace_context(&incoming);
            let child = extract_w3c_trace_context(&execution);
            assert_eq!(
                parent.span().span_context().trace_id(),
                child.span().span_context().trace_id()
            );
            assert_eq!(
                parent.span().span_context().trace_flags(),
                child.span().span_context().trace_flags()
            );
            assert_eq!(incoming.tracestate, execution.tracestate);
            assert!(child.span().span_context().is_valid());
        }
    }

    #[test]
    fn empty_carrier_never_inherits_the_export_or_worker_threads_context() {
        let ambient = allocate_execution_trace_context(&TraceContextCarrier::default());
        let _guard = extract_w3c_trace_context(&ambient).attach();
        assert!(!extract_w3c_trace_context(&TraceContextCarrier::default())
            .span()
            .span_context()
            .is_valid());
        let root = allocate_execution_trace_context(&TraceContextCarrier::default());
        assert_ne!(
            extract_w3c_trace_context(&root)
                .span()
                .span_context()
                .trace_id(),
            extract_w3c_trace_context(&ambient)
                .span()
                .span_context()
                .trace_id()
        );
    }

    #[test]
    fn legacy_lifecycle_context_defaults_execution_identity_fields() {
        let mut value = serde_json::to_value(attempt()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("execution_trace_context");
        value
            .as_object_mut()
            .unwrap()
            .remove("previous_attempt_trace_context");
        let decoded: TaskAttemptContext = serde_json::from_value(value).unwrap();
        assert!(decoded.execution_trace_context.is_empty());
        assert!(decoded.previous_attempt_trace_context.is_empty());
        assert_eq!(decoded.context_version, LIFECYCLE_CONTEXT_VERSION);
    }

    struct CountingSink {
        workers: Arc<AtomicU32>,
        tasks: Arc<AtomicU32>,
    }

    struct PanickingSink;

    struct RecordingExporter {
        events: Arc<Mutex<Vec<LifecycleEvent>>>,
        shutdowns: Arc<AtomicU32>,
    }

    impl LifecycleExporter for RecordingExporter {
        fn export(&mut self, events: &[LifecycleEvent]) -> Result<(), String> {
            self.events.lock().unwrap().extend_from_slice(events);
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), String> {
            self.shutdowns.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct BlockingExporter {
        started: Sender<()>,
        release: Receiver<()>,
        blocked: bool,
    }

    impl LifecycleExporter for BlockingExporter {
        fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
            if !self.blocked {
                self.blocked = true;
                let _ = self.started.send(());
                self.release.recv().map_err(|error| error.to_string())?;
            }
            Ok(())
        }
    }

    struct FailingShutdownExporter;

    impl LifecycleExporter for FailingShutdownExporter {
        fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), String> {
            Err("collector unavailable".to_string())
        }
    }

    struct FailingExportExporter;

    impl LifecycleExporter for FailingExportExporter {
        fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
            Err("collector unavailable".to_string())
        }
    }

    struct PanickingExportExporter;

    impl LifecycleExporter for PanickingExportExporter {
        fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
            panic!("collector panicked")
        }
    }

    struct PanickingShutdownExporter;

    impl LifecycleExporter for PanickingShutdownExporter {
        fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), String> {
            panic!("collector shutdown panicked")
        }
    }

    impl EventEmitter for PanickingSink {
        fn on_task_lifecycle(&self, _: &TaskLifecycleEvent) {
            panic!("export failed");
        }
    }

    impl EventEmitter for CountingSink {
        fn on_worker_lifecycle(&self, _: &WorkerLifecycleEvent) {
            self.workers.fetch_add(1, Ordering::Relaxed);
        }

        fn on_task_lifecycle(&self, _: &TaskLifecycleEvent) {
            self.tasks.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn worker() -> WorkerTelemetryContext {
        let context = RunnerContext::new(
            RunnerId::from_string("worker-1"),
            Arc::from("orders"),
            "PersistentTokioWorker",
        );
        WorkerTelemetryContext::from(&context)
    }

    fn attempt() -> TaskAttemptContext {
        TaskAttemptContext::new(
            Arc::from("orders"),
            TaskId::new("orders", "charge"),
            InvocationId::from_string("invocation-1"),
            0,
            "default",
            None,
            None,
            worker(),
            TraceContextCarrier::default(),
        )
    }

    #[test]
    fn task_attempt_context_has_stable_native_identity() {
        let context = attempt();
        assert_eq!(context.context_version, LIFECYCLE_CONTEXT_VERSION);
        assert_eq!(context.attempt, 0);
        assert_eq!(context.app_id.as_ref(), "orders");
        assert_eq!(context.worker.runner_id.as_str(), "worker-1");
    }

    #[test]
    fn composite_filters_context_bearing_events() {
        let health_workers = Arc::new(AtomicU32::new(0));
        let health_tasks = Arc::new(AtomicU32::new(0));
        let task_workers = Arc::new(AtomicU32::new(0));
        let task_tasks = Arc::new(AtomicU32::new(0));

        let mut composite = CompositeEmitter::new();
        composite.add_sink(
            EventLevel::WorkerHealth,
            CountingSink {
                workers: Arc::clone(&health_workers),
                tasks: Arc::clone(&health_tasks),
            },
        );
        composite.add_sink(
            EventLevel::TaskLifecycle,
            CountingSink {
                workers: Arc::clone(&task_workers),
                tasks: Arc::clone(&task_tasks),
            },
        );

        composite.on_worker_lifecycle(&WorkerLifecycleEvent::started(worker()));
        composite.on_task_lifecycle(&TaskLifecycleEvent::submitted(attempt()));

        assert_eq!(health_workers.load(Ordering::Relaxed), 1);
        assert_eq!(task_workers.load(Ordering::Relaxed), 1);
        assert_eq!(health_tasks.load(Ordering::Relaxed), 0);
        assert_eq!(task_tasks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn one_exporter_failure_does_not_stop_other_sinks() {
        let workers = Arc::new(AtomicU32::new(0));
        let tasks = Arc::new(AtomicU32::new(0));
        let mut composite = CompositeEmitter::new();
        composite.add_sink(EventLevel::TaskLifecycle, PanickingSink);
        composite.add_sink(
            EventLevel::TaskLifecycle,
            CountingSink {
                workers,
                tasks: Arc::clone(&tasks),
            },
        );

        composite.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));

        assert_eq!(tasks.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn w3c_carrier_round_trips_through_otel_propagation() {
        let carrier = TraceContextCarrier {
            traceparent: Some(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            tracestate: Some("vendor=value".to_string()),
        };
        assert!(is_valid_w3c_trace_context(&carrier));
        let _guard = extract_w3c_trace_context(&carrier).attach();

        assert_eq!(capture_w3c_trace_context(), carrier);
    }

    #[test]
    fn invalid_w3c_carrier_is_rejected() {
        assert!(!is_valid_w3c_trace_context(&TraceContextCarrier {
            traceparent: Some("not-a-traceparent".to_string()),
            tracestate: None,
        }));
    }

    #[test]
    fn async_export_flushes_accepted_events_and_shuts_down() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let shutdowns = Arc::new(AtomicU32::new(0));
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                queue_capacity: 8,
                control_capacity: 4,
                max_batch_size: 4,
                scheduled_delay: Duration::from_millis(2),
            },
            RecordingExporter {
                events: Arc::clone(&events),
                shutdowns: Arc::clone(&shutdowns),
            },
        );

        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        emitter.on_worker_lifecycle(&WorkerLifecycleEvent::started(worker()));
        let flushed = emitter.flush(Duration::from_secs(1)).unwrap();
        assert_eq!(flushed.accepted, 2);
        assert_eq!(flushed.exported, 2);
        assert_eq!(events.lock().unwrap().len(), 2);

        let stopped = emitter.shutdown(Duration::from_secs(1)).unwrap();
        assert_eq!(stopped.exported, 2);
        assert_eq!(shutdowns.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn async_export_drops_when_full_without_blocking_task_path() {
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel();
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                queue_capacity: 2,
                control_capacity: 2,
                max_batch_size: 1,
                scheduled_delay: Duration::from_millis(1),
            },
            BlockingExporter {
                started: started_tx,
                release: release_rx,
                blocked: false,
            },
        );
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let started = Instant::now();
        for _ in 0..32 {
            emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        }
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(emitter.stats().dropped > 0);
        assert_eq!(
            emitter.flush(Duration::from_millis(10)),
            Err("telemetry flush timed out")
        );

        release_tx.send(()).unwrap();
        let _ = emitter.shutdown(Duration::from_secs(1));
    }

    #[test]
    fn async_shutdown_reports_exporter_failure() {
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                scheduled_delay: Duration::from_millis(1),
                ..AsyncExportConfig::default()
            },
            FailingShutdownExporter,
        );
        let error = emitter.shutdown(Duration::from_secs(1)).unwrap_err();
        assert_eq!(error, "collector unavailable");
    }

    #[test]
    fn async_export_accounts_for_collector_outage() {
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                scheduled_delay: Duration::from_millis(1),
                ..AsyncExportConfig::default()
            },
            FailingExportExporter,
        );
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));

        let stats = emitter.flush(Duration::from_secs(1)).unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.exported, 0);
        assert_eq!(stats.export_failed, 1);
        emitter.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn async_flush_control_admission_is_bounded() {
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel();
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                queue_capacity: 1,
                control_capacity: 2,
                max_batch_size: 1,
                scheduled_delay: Duration::from_millis(1),
            },
            BlockingExporter {
                started: started_tx,
                release: release_rx,
                blocked: false,
            },
        );
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert_eq!(
            emitter.flush(Duration::from_millis(5)),
            Err("telemetry flush timed out")
        );
        assert_eq!(
            emitter.flush(Duration::from_millis(5)),
            Err("telemetry flush timed out")
        );
        assert_eq!(
            emitter.flush(Duration::from_millis(5)),
            Err("telemetry control queue is full")
        );

        release_tx.send(()).unwrap();
        emitter.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn async_export_panic_is_accounted_without_panicking_task_path() {
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                scheduled_delay: Duration::from_millis(1),
                ..AsyncExportConfig::default()
            },
            PanickingExportExporter,
        );
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));

        let stats = emitter.flush(Duration::from_secs(1)).unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.exported, 0);
        assert_eq!(stats.export_failed, 1);
        emitter.shutdown(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn async_shutdown_panic_is_reported_without_escaping_worker() {
        let emitter =
            BoundedAsyncEmitter::new(AsyncExportConfig::default(), PanickingShutdownExporter);
        assert_eq!(
            emitter.shutdown(Duration::from_secs(1)),
            Err("telemetry exporter panicked during shutdown".to_string())
        );
    }

    #[test]
    fn async_shutdown_is_bounded_and_rejects_new_events() {
        let (started_tx, started_rx) = channel();
        let (release_tx, release_rx) = channel();
        let emitter = BoundedAsyncEmitter::new(
            AsyncExportConfig {
                queue_capacity: 2,
                control_capacity: 2,
                max_batch_size: 1,
                scheduled_delay: Duration::from_millis(1),
            },
            BlockingExporter {
                started: started_tx,
                release: release_rx,
                blocked: false,
            },
        );
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let started = Instant::now();
        assert_eq!(
            emitter.shutdown(Duration::from_millis(10)),
            Err("telemetry shutdown timed out".to_string())
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        emitter.on_task_lifecycle(&TaskLifecycleEvent::started(attempt()));
        let closing_stats = emitter.stats();
        assert_eq!(closing_stats.accepted, 1);
        assert_eq!(closing_stats.rejected_after_shutdown, 1);
        assert_eq!(closing_stats.dropped, 1);

        release_tx.send(()).unwrap();
        let stopped = emitter.shutdown(Duration::from_secs(1)).unwrap();
        assert_eq!(stopped.exported, 1);
        assert_eq!(stopped.rejected_after_shutdown, 1);
    }

    #[test]
    fn worker_state_new_is_idle() {
        let runner_id = RunnerId::new();
        let state = WorkerState::new(runner_id.clone());
        assert_eq!(state.runner_id, runner_id);
        assert!(state.current_invocation.is_none());
        assert_eq!(state.invocations_completed, 0);
    }
}
