//! OTLP/HTTP-Protobuf export for Rustvello's native lifecycle events.
//!
//! Network work happens only behind [`rustvello_core::observability::BoundedAsyncEmitter`].

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::trace::{Link, SpanContext, SpanKind, Status, TraceContextExt};
use opentelemetry::{InstrumentationScope, Key, KeyValue};
use opentelemetry_sdk::logs::{SdkLogRecord, SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::trace::SpanData;
use opentelemetry_sdk::Resource;
use rustvello_core::observability::{
    extract_w3c_trace_context, LifecycleEvent, LifecycleExporter, TaskAttemptContext,
    TaskLifecycleEvent, TaskLifecycleKind, WorkerLifecycleEvent, WorkerLifecycleKind,
    WorkerTelemetryContext, LIFECYCLE_CONTEXT_VERSION,
};

pub const MAPPING_REVISION: &str = "rustvello-otel.v1";
pub const INSTRUMENTATION_SCOPE: &str = "rustvello.lifecycle";
pub const INSTRUMENTATION_SCOPE_VERSION: &str = "1";
/// Maximum protobuf request body accepted by the qualified IH receiver.
pub const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

mod transport;
pub use transport::{OtlpExportAccounting, OtlpExportStats, SignalExportStats};

/// Bounds and transport configuration for one lifecycle exporter.
#[derive(Clone)]
pub struct OtlpLifecycleConfig {
    pub endpoint: String,
    pub bearer_token: String,
    /// Total budget for one export call, including mapping and all HTTP signals.
    pub export_timeout: Duration,
    /// Encoded protobuf body limit; may be lowered from the receiver's 4 MiB cap.
    pub max_request_bytes: usize,
    pub max_worker_resources: usize,
    /// Bound on open attempts and, separately, recent completed-attempt history.
    /// Duplicate suppression is limited to this recent history window.
    pub max_open_attempts: usize,
}

impl fmt::Debug for OtlpLifecycleConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpLifecycleConfig")
            .field("endpoint", &self.endpoint)
            .field("bearer_token", &"[REDACTED]")
            .field("export_timeout", &self.export_timeout)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_worker_resources", &self.max_worker_resources)
            .field("max_open_attempts", &self.max_open_attempts)
            .finish()
    }
}

impl OtlpLifecycleConfig {
    pub fn new(endpoint: impl Into<String>, bearer_token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            bearer_token: bearer_token.into(),
            export_timeout: Duration::from_secs(5),
            max_request_bytes: MAX_REQUEST_BYTES,
            max_worker_resources: 256,
            max_open_attempts: 4_096,
        }
    }

    fn validate(&self) -> Result<(), String> {
        let url =
            reqwest::Url::parse(&self.endpoint).map_err(|_| "invalid OTLP endpoint".to_owned())?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err("OTLP endpoint must use http or https".to_owned());
        }
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "OTLP endpoint must not contain credentials, query, or fragment".to_owned(),
            );
        }
        if self.bearer_token.is_empty() {
            return Err("OTLP bearer token must not be empty".to_owned());
        }
        if self.export_timeout.is_zero()
            || self.max_request_bytes == 0
            || self.max_worker_resources == 0
            || self.max_open_attempts == 0
        {
            return Err("OTLP exporter bounds must be positive".to_owned());
        }
        if self.max_request_bytes > MAX_REQUEST_BYTES {
            return Err("OTLP request bound exceeds receiver's 4 MiB cap".to_owned());
        }
        Ok(())
    }
}

/// Immutable resource facts; no SDK queues, timers, or drop-triggered exports.
struct WorkerPipeline {
    resource: opentelemetry_proto::transform::common::tonic::ResourceAttributesWithSchema,
    metric_time: SystemTime,
}

/// An observed start, with no auto-ending SDK span.
struct OpenAttempt {
    context: TaskAttemptContext,
    start_time: SystemTime,
}

/// Real OpenTelemetry adapter for native Rustvello lifecycle events.
///
/// Use behind `BoundedAsyncEmitter`: export performs bounded HTTP requests on
/// that emitter's worker. Each signal is acknowledged before export returns.
/// Lifecycle batch accounting is conservative on mixed success; `accounting()`
/// provides exact signal counts. Failed or partial requests are never retried.
pub struct OtlpLifecycleExporter {
    config: OtlpLifecycleConfig,
    workers: HashMap<WorkerKey, WorkerPipeline>,
    open_attempts: HashMap<AttemptKey, OpenAttempt>,
    closed_attempts: HashSet<AttemptKey>,
    closed_order: VecDeque<AttemptKey>,
    logger: SdkLogger,
    transport: transport::Transport,
    pending: transport::Pending,
    closed: bool,
}

type WorkerKey = (String, String);
type AttemptKey = (String, String, u32);

impl fmt::Debug for OtlpLifecycleExporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtlpLifecycleExporter")
            .field("config", &self.config)
            .field("worker_resources", &self.workers.len())
            .field("open_attempts", &self.open_attempts.len())
            .field("closed", &self.closed)
            .finish()
    }
}

impl OtlpLifecycleExporter {
    pub fn new(config: OtlpLifecycleConfig) -> Result<Self, String> {
        config.validate()?;
        let logger = SdkLoggerProvider::builder()
            .build()
            .logger_with_scope(instrumentation_scope());
        Ok(Self {
            transport: transport::Transport::new(),
            pending: transport::Pending::default(),
            config,
            workers: HashMap::new(),
            open_attempts: HashMap::new(),
            closed_attempts: HashSet::new(),
            closed_order: VecDeque::new(),
            logger,
            closed: false,
        })
    }

    /// Clone this handle before moving the exporter into the async emitter.
    pub fn accounting(&self) -> OtlpExportAccounting {
        self.transport.accounting.clone()
    }

    /// Snapshot transport acknowledgements separately from lifecycle queue stats.
    pub fn export_stats(&self) -> OtlpExportStats {
        self.transport.accounting.stats()
    }

    fn pipeline(&mut self, worker: &WorkerTelemetryContext) -> Result<(), String> {
        validate_worker(worker)?;
        let key = worker_key(worker);
        if !self.workers.contains_key(&key) {
            if self.workers.len() >= self.config.max_worker_resources {
                return Err("OTLP worker resource bound reached".to_owned());
            }
            self.workers.insert(
                key,
                WorkerPipeline {
                    resource: (&worker_resource(worker)).into(),
                    metric_time: SystemTime::now(),
                },
            );
        }
        Ok(())
    }

    fn log(&mut self, key: &WorkerKey, record: SdkLogRecord) {
        let resource = &self.workers.get(key).expect("validated resource").resource;
        self.pending
            .logs
            .push(((&record, &instrumentation_scope()), resource).into());
    }

    fn remember_closed(&mut self, key: AttemptKey) {
        if self.closed_attempts.insert(key.clone()) {
            self.closed_order.push_back(key);
        }
        while self.closed_order.len() > self.config.max_open_attempts {
            if let Some(key) = self.closed_order.pop_front() {
                self.closed_attempts.remove(&key);
            }
        }
    }

    fn abandon(&mut self, worker: Option<&WorkerKey>) -> usize {
        let keys: Vec<_> = self
            .open_attempts
            .iter()
            .filter(|(_, open)| worker.is_none_or(|key| worker_key(&open.context.worker) == *key))
            .map(|(key, _)| key.clone())
            .collect();
        let count = keys.len();
        for key in keys {
            self.open_attempts.remove(&key);
            self.remember_closed(key);
        }
        self.transport
            .accounting
            .update(|stats| stats.incomplete_attempts += count as u64);
        count
    }

    fn process_worker(&mut self, event: &WorkerLifecycleEvent) -> Result<(), String> {
        let record = emit_worker_log(&self.logger, event)?;
        self.pipeline(&event.context)?;
        let key = worker_key(&event.context);
        self.log(&key, record);
        if matches!(event.kind, WorkerLifecycleKind::Stopped) {
            let abandoned = self.abandon(Some(&key));
            self.workers.remove(&key);
            if abandoned > 0 {
                return Err(format!(
                    "worker stopped with {abandoned} incomplete attempts"
                ));
            }
        }
        Ok(())
    }

    fn process_task(&mut self, event: &TaskLifecycleEvent) -> Result<(), String> {
        validate_attempt(&event.context)?;
        let time = system_time(event.event_time)?;
        let key = worker_key(&event.context.worker);
        let attempt_key = attempt_key(&event.context);
        let terminal = terminal_outcome(&event.kind).is_some();
        let started = matches!(event.kind, TaskLifecycleKind::Started);
        if (started && self.open_attempts.contains_key(&attempt_key))
            || ((started || terminal) && self.closed_attempts.contains(&attempt_key))
        {
            self.transport
                .accounting
                .update(|stats| stats.duplicate_events += 1);
            return Err("duplicate attempt lifecycle event".to_owned());
        }
        let execution = extract_w3c_trace_context(&event.context.execution_trace_context)
            .span()
            .span_context()
            .clone();
        let correlation = if matches!(event.kind, TaskLifecycleKind::Submitted) {
            None
        } else {
            execution.is_valid().then_some(&execution)
        };
        let record = emit_task_log(&self.logger, event, correlation)?;
        self.pipeline(&event.context.worker)?;
        // Logs remain useful when span identity, a start, or capacity is missing.
        self.log(&key, record);
        if started {
            if !execution.is_valid() {
                return Err("missing runtime execution span identity".to_owned());
            }
            if self.open_attempts.len() >= self.config.max_open_attempts {
                self.transport
                    .accounting
                    .update(|stats| stats.incomplete_attempts += 1);
                self.remember_closed(attempt_key);
                return Err("OTLP open attempt bound reached".to_owned());
            }
            self.open_attempts.insert(
                attempt_key,
                OpenAttempt {
                    context: event.context.clone(),
                    start_time: time,
                },
            );
        } else if terminal {
            let Some(open) = self.open_attempts.get(&attempt_key) else {
                self.remember_closed(attempt_key);
                self.transport
                    .accounting
                    .update(|stats| stats.incomplete_attempts += 1);
                return Err("terminal event has no observed start".to_owned());
            };
            if open.context != event.context || time < open.start_time {
                return Err("terminal event disagrees with observed start".to_owned());
            }
            let open = self
                .open_attempts
                .remove(&attempt_key)
                .expect("checked start");
            self.remember_closed(attempt_key);
            let span = completed_span(open, event)?;
            if span.span_context.is_sampled() {
                let resource = &self.workers.get(&key).expect("validated resource").resource;
                self.pending.traces.extend(
                    opentelemetry_proto::transform::trace::tonic::group_spans_by_resource_and_scope(
                        vec![span],
                        resource,
                    ),
                );
            } else {
                self.transport
                    .accounting
                    .update(|stats| stats.unsampled_attempts += 1);
            }
            let pipeline = self.workers.get_mut(&key).expect("validated resource");
            let start = pipeline.metric_time;
            pipeline.metric_time = SystemTime::now().max(start + Duration::from_nanos(1));
            let request = transport::completion_metric(
                &pipeline.resource,
                event,
                start,
                pipeline.metric_time,
            )?;
            self.pending.metrics.extend(request.resource_metrics);
        }
        Ok(())
    }
}

impl LifecycleExporter for OtlpLifecycleExporter {
    fn export(&mut self, events: &[LifecycleEvent]) -> Result<(), String> {
        if self.closed {
            return Err("OTLP exporter is shut down".to_owned());
        }
        let deadline = Instant::now()
            .checked_add(self.config.export_timeout)
            .ok_or_else(|| "OTLP export timeout overflow".to_owned())?;
        let mut first_error = None;
        for (index, event) in events.iter().enumerate() {
            if Instant::now() >= deadline {
                let remaining = (events.len() - index) as u64;
                self.transport.accounting.update(|stats| {
                    stats.failed_events += remaining;
                    stats.unprocessed_events += remaining;
                });
                first_error.get_or_insert_with(|| {
                    "OTLP export deadline exhausted during mapping".to_owned()
                });
                break;
            }
            let result = match event {
                LifecycleEvent::Worker(event) => self.process_worker(event),
                LifecycleEvent::Task(event) => self.process_task(event),
                _ => Err("unsupported native lifecycle event".to_owned()),
            };
            if let Err(error) = result {
                self.transport
                    .accounting
                    .update(|stats| stats.failed_events += 1);
                first_error.get_or_insert(error);
            }
        }
        if let Err(error) =
            self.transport
                .flush(&self.config, std::mem::take(&mut self.pending), deadline)
        {
            first_error.get_or_insert(error);
        }
        first_error.map_or(Ok(()), Err)
    }

    fn shutdown(&mut self) -> Result<(), String> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let abandoned = self.abandon(None);
        self.workers.clear();
        self.closed_attempts.clear();
        self.closed_order.clear();
        if abandoned > 0 {
            Err(format!(
                "shutdown discarded {abandoned} incomplete attempts"
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for OtlpLifecycleExporter {
    fn drop(&mut self) {
        self.abandon(None);
    }
}

fn instrumentation_scope() -> InstrumentationScope {
    InstrumentationScope::builder(INSTRUMENTATION_SCOPE)
        .with_version(INSTRUMENTATION_SCOPE_VERSION)
        .build()
}

fn worker_resource(worker: &WorkerTelemetryContext) -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.name", "rustvello"),
        KeyValue::new("service.namespace", worker.app_id.to_string()),
        KeyValue::new("service.instance.id", worker.runner_id.to_string()),
        KeyValue::new("rustvello.app.id", worker.app_id.to_string()),
        KeyValue::new("rustvello.worker.id", worker.runner_id.to_string()),
        KeyValue::new("host.name", worker.hostname.clone()),
        KeyValue::new("process.pid", i64::from(worker.process_id)),
        KeyValue::new(
            "rustvello.worker.language",
            worker.runner_language.to_string(),
        ),
        KeyValue::new(
            "rustvello.worker.executor",
            worker.executor_kind.to_string(),
        ),
        KeyValue::new("rustvello.worker.class", worker.runner_cls.to_string()),
    ];
    if let Some(parent) = &worker.parent_runner_id {
        attributes.push(KeyValue::new(
            "rustvello.worker.parent.id",
            parent.to_string(),
        ));
    }
    Resource::builder_empty()
        .with_attributes(attributes)
        .build()
}

fn task_span_attributes(context: &TaskAttemptContext) -> Vec<KeyValue> {
    task_attribute_values(context, "executor")
        .into_iter()
        .map(|(key, value)| match value {
            AnyValue::Int(value) => KeyValue::new(key, value),
            AnyValue::String(value) => KeyValue::new(key, value.to_string()),
            _ => unreachable!("task attributes contain only integer and string values"),
        })
        .collect()
}

fn task_attribute_values(context: &TaskAttemptContext, role: &'static str) -> Vec<(Key, AnyValue)> {
    let mut attributes = vec![
        (
            Key::new("telemetry.mapping.revision"),
            MAPPING_REVISION.into(),
        ),
        (
            Key::new("rustvello.task.id"),
            context.task_id.to_string().into(),
        ),
        (
            Key::new("rustvello.invocation.id"),
            context.invocation_id.to_string().into(),
        ),
        (
            Key::new("rustvello.attempt"),
            i64::from(context.attempt).into(),
        ),
        (
            Key::new("rustvello.queue"),
            context.queue.to_string().into(),
        ),
        (
            Key::new("thread.id"),
            i64::try_from(context.worker.thread_id)
                .unwrap_or(i64::MAX)
                .into(),
        ),
        (Key::new("rustvello.worker.role"), role.into()),
    ];
    if let Some(parent) = &context.parent_invocation_id {
        attributes.push((
            Key::new("rustvello.parent.invocation.id"),
            parent.to_string().into(),
        ));
    }
    if let Some(workflow) = &context.workflow {
        attributes.extend([
            (
                Key::new("rustvello.workflow.id"),
                workflow.workflow_id.to_string().into(),
            ),
            (
                Key::new("rustvello.workflow.type"),
                workflow.workflow_type.to_string().into(),
            ),
            (
                Key::new("rustvello.workflow.depth"),
                i64::from(workflow.depth).into(),
            ),
        ]);
        if let Some(parent) = &workflow.parent_id {
            attributes.push((
                Key::new("rustvello.workflow.parent.id"),
                parent.to_string().into(),
            ));
        }
    }
    attributes
}

fn emit_worker_log(
    logger: &SdkLogger,
    event: &WorkerLifecycleEvent,
) -> Result<SdkLogRecord, String> {
    validate_worker(&event.context)?;
    let name = match event.kind {
        WorkerLifecycleKind::Started => "worker.started",
        WorkerLifecycleKind::Stopped => "worker.stopped",
        _ => return Err("unsupported worker lifecycle event".to_owned()),
    };
    let mut record = logger.create_log_record();
    record.set_event_name(name);
    record.set_body(name.into());
    record.set_timestamp(system_time(event.event_time)?);
    record.set_observed_timestamp(SystemTime::now());
    record.set_severity_number(Severity::Info);
    record.set_severity_text("INFO");
    record.add_attributes([
        (
            Key::new("telemetry.mapping.revision"),
            AnyValue::from(MAPPING_REVISION),
        ),
        (Key::new("rustvello.event"), AnyValue::from(name)),
    ]);
    Ok(record)
}

fn emit_task_log(
    logger: &SdkLogger,
    event: &TaskLifecycleEvent,
    active_attempt: Option<&SpanContext>,
) -> Result<SdkLogRecord, String> {
    let (name, severity, role) = match event.kind {
        TaskLifecycleKind::Submitted => ("task.submitted", Severity::Info, "submitter"),
        TaskLifecycleKind::Started => ("task.started", Severity::Info, "executor"),
        TaskLifecycleKind::Succeeded { .. } => ("task.succeeded", Severity::Info, "executor"),
        TaskLifecycleKind::Failed { .. } => ("task.failed", Severity::Error, "executor"),
        TaskLifecycleKind::RetryScheduled { .. } => {
            ("task.retry_scheduled", Severity::Info, "executor")
        }
        _ => return Err("unsupported task lifecycle event".to_owned()),
    };
    let mut attributes = task_attribute_values(&event.context, role);
    attributes.push((Key::new("rustvello.event"), name.into()));
    match &event.kind {
        TaskLifecycleKind::Succeeded { duration } => attributes.push((
            Key::new("rustvello.duration.s"),
            duration.as_secs_f64().into(),
        )),
        TaskLifecycleKind::Failed {
            error_type,
            duration,
        } => {
            attributes.push((
                Key::new("rustvello.duration.s"),
                duration.as_secs_f64().into(),
            ));
            attributes.push((
                Key::new("rustvello.error.type"),
                sanitized_error_type(error_type).into(),
            ));
        }
        TaskLifecycleKind::RetryScheduled { next_attempt } => {
            if *next_attempt <= event.context.attempt {
                return Err("retry next_attempt must be greater than attempt".to_owned());
            }
            attributes.push((
                Key::new("rustvello.next_attempt"),
                i64::from(*next_attempt).into(),
            ));
        }
        _ => {}
    }
    let mut record = logger.create_log_record();
    record.set_event_name(name);
    record.set_body(name.into());
    record.set_timestamp(system_time(event.event_time)?);
    record.set_observed_timestamp(SystemTime::now());
    record.set_severity_number(severity);
    record.set_severity_text(severity.name());
    record.add_attributes(attributes);
    let correlation = active_attempt
        .filter(|context| context.is_valid())
        .cloned()
        .or_else(|| {
            let context = extract_w3c_trace_context(&event.context.trace_context);
            let span_context = context.span().span_context().clone();
            span_context.is_valid().then_some(span_context)
        });
    if let Some(context) = correlation {
        record.set_trace_context(
            context.trace_id(),
            context.span_id(),
            Some(context.trace_flags()),
        );
    }
    Ok(record)
}

fn completed_span(open: OpenAttempt, event: &TaskLifecycleEvent) -> Result<SpanData, String> {
    let execution = extract_w3c_trace_context(&open.context.execution_trace_context)
        .span()
        .span_context()
        .clone();
    let parent = extract_w3c_trace_context(&open.context.trace_context)
        .span()
        .span_context()
        .clone();
    let previous = extract_w3c_trace_context(&open.context.previous_attempt_trace_context)
        .span()
        .span_context()
        .clone();
    let mut links = opentelemetry_sdk::trace::SpanLinks::default();
    if previous.is_valid() {
        links.links.push(Link::new(previous, Vec::new(), 0));
    }
    let mut attributes = task_span_attributes(&open.context);
    let status = match &event.kind {
        TaskLifecycleKind::Succeeded { duration } => {
            attributes.extend([
                KeyValue::new("rustvello.duration.s", duration.as_secs_f64()),
                KeyValue::new("outcome", "success"),
            ]);
            Status::Ok
        }
        TaskLifecycleKind::Failed {
            error_type,
            duration,
        } => {
            let category = sanitized_error_type(error_type);
            attributes.extend([
                KeyValue::new("rustvello.duration.s", duration.as_secs_f64()),
                KeyValue::new("rustvello.error.type", category),
                KeyValue::new("outcome", "failed"),
            ]);
            Status::error(category)
        }
        _ => return Err("only terminal lifecycle events can finish a span".to_owned()),
    };
    Ok(SpanData {
        span_context: SpanContext::new(
            execution.trace_id(),
            execution.span_id(),
            execution.trace_flags(),
            false,
            execution.trace_state().clone(),
        ),
        parent_span_id: parent.span_id(),
        parent_span_is_remote: parent.is_remote(),
        span_kind: SpanKind::Consumer,
        name: "rustvello.task.execute".into(),
        start_time: open.start_time,
        end_time: system_time(event.event_time)?,
        attributes,
        dropped_attributes_count: 0,
        events: Default::default(),
        links,
        status,
        instrumentation_scope: instrumentation_scope(),
    })
}

fn terminal_outcome(kind: &TaskLifecycleKind) -> Option<&'static str> {
    match kind {
        TaskLifecycleKind::Succeeded { .. } => Some("success"),
        TaskLifecycleKind::Failed { .. } => Some("failed"),
        _ => None,
    }
}

fn sanitized_error_type(value: &str) -> &'static str {
    match value {
        "AssertionError" => "AssertionError",
        "BackendError" => "BackendError",
        "IOError" => "IOError",
        "KeyError" => "KeyError",
        "OSError" => "OSError",
        "RuntimeError" => "RuntimeError",
        "SerializationError" => "SerializationError",
        "TaskExecutionError" => "TaskExecutionError",
        "TimeoutError" => "TimeoutError",
        "TypeError" => "TypeError",
        "ValueError" => "ValueError",
        _ => "TaskExecutionError",
    }
}

fn system_time(value: chrono::DateTime<chrono::Utc>) -> Result<SystemTime, String> {
    let seconds = u64::try_from(value.timestamp())
        .map_err(|_| "lifecycle event predates Unix epoch".to_owned())?;
    let duration = Duration::new(seconds, value.timestamp_subsec_nanos());
    u64::try_from(duration.as_nanos())
        .map_err(|_| "lifecycle timestamp overflows OTLP nanoseconds".to_owned())?;
    UNIX_EPOCH
        .checked_add(duration)
        .ok_or_else(|| "lifecycle timestamp overflow".to_owned())
}

fn worker_key(worker: &WorkerTelemetryContext) -> WorkerKey {
    (worker.app_id.to_string(), worker.runner_id.to_string())
}

fn attempt_key(context: &TaskAttemptContext) -> AttemptKey {
    (
        context.app_id.to_string(),
        context.invocation_id.to_string(),
        context.attempt,
    )
}

fn validate_worker(worker: &WorkerTelemetryContext) -> Result<(), String> {
    if worker.context_version != LIFECYCLE_CONTEXT_VERSION {
        return Err(format!(
            "unsupported native lifecycle context version {}",
            worker.context_version
        ));
    }
    Ok(())
}

fn validate_attempt(context: &TaskAttemptContext) -> Result<(), String> {
    if context.context_version != LIFECYCLE_CONTEXT_VERSION {
        return Err(format!(
            "unsupported native lifecycle context version {}",
            context.context_version
        ));
    }
    validate_worker(&context.worker)?;
    if context.app_id != context.worker.app_id {
        return Err("attempt and worker app identity disagree".to_owned());
    }
    for carrier in [
        &context.trace_context,
        &context.execution_trace_context,
        &context.previous_attempt_trace_context,
    ] {
        if !rustvello_core::observability::is_valid_w3c_trace_context(carrier) {
            return Err("invalid lifecycle trace context".to_owned());
        }
    }
    let parent = extract_w3c_trace_context(&context.trace_context)
        .span()
        .span_context()
        .clone();
    let execution = extract_w3c_trace_context(&context.execution_trace_context)
        .span()
        .span_context()
        .clone();
    let previous = extract_w3c_trace_context(&context.previous_attempt_trace_context)
        .span()
        .span_context()
        .clone();
    if execution.is_valid()
        && parent.is_valid()
        && (execution.trace_id() != parent.trace_id()
            || execution.span_id() == parent.span_id()
            || execution.trace_flags() != parent.trace_flags())
    {
        return Err("execution identity disagrees with inherited parent".to_owned());
    }
    if execution.is_valid()
        && previous.is_valid()
        && execution.trace_id() == previous.trace_id()
        && execution.span_id() == previous.span_id()
    {
        return Err("execution span cannot link to itself".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod adapter_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_redacts_token_and_validates_bounds() {
        let config = OtlpLifecycleConfig::new("http://127.0.0.1:4318/", "secret-token");
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
        assert_eq!(config.endpoint, "http://127.0.0.1:4318");

        let mut invalid = config;
        invalid.max_open_attempts = 0;
        assert!(OtlpLifecycleExporter::new(invalid).is_err());
    }

    #[test]
    fn error_type_is_strictly_allowlisted() {
        assert_eq!(sanitized_error_type("ValueError"), "ValueError");
        assert_eq!(
            sanitized_error_type("password=do-not-export"),
            "TaskExecutionError"
        );
    }
}
