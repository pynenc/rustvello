use super::*;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use opentelemetry_proto::tonic::collector::{logs, metrics, trace};
use opentelemetry_proto::tonic::metrics::v1::metric;
use prost::Message;
use rustvello_core::context::RunnerContext;
use rustvello_core::observability::{
    allocate_execution_trace_context, AsyncExportConfig, BoundedAsyncEmitter, EventEmitter,
    TraceContextCarrier,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

#[derive(Clone)]
struct Request {
    path: String,
    headers: String,
    body: Vec<u8>,
}

struct Reply {
    status: u16,
    body: Vec<u8>,
    content_type: &'static str,
    delay: Duration,
    body_delay: Duration,
    length: bool,
}

impl Default for Reply {
    fn default() -> Self {
        Self {
            status: 200,
            body: Vec::new(),
            content_type: "application/x-protobuf",
            delay: Duration::ZERO,
            body_delay: Duration::ZERO,
            length: true,
        }
    }
}

/// Real loopback receiver with deterministic fault replies and bounded teardown.
struct Receiver {
    endpoint: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    // Deadline tests need their own scheduling budget; concurrent cold HTTP
    // clients otherwise contend for platform TLS initialization on CI hosts.
    _isolation: MutexGuard<'static, ()>,
}

static RECEIVER_ISOLATION: Mutex<()> = Mutex::new(());

impl Receiver {
    fn new(reply: impl Fn(&Request) -> Reply + Send + 'static) -> Self {
        let isolation = RECEIVER_ISOLATION
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let received = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopped.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("{error}"),
                };
                // macOS accepted sockets can inherit the listener's nonblocking
                // flag; request parsing must wait for bytes under its timeout.
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut headers = String::new();
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        break;
                    }
                    headers.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                    assert!(headers.len() < 16_384);
                }
                let Some(path) = headers.split_whitespace().nth(1).map(str::to_owned) else {
                    continue;
                };
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                assert!(length < 1_048_576);
                let mut body = vec![0; length];
                if reader.read_exact(&mut body).is_err() {
                    continue;
                }
                let request = Request {
                    path,
                    headers,
                    body,
                };
                let response = reply(&request);
                received.lock().unwrap().push(request);
                thread::sleep(response.delay);
                let mut headers = format!(
                    "HTTP/1.1 {} Test\r\nContent-Type: {}\r\nConnection: close\r\n",
                    response.status, response.content_type
                );
                if response.length {
                    headers.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
                }
                headers.push_str("\r\n");
                let _ = stream.write_all(headers.as_bytes());
                thread::sleep(response.body_delay);
                let _ = stream.write_all(&response.body);
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
            _isolation: isolation,
        }
    }

    fn config(&self) -> OtlpLifecycleConfig {
        let mut config = OtlpLifecycleConfig::new(&self.endpoint, "test-token");
        // Semantic mapping tests use the production budget. Dedicated timeout
        // cases below override it and assert their much shorter deadlines.
        config.export_timeout = Duration::from_secs(5);
        config
    }

    fn exporter(&self) -> OtlpLifecycleExporter {
        OtlpLifecycleExporter::new(self.config()).unwrap()
    }

    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    fn spans(&self) -> Vec<opentelemetry_proto::tonic::trace::v1::Span> {
        self.requests()
            .into_iter()
            .filter(|r| r.path == "/v1/traces")
            .flat_map(|r| {
                trace::v1::ExportTraceServiceRequest::decode(r.body.as_slice())
                    .unwrap()
                    .resource_spans
            })
            .flat_map(|r| r.scope_spans)
            .flat_map(|s| s.spans)
            .collect()
    }

    fn logs(&self) -> Vec<opentelemetry_proto::tonic::logs::v1::LogRecord> {
        self.requests()
            .into_iter()
            .filter(|r| r.path == "/v1/logs")
            .flat_map(|r| {
                logs::v1::ExportLogsServiceRequest::decode(r.body.as_slice())
                    .unwrap()
                    .resource_logs
            })
            .flat_map(|r| r.scope_logs)
            .flat_map(|s| s.log_records)
            .collect()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap();
    }
}

fn attempt(app: &str, invocation: &str, worker: &str, number: u32) -> TaskAttemptContext {
    let worker = RunnerContext::new(
        RunnerId::from_string(worker),
        Arc::from(app),
        "PersistentTokioWorker",
    );
    let parent = TraceContextCarrier {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned()),
        tracestate: Some("vendor=value".to_owned()),
    };
    let mut context = TaskAttemptContext::new(
        Arc::<str>::from(app),
        TaskId::new("orders", "charge"),
        InvocationId::from_string(invocation),
        number,
        "default",
        None,
        None,
        WorkerTelemetryContext::from(&worker),
        parent,
    );
    context.execution_trace_context = allocate_execution_trace_context(&context.trace_context);
    context
}

fn task(event: TaskLifecycleEvent) -> LifecycleEvent {
    LifecycleEvent::Task(Box::new(event))
}
fn worker(event: WorkerLifecycleEvent) -> LifecycleEvent {
    LifecycleEvent::Worker(event)
}

fn pair(context: TaskAttemptContext) -> [LifecycleEvent; 2] {
    let start = TaskLifecycleEvent::started(context.clone());
    let mut end = TaskLifecycleEvent::succeeded(context, Duration::from_millis(5));
    end.event_time = start.event_time + chrono::Duration::milliseconds(5);
    [task(start), task(end)]
}

#[test]
fn execution_identity_parent_retry_links_and_log_correlation_are_exact() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let first = attempt("orders", "invocation", "worker-1", 0);
    let mut retry = attempt("orders", "invocation", "worker-2", 1);
    retry.previous_attempt_trace_context = first.execution_trace_context.clone();
    let mut events = pair(first.clone());
    if let LifecycleEvent::Task(event) = &mut events[1] {
        event.kind = TaskLifecycleKind::Failed {
            error_type: "secret-raw-error".to_owned(),
            duration: Duration::from_millis(5),
        };
    }
    exporter.export(&events).unwrap();
    exporter
        .export(&[task(TaskLifecycleEvent::retry_scheduled(first.clone(), 1))])
        .unwrap();
    exporter.export(&pair(retry.clone())).unwrap();
    exporter.shutdown().unwrap();
    let spans = receiver.spans();
    assert_eq!(spans.len(), 2);
    let carrier_context = |carrier: &TraceContextCarrier| {
        extract_w3c_trace_context(carrier)
            .span()
            .span_context()
            .clone()
    };
    for (span, context) in spans.iter().zip([&first, &retry]) {
        assert_eq!(span.span_id, context_fn(context).span_id().to_bytes());
        assert_eq!(span.trace_id, context_fn(context).trace_id().to_bytes());
        assert_eq!(
            span.parent_span_id,
            carrier_context(&first.trace_context).span_id().to_bytes()
        );
        assert_eq!(span.kind, 5);
        assert!(span.end_time_unix_nano > span.start_time_unix_nano);
    }
    assert!(spans[0].links.is_empty());
    assert_eq!(spans[1].links.len(), 1);
    assert_eq!(spans[1].links[0].span_id, spans[0].span_id);
    assert_eq!(spans[1].links[0].trace_id, spans[0].trace_id);
    assert_eq!(spans[0].status.as_ref().unwrap().code, 2);
    assert_eq!(spans[1].status.as_ref().unwrap().code, 1);
    for log in receiver.logs() {
        assert_eq!(
            log.span_id,
            if log.event_name == "task.retry_scheduled"
                || log.attributes.iter().any(|a| a.key == "rustvello.attempt"
                    && a.value.as_ref().unwrap().value
                        == Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::IntValue(0)
                        ))
            {
                spans[0].span_id.clone()
            } else {
                spans[1].span_id.clone()
            }
        );
    }
    for request in receiver.requests() {
        assert!(request
            .headers
            .to_lowercase()
            .contains("authorization: bearer test-token"));
        assert!(!request
            .body
            .windows(b"secret-raw-error".len())
            .any(|w| w == b"secret-raw-error"));
    }
    let stats = exporter.accounting().stats();
    assert_eq!(stats.traces.acknowledged, 2);
    assert_eq!(stats.logs.acknowledged, 5);
    assert_eq!(stats.metrics.acknowledged, 2);
}

fn context_fn(context: &TaskAttemptContext) -> SpanContext {
    extract_w3c_trace_context(&context.execution_trace_context)
        .span()
        .span_context()
        .clone()
}

#[test]
fn child_parent_is_the_emitted_execution_span() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let parent = attempt("orders", "parent", "worker", 0);
    let mut child = attempt("orders", "child", "worker", 0);
    child.trace_context = parent.execution_trace_context.clone();
    child.execution_trace_context = allocate_execution_trace_context(&child.trace_context);
    exporter.export(&pair(parent)).unwrap();
    exporter.export(&pair(child)).unwrap();
    let spans = receiver.spans();
    assert_eq!(spans[1].parent_span_id, spans[0].span_id);
}

#[test]
fn unsampled_logs_keep_runtime_identity_without_exporting_spans() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let mut context = attempt("orders", "unsampled", "worker", 0);
    context.trace_context.traceparent =
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".to_owned());
    context.execution_trace_context = allocate_execution_trace_context(&context.trace_context);
    exporter.export(&pair(context.clone())).unwrap();
    assert!(receiver.spans().is_empty());
    assert_eq!(receiver.logs().len(), 2);
    for log in receiver.logs() {
        assert_eq!(log.flags, 0);
        assert_eq!(log.span_id, context_fn(&context).span_id().to_bytes());
    }
    assert_eq!(exporter.accounting().stats().unsampled_attempts, 1);
    assert_eq!(exporter.accounting().stats().metrics.acknowledged, 1);
}

#[test]
fn missing_start_emits_only_truthful_terminal_log() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let events = pair(attempt("orders", "missing", "worker", 0));
    assert!(exporter
        .export(&events[1..])
        .unwrap_err()
        .contains("no observed start"));
    assert_eq!(receiver.logs()[0].event_name, "task.succeeded");
    assert!(receiver.spans().is_empty());
    assert_eq!(exporter.accounting().stats().incomplete_attempts, 1);
    assert_eq!(exporter.accounting().stats().metrics.attempted, 0);
}

#[test]
fn missing_runtime_identity_never_allocates_an_export_thread_span() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let mut context = attempt("orders", "missing", "worker", 0);
    context.execution_trace_context = TraceContextCarrier::default();
    assert!(exporter.export(&pair(context)).is_err());
    assert!(receiver.spans().is_empty());
    assert_eq!(receiver.logs().len(), 2);
}

#[test]
fn duplicates_do_not_end_or_replace_an_open_span_or_recount_completion() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let events = pair(attempt("orders", "duplicate", "worker", 0));
    exporter.export(&events[..1]).unwrap();
    assert!(exporter.export(&events[..1]).is_err());
    assert!(receiver.spans().is_empty());
    exporter.export(&events[1..]).unwrap();
    assert!(exporter.export(&events).is_err());
    assert_eq!(receiver.spans().len(), 1);
    assert_eq!(exporter.accounting().stats().metrics.acknowledged, 1);
    assert_eq!(exporter.accounting().stats().duplicate_events, 3);
}

#[test]
fn worker_stop_abandons_open_attempts_releases_capacity_and_never_completes_them() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut config = receiver.config();
    config.max_worker_resources = 1;
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let context = attempt("orders", "abandoned", "worker", 0);
    let events = pair(context.clone());
    exporter.export(&events[..1]).unwrap();
    assert!(exporter
        .export(&[worker(WorkerLifecycleEvent::stopped(context.worker))])
        .is_err());
    assert_eq!(exporter.accounting().stats().incomplete_attempts, 1);
    assert!(exporter.open_attempts.is_empty());
    assert!(exporter.workers.is_empty());
    assert!(receiver.spans().is_empty());
    assert!(exporter.export(&events[1..]).is_err());
    exporter
        .export(&pair(attempt("orders", "next", "other-worker", 0)))
        .unwrap();
    assert_eq!(receiver.spans().len(), 1);
}

#[test]
fn shutdown_and_drop_account_incomplete_attempts_without_network_work() {
    let receiver = Receiver::new(|_| Reply::default());
    for explicit in [true, false] {
        let mut exporter = receiver.exporter();
        let accounting = exporter.accounting();
        exporter
            .export(&pair(attempt("orders", "unfinished", "worker", 0))[..1])
            .unwrap();
        let requests = receiver.requests().len();
        if explicit {
            let start = Instant::now();
            assert!(exporter.shutdown().is_err());
            assert!(start.elapsed() < Duration::from_millis(100));
            exporter.shutdown().unwrap();
            assert!(exporter.export(&[]).is_err());
        }
        drop(exporter);
        assert_eq!(accounting.stats().incomplete_attempts, 1);
        assert_eq!(receiver.requests().len(), requests);
        assert!(receiver.spans().is_empty());
    }
}

#[test]
fn app_qualified_keys_isolate_equal_worker_and_attempt_names() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let a = pair(attempt("app:a", "same", "worker", 0));
    let b = pair(attempt("app:b", "same", "worker", 0));
    exporter.export(&[a[0].clone(), b[0].clone()]).unwrap();
    assert_eq!(exporter.workers.len(), 2);
    assert_eq!(exporter.open_attempts.len(), 2);
    exporter.export(&[a[1].clone(), b[1].clone()]).unwrap();
    assert_eq!(receiver.spans().len(), 2);
    assert_ne!(
        worker_key(&attempt("a:b", "inv", "c", 0).worker),
        worker_key(&attempt("a", "inv", "b:c", 0).worker)
    );
    assert_ne!(
        attempt_key(&attempt("a:b", "c", "w", 0)),
        attempt_key(&attempt("a", "b:c", "w", 0))
    );
}

#[test]
fn open_and_recent_history_bounds_hold_without_eviction_completion() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut config = receiver.config();
    config.max_open_attempts = 1;
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let a = pair(attempt("orders", "a", "worker", 0));
    let b = pair(attempt("orders", "b", "worker", 0));
    exporter.export(&a[..1]).unwrap();
    assert!(exporter.export(&b[..1]).is_err());
    assert_eq!(exporter.open_attempts.len(), 1);
    assert!(receiver.spans().is_empty());
    exporter.export(&a[1..]).unwrap();
    assert!(exporter.export(&b[1..]).is_err());
    assert_eq!(receiver.spans().len(), 1);
    for index in 0..5 {
        exporter
            .export(&pair(attempt(
                "orders",
                &format!("next-{index}"),
                "worker",
                0,
            )))
            .unwrap();
        assert!(exporter.closed_attempts.len() <= 1);
        assert!(exporter.closed_order.len() <= 1);
    }
}

#[test]
fn resource_overflow_and_invalid_event_do_not_skip_later_batch_events() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut config = receiver.config();
    config.max_worker_resources = 1;
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let context = attempt("orders", "valid", "worker", 0);
    exporter
        .export(&[task(TaskLifecycleEvent::submitted(context.clone()))])
        .unwrap();
    let other = attempt("orders", "other", "other-worker", 0);
    let mut invalid = context.clone();
    invalid.context_version = 99;
    assert!(exporter
        .export(&[
            task(TaskLifecycleEvent::submitted(other)),
            task(TaskLifecycleEvent::submitted(invalid)),
            task(TaskLifecycleEvent::submitted(context)),
        ])
        .is_err());
    assert_eq!(receiver.logs().len(), 2);
    assert_eq!(exporter.workers.len(), 1);
    assert_eq!(exporter.accounting().stats().failed_events, 2);
}

#[test]
fn mismatched_terminal_and_invalid_timestamp_leave_start_intact() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let events = pair(attempt("orders", "inv", "worker", 0));
    exporter.export(&events[..1]).unwrap();
    let mut wrong = events[1].clone();
    if let LifecycleEvent::Task(event) = &mut wrong {
        event.context.worker.runner_id = RunnerId::from_string("wrong");
    }
    assert!(exporter.export(&[wrong]).is_err());
    let mut wrong = events[1].clone();
    if let LifecycleEvent::Task(event) = &mut wrong {
        event.event_time = chrono::DateTime::from_timestamp(-1, 0).unwrap();
    }
    assert!(exporter.export(&[wrong]).is_err());
    assert!(receiver.spans().is_empty());
    assert_eq!(exporter.open_attempts.len(), 1);
    exporter.export(&events[1..]).unwrap();
    assert_eq!(receiver.spans().len(), 1);
}

#[test]
fn every_signal_accounts_for_401_503_malformed_and_partial_rejection() {
    for failure in 0..4 {
        let receiver = Receiver::new(move |_| match failure {
            0 => Reply {
                status: 401,
                ..Reply::default()
            },
            1 => Reply {
                status: 503,
                ..Reply::default()
            },
            2 => Reply {
                body: vec![0xff],
                ..Reply::default()
            },
            _ => Reply {
                body: vec![0x0a, 0x02, 0x08, 0x01],
                ..Reply::default()
            },
        });
        let mut exporter = receiver.exporter();
        assert!(exporter
            .export(&pair(attempt("orders", "inv", "worker", 0)))
            .is_err());
        let stats = exporter.accounting().stats();
        for (signal, count) in [(stats.logs, 2), (stats.traces, 1), (stats.metrics, 1)] {
            assert_eq!(signal.attempted, count);
            assert_eq!(
                signal.acknowledged,
                if failure == 3 { count - 1 } else { 0 }
            );
            assert_eq!(signal.rejected, if failure == 3 { 1 } else { 0 });
            assert_eq!(signal.failed, if failure == 3 { 0 } else { count });
        }
        exporter.shutdown().unwrap();
        assert_eq!(receiver.requests().len(), 3, "no request may be retried");
    }
}

#[test]
fn one_signal_failure_does_not_prevent_other_signals_from_exporting() {
    for failed in ["/v1/logs", "/v1/traces", "/v1/metrics"] {
        let receiver = Receiver::new(move |request| Reply {
            status: if request.path == failed { 503 } else { 200 },
            ..Reply::default()
        });
        let mut exporter = receiver.exporter();
        assert!(exporter
            .export(&pair(attempt("orders", "inv", "worker", 0)))
            .is_err());
        let stats = exporter.accounting().stats();
        for (path, signal) in [
            ("/v1/logs", stats.logs),
            ("/v1/traces", stats.traces),
            ("/v1/metrics", stats.metrics),
        ] {
            assert_eq!(
                signal.attempted,
                signal.acknowledged + signal.failed + signal.rejected
            );
            if path == failed {
                assert_eq!(signal.failed, signal.attempted);
            } else {
                assert_eq!(signal.acknowledged, signal.attempted);
            }
        }
    }
}

#[test]
fn partial_warning_is_not_loss_and_receiver_error_text_is_not_exposed() {
    let body = trace::v1::ExportTraceServiceResponse {
        partial_success: Some(trace::v1::ExportTracePartialSuccess {
            rejected_spans: 0,
            error_message: "private receiver warning".to_owned(),
        }),
    }
    .encode_to_vec();
    let receiver = Receiver::new(move |_| Reply {
        body: body.clone(),
        ..Reply::default()
    });
    let mut exporter = receiver.exporter();
    exporter
        .export(&pair(attempt("orders", "inv", "worker", 0)))
        .unwrap();
    assert_eq!(exporter.accounting().stats().response_warnings, 3);
    assert_eq!(exporter.accounting().stats().logs.acknowledged, 2);
}

#[test]
fn malformed_rejection_counts_and_response_bounds_are_visible_failures() {
    for fault in 0..6 {
        let receiver = Receiver::new(move |_| match fault {
            0 => Reply {
                body: vec![0x0a, 0x02, 0x08, 0x02],
                ..Reply::default()
            },
            1 => Reply {
                body: trace::v1::ExportTraceServiceResponse {
                    partial_success: Some(trace::v1::ExportTracePartialSuccess {
                        rejected_spans: -1,
                        error_message: String::new(),
                    }),
                }
                .encode_to_vec(),
                ..Reply::default()
            },
            2 => Reply {
                body: vec![0; 65_537],
                ..Reply::default()
            },
            3 => Reply {
                body: vec![0; 65_537],
                length: false,
                ..Reply::default()
            },
            4 => Reply {
                content_type: "text/html",
                ..Reply::default()
            },
            _ => Reply {
                status: 302,
                ..Reply::default()
            },
        });
        let mut exporter = receiver.exporter();
        assert!(exporter
            .export(&[task(TaskLifecycleEvent::submitted(attempt(
                "orders", "inv", "worker", 0
            )))])
            .is_err());
        assert_eq!(exporter.accounting().stats().logs.failed, 1);
        assert_eq!(exporter.accounting().stats().logs.acknowledged, 0);
        assert_eq!(receiver.requests().len(), 1);
    }
}

#[test]
fn real_http_failure_reaches_outer_emitter_accounting() {
    for failure in [401, 503, 200] {
        let receiver = Receiver::new(move |_| Reply {
            status: failure,
            body: if failure == 200 {
                vec![0x0a, 0x02, 0x08, 0x01]
            } else {
                Vec::new()
            },
            ..Reply::default()
        });
        let exporter = receiver.exporter();
        let accounting = exporter.accounting();
        let emitter = BoundedAsyncEmitter::new(AsyncExportConfig::default(), exporter);
        emitter.on_task_lifecycle(&TaskLifecycleEvent::submitted(attempt(
            "orders", "inv", "worker", 0,
        )));
        let stats = emitter.flush(Duration::from_secs(2)).unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.exported, 0);
        assert_eq!(stats.export_failed, 1);
        assert_eq!(accounting.stats().logs.acknowledged, 0);
        emitter.shutdown(Duration::from_secs(2)).unwrap();
    }
}

#[test]
fn real_timeout_overflow_and_shutdown_keep_task_path_bounded() {
    let receiver = Receiver::new(|_| Reply {
        delay: Duration::from_millis(200),
        ..Reply::default()
    });
    let mut config = receiver.config();
    config.export_timeout = Duration::from_millis(100);
    let exporter = OtlpLifecycleExporter::new(config).unwrap();
    let accounting = exporter.accounting();
    let emitter = BoundedAsyncEmitter::new(
        AsyncExportConfig {
            queue_capacity: 2,
            max_batch_size: 1,
            scheduled_delay: Duration::from_millis(1),
            ..AsyncExportConfig::default()
        },
        exporter,
    );
    let event = TaskLifecycleEvent::submitted(attempt("orders", "inv", "worker", 0));
    emitter.on_task_lifecycle(&event);
    let wait = Instant::now();
    while receiver.requests().is_empty() {
        assert!(wait.elapsed() < Duration::from_secs(2));
        thread::sleep(Duration::from_millis(1));
    }
    let start = Instant::now();
    for _ in 0..32 {
        emitter.on_task_lifecycle(&event);
    }
    assert!(start.elapsed() < Duration::from_millis(50));
    assert!(emitter.stats().dropped > 0);
    assert!(emitter.flush(Duration::from_millis(5)).is_err());
    let start = Instant::now();
    assert!(emitter.shutdown(Duration::from_millis(5)).is_err());
    assert!(start.elapsed() < Duration::from_millis(100));
    emitter.on_task_lifecycle(&event);
    assert_eq!(emitter.stats().rejected_after_shutdown, 1);
    let stats = emitter.shutdown(Duration::from_secs(3)).unwrap();
    assert_eq!(stats.exported, 0);
    assert_eq!(stats.export_failed, stats.accepted);
    assert_eq!(accounting.stats().logs.failed, stats.accepted);
}

#[test]
fn metric_delta_intervals_do_not_overlap_or_include_attempt_identity() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    exporter
        .export(&pair(attempt("orders", "first", "worker", 0)))
        .unwrap();
    exporter
        .export(&pair(attempt("orders", "second", "worker", 0)))
        .unwrap();
    exporter.shutdown().unwrap();
    let requests: Vec<_> = receiver
        .requests()
        .into_iter()
        .filter(|r| r.path == "/v1/metrics")
        .collect();
    assert_eq!(requests.len(), 2);
    let mut prior = 0;
    for request in requests {
        let request =
            metrics::v1::ExportMetricsServiceRequest::decode(request.body.as_slice()).unwrap();
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        let Some(metric::Data::Sum(sum)) = &metric.data else {
            panic!("expected sum")
        };
        assert_eq!(sum.aggregation_temporality, 1);
        assert!(sum.is_monotonic);
        let point = &sum.data_points[0];
        assert!(point.start_time_unix_nano >= prior);
        assert!(point.time_unix_nano > point.start_time_unix_nano);
        prior = point.time_unix_nano;
        let keys: HashSet<_> = point
            .attributes
            .iter()
            .map(|attribute| attribute.key.as_str())
            .collect();
        assert_eq!(
            keys,
            HashSet::from(["rustvello.task.id", "rustvello.queue", "outcome"])
        );
    }
}

#[test]
fn a_large_outer_batch_uses_only_three_requests_and_exact_partial_counts() {
    let receiver = Receiver::new(|_| Reply {
        body: vec![0x0a, 0x02, 0x08, 0x01],
        ..Reply::default()
    });
    let mut exporter = receiver.exporter();
    let events: Vec<_> = (0..32)
        .flat_map(|index| {
            pair(attempt(
                "orders",
                &format!("inv-{index}"),
                &format!("worker-{}", index % 2),
                0,
            ))
        })
        .collect();
    assert!(exporter.export(&events).is_err());
    assert_eq!(receiver.requests().len(), 3);
    let stats = exporter.export_stats();
    for (signal, count) in [(stats.logs, 64), (stats.traces, 32), (stats.metrics, 32)] {
        assert_eq!(signal.attempted, count);
        assert_eq!(signal.acknowledged, count - 1);
        assert_eq!(signal.rejected, 1);
        assert_eq!(signal.failed, 0);
        assert_eq!(signal.not_sent, 0);
    }
    exporter.shutdown().unwrap();
    assert_eq!(receiver.requests().len(), 3);
}

#[test]
fn shared_deadline_bounds_whole_batch_and_accounts_for_unsent_signals() {
    let receiver = Receiver::new(|_| Reply {
        delay: Duration::from_millis(250),
        ..Reply::default()
    });
    let mut config = receiver.config();
    config.export_timeout = Duration::from_millis(100);
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let events: Vec<_> = (0..32)
        .flat_map(|index| pair(attempt("orders", &format!("inv-{index}"), "worker", 0)))
        .collect();
    let start = Instant::now();
    assert!(exporter.export(&events).is_err());
    assert!(start.elapsed() < Duration::from_millis(225));
    let stats = exporter.export_stats();
    assert_eq!(stats.logs.attempted, 64);
    assert_eq!(stats.logs.failed, 64);
    assert_eq!(stats.traces.attempted, 0);
    assert_eq!(stats.traces.not_sent, 32);
    assert_eq!(stats.metrics.attempted, 0);
    assert_eq!(stats.metrics.not_sent, 32);
    assert_eq!(stats.unprocessed_events, 0);
    exporter.shutdown().unwrap();
    assert_eq!(receiver.requests().len(), 1);
}

#[test]
fn later_signal_uses_remaining_budget_instead_of_restarting_timeout() {
    // Generous budgets: CI runners (macOS in particular) add tens of milliseconds of jitter and
    // the assertion is about the remaining budget being shared, not about absolute latency.
    let receiver = Receiver::new(|request| Reply {
        delay: if request.path == "/v1/logs" {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(1_500)
        },
        ..Reply::default()
    });
    let mut config = receiver.config();
    config.export_timeout = Duration::from_millis(500);
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let start = Instant::now();
    assert!(exporter
        .export(&pair(attempt("orders", "inv", "worker", 0)))
        .is_err());
    assert!(start.elapsed() < Duration::from_millis(1_200));
    let stats = exporter.export_stats();
    assert_eq!(stats.logs.acknowledged, 2);
    assert_eq!(stats.traces.failed, 1);
    assert_eq!(stats.metrics.not_sent, 1);
    assert_eq!(receiver.requests().len(), 2);
}

#[test]
fn exhausted_mapping_budget_counts_events_without_claiming_network_loss() {
    let mut config = OtlpLifecycleConfig::new("http://127.0.0.1:1", "token");
    config.export_timeout = Duration::from_nanos(1);
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let events = pair(attempt("orders", "inv", "worker", 0));
    assert!(exporter.export(&events).is_err());
    let stats = exporter.export_stats();
    assert_eq!(stats.unprocessed_events, 2);
    assert_eq!(stats.failed_events, 2);
    assert_eq!(stats.logs, SignalExportStats::default());
    assert_eq!(stats.traces, SignalExportStats::default());
    assert_eq!(stats.metrics, SignalExportStats::default());
    exporter.shutdown().unwrap();
}

#[test]
fn submitted_logs_use_submitter_parent_not_a_future_execution() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let context = attempt("orders", "inv", "worker", 0);
    exporter
        .export(&[task(TaskLifecycleEvent::submitted(context.clone()))])
        .unwrap();
    let logs = receiver.logs();
    let parent = extract_w3c_trace_context(&context.trace_context)
        .span()
        .span_context()
        .clone();
    assert_eq!(logs[0].span_id, parent.span_id().to_bytes());
    assert!(receiver.spans().is_empty());
}

#[test]
fn invalid_retry_app_and_trace_identity_are_rejected_before_export() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let context = attempt("orders", "inv", "worker", 0);
    assert!(exporter
        .export(&[task(TaskLifecycleEvent::retry_scheduled(
            context.clone(),
            0
        ))])
        .is_err());
    let mut wrong = context.clone();
    wrong.app_id = Arc::from("different-app");
    assert!(exporter.export(&pair(wrong)).is_err());
    let mut wrong = context.clone();
    wrong.execution_trace_context = context.trace_context.clone();
    assert!(exporter.export(&pair(wrong)).is_err());
    let mut wrong = context.clone();
    wrong.previous_attempt_trace_context = wrong.execution_trace_context.clone();
    assert!(exporter.export(&pair(wrong)).is_err());
    let mut wrong = context;
    wrong.execution_trace_context.traceparent = Some("invalid".to_owned());
    assert!(exporter.export(&pair(wrong)).is_err());
    assert!(receiver.requests().is_empty());
}

#[test]
fn worker_stop_only_abandons_attempts_in_its_app() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let a = attempt("a", "same", "same", 0);
    let b = attempt("b", "same", "same", 0);
    let a_events = pair(a.clone());
    let b_events = pair(b);
    exporter
        .export(&[a_events[0].clone(), b_events[0].clone()])
        .unwrap();
    assert!(exporter
        .export(&[worker(WorkerLifecycleEvent::stopped(a.worker.clone()))])
        .is_err());
    // Repeated stop cannot recreate or finish the abandoned execution.
    exporter
        .export(&[worker(WorkerLifecycleEvent::stopped(a.worker))])
        .unwrap();
    assert_eq!(exporter.export_stats().incomplete_attempts, 1);
    exporter.export(&b_events[1..]).unwrap();
    assert_eq!(receiver.spans().len(), 1);
}

#[test]
fn response_body_timeout_is_unconfirmed_delivery_and_consumes_shared_budget() {
    let receiver = Receiver::new(|_| Reply {
        body: vec![0x0a, 0x00],
        body_delay: Duration::from_millis(250),
        ..Reply::default()
    });
    let mut config = receiver.config();
    config.export_timeout = Duration::from_millis(100);
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let start = Instant::now();
    let error = exporter
        .export(&pair(attempt("orders", "inv", "worker", 0)))
        .unwrap_err();
    assert!(error.contains("response read"));
    assert!(start.elapsed() < Duration::from_millis(225));
    let stats = exporter.export_stats();
    assert_eq!(stats.logs.failed, 2);
    assert_eq!(stats.logs.acknowledged, 0);
    assert_eq!(stats.traces.not_sent, 1);
    assert_eq!(stats.metrics.not_sent, 1);
}

#[test]
fn worker_lifecycle_has_resource_facts_without_task_or_trace_fabrication() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut exporter = receiver.exporter();
    let context = attempt("orders", "unused", "worker", 0).worker;
    exporter
        .export(&[
            worker(WorkerLifecycleEvent::started(context.clone())),
            worker(WorkerLifecycleEvent::stopped(context)),
        ])
        .unwrap();
    assert_eq!(receiver.requests().len(), 1);
    let logs = receiver.logs();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].event_name, "worker.started");
    assert_eq!(logs[1].event_name, "worker.stopped");
    for log in logs {
        assert!(log.trace_id.is_empty());
        assert!(log.span_id.is_empty());
        let keys: HashSet<_> = log.attributes.iter().map(|a| a.key.as_str()).collect();
        assert_eq!(
            keys,
            HashSet::from(["telemetry.mapping.revision", "rustvello.event"])
        );
    }
    let request =
        logs::v1::ExportLogsServiceRequest::decode(receiver.requests()[0].body.as_slice()).unwrap();
    for resource in request.resource_logs {
        assert_eq!(
            resource.scope_logs[0].scope.as_ref().unwrap().name,
            INSTRUMENTATION_SCOPE
        );
        let attributes = resource.resource.unwrap().attributes;
        assert!(attributes.iter().any(|a| a.key == "rustvello.app.id"));
        assert!(attributes.iter().any(|a| a.key == "rustvello.worker.id"));
        assert!(attributes.iter().any(|a| a.key == "process.pid"));
    }
    assert!(exporter.workers.is_empty());
    assert!(receiver.spans().is_empty());
}

#[test]
fn request_byte_limit_defaults_to_receiver_cap_and_cannot_exceed_it() {
    let mut config = OtlpLifecycleConfig::new("http://127.0.0.1:1", "token");
    assert_eq!(config.max_request_bytes, 4 * 1024 * 1024);
    for invalid in [0, MAX_REQUEST_BYTES + 1, usize::MAX] {
        config.max_request_bytes = invalid;
        assert!(OtlpLifecycleExporter::new(config.clone()).is_err());
    }
    config.max_request_bytes = 1;
    assert!(OtlpLifecycleExporter::new(config).is_ok());
}

#[test]
fn oversized_task_and_worker_strings_reject_all_signal_bodies_without_network() {
    let receiver = Receiver::new(|_| Reply::default());
    for large_worker in [false, true] {
        let mut exporter = receiver.exporter();
        let mut context = attempt("orders", "inv", "worker", 0);
        let oversized = "x".repeat(MAX_REQUEST_BYTES);
        if large_worker {
            context.worker.hostname = oversized;
        } else {
            context.task_id = TaskId::new("orders", &oversized);
        }
        let error = exporter.export(&pair(context)).unwrap_err();
        assert!(error.contains("request body exceeds byte bound"));
        assert!(receiver.requests().is_empty());
        let stats = exporter.export_stats();
        for (signal, count) in [(stats.logs, 2), (stats.traces, 1), (stats.metrics, 1)] {
            assert_eq!(
                signal,
                SignalExportStats {
                    not_sent: count,
                    ..Default::default()
                }
            );
        }
        assert_eq!(stats.failed_events, 0);
        assert_eq!(stats.unprocessed_events, 0);
        assert_eq!(stats.incomplete_attempts, 0);
        assert!(exporter.open_attempts.is_empty());
        exporter.shutdown().unwrap();
    }
}

#[test]
fn configured_request_limit_accepts_exact_size_and_rejects_one_byte_less() {
    let receiver = Receiver::new(|_| Reply::default());
    let event = worker(WorkerLifecycleEvent::started(
        attempt("orders", "inv", "worker", 0).worker,
    ));
    receiver
        .exporter()
        .export(std::slice::from_ref(&event))
        .unwrap();
    let size = receiver.requests()[0].body.len();
    for (limit, accepted) in [(size, true), (size - 1, false)] {
        let mut config = receiver.config();
        config.max_request_bytes = limit;
        let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
        assert_eq!(
            exporter.export(std::slice::from_ref(&event)).is_ok(),
            accepted
        );
        let stats = exporter.export_stats();
        assert_eq!(stats.logs.acknowledged, u64::from(accepted));
        assert_eq!(stats.logs.not_sent, u64::from(!accepted));
        assert_eq!(stats.logs.failed, 0);
        assert_eq!(stats.failed_events, 0);
    }
    assert_eq!(receiver.requests().len(), 2);
}

#[test]
fn oversized_batched_logs_do_not_suppress_smaller_trace_and_metric_requests() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut config = receiver.config();
    config.max_request_bytes = 4096;
    let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
    let context = attempt("orders", "inv", "worker", 0);
    let mut events: Vec<_> = (0..16)
        .map(|_| task(TaskLifecycleEvent::submitted(context.clone())))
        .collect();
    events.extend(pair(context));
    assert!(exporter.export(&events).is_err());
    let stats = exporter.export_stats();
    assert_eq!(stats.logs.not_sent, 18);
    assert_eq!(stats.logs.attempted, 0);
    assert_eq!(stats.traces.acknowledged, 1);
    assert_eq!(stats.metrics.acknowledged, 1);
    assert_eq!(stats.failed_events, 0);
    assert_eq!(receiver.requests().len(), 2);
}

#[test]
fn worker_stop_cleans_up_after_oversized_body_and_counts_native_abandonment_separately() {
    let receiver = Receiver::new(|_| Reply::default());
    for incomplete in [false, true] {
        let mut config = receiver.config();
        config.max_request_bytes = 1;
        let mut exporter = OtlpLifecycleExporter::new(config).unwrap();
        let context = attempt("orders", "inv", "worker", 0);
        if incomplete {
            assert!(exporter.export(&pair(context.clone())[..1]).is_err());
            assert_eq!(exporter.export_stats().logs.not_sent, 1);
            assert_eq!(exporter.export_stats().failed_events, 0);
        }
        assert!(exporter
            .export(&[worker(WorkerLifecycleEvent::stopped(context.worker))])
            .is_err());
        assert!(exporter.workers.is_empty());
        assert!(exporter.open_attempts.is_empty());
        let stats = exporter.export_stats();
        assert_eq!(stats.logs.not_sent, 1 + u64::from(incomplete));
        assert_eq!(stats.logs.attempted, 0);
        assert_eq!(stats.logs.failed, 0);
        assert_eq!(stats.failed_events, u64::from(incomplete));
        assert_eq!(stats.incomplete_attempts, u64::from(incomplete));
        assert_eq!(stats.traces, SignalExportStats::default());
        assert_eq!(stats.metrics, SignalExportStats::default());
        exporter.shutdown().unwrap();
        assert_eq!(exporter.export_stats(), stats);
    }
    assert!(receiver.requests().is_empty());
}

#[test]
fn worker_stop_cleans_up_despite_malformed_response_and_accounts_both_failures() {
    let failed = Arc::new(AtomicBool::new(false));
    let fail_response = Arc::clone(&failed);
    let receiver = Receiver::new(move |_| Reply {
        body: if fail_response.load(Ordering::Acquire) {
            vec![0xff]
        } else {
            Vec::new()
        },
        ..Reply::default()
    });
    let mut exporter = receiver.exporter();
    let context = attempt("orders", "inv", "worker", 0);
    exporter.export(&pair(context.clone())[..1]).unwrap();
    failed.store(true, Ordering::Release);
    assert!(exporter
        .export(&[worker(WorkerLifecycleEvent::stopped(context.worker))])
        .is_err());
    assert!(exporter.workers.is_empty());
    assert!(exporter.open_attempts.is_empty());
    let stats = exporter.export_stats();
    assert_eq!(stats.logs.attempted, 2);
    assert_eq!(stats.logs.acknowledged, 1);
    assert_eq!(stats.logs.failed, 1);
    assert_eq!(stats.logs.not_sent, 0);
    assert_eq!(stats.failed_events, 1);
    assert_eq!(stats.incomplete_attempts, 1);
    assert!(receiver.spans().is_empty());
    exporter.shutdown().unwrap();
    assert_eq!(exporter.export_stats(), stats);
    assert_eq!(receiver.requests().len(), 2);
}

#[test]
fn oversized_request_failure_reaches_outer_emitter_without_network_loss_claim() {
    let receiver = Receiver::new(|_| Reply::default());
    let mut config = receiver.config();
    config.max_request_bytes = 1;
    let exporter = OtlpLifecycleExporter::new(config).unwrap();
    let accounting = exporter.accounting();
    let emitter = BoundedAsyncEmitter::new(AsyncExportConfig::default(), exporter);
    emitter.on_task_lifecycle(&TaskLifecycleEvent::submitted(attempt(
        "orders", "inv", "worker", 0,
    )));
    let outer = emitter.shutdown(Duration::from_secs(2)).unwrap();
    assert_eq!(outer.accepted, 1);
    assert_eq!(outer.export_failed, 1);
    assert_eq!(outer.exported, 0);
    let stats = accounting.stats();
    assert_eq!(stats.logs.not_sent, 1);
    assert_eq!(stats.logs.attempted, 0);
    assert_eq!(stats.logs.failed, 0);
    assert_eq!(stats.failed_events, 0);
    assert!(receiver.requests().is_empty());
}
