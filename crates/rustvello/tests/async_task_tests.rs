//! Native async tasks: `#[rustvello::task]` on an `async fn`.
//!
//! These tests exercise real async I/O (a local TCP echo server), concurrency
//! without blocking threads, context propagation across `.await` points,
//! retries, errors, panics, workflows, dev mode and cancellation safety.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use rustvello::prelude::*;
use rustvello::runner::ShutdownOutcome;
use rustvello_core::context::get_invocation_context;
use rustvello_core::observability::capture_w3c_trace_context;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const POLL: Duration = Duration::from_millis(10);

// ---------------------------------------------------------------------------
// A local TCP echo server
// ---------------------------------------------------------------------------

/// Start a line-based TCP echo server on the current runtime.
async fn echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if write
                        .write_all(format!("{line}\n").as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
    });
    addr
}

async fn echo_once(addr: &str, message: &str) -> RustvelloResult<String> {
    let io = |error: std::io::Error| RustvelloError::runner_err(format!("echo I/O: {error}"));
    let stream = TcpStream::connect(addr).await.map_err(io)?;
    let (read, mut write) = stream.into_split();
    write
        .write_all(format!("{message}\n").as_bytes())
        .await
        .map_err(io)?;
    let mut line = String::new();
    BufReader::new(read)
        .read_line(&mut line)
        .await
        .map_err(io)?;
    Ok(line.trim_end().to_owned())
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

/// Real network I/O: one round trip through the echo server.
#[rustvello::task(module = "async_tests")]
async fn tcp_echo(addr: String, message: String) -> RustvelloResult<String> {
    echo_once(&addr, &message).await
}

/// No parameters, infallible return type.
#[rustvello::task(module = "async_tests")]
async fn async_ping() -> String {
    tokio::task::yield_now().await;
    "pong".to_owned()
}

static ACTIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static RUNTIME_THREADS: Mutex<Vec<std::thread::ThreadId>> = Mutex::new(Vec::new());

/// Sleeps while counting how many bodies are in flight at once.
#[rustvello::task(module = "async_tests")]
async fn concurrent_sleep(ms: u64) -> u64 {
    let now = ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
    PEAK.fetch_max(now, Ordering::SeqCst);
    RUNTIME_THREADS
        .lock()
        .unwrap()
        .push(std::thread::current().id());
    tokio::time::sleep(Duration::from_millis(ms)).await;
    ACTIVE.fetch_sub(1, Ordering::SeqCst);
    ms
}

/// Fails on the first attempt, succeeds on the retry.
#[rustvello::task(module = "async_tests", max_retries = 2)]
async fn flaky_async(addr: String) -> RustvelloResult<u32> {
    let retries = get_invocation_context().unwrap().num_retries;
    // Await real I/O before deciding, so the retry decision follows an await.
    echo_once(&addr, "attempt").await?;
    if retries == 0 {
        return Err(RustvelloError::TaskExecution {
            error_type: "TransientNetworkError".into(),
            message: "first attempt fails".into(),
            traceback: None,
        });
    }
    Ok(retries)
}

#[rustvello::task(module = "async_tests")]
async fn always_fails() -> RustvelloResult<()> {
    tokio::task::yield_now().await;
    Err(RustvelloError::TaskExecution {
        error_type: "UpstreamUnavailable".into(),
        message: "no upstream".into(),
        traceback: None,
    })
}

#[rustvello::task(module = "async_tests")]
async fn panics_after_await() -> u32 {
    tokio::task::yield_now().await;
    panic!("async body panicked");
}

/// The application a parent task submits children through.
static SUBMITTER: OnceLock<Arc<RustvelloApp>> = OnceLock::new();

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
struct ContextReport {
    before: String,
    after: String,
    trace_matches: bool,
    child_id: String,
    child_result: String,
}

/// Checks context identity across awaits, then submits and awaits a child.
#[rustvello::task(module = "async_tests")]
async fn context_parent(addr: String) -> RustvelloResult<ContextReport> {
    let before = get_invocation_context().unwrap();
    // Real I/O plus a timer: the body is suspended and may resume elsewhere.
    echo_once(&addr, "hop").await?;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let after = get_invocation_context().unwrap();
    let trace_matches = capture_w3c_trace_context() == after.trace_context;
    let app = Arc::clone(SUBMITTER.get().expect("submitter set"));
    let child = app
        .submit_call(
            &TcpEchoTask::new(),
            TcpEchoParams {
                addr,
                message: "child".into(),
            },
        )
        .await?;
    let child_result = child.wait(POLL).await?;
    Ok(ContextReport {
        before: before.invocation_id.to_string(),
        after: after.invocation_id.to_string(),
        trace_matches,
        child_id: child.invocation_id().to_string(),
        child_result,
    })
}

/// An async workflow root using the async deterministic helpers.
#[rustvello::workflow(module = "async_tests")]
async fn async_workflow(label: String) -> RustvelloResult<String> {
    let mut root = WorkflowRoot::current()?;
    let first = root.uuid_async().await?;
    tokio::task::yield_now().await;
    let second = root.uuid_async().await?;
    Ok(format!("{label}:{}", first != second))
}

static HANG_ARMED: AtomicBool = AtomicBool::new(true);
static HANG_DROPPED: AtomicBool = AtomicBool::new(false);
static HANG_STARTED: OnceLock<tokio::sync::Notify> = OnceLock::new();

struct SetOnDrop(&'static AtomicBool);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// The first execution never finishes; later executions succeed.
#[rustvello::task(module = "async_tests")]
async fn hangs_once() -> String {
    if HANG_ARMED.swap(false, Ordering::SeqCst) {
        let _guard = SetOnDrop(&HANG_DROPPED);
        HANG_STARTED.get_or_init(Default::default).notify_one();
        std::future::pending::<()>().await;
    }
    "recovered".to_owned()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn app(app_id: &str) -> RustvelloApp {
    let mut app = RustvelloApp::new(AppConfig::new(app_id));
    app.register(TcpEchoTask::new()).unwrap();
    app.register(AsyncPingTask::new()).unwrap();
    app.register(ConcurrentSleepTask::new()).unwrap();
    app.register(FlakyAsyncTask::new()).unwrap();
    app.register(AlwaysFailsTask::new()).unwrap();
    app.register(PanicsAfterAwaitTask::new()).unwrap();
    app.register(ContextParentTask::new()).unwrap();
    app.register(AsyncWorkflowTask::new()).unwrap();
    app.register(HangsOnceTask::new()).unwrap();
    app
}

/// Run a persistent runner over `app`'s backends until `done` resolves.
async fn run_until<F: std::future::Future<Output = ()> + Send>(
    app: &RustvelloApp,
    num_workers: usize,
    done: F,
) {
    let runner = PersistentTokioRunner::new(
        app.config.app_id.clone(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
        None,
    )
    .with_num_workers(num_workers)
    .with_idle_sleep(5);
    let outcome = tokio::time::timeout(
        Duration::from_secs(20),
        runner.with_bounded_shutdown(done, Duration::from_secs(5)),
    )
    .await
    .expect("runner finished in time")
    .unwrap();
    assert_eq!(outcome, ShutdownOutcome::Drained);
}

async fn wait_terminal(app: &RustvelloApp, ids: &[InvocationId]) {
    for id in ids {
        while !app.get_status(id).await.unwrap().is_terminal() {
            tokio::time::sleep(POLL).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Macro output
// ---------------------------------------------------------------------------

#[test]
fn macro_marks_async_tasks() {
    let echo = TcpEchoTask::new();
    assert!(Task::is_async(&echo));
    assert!(!Task::config(&echo).blocking);
    assert_eq!(
        Task::task_id(&echo).to_string(),
        "rust::async_tests.tcp_echo"
    );

    let workflow = AsyncWorkflowTask::new();
    assert!(Task::is_async(&workflow));
    let config = Task::config(&workflow);
    assert!(config.is_workflow_task);
    assert!(
        !config.blocking,
        "async workflow bodies await children on the runtime"
    );

    let registry = {
        let mut registry = TaskRegistry::new();
        registry.register_typed(AsyncPingTask::new()).unwrap();
        registry
    };
    let dyn_task = registry
        .get_dyn(Task::task_id(&AsyncPingTask::new()))
        .unwrap();
    assert!(dyn_task.is_async());
}

// ---------------------------------------------------------------------------
// Synchronous entry points still work
// ---------------------------------------------------------------------------

#[test]
fn run_outside_any_runtime() {
    assert_eq!(Task::run(&AsyncPingTask::new(), ()).unwrap(), "pong");
}

#[tokio::test(flavor = "current_thread")]
async fn run_inside_current_thread_runtime() {
    assert_eq!(Task::run(&AsyncPingTask::new(), ()).unwrap(), "pong");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_sync_inside_multi_thread_runtime() {
    let addr = echo_server().await;
    let app = app("async-exec-sync");
    let echoed = app
        .execute_sync(
            &TcpEchoTask::new(),
            TcpEchoParams {
                addr,
                message: "inline".into(),
            },
        )
        .unwrap();
    assert_eq!(echoed, "inline");
}

#[tokio::test]
async fn dev_mode_call_awaits_async_body() {
    let addr = echo_server().await;
    let mut config = AppConfig::new("async-dev-mode");
    config.dev_mode_force_sync = true;
    let mut app = RustvelloApp::with_backends(
        config,
        Arc::new(rustvello::mem::broker::MemBroker::new()),
        Arc::new(rustvello::mem::orchestrator::MemOrchestrator::new()),
        Arc::new(rustvello::mem::state_backend::MemStateBackend::new()),
        Arc::new(ClientDataStoreManager::new(
            Arc::new(rustvello::mem::client_data_store::MemClientDataStore::new()),
            ClientDataStoreConfig::default(),
        )),
    );
    app.register(TcpEchoTask::new()).unwrap();
    let invocation = app
        .call(
            &TcpEchoTask::new(),
            TcpEchoParams {
                addr,
                message: "dev".into(),
            },
        )
        .await
        .unwrap();
    assert!(invocation.is_sync());
    assert_eq!(invocation.result().await.unwrap(), "dev");
}

// ---------------------------------------------------------------------------
// Runner execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn run_one_executes_async_io_task() {
    let addr = echo_server().await;
    let app = app("async-run-one");
    let handle = app
        .submit_call(
            &TcpEchoTask::new(),
            TcpEchoParams {
                addr,
                message: "hello over tcp".into(),
            },
        )
        .await
        .unwrap();
    let runner = PersistentTokioRunner::new(
        "async-run-one".into(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
        None,
    );
    runner.run_one().await.unwrap();
    assert_eq!(handle.result().await.unwrap(), "hello over tcp");
}

/// On a single-threaded runtime every body runs on the runtime thread (no
/// blocking pool), the bodies overlap, and the worker count caps them.
#[tokio::test(flavor = "current_thread")]
async fn async_bodies_overlap_on_runtime_without_blocking_threads() {
    ACTIVE.store(0, Ordering::SeqCst);
    PEAK.store(0, Ordering::SeqCst);
    RUNTIME_THREADS.lock().unwrap().clear();
    let app = app("async-concurrency");
    let mut ids = Vec::new();
    for _ in 0..12 {
        ids.push(
            app.submit_call(
                &ConcurrentSleepTask::new(),
                ConcurrentSleepParams { ms: 150 },
            )
            .await
            .unwrap()
            .invocation_id()
            .clone(),
        );
    }
    let start = Instant::now();
    run_until(&app, 4, wait_terminal(&app, &ids)).await;
    let elapsed = start.elapsed();

    for id in &ids {
        assert_eq!(
            app.get_status(id).await.unwrap(),
            InvocationStatus::Success,
            "{id}"
        );
    }
    assert_eq!(
        PEAK.load(Ordering::SeqCst),
        4,
        "worker count bounds async bodies"
    );
    // 12 bodies of 150ms on 4 workers need ~3 rounds; serial would take 1.8s.
    assert!(elapsed < Duration::from_millis(1500), "took {elapsed:?}");
    let test_thread = std::thread::current().id();
    assert!(RUNTIME_THREADS
        .lock()
        .unwrap()
        .iter()
        .all(|thread| *thread == test_thread));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_retry_then_success() {
    let addr = echo_server().await;
    let app = app("async-retry");
    let handle = app
        .submit_call(&FlakyAsyncTask::new(), FlakyAsyncParams { addr })
        .await
        .unwrap();
    let id = handle.invocation_id().clone();
    run_until(&app, 1, wait_terminal(&app, std::slice::from_ref(&id))).await;
    assert_eq!(
        handle.result().await.unwrap(),
        1,
        "succeeded on first retry"
    );
    let history = app.state_backend().get_history(&id).await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Retry)
            .count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_error_and_panic_reach_failed() {
    let app = app("async-errors");
    let failed = app
        .submit_call(&AlwaysFailsTask::new(), ())
        .await
        .unwrap()
        .invocation_id()
        .clone();
    let panicked = app
        .submit_call(&PanicsAfterAwaitTask::new(), ())
        .await
        .unwrap()
        .invocation_id()
        .clone();
    let ids = [failed.clone(), panicked.clone()];
    run_until(&app, 2, wait_terminal(&app, &ids)).await;

    let state = app.state_backend();
    assert_eq!(
        app.get_status(&failed).await.unwrap(),
        InvocationStatus::Failed
    );
    let error = state.get_error(&failed).await.unwrap().unwrap();
    assert_eq!(error.error_type, "UpstreamUnavailable");
    assert_eq!(error.message, "no upstream");

    assert_eq!(
        app.get_status(&panicked).await.unwrap(),
        InvocationStatus::Failed
    );
    let error = state.get_error(&panicked).await.unwrap().unwrap();
    assert!(error.message.contains("async body panicked"), "{error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn context_survives_await_and_links_children() {
    let addr = echo_server().await;
    let app = Arc::new(app("async-context"));
    SUBMITTER.set(Arc::clone(&app)).ok();
    let parent = app
        .submit_call(&ContextParentTask::new(), ContextParentParams { addr })
        .await
        .unwrap();
    let parent_id = parent.invocation_id().clone();
    run_until(
        &app,
        2,
        wait_terminal(&app, std::slice::from_ref(&parent_id)),
    )
    .await;

    let report = parent.result().await.unwrap();
    assert_eq!(report.before, parent_id.to_string());
    assert_eq!(report.after, parent_id.to_string());
    assert!(report.trace_matches, "W3C context attached across awaits");
    assert_eq!(report.child_result, "child");

    let child = app
        .state_backend()
        .get_invocation(&InvocationId::from_string(report.child_id))
        .await
        .unwrap();
    assert_eq!(child.parent_invocation_id, Some(parent_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_workflow_root_uses_async_replay_helpers() {
    let app = app("async-workflow");
    let handle = app
        .submit_call(
            &AsyncWorkflowTask::new(),
            AsyncWorkflowParams { label: "wf".into() },
        )
        .await
        .unwrap();
    let id = handle.invocation_id().clone();
    let workflow = app
        .state_backend()
        .get_invocation(&id)
        .await
        .unwrap()
        .workflow
        .unwrap();
    assert_eq!(workflow.workflow_id, id);
    run_until(&app, 1, wait_terminal(&app, std::slice::from_ref(&id))).await;
    assert_eq!(handle.result().await.unwrap(), "wf:true");
}

#[cfg(feature = "rayon")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rayon_runner_awaits_async_tasks_on_runtime() {
    let addr = echo_server().await;
    let app = app("async-rayon");
    let handle = app
        .submit_call(
            &TcpEchoTask::new(),
            TcpEchoParams {
                addr,
                message: "rayon".into(),
            },
        )
        .await
        .unwrap();
    let runner = RayonRunner::new(
        "async-rayon".into(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
    )
    .unwrap();
    runner.run_one().await.unwrap();
    assert_eq!(handle.result().await.unwrap(), "rayon");
}

// ---------------------------------------------------------------------------
// Cancellation safety
// ---------------------------------------------------------------------------

/// Dropping the runner mid-await aborts the body (no detached work), and the
/// invocation it owned is recovered and completed by another runner instead
/// of staying `Running` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_async_body_is_aborted_and_invocation_recovered() {
    HANG_ARMED.store(true, Ordering::SeqCst);
    HANG_DROPPED.store(false, Ordering::SeqCst);
    let started = HANG_STARTED.get_or_init(Default::default);
    let mut app = app("async-cancel");
    app.config.heartbeat_interval_seconds = 1;
    app.config.runner_dead_after_seconds = 2;
    app.config.recovery_check_interval_seconds = 1;
    let handle = app.submit_call(&HangsOnceTask::new(), ()).await.unwrap();
    let id = handle.invocation_id().clone();

    let first = PersistentTokioRunner::new(
        app.config.app_id.clone(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
        None,
    )
    .with_num_workers(1);
    let outcome = first
        .with_bounded_shutdown(started.notified(), Duration::from_millis(50))
        .await
        .unwrap();
    assert_eq!(outcome, ShutdownOutcome::DeadlineElapsed);
    // Abort is delivered on the runtime; give it a moment to drop the body.
    for _ in 0..100 {
        if HANG_DROPPED.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    assert!(HANG_DROPPED.load(Ordering::SeqCst), "body aborted on drop");
    assert_eq!(
        app.get_status(&id).await.unwrap(),
        InvocationStatus::Running
    );

    run_until(&app, 1, async {
        loop {
            if app.get_status(&id).await.unwrap() == InvocationStatus::Success {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await;
    assert_eq!(handle.result().await.unwrap(), "recovered");
}
