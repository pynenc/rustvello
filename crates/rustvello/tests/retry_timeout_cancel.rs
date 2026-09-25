//! Retry backoff, execution deadlines and user cancellation (in-memory backends;
//! the native async abort tests also run on SQLite).
//!
//! The durable (process-kill) variant of the backoff test lives in
//! `durable_retry_kill.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello::app::RustvelloApp;
use rustvello::prelude::*;
use rustvello_core::task::TaskFn;

const TASK: (&str, &str) = ("m2", "task");

fn task_id() -> TaskId {
    TaskId::new(TASK.0, TASK.1)
}

/// A worker app and a client app sharing the same in-memory backends.
fn apps(config: TaskConfig, body: TaskFn) -> (RustvelloApp, RustvelloApp) {
    let mut app_config = AppConfig::new("m2-app");
    app_config.cancellation_check_interval_seconds = 0.05;
    let mut worker = RustvelloApp::new(app_config.clone());
    worker
        .register_task(task_id(), config.clone(), body)
        .unwrap();
    let mut client = RustvelloApp::with_backends(
        app_config,
        worker.broker(),
        worker.orchestrator(),
        worker.state_backend(),
        worker.client_data_store(),
    );
    client
        .register_task(task_id(), config, Arc::new(|_| Ok("null".into())))
        .unwrap();
    (worker, client)
}

fn fail_first(calls: &Arc<AtomicUsize>) -> TaskFn {
    let calls = Arc::clone(calls);
    Arc::new(move |_| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(RustvelloError::TaskExecution {
                error_type: "Flaky".into(),
                message: "first attempt fails".into(),
                traceback: None,
            })
        } else {
            Ok("\"done\"".into())
        }
    })
}

fn sleeping(calls: &Arc<AtomicUsize>, sleep: Duration) -> TaskFn {
    let calls = Arc::clone(calls);
    Arc::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(sleep);
        Ok("\"late\"".into())
    })
}

async fn status(app: &RustvelloApp, id: &InvocationId) -> InvocationStatus {
    app.get_status(id).await.unwrap()
}

#[tokio::test]
async fn retry_waits_for_its_backoff_delay_then_runs_once() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.max_retries = 3;
    config.retry_delay_ms = 400;
    config.retry_jitter = RetryJitter::None;
    let (worker, client) = apps(config, fail_first(&calls));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let runner = worker.into_runner();

    assert!(runner.run_one().await.unwrap());
    let failed_at = Instant::now();
    assert_eq!(status(&client, &id).await, InvocationStatus::Retry);
    // Not due yet: nothing to run, and the broker does not count it.
    assert!(!runner.run_one().await.unwrap());
    assert_eq!(client.broker().count_invocations(None).await.unwrap(), 0);

    while !runner.run_one().await.unwrap() {
        assert!(
            failed_at.elapsed() < Duration::from_secs(5),
            "retry never fired"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        failed_at.elapsed() >= Duration::from_millis(390),
        "retry fired after {:?}",
        failed_at.elapsed()
    );
    assert_eq!(status(&client, &id).await, InvocationStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(!runner.run_one().await.unwrap());
}

#[tokio::test]
async fn zero_delay_keeps_immediate_retries() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.max_retries = 1;
    let (worker, client) = apps(config, fail_first(&calls));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let runner = worker.into_runner();
    assert!(runner.run_one().await.unwrap());
    assert!(runner.run_one().await.unwrap());
    assert_eq!(status(&client, &id).await, InvocationStatus::Success);
}

#[tokio::test]
async fn blocking_task_past_its_deadline_fails_with_timeout_error() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.blocking = true;
    config.timeout_ms = Some(150);
    let (worker, client) = apps(config, sleeping(&calls, Duration::from_millis(1_500)));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let runner = worker.into_runner();
    let started = Instant::now();
    assert!(runner.run_one().await.unwrap());
    assert!(
        started.elapsed() < Duration::from_millis(1_200),
        "runner waited for the abandoned body: {:?}",
        started.elapsed()
    );
    assert_eq!(status(&client, &id).await, InvocationStatus::Failed);
    let error = client
        .state_backend()
        .get_error(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error.error_type, TASK_TIMEOUT_ERROR);
    // The abandoned body finishes later; its result is discarded.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(status(&client, &id).await, InvocationStatus::Failed);
    assert_eq!(client.get_result(&id).await.unwrap(), None);
}

#[tokio::test]
async fn timeout_retries_per_policy_unless_disabled() {
    for (retry_on_timeout, expected_calls, final_status) in [
        (true, 2, InvocationStatus::Failed),
        (false, 1, InvocationStatus::Failed),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut config = TaskConfig::default();
        config.blocking = true;
        config.max_retries = 1;
        config.timeout_ms = Some(50);
        config.retry_on_timeout = retry_on_timeout;
        let (worker, client) = apps(config, sleeping(&calls, Duration::from_millis(300)));
        let id = client
            .submit(&task_id(), SerializedArguments::new())
            .await
            .unwrap();
        let runner = worker.into_runner();
        while runner.run_one().await.unwrap() {}
        assert_eq!(status(&client, &id).await, final_status);
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    }
}

#[tokio::test]
async fn inline_sync_task_result_past_deadline_is_discarded() {
    // Non-blocking sync bodies cannot be preempted; the deadline is checked
    // when they return.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.timeout_ms = Some(20);
    let (worker, client) = apps(config, sleeping(&calls, Duration::from_millis(100)));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    assert!(worker.into_runner().run_one().await.unwrap());
    assert_eq!(status(&client, &id).await, InvocationStatus::Failed);
    assert_eq!(client.get_result(&id).await.unwrap(), None);
}

#[tokio::test]
async fn cancelled_queued_invocation_never_runs() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (worker, client) = apps(TaskConfig::default(), fail_first(&calls));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    assert_eq!(client.cancel(&id).await.unwrap(), CancelOutcome::Cancelled);
    assert_eq!(
        client.cancel(&id).await.unwrap(),
        CancelOutcome::AlreadyFinal(InvocationStatus::Cancelled)
    );
    let runner = worker.into_runner();
    while runner.run_one().await.unwrap() {}
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(status(&client, &id).await, InvocationStatus::Cancelled);
    let history = client.state_backend().get_history(&id).await.unwrap();
    assert_eq!(
        history.last().unwrap().status_record.status,
        InvocationStatus::Cancelled
    );
}

#[tokio::test]
async fn cancel_during_backoff_prevents_the_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.max_retries = 2;
    config.retry_delay_ms = 200;
    config.retry_jitter = RetryJitter::None;
    let (worker, client) = apps(config, fail_first(&calls));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let runner = worker.into_runner();
    assert!(runner.run_one().await.unwrap());
    assert_eq!(status(&client, &id).await, InvocationStatus::Retry);
    assert_eq!(client.cancel(&id).await.unwrap(), CancelOutcome::Cancelled);
    tokio::time::sleep(Duration::from_millis(300)).await;
    while runner.run_one().await.unwrap() {}
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(status(&client, &id).await, InvocationStatus::Cancelled);
}

#[tokio::test]
async fn cancelling_a_running_invocation_abandons_the_attempt_and_discards_its_result() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.blocking = true;
    let (worker, client) = apps(config, sleeping(&calls, Duration::from_millis(1_000)));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let runner = Arc::new(worker.into_runner());
    let running = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move {
            let started = Instant::now();
            runner.run_one().await.unwrap();
            started.elapsed()
        }
    });
    while status(&client, &id).await != InvocationStatus::Running {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(client.cancel(&id).await.unwrap(), CancelOutcome::Cancelled);
    let elapsed = running.await.unwrap();
    assert!(
        elapsed < Duration::from_millis(800),
        "worker did not stop at the cancellation check: {elapsed:?}"
    );
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(status(&client, &id).await, InvocationStatus::Cancelled);
    assert_eq!(client.get_result(&id).await.unwrap(), None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancel_of_finished_invocation_changes_nothing() {
    let (worker, client) = apps(TaskConfig::default(), Arc::new(|_| Ok("1".into())));
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    assert!(worker.into_runner().run_one().await.unwrap());
    assert_eq!(
        client.cancel(&id).await.unwrap(),
        CancelOutcome::AlreadyFinal(InvocationStatus::Success)
    );
    assert_eq!(client.get_result(&id).await.unwrap().as_deref(), Some("1"));
}

// ---------------------------------------------------------------------------
// Native async bodies: a deadline or a cancel aborts the body itself
// ---------------------------------------------------------------------------

/// Counts how many body futures were dropped (aborted or finished).
struct CountOnDrop(&'static AtomicUsize);

impl Drop for CountOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Probes of one async ticker task: starts, dropped bodies and side effects.
struct Probes {
    starts: AtomicUsize,
    drops: AtomicUsize,
    ticks: AtomicUsize,
}

impl Probes {
    const fn new() -> Self {
        Self {
            starts: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            ticks: AtomicUsize::new(0),
        }
    }

    /// Keep producing a side effect every 10 ms for 10 s unless aborted.
    async fn tick_forever(&'static self) -> u32 {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let _guard = CountOnDrop(&self.drops);
        for _ in 0..1_000 {
            self.ticks.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        0
    }

    /// Wait until `drops` reaches `expected`, then check no body still ticks.
    async fn assert_aborted(&self, expected: usize) {
        let waited = Instant::now();
        while self.drops.load(Ordering::SeqCst) < expected {
            assert!(
                waited.elapsed() < Duration::from_secs(2),
                "async body was not aborted"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(self.drops.load(Ordering::SeqCst), expected);
        let ticks = self.ticks.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            self.ticks.load(Ordering::SeqCst),
            ticks,
            "an abandoned async body kept producing side effects"
        );
    }
}

static MEM_TIMEOUT: Probes = Probes::new();
static MEM_CANCEL: Probes = Probes::new();
#[cfg(feature = "sqlite")]
static SQLITE_TIMEOUT: Probes = Probes::new();
#[cfg(feature = "sqlite")]
static SQLITE_CANCEL: Probes = Probes::new();

#[rustvello::task(module = "m2_async", timeout_ms = 100, max_retries = 1)]
async fn mem_timeout_ticker() -> u32 {
    MEM_TIMEOUT.tick_forever().await
}

#[rustvello::task(module = "m2_async")]
async fn mem_cancel_ticker() -> u32 {
    MEM_CANCEL.tick_forever().await
}

#[cfg(feature = "sqlite")]
#[rustvello::task(module = "m2_async", timeout_ms = 100, max_retries = 1)]
async fn sqlite_timeout_ticker() -> u32 {
    SQLITE_TIMEOUT.tick_forever().await
}

#[cfg(feature = "sqlite")]
#[rustvello::task(module = "m2_async")]
async fn sqlite_cancel_ticker() -> u32 {
    SQLITE_CANCEL.tick_forever().await
}

/// A worker app and a client app sharing in-memory backends, both with `make()`.
fn typed_mem_apps<T: Task>(make: fn() -> T) -> (RustvelloApp, RustvelloApp) {
    let mut app_config = AppConfig::new("m2-async-app");
    app_config.cancellation_check_interval_seconds = 0.05;
    let mut worker = RustvelloApp::new(app_config.clone());
    worker.register(make()).unwrap();
    let mut client = RustvelloApp::with_backends(
        app_config,
        worker.broker(),
        worker.orchestrator(),
        worker.state_backend(),
        worker.client_data_store(),
    );
    client.register(make()).unwrap();
    (worker, client)
}

/// A worker app and a client app on one SQLite database, both with `make()`.
#[cfg(feature = "sqlite")]
async fn typed_sqlite_apps<T: Task>(
    make: fn() -> T,
    dir: &tempfile::TempDir,
) -> (RustvelloApp, RustvelloApp) {
    let path = dir.path().join("m2-async.db");
    let path = path.to_str().unwrap();
    let mut apps = Vec::new();
    for _ in 0..2 {
        let mut app = Rustvello::builder()
            .app_id("m2-async-sqlite")
            .sqlite(path, "m2-async-sqlite")
            .build()
            .await
            .unwrap();
        app.config.cancellation_check_interval_seconds = 0.05;
        app.register(make()).unwrap();
        apps.push(app);
    }
    let client = apps.pop().unwrap();
    (apps.pop().unwrap(), client)
}

/// A timed-out async attempt fails with `TaskTimeoutError`, its body is
/// aborted (no side effect after the deadline), and the retry policy applies:
/// the retry times out and is aborted too.
async fn async_timeout_aborts_body_and_retries<T>(
    worker: RustvelloApp,
    client: RustvelloApp,
    task: T,
    probes: &'static Probes,
) where
    T: Task<Params = ()>,
{
    let id = client
        .submit_call(&task, ())
        .await
        .unwrap()
        .invocation_id()
        .clone();
    let runner = worker.into_runner();
    let started = Instant::now();
    while runner.run_one().await.unwrap() {}
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "runner waited for the async bodies: {:?}",
        started.elapsed()
    );
    assert_eq!(status(&client, &id).await, InvocationStatus::Failed);
    let error = client
        .state_backend()
        .get_error(&id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error.error_type, TASK_TIMEOUT_ERROR);
    assert_eq!(probes.starts.load(Ordering::SeqCst), 2, "one retry");
    probes.assert_aborted(2).await;
    assert_eq!(client.get_result(&id).await.unwrap(), None);
}

/// Cancelling a running async invocation aborts its body at the next await.
async fn async_cancel_aborts_running_body<T>(
    worker: RustvelloApp,
    client: RustvelloApp,
    task: T,
    probes: &'static Probes,
) where
    T: Task<Params = ()>,
{
    let id = client
        .submit_call(&task, ())
        .await
        .unwrap()
        .invocation_id()
        .clone();
    let runner = Arc::new(worker.into_runner());
    let running = tokio::spawn({
        let runner = Arc::clone(&runner);
        async move {
            let started = Instant::now();
            runner.run_one().await.unwrap();
            started.elapsed()
        }
    });
    while probes.ticks.load(Ordering::SeqCst) < 3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(status(&client, &id).await, InvocationStatus::Running);
    assert_eq!(client.cancel(&id).await.unwrap(), CancelOutcome::Cancelled);
    let elapsed = running.await.unwrap();
    assert!(
        elapsed < Duration::from_secs(2),
        "worker did not stop at the cancellation check: {elapsed:?}"
    );
    probes.assert_aborted(1).await;
    assert_eq!(status(&client, &id).await, InvocationStatus::Cancelled);
    assert_eq!(client.get_result(&id).await.unwrap(), None);
    assert_eq!(probes.starts.load(Ordering::SeqCst), 1);
    assert!(!runner.run_one().await.unwrap());
}

#[tokio::test]
async fn async_body_past_its_deadline_is_aborted_and_retried() {
    let (worker, client) = typed_mem_apps(MemTimeoutTickerTask::new);
    let task = MemTimeoutTickerTask::new();
    async_timeout_aborts_body_and_retries(worker, client, task, &MEM_TIMEOUT).await;
}

#[tokio::test]
async fn cancelling_a_running_async_invocation_aborts_its_body() {
    let (worker, client) = typed_mem_apps(MemCancelTickerTask::new);
    let task = MemCancelTickerTask::new();
    async_cancel_aborts_running_body(worker, client, task, &MEM_CANCEL).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_async_body_past_its_deadline_is_aborted_and_retried() {
    let dir = tempfile::tempdir().unwrap();
    let (worker, client) = typed_sqlite_apps(SqliteTimeoutTickerTask::new, &dir).await;
    let task = SqliteTimeoutTickerTask::new();
    async_timeout_aborts_body_and_retries(worker, client, task, &SQLITE_TIMEOUT).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_cancelling_a_running_async_invocation_aborts_its_body() {
    let dir = tempfile::tempdir().unwrap();
    let (worker, client) = typed_sqlite_apps(SqliteCancelTickerTask::new, &dir).await;
    let task = SqliteCancelTickerTask::new();
    async_cancel_aborts_running_body(worker, client, task, &SQLITE_CANCEL).await;
}

/// A blocking sync body cannot be preempted, but it can watch the attempt
/// signal and stop by itself once the runner abandons the attempt.
#[tokio::test]
async fn blocking_body_can_stop_cooperatively_on_its_attempt_signal() {
    let stopped = Arc::new(AtomicUsize::new(0));
    let mut config = TaskConfig::default();
    config.blocking = true;
    config.timeout_ms = Some(100);
    let body: TaskFn = Arc::new({
        let stopped = Arc::clone(&stopped);
        move |_| {
            let signal = current_attempt_signal().expect("runner attempt signal");
            let started = Instant::now();
            while !signal.is_abandoned() {
                assert!(started.elapsed() < Duration::from_secs(5));
                std::thread::sleep(Duration::from_millis(5));
            }
            stopped.fetch_add(1, Ordering::SeqCst);
            Ok("\"stopped\"".into())
        }
    });
    let (worker, client) = apps(config, body);
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    assert!(worker.into_runner().run_one().await.unwrap());
    let waited = Instant::now();
    while stopped.load(Ordering::SeqCst) == 0 {
        assert!(
            waited.elapsed() < Duration::from_secs(2),
            "body never saw the abandon signal"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(status(&client, &id).await, InvocationStatus::Failed);
    assert_eq!(client.get_result(&id).await.unwrap(), None);
}
