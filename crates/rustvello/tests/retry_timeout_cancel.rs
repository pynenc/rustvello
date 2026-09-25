//! Retry backoff, execution deadlines and user cancellation (in-memory backends).
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
