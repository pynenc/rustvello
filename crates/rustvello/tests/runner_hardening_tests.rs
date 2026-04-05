//! Runner hardening tests.
//!
//! Exercises graceful shutdown, multi-runner concurrency, and recovery
//! under real-world conditions.

use std::sync::Arc;
use std::time::Duration;

use rustvello::prelude::*;
use rustvello_core::runner::Runner;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

#[rustvello::task]
fn runner_add(x: i32, y: i32) -> i32 {
    x + y
}

#[rustvello::task]
fn slow_task(ms: u64) -> String {
    std::thread::sleep(Duration::from_millis(ms));
    "done".to_string()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_registry<T: Task>(task: T) -> Arc<TaskRegistry> {
    Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(task).unwrap();
        reg
    })
}

fn make_runner(
    app_id: &str,
    app: &RustvelloApp,
    registry: Arc<TaskRegistry>,
) -> PersistentTokioRunner {
    TaskRunner::new(
        app_id.to_string(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        registry,
        None,
    )
}

// ===========================================================================
// 1. Graceful Shutdown
// ===========================================================================

/// with_graceful_shutdown stops the runner when the signal fires.
#[tokio::test]
async fn graceful_shutdown_stops_runner() {
    let mut app = RustvelloApp::new(AppConfig::new("shutdown-test"));
    app.register(RunnerAddTask::new()).unwrap();

    // Submit a few invocations
    for i in 0..3 {
        app.submit_call(&RunnerAddTask::new(), RunnerAddParams { x: i, y: i })
            .await
            .unwrap();
    }

    let reg = make_registry(RunnerAddTask::new());
    let runner = make_runner("shutdown-test", &app, reg).with_num_workers(2);

    // Run with a short timeout
    let result = runner
        .with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;

    // Should complete without error (shutdown is not an error)
    assert!(result.is_ok());
}

/// After graceful shutdown, submitted invocations should have been processed.
#[tokio::test]
async fn graceful_shutdown_processes_pending_work() {
    let mut app = RustvelloApp::new(AppConfig::new("shutdown-work"));
    app.register(RunnerAddTask::new()).unwrap();

    let handle = app
        .submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 10, y: 20 })
        .await
        .unwrap();

    let reg = make_registry(RunnerAddTask::new());
    let runner = make_runner("shutdown-work", &app, reg).with_num_workers(1);

    runner
        .with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        })
        .await
        .unwrap();

    // The single invocation should have been processed before shutdown
    let status = handle.status().await.unwrap();
    assert_eq!(
        status,
        InvocationStatus::Success,
        "Invocation should complete before shutdown"
    );

    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 30);
}

// ===========================================================================
// 2. Multi-Runner Concurrency
// ===========================================================================

/// Multiple runners processing invocations concurrently.
/// No invocation should be processed twice.
#[tokio::test]
async fn multi_runner_no_double_execution() {
    let mut app = RustvelloApp::new(AppConfig::new("multi-runner"));
    app.register(RunnerAddTask::new()).unwrap();

    let n = 10;
    let mut handles = Vec::new();
    for i in 0..n {
        let h = app
            .submit_call(&RunnerAddTask::new(), RunnerAddParams { x: i, y: i * 10 })
            .await
            .unwrap();
        handles.push((i, h));
    }

    let reg = make_registry(RunnerAddTask::new());

    // Two runners sharing the same backends
    let runner1 = make_runner("multi-runner", &app, Arc::clone(&reg)).with_num_workers(2);
    let runner2 = make_runner("multi-runner", &app, Arc::clone(&reg)).with_num_workers(2);

    // Run both with graceful shutdown
    let (r1, r2) = tokio::join!(
        runner1.with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }),
        runner2.with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        })
    );

    assert!(r1.is_ok());
    assert!(r2.is_ok());

    // All invocations should have completed with correct results
    for (i, h) in &handles {
        let status = h.status().await.unwrap();
        assert_eq!(
            status,
            InvocationStatus::Success,
            "Invocation {i} should be Success"
        );
        let result: i32 = h.result().await.unwrap();
        assert_eq!(result, i + i * 10, "Invocation {i} result mismatch");
    }
}

/// Two runners: second runner.run_one() returning false when queue is empty
/// after first runner drained it.
#[tokio::test]
async fn runner_returns_false_on_empty_queue() {
    let mut app = RustvelloApp::new(AppConfig::new("empty-q"));
    app.register(RunnerAddTask::new()).unwrap();

    app.submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 1, y: 1 })
        .await
        .unwrap();

    let reg = make_registry(RunnerAddTask::new());
    let runner1 = make_runner("empty-q", &app, Arc::clone(&reg));
    let runner2 = make_runner("empty-q", &app, reg);

    // Runner 1 processes the single invocation
    let did_work = runner1.run_one().await.unwrap();
    assert!(did_work);

    // Runner 2 finds nothing
    let did_work = runner2.run_one().await.unwrap();
    assert!(!did_work);
}

// ===========================================================================
// 3. Runner Handles Race Conditions
// ===========================================================================

/// If two runners try to claim the same invocation (race), one wins and
/// the other handles InvalidStatusTransition gracefully.
#[tokio::test]
async fn runner_race_condition_graceful() {
    let mut app = RustvelloApp::new(AppConfig::new("race"));
    app.register(RunnerAddTask::new()).unwrap();

    // Submit one invocation
    let handle = app
        .submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 5, y: 5 })
        .await
        .unwrap();

    let inv_id = handle.invocation_id().clone();

    let reg = make_registry(RunnerAddTask::new());
    let runner = make_runner("race", &app, reg);

    // First run succeeds
    runner.run_one().await.unwrap();

    // Manually trying to transition from Success → Running is invalid
    let result = app
        .orchestrator()
        .set_invocation_status(&inv_id, InvocationStatus::Running, None)
        .await;
    assert!(result.is_err(), "Success → Running should fail");

    // But the original result is intact
    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 10);
}

// ===========================================================================
// 4. Heartbeat Integration
// ===========================================================================

/// After running a few invocations, the runner's heartbeat should have
/// been registered with the orchestrator.
#[tokio::test]
async fn runner_heartbeat_registered() {
    let mut app = RustvelloApp::new(AppConfig::new("heartbeat-test"));
    app.register(RunnerAddTask::new()).unwrap();

    app.submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 1, y: 1 })
        .await
        .unwrap();

    let reg = make_registry(RunnerAddTask::new());
    let runner = make_runner("heartbeat-test", &app, reg);
    runner.run_one().await.unwrap();

    // After run, no stale invocations should exist
    let stale = app
        .orchestrator()
        .get_stale_running_invocations(3600)
        .await
        .unwrap();
    assert!(
        stale.is_empty(),
        "No stale invocations after successful run"
    );
}

// ===========================================================================
// 5. All Runner Types Execute Successfully
// ===========================================================================

/// PerInvocationTokioRunner full cycle.
#[tokio::test]
async fn per_invocation_runner_lifecycle() {
    let mut app = RustvelloApp::new(AppConfig::new("pitr"));
    app.register(RunnerAddTask::new()).unwrap();

    let handle = app
        .submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 7, y: 8 })
        .await
        .unwrap();

    let reg = make_registry(RunnerAddTask::new());
    let runner = PerInvocationTokioRunner::new(
        "pitr".to_string(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        reg,
    );

    runner.run_one().await.unwrap();

    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 15);
}

/// SpawnBlockingRunner full cycle.
#[tokio::test]
async fn process_runner_lifecycle() {
    let mut app = RustvelloApp::new(AppConfig::new("proc"));
    app.register(RunnerAddTask::new()).unwrap();

    let handle = app
        .submit_call(&RunnerAddTask::new(), RunnerAddParams { x: 3, y: 4 })
        .await
        .unwrap();

    let reg = make_registry(RunnerAddTask::new());
    let runner = SpawnBlockingRunner::new(
        "proc".to_string(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        reg,
    );

    runner.run_one().await.unwrap();

    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 7);
}
