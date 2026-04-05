//! App-level integration tests.
//!
//! Covers: app ID isolation, blocking/waiter control, concurrency-control
//! types at the integration level, concurrent set_pending atomicity,
//! recovery from stale invocations, and large argument handling.

use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::runner::Runner;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

#[rustvello::task]
fn int_add(x: i32, y: i32) -> i32 {
    x + y
}

#[rustvello::task(max_retries = 2)]
fn int_fail(_n: i32) -> i32 {
    panic!("int_fail: boom");
}

#[rustvello::task]
fn echo_big(data: String) -> String {
    data
}

// ---------------------------------------------------------------------------
// Helper: create a runner bound to an app's backends
// ---------------------------------------------------------------------------

fn make_registry<T: Task>(task: T) -> Arc<TaskRegistry> {
    Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(task).unwrap();
        reg
    })
}

fn make_runner_for(app: &RustvelloApp, registry: Arc<TaskRegistry>) -> PersistentTokioRunner {
    TaskRunner::new(
        app.config.app_id.clone(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        registry,
        None,
    )
}

// ===========================================================================
// 1. App ID Isolation
// ===========================================================================

/// Two apps sharing the same in-memory backends but with different app IDs
/// should be able to register the same task and submit invocations
/// independently.
#[tokio::test]
async fn app_id_isolation_independent_execution() {
    let mut app_a = RustvelloApp::new(AppConfig::new("app-a"));
    let mut app_b = RustvelloApp::new(AppConfig::new("app-b"));
    app_a.register(IntAddTask::new()).unwrap();
    app_b.register(IntAddTask::new()).unwrap();

    let handle_a = app_a
        .submit_call(&IntAddTask::new(), IntAddParams { x: 1, y: 2 })
        .await
        .unwrap();
    let handle_b = app_b
        .submit_call(&IntAddTask::new(), IntAddParams { x: 10, y: 20 })
        .await
        .unwrap();

    // Run each app's invocation
    let reg = make_registry(IntAddTask::new());
    let runner_a = make_runner_for(&app_a, Arc::clone(&reg));
    let runner_b = make_runner_for(&app_b, Arc::clone(&reg));

    runner_a.run_one().await.unwrap();
    runner_b.run_one().await.unwrap();

    assert_eq!(handle_a.result().await.unwrap(), 3);
    assert_eq!(handle_b.result().await.unwrap(), 30);
}

/// Purging one app should not affect another app's data.
#[tokio::test]
async fn app_id_isolation_purge_does_not_affect_sibling() {
    let mut app_a = RustvelloApp::new(AppConfig::new("purge-a"));
    let mut app_b = RustvelloApp::new(AppConfig::new("purge-b"));
    app_a.register(IntAddTask::new()).unwrap();
    app_b.register(IntAddTask::new()).unwrap();

    let _handle_a = app_a
        .submit_call(&IntAddTask::new(), IntAddParams { x: 1, y: 1 })
        .await
        .unwrap();
    let handle_b = app_b
        .submit_call(&IntAddTask::new(), IntAddParams { x: 5, y: 5 })
        .await
        .unwrap();

    // Run both
    let reg = make_registry(IntAddTask::new());
    make_runner_for(&app_a, Arc::clone(&reg))
        .run_one()
        .await
        .unwrap();
    make_runner_for(&app_b, Arc::clone(&reg))
        .run_one()
        .await
        .unwrap();

    // Purge app_a
    app_a.purge().await.unwrap();

    // app_b's result should still be reachable
    let result: i32 = handle_b.result().await.unwrap();
    assert_eq!(result, 10);
}

// ===========================================================================
// 2. Waiter / Blocking Control
// ===========================================================================

/// Test set_waiting_for / get_waiters / release_waiters.
#[tokio::test]
async fn waiter_set_and_release() {
    let orch = rustvello_mem::orchestrator::MemOrchestrator::new();

    // Create two invocations
    let task_id = TaskId::new("mod", "waiter_task");
    let call1 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let call2 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv1 = orch.register_invocation(&call1).await.unwrap();
    let inv2 = orch.register_invocation(&call2).await.unwrap();

    // inv1 waits on inv2
    orch.set_waiting_for(&inv1, &inv2).await.unwrap();

    let waiters = orch.get_waiters(&inv2).await.unwrap();
    assert_eq!(waiters.len(), 1);
    assert_eq!(waiters[0], inv1);

    // Release
    let released = orch.release_waiters(&inv2).await.unwrap();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0], inv1);

    // After release, no more waiters
    let waiters = orch.get_waiters(&inv2).await.unwrap();
    assert!(waiters.is_empty());
}

/// Test get_blocking_invocations returns invocations that have waiters.
#[tokio::test]
async fn waiter_get_blocking_invocations() {
    let orch = rustvello_mem::orchestrator::MemOrchestrator::new();

    let task_id = TaskId::new("mod", "blocking_task");
    let call1 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let call2 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let call3 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv1 = orch.register_invocation(&call1).await.unwrap();
    let inv2 = orch.register_invocation(&call2).await.unwrap();
    let inv3 = orch.register_invocation(&call3).await.unwrap();

    // inv1 and inv3 wait on inv2
    orch.set_waiting_for(&inv1, &inv2).await.unwrap();
    orch.set_waiting_for(&inv3, &inv2).await.unwrap();

    let blocking = orch.get_blocking_invocations(10).await.unwrap();
    // inv2 is blocking (others depend on it, and it's in Registered = available_for_run)
    assert!(blocking.contains(&inv2));
}

/// Max limit is respected for get_blocking_invocations.
#[tokio::test]
async fn waiter_blocking_max_limit() {
    let orch = rustvello_mem::orchestrator::MemOrchestrator::new();

    let task_id = TaskId::new("mod", "limit_task");
    let mut blockers = Vec::new();
    for _ in 0..5 {
        let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
        let inv = orch.register_invocation(&call).await.unwrap();
        blockers.push(inv);
    }

    // Create waiters for each blocker
    for blocker in &blockers {
        let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
        let waiter = orch.register_invocation(&call).await.unwrap();
        orch.set_waiting_for(&waiter, blocker).await.unwrap();
    }

    let blocking = orch.get_blocking_invocations(3).await.unwrap();
    assert!(blocking.len() <= 3);
}

// ===========================================================================
// 3. Registration Concurrency Control Types
// ===========================================================================

/// CC type = Task: second invocation of the same task gets
/// ConcurrencyControlled status.
#[tokio::test]
async fn cc_task_registration_blocks_duplicate() {
    use rustvello_mem::orchestrator::MemOrchestrator;
    let orch = MemOrchestrator::new();
    let task_id = TaskId::new("mod", "cc_reg_task");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    // First invocation — registers OK
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = RunnerId::from_string("runner-1");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();

    // second check: concurrency should be blocked
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    assert!(!allowed);

    // Remove from index → now allowed again
    orch.remove_from_concurrency_index(&inv1).await.unwrap();
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    assert!(allowed);
}

/// CC type = Argument: same args blocked, different args allowed.
#[tokio::test]
async fn cc_argument_blocks_same_args() {
    use rustvello_mem::orchestrator::MemOrchestrator;
    let orch = MemOrchestrator::new();
    let task_id = TaskId::new("mod", "cc_arg_task");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Argument;
    config.running_concurrency = Some(1);

    let mut args1 = SerializedArguments::new();
    args1.insert("x", "1");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "2");

    // First invocation with args1
    let call = CallDTO::new(task_id.clone(), args1.clone());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = RunnerId::from_string("runner-1");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&args1))
        .await
        .unwrap();

    // Same args → blocked
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args1))
        .await
        .unwrap();
    assert!(!allowed);

    // Different args → allowed
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args2))
        .await
        .unwrap();
    assert!(allowed);
}

// ===========================================================================
// 4. Concurrent set_pending Atomicity
// ===========================================================================

/// Two concurrent tasks trying set_pending on the same invocation: exactly
/// one should succeed.
#[tokio::test]
async fn set_pending_atomicity_concurrent() {
    use rustvello_mem::orchestrator::MemOrchestrator;
    let orch = Arc::new(MemOrchestrator::new());

    let task_id = TaskId::new("mod", "atomic_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner1 = RunnerId::from_string("runner-1");
    let runner2 = RunnerId::from_string("runner-2");

    let orch1 = Arc::clone(&orch);
    let orch2 = Arc::clone(&orch);
    let inv1 = inv_id.clone();
    let inv2 = inv_id.clone();
    let r1 = runner1.clone();
    let r2 = runner2.clone();

    let (result1, result2) = tokio::join!(
        async move {
            orch1
                .set_invocation_status(&inv1, InvocationStatus::Pending, Some(&r1))
                .await
        },
        async move {
            orch2
                .set_invocation_status(&inv2, InvocationStatus::Pending, Some(&r2))
                .await
        }
    );

    // Exactly one should succeed
    let successes = [result1.is_ok(), result2.is_ok()]
        .iter()
        .filter(|&&s| s)
        .count();
    assert_eq!(successes, 1, "Exactly one set_pending should succeed");
}

// ===========================================================================
// 5. Recovery Integration
// ===========================================================================

/// Recovery: stale pending invocations get rerouted when a new runner
/// starts and runs the main loop. We test manually at the orchestrator level.
#[tokio::test]
async fn recovery_stale_pending() {
    let orch = rustvello_mem::orchestrator::MemOrchestrator::new();

    let task_id = TaskId::new("mod", "recovery_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_id = orch.register_invocation(&call).await.unwrap();

    // Simulate: Registered → Pending, then runner dies
    let dead_runner = RunnerId::from_string("dead-runner");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&dead_runner))
        .await
        .unwrap();

    // Stale detection: max_pending_seconds = 0 means immediate
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let stale = orch.get_stale_pending_invocations(0).await.unwrap();
    assert!(!stale.is_empty(), "Should detect stale pending invocation");
    assert!(stale.contains(&inv_id));

    // Transition to recovery
    orch.set_invocation_status(&inv_id, InvocationStatus::PendingRecovery, None)
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Rerouted, None)
        .await
        .unwrap();

    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Rerouted);
}

/// Recovery: stale running invocations with dead runners get detected.
#[tokio::test]
async fn recovery_stale_running() {
    let orch = rustvello_mem::orchestrator::MemOrchestrator::new();

    let task_id = TaskId::new("mod", "recovery_running_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let dead_runner = RunnerId::from_string("dead-runner-2");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&dead_runner))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&dead_runner))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let stale = orch.get_stale_running_invocations(0).await.unwrap();
    assert!(!stale.is_empty(), "Should detect stale running invocation");
    assert!(stale.contains(&inv_id));

    // Transition to recovery
    orch.set_invocation_status(&inv_id, InvocationStatus::RunningRecovery, None)
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Rerouted, None)
        .await
        .unwrap();

    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Rerouted);
}

// ===========================================================================
// 6. Large Argument Stress
// ===========================================================================

/// A ~1MB argument travels through the full pipeline without loss.
#[tokio::test]
async fn large_argument_round_trip() {
    let mut app = RustvelloApp::new(AppConfig::new("large-arg"));
    app.register(EchoBigTask::new()).unwrap();

    // 1 MB of 'A'
    let big = "A".repeat(1_000_000);
    let handle = app
        .submit_call(&EchoBigTask::new(), EchoBigParams { data: big.clone() })
        .await
        .unwrap();

    let runner = make_runner_for(&app, make_registry(EchoBigTask::new()));
    runner.run_one().await.unwrap();

    let result: String = handle.result().await.unwrap();
    assert_eq!(result.len(), big.len());
    assert_eq!(result, big);
}
