//! Concurrency stress tests.
//!
//! Validates backend correctness under contention with 100+ concurrent operations.

use std::sync::Arc;
use std::time::Duration;

use rustvello::prelude::*;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

#[rustvello::task]
fn stress_add(x: i32, y: i32) -> i32 {
    x + y
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
// 1. High-volume single runner
// ===========================================================================

/// Submit 200 invocations and process with one runner (8 workers).
#[tokio::test]
async fn stress_200_invocations_single_runner() {
    let n = 200;
    let mut app = RustvelloApp::new(AppConfig::new("stress-single"));
    app.register(StressAddTask::new()).unwrap();

    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let h = app
            .submit_call(&StressAddTask::new(), StressAddParams { x: i as i32, y: 1 })
            .await
            .unwrap();
        handles.push((i, h));
    }

    let reg = make_registry(StressAddTask::new());
    let runner = make_runner("stress-single", &app, reg).with_num_workers(8);

    tokio::time::timeout(
        Duration::from_secs(30),
        runner.with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }),
    )
    .await
    .expect("runner did not finish in time")
    .unwrap();

    for (i, h) in &handles {
        let status = h.status().await.unwrap();
        assert_eq!(status, InvocationStatus::Success, "invocation {i} failed");
        let result: i32 = h.result().await.unwrap();
        assert_eq!(result, *i as i32 + 1, "invocation {i} wrong result");
    }
}

// ===========================================================================
// 2. Multi-runner contention
// ===========================================================================

/// Submit 200 invocations, run 4 runners with 4 workers each.
/// Every invocation must complete exactly once.
#[tokio::test]
async fn stress_200_invocations_multi_runner() {
    let n = 200;
    let mut app = RustvelloApp::new(AppConfig::new("stress-multi"));
    app.register(StressAddTask::new()).unwrap();

    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let h = app
            .submit_call(&StressAddTask::new(), StressAddParams { x: i as i32, y: 1 })
            .await
            .unwrap();
        handles.push((i, h));
    }

    let reg = make_registry(StressAddTask::new());
    let shutdown_after = Duration::from_secs(10);

    let r1 = make_runner("stress-multi", &app, Arc::clone(&reg)).with_num_workers(4);
    let r2 = make_runner("stress-multi", &app, Arc::clone(&reg)).with_num_workers(4);
    let r3 = make_runner("stress-multi", &app, Arc::clone(&reg)).with_num_workers(4);
    let r4 = make_runner("stress-multi", &app, reg).with_num_workers(4);

    let (a, b, c, d) = tokio::join!(
        r1.with_graceful_shutdown(tokio::time::sleep(shutdown_after)),
        r2.with_graceful_shutdown(tokio::time::sleep(shutdown_after)),
        r3.with_graceful_shutdown(tokio::time::sleep(shutdown_after)),
        r4.with_graceful_shutdown(tokio::time::sleep(shutdown_after)),
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    d.unwrap();

    for (i, h) in &handles {
        let status = h.status().await.unwrap();
        assert_eq!(status, InvocationStatus::Success, "invocation {i} failed");
        let result: i32 = h.result().await.unwrap();
        assert_eq!(result, *i as i32 + 1, "invocation {i} wrong result");
    }
}

// ===========================================================================
// 3. Concurrent broker route/retrieve
// ===========================================================================

/// 100 concurrent route + 100 concurrent retrieve on a shared broker.
/// Every routed ID must be retrieved exactly once.
#[tokio::test]
async fn stress_concurrent_route_and_retrieve() {
    use rustvello_core::broker::Broker;
    use rustvello_mem::broker::MemBroker;
    use rustvello_mem::orchestrator::MemOrchestrator;
    use rustvello_proto::call::{CallDTO, SerializedArguments};
    use rustvello_proto::identifiers::TaskId;
    use std::collections::HashSet;
    use tokio::sync::Barrier;

    let n = 100;
    let broker: Arc<dyn Broker> = Arc::new(MemBroker::new());
    let orch = MemOrchestrator::new();
    let task_id = TaskId::new("stress", "route_test");

    // Register and route invocations
    let mut ids = Vec::with_capacity(n);
    for _ in 0..n {
        let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
        let inv_id = orch.register_invocation(&call).await.unwrap();
        ids.push(inv_id);
    }

    // Route all concurrently
    let barrier = Arc::new(Barrier::new(n));
    let mut route_handles = Vec::with_capacity(n);
    for id in &ids {
        let b = Arc::clone(&broker);
        let bar = Arc::clone(&barrier);
        let id = id.clone();
        route_handles.push(tokio::spawn(async move {
            bar.wait().await;
            b.route_invocation(&id).await.unwrap();
        }));
    }
    for h in route_handles {
        h.await.unwrap();
    }

    assert_eq!(broker.count_invocations(None).await.unwrap(), n);

    // Retrieve all concurrently — each should get a unique ID
    let barrier = Arc::new(Barrier::new(n));
    let mut retrieve_handles = Vec::with_capacity(n);
    for _ in 0..n {
        let b = Arc::clone(&broker);
        let bar = Arc::clone(&barrier);
        retrieve_handles.push(tokio::spawn(async move {
            bar.wait().await;
            b.retrieve_invocation(None).await.unwrap()
        }));
    }

    let mut retrieved = HashSet::new();
    for h in retrieve_handles {
        if let Some(id) = h.await.unwrap() {
            assert!(retrieved.insert(id.to_string()), "duplicate retrieve");
        }
    }
    assert_eq!(retrieved.len(), n, "should retrieve all {n} invocations");
}

// ===========================================================================
// 4. Concurrent status transitions (only one should win)
// ===========================================================================

/// 50 concurrent attempts to claim an invocation (Registered → Pending).
/// Exactly one should succeed; the rest should fail.
#[tokio::test]
async fn stress_concurrent_status_claim() {
    use rustvello_mem::orchestrator::MemOrchestrator;
    use rustvello_proto::call::{CallDTO, SerializedArguments};
    use rustvello_proto::identifiers::{RunnerId, TaskId};
    use rustvello_proto::status::InvocationStatus;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    let n = 50;
    let orch = Arc::new(MemOrchestrator::new());
    let task_id = TaskId::new("stress", "claim_test");
    let call = CallDTO::new(task_id, SerializedArguments::default());
    let inv_id = Arc::new(orch.register_invocation(&call).await.unwrap());

    let success_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(n));

    let mut join_handles = Vec::with_capacity(n);
    for _ in 0..n {
        let o = Arc::clone(&orch);
        let id = Arc::clone(&inv_id);
        let sc = Arc::clone(&success_count);
        let bar = Arc::clone(&barrier);
        join_handles.push(tokio::spawn(async move {
            let runner_id = RunnerId::new();
            bar.wait().await;
            if o.set_invocation_status(&id, InvocationStatus::Pending, Some(&runner_id))
                .await
                .is_ok()
            {
                sc.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in join_handles {
        h.await.unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::Relaxed),
        1,
        "exactly one runner should claim the invocation"
    );
}
