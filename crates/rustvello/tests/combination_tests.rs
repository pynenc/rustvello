//! Combination matrix tests: (backend × runner) lifecycle validation.
//!
//! Exercises full invocation lifecycle, retry, concurrency control,
//! and error propagation across all backend and runner combinations.
//!
//! Backends: Mem, SQLite (feature-gated)
//! Runners:  PersistentTokio, Rayon (feature-gated)

use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::invocation::InvocationDTO;

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

#[rustvello::task]
fn combo_add(x: i32, y: i32) -> i32 {
    x + y
}

#[rustvello::task(max_retries = 2)]
fn combo_fail(_n: i32) -> RustvelloResult<i32> {
    Err(RustvelloError::runner_err("combo_fail: boom".to_string()))
}

#[rustvello::task(concurrency = "task")]
fn combo_cc(x: i32) -> i32 {
    x * 2
}

#[rustvello::task]
fn combo_error(msg: String) -> RustvelloResult<String> {
    Err(RustvelloError::runner_err(msg))
}

// ---------------------------------------------------------------------------
// Backend factory
// ---------------------------------------------------------------------------

struct Backends {
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn InvocationControlBackend>,
    state_backend: Arc<dyn StateBackend>,
}

fn mem_backends() -> Backends {
    Backends {
        broker: Arc::new(rustvello_mem::broker::MemBroker::new()),
        orchestrator: Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()),
        state_backend: Arc::new(rustvello_mem::state_backend::MemStateBackend::new()),
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_backends() -> Backends {
    let db = Arc::new(rustvello_sqlite::db::Database::in_memory().unwrap());
    Backends {
        broker: Arc::new(rustvello_sqlite::broker::SqliteBroker::new(Arc::clone(&db))),
        orchestrator: Arc::new(rustvello_sqlite::orchestrator::SqliteOrchestrator::new(
            Arc::clone(&db),
        )),
        state_backend: Arc::new(rustvello_sqlite::state_backend::SqliteStateBackend::new(db)),
    }
}

// ---------------------------------------------------------------------------
// Runner factory
// ---------------------------------------------------------------------------

enum RunnerKind {
    PersistentTokio,
    #[cfg(feature = "rayon")]
    Rayon,
}

fn make_runner(kind: &RunnerKind, b: &Backends, registry: Arc<TaskRegistry>) -> Box<dyn Runner> {
    match kind {
        RunnerKind::PersistentTokio => Box::new(PersistentTokioRunner::new(
            "combo-test".to_string(),
            AppConfig::default(),
            Arc::clone(&b.broker),
            Arc::clone(&b.orchestrator),
            Arc::clone(&b.state_backend),
            Arc::clone(&registry),
            None,
        )),
        #[cfg(feature = "rayon")]
        RunnerKind::Rayon => Box::new(
            RayonRunner::new(
                "combo-test".to_string(),
                AppConfig::default(),
                Arc::clone(&b.broker),
                Arc::clone(&b.orchestrator),
                Arc::clone(&b.state_backend),
                Arc::clone(&registry),
            )
            .expect("test: failed to build RayonRunner"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Helper: seed invocation (low-level)
// ---------------------------------------------------------------------------

async fn seed_invocation(
    b: &Backends,
    task_id: &TaskId,
    args: SerializedArguments,
) -> rustvello_proto::identifiers::InvocationId {
    let call = CallDTO::new(task_id.clone(), args);
    let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id.clone(), call.call_id.clone());
    b.state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    b.broker.route_invocation(&inv_id).await.unwrap();
    inv_id
}

fn add_registry() -> Arc<TaskRegistry> {
    Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(ComboAddTask::new()).unwrap();
        reg
    })
}

fn fail_registry() -> Arc<TaskRegistry> {
    Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(ComboFailTask::new()).unwrap();
        reg
    })
}

fn error_registry() -> Arc<TaskRegistry> {
    Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(ComboErrorTask::new()).unwrap();
        reg
    })
}

// ===========================================================================
// Macro to generate the cross-product of tests
// ===========================================================================

/// Generate a test function for each (backend, runner) combination.
macro_rules! combo_test {
    ($test_name:ident, $test_fn:ident) => {
        mod $test_name {
            use super::*;

            #[tokio::test]
            async fn mem_persistent_tokio() {
                $test_fn(mem_backends(), RunnerKind::PersistentTokio).await;
            }

            #[cfg(feature = "rayon")]
            #[tokio::test]
            async fn mem_rayon() {
                $test_fn(mem_backends(), RunnerKind::Rayon).await;
            }

            #[cfg(feature = "sqlite")]
            #[tokio::test]
            async fn sqlite_persistent_tokio() {
                $test_fn(sqlite_backends(), RunnerKind::PersistentTokio).await;
            }

            #[cfg(all(feature = "sqlite", feature = "rayon"))]
            #[tokio::test]
            async fn sqlite_rayon() {
                $test_fn(sqlite_backends(), RunnerKind::Rayon).await;
            }
        }
    };
}

// ===========================================================================
// Test 1: Full lifecycle — submit → run → get result
// ===========================================================================

async fn do_full_lifecycle(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboAddTask::new()).clone();
    let mut args = SerializedArguments::new();
    args.insert("x", "10");
    args.insert("y", "32");
    let inv_id = seed_invocation(&b, &task_id, args).await;

    let runner = make_runner(&rk, &b, add_registry());
    runner.run_one().await.unwrap();

    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);

    let result_raw = b.state_backend.get_result(&inv_id).await.unwrap().unwrap();
    let result: i32 = serde_json::from_str(&result_raw).unwrap();
    assert_eq!(result, 42);
}

combo_test!(full_lifecycle, do_full_lifecycle);

// ===========================================================================
// Test 2: Retry exhaustion — task always fails
// ===========================================================================

async fn do_retry_exhaustion(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboFailTask::new()).clone();
    let mut args = SerializedArguments::new();
    args.insert("_n", "1");
    let inv_id = seed_invocation(&b, &task_id, args).await;

    let runner = make_runner(&rk, &b, fail_registry());
    // Run up to max_retries + 1 times (initial + 2 retries)
    for _ in 0..3 {
        runner.run_one().await.unwrap();
    }

    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Failed);

    let error = b.state_backend.get_error(&inv_id).await.unwrap();
    assert!(error.is_some());
    assert!(error.unwrap().message.contains("combo_fail: boom"));
}

combo_test!(retry_exhaustion, do_retry_exhaustion);

// ===========================================================================
// Test 3: Error propagation — error message survives round-trip
// ===========================================================================

async fn do_error_propagation(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboErrorTask::new()).clone();
    let mut args = SerializedArguments::new();
    args.insert("msg", "\"custom error message\"");
    let inv_id = seed_invocation(&b, &task_id, args).await;

    let runner = make_runner(&rk, &b, error_registry());
    runner.run_one().await.unwrap();

    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Failed);

    let error = b.state_backend.get_error(&inv_id).await.unwrap().unwrap();
    assert!(error.message.contains("custom error message"));
}

combo_test!(error_propagation, do_error_propagation);

// ===========================================================================
// Test 4: Multiple invocations — sequential processing
// ===========================================================================

async fn do_multiple_invocations(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboAddTask::new()).clone();
    let mut inv_ids = Vec::new();
    for i in 0..5 {
        let mut args = SerializedArguments::new();
        args.insert("x", i.to_string());
        args.insert("y", (i * 10).to_string());
        inv_ids.push(seed_invocation(&b, &task_id, args).await);
    }

    let runner = make_runner(&rk, &b, add_registry());
    for _ in 0..5 {
        runner.run_one().await.unwrap();
    }

    for (i, inv_id) in inv_ids.iter().enumerate() {
        let status = b.orchestrator.get_invocation_status(inv_id).await.unwrap();
        assert_eq!(status.status, InvocationStatus::Success);
        let result_raw = b.state_backend.get_result(inv_id).await.unwrap().unwrap();
        let result: i32 = serde_json::from_str(&result_raw).unwrap();
        let expected = (i as i32) + (i as i32) * 10;
        assert_eq!(result, expected);
    }
}

combo_test!(multiple_invocations, do_multiple_invocations);

// ===========================================================================
// Test 5: Status transition validation
// ===========================================================================

async fn do_status_transitions(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboAddTask::new()).clone();
    let mut args = SerializedArguments::new();
    args.insert("x", "1");
    args.insert("y", "2");
    let inv_id = seed_invocation(&b, &task_id, args).await;

    // Before running: Registered
    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Registered);

    let runner = make_runner(&rk, &b, add_registry());
    runner.run_one().await.unwrap();

    // After running: Success
    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);

    // History should contain the status transitions
    let history = b.state_backend.get_history(&inv_id).await.unwrap();
    assert!(
        history.len() >= 3,
        "Expected >= 3 history entries (Registered, Pending, Running/Success), got {}",
        history.len()
    );
}

combo_test!(status_transitions, do_status_transitions);

// ===========================================================================
// Test 6: Empty queue — run_one returns false when nothing to process
// ===========================================================================

async fn do_empty_queue_returns_false(b: Backends, rk: RunnerKind) {
    let runner = make_runner(&rk, &b, add_registry());
    let did_work = runner.run_one().await.unwrap();
    assert!(!did_work);
}

combo_test!(empty_queue_returns_false, do_empty_queue_returns_false);

// ===========================================================================
// Test 7: Purge after processing — verify cleanup
// ===========================================================================

async fn do_purge_after_processing(b: Backends, rk: RunnerKind) {
    let task_id = Task::task_id(&ComboAddTask::new()).clone();
    let mut args = SerializedArguments::new();
    args.insert("x", "5");
    args.insert("y", "5");
    let inv_id = seed_invocation(&b, &task_id, args).await;

    let runner = make_runner(&rk, &b, add_registry());
    runner.run_one().await.unwrap();

    // Verify result exists
    assert!(b.state_backend.get_result(&inv_id).await.unwrap().is_some());

    // Purge all backends
    b.broker.purge(None).await.unwrap();
    b.orchestrator.purge().await.unwrap();
    b.state_backend.purge().await.unwrap();

    // After purge, queue should be empty
    let did_work = runner.run_one().await.unwrap();
    assert!(!did_work);
}

combo_test!(purge_after_processing, do_purge_after_processing);
