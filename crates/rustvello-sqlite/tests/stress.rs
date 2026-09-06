//! SQLite contention tests for the scheduled soak lane.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};
use rustvello_sqlite::broker::SqliteBroker;
use rustvello_sqlite::db::Database;
use rustvello_sqlite::orchestrator::SqliteOrchestrator;
use tempfile::TempDir;
use tokio::sync::Barrier;

fn persistent_db(name: &str) -> (TempDir, Arc<Database>) {
    let dir = tempfile::tempdir().expect("temporary SQLite directory");
    let path = dir.path().join("stress.db");
    let db = Arc::new(Database::open(path, name).expect("open SQLite database"));
    (dir, db)
}

fn call(task_id: &TaskId, args: &SerializedArguments) -> CallDTO {
    CallDTO::new(task_id.clone(), args.clone())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow stress lane"]
async fn sqlite_concurrent_route_and_retrieve_is_exactly_once() {
    let (_dir, db) = persistent_db("route-retrieve");
    let broker: Arc<dyn Broker> = Arc::new(SqliteBroker::new(db));
    let count = 200;
    let ids: Vec<_> = (0..count).map(|_| InvocationId::new()).collect();

    let barrier = Arc::new(Barrier::new(count));
    let mut routes = Vec::with_capacity(count);
    for id in &ids {
        let broker = Arc::clone(&broker);
        let barrier = Arc::clone(&barrier);
        let id = id.clone();
        routes.push(tokio::spawn(async move {
            barrier.wait().await;
            broker.route_invocation(&id).await
        }));
    }
    for route in routes {
        route.await.expect("route task").expect("route invocation");
    }

    let barrier = Arc::new(Barrier::new(count));
    let mut retrieves = Vec::with_capacity(count);
    for _ in 0..count {
        let broker = Arc::clone(&broker);
        let barrier = Arc::clone(&barrier);
        retrieves.push(tokio::spawn(async move {
            barrier.wait().await;
            broker.retrieve_invocation(None).await
        }));
    }
    let mut seen = HashSet::with_capacity(count);
    for retrieve in retrieves {
        let id = retrieve
            .await
            .expect("retrieve task")
            .expect("retrieve invocation")
            .expect("queued invocation");
        assert!(seen.insert(id), "invocation retrieved more than once");
    }
    assert_eq!(seen.len(), count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow stress lane"]
async fn sqlite_status_claim_has_one_winner() {
    let (_dir, db) = persistent_db("status-claim");
    let orchestrator: Arc<dyn InvocationControlBackend> = Arc::new(SqliteOrchestrator::new(db));
    let task_id = TaskId::new("stress", "claim");
    let invocation_id = Arc::new(
        orchestrator
            .register_invocation(&call(&task_id, &SerializedArguments::new()))
            .await
            .expect("register invocation"),
    );
    let contenders = 64;
    let winners = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(contenders));
    let mut tasks = Vec::with_capacity(contenders);
    for _ in 0..contenders {
        let orchestrator = Arc::clone(&orchestrator);
        let invocation_id = Arc::clone(&invocation_id);
        let winners = Arc::clone(&winners);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let runner = RunnerId::new();
            if orchestrator
                .set_invocation_status(&invocation_id, InvocationStatus::Pending, Some(&runner))
                .await
                .is_ok()
            {
                winners.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for task in tasks {
        task.await.expect("claim task");
    }
    assert_eq!(winners.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow stress lane"]
async fn sqlite_concurrency_slot_acquisition_is_atomic() {
    let (_dir, db) = persistent_db("slot-claim");
    let orchestrator: Arc<dyn InvocationControlBackend> = Arc::new(SqliteOrchestrator::new(db));
    let task_id = TaskId::new("stress", "slot");
    let mut args = SerializedArguments::new();
    args.insert("account", "same");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Argument;
    config.running_concurrency = Some(1);

    let contenders = 48;
    let mut ids = Vec::with_capacity(contenders);
    for _ in 0..contenders {
        let id = orchestrator
            .register_invocation(&call(&task_id, &args))
            .await
            .expect("register invocation");
        let runner = RunnerId::new();
        orchestrator
            .set_invocation_status(&id, InvocationStatus::Pending, Some(&runner))
            .await
            .expect("set pending");
        ids.push(id);
    }

    let winners = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(contenders));
    let config = Arc::new(config);
    let args = Arc::new(args);
    let mut tasks = Vec::with_capacity(contenders);
    for id in ids {
        let orchestrator = Arc::clone(&orchestrator);
        let task_id = task_id.clone();
        let winners = Arc::clone(&winners);
        let barrier = Arc::clone(&barrier);
        let config = Arc::clone(&config);
        let args = Arc::clone(&args);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            if orchestrator
                .try_acquire_concurrency_slot(&id, &task_id, &config, Some(&args))
                .await
                .expect("acquire slot")
            {
                winners.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for task in tasks {
        task.await.expect("slot task");
    }
    assert_eq!(winners.load(Ordering::Relaxed), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "slow stress lane"]
async fn sqlite_recovery_claims_each_stale_invocation_once() {
    let (_dir, db) = persistent_db("recovery");
    let orchestrator: Arc<dyn InvocationControlBackend> = Arc::new(SqliteOrchestrator::new(db));
    let task_id = TaskId::new("stress", "recovery");
    let count = 80;
    for _ in 0..count {
        let id = orchestrator
            .register_invocation(&call(&task_id, &SerializedArguments::new()))
            .await
            .expect("register invocation");
        orchestrator
            .set_invocation_status(&id, InvocationStatus::Pending, Some(&RunnerId::new()))
            .await
            .expect("set pending");
    }
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let stale = orchestrator
        .get_stale_pending_invocations(0)
        .await
        .expect("query stale pending");
    assert_eq!(stale.len(), count);

    let recovered = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::with_capacity(count * 2);
    for id in stale {
        for _ in 0..2 {
            let orchestrator = Arc::clone(&orchestrator);
            let recovered = Arc::clone(&recovered);
            let id = id.clone();
            tasks.push(tokio::spawn(async move {
                if orchestrator
                    .set_invocation_status(&id, InvocationStatus::PendingRecovery, None)
                    .await
                    .is_ok()
                {
                    recovered.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
    }
    for task in tasks {
        task.await.expect("recovery task");
    }
    assert_eq!(recovered.load(Ordering::Relaxed), count);
}
