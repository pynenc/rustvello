//! Transaction and routing boundaries of the SQLite dequeue lease.

use std::sync::Arc;
use std::time::Duration;

use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::OrchestratorStatus;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId, TaskLanguage};
use rustvello_proto::status::InvocationStatus;

use crate::{broker::SqliteBroker, db::Database, orchestrator::SqliteOrchestrator};

fn count(db: &Database, table: &str) -> i64 {
    db.conn
        .lock()
        .unwrap()
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

fn expire(db: &Database) {
    db.conn
        .lock()
        .unwrap()
        .execute("UPDATE broker_reservations SET expires_at_ms = 0", [])
        .unwrap();
}

#[tokio::test]
async fn pending_commit_and_reservation_ack_roll_back_together() {
    let db = Arc::new(Database::in_memory().unwrap());
    let broker = SqliteBroker::new(Arc::clone(&db));
    let control = SqliteOrchestrator::new(Arc::clone(&db));
    let task = TaskId::new("reservation", "task");
    let id = control
        .register_invocation(&CallDTO::new(task.clone(), SerializedArguments::new()))
        .await
        .unwrap();
    broker.route_invocation_for_task(&id, &task).await.unwrap();
    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(id.clone())
    );
    assert_eq!(count(&db, "broker_queue"), 1);
    assert_eq!(count(&db, "broker_reservations"), 1);

    db.conn
        .lock()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_ack BEFORE DELETE ON broker_queue
         BEGIN SELECT RAISE(ABORT, 'injected ack failure'); END;",
        )
        .unwrap();
    let owner = RunnerId::new();
    assert!(control
        .set_invocation_status(&id, InvocationStatus::Pending, Some(&owner))
        .await
        .is_err());
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().status,
        InvocationStatus::Registered
    );
    assert_eq!(count(&db, "broker_queue"), 1);
    assert_eq!(count(&db, "broker_reservations"), 1);
    db.conn
        .lock()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_ack")
        .unwrap();

    expire(&db);
    assert_eq!(
        broker.retrieve_invocation(None).await.unwrap(),
        Some(id.clone())
    );
    control
        .set_invocation_status(&id, InvocationStatus::Pending, Some(&owner))
        .await
        .unwrap();
    assert_eq!(count(&db, "broker_queue"), 0);
    assert_eq!(count(&db, "broker_reservations"), 0);
    let old = RunnerId::new();
    assert!(control
        .set_invocation_status(&id, InvocationStatus::Running, Some(&old))
        .await
        .is_err());
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().runner_id,
        Some(owner.clone())
    );
    control
        .set_invocation_status(&id, InvocationStatus::Running, Some(&owner))
        .await
        .unwrap();
    control
        .set_invocation_status(&id, InvocationStatus::Success, Some(&owner))
        .await
        .unwrap();
    // Late publication cannot revive a completed invocation.
    broker.route_invocation_for_task(&id, &task).await.unwrap();
    assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
    assert_eq!(count(&db, "broker_queue"), 0);
}

#[tokio::test]
async fn expired_reservation_preserves_queue_task_language_and_priority() {
    let db = Arc::new(Database::in_memory().unwrap());
    let broker = SqliteBroker::new(Arc::clone(&db));
    let low = InvocationId::new();
    let high = InvocationId::new();
    let rust = TaskId::new("reservation", "rust");
    broker
        .route_invocation_with_options(&low, Some(&rust), "alpha", 0.0)
        .await
        .unwrap();
    broker
        .route_invocation_with_options(&high, Some(&rust), "alpha", 1.0)
        .await
        .unwrap();
    assert_eq!(
        broker
            .retrieve_invocation_for_language_from_queue(TaskLanguage::Rust, "alpha")
            .await
            .unwrap(),
        Some(high.clone())
    );
    assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
    expire(&db);
    assert_eq!(broker.count_invocations(None).await.unwrap(), 2);
    assert_eq!(
        broker
            .retrieve_invocation_for_language_from_queue(TaskLanguage::Python, "alpha")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("beta", None)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("alpha", Some(&TaskId::new("other", "task")))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("alpha", Some(&rust))
            .await
            .unwrap(),
        Some(high)
    );
    broker.purge(Some(&rust)).await.unwrap();
    expire(&db);
    assert_eq!(count(&db, "broker_reservations"), 0);
    assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
}

#[test]
fn reservation_lease_is_explicitly_bounded() {
    for duration in [
        Duration::ZERO,
        Duration::from_millis(99),
        Duration::from_secs(3601),
        Duration::MAX,
    ] {
        assert!(SqliteBroker::new(Arc::new(Database::in_memory().unwrap()))
            .with_reservation_lease(duration)
            .is_err());
    }
    for duration in [
        Duration::from_millis(100),
        Duration::from_secs(60),
        Duration::from_secs(3600),
    ] {
        assert!(SqliteBroker::new(Arc::new(Database::in_memory().unwrap()))
            .with_reservation_lease(duration)
            .is_ok());
    }
}
