use std::sync::Arc;

use rustvello_core::orchestrator::{
    OrchestratorBlocking, OrchestratorQuery, OrchestratorRecovery, OrchestratorStatus,
};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::status::InvocationStatus;

use crate::db::Database;

use super::SqliteOrchestrator;

fn make_orchestrator() -> SqliteOrchestrator {
    let db = Arc::new(Database::in_memory().unwrap());
    SqliteOrchestrator::new(db)
}

fn make_call() -> CallDTO {
    let task_id = TaskId::new("test", "my_task");
    let mut args = SerializedArguments::new();
    args.insert("x", "42");
    CallDTO::new(task_id, args)
}

#[tokio::test]
async fn test_register_and_status() {
    let orch = make_orchestrator();
    let call = make_call();

    let inv_id = orch.register_invocation(&call).await.unwrap();
    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Registered);
}

#[tokio::test]
async fn test_status_transitions() {
    let orch = make_orchestrator();
    let call = make_call();
    let inv_id = orch.register_invocation(&call).await.unwrap();
    let runner = RunnerId::new();

    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner))
        .await
        .unwrap();

    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);
}

#[tokio::test]
async fn test_invalid_transition() {
    let orch = make_orchestrator();
    let call = make_call();
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let result = orch
        .set_invocation_status(&inv_id, InvocationStatus::Running, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_by_task() {
    let orch = make_orchestrator();
    let call = make_call();

    orch.register_invocation(&call).await.unwrap();
    orch.register_invocation(&call).await.unwrap();

    let ids = orch.get_invocations_by_task(&call.task_id).await.unwrap();
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn test_query_by_call() {
    let orch = make_orchestrator();
    let call = make_call();

    orch.register_invocation(&call).await.unwrap();
    orch.register_invocation(&call).await.unwrap();

    let ids = orch.get_invocations_by_call(&call.call_id).await.unwrap();
    assert_eq!(ids.len(), 2);
}

#[tokio::test]
async fn test_query_by_status() {
    let orch = make_orchestrator();
    let call = make_call();
    let runner = RunnerId::from_string("test-runner");

    let inv1 = orch.register_invocation(&call).await.unwrap();
    let _inv2 = orch.register_invocation(&call).await.unwrap();

    // Both should be Registered
    let registered = orch
        .get_invocations_by_status(InvocationStatus::Registered, None)
        .await
        .unwrap();
    assert_eq!(registered.len(), 2);

    // Move one to Pending
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();

    let pending = orch
        .get_invocations_by_status(InvocationStatus::Pending, None)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);

    let still_registered = orch
        .get_invocations_by_status(InvocationStatus::Registered, None)
        .await
        .unwrap();
    assert_eq!(still_registered.len(), 1);
}

#[tokio::test]
async fn test_query_by_status_with_task_filter() {
    let orch = make_orchestrator();
    let call1 = make_call();
    let call2 = {
        let task_id = TaskId::new("other", "task");
        let mut args = SerializedArguments::new();
        args.insert("y", "99");
        CallDTO::new(task_id, args)
    };

    orch.register_invocation(&call1).await.unwrap();
    orch.register_invocation(&call2).await.unwrap();

    let filtered = orch
        .get_invocations_by_status(InvocationStatus::Registered, Some(&call1.task_id))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
}

#[tokio::test]
async fn test_waiting_for_and_release() {
    let orch = make_orchestrator();
    let call = make_call();

    let inv1 = orch.register_invocation(&call).await.unwrap();
    let inv2 = orch.register_invocation(&call).await.unwrap();

    // inv1 is waiting for inv2
    orch.set_waiting_for(&inv1, &inv2).await.unwrap();

    let waiters = orch.get_waiters(&inv2).await.unwrap();
    assert_eq!(waiters.len(), 1);
    assert_eq!(waiters[0], inv1);

    // Release waiters of inv2
    let released = orch.release_waiters(&inv2).await.unwrap();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0], inv1);

    // No more waiters
    let waiters = orch.get_waiters(&inv2).await.unwrap();
    assert!(waiters.is_empty());
}

#[tokio::test]
async fn test_invocation_not_found() {
    let orch = make_orchestrator();
    let fake_id = InvocationId::from_string("nonexistent");
    let result = orch.get_invocation_status(&fake_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_register_heartbeat() {
    let orch = make_orchestrator();
    let runner_id = RunnerId::from_string("runner-1");

    orch.register_heartbeat(&runner_id, false).await.unwrap();

    // Verify heartbeat was stored
    let conn = orch.db.conn.lock().unwrap();
    let ts: String = conn
        .query_row(
            "SELECT last_heartbeat FROM runner_heartbeats WHERE runner_id = ?1",
            [runner_id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!ts.is_empty());
}

#[tokio::test]
async fn test_stale_pending_invocations() {
    let orch = make_orchestrator();
    let call = make_call();
    let runner_id = RunnerId::from_string("stale-runner");

    let inv_id = orch.register_invocation(&call).await.unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();

    // With a large threshold, nothing is stale
    let stale = orch.get_stale_pending_invocations(3600).await.unwrap();
    assert!(stale.is_empty());

    // Backdate the status record timestamp
    {
        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(120)).to_rfc3339();
        let conn = orch.db.conn.lock().unwrap();
        conn.execute(
            "UPDATE status_records SET timestamp = ?1 WHERE invocation_id = ?2",
            rusqlite::params![&old_time, inv_id.as_str()],
        )
        .unwrap();
    }

    // With 60s threshold, the invocation is stale
    let stale = orch.get_stale_pending_invocations(60).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], inv_id);
}

#[tokio::test]
async fn test_stale_running_no_heartbeat() {
    let orch = make_orchestrator();
    let call = make_call();
    let runner_id = RunnerId::from_string("dead-runner");

    let inv_id = orch.register_invocation(&call).await.unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();

    // Runner never sent heartbeat — should be considered stale
    let stale = orch.get_stale_running_invocations(60).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], inv_id);
}

#[tokio::test]
async fn test_stale_running_with_recent_heartbeat() {
    let orch = make_orchestrator();
    let call = make_call();
    let runner_id = RunnerId::from_string("alive-runner");

    let inv_id = orch.register_invocation(&call).await.unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();

    // Runner sends heartbeat
    orch.register_heartbeat(&runner_id, false).await.unwrap();

    // Should NOT be stale (heartbeat is recent)
    let stale = orch.get_stale_running_invocations(60).await.unwrap();
    assert!(stale.is_empty());
}

#[tokio::test]
async fn test_stale_running_with_old_heartbeat() {
    let orch = make_orchestrator();
    let call = make_call();
    let runner_id = RunnerId::from_string("dying-runner");

    let inv_id = orch.register_invocation(&call).await.unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();

    // Backdate the heartbeat to simulate a dead runner
    {
        let old_time = (chrono::Utc::now() - chrono::Duration::seconds(600)).to_rfc3339();
        let creation_time = (chrono::Utc::now() - chrono::Duration::seconds(700)).to_rfc3339();
        let conn = orch.db.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO runner_heartbeats (runner_id, creation_time, last_heartbeat) VALUES (?1, ?2, ?3)",
            rusqlite::params![runner_id.as_str(), &creation_time, &old_time],
        )
        .unwrap();
    }

    // With 300s threshold, the runner is dead
    let stale = orch.get_stale_running_invocations(300).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], inv_id);
}
