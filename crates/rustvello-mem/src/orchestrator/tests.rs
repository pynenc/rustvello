use std::sync::Arc;

use chrono::Utc;

use rustvello_core::orchestrator::{
    OrchestratorBlocking, OrchestratorQuery, OrchestratorRecovery, OrchestratorStatus,
};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{RunnerId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::MemOrchestrator;

fn make_call() -> CallDTO {
    let task_id = TaskId::new("test.module", "my_task");
    let mut args = SerializedArguments::new();
    args.insert("x", "42");
    CallDTO::new(task_id, args)
}

#[tokio::test]
async fn test_register_and_get_status() {
    let orch = MemOrchestrator::new();
    let call = make_call();

    let inv_id = orch.register_invocation(&call).await.unwrap();
    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Registered);
}

#[tokio::test]
async fn test_status_transitions() {
    let orch = MemOrchestrator::new();
    let call = make_call();
    let inv_id = orch.register_invocation(&call).await.unwrap();
    let runner = RunnerId::new();

    // Registered -> Pending (runner claims ownership)
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    // Pending -> Running
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner))
        .await
        .unwrap();
    // Running -> Success
    orch.set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner))
        .await
        .unwrap();

    let status = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);
}

#[tokio::test]
async fn test_invalid_transition() {
    let orch = MemOrchestrator::new();
    let call = make_call();
    let inv_id = orch.register_invocation(&call).await.unwrap();

    // Registered -> Running is invalid (must go through Pending)
    let result = orch
        .set_invocation_status(&inv_id, InvocationStatus::Running, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_query_by_status() {
    let orch = MemOrchestrator::new();
    let call = make_call();
    let runner = RunnerId::from_string("test-runner");

    let inv1 = orch.register_invocation(&call).await.unwrap();
    let _inv2 = orch.register_invocation(&call).await.unwrap();

    // Move inv1 to Pending
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();

    let registered = orch
        .get_invocations_by_status(InvocationStatus::Registered, None)
        .await
        .unwrap();
    assert_eq!(registered.len(), 1);

    let pending = orch
        .get_invocations_by_status(InvocationStatus::Pending, None)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn test_waiting_for() {
    let orch = MemOrchestrator::new();
    let call = make_call();

    let parent = orch.register_invocation(&call).await.unwrap();
    let child = orch.register_invocation(&call).await.unwrap();

    orch.set_waiting_for(&parent, &child).await.unwrap();

    let waiters = orch.get_waiters(&child).await.unwrap();
    assert_eq!(waiters.len(), 1);

    let released = orch.release_waiters(&child).await.unwrap();
    assert_eq!(released.len(), 1);

    // After release, no more waiters
    let waiters = orch.get_waiters(&child).await.unwrap();
    assert_eq!(waiters.len(), 0);
}

#[tokio::test]
async fn test_register_heartbeat() {
    let orch = MemOrchestrator::new();
    let runner_id = RunnerId::from_string("runner-1");

    orch.register_heartbeat(&runner_id, false).await.unwrap();

    let state = orch.state.lock().await;
    assert!(state.heartbeats.contains_key("runner-1"));
    let age = (Utc::now() - state.heartbeats["runner-1"]).num_seconds();
    assert!(age < 1);
}

#[tokio::test]
async fn test_stale_pending_invocations() {
    let orch = MemOrchestrator::new();
    let call = make_call();
    let runner_id = RunnerId::from_string("stale-runner");

    // Create an invocation and move to Pending
    let inv_id = orch.register_invocation(&call).await.unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();

    // With a very large threshold, nothing is stale
    let stale = orch.get_stale_pending_invocations(3600).await.unwrap();
    assert!(stale.is_empty());

    // Manually backdate the status record timestamp
    {
        let mut state = orch.state.lock().await;
        if let Some(record) = state.status_records.get_mut(inv_id.as_str()) {
            record.timestamp = chrono::Utc::now() - chrono::Duration::seconds(120);
        }
    }

    // With a 60s threshold, the invocation is stale
    let stale = orch.get_stale_pending_invocations(60).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], inv_id);
}

#[tokio::test]
async fn test_stale_running_no_heartbeat() {
    let orch = MemOrchestrator::new();
    let call = make_call();
    let runner_id = RunnerId::from_string("dead-runner");

    // Create an invocation and move to Running
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
    let orch = MemOrchestrator::new();
    let call = make_call();
    let runner_id = RunnerId::from_string("alive-runner");

    // Create an invocation and move to Running
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
    let orch = MemOrchestrator::new();
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
        let mut state = orch.state.lock().await;
        state.heartbeats.insert(
            Arc::from("dying-runner"),
            Utc::now() - chrono::Duration::seconds(600),
        );
    }

    // With a 300s threshold, the runner is dead
    let stale = orch.get_stale_running_invocations(300).await.unwrap();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0], inv_id);
}
