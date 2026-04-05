//! Workflow integration tests.
//!
//! Exercises workflow data persistence, isolation, deterministic operations,
//! and workflow type discovery at the integration level using the in-memory
//! state backend.

use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::state_backend::{StateBackend, StateBackendCore, StateBackendQuery};
use rustvello_core::workflow::DeterministicExecutor;

// ===========================================================================
// 1. Workflow Data Persistence (via state backend directly)
// ===========================================================================

/// Set and retrieve workflow data through the state backend.
#[tokio::test]
async fn workflow_data_set_get() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();
    let wf_id = InvocationId::new();

    sb.set_workflow_data(&wf_id, "counter", "42").await.unwrap();

    let val = sb.get_workflow_data(&wf_id, "counter").await.unwrap();
    assert_eq!(val.as_deref(), Some("42"));
}

/// Updating workflow data overwrites previous value.
#[tokio::test]
async fn workflow_data_update() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();
    let wf_id = InvocationId::new();

    sb.set_workflow_data(&wf_id, "key", "v1").await.unwrap();
    sb.set_workflow_data(&wf_id, "key", "v2").await.unwrap();

    let val = sb.get_workflow_data(&wf_id, "key").await.unwrap();
    assert_eq!(val.as_deref(), Some("v2"));
}

/// Getting non-existent key returns None.
#[tokio::test]
async fn workflow_data_missing_key() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();
    let wf_id = InvocationId::new();

    let val = sb.get_workflow_data(&wf_id, "nonexistent").await.unwrap();
    assert!(val.is_none());
}

/// Workflow data is isolated between different workflows.
#[tokio::test]
async fn workflow_data_isolation() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();
    let wf1 = InvocationId::new();
    let wf2 = InvocationId::new();

    sb.set_workflow_data(&wf1, "name", "workflow_one")
        .await
        .unwrap();
    sb.set_workflow_data(&wf2, "name", "workflow_two")
        .await
        .unwrap();

    assert_eq!(
        sb.get_workflow_data(&wf1, "name").await.unwrap().as_deref(),
        Some("workflow_one")
    );
    assert_eq!(
        sb.get_workflow_data(&wf2, "name").await.unwrap().as_deref(),
        Some("workflow_two")
    );
}

// ===========================================================================
// 2. Workflow Type Discovery
// ===========================================================================

/// store_workflow_run + get_all_workflow_types returns stored types.
#[tokio::test]
async fn workflow_type_discovery() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();

    let task_a = TaskId::new("mod", "task_a");
    let task_b = TaskId::new("mod", "task_b");

    let wf1 = WorkflowIdentity::root(InvocationId::new(), task_a.clone());
    let wf2 = WorkflowIdentity::root(InvocationId::new(), task_b.clone());

    sb.store_workflow_run(&wf1).await.unwrap();
    sb.store_workflow_run(&wf2).await.unwrap();

    let types = sb.get_all_workflow_types().await.unwrap();
    assert!(types.contains(&task_a));
    assert!(types.contains(&task_b));
}

/// get_workflow_runs returns all runs of a given type.
#[tokio::test]
async fn workflow_runs_listing() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();

    let task_id = TaskId::new("mod", "run_task");
    let wf1 = WorkflowIdentity::root(InvocationId::new(), task_id.clone());
    let wf2 = WorkflowIdentity::root(InvocationId::new(), task_id.clone());

    sb.store_workflow_run(&wf1).await.unwrap();
    sb.store_workflow_run(&wf2).await.unwrap();

    let runs = sb.get_workflow_runs(&task_id).await.unwrap();
    assert_eq!(runs.len(), 2);
}

/// get_workflow_invocations tracks members of a workflow.
#[tokio::test]
async fn workflow_invocations_tracked() {
    let sb = rustvello_mem::state_backend::MemStateBackend::new();

    let wf_id = InvocationId::new();
    let task_id = TaskId::new("mod", "wf_parent");

    // Store the root workflow run
    let wf = WorkflowIdentity::root(wf_id.clone(), task_id.clone());
    sb.store_workflow_run(&wf).await.unwrap();

    // Upsert some invocations that belong to this workflow
    let child_inv_id = InvocationId::new();
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_dto = InvocationDTO::with_workflow(
        child_inv_id.clone(),
        task_id.clone(),
        call.call_id.clone(),
        None,
        wf.clone(),
    );
    sb.upsert_invocation(&inv_dto, &call).await.unwrap();

    let members = sb.get_workflow_invocations(&wf_id).await.unwrap();
    assert!(
        members.contains(&child_inv_id),
        "Workflow invocations should include upserted member"
    );
}

// ===========================================================================
// 3. Deterministic Operations
// ===========================================================================

/// DeterministicExecutor produces deterministic random values.
#[tokio::test]
async fn deterministic_random_is_seeded() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let mut exec = DeterministicExecutor::new(wf_id.clone(), Arc::clone(&sb));

    let r1 = exec.random().await.unwrap();
    let r2 = exec.random().await.unwrap();

    // Both should be in [0, 1) range
    assert!((0.0..1.0).contains(&r1));
    assert!((0.0..1.0).contains(&r2));

    // They should be different (distinct sequences)
    assert_ne!(r1, r2);
}

/// DeterministicExecutor utc_now produces ascending timestamps.
#[tokio::test]
async fn deterministic_time_ascending() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let mut exec = DeterministicExecutor::new(wf_id, Arc::clone(&sb));

    let t1 = exec.utc_now().await.unwrap();
    let t2 = exec.utc_now().await.unwrap();
    let t3 = exec.utc_now().await.unwrap();

    assert!(t1 < t2, "Timestamps should be ascending");
    assert!(t2 < t3, "Timestamps should be ascending");
}

/// DeterministicExecutor uuid produces valid UUIDs.
#[tokio::test]
async fn deterministic_uuid_valid() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let mut exec = DeterministicExecutor::new(wf_id, Arc::clone(&sb));

    let u1 = exec.uuid().await.unwrap();
    let u2 = exec.uuid().await.unwrap();

    // Should be valid UUID format (36 chars with hyphens)
    assert_eq!(u1.len(), 36);
    assert_eq!(u2.len(), 36);
    assert_ne!(u1, u2);
}

/// Replaying deterministic operations returns the same values.
#[tokio::test]
async fn deterministic_replay_consistency() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    // First execution
    let mut exec1 = DeterministicExecutor::new(wf_id.clone(), Arc::clone(&sb));
    let r1 = exec1.random().await.unwrap();
    let t1 = exec1.utc_now().await.unwrap();
    let u1 = exec1.uuid().await.unwrap();

    // Second execution (replay — should get same values from state backend)
    let mut exec2 = DeterministicExecutor::new(wf_id, Arc::clone(&sb));
    let r2 = exec2.random().await.unwrap();
    let t2 = exec2.utc_now().await.unwrap();
    let u2 = exec2.uuid().await.unwrap();

    assert_eq!(r1, r2, "Replayed random should match");
    assert_eq!(t1, t2, "Replayed time should match");
    assert_eq!(u1, u2, "Replayed UUID should match");
}

/// Mixed deterministic operations produce isolated sequences.
#[tokio::test]
async fn deterministic_mixed_operations() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let mut exec = DeterministicExecutor::new(wf_id, Arc::clone(&sb));

    let r = exec.random().await.unwrap();
    let t = exec.utc_now().await.unwrap();
    let u = exec.uuid().await.unwrap();
    let r2 = exec.random().await.unwrap();

    // All should produce distinct values
    assert!((0.0..1.0).contains(&r));
    assert!((0.0..1.0).contains(&r2));
    assert_ne!(r, r2);
    assert_eq!(u.len(), 36);
    assert!(t.timestamp() > 0);
}

// ===========================================================================
// 4. Sub-Workflow Boundary Tests
// ===========================================================================

/// WorkflowIdentity::sub_workflow creates a separate workflow with a parent link.
#[test]
fn sub_workflow_identity_structure() {
    let root_wf_id = InvocationId::new();
    let root_task = TaskId::new("mod", "parent_task");
    let root = WorkflowIdentity::root(root_wf_id.clone(), root_task.clone());

    let child_inv_id = InvocationId::new();
    let child_task = TaskId::new("mod", "child_task");
    let sub = WorkflowIdentity::sub_workflow(
        child_inv_id.clone(),
        child_task.clone(),
        root.workflow_id.clone(),
    );

    assert_eq!(sub.workflow_id, child_inv_id); // own workflow
    assert_eq!(sub.workflow_type, child_task);
    assert_eq!(sub.parent_id, Some(root_wf_id)); // linked to parent
    assert!(sub.is_sub_workflow());
}

/// Child workflow inherits parent's workflow_id.
#[test]
fn child_workflow_inherits_id() {
    let root_wf_id = InvocationId::new();
    let root_task = TaskId::new("mod", "root_task");

    let child_inv = InvocationId::new();
    let child =
        WorkflowIdentity::child(root_wf_id.clone(), root_task.clone(), child_inv.clone(), 1);

    assert_eq!(child.workflow_id, root_wf_id); // shares workflow
    assert_eq!(child.depth, 1);
    assert_eq!(child.parent_id, Some(child_inv));
    // child also has parent_id set (so is_sub_workflow is true for both child and sub_workflow)
    assert!(child.is_sub_workflow());
}
