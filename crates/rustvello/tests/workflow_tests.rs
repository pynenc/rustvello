//! Workflow integration tests.
//!
//! Exercises workflow data persistence, isolation, deterministic operations,
//! and workflow type discovery at the integration level using the in-memory
//! state backend.

use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::context::{InvocationContext, INVOCATION_CTX};
use rustvello_core::state_backend::{StateBackend, StateBackendCore, StateBackendQuery};
use rustvello_core::workflow::WorkflowRoot;

#[rustvello::workflow]
fn deterministic_root_values() -> RustvelloResult<String> {
    let mut root = WorkflowRoot::current()?;
    Ok(format!(
        "{}|{}|{}",
        root.random()?,
        root.utc_now()?.to_rfc3339(),
        root.uuid()?
    ))
}

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

#[tokio::test]
async fn workflow_root_operations_run_through_task_runner() {
    let mut app = RustvelloApp::new(AppConfig::new("workflow-root-runner"));
    app.register(DeterministicRootValuesTask::new()).unwrap();
    let handle = app
        .submit_call(&DeterministicRootValuesTask::new(), ())
        .await
        .unwrap();
    let runner = TaskRunner::new(
        "workflow-test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new({
            let mut registry = TaskRegistry::new();
            registry
                .register_typed(DeterministicRootValuesTask::new())
                .unwrap();
            registry
        }),
        None,
    );

    runner.run_one().await.unwrap();
    let result: String = handle.result().await.unwrap();
    let values: Vec<&str> = result.split('|').collect();
    assert_eq!(values.len(), 3);
    assert!(values[0].parse::<f64>().is_ok());
    assert!(chrono::DateTime::parse_from_rfc3339(values[1]).is_ok());
    assert!(uuid::Uuid::parse_str(values[2]).is_ok());
}

fn workflow_context(
    sb: Arc<dyn StateBackend>,
    workflow_id: InvocationId,
    invocation_id: InvocationId,
    is_workflow_defining: bool,
) -> InvocationContext {
    InvocationContext {
        invocation_id,
        task_id: TaskId::new("tests", "workflow"),
        workflow: Some(WorkflowIdentity::root(
            workflow_id,
            TaskId::new("tests", "workflow"),
        )),
        is_workflow_defining,
        state_backend: Some(sb),
        parent_invocation_id: None,
        num_retries: 0,
    }
}

/// WorkflowRoot produces deterministic random values.
#[tokio::test]
async fn deterministic_random_is_seeded() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();
    let ctx = workflow_context(Arc::clone(&sb), wf_id.clone(), wf_id, true);
    let (r1, r2) = INVOCATION_CTX
        .scope(ctx, async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.random_async().await.unwrap(),
                root.random_async().await.unwrap(),
            )
        })
        .await;

    // Both should be in [0, 1) range
    assert!((0.0..1.0).contains(&r1));
    assert!((0.0..1.0).contains(&r2));

    // They should be different (distinct sequences)
    assert_ne!(r1, r2);
}

/// WorkflowRoot utc_now produces ascending timestamps.
#[tokio::test]
async fn deterministic_time_ascending() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let ctx = workflow_context(Arc::clone(&sb), wf_id.clone(), wf_id, true);
    let (t1, t2, t3) = INVOCATION_CTX
        .scope(ctx, async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.utc_now_async().await.unwrap(),
                root.utc_now_async().await.unwrap(),
                root.utc_now_async().await.unwrap(),
            )
        })
        .await;

    assert!(t1 < t2, "Timestamps should be ascending");
    assert!(t2 < t3, "Timestamps should be ascending");
}

/// WorkflowRoot uuid produces valid UUIDs.
#[tokio::test]
async fn deterministic_uuid_valid() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let ctx = workflow_context(Arc::clone(&sb), wf_id.clone(), wf_id, true);
    let (u1, u2) = INVOCATION_CTX
        .scope(ctx, async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.uuid_async().await.unwrap(),
                root.uuid_async().await.unwrap(),
            )
        })
        .await;

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

    let ctx = workflow_context(Arc::clone(&sb), wf_id.clone(), wf_id, true);
    let first = INVOCATION_CTX
        .scope(ctx.clone(), async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.random_async().await.unwrap(),
                root.utc_now_async().await.unwrap(),
                root.uuid_async().await.unwrap(),
            )
        })
        .await;
    let second = INVOCATION_CTX
        .scope(ctx, async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.random_async().await.unwrap(),
                root.utc_now_async().await.unwrap(),
                root.uuid_async().await.unwrap(),
            )
        })
        .await;

    assert_eq!(first, second, "replay should reuse all recorded values");
}

/// Mixed deterministic operations produce isolated sequences.
#[tokio::test]
async fn deterministic_mixed_operations() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let wf_id = InvocationId::new();

    let ctx = workflow_context(Arc::clone(&sb), wf_id.clone(), wf_id, true);
    let (r, t, u, r2) = INVOCATION_CTX
        .scope(ctx, async {
            let mut root = WorkflowRoot::current().unwrap();
            (
                root.random_async().await.unwrap(),
                root.utc_now_async().await.unwrap(),
                root.uuid_async().await.unwrap(),
                root.random_async().await.unwrap(),
            )
        })
        .await;

    // All should produce distinct values
    assert!((0.0..1.0).contains(&r));
    assert!((0.0..1.0).contains(&r2));
    assert_ne!(r, r2);
    assert_eq!(u.len(), 36);
    assert!(t.timestamp() > 0);
}

#[test]
fn workflow_root_requires_running_context() {
    assert!(matches!(
        WorkflowRoot::current(),
        Err(RustvelloError::WorkflowContextUnavailable)
    ));
}

#[tokio::test]
async fn workflow_root_rejects_ordinary_task() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let inv_id = InvocationId::new();
    let ctx = InvocationContext {
        invocation_id: inv_id.clone(),
        task_id: TaskId::new("tests", "ordinary"),
        workflow: None,
        is_workflow_defining: false,
        state_backend: Some(sb),
        parent_invocation_id: None,
        num_retries: 0,
    };

    let error = INVOCATION_CTX
        .scope(ctx, async { WorkflowRoot::current().err().unwrap() })
        .await;
    assert!(matches!(
        error,
        RustvelloError::WorkflowMembershipRequired { invocation_id } if invocation_id == inv_id
    ));
}

#[tokio::test]
async fn workflow_root_rejects_child_member() {
    let sb: Arc<dyn StateBackend> = Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let workflow_id = InvocationId::new();
    let child_id = InvocationId::new();
    let ctx = workflow_context(sb, workflow_id.clone(), child_id.clone(), false);

    let error = INVOCATION_CTX
        .scope(ctx, async { WorkflowRoot::current().err().unwrap() })
        .await;
    assert!(matches!(
        error,
        RustvelloError::WorkflowRootRequired { invocation_id, workflow_id: actual }
            if invocation_id == child_id && actual == workflow_id
    ));
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
