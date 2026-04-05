use std::sync::Arc;

use rustvello_core::state_backend::{StateBackendCore, StateBackendQuery};

use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use rustvello_core::error::TaskError;

use crate::db::Database;

use super::SqliteStateBackend;

fn make_backend() -> SqliteStateBackend {
    let db = Arc::new(Database::in_memory().unwrap());
    SqliteStateBackend::new(db)
}

fn make_fixtures() -> (InvocationDTO, CallDTO) {
    let task_id = TaskId::new("test", "my_task");
    let mut args = SerializedArguments::new();
    args.insert("x", "42");
    let call = CallDTO::new(task_id.clone(), args);
    let inv = InvocationDTO::new(InvocationId::new(), task_id, call.call_id.clone());
    (inv, call)
}

#[tokio::test]
async fn test_upsert_and_get() {
    let backend = make_backend();
    let (inv, call) = make_fixtures();

    backend.upsert_invocation(&inv, &call).await.unwrap();

    let got = backend.get_invocation(&inv.invocation_id).await.unwrap();
    assert_eq!(got.invocation_id.as_str(), inv.invocation_id.as_str());
}

#[tokio::test]
async fn test_results() {
    let backend = make_backend();
    let inv_id = InvocationId::new();

    assert!(backend.get_result(&inv_id).await.unwrap().is_none());

    backend.store_result(&inv_id, "42").await.unwrap();
    assert_eq!(
        backend.get_result(&inv_id).await.unwrap(),
        Some("42".to_string())
    );
}

#[tokio::test]
async fn test_errors() {
    let backend = make_backend();
    let inv_id = InvocationId::new();

    let error = TaskError {
        error_type: "ValueError".to_string(),
        message: "bad value".to_string(),
        traceback: Some("line 1\nline 2".to_string()),
    };

    backend.store_error(&inv_id, &error).await.unwrap();
    let got = backend.get_error(&inv_id).await.unwrap().unwrap();
    assert_eq!(got.error_type, "ValueError");
    assert_eq!(got.traceback, Some("line 1\nline 2".to_string()));
}

#[tokio::test]
async fn test_history() {
    let backend = make_backend();
    let inv_id = InvocationId::new();

    let h1 = InvocationHistory::new(
        inv_id.clone(),
        InvocationStatusRecord::new(InvocationStatus::Registered, None),
        Some("created".to_string()),
    );
    let h2 = InvocationHistory::new(
        inv_id.clone(),
        InvocationStatusRecord::new(InvocationStatus::Pending, None),
        None,
    );

    backend.add_history(&h1).await.unwrap();
    backend.add_history(&h2).await.unwrap();

    let histories = backend.get_history(&inv_id).await.unwrap();
    assert_eq!(histories.len(), 2);
    assert_eq!(
        histories[0].status_record.status,
        InvocationStatus::Registered
    );
    assert_eq!(histories[1].status_record.status, InvocationStatus::Pending);
}

#[tokio::test]
async fn test_get_call() {
    let backend = make_backend();
    let (inv, call) = make_fixtures();

    backend.upsert_invocation(&inv, &call).await.unwrap();

    let got_call = backend.get_call(&call.call_id).await.unwrap();
    assert_eq!(got_call.call_id, call.call_id);
    assert_eq!(got_call.task_id, call.task_id);
}

#[tokio::test]
async fn test_purge() {
    let backend = make_backend();
    let (inv, call) = make_fixtures();
    let inv_id = inv.invocation_id.clone();

    backend.upsert_invocation(&inv, &call).await.unwrap();
    backend.store_result(&inv_id, "42").await.unwrap();
    backend.purge().await.unwrap();

    // After purge, result should be gone
    assert!(backend.get_result(&inv_id).await.unwrap().is_none());
}

#[tokio::test]
async fn test_not_found_error() {
    let backend = make_backend();
    let fake_id = InvocationId::from_string("nonexistent");

    let result = backend.get_invocation(&fake_id).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_not_found_returns_none() {
    let backend = make_backend();
    let fake_id = InvocationId::from_string("nonexistent");

    let result = backend.get_error(&fake_id).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_workflow_invocations() {
    let backend = make_backend();
    let task_id = TaskId::new("mod", "task");
    let mut args = SerializedArguments::new();
    args.insert("x", "1");

    let root_id = InvocationId::from_string("root-1");
    let wf = WorkflowIdentity::root(root_id.clone(), task_id.clone());
    let call = CallDTO::new(task_id.clone(), args.clone());
    let inv = InvocationDTO::with_workflow(
        root_id.clone(),
        task_id.clone(),
        call.call_id.clone(),
        None,
        wf.clone(),
    );
    backend.upsert_invocation(&inv, &call).await.unwrap();

    // Child invocation in same workflow
    let child_id = InvocationId::from_string("child-1");
    let call2 = CallDTO::new(task_id.clone(), args);
    let inv2 = InvocationDTO::with_workflow(
        child_id.clone(),
        task_id.clone(),
        call2.call_id.clone(),
        Some(root_id.clone()),
        wf,
    );
    backend.upsert_invocation(&inv2, &call2).await.unwrap();

    // Workflow query
    let members = backend.get_workflow_invocations(&root_id).await.unwrap();
    assert_eq!(members.len(), 2);

    // Child query
    let children = backend.get_child_invocations(&root_id).await.unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].as_str(), "child-1");
}

#[tokio::test]
async fn test_get_invocation_with_workflow_round_trip() {
    let backend = make_backend();
    let task_id = TaskId::new("mod", "func");
    let mut args = SerializedArguments::new();
    args.insert("k", "v");
    let call = CallDTO::new(task_id.clone(), args);
    let inv_id = InvocationId::from_string("inv-wf");
    let wf = WorkflowIdentity::root(inv_id.clone(), task_id.clone());

    let inv = InvocationDTO::with_workflow(
        inv_id.clone(),
        task_id.clone(),
        call.call_id.clone(),
        None,
        wf,
    );
    backend.upsert_invocation(&inv, &call).await.unwrap();

    let got = backend.get_invocation(&inv_id).await.unwrap();
    assert!(got.workflow.is_some());
    let got_wf = got.workflow.unwrap();
    assert_eq!(got_wf.workflow_id.as_str(), "inv-wf");
    assert_eq!(got_wf.workflow_type.name(), "func");
}

// --- Workflow discovery tests ---

#[tokio::test]
async fn test_store_and_get_workflow_runs() {
    let backend = make_backend();
    let task_id = TaskId::new("mod", "my_workflow");
    let wf_id = InvocationId::from_string("wf-run-1");
    let wf = WorkflowIdentity::root(wf_id.clone(), task_id.clone());

    backend.store_workflow_run(&wf).await.unwrap();

    let types = backend.get_all_workflow_types().await.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0].to_string(), task_id.to_string());

    let runs = backend.get_workflow_runs(&task_id).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].workflow_id.as_str(), "wf-run-1");
}

#[tokio::test]
async fn test_multiple_workflow_types() {
    let backend = make_backend();
    let task_a = TaskId::new("mod", "workflow_a");
    let task_b = TaskId::new("mod", "workflow_b");

    backend
        .store_workflow_run(&WorkflowIdentity::root(
            InvocationId::from_string("wf-a"),
            task_a.clone(),
        ))
        .await
        .unwrap();
    backend
        .store_workflow_run(&WorkflowIdentity::root(
            InvocationId::from_string("wf-b"),
            task_b.clone(),
        ))
        .await
        .unwrap();

    let types = backend.get_all_workflow_types().await.unwrap();
    assert_eq!(types.len(), 2);

    assert_eq!(backend.get_workflow_runs(&task_a).await.unwrap().len(), 1);
    assert_eq!(backend.get_workflow_runs(&task_b).await.unwrap().len(), 1);
}

#[tokio::test]
async fn test_multiple_runs_same_type() {
    let backend = make_backend();
    let task_id = TaskId::new("mod", "my_workflow");

    for i in 0..3 {
        backend
            .store_workflow_run(&WorkflowIdentity::root(
                InvocationId::from_string(format!("wf-{i}")),
                task_id.clone(),
            ))
            .await
            .unwrap();
    }

    let types = backend.get_all_workflow_types().await.unwrap();
    assert_eq!(types.len(), 1);

    let runs = backend.get_workflow_runs(&task_id).await.unwrap();
    assert_eq!(runs.len(), 3);
}

// --- Workflow data tests ---

#[tokio::test]
async fn test_workflow_data_set_get() {
    let backend = make_backend();
    let wf_id = InvocationId::from_string("wf-data-1");

    backend
        .set_workflow_data(&wf_id, "key1", "value1")
        .await
        .unwrap();
    let val = backend.get_workflow_data(&wf_id, "key1").await.unwrap();
    assert_eq!(val, Some("value1".to_string()));

    let val = backend.get_workflow_data(&wf_id, "missing").await.unwrap();
    assert!(val.is_none());
}

#[tokio::test]
async fn test_workflow_data_update() {
    let backend = make_backend();
    let wf_id = InvocationId::from_string("wf-data-2");

    backend
        .set_workflow_data(&wf_id, "counter", "1")
        .await
        .unwrap();
    backend
        .set_workflow_data(&wf_id, "counter", "2")
        .await
        .unwrap();
    let val = backend.get_workflow_data(&wf_id, "counter").await.unwrap();
    assert_eq!(val, Some("2".to_string()));
}

#[tokio::test]
async fn test_workflow_data_isolation() {
    let backend = make_backend();
    let wf1 = InvocationId::from_string("wf-iso-1");
    let wf2 = InvocationId::from_string("wf-iso-2");

    backend
        .set_workflow_data(&wf1, "key", "val1")
        .await
        .unwrap();
    backend
        .set_workflow_data(&wf2, "key", "val2")
        .await
        .unwrap();

    assert_eq!(
        backend.get_workflow_data(&wf1, "key").await.unwrap(),
        Some("val1".to_string())
    );
    assert_eq!(
        backend.get_workflow_data(&wf2, "key").await.unwrap(),
        Some("val2".to_string())
    );
}

#[tokio::test]
async fn test_workflow_data_purge() {
    let backend = make_backend();
    let wf_id = InvocationId::from_string("wf-purge");

    backend
        .set_workflow_data(&wf_id, "key", "val")
        .await
        .unwrap();
    backend.purge().await.unwrap();

    let val = backend.get_workflow_data(&wf_id, "key").await.unwrap();
    assert!(val.is_none());
}
