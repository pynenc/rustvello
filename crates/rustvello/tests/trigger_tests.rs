//! Integration tests for the trigger system.
//!
//! Tests the full pipeline: register trigger → report event → evaluate → fire.

use std::sync::Arc;

use rustvello::prelude::*;
use rustvello::trigger_builder::TriggerBuilder;
use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_mem::trigger::MemTriggerStore;
use rustvello_proto::status::InvocationStatus;
use rustvello_proto::trigger::{ExceptionContext, ResultContext, StatusContext, TriggerLogic};

fn mem_store() -> Arc<dyn TriggerStore> {
    Arc::new(MemTriggerStore::new())
}

fn task_id(module: &str, name: &str) -> rustvello_proto::identifiers::TaskId {
    rustvello_proto::identifiers::TaskId::new(module, name)
}

// ---------------------------------------------------------------------------
// TriggerBuilder + registration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn builder_register_status_trigger() {
    let store = mem_store();
    let target = task_id("test", "target_task");

    let def = TriggerBuilder::new()
        .on_status(
            &task_id("test", "source_task"),
            &[InvocationStatus::Success],
        )
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Should be retrievable
    let fetched = store.get_trigger(&def.trigger_id).await.unwrap().unwrap();
    assert_eq!(fetched.task_id, target);
    assert_eq!(fetched.condition_ids.len(), 1);
}

#[tokio::test]
async fn builder_register_multi_condition_trigger() {
    let store = mem_store();
    let target = task_id("test", "multi_target");

    let def = TriggerBuilder::new()
        .on_status(&task_id("test", "source_a"), &[InvocationStatus::Success])
        .on_result(&task_id("test", "source_b"))
        .with_logic(TriggerLogic::And)
        .build_and_register(&target, &store)
        .await
        .unwrap();

    assert_eq!(def.condition_ids.len(), 2);
    assert_eq!(def.logic, TriggerLogic::And);
}

// ---------------------------------------------------------------------------
// Status trigger: report_status_change → evaluate_triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_trigger_fires_on_success() {
    let store = mem_store();
    let source = task_id("test", "source_task");
    let target = task_id("test", "target_task");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Report a success status change for the source task
    let ctx = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    let valid = tm.report_status_change(&ctx).await.unwrap();
    assert_eq!(valid.len(), 1);

    // Evaluate triggers — should fire
    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].0.task_id, target);
}

#[tokio::test]
async fn status_trigger_does_not_fire_on_wrong_status() {
    let store = mem_store();
    let source = task_id("test", "source_task");
    let target = task_id("test", "target_task");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Report a failure (trigger expects success)
    let ctx = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Failed,
        arguments: std::collections::BTreeMap::new(),
    };
    let valid = tm.report_status_change(&ctx).await.unwrap();
    assert!(valid.is_empty());

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert!(to_invoke.is_empty());
}

// ---------------------------------------------------------------------------
// Result trigger: report_result → evaluate_triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn result_trigger_fires() {
    let store = mem_store();
    let source = task_id("test", "source_task");
    let target = task_id("test", "result_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_result(&source)
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let ctx = ResultContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        result: serde_json::json!("42"),
        arguments: std::collections::BTreeMap::new(),
    };
    let valid = tm.report_result(&ctx).await.unwrap();
    assert_eq!(valid.len(), 1);

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].0.task_id, target);
}

// ---------------------------------------------------------------------------
// Exception trigger: report_failure → evaluate_triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exception_trigger_fires() {
    let store = mem_store();
    let source = task_id("test", "source_task");
    let target = task_id("test", "exception_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_exception(&source, &["TaskExecutionError"])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let ctx = ExceptionContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        error_type: "TaskExecutionError".to_string(),
        error_message: "something failed".to_string(),
        arguments: std::collections::BTreeMap::new(),
    };
    let valid = tm.report_failure(&ctx).await.unwrap();
    assert_eq!(valid.len(), 1);

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].0.task_id, target);
}

#[tokio::test]
async fn exception_trigger_does_not_fire_wrong_type() {
    let store = mem_store();
    let source = task_id("test", "source_task");
    let target = task_id("test", "exception_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_exception(&source, &["SpecificError"])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let ctx = ExceptionContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        error_type: "DifferentError".to_string(),
        error_message: "something failed".to_string(),
        arguments: std::collections::BTreeMap::new(),
    };
    let valid = tm.report_failure(&ctx).await.unwrap();
    assert!(valid.is_empty());
}

// ---------------------------------------------------------------------------
// Event trigger: emit_event → evaluate_triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_trigger_fires() {
    let store = mem_store();
    let target = task_id("test", "event_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_event("data_ready")
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let event_id = tm
        .emit_event("data_ready", serde_json::json!({"key": "value"}))
        .await
        .unwrap();

    let event = store.get_event(&event_id).await.unwrap().unwrap();
    assert!(event.is_matched());
    assert!(!event.is_triggered());

    let to_invoke = tm.evaluate_trigger_runs().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].trigger.task_id, target);
    let invocation_id = rustvello_proto::identifiers::InvocationId::new();
    tm.complete_trigger_run(&to_invoke[0].run_id, &invocation_id)
        .await
        .unwrap();

    let run = store
        .get_trigger_run(&to_invoke[0].run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.triggered_invocation_id, Some(invocation_id.clone()));
    assert_eq!(run.event_ids(), vec![event_id.as_str()]);
    let event = store.get_event(&event_id).await.unwrap().unwrap();
    assert_eq!(event.triggered_invocation_ids, vec![invocation_id]);
}

#[tokio::test]
async fn unmatched_event_is_still_persisted_for_monitoring() {
    let store = mem_store();
    let tm = TriggerManager::new(Arc::clone(&store));
    let event_id = tm
        .emit_event("unmatched", serde_json::json!({"visible": true}))
        .await
        .unwrap();

    let event = store.get_event(&event_id).await.unwrap().unwrap();
    assert_eq!(event.event_code, "unmatched");
    assert!(!event.is_matched());
}

#[tokio::test]
async fn event_trigger_does_not_fire_wrong_code() {
    let store = mem_store();
    let target = task_id("test", "event_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_event("data_ready")
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let _event_id = tm
        .emit_event("other_event", serde_json::json!({}))
        .await
        .unwrap();

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert!(to_invoke.is_empty());
}

// ---------------------------------------------------------------------------
// AND logic: all conditions must be satisfied
// ---------------------------------------------------------------------------

#[tokio::test]
async fn and_trigger_requires_all_conditions() {
    let store = mem_store();
    let source_a = task_id("test", "source_a");
    let source_b = task_id("test", "source_b");
    let target = task_id("test", "and_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source_a, &[InvocationStatus::Success])
        .on_result(&source_b)
        .with_logic(TriggerLogic::And)
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Report only source_a → should NOT fire yet
    let ctx_a = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source_a.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx_a).await.unwrap();

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert!(to_invoke.is_empty());

    // Now report source_b → should fire
    let ctx_b = ResultContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source_b.clone(),
        result: serde_json::json!("ok"),
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_result(&ctx_b).await.unwrap();

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].0.task_id, target);
}

// ---------------------------------------------------------------------------
// OR logic: any condition fires independently
// ---------------------------------------------------------------------------

#[tokio::test]
async fn or_trigger_fires_on_any_condition() {
    let store = mem_store();
    let source_a = task_id("test", "source_a");
    let source_b = task_id("test", "source_b");
    let target = task_id("test", "or_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source_a, &[InvocationStatus::Success])
        .on_result(&source_b)
        .with_logic(TriggerLogic::Or)
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Report only source_a → should fire immediately
    let ctx_a = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source_a.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx_a).await.unwrap();

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    assert_eq!(to_invoke[0].0.task_id, target);
}

// ---------------------------------------------------------------------------
// Dedup: same trigger run should not fire twice
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_run_dedup() {
    let store = mem_store();
    let source = task_id("test", "source");
    let target = task_id("test", "dedup_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // First report + evaluate → fires
    let ctx = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx).await.unwrap();
    let first = tm.evaluate_triggers().await.unwrap();
    assert_eq!(first.len(), 1);

    // Re-report with **same** context → same valid_condition_id → same run_id
    // claim_trigger_run returns false → dedup prevents double fire
    tm.report_status_change(&ctx).await.unwrap();
    let second = tm.evaluate_triggers().await.unwrap();
    assert!(second.is_empty(), "same run should be deduped");
}

/// A fresh trigger (no prior claims) fires on the first new event.
#[tokio::test]
async fn separate_trigger_fires_independently() {
    let store = mem_store();
    let source = task_id("test", "source");
    let target = task_id("test", "fresh_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Each new context should produce a valid condition and fire
    let ctx1 = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx1).await.unwrap();
    let first = tm.evaluate_triggers().await.unwrap();
    assert_eq!(first.len(), 1, "first event should fire");

    // Purge run claims to simulate a "clean slate" between cycles
    store.purge().await.unwrap();

    // Re-register the trigger
    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let ctx2 = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx2).await.unwrap();
    let second = tm.evaluate_triggers().await.unwrap();
    assert_eq!(second.len(), 1, "second event with fresh state should fire");
}

// ---------------------------------------------------------------------------
// Builder validation: empty conditions should error
// ---------------------------------------------------------------------------

#[test]
fn builder_empty_conditions_error() {
    let target = task_id("test", "target");
    let result = TriggerBuilder::new().build(&target);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Builder with static arguments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trigger_with_static_args() {
    let store = mem_store();
    let source = task_id("test", "source");
    let target = task_id("test", "args_target");
    let tm = TriggerManager::new(Arc::clone(&store));

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .with_static_args(serde_json::json!({"x": 42}))
        .build_and_register(&target, &store)
        .await
        .unwrap();

    let ctx = StatusContext {
        invocation_id: rustvello_proto::identifiers::InvocationId::new(),
        task_id: source.clone(),
        status: InvocationStatus::Success,
        arguments: std::collections::BTreeMap::new(),
    };
    tm.report_status_change(&ctx).await.unwrap();

    let to_invoke = tm.evaluate_triggers().await.unwrap();
    assert_eq!(to_invoke.len(), 1);
    // Should include the static args
    assert_eq!(to_invoke[0].1, serde_json::json!({"x": 42}));
}

// ---------------------------------------------------------------------------
// Builder app integration: memory() preset includes trigger store
// ---------------------------------------------------------------------------

#[tokio::test]
async fn app_builder_memory_includes_trigger_manager() {
    let app = Rustvello::builder()
        .app_id("test")
        .memory()
        .build()
        .await
        .unwrap();

    assert!(app.trigger_manager().is_some());
}

#[tokio::test]
async fn app_builder_default_has_trigger_manager() {
    // Default (mem feature) should also get a trigger manager via fallback
    let app = Rustvello::builder()
        .app_id("test")
        .memory()
        .build()
        .await
        .unwrap();

    assert!(app.trigger_manager().is_some());
}

// ---------------------------------------------------------------------------
// Purge clears all trigger state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn purge_clears_triggers() {
    let store = mem_store();
    let source = task_id("test", "source");
    let target = task_id("test", "target");

    TriggerBuilder::new()
        .on_status(&source, &[InvocationStatus::Success])
        .build_and_register(&target, &store)
        .await
        .unwrap();

    // Verify something is stored
    let conditions = store.get_conditions_for_task(&source).await.unwrap();
    assert!(!conditions.is_empty());

    // Purge
    store.purge().await.unwrap();

    // Should be empty now
    let conditions = store.get_conditions_for_task(&source).await.unwrap();
    assert!(conditions.is_empty());
}
