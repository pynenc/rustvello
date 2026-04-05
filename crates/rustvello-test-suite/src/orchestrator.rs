//! Shared orchestrator test definitions.
//!
//! Each function tests a specific behavior of the [`Orchestrator`] trait.

use rustvello_core::orchestrator::Orchestrator;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatus;

use crate::helpers::test_task_id;

fn make_call(task_id: &TaskId) -> CallDTO {
    CallDTO::new(task_id.clone(), SerializedArguments::default())
}

/// Register an invocation and verify initial status is Registered.
pub async fn test_register_invocation(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let record = orch.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(record.status, InvocationStatus::Registered);
}

/// Transition: Registered → Pending → Running → Success.
pub async fn test_status_transitions(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner_id = rustvello_proto::identifiers::RunnerId::new();
    let rec = orch
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    assert_eq!(rec.status, InvocationStatus::Pending);

    let rec = orch
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();
    assert_eq!(rec.status, InvocationStatus::Running);

    let rec = orch
        .set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner_id))
        .await
        .unwrap();
    assert_eq!(rec.status, InvocationStatus::Success);
}

/// Query invocations by task.
pub async fn test_get_invocations_by_task(orch: &dyn Orchestrator) {
    let task_a = test_task_id("task_a");
    let task_b = test_task_id("task_b");

    let call_a = make_call(&task_a);
    let call_b = make_call(&task_b);

    let inv_a = orch.register_invocation(&call_a).await.unwrap();
    let _inv_b = orch.register_invocation(&call_b).await.unwrap();

    let invs = orch.get_invocations_by_task(&task_a).await.unwrap();
    assert!(invs.contains(&inv_a));
    assert_eq!(invs.len(), 1);
}

/// Query invocations by status.
pub async fn test_get_invocations_by_status(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let registered = orch
        .get_invocations_by_status(InvocationStatus::Registered, None)
        .await
        .unwrap();
    assert!(registered.contains(&inv_id));

    let runner_id = rustvello_proto::identifiers::RunnerId::from_string("test-runner");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();

    let registered = orch
        .get_invocations_by_status(InvocationStatus::Registered, None)
        .await
        .unwrap();
    assert!(!registered.contains(&inv_id));

    let pending = orch
        .get_invocations_by_status(InvocationStatus::Pending, None)
        .await
        .unwrap();
    assert!(pending.contains(&inv_id));
}

/// Concurrency control check.
pub async fn test_concurrency_control(orch: &dyn Orchestrator) {
    let task_id = test_task_id("cc_task");
    let config = TaskConfig::default();

    let result = orch
        .check_running_concurrency(&task_id, &config, None)
        .await
        .unwrap();
    // With default config (no concurrency limit), should always authorize
    assert!(result);
}

/// Count invocations with optional task/status filters.
pub async fn test_count_invocations(orch: &dyn Orchestrator) {
    let task_a = test_task_id("count_a");
    let task_b = test_task_id("count_b");

    let call_a1 = make_call(&task_a);
    let call_a2 = make_call(&task_a);
    let call_b1 = make_call(&task_b);

    let _inv_a1 = orch.register_invocation(&call_a1).await.unwrap();
    let inv_a2 = orch.register_invocation(&call_a2).await.unwrap();
    let _inv_b1 = orch.register_invocation(&call_b1).await.unwrap();

    // Count all
    let total = orch.count_invocations(None, None).await.unwrap();
    assert_eq!(total, 3);

    // Count by task
    let count_a = orch.count_invocations(Some(&task_a), None).await.unwrap();
    assert_eq!(count_a, 2);

    // Count by status
    let count_reg = orch
        .count_invocations(None, Some(&[InvocationStatus::Registered]))
        .await
        .unwrap();
    assert_eq!(count_reg, 3);

    // Move one to Pending, then count
    let runner_id = rustvello_proto::identifiers::RunnerId::from_string("counter");
    orch.set_invocation_status(&inv_a2, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    let count_reg_a = orch
        .count_invocations(Some(&task_a), Some(&[InvocationStatus::Registered]))
        .await
        .unwrap();
    assert_eq!(count_reg_a, 1);
}

/// Paginate invocation ids.
pub async fn test_get_invocation_ids_paginated(orch: &dyn Orchestrator) {
    let task_id = test_task_id("paged");
    for _ in 0..5 {
        let call = make_call(&task_id);
        orch.register_invocation(&call).await.unwrap();
    }

    let page1 = orch
        .get_invocation_ids_paginated(Some(&task_id), None, 2, 0)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2);

    let page2 = orch
        .get_invocation_ids_paginated(Some(&task_id), None, 2, 2)
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);

    // No overlap
    for id in &page1 {
        assert!(!page2.contains(id));
    }
}

/// Filter a set of ids by status.
pub async fn test_filter_by_status(orch: &dyn Orchestrator) {
    let task_id = test_task_id("filter_task");
    let c1 = make_call(&task_id);
    let c2 = make_call(&task_id);
    let c3 = make_call(&task_id);

    let inv1 = orch.register_invocation(&c1).await.unwrap();
    let inv2 = orch.register_invocation(&c2).await.unwrap();
    let inv3 = orch.register_invocation(&c3).await.unwrap();

    let runner_id = rustvello_proto::identifiers::RunnerId::from_string("filter-runner");
    orch.set_invocation_status(&inv2, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();

    let all = vec![inv1.clone(), inv2.clone(), inv3.clone()];
    let registered = orch
        .filter_by_status(&all, &[InvocationStatus::Registered])
        .await
        .unwrap();
    assert_eq!(registered.len(), 2);
    assert!(registered.contains(&inv1));
    assert!(registered.contains(&inv3));
    assert!(!registered.contains(&inv2));
}

/// Active runner ids based on heartbeat timeout.
pub async fn test_get_active_runner_ids(orch: &dyn Orchestrator) {
    let runner_id = rustvello_proto::identifiers::RunnerId::from_string("active-runner");
    orch.register_heartbeat(&runner_id, false).await.unwrap();

    let active = orch.get_active_runner_ids(60).await.unwrap();
    assert!(active.iter().any(|r| r.to_string() == "active-runner"));
}

/// Purge clears invocations.
pub async fn test_purge(orch: &dyn Orchestrator) {
    let task_id = test_task_id("purge_task");
    let call = make_call(&task_id);
    let _inv = orch.register_invocation(&call).await.unwrap();

    let before = orch.count_invocations(None, None).await.unwrap();
    assert!(before >= 1);

    orch.purge().await.unwrap();

    let after = orch.count_invocations(None, None).await.unwrap();
    assert_eq!(after, 0);
}

// ===========================================================================
// Negative path tests
// ===========================================================================

/// Reject an invalid status transition (Registered → Running skips Pending).
pub async fn test_invalid_status_transition(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner_id = rustvello_proto::identifiers::RunnerId::new();
    let result = orch
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await;
    assert!(result.is_err(), "Registered → Running should be rejected");
}

/// Reject transition from a terminal state (Success → Pending).
pub async fn test_terminal_state_no_transition(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner_id = rustvello_proto::identifiers::RunnerId::new();
    // Drive to Success
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner_id))
        .await
        .unwrap();

    // Any transition from Success should fail
    let result = orch
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await;
    assert!(result.is_err(), "Success → Pending should be rejected");
}

/// Get status of a non-existent invocation should return an error.
pub async fn test_get_missing_invocation(orch: &dyn Orchestrator) {
    let fake_id = rustvello_proto::identifiers::InvocationId::from_string(
        "nonexistent::task||missing-call-id".to_string(),
    );
    let result = orch.get_invocation_status(&fake_id).await;
    assert!(result.is_err(), "Missing invocation should return error");
}

/// CC type = Task with running_concurrency = 1: blocks second invocation.
pub async fn test_cc_task_blocks_duplicate(orch: &dyn Orchestrator) {
    let task_id = test_task_id("cc_task_block");
    let mut config = TaskConfig::default();
    config.concurrency_control = rustvello_proto::status::ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    let call = make_call(&task_id);
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("cc-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();

    // Task-level CC: second invocation should be blocked
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    assert!(!allowed, "Task CC should block second invocation");

    // Remove from index → allowed again
    orch.remove_from_concurrency_index(&inv1).await.unwrap();
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    assert!(allowed, "Should be allowed after removing from index");
}

/// CC type = Argument: same args blocked, different args allowed.
pub async fn test_cc_argument_same_args_blocked(orch: &dyn Orchestrator) {
    let task_id = test_task_id("cc_arg_block");
    let mut config = TaskConfig::default();
    config.concurrency_control = rustvello_proto::status::ConcurrencyControlType::Argument;
    config.running_concurrency = Some(1);

    let mut args1 = SerializedArguments::new();
    args1.insert("x", "1");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "2");

    let call = CallDTO::new(task_id.clone(), args1.clone());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("cc-arg-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&args1))
        .await
        .unwrap();

    // Same args → blocked
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args1))
        .await
        .unwrap();
    assert!(!allowed, "Same args should be blocked");

    // Different args → allowed
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args2))
        .await
        .unwrap();
    assert!(allowed, "Different args should be allowed");
}

/// CC type = Argument with key_arguments subset: only the subset determines blocking.
pub async fn test_cc_key_arguments_subset(orch: &dyn Orchestrator) {
    let task_id = test_task_id("cc_keyarg");
    let mut config = TaskConfig::default();
    config.concurrency_control = rustvello_proto::status::ConcurrencyControlType::Argument;
    config.running_concurrency = Some(1);
    config.key_arguments = vec!["order_id".to_string()];

    // Index with key_arguments subset (only "order_id" matters)
    let mut key_args = SerializedArguments::new();
    key_args.insert("order_id", "100");

    let mut full_args1 = SerializedArguments::new();
    full_args1.insert("order_id", "100");
    full_args1.insert("quantity", "5");

    let call = CallDTO::new(task_id.clone(), full_args1.clone());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("key-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    // Index with only the key_argument subset
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&key_args))
        .await
        .unwrap();

    // Same order_id (different quantity) → blocked (keyed on order_id only)
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&key_args))
        .await
        .unwrap();
    assert!(!allowed, "Same key_argument value should be blocked");

    // Different order_id → allowed
    let mut diff_key = SerializedArguments::new();
    diff_key.insert("order_id", "200");
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&diff_key))
        .await
        .unwrap();
    assert!(allowed, "Different key_argument value should be allowed");
}

/// Record and retrieve atomic service executions.
///
/// Backends that don't override the default no-op trait impls will return an
/// empty timeline — the test gracefully skips assertions in that case.
pub async fn test_atomic_service_timeline(orch: &dyn Orchestrator) {
    let runner_id = rustvello_proto::identifiers::RunnerId::from_string("atomic-runner");
    let now = chrono::Utc::now();
    let start = now - chrono::Duration::seconds(10);
    let end = now - chrono::Duration::seconds(5);

    orch.record_atomic_service_execution(&runner_id, start, end)
        .await
        .unwrap();

    let timeline = orch.get_atomic_service_timeline().await.unwrap();
    // Default trait impl returns empty vec; only assert when the backend
    // actually persists atomic-service executions.
    if !timeline.is_empty() {
        assert_eq!(timeline[0].runner_id, "atomic-runner");
        assert!(timeline[0].duration_secs() > 0.0);
    }
}

/// Stale pending invocations are detected after timeout.
pub async fn test_stale_pending_detection(orch: &dyn Orchestrator) {
    let task_id = test_task_id("stale_pending");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner = rustvello_proto::identifiers::RunnerId::from_string("dead-runner");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();

    // Wait briefly then check with 0-second timeout (immediate stale)
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let stale = orch.get_stale_pending_invocations(0).await.unwrap();
    assert!(
        stale.contains(&inv_id),
        "Should detect stale pending invocation"
    );
}

/// Stale running invocations are detected after timeout.
pub async fn test_stale_running_detection(orch: &dyn Orchestrator) {
    let task_id = test_task_id("stale_running");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner = rustvello_proto::identifiers::RunnerId::from_string("dead-runner-2");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let stale = orch.get_stale_running_invocations(0).await.unwrap();
    assert!(
        stale.contains(&inv_id),
        "Should detect stale running invocation"
    );
}

/// Ownership violation: a different runner cannot transition a Pending invocation.
pub async fn test_ownership_violation(orch: &dyn Orchestrator) {
    let task_id = test_task_id("test_task");
    let call = make_call(&task_id);
    let inv_id = orch.register_invocation(&call).await.unwrap();

    let runner_a = rustvello_proto::identifiers::RunnerId::new();
    let runner_b = rustvello_proto::identifiers::RunnerId::new();

    // runner_a claims the invocation
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_a))
        .await
        .unwrap();

    // runner_b tries to transition it — should be rejected
    let result = orch
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_b))
        .await;
    assert!(
        result.is_err(),
        "Different runner should not own this invocation"
    );
}

/// Macro to generate all orchestrator suite tests.
#[macro_export]
macro_rules! orchestrator_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_orch_register_invocation() {
            let orch = $setup;
            $crate::orchestrator::test_register_invocation(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_status_transitions() {
            let orch = $setup;
            $crate::orchestrator::test_status_transitions(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_get_invocations_by_task() {
            let orch = $setup;
            $crate::orchestrator::test_get_invocations_by_task(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_get_invocations_by_status() {
            let orch = $setup;
            $crate::orchestrator::test_get_invocations_by_status(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_concurrency_control() {
            let orch = $setup;
            $crate::orchestrator::test_concurrency_control(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_count_invocations() {
            let orch = $setup;
            $crate::orchestrator::test_count_invocations(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_get_invocation_ids_paginated() {
            let orch = $setup;
            $crate::orchestrator::test_get_invocation_ids_paginated(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_filter_by_status() {
            let orch = $setup;
            $crate::orchestrator::test_filter_by_status(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_get_active_runner_ids() {
            let orch = $setup;
            $crate::orchestrator::test_get_active_runner_ids(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_purge() {
            let orch = $setup;
            $crate::orchestrator::test_purge(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_invalid_status_transition() {
            let orch = $setup;
            $crate::orchestrator::test_invalid_status_transition(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_terminal_state_no_transition() {
            let orch = $setup;
            $crate::orchestrator::test_terminal_state_no_transition(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_get_missing_invocation() {
            let orch = $setup;
            $crate::orchestrator::test_get_missing_invocation(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_ownership_violation() {
            let orch = $setup;
            $crate::orchestrator::test_ownership_violation(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_cc_task_blocks_duplicate() {
            let orch = $setup;
            $crate::orchestrator::test_cc_task_blocks_duplicate(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_cc_argument_same_args_blocked() {
            let orch = $setup;
            $crate::orchestrator::test_cc_argument_same_args_blocked(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_cc_key_arguments_subset() {
            let orch = $setup;
            $crate::orchestrator::test_cc_key_arguments_subset(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_atomic_service_timeline() {
            let orch = $setup;
            $crate::orchestrator::test_atomic_service_timeline(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_stale_pending_detection() {
            let orch = $setup;
            $crate::orchestrator::test_stale_pending_detection(&orch).await;
        }

        #[tokio::test]
        async fn suite_orch_stale_running_detection() {
            let orch = $setup;
            $crate::orchestrator::test_stale_running_detection(&orch).await;
        }
    };
}

/// Async-setup variant of [`orchestrator_suite!`] for testcontainers backends.
///
/// `$setup` is an async expression returning `(_guard, orchestrator)`.
/// Tests are `#[ignore = "requires Docker"]`.
#[macro_export]
macro_rules! async_orchestrator_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_register_invocation() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_register_invocation(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_status_transitions() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_status_transitions(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_get_invocations_by_task() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_get_invocations_by_task(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_get_invocations_by_status() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_get_invocations_by_status(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_concurrency_control() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_concurrency_control(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_count_invocations() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_count_invocations(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_get_invocation_ids_paginated() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_get_invocation_ids_paginated(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_filter_by_status() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_filter_by_status(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_get_active_runner_ids() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_get_active_runner_ids(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_purge() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_purge(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_invalid_status_transition() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_invalid_status_transition(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_terminal_state_no_transition() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_terminal_state_no_transition(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_get_missing_invocation() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_get_missing_invocation(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_ownership_violation() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_ownership_violation(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_cc_task_blocks_duplicate() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_cc_task_blocks_duplicate(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_cc_argument_same_args_blocked() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_cc_argument_same_args_blocked(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_cc_key_arguments_subset() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_cc_key_arguments_subset(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_atomic_service_timeline() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_atomic_service_timeline(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_stale_pending_detection() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_stale_pending_detection(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_orch_stale_running_detection() {
            let (_c, orch) = $setup.await;
            $crate::orchestrator::test_stale_running_detection(&orch).await;
        }
    };
}
