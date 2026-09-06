//! Dedicated concurrency control test suite.
//!
//! Covers scenarios beyond the basic CC tests in `orchestrator.rs`:
//! - Atomic `try_acquire_concurrency_slot` behavior
//! - Multi-pair argument intersection
//! - Unlimited CC type passthrough
//! - Sentinel empty-args behavior
//! - Slot release re-enables acquisition

use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::TaskConfig;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use crate::helpers::test_task_id;

/// `try_acquire_concurrency_slot` returns true and indexes when under the limit.
pub async fn test_try_acquire_slot_under_limit(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_acquire_ok");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(2);

    let mut args = SerializedArguments::new();
    args.insert("x", "1");

    let call = CallDTO::new(task_id.clone(), args.clone());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("acq-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();

    let acquired = orch
        .try_acquire_concurrency_slot(&inv1, &task_id, &config, Some(&args))
        .await
        .unwrap();
    assert!(acquired, "Should acquire slot when under the limit");
}

/// `try_acquire_concurrency_slot` returns false when at the limit.
pub async fn test_try_acquire_slot_at_limit(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_acquire_full");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    let mut args = SerializedArguments::new();
    args.insert("x", "1");

    // Fill the slot
    let call1 = CallDTO::new(task_id.clone(), args.clone());
    let inv1 = orch.register_invocation(&call1).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("acq-runner-1");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    let acquired = orch
        .try_acquire_concurrency_slot(&inv1, &task_id, &config, Some(&args))
        .await
        .unwrap();
    assert!(acquired, "First slot should be acquired");

    // Second invocation should be rejected
    let call2 = CallDTO::new(task_id.clone(), args.clone());
    let inv2 = orch.register_invocation(&call2).await.unwrap();
    let runner2 = rustvello_proto::identifiers::RunnerId::from_string("acq-runner-2");
    orch.set_invocation_status(&inv2, InvocationStatus::Pending, Some(&runner2))
        .await
        .unwrap();
    let acquired = orch
        .try_acquire_concurrency_slot(&inv2, &task_id, &config, Some(&args))
        .await
        .unwrap();
    assert!(!acquired, "Second slot should be rejected at limit");
}

/// After releasing a slot, a new invocation can acquire it.
pub async fn test_try_acquire_after_release(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_release");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    let mut args = SerializedArguments::new();
    args.insert("x", "1");

    let call1 = CallDTO::new(task_id.clone(), args.clone());
    let inv1 = orch.register_invocation(&call1).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("rel-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.try_acquire_concurrency_slot(&inv1, &task_id, &config, Some(&args))
        .await
        .unwrap();

    // Release the slot
    orch.remove_from_concurrency_index(&inv1).await.unwrap();

    // New invocation should succeed
    let call2 = CallDTO::new(task_id.clone(), args.clone());
    let inv2 = orch.register_invocation(&call2).await.unwrap();
    let runner2 = rustvello_proto::identifiers::RunnerId::from_string("rel-runner-2");
    orch.set_invocation_status(&inv2, InvocationStatus::Pending, Some(&runner2))
        .await
        .unwrap();
    let acquired = orch
        .try_acquire_concurrency_slot(&inv2, &task_id, &config, Some(&args))
        .await
        .unwrap();
    assert!(acquired, "Should acquire after release");
}

/// Unlimited CC type always acquires the slot.
pub async fn test_unlimited_cc_always_acquires(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_unlimited");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Unlimited;
    config.running_concurrency = Some(1); // limit is irrelevant for Unlimited

    let mut args = SerializedArguments::new();
    args.insert("x", "1");

    for i in 0..5 {
        let call = CallDTO::new(task_id.clone(), args.clone());
        let inv = orch.register_invocation(&call).await.unwrap();
        let runner = rustvello_proto::identifiers::RunnerId::from_string(format!("unl-{i}"));
        orch.set_invocation_status(&inv, InvocationStatus::Pending, Some(&runner))
            .await
            .unwrap();
        let acquired = orch
            .try_acquire_concurrency_slot(&inv, &task_id, &config, Some(&args))
            .await
            .unwrap();
        assert!(acquired, "Unlimited CC should always acquire (iter {i})");
    }
}

/// Multi-pair arguments: intersection logic blocks only when all pairs match.
pub async fn test_multi_pair_argument_cc(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_multipair");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Argument;
    config.running_concurrency = Some(1);

    // First invocation: user_id=alice, region=us
    let mut args1 = SerializedArguments::new();
    args1.insert("user_id", "alice");
    args1.insert("region", "us");

    let call1 = CallDTO::new(task_id.clone(), args1.clone());
    let inv1 = orch.register_invocation(&call1).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("mp-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&args1))
        .await
        .unwrap();

    // Same user_id, same region → blocked (full intersection)
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args1))
        .await
        .unwrap();
    assert!(!allowed, "Exact same multi-pair args should be blocked");

    // Same user_id, different region → allowed (partial overlap, not full intersection)
    let mut args_diff_region = SerializedArguments::new();
    args_diff_region.insert("user_id", "alice");
    args_diff_region.insert("region", "eu");
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args_diff_region))
        .await
        .unwrap();
    assert!(
        allowed,
        "Different region with same user_id should be allowed (no full intersection)"
    );

    // Different user_id, same region → allowed
    let mut args_diff_user = SerializedArguments::new();
    args_diff_user.insert("user_id", "bob");
    args_diff_user.insert("region", "us");
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args_diff_user))
        .await
        .unwrap();
    assert!(
        allowed,
        "Different user_id with same region should be allowed"
    );
}

/// Empty args use the sentinel `("", "")` pair — task-scoped CC behavior.
pub async fn test_sentinel_empty_args(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_sentinel");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    // Empty SerializedArguments → cc_arg_pairs() returns [("", "")]
    let args = SerializedArguments::new();
    assert_eq!(
        args.cc_arg_pairs(),
        vec![(String::new(), String::new())],
        "Empty args should produce sentinel pair"
    );

    let call = CallDTO::new(task_id.clone(), args.clone());
    let inv1 = orch.register_invocation(&call).await.unwrap();
    let runner = rustvello_proto::identifiers::RunnerId::from_string("sent-runner");
    orch.set_invocation_status(&inv1, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&args))
        .await
        .unwrap();

    // Second invocation with same empty args should be blocked
    let allowed = orch
        .check_running_concurrency(&task_id, &config, Some(&args))
        .await
        .unwrap();
    assert!(
        !allowed,
        "Sentinel empty args should block second invocation"
    );
}

/// Empty task-level arguments reserve one atomic slot just like populated arguments.
pub async fn test_atomic_sentinel_empty_args(orch: &dyn InvocationControlBackend) {
    let task_id = test_task_id("cc_atomic_sentinel");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    let args = SerializedArguments::new();

    let first = orch
        .register_invocation(&CallDTO::new(task_id.clone(), args.clone()))
        .await
        .unwrap();
    let second = orch
        .register_invocation(&CallDTO::new(task_id.clone(), args.clone()))
        .await
        .unwrap();

    assert!(orch
        .try_acquire_concurrency_slot(&first, &task_id, &config, Some(&args))
        .await
        .unwrap());
    assert!(!orch
        .try_acquire_concurrency_slot(&second, &task_id, &config, Some(&args))
        .await
        .unwrap());
}

/// Macro to generate all concurrency control suite tests.
///
/// # Example
///
/// ```rust,ignore
/// use rustvello_test_suite::concurrency_suite;
/// use rustvello_mem::orchestrator::MemOrchestrator;
///
/// concurrency_suite!(MemOrchestrator::new());
/// ```
#[macro_export]
macro_rules! concurrency_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_cc_try_acquire_under_limit() {
            let orch = $setup;
            $crate::concurrency::test_try_acquire_slot_under_limit(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_try_acquire_at_limit() {
            let orch = $setup;
            $crate::concurrency::test_try_acquire_slot_at_limit(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_try_acquire_after_release() {
            let orch = $setup;
            $crate::concurrency::test_try_acquire_after_release(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_unlimited_always_acquires() {
            let orch = $setup;
            $crate::concurrency::test_unlimited_cc_always_acquires(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_multi_pair_argument() {
            let orch = $setup;
            $crate::concurrency::test_multi_pair_argument_cc(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_sentinel_empty_args() {
            let orch = $setup;
            $crate::concurrency::test_sentinel_empty_args(&orch).await;
        }

        #[tokio::test]
        async fn suite_cc_atomic_sentinel_empty_args() {
            let orch = $setup;
            $crate::concurrency::test_atomic_sentinel_empty_args(&orch).await;
        }
    };
}

/// Async-setup variant for testcontainers backends.
#[macro_export]
macro_rules! async_concurrency_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_try_acquire_under_limit() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_try_acquire_slot_under_limit(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_try_acquire_at_limit() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_try_acquire_slot_at_limit(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_try_acquire_after_release() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_try_acquire_after_release(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_unlimited_always_acquires() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_unlimited_cc_always_acquires(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_multi_pair_argument() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_multi_pair_argument_cc(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_sentinel_empty_args() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_sentinel_empty_args(&orch).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cc_atomic_sentinel_empty_args() {
            let (_c, orch) = $setup.await;
            $crate::concurrency::test_atomic_sentinel_empty_args(&orch).await;
        }
    };
}
