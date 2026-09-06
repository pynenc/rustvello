//! Shared lifecycle test definitions.
//!
//! These tests exercise cross-component interactions that require a broker,
//! orchestrator, and state backend working together — the core invocation
//! lifecycle: register → route → retrieve → execute → store result.

use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::error::TaskError;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::RunnerId;
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::InvocationStatus;

use crate::helpers::test_task_id;

/// Convenience bundle for passing all three backends.
pub struct BackendTriple {
    pub broker: Arc<dyn Broker>,
    pub orchestrator: Arc<dyn InvocationControlBackend>,
    pub state_backend: Arc<dyn StateBackend>,
}

/// Full happy-path lifecycle: register → route → retrieve → run → store result.
pub async fn test_full_lifecycle_success(b: &BackendTriple) {
    let task_id = test_task_id("lifecycle_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
    let runner_id = RunnerId::new();

    // 1. Register invocation via orchestrator
    let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
    let record = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(record.status, InvocationStatus::Registered);

    // 2. Route via broker
    b.broker.route_invocation(&inv_id).await.unwrap();

    // 3. Retrieve from broker
    let retrieved = b.broker.retrieve_invocation(None).await.unwrap();
    assert_eq!(retrieved, Some(inv_id.clone()));

    // 4. Store invocation data in state backend
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    b.state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();

    // 5. Transition: Registered → Pending → Running → Success
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner_id))
        .await
        .unwrap();

    // 6. Store result
    b.state_backend
        .store_result(&inv_id, r#""done""#)
        .await
        .unwrap();

    // 7. Verify final state
    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);

    let result = b.state_backend.get_result(&inv_id).await.unwrap();
    assert_eq!(result, Some(r#""done""#.to_string()));
}

/// Lifecycle with failure: register → route → retrieve → fail → store error.
pub async fn test_full_lifecycle_failure(b: &BackendTriple) {
    let task_id = test_task_id("failing_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
    let runner_id = RunnerId::new();

    let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
    b.broker.route_invocation(&inv_id).await.unwrap();

    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    b.state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();

    // Transition to Failed
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Failed, Some(&runner_id))
        .await
        .unwrap();

    // Store error
    let err = TaskError {
        error_type: "RuntimeError".to_string(),
        message: "something broke".to_string(),
        traceback: Some("at test".to_string()),
    };
    b.state_backend.store_error(&inv_id, &err).await.unwrap();

    let status = b.orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Failed);

    let got_err = b.state_backend.get_error(&inv_id).await.unwrap().unwrap();
    assert_eq!(got_err.error_type, "RuntimeError");
}

/// Multiple invocations with interleaved execution.
pub async fn test_multiple_invocations(b: &BackendTriple) {
    let task_id = test_task_id("multi_task");
    let runner_id = RunnerId::new();

    // Register and route 3 invocations
    let mut inv_ids = Vec::new();
    for i in 0..3 {
        let mut args = SerializedArguments::new();
        args.insert("n", i.to_string());
        let call = CallDTO::new(task_id.clone(), args);
        let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
        b.broker.route_invocation(&inv_id).await.unwrap();
        inv_ids.push(inv_id);
    }

    // Verify 3 invocations in broker
    assert_eq!(b.broker.count_invocations(None).await.unwrap(), 3);

    // Process them in FIFO order
    for inv_id in &inv_ids {
        let retrieved = b.broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(retrieved.as_ref(), Some(inv_id));

        b.orchestrator
            .set_invocation_status(inv_id, InvocationStatus::Pending, Some(&runner_id))
            .await
            .unwrap();
        b.orchestrator
            .set_invocation_status(inv_id, InvocationStatus::Running, Some(&runner_id))
            .await
            .unwrap();
        b.orchestrator
            .set_invocation_status(inv_id, InvocationStatus::Success, Some(&runner_id))
            .await
            .unwrap();
    }

    // All processed — broker is empty
    assert_eq!(b.broker.count_invocations(None).await.unwrap(), 0);
}

/// Purge across all three backends.
pub async fn test_purge_all_backends(b: &BackendTriple) {
    let task_id = test_task_id("purge_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());

    let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
    b.broker.route_invocation(&inv_id).await.unwrap();

    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    b.state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    b.state_backend
        .store_result(&inv_id, r#""purged""#)
        .await
        .unwrap();

    // Purge all three
    b.broker.purge(None).await.unwrap();
    b.orchestrator.purge().await.unwrap();
    b.state_backend.purge().await.unwrap();

    // Verify everything is gone
    assert_eq!(b.broker.count_invocations(None).await.unwrap(), 0);
    assert_eq!(
        b.orchestrator.count_invocations(None, None).await.unwrap(),
        0
    );
    assert!(b.state_backend.get_invocation(&inv_id).await.is_err());
}

/// Broker and orchestrator counts stay consistent.
pub async fn test_broker_orchestrator_consistency(b: &BackendTriple) {
    let task_id = test_task_id("consistency_task");
    let runner_id = RunnerId::new();

    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
    let inv_id = b.orchestrator.register_invocation(&call).await.unwrap();
    b.broker.route_invocation(&inv_id).await.unwrap();

    // Orchestrator has 1 Registered, broker has 1 queued
    assert_eq!(
        b.orchestrator.count_invocations(None, None).await.unwrap(),
        1
    );
    assert_eq!(b.broker.count_invocations(None).await.unwrap(), 1);

    // Retrieve and process
    let _ = b.broker.retrieve_invocation(None).await.unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner_id))
        .await
        .unwrap();
    b.orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner_id))
        .await
        .unwrap();

    // Broker empty, orchestrator still has the record
    assert_eq!(b.broker.count_invocations(None).await.unwrap(), 0);
    assert_eq!(
        b.orchestrator.count_invocations(None, None).await.unwrap(),
        1
    );
}

/// Macro to generate all lifecycle suite tests for a given backend triple setup.
///
/// # Example
///
/// ```rust,ignore
/// use rustvello_test_suite::{lifecycle_suite, lifecycle::BackendTriple};
/// use std::sync::Arc;
///
/// lifecycle_suite!({
///     BackendTriple {
///         broker: Arc::new(MemBroker::new()),
///         orchestrator: Arc::new(MemOrchestrator::new()),
///         state_backend: Arc::new(MemStateBackend::new()),
///     }
/// });
/// ```
#[macro_export]
macro_rules! lifecycle_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_lifecycle_full_success() {
            let triple = $setup;
            $crate::lifecycle::test_full_lifecycle_success(&triple).await;
        }

        #[tokio::test]
        async fn suite_lifecycle_full_failure() {
            let triple = $setup;
            $crate::lifecycle::test_full_lifecycle_failure(&triple).await;
        }

        #[tokio::test]
        async fn suite_lifecycle_multiple_invocations() {
            let triple = $setup;
            $crate::lifecycle::test_multiple_invocations(&triple).await;
        }

        #[tokio::test]
        async fn suite_lifecycle_purge_all() {
            let triple = $setup;
            $crate::lifecycle::test_purge_all_backends(&triple).await;
        }

        #[tokio::test]
        async fn suite_lifecycle_broker_orchestrator_consistency() {
            let triple = $setup;
            $crate::lifecycle::test_broker_orchestrator_consistency(&triple).await;
        }
    };
}

/// Async-setup variant of [`lifecycle_suite!`] for testcontainers backends.
///
/// `$setup` is an async expression returning `(_guard, BackendTriple)`.
/// Tests are `#[ignore = "requires Docker"]`.
#[macro_export]
macro_rules! async_lifecycle_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_lifecycle_full_success() {
            let (_c, triple) = $setup.await;
            $crate::lifecycle::test_full_lifecycle_success(&triple).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_lifecycle_full_failure() {
            let (_c, triple) = $setup.await;
            $crate::lifecycle::test_full_lifecycle_failure(&triple).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_lifecycle_multiple_invocations() {
            let (_c, triple) = $setup.await;
            $crate::lifecycle::test_multiple_invocations(&triple).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_lifecycle_purge_all() {
            let (_c, triple) = $setup.await;
            $crate::lifecycle::test_purge_all_backends(&triple).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_lifecycle_broker_orchestrator_consistency() {
            let (_c, triple) = $setup.await;
            $crate::lifecycle::test_broker_orchestrator_consistency(&triple).await;
        }
    };
}
