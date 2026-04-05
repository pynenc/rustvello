//! Shared state backend test definitions.
//!
//! Each function tests a specific behavior of the [`StateBackend`] trait.

use rustvello_core::error::TaskError;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::helpers::test_task_id;

fn make_test_data() -> (InvocationDTO, CallDTO) {
    let task_id = test_task_id("test_task");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
    let inv_id = InvocationId::new();
    let inv = InvocationDTO::new(inv_id, task_id, call.call_id.clone());
    (inv, call)
}

/// Upsert and retrieve an invocation.
pub async fn test_upsert_and_get(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    let got = sb.get_invocation(&inv.invocation_id).await.unwrap();
    assert_eq!(got.invocation_id, inv.invocation_id);
    assert_eq!(got.task_id, inv.task_id);
    assert_eq!(got.call_id, inv.call_id);
}

/// Retrieve a call by ID.
pub async fn test_get_call(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    let got = sb.get_call(&call.call_id).await.unwrap();
    assert_eq!(got.call_id, call.call_id);
    assert_eq!(got.task_id, call.task_id);
}

/// Store and retrieve a result.
pub async fn test_store_and_get_result(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    sb.store_result(&inv.invocation_id, r#""hello""#)
        .await
        .unwrap();
    let result = sb.get_result(&inv.invocation_id).await.unwrap();
    assert_eq!(result, Some(r#""hello""#.to_string()));
}

/// Get result returns None when not stored.
pub async fn test_get_result_none(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    let result = sb.get_result(&inv.invocation_id).await.unwrap();
    assert_eq!(result, None);
}

/// Store and retrieve an error.
pub async fn test_store_and_get_error(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    let err = TaskError {
        error_type: "ValueError".to_string(),
        message: "invalid input".to_string(),
        traceback: Some("at line 42".to_string()),
    };
    sb.store_error(&inv.invocation_id, &err).await.unwrap();

    let got = sb.get_error(&inv.invocation_id).await.unwrap().unwrap();
    assert_eq!(got.error_type, "ValueError");
    assert_eq!(got.message, "invalid input");
}

/// Add and retrieve history entries.
pub async fn test_history(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();

    let h1 = InvocationHistory::new(
        inv.invocation_id.clone(),
        InvocationStatusRecord::new(InvocationStatus::Registered, None),
        None,
    );
    let h2 = InvocationHistory::new(
        inv.invocation_id.clone(),
        InvocationStatusRecord::new(InvocationStatus::Pending, None),
        None,
    );
    sb.add_history(&h1).await.unwrap();
    sb.add_history(&h2).await.unwrap();

    let history = sb.get_history(&inv.invocation_id).await.unwrap();
    assert!(history.len() >= 2);
}

/// Purge removes all data.
pub async fn test_purge(sb: &dyn StateBackend) {
    let (inv, call) = make_test_data();
    sb.upsert_invocation(&inv, &call).await.unwrap();
    sb.store_result(&inv.invocation_id, r#""done""#)
        .await
        .unwrap();

    sb.purge().await.unwrap();

    // After purge, getting the invocation should fail
    let result = sb.get_invocation(&inv.invocation_id).await;
    assert!(result.is_err());
}

/// Macro to generate all state backend suite tests.
#[macro_export]
macro_rules! state_backend_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_sb_upsert_and_get() {
            let sb = $setup;
            $crate::state_backend::test_upsert_and_get(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_get_call() {
            let sb = $setup;
            $crate::state_backend::test_get_call(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_store_and_get_result() {
            let sb = $setup;
            $crate::state_backend::test_store_and_get_result(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_get_result_none() {
            let sb = $setup;
            $crate::state_backend::test_get_result_none(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_store_and_get_error() {
            let sb = $setup;
            $crate::state_backend::test_store_and_get_error(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_history() {
            let sb = $setup;
            $crate::state_backend::test_history(&sb).await;
        }

        #[tokio::test]
        async fn suite_sb_purge() {
            let sb = $setup;
            $crate::state_backend::test_purge(&sb).await;
        }
    };
}

/// Async-setup variant of [`state_backend_suite!`] for testcontainers backends.
///
/// `$setup` is an async expression returning `(_guard, state_backend)`.
/// Tests are `#[ignore = "requires Docker"]`.
#[macro_export]
macro_rules! async_state_backend_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_upsert_and_get() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_upsert_and_get(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_get_call() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_get_call(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_store_and_get_result() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_store_and_get_result(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_get_result_none() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_get_result_none(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_store_and_get_error() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_store_and_get_error(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_history() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_history(&sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_sb_purge() {
            let (_c, sb) = $setup.await;
            $crate::state_backend::test_purge(&sb).await;
        }
    };
}
