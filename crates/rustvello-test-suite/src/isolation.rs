//! App-ID isolation tests.
//!
//! These tests verify that two backends sharing the same underlying
//! database/connection but created with different `app_id` values
//! cannot see each other's data.

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::InvocationStatus;
use rustvello_proto::trigger::{EventCondition, TriggerCondition};

use crate::helpers::test_task_id;

// ── Broker ──────────────────────────────────────────────────────────

/// Invocations routed through broker A must not be visible to broker B.
pub async fn test_broker_isolation(a: &dyn Broker, b: &dyn Broker) {
    let inv = InvocationId::new();
    a.route_invocation(&inv).await.unwrap();

    // B must not see A's invocation
    let got_b = b.retrieve_invocation(None).await.unwrap();
    assert_eq!(got_b, None, "Broker B must not see Broker A's data");

    // A still retrieves its own invocation
    let got_a = a.retrieve_invocation(None).await.unwrap();
    assert_eq!(got_a, Some(inv));
}

// ── Orchestrator ────────────────────────────────────────────────────

/// Invocations registered in orchestrator A must not appear in B.
pub async fn test_orchestrator_isolation(a: &dyn Orchestrator, b: &dyn Orchestrator) {
    let task_id = test_task_id("iso_orch");
    let call = CallDTO::new(task_id, SerializedArguments::default());

    let inv_id = a.register_invocation(&call).await.unwrap();

    // B must not resolve A's invocation
    let got_b = b.get_invocation_status(&inv_id).await;
    assert!(
        got_b.is_err(),
        "Orchestrator B must not resolve Orchestrator A's invocation"
    );

    // A sees it
    let got_a = a.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(got_a.status, InvocationStatus::Registered);
}

// ── State backend ───────────────────────────────────────────────────

/// Calls upserted by state A must not be visible to state B.
pub async fn test_state_backend_isolation(a: &dyn StateBackend, b: &dyn StateBackend) {
    let task_id = test_task_id("iso_state");
    let call = CallDTO::new(task_id.clone(), SerializedArguments::default());
    let call_id = call.call_id.clone();
    let inv_id = InvocationId::new();
    let inv = InvocationDTO::new(inv_id.clone(), task_id, call_id.clone());

    a.upsert_invocation(&inv, &call).await.unwrap();

    // B must not see A's data
    let got_b = b.get_call(&call_id).await;
    assert!(got_b.is_err(), "State B must not see State A's call");

    let got_b_inv = b.get_invocation(&inv_id).await;
    assert!(
        got_b_inv.is_err(),
        "State B must not see State A's invocation"
    );

    // A sees it
    a.get_call(&call_id).await.unwrap();
    a.get_invocation(&inv_id).await.unwrap();
}

// ── Client data store ───────────────────────────────────────────────

/// Data stored by CDS A must not be visible to CDS B.
pub async fn test_client_data_store_isolation(a: &dyn ClientDataStore, b: &dyn ClientDataStore) {
    a.store("iso_key", "iso_value").await.unwrap();

    let got_b = b.retrieve("iso_key").await;
    assert!(got_b.is_err(), "CDS B must not see CDS A's data");

    let got_a = a.retrieve("iso_key").await.unwrap();
    assert_eq!(got_a, "iso_value");
}

// ── Trigger store ───────────────────────────────────────────────────

/// Conditions registered by A must not appear in B's queries.
pub async fn test_trigger_store_isolation(a: &dyn TriggerStore, b: &dyn TriggerStore) {
    let condition = TriggerCondition::Event(EventCondition {
        event_code: "iso_test_event".to_string(),
        payload_filter: None,
    });

    let cond_id = a.register_condition(&condition).await.unwrap();

    // B must not see A's condition
    let got_b = b.get_condition(&cond_id).await.unwrap();
    assert!(
        got_b.is_none(),
        "Trigger store B must not see Trigger store A's condition"
    );

    let got_b_events = b.get_event_conditions("iso_test_event").await.unwrap();
    assert!(
        got_b_events.is_empty(),
        "Trigger store B must not see Trigger store A's event conditions"
    );

    // A sees its own condition
    let got_a = a.get_condition(&cond_id).await.unwrap();
    assert!(got_a.is_some());
}

// ── Macro ───────────────────────────────────────────────────────────

/// Generate `#[ignore]` integration tests that exercise app-ID isolation
/// for every backend component.
///
/// The `$setup` expression must return
/// `(container, broker_a, broker_b, orch_a, orch_b, state_a, state_b,
///   trigger_a, trigger_b, cds_a, cds_b)`.
#[macro_export]
macro_rules! async_isolation_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_isolation_broker() {
            let (_c, ba, bb, _, _, _, _, _, _, _, _) = $setup.await;
            $crate::isolation::test_broker_isolation(&ba, &bb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_isolation_orchestrator() {
            let (_c, _, _, oa, ob, _, _, _, _, _, _) = $setup.await;
            $crate::isolation::test_orchestrator_isolation(&oa, &ob).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_isolation_state_backend() {
            let (_c, _, _, _, _, sa, sb, _, _, _, _) = $setup.await;
            $crate::isolation::test_state_backend_isolation(&sa, &sb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_isolation_trigger_store() {
            let (_c, _, _, _, _, _, _, ta, tb, _, _) = $setup.await;
            $crate::isolation::test_trigger_store_isolation(&ta, &tb).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_isolation_client_data_store() {
            let (_c, _, _, _, _, _, _, _, _, ca, cb) = $setup.await;
            $crate::isolation::test_client_data_store_isolation(&ca, &cb).await;
        }
    };
}
