//! Shared trigger store test definitions.
//!
//! Each function tests a specific behavior of the [`TriggerStore`] trait.
//! Backend crates call these with their concrete implementation.

use chrono::Utc;
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatus;
use rustvello_proto::trigger::{
    ConditionContext, ConditionId, CronCondition, EventCondition, EventContext, EventQuery,
    EventRecord, ExceptionCondition, ResultCondition, StatusCondition, TriggerCondition,
    TriggerDefinitionDTO, TriggerDefinitionId, TriggerLogic, TriggerRunId, TriggerRunParticipant,
    TriggerRunQuery, TriggerRunRecord, ValidCondition,
};

use crate::helpers::test_task_id;

/// Register a condition and retrieve it by ID.
pub async fn test_register_and_get_condition(store: &dyn TriggerStore) {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    let id = store.register_condition(&cond).await.unwrap();
    let got = store.get_condition(&id).await.unwrap();
    assert!(got.is_some());
    assert_eq!(got.unwrap().condition_id(), id);
}

/// Query conditions by source task.
pub async fn test_get_conditions_for_task(store: &dyn TriggerStore) {
    let task_id = test_task_id("task");
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: task_id.clone(),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    store.register_condition(&cond).await.unwrap();

    let conds = store.get_conditions_for_task(&task_id).await.unwrap();
    assert_eq!(conds.len(), 1);

    let other = TaskId::new("mod", "other");
    let conds = store.get_conditions_for_task(&other).await.unwrap();
    assert!(conds.is_empty());
}

/// Query event conditions by event code.
pub async fn test_get_event_conditions(store: &dyn TriggerStore) {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    store.register_condition(&cond).await.unwrap();

    let got = store.get_event_conditions("payment").await.unwrap();
    assert_eq!(got.len(), 1);

    let got = store.get_event_conditions("other").await.unwrap();
    assert!(got.is_empty());
}

/// Query cron conditions.
pub async fn test_get_cron_conditions(store: &dyn TriggerStore) {
    let cond = TriggerCondition::Cron(CronCondition {
        cron_expression: "* * * * *".to_string(),
        min_interval_seconds: 50,
    });
    store.register_condition(&cond).await.unwrap();

    let conds = store.get_cron_conditions().await.unwrap();
    assert_eq!(conds.len(), 1);
}

/// Register a trigger definition and retrieve it.
pub async fn test_register_and_get_trigger(store: &dyn TriggerStore) {
    let task_id = test_task_id("target");
    let cond_ids = vec![ConditionId::from("c1".to_string())];
    let trigger_id =
        TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::Or);

    let trigger = TriggerDefinitionDTO {
        trigger_id: trigger_id.clone(),
        task_id,
        condition_ids: cond_ids,
        logic: TriggerLogic::Or,
        argument_template: Some(serde_json::json!({"key": "value"})),
    };
    store.register_trigger(&trigger).await.unwrap();

    let got = store.get_trigger(&trigger_id).await.unwrap();
    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(got.logic, TriggerLogic::Or);
    assert!(got.argument_template.is_some());
}

/// Query triggers linked to a condition.
pub async fn test_get_triggers_for_condition(store: &dyn TriggerStore) {
    let cond_id = ConditionId::from("c1".to_string());
    let task_id = test_task_id("target");
    let trigger = TriggerDefinitionDTO {
        trigger_id: TriggerDefinitionDTO::compute_trigger_id(
            &task_id,
            &[cond_id.clone()],
            TriggerLogic::Or,
        ),
        task_id,
        condition_ids: vec![cond_id.clone()],
        logic: TriggerLogic::Or,
        argument_template: None,
    };
    store.register_trigger(&trigger).await.unwrap();

    let triggers = store.get_triggers_for_condition(&cond_id).await.unwrap();
    assert_eq!(triggers.len(), 1);
}

/// Remove all triggers for a task.
pub async fn test_remove_triggers_for_task(store: &dyn TriggerStore) {
    let task_id = test_task_id("target");
    let trigger = TriggerDefinitionDTO {
        trigger_id: TriggerDefinitionId::from("t1".to_string()),
        task_id: task_id.clone(),
        condition_ids: vec![],
        logic: TriggerLogic::And,
        argument_template: None,
    };
    store.register_trigger(&trigger).await.unwrap();

    let removed = store.remove_triggers_for_task(&task_id).await.unwrap();
    assert_eq!(removed, 1);

    let got = store
        .get_trigger(&TriggerDefinitionId::from("t1".to_string()))
        .await
        .unwrap();
    assert!(got.is_none());
}

/// Record, retrieve, and clear valid conditions.
pub async fn test_valid_condition_lifecycle(store: &dyn TriggerStore) {
    let vc = ValidCondition::new(
        ConditionId::from("c1".to_string()),
        ConditionContext::Event(EventContext {
            event_id: "e1".to_string(),
            event_code: "test".to_string(),
            payload: serde_json::json!({}),
        }),
    );
    let vc_id = vc.valid_condition_id.clone();

    store.record_valid_condition(&vc).await.unwrap();
    let vcs = store.get_valid_conditions().await.unwrap();
    assert_eq!(vcs.len(), 1);

    store.clear_valid_conditions(&[vc_id]).await.unwrap();
    let vcs = store.get_valid_conditions().await.unwrap();
    assert!(vcs.is_empty());
}

/// Cron execution with optimistic locking.
pub async fn test_cron_execution_optimistic_lock(store: &dyn TriggerStore) {
    let cond_id = ConditionId::from("cron1".to_string());
    let now = Utc::now();

    // First store succeeds (no previous execution)
    assert!(store
        .store_cron_execution(&cond_id, now, None)
        .await
        .unwrap());

    // Second store with wrong expected value fails
    assert!(!store
        .store_cron_execution(&cond_id, now, None)
        .await
        .unwrap());

    // Store with correct expected value succeeds
    let later = now + chrono::Duration::seconds(60);
    assert!(store
        .store_cron_execution(&cond_id, later, Some(now))
        .await
        .unwrap());
}

/// Claim trigger run deduplication.
pub async fn test_claim_trigger_run_dedup(store: &dyn TriggerStore) {
    let run_id = TriggerRunId::from("run-1".to_string());

    assert!(store.claim_trigger_run(&run_id).await.unwrap());
    assert!(!store.claim_trigger_run(&run_id).await.unwrap());
}

/// Purge clears all trigger data.
pub async fn test_purge_clears_all(store: &dyn TriggerStore) {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "test".to_string(),
        payload_filter: None,
    });
    store.register_condition(&cond).await.unwrap();
    store.purge().await.unwrap();

    let got = store.get_event_conditions("test").await.unwrap();
    assert!(got.is_empty());
}

/// Multiple cron conditions can coexist and be retrieved.
pub async fn test_multiple_cron_conditions(store: &dyn TriggerStore) {
    let cond1 = TriggerCondition::Cron(CronCondition {
        cron_expression: "0 * * * *".to_string(), // every hour
        min_interval_seconds: 50,
    });
    let cond2 = TriggerCondition::Cron(CronCondition {
        cron_expression: "*/5 * * * *".to_string(), // every 5 minutes
        min_interval_seconds: 240,
    });
    let cond3 = TriggerCondition::Cron(CronCondition {
        cron_expression: "0 0 * * *".to_string(), // daily at midnight
        min_interval_seconds: 3500,
    });

    let id1 = store.register_condition(&cond1).await.unwrap();
    let id2 = store.register_condition(&cond2).await.unwrap();
    let id3 = store.register_condition(&cond3).await.unwrap();

    // All three should be distinct
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);

    let crons = store.get_cron_conditions().await.unwrap();
    assert_eq!(crons.len(), 3);
}

/// Cron execution history: get_last_cron_execution returns None initially,
/// then the stored value.
pub async fn test_cron_execution_history(store: &dyn TriggerStore) {
    let cond_id = ConditionId::from("cron_hist_1".to_string());

    // Initially no execution
    let last = store.get_last_cron_execution(&cond_id).await.unwrap();
    assert!(last.is_none(), "Should have no initial execution");

    // Store an execution
    let now = Utc::now();
    assert!(store
        .store_cron_execution(&cond_id, now, None)
        .await
        .unwrap());

    // Now last execution should be set
    let last = store.get_last_cron_execution(&cond_id).await.unwrap();
    assert!(last.is_some(), "Should have recorded execution");
    let diff = (last.unwrap() - now).num_milliseconds().unsigned_abs();
    assert!(diff < 1000, "Stored timestamp should match");
}

/// Sequential cron executions with correct optimistic lock chaining.
pub async fn test_cron_sequential_executions(store: &dyn TriggerStore) {
    let cond_id = ConditionId::from("cron_seq_1".to_string());
    let t1 = Utc::now() - chrono::Duration::seconds(120);
    let t2 = Utc::now() - chrono::Duration::seconds(60);
    let t3 = Utc::now();

    // First execution (no previous)
    assert!(store
        .store_cron_execution(&cond_id, t1, None)
        .await
        .unwrap());

    // Second with correct expected (t1)
    assert!(store
        .store_cron_execution(&cond_id, t2, Some(t1))
        .await
        .unwrap());

    // Third with wrong expected (t1 instead of t2) → rejected
    assert!(!store
        .store_cron_execution(&cond_id, t3, Some(t1))
        .await
        .unwrap());

    // Third with correct expected (t2)
    assert!(store
        .store_cron_execution(&cond_id, t3, Some(t2))
        .await
        .unwrap());

    // Verify final state
    let last = store.get_last_cron_execution(&cond_id).await.unwrap();
    assert!(last.is_some());
    let diff = (last.unwrap() - t3).num_milliseconds().unsigned_abs();
    assert!(diff < 1000, "Final timestamp should be t3");
}

/// AND trigger logic requires all conditions to be valid.
pub async fn test_trigger_and_logic(store: &dyn TriggerStore) {
    let task_id = test_task_id("and_target");
    let cond1 = TriggerCondition::Event(EventCondition {
        event_code: "evt_a".to_string(),
        payload_filter: None,
    });
    let cond2 = TriggerCondition::Event(EventCondition {
        event_code: "evt_b".to_string(),
        payload_filter: None,
    });
    let id1 = store.register_condition(&cond1).await.unwrap();
    let id2 = store.register_condition(&cond2).await.unwrap();

    let trigger = TriggerDefinitionDTO {
        trigger_id: TriggerDefinitionDTO::compute_trigger_id(
            &task_id,
            &[id1.clone(), id2.clone()],
            TriggerLogic::And,
        ),
        task_id,
        condition_ids: vec![id1.clone(), id2.clone()],
        logic: TriggerLogic::And,
        argument_template: None,
    };
    store.register_trigger(&trigger).await.unwrap();

    // Verify both conditions are linked
    let triggers1 = store.get_triggers_for_condition(&id1).await.unwrap();
    let triggers2 = store.get_triggers_for_condition(&id2).await.unwrap();
    assert_eq!(triggers1.len(), 1);
    assert_eq!(triggers2.len(), 1);
    assert_eq!(triggers1[0].logic, TriggerLogic::And);
    assert_eq!(triggers1[0].condition_ids.len(), 2);
}

/// `get_all_conditions` returns every condition type, not just cron.
pub async fn test_get_all_conditions(store: &dyn TriggerStore) {
    // Register one condition per type (Cron, Event, Status, Result, Exception)
    let task_id = test_task_id("all_cond_task");

    let cron = TriggerCondition::Cron(CronCondition {
        cron_expression: "0 0 * * *".to_string(),
        min_interval_seconds: 50,
    });
    let event = TriggerCondition::Event(EventCondition {
        event_code: "all_evt".to_string(),
        payload_filter: None,
    });
    let status = TriggerCondition::Status(StatusCondition {
        task_id: task_id.clone(),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    let result = TriggerCondition::Result(ResultCondition {
        task_id: task_id.clone(),
        argument_filter: None,
        result_filter: None,
    });
    let exception = TriggerCondition::Exception(ExceptionCondition {
        task_id: task_id.clone(),
        exception_types: vec!["ValueError".to_string()],
        argument_filter: None,
    });

    let id_cron = store.register_condition(&cron).await.unwrap();
    let id_event = store.register_condition(&event).await.unwrap();
    let id_status = store.register_condition(&status).await.unwrap();
    let id_result = store.register_condition(&result).await.unwrap();
    let id_exception = store.register_condition(&exception).await.unwrap();

    let all = store.get_all_conditions().await.unwrap();
    let all_ids: std::collections::HashSet<String> =
        all.iter().map(|(id, _)| id.as_str().to_owned()).collect();

    assert!(
        all_ids.contains(id_cron.as_str()),
        "get_all_conditions must include Cron conditions"
    );
    assert!(
        all_ids.contains(id_event.as_str()),
        "get_all_conditions must include Event conditions"
    );
    assert!(
        all_ids.contains(id_status.as_str()),
        "get_all_conditions must include Status conditions"
    );
    assert!(
        all_ids.contains(id_result.as_str()),
        "get_all_conditions must include Result conditions"
    );
    assert!(
        all_ids.contains(id_exception.as_str()),
        "get_all_conditions must include Exception conditions"
    );
    assert!(
        all.len() >= 5,
        "Expected at least 5 conditions, got {}",
        all.len()
    );
}

/// Monitoring-capable stores preserve and query event/run relationships.
pub async fn test_monitoring_records(store: &dyn TriggerStore) {
    let event = EventRecord {
        event_id: "suite-event".into(),
        event_code: "suite-code".into(),
        payload: serde_json::json!({"value": 1}),
        timestamp: Utc::now(),
        matched_condition_ids: vec![ConditionId::from("suite-condition")],
        valid_condition_ids: vec!["suite-valid".into()],
        triggered_invocation_ids: Vec::new(),
        emitted_by_invocation_id: None,
        emitted_by_task_id: None,
        emitted_by_runner_id: None,
    };
    store.store_event(&event).await.unwrap();
    let run = TriggerRunRecord {
        trigger_run_id: TriggerRunId::from("suite-run"),
        trigger_id: TriggerDefinitionId::from("suite-trigger"),
        task_id: test_task_id("suite-target"),
        logic: TriggerLogic::Or,
        arguments: serde_json::json!({}),
        participants: vec![TriggerRunParticipant {
            context_type: "event".into(),
            condition_id: ConditionId::from("suite-condition"),
            valid_condition_id: "suite-valid".into(),
            event_id: Some(event.event_id.clone()),
            source_invocation_id: None,
            context_summary: "suite-code".into(),
        }],
        claimed_at: Utc::now(),
        executed_at: None,
        triggered_invocation_id: None,
        atomic_service_run_id: None,
        atomic_service_runner_id: None,
    };
    store.store_trigger_run(&run).await.unwrap();

    assert_eq!(
        store
            .get_events(&EventQuery {
                event_code: Some("suite-code".into()),
                ..EventQuery::default()
            })
            .await
            .unwrap(),
        vec![event]
    );
    assert_eq!(
        store
            .get_trigger_runs(&TriggerRunQuery {
                event_id: Some("suite-event".into()),
                ..TriggerRunQuery::default()
            })
            .await
            .unwrap(),
        vec![run]
    );
}

/// Macro to generate all trigger suite tests for a given setup expression.
///
/// # Example
///
/// ```rust,ignore
/// use rustvello_test_suite::trigger_suite;
/// use rustvello_mem::trigger::MemTriggerStore;
///
/// trigger_suite!(MemTriggerStore::new());
/// ```
#[macro_export]
macro_rules! trigger_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_trigger_register_and_get_condition() {
            let store = $setup;
            $crate::trigger::test_register_and_get_condition(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_get_conditions_for_task() {
            let store = $setup;
            $crate::trigger::test_get_conditions_for_task(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_get_event_conditions() {
            let store = $setup;
            $crate::trigger::test_get_event_conditions(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_get_cron_conditions() {
            let store = $setup;
            $crate::trigger::test_get_cron_conditions(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_register_and_get_trigger() {
            let store = $setup;
            $crate::trigger::test_register_and_get_trigger(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_get_triggers_for_condition() {
            let store = $setup;
            $crate::trigger::test_get_triggers_for_condition(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_remove_triggers_for_task() {
            let store = $setup;
            $crate::trigger::test_remove_triggers_for_task(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_valid_condition_lifecycle() {
            let store = $setup;
            $crate::trigger::test_valid_condition_lifecycle(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_cron_execution_optimistic_lock() {
            let store = $setup;
            $crate::trigger::test_cron_execution_optimistic_lock(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_claim_trigger_run_dedup() {
            let store = $setup;
            $crate::trigger::test_claim_trigger_run_dedup(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_purge_clears_all() {
            let store = $setup;
            $crate::trigger::test_purge_clears_all(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_multiple_cron_conditions() {
            let store = $setup;
            $crate::trigger::test_multiple_cron_conditions(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_cron_execution_history() {
            let store = $setup;
            $crate::trigger::test_cron_execution_history(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_cron_sequential_executions() {
            let store = $setup;
            $crate::trigger::test_cron_sequential_executions(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_and_logic() {
            let store = $setup;
            $crate::trigger::test_trigger_and_logic(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_get_all_conditions() {
            let store = $setup;
            $crate::trigger::test_get_all_conditions(&store).await;
        }

        #[tokio::test]
        async fn suite_trigger_monitoring_records() {
            let store = $setup;
            $crate::trigger::test_monitoring_records(&store).await;
        }
    };
}

/// Async-setup variant of [`trigger_suite!`] for testcontainers backends.
///
/// `$setup` is an async expression returning `(_guard, trigger_store)`.
/// Tests are `#[ignore = "requires Docker"]`.
#[macro_export]
macro_rules! async_trigger_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_register_and_get_condition() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_register_and_get_condition(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_get_conditions_for_task() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_get_conditions_for_task(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_get_event_conditions() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_get_event_conditions(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_get_cron_conditions() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_get_cron_conditions(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_register_and_get_trigger() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_register_and_get_trigger(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_get_triggers_for_condition() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_get_triggers_for_condition(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_remove_triggers_for_task() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_remove_triggers_for_task(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_valid_condition_lifecycle() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_valid_condition_lifecycle(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_cron_execution_optimistic_lock() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_cron_execution_optimistic_lock(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_claim_trigger_run_dedup() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_claim_trigger_run_dedup(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_purge_clears_all() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_purge_clears_all(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_multiple_cron_conditions() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_multiple_cron_conditions(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_cron_execution_history() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_cron_execution_history(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_cron_sequential_executions() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_cron_sequential_executions(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_and_logic() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_trigger_and_logic(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_get_all_conditions() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_get_all_conditions(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_trigger_monitoring_records() {
            let (_c, store) = $setup.await;
            $crate::trigger::test_monitoring_records(&store).await;
        }
    };
}
