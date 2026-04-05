use std::sync::Arc;

use chrono::Utc;

use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::*;

use crate::db::Database;

use super::SqliteTriggerStore;

fn make_store() -> SqliteTriggerStore {
    let db = Arc::new(Database::in_memory().unwrap());
    SqliteTriggerStore::new(db)
}

#[tokio::test]
async fn register_and_get_condition() {
    let store = make_store();
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    let id = store.register_condition(&cond).await.unwrap();
    let got = store.get_condition(&id).await.unwrap();
    assert!(got.is_some());
}

#[tokio::test]
async fn get_conditions_for_task() {
    let store = make_store();
    let task_id = TaskId::new("mod", "task");
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: task_id.clone(),
        statuses: vec![rustvello_proto::status::InvocationStatus::Success],
        argument_filter: None,
    });
    store.register_condition(&cond).await.unwrap();

    let conds = store.get_conditions_for_task(&task_id).await.unwrap();
    assert_eq!(conds.len(), 1);
}

#[tokio::test]
async fn get_cron_conditions() {
    let store = make_store();
    let cond = TriggerCondition::Cron(CronCondition {
        cron_expression: "* * * * *".to_string(),
        min_interval_seconds: 50,
    });
    store.register_condition(&cond).await.unwrap();

    let conds = store.get_cron_conditions().await.unwrap();
    assert_eq!(conds.len(), 1);
}

#[tokio::test]
async fn get_event_conditions() {
    let store = make_store();
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

#[tokio::test]
async fn register_and_get_trigger() {
    let store = make_store();
    let task_id = TaskId::new("mod", "target");
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

#[tokio::test]
async fn get_triggers_for_condition() {
    let store = make_store();
    let cond_id = ConditionId::from("c1".to_string());
    let task_id = TaskId::new("mod", "target");
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

#[tokio::test]
async fn remove_triggers_for_task() {
    let store = make_store();
    let task_id = TaskId::new("mod", "target");
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

#[tokio::test]
async fn valid_condition_lifecycle() {
    let store = make_store();
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

#[tokio::test]
async fn cron_execution_optimistic_lock() {
    let store = make_store();
    let cond_id = ConditionId::from("cron1".to_string());
    let now = Utc::now();

    assert!(store
        .store_cron_execution(&cond_id, now, None)
        .await
        .unwrap());

    // Duplicate insert fails (optimistic lock)
    assert!(!store
        .store_cron_execution(&cond_id, now, None)
        .await
        .unwrap());

    // Correct expected value succeeds
    let later = now + chrono::Duration::seconds(60);
    assert!(store
        .store_cron_execution(&cond_id, later, Some(now))
        .await
        .unwrap());
}

#[tokio::test]
async fn claim_trigger_run_dedup() {
    let store = make_store();
    let run_id = TriggerRunId::from("run-1".to_string());

    assert!(store.claim_trigger_run(&run_id).await.unwrap());
    assert!(!store.claim_trigger_run(&run_id).await.unwrap());
}

#[tokio::test]
async fn purge_clears_all() {
    let store = make_store();
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "test".to_string(),
        payload_filter: None,
    });
    store.register_condition(&cond).await.unwrap();
    store.purge().await.unwrap();

    let got = store.get_event_conditions("test").await.unwrap();
    assert!(got.is_empty());
}
