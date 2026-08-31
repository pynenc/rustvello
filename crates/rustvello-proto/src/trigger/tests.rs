use std::collections::BTreeMap;

use crate::identifiers::{InvocationId, TaskId};
use crate::status::InvocationStatus;
use crate::trigger::*;

#[test]
fn condition_id_deterministic() {
    let c1 = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    let c2 = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    assert_eq!(c1.condition_id(), c2.condition_id());
}

#[test]
fn condition_id_differs_by_type() {
    let c1 = TriggerCondition::Event(EventCondition {
        event_code: "test".to_string(),
        payload_filter: None,
    });
    let c2 = TriggerCondition::Cron(CronCondition {
        cron_expression: "test".to_string(),
        min_interval_seconds: 50,
    });
    assert_ne!(c1.condition_id(), c2.condition_id());
}

#[test]
fn condition_id_differs_by_value() {
    let c1 = TriggerCondition::Event(EventCondition {
        event_code: "a".to_string(),
        payload_filter: None,
    });
    let c2 = TriggerCondition::Event(EventCondition {
        event_code: "b".to_string(),
        payload_filter: None,
    });
    assert_ne!(c1.condition_id(), c2.condition_id());
}

#[test]
fn status_condition_satisfied() {
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success, InvocationStatus::Failed],
        argument_filter: None,
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn status_condition_wrong_status() {
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Failed,
        arguments: BTreeMap::new(),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn status_condition_wrong_task() {
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task_a"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task_b"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn event_condition_satisfied() {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    let ctx = ConditionContext::Event(EventContext {
        event_id: "evt-1".to_string(),
        event_code: "payment".to_string(),
        payload: serde_json::json!({}),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn event_condition_wrong_code() {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "payment".to_string(),
        payload_filter: None,
    });
    let ctx = ConditionContext::Event(EventContext {
        event_id: "evt-1".to_string(),
        event_code: "refund".to_string(),
        payload: serde_json::json!({}),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn result_condition_satisfied() {
    let cond = TriggerCondition::Result(ResultCondition {
        task_id: TaskId::new("mod", "task"),
        argument_filter: None,
        result_filter: None,
    });
    let ctx = ConditionContext::Result(ResultContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        result: serde_json::json!(42),
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn exception_condition_matches_type() {
    let cond = TriggerCondition::Exception(ExceptionCondition {
        task_id: TaskId::new("mod", "task"),
        exception_types: vec!["TimeoutError".to_string()],
        argument_filter: None,
    });
    let ctx = ConditionContext::Exception(ExceptionContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        error_type: "TimeoutError".to_string(),
        error_message: "timed out".to_string(),
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn exception_condition_empty_types_matches_all() {
    let cond = TriggerCondition::Exception(ExceptionCondition {
        task_id: TaskId::new("mod", "task"),
        exception_types: vec![],
        argument_filter: None,
    });
    let ctx = ConditionContext::Exception(ExceptionContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        error_type: "AnyError".to_string(),
        error_message: "something".to_string(),
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn mismatched_condition_context_returns_false() {
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "test".to_string(),
        payload_filter: None,
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn source_task_ids() {
    let cron = TriggerCondition::Cron(CronCondition {
        cron_expression: "* * * * *".to_string(),
        min_interval_seconds: 50,
    });
    assert!(cron.source_task_ids().is_empty());

    let event = TriggerCondition::Event(EventCondition {
        event_code: "test".to_string(),
        payload_filter: None,
    });
    assert!(event.source_task_ids().is_empty());

    let status = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    assert_eq!(status.source_task_ids(), vec![TaskId::new("mod", "task")]);
}

#[test]
fn trigger_logic_serde() {
    let logic = TriggerLogic::And;
    let json = serde_json::to_string(&logic).unwrap();
    let back: TriggerLogic = serde_json::from_str(&json).unwrap();
    assert_eq!(back, logic);
}

#[test]
fn trigger_definition_id_deterministic() {
    let task_id = TaskId::new("mod", "target");
    let cond_ids = vec![
        ConditionId("aaa".to_string()),
        ConditionId("bbb".to_string()),
    ];
    let id1 = TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::And);
    let id2 = TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::And);
    assert_eq!(id1, id2);
}

#[test]
fn trigger_definition_id_differs_by_logic() {
    let task_id = TaskId::new("mod", "target");
    let cond_ids = vec![ConditionId("aaa".to_string())];
    let id_and = TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::And);
    let id_or = TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::Or);
    assert_ne!(id_and, id_or);
}

#[test]
fn trigger_definition_id_order_independent() {
    let task_id = TaskId::new("mod", "target");
    let ids1 = vec![
        ConditionId("bbb".to_string()),
        ConditionId("aaa".to_string()),
    ];
    let ids2 = vec![
        ConditionId("aaa".to_string()),
        ConditionId("bbb".to_string()),
    ];
    let id1 = TriggerDefinitionDTO::compute_trigger_id(&task_id, &ids1, TriggerLogic::And);
    let id2 = TriggerDefinitionDTO::compute_trigger_id(&task_id, &ids2, TriggerLogic::And);
    assert_eq!(id1, id2);
}

#[test]
fn event_record_round_trip_preserves_monitoring_links() {
    let record = EventRecord {
        event_id: "event-1".into(),
        event_code: "payment".into(),
        payload: serde_json::json!({"order_id": 42}),
        timestamp: chrono::Utc::now(),
        matched_condition_ids: vec![ConditionId::from("condition-1")],
        valid_condition_ids: vec!["valid-1".into()],
        triggered_invocation_ids: vec![InvocationId::from_string("inv-2")],
        emitted_by_invocation_id: Some(InvocationId::from_string("inv-1")),
        emitted_by_task_id: Some(TaskId::new("mod", "source")),
        emitted_by_runner_id: Some(crate::identifiers::RunnerId::from_string("runner-1")),
    };

    let json = serde_json::to_string(&record).unwrap();
    let decoded: EventRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, record);
    assert!(decoded.is_matched());
    assert!(decoded.is_triggered());
}

#[test]
fn trigger_run_round_trip_preserves_participant_mapping() {
    let record = TriggerRunRecord {
        trigger_run_id: TriggerRunId::from("run-1"),
        trigger_id: TriggerDefinitionId::from("trigger-1"),
        task_id: TaskId::new("mod", "target"),
        logic: TriggerLogic::And,
        arguments: serde_json::json!({"order_id": 42}),
        participants: vec![TriggerRunParticipant {
            context_type: "event".into(),
            condition_id: ConditionId::from("condition-1"),
            valid_condition_id: "valid-1".into(),
            event_id: Some("event-1".into()),
            source_invocation_id: None,
            context_summary: "payment".into(),
        }],
        claimed_at: chrono::Utc::now(),
        executed_at: None,
        triggered_invocation_id: None,
        atomic_service_run_id: None,
        atomic_service_runner_id: None,
    };

    let json = serde_json::to_string(&record).unwrap();
    let decoded: TriggerRunRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(decoded.event_ids(), vec!["event-1"]);
}

#[test]
fn valid_condition_id_format() {
    let cond_id = ConditionId("abc123".to_string());
    let ctx = ConditionContext::Event(EventContext {
        event_id: "evt-1".to_string(),
        event_code: "test".to_string(),
        payload: serde_json::json!({}),
    });
    let vc = ValidCondition::new(cond_id.clone(), ctx);
    assert!(vc.valid_condition_id.starts_with("valid_abc123_"));
}

#[test]
fn condition_serde_round_trip() {
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success, InvocationStatus::Failed],
        argument_filter: None,
    });
    let json = serde_json::to_string(&cond).unwrap();
    let back: TriggerCondition = serde_json::from_str(&json).unwrap();
    assert_eq!(cond.condition_id(), back.condition_id());
}

#[test]
fn context_serde_round_trip() {
    let ctx = ConditionContext::Result(ResultContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        result: serde_json::json!({"value": 42}),
        arguments: BTreeMap::new(),
    });
    let json = serde_json::to_string(&ctx).unwrap();
    let back: ConditionContext = serde_json::from_str(&json).unwrap();
    assert_eq!(ctx.context_id(), back.context_id());
}

#[test]
fn trigger_definition_serde_round_trip() {
    let task_id = TaskId::new("mod", "target");
    let cond_ids = vec![ConditionId("c1".to_string())];
    let trigger = TriggerDefinitionDTO {
        trigger_id: TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::Or),
        task_id,
        condition_ids: cond_ids,
        logic: TriggerLogic::Or,
        argument_template: Some(serde_json::json!({"key": "value"})),
    };
    let json = serde_json::to_string(&trigger).unwrap();
    let back: TriggerDefinitionDTO = serde_json::from_str(&json).unwrap();
    assert_eq!(back.trigger_id, trigger.trigger_id);
    assert_eq!(back.logic, TriggerLogic::Or);
}

// ── Filter & composite tests ────────────────────────────────

#[test]
fn status_argument_filter_matches() {
    let mut filter = BTreeMap::new();
    filter.insert("region".to_string(), serde_json::json!("eu"));
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: Some(filter),
    });
    let mut args = BTreeMap::new();
    args.insert("region".to_string(), r#""eu""#.to_string());
    args.insert("extra".to_string(), r#""ignored""#.to_string());
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: args,
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn status_argument_filter_rejects() {
    let mut filter = BTreeMap::new();
    filter.insert("region".to_string(), serde_json::json!("eu"));
    let cond = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: Some(filter),
    });
    let mut args = BTreeMap::new();
    args.insert("region".to_string(), r#""us""#.to_string());
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: args,
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn result_filter_matches() {
    let cond = TriggerCondition::Result(ResultCondition {
        task_id: TaskId::new("mod", "task"),
        argument_filter: None,
        result_filter: Some(serde_json::json!({"status": "approved"})),
    });
    let ctx = ConditionContext::Result(ResultContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        result: serde_json::json!({"status": "approved"}),
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn result_filter_rejects() {
    let cond = TriggerCondition::Result(ResultCondition {
        task_id: TaskId::new("mod", "task"),
        argument_filter: None,
        result_filter: Some(serde_json::json!(42)),
    });
    let ctx = ConditionContext::Result(ResultContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        result: serde_json::json!(99),
        arguments: BTreeMap::new(),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn event_payload_filter_matches() {
    let mut filter = BTreeMap::new();
    filter.insert("type".to_string(), serde_json::json!("order"));
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "placed".to_string(),
        payload_filter: Some(filter),
    });
    let ctx = ConditionContext::Event(EventContext {
        event_id: "e-1".to_string(),
        event_code: "placed".to_string(),
        payload: serde_json::json!({"type": "order", "amount": 100}),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn event_payload_filter_rejects() {
    let mut filter = BTreeMap::new();
    filter.insert("type".to_string(), serde_json::json!("order"));
    let cond = TriggerCondition::Event(EventCondition {
        event_code: "placed".to_string(),
        payload_filter: Some(filter),
    });
    let ctx = ConditionContext::Event(EventContext {
        event_id: "e-1".to_string(),
        event_code: "placed".to_string(),
        payload: serde_json::json!({"type": "refund"}),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn composite_and_all_satisfied() {
    let cond = TriggerCondition::Composite(CompositeCondition {
        logic: CompositeLogic::And,
        children: vec![
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Success],
                argument_filter: None,
            }),
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Success, InvocationStatus::Failed],
                argument_filter: None,
            }),
        ],
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn composite_and_one_fails() {
    let cond = TriggerCondition::Composite(CompositeCondition {
        logic: CompositeLogic::And,
        children: vec![
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Success],
                argument_filter: None,
            }),
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Failed],
                argument_filter: None,
            }),
        ],
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(!cond.is_satisfied_by(&ctx));
}

#[test]
fn composite_or_one_satisfied() {
    let cond = TriggerCondition::Composite(CompositeCondition {
        logic: CompositeLogic::Or,
        children: vec![
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Failed],
                argument_filter: None,
            }),
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Success],
                argument_filter: None,
            }),
        ],
    });
    let ctx = ConditionContext::Status(StatusContext {
        invocation_id: InvocationId::from_string("inv-1"),
        task_id: TaskId::new("mod", "task"),
        status: InvocationStatus::Success,
        arguments: BTreeMap::new(),
    });
    assert!(cond.is_satisfied_by(&ctx));
}

#[test]
fn composite_serde_round_trip() {
    let cond = TriggerCondition::Composite(CompositeCondition {
        logic: CompositeLogic::And,
        children: vec![
            TriggerCondition::Event(EventCondition {
                event_code: "test".to_string(),
                payload_filter: None,
            }),
            TriggerCondition::Status(StatusCondition {
                task_id: TaskId::new("mod", "task"),
                statuses: vec![InvocationStatus::Success],
                argument_filter: None,
            }),
        ],
    });
    let json = serde_json::to_string(&cond).unwrap();
    let back: TriggerCondition = serde_json::from_str(&json).unwrap();
    assert_eq!(cond.condition_id(), back.condition_id());
}

#[test]
fn condition_id_differs_with_argument_filter() {
    let c1 = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: None,
    });
    let mut filter = BTreeMap::new();
    filter.insert("region".to_string(), serde_json::json!("eu"));
    let c2 = TriggerCondition::Status(StatusCondition {
        task_id: TaskId::new("mod", "task"),
        statuses: vec![InvocationStatus::Success],
        argument_filter: Some(filter),
    });
    assert_ne!(c1.condition_id(), c2.condition_id());
}
