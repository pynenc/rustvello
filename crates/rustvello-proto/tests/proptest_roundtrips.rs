//! Property-based tests for status transitions and serde round-trips.

use proptest::prelude::*;
use std::collections::BTreeMap;

use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::identifiers::{CallId, InvocationId, RunnerId};
use rustvello_proto::invocation::{InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus, InvocationStatusRecord};
use rustvello_proto::trigger::{
    ConditionContext, CronCondition, EventCondition, EventContext, StatusCondition,
    TriggerCondition, TriggerLogic,
};

// ---------------------------------------------------------------------------
// Arbitrary implementations
// ---------------------------------------------------------------------------

fn arb_invocation_status() -> impl Strategy<Value = InvocationStatus> {
    prop_oneof![
        Just(InvocationStatus::Registered),
        Just(InvocationStatus::Pending),
        Just(InvocationStatus::Running),
        Just(InvocationStatus::Success),
        Just(InvocationStatus::Failed),
        Just(InvocationStatus::Retry),
        Just(InvocationStatus::ConcurrencyControlled),
        Just(InvocationStatus::ConcurrencyControlledFinal),
        Just(InvocationStatus::Rerouted),
        Just(InvocationStatus::PendingRecovery),
        Just(InvocationStatus::RunningRecovery),
        Just(InvocationStatus::Paused),
        Just(InvocationStatus::Killed),
    ]
}

fn arb_concurrency_control_type() -> impl Strategy<Value = ConcurrencyControlType> {
    prop_oneof![
        Just(ConcurrencyControlType::None),
        Just(ConcurrencyControlType::Unlimited),
        Just(ConcurrencyControlType::Task),
        Just(ConcurrencyControlType::Argument),
    ]
}

fn arb_trigger_logic() -> impl Strategy<Value = TriggerLogic> {
    prop_oneof![Just(TriggerLogic::And), Just(TriggerLogic::Or),]
}

// ---------------------------------------------------------------------------
// Status transition property tests
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn terminal_status_has_no_transitions(status in arb_invocation_status()) {
        if status.is_terminal() {
            prop_assert!(status.valid_transitions().is_empty(),
                "Terminal status {:?} should have no valid transitions", status);
        }
    }

    #[test]
    fn transition_graph_is_consistent(
        from in arb_invocation_status(),
        to in arb_invocation_status(),
    ) {
        // can_transition_to should agree with valid_transitions().contains()
        prop_assert_eq!(
            from.can_transition_to(to),
            from.valid_transitions().contains(&to),
            "Inconsistency for {:?} -> {:?}", from, to
        );
    }

    #[test]
    fn non_terminal_status_has_at_least_one_transition(status in arb_invocation_status()) {
        if !status.is_terminal() {
            prop_assert!(!status.valid_transitions().is_empty(),
                "Non-terminal status {:?} should have at least one transition", status);
        }
    }

    #[test]
    fn arbitrary_status_sequences_never_escape_the_declared_graph(
        candidates in proptest::collection::vec(arb_invocation_status(), 0..80),
    ) {
        let mut current = InvocationStatus::Registered;
        for candidate in candidates {
            let accepted = current.can_transition_to(candidate);
            prop_assert_eq!(
                accepted,
                current.valid_transitions().contains(&candidate)
            );
            if accepted {
                prop_assert!(!current.is_terminal());
                current = candidate;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arguments, trigger filters, and workflow replay-history properties
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn serialized_arguments_roundtrip_and_hash_are_order_independent(
        entries in proptest::collection::btree_map(
            "[a-zA-Z_][a-zA-Z0-9_]{0,12}",
            ".{0,40}",
            0..16,
        ),
    ) {
        let args = SerializedArguments(entries.clone());
        let json = serde_json::to_string(&args).unwrap();
        let decoded: SerializedArguments = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(&decoded, &args);

        let mut reversed = SerializedArguments::new();
        for (key, value) in entries.iter().rev() {
            reversed.insert(key.clone(), value.clone());
        }
        prop_assert_eq!(args.compute_args_id(), reversed.compute_args_id());
    }

    #[test]
    fn concurrency_pairs_preserve_every_argument(
        entries in proptest::collection::btree_map(
            "[a-z]{1,10}",
            "[a-zA-Z0-9_-]{0,20}",
            0..12,
        ),
    ) {
        let args = SerializedArguments(entries.clone());
        let pairs = args.cc_arg_pairs();
        if entries.is_empty() {
            prop_assert_eq!(pairs, vec![(String::new(), String::new())]);
        } else {
            prop_assert_eq!(pairs, entries.into_iter().collect::<Vec<_>>());
        }
    }

    #[test]
    fn event_payload_filter_matches_subsets_and_rejects_changed_values(
        key in "[a-z]{1,12}",
        value in "[a-zA-Z0-9_-]{0,20}",
    ) {
        let mut filter = BTreeMap::new();
        filter.insert(key.clone(), serde_json::Value::String(value.clone()));
        let condition = TriggerCondition::Event(EventCondition {
            event_code: "event.test".to_string(),
            payload_filter: Some(filter),
        });
        let matching = ConditionContext::Event(EventContext {
            event_id: "evt-1".to_string(),
            event_code: "event.test".to_string(),
            payload: serde_json::json!({ key.clone(): value }),
        });
        prop_assert!(condition.is_satisfied_by(&matching));

        let changed = ConditionContext::Event(EventContext {
            event_id: "evt-2".to_string(),
            event_code: "event.test".to_string(),
            payload: serde_json::json!({ key: "different" }),
        });
        prop_assert!(!condition.is_satisfied_by(&changed));
    }

    #[test]
    fn workflow_replay_history_roundtrip_preserves_order(
        statuses in proptest::collection::vec(arb_invocation_status(), 0..50),
        depth in 0u32..20,
    ) {
        let workflow_id = InvocationId::from_string("workflow-property");
        let identity = WorkflowIdentity::child(
            workflow_id.clone(),
            TaskId::new("property", "workflow"),
            InvocationId::from_string("parent-property"),
            depth,
        );
        let identity_json = serde_json::to_string(&identity).unwrap();
        let decoded_identity: WorkflowIdentity = serde_json::from_str(&identity_json).unwrap();
        prop_assert_eq!(decoded_identity.workflow_id, workflow_id.clone());
        prop_assert_eq!(decoded_identity.depth, depth);

        let history: Vec<_> = statuses
            .iter()
            .copied()
            .map(|status| {
                InvocationHistory::new(
                    workflow_id.clone(),
                    InvocationStatusRecord::new(status, None),
                    None,
                )
            })
            .collect();
        let json = serde_json::to_string(&history).unwrap();
        let decoded: Vec<InvocationHistory> = serde_json::from_str(&json).unwrap();
        let decoded_statuses: Vec<_> = decoded
            .into_iter()
            .map(|entry| entry.status_record.status)
            .collect();
        prop_assert_eq!(decoded_statuses, statuses);
    }
}

// ---------------------------------------------------------------------------
// Serde round-trip property tests
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn invocation_status_serde_roundtrip(status in arb_invocation_status()) {
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: InvocationStatus = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(status, deserialized);
    }

    #[test]
    fn invocation_status_display_roundtrip(status in arb_invocation_status()) {
        // Display produces a string that is non-empty
        let display = status.to_string();
        prop_assert!(!display.is_empty());
    }

    #[test]
    fn concurrency_control_type_serde_roundtrip(cct in arb_concurrency_control_type()) {
        let json = serde_json::to_string(&cct).unwrap();
        let deserialized: ConcurrencyControlType = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(cct, deserialized);
    }

    #[test]
    fn trigger_logic_serde_roundtrip(logic in arb_trigger_logic()) {
        let json = serde_json::to_string(&logic).unwrap();
        let deserialized: TriggerLogic = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(logic, deserialized);
    }

    #[test]
    fn status_record_serde_roundtrip(status in arb_invocation_status()) {
        let record = InvocationStatusRecord::new(status, None);
        let json = serde_json::to_string(&record).unwrap();
        let deserialized: InvocationStatusRecord = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(record.status, deserialized.status);
    }

    #[test]
    fn cron_condition_serde_roundtrip(
        expr in "[0-9]{1,2} [0-9]{1,2} \\* \\* \\*",
    ) {
        let cond = TriggerCondition::Cron(CronCondition {
            cron_expression: expr.clone(),
            min_interval_seconds: 60,
        });
        let json = serde_json::to_string(&cond).unwrap();
        let deserialized: TriggerCondition = serde_json::from_str(&json).unwrap();
        if let TriggerCondition::Cron(c) = &deserialized {
            prop_assert_eq!(&c.cron_expression, &expr);
        } else {
            prop_assert!(false, "Expected Cron variant");
        }
    }

    #[test]
    fn status_condition_serde_roundtrip(
        s1 in arb_invocation_status(),
        s2 in arb_invocation_status(),
    ) {
        let task_id = TaskId::new("m", "t");
        let cond = TriggerCondition::Status(StatusCondition {
            task_id: task_id.clone(),
            statuses: vec![s1, s2],
            argument_filter: None,
        });
        let json = serde_json::to_string(&cond).unwrap();
        let deserialized: TriggerCondition = serde_json::from_str(&json).unwrap();
        if let TriggerCondition::Status(sc) = &deserialized {
            prop_assert_eq!(&sc.statuses, &vec![s1, s2]);
        } else {
            prop_assert!(false, "Expected Status variant");
        }
    }

    #[test]
    fn event_condition_serde_roundtrip(code in "[a-z]{3,10}") {
        let cond = TriggerCondition::Event(EventCondition {
            event_code: code.clone(),
            payload_filter: None,
        });
        let json = serde_json::to_string(&cond).unwrap();
        let deserialized: TriggerCondition = serde_json::from_str(&json).unwrap();
        if let TriggerCondition::Event(ec) = &deserialized {
            prop_assert_eq!(&ec.event_code, &code);
        } else {
            prop_assert!(false, "Expected Event variant");
        }
    }
}

// ---------------------------------------------------------------------------
// Identifier round-trip tests
// ---------------------------------------------------------------------------

/// Strategy for non-empty strings without separator characters.
fn name_no_dots() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_]{0,20}"
}

fn module_segment() -> impl Strategy<Value = String> {
    "[a-zA-Z_][a-zA-Z0-9_.]{0,30}"
}

fn args_no_colon() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_-]{1,40}"
}

proptest! {
    /// TaskId Display → FromStr round-trip (local, no language prefix).
    #[test]
    fn task_id_local_roundtrip(
        module in module_segment(),
        name in name_no_dots(),
    ) {
        let tid = TaskId::new(&module, &name);
        let s = tid.to_string();
        let parsed: TaskId = s.parse().unwrap();
        prop_assert_eq!(parsed.module(), tid.module());
        prop_assert_eq!(parsed.name(), tid.name());
        prop_assert_eq!(parsed.language(), tid.language());
    }

    /// TaskId Display → FromStr round-trip (foreign, with language prefix).
    #[test]
    fn task_id_foreign_roundtrip(
        language in "[a-z]{2,10}",
        module in module_segment(),
        name in name_no_dots(),
    ) {
        let tid = TaskId::foreign(&language, &module, &name);
        let s = tid.to_string();
        let parsed: TaskId = s.parse().unwrap();
        prop_assert_eq!(parsed.module(), tid.module());
        prop_assert_eq!(parsed.name(), tid.name());
        prop_assert_eq!(parsed.language(), tid.language());
    }

    /// CallId Display → FromStr round-trip.
    #[test]
    fn call_id_roundtrip(
        module in module_segment(),
        name in name_no_dots(),
        args_id in args_no_colon(),
    ) {
        let tid = TaskId::new(&module, &name);
        let cid = CallId::new(tid.clone(), args_id.as_str());
        let s = cid.to_string();
        let parsed: CallId = s.parse().unwrap();
        prop_assert_eq!(parsed.task_id.module(), tid.module());
        prop_assert_eq!(parsed.task_id.name(), tid.name());
        prop_assert_eq!(&parsed.args_id, &cid.args_id);
    }

    /// InvocationId Display → from_string round-trip.
    #[test]
    fn invocation_id_roundtrip(seed in any::<u128>()) {
        let uuid = uuid::Uuid::from_u128(seed);
        let inv_id = InvocationId::from_string(uuid.to_string());
        let s = inv_id.to_string();
        let parsed = InvocationId::from_string(s.clone());
        prop_assert_eq!(parsed.to_string(), s);
    }

    /// RunnerId Display → from_string round-trip.
    #[test]
    fn runner_id_roundtrip(seed in any::<u128>()) {
        let uuid = uuid::Uuid::from_u128(seed);
        let rid = RunnerId::from_string(uuid.to_string());
        let s = rid.to_string();
        let parsed = RunnerId::from_string(s.clone());
        prop_assert_eq!(parsed.to_string(), s);
    }
}
