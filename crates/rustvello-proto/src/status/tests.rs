use crate::identifiers::RunnerId;
use crate::status::*;

// --- Config completeness ---

#[test]
fn all_statuses_have_definitions() {
    for status in ALL_STATUSES {
        // Should not panic
        let _ = STATUS_CONFIG.definition(*status);
    }
}

#[test]
fn all_transitions_target_valid_statuses() {
    for status in ALL_STATUSES {
        for target in STATUS_CONFIG.definition(*status).allowed_transitions.iter() {
            assert!(
                ALL_STATUSES.contains(target),
                "{status:?} references undefined transition target: {target:?}"
            );
        }
    }
}

#[test]
fn final_statuses_have_no_transitions() {
    for status in InvocationStatus::final_statuses() {
        assert!(
            status.valid_transitions().is_empty(),
            "final status {status:?} has transitions"
        );
    }
}

#[test]
fn no_status_both_acquires_and_releases() {
    for status in ALL_STATUSES {
        let def = STATUS_CONFIG.definition(*status);
        assert!(
            !(def.acquires_ownership && def.releases_ownership),
            "{status:?} cannot both acquire and release ownership"
        );
    }
}

#[test]
fn no_status_both_overrides_and_requires() {
    for status in ALL_STATUSES {
        let def = STATUS_CONFIG.definition(*status);
        assert!(
            !(def.overrides_ownership && def.requires_ownership),
            "{status:?} cannot both override and require ownership"
        );
    }
}

// --- Transition validation ---

#[test]
fn valid_transitions_from_none() {
    assert!(validate_transition(None, InvocationStatus::Registered).is_ok());
    assert!(validate_transition(None, InvocationStatus::Running).is_err());
}

#[test]
fn status_transitions() {
    use InvocationStatus::*;
    assert!(Registered.can_transition_to(Pending));
    assert!(Registered.can_transition_to(ConcurrencyControlled));
    assert!(Registered.can_transition_to(ConcurrencyControlledFinal));
    assert!(!Registered.can_transition_to(Running));
    assert!(!Registered.can_transition_to(Rerouted));

    assert!(Pending.can_transition_to(Running));
    assert!(Pending.can_transition_to(Killed));
    assert!(Pending.can_transition_to(Rerouted));
    assert!(!Pending.can_transition_to(Failed));
    assert!(Pending.can_transition_to(PendingRecovery));

    assert!(Running.can_transition_to(Paused));
    assert!(Running.can_transition_to(Killed));
    assert!(Running.can_transition_to(Success));
    assert!(Running.can_transition_to(Failed));
    assert!(Running.can_transition_to(Retry));
    assert!(Running.can_transition_to(RunningRecovery));

    assert!(Paused.can_transition_to(Running));
    assert!(Paused.can_transition_to(Killed));

    assert!(Killed.can_transition_to(Rerouted));
    assert!(!Killed.can_transition_to(Running));

    assert!(Retry.can_transition_to(Pending));

    assert!(ConcurrencyControlled.can_transition_to(Rerouted));
    assert!(!ConcurrencyControlled.can_transition_to(Pending));

    assert!(Rerouted.can_transition_to(Pending));
    assert!(Rerouted.can_transition_to(ConcurrencyControlled));

    assert!(!Success.can_transition_to(Running));
    assert!(!Failed.can_transition_to(Pending));
}

#[test]
fn terminal_states() {
    assert_eq!(
        InvocationStatus::final_statuses(),
        &[
            InvocationStatus::Success,
            InvocationStatus::Failed,
            InvocationStatus::ConcurrencyControlledFinal,
        ]
    );
    assert!(InvocationStatus::Success.is_terminal());
    assert!(InvocationStatus::Failed.is_terminal());
    assert!(InvocationStatus::ConcurrencyControlledFinal.is_terminal());
    assert!(!InvocationStatus::Rerouted.is_terminal());
    assert!(!InvocationStatus::Running.is_terminal());
    assert!(!InvocationStatus::Retry.is_terminal());
    assert!(!InvocationStatus::Registered.is_terminal());
    assert!(!InvocationStatus::Pending.is_terminal());
    assert!(!InvocationStatus::ConcurrencyControlled.is_terminal());
    assert!(!InvocationStatus::PendingRecovery.is_terminal());
    assert!(!InvocationStatus::RunningRecovery.is_terminal());
    assert!(!InvocationStatus::Paused.is_terminal());
    assert!(!InvocationStatus::Killed.is_terminal());
}

#[test]
fn available_for_run() {
    assert!(InvocationStatus::Registered.is_available_for_run());
    assert!(InvocationStatus::Rerouted.is_available_for_run());
    assert!(InvocationStatus::Retry.is_available_for_run());
    assert!(!InvocationStatus::Running.is_available_for_run());
    assert!(!InvocationStatus::Pending.is_available_for_run());
    assert!(!InvocationStatus::Success.is_available_for_run());
}

#[test]
fn recovery_transitions() {
    use InvocationStatus::*;
    assert!(Pending.can_transition_to(PendingRecovery));
    assert!(PendingRecovery.can_transition_to(Rerouted));
    assert!(Running.can_transition_to(RunningRecovery));
    assert!(RunningRecovery.can_transition_to(Rerouted));
    // Recovery statuses do NOT go back to Running directly
    assert!(!PendingRecovery.can_transition_to(Running));
    assert!(!RunningRecovery.can_transition_to(Running));
}

// --- Ownership validation ---

#[test]
fn ownership_required_for_pending() {
    let record = InvocationStatusRecord::new(
        InvocationStatus::Pending,
        Some(RunnerId::from_string("runner-a")),
    );
    // Same runner: ok
    let r = validate_ownership(
        Some(&record),
        InvocationStatus::Running,
        Some(&RunnerId::from_string("runner-a")),
    );
    assert!(r.is_ok());

    // Different runner: rejected
    let r = validate_ownership(
        Some(&record),
        InvocationStatus::Running,
        Some(&RunnerId::from_string("runner-b")),
    );
    assert!(r.is_err());
}

#[test]
fn ownership_override_for_recovery() {
    let record = InvocationStatusRecord::new(
        InvocationStatus::Pending,
        Some(RunnerId::from_string("dead-runner")),
    );
    // PendingRecovery overrides ownership — any runner can initiate
    let r = validate_ownership(
        Some(&record),
        InvocationStatus::PendingRecovery,
        Some(&RunnerId::from_string("recovery-runner")),
    );
    assert!(r.is_ok());
}

#[test]
fn ownership_acquire_needs_runner_id() {
    let record = InvocationStatusRecord::new(InvocationStatus::Registered, None);
    // Pending acquires ownership — needs runner_id
    let r = validate_ownership(Some(&record), InvocationStatus::Pending, None);
    assert!(r.is_err());
}

#[test]
fn compute_owner_acquire() {
    let runner = RunnerId::from_string("r1");
    let owner = compute_new_owner(None, InvocationStatus::Pending, Some(runner.clone()));
    assert_eq!(owner, Some(runner));
}

#[test]
fn compute_owner_release() {
    let record =
        InvocationStatusRecord::new(InvocationStatus::Running, Some(RunnerId::from_string("r1")));
    let owner = compute_new_owner(
        Some(&record),
        InvocationStatus::Success,
        Some(RunnerId::from_string("r1")),
    );
    assert!(owner.is_none());
}

#[test]
fn compute_owner_keep() {
    let record =
        InvocationStatusRecord::new(InvocationStatus::Running, Some(RunnerId::from_string("r1")));
    // Paused does not acquire or release
    let owner = compute_new_owner(
        Some(&record),
        InvocationStatus::Paused,
        Some(RunnerId::from_string("r1")),
    );
    assert_eq!(owner, Some(RunnerId::from_string("r1")));
}

// --- Full state machine transition ---

#[test]
fn full_lifecycle_happy_path() {
    // None -> Registered
    let r1 = status_record_transition(None, InvocationStatus::Registered, None).unwrap();
    assert_eq!(r1.status, InvocationStatus::Registered);
    assert!(r1.runner_id.is_none());

    // Registered -> Pending (acquires ownership)
    let runner = RunnerId::from_string("runner-1");
    let r2 = status_record_transition(Some(&r1), InvocationStatus::Pending, Some(&runner)).unwrap();
    assert_eq!(r2.status, InvocationStatus::Pending);
    assert_eq!(r2.runner_id.as_ref().unwrap(), &runner);

    // Pending -> Running
    let r3 = status_record_transition(Some(&r2), InvocationStatus::Running, Some(&runner)).unwrap();
    assert_eq!(r3.status, InvocationStatus::Running);

    // Running -> Success (releases ownership)
    let r4 = status_record_transition(Some(&r3), InvocationStatus::Success, Some(&runner)).unwrap();
    assert_eq!(r4.status, InvocationStatus::Success);
    assert!(r4.runner_id.is_none());
}

#[test]
fn pause_running_lifecycle() {
    let runner = RunnerId::from_string("r1");
    let running = InvocationStatusRecord::new(InvocationStatus::Running, Some(runner.clone()));

    let paused =
        status_record_transition(Some(&running), InvocationStatus::Paused, Some(&runner)).unwrap();
    assert_eq!(paused.status, InvocationStatus::Paused);

    let running_again =
        status_record_transition(Some(&paused), InvocationStatus::Running, Some(&runner)).unwrap();
    assert_eq!(running_again.status, InvocationStatus::Running);

    let success = status_record_transition(
        Some(&running_again),
        InvocationStatus::Success,
        Some(&runner),
    )
    .unwrap();
    assert_eq!(success.status, InvocationStatus::Success);
    assert!(success.runner_id.is_none());
}

#[test]
fn kill_and_reroute() {
    let runner = RunnerId::from_string("r1");
    let running = InvocationStatusRecord::new(InvocationStatus::Running, Some(runner.clone()));

    let killed =
        status_record_transition(Some(&running), InvocationStatus::Killed, Some(&runner)).unwrap();
    assert_eq!(killed.status, InvocationStatus::Killed);
    assert!(killed.runner_id.is_none()); // releases ownership

    let rerouted =
        status_record_transition(Some(&killed), InvocationStatus::Rerouted, None).unwrap();
    assert_eq!(rerouted.status, InvocationStatus::Rerouted);
}

// --- Display and FromStr ---

#[test]
fn status_display() {
    assert_eq!(InvocationStatus::Registered.to_string(), "REGISTERED");
    assert_eq!(InvocationStatus::Pending.to_string(), "PENDING");
    assert_eq!(InvocationStatus::Running.to_string(), "RUNNING");
    assert_eq!(InvocationStatus::Success.to_string(), "SUCCESS");
    assert_eq!(InvocationStatus::Failed.to_string(), "FAILED");
    assert_eq!(InvocationStatus::Retry.to_string(), "RETRY");
    assert_eq!(
        InvocationStatus::ConcurrencyControlled.to_string(),
        "CONCURRENCY_CONTROLLED"
    );
    assert_eq!(
        InvocationStatus::ConcurrencyControlledFinal.to_string(),
        "CONCURRENCY_CONTROLLED_FINAL"
    );
    assert_eq!(InvocationStatus::Rerouted.to_string(), "REROUTED");
    assert_eq!(
        InvocationStatus::PendingRecovery.to_string(),
        "PENDING_RECOVERY"
    );
    assert_eq!(
        InvocationStatus::RunningRecovery.to_string(),
        "RUNNING_RECOVERY"
    );
    assert_eq!(InvocationStatus::Paused.to_string(), "PAUSED");
    assert_eq!(InvocationStatus::Killed.to_string(), "KILLED");
}

#[test]
fn status_from_str() {
    assert_eq!(
        "PAUSED".parse::<InvocationStatus>().unwrap(),
        InvocationStatus::Paused
    );
    assert_eq!(
        "RUNNING".parse::<InvocationStatus>().unwrap(),
        InvocationStatus::Running
    );
    assert_eq!(
        "KILLED".parse::<InvocationStatus>().unwrap(),
        InvocationStatus::Killed
    );
    assert!("GARBAGE".parse::<InvocationStatus>().is_err());
}

#[test]
fn status_record_new() {
    let record =
        InvocationStatusRecord::new(InvocationStatus::Running, Some(RunnerId::from_string("r1")));
    assert_eq!(record.status, InvocationStatus::Running);
    assert_eq!(record.runner_id.as_ref().unwrap().as_str(), "r1");
}

#[test]
fn concurrency_control_type_default() {
    let cc: ConcurrencyControlType = ConcurrencyControlType::default();
    assert_eq!(cc, ConcurrencyControlType::Unlimited);
}

#[test]
fn serde_round_trip_status() {
    let s = InvocationStatus::ConcurrencyControlled;
    let json = serde_json::to_string(&s).unwrap();
    let back: InvocationStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(s, back);
}

#[test]
fn serde_round_trip_new_statuses() {
    for status in [
        InvocationStatus::Paused,
        InvocationStatus::Running,
        InvocationStatus::Killed,
    ] {
        let json = serde_json::to_string(&status).unwrap();
        let back: InvocationStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, back);
    }
}
