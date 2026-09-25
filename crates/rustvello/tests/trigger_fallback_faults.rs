//! M1.1/M1.2: the trigger outbox on the fallback publication path.
//!
//! Backends without a co-located publication transaction (memory, MongoDB)
//! publish a trigger run through ordered, individually idempotent writes. A
//! write failure injected at each boundary aborts the iteration the way a crash
//! would; the next iterations must recover to exactly one logical invocation.
//! The same cases run against a trigger store that uses the default,
//! non-transactional claim sequence (Redis, MongoDB).
#![cfg(feature = "fault-injection")]

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustvello::prelude::*;
use rustvello_core::failpoints;
use rustvello_core::trigger::{trigger_run_invocation_id, TriggerManager, TriggerStore};
use rustvello_mem::trigger::MemTriggerStore;

/// Failpoints are process-global; the cases run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const EVENT: &str = "fallback_ready";

fn target() -> TaskId {
    TaskId::new("fallback", "target")
}

fn runner() -> RunnerId {
    RunnerId::from_string("fallback-atomic-service")
}

/// Delegates storage to memory but keeps the trait's default (non-atomic) claim.
struct SequentialClaimStore(MemTriggerStore);

#[async_trait]
impl TriggerStore for SequentialClaimStore {
    async fn register_condition(&self, c: &TriggerCondition) -> RustvelloResult<ConditionId> {
        self.0.register_condition(c).await
    }
    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        self.0.get_condition(id).await
    }
    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        self.0.get_conditions_for_task(task_id).await
    }
    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        self.0.get_cron_conditions().await
    }
    async fn get_event_conditions(
        &self,
        code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        self.0.get_event_conditions(code).await
    }
    async fn register_trigger(&self, t: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        self.0.register_trigger(t).await
    }
    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        self.0.get_trigger(id).await
    }
    async fn get_triggers_for_condition(
        &self,
        id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        self.0.get_triggers_for_condition(id).await
    }
    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        self.0.remove_triggers_for_task(task_id).await
    }
    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        self.0.record_valid_condition(vc).await
    }
    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        self.0.get_valid_conditions().await
    }
    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        self.0.clear_valid_conditions(ids).await
    }
    async fn get_last_cron_execution(
        &self,
        id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        self.0.get_last_cron_execution(id).await
    }
    async fn store_cron_execution(
        &self,
        id: &ConditionId,
        time: DateTime<Utc>,
        expected: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        self.0.store_cron_execution(id, time, expected).await
    }
    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        self.0.claim_trigger_run(run_id).await
    }
    async fn store_event(&self, event: &EventRecord) -> RustvelloResult<()> {
        self.0.store_event(event).await
    }
    async fn get_event(&self, id: &str) -> RustvelloResult<Option<EventRecord>> {
        self.0.get_event(id).await
    }
    async fn get_events(&self, query: &EventQuery) -> RustvelloResult<Vec<EventRecord>> {
        self.0.get_events(query).await
    }
    async fn store_trigger_run(&self, run: &TriggerRunRecord) -> RustvelloResult<()> {
        self.0.store_trigger_run(run).await
    }
    async fn get_trigger_run(
        &self,
        id: &TriggerRunId,
    ) -> RustvelloResult<Option<TriggerRunRecord>> {
        self.0.get_trigger_run(id).await
    }
    async fn get_trigger_runs(
        &self,
        query: &TriggerRunQuery,
    ) -> RustvelloResult<Vec<TriggerRunRecord>> {
        self.0.get_trigger_runs(query).await
    }
    async fn purge(&self) -> RustvelloResult<()> {
        self.0.purge().await
    }
    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        self.0.get_all_conditions().await
    }
}

async fn app(sequential_claims: bool) -> RustvelloApp {
    let mut app = unrouted_app(sequential_claims).await;
    register_target(&mut app);
    app
}

fn register_target(app: &mut RustvelloApp) {
    app.register_task(
        target(),
        TaskConfig::default(),
        Arc::new(|_| Ok("null".into())),
    )
    .unwrap();
}

async fn unrouted_app(sequential_claims: bool) -> RustvelloApp {
    let mut app = Rustvello::builder()
        .app_id("fallback-faults")
        .memory()
        .build()
        .await
        .unwrap();
    if sequential_claims {
        app.set_trigger_manager(TriggerManager::new(Arc::new(SequentialClaimStore(
            MemTriggerStore::new(),
        ))));
    }
    let manager = app.trigger_manager().unwrap().clone();
    TriggerBuilder::new()
        .on_event(EVENT)
        .with_static_args(serde_json::json!({"batch": 3}))
        .build_and_register(&target(), manager.store())
        .await
        .unwrap();
    manager
        .emit_event(EVENT, serde_json::json!({}))
        .await
        .unwrap();
    app
}

async fn assert_exactly_once(app: &RustvelloApp, case: &str) {
    let invocations = app
        .orchestrator()
        .get_existing_invocations(&target(), None, ALL_STATUSES)
        .await
        .unwrap();
    assert_eq!(invocations.len(), 1, "{case}: {invocations:?}");
    let invocation_id = &invocations[0];
    let store = app.trigger_manager().unwrap().store();
    let runs = store
        .get_trigger_runs(&TriggerRunQuery::default())
        .await
        .unwrap();
    assert_eq!(runs.len(), 1, "{case}");
    assert_eq!(
        runs[0].triggered_invocation_id.as_ref(),
        Some(invocation_id),
        "{case}"
    );
    assert_eq!(
        &trigger_run_invocation_id(&runs[0].trigger_run_id),
        invocation_id
    );
    assert!(
        store.get_pending_trigger_runs(10).await.unwrap().is_empty(),
        "{case}"
    );
    assert!(
        store.get_valid_conditions().await.unwrap().is_empty(),
        "{case}"
    );
    let history = app
        .state_backend()
        .get_history(invocation_id)
        .await
        .unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Registered)
            .count(),
        1,
        "{case}: one registration"
    );
    // Any duplicate delivery of the same id is dropped at retrieval.
    let mut delivered = Vec::new();
    for _ in 0..3 {
        delivered.extend(app.get_invocations_to_run(10, &runner()).await.unwrap());
    }
    assert_eq!(
        delivered,
        vec![invocation_id.clone()],
        "{case}: one logical delivery"
    );
}

const POINTS: &[&str] = &[
    "trigger.claim.claimed",
    "trigger.claim.recorded",
    "trigger.claimed",
    "trigger.fallback.registered",
    "trigger.fallback.stored",
    "trigger.fallback.history",
    "trigger.fallback.routed",
    "trigger.published",
    "trigger.completed",
];

#[tokio::test]
async fn fallback_trigger_publication_is_exactly_once_after_failure_at_every_boundary() {
    let _serial = SERIAL.lock().await;
    for sequential_claims in [false, true] {
        for point in POINTS {
            // The atomic memory claim has no internal boundaries.
            if !sequential_claims && point.starts_with("trigger.claim.") {
                continue;
            }
            let case = format!("{point} sequential_claims={sequential_claims}");
            let app = app(sequential_claims).await;
            failpoints::arm_error(Some(point));
            let failed = app.trigger_loop_iteration(&runner()).await;
            failpoints::arm_error(None);
            assert!(failed.is_err(), "{case}: failpoint was not reached");
            let recovered = app.trigger_loop_iteration(&runner()).await.unwrap();
            assert!(app
                .trigger_loop_iteration(&runner())
                .await
                .unwrap()
                .is_empty());
            // Only a failure after completion leaves nothing for recovery to do.
            let expected = usize::from(*point != "trigger.completed");
            assert_eq!(
                recovered.len(),
                expected,
                "{case}: recovery publishes the firing"
            );
            assert_exactly_once(&app, &case).await;
        }
    }
}

#[tokio::test]
async fn unrouted_trigger_run_stays_pending_until_a_runner_can_publish_it() {
    let _serial = SERIAL.lock().await;
    let mut app = unrouted_app(false).await;
    assert!(app
        .trigger_loop_iteration(&runner())
        .await
        .unwrap()
        .is_empty());
    let store = app.trigger_manager().unwrap().store();
    assert_eq!(store.get_pending_trigger_runs(10).await.unwrap().len(), 1);
    register_target(&mut app);
    assert_eq!(
        app.trigger_loop_iteration(&runner()).await.unwrap().len(),
        1
    );
    assert_exactly_once(&app, "unrouted").await;
}

/// A condition shared by two triggers is consumed only after both are claimed.
#[tokio::test]
async fn shared_condition_fires_every_trigger_after_failure_between_claims() {
    let _serial = SERIAL.lock().await;
    for (sequential_claims, point) in [
        (true, "trigger.claim.claimed"),
        (true, "trigger.claim.recorded"),
        (false, "trigger.claimed"),
    ] {
        let case = format!("{point} sequential_claims={sequential_claims}");
        let mut app = app(sequential_claims).await;
        let second = TaskId::new("fallback", "second_target");
        app.register_task(
            second.clone(),
            TaskConfig::default(),
            Arc::new(|_| Ok("null".into())),
        )
        .unwrap();
        let manager = app.trigger_manager().unwrap().clone();
        TriggerBuilder::new()
            .on_event("shared_ready")
            .build_and_register(&target(), manager.store())
            .await
            .unwrap();
        TriggerBuilder::new()
            .on_event("shared_ready")
            .build_and_register(&second, manager.store())
            .await
            .unwrap();
        // Drain the fixture's own firing first.
        app.trigger_loop_iteration(&runner()).await.unwrap();
        manager
            .emit_event("shared_ready", serde_json::json!({}))
            .await
            .unwrap();

        failpoints::arm_error(Some(point));
        assert!(
            app.trigger_loop_iteration(&runner()).await.is_err(),
            "{case}"
        );
        failpoints::arm_error(None);
        app.trigger_loop_iteration(&runner()).await.unwrap();
        app.trigger_loop_iteration(&runner()).await.unwrap();

        for task in [target(), second.clone()] {
            let invocations = app
                .orchestrator()
                .get_existing_invocations(&task, None, ALL_STATUSES)
                .await
                .unwrap();
            // target(): one from the fixture event, one from the shared event.
            let expected = if task == second { 1 } else { 2 };
            assert_eq!(invocations.len(), expected, "{case}: {task}");
        }
        assert!(manager
            .store()
            .get_pending_trigger_runs(10)
            .await
            .unwrap()
            .is_empty());
    }
}
