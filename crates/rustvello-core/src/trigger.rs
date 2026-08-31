//! Trigger system — backend trait and evaluation logic.
//!
//! Provides [`TriggerStore`] (async trait for persistence) and
//! [`TriggerManager`] (business logic for condition evaluation and
//! trigger firing).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use croner::Cron;

use crate::context::{get_invocation_context, get_or_create_runner_context};
use crate::error::RustvelloResult;
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionContext, ConditionId, EventQuery, EventRecord, TriggerCondition, TriggerDefinitionDTO,
    TriggerDefinitionId, TriggerLogic, TriggerRunId, TriggerRunParticipant, TriggerRunQuery,
    TriggerRunRecord, ValidCondition,
};

// ---------------------------------------------------------------------------
// TriggerStore — backend trait
// ---------------------------------------------------------------------------

/// Async persistence interface for the trigger subsystem.
///
/// Mirrors pynenc's `BaseTrigger` storage methods. Implementations must
/// be thread-safe (`Send + Sync`).
#[async_trait]
pub trait TriggerStore: Send + Sync {
    // -- Condition CRUD --

    /// Register a condition and return its deterministic ID.
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId>;

    /// Get a condition by ID.
    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>>;

    /// Get all conditions that watch a specific task.
    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>>;

    /// Get all cron conditions.
    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>>;

    /// Get all event conditions matching an event code.
    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>>;

    // -- Trigger definition CRUD --

    /// Register a trigger definition.
    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()>;

    /// Get a trigger definition by ID.
    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>>;

    /// Get all trigger definitions that reference a given condition.
    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>>;

    /// Remove all trigger definitions for a task.
    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32>;

    // -- Valid condition management --

    /// Record a condition that has been evaluated and found satisfied.
    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()>;

    /// Get all pending valid conditions.
    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>>;

    /// Clear valid conditions by their IDs (after processing).
    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()>;

    // -- Cron state --

    /// Get the last cron execution time for a condition.
    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>>;

    /// Store a cron execution time with optimistic locking.
    /// Returns `true` if the store succeeded (expected_last matched).
    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool>;

    // -- Execution claims (distributed dedup) --

    /// Attempt to claim a trigger run. Returns `true` if this is first claim.
    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool>;

    // -- Monitoring records --

    /// Persist an emitted event, including events that matched no condition.
    async fn store_event(&self, event: &EventRecord) -> RustvelloResult<()>;

    /// Look up one persisted event by ID.
    async fn get_event(&self, event_id: &str) -> RustvelloResult<Option<EventRecord>>;

    /// Query persisted events using the supplied filters.
    async fn get_events(&self, query: &EventQuery) -> RustvelloResult<Vec<EventRecord>>;

    /// Persist one claimed trigger run and its participants.
    async fn store_trigger_run(&self, run: &TriggerRunRecord) -> RustvelloResult<()>;

    /// Look up one persisted trigger run by ID.
    async fn get_trigger_run(
        &self,
        run_id: &TriggerRunId,
    ) -> RustvelloResult<Option<TriggerRunRecord>>;

    /// Query persisted trigger runs using the supplied filters.
    async fn get_trigger_runs(
        &self,
        query: &TriggerRunQuery,
    ) -> RustvelloResult<Vec<TriggerRunRecord>>;

    /// Purge all trigger data.
    async fn purge(&self) -> RustvelloResult<()>;

    /// Get all registered conditions regardless of type.
    ///
    /// Every backend **must** return the complete set of stored conditions
    /// (Cron, Status, Event, Result, Exception, Composite).
    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>>;
}

// ---------------------------------------------------------------------------
// TriggerManager — evaluation logic
// ---------------------------------------------------------------------------

/// Business logic layer for the trigger system.
///
/// Wraps a `dyn TriggerStore` and implements condition evaluation,
/// trigger firing, and execution dedup. Modelled after pynenc's
/// `BaseTrigger` evaluation methods.
#[derive(Clone)]
pub struct TriggerManager {
    store: Arc<dyn TriggerStore>,
}

/// A claimed trigger execution ready for invocation creation.
#[derive(Debug, Clone)]
pub struct TriggerExecution {
    pub run_id: TriggerRunId,
    pub trigger: TriggerDefinitionDTO,
    pub arguments: serde_json::Value,
}

impl TriggerManager {
    pub fn new(store: Arc<dyn TriggerStore>) -> Self {
        Self { store }
    }

    /// Access the underlying store (for registration passthrough).
    pub fn store(&self) -> &Arc<dyn TriggerStore> {
        &self.store
    }

    // -- Event reporting (called by runner after task completion) --

    /// Match conditions for a task against a context, record valid ones, and return them.
    async fn evaluate_task_conditions(
        &self,
        task_id: &rustvello_proto::identifiers::TaskId,
        condition_ctx: ConditionContext,
    ) -> RustvelloResult<Vec<ValidCondition>> {
        let conditions = self.store.get_conditions_for_task(task_id).await?;
        let mut valid = Vec::new();

        for (cond_id, cond) in &conditions {
            if cond.is_satisfied_by(&condition_ctx) {
                let vc = ValidCondition::new(cond_id.clone(), condition_ctx.clone());
                self.store.record_valid_condition(&vc).await?;
                valid.push(vc);
            }
        }

        Ok(valid)
    }

    /// Report a status change — finds and records matching StatusConditions.
    pub async fn report_status_change(
        &self,
        ctx: &rustvello_proto::trigger::StatusContext,
    ) -> RustvelloResult<Vec<ValidCondition>> {
        self.evaluate_task_conditions(&ctx.task_id, ConditionContext::Status(ctx.clone()))
            .await
    }

    /// Report a successful task result — finds and records matching ResultConditions.
    pub async fn report_result(
        &self,
        ctx: &rustvello_proto::trigger::ResultContext,
    ) -> RustvelloResult<Vec<ValidCondition>> {
        self.evaluate_task_conditions(&ctx.task_id, ConditionContext::Result(ctx.clone()))
            .await
    }

    /// Report a task failure — finds and records matching ExceptionConditions.
    pub async fn report_failure(
        &self,
        ctx: &rustvello_proto::trigger::ExceptionContext,
    ) -> RustvelloResult<Vec<ValidCondition>> {
        self.evaluate_task_conditions(&ctx.task_id, ConditionContext::Exception(ctx.clone()))
            .await
    }

    /// Emit a custom event — finds and records matching EventConditions.
    /// Returns the generated event ID.
    pub async fn emit_event(
        &self,
        event_code: &str,
        payload: serde_json::Value,
    ) -> RustvelloResult<String> {
        let event_id = uuid::Uuid::new_v4().to_string();
        let event_ctx = rustvello_proto::trigger::EventContext {
            event_id: event_id.clone(),
            event_code: event_code.to_string(),
            payload: payload.clone(),
        };
        let condition_ctx = ConditionContext::Event(event_ctx);

        let invocation_ctx = get_invocation_context();
        let runner_ctx = get_or_create_runner_context();
        let mut event_record = EventRecord {
            event_id: event_id.clone(),
            event_code: event_code.to_string(),
            payload,
            timestamp: Utc::now(),
            matched_condition_ids: Vec::new(),
            valid_condition_ids: Vec::new(),
            triggered_invocation_ids: Vec::new(),
            emitted_by_invocation_id: invocation_ctx
                .as_ref()
                .map(|context| context.invocation_id.clone()),
            emitted_by_task_id: invocation_ctx.map(|context| context.task_id),
            emitted_by_runner_id: Some(runner_ctx.runner_id),
        };
        self.store_event_for_monitoring(&event_record).await;

        let conditions = self.store.get_event_conditions(event_code).await?;
        for (cond_id, cond) in &conditions {
            if cond.is_satisfied_by(&condition_ctx) {
                let vc = ValidCondition::new(cond_id.clone(), condition_ctx.clone());
                self.store.record_valid_condition(&vc).await?;
                event_record.matched_condition_ids.push(cond_id.clone());
                event_record.valid_condition_ids.push(vc.valid_condition_id);
            }
        }

        if event_record.is_matched() {
            self.store_event_for_monitoring(&event_record).await;
        }

        Ok(event_id)
    }

    async fn store_event_for_monitoring(&self, event: &EventRecord) {
        if let Err(error) = self.store.store_event(event).await {
            tracing::debug!(%error, event_id = %event.event_id, "event monitoring record unavailable");
        }
    }

    // -- Cron evaluation --

    /// Evaluate all cron conditions against the current time.
    ///
    /// For each cron condition:
    /// 1. Parse the `cron_expression` with the `croner` crate; log and skip on syntax error.
    /// 2. Check whether the current minute matches the schedule via `is_time_matched`.
    /// 3. Also enforce `min_interval_seconds` to prevent double-firing within the same minute.
    /// 4. Use optimistic locking (`store_cron_execution`) across multiple runner instances.
    pub async fn evaluate_cron_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let cron_conditions = self.store.get_cron_conditions().await?;
        let now = Utc::now();
        let mut valid = Vec::new();

        for (cond_id, cond) in &cron_conditions {
            if let rustvello_proto::trigger::TriggerCondition::Cron(cron) = cond {
                // Parse and validate the cron expression.
                let schedule = match Cron::from_str(&cron.cron_expression) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(
                            "Cron condition {} has invalid expression {:?}: {}",
                            cond_id,
                            cron.cron_expression,
                            e
                        );
                        continue;
                    }
                };

                // Check minimum interval (prevents double-firing within the same schedule slot).
                let last_exec = self.store.get_last_cron_execution(cond_id).await?;
                let interval_ok = match last_exec {
                    Some(last) => {
                        (now - last).num_seconds()
                            >= i64::try_from(cron.min_interval_seconds).unwrap_or(i64::MAX)
                    }
                    None => true,
                };
                if !interval_ok {
                    continue;
                }

                // Check if the current time matches the cron schedule.
                let matches = match schedule.is_time_matching(&now) {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!(
                            "Cron match check failed for condition {} (expr {:?}): {}",
                            cond_id,
                            cron.cron_expression,
                            e
                        );
                        continue;
                    }
                };
                if !matches {
                    continue;
                }

                // Optimistic lock — only one runner claims this execution slot.
                let claimed = self
                    .store
                    .store_cron_execution(cond_id, now, last_exec)
                    .await?;

                if claimed {
                    let ctx = ConditionContext::Cron(rustvello_proto::trigger::CronContext {
                        timestamp: now,
                        last_execution: last_exec,
                    });
                    let vc = ValidCondition::new(cond_id.clone(), ctx);
                    self.store.record_valid_condition(&vc).await?;
                    valid.push(vc);
                }
            }
        }

        Ok(valid)
    }

    // -- Trigger evaluation pipeline --

    /// Process all pending valid conditions and determine which triggers should fire.
    ///
    /// Returns a list of (trigger definition, arguments) pairs ready for invocation.
    pub async fn evaluate_trigger_runs(&self) -> RustvelloResult<Vec<TriggerExecution>> {
        let valid_conditions = self.store.get_valid_conditions().await?;
        if valid_conditions.is_empty() {
            return Ok(vec![]);
        }

        // Group valid conditions by condition_id
        let mut by_condition: HashMap<ConditionId, Vec<&ValidCondition>> = HashMap::new();
        for vc in &valid_conditions {
            by_condition
                .entry(vc.condition_id.clone())
                .or_default()
                .push(vc);
        }

        // Find all trigger definitions affected by these conditions
        let mut trigger_map: HashMap<TriggerDefinitionId, TriggerDefinitionDTO> = HashMap::new();
        for cond_id in by_condition.keys() {
            let triggers = self.store.get_triggers_for_condition(cond_id).await?;
            for t in triggers {
                trigger_map.entry(t.trigger_id.clone()).or_insert(t);
            }
        }

        let mut to_invoke = Vec::new();
        let mut to_clear: Vec<String> = Vec::new();

        for trigger in trigger_map.values() {
            match trigger.logic {
                TriggerLogic::And => {
                    // All conditions must be satisfied
                    let all_satisfied = trigger
                        .condition_ids
                        .iter()
                        .all(|cid| by_condition.contains_key(cid));

                    if all_satisfied {
                        // Build a run ID from all valid condition IDs (deterministic)
                        let mut vc_ids: Vec<String> = trigger
                            .condition_ids
                            .iter()
                            .filter_map(|cid| {
                                by_condition.get(cid).and_then(|vcs| {
                                    vcs.first().map(|vc| vc.valid_condition_id.clone())
                                })
                            })
                            .collect();
                        vc_ids.sort();
                        let run_id = TriggerRunId::from(format!(
                            "run_{}_{}",
                            trigger.trigger_id.as_str(),
                            vc_ids.join("_")
                        ));

                        if self.store.claim_trigger_run(&run_id).await? {
                            let args = trigger
                                .argument_template
                                .clone()
                                .unwrap_or(serde_json::Value::Object(Default::default()));
                            let selected: Vec<ValidCondition> = trigger
                                .condition_ids
                                .iter()
                                .filter_map(|cid| {
                                    by_condition.get(cid)?.first().map(|vc| (*vc).clone())
                                })
                                .collect();
                            self.record_claimed_run(&run_id, trigger, &args, &selected)
                                .await;
                            to_invoke.push(TriggerExecution {
                                run_id,
                                trigger: trigger.clone(),
                                arguments: args,
                            });

                            // Mark all valid conditions used in this trigger for clearing
                            for cid in &trigger.condition_ids {
                                if let Some(vcs) = by_condition.get(cid) {
                                    for vc in vcs {
                                        to_clear.push(vc.valid_condition_id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                TriggerLogic::Or => {
                    // Any condition is sufficient — one invocation per valid condition
                    for cid in &trigger.condition_ids {
                        if let Some(vcs) = by_condition.get(cid) {
                            for vc in vcs {
                                let run_id = TriggerRunId::from(format!(
                                    "run_{}_{}",
                                    trigger.trigger_id.as_str(),
                                    vc.valid_condition_id
                                ));

                                if self.store.claim_trigger_run(&run_id).await? {
                                    let args = trigger
                                        .argument_template
                                        .clone()
                                        .unwrap_or(serde_json::Value::Object(Default::default()));
                                    self.record_claimed_run(
                                        &run_id,
                                        trigger,
                                        &args,
                                        &[(*vc).clone()],
                                    )
                                    .await;
                                    to_invoke.push(TriggerExecution {
                                        run_id,
                                        trigger: trigger.clone(),
                                        arguments: args,
                                    });
                                    to_clear.push(vc.valid_condition_id.clone());
                                }
                            }
                        }
                    }
                }
                _ => {
                    // Future logic variants: treat like Or for forward-compat
                    tracing::warn!(
                        trigger_id = %trigger.trigger_id,
                        logic = ?trigger.logic,
                        "Unknown TriggerLogic variant; falling back to Or semantics"
                    );
                    for cid in &trigger.condition_ids {
                        if let Some(vcs) = by_condition.get(cid) {
                            for vc in vcs {
                                let run_id = TriggerRunId::from(format!(
                                    "run_{}_{}",
                                    trigger.trigger_id.as_str(),
                                    vc.valid_condition_id
                                ));
                                if self.store.claim_trigger_run(&run_id).await? {
                                    let args = trigger
                                        .argument_template
                                        .clone()
                                        .unwrap_or(serde_json::Value::Object(Default::default()));
                                    self.record_claimed_run(
                                        &run_id,
                                        trigger,
                                        &args,
                                        &[(*vc).clone()],
                                    )
                                    .await;
                                    to_invoke.push(TriggerExecution {
                                        run_id,
                                        trigger: trigger.clone(),
                                        arguments: args,
                                    });
                                    to_clear.push(vc.valid_condition_id.clone());
                                }
                            }
                        }
                    }
                }
            }
        }

        // Clear processed valid conditions
        if !to_clear.is_empty() {
            self.store.clear_valid_conditions(&to_clear).await?;
        }

        Ok(to_invoke)
    }

    /// Compatibility projection for callers that do not need run attribution.
    pub async fn evaluate_triggers(
        &self,
    ) -> RustvelloResult<Vec<(TriggerDefinitionDTO, serde_json::Value)>> {
        Ok(self
            .evaluate_trigger_runs()
            .await?
            .into_iter()
            .map(|execution| (execution.trigger, execution.arguments))
            .collect())
    }

    async fn record_claimed_run(
        &self,
        run_id: &TriggerRunId,
        trigger: &TriggerDefinitionDTO,
        arguments: &serde_json::Value,
        valid_conditions: &[ValidCondition],
    ) {
        let participants = valid_conditions
            .iter()
            .map(participant_from_valid_condition)
            .collect();
        let record = TriggerRunRecord {
            trigger_run_id: run_id.clone(),
            trigger_id: trigger.trigger_id.clone(),
            task_id: trigger.task_id.clone(),
            logic: trigger.logic,
            arguments: arguments.clone(),
            participants,
            claimed_at: Utc::now(),
            executed_at: None,
            triggered_invocation_id: None,
            atomic_service_run_id: None,
            atomic_service_runner_id: None,
        };
        if let Err(error) = self.store.store_trigger_run(&record).await {
            tracing::debug!(%error, trigger_run_id = %run_id, "trigger-run monitoring record unavailable");
        }
    }

    /// Attach the invocation created for a previously claimed trigger run.
    pub async fn complete_trigger_run(
        &self,
        run_id: &TriggerRunId,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let Some(mut run) = self.store.get_trigger_run(run_id).await? else {
            return Ok(());
        };
        run.triggered_invocation_id = Some(invocation_id.clone());
        run.executed_at = Some(Utc::now());
        self.store.store_trigger_run(&run).await?;

        for event_id in run.event_ids() {
            if let Some(mut event) = self.store.get_event(event_id).await? {
                if !event.triggered_invocation_ids.contains(invocation_id) {
                    event.triggered_invocation_ids.push(invocation_id.clone());
                    self.store.store_event(&event).await?;
                }
            }
        }
        Ok(())
    }
}

fn participant_from_valid_condition(vc: &ValidCondition) -> TriggerRunParticipant {
    let (context_type, event_id, source_invocation_id, context_summary) = match &vc.context {
        ConditionContext::Event(context) => (
            "event",
            Some(context.event_id.clone()),
            None,
            context.event_code.clone(),
        ),
        ConditionContext::Status(context) => (
            "status",
            None,
            Some(context.invocation_id.clone()),
            format!("{} {:?}", context.task_id, context.status),
        ),
        ConditionContext::Result(context) => (
            "result",
            None,
            Some(context.invocation_id.clone()),
            context.task_id.to_string(),
        ),
        ConditionContext::Exception(context) => (
            "exception",
            None,
            Some(context.invocation_id.clone()),
            format!("{} {}", context.task_id, context.error_type),
        ),
        ConditionContext::Cron(context) => ("cron", None, None, context.timestamp.to_rfc3339()),
        _ => ("unknown", None, None, String::new()),
    };
    TriggerRunParticipant {
        context_type: context_type.into(),
        condition_id: vc.condition_id.clone(),
        valid_condition_id: vc.valid_condition_id.clone(),
        event_id,
        source_invocation_id,
        context_summary,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TriggerManager tests require a backend — see rustvello-mem tests
    // and integration tests. Here we just verify construction.

    #[test]
    fn trigger_logic_display() {
        assert_eq!(TriggerLogic::And.to_string(), "AND");
        assert_eq!(TriggerLogic::Or.to_string(), "OR");
    }
}
