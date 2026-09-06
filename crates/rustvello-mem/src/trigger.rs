//! In-memory trigger store implementation.

use std::collections::HashMap;
use tokio::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rustvello_core::error::RustvelloResult;
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionId, EventQuery, EventRecord, TriggerCondition, TriggerDefinitionDTO,
    TriggerDefinitionId, TriggerRunId, TriggerRunQuery, TriggerRunRecord, ValidCondition,
};

struct TriggerState {
    conditions: HashMap<String, TriggerCondition>,
    /// source_task_id -> list of condition IDs
    source_task_conditions: HashMap<String, Vec<ConditionId>>,
    /// event_code -> list of condition IDs
    event_conditions: HashMap<String, Vec<ConditionId>>,
    /// condition IDs of cron conditions
    cron_condition_ids: Vec<ConditionId>,
    triggers: HashMap<String, TriggerDefinitionDTO>,
    /// condition_id -> list of trigger IDs
    condition_triggers: HashMap<String, Vec<TriggerDefinitionId>>,
    valid_conditions: HashMap<String, ValidCondition>,
    cron_executions: HashMap<String, DateTime<Utc>>,
    trigger_run_claims: HashMap<String, DateTime<Utc>>,
    events: HashMap<String, EventRecord>,
    trigger_runs: HashMap<String, TriggerRunRecord>,
}

/// In-memory trigger store for testing and development.
pub struct MemTriggerStore {
    state: Mutex<TriggerState>,
}

impl MemTriggerStore {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(TriggerState {
                conditions: HashMap::new(),
                source_task_conditions: HashMap::new(),
                event_conditions: HashMap::new(),
                cron_condition_ids: Vec::new(),
                triggers: HashMap::new(),
                condition_triggers: HashMap::new(),
                valid_conditions: HashMap::new(),
                cron_executions: HashMap::new(),
                trigger_run_claims: HashMap::new(),
                events: HashMap::new(),
                trigger_runs: HashMap::new(),
            }),
        }
    }
}

impl Default for MemTriggerStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TriggerStore for MemTriggerStore {
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId> {
        let cond_id = condition.condition_id();
        let mut state = self.state.lock().await;

        state
            .conditions
            .insert(cond_id.as_str().to_owned(), condition.clone());

        // Index by source task IDs
        for task_id in condition.source_task_ids() {
            let vec = state
                .source_task_conditions
                .entry(task_id.to_string())
                .or_default();
            if !vec.contains(&cond_id) {
                vec.push(cond_id.clone());
            }
        }

        // Index by event code
        if let TriggerCondition::Event(evt) = condition {
            let vec = state
                .event_conditions
                .entry(evt.event_code.clone())
                .or_default();
            if !vec.contains(&cond_id) {
                vec.push(cond_id.clone());
            }
        }

        // Track cron conditions
        if matches!(condition, TriggerCondition::Cron(_))
            && !state.cron_condition_ids.contains(&cond_id)
        {
            state.cron_condition_ids.push(cond_id.clone());
        }

        Ok(cond_id)
    }

    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        let state = self.state.lock().await;
        Ok(state.conditions.get(id.as_str()).cloned())
    }

    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let state = self.state.lock().await;
        let key = task_id.to_string();
        let cond_ids = state.source_task_conditions.get(&key);

        let mut result = Vec::new();
        if let Some(ids) = cond_ids {
            for cid in ids {
                if let Some(cond) = state.conditions.get(cid.as_str()) {
                    result.push((cid.clone(), cond.clone()));
                }
            }
        }
        Ok(result)
    }

    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let state = self.state.lock().await;
        let mut result = Vec::new();
        for cid in &state.cron_condition_ids {
            if let Some(cond) = state.conditions.get(cid.as_str()) {
                result.push((cid.clone(), cond.clone()));
            }
        }
        Ok(result)
    }

    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let state = self.state.lock().await;
        let cond_ids = state.event_conditions.get(event_code);

        let mut result = Vec::new();
        if let Some(ids) = cond_ids {
            for cid in ids {
                if let Some(cond) = state.conditions.get(cid.as_str()) {
                    result.push((cid.clone(), cond.clone()));
                }
            }
        }
        Ok(result)
    }

    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;

        state
            .triggers
            .insert(trigger.trigger_id.as_str().to_owned(), trigger.clone());

        // Index condition -> trigger
        for cid in &trigger.condition_ids {
            state
                .condition_triggers
                .entry(cid.as_str().to_owned())
                .or_default()
                .push(trigger.trigger_id.clone());
        }

        Ok(())
    }

    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        let state = self.state.lock().await;
        Ok(state.triggers.get(id.as_str()).cloned())
    }

    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        let state = self.state.lock().await;
        let trigger_ids = state.condition_triggers.get(cond_id.as_str());

        let mut result = Vec::new();
        if let Some(ids) = trigger_ids {
            for tid in ids {
                if let Some(trigger) = state.triggers.get(tid.as_str()) {
                    result.push(trigger.clone());
                }
            }
        }
        Ok(result)
    }

    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        let mut state = self.state.lock().await;
        let task_str = task_id.to_string();

        let ids_to_remove: Vec<String> = state
            .triggers
            .iter()
            .filter(|(_, t)| t.task_id.to_string() == task_str)
            .map(|(id, _)| id.clone())
            .collect();

        let count = u32::try_from(ids_to_remove.len()).unwrap_or(u32::MAX);
        for id in &ids_to_remove {
            if let Some(trigger) = state.triggers.remove(id) {
                // Remove from condition_triggers index
                for cid in &trigger.condition_ids {
                    if let Some(tids) = state.condition_triggers.get_mut(cid.as_str()) {
                        tids.retain(|tid| tid.as_str() != *id);
                    }
                }
            }
        }

        Ok(count)
    }

    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .valid_conditions
            .insert(vc.valid_condition_id.clone(), vc.clone());
        Ok(())
    }

    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let state = self.state.lock().await;
        Ok(state.valid_conditions.values().cloned().collect())
    }

    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        for id in ids {
            state.valid_conditions.remove(id);
        }
        Ok(())
    }

    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        let state = self.state.lock().await;
        Ok(state.cron_executions.get(cond_id.as_str()).copied())
    }

    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        let mut state = self.state.lock().await;
        let current = state.cron_executions.get(cond_id.as_str()).copied();

        // Optimistic locking: only update if current matches expected
        if current == expected_last {
            state
                .cron_executions
                .insert(cond_id.as_str().to_owned(), time);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        let mut state = self.state.lock().await;
        if state.trigger_run_claims.contains_key(run_id.as_str()) {
            Ok(false)
        } else {
            state
                .trigger_run_claims
                .insert(run_id.as_str().to_owned(), Utc::now());
            Ok(true)
        }
    }

    async fn store_event(&self, event: &EventRecord) -> RustvelloResult<()> {
        self.state
            .lock()
            .await
            .events
            .insert(event.event_id.clone(), event.clone());
        Ok(())
    }

    async fn get_event(&self, event_id: &str) -> RustvelloResult<Option<EventRecord>> {
        Ok(self.state.lock().await.events.get(event_id).cloned())
    }

    async fn get_events(&self, query: &EventQuery) -> RustvelloResult<Vec<EventRecord>> {
        let state = self.state.lock().await;
        let mut events: Vec<EventRecord> = state
            .events
            .values()
            .filter(|event| {
                query
                    .event_code
                    .as_ref()
                    .is_none_or(|code| &event.event_code == code)
                    && query
                        .emitted_by_invocation_id
                        .as_ref()
                        .is_none_or(|id| event.emitted_by_invocation_id.as_ref() == Some(id))
                    && query.start.is_none_or(|start| event.timestamp >= start)
                    && query.end.is_none_or(|end| event.timestamp <= end)
                    && query
                        .matched
                        .is_none_or(|matched| event.is_matched() == matched)
                    && query
                        .triggered
                        .is_none_or(|triggered| event.is_triggered() == triggered)
            })
            .cloned()
            .collect();
        events.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
        if let Some(offset) = query.offset {
            events = events.into_iter().skip(offset).collect();
        }
        if let Some(limit) = query.limit {
            events.truncate(limit);
        }
        Ok(events)
    }

    async fn store_trigger_run(&self, run: &TriggerRunRecord) -> RustvelloResult<()> {
        self.state
            .lock()
            .await
            .trigger_runs
            .insert(run.trigger_run_id.to_string(), run.clone());
        Ok(())
    }

    async fn get_trigger_run(
        &self,
        run_id: &TriggerRunId,
    ) -> RustvelloResult<Option<TriggerRunRecord>> {
        Ok(self
            .state
            .lock()
            .await
            .trigger_runs
            .get(run_id.as_str())
            .cloned())
    }

    async fn get_trigger_runs(
        &self,
        query: &TriggerRunQuery,
    ) -> RustvelloResult<Vec<TriggerRunRecord>> {
        let state = self.state.lock().await;
        let mut runs: Vec<TriggerRunRecord> = state
            .trigger_runs
            .values()
            .filter(|run| {
                query
                    .event_id
                    .as_deref()
                    .is_none_or(|id| run.event_ids().contains(&id))
                    && query
                        .source_invocation_id
                        .as_ref()
                        .is_none_or(|id| run.source_invocation_ids().contains(&id))
                    && query
                        .triggered_invocation_id
                        .as_ref()
                        .is_none_or(|id| run.triggered_invocation_id.as_ref() == Some(id))
                    && query.start.is_none_or(|start| run.claimed_at >= start)
                    && query.end.is_none_or(|end| run.claimed_at <= end)
            })
            .cloned()
            .collect();
        runs.sort_by(|left, right| right.claimed_at.cmp(&left.claimed_at));
        if let Some(limit) = query.limit {
            runs.truncate(limit);
        }
        Ok(runs)
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state.conditions.clear();
        state.source_task_conditions.clear();
        state.event_conditions.clear();
        state.cron_condition_ids.clear();
        state.triggers.clear();
        state.condition_triggers.clear();
        state.valid_conditions.clear();
        state.cron_executions.clear();
        state.trigger_run_claims.clear();
        state.events.clear();
        state.trigger_runs.clear();
        Ok(())
    }

    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let state = self.state.lock().await;
        Ok(state
            .conditions
            .iter()
            .map(|(id, cond)| (ConditionId::from(id.clone()), cond.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_proto::trigger::*;

    #[tokio::test]
    async fn register_and_get_condition() {
        let store = MemTriggerStore::new();
        let cond = TriggerCondition::Event(EventCondition {
            event_code: "payment".to_string(),
            payload_filter: None,
        });
        let id = store.register_condition(&cond).await.unwrap();
        let got = store.get_condition(&id).await.unwrap();
        assert!(got.is_some());
        assert_eq!(got.unwrap().condition_id(), id);
    }

    #[tokio::test]
    async fn get_conditions_for_task() {
        let store = MemTriggerStore::new();
        let task_id = TaskId::new("mod", "task");
        let cond = TriggerCondition::Status(StatusCondition {
            task_id: task_id.clone(),
            statuses: vec![rustvello_proto::status::InvocationStatus::Success],
            argument_filter: None,
        });
        store.register_condition(&cond).await.unwrap();

        let conds = store.get_conditions_for_task(&task_id).await.unwrap();
        assert_eq!(conds.len(), 1);

        let other = TaskId::new("mod", "other");
        let conds = store.get_conditions_for_task(&other).await.unwrap();
        assert!(conds.is_empty());
    }

    #[tokio::test]
    async fn get_event_conditions() {
        let store = MemTriggerStore::new();
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
    async fn get_cron_conditions() {
        let store = MemTriggerStore::new();
        let cond = TriggerCondition::Cron(CronCondition {
            cron_expression: "* * * * *".to_string(),
            min_interval_seconds: 50,
        });
        store.register_condition(&cond).await.unwrap();

        let conds = store.get_cron_conditions().await.unwrap();
        assert_eq!(conds.len(), 1);
    }

    #[tokio::test]
    async fn register_and_get_trigger() {
        let store = MemTriggerStore::new();
        let task_id = TaskId::new("mod", "target");
        let cond_ids = vec![ConditionId::from("c1".to_string())];
        let trigger_id =
            TriggerDefinitionDTO::compute_trigger_id(&task_id, &cond_ids, TriggerLogic::Or);

        let trigger = TriggerDefinitionDTO {
            trigger_id: trigger_id.clone(),
            task_id,
            condition_ids: cond_ids,
            logic: TriggerLogic::Or,
            argument_template: None,
        };
        store.register_trigger(&trigger).await.unwrap();

        let got = store.get_trigger(&trigger_id).await.unwrap();
        assert!(got.is_some());
    }

    #[tokio::test]
    async fn get_triggers_for_condition() {
        let store = MemTriggerStore::new();
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
        let store = MemTriggerStore::new();
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
        let store = MemTriggerStore::new();
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
        let store = MemTriggerStore::new();
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

    #[tokio::test]
    async fn claim_trigger_run_dedup() {
        let store = MemTriggerStore::new();
        let run_id = TriggerRunId::from("run-1".to_string());

        assert!(store.claim_trigger_run(&run_id).await.unwrap());
        assert!(!store.claim_trigger_run(&run_id).await.unwrap());
    }

    #[tokio::test]
    async fn purge_clears_all() {
        let store = MemTriggerStore::new();
        let cond = TriggerCondition::Event(EventCondition {
            event_code: "test".to_string(),
            payload_filter: None,
        });
        store.register_condition(&cond).await.unwrap();
        store.purge().await.unwrap();

        let got = store.get_event_conditions("test").await.unwrap();
        assert!(got.is_empty());
    }
}
