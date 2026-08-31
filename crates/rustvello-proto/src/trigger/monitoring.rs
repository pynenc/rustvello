//! Durable monitoring records for emitted events and trigger executions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::identifiers::{InvocationId, RunnerId, TaskId};

use super::{ConditionId, TriggerDefinitionId, TriggerLogic, TriggerRunId};

/// One emitted custom event, persisted whether or not a condition matched.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub event_id: String,
    pub event_code: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    pub matched_condition_ids: Vec<ConditionId>,
    pub valid_condition_ids: Vec<String>,
    pub triggered_invocation_ids: Vec<InvocationId>,
    pub emitted_by_invocation_id: Option<InvocationId>,
    pub emitted_by_task_id: Option<TaskId>,
    pub emitted_by_runner_id: Option<RunnerId>,
}

impl EventRecord {
    pub fn is_matched(&self) -> bool {
        !self.matched_condition_ids.is_empty()
    }

    pub fn is_triggered(&self) -> bool {
        !self.triggered_invocation_ids.is_empty()
    }
}

/// Source that satisfied one condition participating in a trigger run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerRunParticipant {
    pub context_type: String,
    pub condition_id: ConditionId,
    pub valid_condition_id: String,
    pub event_id: Option<String>,
    pub source_invocation_id: Option<InvocationId>,
    pub context_summary: String,
}

/// Durable record of one claimed trigger execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerRunRecord {
    pub trigger_run_id: TriggerRunId,
    pub trigger_id: TriggerDefinitionId,
    pub task_id: TaskId,
    pub logic: TriggerLogic,
    pub arguments: serde_json::Value,
    pub participants: Vec<TriggerRunParticipant>,
    pub claimed_at: DateTime<Utc>,
    pub executed_at: Option<DateTime<Utc>>,
    pub triggered_invocation_id: Option<InvocationId>,
    pub atomic_service_run_id: Option<String>,
    pub atomic_service_runner_id: Option<RunnerId>,
}

impl TriggerRunRecord {
    pub fn event_ids(&self) -> Vec<&str> {
        self.participants
            .iter()
            .filter_map(|participant| participant.event_id.as_deref())
            .collect()
    }

    pub fn source_invocation_ids(&self) -> Vec<&InvocationId> {
        self.participants
            .iter()
            .filter_map(|participant| participant.source_invocation_id.as_ref())
            .collect()
    }
}

/// Filters for monitoring event queries.
#[derive(Debug, Clone, Default)]
pub struct EventQuery {
    pub event_code: Option<String>,
    pub emitted_by_invocation_id: Option<InvocationId>,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}

/// Filters for monitoring trigger-run queries.
#[derive(Debug, Clone, Default)]
pub struct TriggerRunQuery {
    pub event_id: Option<String>,
    pub source_invocation_id: Option<InvocationId>,
    pub triggered_invocation_id: Option<InvocationId>,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<usize>,
}
