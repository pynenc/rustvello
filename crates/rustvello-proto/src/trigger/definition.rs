//! Trigger definition DTO and valid condition types.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identifiers::TaskId;

use super::context::ConditionContext;
use super::{ConditionId, TriggerDefinitionId, TriggerLogic};

/// A trigger definition links conditions to a target task.
///
/// When the conditions are satisfied (according to `logic`), the target
/// task is invoked with the given `argument_template`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDefinitionDTO {
    pub trigger_id: TriggerDefinitionId,
    pub task_id: TaskId,
    pub condition_ids: Vec<ConditionId>,
    pub logic: TriggerLogic,
    /// Optional static arguments to pass to the triggered task.
    pub argument_template: Option<serde_json::Value>,
}

impl TriggerDefinitionDTO {
    /// Compute a deterministic trigger ID from (task_id + sorted conditions + logic).
    pub fn compute_trigger_id(
        task_id: &TaskId,
        condition_ids: &[ConditionId],
        logic: TriggerLogic,
    ) -> TriggerDefinitionId {
        let mut hasher = Sha256::new();
        hasher.update(task_id.to_string().as_bytes());
        hasher.update(b"|");
        let mut sorted: Vec<&str> = condition_ids.iter().map(ConditionId::as_str).collect();
        sorted.sort();
        for id in &sorted {
            hasher.update(id.as_bytes());
            hasher.update(b",");
        }
        hasher.update(b"|");
        hasher.update(logic.to_string().as_bytes());
        TriggerDefinitionId(format!("{:x}", hasher.finalize()))
    }
}

/// A condition that has been evaluated and found to be satisfied.
///
/// Stored in the trigger store until the trigger definition is evaluated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidCondition {
    /// Unique ID: "valid_{condition_id}_{context_id}"
    pub valid_condition_id: String,
    pub condition_id: ConditionId,
    pub context: ConditionContext,
}

impl ValidCondition {
    pub fn new(condition_id: ConditionId, context: ConditionContext) -> Self {
        let context_id = context.context_id();
        let valid_condition_id = format!("valid_{}_{}", condition_id.0, context_id);
        Self {
            valid_condition_id,
            condition_id,
            context,
        }
    }
}
