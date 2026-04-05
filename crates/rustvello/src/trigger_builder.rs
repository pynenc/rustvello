//! Fluent builder for composing trigger definitions.
//!
//! Mirrors pynenc's `TriggerBuilder` — provides an ergonomic API for
//! constructing trigger conditions and definitions.

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::InvocationStatus;
use rustvello_proto::trigger::{
    CronCondition, EventCondition, ExceptionCondition, ResultCondition, StatusCondition,
    TriggerCondition, TriggerDefinitionDTO, TriggerLogic,
};
use std::str::FromStr;
use std::sync::Arc;

/// Fluent builder for constructing trigger definitions.
///
/// # Example
///
/// ```rust,ignore
/// TriggerBuilder::new()
///     .on_status(&other_task_id, &[InvocationStatus::Success])
///     .on_event("payment_received")
///     .with_logic(TriggerLogic::Or)
///     .with_static_args(serde_json::json!({"key": "value"}))
///     .build(&target_task_id)
/// ```
#[must_use]
pub struct TriggerBuilder {
    conditions: Vec<TriggerCondition>,
    logic: TriggerLogic,
    argument_template: Option<serde_json::Value>,
}

impl TriggerBuilder {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            logic: TriggerLogic::And,
            argument_template: None,
        }
    }

    /// Add a cron condition with default min_interval (50s).
    ///
    /// # Errors
    /// Returns `RustvelloError::Trigger` if the cron expression is invalid.
    pub fn on_cron(self, expression: &str) -> RustvelloResult<Self> {
        self.on_cron_with_interval(expression, 50)
    }

    /// Add a cron condition with custom min_interval.
    ///
    /// # Errors
    /// Returns `RustvelloError::Trigger` if the cron expression is invalid.
    pub fn on_cron_with_interval(
        mut self,
        expression: &str,
        min_interval_seconds: u64,
    ) -> RustvelloResult<Self> {
        croner::Cron::from_str(expression).map_err(|e| RustvelloError::Configuration {
            message: format!("invalid cron expression {:?}: {}", expression, e),
        })?;
        self.conditions.push(TriggerCondition::Cron(CronCondition {
            cron_expression: expression.to_string(),
            min_interval_seconds,
        }));
        Ok(self)
    }

    /// Add a status condition — fires when the given task transitions to one of the statuses.
    pub fn on_status(mut self, task_id: &TaskId, statuses: &[InvocationStatus]) -> Self {
        self.conditions
            .push(TriggerCondition::Status(StatusCondition {
                task_id: task_id.clone(),
                statuses: statuses.to_vec(),
                argument_filter: None,
            }));
        self
    }

    /// Add an event condition — fires when a custom event with the given code is emitted.
    pub fn on_event(mut self, event_code: &str) -> Self {
        self.conditions
            .push(TriggerCondition::Event(EventCondition {
                event_code: event_code.to_string(),
                payload_filter: None,
            }));
        self
    }

    /// Add a result condition — fires when the given task completes successfully.
    pub fn on_result(mut self, task_id: &TaskId) -> Self {
        self.conditions
            .push(TriggerCondition::Result(ResultCondition {
                task_id: task_id.clone(),
                argument_filter: None,
                result_filter: None,
            }));
        self
    }

    /// Add a result condition for any successful completion of the given task.
    ///
    /// Equivalent to `on_result` — provided for parity with pynenc's
    /// `TriggerBuilder.on_any_result()`.
    pub fn on_any_result(self, task_id: &TaskId) -> Self {
        self.on_result(task_id)
    }

    /// Add an exception condition — fires when the given task fails.
    pub fn on_exception(mut self, task_id: &TaskId, exception_types: &[&str]) -> Self {
        self.conditions
            .push(TriggerCondition::Exception(ExceptionCondition {
                task_id: task_id.clone(),
                exception_types: exception_types.iter().map(ToString::to_string).collect(),
                argument_filter: None,
            }));
        self
    }

    /// Set the trigger logic (AND or OR). Default is AND.
    pub fn with_logic(mut self, logic: TriggerLogic) -> Self {
        self.logic = logic;
        self
    }

    /// Set static arguments to pass to the triggered task.
    pub fn with_static_args(mut self, args: serde_json::Value) -> Self {
        self.argument_template = Some(args);
        self
    }

    /// Build the trigger definition for the given target task.
    ///
    /// Returns the definition DTO without registering anything — use
    /// [`build_and_register`] to also persist the trigger.
    pub fn build(self, target_task_id: &TaskId) -> RustvelloResult<TriggerDefinitionDTO> {
        if self.conditions.is_empty() {
            return Err(RustvelloError::Configuration {
                message: "trigger must have at least one condition".into(),
            });
        }

        let condition_ids: Vec<_> = self
            .conditions
            .iter()
            .map(TriggerCondition::condition_id)
            .collect();
        let trigger_id =
            TriggerDefinitionDTO::compute_trigger_id(target_task_id, &condition_ids, self.logic);

        Ok(TriggerDefinitionDTO {
            trigger_id,
            task_id: target_task_id.clone(),
            condition_ids,
            logic: self.logic,
            argument_template: self.argument_template,
        })
    }

    /// Build, register conditions, and register the trigger definition in one step.
    pub async fn build_and_register(
        self,
        target_task_id: &TaskId,
        store: &Arc<dyn TriggerStore>,
    ) -> RustvelloResult<TriggerDefinitionDTO> {
        if self.conditions.is_empty() {
            return Err(RustvelloError::Configuration {
                message: "trigger must have at least one condition".into(),
            });
        }

        // Register all conditions
        let mut condition_ids = Vec::with_capacity(self.conditions.len());
        for cond in &self.conditions {
            let cid = store.register_condition(cond).await?;
            condition_ids.push(cid);
        }

        let trigger_id =
            TriggerDefinitionDTO::compute_trigger_id(target_task_id, &condition_ids, self.logic);

        let definition = TriggerDefinitionDTO {
            trigger_id,
            task_id: target_task_id.clone(),
            condition_ids,
            logic: self.logic,
            argument_template: self.argument_template,
        };

        store.register_trigger(&definition).await?;

        Ok(definition)
    }

    /// Get the conditions (for inspection/testing).
    pub fn conditions(&self) -> &[TriggerCondition] {
        &self.conditions
    }
}

impl Default for TriggerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_empty_conditions_error() {
        let result = TriggerBuilder::new().build(&TaskId::new("mod", "task"));
        assert!(result.is_err());
    }

    #[test]
    fn builder_single_condition() {
        let task_id = TaskId::new("mod", "target");
        let trigger = TriggerBuilder::new()
            .on_event("payment")
            .build(&task_id)
            .unwrap();

        assert_eq!(trigger.task_id, task_id);
        assert_eq!(trigger.condition_ids.len(), 1);
        assert_eq!(trigger.logic, TriggerLogic::And);
    }

    #[test]
    fn builder_multiple_conditions_or_logic() {
        let source = TaskId::new("mod", "source");
        let target = TaskId::new("mod", "target");

        let trigger = TriggerBuilder::new()
            .on_status(&source, &[InvocationStatus::Success])
            .on_event("manual_trigger")
            .with_logic(TriggerLogic::Or)
            .with_static_args(serde_json::json!({"key": "value"}))
            .build(&target)
            .unwrap();

        assert_eq!(trigger.condition_ids.len(), 2);
        assert_eq!(trigger.logic, TriggerLogic::Or);
        assert!(trigger.argument_template.is_some());
    }

    #[test]
    fn builder_cron() {
        let target = TaskId::new("mod", "cleanup");
        let trigger = TriggerBuilder::new()
            .on_cron("0 * * * *")
            .unwrap()
            .build(&target)
            .unwrap();

        assert_eq!(trigger.condition_ids.len(), 1);
    }

    #[test]
    fn builder_cron_invalid_expression() {
        let result = TriggerBuilder::new().on_cron("garbage");
        assert!(result.is_err());
    }

    #[test]
    fn builder_exception_condition() {
        let source = TaskId::new("mod", "risky_task");
        let target = TaskId::new("mod", "alert_task");

        let trigger = TriggerBuilder::new()
            .on_exception(&source, &["TimeoutError", "ConnectionError"])
            .build(&target)
            .unwrap();

        assert_eq!(trigger.condition_ids.len(), 1);
    }

    #[test]
    fn builder_result_condition() {
        let source = TaskId::new("mod", "producer");
        let target = TaskId::new("mod", "consumer");

        let trigger = TriggerBuilder::new()
            .on_result(&source)
            .build(&target)
            .unwrap();

        assert_eq!(trigger.condition_ids.len(), 1);
    }
}
