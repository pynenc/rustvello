//! Trigger condition types and polymorphic condition enum.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::identifiers::TaskId;
use crate::status::InvocationStatus;

use super::context::ConditionContext;
use super::filter::{argument_filter_id, result_filter_id};
use super::filter::{check_argument_filter, check_payload_filter, check_result_filter};
use super::ConditionId;

// ---------------------------------------------------------------------------
// Condition types
// ---------------------------------------------------------------------------

/// A condition that fires on a cron schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronCondition {
    /// Cron expression (e.g. "0 * * * *").
    pub cron_expression: String,
    /// Minimum seconds between firings (default 50, prevents double-fire).
    pub min_interval_seconds: u64,
}

/// A condition that fires on invocation status changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusCondition {
    /// The task to watch.
    pub task_id: TaskId,
    /// Which statuses trigger the condition.
    pub statuses: Vec<InvocationStatus>,
    /// Optional argument subset match filter (None = match any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_filter: Option<BTreeMap<String, serde_json::Value>>,
}

/// A condition that fires on custom application events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCondition {
    /// Event code to match.
    pub event_code: String,
    /// Optional payload subset match filter (None = match any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_filter: Option<BTreeMap<String, serde_json::Value>>,
}

/// A condition that fires on successful task completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultCondition {
    /// The task whose results to watch.
    pub task_id: TaskId,
    /// Optional argument subset match filter (None = match any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_filter: Option<BTreeMap<String, serde_json::Value>>,
    /// Optional result value equality filter (None = match any result).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_filter: Option<serde_json::Value>,
}

/// A condition that fires on task failure with specific error types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionCondition {
    /// The task whose failures to watch.
    pub task_id: TaskId,
    /// Error type names to match (empty = match all errors).
    pub exception_types: Vec<String>,
    /// Optional argument subset match filter (None = match any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argument_filter: Option<BTreeMap<String, serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// Polymorphic condition enum
// ---------------------------------------------------------------------------

/// Logic for combining conditions inside a `CompositeCondition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CompositeLogic {
    And,
    Or,
}

/// A composite condition that combines children with AND/OR logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeCondition {
    pub logic: CompositeLogic,
    pub children: Vec<TriggerCondition>,
}

/// A trigger condition — polymorphic via enum.
///
/// Each variant maps 1:1 to a pynenc TriggerCondition subclass.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TriggerCondition {
    Cron(CronCondition),
    Status(StatusCondition),
    Event(EventCondition),
    Result(ResultCondition),
    Exception(ExceptionCondition),
    Composite(CompositeCondition),
}

impl std::fmt::Display for TriggerCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cron(c) => write!(f, "Cron({})", c.cron_expression),
            Self::Status(c) => write!(f, "Status(task={})", c.task_id),
            Self::Event(c) => write!(f, "Event({})", c.event_code),
            Self::Result(c) => write!(f, "Result(task={})", c.task_id),
            Self::Exception(c) => write!(f, "Exception(task={})", c.task_id),
            Self::Composite(c) => {
                write!(f, "Composite({:?}, {} children)", c.logic, c.children.len())
            }
        }
    }
}

impl TriggerCondition {
    /// Compute a deterministic condition ID matching pynenc's format.
    ///
    /// Status values use the lowercase StrEnum representation (e.g. `"success"`,
    /// `"concurrency_controlled"`) — produced by lowercasing the UPPER_SNAKE
    /// `Display` output of `InvocationStatus`.
    pub fn condition_id(&self) -> ConditionId {
        match self {
            Self::Cron(c) => ConditionId(format!("cron_{}", c.cron_expression)),
            Self::Status(c) => {
                let mut statuses: Vec<String> = c
                    .statuses
                    .iter()
                    .map(|s| s.to_string().to_lowercase())
                    .collect();
                statuses.sort();
                let statuses_str = statuses.join("_");
                let filter_id = argument_filter_id(&c.argument_filter);
                ConditionId(format!(
                    "condition#{}#{}#{}",
                    c.task_id, statuses_str, filter_id
                ))
            }
            Self::Event(c) => {
                let filter_id = argument_filter_id(&c.payload_filter);
                ConditionId(format!("event#{}#{}", c.event_code, filter_id))
            }
            Self::Result(c) => {
                // Build the inner status condition_id (always SUCCESS)
                let status_cond = Self::Status(StatusCondition {
                    task_id: c.task_id.clone(),
                    statuses: vec![InvocationStatus::Success],
                    argument_filter: c.argument_filter.clone(),
                });
                let base_id = status_cond.condition_id();
                let rf_id = result_filter_id(&c.result_filter);
                ConditionId(format!("{}_result_{}", base_id.0, rf_id))
            }
            Self::Exception(c) => {
                // Build the inner status condition_id (always FAILED)
                let status_cond = Self::Status(StatusCondition {
                    task_id: c.task_id.clone(),
                    statuses: vec![InvocationStatus::Failed],
                    argument_filter: c.argument_filter.clone(),
                });
                let base_id = status_cond.condition_id();
                let exception_str = if c.exception_types.is_empty() {
                    "any".to_string()
                } else {
                    let mut types = c.exception_types.clone();
                    types.sort();
                    types.join("_")
                };
                ConditionId(format!("{}_exception_{}", base_id.0, exception_str))
            }
            Self::Composite(c) => {
                let mut child_ids: Vec<String> =
                    c.children.iter().map(|ch| ch.condition_id().0).collect();
                child_ids.sort();
                let logic = format!("{:?}", c.logic).to_lowercase();
                ConditionId(format!("composite#{}#{}", logic, child_ids.join(",")))
            }
        }
    }

    /// Returns the task IDs this condition watches (empty for Cron/Event/Composite).
    pub fn source_task_ids(&self) -> Vec<TaskId> {
        match self {
            Self::Cron(_) | Self::Event(_) => vec![],
            Self::Status(c) => vec![c.task_id.clone()],
            Self::Result(c) => vec![c.task_id.clone()],
            Self::Exception(c) => vec![c.task_id.clone()],
            Self::Composite(c) => {
                let mut ids: Vec<TaskId> =
                    c.children.iter().flat_map(Self::source_task_ids).collect();
                ids.sort_by_key(std::string::ToString::to_string);
                ids.dedup_by(|a, b| a.to_string() == b.to_string());
                ids
            }
        }
    }

    /// Check if this condition is satisfied by the given context.
    pub fn is_satisfied_by(&self, ctx: &ConditionContext) -> bool {
        match (self, ctx) {
            (Self::Cron(_), ConditionContext::Cron(_)) => {
                // Cron satisfaction is handled externally via schedule checks
                true
            }
            (Self::Status(cond), ConditionContext::Status(ctx)) => {
                cond.task_id == ctx.task_id
                    && cond.statuses.contains(&ctx.status)
                    && check_argument_filter(&cond.argument_filter, &ctx.arguments)
            }
            (Self::Event(cond), ConditionContext::Event(ctx)) => {
                cond.event_code == ctx.event_code
                    && check_payload_filter(&cond.payload_filter, &ctx.payload)
            }
            (Self::Result(cond), ConditionContext::Result(ctx)) => {
                cond.task_id == ctx.task_id
                    && check_argument_filter(&cond.argument_filter, &ctx.arguments)
                    && check_result_filter(&cond.result_filter, &ctx.result)
            }
            (Self::Exception(cond), ConditionContext::Exception(ctx)) => {
                cond.task_id == ctx.task_id
                    && (cond.exception_types.is_empty()
                        || cond.exception_types.contains(&ctx.error_type))
                    && check_argument_filter(&cond.argument_filter, &ctx.arguments)
            }
            (Self::Composite(cond), ctx) => match cond.logic {
                CompositeLogic::And => cond.children.iter().all(|c| c.is_satisfied_by(ctx)),
                CompositeLogic::Or => cond.children.iter().any(|c| c.is_satisfied_by(ctx)),
            },
            _ => false, // mismatched condition/context types
        }
    }
}
