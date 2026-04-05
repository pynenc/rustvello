//! SQLite trigger store implementation.

mod store;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::trigger::{ConditionId, TriggerCondition, TriggerLogic};

use crate::db::{sql_err, Database};

/// SQLite-backed trigger store implementation.
pub struct SqliteTriggerStore {
    db: Arc<Database>,
}

impl SqliteTriggerStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn parse_logic(s: &str) -> RustvelloResult<TriggerLogic> {
    match s {
        "OR" => Ok(TriggerLogic::Or),
        "AND" => Ok(TriggerLogic::And),
        other => Err(RustvelloError::state_backend(format!(
            "unknown trigger logic in database: {other:?}"
        ))),
    }
}

/// Return the discriminant tag for a condition variant.
fn condition_type_tag(condition: &TriggerCondition) -> &'static str {
    match condition {
        TriggerCondition::Cron(_) => "Cron",
        TriggerCondition::Status(_) => "Status",
        TriggerCondition::Event(_) => "Event",
        TriggerCondition::Result(_) => "Result",
        TriggerCondition::Exception(_) => "Exception",
        TriggerCondition::Composite(_) => "Composite",
        _ => "Unknown",
    }
}

/// Get condition IDs associated with a trigger.
fn get_condition_ids_for_trigger(
    conn: &rusqlite::Connection,
    trigger_id: &str,
) -> RustvelloResult<Vec<ConditionId>> {
    let mut stmt = conn
        .prepare("SELECT condition_id FROM trg_condition_triggers WHERE trigger_id = ?1")
        .map_err(sql_err)?;
    let ids: Vec<String> = stmt
        .query_map(rusqlite::params![trigger_id], |row| row.get(0))
        .map_err(sql_err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_err)?;
    Ok(ids.into_iter().map(ConditionId::from).collect())
}
