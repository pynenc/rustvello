use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionContext, ConditionId, TriggerCondition, TriggerDefinitionDTO, TriggerDefinitionId,
    TriggerRunId, ValidCondition,
};

use crate::db::{blocking, lock_err, sql_err};

use super::{condition_type_tag, get_condition_ids_for_trigger, parse_logic, SqliteTriggerStore};

#[async_trait]
impl TriggerStore for SqliteTriggerStore {
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId> {
        let db = Arc::clone(&self.db);
        let condition = condition.clone();
        blocking(move || {

            let cond_id = condition.condition_id();
            let json = serde_json::to_string(&condition).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;
            let cond_type = condition_type_tag(&condition);
            let event_code = match &condition {
                TriggerCondition::Event(evt) => Some(evt.event_code.clone()),
                _ => None,
            };

            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            tx.execute(
                "INSERT OR REPLACE INTO trg_conditions (condition_id, condition_type, event_code, condition_json) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![cond_id.as_str(), cond_type, &event_code, &json],
            )
            .map_err(sql_err)?;

            for task_id in condition.source_task_ids() {
                tx.execute(
                    "INSERT OR IGNORE INTO trg_source_task_conditions (task_id, condition_id) VALUES (?1, ?2)",
                    rusqlite::params![&task_id.to_string(), cond_id.as_str()],
                )
                .map_err(sql_err)?;
            }

            tx.commit().map_err(sql_err)?;
            Ok(cond_id)

        })
        .await
    }

    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        let db = Arc::clone(&self.db);
        let id = id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT condition_json FROM trg_conditions WHERE condition_id = ?1")
                .map_err(sql_err)?;

            let result = stmt
                .query_row(rusqlite::params![&id.as_str()], |row| {
                    let json: String = row.get(0)?;
                    Ok(json)
                })
                .optional()
                .map_err(sql_err)?;

            match result {
                Some(json) => {
                    let cond: TriggerCondition =
                        serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                            message: e.to_string(),
                        })?;
                    Ok(Some(cond))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT c.condition_id, c.condition_json
                     FROM trg_conditions c
                     INNER JOIN trg_source_task_conditions stc ON c.condition_id = stc.condition_id
                     WHERE stc.task_id = ?1",
                )
                .map_err(sql_err)?;

            let rows = stmt
                .query_map(rusqlite::params![&task_id.to_string()], |row| {
                    let id: String = row.get(0)?;
                    let json: String = row.get(1)?;
                    Ok((id, json))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (id, json) = row.map_err(sql_err)?;
                let cond: TriggerCondition =
                    serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id), cond));
            }
            Ok(result)
        })
        .await
    }

    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = Arc::clone(&self.db);
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT condition_id, condition_json FROM trg_conditions WHERE condition_type = 'Cron'")
                .map_err(sql_err)?;

            let rows = stmt
                .query_map([], |row| {
                    let id: String = row.get(0)?;
                    let json: String = row.get(1)?;
                    Ok((id, json))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (id, json) = row.map_err(sql_err)?;
                let cond: TriggerCondition =
                    serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id), cond));
            }
            Ok(result)

        })
        .await
    }

    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT condition_id, condition_json FROM trg_conditions")
                .map_err(sql_err)?;

            let rows = stmt
                .query_map([], |row| {
                    let id: String = row.get(0)?;
                    let json: String = row.get(1)?;
                    Ok((id, json))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (id, json) = row.map_err(sql_err)?;
                let cond: TriggerCondition =
                    serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id), cond));
            }
            Ok(result)
        })
        .await
    }

    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = Arc::clone(&self.db);
        let event_code = event_code.to_owned();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT condition_id, condition_json FROM trg_conditions WHERE condition_type = 'Event' AND event_code = ?1")
                .map_err(sql_err)?;

            let rows = stmt
                .query_map(rusqlite::params![event_code], |row| {
                    let id: String = row.get(0)?;
                    let json: String = row.get(1)?;
                    Ok((id, json))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (id, json) = row.map_err(sql_err)?;
                let cond: TriggerCondition =
                    serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id), cond));
            }
            Ok(result)

        })
        .await
    }

    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let trigger = trigger.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let arg_tmpl = trigger.argument_template.as_ref().map(ToString::to_string);

            tx.execute(
                "INSERT OR REPLACE INTO trg_triggers (trigger_id, task_id, logic, argument_template) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    &trigger.trigger_id.as_str(),
                    &trigger.task_id.to_string(),
                    &trigger.logic.to_string(),
                    &arg_tmpl,
                ],
            )
            .map_err(sql_err)?;

            for cid in &trigger.condition_ids {
                tx.execute(
                    "INSERT OR IGNORE INTO trg_condition_triggers (condition_id, trigger_id) VALUES (?1, ?2)",
                    rusqlite::params![cid.as_str(), &trigger.trigger_id.as_str()],
                )
                .map_err(sql_err)?;
            }

            tx.commit().map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        let db = Arc::clone(&self.db);
        let id = id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT task_id, logic, argument_template FROM trg_triggers WHERE trigger_id = ?1",
                )
                .map_err(sql_err)?;

            let result = stmt
                .query_row(rusqlite::params![&id.as_str()], |row| {
                    let task_id_str: String = row.get(0)?;
                    let logic_str: String = row.get(1)?;
                    let arg_tmpl: Option<String> = row.get(2)?;
                    Ok((task_id_str, logic_str, arg_tmpl))
                })
                .optional()
                .map_err(sql_err)?;

            match result {
                Some((task_id_str, logic_str, arg_tmpl)) => {
                    let task_id: TaskId =
                        task_id_str
                            .parse()
                            .map_err(|e| RustvelloError::state_backend(format!("invalid task_id in database: {e}")))?;
                    let logic = parse_logic(&logic_str)?;
                    let argument_template = arg_tmpl
                        .map(|s| serde_json::from_str(&s))
                        .transpose()
                        .map_err(|e| RustvelloError::Serialization {
                            message: e.to_string(),
                        })?;

                    let condition_ids = get_condition_ids_for_trigger(&conn, id.as_str())?;

                    Ok(Some(TriggerDefinitionDTO {
                        trigger_id: id.clone(),
                        task_id,
                        condition_ids,
                        logic,
                        argument_template,
                    }))
                }
                None => Ok(None),
            }

        })
        .await
    }

    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        let db = Arc::clone(&self.db);
        let cond_id = cond_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT t.trigger_id, t.task_id, t.logic, t.argument_template
                     FROM trg_triggers t
                     INNER JOIN trg_condition_triggers ct ON t.trigger_id = ct.trigger_id
                     WHERE ct.condition_id = ?1",
                )
                .map_err(sql_err)?;

            let rows = stmt
                .query_map(rusqlite::params![cond_id.as_str()], |row| {
                    let trigger_id: String = row.get(0)?;
                    let task_id_str: String = row.get(1)?;
                    let logic_str: String = row.get(2)?;
                    let arg_tmpl: Option<String> = row.get(3)?;
                    Ok((trigger_id, task_id_str, logic_str, arg_tmpl))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (trigger_id, task_id_str, logic_str, arg_tmpl) = row.map_err(sql_err)?;
                let task_id: TaskId = task_id_str.parse().map_err(|e| {
                    RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
                })?;
                let logic = parse_logic(&logic_str)?;
                let argument_template = arg_tmpl
                    .map(|s| serde_json::from_str(&s))
                    .transpose()
                    .map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                let condition_ids = get_condition_ids_for_trigger(&conn, &trigger_id)?;

                result.push(TriggerDefinitionDTO {
                    trigger_id: TriggerDefinitionId::from(trigger_id),
                    task_id,
                    condition_ids,
                    logic,
                    argument_template,
                });
            }
            Ok(result)
        })
        .await
    }

    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let task_str = task_id.to_string();

            let mut stmt = tx
                .prepare("SELECT trigger_id FROM trg_triggers WHERE task_id = ?1")
                .map_err(sql_err)?;
            let ids: Vec<String> = stmt
                .query_map(rusqlite::params![&task_str], |row| row.get(0))
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            drop(stmt);

            let count = u32::try_from(ids.len()).unwrap_or(u32::MAX);
            for id in &ids {
                tx.execute(
                    "DELETE FROM trg_condition_triggers WHERE trigger_id = ?1",
                    rusqlite::params![id],
                )
                .map_err(sql_err)?;
                tx.execute(
                    "DELETE FROM trg_triggers WHERE trigger_id = ?1",
                    rusqlite::params![id],
                )
                .map_err(sql_err)?;
            }

            tx.commit().map_err(sql_err)?;
            Ok(count)
        })
        .await
    }

    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let vc = vc.clone();
        blocking(move || {

            let context_json =
                serde_json::to_string(&vc.context).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO trg_valid_conditions (valid_condition_id, condition_id, context_json) VALUES (?1, ?2, ?3)",
                rusqlite::params![&vc.valid_condition_id, &vc.condition_id.as_str(), &context_json],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let db = Arc::clone(&self.db);
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT valid_condition_id, condition_id, context_json FROM trg_valid_conditions",
                )
                .map_err(sql_err)?;

            let rows = stmt
                .query_map([], |row| {
                    let vc_id: String = row.get(0)?;
                    let cond_id: String = row.get(1)?;
                    let ctx_json: String = row.get(2)?;
                    Ok((vc_id, cond_id, ctx_json))
                })
                .map_err(sql_err)?;

            let mut result = Vec::new();
            for row in rows {
                let (vc_id, cond_id, ctx_json) = row.map_err(sql_err)?;
                let context: ConditionContext =
                    serde_json::from_str(&ctx_json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(ValidCondition {
                    valid_condition_id: vc_id,
                    condition_id: ConditionId::from(cond_id),
                    context,
                });
            }
            Ok(result)

        })
        .await
    }

    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let ids = ids.to_vec();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            for id in ids {
                conn.execute(
                    "DELETE FROM trg_valid_conditions WHERE valid_condition_id = ?1",
                    rusqlite::params![id],
                )
                .map_err(sql_err)?;
            }
            Ok(())
        })
        .await
    }

    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        let db = Arc::clone(&self.db);
        let cond_id = cond_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT last_execution FROM trg_cron_executions WHERE condition_id = ?1")
                .map_err(sql_err)?;

            let result = stmt
                .query_row(rusqlite::params![cond_id.as_str()], |row| {
                    let ts: String = row.get(0)?;
                    Ok(ts)
                })
                .optional()
                .map_err(sql_err)?;

            match result {
                Some(ts) => {
                    let dt = DateTime::parse_from_rfc3339(&ts)
                        .map(|dt| dt.with_timezone(&Utc))
                        .map_err(|e| {
                            RustvelloError::state_backend(format!("invalid timestamp: {}", e))
                        })?;
                    Ok(Some(dt))
                }
                None => Ok(None),
            }
        })
        .await
    }

    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        let db = Arc::clone(&self.db);
        let cond_id = cond_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let time_str = time.to_rfc3339();

            let changed = match expected_last {
                None => {
                    conn.execute(
                        "INSERT OR IGNORE INTO trg_cron_executions (condition_id, last_execution) VALUES (?1, ?2)",
                        rusqlite::params![cond_id.as_str(), &time_str],
                    )
                    .map_err(sql_err)?
                }
                Some(expected) => {
                    let expected_str = expected.to_rfc3339();
                    conn.execute(
                        "UPDATE trg_cron_executions SET last_execution = ?1 WHERE condition_id = ?2 AND last_execution = ?3",
                        rusqlite::params![&time_str, cond_id.as_str(), &expected_str],
                    )
                    .map_err(sql_err)?
                }
            };

            Ok(changed > 0)

        })
        .await
    }

    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        let db = Arc::clone(&self.db);
        let run_id = run_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let now = Utc::now().to_rfc3339();

            let changed = conn
                .execute(
                    "INSERT OR IGNORE INTO trg_trigger_run_claims (trigger_run_id, claimed_at) VALUES (?1, ?2)",
                    rusqlite::params![run_id.as_str(), &now],
                )
                .map_err(sql_err)?;

            Ok(changed > 0)

        })
        .await
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute_batch(
                "DELETE FROM trg_trigger_run_claims;
                 DELETE FROM trg_cron_executions;
                 DELETE FROM trg_valid_conditions;
                 DELETE FROM trg_source_task_conditions;
                 DELETE FROM trg_condition_triggers;
                 DELETE FROM trg_triggers;
                 DELETE FROM trg_conditions;",
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }
}
