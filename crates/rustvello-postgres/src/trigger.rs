//! PostgreSQL trigger store implementation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionContext, ConditionId, TriggerCondition, TriggerDefinitionDTO, TriggerDefinitionId,
    TriggerLogic, TriggerRunId, ValidCondition,
};

use crate::db::{pg_err, Database};

/// PostgreSQL-backed trigger store implementation.
pub struct PostgresTriggerStore {
    db: Arc<Database>,
}

impl PostgresTriggerStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn parse_logic(s: &str) -> RustvelloResult<TriggerLogic> {
    match s {
        "AND" => Ok(TriggerLogic::And),
        "OR" => Ok(TriggerLogic::Or),
        other => Err(RustvelloError::state_backend(format!(
            "unknown trigger logic value: {other:?}"
        ))),
    }
}

// parse_task_id removed — use TaskId::from_str (via `.parse()`) instead.

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

#[async_trait]
impl TriggerStore for PostgresTriggerStore {
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId> {
        let cond_id = condition.condition_id();
        let json = serde_json::to_string(condition).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let cond_type = condition_type_tag(condition);
        let event_code: Option<&str> = match condition {
            TriggerCondition::Event(evt) => Some(&evt.event_code),
            _ => None,
        };

        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        tx
            .execute(
                "INSERT INTO trg_conditions (condition_id, condition_type, condition_json, event_code) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (condition_id) DO UPDATE SET condition_type = $2, condition_json = $3, event_code = $4",
                &[&cond_id.as_str(), &cond_type, &json, &event_code],
            )
            .await
            .map_err(pg_err)?;

        // Index by source task IDs
        for task_id in condition.source_task_ids() {
            tx.execute(
                "INSERT INTO trg_source_task_conditions (task_id, condition_id) VALUES ($1, $2)
                     ON CONFLICT DO NOTHING",
                &[&task_id.to_string(), &cond_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        }

        tx.commit().await.map_err(pg_err)?;

        Ok(cond_id)
    }

    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        let client = self.db.conn().await?;

        let row = client
            .query_opt(
                "SELECT condition_json FROM trg_conditions WHERE condition_id = $1",
                &[&id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        match row {
            Some(r) => {
                let json: String = r.get(0);
                let cond: TriggerCondition =
                    serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(cond))
            }
            None => Ok(None),
        }
    }

    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT c.condition_id, c.condition_json
                 FROM trg_conditions c
                 INNER JOIN trg_source_task_conditions stc ON c.condition_id = stc.condition_id
                 WHERE stc.task_id = $1",
                &[&task_id.to_string()],
            )
            .await
            .map_err(pg_err)?;

        let mut result = Vec::new();
        for row in &rows {
            let id: String = row.get(0);
            let json: String = row.get(1);
            let cond: TriggerCondition =
                serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push((ConditionId::from(id), cond));
        }
        Ok(result)
    }

    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT condition_id, condition_json FROM trg_conditions WHERE condition_type = 'Cron'",
                &[],
            )
            .await
            .map_err(pg_err)?;

        let mut result = Vec::new();
        for row in &rows {
            let id: String = row.get(0);
            let json: String = row.get(1);
            let cond: TriggerCondition =
                serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push((ConditionId::from(id), cond));
        }
        Ok(result)
    }

    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT condition_id, condition_json FROM trg_conditions \
                 WHERE condition_type = 'Event' AND event_code = $1",
                &[&event_code],
            )
            .await
            .map_err(pg_err)?;

        let mut result = Vec::new();
        for row in &rows {
            let id: String = row.get(0);
            let json: String = row.get(1);
            let cond: TriggerCondition =
                serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push((ConditionId::from(id), cond));
        }
        Ok(result)
    }

    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;
        let arg_tmpl = trigger.argument_template.as_ref().map(ToString::to_string);

        tx
            .execute(
                "INSERT INTO trg_triggers (trigger_id, task_id, logic, argument_template) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (trigger_id) DO UPDATE SET task_id = $2, logic = $3, argument_template = $4",
                &[
                    &trigger.trigger_id.as_str(),
                    &trigger.task_id.to_string(),
                    &trigger.logic.to_string(),
                    &arg_tmpl as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(pg_err)?;

        // Insert condition mappings
        for cid in &trigger.condition_ids {
            tx.execute(
                "INSERT INTO trg_condition_triggers (condition_id, trigger_id) VALUES ($1, $2)
                     ON CONFLICT DO NOTHING",
                &[&cid.as_str(), &trigger.trigger_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        }

        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }

    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        let client = self.db.conn().await?;

        let row = client
            .query_opt(
                "SELECT task_id, logic, argument_template FROM trg_triggers WHERE trigger_id = $1",
                &[&id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        match row {
            Some(r) => {
                let task_id_str: String = r.get(0);
                let logic_str: String = r.get(1);
                let arg_tmpl: Option<String> = r.get(2);

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

                let condition_ids = self
                    .get_condition_ids_for_trigger(&client, id.as_str())
                    .await?;

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
    }

    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT t.trigger_id, t.task_id, t.logic, t.argument_template
                 FROM trg_triggers t
                 INNER JOIN trg_condition_triggers ct ON t.trigger_id = ct.trigger_id
                 WHERE ct.condition_id = $1",
                &[&cond_id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        // Collect trigger IDs for batch condition lookup
        let trigger_ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        let mut cond_map = self
            .get_condition_ids_for_triggers(&client, &trigger_ids)
            .await?;

        let mut result = Vec::new();
        for row in &rows {
            let trigger_id: String = row.get(0);
            let task_id_str: String = row.get(1);
            let logic_str: String = row.get(2);
            let arg_tmpl: Option<String> = row.get(3);

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
            let condition_ids = cond_map.remove(&trigger_id).unwrap_or_default();

            result.push(TriggerDefinitionDTO {
                trigger_id: TriggerDefinitionId::from(trigger_id),
                task_id,
                condition_ids,
                logic,
                argument_template,
            });
        }
        Ok(result)
    }

    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        let client = self.db.conn().await?;
        let task_str = task_id.to_string();

        // Batch delete condition mappings via subquery
        client
            .execute(
                "DELETE FROM trg_condition_triggers WHERE trigger_id IN \
                 (SELECT trigger_id FROM trg_triggers WHERE task_id = $1)",
                &[&task_str],
            )
            .await
            .map_err(pg_err)?;

        // Batch delete triggers
        let deleted = client
            .execute("DELETE FROM trg_triggers WHERE task_id = $1", &[&task_str])
            .await
            .map_err(pg_err)?;

        Ok(u32::try_from(deleted).unwrap_or(u32::MAX))
    }

    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        let context_json =
            serde_json::to_string(&vc.context).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;

        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO trg_valid_conditions (valid_condition_id, condition_id, context_json) VALUES ($1, $2, $3)
                 ON CONFLICT (valid_condition_id) DO UPDATE SET condition_id = $2, context_json = $3",
                &[&vc.valid_condition_id, &vc.condition_id.as_str(), &context_json],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT valid_condition_id, condition_id, context_json FROM trg_valid_conditions",
                &[],
            )
            .await
            .map_err(pg_err)?;

        let mut result = Vec::new();
        for row in &rows {
            let vc_id: String = row.get(0);
            let cond_id: String = row.get(1);
            let ctx_json: String = row.get(2);
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
    }

    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let client = self.db.conn().await?;
        client
            .execute(
                "DELETE FROM trg_valid_conditions WHERE valid_condition_id = ANY($1)",
                &[&ids],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        let client = self.db.conn().await?;

        let row = client
            .query_opt(
                "SELECT last_execution FROM trg_cron_executions WHERE condition_id = $1",
                &[&cond_id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        Ok(row.map(|r| r.get::<_, DateTime<Utc>>(0)))
    }

    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        let client = self.db.conn().await?;

        let changed = match expected_last {
            None => {
                // Only insert if no existing row
                client
                    .execute(
                        "INSERT INTO trg_cron_executions (condition_id, last_execution) VALUES ($1, $2)
                         ON CONFLICT DO NOTHING",
                        &[&cond_id.as_str(), &time],
                    )
                    .await
                    .map_err(pg_err)?
            }
            Some(expected) => client
                .execute(
                    "UPDATE trg_cron_executions SET last_execution = $1
                         WHERE condition_id = $2 AND last_execution = $3",
                    &[&time, &cond_id.as_str(), &expected],
                )
                .await
                .map_err(pg_err)?,
        };

        Ok(changed > 0)
    }

    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        let client = self.db.conn().await?;
        let now = Utc::now();

        let changed = client
            .execute(
                "INSERT INTO trg_trigger_run_claims (trigger_run_id, claimed_at) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
                &[&run_id.as_str(), &now],
            )
            .await
            .map_err(pg_err)?;

        Ok(changed > 0)
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .batch_execute(
                "DELETE FROM trg_trigger_run_claims;
                 DELETE FROM trg_cron_executions;
                 DELETE FROM trg_valid_conditions;
                 DELETE FROM trg_source_task_conditions;
                 DELETE FROM trg_condition_triggers;
                 DELETE FROM trg_triggers;
                 DELETE FROM trg_conditions;",
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT condition_id, condition_json FROM trg_conditions",
                &[],
            )
            .await
            .map_err(pg_err)?;

        let mut result = Vec::new();
        for row in &rows {
            let id: String = row.get(0);
            let json: String = row.get(1);
            let cond: TriggerCondition =
                serde_json::from_str(&json).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push((ConditionId::from(id), cond));
        }
        Ok(result)
    }
}

// Helper methods
impl PostgresTriggerStore {
    async fn get_condition_ids_for_trigger(
        &self,
        client: &deadpool_postgres::Client,
        trigger_id: &str,
    ) -> RustvelloResult<Vec<ConditionId>> {
        let rows = client
            .query(
                "SELECT condition_id FROM trg_condition_triggers WHERE trigger_id = $1",
                &[&trigger_id],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| ConditionId::from(r.get::<_, String>(0)))
            .collect())
    }

    /// Batch-fetch condition IDs for multiple triggers in a single query.
    async fn get_condition_ids_for_triggers(
        &self,
        client: &deadpool_postgres::Client,
        trigger_ids: &[String],
    ) -> RustvelloResult<HashMap<String, Vec<ConditionId>>> {
        if trigger_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = client
            .query(
                "SELECT trigger_id, condition_id FROM trg_condition_triggers \
                 WHERE trigger_id = ANY($1)",
                &[&trigger_ids],
            )
            .await
            .map_err(pg_err)?;

        let mut map: HashMap<String, Vec<ConditionId>> = HashMap::new();
        for row in &rows {
            let tid: String = row.get(0);
            let cid: String = row.get(1);
            map.entry(tid).or_default().push(ConditionId::from(cid));
        }
        Ok(map)
    }
}
