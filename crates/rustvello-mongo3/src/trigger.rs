use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::bson::doc;
use mongodb::error::{ErrorKind, WriteFailure};

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionId, TriggerCondition, TriggerDefinitionDTO, TriggerDefinitionId, TriggerRunId,
    ValidCondition,
};

use crate::connection::{mongo_err, MongoPool};

const COND_COL: &str = "trg_conditions";
const TRIGGER_COL: &str = "trg_definitions";
const VALID_COL: &str = "trg_valid_conditions";
const CRON_EXEC_COL: &str = "trg_cron_executions";
const RUN_COL: &str = "trg_runs";

/// MongoDB-backed trigger store.
#[non_exhaustive]
pub struct Mongo3TriggerStore {
    pool: Arc<MongoPool>,
}

impl Mongo3TriggerStore {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TriggerStore for Mongo3TriggerStore {
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId> {
        let cond_id = condition.condition_id();
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        let json = serde_json::to_string(condition).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;

        let task_ids: Vec<String> = condition
            .source_task_ids()
            .iter()
            .map(ToString::to_string)
            .collect();

        // Classify condition type for efficient server-side queries
        let condition_type = match condition {
            TriggerCondition::Cron(_) => "Cron",
            TriggerCondition::Event(_) => "Event",
            TriggerCondition::Status(_) => "Status",
            TriggerCondition::Result(_) => "Result",
            TriggerCondition::Exception(_) => "Exception",
            TriggerCondition::Composite(_) => "Composite",
            _ => "Other",
        };

        let mut set_fields = doc! {
            "data": &json,
            "task_ids": &task_ids,
            "condition_type": condition_type,
        };

        // Store event_code for direct querying
        if let TriggerCondition::Event(ev) = condition {
            set_fields.insert("event_code", ev.event_code.clone());
        }

        let update_doc = doc! { "$set": set_fields };

        let filter = doc! { "_id": cond_id.as_str().to_owned() };
        col.update_one(
            filter,
            update_doc,
            Some(
                mongodb::options::UpdateOptions::builder()
                    .upsert(true)
                    .build(),
            ),
        )
        .await
        .map_err(mongo_err)?;

        Ok(cond_id)
    }

    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        let filter = doc! { "_id": &id.as_str() };
        let result = col.find_one(filter, None).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("data")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                let c: TriggerCondition =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }

    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        let filter = doc! { "task_ids": task_id.to_string() };
        let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(data)) = (d.get_str("_id"), d.get_str("data")) {
                let cond: TriggerCondition =
                    serde_json::from_str(data).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id.to_string()), cond));
            }
        }
        Ok(result)
    }

    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        // Server-side filter by condition_type instead of fetching all and filtering client-side
        let mut cursor = col
            .find(doc! { "condition_type": "Cron" }, None)
            .await
            .map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(data)) = (d.get_str("_id"), d.get_str("data")) {
                let cond: TriggerCondition =
                    serde_json::from_str(data).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id.to_string()), cond));
            }
        }
        Ok(result)
    }

    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        // Server-side filter by condition_type and event_code
        let mut cursor = col
            .find(
                doc! { "condition_type": "Event", "event_code": event_code },
                None,
            )
            .await
            .map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(data)) = (d.get_str("_id"), d.get_str("data")) {
                let cond: TriggerCondition =
                    serde_json::from_str(data).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id.to_string()), cond));
            }
        }
        Ok(result)
    }

    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(TRIGGER_COL);
        let json = serde_json::to_string(trigger).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;

        let cond_ids: Vec<String> = trigger
            .condition_ids
            .iter()
            .map(|c| c.as_str().to_owned())
            .collect();

        let filter = doc! { "_id": &trigger.trigger_id.as_str() };
        let update = doc! {
            "$set": {
                "data": &json,
                "task_id": trigger.task_id.to_string(),
                "condition_ids": &cond_ids,
            }
        };
        col.update_one(
            filter,
            update,
            Some(
                mongodb::options::UpdateOptions::builder()
                    .upsert(true)
                    .build(),
            ),
        )
        .await
        .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(TRIGGER_COL);
        let filter = doc! { "_id": &id.as_str() };
        let result = col.find_one(filter, None).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("data")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                let t: TriggerDefinitionDTO =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(TRIGGER_COL);
        let filter = doc! { "condition_ids": cond_id.as_str() };
        let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let t: TriggerDefinitionDTO =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(t);
            }
        }
        Ok(result)
    }

    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(TRIGGER_COL);
        let filter = doc! { "task_id": task_id.to_string() };
        let result = col.delete_many(filter, None).await.map_err(mongo_err)?;
        Ok(u32::try_from(result.deleted_count).unwrap_or(u32::MAX))
    }

    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(VALID_COL);
        let json = serde_json::to_string(vc).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let filter = doc! { "_id": &vc.valid_condition_id };
        let update = doc! { "$set": { "data": &json } };
        col.update_one(
            filter,
            update,
            Some(
                mongodb::options::UpdateOptions::builder()
                    .upsert(true)
                    .build(),
            ),
        )
        .await
        .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(VALID_COL);
        let mut cursor = col.find(doc! {}, None).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let vc: ValidCondition =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(vc);
            }
        }
        Ok(result)
    }

    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(VALID_COL);
        let bson_ids: Vec<mongodb::bson::Bson> = ids
            .iter()
            .map(|id| mongodb::bson::Bson::String(id.clone()))
            .collect();
        let filter = doc! { "_id": { "$in": bson_ids } };
        col.delete_many(filter, None).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CRON_EXEC_COL);
        let filter = doc! { "_id": cond_id.as_str() };
        let result = col.find_one(filter, None).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let ts = d
                    .get_str("timestamp")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                let dt = DateTime::parse_from_rfc3339(ts)
                    .map(|d| d.with_timezone(&Utc))
                    .map_err(|e| RustvelloError::Serialization {
                        message: format!("cron timestamp: {}", e),
                    })?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }

    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CRON_EXEC_COL);

        // Atomic compare-and-swap: filter includes the expected timestamp value
        let filter = match expected_last {
            Some(ts) => doc! { "_id": cond_id.as_str(), "timestamp": ts.to_rfc3339() },
            None => doc! { "_id": cond_id.as_str(), "timestamp": { "$exists": false } },
        };
        let update = doc! { "$set": { "timestamp": time.to_rfc3339() } };

        let result = col
            .update_one(
                filter,
                update,
                Some(
                    mongodb::options::UpdateOptions::builder()
                        .upsert(expected_last.is_none())
                        .build(),
                ),
            )
            .await;

        match result {
            Ok(r) => Ok(r.matched_count > 0 || r.upserted_id.is_some()),
            Err(e) => {
                // Duplicate key error (code 11000) means another runner raced us
                // on the first execution — treat as "not claimed"
                if matches!(*e.kind, ErrorKind::Write(WriteFailure::WriteError(ref we)) if we.code == 11000)
                {
                    Ok(false)
                } else {
                    Err(mongo_err(e))
                }
            }
        }
    }

    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RUN_COL);
        let doc = doc! { "_id": run_id.as_str().to_owned(), "claimed": true };
        match col.insert_one(doc, None).await {
            Ok(_) => Ok(true),
            Err(e) => {
                // Duplicate key error (code 11000) means already claimed
                if matches!(*e.kind, ErrorKind::Write(WriteFailure::WriteError(ref we)) if we.code == 11000)
                {
                    Ok(false)
                } else {
                    Err(mongo_err(e))
                }
            }
        }
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        for col_name in [COND_COL, TRIGGER_COL, VALID_COL, CRON_EXEC_COL, RUN_COL] {
            let col = db.collection::<mongodb::bson::Document>(col_name);
            col.delete_many(doc! {}, None).await.map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COND_COL);
        let mut cursor = col.find(doc! {}, None).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(data)) = (d.get_str("_id"), d.get_str("data")) {
                let cond: TriggerCondition =
                    serde_json::from_str(data).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push((ConditionId::from(id.to_string()), cond));
            }
        }
        Ok(result)
    }
}
