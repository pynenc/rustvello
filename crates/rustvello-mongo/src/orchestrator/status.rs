use async_trait::async_trait;
use chrono::Utc;
use mongodb::bson::doc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::{OrchestratorConcurrency, OrchestratorStatus};
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::{deserialize_record, serialize_record, MongoOrchestrator, STATUS_COL};
use crate::connection::mongo_err;

#[async_trait]
impl OrchestratorStatus for MongoOrchestrator {
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId> {
        let inv_id = InvocationId::new();
        let record = InvocationStatusRecord {
            status: InvocationStatus::Registered,
            timestamp: Utc::now(),
            runner_id: None,
        };
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let doc = doc! {
            "_id": inv_id.to_string(),
            "task_id": call.task_id.to_string(),
            "call_id": call.call_id.to_string(),
            "status_name": record.status.to_string(),
            "runner_id": mongodb::bson::Bson::Null,
            "record": serialize_record(&record)?,
        };
        col.insert_one(doc).await.map_err(mongo_err)?;
        Ok(inv_id)
    }

    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let filter = doc! { "_id": invocation_id.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("record")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                deserialize_record(s)
            }
            None => Err(RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            }),
        }
    }

    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        use rustvello_proto::status::status_record_transition;

        // Read current status and validate transition
        let current_record = self.get_invocation_status(invocation_id).await?;
        let new_record = status_record_transition(Some(&current_record), status, runner_id)
            .map_err(|e| {
                rustvello_core::error::status_machine_error_to_rustvello(
                    e,
                    invocation_id,
                    current_record.status,
                )
            })?;

        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        // Optimistic CAS: only update if status_name still matches what we read
        let filter = doc! {
            "_id": invocation_id.to_string(),
            "status_name": current_record.status.to_string(),
        };
        let update = doc! { "$set": {
            "record": serialize_record(&new_record)?,
            "status_name": new_record.status.to_string(),
            "runner_id": new_record.runner_id.as_ref().map(std::string::ToString::to_string),
        } };
        let result = col.update_one(filter, update).await.map_err(mongo_err)?;
        if result.matched_count == 0 {
            return Err(RustvelloError::state_backend(
                "concurrent status modification detected".to_string(),
            ));
        }
        Ok(new_record)
    }

    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let record = InvocationStatusRecord {
            status: InvocationStatus::Registered,
            timestamp: Utc::now(),
            runner_id: runner_id.cloned(),
        };
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let doc_to_insert = doc! {
            "_id": invocation_id.to_string(),
            "task_id": call.task_id.to_string(),
            "call_id": call.call_id.to_string(),
            "status_name": record.status.to_string(),
            "runner_id": runner_id.map(|r| r.as_str().to_string()),
            "record": serialize_record(&record)?,
        };
        // Insert only if not already present; ignore duplicate key error
        match col.insert_one(doc_to_insert).await {
            Ok(_) => {}
            Err(e) => {
                if !e.to_string().contains("E11000") {
                    return Err(mongo_err(e));
                }
            }
        }
        Ok(record)
    }

    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>("orch_retries");
        let filter = doc! { "_id": invocation_id.to_string() };
        let update = doc! { "$inc": { "count": 1 } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        // Read back the new value
        let result = col
            .find_one(doc! { "_id": invocation_id.to_string() })
            .await
            .map_err(mongo_err)?;
        match result {
            Some(d) => Ok(d.get_i32("count").unwrap_or(0) as u32),
            None => Ok(0),
        }
    }

    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>("orch_retries");
        let result = col
            .find_one(doc! { "_id": invocation_id.to_string() })
            .await
            .map_err(mongo_err)?;
        Ok(result.and_then(|d| d.get_i32("count").ok()).unwrap_or(0) as u32)
    }

    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let inv_str = invocation_id.to_string();
        // Remove status
        db.collection::<mongodb::bson::Document>(STATUS_COL)
            .delete_one(doc! { "_id": &inv_str })
            .await
            .map_err(mongo_err)?;
        // Remove from CC
        self.remove_from_concurrency_index(invocation_id).await?;
        // Remove waiters
        db.collection::<mongodb::bson::Document>(super::WAITERS_COL)
            .delete_one(doc! { "_id": &inv_str })
            .await
            .map_err(mongo_err)?;
        // Remove retries
        db.collection::<mongodb::bson::Document>("orch_retries")
            .delete_one(doc! { "_id": &inv_str })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        for col_name in [
            STATUS_COL,
            super::WAITERS_COL,
            super::CC_COL,
            super::HEARTBEAT_COL,
            "orch_retries",
        ] {
            db.collection::<mongodb::bson::Document>(col_name)
                .delete_many(doc! {})
                .await
                .map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn schedule_auto_purge(&self, _invocation_id: &InvocationId) -> RustvelloResult<()> {
        Err(RustvelloError::NotSupported {
            backend: "MongoDB".into(),
            method: "schedule_auto_purge".into(),
        })
    }

    async fn run_auto_purge(&self, _max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>> {
        Err(RustvelloError::NotSupported {
            backend: "MongoDB".into(),
            method: "run_auto_purge".into(),
        })
    }
}
