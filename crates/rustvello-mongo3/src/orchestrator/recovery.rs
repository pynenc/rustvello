use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mongodb::bson::doc;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{
    ActiveRunnerInfo, AtomicServiceExecution, OrchestratorRecovery,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId};

use super::{
    deserialize_record, Mongo3Orchestrator, ATOMIC_TIMELINE_COL, HEARTBEAT_COL, STATUS_COL,
};
use crate::connection::mongo_err;

#[async_trait]
impl OrchestratorRecovery for Mongo3Orchestrator {
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        _can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HEARTBEAT_COL);
        let filter = doc! { "_id": runner_id.to_string() };
        let now = Utc::now().to_rfc3339();
        let update = doc! { "$set": { "timestamp": &now } };
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

    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(max_pending_seconds).unwrap_or(i64::MAX));

        // Single query: fetch Pending invocations with their record.
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let filter = doc! { "status_name": "Pending" };
        let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;

        let mut stale = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(record_str)) = (d.get_str("_id"), d.get_str("record")) {
                if let Ok(record) = deserialize_record(record_str) {
                    if record.timestamp < threshold {
                        stale.push(InvocationId::from_string(id.to_string()));
                    }
                }
            }
        }
        Ok(stale)
    }

    /// Uses a `$lookup` aggregation pipeline to avoid N+1 queries.
    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(
                i64::try_from(runner_dead_after_seconds).unwrap_or(i64::MAX),
            );
        let threshold_str = threshold.to_rfc3339();

        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);

        // Single aggregation: Running → lookup heartbeat → filter stale
        let pipeline = vec![
            // Filter to Running invocations that have a runner_id
            doc! { "$match": {
                "status_name": "Running",
                "runner_id": { "$exists": true, "$ne": mongodb::bson::Bson::Null },
            } },
            // Join with heartbeat collection on runner_id
            doc! { "$lookup": {
                "from": HEARTBEAT_COL,
                "localField": "runner_id",
                "foreignField": "_id",
                "as": "heartbeat",
            } },
            // Unwind heartbeat (preserveNullAndEmptyArrays: missing = stale)
            doc! { "$unwind": {
                "path": "$heartbeat",
                "preserveNullAndEmptyArrays": true,
            } },
            // Filter stale: no heartbeat OR timestamp < threshold
            doc! { "$match": {
                "$or": [
                    { "heartbeat": { "$eq": mongodb::bson::Bson::Null } },
                    { "heartbeat.timestamp": { "$lt": &threshold_str } },
                ],
            } },
            // Project only _id
            doc! { "$project": { "_id": 1 } },
        ];

        let mut cursor = col.aggregate(pipeline, None).await.map_err(mongo_err)?;
        let mut stale = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                stale.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(stale)
    }

    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let threshold_str = threshold.to_rfc3339();
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HEARTBEAT_COL);
        let filter = doc! { "timestamp": { "$gte": &threshold_str } };
        let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(RunnerId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        _can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let threshold_str = threshold.to_rfc3339();
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HEARTBEAT_COL);
        let filter = doc! { "timestamp": { "$gte": &threshold_str } };
        let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(id), Ok(ts_str)) = (d.get_str("_id"), d.get_str("timestamp")) {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                    let dt_utc: DateTime<Utc> = dt.into();
                    result.push(ActiveRunnerInfo {
                        runner_id: RunnerId::from_string(id.to_string()),
                        creation_time: dt_utc,
                        last_heartbeat: dt_utc,
                        can_run_atomic_service: true,
                        last_service_start: None,
                        last_service_end: None,
                    });
                }
            }
        }
        Ok(result)
    }

    async fn record_atomic_service_execution(
        &self,
        runner_id: &RunnerId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(ATOMIC_TIMELINE_COL);
        col.insert_one(
            doc! {
                "runner_id": runner_id.to_string(),
                "start": mongodb::bson::DateTime::from_millis(start.timestamp_millis()),
                "end": mongodb::bson::DateTime::from_millis(end.timestamp_millis()),
            },
            None,
        )
        .await
        .map_err(mongo_err)?;

        let options = mongodb::options::FindOptions::builder()
            .sort(doc! { "start": -1, "_id": -1 })
            .skip(Some(200))
            .build();
        let mut cursor = col.find(doc! {}, Some(options)).await.map_err(mongo_err)?;
        let mut stale_ids = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Some(id) = d.get("_id") {
                stale_ids.push(id.clone());
            }
        }
        if !stale_ids.is_empty() {
            col.delete_many(doc! { "_id": { "$in": stale_ids } }, None)
                .await
                .map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(ATOMIC_TIMELINE_COL);
        let options = mongodb::options::FindOptions::builder()
            .sort(doc! { "start": -1, "_id": -1 })
            .limit(Some(200))
            .build();
        let mut cursor = col.find(doc! {}, Some(options)).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            let start = d
                .get_datetime("start")
                .map_err(|e| rustvello_core::error::RustvelloError::state_backend(e.to_string()))?;
            let end = d
                .get_datetime("end")
                .map_err(|e| rustvello_core::error::RustvelloError::state_backend(e.to_string()))?;
            let start = DateTime::<Utc>::from_timestamp_millis(start.timestamp_millis())
                .ok_or_else(|| {
                    rustvello_core::error::RustvelloError::state_backend(
                        "invalid atomic service start timestamp",
                    )
                })?;
            let end = DateTime::<Utc>::from_timestamp_millis(end.timestamp_millis()).ok_or_else(
                || {
                    rustvello_core::error::RustvelloError::state_backend(
                        "invalid atomic service end timestamp",
                    )
                },
            )?;
            result.push(AtomicServiceExecution {
                runner_id: d
                    .get_str("runner_id")
                    .map_err(|e| {
                        rustvello_core::error::RustvelloError::state_backend(e.to_string())
                    })?
                    .to_string(),
                start,
                end,
            });
        }
        Ok(result)
    }
}
