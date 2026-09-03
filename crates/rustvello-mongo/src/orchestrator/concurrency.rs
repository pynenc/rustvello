use async_trait::async_trait;
use mongodb::{
    bson::doc,
    error::{ErrorKind, WriteFailure},
};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{sleep, Duration};

use rustvello_core::error::{InfraErrorKind, RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::ConcurrencyControlType;

use super::{cc_pair_mongo_key, MongoOrchestrator, CC_COL, STATUS_COL};
use crate::connection::mongo_err;

const CC_LOCK_ID: &str = "__rustvello_concurrency_lock__";
const CC_LOCK_TIMEOUT_MS: i64 = 5_000;
const CC_LOCK_ATTEMPTS: usize = 500;
static CC_LOCK_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        ErrorKind::Write(WriteFailure::WriteError(write_error)) if write_error.code == 11_000
    )
}

impl MongoOrchestrator {
    async fn acquire_concurrency_lock(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<String> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CC_COL);
        let owner = format!(
            "{}:{}:{}",
            std::process::id(),
            invocation_id,
            CC_LOCK_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );

        for _ in 0..CC_LOCK_ATTEMPTS {
            let now = mongodb::bson::DateTime::now();
            let expires_at =
                mongodb::bson::DateTime::from_millis(now.timestamp_millis() + CC_LOCK_TIMEOUT_MS);
            let filter = doc! {
                "_id": CC_LOCK_ID,
                "$or": [
                    { "locked_until": { "$lte": now } },
                    { "owner": &owner },
                ],
            };
            let update = doc! { "$set": { "owner": &owner, "locked_until": expires_at } };

            match col.update_one(filter, update).upsert(true).await {
                Ok(result) if result.matched_count == 1 || result.upserted_id.is_some() => {
                    return Ok(owner);
                }
                Ok(_) => sleep(Duration::from_millis(10)).await,
                Err(error) if is_duplicate_key(&error) => sleep(Duration::from_millis(10)).await,
                Err(error) => return Err(mongo_err(error)),
            }
        }

        Err(RustvelloError::Infrastructure {
            kind: InfraErrorKind::Timeout,
            message: "timed out acquiring MongoDB concurrency-control lock".to_string(),
            source: None,
        })
    }

    async fn release_concurrency_lock(&self, owner: &str) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        db.collection::<mongodb::bson::Document>(CC_COL)
            .update_one(
                doc! { "_id": CC_LOCK_ID, "owner": owner },
                doc! { "$set": { "locked_until": mongodb::bson::DateTime::from_millis(0) } },
            )
            .await
            .map_err(mongo_err)?;
        Ok(())
    }
}

#[async_trait]
impl OrchestratorConcurrency for MongoOrchestrator {
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        let db = self.pool.db().await?;

        // Get candidate invocation IDs via per-pair intersection
        let candidates: Vec<String> = match cc_args {
            Some(args) => {
                let pairs = args.cc_arg_pairs();
                let col = db.collection::<mongodb::bson::Document>(CC_COL);
                let mut result: Option<std::collections::HashSet<String>> = None;
                for (k, v) in &pairs {
                    let mongo_key = cc_pair_mongo_key(task_id, k, v);
                    let filter = doc! { "_id": &mongo_key };
                    let members: Vec<String> =
                        match col.find_one(filter).await.map_err(mongo_err)? {
                            Some(d) => {
                                let empty = Vec::new();
                                d.get_array("invocations")
                                    .unwrap_or(&empty)
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToString::to_string))
                                    .collect()
                            }
                            None => Vec::new(),
                        };
                    let set: std::collections::HashSet<String> = members.into_iter().collect();
                    result = Some(match result {
                        Some(prev) => prev.intersection(&set).cloned().collect(),
                        None => set,
                    });
                    if result
                        .as_ref()
                        .is_some_and(std::collections::HashSet::is_empty)
                    {
                        break;
                    }
                }
                result.map(|s| s.into_iter().collect()).unwrap_or_default()
            }
            None => {
                // Task-level CC: all invocations for this task
                let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
                let filter = doc! { "task_id": task_id.to_string() };
                let mut cursor = col.find(filter).await.map_err(mongo_err)?;
                let mut ids = Vec::new();
                use futures_util::StreamExt;
                while let Some(doc_result) = StreamExt::next(&mut cursor).await {
                    let d = doc_result.map_err(mongo_err)?;
                    if let Ok(id) = d.get_str("_id") {
                        ids.push(id.to_string());
                    }
                }
                ids
            }
        };

        if candidates.is_empty() {
            let limit = task_config.running_concurrency.unwrap_or(1) as usize;
            return Ok(0 < limit);
        }

        // Batch query: count Pending/Running among candidates
        let status_col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let bson_ids: Vec<mongodb::bson::Bson> = candidates
            .into_iter()
            .map(mongodb::bson::Bson::String)
            .collect();
        let count_filter = doc! {
            "_id": { "$in": &bson_ids },
            "status_name": { "$in": ["PENDING", "RUNNING"] },
        };
        let count = status_col
            .count_documents(count_filter)
            .await
            .map_err(mongo_err)?;
        let count = usize::try_from(count).unwrap_or(usize::MAX);

        let limit = task_config.running_concurrency.unwrap_or(1) as usize;
        Ok(count < limit)
    }

    async fn index_for_concurrency_control(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<()> {
        let Some(args) = cc_args else {
            return Ok(());
        };
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CC_COL);
        let pairs = args.cc_arg_pairs();

        for (k, v) in &pairs {
            let mongo_key = cc_pair_mongo_key(task_id, k, v);
            let filter = doc! { "_id": &mongo_key };
            let update = doc! { "$addToSet": { "invocations": invocation_id.to_string() } };
            col.update_one(filter, update)
                .upsert(true)
                .await
                .map_err(mongo_err)?;
        }
        Ok(())
    }

    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CC_COL);
        let update = doc! { "$pull": { "invocations": invocation_id.to_string() } };
        col.update_many(doc! {}, update).await.map_err(mongo_err)?;
        Ok(())
    }

    /// Atomically check and index using a short database-backed mutex.
    ///
    /// MongoDB guarantees atomic updates for one document even on standalone
    /// deployments. The mutex serializes the multi-document pair intersection
    /// and index updates without requiring replica-set transactions.
    async fn try_acquire_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
            self.index_for_concurrency_control(invocation_id, task_id, cc_args)
                .await?;
            return Ok(true);
        }

        let Some(args) = cc_args else {
            return self
                .check_running_concurrency(task_id, task_config, cc_args)
                .await;
        };

        let owner = self.acquire_concurrency_lock(invocation_id).await?;
        let result = async {
            let db = self.pool.db().await?;
            let cc_col = db.collection::<mongodb::bson::Document>(CC_COL);
            let pairs = args.cc_arg_pairs();
            let mut intersection: Option<std::collections::HashSet<String>> = None;

            for (key, value) in &pairs {
                let mongo_key = cc_pair_mongo_key(task_id, key, value);
                let members: Vec<String> = match cc_col
                    .find_one(doc! { "_id": &mongo_key })
                    .await
                    .map_err(mongo_err)?
                {
                    Some(document) => {
                        let empty = Vec::new();
                        document
                            .get_array("invocations")
                            .unwrap_or(&empty)
                            .iter()
                            .filter_map(|value| value.as_str().map(ToString::to_string))
                            .collect()
                    }
                    None => Vec::new(),
                };
                let members: std::collections::HashSet<String> = members.into_iter().collect();
                intersection = Some(match intersection {
                    Some(previous) => previous.intersection(&members).cloned().collect(),
                    None => members,
                });
                if intersection
                    .as_ref()
                    .is_some_and(std::collections::HashSet::is_empty)
                {
                    break;
                }
            }

            if intersection.map_or(0, |members| members.len())
                < task_config.running_concurrency.unwrap_or(1) as usize
            {
                self.index_for_concurrency_control(invocation_id, task_id, Some(args))
                    .await?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        .await;
        self.release_concurrency_lock(&owner).await?;
        result
    }
}
