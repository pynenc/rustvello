use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::ConcurrencyControlType;

use super::{cc_pair_mongo_key, Mongo3Orchestrator, CC_COL, STATUS_COL};
use crate::connection::mongo_err;

#[async_trait]
impl OrchestratorConcurrency for Mongo3Orchestrator {
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
                        match col.find_one(filter, None).await.map_err(mongo_err)? {
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
                let mut cursor = col.find(filter, None).await.map_err(mongo_err)?;
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
            "status_name": { "$in": ["Pending", "Running"] },
        };
        let count = status_col
            .count_documents(count_filter, None)
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
        col.update_many(doc! {}, update, None)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    /// Atomic check-and-index using a MongoDB transaction.
    ///
    /// Starts a session + transaction, checks the concurrency count, and
    /// only indexes the new invocation if the limit has not been reached.
    /// Requires a replica set deployment (standalone MongoDB does not
    /// support multi-document transactions).
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

        let db = self.pool.db().await?;
        let client = self.pool.client().await?;
        let mut session = client.start_session(None).await.map_err(mongo_err)?;
        session.start_transaction(None).await.map_err(mongo_err)?;

        let cc_col = db.collection::<mongodb::bson::Document>(CC_COL);
        let status_col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let pairs = args.cc_arg_pairs();

        // Intersect per-pair CC sets within the transaction
        let mut intersection: Option<std::collections::HashSet<String>> = None;
        for (k, v) in &pairs {
            let mongo_key = cc_pair_mongo_key(task_id, k, v);
            let filter = doc! { "_id": &mongo_key };
            let members: Vec<String> = match cc_col
                .find_one_with_session(filter, None, &mut session)
                .await
                .map_err(mongo_err)?
            {
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
            intersection = Some(match intersection {
                Some(prev) => prev.intersection(&set).cloned().collect(),
                None => set,
            });
            if intersection
                .as_ref()
                .is_some_and(std::collections::HashSet::is_empty)
            {
                break;
            }
        }

        let candidates: Vec<String> = intersection
            .map(|s| s.into_iter().collect())
            .unwrap_or_default();

        // Count active (Pending/Running) invocations
        let count = if candidates.is_empty() {
            0
        } else {
            let bson_ids: Vec<mongodb::bson::Bson> = candidates
                .into_iter()
                .map(mongodb::bson::Bson::String)
                .collect();
            let count_filter = doc! {
                "_id": { "$in": &bson_ids },
                "status_name": { "$in": ["Pending", "Running"] },
            };
            let c = status_col
                .count_documents_with_session(count_filter, None, &mut session)
                .await
                .map_err(mongo_err)?;
            usize::try_from(c).unwrap_or(usize::MAX)
        };

        let limit = task_config.running_concurrency.unwrap_or(1) as usize;

        if count < limit {
            // Index the invocation within the same transaction
            for (k, v) in &pairs {
                let mongo_key = cc_pair_mongo_key(task_id, k, v);
                let filter = doc! { "_id": &mongo_key };
                let update = doc! { "$addToSet": { "invocations": invocation_id.to_string() } };
                cc_col
                    .update_one_with_session(
                        filter,
                        update,
                        Some(
                            mongodb::options::UpdateOptions::builder()
                                .upsert(true)
                                .build(),
                        ),
                        &mut session,
                    )
                    .await
                    .map_err(mongo_err)?;
            }
            session.commit_transaction().await.map_err(mongo_err)?;
            Ok(true)
        } else {
            session.abort_transaction().await.map_err(mongo_err)?;
            Ok(false)
        }
    }
}
