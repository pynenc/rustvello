use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{OrchestratorQuery, OrchestratorStatus};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::{cc_pair_mongo_key, MongoOrchestrator, CC_COL, STATUS_COL, WAITERS_COL};
use crate::connection::mongo_err;

#[async_trait]
impl OrchestratorQuery for MongoOrchestrator {
    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let filter = doc! { "task_id": task_id.to_string() };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn get_invocations_by_call(
        &self,
        call_id: &CallId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let filter = doc! { "call_id": call_id.to_string() };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);

        // Server-side filter using the indexed status_name field
        let mut filter = doc! { "status_name": status.to_string() };
        if let Some(tid) = task_id {
            filter.insert("task_id", tid.to_string());
        }

        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let mut filter = doc! {};
        if let Some(tid) = task_id {
            filter.insert("task_id", tid.to_string());
        }
        if let Some(statuses) = statuses {
            let status_strs: Vec<mongodb::bson::Bson> = statuses
                .iter()
                .map(|s| mongodb::bson::Bson::String(s.to_string()))
                .collect();
            filter.insert("status_name", doc! { "$in": status_strs });
        }
        let count = col.count_documents(filter).await.map_err(mongo_err)?;
        Ok(count as usize)
    }

    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let mut filter = doc! {};
        if let Some(tid) = task_id {
            filter.insert("task_id", tid.to_string());
        }
        if let Some(statuses) = statuses {
            let status_strs: Vec<mongodb::bson::Bson> = statuses
                .iter()
                .map(|s| mongodb::bson::Bson::String(s.to_string()))
                .collect();
            filter.insert("status_name", doc! { "$in": status_strs });
        }
        let mut cursor = col
            .find(filter)
            .skip(offset as u64)
            .limit(limit as i64)
            .await
            .map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WAITERS_COL);
        // Phase 1: find invocations that are waited on (have non-empty waiters)
        let mut cursor = col
            .find(doc! { "waiters": { "$exists": true, "$ne": [] } })
            .await
            .map_err(mongo_err)?;
        let mut candidates = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                candidates.push(id.to_string());
            }
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 2: find which candidates are themselves waiting on something
        // (i.e., appear in any other document's waiters array)
        let candidate_bsons: Vec<mongodb::bson::Bson> = candidates
            .iter()
            .map(|s| mongodb::bson::Bson::String(s.clone()))
            .collect();
        let mut waiting_cursor = col
            .find(doc! { "waiters": { "$in": &candidate_bsons } })
            .await
            .map_err(mongo_err)?;
        let mut self_waiting: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(doc_result) = StreamExt::next(&mut waiting_cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(arr) = d.get_array("waiters") {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        if candidate_bsons.iter().any(|b| b.as_str() == Some(s)) {
                            self_waiting.insert(s.to_string());
                        }
                    }
                }
            }
        }

        // Phase 3: filter candidates — exclude self-waiting, check runnable status
        let mut result = Vec::new();
        for id in candidates {
            if self_waiting.contains(&id) {
                continue;
            }
            let inv_id = InvocationId::from_string(id);
            if let Ok(record) = self.get_invocation_status(&inv_id).await {
                if record.status.is_available_for_run() {
                    result.push(inv_id);
                    if result.len() >= max_num {
                        break;
                    }
                }
            }
        }
        Ok(result)
    }

    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        // Empty statuses means "no filter — return all" (matches mem/sqlite).
        let db = self.pool.db().await?;
        let status_strs: Vec<mongodb::bson::Bson> = statuses
            .iter()
            .map(|s| mongodb::bson::Bson::String(s.to_string()))
            .collect();

        let candidates: Vec<String> = match cc_args {
            Some(args) => {
                // Arg-level CC: per-pair intersection
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
                let mut filter = doc! { "task_id": task_id.to_string() };
                if !statuses.is_empty() {
                    filter.insert("status_name", doc! { "$in": &status_strs });
                }
                let mut cursor = col.find(filter).await.map_err(mongo_err)?;
                let mut result = Vec::new();
                use futures_util::StreamExt;
                while let Some(doc_result) = StreamExt::next(&mut cursor).await {
                    let d = doc_result.map_err(mongo_err)?;
                    if let Ok(id) = d.get_str("_id") {
                        result.push(InvocationId::from_string(id.to_string()));
                    }
                }
                return Ok(result);
            }
        };

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Filter candidates by status (empty statuses = no filter)
        if statuses.is_empty() {
            return Ok(candidates
                .into_iter()
                .map(InvocationId::from_string)
                .collect());
        }
        let col = db.collection::<mongodb::bson::Document>(STATUS_COL);
        let bson_ids: Vec<mongodb::bson::Bson> = candidates
            .into_iter()
            .map(mongodb::bson::Bson::String)
            .collect();
        let filter = doc! {
            "_id": { "$in": &bson_ids },
            "status_name": { "$in": &status_strs },
        };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }
}
