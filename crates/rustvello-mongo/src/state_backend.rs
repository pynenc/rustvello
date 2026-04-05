use std::sync::Arc;

use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::state_backend::{
    StateBackendCore, StateBackendQuery, StateBackendRunner, StoredRunnerContext,
};
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};

use crate::connection::{mongo_err, MongoPool};

const INV_COL: &str = "state_invocations";
const CALL_COL: &str = "state_calls";
const RESULT_COL: &str = "state_results";
const ERROR_COL: &str = "state_errors";
const HISTORY_COL: &str = "state_history";
const WF_RUNS_COL: &str = "state_workflow_runs";
const WF_DATA_COL: &str = "state_workflow_data";
const APP_INFOS_COL: &str = "state_app_infos";
const WF_SUB_COL: &str = "state_workflow_sub_invocations";
const RUNNER_CTX_COL: &str = "state_runner_contexts";

/// MongoDB-backed state backend.
///
/// Uses separate collections for invocations, calls, results, errors,
/// and history entries. Documents are stored as JSON strings within
/// BSON documents keyed by their IDs.
#[non_exhaustive]
pub struct MongoStateBackend {
    pool: Arc<MongoPool>,
}

impl MongoStateBackend {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StateBackendCore for MongoStateBackend {
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;

        let inv_json =
            serde_json::to_string(invocation).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;
        let call_json = serde_json::to_string(call).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;

        // Upsert invocation
        let inv_col = db.collection::<mongodb::bson::Document>(INV_COL);
        let filter = doc! { "_id": invocation.invocation_id.to_string() };
        let update = doc! {
            "$set": {
                "data": &inv_json,
                "workflow_id": invocation.workflow.as_ref().map(|w| w.workflow_id.to_string()),
                "parent_invocation_id": invocation.parent_invocation_id.as_ref().map(ToString::to_string),
            }
        };
        inv_col
            .update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;

        // Upsert call
        let call_col = db.collection::<mongodb::bson::Document>(CALL_COL);
        let filter = doc! { "_id": call.call_id.to_string() };
        let update = doc! { "$set": { "data": &call_json } };
        call_col
            .update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;

        Ok(())
    }

    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(INV_COL);
        let filter = doc! { "_id": invocation_id.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("data")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }
            None => Err(RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            }),
        }
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(CALL_COL);
        let filter = doc! { "_id": call_id.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("data")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }
            None => Err(RustvelloError::state_backend(format!(
                "call not found: {}",
                call_id
            ))),
        }
    }

    async fn store_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RESULT_COL);
        let filter = doc! { "_id": invocation_id.to_string() };
        let update = doc! { "$set": { "result": result } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RESULT_COL);
        let filter = doc! { "_id": invocation_id.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        Ok(result.and_then(|d| d.get_str("result").ok().map(ToString::to_string)))
    }

    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(ERROR_COL);
        let json = serde_json::to_string(error).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let filter = doc! { "_id": invocation_id.to_string() };
        let update = doc! { "$set": { "error": &json } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(ERROR_COL);
        let filter = doc! { "_id": invocation_id.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => match d.get_str("error") {
                Ok(s) => {
                    let err: TaskError =
                        serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                            message: e.to_string(),
                        })?;
                    Ok(Some(err))
                }
                Err(_) => Ok(None),
            },
            None => Ok(None),
        }
    }

    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HISTORY_COL);
        let json = serde_json::to_string(history).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let runner_id = history
            .runner_id
            .as_ref()
            .or(history.status_record.runner_id.as_ref())
            .map(|r| r.as_str().to_string());
        let ts = history
            .history_timestamp
            .unwrap_or(history.status_record.timestamp);
        let doc = doc! {
            "invocation_id": history.invocation_id.to_string(),
            "runner_id": runner_id,
            "timestamp": mongodb::bson::DateTime::from_millis(ts.timestamp_millis()),
            "data": &json,
        };
        col.insert_one(doc).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HISTORY_COL);
        let filter = doc! { "invocation_id": invocation_id.to_string() };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;

        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let h: InvocationHistory =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(h);
            }
        }
        Ok(result)
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        for col_name in [
            INV_COL,
            CALL_COL,
            RESULT_COL,
            ERROR_COL,
            HISTORY_COL,
            WF_RUNS_COL,
            WF_DATA_COL,
            APP_INFOS_COL,
            WF_SUB_COL,
            RUNNER_CTX_COL,
        ] {
            let col = db.collection::<mongodb::bson::Document>(col_name);
            col.delete_many(doc! {}).await.map_err(mongo_err)?;
        }
        Ok(())
    }
}

#[async_trait]
impl StateBackendQuery for MongoStateBackend {
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(INV_COL);
        let filter = doc! { "workflow_id": workflow_id.to_string() };
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

    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(INV_COL);
        let filter = doc! { "parent_invocation_id": parent_invocation_id.to_string() };
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

    async fn store_workflow_run(&self, workflow: &WorkflowIdentity) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_RUNS_COL);
        let filter = doc! { "_id": workflow.workflow_id.as_str() };
        let update = doc! { "$set": {
            "workflow_type": workflow.workflow_type.to_string(),
            "parent_workflow_id": workflow.parent_id.as_ref().map(|id| id.as_str().to_string()),
            "depth": workflow.depth as i32,
        }};
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_all_workflow_types(&self) -> RustvelloResult<Vec<TaskId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_RUNS_COL);
        let mut cursor = col.find(doc! {}).await.map_err(mongo_err)?;
        let mut types = std::collections::HashSet::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(t) = d.get_str("workflow_type") {
                types.insert(t.to_string());
            }
        }
        types
            .into_iter()
            .map(|s| {
                s.parse::<TaskId>().map_err(|e| {
                    RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
                })
            })
            .collect()
    }

    async fn get_workflow_runs(
        &self,
        workflow_type: &TaskId,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_RUNS_COL);
        let filter = doc! { "workflow_type": workflow_type.to_string() };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            let wf_id = d
                .get_str("_id")
                .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
            let wf_type = d
                .get_str("workflow_type")
                .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
            let parent_id = d
                .get_str("parent_workflow_id")
                .ok()
                .map(std::string::ToString::to_string);
            let depth = d.get_i32("depth").unwrap_or(0);
            let task_id = wf_type.parse::<TaskId>().map_err(|e| {
                RustvelloError::state_backend(format!("invalid workflow task_id in database: {e}"))
            })?;
            result.push(WorkflowIdentity {
                workflow_id: InvocationId::from_string(wf_id.to_string()),
                workflow_type: task_id,
                parent_id: parent_id.map(InvocationId::from_string),
                depth: u32::try_from(depth).unwrap_or(0),
            });
        }
        Ok(result)
    }

    async fn set_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
        value: &str,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_DATA_COL);
        let doc_id = format!("{}:{}", workflow_id.as_str(), key);
        let filter = doc! { "_id": &doc_id };
        let update =
            doc! { "$set": { "workflow_id": workflow_id.as_str(), "key": key, "value": value } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_DATA_COL);
        let doc_id = format!("{}:{}", workflow_id.as_str(), key);
        let filter = doc! { "_id": &doc_id };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        Ok(result.and_then(|d| d.get_str("value").ok().map(ToString::to_string)))
    }

    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(APP_INFOS_COL);
        let filter = doc! { "_id": app_id };
        let update = doc! { "$set": { "info_json": info_json } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(APP_INFOS_COL);
        let filter = doc! { "_id": app_id };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        Ok(result.and_then(|d| d.get_str("info_json").ok().map(ToString::to_string)))
    }

    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(APP_INFOS_COL);
        let mut cursor = col.find(doc! {}).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let (Ok(app_id), Ok(info)) = (d.get_str("_id"), d.get_str("info_json")) {
                result.push((app_id.to_string(), info.to_string()));
            }
        }
        Ok(result)
    }

    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_SUB_COL);
        let doc_id = format!("{}:{}", workflow_id.as_str(), sub_inv_id.as_str());
        let filter = doc! { "_id": &doc_id };
        let update = doc! { "$set": {
            "workflow_id": workflow_id.as_str(),
            "sub_invocation_id": sub_inv_id.as_str(),
        }};
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_SUB_COL);
        let filter = doc! { "workflow_id": workflow_id.as_str() };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(id) = d.get_str("sub_invocation_id") {
                result.push(InvocationId::from_string(id.to_string()));
            }
        }
        Ok(result)
    }

    async fn get_all_workflow_runs(&self) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WF_RUNS_COL);
        let mut cursor = col.find(doc! {}).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            let wf_id = d.get_str("_id").unwrap_or_default().to_string();
            let wf_type_str = d.get_str("workflow_type").unwrap_or_default();
            let task_id = wf_type_str.parse::<TaskId>().map_err(|e| {
                RustvelloError::state_backend(format!("invalid workflow task_id in database: {e}"))
            })?;
            let parent_id = d
                .get_str("parent_workflow_id")
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| InvocationId::from_string(s.to_string()));
            let depth = u32::try_from(d.get_i32("depth").unwrap_or(0)).unwrap_or(0);
            result.push(WorkflowIdentity {
                workflow_id: InvocationId::from_string(wf_id),
                workflow_type: task_id,
                parent_id,
                depth,
            });
        }
        Ok(result)
    }
}

#[async_trait]
impl StateBackendRunner for MongoStateBackend {
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RUNNER_CTX_COL);
        let json = serde_json::to_string(context).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let filter = doc! { "_id": &context.runner_id };
        let update = doc! { "$set": {
            "data": &json,
            "parent_runner_id": &context.parent_runner_id,
        }};
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RUNNER_CTX_COL);
        let filter = doc! { "_id": runner_id };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let s = d
                    .get_str("data")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                let ctx: StoredRunnerContext =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(ctx))
            }
            None => Ok(None),
        }
    }

    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RUNNER_CTX_COL);
        let filter = doc! { "parent_runner_id": parent_runner_id };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let ctx: StoredRunnerContext =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(ctx);
            }
        }
        Ok(result)
    }

    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HISTORY_COL);
        let filter = doc! { "runner_id": runner_id };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(inv_id) = d.get_str("invocation_id") {
                if seen.insert(inv_id.to_string()) {
                    result.push(InvocationId::from_string(inv_id.to_string()));
                }
            }
        }
        let iter = result.into_iter().skip(offset);
        let ids: Vec<InvocationId> = if limit > 0 {
            iter.take(limit).collect()
        } else {
            iter.collect()
        };
        Ok(ids)
    }

    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HISTORY_COL);
        let filter = doc! { "runner_id": runner_id };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut seen = std::collections::HashSet::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(inv_id) = d.get_str("invocation_id") {
                seen.insert(inv_id.to_string());
            }
        }
        Ok(seen.len())
    }

    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(HISTORY_COL);
        let start_bson = mongodb::bson::DateTime::from_millis(start.timestamp_millis());
        let end_bson = mongodb::bson::DateTime::from_millis(end.timestamp_millis());
        let filter = doc! {
            "timestamp": { "$gte": start_bson, "$lte": end_bson }
        };
        let opts = mongodb::options::FindOptions::builder()
            .sort(doc! { "timestamp": 1 })
            .skip(Some(offset as u64))
            .limit(if limit > 0 { Some(limit as i64) } else { None })
            .build();
        let mut cursor = col
            .find(filter)
            .with_options(opts)
            .await
            .map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let h: InvocationHistory =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(h);
            }
        }
        Ok(result)
    }

    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(RUNNER_CTX_COL);
        let filter = doc! { "_id": { "$regex": partial_id } };
        let mut cursor = col.find(filter).await.map_err(mongo_err)?;
        let mut result = Vec::new();
        use futures_util::StreamExt;
        while let Some(doc_result) = StreamExt::next(&mut cursor).await {
            let d = doc_result.map_err(mongo_err)?;
            if let Ok(s) = d.get_str("data") {
                let ctx: StoredRunnerContext =
                    serde_json::from_str(s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(ctx);
            }
        }
        Ok(result)
    }
}
