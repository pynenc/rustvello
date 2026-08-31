use std::sync::Arc;

use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::connection::{mongo_err, MongoPool};

const COLLECTION: &str = "broker_queue";

/// MongoDB-backed broker using a capped-style collection for FIFO ordering.
///
/// Each document has `{ _id, invocation_id, task_id (optional), ts }`.
/// `retrieve_invocation` uses `find_one_and_delete` with ascending `_id` sort.
#[non_exhaustive]
pub struct MongoBroker {
    pool: Arc<MongoPool>,
}

impl MongoBroker {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Broker for MongoBroker {
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let doc = doc! {
            "invocation_id": invocation_id.to_string(),
            "task_id": mongodb::bson::Bson::Null,
        };
        col.insert_one(doc).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let doc = doc! {
            "invocation_id": invocation_id.to_string(),
            "task_id": task_id.to_string(),
        };
        col.insert_one(doc).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn route_invocations(&self, ids: &[InvocationId]) -> RustvelloResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let docs: Vec<mongodb::bson::Document> = ids
            .iter()
            .map(|id| {
                doc! {
                    "invocation_id": id.to_string(),
                    "task_id": mongodb::bson::Bson::Null,
                }
            })
            .collect();
        col.insert_many(docs).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let filter = match task_id {
            Some(tid) => doc! { "task_id": tid.to_string() },
            None => doc! {},
        };
        let result = col
            .find_one_and_delete(filter)
            .sort(doc! { "_id": 1 })
            .await
            .map_err(mongo_err)?;
        match result {
            Some(d) => {
                let inv_str = d.get_str("invocation_id").map_err(|e| {
                    rustvello_core::error::RustvelloError::state_backend(e.to_string())
                })?;
                Ok(Some(InvocationId::from_string(inv_str.to_string())))
            }
            None => Ok(None),
        }
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let filter = match task_id {
            Some(tid) => doc! { "task_id": tid.to_string() },
            None => doc! {},
        };
        let count = col.count_documents(filter).await.map_err(mongo_err)?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let filter = match task_id {
            Some(tid) => doc! { "task_id": tid.to_string() },
            None => doc! {},
        };
        col.delete_many(filter).await.map_err(mongo_err)?;
        Ok(())
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let global = col
            .find_one_and_delete(doc! { "task_id": mongodb::bson::Bson::Null })
            .sort(doc! { "_id": 1 })
            .await
            .map_err(mongo_err)?;
        let filter = if language.is_empty() {
            doc! { "task_id": { "$regex": "^[^:]+\\." } }
        } else {
            let prefix = format!("^{language}::");
            doc! { "task_id": { "$regex": &prefix } }
        };
        let result = match global {
            Some(document) => Some(document),
            None => col
                .find_one_and_delete(filter)
                .sort(doc! { "_id": 1 })
                .await
                .map_err(mongo_err)?,
        };
        match result {
            Some(d) => {
                let inv_str = d.get_str("invocation_id").map_err(|e| {
                    rustvello_core::error::RustvelloError::state_backend(e.to_string())
                })?;
                Ok(Some(InvocationId::from_string(inv_str.to_string())))
            }
            None => Ok(None),
        }
    }
}
