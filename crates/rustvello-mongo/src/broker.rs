use std::sync::Arc;

use async_trait::async_trait;
use mongodb::bson::{doc, Bson, Document, Regex};

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::connection::{mongo_err, MongoPool};

const COLLECTION: &str = "broker_queue";

#[non_exhaustive]
pub struct MongoBroker {
    pool: Arc<MongoPool>,
}

impl MongoBroker {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }

    fn invocation_id(document: &Document) -> RustvelloResult<InvocationId> {
        let value = document
            .get_str("invocation_id")
            .map_err(|error| RustvelloError::state_backend(error.to_string()))?;
        Ok(InvocationId::from_string(value.to_owned()))
    }
}

#[async_trait]
impl Broker for MongoBroker {
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let db = self.pool.db().await?;
        let task_id = task_id.map_or(Bson::Null, |task| Bson::String(task.to_string()));
        db.collection::<Document>(COLLECTION)
            .insert_one(doc! {
                "invocation_id": invocation_id.to_string(),
                "task_id": task_id,
                "queue_name": queue_name,
                "priority": priority,
            })
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, None, DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, Some(task_id), DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn retrieve_invocation_from_queue(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        let db = self.pool.db().await?;
        let mut filter = doc! { "queue_name": queue_name };
        if let Some(task_id) = task_id {
            filter.insert("task_id", task_id.to_string());
        }
        db.collection::<Document>(COLLECTION)
            .find_one_and_delete(filter)
            .sort(doc! { "priority": -1, "_id": 1 })
            .await
            .map_err(mongo_err)?
            .as_ref()
            .map(Self::invocation_id)
            .transpose()
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_from_queue(DEFAULT_QUEUE, task_id)
            .await
    }

    async fn retrieve_invocation_for_language_from_queue(
        &self,
        language: &str,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        let language_filter = if language.is_empty() {
            doc! { "$or": [
                { "task_id": Bson::Null },
                { "task_id": { "$not": Bson::RegularExpression(Regex { pattern: "::".to_string(), options: String::new() }) } }
            ] }
        } else {
            doc! { "$or": [
                { "task_id": Bson::Null },
                { "task_id": { "$regex": format!("^{language}::") } }
            ] }
        };
        let filter = doc! { "queue_name": queue_name, "$and": [language_filter] };
        let db = self.pool.db().await?;
        db.collection::<Document>(COLLECTION)
            .find_one_and_delete(filter)
            .sort(doc! { "priority": -1, "_id": 1 })
            .await
            .map_err(mongo_err)?
            .as_ref()
            .map(Self::invocation_id)
            .transpose()
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_for_language_from_queue(language, DEFAULT_QUEUE)
            .await
    }

    async fn count_invocations_in_queues(
        &self,
        queue_names: &[String],
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize> {
        for queue_name in queue_names {
            validate_routing(queue_name, 0.0)?;
        }
        let mut filter = Document::new();
        if !queue_names.is_empty() {
            filter.insert("queue_name", doc! { "$in": queue_names });
        }
        if let Some(task_id) = task_id {
            filter.insert("task_id", task_id.to_string());
        }
        let db = self.pool.db().await?;
        let count = db
            .collection::<Document>(COLLECTION)
            .count_documents(filter)
            .await
            .map_err(mongo_err)?;
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let filter = task_id.map_or_else(Document::new, |task_id| {
            doc! { "task_id": task_id.to_string() }
        });
        let db = self.pool.db().await?;
        db.collection::<Document>(COLLECTION)
            .delete_many(filter)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }
}
