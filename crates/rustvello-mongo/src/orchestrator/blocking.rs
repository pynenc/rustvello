use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorBlocking;
use rustvello_proto::identifiers::InvocationId;

use super::{MongoOrchestrator, WAITERS_COL};
use crate::connection::mongo_err;

#[async_trait]
impl OrchestratorBlocking for MongoOrchestrator {
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WAITERS_COL);
        let filter = doc! { "_id": waited_on.to_string() };
        let update = doc! { "$addToSet": { "waiters": waiter.to_string() } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(WAITERS_COL);
        let filter = doc! { "_id": waited_on.to_string() };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let arr = match d.get_array("waiters") {
                    Ok(a) => a.clone(),
                    Err(_) => {
                        tracing::warn!(
                            "corrupt 'waiters' field for invocation {}, treating as empty",
                            waited_on
                        );
                        Vec::new()
                    }
                };
                Ok(arr
                    .into_iter()
                    .filter_map(|v| v.as_str().map(|s| InvocationId::from_string(s.to_string())))
                    .collect())
            }
            None => Ok(Vec::new()),
        }
    }

    async fn release_waiters(
        &self,
        completed: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let waiters = self.get_waiters(completed).await?;
        if !waiters.is_empty() {
            let db = self.pool.db().await?;
            let col = db.collection::<mongodb::bson::Document>(WAITERS_COL);
            col.delete_one(doc! { "_id": completed.to_string() })
                .await
                .map_err(mongo_err)?;
        }
        Ok(waiters)
    }
}
