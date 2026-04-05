use std::sync::Arc;

use async_trait::async_trait;
use mongodb::bson::doc;

use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::{RustvelloError, RustvelloResult};

use crate::connection::{mongo_err, MongoPool};

const COLLECTION: &str = "client_data_store";

/// MongoDB-backed client data store.
///
/// Uses a simple collection with `{ _id: key, value: string }` documents.
#[non_exhaustive]
pub struct MongoClientDataStore {
    pool: Arc<MongoPool>,
}

impl MongoClientDataStore {
    pub fn new(pool: Arc<MongoPool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ClientDataStore for MongoClientDataStore {
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let filter = doc! { "_id": key };
        let update = doc! { "$set": { "value": value } };
        col.update_one(filter, update)
            .upsert(true)
            .await
            .map_err(mongo_err)?;
        Ok(())
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        let filter = doc! { "_id": key };
        let result = col.find_one(filter).await.map_err(mongo_err)?;
        match result {
            Some(d) => {
                let val = d
                    .get_str("value")
                    .map_err(|e| RustvelloError::state_backend(e.to_string()))?;
                Ok(val.to_string())
            }
            None => Err(RustvelloError::state_backend(format!(
                "CDS key not found: {}",
                key
            ))),
        }
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = self.pool.db().await?;
        let col = db.collection::<mongodb::bson::Document>(COLLECTION);
        col.delete_many(doc! {}).await.map_err(mongo_err)?;
        Ok(())
    }
}
