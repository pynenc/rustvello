//! PostgreSQL-backed [`ClientDataStore`] implementation.

use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::RustvelloResult;

use crate::db::{pg_err, Database};

/// PostgreSQL-backed client data store.
///
/// Stores large serialized values in a `client_data` table.
/// Persists across process restarts.
pub struct PostgresClientDataStore {
    db: Arc<Database>,
}

impl PostgresClientDataStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl ClientDataStore for PostgresClientDataStore {
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO client_data (data_key, data_value) VALUES ($1, $2)
                 ON CONFLICT (data_key) DO UPDATE SET data_value = $2",
                &[&key, &value],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        let client = self.db.conn().await?;
        let row = client
            .query_opt(
                "SELECT data_value FROM client_data WHERE data_key = $1",
                &[&key],
            )
            .await
            .map_err(pg_err)?
            .ok_or_else(|| {
                rustvello_core::error::RustvelloError::state_backend(format!(
                    "key not found: {key}"
                ))
            })?;
        Ok(row.get(0))
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute("DELETE FROM client_data", &[])
            .await
            .map_err(pg_err)?;
        Ok(())
    }
}
