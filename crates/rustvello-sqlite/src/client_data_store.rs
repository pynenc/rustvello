//! SQLite-backed [`ClientDataStore`] implementation.

use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::{RustvelloError, RustvelloResult};

use crate::db::{blocking, lock_err, sql_err, Database};

/// SQLite-backed client data store.
///
/// Stores large serialized values in a `client_data` table.
/// Persists across process restarts.
pub struct SqliteClientDataStore {
    db: Arc<Database>,
}

impl SqliteClientDataStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl ClientDataStore for SqliteClientDataStore {
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let key = key.to_owned();
        let value = value.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO client_data (data_key, data_value) VALUES (?1, ?2)",
                [&key, &value],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        let db = Arc::clone(&self.db);
        let key = key.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.query_row(
                "SELECT data_value FROM client_data WHERE data_key = ?1",
                [&key],
                |row| row.get(0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    RustvelloError::state_backend(format!("key not found: {key}"))
                }
                other => sql_err(other),
            })
        })
        .await
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute("DELETE FROM client_data", [])
                .map_err(sql_err)?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_store() -> SqliteClientDataStore {
        let db = Arc::new(Database::in_memory().unwrap());
        SqliteClientDataStore::new(db)
    }

    #[tokio::test]
    async fn store_and_retrieve() {
        let store = make_store();
        store.store("k1", "v1").await.unwrap();
        assert_eq!(store.retrieve("k1").await.unwrap(), "v1");
    }

    #[tokio::test]
    async fn retrieve_missing_key_errors() {
        let store = make_store();
        let err = store.retrieve("nonexistent").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn purge_removes_all() {
        let store = make_store();
        store.store("k1", "v1").await.unwrap();
        store.store("k2", "v2").await.unwrap();
        store.purge().await.unwrap();
        assert!(store.retrieve("k1").await.is_err());
        assert!(store.retrieve("k2").await.is_err());
    }

    #[tokio::test]
    async fn upsert_semantics() {
        let store = make_store();
        store.store("k1", "original").await.unwrap();
        store.store("k1", "updated").await.unwrap();
        assert_eq!(store.retrieve("k1").await.unwrap(), "updated");
    }
}
