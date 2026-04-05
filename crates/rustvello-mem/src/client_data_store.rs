//! In-memory [`ClientDataStore`] backend.

use std::collections::HashMap;
use tokio::sync::Mutex;

use async_trait::async_trait;

use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::{RustvelloError, RustvelloResult};

/// In-memory client data store backed by a `HashMap`.
///
/// All data lives in process memory and is lost on restart.
/// Suitable for tests and development.
pub struct MemClientDataStore {
    data: Mutex<HashMap<String, String>>,
}

impl MemClientDataStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemClientDataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ClientDataStore for MemClientDataStore {
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        self.data
            .lock()
            .await
            .insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        self.data
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or_else(|| RustvelloError::state_backend(format!("key not found: {key}")))
    }

    async fn purge(&self) -> RustvelloResult<()> {
        self.data.lock().await.clear();
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "In-Memory"
    }

    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        let count = self.data.lock().await.len();
        vec![("Stored Entries", count.to_string())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_and_retrieve() {
        let store = MemClientDataStore::new();
        store.store("k1", "v1").await.unwrap();
        assert_eq!(store.retrieve("k1").await.unwrap(), "v1");
    }

    #[tokio::test]
    async fn retrieve_missing_key_errors() {
        let store = MemClientDataStore::new();
        let err = store.retrieve("nonexistent").await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn purge_removes_all() {
        let store = MemClientDataStore::new();
        store.store("k1", "v1").await.unwrap();
        store.store("k2", "v2").await.unwrap();
        store.purge().await.unwrap();
        assert!(store.retrieve("k1").await.is_err());
        assert!(store.retrieve("k2").await.is_err());
    }

    #[tokio::test]
    async fn upsert_semantics() {
        let store = MemClientDataStore::new();
        store.store("k1", "v1").await.unwrap();
        store.store("k1", "v2").await.unwrap();
        assert_eq!(store.retrieve("k1").await.unwrap(), "v2");
    }
}
