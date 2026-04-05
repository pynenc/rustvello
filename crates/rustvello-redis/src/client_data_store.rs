use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::RustvelloResult;

use crate::connection::{redis_err, scan_keys, RedisPool};

/// Redis-backed client data store.
///
/// Uses simple GET/SET with content-hash keys for deduplication.
#[non_exhaustive]
pub struct RedisClientDataStore {
    pool: Arc<RedisPool>,
    key_prefix: String,
}

impl RedisClientDataStore {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let p = pool.prefix();
        Self {
            key_prefix: format!("{p}cds:"),
            pool,
        }
    }
}

#[async_trait]
impl ClientDataStore for RedisClientDataStore {
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let rkey = format!("{}{}", self.key_prefix, key);
        conn.set::<_, _, ()>(&rkey, value).await.map_err(redis_err)
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        let mut conn = self.pool.conn().await?;
        let rkey = format!("{}{}", self.key_prefix, key);
        let val: Option<String> = conn.get(&rkey).await.map_err(redis_err)?;
        val.ok_or_else(|| {
            rustvello_core::error::RustvelloError::state_backend(format!(
                "CDS key not found: {}",
                key
            ))
        })
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", self.key_prefix)).await?;
        if !keys.is_empty() {
            conn.del::<_, ()>(keys).await.map_err(redis_err)?;
        }
        Ok(())
    }
}
