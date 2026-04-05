use redis::aio::MultiplexedConnection;
use redis::Client;
use tokio::sync::Mutex;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::reconnectable::Reconnectable;

/// Shared Redis connection pool.
///
/// Wraps a `MultiplexedConnection` which internally multiplexes
/// multiple concurrent requests over a single TCP connection.
///
/// Data is isolated by `app_id`: every Redis key is prefixed with
/// `rustvello:{app_id}:`, so two pools created with different `app_id`
/// values against the same Redis server will not see each other's data.
#[non_exhaustive]
pub struct RedisPool {
    client: Client,
    conn: Mutex<Option<MultiplexedConnection>>,
    /// Precomputed key root: `"rustvello:{app_id}:"`.
    prefix: String,
}

impl RedisPool {
    /// Create a new pool from a Redis URI (e.g. `redis://127.0.0.1/`).
    ///
    /// The `app_id` is used to namespace every key as `rustvello:{app_id}:…`.
    pub fn new(uri: &str, app_id: &str) -> RustvelloResult<Self> {
        let client = Client::open(uri).map_err(|e| RustvelloError::Configuration {
            message: format!("invalid Redis URI: {}", e),
        })?;
        Ok(Self {
            client,
            conn: Mutex::new(None),
            prefix: format!("rustvello:{app_id}:"),
        })
    }

    /// Key prefix including the trailing colon: `"rustvello:{app_id}:"`.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Get or create a multiplexed connection.
    pub async fn conn(&self) -> RustvelloResult<MultiplexedConnection> {
        let mut guard = self.conn.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let c = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| RustvelloError::state_backend(format!("Redis connect: {}", e)))?;
        *guard = Some(c.clone());
        Ok(c)
    }
}

pub(crate) fn redis_err(e: redis::RedisError) -> RustvelloError {
    RustvelloError::state_backend(format!("Redis: {}", e))
}

#[async_trait::async_trait]
impl Reconnectable for RedisPool {
    async fn health_check(&self) -> bool {
        match self.conn().await {
            Ok(mut c) => redis::cmd("PING")
                .query_async::<String>(&mut c)
                .await
                .is_ok(),
            Err(_) => false,
        }
    }

    async fn reconnect(&self) -> RustvelloResult<()> {
        let mut guard = self.conn.lock().await;
        // Drop old connection
        *guard = None;
        let c = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| RustvelloError::state_backend(format!("Redis reconnect: {}", e)))?;
        *guard = Some(c);
        Ok(())
    }
}

/// Scan Redis keys matching a pattern using cursor-based SCAN.
/// Unlike KEYS, SCAN does not block the server for the entire keyspace.
pub(crate) async fn scan_keys(
    conn: &mut MultiplexedConnection,
    pattern: &str,
) -> RustvelloResult<Vec<String>> {
    let mut cursor: u64 = 0;
    let mut keys = Vec::new();
    loop {
        let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(conn)
            .await
            .map_err(redis_err)?;
        keys.extend(batch);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_new_valid_uri() {
        let pool = RedisPool::new("redis://127.0.0.1/", "test");
        assert!(pool.is_ok());
    }

    #[test]
    fn pool_new_invalid_uri() {
        let pool = RedisPool::new("not-a-uri", "test");
        assert!(pool.is_err());
        let err = match pool {
            Err(e) => e,
            Ok(_) => panic!("expected error for invalid URI"),
        };
        assert!(
            matches!(err, RustvelloError::Configuration { .. }),
            "expected Configuration, got {:?}",
            err
        );
    }

    #[test]
    fn redis_err_maps_to_storage() {
        let redis_error = redis::RedisError::from((redis::ErrorKind::IoError, "test IO error"));
        let mapped = redis_err(redis_error);
        assert!(
            matches!(mapped, RustvelloError::Infrastructure { .. }),
            "expected Infrastructure, got {:?}",
            mapped
        );
    }
}
