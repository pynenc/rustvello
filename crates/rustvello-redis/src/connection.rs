use redis::aio::MultiplexedConnection;
use redis::{AsyncConnectionConfig, Client, TlsCertificates};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::reconnectable::Reconnectable;

static NEXT_DOMAIN: AtomicU64 = AtomicU64::new(1);

/// Bounded Redis runtime policy shared by every Rustvello Redis port.
#[derive(Clone, Debug)]
pub struct RedisOptions {
    /// Maximum time used to establish a connection.
    pub connection_timeout_ms: u64,
    /// Maximum time allowed for one Redis response.
    pub operation_timeout_ms: u64,
    /// Time a dequeued invocation remains reserved before it is redelivered.
    pub delivery_lease_ms: u64,
    /// Maximum queued plus leased invocations in this application namespace.
    pub max_queue_rows: u32,
    /// Maximum serialized submission/result/error payload size.
    pub max_payload_bytes: u32,
    /// Verify server-side non-eviction and persistence before accepting work.
    pub require_durable_server: bool,
}

impl Default for RedisOptions {
    fn default() -> Self {
        Self {
            connection_timeout_ms: 1_000,
            operation_timeout_ms: 1_000,
            delivery_lease_ms: 5_000,
            max_queue_rows: 10_000,
            max_payload_bytes: 64 * 1024,
            require_durable_server: false,
        }
    }
}

/// Private trust anchors for a `rediss://` endpoint.
#[derive(Clone, Debug)]
pub struct RedisTlsOptions {
    root_certificate_pem: Vec<u8>,
}

impl RedisTlsOptions {
    /// Trust exactly the supplied private CA while retaining normal hostname checks.
    pub fn private_ca_pem(root_certificate_pem: Vec<u8>) -> RustvelloResult<Self> {
        if root_certificate_pem.is_empty() || root_certificate_pem.len() > 1024 * 1024 {
            return Err(RustvelloError::Configuration {
                message: "Redis TLS CA has invalid bounds".into(),
            });
        }
        Ok(Self {
            root_certificate_pem,
        })
    }
}

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
    conn: Mutex<Option<(MultiplexedConnection, std::time::Instant)>>,
    /// Precomputed key root: `"rustvello:{app_id}:"`.
    prefix: String,
    pub(crate) domain: Arc<str>,
    pub(crate) options: RedisOptions,
}

impl RedisPool {
    /// Create a new pool from a Redis URI (e.g. `redis://127.0.0.1/`).
    ///
    /// The `app_id` is used to namespace every key as `rustvello:{app_id}:…`.
    pub fn new(uri: &str, app_id: &str) -> RustvelloResult<Self> {
        Self::new_with_options(uri, app_id, RedisOptions::default())
    }

    /// Create a bounded plaintext Redis pool.
    pub fn new_with_options(
        uri: &str,
        app_id: &str,
        options: RedisOptions,
    ) -> RustvelloResult<Self> {
        if uri.starts_with("rediss://") {
            return Err(RustvelloError::Configuration {
                message: "rediss:// requires RedisPool::new_tls_with_options".into(),
            });
        }
        let client = Client::open(uri).map_err(|e| RustvelloError::Configuration {
            message: format!("invalid Redis URI: {}", e),
        })?;
        Self::from_client(client, app_id, options)
    }

    /// Create a bounded TLS Redis pool with explicit private trust roots.
    pub fn new_tls_with_options(
        uri: &str,
        app_id: &str,
        options: RedisOptions,
        tls: RedisTlsOptions,
    ) -> RustvelloResult<Self> {
        if !uri.starts_with("rediss://") || uri.contains("#insecure") {
            return Err(RustvelloError::Configuration {
                message: "Redis TLS requires a secure rediss:// URI".into(),
            });
        }
        let client = Client::build_with_tls(
            uri,
            TlsCertificates {
                client_tls: None,
                root_cert: Some(tls.root_certificate_pem),
            },
        )
        .map_err(|e| RustvelloError::Configuration {
            message: format!("invalid Redis TLS profile: {e}"),
        })?;
        Self::from_client(client, app_id, options)
    }

    fn from_client(client: Client, app_id: &str, options: RedisOptions) -> RustvelloResult<Self> {
        if app_id.is_empty()
            || app_id.len() > 128
            || options.connection_timeout_ms == 0
            || options.operation_timeout_ms == 0
            || options.delivery_lease_ms == 0
            || options.max_queue_rows == 0
            || options.max_payload_bytes == 0
        {
            return Err(RustvelloError::Configuration {
                message: "invalid bounded Redis runtime options".into(),
            });
        }
        let domain_number = NEXT_DOMAIN.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            client,
            conn: Mutex::new(None),
            prefix: format!("rustvello:{app_id}:"),
            domain: format!("redis-publication-{domain_number}").into(),
            options,
        })
    }

    /// Key prefix including the trailing colon: `"rustvello:{app_id}:"`.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Verify that queued work cannot be evicted and Redis has an active
    /// persistence policy. This check is intentionally opt-in for development
    /// servers, but production task runtimes should require it at startup.
    pub async fn verify_server_policy(&self) -> RustvelloResult<()> {
        let mut conn = self.conn().await?;
        let policy: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory-policy")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let append_only: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("appendonly")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let save: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("save")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let noeviction = policy.last().is_some_and(|value| value == "noeviction");
        let persistent = append_only.last().is_some_and(|value| value == "yes")
            || save.last().is_some_and(|value| !value.trim().is_empty());
        if !noeviction || !persistent {
            return Err(RustvelloError::Configuration {
                message: "Redis task runtime requires maxmemory-policy=noeviction and AOF or RDB persistence".into(),
            });
        }
        Ok(())
    }

    /// Get or create a multiplexed connection.
    pub async fn conn(&self) -> RustvelloResult<MultiplexedConnection> {
        let mut guard = self.conn.lock().await;
        if let Some((connection, checked)) = guard.as_mut() {
            if checked.elapsed() < Duration::from_millis(250) {
                return Ok(connection.clone());
            }
            let mut probe = connection.clone();
            if redis::cmd("PING")
                .query_async::<String>(&mut probe)
                .await
                .is_ok()
            {
                *checked = std::time::Instant::now();
                return Ok(connection.clone());
            }
            *guard = None;
        }
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Duration::from_millis(self.options.connection_timeout_ms))
            .set_response_timeout(Duration::from_millis(self.options.operation_timeout_ms));
        let c = self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| RustvelloError::state_backend(format!("Redis connect: {}", e)))?;
        *guard = Some((c.clone(), std::time::Instant::now()));
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
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Duration::from_millis(self.options.connection_timeout_ms))
            .set_response_timeout(Duration::from_millis(self.options.operation_timeout_ms));
        let c = self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|e| RustvelloError::state_backend(format!("Redis reconnect: {}", e)))?;
        *guard = Some((c, std::time::Instant::now()));
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
