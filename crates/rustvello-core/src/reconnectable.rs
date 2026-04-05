//! Reconnectable trait for backend connection pools.
//!
//! Network-backed pools (Redis, MongoDB, RabbitMQ) can become stale when
//! the remote server restarts or the network partitions. This trait
//! provides a uniform health-check / reconnect interface that the runner
//! can call periodically.

use crate::error::RustvelloResult;
use async_trait::async_trait;

/// A connection pool that can verify its health and re-establish connectivity.
///
/// Implementations should be cheap to call (e.g. a `PING` command) and
/// idempotent — calling `reconnect()` on a healthy pool is a no-op.
#[async_trait]
pub trait Reconnectable: Send + Sync {
    /// Returns `true` if the pool has at least one usable connection.
    async fn health_check(&self) -> bool;

    /// Attempt to re-establish connectivity.
    ///
    /// Implementations should log the reconnection attempt internally.
    /// Returns `Ok(())` on success, or an error if reconnection fails.
    async fn reconnect(&self) -> RustvelloResult<()>;
}
