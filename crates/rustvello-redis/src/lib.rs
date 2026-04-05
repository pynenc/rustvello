//! Redis backend implementations for Rustvello.
//!
//! Provides all five backend components backed by Redis:
//! - [`RedisBroker`] — FIFO invocation queue via Redis lists
//! - [`RedisOrchestrator`] — Invocation lifecycle management with atomic Redis operations
//! - [`RedisStateBackend`] — Invocation/call persistence in Redis hashes
//! - [`RedisClientDataStore`] — Content-hash key-value store
//! - [`RedisTriggerStore`] — Trigger and condition persistence
//!
//! Suitable for distributed, multi-process deployments.

pub mod broker;
pub mod client_data_store;
mod connection;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::RedisBroker;
    pub use crate::client_data_store::RedisClientDataStore;
    pub use crate::connection::RedisPool;
    pub use crate::orchestrator::RedisOrchestrator;
    pub use crate::state_backend::RedisStateBackend;
    pub use crate::trigger::RedisTriggerStore;
}
