//! PostgreSQL backend implementations for Rustvello.
//!
//! Provides persistent storage using PostgreSQL, suitable for multi-node
//! production deployments with full ACID guarantees and connection pooling.

#[cfg(test)]
mod acceptance;
mod bounded;
pub mod broker;
pub mod client_data_store;
pub mod db;
pub mod failpoints;
pub mod orchestrator;
mod publication;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::PostgresBroker;
    pub use crate::client_data_store::PostgresClientDataStore;
    #[cfg(feature = "tls")]
    pub use crate::db::PostgresTlsOptions;
    pub use crate::db::{Database, PostgresOptions};
    pub use crate::orchestrator::PostgresOrchestrator;
    pub use crate::state_backend::PostgresStateBackend;
    pub use crate::trigger::PostgresTriggerStore;
}
