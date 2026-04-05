//! PostgreSQL backend implementations for Rustvello.
//!
//! Provides persistent storage using PostgreSQL, suitable for multi-node
//! production deployments with full ACID guarantees and connection pooling.

pub mod broker;
pub mod client_data_store;
pub mod db;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::PostgresBroker;
    pub use crate::client_data_store::PostgresClientDataStore;
    pub use crate::db::Database;
    pub use crate::orchestrator::PostgresOrchestrator;
    pub use crate::state_backend::PostgresStateBackend;
    pub use crate::trigger::PostgresTriggerStore;
}
