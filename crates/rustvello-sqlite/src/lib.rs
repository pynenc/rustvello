//! SQLite backend implementations for Rustvello.
//!
//! Provides persistent storage using SQLite, suitable for single-node
//! production deployments and development.

pub mod broker;
pub mod client_data_store;
pub mod db;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::SqliteBroker;
    pub use crate::client_data_store::SqliteClientDataStore;
    pub use crate::orchestrator::SqliteOrchestrator;
    pub use crate::state_backend::SqliteStateBackend;
    pub use crate::trigger::SqliteTriggerStore;
}
