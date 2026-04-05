//! MongoDB backend implementations for Rustvello.
//!
//! Provides all five backend components backed by MongoDB:
//! - [`MongoBroker`] — FIFO invocation queue via MongoDB collections
//! - [`MongoOrchestrator`] — Invocation lifecycle management
//! - [`MongoStateBackend`] — Invocation/call persistence in MongoDB documents
//! - [`MongoClientDataStore`] — Content-hash key-value store
//! - [`MongoTriggerStore`] — Trigger and condition persistence
//!
//! Suitable for distributed, multi-process deployments.

pub mod broker;
pub mod client_data_store;
mod connection;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::MongoBroker;
    pub use crate::client_data_store::MongoClientDataStore;
    pub use crate::connection::MongoPool;
    pub use crate::orchestrator::MongoOrchestrator;
    pub use crate::state_backend::MongoStateBackend;
    pub use crate::trigger::MongoTriggerStore;
}
