//! MongoDB 3.6+ backend implementations for Rustvello.
//!
//! Uses the `mongodb` crate v2 driver, which supports MongoDB server 3.6+.
//! For MongoDB 4.2+ deployments, prefer `rustvello-mongo` which uses the
//! latest driver version.
//!
//! Provides all five backend components backed by MongoDB:
//! - [`Mongo3Broker`] — FIFO invocation queue via MongoDB collections
//! - [`Mongo3Orchestrator`] — Invocation lifecycle management
//! - [`Mongo3StateBackend`] — Invocation/call persistence in MongoDB documents
//! - [`Mongo3ClientDataStore`] — Content-hash key-value store
//! - [`Mongo3TriggerStore`] — Trigger and condition persistence

pub mod broker;
pub mod client_data_store;
mod connection;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::Mongo3Broker;
    pub use crate::client_data_store::Mongo3ClientDataStore;
    pub use crate::connection::MongoPool;
    pub use crate::orchestrator::Mongo3Orchestrator;
    pub use crate::state_backend::Mongo3StateBackend;
    pub use crate::trigger::Mongo3TriggerStore;
}
