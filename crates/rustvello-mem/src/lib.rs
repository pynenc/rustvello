//! In-memory backend implementations for Rustvello.
//!
//! These implementations store everything in process memory and are suitable
//! for testing and development. They are not distributed and not persistent.

pub mod broker;
pub mod client_data_store;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;

pub mod prelude {
    pub use crate::broker::MemBroker;
    pub use crate::client_data_store::MemClientDataStore;
    pub use crate::orchestrator::MemOrchestrator;
    pub use crate::state_backend::MemStateBackend;
    pub use crate::trigger::MemTriggerStore;
}
