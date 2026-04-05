//! Data transfer objects and wire types for the Rustvello distributed task system.
//!
//! This crate contains the shared data model used across all Rustvello components:
//! - Identifiers ([`TaskId`], [`CallId`], [`InvocationId`])
//! - Status types and state machine definitions
//! - DTOs for persistence and wire transfer
//! - Configuration types

pub mod call;
pub mod config;
pub mod identifiers;
pub mod invocation;
pub mod status;
pub mod trigger;

pub mod prelude {
    pub use crate::call::*;
    pub use crate::config::*;
    pub use crate::identifiers::*;
    pub use crate::invocation::*;
    pub use crate::status::*;
    pub use crate::trigger::*;
}
