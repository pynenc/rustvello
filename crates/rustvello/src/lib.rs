//! Rustvello — A distributed task library for Rust.
//!
//! Rustvello provides a framework for defining, routing, and executing
//! distributed tasks, inspired by the [pynenc](https://pynenc.org) Python library.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use rustvello::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create app with the builder (in-memory backends by default)
//!     let mut app = Rustvello::builder()
//!         .app_id("my-app")
//!         .build().await?;
//!     Ok(())
//! }
//! ```

pub mod app;
pub mod builder;
pub mod logging;
pub mod orchestration;
pub mod runner;
pub mod task_config;
pub mod trigger_builder;

// Re-export core traits and types
pub use rustvello_core as core;
pub use rustvello_proto as proto;

// Re-export the task proc macro so users can write `#[rustvello::task]`
pub use rustvello_macros::task;

/// Internal re-exports used by the `#[rustvello::task]` proc macro.
/// Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use crate::app::TaskEntry;
    pub use inventory;
    pub use rustvello_core;
    pub use rustvello_proto;
    pub use serde;
}

#[cfg(feature = "mem")]
#[cfg_attr(docsrs, doc(cfg(feature = "mem")))]
pub use rustvello_mem as mem;

#[cfg(feature = "sqlite")]
#[cfg_attr(docsrs, doc(cfg(feature = "sqlite")))]
pub use rustvello_sqlite as sqlite;

#[cfg(feature = "prometheus")]
#[cfg_attr(docsrs, doc(cfg(feature = "prometheus")))]
pub use rustvello_prometheus as prometheus;

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub use rustvello_postgres as postgres;

#[cfg(feature = "redis")]
#[cfg_attr(docsrs, doc(cfg(feature = "redis")))]
pub use rustvello_redis as redis;

#[cfg(feature = "mongodb")]
#[cfg_attr(docsrs, doc(cfg(feature = "mongodb")))]
pub use rustvello_mongo as mongodb;

#[cfg(feature = "mongodb3")]
#[cfg_attr(docsrs, doc(cfg(feature = "mongodb3")))]
pub use rustvello_mongo3 as mongodb3;

#[cfg(feature = "rabbitmq")]
#[cfg_attr(docsrs, doc(cfg(feature = "rabbitmq")))]
pub use rustvello_rabbitmq as rabbitmq;

#[allow(unused_imports)]
pub mod prelude {
    pub use crate::app::{RustvelloApp, TaskEntry};
    pub use crate::builder::Rustvello;
    #[cfg(feature = "rayon")]
    pub use crate::runner::RayonRunner;
    pub use crate::runner::{
        PerInvocationTokioRunner, PersistentTokioRunner, SpawnBlockingRunner, TaskRunner,
    };
    pub use crate::trigger_builder::TriggerBuilder;
    pub use rustvello_core::prelude::*;
    // Re-export proto types for convenient * imports by consumers
    pub use rustvello_proto::call::*;
    pub use rustvello_proto::config::*;
    pub use rustvello_proto::identifiers::*;
    pub use rustvello_proto::invocation::*;
    pub use rustvello_proto::status::*;
    pub use rustvello_proto::trigger::*;
}
