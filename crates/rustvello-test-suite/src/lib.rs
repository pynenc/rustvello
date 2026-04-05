//! Shared test definitions for rustvello backend implementations.
//!
//! This crate provides reusable async test functions that exercise the
//! abstract traits ([`Broker`], [`Orchestrator`], [`StateBackend`], etc.)
//! Each backend crate (mem, sqlite, redis, …) calls these functions with
//! its own concrete implementation to verify consistent behavior.
//!
//! # Usage
//!
//! ```rust,ignore
//! // In crates/rustvello-mem/tests/suite.rs
//! use rustvello_mem::broker::MemBroker;
//!
//! #[tokio::test]
//! async fn suite_broker_route_and_retrieve() {
//!     let broker = MemBroker::new();
//!     rustvello_test_suite::broker::test_route_and_retrieve(&broker).await;
//! }
//! ```

pub mod broker;
pub mod client_data_store;
pub mod concurrency;
pub mod helpers;
pub mod isolation;
pub mod lifecycle;
pub mod orchestrator;
pub mod state_backend;
pub mod trigger;
