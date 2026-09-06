// pyo3 proc-macro generates `Into::into()` calls on errors that are already `PyErr`,
// which clippy flags as useless. Suppress crate-wide.
#![allow(clippy::useless_conversion)]
// pyo3's `create_exception!` macro generates `#[cfg(feature = "gil-refs")]`
// which the crate doesn't define — suppress the resulting cfg warnings.
#![allow(unexpected_cfgs)]

//! PyO3 type wrappers for exposing Rustvello to Python.
//!
//! This crate wraps Rust types as Python classes using PyO3.
//! It is consumed by `py-rustvello` to build the Python wheel.

// Macro modules — must appear before consumers.
#[macro_use]
mod state_backend_macro;
#[macro_use]
mod orchestrator_macro;
#[macro_use]
mod trigger_store_macro;
#[macro_use]
mod broker_macro;
#[macro_use]
mod client_data_store_macro;

pub mod app;
pub mod backend_extract;
pub mod broker;
pub mod client_data_store;
pub mod config;
pub mod error;
pub mod identifiers;
pub mod invocation;
pub mod logging;
pub mod orchestrator;
pub mod runner;
pub mod runtime;
pub mod state_backend;
pub mod status;
pub mod trigger;
pub mod utils;
pub mod workflow;

// Backend-specific modules (feature-gated)
#[cfg(feature = "mongodb")]
pub mod mongo;
#[cfg(feature = "mongodb3")]
pub mod mongo3;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "sqlite")]
pub mod sqlite;
