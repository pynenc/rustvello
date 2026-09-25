//! Runner implementations for Rustvello.
//!
//! This module provides multiple runner types for executing tasks:
//!
//! - [`PersistentTokioRunner`]: Persistent worker pool using tokio tasks (default).
//! - [`RayonRunner`]: Uses rayon thread pool for CPU-bound tasks (feature-gated).
//!
//! [`PersistentTokioRunner::with_subprocess_executor`] swaps the in-process executor for a pool
//! of worker processes (one interpreter per process for Python tasks).
//!
//! The [`TaskRunner`] type alias points to [`PersistentTokioRunner`].

mod attempt;
mod bounded;
mod control_plane;
mod dispatcher;
mod executor;
pub(crate) mod executor_common;
mod persistent_tokio;
#[cfg(feature = "rayon")]
mod rayon_runner;

pub use bounded::ShutdownOutcome;
pub use executor::SubprocessSpec;
pub use persistent_tokio::PersistentTokioRunner;
#[cfg(feature = "rayon")]
pub use rayon_runner::RayonRunner;

/// Alias for the default Tokio-backed runner.
pub type TaskRunner = PersistentTokioRunner;
