//! Runner implementations for Rustvello.
//!
//! This module provides multiple runner types for executing tasks:
//!
//! - [`PersistentTokioRunner`]: Persistent worker pool using tokio tasks (default).
//! - [`RayonRunner`]: Uses rayon thread pool for CPU-bound tasks (feature-gated).
//!
//! The [`TaskRunner`] type alias points to [`PersistentTokioRunner`].

use std::sync::Arc;
use std::time::Duration;

use rustvello_core::observability::EventEmitter;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

mod control_plane;
mod dispatcher;
mod executor;
pub(crate) mod executor_common;
mod persistent_tokio;
#[cfg(feature = "rayon")]
mod rayon_runner;

pub use persistent_tokio::PersistentTokioRunner;
#[cfg(feature = "rayon")]
pub use rayon_runner::RayonRunner;

/// Alias for the default Tokio-backed runner.
pub type TaskRunner = PersistentTokioRunner;

/// Wrapper that delegates all events to an inner `Arc<dyn EventEmitter>`.
///
/// Used by runners to chain their own `CompositeEmitter` on top of a
/// user-provided emitter.
pub(crate) struct PrevEmitterWrapper(pub(crate) Arc<dyn EventEmitter>);

impl EventEmitter for PrevEmitterWrapper {
    fn on_worker_started(&self, r: &RunnerId) {
        self.0.on_worker_started(r);
    }
    fn on_worker_shutdown(&self, r: &RunnerId) {
        self.0.on_worker_shutdown(r);
    }
    fn on_task_submitted(&self, t: &TaskId, i: &InvocationId) {
        self.0.on_task_submitted(t, i);
    }
    fn on_task_started(&self, t: &TaskId, i: &InvocationId) {
        self.0.on_task_started(t, i);
    }
    fn on_task_succeeded(&self, t: &TaskId, i: &InvocationId, d: Duration) {
        self.0.on_task_succeeded(t, i, d);
    }
    fn on_task_failed(&self, t: &TaskId, i: &InvocationId, e: &str, d: Duration) {
        self.0.on_task_failed(t, i, e, d);
    }
    fn on_task_retried(&self, t: &TaskId, i: &InvocationId, a: u32) {
        self.0.on_task_retried(t, i, a);
    }
    fn on_queue_depth(&self, q: &str, d: usize) {
        self.0.on_queue_depth(q, d);
    }
    fn on_cc_rejected(&self, t: &TaskId) {
        self.0.on_cc_rejected(t);
    }
}
