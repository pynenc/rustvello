//! Task middleware for pre/post execution hooks.
//!
//! Middleware runs runner-local (not distributed). For distributed status
//! callbacks, use the trigger system instead.

use async_trait::async_trait;

use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::error::{RustvelloError, RustvelloResult};

/// Hook that runs before and after each task invocation on this runner.
///
/// Middlewares are called in registration order (FIFO for `before`,
/// LIFO for `after`). If any `before` hook returns `Err`, the task
/// is not executed and the error is propagated.
///
/// # Example
///
/// ```rust
/// use async_trait::async_trait;
/// use rustvello_core::middleware::TaskMiddleware;
/// use rustvello_core::error::{RustvelloError, RustvelloResult};
/// use rustvello_proto::identifiers::{InvocationId, TaskId};
///
/// struct LoggingMiddleware;
///
/// #[async_trait]
/// impl TaskMiddleware for LoggingMiddleware {
///     async fn before(&self, inv_id: &InvocationId, task_id: &TaskId) -> RustvelloResult<()> {
///         println!("Starting {} for {}", task_id, inv_id);
///         Ok(())
///     }
///
///     async fn after(
///         &self,
///         inv_id: &InvocationId,
///         task_id: &TaskId,
///         result: &Result<String, RustvelloError>,
///     ) -> RustvelloResult<()> {
///         println!("Finished {} for {}: {}", task_id, inv_id, result.is_ok());
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait TaskMiddleware: Send + Sync {
    /// Called before the task function runs.
    async fn before(&self, inv_id: &InvocationId, task_id: &TaskId) -> RustvelloResult<()>;

    /// Called after the task function runs (regardless of success/failure).
    async fn after(
        &self,
        inv_id: &InvocationId,
        task_id: &TaskId,
        result: &Result<String, RustvelloError>,
    ) -> RustvelloResult<()>;
}
