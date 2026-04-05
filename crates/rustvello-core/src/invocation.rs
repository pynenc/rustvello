//! Invocation types for tracking task execution.
//!
//! Three types form a unified invocation model:
//!
//! - [`InvocationHandle`] — distributed execution handle (status via backends)
//! - [`SyncInvocation`] — sync execution result (status in-memory)
//! - [`Invocation`] — unified enum combining both

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;

use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::status::InvocationStatus;

use crate::error::{RustvelloError, RustvelloResult};
use crate::orchestrator::Orchestrator;
use crate::state_backend::StateBackend;

/// Handle to a submitted invocation with typed result access.
///
/// Mirrors pynenc's `BaseInvocation` hierarchy. Provides:
/// - Status checking (non-blocking)
/// - Typed result retrieval (blocking or polling)
/// - Access to the invocation identity
///
/// # Example
///
/// ```rust,no_run
/// use rustvello_core::invocation::InvocationHandle;
///
/// // After submitting a task:
/// // let handle: InvocationHandle<i32> = app.submit(&my_task, params).await?;
/// // let status = handle.status().await?;
/// // let result: i32 = handle.result().await?;
/// ```
pub struct InvocationHandle<R: DeserializeOwned = String> {
    invocation_id: InvocationId,
    orchestrator: Arc<dyn Orchestrator>,
    state_backend: Arc<dyn StateBackend>,
    _result_type: PhantomData<R>,
}

impl<R: DeserializeOwned> InvocationHandle<R> {
    /// Create a new handle from raw parts.
    pub fn new(
        invocation_id: InvocationId,
        orchestrator: Arc<dyn Orchestrator>,
        state_backend: Arc<dyn StateBackend>,
    ) -> Self {
        Self {
            invocation_id,
            orchestrator,
            state_backend,
            _result_type: PhantomData,
        }
    }

    /// Get the invocation's unique identifier.
    pub fn invocation_id(&self) -> &InvocationId {
        &self.invocation_id
    }

    /// Get the current status of this invocation.
    pub async fn status(&self) -> RustvelloResult<InvocationStatus> {
        let record = self
            .orchestrator
            .get_invocation_status(&self.invocation_id)
            .await?;
        Ok(record.status)
    }

    /// Check if the invocation has finished (success or failure).
    pub async fn is_done(&self) -> RustvelloResult<bool> {
        Ok(self.status().await?.is_terminal())
    }

    /// Get the typed result of a completed invocation.
    ///
    /// Returns an error if the invocation is not yet complete or failed.
    pub async fn result(&self) -> RustvelloResult<R> {
        let status = self.status().await?;
        match status {
            InvocationStatus::Success => {
                let raw = self
                    .state_backend
                    .get_result(&self.invocation_id)
                    .await?
                    .ok_or_else(|| RustvelloError::Internal {
                        message: format!(
                            "invocation {} has SUCCESS status but no stored result",
                            self.invocation_id
                        ),
                    })?;
                serde_json::from_str(&raw).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }
            InvocationStatus::Failed => {
                let err = self.state_backend.get_error(&self.invocation_id).await?;
                Err(RustvelloError::runner_err(err.map_or_else(
                    || "unknown error".to_string(),
                    |e| e.to_string(),
                )))
            }
            other => Err(RustvelloError::Internal {
                message: format!(
                    "invocation {} is not finished (status: {})",
                    self.invocation_id, other
                ),
            }),
        }
    }

    /// Wait for the invocation to complete, polling at the given interval.
    ///
    /// Returns the typed result once the invocation reaches a terminal state.
    ///
    /// **Note:** Uses a fixed poll interval with no backoff. For long-running
    /// tasks, prefer a longer interval (e.g., 500ms–2s) to reduce backend load.
    pub async fn wait(&self, poll_interval: Duration) -> RustvelloResult<R> {
        loop {
            if self.is_done().await? {
                return self.result().await;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Wait for the invocation to complete with a timeout.
    ///
    /// Returns `Err(Timeout)` if the invocation does not complete within the given duration.
    pub async fn wait_timeout(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> RustvelloResult<R> {
        tokio::time::timeout(timeout, self.wait(poll_interval))
            .await
            .map_err(|_| {
                RustvelloError::runner_err(format!(
                    "timeout waiting for invocation {}",
                    self.invocation_id
                ))
            })?
    }

    /// Erase the result type, returning a handle that yields raw JSON strings.
    pub fn into_untyped(self) -> InvocationHandle<String> {
        InvocationHandle {
            invocation_id: self.invocation_id,
            orchestrator: self.orchestrator,
            state_backend: self.state_backend,
            _result_type: PhantomData,
        }
    }
}

impl<R: DeserializeOwned> std::fmt::Debug for InvocationHandle<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationHandle")
            .field("invocation_id", &self.invocation_id)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// SyncInvocation — mirrors pynenc's ConcurrentInvocation
// ---------------------------------------------------------------------------

/// Synchronous invocation result — created when `dev_mode_force_sync = true`.
///
/// Mirrors pynenc's `ConcurrentInvocation`. The task has already been executed
/// (with retries if configured); this wrapper provides the same interface as
/// [`InvocationHandle`] for unified API usage via [`Invocation`].
///
/// No backend interaction — status and result are held in memory.
pub struct SyncInvocation<R> {
    invocation_id: InvocationId,
    status: InvocationStatus,
    result: Result<R, RustvelloError>,
}

impl<R> SyncInvocation<R> {
    /// Create a successful sync invocation.
    pub fn success(invocation_id: InvocationId, result: R) -> Self {
        Self {
            invocation_id,
            status: InvocationStatus::Success,
            result: Ok(result),
        }
    }

    /// Create a failed sync invocation.
    pub fn failed(invocation_id: InvocationId, error: RustvelloError) -> Self {
        Self {
            invocation_id,
            status: InvocationStatus::Failed,
            result: Err(error),
        }
    }

    /// Get the invocation's unique identifier.
    pub fn invocation_id(&self) -> &InvocationId {
        &self.invocation_id
    }

    /// Get the status (always terminal for sync invocations).
    pub fn status(&self) -> InvocationStatus {
        self.status
    }

    /// Check if done (always true for sync invocations).
    pub fn is_done(&self) -> bool {
        true
    }
}

impl<R> std::fmt::Debug for SyncInvocation<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncInvocation")
            .field("invocation_id", &self.invocation_id)
            .field("status", &self.status)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Invocation<R> — unified enum for both execution paths
// ---------------------------------------------------------------------------

/// Unified invocation type — same API for sync and distributed execution.
///
/// Created by `app.call()`. The caller doesn't need to know whether the task
/// was executed synchronously or routed through the broker.
///
/// Matches pynenc's pattern where `Task._call()` returns either a
/// `ConcurrentInvocation` or `DistributedInvocation` through a common
/// `BaseInvocation` interface.
#[non_exhaustive]
pub enum Invocation<R: DeserializeOwned> {
    /// Sync execution (dev mode). Result is already computed.
    Sync(SyncInvocation<R>),
    /// Distributed execution. Result available after runner processes it.
    Distributed(InvocationHandle<R>),
}

impl<R: DeserializeOwned> Invocation<R> {
    /// Get the invocation's unique identifier.
    pub fn invocation_id(&self) -> &InvocationId {
        match self {
            Self::Sync(s) => s.invocation_id(),
            Self::Distributed(d) => d.invocation_id(),
        }
    }

    /// Get the current status of this invocation.
    pub async fn status(&self) -> RustvelloResult<InvocationStatus> {
        match self {
            Self::Sync(s) => Ok(s.status()),
            Self::Distributed(d) => d.status().await,
        }
    }

    /// Check if the invocation has finished (success or failure).
    pub async fn is_done(&self) -> RustvelloResult<bool> {
        match self {
            Self::Sync(s) => Ok(s.is_done()),
            Self::Distributed(d) => d.is_done().await,
        }
    }

    /// Get the typed result.
    ///
    /// For sync invocations, returns immediately.
    /// For distributed invocations, may fail if not yet complete.
    pub async fn result(self) -> RustvelloResult<R> {
        match self {
            Self::Sync(s) => s.result,
            Self::Distributed(d) => d.result().await,
        }
    }

    /// Wait for the invocation to complete, polling at the given interval.
    pub async fn wait(self, poll_interval: Duration) -> RustvelloResult<R> {
        match self {
            Self::Sync(s) => s.result,
            Self::Distributed(d) => d.wait(poll_interval).await,
        }
    }

    /// Wait with a timeout.
    pub async fn wait_timeout(
        self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> RustvelloResult<R> {
        match self {
            Self::Sync(s) => s.result,
            Self::Distributed(d) => d.wait_timeout(timeout, poll_interval).await,
        }
    }

    /// Returns `true` if this is a sync invocation.
    pub fn is_sync(&self) -> bool {
        matches!(self, Self::Sync(_))
    }

    /// Returns `true` if this is a distributed invocation.
    pub fn is_distributed(&self) -> bool {
        matches!(self, Self::Distributed(_))
    }
}

impl<R: DeserializeOwned> std::fmt::Debug for Invocation<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sync(s) => f.debug_tuple("Invocation::Sync").field(s).finish(),
            Self::Distributed(d) => f.debug_tuple("Invocation::Distributed").field(d).finish(),
        }
    }
}
