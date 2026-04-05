//! Execution context for tasks running inside a rustvello runner.
//!
//! Provides `tokio::task_local!` context so that a running task can discover
//! its own invocation identity, workflow membership, and the runner that
//! is executing it.  This mirrors pynenc's `context.py` module.
//!
//! # Usage
//!
//! The [`TaskRunner`] sets both contexts before executing a task:
//!
//! ```rust,ignore
//! use rustvello_core::context::*;
//!
//! INVOCATION_CTX.scope(inv_ctx, RUNNER_CTX.scope(run_ctx, async {
//!     // inside here, get_invocation_context() returns Some(...)
//!     let ctx = get_invocation_context().unwrap();
//! })).await;
//! ```
//!
//! When a task calls `app.call()` inside its body, the app layer reads
//! the current `InvocationContext` to determine parent/workflow inheritance.

use std::sync::Arc;

use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::WorkflowIdentity;
use serde::{Deserialize, Serialize};

/// Get a numeric thread identifier. Uses `ThreadId`'s debug representation
/// since `as_u64()` is nightly-only (`thread_id_value` feature).
/// If the `Debug` format changes in future Rust releases, falls back to 0.
fn current_thread_id() -> u64 {
    let id = std::thread::current().id();
    let debug = format!("{id:?}");
    // ThreadId debug format is "ThreadId(N)" — extract N.
    // This has been stable since Rust 1.0 and is unlikely to change,
    // but we fall back to 0 if parsing fails.
    debug
        .trim_start_matches("ThreadId(")
        .trim_end_matches(')')
        .parse()
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// InvocationContext — set per-invocation by the runner
// ---------------------------------------------------------------------------

/// Context for the currently executing invocation.
///
/// Stored in a `tokio::task_local!` variable so any code running inside the
/// task's future can retrieve it without passing references through the
/// call stack.
#[derive(Debug, Clone)]
pub struct InvocationContext {
    /// The invocation being executed.
    pub invocation_id: InvocationId,
    /// The task that is being executed.
    pub task_id: TaskId,
    /// The workflow this invocation belongs to.
    pub workflow: WorkflowIdentity,
    /// The parent invocation that spawned this one (None for top-level).
    pub parent_invocation_id: Option<InvocationId>,
    /// The current retry attempt number (0 for first attempt).
    pub num_retries: u32,
}

// ---------------------------------------------------------------------------
// RunnerContext — set per-runner by the runner's main loop
// ---------------------------------------------------------------------------

/// Context identifying the runner that is executing the current task.
///
/// Mirrors pynenc's `RunnerContext` with hierarchical parent chain,
/// process/host metadata, and JSON serialization for monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerContext {
    /// The runner's unique identifier.
    pub runner_id: RunnerId,
    /// The class/type name of the runner (e.g. "PersistentTokioRunner", "ExternalRunner").
    ///
    /// Set at runner creation time so monitoring and recovery can distinguish
    /// runner types without introspecting the `app_id`.
    pub runner_cls: Arc<str>,
    /// The application identifier (shared via `Arc` to avoid per-invocation clones).
    #[serde(
        serialize_with = "serialize_arc_str",
        deserialize_with = "deserialize_arc_str"
    )]
    pub app_id: Arc<str>,
    /// Process ID of the runner.
    pub pid: u32,
    /// Hostname where the runner is executing.
    pub hostname: String,
    /// Thread ID (or tokio task ID) of the current execution.
    pub thread_id: u64,
    /// Optional parent context (for hierarchical runner relationships).
    pub parent_ctx: Option<Box<RunnerContext>>,
}

fn serialize_arc_str<S: serde::Serializer>(v: &Arc<str>, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(v)
}

fn deserialize_arc_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Arc<str>, D::Error> {
    let s = String::deserialize(d)?;
    Ok(Arc::from(s.as_str()))
}

impl RunnerContext {
    /// Create a new `RunnerContext` capturing current process/host metadata.
    pub fn new(runner_id: RunnerId, app_id: Arc<str>, runner_cls: impl Into<Arc<str>>) -> Self {
        Self {
            runner_id,
            runner_cls: runner_cls.into(),
            app_id,
            pid: std::process::id(),
            hostname: Self::get_hostname(),
            thread_id: current_thread_id(),
            parent_ctx: None,
        }
    }

    /// Create a child context with this context as the parent.
    ///
    /// The child inherits the parent's `runner_cls` — use this for worker tasks
    /// that run under the same runner type as the parent.
    pub fn new_child(&self, runner_id: RunnerId) -> Self {
        Self {
            runner_id,
            runner_cls: Arc::clone(&self.runner_cls),
            app_id: Arc::clone(&self.app_id),
            pid: std::process::id(),
            hostname: self.hostname.clone(),
            thread_id: current_thread_id(),
            parent_ctx: Some(Box::new(self.clone())),
        }
    }

    /// Get the root runner_id by traversing up the parent chain.
    pub fn root_runner_id(&self) -> &RunnerId {
        match &self.parent_ctx {
            Some(parent) => parent.root_runner_id(),
            None => &self.runner_id,
        }
    }

    /// Create an external runner context (hostname-pid identity).
    ///
    /// Used when code runs outside any runner (scripts, CLI, tests).
    /// Matches pynenc's `ExternalRunner.get_default_external_runner_context()`.
    pub fn external() -> Self {
        let hostname = Self::get_hostname();
        let pid = std::process::id();
        let runner_id = RunnerId::from_string(format!("{hostname}-{pid}"));
        Self {
            runner_id,
            runner_cls: Arc::from("ExternalRunner"),
            app_id: Arc::from("external"),
            pid,
            hostname,
            thread_id: current_thread_id(),
            parent_ctx: None,
        }
    }

    pub(crate) fn get_hostname() -> String {
        hostname::get().map_or_else(
            |_| "unknown".to_string(),
            |h| h.to_string_lossy().into_owned(),
        )
    }
}

// ---------------------------------------------------------------------------
// task_local storage
// ---------------------------------------------------------------------------

tokio::task_local! {
    /// The invocation context for the currently running task.
    pub static INVOCATION_CTX: InvocationContext;
    /// The runner context for the current execution environment.
    pub static RUNNER_CTX: RunnerContext;
}

// ---------------------------------------------------------------------------
// Thread-local fallbacks for spawn_blocking / rayon
// ---------------------------------------------------------------------------

// `tokio::task_local!` does NOT cross `spawn_blocking` boundaries.
// To ensure child-task submissions from blocking tasks still capture the
// parent worker's runner identity, we mirror pynenc's `threading.local()`
// approach: a `std::thread_local!` that is set by each runner before
// entering `spawn_blocking` and is cleared afterwards.
std::thread_local! {
    static THREAD_RUNNER_CTX: std::cell::RefCell<Option<RunnerContext>> =
        const { std::cell::RefCell::new(None) };
    static THREAD_INVOCATION_CTX: std::cell::RefCell<Option<InvocationContext>> =
        const { std::cell::RefCell::new(None) };
}

/// Set the thread-local runner context (for use before `spawn_blocking`).
pub fn set_thread_runner_context(ctx: RunnerContext) {
    THREAD_RUNNER_CTX.with(|cell| {
        *cell.borrow_mut() = Some(ctx);
    });
}

/// Clear the thread-local runner context.
pub fn clear_thread_runner_context() {
    THREAD_RUNNER_CTX.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Set the thread-local invocation context (for use before `spawn_blocking`).
pub fn set_thread_invocation_context(ctx: InvocationContext) {
    THREAD_INVOCATION_CTX.with(|cell| {
        *cell.borrow_mut() = Some(ctx);
    });
}

/// Clear the thread-local invocation context.
pub fn clear_thread_invocation_context() {
    THREAD_INVOCATION_CTX.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Get the current invocation context, if running inside a task.
///
/// Resolution order:
/// 1. tokio `INVOCATION_CTX` task_local (async task execution path)
/// 2. `std::thread_local` fallback (spawn_blocking / rayon path)
///
/// Returns `None` when called outside a runner-managed task execution
/// (e.g. from a test or from top-level application code).
pub fn get_invocation_context() -> Option<InvocationContext> {
    // 1. Try tokio task-local
    if let Ok(ctx) = INVOCATION_CTX.try_with(Clone::clone) {
        return Some(ctx);
    }
    // 2. Try thread-local fallback (spawn_blocking / rayon)
    THREAD_INVOCATION_CTX.with(|cell| cell.borrow().clone())
}

/// Access the current invocation context by reference, avoiding a clone
/// when the tokio task-local is available.
///
/// Returns `None` when called outside a runner-managed task execution.
pub fn with_invocation_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&InvocationContext) -> R,
{
    get_invocation_context().as_ref().map(f)
}

/// Get the current runner context, if set.
pub fn get_runner_context() -> Option<RunnerContext> {
    RUNNER_CTX.try_with(Clone::clone).ok()
}

/// Access the current runner context by reference, avoiding a clone.
pub fn with_runner_context<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&RunnerContext) -> R,
{
    RUNNER_CTX.try_with(f).ok()
}

/// Get the runner ID for the current execution context.
///
/// Mirrors pynenc's `get_or_create_runner_context()` — **never returns None**.
///
/// Resolution order:
/// 1. tokio `RUNNER_CTX` task_local (set by runner during async task execution)
/// 2. `std::thread_local` fallback (set for `spawn_blocking` tasks)
/// 3. External runner identity: `"{hostname}-{pid}"` (matches pynenc's `ExternalRunner`)
pub fn get_or_create_runner_id() -> RunnerId {
    // 1. Try tokio task-local (async task execution path)
    if let Some(rid) = with_runner_context(|ctx| ctx.runner_id.clone()) {
        return rid;
    }

    // 2. Try thread-local fallback (spawn_blocking path)
    if let Some(rid) =
        THREAD_RUNNER_CTX.with(|cell| cell.borrow().as_ref().map(|ctx| ctx.runner_id.clone()))
    {
        return rid;
    }

    // 3. External runner identity (top-level submission from non-runner code)
    external_runner_id()
}

/// Get the full runner context for the current execution.
///
/// Same resolution as [`get_or_create_runner_id`] but returns the full context.
pub fn get_or_create_runner_context() -> RunnerContext {
    // 1. Try tokio task-local
    if let Ok(ctx) = RUNNER_CTX.try_with(Clone::clone) {
        return ctx;
    }

    // 2. Try thread-local fallback
    if let Some(ctx) = THREAD_RUNNER_CTX.with(|cell| cell.borrow().clone()) {
        return ctx;
    }

    // 3. External runner context
    RunnerContext::external()
}

/// Build a stable external runner ID: `"{hostname}-{pid}"`.
///
/// Matches pynenc's `ExternalRunner` which uses hostname-pid since external
/// processes are not managed by the framework.
fn external_runner_id() -> RunnerId {
    let hostname = RunnerContext::get_hostname();
    let pid = std::process::id();
    RunnerId::from_string(format!("{hostname}-{pid}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_invocation_ctx() -> InvocationContext {
        let inv_id = InvocationId::from_string("inv-1");
        let task_id = TaskId::new("mod", "my_task");
        InvocationContext {
            invocation_id: inv_id.clone(),
            task_id: task_id.clone(),
            workflow: WorkflowIdentity::root(inv_id, task_id),
            parent_invocation_id: None,
            num_retries: 0,
        }
    }

    fn sample_runner_ctx() -> RunnerContext {
        RunnerContext::new(
            RunnerId::from_string("runner-1"),
            Arc::from("test-app"),
            "TestRunner",
        )
    }

    #[tokio::test]
    async fn context_not_set_outside_scope() {
        assert!(get_invocation_context().is_none());
        assert!(get_runner_context().is_none());
    }

    #[tokio::test]
    async fn invocation_context_set_get() {
        let ctx = sample_invocation_ctx();
        INVOCATION_CTX
            .scope(ctx.clone(), async {
                let got = get_invocation_context().unwrap();
                assert_eq!(got.invocation_id, ctx.invocation_id);
                assert_eq!(got.task_id, ctx.task_id);
                assert!(got.parent_invocation_id.is_none());
            })
            .await;
    }

    #[tokio::test]
    async fn runner_context_set_get() {
        let ctx = sample_runner_ctx();
        RUNNER_CTX
            .scope(ctx, async {
                let got = get_runner_context().unwrap();
                assert_eq!(got.runner_id, RunnerId::from_string("runner-1"));
                assert_eq!(&*got.app_id, "test-app");
            })
            .await;
    }

    #[tokio::test]
    async fn nested_invocation_scopes() {
        let outer = sample_invocation_ctx();
        let inner = InvocationContext {
            invocation_id: InvocationId::from_string("inv-inner"),
            task_id: TaskId::new("mod", "inner_task"),
            workflow: outer.workflow.clone(),
            parent_invocation_id: Some(outer.invocation_id.clone()),
            num_retries: 0,
        };

        INVOCATION_CTX
            .scope(outer.clone(), async {
                // Outer context visible
                assert_eq!(
                    get_invocation_context().unwrap().invocation_id.as_str(),
                    "inv-1"
                );

                // Inner scope overrides
                INVOCATION_CTX
                    .scope(inner, async {
                        let ctx = get_invocation_context().unwrap();
                        assert_eq!(ctx.invocation_id.as_str(), "inv-inner");
                        assert_eq!(ctx.parent_invocation_id.as_ref().unwrap().as_str(), "inv-1");
                    })
                    .await;

                // Outer context restored
                assert_eq!(
                    get_invocation_context().unwrap().invocation_id.as_str(),
                    "inv-1"
                );
            })
            .await;
    }

    #[tokio::test]
    async fn both_contexts_together() {
        let inv_ctx = sample_invocation_ctx();
        let run_ctx = sample_runner_ctx();

        INVOCATION_CTX
            .scope(
                inv_ctx,
                RUNNER_CTX.scope(run_ctx, async {
                    assert!(get_invocation_context().is_some());
                    assert!(get_runner_context().is_some());
                }),
            )
            .await;

        // Outside both scopes
        assert!(get_invocation_context().is_none());
        assert!(get_runner_context().is_none());
    }
}
