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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use rustvello_proto::identifiers::{ExecutorKind, InvocationId, RunnerId, TaskId, TaskLanguage};
use rustvello_proto::invocation::{TraceContextCarrier, WorkflowIdentity};
use serde::{Deserialize, Serialize};

use crate::state_backend::StateBackend;

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
#[derive(Clone)]
pub struct InvocationContext {
    /// The invocation being executed.
    pub invocation_id: InvocationId,
    /// The task that is being executed.
    pub task_id: TaskId,
    /// The workflow this invocation belongs to, if any.
    pub workflow: Option<WorkflowIdentity>,
    /// Whether this invocation defines the workflow identity.
    pub is_workflow_defining: bool,
    /// Persistence used by root-scoped deterministic workflow operations.
    pub state_backend: Option<Arc<dyn StateBackend>>,
    /// The parent invocation that spawned this one (None for top-level).
    pub parent_invocation_id: Option<InvocationId>,
    /// The current retry attempt number (0 for first attempt).
    pub num_retries: u32,
    /// Persisted execution span identity inherited by child submissions.
    pub trace_context: TraceContextCarrier,
}

impl std::fmt::Debug for InvocationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationContext")
            .field("invocation_id", &self.invocation_id)
            .field("task_id", &self.task_id)
            .field("workflow", &self.workflow)
            .field("is_workflow_defining", &self.is_workflow_defining)
            .field("parent_invocation_id", &self.parent_invocation_id)
            .field("num_retries", &self.num_retries)
            .field("trace_context", &self.trace_context)
            .finish_non_exhaustive()
    }
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
    /// Runtime language this runner executes.
    pub runner_language: TaskLanguage,
    /// Local executor family used by this runner or worker.
    #[serde(default)]
    pub executor_kind: ExecutorKind,
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
        Self::new_with_language(runner_id, app_id, runner_cls, TaskLanguage::Rust)
    }

    /// Create a new `RunnerContext` with an explicit runtime language.
    pub fn new_with_language(
        runner_id: RunnerId,
        app_id: Arc<str>,
        runner_cls: impl Into<Arc<str>>,
        runner_language: TaskLanguage,
    ) -> Self {
        let executor_kind = match runner_language {
            TaskLanguage::Rust => ExecutorKind::Tokio,
            TaskLanguage::Python => ExecutorKind::Python,
        };
        Self::new_with_runtime(
            runner_id,
            app_id,
            runner_cls,
            runner_language,
            executor_kind,
        )
    }

    /// Create a runner context with explicit language and executor identity.
    pub fn new_with_runtime(
        runner_id: RunnerId,
        app_id: Arc<str>,
        runner_cls: impl Into<Arc<str>>,
        runner_language: TaskLanguage,
        executor_kind: ExecutorKind,
    ) -> Self {
        Self {
            runner_id,
            runner_cls: runner_cls.into(),
            runner_language,
            executor_kind,
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
        self.new_child_with_cls(runner_id, Arc::clone(&self.runner_cls))
    }

    /// Create a child context with an explicit worker class.
    pub fn new_child_with_cls(&self, runner_id: RunnerId, runner_cls: impl Into<Arc<str>>) -> Self {
        Self {
            runner_id,
            runner_cls: runner_cls.into(),
            runner_language: self.runner_language,
            executor_kind: self.executor_kind,
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
            runner_language: TaskLanguage::Rust,
            executor_kind: ExecutorKind::Tokio,
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

/// Carry the caller's invocation and runner contexts into `future`.
///
/// Task-locals do not follow a future onto another thread or runtime, and the
/// thread-local fallback is only visible on the thread that set it. This
/// captures whichever context is current (task-local first, then thread-local)
/// and re-establishes it as task-locals around `future`, so the contexts
/// survive every `.await` wherever the future is polled.
pub fn scope_current_contexts<'a, T: 'a>(
    future: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>> {
    let runner = RUNNER_CTX
        .try_with(Clone::clone)
        .ok()
        .or_else(|| THREAD_RUNNER_CTX.with(|cell| cell.borrow().clone()));
    let future = match runner {
        Some(runner) => Box::pin(RUNNER_CTX.scope(runner, future)),
        None => future,
    };
    match get_invocation_context() {
        Some(invocation) => Box::pin(INVOCATION_CTX.scope(invocation, future)),
        None => future,
    }
}

// ---------------------------------------------------------------------------
// AttemptSignal — the runner abandoned the running attempt
// ---------------------------------------------------------------------------

type AbandonHook = Box<dyn FnOnce() + Send>;

/// Raised by the runner when it abandons a running attempt: its execution
/// deadline expired or its invocation was cancelled.
///
/// The runner aborts native async Rust bodies itself. Code it cannot preempt
/// (a synchronous body on a blocking thread, a coroutine on a foreign event
/// loop) can poll [`AttemptSignal::is_abandoned`] or register a hook with
/// [`AttemptSignal::on_abandon`] to stop cooperatively. The result of an
/// abandoned attempt is discarded either way.
#[derive(Clone, Default)]
pub struct AttemptSignal(Arc<AttemptSignalInner>);

#[derive(Default)]
struct AttemptSignalInner {
    abandoned: AtomicBool,
    hooks: Mutex<Vec<AbandonHook>>,
}

impl AttemptSignal {
    /// A signal that has not been raised.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the runner has abandoned this attempt.
    pub fn is_abandoned(&self) -> bool {
        self.0.abandoned.load(Ordering::Acquire)
    }

    /// Run `hook` once when the attempt is abandoned; at once if it already was.
    ///
    /// Hooks run on the thread that raises the signal (a blocking thread in the
    /// runner), so they must not block for long.
    pub fn on_abandon<F: FnOnce() + Send + 'static>(&self, hook: F) {
        let mut hooks = self.0.hooks.lock().unwrap_or_else(PoisonError::into_inner);
        if self.is_abandoned() {
            drop(hooks);
            hook();
        } else {
            hooks.push(Box::new(hook));
        }
    }

    /// Mark the attempt abandoned and run the registered hooks (once).
    pub fn abandon(&self) {
        let hooks = {
            let mut hooks = self.0.hooks.lock().unwrap_or_else(PoisonError::into_inner);
            self.0.abandoned.store(true, Ordering::Release);
            std::mem::take(&mut *hooks)
        };
        for hook in hooks {
            hook();
        }
    }
}

impl std::fmt::Debug for AttemptSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttemptSignal")
            .field("abandoned", &self.is_abandoned())
            .finish_non_exhaustive()
    }
}

tokio::task_local! {
    /// The abandon signal of the attempt being executed.
    pub static ATTEMPT_SIGNAL: AttemptSignal;
}

std::thread_local! {
    static THREAD_ATTEMPT_SIGNAL: std::cell::RefCell<Option<AttemptSignal>> =
        const { std::cell::RefCell::new(None) };
}

/// Set the thread-local attempt signal (for use in `spawn_blocking` / rayon).
pub fn set_thread_attempt_signal(signal: Option<AttemptSignal>) {
    THREAD_ATTEMPT_SIGNAL.with(|cell| {
        *cell.borrow_mut() = signal;
    });
}

/// Clear the thread-local attempt signal.
pub fn clear_thread_attempt_signal() {
    set_thread_attempt_signal(None);
}

/// The abandon signal of the running attempt, if called inside one.
///
/// Resolution order matches [`get_invocation_context`]: the task-local first,
/// then the thread-local set for blocking and Rayon threads.
pub fn current_attempt_signal() -> Option<AttemptSignal> {
    if let Ok(signal) = ATTEMPT_SIGNAL.try_with(Clone::clone) {
        return Some(signal);
    }
    THREAD_ATTEMPT_SIGNAL.with(|cell| cell.borrow().clone())
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
            workflow: Some(WorkflowIdentity::root(inv_id, task_id)),
            is_workflow_defining: true,
            state_backend: None,
            parent_invocation_id: None,
            num_retries: 0,
            trace_context: Default::default(),
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
            is_workflow_defining: false,
            state_backend: outer.state_backend.clone(),
            parent_invocation_id: Some(outer.invocation_id.clone()),
            num_retries: 0,
            trace_context: Default::default(),
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

    #[test]
    fn attempt_signal_runs_hooks_once_including_late_ones() {
        use std::sync::atomic::AtomicUsize;

        let signal = AttemptSignal::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let hook = |fired: &Arc<AtomicUsize>| {
            let fired = Arc::clone(fired);
            move || {
                fired.fetch_add(1, Ordering::SeqCst);
            }
        };
        signal.on_abandon(hook(&fired));
        assert!(!signal.is_abandoned());
        assert_eq!(fired.load(Ordering::SeqCst), 0);

        signal.clone().abandon();
        assert!(signal.is_abandoned());
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        signal.abandon();
        assert_eq!(fired.load(Ordering::SeqCst), 1, "hooks run once");

        // Registered after the fact: runs immediately.
        signal.on_abandon(hook(&fired));
        assert_eq!(fired.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn attempt_signal_resolves_task_local_then_thread_local() {
        assert!(current_attempt_signal().is_none());
        let signal = AttemptSignal::new();
        ATTEMPT_SIGNAL
            .scope(signal.clone(), async {
                current_attempt_signal().unwrap().abandon();
            })
            .await;
        assert!(signal.is_abandoned());

        let thread_signal = AttemptSignal::new();
        let seen = std::thread::spawn({
            let thread_signal = thread_signal.clone();
            move || {
                set_thread_attempt_signal(Some(thread_signal));
                let seen = current_attempt_signal().is_some();
                clear_thread_attempt_signal();
                seen && current_attempt_signal().is_none()
            }
        })
        .join()
        .unwrap();
        assert!(seen);
    }
}
