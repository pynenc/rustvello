//! Runner identification and display info.
//!
//! Mirrors pynmon's `RunnerInfo` — provides all data needed to display
//! runner lanes with proper hierarchy (runner → worker groups).

use rustvello_core::state_backend::StoredRunnerContext;

/// Information about a runner for timeline labels.
///
/// Matches pynmon's RunnerInfo: runner_cls, runner_id, hostname, pid,
/// thread_id, and optional parent references for grouping.
#[derive(Debug, Clone)]
pub struct RunnerInfo {
    /// Runner class/type name (e.g. "TaskRunner", "PPRWorker").
    pub runner_cls: String,
    /// Full runner identifier (UUID).
    pub runner_id: String,
    /// Hostname where the runner is executing.
    pub hostname: String,
    /// Process ID.
    pub pid: u32,
    /// Thread ID.
    pub thread_id: u64,
    /// Parent runner class (if this is a child worker).
    pub parent_runner_cls: Option<String>,
    /// Parent runner ID (if this is a child worker).
    pub parent_runner_id: Option<String>,
}

impl RunnerInfo {
    /// Create from a runner ID string (fallback when no StoredRunnerContext is available).
    pub fn from_id(runner_id: &str) -> Self {
        Self {
            runner_cls: "Unknown".to_owned(),
            runner_id: runner_id.to_owned(),
            hostname: "unknown".to_owned(),
            pid: 0,
            thread_id: 0,
            parent_runner_cls: None,
            parent_runner_id: None,
        }
    }

    /// Create from a StoredRunnerContext with full metadata.
    pub fn from_context(ctx: &StoredRunnerContext) -> Self {
        Self {
            runner_cls: ctx.runner_cls.clone(),
            runner_id: ctx.runner_id.clone(),
            hostname: ctx.hostname.clone(),
            pid: ctx.pid,
            thread_id: ctx.thread_id,
            parent_runner_cls: ctx.parent_runner_cls.clone(),
            parent_runner_id: ctx.parent_runner_id.clone(),
        }
    }

    /// Create the "ExternalRunner" info for entries with no assigned runner.
    pub fn external_runner() -> Self {
        Self {
            runner_cls: "ExternalRunner".to_owned(),
            runner_id: "unassigned".to_owned(),
            hostname: String::new(),
            pid: 0,
            thread_id: 0,
            parent_runner_cls: None,
            parent_runner_id: None,
        }
    }

    /// Group identifier — parent's runner_id or self if no parent.
    pub fn group_id(&self) -> &str {
        self.parent_runner_id.as_deref().unwrap_or(&self.runner_id)
    }

    /// Whether this runner has a parent (is a child worker).
    pub fn has_parent(&self) -> bool {
        self.parent_runner_id.is_some()
    }

    /// Display label: RunnerClass(short_id).
    pub fn label(&self) -> String {
        let short = crate::util::formatting::truncate_id(&self.runner_id);
        format!("{}({})", self.runner_cls, short)
    }

    /// Details line: hostname (pid:X) — matches pynmon.
    pub fn details(&self) -> String {
        if self.hostname.is_empty() || self.hostname == "unknown" {
            return String::new();
        }
        format!("{} (pid:{})", self.hostname, self.pid)
    }
}
