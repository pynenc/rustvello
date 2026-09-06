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
    /// Runtime language this runner executes.
    pub runner_language: String,
    /// Local executor family used by the runner.
    pub executor_kind: String,
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
            runner_language: "unknown".to_owned(),
            executor_kind: "unknown".to_owned(),
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
            runner_language: ctx.runner_language.to_string(),
            executor_kind: ctx.executor_kind.to_string(),
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
            runner_language: "external".to_owned(),
            executor_kind: "external".to_owned(),
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
        if self.runner_language == "unknown" {
            format!("{}({})", self.runner_cls, short)
        } else {
            format!("{}: {}({})", self.runner_language, self.runner_cls, short)
        }
    }

    /// Display label without the language prefix, for views that render the
    /// language as a separate badge.
    pub fn name_label(&self) -> String {
        let short = crate::util::formatting::truncate_id(&self.runner_id);
        format!("{}({})", self.runner_cls, short)
    }

    /// Secondary lane label with location and local executor metadata.
    pub fn details(&self) -> String {
        let mut parts = Vec::new();
        if !self.hostname.is_empty() && self.hostname != "unknown" {
            parts.push(format!("{} (pid:{})", self.hostname, self.pid));
        }
        if self.executor_kind != "unknown" && self.executor_kind != "external" {
            parts.push(self.executor_kind.clone());
        }
        parts.join(" | ")
    }
}
