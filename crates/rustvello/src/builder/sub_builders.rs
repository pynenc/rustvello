//! Scoped sub-builders for grouped configuration.
//!
//! These provide an ergonomic, discoverable API for configuring related
//! settings. Each sub-builder borrows `RustvelloBuilder`, exposes group-
//! specific setters, and returns the parent builder via `.done()`.

use cistell_core::ConfigValue;
use rustvello_proto::config::{ArgumentPrintMode, LogFormat};

use super::{argument_print_mode_str, log_format_str, RustvelloBuilder};

// ===========================================================================
// LoggingBuilder
// ===========================================================================

/// Sub-builder for logging and display settings.
pub struct LoggingBuilder {
    parent: RustvelloBuilder,
}

impl LoggingBuilder {
    pub(super) fn new(parent: RustvelloBuilder) -> Self {
        Self { parent }
    }

    /// Set the logging level (trace, debug, info, warn, error).
    pub fn level(mut self, level: impl Into<String>) -> Self {
        self.parent
            .programmatic
            .insert("logging_level", ConfigValue::String(level.into()));
        self
    }

    /// Set the log output format (Text or Json).
    pub fn format(mut self, format: LogFormat) -> Self {
        let s = log_format_str(format);
        self.parent
            .programmatic
            .insert("log_format", ConfigValue::String(s.to_string()));
        self
    }

    /// Set whether to use ANSI colors (None = auto-detect TTY).
    pub fn use_colors(mut self, use_colors: Option<bool>) -> Self {
        if let Some(b) = use_colors {
            self.parent
                .programmatic
                .insert("log_use_colors", ConfigValue::Bool(b));
        }
        self
    }

    /// Set compact context mode in logs.
    pub fn compact_context(mut self, compact: bool) -> Self {
        self.parent
            .programmatic
            .insert("compact_log_context", ConfigValue::Bool(compact));
        self
    }

    /// Set the argument print mode (Full, Keys, Truncated, Hidden).
    pub fn argument_print_mode(mut self, mode: ArgumentPrintMode) -> Self {
        let s = argument_print_mode_str(mode);
        self.parent
            .programmatic
            .insert("argument_print_mode", ConfigValue::String(s.to_string()));
        self.parent
            .programmatic
            .insert("print_arguments", ConfigValue::Bool(true));
        self
    }

    /// Hide all argument values in logs and display.
    pub fn hide_arguments(mut self) -> Self {
        self.parent
            .programmatic
            .insert("print_arguments", ConfigValue::Bool(false));
        self
    }

    /// Set the maximum length for truncated argument display.
    pub fn truncate_arguments_length(mut self, length: usize) -> Self {
        self.parent.programmatic.insert(
            "truncate_arguments_length",
            ConfigValue::Integer(length as i64),
        );
        self
    }

    /// Finish logging configuration, returning to the parent builder.
    pub fn done(self) -> RustvelloBuilder {
        self.parent
    }
}

// ===========================================================================
// PerformanceBuilder
// ===========================================================================

/// Sub-builder for performance and scheduling settings.
pub struct PerformanceBuilder {
    parent: RustvelloBuilder,
}

impl PerformanceBuilder {
    pub(super) fn new(parent: RustvelloBuilder) -> Self {
        Self { parent }
    }

    /// Set the TTL for cached invocation status checks (seconds, 0 = no cache).
    pub fn cached_status_time(mut self, seconds: f64) -> Self {
        self.parent
            .programmatic
            .insert("cached_status_time_seconds", ConfigValue::Float(seconds));
        self
    }

    /// Set hours after which final invocations can be auto-purged (0 = disabled).
    pub fn auto_final_invocation_purge_hours(mut self, hours: f64) -> Self {
        self.parent.programmatic.insert(
            "auto_final_invocation_purge_hours",
            ConfigValue::Float(hours),
        );
        self
    }

    /// Set the scheduler evaluation interval in seconds.
    pub fn scheduler_interval(mut self, seconds: u64) -> Self {
        self.parent.programmatic.insert(
            "scheduler_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set whether the trigger scheduler is enabled.
    pub fn enable_scheduler(mut self, enabled: bool) -> Self {
        self.parent
            .programmatic
            .insert("enable_scheduler", ConfigValue::Bool(enabled));
        self
    }

    /// Finish performance configuration, returning to the parent builder.
    pub fn done(self) -> RustvelloBuilder {
        self.parent
    }
}

// ===========================================================================
// ReliabilityBuilder
// ===========================================================================

/// Sub-builder for reliability, recovery, and heartbeat settings.
pub struct ReliabilityBuilder {
    parent: RustvelloBuilder,
}

impl ReliabilityBuilder {
    pub(super) fn new(parent: RustvelloBuilder) -> Self {
        Self { parent }
    }

    /// Set the maximum pending time for invocations (in seconds).
    pub fn max_pending_seconds(mut self, seconds: u64) -> Self {
        self.parent
            .programmatic
            .insert("max_pending_seconds", ConfigValue::Integer(seconds as i64));
        self
    }

    /// Set the heartbeat interval for runners (in seconds).
    pub fn heartbeat_interval(mut self, seconds: u64) -> Self {
        self.parent.programmatic.insert(
            "heartbeat_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set how long (seconds) before a runner without heartbeats is dead.
    pub fn runner_dead_after(mut self, seconds: u64) -> Self {
        self.parent.programmatic.insert(
            "runner_dead_after_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set how often (seconds) to scan for stale invocations.
    pub fn recovery_check_interval(mut self, seconds: u64) -> Self {
        self.parent.programmatic.insert(
            "recovery_check_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set the cron expression for recovering pending invocations.
    pub fn recover_pending_cron(mut self, cron: impl Into<String>) -> Self {
        self.parent
            .programmatic
            .insert("recover_pending_cron", ConfigValue::String(cron.into()));
        self
    }

    /// Set the cron expression for recovering running invocations.
    pub fn recover_running_cron(mut self, cron: impl Into<String>) -> Self {
        self.parent
            .programmatic
            .insert("recover_running_cron", ConfigValue::String(cron.into()));
        self
    }

    /// Set whether the orchestrator uses blocking/waiting semantics.
    pub fn blocking_control(mut self, enabled: bool) -> Self {
        self.parent
            .programmatic
            .insert("blocking_control", ConfigValue::Bool(enabled));
        self
    }

    /// Finish reliability configuration, returning to the parent builder.
    pub fn done(self) -> RustvelloBuilder {
        self.parent
    }
}
