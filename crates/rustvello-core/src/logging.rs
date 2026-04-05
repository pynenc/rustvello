//! Logging configuration types for rustvello.
//!
//! Provides [`LogConfig`] for controlling log format, level, and output.
//! The actual subscriber initialization (`init_logging`) lives in
//! the `rustvello` application crate (`rustvello::logging`).

use serde::{Deserialize, Serialize};

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LogFormat {
    /// Human-readable text with optional ANSI colors.
    Text,
    /// Structured JSON (one object per line).
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Text
    }
}

/// Output stream selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LogStream {
    Stderr,
    Stdout,
}

impl Default for LogStream {
    fn default() -> Self {
        Self::Stderr
    }
}

/// Configuration for logging output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LogConfig {
    /// Minimum log level (trace, debug, info, warn, error).
    pub level: String,
    /// Output format.
    pub format: LogFormat,
    /// Whether to use ANSI colors in text format (None = auto-detect TTY).
    pub use_colors: Option<bool>,
    /// Whether to show compact context (abbreviated class names, truncated IDs).
    pub compact_context: bool,
    /// Output stream: stderr (default) or stdout.
    pub stream: LogStream,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Text,
            use_colors: None,
            compact_context: true,
            stream: LogStream::Stderr,
        }
    }
}

impl LogConfig {
    /// Build a [`LogConfig`] from an [`AppConfig`]-style set of fields.
    pub fn from_app_fields(
        level: &str,
        format: LogFormat,
        use_colors: Option<bool>,
        compact_context: bool,
    ) -> Self {
        Self {
            level: level.to_owned(),
            format,
            use_colors,
            compact_context,
            stream: LogStream::Stderr,
        }
    }
}
