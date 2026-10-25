//! PyO3 wrapper for Rust logging initialization.
//!
//! Exposes [`init_logging`] so that Python (pynenc) can configure the Rust
//! tracing subscriber with the same level and format used on the Python side.

use pyo3::prelude::*;

use rustvello::logging::{LogConfig, LogFormat, LogStream};

/// Initialise the Rust tracing subscriber.
///
/// Call this **once** during app startup. Subsequent calls are harmless no-ops
/// (the tracing global subscriber can only be set once).
///
/// :param str level: Minimum log level — "trace", "debug", "info", "warn", or "error".
/// :param str format: Output format — "text" (default) or "json".
/// :param bool | None use_colors: Override color detection. ``None`` = auto-detect TTY.
/// :param bool compact_context: Abbreviate class names and truncate IDs to 8 chars.
/// :param str stream: Output stream — "stderr" (default) or "stdout".
#[pyfunction]
#[pyo3(signature = (level="info", format="text", use_colors=None, compact_context=true, stream="stderr"))]
pub fn init_logging(
    level: &str,
    format: &str,
    use_colors: Option<bool>,
    compact_context: bool,
    stream: &str,
) -> PyResult<()> {
    let fmt = match format.to_lowercase().as_str() {
        "json" => LogFormat::Json,
        _ => LogFormat::Text,
    };
    let log_stream = match stream.to_lowercase().as_str() {
        "stdout" => LogStream::Stdout,
        _ => LogStream::Stderr,
    };
    let mut config = LogConfig::default();
    config.level = level.to_owned();
    config.format = fmt;
    config.use_colors = use_colors;
    config.compact_context = compact_context;
    config.stream = log_stream;
    rustvello::logging::init_logging(&config);
    Ok(())
}

/// Start the Rust logger from an `AppConfig` (`logging_level`, `log_format`,
/// `compact_log_context`, so `RUSTVELLO__LOGGING_LEVEL` and friends apply).
///
/// `App.run()` and `App.start_monitor()` call this, so runner warnings and
/// errors reach the logs without an explicit `init_logging()`. The first
/// initialisation wins: an earlier `init_logging()` call keeps its settings,
/// and `RUST_LOG` still overrides the level.
#[pyfunction]
pub fn init_logging_from_config(config: &crate::config::PyAppConfig) -> PyResult<()> {
    let app = &config.inner;
    let mut log_config = LogConfig::default();
    log_config.level = app.logging_level.clone();
    log_config.format = match app.log_format {
        rustvello_proto::config::LogFormat::Json => LogFormat::Json,
        _ => LogFormat::Text,
    };
    log_config.compact_context = app.compact_log_context;
    // stderr, as init_logging(): stdout belongs to the program's own output.
    log_config.stream = LogStream::Stderr;
    rustvello::logging::init_logging(&log_config);
    Ok(())
}
