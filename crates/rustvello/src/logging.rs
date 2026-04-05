//! Logging initialization for rustvello.
//!
//! Provides [`init_logging`] to wire up `tracing-subscriber` with structured
//! span context (runner_id, app_id, invocation_id, task_id).
//!
//! Configuration types ([`LogConfig`], [`LogFormat`], [`LogStream`]) live in
//! `rustvello-core` and are re-exported here for convenience.
//!
//! Mirrors pynenc's `create_logger()` from `pynenc/util/log.py`.
//!
//! ## Text format
//!
//! ```text
//! 2026-03-27T10:23:45.123Z INFO  [R] my_app [PTR(a86ab1f8).W(d1241003)cc6c0e34-xxxx-...:core_tasks.recover] rustvello::runner Invocation completed
//! ```
//!
//! - Timestamp: UTC ISO 8601 with milliseconds
//! - Level: 5-char padded, colored per severity
//! - System tag: `[R]` (orange) / `[P]` (blue) — compact, aligned
//! - App ID: plain text, identifies the application
//! - Bracket context: `CLS(rid).W(wid)inv_id:task_key`
//!   - CLS from runner span's `cls` field (PTR, PITR, RR, …)
//!   - Runner/worker IDs truncated to 8 chars
//!   - Invocation IDs are full UUIDs (never truncated)
//! - Target: Rust module path or Python logger name
//! - Message: the log message

use std::fmt::Write as _;
use std::io;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::fmt as fmt_subscriber;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

pub use rustvello_core::logging::{LogConfig, LogFormat, LogStream};

/// ANSI color codes matching pynenc's Colors class.
mod colors {
    pub const RESET: &str = "\x1b[0m";
    pub const GREEN: &str = "\x1b[32m";
    pub const CYAN: &str = "\x1b[36m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const RED: &str = "\x1b[31m";
    pub const _WHITE_RED_BG: &str = "\x1b[37m\x1b[41m";
    pub const BLUE: &str = "\x1b[34m";
    pub const DIM: &str = "\x1b[2m";
    /// Rust orange (256-color).
    pub const ORANGE: &str = "\x1b[38;5;208m";
}

/// Truncate length for IDs in compact mode (matching pynenc's 8-char truncation).
const TRUNCATE_LENGTH: usize = 8;

/// Truncate an ID to `TRUNCATE_LENGTH` chars when compact mode is enabled.
///
/// Uses `char_indices` to find a valid UTF-8 boundary instead of byte-indexing.
fn maybe_truncate(value: &str, compact: bool) -> &str {
    if compact {
        if let Some((idx, _)) = value.char_indices().nth(TRUNCATE_LENGTH) {
            &value[..idx]
        } else {
            value
        }
    } else {
        value
    }
}

/// Return the ANSI color code for a log level.
fn level_color(level: &Level) -> &'static str {
    match *level {
        Level::ERROR => colors::RED,
        Level::WARN => colors::YELLOW,
        Level::INFO => colors::GREEN,
        Level::DEBUG => colors::CYAN,
        Level::TRACE => colors::DIM,
    }
}

// ── Custom event formatter ────────────────────────────────────────────────────

/// Rustvello log formatter producing unified pynenc-compatible output.
///
/// Format: `TIMESTAMP LEVEL [R] app_id [CLS(id).W(wid)inv_uuid:task.key] target message`
///
/// Bracket context is built from tracing spans (outer to inner):
/// - `runner` span → `CLS(<id>)` and `app_id`
/// - `worker` span → `.W(<id>)`
/// - `invocation` span → `<full_inv_id>:<task_key>`
///
/// Runner/worker IDs are truncated to 8 chars; invocation IDs are full UUIDs.
struct RustvelloFormatter {
    use_colors: bool,
    compact_context: bool,
}

impl<S, N> FormatEvent<S, N> for RustvelloFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let level = *event.metadata().level();
        let color = if self.use_colors {
            level_color(&level)
        } else {
            ""
        };
        let reset = if self.use_colors { colors::RESET } else { "" };
        let dim = if self.use_colors { colors::DIM } else { "" };
        let blue = if self.use_colors { colors::BLUE } else { "" };
        let orange = if self.use_colors { colors::ORANGE } else { "" };

        // ── Timestamp (UTC ISO 8601) ──────────────────────────────
        let now = chrono::Utc::now();
        let ts = now.format("%Y-%m-%dT%H:%M:%S%.3fZ");
        write!(writer, "{dim}{ts}{reset} ")?;

        // ── Level (5-char padded) ─────────────────────────────────
        let level_str = format!("{:<5}", level);
        write!(writer, "{color}{level_str}{reset} ")?;

        // ── System tag [R] (orange, aligned with [P]) ────────────
        write!(writer, "{orange}[R]{reset} ")?;

        // ── App ID ────────────────────────────────────────────────
        let (app_id, context) = self.build_bracket_context(ctx);
        if !app_id.is_empty() {
            write!(writer, "{blue}{app_id}{reset} ")?;
        }

        // ── Bracket context ───────────────────────────────────────
        if !context.is_empty() {
            write!(writer, "{dim}[{reset}{context}{dim}]{reset} ")?;
        }

        // ── Target ────────────────────────────────────────────────
        let target = event.metadata().target();
        write!(writer, "{dim}{target}{reset} ")?;

        // ── Message ───────────────────────────────────────────────
        write!(writer, "{color}")?;
        ctx.format_fields(writer.by_ref(), event)?;
        write!(writer, "{reset}")?;

        writeln!(writer)
    }
}

impl RustvelloFormatter {
    /// Build bracket context and extract app_id from the current span chain.
    ///
    /// Returns `(app_id, bracket_context)`.
    fn build_bracket_context<S, N>(&self, ctx: &FmtContext<'_, S, N>) -> (String, String)
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        let sc = extract_span_context(ctx);
        let bracket = format_bracket(&sc, self.compact_context);
        (sc.app_id, bracket)
    }
}

/// Strip ANSI escape sequences from a string.
///
/// `DefaultFields` may add ANSI codes (italic for names, dim for `=`) when
/// the subscriber has colors enabled.  We need plain text for field parsing.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_esc = false;
    for ch in s.chars() {
        if in_esc {
            // ANSI escape: ESC [ ... letter
            if ch.is_ascii_alphabetic() {
                in_esc = false;
            }
        } else if ch == '\x1b' {
            in_esc = true;
        } else {
            out.push(ch);
        }
    }
    out
}

/// Parse `key=value` pairs from a span's formatted fields string.
///
/// Strips surrounding double quotes from values (DefaultFields uses Debug
/// format for string literals, producing `cls="PTR"` instead of `cls=PTR`).
fn parse_span_fields(fields_str: &str) -> std::collections::HashMap<&str, &str> {
    let mut map = std::collections::HashMap::new();
    for part in fields_str.split_whitespace() {
        if let Some(eq_idx) = part.find('=') {
            let key = &part[..eq_idx];
            let value = part[eq_idx + 1..].trim_matches('"');
            map.insert(key, value);
        }
    }
    map
}

// ── Shared span context extraction ────────────────────────────────────────────

/// Raw (untruncated) context fields extracted from tracing spans.
#[derive(Debug, Default)]
struct SpanContext {
    app_id: String,
    runner_class: String,
    runner_id: String,
    worker_id: String,
    invocation_id: String,
    task_id: String,
}

/// Extract structured context from the current span chain.
///
/// Iterates outer→inner via `from_root()` collecting raw field values.
fn extract_span_context<S, N>(ctx: &FmtContext<'_, S, N>) -> SpanContext
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    let mut sc = SpanContext::default();

    if let Some(scope) = ctx.event_scope() {
        for span in scope.from_root() {
            let name = span.name();
            let exts = span.extensions();

            if let Some(fields) = exts.get::<fmt::FormattedFields<N>>() {
                let raw = fields.to_string();
                let fields_str = strip_ansi(&raw);
                let field_map = parse_span_fields(&fields_str);

                match name {
                    "runner" => {
                        sc.runner_class = field_map.get("cls").unwrap_or(&"R").to_string();
                        if let Some(rid) = field_map.get("runner_id") {
                            sc.runner_id = rid.to_string();
                        }
                        if let Some(aid) = field_map.get("app_id") {
                            sc.app_id = aid.to_string();
                        }
                    }
                    "worker" => {
                        if let Some(wid) = field_map.get("worker_id") {
                            sc.worker_id = wid.to_string();
                        }
                    }
                    "invocation" => {
                        if let Some(inv_id) = field_map.get("invocation_id") {
                            sc.invocation_id = inv_id.to_string();
                        }
                        if let Some(tid) = field_map.get("task_id") {
                            sc.task_id = tid.to_string();
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    sc
}

/// Build the bracket context string from a [`SpanContext`].
///
/// Returns `CLS(runner_id).W(worker_id)inv_uuid:task_key` with
/// runner/worker IDs truncated when `compact` is true.
fn format_bracket(sc: &SpanContext, compact: bool) -> String {
    let mut bracket = String::new();
    if !sc.runner_id.is_empty() {
        let truncated = maybe_truncate(&sc.runner_id, compact);
        write!(bracket, "{}({truncated})", sc.runner_class).ok();
    }
    if !sc.worker_id.is_empty() {
        let truncated = maybe_truncate(&sc.worker_id, compact);
        write!(bracket, ".W({truncated})").ok();
    }
    if !sc.invocation_id.is_empty() {
        bracket.push_str(&sc.invocation_id);
        if !sc.task_id.is_empty() {
            bracket.push(':');
            bracket.push_str(&sc.task_id);
        }
    }
    bracket
}

// ── JSON formatter ────────────────────────────────────────────────────────────

/// Visitor that captures the first `message` field from a tracing event.
struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            write!(self.0, "{value:?}").ok();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        }
    }
}

/// Rustvello JSON formatter producing NDJSON compatible with Python's `JsonFormatter`.
///
/// Schema (matching `pynenc/util/log.py` `JsonFormatter`):
/// ```json
/// {
///   "timestamp": "2025-01-15T10:23:45.123Z",
///   "severity": "INFO",
///   "system": "rust",
///   "logger": "rustvello::runner",
///   "message": "status transition",
///   "runner_class": "PTR",
///   "runner_id": "full-uuid",
///   "invocation_id": "full-uuid",
///   "task_id": "task.key",
///   "text": "…human-readable line…"
/// }
/// ```
struct RustvelloJsonFormatter {
    compact_context: bool,
}

impl<S, N> FormatEvent<S, N> for RustvelloJsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let level = *event.metadata().level();
        let target = event.metadata().target();

        // Capture the message from the event fields.
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        let message = visitor.0;

        // Timestamp
        let now = chrono::Utc::now();
        let ts = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

        // Severity — uppercase, matching Python's record.levelname
        let severity = match level {
            Level::ERROR => "ERROR",
            Level::WARN => "WARNING",
            Level::INFO => "INFO",
            Level::DEBUG => "DEBUG",
            Level::TRACE => "TRACE",
        };

        // Span context
        let sc = extract_span_context(ctx);
        let bracket = format_bracket(&sc, self.compact_context);

        // Human-readable text line (matching text format, no ANSI)
        let level_display = format!("{:<5}", level);
        let app_part = if sc.app_id.is_empty() {
            String::new()
        } else {
            format!(" {}", sc.app_id)
        };
        let bracket_part = if bracket.is_empty() {
            String::new()
        } else {
            format!(" [{bracket}]")
        };
        let text = format!("{ts} {level_display} [R]{app_part}{bracket_part} {target} {message}");

        // Build JSON object — include context fields only when present
        let mut obj = serde_json::Map::new();
        obj.insert("timestamp".into(), serde_json::Value::String(ts));
        obj.insert(
            "severity".into(),
            serde_json::Value::String(severity.into()),
        );
        obj.insert("system".into(), serde_json::Value::String("rust".into()));
        obj.insert("logger".into(), serde_json::Value::String(target.into()));
        obj.insert("message".into(), serde_json::Value::String(message));
        if !sc.runner_class.is_empty() {
            obj.insert(
                "runner_class".into(),
                serde_json::Value::String(sc.runner_class),
            );
        }
        if !sc.runner_id.is_empty() {
            obj.insert("runner_id".into(), serde_json::Value::String(sc.runner_id));
        }
        if !sc.worker_id.is_empty() {
            obj.insert("worker_id".into(), serde_json::Value::String(sc.worker_id));
        }
        if !sc.invocation_id.is_empty() {
            obj.insert(
                "invocation_id".into(),
                serde_json::Value::String(sc.invocation_id),
            );
        }
        if !sc.task_id.is_empty() {
            obj.insert("task_id".into(), serde_json::Value::String(sc.task_id));
        }
        obj.insert("text".into(), serde_json::Value::String(text));

        let json = serde_json::Value::Object(obj);
        // serde_json::to_string won't fail for simple string maps
        write!(
            writer,
            "{}",
            serde_json::to_string(&json).unwrap_or_default()
        )?;
        writeln!(writer)
    }
}

/// Initialize the global tracing subscriber based on [`LogConfig`].
///
/// The `RUST_LOG` env var takes precedence over [`LogConfig::level`] when set.
/// Call this once at program startup (before spawning any async tasks).
pub fn init_logging(config: &LogConfig) {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let use_colors = config.use_colors.unwrap_or_else(|| {
        // Auto-detect: colors if the output stream is a TTY
        use std::io::IsTerminal;
        match config.stream {
            LogStream::Stderr => io::stderr().is_terminal(),
            LogStream::Stdout => io::stdout().is_terminal(),
            _ => false,
        }
    });

    match config.format {
        LogFormat::Json => {
            let formatter = RustvelloJsonFormatter {
                compact_context: config.compact_context,
            };

            match config.stream {
                LogStream::Stdout => {
                    let _ = fmt_subscriber()
                        .with_writer(io::stdout)
                        .with_env_filter(env_filter)
                        .with_target(false)
                        .event_format(formatter)
                        .try_init();
                }
                LogStream::Stderr => {
                    let _ = fmt_subscriber()
                        .with_env_filter(env_filter)
                        .with_target(false)
                        .event_format(formatter)
                        .try_init();
                }
                _ => {}
            }
        }
        LogFormat::Text => {
            let formatter = RustvelloFormatter {
                use_colors,
                compact_context: config.compact_context,
            };

            match config.stream {
                LogStream::Stdout => {
                    let _ = fmt_subscriber()
                        .with_writer(io::stdout)
                        .with_env_filter(env_filter)
                        .with_target(false)
                        .event_format(formatter)
                        .try_init();
                }
                LogStream::Stderr => {
                    let _ = fmt_subscriber()
                        .with_env_filter(env_filter)
                        .with_target(false)
                        .event_format(formatter)
                        .try_init();
                }
                _ => {}
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn captured_string(buf: &BufWriter) -> String {
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn test_bracket_context_with_spans() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloFormatter {
                use_colors: false,
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runner_span = tracing::info_span!(
            "runner",
            runner_id = %"aaaa1111-2222-3333-4444-555566667777",
            cls = "PTR",
            app_id = "my_app",
        );
        let _r = runner_span.enter();

        let worker_span = tracing::info_span!(
            "worker",
            worker_id = %"wwww1111-2222-3333-4444-555566667777",
        );
        let _w = worker_span.enter();

        let inv_span = tracing::info_span!(
            "invocation",
            invocation_id = %"bbbb1111-2222-3333-4444-555566667777",
            task_id = tracing::field::Empty,
        );
        let _i = inv_span.enter();

        // Record task_id after span creation (mirrors real runner code)
        tracing::Span::current().record("task_id", tracing::field::display("test.my_task"));

        tracing::info!("Hello from test");

        let output = captured_string(&buf);
        eprintln!("CAPTURED: {output}");

        // System tag — language indicator [R] for Rust
        assert!(
            output.contains("[R]"),
            "Should contain [R] language tag. Got: {output}"
        );
        // Runner class + truncated ID (outer → inner ordering)
        assert!(
            output.contains("PTR(aaaa1111)"),
            "Should contain runner CLS(truncated_id). Got: {output}"
        );
        // App ID (standalone, before bracket)
        assert!(
            output.contains("my_app"),
            "Should contain app_id. Got: {output}"
        );
        // Worker
        assert!(
            output.contains(".W(wwww1111)"),
            "Should contain worker. Got: {output}"
        );
        // Invocation + task (full UUID)
        assert!(
            output.contains("bbbb1111-2222-3333-4444-555566667777:test.my_task"),
            "Should contain full inv_uuid:task_key. Got: {output}"
        );
        // Full bracket in correct order (app_id NOT in bracket)
        assert!(
            output.contains(
                "[PTR(aaaa1111).W(wwww1111)bbbb1111-2222-3333-4444-555566667777:test.my_task]"
            ),
            "Should contain full bracket context. Got: {output}"
        );
        assert!(
            output.contains("Hello from test"),
            "Should contain message. Got: {output}"
        );
    }

    #[test]
    fn test_format_no_spans() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloFormatter {
                use_colors: false,
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::info!("No context here");

        let output = captured_string(&buf);
        assert!(output.contains("[R]"), "Should contain [R] tag");
        assert!(output.contains("No context here"), "Should contain message");
        // No bracket context when no spans
        assert!(!output.contains("[PTR"), "Should not have bracket context");
    }

    /// Reproduces the real runner pattern: nested async spans using
    /// `.instrument()` instead of `.enter()`, including `tokio::spawn`.
    ///
    /// Uses current-thread runtime because `set_default()` is thread-local;
    /// multi-thread would lose the subscriber on spawned worker threads.
    #[tokio::test]
    async fn test_bracket_context_async_instrument() {
        use tracing::Instrument;

        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloFormatter {
                use_colors: false,
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runner_span = tracing::info_span!(
            "runner",
            runner_id = %"aaaa1111-2222-3333-4444-555566667777",
            cls = "PTR",
            app_id = "my_app",
        );

        // Mirrors real runner: run_impl().instrument(runner_span)
        // Inside run_impl, worker_span is created (parent = runner)
        // Worker is spawned with instrument(worker_span)
        async {
            let worker_span = tracing::info_span!(
                "worker",
                worker_id = %"wwww1111-2222-3333-4444-555566667777",
            );

            // Mirrors: worker_handles.spawn(worker_loop().instrument(worker_span))
            let handle = tokio::spawn(
                async {
                    let inv_span = tracing::info_span!(
                        "invocation",
                        invocation_id = %"bbbb1111-2222-3333-4444-555566667777",
                        task_id = tracing::field::Empty,
                    );

                    async {
                        tracing::Span::current()
                            .record("task_id", tracing::field::display("test.my_task"));
                        tracing::info!("Invocation completed status:success");
                    }
                    .instrument(inv_span)
                    .await;
                }
                .instrument(worker_span),
            );

            handle.await.unwrap();
        }
        .instrument(runner_span)
        .await;

        let output = captured_string(&buf);
        eprintln!("ASYNC CAPTURED: {output}");

        // Language tag
        assert!(
            output.contains("[R]"),
            "Should contain [R] language tag. Got: {output}"
        );
        // Runner
        assert!(
            output.contains("PTR(aaaa1111)"),
            "Should contain runner context. Got: {output}"
        );
        // App ID (standalone, before bracket)
        assert!(
            output.contains("my_app"),
            "Should contain app_id. Got: {output}"
        );
        // Worker
        assert!(
            output.contains(".W(wwww1111)"),
            "Should contain worker context. Got: {output}"
        );
        // Invocation + task (full UUID)
        assert!(
            output.contains("bbbb1111-2222-3333-4444-555566667777:test.my_task"),
            "Should contain invocation context. Got: {output}"
        );
        // Full bracket (app_id NOT in bracket)
        assert!(
            output.contains(
                "[PTR(aaaa1111).W(wwww1111)bbbb1111-2222-3333-4444-555566667777:test.my_task]"
            ),
            "Should contain full bracket context. Got: {output}"
        );
    }

    /// Regression test: `DefaultFields` adds ANSI escape codes to span field
    /// names when the subscriber has ANSI enabled (TTY auto-detect).  Without
    /// `strip_ansi()`, `parse_span_fields` cannot match keys like `runner_id`
    /// because the actual stored key is `\x1b[3mrunner_id\x1b[0m\x1b[2m`.
    ///
    /// This test enables ANSI on the subscriber (`.with_ansi(true)`) while
    /// keeping the formatter output plain-text (`use_colors: false`) so that
    /// assertions remain simple. It verifies all context fields:
    ///   - Language tag `[R]`
    ///   - `runner_id` (via `runner` span)
    ///   - `app_id` (via `runner` span)
    ///   - `worker_id` (via `worker` span)
    ///   - `invocation_id` (via `invocation` span)
    ///   - `task_id` (via `invocation` span, recorded after creation)
    #[test]
    fn test_bracket_context_with_ansi_enabled() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        // Key: `.with_ansi(true)` makes DefaultFields ANSI-encode span field
        // names/values — this is the exact condition that caused the bug.
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(true)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloFormatter {
                use_colors: false,
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runner_span = tracing::info_span!(
            "runner",
            runner_id = %"aaaa1111-2222-3333-4444-555566667777",
            cls = "PTR",
            app_id = "my_app",
        );
        let _r = runner_span.enter();

        let worker_span = tracing::info_span!(
            "worker",
            worker_id = %"wwww1111-2222-3333-4444-555566667777",
        );
        let _w = worker_span.enter();

        let inv_span = tracing::info_span!(
            "invocation",
            invocation_id = %"bbbb1111-2222-3333-4444-555566667777",
            task_id = tracing::field::Empty,
        );
        let _i = inv_span.enter();

        tracing::Span::current().record("task_id", tracing::field::display("test.my_task"));

        tracing::info!("ANSI regression test");

        let output = captured_string(&buf);
        eprintln!("ANSI CAPTURED: {output}");

        // Language tag
        assert!(
            output.contains("[R]"),
            "Language tag [R] missing. Got: {output}"
        );
        // Runner ID (proves strip_ansi works on runner_id key)
        assert!(
            output.contains("PTR(aaaa1111)"),
            "runner_id missing — strip_ansi may have regressed. Got: {output}"
        );
        // App ID (proves strip_ansi works on app_id key — standalone)
        assert!(
            output.contains("my_app"),
            "app_id missing — strip_ansi may have regressed. Got: {output}"
        );
        // Worker ID
        assert!(
            output.contains(".W(wwww1111)"),
            "worker_id missing — strip_ansi may have regressed. Got: {output}"
        );
        // Invocation ID + task_id (full UUID)
        assert!(
            output.contains("bbbb1111-2222-3333-4444-555566667777:test.my_task"),
            "invocation_id or task_id missing. Got: {output}"
        );
        // Full bracket context — all fields present and correctly ordered
        assert!(
            output.contains(
                "[PTR(aaaa1111).W(wwww1111)bbbb1111-2222-3333-4444-555566667777:test.my_task]"
            ),
            "Full bracket context missing or malformed. Got: {output}"
        );
    }

    #[test]
    fn test_strip_ansi() {
        // Typical DefaultFields output with ANSI:
        // \x1b[3mrunner_id\x1b[0m\x1b[2m=\x1b[0maaaa1111
        let ansi_encoded =
            "\x1b[3mrunner_id\x1b[0m\x1b[2m=\x1b[0maaaa1111 \x1b[3mcls\x1b[0m\x1b[2m=\x1b[0m\"PTR\"";
        let stripped = strip_ansi(ansi_encoded);
        assert_eq!(stripped, "runner_id=aaaa1111 cls=\"PTR\"");

        // Already plain text — should pass through unchanged
        let plain = "runner_id=aaaa1111 cls=\"PTR\"";
        assert_eq!(strip_ansi(plain), plain);

        // Empty string
        assert_eq!(strip_ansi(""), "");
    }

    // ── JSON formatter tests ──────────────────────────────────────────────

    #[test]
    fn test_json_formatter_produces_valid_ndjson() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloJsonFormatter {
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::info!("Simple JSON test");

        let output = captured_string(&buf);
        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("Should be valid JSON");
        let obj = parsed.as_object().expect("Should be a JSON object");

        assert_eq!(obj["severity"], "INFO");
        assert_eq!(obj["system"], "rust");
        assert_eq!(obj["message"], "Simple JSON test");
        assert!(obj.contains_key("timestamp"));
        assert!(obj.contains_key("logger"));
        assert!(obj.contains_key("text"));
        assert!(obj["text"].as_str().unwrap().contains("[R]"));
        // No context fields when no spans
        assert!(!obj.contains_key("runner_class"));
        assert!(!obj.contains_key("invocation_id"));
    }

    #[test]
    fn test_json_formatter_includes_span_context() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("info"))
            .with_target(false)
            .event_format(RustvelloJsonFormatter {
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let runner_span = tracing::info_span!(
            "runner",
            runner_id = %"aaaa1111-2222-3333-4444-555566667777",
            cls = "PTR",
            app_id = "my_app",
        );
        let _r = runner_span.enter();

        let worker_span = tracing::info_span!(
            "worker",
            worker_id = %"wwww1111-2222-3333-4444-555566667777",
        );
        let _w = worker_span.enter();

        let inv_span = tracing::info_span!(
            "invocation",
            invocation_id = %"bbbb1111-2222-3333-4444-555566667777",
            task_id = tracing::field::Empty,
        );
        let _i = inv_span.enter();

        tracing::Span::current().record("task_id", tracing::field::display("test.my_task"));

        tracing::info!("Context JSON test");

        let output = captured_string(&buf);
        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("Should be valid JSON");
        let obj = parsed.as_object().expect("Should be a JSON object");

        // Structured context fields (full, untruncated IDs)
        assert_eq!(obj["runner_class"], "PTR");
        assert_eq!(obj["runner_id"], "aaaa1111-2222-3333-4444-555566667777");
        assert_eq!(obj["worker_id"], "wwww1111-2222-3333-4444-555566667777");
        assert_eq!(obj["invocation_id"], "bbbb1111-2222-3333-4444-555566667777");
        assert_eq!(obj["task_id"], "test.my_task");
        assert_eq!(obj["severity"], "INFO");
        assert_eq!(obj["system"], "rust");

        // text field has bracket context with truncated IDs
        let text = obj["text"].as_str().unwrap();
        assert!(text.contains("[R]"), "text should contain [R]. Got: {text}");
        assert!(
            text.contains("my_app"),
            "text should contain app_id. Got: {text}"
        );
        assert!(
            text.contains("PTR(aaaa1111)"),
            "text should contain truncated runner. Got: {text}"
        );
        assert!(
            text.contains(".W(wwww1111)"),
            "text should contain truncated worker. Got: {text}"
        );
        assert!(
            text.contains("Context JSON test"),
            "text should contain message. Got: {text}"
        );
    }

    #[test]
    fn test_json_formatter_severity_mapping() {
        let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
        let subscriber = tracing_subscriber::fmt::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_env_filter(EnvFilter::new("warn"))
            .with_target(false)
            .event_format(RustvelloJsonFormatter {
                compact_context: true,
            })
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        tracing::warn!("Warning test");

        let output = captured_string(&buf);
        let parsed: serde_json::Value =
            serde_json::from_str(output.trim()).expect("Should be valid JSON");
        // tracing WARN maps to Python's WARNING
        assert_eq!(parsed["severity"], "WARNING");
    }
}
