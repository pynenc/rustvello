//! Log line parsing and entity extraction for the Log Explorer.
//!
//! Parses structured log lines emitted by rustvello's logging layer and
//! extracts the runner context chain, invocation ID, task key, timestamp,
//! and structured entity references from the message body.
//!
//! Supports **three log formats**:
//!
//! 1. **tracing-subscriber default** (what `tracing` emits out of the box):
//!    ```text
//!    2026-03-21T15:39:09.107782Z  INFO runner{runner_id=UUID cls=PTR}:worker{worker_id=UUID}:invocation{invocation_id=UUID task_id=KEY}: target: message
//!    ```
//!
//! 2. **Unified format** (from `RustvelloFormatter` / pynenc `[P]`):
//!    ```text
//!    2026-03-27T10:23:45.123Z INFO  [R] my_app [PTR(a86ab1f8).W(d1241003)cc6c0e34-5678-9abc-def0-123456789abc:task.key] rustvello::runner message
//!    2026-03-27T10:23:45.123Z WARN  [P] my_app [TR(runner_1)inv_1234:my_task] pynenc.runner message
//!    ```
//!
//! 3. **Legacy custom bracket** (older format):
//!    ```text
//!    2026-03-05 00:13:51.125+01:00 INFO rustvello::app [R(a86ab1f8)UUID:task.key] message
//!    ```
//!
//! Supports multi-line input: [`parse_log_lines`] splits a block of text
//! into individual log entries (each starting with a timestamp + level) and
//! returns a [`ParsedLogLine`] for every entry.

use std::collections::HashSet;
use std::sync::OnceLock;

use regex::Regex;

// ── Regex helpers ─────────────────────────────────────────────────────────────

fn ansi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1b\[[0-9;]*m").expect("valid regex"))
}

fn uuid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
            .expect("valid regex")
    })
}

fn bracket_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\[([^\]]+)\]").expect("valid regex"))
}

fn runner_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"([A-Za-z]+)\(([^)]+)\)").expect("valid regex"))
}

fn timestamp_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)")
            .expect("valid regex")
    })
}

/// Matches the start of a new log entry: date + optional TZ + level keyword.
fn log_line_start_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?m)^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?\s+(?:TRACE|DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|CRIT)\s",
        )
        .expect("valid regex")
    })
}

/// Structured entity references: `key:value` where value is a UUID or dotted ID.
fn entity_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)(invocation|runner|worker|task|parent-invocation|child-invocation|new-invocation|current-owner-runner|attempted-owner-runner|workflow|sub-workflow|parent-workflow):([0-9a-zA-Z._-]+(?:-[0-9a-fA-F]{4}){0,4})",
        )
        .expect("valid regex")
    })
}

/// List form: `invocations:[id,id,...]`.
fn entity_list_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)(invocations|runners|workers|workflows):\[([^\]]*)\]")
            .expect("valid regex")
    })
}

/// Tracing span chain: `span_name{key=value ...}:span_name{key=value ...}: target: message`
fn tracing_span_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(\w+)\{([^}]*)\}").expect("valid regex"))
}

/// Custom bracket log line: `TIMESTAMP LEVEL TARGET [bracket] message`
fn bracket_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s+(TRACE|DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\s+(\S+)\s+(?:\[[^\]]*\]\s+)?(?:-\s+)?(.*)",
        )
        .expect("valid regex")
    })
}

/// Unified format: `TIMESTAMP LEVEL [R|P] APP_ID [bracket_ctx] target message`
///
/// When no bracket is present, the first word after the system tag is the
/// target (no app_id): `TIMESTAMP LEVEL [R|P] target message`
fn unified_line_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\s+(TRACE|DEBUG|INFO|WARN|ERROR|CRIT)\s+\[(R|P)\]\s+(\S+)\s+(?:\[([^\]]*)\]\s+)?(.*)",
        )
        .expect("valid regex")
    })
}

// ── data model ────────────────────────────────────────────────────────────────

/// One runner entry extracted from the log context chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRunner {
    /// Compact class abbreviation, e.g. `"R"` or `"runner"`.
    pub cls_abbr: String,
    /// Runner ID (full or truncated as it appeared in the log).
    pub partial_id: String,
}

/// A structured entity reference extracted from the log message body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityRef {
    /// Reference type — `"invocation"`, `"runner"`, `"worker"`, `"task"`, etc.
    pub kind: String,
    /// The entity ID or key.
    pub value: String,
}

/// Structured result of parsing a single log line.
#[derive(Debug, Clone, Default)]
pub struct ParsedLogLine {
    /// The original raw log line text.
    pub raw: String,
    /// Ordered runner chain (parent first).
    pub runners: Vec<ParsedRunner>,
    /// Worker ID from the worker span context.
    pub worker_id: Option<String>,
    /// Full UUID invocation ID from context.
    pub invocation_id: Option<String>,
    /// Task `module.func` key from context.
    pub task_key: Option<String>,
    /// Raw context string (bracket content or span chain).
    pub raw_bracket: Option<String>,
    /// Parsed timestamp string from the log line.
    pub timestamp: Option<String>,
    /// Log level (INFO, WARNING, ERROR, …).
    pub level: Option<String>,
    /// System origin tag: "RUST" or "PYTHON" (unified format only).
    pub system: Option<String>,
    /// Logger name / module / target.
    pub module: Option<String>,
    /// Message body after the header.
    pub message: String,
    /// Structured entity references from the message.
    pub entity_refs: Vec<EntityRef>,
    /// `true` when at least one component was extracted.
    pub is_valid: bool,
}

impl ParsedLogLine {
    /// Return the first 7 characters of `worker_id` for compact display.
    pub fn short_worker_id(&self) -> Option<String> {
        self.worker_id.as_ref().map(|w| short_id(w))
    }
}

// ── public API ────────────────────────────────────────────────────────────────

/// Parse a multi-line log text into individual parsed lines.
///
/// Each log entry starts with a timestamp and log level. Lines that do not
/// match a new entry are appended to the previous one (multi-line stack traces).
pub fn parse_log_lines(text: &str) -> Vec<ParsedLogLine> {
    let entries = split_log_entries(text);
    entries.iter().map(|e| parse_log_line(e)).collect()
}

/// Parse a single log line.
///
/// Strips ANSI escape codes before parsing. Handles JSON log lines by
/// extracting the `text` field. Supports both tracing span format and
/// custom bracket format.
pub fn parse_log_line(line: &str) -> ParsedLogLine {
    let raw = line.trim().to_owned();

    // Handle JSON log lines by extracting human-readable text field
    let clean = if let Some(json_text) = try_extract_json_text(&raw) {
        json_text
    } else {
        strip_ansi(&raw)
    };

    // Try unified format first (new [RUST]/[PYTHON] tag format)
    if let Some(parsed) = try_parse_unified_format(&clean, &raw) {
        return parsed;
    }

    // Try tracing-subscriber span format next (most common in real logs)
    if let Some(parsed) = try_parse_tracing_format(&clean, &raw) {
        return parsed;
    }

    // Fall back to custom bracket format
    parse_bracket_format(&clean, &raw)
}

// ── splitting helpers ─────────────────────────────────────────────────────────

fn split_log_entries(text: &str) -> Vec<String> {
    let clean = strip_ansi(text);
    let starts: Vec<usize> = log_line_start_re()
        .find_iter(&clean)
        .map(|m| m.start())
        .collect();

    if starts.is_empty() {
        return clean
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(std::borrow::ToOwned::to_owned)
            .collect();
    }

    starts
        .iter()
        .enumerate()
        .map(|(i, &start)| {
            let end = starts.get(i + 1).copied().unwrap_or(clean.len());
            clean[start..end].trim().to_owned()
        })
        .collect()
}

// ── unified format parsing ────────────────────────────────────────────────────

/// Try to parse a line in the unified format:
/// `TIMESTAMP LEVEL [R|P] APP_ID [bracket_ctx] target message`
///
/// When no bracket is present, the first word is the target (not app_id):
/// `TIMESTAMP LEVEL [R|P] target message`
fn try_parse_unified_format(clean: &str, raw: &str) -> Option<ParsedLogLine> {
    let caps = unified_line_re().captures(clean)?;

    let timestamp = Some(caps[1].to_owned());
    let level_str = &caps[2];
    let level = Some(match level_str {
        "WARN" => "WARNING".to_owned(),
        "CRIT" => "CRITICAL".to_owned(),
        _ => level_str.to_owned(),
    });
    let system_tag = &caps[3];
    let system = Some(match system_tag {
        "R" => "RUST".to_owned(),
        "P" => "PYTHON".to_owned(),
        _ => system_tag.to_owned(),
    });

    let first_word = caps[4].to_owned();
    let bracket_str = caps.get(5).map(|m| m.as_str());
    let rest = caps[6].to_owned();

    // When bracket is present: first_word = app_id, rest = "target message"
    // When no bracket: first_word = target/module, rest = message
    let (module, message) = if bracket_str.is_some() {
        // Split rest into target (first word) and message (remainder)
        if let Some(space_pos) = rest.find(' ') {
            let target = rest[..space_pos].to_owned();
            let msg = rest[space_pos + 1..].to_owned();
            (Some(target), msg)
        } else {
            // rest is just the target with no message
            (Some(rest), String::new())
        }
    } else {
        (Some(first_word.clone()), rest)
    };

    // Parse bracket context: CLS(id).W(wid)inv_id:task.key
    let mut runners = Vec::new();
    let mut worker_id = None;
    let mut invocation_id = None;
    let mut task_key = None;
    let mut raw_bracket = None;

    if let Some(br) = bracket_str {
        let br = br.trim();
        if !br.is_empty() {
            raw_bracket = Some(br.to_owned());

            // Extract runners and workers: CLS(id)
            for cap in runner_re().captures_iter(br) {
                let cls = cap[1].to_owned();
                let pid = cap[2].to_owned();
                if cls == "W" {
                    worker_id = Some(pid);
                } else {
                    runners.push(ParsedRunner {
                        cls_abbr: cls,
                        partial_id: pid,
                    });
                }
            }

            // After last runner match: inv_id:task_key or :task_key
            let after_runners = runner_re()
                .find_iter(br)
                .last()
                .map_or(br, |m| &br[m.end()..]);

            if let Some(colon_pos) = after_runners.find(':') {
                let before = &after_runners[..colon_pos];
                let after = &after_runners[colon_pos + 1..];
                if !before.is_empty() {
                    invocation_id = Some(before.to_owned());
                }
                if !after.is_empty() {
                    task_key = Some(after.to_owned());
                }
            } else if !after_runners.is_empty() {
                invocation_id = Some(after_runners.to_owned());
            }
        }
    }

    let entity_refs = extract_entity_refs(&message);

    let is_valid = !runners.is_empty()
        || worker_id.is_some()
        || invocation_id.is_some()
        || task_key.is_some()
        || !entity_refs.is_empty();

    Some(ParsedLogLine {
        raw: raw.to_owned(),
        runners,
        worker_id,
        invocation_id,
        task_key,
        raw_bracket,
        timestamp,
        level,
        system,
        module,
        message,
        entity_refs,
        is_valid,
    })
}

// ── tracing-subscriber format parsing ─────────────────────────────────────────

/// Try to parse a line in tracing-subscriber's default format:
/// `TIMESTAMP  LEVEL span{k=v}:span{k=v}: target: message`
fn try_parse_tracing_format(clean: &str, raw: &str) -> Option<ParsedLogLine> {
    // Check if the line contains tracing span syntax: word{...}
    if !tracing_span_re().is_match(clean) {
        return None;
    }

    let timestamp = extract_timestamp(clean);
    let level = extract_level(clean);

    // Extract runner/invocation info from spans
    let mut runners = Vec::new();
    let mut worker_id = None;
    let mut invocation_id = None;
    let mut task_key = None;
    let mut raw_spans = Vec::new();

    for cap in tracing_span_re().captures_iter(clean) {
        let span_name = &cap[1];
        let fields_str = &cap[2];
        let fields = parse_kv_fields(fields_str);
        raw_spans.push(format!("{}{{...}}", span_name));

        match span_name {
            "runner" => {
                if let Some(rid) = fields.get("runner_id") {
                    let cls = fields
                        .get("cls")
                        .map_or_else(|| "R".to_owned(), std::string::ToString::to_string);
                    runners.push(ParsedRunner {
                        cls_abbr: cls,
                        partial_id: rid.to_string(),
                    });
                }
            }
            "invocation" => {
                if let Some(inv) = fields.get("invocation_id") {
                    invocation_id = Some(inv.to_string());
                }
                if let Some(tid) = fields.get("task_id") {
                    if !tid.is_empty() {
                        task_key = Some(tid.to_string());
                    }
                }
            }
            "worker" => {
                if let Some(wid) = fields.get("worker_id") {
                    worker_id = Some(wid.to_string());
                }
            }
            _ => {
                // Generic span — extract any UUIDs or IDs
                for v in fields.values() {
                    if uuid_re().is_match(v) && invocation_id.is_none() {
                        // Could be a relevant UUID
                    }
                }
            }
        }
    }

    // Extract target and message from the part after all spans
    // Pattern: ...}: target::path: message
    let (module, message) = extract_target_message_tracing(clean);

    let entity_refs = extract_entity_refs(&message);

    let raw_bracket = if !raw_spans.is_empty() {
        // Build a compact representation for display
        let mut bracket = String::new();
        for r in &runners {
            bracket.push_str(&format!("{}({})", r.cls_abbr, short_id(&r.partial_id)));
        }
        if let Some(ref inv) = invocation_id {
            bracket.push_str(&short_id(inv));
        }
        if let Some(ref task) = task_key {
            bracket.push(':');
            bracket.push_str(task);
        }
        if bracket.is_empty() {
            None
        } else {
            Some(bracket)
        }
    } else {
        None
    };

    let is_valid = !runners.is_empty()
        || worker_id.is_some()
        || invocation_id.is_some()
        || task_key.is_some()
        || !entity_refs.is_empty();

    Some(ParsedLogLine {
        raw: raw.to_owned(),
        runners,
        worker_id,
        invocation_id,
        task_key,
        raw_bracket,
        timestamp,
        level,
        system: None,
        module,
        message,
        entity_refs,
        is_valid,
    })
}

/// Parse `key=value` pairs from a tracing span fields string.
fn parse_kv_fields(fields_str: &str) -> std::collections::HashMap<&str, &str> {
    let mut map = std::collections::HashMap::new();
    for part in fields_str.split_whitespace() {
        if let Some(eq_idx) = part.find('=') {
            let key = &part[..eq_idx];
            let value = &part[eq_idx + 1..];
            map.insert(key, value);
        }
    }
    map
}

/// Extract target module and message from a tracing-format line.
///
/// After the last `}: ` we have `target::path: message`
fn extract_target_message_tracing(line: &str) -> (Option<String>, String) {
    // Find the last "}:" which ends the span chain, then look for "target: message"
    if let Some(pos) = line.rfind("}:") {
        let after = &line[pos + 2..].trim_start();
        // After span chain: "target::module: message"
        // The target ends at the next ": " after the span chain
        if let Some(colon_pos) = after.find(": ") {
            let target = after[..colon_pos].trim();
            let msg = after[colon_pos + 2..].trim();
            return (
                if target.is_empty() {
                    None
                } else {
                    Some(target.to_owned())
                },
                msg.to_owned(),
            );
        }
        return (None, after.to_string());
    }
    (None, line.to_owned())
}

/// Shorten an ID to first 8 chars for display.
fn short_id(id: &str) -> String {
    crate::util::formatting::short_id(id)
}

// ── bracket format parsing ────────────────────────────────────────────────────

/// Parse bracket-format line: `TIMESTAMP LEVEL TARGET [bracket] message`
fn parse_bracket_format(clean: &str, raw: &str) -> ParsedLogLine {
    let timestamp = extract_timestamp(clean);
    let level = extract_level(clean);
    let bracket = extract_bracket(clean);
    let entity_refs = extract_entity_refs(clean);

    let (module, message) = if let Some(caps) = bracket_line_re().captures(clean) {
        (
            caps.get(3).map(|m| m.as_str().to_owned()),
            caps.get(4)
                .map(|m| m.as_str().to_owned())
                .unwrap_or_default(),
        )
    } else {
        (None, clean.to_owned())
    };

    if bracket.is_none() && entity_refs.is_empty() {
        return ParsedLogLine {
            raw: raw.to_owned(),
            timestamp,
            level,
            module,
            message,
            ..Default::default()
        };
    }

    let mut parsed = if let Some(ref br) = bracket {
        parse_bracket(br)
    } else {
        ParsedLogLine::default()
    };

    parsed.raw = raw.to_owned();
    parsed.timestamp = timestamp;
    parsed.level = level;
    parsed.module = module;
    parsed.message = message;
    parsed.entity_refs = entity_refs;
    parsed.is_valid = parsed.is_valid || !parsed.entity_refs.is_empty();
    parsed
}

// ── common parsing helpers ────────────────────────────────────────────────────

fn try_extract_json_text(line: &str) -> Option<String> {
    let stripped = line.trim();
    if !stripped.starts_with('{') {
        return None;
    }
    let obj: serde_json::Value = serde_json::from_str(stripped).ok()?;
    obj.get("text")
        .and_then(|v| v.as_str())
        .map(std::borrow::ToOwned::to_owned)
}

fn strip_ansi(s: &str) -> String {
    ansi_re().replace_all(s, "").to_string()
}

fn extract_timestamp(text: &str) -> Option<String> {
    timestamp_re().find(text).map(|m| m.as_str().to_owned())
}

fn extract_level(text: &str) -> Option<String> {
    const LEVELS: &[&str] = &[
        "CRITICAL", "ERROR", "WARNING", "WARN", "INFO", "DEBUG", "TRACE", "CRIT",
    ];
    for &lvl in LEVELS {
        if let Some(pos) = text.find(lvl) {
            let before_ok = pos == 0 || !text.as_bytes()[pos - 1].is_ascii_alphanumeric();
            let after_pos = pos + lvl.len();
            let after_ok =
                after_pos >= text.len() || !text.as_bytes()[after_pos].is_ascii_alphanumeric();
            if before_ok && after_ok {
                let canonical = match lvl {
                    "WARN" => "WARNING",
                    "CRIT" => "CRITICAL",
                    _ => lvl,
                };
                return Some(canonical.to_owned());
            }
        }
    }
    None
}

fn extract_bracket(text: &str) -> Option<String> {
    bracket_re()
        .captures(text)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_owned())
}

fn parse_bracket(bracket: &str) -> ParsedLogLine {
    let runners: Vec<ParsedRunner> = runner_re()
        .captures_iter(bracket)
        .map(|c| ParsedRunner {
            cls_abbr: c[1].to_owned(),
            partial_id: c[2].to_owned(),
        })
        .collect();

    let inv_id = uuid_re().find(bracket).map(|m| m.as_str().to_owned());
    let task_key = extract_task_key(bracket, inv_id.as_deref());

    let is_valid = !runners.is_empty() || inv_id.is_some() || task_key.is_some();

    ParsedLogLine {
        runners,
        invocation_id: inv_id,
        task_key,
        raw_bracket: Some(bracket.to_owned()),
        is_valid,
        ..Default::default()
    }
}

fn extract_task_key(bracket: &str, inv_id: Option<&str>) -> Option<String> {
    let after = if let Some(id) = inv_id {
        let lower = bracket.to_lowercase();
        let id_lower = id.to_lowercase();
        if let Some(idx) = lower.find(&id_lower) {
            &bracket[idx + id.len()..]
        } else {
            bracket
        }
    } else {
        let last_end = runner_re().find_iter(bracket).last().map_or(0, |m| m.end());
        &bracket[last_end..]
    };

    if let Some(stripped) = after.strip_prefix(':') {
        let key = stripped.trim();
        if !key.is_empty() {
            return Some(key.to_owned());
        }
    }
    None
}

fn extract_entity_refs(text: &str) -> Vec<EntityRef> {
    let mut refs = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();

    for cap in entity_ref_re().captures_iter(text) {
        let kind = cap[1].to_lowercase();
        let value = cap[2].to_owned();
        let key = (kind.clone(), value.clone());
        if seen.insert(key) {
            refs.push(EntityRef { kind, value });
        }
    }

    for cap in entity_list_re().captures_iter(text) {
        let plural = cap[1].to_lowercase();
        let singular = plural.trim_end_matches('s').to_owned();
        for item in cap[2].split(',') {
            let val = item.trim().to_owned();
            if !val.is_empty() {
                let key = (singular.clone(), val.clone());
                if seen.insert(key) {
                    refs.push(EntityRef {
                        kind: singular.clone(),
                        value: val,
                    });
                }
            }
        }
    }

    refs
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tracing_span_format() {
        let line = "2026-03-21T15:39:09.107782Z  INFO runner{runner_id=5719fe43-33fb-4979-a024-7b092f0fff6f cls=PTR}:worker{worker_id=aabb1122-3344-5566-7788-99aabbccddee}:invocation{invocation_id=69bec730-710e-4780-b347-3918d7de571f task_id=rust::test.child_task}: rustvello::runner: Invocation completed successfully";
        let parsed = parse_log_line(line);
        assert!(parsed.is_valid, "should be valid");
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert_eq!(parsed.runners.len(), 1);
        assert_eq!(parsed.runners[0].cls_abbr, "PTR");
        assert_eq!(
            parsed.runners[0].partial_id,
            "5719fe43-33fb-4979-a024-7b092f0fff6f"
        );
        assert_eq!(
            parsed.worker_id.as_deref(),
            Some("aabb1122-3344-5566-7788-99aabbccddee"),
        );
        assert_eq!(
            parsed.invocation_id.as_deref(),
            Some("69bec730-710e-4780-b347-3918d7de571f")
        );
        assert_eq!(parsed.task_key.as_deref(), Some("rust::test.child_task"));
        assert_eq!(parsed.module.as_deref(), Some("rustvello::runner"));
        assert_eq!(parsed.message, "Invocation completed successfully");
    }

    #[test]
    fn test_parse_tracing_span_no_invocation() {
        let line = "2026-03-21T15:39:09.100Z  INFO runner{runner_id=abc12345-1234-5678-9abc-def012345678 cls=PTR}: rustvello::runner: Starting up";
        let parsed = parse_log_line(line);
        assert!(parsed.is_valid);
        assert_eq!(parsed.runners.len(), 1);
        assert!(parsed.invocation_id.is_none());
        assert_eq!(parsed.message, "Starting up");
    }

    #[test]
    fn test_parse_structured_line() {
        let line = "2024-01-15T10:30:00.123Z INFO my_module - Processing invocation:abc12345-1234-1234-1234-123456789abc";
        let parsed = parse_log_line(line);
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert!(!parsed.entity_refs.is_empty());
        assert_eq!(parsed.entity_refs[0].kind, "invocation");
        assert_eq!(
            parsed.entity_refs[0].value,
            "abc12345-1234-1234-1234-123456789abc"
        );
    }

    #[test]
    fn test_parse_bracket_context() {
        let line = "2024-01-15 10:30:00.123+01:00 INFO rustvello.app [TR(a86ab1f8)8f238bbb-0cba-45af-b56e-5a472e86ea97:module.func] message";
        let parsed = parse_log_line(line);
        assert!(parsed.is_valid);
        assert_eq!(parsed.runners.len(), 1);
        assert_eq!(parsed.runners[0].cls_abbr, "TR");
        assert_eq!(parsed.runners[0].partial_id, "a86ab1f8");
        assert_eq!(
            parsed.invocation_id.as_deref(),
            Some("8f238bbb-0cba-45af-b56e-5a472e86ea97")
        );
        assert_eq!(parsed.task_key.as_deref(), Some("module.func"));
    }

    #[test]
    fn test_parse_bracket_chain() {
        let line = "2024-01-15 10:30:00 INFO app [MTR(parent01).TR(child001)uuid1234-5678-9abc-def0-123456789abc:task.key] msg";
        let parsed = parse_log_line(line);
        assert_eq!(parsed.runners.len(), 2);
        assert_eq!(parsed.runners[0].cls_abbr, "MTR");
        assert_eq!(parsed.runners[1].cls_abbr, "TR");
    }

    #[test]
    fn test_entity_list_refs() {
        let line = "Processing invocations:[aaa-bbbb-cccc-dddd-eeeeeeeeeeee,fff-1111-2222-3333-444444444444]";
        let parsed = parse_log_line(line);
        let inv_refs: Vec<_> = parsed
            .entity_refs
            .iter()
            .filter(|r| r.kind == "invocation")
            .collect();
        assert_eq!(inv_refs.len(), 2);
    }

    #[test]
    fn test_strip_ansi() {
        let input = "\x1b[32mGreen text\x1b[0m";
        assert_eq!(strip_ansi(input), "Green text");
    }

    #[test]
    fn test_json_log_line() {
        let line = r#"{"timestamp":"2024-01-15 10:30:00","severity":"INFO","logger":"app","message":"hello","text":"2024-01-15 10:30:00 INFO     app [TR(abc12345)] hello"}"#;
        let parsed = parse_log_line(line);
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert!(parsed.is_valid);
        assert_eq!(parsed.runners[0].cls_abbr, "TR");
    }

    #[test]
    fn test_multi_line_split() {
        let text = "2024-01-15 10:30:00 INFO app - first line\n  continuation\n2024-01-15 10:30:01 ERROR app - second line";
        let entries = parse_log_lines(text);
        assert_eq!(entries.len(), 2);
        assert!(entries[0].raw.contains("continuation"));
        assert_eq!(entries[1].level.as_deref(), Some("ERROR"));
    }

    #[test]
    fn test_parse_unified_rust_format() {
        let line = "2026-03-27T10:23:45.123Z INFO  [R] my_app [PTR(a86ab1f8).W(d1241003)cc6c0e34-5678-9abc-def0-123456789abc:core_tasks.recover] rustvello::runner Invocation completed";
        let parsed = parse_log_line(line);
        assert!(parsed.is_valid);
        assert_eq!(
            parsed.timestamp.as_deref(),
            Some("2026-03-27T10:23:45.123Z")
        );
        assert_eq!(parsed.level.as_deref(), Some("INFO"));
        assert_eq!(parsed.system.as_deref(), Some("RUST"));
        assert_eq!(parsed.module.as_deref(), Some("rustvello::runner"));
        assert_eq!(parsed.runners.len(), 1); // PTR only (W is worker)
        assert_eq!(parsed.runners[0].cls_abbr, "PTR");
        assert_eq!(parsed.runners[0].partial_id, "a86ab1f8");
        assert_eq!(parsed.worker_id.as_deref(), Some("d1241003"));
        assert_eq!(
            parsed.invocation_id.as_deref(),
            Some("cc6c0e34-5678-9abc-def0-123456789abc")
        );
        assert_eq!(parsed.task_key.as_deref(), Some("core_tasks.recover"));
        assert_eq!(parsed.message, "Invocation completed");
    }

    #[test]
    fn test_parse_unified_python_format() {
        let line = "2026-03-27T10:23:45.123Z WARN  [P] my_py_app [TR(abcd1234)] pynenc.runner Status change";
        let parsed = parse_log_line(line);
        assert!(parsed.is_valid);
        assert_eq!(parsed.level.as_deref(), Some("WARNING"));
        assert_eq!(parsed.system.as_deref(), Some("PYTHON"));
        assert_eq!(parsed.module.as_deref(), Some("pynenc.runner"));
        assert_eq!(parsed.runners.len(), 1);
        assert_eq!(parsed.runners[0].cls_abbr, "TR");
        assert_eq!(parsed.runners[0].partial_id, "abcd1234");
        assert!(parsed.invocation_id.is_none());
    }

    #[test]
    fn test_parse_unified_no_fields() {
        // No bracket → first word after [R] is the target, rest is message
        let line = "2026-03-27T10:23:45.123Z DEBUG [R] rustvello::app Starting up";
        let parsed = parse_log_line(line);
        assert_eq!(parsed.level.as_deref(), Some("DEBUG"));
        assert_eq!(parsed.system.as_deref(), Some("RUST"));
        assert_eq!(parsed.module.as_deref(), Some("rustvello::app"));
        assert_eq!(parsed.message, "Starting up");
        assert!(parsed.runners.is_empty());
    }
}
