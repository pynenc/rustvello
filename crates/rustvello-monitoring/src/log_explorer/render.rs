//! HTML rendering helpers for the Log Explorer.
//!
//! Transforms parsed log lines into safe HTML with inline hyperlinks for
//! runner IDs, invocation UUIDs, task keys, and structured entity references.
//! Produces two-column layout parts (header + message) matching the pynmon
//! log explorer design.
//!
//! Supports both tracing-subscriber span format and custom bracket format.

use std::sync::OnceLock;

use regex::Regex;

use crate::util::escape::xml_escape;

// ── Regex constants ───────────────────────────────────────────────────────────

fn inline_entity_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)((?:invocation|runner|worker|task|parent-invocation|child-invocation|new-invocation|current-owner-runner|attempted-owner-runner|workflow|sub-workflow|parent-workflow):[0-9a-zA-Z._-]+(?:-[0-9a-fA-F]{4}){0,4})",
        )
        .expect("valid regex")
    })
}

fn status_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"((?:from_status|to_status|status):([A-Za-z_]+))|(InvocationStatus\.([A-Za-z_]+))",
        )
        .expect("valid regex")
    })
}

// ── Status colors ─────────────────────────────────────────────────────────────

fn status_hex_color(status: &str) -> Option<&'static str> {
    match status.to_uppercase().replace('-', "_").as_str() {
        "REGISTERED" => Some("#6c757d"),
        "CONCURRENCY_CONTROLLED" => Some("#7c5e10"),
        "CONCURRENCY_CONTROLLED_FINAL" => Some("#7c5e10"),
        "REROUTED" => Some("#2e7d32"),
        "PENDING" => Some("#856404"),
        "PENDING_RECOVERY" => Some("#b06000"),
        "RUNNING" => Some("#084298"),
        "RUNNING_RECOVERY" => Some("#6a3d9a"),
        "PAUSED" => Some("#3d0a91"),
        "KILLED" => Some("#6c757d"),
        "SUCCESS" => Some("#0f5132"),
        "FAILED" => Some("#842029"),
        "RETRY" => Some("#e8a020"),
        "CANCELLED" => Some("#6c757d"),
        _ => None,
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Return the URL for an entity reference hyperlink.
pub fn entity_link_url(kind: &str, value: &str) -> String {
    match kind {
        "invocation" | "parent-invocation" | "child-invocation" | "new-invocation" => {
            format!("/invocations/{}", xml_escape(value))
        }
        "runner" | "worker" | "current-owner-runner" | "attempted-owner-runner" => {
            format!("/runners/{}", xml_escape(value))
        }
        "task" => format!("/tasks/{}", xml_escape(value)),
        "workflow" | "sub-workflow" | "parent-workflow" => "/workflows/runs".to_owned(),
        _ => String::new(),
    }
}

/// Render the message part of a log line with entity links and status coloring.
pub fn render_message_html(message: &str) -> String {
    if message.is_empty() {
        return String::new();
    }
    // linkify_entities runs on raw text; entity_replacer escapes each segment.
    let linked = linkify_entities(message);
    colorize_statuses(&linked)
}

// ── Internal rendering ────────────────────────────────────────────────────────

fn linkify_entities(raw: &str) -> String {
    let re = inline_entity_re();
    let mut result = String::with_capacity(raw.len());
    let mut last_end = 0;
    for m in re.find_iter(raw) {
        // Escape the text segment before this match
        result.push_str(&xml_escape(&raw[last_end..m.start()]));
        // Replace the entity token (entity_replacer handles its own escaping)
        result.push_str(&entity_replacer(m.as_str()));
        last_end = m.end();
    }
    // Escape trailing text
    result.push_str(&xml_escape(&raw[last_end..]));
    result
}

fn entity_replacer(token: &str) -> String {
    if let Some(colon_idx) = token.find(':') {
        let kind = &token[..colon_idx];
        let value = &token[colon_idx + 1..];
        let url = entity_link_url(kind, value);
        if !url.is_empty() {
            let data_attr = match kind {
                "runner" | "worker" | "current-owner-runner" | "attempted-owner-runner" => {
                    format!(r#" data-runner-id="{}""#, xml_escape(value))
                }
                "invocation" | "parent-invocation" | "child-invocation" | "new-invocation" => {
                    format!(r#" data-invocation-id="{}""#, xml_escape(value))
                }
                "task" => format!(r#" data-task-key="{}""#, xml_escape(value)),
                _ => String::new(),
            };
            return format!(
                r#"<a href="{}" class="log-entity-link"{} title="{}: {}">{}</a>"#,
                url,
                data_attr,
                xml_escape(kind),
                xml_escape(value),
                xml_escape(token),
            );
        }
    }
    xml_escape(token)
}

fn sub_outside_tags(
    re: &Regex,
    html: &str,
    replacer: impl Fn(&regex::Captures) -> String,
) -> String {
    let mut result = String::with_capacity(html.len());
    let bytes = html.as_bytes();
    let mut segments: Vec<(usize, usize, bool)> = Vec::new();
    let mut seg_start = 0;
    let mut in_tag = false;
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'<' {
            if !in_tag && i > seg_start {
                segments.push((seg_start, i, false));
            }
            in_tag = true;
            seg_start = i;
        } else if b == b'>' && in_tag {
            segments.push((seg_start, i + 1, true));
            in_tag = false;
            seg_start = i + 1;
        }
    }
    if seg_start < html.len() {
        segments.push((seg_start, html.len(), in_tag));
    }

    for (start, end, is_tag) in segments {
        let slice = &html[start..end];
        if is_tag {
            result.push_str(slice);
        } else {
            result.push_str(re.replace_all(slice, &replacer).as_ref());
        }
    }

    result
}

fn colorize_statuses(html: &str) -> String {
    sub_outside_tags(status_token_re(), html, |caps| {
        // Group 1+2: status:NAME or from_status:NAME
        // Group 3+4: InvocationStatus.NAME
        let (full_token, status_name) = if let Some(g1) = caps.get(1) {
            (g1.as_str(), caps[2].to_owned())
        } else if let Some(g3) = caps.get(3) {
            (g3.as_str(), caps[4].to_owned())
        } else {
            return caps[0].to_owned();
        };

        if let Some(hex) = status_hex_color(&status_name) {
            format!(
                r#"<span class="status-token" style="color:{};" title="{}">{}</span>"#,
                hex,
                status_name.to_uppercase(),
                xml_escape(full_token),
            )
        } else {
            xml_escape(full_token)
        }
    })
}

/// Return the CSS card class for a log level.
pub fn level_card_class(level: &str) -> &'static str {
    match level.to_uppercase().as_str() {
        "INFO" => "level-info",
        "WARNING" | "WARN" => "level-warning",
        "ERROR" => "level-error",
        "CRITICAL" => "level-critical",
        "DEBUG" => "level-debug",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_link_url() {
        assert_eq!(entity_link_url("invocation", "abc"), "/invocations/abc");
        assert_eq!(entity_link_url("runner", "r1"), "/runners/r1");
        assert_eq!(
            entity_link_url("task", "rust::mod.func"),
            "/tasks/rust::mod.func"
        );
        assert_eq!(entity_link_url("workflow", "w1"), "/workflows/runs");
    }

    #[test]
    fn test_level_card_class() {
        assert_eq!(level_card_class("INFO"), "level-info");
        assert_eq!(level_card_class("ERROR"), "level-error");
        assert_eq!(level_card_class("WARNING"), "level-warning");
    }

    #[test]
    fn test_entity_replacer() {
        let result = entity_replacer("invocation:abc-1234-5678-9abc-def012345678");
        assert!(result.contains("data-invocation-id"));
        assert!(result.contains("/invocations/"));
    }

    #[test]
    fn test_status_colorization() {
        let html = "status:RUNNING something status:FAILED";
        let result = colorize_statuses(html);
        assert!(result.contains("status-token"));
        assert!(result.contains("RUNNING"));
    }

    #[test]
    fn test_render_message_html() {
        let msg = "Invocation completed successfully invocation:abc-1234-5678-9abc-def012345678";
        let html = render_message_html(msg);
        assert!(html.contains("log-entity-link"));
        assert!(html.contains("/invocations/"));
    }
}
