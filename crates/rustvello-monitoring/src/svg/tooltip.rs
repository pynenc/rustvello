//! Tooltip text formatting for SVG timeline elements.
//!
//! Mirrors pynmon's tooltip format: invocation ID, task, status, time, runner.

use chrono::{DateTime, Utc};
use rustvello_proto::status::InvocationStatus;

use crate::util::formatting::format_duration_secs;

/// Format a tooltip for a status transition point.
pub fn format_point_tooltip(
    invocation_id: &str,
    task_id: &str,
    status: &InvocationStatus,
    timestamp: DateTime<Utc>,
    runner_id: Option<&str>,
) -> String {
    let mut lines = vec![format!(
        "Invocation: {}",
        truncate_for_tooltip(invocation_id)
    )];
    if !task_id.is_empty() {
        lines.push(format!("Task: {task_id}"));
    }
    lines.push(format!("Status: {status:?}"));
    lines.push(format!("Time: {}", timestamp.format("%H:%M:%S%.3f")));
    if let Some(rid) = runner_id {
        lines.push(format!("Runner: {}", truncate_for_tooltip(rid)));
    }
    lines.join("\n")
}

/// Format a tooltip for a status duration segment.
pub fn format_segment_tooltip(
    invocation_id: &str,
    task_id: &str,
    status: &InvocationStatus,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    runner_id: Option<&str>,
) -> String {
    let duration_secs = (end - start).num_milliseconds() as f64 / 1000.0;
    let mut lines = vec![format!(
        "Invocation: {}",
        truncate_for_tooltip(invocation_id)
    )];
    if !task_id.is_empty() {
        lines.push(format!("Task: {task_id}"));
    }
    lines.push(format!("Status: {status:?}"));
    lines.push(format!(
        "{} → {}",
        start.format("%H:%M:%S%.3f"),
        end.format("%H:%M:%S%.3f"),
    ));
    lines.push(format!("Duration: {}", format_duration_secs(duration_secs)));
    if let Some(rid) = runner_id {
        lines.push(format!("Runner: {}", truncate_for_tooltip(rid)));
    }
    lines.join("\n")
}

/// Truncate an ID for tooltip display (show first 12 chars), respecting char boundaries.
fn truncate_for_tooltip(id: &str) -> &str {
    match id.char_indices().nth(12) {
        Some((idx, _)) => &id[..idx],
        None => id,
    }
}
