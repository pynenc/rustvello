//! Grid lines, time axis labels, and legend rendering.

use std::fmt::Write;

use super::bounds::TimelineBounds;
use super::config::TimelineConfig;
use crate::util::status_colors;
use rustvello_proto::status::InvocationStatus;

/// Compute the tick interval in seconds based on the total duration, matching pynenc tiers.
fn tick_interval_secs(dur_secs: f64) -> f64 {
    if dur_secs <= 0.5 {
        0.05
    } else if dur_secs <= 2.0 {
        0.2
    } else if dur_secs <= 5.0 {
        0.5
    } else if dur_secs <= 10.0 {
        1.0
    } else if dur_secs <= 30.0 {
        5.0
    } else if dur_secs <= 120.0 {
        15.0
    } else if dur_secs <= 600.0 {
        60.0
    } else if dur_secs <= 3600.0 {
        300.0
    } else if dur_secs <= 21600.0 {
        1800.0
    } else {
        3600.0
    }
}

/// Compute tick positions at regular intervals from start to end.
fn tick_positions(bounds: &TimelineBounds) -> Vec<chrono::DateTime<chrono::Utc>> {
    let dur_ms = (bounds.end - bounds.start).num_milliseconds() as f64;
    let dur_secs = dur_ms / 1000.0;
    if dur_secs <= 0.0 {
        return vec![];
    }
    let interval = tick_interval_secs(dur_secs);
    let interval_ms = (interval * 1000.0) as i64;
    let mut ticks = vec![];
    let mut current = bounds.start;
    while current <= bounds.end {
        ticks.push(current);
        current += chrono::Duration::milliseconds(interval_ms);
    }
    ticks
}

/// Format a tick label based on total visible duration, matching pynenc tiers.
fn format_tick_label(t: chrono::DateTime<chrono::Utc>, dur_secs: f64) -> String {
    if dur_secs <= 10.0 {
        t.format("%H:%M:%S%.3f").to_string()
    } else if dur_secs <= 3600.0 {
        t.format("%H:%M:%S").to_string()
    } else if dur_secs <= 86400.0 {
        t.format("%H:%M").to_string()
    } else {
        t.format("%m/%d %H:%M").to_string()
    }
}

/// Render vertical grid lines at regular time intervals.
pub fn render_grid(
    buf: &mut String,
    config: &TimelineConfig,
    bounds: &TimelineBounds,
    total_height: f64,
) {
    let ticks = tick_positions(bounds);
    let stroke_color = "#e0e0e0";
    for t in &ticks {
        let x = bounds.time_to_x(*t);
        let top = config.top_margin;
        let bot = total_height - config.bottom_margin;
        let _ = write!(
            buf,
            "<line x1=\"{x:.1}\" y1=\"{top}\" x2=\"{x:.1}\" y2=\"{bot}\" stroke=\"{stroke_color}\" stroke-width=\"1\" stroke-dasharray=\"4,4\"/>",
        );
    }
}

/// Render time axis labels along the top with tick marks.
pub fn render_time_axis(buf: &mut String, config: &TimelineConfig, bounds: &TimelineBounds) {
    let dur_ms = (bounds.end - bounds.start).num_milliseconds() as f64;
    let dur_secs = dur_ms / 1000.0;
    if dur_secs <= 0.0 {
        return;
    }
    let ticks = tick_positions(bounds);
    let axis_y = config.top_margin - 5.0;
    let axis_color = "#999";
    let text_fill = "#666";

    // Axis line
    let _ = write!(
        buf,
        "<line x1=\"{left:.1}\" y1=\"{axis_y}\" x2=\"{right:.1}\" y2=\"{axis_y}\" stroke=\"{axis_color}\" stroke-width=\"1\"/>",
        left = config.left_margin,
        right = config.width,
    );

    for t in &ticks {
        let x = bounds.time_to_x(*t);
        let label = format_tick_label(*t, dur_secs);
        let tick_top = axis_y - 5.0;
        // Tick mark
        let _ = write!(
            buf,
            "<line x1=\"{x:.1}\" y1=\"{axis_y}\" x2=\"{x:.1}\" y2=\"{tick_top}\" stroke=\"{axis_color}\" stroke-width=\"1\"/>",
        );
        // Label
        let label_y = axis_y - 10.0;
        let _ = write!(
            buf,
            "<text x=\"{x:.1}\" y=\"{label_y}\" text-anchor=\"middle\" font-size=\"10\" fill=\"{text_fill}\">{label}</text>",
        );
    }
}

/// Render the status color legend at the bottom.
pub fn render_legend(buf: &mut String, config: &TimelineConfig, total_height: f64) {
    let statuses = [
        InvocationStatus::Registered,
        InvocationStatus::ConcurrencyControlled,
        InvocationStatus::ConcurrencyControlledFinal,
        InvocationStatus::Rerouted,
        InvocationStatus::Pending,
        InvocationStatus::PendingRecovery,
        InvocationStatus::Running,
        InvocationStatus::RunningRecovery,
        InvocationStatus::Paused,
        InvocationStatus::Resumed,
        InvocationStatus::Killed,
        InvocationStatus::Success,
        InvocationStatus::Failed,
        InvocationStatus::Retry,
    ];

    let mut y = total_height - config.bottom_margin + 15.0;
    let mut x = config.left_margin;

    let label_fill = "#333";
    for status in &statuses {
        let color = status_colors::hex_color(status);
        let status_name = crate::util::escape::xml_escape(&format!("{status:?}"));
        let label_width = status_name.len() as f64 * 7.0 + 30.0;
        // Wrap to next line if we'd exceed the available width
        if x + label_width > config.width - 100.0 {
            x = config.left_margin;
            y += 18.0;
        }
        let ry = y - 9.0;
        let _ = write!(
            buf,
            "<rect x=\"{x:.1}\" y=\"{ry}\" width=\"12\" height=\"12\" fill=\"{color}\" rx=\"2\"/>",
        );
        let tx = x + 16.0;
        let _ = write!(
            buf,
            "<text x=\"{tx}\" y=\"{y}\" font-size=\"11\" fill=\"{label_fill}\">{status_name}</text>",
        );
        x += label_width;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_tick_interval_tiers() {
        // Sub-second tiers
        assert_eq!(tick_interval_secs(0.3), 0.05);
        assert_eq!(tick_interval_secs(0.5), 0.05);
        assert_eq!(tick_interval_secs(1.0), 0.2);
        assert_eq!(tick_interval_secs(2.0), 0.2);
        assert_eq!(tick_interval_secs(3.0), 0.5);
        assert_eq!(tick_interval_secs(5.0), 0.5);
        assert_eq!(tick_interval_secs(7.0), 1.0);
        assert_eq!(tick_interval_secs(10.0), 1.0);
        assert_eq!(tick_interval_secs(15.0), 5.0);
        assert_eq!(tick_interval_secs(30.0), 5.0);
        assert_eq!(tick_interval_secs(60.0), 15.0);
        assert_eq!(tick_interval_secs(120.0), 15.0);
        assert_eq!(tick_interval_secs(300.0), 60.0);
        assert_eq!(tick_interval_secs(600.0), 60.0);
        assert_eq!(tick_interval_secs(1800.0), 300.0);
        assert_eq!(tick_interval_secs(3600.0), 300.0);
        assert_eq!(tick_interval_secs(7200.0), 1800.0);
        assert_eq!(tick_interval_secs(21600.0), 1800.0);
        assert_eq!(tick_interval_secs(86400.0), 3600.0);
    }

    #[test]
    fn test_tick_label_format_tiers() {
        let t = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 45).unwrap();
        // Sub-10s: show milliseconds
        assert!(format_tick_label(t, 5.0).contains("."));
        // Sub-1h: HH:MM:SS
        assert_eq!(format_tick_label(t, 60.0), "14:30:45");
        // Sub-24h: HH:MM
        assert_eq!(format_tick_label(t, 7200.0), "14:30");
        // >24h: MM/DD HH:MM
        assert_eq!(format_tick_label(t, 100000.0), "01/15 14:30");
    }

    #[test]
    fn test_tick_positions_generate_ticks() {
        let config = TimelineConfig::default();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = start + chrono::Duration::seconds(30);
        let bounds = TimelineBounds::new(start, end, config.left_margin, config.drawable_width());
        let ticks = tick_positions(&bounds);
        // 30 seconds / 5s interval = ~6 ticks (start + 5 more)
        assert!(ticks.len() >= 6);
        assert_eq!(ticks[0], start);
    }

    #[test]
    fn test_legend_contains_all_14_statuses() {
        let config = TimelineConfig::default();
        let mut buf = String::new();
        render_legend(&mut buf, &config, 500.0);
        // All 14 statuses should appear in legend
        assert!(buf.contains("Registered"));
        assert!(buf.contains("ConcurrencyControlled"));
        assert!(buf.contains("ConcurrencyControlledFinal"));
        assert!(buf.contains("Rerouted"));
        assert!(buf.contains("Pending"));
        assert!(buf.contains("PendingRecovery"));
        assert!(buf.contains("Running"));
        assert!(buf.contains("RunningRecovery"));
        assert!(buf.contains("Paused"));
        assert!(buf.contains("Resumed"));
        assert!(buf.contains("Killed"));
        assert!(buf.contains("Success"));
        assert!(buf.contains("Failed"));
        assert!(buf.contains("Retry"));
    }

    #[test]
    fn test_grid_uses_smart_ticks() {
        let config = TimelineConfig::default();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = start + chrono::Duration::seconds(60);
        let bounds = TimelineBounds::new(start, end, config.left_margin, config.drawable_width());
        let mut buf = String::new();
        render_grid(&mut buf, &config, &bounds, 400.0);
        // Should produce grid lines
        assert!(buf.contains("<line"));
        assert!(buf.contains("stroke-dasharray=\"4,4\""));
    }

    #[test]
    fn test_time_axis_has_axis_line_and_tick_marks() {
        let config = TimelineConfig::default();
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let end = start + chrono::Duration::seconds(120);
        let bounds = TimelineBounds::new(start, end, config.left_margin, config.drawable_width());
        let mut buf = String::new();
        render_time_axis(&mut buf, &config, &bounds);
        // Should have axis line and text labels
        assert!(buf.contains("<line"));
        assert!(buf.contains("<text"));
        assert!(buf.contains("12:00:"));
    }
}
