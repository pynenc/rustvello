//! Factory functions for creating visual timeline elements.

use chrono::{DateTime, Utc};
use rustvello_proto::status::InvocationStatus;

use super::bounds::TimelineBounds;
use super::config::TimelineConfig;
use super::models::{StatusPoint, StatusSegment};
use super::tooltip;
use crate::util::status_colors;

/// Create a status transition point at the given time.
#[allow(clippy::too_many_arguments)]
pub fn create_status_point(
    invocation_id: &str,
    task_id: &str,
    status: &InvocationStatus,
    timestamp: DateTime<Utc>,
    runner_id: Option<&str>,
    bounds: &TimelineBounds,
    _config: &TimelineConfig,
    y_center: f64,
) -> StatusPoint {
    let x = bounds.time_to_x(timestamp);
    let color = status_colors::hex_color(status).to_owned();
    let tt = tooltip::format_point_tooltip(invocation_id, task_id, status, timestamp, runner_id);

    StatusPoint {
        invocation_id: invocation_id.to_owned(),
        task_id: task_id.to_owned(),
        status: *status,
        timestamp,
        x,
        y: y_center,
        color,
        tooltip: tt,
    }
}

/// Create a status duration segment between two timestamps.
///
/// `color_status` determines the bar color — for outcome-aware coloring,
/// this is the next status (e.g. Success) instead of the current one.
#[allow(clippy::too_many_arguments)]
pub fn create_status_segment(
    invocation_id: &str,
    task_id: &str,
    status: &InvocationStatus,
    color_status: &InvocationStatus,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    runner_id: Option<&str>,
    bounds: &TimelineBounds,
    config: &TimelineConfig,
    y_center: f64,
    is_ongoing: bool,
) -> StatusSegment {
    let x = bounds.time_to_x(start);
    let end_x = bounds.time_to_x(end);
    let width = (end_x - x).max(config.min_segment_width);
    let height = config.segment_height;
    let y = y_center - height / 2.0;
    let color = status_colors::hex_color(color_status).to_owned();
    let tt = tooltip::format_segment_tooltip(invocation_id, task_id, status, start, end, runner_id);

    StatusSegment {
        invocation_id: invocation_id.to_owned(),
        task_id: task_id.to_owned(),
        status: *status,
        start,
        end,
        x,
        width,
        y,
        height,
        color,
        tooltip: tt,
        is_ongoing,
    }
}
