//! Visual element models for the timeline.

use chrono::{DateTime, Utc};
use rustvello_proto::status::InvocationStatus;

/// A point in time representing a status transition.
#[derive(Debug, Clone)]
pub struct StatusPoint {
    pub invocation_id: String,
    pub task_id: String,
    pub status: InvocationStatus,
    pub timestamp: DateTime<Utc>,
    pub x: f64,
    pub y: f64,
    pub color: String,
    pub tooltip: String,
}

/// A horizontal bar representing a duration in a specific status.
#[derive(Debug, Clone)]
pub struct StatusSegment {
    pub invocation_id: String,
    pub task_id: String,
    pub status: InvocationStatus,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub x: f64,
    pub width: f64,
    pub y: f64,
    pub height: f64,
    pub color: String,
    pub tooltip: String,
    /// Whether this segment is still ongoing (no terminal status yet).
    pub is_ongoing: bool,
}

/// A connecting line between consecutive elements of the same invocation.
#[derive(Debug, Clone)]
pub struct StatusLine {
    pub invocation_id: String,
    pub x1: f64,
    pub y1: f64,
    pub x2: f64,
    pub y2: f64,
    pub color: String,
}

/// An invocation bar spanning the full timeline for a single invocation.
#[derive(Debug, Clone)]
pub struct InvocationBar {
    pub invocation_id: String,
    pub runner_id: String,
    pub short_inv_id: String,
}
