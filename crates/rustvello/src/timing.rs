//! # Timing Module
//!
//! Provides constants and functions for managing durations and intervals.
//!
//! ## Usage
//! Use predefined constants or functions to calculate intervals dynamically:
//! ```rust
//! use rustvello::prelude::*;
//! use rustvello::timing::element_registration_interval;
//! let interval = element_registration_interval(TimestampResolution::Milliseconds);
//! println!("Interval: {:?}", interval);
//! ```

use rustvello_proto::TimestampResolution;
use std::time::Duration;

/// Default flush interval for recordings.
pub const RECORDING_FLUSH_INTERVAL: Duration = Duration::from_secs(1);

/// Default initialization interval.
pub const INITIALIZATION_INTERVAL: Duration = Duration::from_secs(1);

/// Default cluster monitoring interval.
pub const CLUSTER_MONITOR_INTERVAL: Duration = Duration::from_secs(60);

/// Modifier for element registration interval.
pub const ELEMENT_REGISTRATION_RESOLUTION_MODIFIER: f64 = 0.25;

/// Modifier for metric sending interval.
pub const METRIC_SENDING_RESOLUTION_MODIFIER: f64 = 1.0;

/// Adjusts a `Duration` by a percentage modifier.
pub fn adjust_duration_by_modifier(duration: Duration, modifier: f64) -> Duration {
    Duration::from_nanos((duration.as_nanos() as f64 * modifier).max(1.0) as u64)
}

/// Interval for element registration.
pub fn element_registration_interval(finest_resolution: TimestampResolution) -> Duration {
    adjust_duration_by_modifier(
        finest_resolution.to_duration(),
        ELEMENT_REGISTRATION_RESOLUTION_MODIFIER,
    )
}

/// Interval for metric sending.
pub fn metric_sending_interval(finest_resolution: TimestampResolution) -> Duration {
    adjust_duration_by_modifier(
        finest_resolution.to_duration(),
        METRIC_SENDING_RESOLUTION_MODIFIER,
    )
}
