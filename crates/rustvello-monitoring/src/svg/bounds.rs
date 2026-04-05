//! Time-to-pixel coordinate mapping.

use chrono::{DateTime, Utc};

/// Maps time range to pixel coordinates in the drawable area.
#[derive(Debug, Clone)]
pub struct TimelineBounds {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub left_margin: f64,
    pub drawable_width: f64,
}

impl TimelineBounds {
    pub fn new(
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        left_margin: f64,
        drawable_width: f64,
    ) -> Self {
        Self {
            start,
            end,
            left_margin,
            drawable_width,
        }
    }

    /// Total time span in seconds.
    fn total_seconds(&self) -> f64 {
        let dur = self.end - self.start;
        dur.num_milliseconds() as f64 / 1000.0
    }

    /// Convert a timestamp to an x-pixel position.
    pub fn time_to_x(&self, t: DateTime<Utc>) -> f64 {
        let total = self.total_seconds();
        if total <= 0.0 {
            return self.left_margin;
        }
        let elapsed = (t - self.start).num_milliseconds() as f64 / 1000.0;
        self.left_margin + (elapsed / total) * self.drawable_width
    }

    /// Convert a duration in seconds to a pixel width.
    pub fn duration_to_width(&self, seconds: f64) -> f64 {
        let total = self.total_seconds();
        if total <= 0.0 {
            return 0.0;
        }
        (seconds / total) * self.drawable_width
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_time_to_x() {
        let start = Utc::now();
        let end = start + Duration::seconds(100);
        let bounds = TimelineBounds::new(start, end, 100.0, 800.0);

        let mid = start + Duration::seconds(50);
        let x = bounds.time_to_x(mid);
        assert!((x - 500.0).abs() < 1.0); // 100 + 400 = 500

        assert!((bounds.time_to_x(start) - 100.0).abs() < 0.01);
        assert!((bounds.time_to_x(end) - 900.0).abs() < 0.01);
    }

    #[test]
    fn test_duration_to_width() {
        let start = Utc::now();
        let end = start + Duration::seconds(100);
        let bounds = TimelineBounds::new(start, end, 100.0, 800.0);

        let w = bounds.duration_to_width(10.0);
        assert!((w - 80.0).abs() < 0.01);
    }

    #[test]
    fn test_zero_span() {
        let t = Utc::now();
        let bounds = TimelineBounds::new(t, t, 100.0, 800.0);
        assert!((bounds.time_to_x(t) - 100.0).abs() < 0.01);
        assert!((bounds.duration_to_width(5.0)).abs() < 0.01);
    }
}
