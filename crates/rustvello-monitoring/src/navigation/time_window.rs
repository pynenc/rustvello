//! Parsing and fitting absolute monitoring time windows.

use chrono::{DateTime, Duration, Utc};

pub const DEFAULT_TARGET_FILL: f64 = 0.82;
const MIN_SELECTION_SPAN: Duration = Duration::milliseconds(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeWindow {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        if end > start {
            Self { start, end }
        } else {
            Self {
                start: start - MIN_SELECTION_SPAN / 2,
                end: start + MIN_SELECTION_SPAN / 2,
            }
        }
    }

    /// Fit an entity interval into a viewport so it occupies `target_fill`.
    pub fn fit(start: DateTime<Utc>, end: DateTime<Utc>, target_fill: f64) -> Self {
        let target_fill = target_fill.clamp(0.1, 1.0);
        let selection = if end > start {
            end - start
        } else {
            MIN_SELECTION_SPAN
        };
        let center = if end > start {
            start + selection / 2
        } else {
            start
        };
        let selection_us = selection.num_microseconds().unwrap_or(i64::MAX).max(1);
        let viewport_us = ((selection_us as f64) / target_fill).ceil() as i64;
        let viewport = Duration::microseconds(viewport_us.max(selection_us));
        Self::new(center - viewport / 2, center + viewport / 2)
    }

    pub fn fit_default(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self::fit(start, end, DEFAULT_TARGET_FILL)
    }

    pub fn input_start(self) -> String {
        input_value(self.start)
    }

    pub fn input_end(self) -> String {
        input_value(self.end)
    }
}

pub fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    let normalized = value.trim().replace(' ', "+");
    DateTime::parse_from_rfc3339(&normalized)
        .map(|datetime| datetime.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%dT%H:%M:%S%.f")
                .map(|datetime| datetime.and_utc())
        })
        .ok()
}

fn input_value(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fitted_window_uses_requested_fill() {
        let start = parse_datetime("2026-09-04T19:40:35.412070+00:00").unwrap();
        let end = parse_datetime("2026-09-04T19:40:35.416259+00:00").unwrap();
        let window = TimeWindow::fit_default(start, end);
        let selection = (end - start).num_microseconds().unwrap() as f64;
        let viewport = (window.end - window.start).num_microseconds().unwrap() as f64;
        assert!((selection / viewport - DEFAULT_TARGET_FILL).abs() < 0.001);
    }

    #[test]
    fn point_window_is_valid_and_centered() {
        let point = parse_datetime("2026-09-04T19:40:35Z").unwrap();
        let window = TimeWindow::fit_default(point, point);
        assert!(window.start < point);
        assert!(window.end > point);
        assert_eq!(point - window.start, window.end - point);
    }

    #[test]
    fn parser_accepts_decoded_positive_offset_and_fractional_utc() {
        assert!(parse_datetime("2026-09-04T19:40:35.412070 00:00").is_some());
        assert!(parse_datetime("2026-09-04T19:40:35.412070").is_some());
    }
}
