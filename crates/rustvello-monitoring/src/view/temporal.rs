//! Page-relative temporal row geometry.

use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemporalExtent {
    pub actual_start: DateTime<Utc>,
    pub actual_end: DateTime<Utc>,
}

impl TemporalExtent {
    pub fn new(actual_start: DateTime<Utc>, actual_end: DateTime<Utc>) -> Self {
        Self {
            actual_start,
            actual_end: actual_end.max(actual_start),
        }
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.actual_start < other.actual_end && other.actual_start < self.actual_end
    }

    pub fn position_within(self, page: Self) -> TemporalPosition {
        let range_us = (page.actual_end - page.actual_start)
            .num_microseconds()
            .unwrap_or(0)
            .max(1) as f64;
        let left = (self.actual_start - page.actual_start)
            .num_microseconds()
            .unwrap_or(0) as f64
            / range_us
            * 100.0;
        let width = (self.actual_end - self.actual_start)
            .num_microseconds()
            .unwrap_or(0)
            .max(0) as f64
            / range_us
            * 100.0;
        TemporalPosition {
            left_percent: left.clamp(0.0, 100.0),
            width_percent: width.clamp(0.0, 100.0),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TemporalPosition {
    pub left_percent: f64,
    pub width_percent: f64,
}

pub fn page_time_range(
    extents: impl IntoIterator<Item = TemporalExtent>,
) -> Option<TemporalExtent> {
    let mut extents = extents.into_iter();
    let first = extents.next()?;
    Some(extents.fold(first, |range, extent| TemporalExtent {
        actual_start: range.actual_start.min(extent.actual_start),
        actual_end: range.actual_end.max(extent.actual_end),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::navigation::parse_datetime;

    fn at(second: u32) -> DateTime<Utc> {
        parse_datetime(&format!("2026-09-04T19:40:{second:02}Z")).unwrap()
    }

    #[test]
    fn positions_use_actual_time_and_keep_zero_width() {
        let page = TemporalExtent::new(at(0), at(10));
        let point = TemporalExtent::new(at(5), at(5));
        let position = point.position_within(page);
        assert_eq!(position.left_percent, 50.0);
        assert_eq!(position.width_percent, 0.0);
    }

    #[test]
    fn overlap_uses_actual_intervals_not_visual_minimums() {
        let first = TemporalExtent::new(at(1), at(2));
        let touching = TemporalExtent::new(at(2), at(3));
        let overlapping = TemporalExtent::new(at(1), at(3));
        assert!(!first.overlaps(touching));
        assert!(first.overlaps(overlapping));
    }
}
