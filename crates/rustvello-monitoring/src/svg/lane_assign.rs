//! Lane assignment algorithm with O(log n) overlap detection.
//!
//! Groups invocation elements into chains, then assigns each chain to a
//! sub-lane within a runner's lane group, using sorted intervals and
//! binary search for efficient overlap detection.

use chrono::{DateTime, Utc};

/// A contiguous chain of elements from a single invocation on a single runner.
///
/// An invocation that spans multiple runners produces multiple chains,
/// each covering only the history entries for that runner.
#[derive(Debug, Clone)]
pub struct ElementChain {
    pub invocation_id: String,
    pub runner_id: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Whether this chain is deferred (e.g., Registered status with no runner).
    pub deferred: bool,
    /// Assigned lane index within the runner group (set during assignment).
    pub lane_index: usize,
    /// Start index (inclusive) into the invocation's history entries.
    pub entry_start: usize,
    /// End index (exclusive) into the invocation's history entries.
    pub entry_end: usize,
}

impl ElementChain {
    pub fn new(
        invocation_id: &str,
        runner_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        deferred: bool,
    ) -> Self {
        Self {
            invocation_id: invocation_id.to_owned(),
            runner_id: runner_id.to_owned(),
            start,
            end,
            deferred,
            lane_index: 0,
            entry_start: 0,
            entry_end: 0,
        }
    }
}

/// Tracks occupied time intervals in a lane for O(log n) overlap detection.
#[derive(Debug, Clone, Default)]
pub struct LaneOccupancy {
    /// Sorted list of (end_time, start_time) intervals.
    /// Sorted by end_time for binary search.
    intervals: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

impl LaneOccupancy {
    /// Check if a time range overlaps with any existing interval.
    pub fn overlaps(&self, start: DateTime<Utc>, end: DateTime<Utc>) -> bool {
        // Find the first interval whose end > start using binary search
        let idx = self
            .intervals
            .binary_search_by(|(interval_end, _)| interval_end.cmp(&start))
            .unwrap_or_else(|i| i);

        // Check intervals from idx onward that could overlap
        for &(interval_end, interval_start) in &self.intervals[idx..] {
            if interval_start >= end {
                break; // No more possible overlaps (intervals are sorted)
            }
            if interval_end > start && interval_start < end {
                return true;
            }
        }

        // Also check backwards from idx (intervals ending after start)
        if idx > 0 {
            for &(interval_end, interval_start) in self.intervals[..idx].iter().rev() {
                if interval_end <= start {
                    break;
                }
                if interval_end > start && interval_start < end {
                    return true;
                }
            }
        }

        false
    }

    /// Insert a new interval, maintaining sorted order by end time.
    pub fn insert(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) {
        let pos = self
            .intervals
            .binary_search_by(|(interval_end, _)| interval_end.cmp(&end))
            .unwrap_or_else(|i| i);
        self.intervals.insert(pos, (end, start));
    }
}

/// Assign chains to lanes within their respective runner groups.
///
/// Two-pass algorithm:
/// 1. Regular chains first (sorted by start time)
/// 2. Deferred chains (typically Registered status)
pub fn assign_lanes(chains: &mut [ElementChain]) {
    // Separate into regular and deferred
    let (mut regular, mut deferred): (Vec<_>, Vec<_>) =
        chains.iter_mut().partition(|c| !c.deferred);

    // Sort regular chains by start time
    regular.sort_by_key(|c| c.start);

    // Track occupancy: runner_id → Vec<LaneOccupancy> (one per sub-lane)
    let mut occupancy: std::collections::HashMap<String, Vec<LaneOccupancy>> =
        std::collections::HashMap::new();

    // First pass: regular chains
    for chain in regular.iter_mut() {
        assign_to_lane(&mut occupancy, chain);
    }

    // Second pass: deferred chains (e.g. Registered-only)
    // Always place on lane 0 — these are just dots (no segment) and should
    // overlay the parent's execution bar rather than creating a separate row.
    deferred.sort_by_key(|c| c.start);
    for chain in deferred.iter_mut() {
        let lanes = occupancy.entry(chain.runner_id.clone()).or_default();
        if lanes.is_empty() {
            lanes.push(LaneOccupancy::default());
        }
        // Insert on lane 0 unconditionally — overlap is intentional
        lanes[0].insert(chain.start, chain.end);
        chain.lane_index = 0;
    }
}

/// Find the first non-overlapping lane for a chain, or create a new one.
fn assign_to_lane(
    occupancy: &mut std::collections::HashMap<String, Vec<LaneOccupancy>>,
    chain: &mut ElementChain,
) {
    let lanes = occupancy.entry(chain.runner_id.clone()).or_default();

    for (i, lane) in lanes.iter_mut().enumerate() {
        if !lane.overlaps(chain.start, chain.end) {
            lane.insert(chain.start, chain.end);
            chain.lane_index = i;
            return;
        }
    }

    // No existing lane fits — create a new one
    let mut new_lane = LaneOccupancy::default();
    new_lane.insert(chain.start, chain.end);
    chain.lane_index = lanes.len();
    lanes.push(new_lane);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_no_overlap() {
        let mut occ = LaneOccupancy::default();
        let t0 = Utc::now();
        occ.insert(t0, t0 + Duration::seconds(10));
        // After the interval — no overlap
        assert!(!occ.overlaps(t0 + Duration::seconds(10), t0 + Duration::seconds(20)));
    }

    #[test]
    fn test_overlap() {
        let mut occ = LaneOccupancy::default();
        let t0 = Utc::now();
        occ.insert(t0, t0 + Duration::seconds(10));
        // Overlapping interval
        assert!(occ.overlaps(t0 + Duration::seconds(5), t0 + Duration::seconds(15)));
    }

    #[test]
    fn test_assign_lanes_non_overlapping() {
        let t0 = Utc::now();
        let mut chains = vec![
            ElementChain::new("inv1", "runner1", t0, t0 + Duration::seconds(10), false),
            ElementChain::new(
                "inv2",
                "runner1",
                t0 + Duration::seconds(10),
                t0 + Duration::seconds(20),
                false,
            ),
        ];
        assign_lanes(&mut chains);
        // Non-overlapping chains should share lane 0
        assert_eq!(chains[0].lane_index, 0);
        assert_eq!(chains[1].lane_index, 0);
    }

    #[test]
    fn test_assign_lanes_overlapping() {
        let t0 = Utc::now();
        let mut chains = vec![
            ElementChain::new("inv1", "runner1", t0, t0 + Duration::seconds(10), false),
            ElementChain::new(
                "inv2",
                "runner1",
                t0 + Duration::seconds(5),
                t0 + Duration::seconds(15),
                false,
            ),
        ];
        assign_lanes(&mut chains);
        // Overlapping chains should be in different lanes
        assert_eq!(chains[0].lane_index, 0);
        assert_eq!(chains[1].lane_index, 1);
    }

    #[test]
    fn test_deferred_chains_overlay_on_lane_zero() {
        let t0 = Utc::now();
        let mut chains = vec![
            ElementChain::new("inv1", "runner1", t0, t0 + Duration::seconds(10), false),
            ElementChain::new(
                "inv-deferred",
                "runner1",
                t0,
                t0 + Duration::seconds(5),
                true,
            ),
        ];
        assign_lanes(&mut chains);
        // Deferred chains always go on lane 0 (overlay on parent's row)
        assert_eq!(chains[0].lane_index, 0);
        assert_eq!(chains[1].lane_index, 0);
    }
}
