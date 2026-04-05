//! Lane and lane group models for the timeline.

use super::models::{StatusLine, StatusPoint, StatusSegment};
use super::runner_info::RunnerInfo;

/// A single sub-lane within a runner's lane group.
///
/// Multiple non-overlapping invocations can share the same sub-lane.
/// Overlapping invocations are placed on separate sub-lanes, expanding
/// the runner's visual height.
#[derive(Debug, Clone)]
pub struct RunnerLane {
    /// Element containers (may contain elements from multiple invocations).
    pub points: Vec<StatusPoint>,
    pub segments: Vec<StatusSegment>,
    pub lines: Vec<StatusLine>,
    /// Y offset within the lane group (computed during layout).
    pub y_offset: f64,
    /// Worker-level runner info (for per-lane labels in multi-worker groups).
    pub worker_info: Option<RunnerInfo>,
}

impl Default for RunnerLane {
    fn default() -> Self {
        Self::new()
    }
}

impl RunnerLane {
    pub fn new() -> Self {
        Self {
            points: Vec::new(),
            segments: Vec::new(),
            lines: Vec::new(),
            y_offset: 0.0,
            worker_info: None,
        }
    }
}

/// A group of lanes for a runner, displayed together with a shared header.
#[derive(Debug, Clone)]
pub struct LaneGroup {
    /// Runner information for the group header.
    pub runner_info: RunnerInfo,
    /// Lanes within this group (one per invocation assigned to this runner).
    pub lanes: Vec<RunnerLane>,
    /// Y position of the group header (computed during layout).
    pub y_start: f64,
    /// Total height of the group.
    pub height: f64,
}

impl LaneGroup {
    pub fn new(runner_info: RunnerInfo) -> Self {
        Self {
            runner_info,
            lanes: Vec::new(),
            y_start: 0.0,
            height: 0.0,
        }
    }

    /// Whether this group has distinct child worker lanes.
    pub fn has_children(&self) -> bool {
        self.lanes.len() > 1
            && self.lanes.iter().any(|lane| {
                lane.worker_info
                    .as_ref()
                    .is_some_and(super::runner_info::RunnerInfo::has_parent)
            })
    }

    /// Additional height reserved at top for the parent header in multi-worker groups.
    /// Uses the same lane_height for visual consistency.
    fn header_height(&self, lane_height: f64) -> f64 {
        if self.has_children() {
            lane_height
        } else {
            0.0
        }
    }

    /// Compute the height of this group based on lane count and config.
    pub fn compute_height(&mut self, lane_height: f64, lane_padding: f64) {
        let n = self.lanes.len().max(1) as f64;
        self.height =
            self.header_height(lane_height) + n * lane_height + (n - 1.0).max(0.0) * lane_padding;
    }

    /// Assign Y offsets to each lane within this group.
    pub fn layout_lanes(&mut self, lane_height: f64, lane_padding: f64) {
        let header = self.header_height(lane_height);
        for (i, lane) in self.lanes.iter_mut().enumerate() {
            lane.y_offset = self.y_start + header + i as f64 * (lane_height + lane_padding);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a RunnerInfo that looks like a worker (has parent).
    fn worker_info(worker_id: &str, parent_id: &str) -> RunnerInfo {
        RunnerInfo {
            runner_cls: "PersistentTokioWorker".to_owned(),
            runner_id: worker_id.to_owned(),
            hostname: "host".to_owned(),
            pid: 1234,
            thread_id: 1,
            parent_runner_cls: Some("PersistentTokioRunner".to_owned()),
            parent_runner_id: Some(parent_id.to_owned()),
        }
    }

    fn make_runner_info(runner_id: &str, cls: &str) -> RunnerInfo {
        RunnerInfo {
            runner_cls: cls.to_owned(),
            runner_id: runner_id.to_owned(),
            hostname: "host".to_owned(),
            pid: 1234,
            thread_id: 0,
            parent_runner_cls: None,
            parent_runner_id: None,
        }
    }

    /// Build a single-lane group (no workers).
    fn single_lane_group(runner_id: &str, cls: &str) -> LaneGroup {
        let mut g = LaneGroup::new(make_runner_info(runner_id, cls));
        g.lanes.push(RunnerLane::new());
        g
    }

    /// Build a multi-worker group with N worker lanes.
    fn multi_worker_group(runner_id: &str, cls: &str, worker_count: usize) -> LaneGroup {
        let mut g = LaneGroup::new(make_runner_info(runner_id, cls));
        for i in 0..worker_count {
            let mut lane = RunnerLane::new();
            lane.worker_info = Some(worker_info(&format!("worker-{i}"), runner_id));
            g.lanes.push(lane);
        }
        g
    }

    #[test]
    fn single_lane_group_height_equals_lane_height() {
        let lane_height = 32.0;
        let lane_padding = 4.0;
        let mut g = single_lane_group("r1", "PersistentTokioRunner");
        g.compute_height(lane_height, lane_padding);
        assert_eq!(g.height, lane_height);
    }

    #[test]
    fn multi_worker_group_has_header_row() {
        let lane_height = 32.0;
        let lane_padding = 4.0;
        let mut g = multi_worker_group("r1", "PersistentTokioRunner", 3);
        g.compute_height(lane_height, lane_padding);
        // header (32) + 3 lanes (3×32) + 2 paddings (2×4) = 136
        let expected = lane_height + 3.0 * lane_height + 2.0 * lane_padding;
        assert_eq!(g.height, expected);
    }

    #[test]
    fn worker_lanes_start_after_header() {
        let lane_height = 32.0;
        let lane_padding = 4.0;
        let mut g = multi_worker_group("r1", "PersistentTokioRunner", 2);
        g.y_start = 100.0;
        g.compute_height(lane_height, lane_padding);
        g.layout_lanes(lane_height, lane_padding);
        assert_eq!(g.lanes[0].y_offset, 100.0 + lane_height);
        assert_eq!(
            g.lanes[1].y_offset,
            100.0 + lane_height + lane_height + lane_padding
        );
    }

    #[test]
    fn all_lane_row_heights_consistent_across_groups() {
        let lane_height = 32.0;
        let lane_padding = 4.0;
        let group_spacing = lane_height * 0.5;

        let mut groups = vec![
            single_lane_group("r1", "PersistentTokioRunner"),
            multi_worker_group("r2", "PersistentTokioRunner", 3),
            single_lane_group("r3", "RayonRunner"),
            multi_worker_group("r4", "PerInvocationTokioRunner", 2),
            single_lane_group("r5", "ExternalRunner"),
        ];

        // Simulate compute_layout
        let mut y = 40.0; // top_margin
        for group in &mut groups {
            group.compute_height(lane_height, lane_padding);
            group.y_start = y;
            group.layout_lanes(lane_height, lane_padding);
            y += group.height + group_spacing;
        }

        // Collect all lane y_offsets across all groups
        let all_offsets: Vec<f64> = groups
            .iter()
            .flat_map(|g| g.lanes.iter().map(|l| l.y_offset))
            .collect();

        // Every gap between consecutive lanes should be one of:
        // - within_group: lane_height + lane_padding (lanes in the same group)
        // - between_groups_no_header: lane_height + group_spacing (next group has no header)
        // - between_groups_with_header: lane_height + group_spacing + lane_height (next group has header)
        let within_group = lane_height + lane_padding;
        let between_no_header = lane_height + group_spacing;
        let between_with_header = lane_height + group_spacing + lane_height;
        for window in all_offsets.windows(2) {
            let gap = window[1] - window[0];
            assert!(
                (gap - within_group).abs() < 0.01
                    || (gap - between_no_header).abs() < 0.01
                    || (gap - between_with_header).abs() < 0.01,
                "Inconsistent lane gap: {gap:.1}, expected {within_group}, {between_no_header}, or {between_with_header}"
            );
        }
    }

    #[test]
    fn group_separator_spacing_is_consistent() {
        let lane_height = 32.0;
        let lane_padding = 4.0;
        let group_spacing = lane_height * 0.5;

        let mut groups = vec![
            single_lane_group("r1", "PersistentTokioRunner"),
            multi_worker_group("r2", "PersistentTokioRunner", 2),
            single_lane_group("r3", "RayonRunner"),
        ];

        let mut y = 40.0;
        for group in &mut groups {
            group.compute_height(lane_height, lane_padding);
            group.y_start = y;
            group.layout_lanes(lane_height, lane_padding);
            y += group.height + group_spacing;
        }

        for pair in groups.windows(2) {
            let sep = pair[0].y_start + pair[0].height;
            let next_start = pair[1].y_start;
            assert_eq!(
                next_start - sep,
                group_spacing,
                "Gap between group separator and next group start should be {group_spacing}"
            );
        }
    }

    #[test]
    fn single_lane_has_no_children() {
        let g = single_lane_group("r1", "PersistentTokioRunner");
        assert!(!g.has_children());
    }

    #[test]
    fn multi_worker_has_children() {
        let g = multi_worker_group("r1", "PersistentTokioRunner", 2);
        assert!(g.has_children());
    }
}
