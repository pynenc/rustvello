//! Timeline data container with computed layout.

use super::bounds::TimelineBounds;
use super::config::TimelineConfig;
use super::lane::LaneGroup;
use super::models::StatusLine;

/// The assembled timeline data ready for rendering.
#[derive(Debug)]
pub struct TimelineData {
    pub config: TimelineConfig,
    pub bounds: TimelineBounds,
    pub groups: Vec<LaneGroup>,
    /// Cross-lane connecting lines (absolute Y coordinates).
    pub global_lines: Vec<StatusLine>,
    /// Total computed SVG height.
    pub total_height: f64,
}

impl TimelineData {
    pub fn new(config: TimelineConfig, bounds: TimelineBounds, mut groups: Vec<LaneGroup>) -> Self {
        let total_height = Self::compute_layout(&config, &mut groups);
        Self {
            config,
            bounds,
            groups,
            global_lines: Vec::new(),
            total_height,
        }
    }

    /// Compute Y positions for all lane groups, returning total SVG height.
    fn compute_layout(config: &TimelineConfig, groups: &mut [LaneGroup]) -> f64 {
        let mut y = config.top_margin;
        let group_spacing = config.lane_height * 0.5;

        for group in groups.iter_mut() {
            group.compute_height(config.lane_height, config.lane_padding);
            group.y_start = y;
            group.layout_lanes(config.lane_height, config.lane_padding);
            y += group.height + group_spacing;
        }

        y + config.bottom_margin
    }
}
