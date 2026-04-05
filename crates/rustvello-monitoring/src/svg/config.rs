//! Timeline rendering configuration.

/// Layout and sizing configuration for the timeline SVG.
#[derive(Debug, Clone)]
pub struct TimelineConfig {
    /// Total SVG width in pixels.
    pub width: f64,
    /// Height of each lane row.
    pub lane_height: f64,
    /// Left margin for labels.
    pub left_margin: f64,
    /// Right margin.
    pub right_margin: f64,
    /// Top margin for axis.
    pub top_margin: f64,
    /// Bottom margin for legend.
    pub bottom_margin: f64,
    /// Vertical padding between elements.
    pub lane_padding: f64,
    /// Radius for status point circles.
    pub point_radius: f64,
    /// Stroke width for connecting lines.
    pub line_stroke_width: f64,
    /// Height of segment bars.
    pub segment_height: f64,
    /// Minimum visible segment width in pixels.
    pub min_segment_width: f64,
}

impl Default for TimelineConfig {
    fn default() -> Self {
        Self {
            width: 2000.0,
            lane_height: 32.0,
            left_margin: 320.0,
            right_margin: 20.0,
            top_margin: 40.0,
            bottom_margin: 70.0,
            lane_padding: 4.0,
            point_radius: 5.0,
            line_stroke_width: 1.5,
            segment_height: 20.0,
            min_segment_width: 2.0,
        }
    }
}

impl TimelineConfig {
    /// Drawable area width (between margins).
    pub fn drawable_width(&self) -> f64 {
        self.width - self.left_margin - self.right_margin
    }
}
