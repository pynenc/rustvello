//! Top-level SVG renderer orchestrating all sub-renderers.

use std::fmt::Write;

use super::data::TimelineData;
use super::{render_axis, render_elements, render_lanes};

/// Renders `TimelineData` into a complete SVG string.
pub struct TimelineSvgRenderer;

impl TimelineSvgRenderer {
    /// Render the timeline data into an SVG string.
    pub fn render(data: &TimelineData) -> String {
        let width = data.config.width;
        let height = data.total_height;

        let mut buf = String::with_capacity(8192);

        // SVG header — responsive with viewBox and machine-readable bounds for
        // the browser's drag-to-zoom interaction. Leave height intrinsic so a
        // wide responsive SVG does not reserve a fixed-height blank viewport.
        let _ = write!(
            buf,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {width} {height}" preserveAspectRatio="xMinYMin meet" data-timeline-start="{start}" data-timeline-end="{end}" data-timeline-left="{left:.1}" data-timeline-right="{right:.1}" style="font-family: system-ui, -apple-system, sans-serif;">"#,
            start = data.bounds.start.to_rfc3339(),
            end = data.bounds.end.to_rfc3339(),
            left = data.bounds.left_margin,
            right = data.bounds.left_margin + data.bounds.drawable_width,
        );

        // Background
        let _ = write!(
            buf,
            r#"<rect width="{width}" height="{height}" fill="white"/>"#,
        );

        // Defs: shadow filter, label clip path, ongoing stripes pattern
        let label_clip_w = data.config.left_margin - 10.0;
        let _ = write!(
            buf,
            concat!(
                r#"<defs>"#,
                r#"<filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">"#,
                r#"<feDropShadow dx="0" dy="1" stdDeviation="1" flood-opacity="0.15"/>"#,
                r#"</filter>"#,
                r#"<clipPath id="label-clip"><rect x="0" y="0" width="{}" height="100%"/></clipPath>"#,
                r#"<pattern id="ongoing-stripes" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)">"#,
                r#"<line x1="0" y1="0" x2="0" y2="8" stroke="rgba(255,255,255,0.3)" stroke-width="4"/>"#,
                r#"</pattern>"#,
                r#"</defs>"#,
            ),
            label_clip_w,
        );

        // Grid lines
        render_axis::render_grid(&mut buf, &data.config, &data.bounds, data.total_height);

        // Time axis labels
        render_axis::render_time_axis(&mut buf, &data.config, &data.bounds);

        // Lane group backgrounds
        render_lanes::render_group_containers(&mut buf, &data.config, &data.groups);

        // Group separators
        render_lanes::render_group_separators(&mut buf, &data.config, &data.groups);

        // Lane labels
        render_lanes::render_lane_labels(&mut buf, &data.config, &data.groups);

        // Connecting lines (behind segments/points)
        render_elements::render_lines(&mut buf, &data.config, &data.groups);

        // Cross-lane connecting lines
        render_elements::render_global_lines(&mut buf, &data.config, &data.global_lines);

        // Segment bars
        render_elements::render_segments(&mut buf, &data.config, &data.groups);

        // Status points (on top)
        render_elements::render_points(&mut buf, &data.config, &data.groups);

        // Legend
        render_axis::render_legend(&mut buf, &data.config, data.total_height);

        // Close SVG
        buf.push_str("</svg>");

        buf
    }
}
