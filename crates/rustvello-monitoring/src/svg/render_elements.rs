//! Rendering of visual elements: segments, points, and connecting lines.

use std::collections::BTreeMap;
use std::fmt::Write;

use super::config::TimelineConfig;
use super::lane::LaneGroup;
use super::models::StatusLine;
use crate::util::escape::xml_escape;

/// Render all status segment bars across all lane groups.
pub fn render_segments(buf: &mut String, _config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        if let Some(lane) = &group.control_plane {
            render_lane_segments(buf, lane);
        }
        for lane in &group.lanes {
            render_lane_segments(buf, lane);
        }
    }
}

fn render_lane_segments(buf: &mut String, lane: &super::lane::RunnerLane) {
    let y_offset = lane.y_offset;
    for seg in &lane.segments {
        let y = seg.y + y_offset;
        let _ = write!(
            buf,
            r#"<rect x="{x:.1}" y="{y:.1}" width="{w:.1}" height="{h:.1}" fill="{color}" rx="2" opacity="0.92" data-invocation-id="{inv_id}" data-task-key="{task_key}" data-start="{start}" data-end="{end}" style="cursor:pointer">"#,
            x = seg.x,
            w = seg.width,
            h = seg.height,
            color = seg.color,
            inv_id = xml_escape(&seg.invocation_id),
            task_key = xml_escape(&seg.task_id),
            start = seg.start.to_rfc3339(),
            end = seg.end.to_rfc3339(),
        );
        let _ = write!(buf, r#"<title>{}</title>"#, xml_escape(&seg.tooltip));
        let _ = write!(buf, "</rect>");
        if seg.is_ongoing {
            let _ = write!(
                buf,
                r#"<rect x="{x:.1}" y="{y:.1}" width="{w:.1}" height="{h:.1}" fill="url(#ongoing-stripes)" rx="3" pointer-events="none"/>"#,
                x = seg.x,
                w = seg.width,
                h = seg.height,
            );
        }
    }
}

/// Render all status transition point circles.
pub fn render_points(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        if let Some(lane) = &group.control_plane {
            render_lane_points(buf, config, lane);
        }
        for lane in &group.lanes {
            render_lane_points(buf, config, lane);
        }
    }
}

fn render_lane_points(buf: &mut String, config: &TimelineConfig, lane: &super::lane::RunnerLane) {
    let y_offset = lane.y_offset;
    for point in &lane.points {
        let cy = point.y + y_offset;
        let _ = write!(
            buf,
            r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{r}" fill="{color}" stroke="white" stroke-width="1.5" data-invocation-id="{inv_id}" data-task-key="{task_key}" data-status="{status}" data-timestamp="{timestamp}" style="cursor:pointer">"#,
            cx = point.x,
            r = config.point_radius,
            color = point.color,
            inv_id = xml_escape(&point.invocation_id),
            task_key = xml_escape(&point.task_id),
            status = xml_escape(&format!("{:?}", point.status)),
            timestamp = point.timestamp.to_rfc3339(),
        );
        let _ = write!(buf, r#"<title>{}</title>"#, xml_escape(&point.tooltip));
        let _ = write!(buf, "</circle>");
    }
}

/// Render one SVG path per invocation for lane-local and cross-lane relations.
pub fn render_relation_paths(
    buf: &mut String,
    config: &TimelineConfig,
    groups: &[LaneGroup],
    global_lines: &[StatusLine],
) {
    let mut paths = BTreeMap::<String, (String, String)>::new();
    for group in groups {
        if let Some(lane) = &group.control_plane {
            let y_offset = lane.y_offset;
            for line in &lane.lines {
                append_path_segment(&mut paths, line, line.y1 + y_offset, line.y2 + y_offset);
            }
        }
        for lane in &group.lanes {
            let y_offset = lane.y_offset;
            for line in &lane.lines {
                append_path_segment(&mut paths, line, line.y1 + y_offset, line.y2 + y_offset);
            }
        }
    }
    for line in global_lines {
        append_path_segment(&mut paths, line, line.y1, line.y2);
    }
    for (invocation_id, (color, path)) in paths {
        let _ = write!(
            buf,
            r#"<path d="{path}" fill="none" stroke="{color}" stroke-width="{sw}" stroke-dasharray="4,2" data-invocation-id="{inv_id}" class="relation-line"/>"#,
            sw = config.line_stroke_width,
            inv_id = xml_escape(&invocation_id),
        );
    }
}

fn append_path_segment(
    paths: &mut BTreeMap<String, (String, String)>,
    line: &StatusLine,
    y1: f64,
    y2: f64,
) {
    let (color, path) = paths
        .entry(line.invocation_id.clone())
        .or_insert_with(|| (line.color.clone(), String::new()));
    if color.is_empty() {
        *color = line.color.clone();
    }
    let _ = write!(path, "M{:.1},{y1:.1} L{:.1},{y2:.1} ", line.x1, line.x2);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::svg::lane::{LaneGroup, RunnerLane};
    use crate::svg::models::StatusSegment;
    use crate::svg::runner_info::RunnerInfo;
    use chrono::Utc;
    use rustvello_proto::status::InvocationStatus;

    fn make_segment(is_ongoing: bool) -> StatusSegment {
        let now = Utc::now();
        StatusSegment {
            invocation_id: "inv1".to_owned(),
            status: InvocationStatus::Running,
            start: now,
            end: now + chrono::Duration::seconds(10),
            x: 100.0,
            width: 200.0,
            y: 10.0,
            height: 20.0,
            color: "#3498db".to_owned(),
            tooltip: "test segment".to_owned(),
            is_ongoing,
            task_id: "rust::test.task".to_owned(),
        }
    }

    fn wrap_in_group(seg: StatusSegment) -> Vec<LaneGroup> {
        let mut lane = RunnerLane::new();
        lane.y_offset = 0.0;
        lane.segments.push(seg);
        vec![LaneGroup {
            runner_info: RunnerInfo::from_id("runner1"),
            control_plane: None,
            lanes: vec![lane],
            y_start: 0.0,
            height: 32.0,
        }]
    }

    #[test]
    fn test_segment_uses_flat_rendering() {
        let groups = wrap_in_group(make_segment(false));
        let config = TimelineConfig::default();
        let mut buf = String::new();
        render_segments(&mut buf, &config, &groups);
        assert!(!buf.contains("filter="));
        assert!(buf.contains(r#"opacity="0.92""#));
    }

    #[test]
    fn test_ongoing_segment_has_stripe_overlay() {
        let groups = wrap_in_group(make_segment(true));
        let config = TimelineConfig::default();
        let mut buf = String::new();
        render_segments(&mut buf, &config, &groups);
        assert!(buf.contains(r#"fill="url(#ongoing-stripes)""#));
    }

    #[test]
    fn test_non_ongoing_segment_has_no_stripe_overlay() {
        let groups = wrap_in_group(make_segment(false));
        let config = TimelineConfig::default();
        let mut buf = String::new();
        render_segments(&mut buf, &config, &groups);
        assert!(!buf.contains("ongoing-stripes"));
    }

    #[test]
    fn test_points_have_stroke_width_1_5() {
        let config = TimelineConfig::default();
        let mut lane = RunnerLane::new();
        lane.y_offset = 0.0;
        lane.points.push(crate::svg::models::StatusPoint {
            invocation_id: "inv1".to_owned(),
            status: InvocationStatus::Success,
            timestamp: Utc::now(),
            x: 150.0,
            y: 20.0,
            color: "#27ae60".to_owned(),
            tooltip: "test point".to_owned(),
            task_id: "rust::test.task".to_owned(),
        });
        let groups = vec![LaneGroup {
            runner_info: RunnerInfo::from_id("runner1"),
            control_plane: None,
            lanes: vec![lane],
            y_start: 0.0,
            height: 32.0,
        }];
        let mut buf = String::new();
        render_points(&mut buf, &config, &groups);
        assert!(buf.contains(r#"stroke-width="1.5""#));
    }

    #[test]
    fn test_lines_have_dash_array() {
        let config = TimelineConfig::default();
        let mut lane = RunnerLane::new();
        lane.y_offset = 0.0;
        lane.lines.push(crate::svg::models::StatusLine {
            invocation_id: "inv1".to_owned(),
            x1: 100.0,
            y1: 20.0,
            x2: 200.0,
            y2: 20.0,
            color: "#3498db".to_owned(),
        });
        let groups = vec![LaneGroup {
            runner_info: RunnerInfo::from_id("runner1"),
            control_plane: None,
            lanes: vec![lane],
            y_start: 0.0,
            height: 32.0,
        }];
        let mut buf = String::new();
        render_relation_paths(&mut buf, &config, &groups, &[]);
        assert!(buf.contains(r#"stroke-dasharray="4,2""#));
        assert!(buf.contains("<path"));
    }
}
