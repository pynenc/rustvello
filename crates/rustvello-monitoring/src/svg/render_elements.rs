//! Rendering of visual elements: segments, points, and connecting lines.

use std::fmt::Write;

use super::config::TimelineConfig;
use super::lane::LaneGroup;
use super::models::StatusLine;
use crate::util::escape::xml_escape;

/// Render all status segment bars across all lane groups.
pub fn render_segments(buf: &mut String, _config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        for lane in &group.lanes {
            let y_offset = lane.y_offset;
            for seg in &lane.segments {
                let y = seg.y + y_offset;
                let _ = write!(
                    buf,
                    r#"<rect x="{x:.1}" y="{y:.1}" width="{w:.1}" height="{h:.1}" fill="{color}" rx="3" opacity="0.9" filter="url(#shadow)" data-invocation-id="{inv_id}" data-task-key="{task_key}" style="cursor:pointer">"#,
                    x = seg.x,
                    w = seg.width,
                    h = seg.height,
                    color = seg.color,
                    inv_id = xml_escape(&seg.invocation_id),
                    task_key = xml_escape(&seg.task_id),
                );
                let _ = write!(buf, r#"<title>{}</title>"#, xml_escape(&seg.tooltip));
                let _ = write!(buf, "</rect>");
                // Ongoing segment: overlay diagonal stripes pattern
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
    }
}

/// Render all status transition point circles.
pub fn render_points(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        for lane in &group.lanes {
            let y_offset = lane.y_offset;
            for point in &lane.points {
                let cy = point.y + y_offset;
                let _ = write!(
                    buf,
                    r#"<circle cx="{cx:.1}" cy="{cy:.1}" r="{r}" fill="{color}" stroke="white" stroke-width="1.5" data-invocation-id="{inv_id}" data-task-key="{task_key}" data-status="{status}" style="cursor:pointer">"#,
                    cx = point.x,
                    r = config.point_radius,
                    color = point.color,
                    inv_id = xml_escape(&point.invocation_id),
                    task_key = xml_escape(&point.task_id),
                    status = xml_escape(&format!("{:?}", point.status)),
                );
                let _ = write!(buf, r#"<title>{}</title>"#, xml_escape(&point.tooltip));
                let _ = write!(buf, "</circle>");
            }
        }
    }
}

/// Render connecting lines between consecutive elements.
pub fn render_lines(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        for lane in &group.lanes {
            let y_offset = lane.y_offset;
            for line in &lane.lines {
                let _ = write!(
                    buf,
                    r#"<line x1="{x1:.1}" y1="{y1:.1}" x2="{x2:.1}" y2="{y2:.1}" stroke="{color}" stroke-width="{sw}" opacity="0.6" stroke-dasharray="4,2" data-invocation-id="{inv_id}" class="relation-line"/>"#,
                    x1 = line.x1,
                    y1 = line.y1 + y_offset,
                    x2 = line.x2,
                    y2 = line.y2 + y_offset,
                    color = line.color,
                    sw = config.line_stroke_width,
                    inv_id = xml_escape(&line.invocation_id),
                );
            }
        }
    }
}

/// Render cross-lane connecting lines (absolute Y coordinates).
///
/// These connect consecutive elements of the same invocation across
/// different runner lanes (e.g. Registered on ExternalRunner → Running on TaskRunner).
pub fn render_global_lines(buf: &mut String, config: &TimelineConfig, lines: &[StatusLine]) {
    for line in lines {
        let _ = write!(
            buf,
            r#"<line x1="{x1:.1}" y1="{y1:.1}" x2="{x2:.1}" y2="{y2:.1}" stroke="{color}" stroke-width="{sw}" opacity="0.5" stroke-dasharray="6,3" data-invocation-id="{inv_id}" class="relation-line"/>"#,
            x1 = line.x1,
            y1 = line.y1,
            x2 = line.x2,
            y2 = line.y2,
            color = line.color,
            sw = config.line_stroke_width,
            inv_id = xml_escape(&line.invocation_id),
        );
    }
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
            task_id: "test.task".to_owned(),
        }
    }

    fn wrap_in_group(seg: StatusSegment) -> Vec<LaneGroup> {
        let mut lane = RunnerLane::new();
        lane.y_offset = 0.0;
        lane.segments.push(seg);
        vec![LaneGroup {
            runner_info: RunnerInfo::from_id("runner1"),
            lanes: vec![lane],
            y_start: 0.0,
            height: 32.0,
        }]
    }

    #[test]
    fn test_segment_has_shadow_filter() {
        let groups = wrap_in_group(make_segment(false));
        let config = TimelineConfig::default();
        let mut buf = String::new();
        render_segments(&mut buf, &config, &groups);
        assert!(buf.contains(r#"filter="url(#shadow)""#));
        assert!(buf.contains(r#"opacity="0.9""#));
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
            task_id: "test.task".to_owned(),
        });
        let groups = vec![LaneGroup {
            runner_info: RunnerInfo::from_id("runner1"),
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
            lanes: vec![lane],
            y_start: 0.0,
            height: 32.0,
        }];
        let mut buf = String::new();
        render_lines(&mut buf, &config, &groups);
        assert!(buf.contains(r#"stroke-dasharray="4,2""#));
    }
}
