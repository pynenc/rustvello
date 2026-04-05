//! Lane background, group headers, and label rendering.

use std::fmt::Write;

use super::config::TimelineConfig;
use super::lane::LaneGroup;
use crate::util::escape::xml_escape;

/// Color palette for runner groups (matching pynenc host color palette).
const GROUP_COLORS: &[&str] = &[
    "#3498db", "#e74c3c", "#2ecc71", "#9b59b6", "#f39c12", "#1abc9c", "#e67e22", "#2c3e50",
];

/// Get a color for a group by index.
fn group_color(index: usize) -> &'static str {
    GROUP_COLORS[index % GROUP_COLORS.len()]
}

/// Render lane group containers with tinted backgrounds (matching pynenc style).
pub fn render_group_containers(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    let white = "#ffffff";
    for (gi, group) in groups.iter().enumerate() {
        let color = group_color(gi);
        let runner_id = xml_escape(&group.runner_info.runner_id);

        // Header row background for multi-worker groups
        if group.has_children() {
            let header_h = config.lane_height;
            let w = config.width;
            let y = group.y_start;
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{header_h}" fill="{white}" data-runner-id="{runner_id}" class="lane-bg"/>"#,
            );
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{header_h}" fill="{color}" opacity="0.05" data-runner-id="{runner_id}" class="lane-bg"/>"#,
            );
        }

        // Render each lane within the group with alternating opacity
        for (li, lane) in group.lanes.iter().enumerate() {
            let lane_h = config.lane_height;
            let y = lane.y_offset;
            let w = config.width;
            let opacity = if li % 2 == 0 { 0.08 } else { 0.15 };
            // Use worker ID when available so hover-highlighting targets the
            // individual worker lane, not the whole parent group.
            let lane_rid = lane
                .worker_info
                .as_ref()
                .map_or_else(|| runner_id.clone(), |wi| xml_escape(&wi.runner_id));
            // White base
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{lane_h}" fill="{white}" data-runner-id="{lane_rid}" class="lane-bg"/>"#,
            );
            // Tinted overlay
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{lane_h}" fill="{color}" opacity="{opacity}" data-runner-id="{lane_rid}" class="lane-bg"/>"#,
            );
        }
    }
}

/// Render runner labels on the left side with clip-path.
///
/// For single-lane groups: one label centered on the group.
/// For multi-worker groups: parent label at top, then per-lane worker labels
/// (matching pynmon's hierarchical display).
pub fn render_lane_labels(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    // Wrap labels in clip-path group to prevent overflow into timeline area
    let _ = write!(buf, r#"<g clip-path="url(#label-clip)">"#);

    for (gi, group) in groups.iter().enumerate() {
        let color = group_color(gi);
        let x = config.left_margin - 10.0;

        // Check if this group has distinct workers (child lanes with their own RunnerInfo)
        let has_children = group.has_children();

        if has_children && group.lanes.len() > 1 {
            // Multi-worker group: parent header + per-lane worker labels
            let runner_id = xml_escape(&group.runner_info.runner_id);

            // Parent label at top of group
            let header_y = group.y_start + 14.0;
            let label = xml_escape(&group.runner_info.label());
            let _ = write!(
                buf,
                "<text x=\"{x}\" y=\"{header_y:.1}\" text-anchor=\"end\" font-size=\"12\" fill=\"{color}\" font-weight=\"bold\" class=\"runner-label\" data-runner-id=\"{runner_id}\" style=\"cursor:pointer\">{label}</text>",
            );

            // Parent details below header
            let details = group.runner_info.details();
            if !details.is_empty() {
                let details_label = xml_escape(&details);
                let details_y = header_y + 12.0;
                let _ = write!(
                    buf,
                    "<text x=\"{x}\" y=\"{details_y:.1}\" text-anchor=\"end\" font-size=\"9\" fill=\"#888\">{details_label}</text>",
                );
            }

            // Per-lane worker labels
            for lane in &group.lanes {
                if let Some(ref worker_info) = lane.worker_info {
                    let worker_label = xml_escape(&worker_info.label());
                    let worker_rid = xml_escape(&worker_info.runner_id);
                    let lane_center_y = lane.y_offset + config.lane_height / 2.0;

                    // Worker label (lighter weight, slightly indented)
                    let _ = write!(
                        buf,
                        "<text x=\"{x}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"10\" fill=\"{color}\" font-weight=\"normal\" class=\"runner-label\" data-runner-id=\"{worker_rid}\" style=\"cursor:pointer\">{worker_label}</text>",
                        lane_center_y - 1.0,
                    );

                    // Worker details: show only what differs from parent
                    let worker_details = format_child_details(worker_info, &group.runner_info);
                    if !worker_details.is_empty() {
                        let wd = xml_escape(&worker_details);
                        let _ = write!(
                            buf,
                            "<text x=\"{x}\" y=\"{:.1}\" text-anchor=\"end\" font-size=\"8\" fill=\"#aaa\">{wd}</text>",
                            lane_center_y + 9.0,
                        );
                    }
                }
            }
        } else {
            // Single runner or single-lane group: centered label
            let runner_id = xml_escape(&group.runner_info.runner_id);

            let label_y = group.y_start + group.height / 2.0 + 4.0;
            let label = xml_escape(&group.runner_info.label());
            let _ = write!(
                buf,
                "<text x=\"{x}\" y=\"{label_y:.1}\" text-anchor=\"end\" font-size=\"12\" fill=\"{color}\" font-weight=\"bold\" class=\"runner-label\" data-runner-id=\"{runner_id}\" style=\"cursor:pointer\">{label}</text>",
            );

            // Details line
            let details = group.runner_info.details();
            if !details.is_empty() {
                let details_label = xml_escape(&details);
                let details_y = label_y + 13.0;
                let _ = write!(
                    buf,
                    "<text x=\"{x}\" y=\"{details_y:.1}\" text-anchor=\"end\" font-size=\"10\" fill=\"#888\">{details_label}</text>",
                );
            }

            // Sub-lane count
            if group.lanes.len() > 1 {
                let count_y = if !details.is_empty() {
                    label_y + 25.0
                } else {
                    label_y + 13.0
                };
                let count_label = format!("{} concurrent", group.lanes.len());
                let _ = write!(
                    buf,
                    "<text x=\"{x}\" y=\"{count_y:.1}\" text-anchor=\"end\" font-size=\"9\" fill=\"#aaa\" font-style=\"italic\">{count_label}</text>",
                );
            }
        }
    }

    let _ = write!(buf, "</g>");
}

/// Format child worker details, showing only what differs from the parent.
/// Matches pynmon's `format_child_details` behavior.
fn format_child_details(
    child: &super::runner_info::RunnerInfo,
    parent: &super::runner_info::RunnerInfo,
) -> String {
    let mut parts = Vec::new();
    if child.hostname != parent.hostname && !child.hostname.is_empty() {
        parts.push(child.hostname.clone());
    }
    if child.pid != parent.pid && child.pid != 0 {
        parts.push(format!("pid:{}", child.pid));
    }
    if child.thread_id != 0 {
        parts.push(format!("thread:{}", child.thread_id));
    }
    parts.join(" ")
}

/// Render horizontal separator lines between groups.
pub fn render_group_separators(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    for group in groups {
        let y = group.y_start + group.height;
        let stroke = "#ddd";
        let x2 = config.width;
        let _ = write!(
            buf,
            "<line x1=\"0\" y1=\"{y:.1}\" x2=\"{x2}\" y2=\"{y:.1}\" stroke=\"{stroke}\" stroke-width=\"0.5\"/>",
        );
    }
}
