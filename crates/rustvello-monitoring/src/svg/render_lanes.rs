//! Lane background, group headers, and label rendering.

use std::fmt::Write;

use super::config::TimelineConfig;
use super::lane::LaneGroup;
use crate::util::escape::xml_escape;

fn runtime_color(language: &str) -> &'static str {
    match language {
        "rust" => "#ce422b",
        "python" => "#3776ab",
        "external" => "#6c757d",
        _ => "#6c757d",
    }
}

struct RunnerPalette {
    strong: &'static str,
    tone_a: &'static str,
    tone_b: &'static str,
}

fn runner_palette(index: usize) -> RunnerPalette {
    const PALETTE: [[&str; 3]; 12] = [
        ["#245c9c", "#377eb8", "#8cbfe6"],
        ["#237a3b", "#42a75b", "#96d39b"],
        ["#a65408", "#df7d16", "#f5ba70"],
        ["#71407f", "#a25fb2", "#d2a0da"],
        ["#196c6b", "#329d98", "#8bd0c8"],
        ["#a12d31", "#d84c50", "#f2a0a1"],
        ["#704434", "#a66c50", "#d6a88f"],
        ["#806707", "#b89913", "#e1cb68"],
        ["#1a658b", "#328fb9", "#88c6df"],
        ["#832650", "#be4f7f", "#e5a0bb"],
        ["#4e6521", "#75943b", "#b3cc78"],
        ["#176a63", "#348f86", "#89c8bc"],
    ];
    let colors = PALETTE[index % PALETTE.len()];
    RunnerPalette {
        strong: colors[0],
        tone_a: colors[1],
        tone_b: colors[2],
    }
}

/// Render lane group containers with tinted backgrounds (matching pynenc style).
pub fn render_group_containers(buf: &mut String, config: &TimelineConfig, groups: &[LaneGroup]) {
    let white = "#ffffff";
    for (group_index, group) in groups.iter().enumerate() {
        let palette = runner_palette(group_index);
        let color = palette.strong;
        let runner_id = xml_escape(&group.runner_info.runner_id);

        // Header/control-plane background for multi-worker groups and
        // atomic-only windows.
        if group.has_children() || group.has_control_plane() {
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
            let opacity = if li % 2 == 0 { 0.055 } else { 0.09 };
            // Use worker ID when available so hover-highlighting targets the
            // individual worker lane, not the whole parent group.
            let lane_rid = lane
                .worker_info
                .as_ref()
                .map_or_else(|| runner_id.clone(), |wi| xml_escape(&wi.runner_id));
            let lane_color = if li % 2 == 0 {
                palette.tone_a
            } else {
                palette.tone_b
            };
            // White base
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{lane_h}" fill="{white}" data-runner-id="{lane_rid}" class="lane-bg"/>"#,
            );
            // Tinted overlay
            let _ = write!(
                buf,
                r#"<rect x="0" y="{y:.1}" width="{w}" height="{lane_h}" fill="{lane_color}" opacity="{opacity}" data-runner-id="{lane_rid}" class="lane-bg"/><rect x="0" y="{y:.1}" width="4" height="{lane_h}" fill="{lane_color}" opacity="0.8" pointer-events="none"/>"#,
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

    for (group_index, group) in groups.iter().enumerate() {
        let palette = runner_palette(group_index);
        let color = palette.strong;

        // Check if this group has distinct workers (child lanes with their own RunnerInfo)
        let has_children = group.has_children();

        if has_children && group.lanes.len() > 1 {
            // Multi-worker group: parent header + per-lane worker labels
            let runner_id = xml_escape(&group.runner_info.runner_id);

            let header_y = group.y_start + config.lane_height / 2.0 + 3.0;
            let label = xml_escape(&format_runner_line(&group.runner_info));
            let text_x = render_language_badge(buf, &group.runner_info, header_y);
            let _ = write!(
                buf,
                "<text x=\"{text_x:.1}\" y=\"{header_y:.1}\" font-size=\"10\" fill=\"{color}\" font-weight=\"700\" class=\"runner-label runner-parent-label\" data-runner-id=\"{runner_id}\">{label}</text>",
            );

            // Per-lane worker labels
            for (index, lane) in group.lanes.iter().enumerate() {
                if let Some(ref worker_info) = lane.worker_info {
                    let worker_label =
                        xml_escape(&format_worker_line(worker_info, &group.runner_info));
                    let worker_rid = xml_escape(&worker_info.runner_id);
                    let lane_center_y = lane.y_offset + config.lane_height / 2.0;
                    let text_y = lane_center_y + 3.0;
                    let worker_color = if index % 2 == 0 {
                        palette.tone_a
                    } else {
                        palette.tone_b
                    };
                    let text_x = 26.0;

                    let _ = write!(
                        buf,
                        "<text x=\"{text_x:.1}\" y=\"{text_y:.1}\" font-size=\"9\" fill=\"{worker_color}\" font-weight=\"600\" class=\"runner-label worker-label\" data-runner-id=\"{worker_rid}\">{worker_label}</text>",
                    );
                }
            }
        } else {
            // Single runner or single-lane group: centered label
            let runner_id = xml_escape(&group.runner_info.runner_id);

            let label_y = group.y_start + group.height / 2.0 + 3.0;
            let label = xml_escape(&format_runner_line(&group.runner_info));
            let text_x = render_language_badge(buf, &group.runner_info, label_y);
            let _ = write!(
                buf,
                "<text x=\"{text_x:.1}\" y=\"{label_y:.1}\" font-size=\"10\" fill=\"{color}\" font-weight=\"700\" class=\"runner-label runner-parent-label\" data-runner-id=\"{runner_id}\">{label}</text>",
            );

            // Sub-lane count
            if group.lanes.len() > 1 {
                let count_y = label_y + 10.0;
                let count_label = format!("{} concurrent", group.lanes.len());
                let _ = write!(
                    buf,
                    "<text x=\"{text_x:.1}\" y=\"{count_y:.1}\" font-size=\"8\" fill=\"#94a3b8\" font-style=\"italic\">{count_label}</text>",
                );
            }
        }
    }

    let _ = write!(buf, "</g>");
}

fn render_language_badge(
    buf: &mut String,
    info: &super::runner_info::RunnerInfo,
    baseline_y: f64,
) -> f64 {
    let language = if info.runner_language.is_empty() {
        "unknown"
    } else {
        &info.runner_language
    };
    let width = (language.len() as f64 * 6.0 + 12.0).max(40.0);
    let color = runtime_color(language);
    let label = xml_escape(language);
    let top = baseline_y - 11.0;
    let _ = write!(
        buf,
        "<rect x=\"10\" y=\"{top:.1}\" width=\"{width:.1}\" height=\"14\" rx=\"3\" fill=\"{color}\" opacity=\"0.95\" pointer-events=\"none\"/><text x=\"{:.1}\" y=\"{baseline_y:.1}\" text-anchor=\"middle\" font-size=\"8\" fill=\"white\" font-weight=\"700\" pointer-events=\"none\">{label}</text>",
        10.0 + width / 2.0,
    );
    18.0 + width
}

fn format_runner_line(info: &super::runner_info::RunnerInfo) -> String {
    let details = info.details();
    if details.is_empty() {
        info.name_label()
    } else {
        format!("{} | {}", info.name_label(), details)
    }
}

fn format_worker_line(
    child: &super::runner_info::RunnerInfo,
    parent: &super::runner_info::RunnerInfo,
) -> String {
    let details = format_child_details(child, parent);
    if details.is_empty() {
        child.name_label()
    } else {
        format!("{} | {}", child.name_label(), details)
    }
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
    if child.executor_kind != parent.executor_kind && child.executor_kind != "unknown" {
        parts.push(child.executor_kind.clone());
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
            "<line x1=\"0\" y1=\"{y:.1}\" x2=\"{x2}\" y2=\"{y:.1}\" stroke=\"{stroke}\" stroke-width=\"0.35\" opacity=\"0.8\"/>",
        );
    }
}
