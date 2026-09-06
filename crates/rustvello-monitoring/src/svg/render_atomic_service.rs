//! Atomic-service execution overlays for the invocation timeline.

use std::fmt::Write;

use super::data::TimelineData;
use crate::util::escape::xml_escape;

const FILL: &str = "#f97316";
const STROKE: &str = "#c2410c";
const MIN_WIDTH: f64 = 10.0;

pub fn render(buf: &mut String, data: &TimelineData) {
    if data.atomic_service_executions.is_empty() {
        return;
    }
    buf.push_str(r#"<g class="atomic-service-windows">"#);
    for execution in &data.atomic_service_executions {
        let Some(group) = data
            .groups
            .iter()
            .find(|group| group.runner_info.runner_id == execution.runner_id)
        else {
            continue;
        };
        if execution.end < data.bounds.start || execution.start > data.bounds.end {
            continue;
        }
        let start = execution.start.max(data.bounds.start);
        let end = execution.end.min(data.bounds.end);
        let x1 = data.bounds.time_to_x(start);
        let raw_width = (data.bounds.time_to_x(end) - x1).max(0.0);
        let width = raw_width.max(MIN_WIDTH);
        let right = data.bounds.left_margin + data.bounds.drawable_width;
        let x = x1.min(right - width).max(data.bounds.left_margin);
        let runner_id = xml_escape(&execution.runner_id);
        let title = xml_escape(&format!(
            "Atomic service on {}\n{} - {}\nduration: {:.3}s",
            execution.runner_id,
            execution.start.to_rfc3339(),
            execution.end.to_rfc3339(),
            execution.duration_secs()
        ));
        // Atomic work belongs to the runner control plane. Reserve a visible
        // status-bar strip in the parent header, leaving worker task lanes
        // unobscured for invocation state segments.
        let marker_y = group.y_start + 4.0;
        let marker_height = 14.0;
        let _ = write!(
            buf,
            r#"<a href="/atomic-service"><g class="atomic-service-window" data-atomic-service="1" data-runner-id="{runner_id}" data-start="{}" data-end="{}"><title>{title}</title><rect x="{x:.2}" y="{marker_y:.2}" width="{width:.2}" height="{marker_height:.2}" rx="1.5" fill="{FILL}" fill-opacity="0.9" stroke="{STROKE}" stroke-width="0.8"/></g></a>"#,
            execution.start.to_rfc3339(),
            execution.end.to_rfc3339(),
        );
    }
    buf.push_str("</g>");
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use rustvello_core::orchestrator::AtomicServiceExecution;

    use super::*;
    use crate::svg::bounds::TimelineBounds;
    use crate::svg::config::TimelineConfig;
    use crate::svg::lane::{LaneGroup, RunnerLane};
    use crate::svg::runner_info::RunnerInfo;

    #[test]
    fn short_execution_is_visible_and_accountable() {
        let start = Utc.with_ymd_and_hms(2026, 9, 4, 12, 0, 0).unwrap();
        let config = TimelineConfig::default();
        let bounds = TimelineBounds::new(
            start,
            start + Duration::seconds(10),
            config.left_margin,
            config.drawable_width(),
        );
        let mut group = LaneGroup::new(RunnerInfo {
            runner_cls: "PersistentTokioRunner".to_owned(),
            runner_language: "rust".to_owned(),
            executor_kind: "tokio".to_owned(),
            runner_id: "runner-1".to_owned(),
            hostname: "host".to_owned(),
            pid: 42,
            thread_id: 1,
            parent_runner_cls: None,
            parent_runner_id: None,
        });
        group.lanes.push(RunnerLane::new());
        let mut data = TimelineData::new(config, bounds, vec![group]);
        data.atomic_service_executions.push(AtomicServiceExecution {
            runner_id: "runner-1".to_owned(),
            start: start + Duration::seconds(2),
            end: start + Duration::milliseconds(2_001),
        });

        let mut svg = String::new();
        render(&mut svg, &data);

        assert!(svg.contains("atomic-service-window"));
        assert!(svg.contains("width=\"10.00\""));
        assert!(svg.contains("height=\"14.00\""));
        assert!(svg.contains("data-runner-id=\"runner-1\""));
        assert!(svg.contains("href=\"/atomic-service\""));
    }

    #[test]
    fn execution_is_drawn_on_the_parent_row_not_every_worker_row() {
        let start = Utc.with_ymd_and_hms(2026, 9, 4, 12, 0, 0).unwrap();
        let config = TimelineConfig::default();
        let bounds = TimelineBounds::new(
            start,
            start + Duration::seconds(10),
            config.left_margin,
            config.drawable_width(),
        );
        let mut group = LaneGroup::new(RunnerInfo {
            runner_cls: "PersistentTokioRunner".to_owned(),
            runner_language: "rust".to_owned(),
            executor_kind: "tokio".to_owned(),
            runner_id: "parent-runner".to_owned(),
            hostname: "host".to_owned(),
            pid: 42,
            thread_id: 0,
            parent_runner_cls: None,
            parent_runner_id: None,
        });
        for index in 0..2 {
            let mut lane = RunnerLane::new();
            lane.worker_info = Some(RunnerInfo {
                runner_cls: "PersistentTokioWorker".to_owned(),
                runner_language: "rust".to_owned(),
                executor_kind: "tokio".to_owned(),
                runner_id: format!("worker-{index}"),
                hostname: "host".to_owned(),
                pid: 42,
                thread_id: index + 1,
                parent_runner_cls: Some("PersistentTokioRunner".to_owned()),
                parent_runner_id: Some("parent-runner".to_owned()),
            });
            group.lanes.push(lane);
        }
        let mut data = TimelineData::new(config.clone(), bounds, vec![group]);
        data.atomic_service_executions.push(AtomicServiceExecution {
            runner_id: "parent-runner".to_owned(),
            start: start + Duration::seconds(2),
            end: start + Duration::milliseconds(2_001),
        });

        let mut svg = String::new();
        render(&mut svg, &data);

        // The group has a 22px parent header; the marker stays inside its top
        // edge instead of spanning the two worker lanes beneath it.
        assert!(svg.contains("y=\"36.00\""));
        assert!(!svg.contains("height=\"70.00\""));
    }
}
