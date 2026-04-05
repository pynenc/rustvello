use super::TimelineDataBuilder;
use chrono::{DateTime, Duration, Utc};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::svg::config::TimelineConfig;

/// Helper: create an InvocationHistory entry with a specific timestamp.
fn make_entry(
    inv_id: &str,
    status: InvocationStatus,
    runner_id: Option<&str>,
    timestamp: DateTime<Utc>,
) -> InvocationHistory {
    let rid = runner_id.map(RunnerId::from_string);
    InvocationHistory {
        invocation_id: InvocationId::from_string(inv_id),
        status_record: InvocationStatusRecord {
            status,
            runner_id: rid.clone(),
            timestamp,
        },
        message: None,
        runner_id: rid,
        registered_by_inv_id: None,
        history_timestamp: None,
    }
}

/// Simulate a typical invocation lifecycle:
/// Registered (no runner) → Pending (runner) → Running (runner) → Success (runner)
fn make_invocation_history(
    inv_id: &str,
    runner_id: &str,
    registered_at: DateTime<Utc>,
    pending_at: DateTime<Utc>,
    running_at: DateTime<Utc>,
    success_at: DateTime<Utc>,
) -> Vec<InvocationHistory> {
    vec![
        make_entry(inv_id, InvocationStatus::Registered, None, registered_at),
        make_entry(
            inv_id,
            InvocationStatus::Pending,
            Some(runner_id),
            pending_at,
        ),
        make_entry(
            inv_id,
            InvocationStatus::Running,
            Some(runner_id),
            running_at,
        ),
        make_entry(
            inv_id,
            InvocationStatus::Success,
            Some(runner_id),
            success_at,
        ),
    ]
}

#[test]
fn test_sequential_invocations_same_runner_share_y_position() {
    // Create 5 invocations, all registered at roughly the same time,
    // but executed SEQUENTIALLY by the same runner.
    let t0 = Utc::now();
    let runner_id = "runner-001";
    let mut builder = TimelineDataBuilder::new(TimelineConfig::default());

    for i in 0..5u32 {
        let registered_at = t0 + Duration::milliseconds(i as i64 * 10); // all registered early
        let pending_at = t0 + Duration::seconds(10 + i as i64 * 5); // sequential execution
        let running_at = pending_at + Duration::milliseconds(100);
        let success_at = pending_at + Duration::seconds(4);
        let inv_id = format!("inv-{i}");

        let history = make_invocation_history(
            &inv_id,
            runner_id,
            registered_at,
            pending_at,
            running_at,
            success_at,
        );
        builder.add_history_batch_for_task(history, "test.task");
    }

    let data = builder.build();

    // Find the runner's lane group (not "unassigned")
    let runner_group = data
        .groups
        .iter()
        .find(|g| g.runner_info.runner_id == runner_id)
        .expect("should have a lane group for the runner");

    // All sequential invocations should fit on ONE sub-lane
    assert_eq!(
        runner_group.lanes.len(),
        1,
        "sequential invocations on the same runner should share one sub-lane, got {}",
        runner_group.lanes.len()
    );

    // All segments and points should be in this single lane,
    // sharing the same y_offset (same visual row).
    let lane = &runner_group.lanes[0];

    // All elements within the lane have positions relative to y_center=0.0.
    // Segments: y = y_center - height/2, Points: y = y_center.
    // What matters is they share the same lane.y_offset.
    assert!(
        !lane.segments.is_empty() || !lane.points.is_empty(),
        "lane should have at least some elements"
    );

    // All 5 invocations' segments should be present in the single lane
    let mut inv_ids_in_lane: Vec<String> = lane
        .segments
        .iter()
        .map(|s| s.invocation_id.clone())
        .chain(lane.points.iter().map(|p| p.invocation_id.clone()))
        .collect();
    inv_ids_in_lane.sort();
    inv_ids_in_lane.dedup();
    assert_eq!(
        inv_ids_in_lane.len(),
        5,
        "all 5 invocations should have elements on this lane, found: {:?}",
        inv_ids_in_lane
    );
}

#[test]
fn test_concurrent_invocations_same_runner_get_multiple_sublanes() {
    // Two invocations overlapping in execution time on the same runner
    let t0 = Utc::now();
    let runner_id = "runner-001";
    let mut builder = TimelineDataBuilder::new(TimelineConfig::default());

    // inv-0: runs from t0+10s to t0+20s
    let h0 = make_invocation_history(
        "inv-0",
        runner_id,
        t0,                         // registered
        t0 + Duration::seconds(10), // pending
        t0 + Duration::seconds(11), // running
        t0 + Duration::seconds(20), // success
    );
    // inv-1: runs from t0+15s to t0+25s (overlaps with inv-0)
    let h1 = make_invocation_history(
        "inv-1",
        runner_id,
        t0 + Duration::seconds(1),  // registered
        t0 + Duration::seconds(15), // pending
        t0 + Duration::seconds(16), // running
        t0 + Duration::seconds(25), // success
    );

    builder.add_history_batch_for_task(h0, "test.task");
    builder.add_history_batch_for_task(h1, "test.task");

    let data = builder.build();

    let runner_group = data
        .groups
        .iter()
        .find(|g| g.runner_info.runner_id == runner_id)
        .expect("should have a lane group for the runner");

    // Overlapping execution → should need 2 sub-lanes
    assert_eq!(
        runner_group.lanes.len(),
        2,
        "concurrent invocations should get separate sub-lanes, got {}",
        runner_group.lanes.len()
    );
}

#[test]
fn test_registered_entries_go_to_unassigned() {
    // Entries with no runner_id should end up in "unassigned" group,
    // not pollute the runner's time-span.
    let t0 = Utc::now();
    let runner_id = "runner-001";
    let mut builder = TimelineDataBuilder::new(TimelineConfig::default());

    let history = make_invocation_history(
        "inv-0",
        runner_id,
        t0,                         // registered (no runner)
        t0 + Duration::seconds(10), // pending (runner)
        t0 + Duration::seconds(11), // running (runner)
        t0 + Duration::seconds(15), // success (runner)
    );
    builder.add_history_batch_for_task(history, "test.task");

    let data = builder.build();

    // Should have exactly 2 groups: "unassigned" and the runner
    assert_eq!(
        data.groups.len(),
        2,
        "should have unassigned + runner groups, got {} groups: {:?}",
        data.groups.len(),
        data.groups
            .iter()
            .map(|g| &g.runner_info.runner_id)
            .collect::<Vec<_>>()
    );

    let unassigned = data
        .groups
        .iter()
        .find(|g| g.runner_info.runner_id == "unassigned")
        .expect("should have an 'unassigned' group for Registered entries");
    assert_eq!(unassigned.lanes.len(), 1);
}

#[test]
fn test_zoom_centering_short_duration_invocation() {
    // An 8ms invocation should be roughly centered in the timeline
    // when explicit time bounds are set with proper padding.
    let t0 =
        chrono::NaiveDateTime::parse_from_str("2026-03-22T09:31:31.700", "%Y-%m-%dT%H:%M:%S%.3f")
            .unwrap()
            .and_utc();
    let runner_id = "runner-001";

    // Simulate an invocation lasting ~8ms
    let registered_at = t0;
    let pending_at = t0 + Duration::milliseconds(1);
    let running_at = t0 + Duration::milliseconds(2);
    let success_at = t0 + Duration::milliseconds(8);

    let history = make_invocation_history(
        "inv-zoom",
        runner_id,
        registered_at,
        pending_at,
        running_at,
        success_at,
    );

    // Compute padding the same way the server does
    let timestamps: Vec<_> = history.iter().map(|h| h.status_record.timestamp).collect();
    let min_t = *timestamps.iter().min().unwrap();
    let max_t = *timestamps.iter().max().unwrap();
    let span = (max_t - min_t).num_milliseconds().max(1);
    let padding_ms = (span as f64 * 0.5).max(100.0) as i64;
    let padding = Duration::milliseconds(padding_ms);
    let zoom_start = min_t - padding;
    let zoom_end = max_t + padding;

    // The padding should be 100ms (minimum) for an 8ms invocation
    assert_eq!(
        padding_ms, 100,
        "padding for 8ms invocation should be 100ms minimum"
    );

    // Build with these zoom bounds
    let config = TimelineConfig::default();
    let mut builder = TimelineDataBuilder::new(config.clone());
    builder.set_time_bounds(zoom_start, zoom_end);
    builder.add_history_batch_for_task(history, "test.task");
    let data = builder.build();

    // The invocation's center (t0 + 4ms) should map to roughly
    // the center of the drawable area
    let inv_center = t0 + Duration::milliseconds(4);
    let x_center = data.bounds.time_to_x(inv_center);
    let drawable_mid = config.left_margin + config.drawable_width() / 2.0;

    // Invocation center should be within 5% of the drawable area center
    let tolerance = config.drawable_width() * 0.05;
    assert!(
        (x_center - drawable_mid).abs() < tolerance,
        "invocation center x={:.1} should be near drawable mid={:.1} (tolerance={:.1})",
        x_center,
        drawable_mid,
        tolerance
    );

    // The total zoom window should be ~208ms (8ms + 2*100ms),
    // NOT 2000ms+ as before
    let total_ms = (zoom_end - zoom_start).num_milliseconds();
    assert!(
        total_ms <= 300,
        "zoom window should be ≤300ms for 8ms invocation, got {}ms",
        total_ms
    );
    assert!(
        total_ms >= 100,
        "zoom window should be ≥100ms, got {}ms",
        total_ms
    );
}

#[test]
fn test_zoom_padding_scales_with_span() {
    // For longer invocations, the padding should scale proportionally
    let _t0 = Utc::now();

    // 10-second invocation
    let span_ms: i64 = 10_000;
    let padding_ms = (span_ms as f64 * 0.5).max(100.0) as i64;
    assert_eq!(
        padding_ms, 5000,
        "10s invocation should get 5000ms padding (50% of span)"
    );

    // 100ms invocation
    let span_ms: i64 = 100;
    let padding_ms = (span_ms as f64 * 0.5).max(100.0) as i64;
    assert_eq!(
        padding_ms, 100,
        "100ms invocation should get 100ms padding (floor)"
    );

    // 0ms invocation (instant)
    let span_ms: i64 = 1; // clamped to 1ms minimum
    let padding_ms = (span_ms as f64 * 0.5).max(100.0) as i64;
    assert_eq!(
        padding_ms, 100,
        "instant invocation should get 100ms padding (floor)"
    );

    // 1-hour invocation
    let span_ms: i64 = 3_600_000;
    let padding_ms = (span_ms as f64 * 0.5).max(100.0) as i64;
    assert_eq!(
        padding_ms, 1_800_000,
        "1h invocation should get 30min padding (50% of span)"
    );
}

#[test]
fn test_rendered_svg_sequential_invocations_same_y() {
    // End-to-end: render the SVG and verify all invocations on the same
    // runner produce elements at the same Y coordinates.
    use crate::svg::render::TimelineSvgRenderer;

    let t0 = Utc::now();
    let runner_id = "runner-001";
    let mut builder = TimelineDataBuilder::new(TimelineConfig::default());

    for i in 0..5u32 {
        let registered_at = t0 + Duration::milliseconds(i as i64 * 10);
        let pending_at = t0 + Duration::seconds(10 + i as i64 * 5);
        let running_at = pending_at + Duration::milliseconds(100);
        let success_at = pending_at + Duration::seconds(4);
        let inv_id = format!("inv-{i}");

        let history = make_invocation_history(
            &inv_id,
            runner_id,
            registered_at,
            pending_at,
            running_at,
            success_at,
        );
        builder.add_history_batch_for_task(history, "test.task");
    }

    let data = builder.build();
    let svg = TimelineSvgRenderer::render(&data);

    // Extract all segment Y values for the runner group (not "unassigned").
    // Segments are rendered as <rect ... y="Y" ... data-invocation-id="inv-X">
    // (not self-closing — they contain <title>)
    let re =
        regex::Regex::new(r#"<rect[^>]*\by="([^"]+)"[^>]*data-invocation-id="(inv-\d)"[^>]*>"#)
            .unwrap();

    let mut y_values: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut inv_ids_found: std::collections::HashSet<String> = std::collections::HashSet::new();

    for cap in re.captures_iter(&svg) {
        let y = cap.get(1).unwrap().as_str().to_owned();
        let inv = cap.get(2).unwrap().as_str().to_owned();
        y_values.insert(y);
        inv_ids_found.insert(inv);
    }

    // Also try pattern where data-invocation-id comes before y
    let re2 =
        regex::Regex::new(r#"<rect[^>]*data-invocation-id="(inv-\d)"[^>]*\by="([^"]+)"[^>]*>"#)
            .unwrap();
    for cap in re2.captures_iter(&svg) {
        let inv = cap.get(1).unwrap().as_str().to_owned();
        let y = cap.get(2).unwrap().as_str().to_owned();
        y_values.insert(y);
        inv_ids_found.insert(inv);
    }

    assert!(
        !inv_ids_found.is_empty(),
        "should have found segment elements in SVG"
    );

    // All segments should share exactly ONE Y value (same sub-lane)
    assert_eq!(
        y_values.len(),
        1,
        "all sequential invocations on same runner should have identical segment Y, got {:?}",
        y_values
    );
}
