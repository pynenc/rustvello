//! Log explorer views.

use std::collections::HashSet;

use askama::Template;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Router;

use crate::histogram::{
    build_histogram, parse_categories, HistogramCategory, HistogramEntry, HistogramPanel,
};
use crate::log_explorer::parser::{self, EntityRef, ParsedLogLine};
use crate::log_explorer::render;
use crate::state::AppState;
use crate::util::view_helpers::{get_active_app, AppResult, HtmlTemplate};
use crate::AppInstance;
use rustvello_proto::prelude::InvocationId;

// ── Form data ─────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize, Default)]
pub struct LogQuery {
    pub log_text: Option<String>,
    pub histogram_status: Option<String>,
}

// ── Analysis data structures ──────────────────────────────────────────────────

/// Per-line analysis result displayed in the template.
pub struct LineAnalysis {
    /// The parsed log line data.
    pub parsed: ParsedLogLine,
    /// Pre-rendered header HTML (timestamp + level + logger + bracket).
    pub header_html: String,
    /// Pre-rendered message HTML (entity links + status colors).
    pub message_html: String,
    /// CSS class for the card level coloring.
    pub level_class: String,
    /// Entity references from the message body (not from bracket).
    pub extra_entity_refs: Vec<EntityRef>,
}

/// Full analysis of a multi-line log block.
pub struct MultiLogAnalysis {
    /// Per-line analysis results.
    pub lines: Vec<LineAnalysis>,
    /// Deduplicated entity refs collected across all lines.
    pub all_entity_refs: Vec<EntityRef>,
    /// True if at least one line parsed successfully.
    pub has_valid: bool,
    /// Count of invocation refs.
    pub inv_ref_count: usize,
    /// Count of runner refs.
    pub runner_ref_count: usize,
    /// Count of task refs.
    pub task_ref_count: usize,
    /// Inline SVG timeline of referenced invocations (empty if none).
    pub svg_content: String,
    /// Query string for linking to the full timeline view.
    pub timeline_qs: String,
    /// Map of invocation ID → task key for cross-highlighting.
    pub inv_task_map: std::collections::HashMap<String, String>,
    /// Occupancy histogram for the same invocation scope.
    pub histogram: HistogramPanel,
}

// ── Template ──────────────────────────────────────────────────────────────────

#[derive(Template)]
#[template(path = "log_explorer/index.html")]
#[allow(dead_code)]
struct LogExplorerTemplate {
    app_id: String,
    app_ids: Vec<String>,
    nav_path: &'static str,
    log_text: String,
    has_input: bool,
    analysis: Option<MultiLogAnalysis>,
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", axum::routing::get(index))
        .route("/analyze", axum::routing::post(analyze))
}

async fn index(State(state): State<AppState>) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    Ok(HtmlTemplate(LogExplorerTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "log_explorer",
        log_text: String::new(),
        has_input: false,
        analysis: None,
    }))
}

async fn analyze(
    State(state): State<AppState>,
    axum::Form(form): axum::Form<LogQuery>,
) -> AppResult<impl IntoResponse> {
    let app = get_active_app(&state)?;
    let categories = parse_categories(form.histogram_status.as_deref());
    let log_text = form.log_text.unwrap_or_default();
    let analysis = if log_text.trim().is_empty() {
        None
    } else {
        Some(analyse_logs(&log_text, &app, categories).await)
    };

    Ok(HtmlTemplate(LogExplorerTemplate {
        app_id: app.app_id.clone(),
        app_ids: state.app_ids().unwrap_or_default(),
        nav_path: "log_explorer",
        log_text,
        has_input: true,
        analysis,
    }))
}

// ── Analysis logic ────────────────────────────────────────────────────────────

async fn analyse_logs(
    log_text: &str,
    app: &AppInstance,
    categories: std::collections::BTreeSet<HistogramCategory>,
) -> MultiLogAnalysis {
    let mut parsed_lines = parser::parse_log_lines(log_text);

    // Resolve truncated runner/worker IDs to full UUIDs via state backend.
    // The log format truncates runner/worker IDs to 8 chars (e.g. "a86ab1f8"),
    // but the state backend stores full UUIDs. We query once per unique
    // partial ID and update all ParsedLogLine fields in-place.
    resolve_partial_runner_ids(&mut parsed_lines, app).await;

    let lines: Vec<LineAnalysis> = parsed_lines
        .into_iter()
        .map(|parsed| {
            let message_html = render::render_message_html(&parsed.message);
            let level_class = parsed
                .level
                .as_deref()
                .map_or("", render::level_card_class)
                .to_owned();

            let extra_entity_refs = parsed.entity_refs.clone();

            LineAnalysis {
                parsed,
                header_html: String::new(),
                message_html,
                level_class,
                extra_entity_refs,
            }
        })
        .collect();

    let has_valid = lines.iter().any(|la| la.parsed.is_valid);
    let all_entity_refs = collect_all_entity_refs(&lines);

    let inv_ref_count = all_entity_refs
        .iter()
        .filter(|r| {
            matches!(
                r.kind.as_str(),
                "invocation" | "parent-invocation" | "child-invocation" | "new-invocation"
            )
        })
        .count();
    let runner_ref_count = all_entity_refs
        .iter()
        .filter(|r| matches!(r.kind.as_str(), "runner" | "worker"))
        .count();
    let task_ref_count = all_entity_refs.iter().filter(|r| r.kind == "task").count();

    // Build inline SVG timeline from referenced invocations
    let (svg_content, timeline_qs, inv_task_map) =
        build_log_timeline(&all_entity_refs, &lines, app).await;
    let histogram = build_log_histogram(&all_entity_refs, &lines, app, categories).await;

    MultiLogAnalysis {
        lines,
        all_entity_refs,
        has_valid,
        inv_ref_count,
        runner_ref_count,
        task_ref_count,
        svg_content,
        timeline_qs,
        inv_task_map,
        histogram,
    }
}

/// Build an inline SVG timeline and query string from log-referenced invocations.
async fn build_log_timeline(
    all_refs: &[EntityRef],
    lines: &[LineAnalysis],
    app: &AppInstance,
) -> (String, String, std::collections::HashMap<String, String>) {
    // Collect invocation IDs from entity refs and parsed context fields. The
    // latter keeps the mini-timeline independent from message-body reference
    // extraction when a production log carries its invocation only in the
    // structured bracket/span context.
    let mut inv_ids: HashSet<String> = all_refs
        .iter()
        .filter(|r| {
            matches!(
                r.kind.as_str(),
                "invocation" | "parent-invocation" | "child-invocation" | "new-invocation"
            )
        })
        .map(|r| r.value.clone())
        .collect();
    inv_ids.extend(
        lines
            .iter()
            .filter_map(|line| line.parsed.invocation_id.clone()),
    );

    if inv_ids.is_empty() {
        return (
            String::new(),
            String::new(),
            std::collections::HashMap::new(),
        );
    }

    // Compute time window from parsed timestamps with 10% padding
    let timestamps: Vec<chrono::DateTime<chrono::Utc>> = lines
        .iter()
        .filter_map(|la| {
            la.parsed.timestamp.as_deref().and_then(|ts| {
                chrono::DateTime::parse_from_rfc3339(ts)
                    .ok()
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .or_else(|| {
                        // Try other common formats
                        chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S%.f")
                            .ok()
                            .map(|dt| dt.and_utc())
                    })
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S")
                            .ok()
                            .map(|dt| dt.and_utc())
                    })
            })
        })
        .collect();

    // Build timeline query string
    let time_window = log_time_window(&timestamps);
    let timeline_qs = if let Some((start, end)) = time_window {
        format!(
            "time_range=custom&start_date={}&end_date={}",
            start.format("%Y-%m-%dT%H:%M:%S"),
            end.format("%Y-%m-%dT%H:%M:%S"),
        )
    } else {
        "time_range=5m".to_owned()
    };

    // Fetch invocation history from state backend
    let mut builder = crate::svg::TimelineDataBuilder::new(crate::svg::TimelineConfig::default());
    if let Some((start, end)) = time_window {
        builder.set_time_bounds(start, end);
    }
    let mut found_any = false;
    let mut runner_ids_seen = std::collections::HashSet::new();

    // Build invocation→task mapping from parsed log lines
    let inv_task_map: std::collections::HashMap<String, String> = lines
        .iter()
        .filter_map(|la| {
            la.parsed
                .invocation_id
                .as_ref()
                .map(|inv| (inv.clone(), la.parsed.task_key.clone().unwrap_or_default()))
        })
        .collect();

    for inv_id_str in &inv_ids {
        let inv_id = InvocationId::from_string(inv_id_str.clone());
        let history = app
            .state_backend
            .get_history(&inv_id)
            .await
            .unwrap_or_default();
        if !history.is_empty() {
            for entry in &history {
                if let Some(ref rid) = entry.runner_id {
                    runner_ids_seen.insert(rid.to_string());
                }
                if let Some(ref rid) = entry.status_record.runner_id {
                    runner_ids_seen.insert(rid.to_string());
                }
            }
            let task_id = inv_task_map.get(inv_id_str).cloned().unwrap_or_default();
            builder.add_history_batch_for_task(history, &task_id);
            found_any = true;
        }
    }

    if !found_any {
        return (String::new(), timeline_qs, inv_task_map);
    }

    // Fetch runner contexts for enriched labels (hostname, PID, runner class)
    let mut runner_contexts = std::collections::HashMap::new();
    for rid in &runner_ids_seen {
        if let Ok(Some(ctx)) = app.state_backend.get_runner_context(rid).await {
            runner_contexts.insert(rid.clone(), ctx);
        }
    }
    builder.set_runner_contexts(runner_contexts);

    let data = builder.build();
    let svg = crate::svg::TimelineSvgRenderer::render(&data);
    (svg, timeline_qs, inv_task_map)
}

fn log_time_window(
    timestamps: &[chrono::DateTime<chrono::Utc>],
) -> Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)> {
    let (min_ts, max_ts) = (timestamps.iter().min()?, timestamps.iter().max()?);
    let duration = (*max_ts - *min_ts).num_milliseconds().max(1) as f64;
    let pad = chrono::Duration::milliseconds((duration * 0.1).max(1000.0) as i64);
    Some((*min_ts - pad, *max_ts + pad))
}

async fn build_log_histogram(
    all_refs: &[EntityRef],
    lines: &[LineAnalysis],
    app: &AppInstance,
    categories: std::collections::BTreeSet<HistogramCategory>,
) -> HistogramPanel {
    let mut invocation_ids: HashSet<String> = all_refs
        .iter()
        .filter(|reference| {
            matches!(
                reference.kind.as_str(),
                "invocation" | "parent-invocation" | "child-invocation" | "new-invocation"
            )
        })
        .map(|reference| reference.value.clone())
        .collect();
    invocation_ids.extend(
        lines
            .iter()
            .filter_map(|line| line.parsed.invocation_id.clone()),
    );

    let timestamps: Vec<chrono::DateTime<chrono::Utc>> = lines
        .iter()
        .filter_map(|line| {
            line.parsed.timestamp.as_deref().and_then(|timestamp| {
                chrono::DateTime::parse_from_rfc3339(timestamp)
                    .ok()
                    .map(|value| value.with_timezone(&chrono::Utc))
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S%.f")
                            .ok()
                            .map(|value| value.and_utc())
                    })
            })
        })
        .collect();
    let mut entries = Vec::new();
    for invocation_id in &invocation_ids {
        let typed_id = InvocationId::from_string(invocation_id.clone());
        let Ok(invocation) = app.state_backend.get_invocation(&typed_id).await else {
            continue;
        };
        let history = app
            .state_backend
            .get_history(&typed_id)
            .await
            .unwrap_or_default();
        let task_id = invocation.task_id.to_string();
        entries.extend(
            history
                .iter()
                .map(|entry| HistogramEntry::from_history(entry, &task_id)),
        );
    }
    let history_window = || {
        let start = entries.iter().map(|entry| entry.timestamp).min()?;
        let mut end = entries.iter().map(|entry| entry.timestamp).max()?;
        if end <= start {
            end = start + chrono::Duration::seconds(1);
        }
        Some((start, end))
    };
    let (start, end) = log_time_window(&timestamps)
        .or_else(history_window)
        .unwrap_or_else(|| {
            let end = chrono::Utc::now();
            (end - chrono::Duration::seconds(5), end)
        });
    let data = build_histogram(&entries, start, end, categories, None);
    let mut scoped_ids = invocation_ids.into_iter().collect::<Vec<_>>();
    scoped_ids.sort();
    let common_params = vec![("inv_ids".to_owned(), scoped_ids.join(","))];
    let timeline_config = crate::svg::TimelineConfig::default();
    HistogramPanel::from_data_with_y_axis_and_plot_bounds(
        &data,
        &common_params,
        "/invocations/timeline",
        true,
        None,
        Some(timeline_config.left_margin),
        Some(timeline_config.left_margin + timeline_config.drawable_width()),
    )
    .with_form_id("log-form")
}

/// Resolve truncated runner/worker IDs in parsed log lines to full UUIDs.
///
/// The log format truncates runner/worker IDs to 8 chars for readability
/// (e.g. `PTR(a86ab1f8)`), but the state backend stores full UUIDs.  This
/// function collects unique partial IDs, queries the backend for matches,
/// and replaces the `partial_id` / `worker_id` fields in-place so that
/// template links point to the correct `/runners/<full-UUID>` URLs.
async fn resolve_partial_runner_ids(lines: &mut [parser::ParsedLogLine], app: &AppInstance) {
    // Collect unique partial IDs that look truncated (not already a full UUID).
    let mut partial_ids = HashSet::new();
    for line in lines.iter() {
        for runner in &line.runners {
            if !uuid_re().is_match(&runner.partial_id) {
                partial_ids.insert(runner.partial_id.clone());
            }
        }
        if let Some(ref wid) = line.worker_id {
            if !uuid_re().is_match(wid) {
                partial_ids.insert(wid.clone());
            }
        }
    }

    if partial_ids.is_empty() {
        return;
    }

    // Query state backend once per partial ID and build a resolution map.
    let mut resolved: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for pid in &partial_ids {
        if let Ok(matches) = app.state_backend.get_matching_runner_contexts(pid).await {
            if matches.len() == 1 {
                // Unique match — safe to resolve.
                resolved.insert(pid.clone(), matches[0].runner_id.clone());
            }
        }
    }

    if resolved.is_empty() {
        return;
    }

    // Replace partial IDs with full UUIDs in all parsed lines.
    for line in lines.iter_mut() {
        for runner in &mut line.runners {
            if let Some(full) = resolved.get(&runner.partial_id) {
                runner.partial_id = full.clone();
            }
        }
        if let Some(ref wid) = line.worker_id {
            if let Some(full) = resolved.get(wid) {
                line.worker_id = Some(full.clone());
            }
        }
    }
}

/// UUID pattern — same as parser's uuid_re but accessible here.
fn uuid_re() -> &'static regex::Regex {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(r"(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
            .expect("valid regex")
    })
}

fn collect_all_entity_refs(lines: &[LineAnalysis]) -> Vec<EntityRef> {
    let mut refs = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for la in lines {
        // Bracket-level refs (runners, invocation, task)
        for runner in &la.parsed.runners {
            let key = ("runner".to_owned(), runner.partial_id.clone());
            if seen.insert(key) {
                refs.push(EntityRef {
                    kind: "runner".to_owned(),
                    value: runner.partial_id.clone(),
                });
            }
        }
        if let Some(ref inv) = la.parsed.invocation_id {
            let key = ("invocation".to_owned(), inv.clone());
            if seen.insert(key) {
                refs.push(EntityRef {
                    kind: "invocation".to_owned(),
                    value: inv.clone(),
                });
            }
        }
        if let Some(ref wid) = la.parsed.worker_id {
            let key = ("worker".to_owned(), wid.clone());
            if seen.insert(key) {
                refs.push(EntityRef {
                    kind: "worker".to_owned(),
                    value: wid.clone(),
                });
            }
        }
        if let Some(ref task) = la.parsed.task_key {
            let key = ("task".to_owned(), task.clone());
            if seen.insert(key) {
                refs.push(EntityRef {
                    kind: "task".to_owned(),
                    value: task.clone(),
                });
            }
        }
        // Message-body refs
        for r in &la.extra_entity_refs {
            let key = (r.kind.clone(), r.value.clone());
            if seen.insert(key) {
                refs.push(r.clone());
            }
        }
    }
    refs
}
