//! Pure invocation-state occupancy histogram model and SVG renderer.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Duration, Utc};
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatus;
use serde::Serialize;

use crate::util::escape::xml_escape;

const MAX_BUCKETS: i64 = 240;
const TARGET_BUCKETS: i64 = 220;
const BAR_GAP_PX: f64 = 1.0;
const MAX_TASK_LEGEND: usize = 12;
const TASK_PALETTE: [&str; 15] = [
    "#4c78a8", "#f58518", "#e45756", "#72b7b2", "#54a24b", "#eeca3b", "#b279a2", "#ff9da6",
    "#9d755d", "#79706e", "#59a14f", "#76b7b2", "#b6992d", "#4c9f70", "#d37295",
];
const HISTOGRAM_PANEL_PREFIX: &str = "histogram-panel";
const RUST_COLOR: &str = "#ce422b";
const PYTHON_COLOR: &str = "#3776ab";
const EXTERNAL_COLOR: &str = "#6c757d";
const UNKNOWN_COLOR: &str = "#6c757d";

static NEXT_HISTOGRAM_PANEL: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HistogramCategory {
    Registered,
    Pending,
    Running,
}

impl HistogramCategory {
    pub const ALL: [Self; 3] = [Self::Registered, Self::Pending, Self::Running];

    const fn index(self) -> usize {
        match self {
            Self::Registered => 0,
            Self::Pending => 1,
            Self::Running => 2,
        }
    }

    pub const fn value(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Pending => "pending",
            Self::Running => "running",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Registered => "Registered",
            Self::Pending => "Pending",
            Self::Running => "Running",
        }
    }
}

pub fn parse_categories(value: Option<&str>) -> BTreeSet<HistogramCategory> {
    value.map_or_else(
        || {
            [HistogramCategory::Pending, HistogramCategory::Running]
                .into_iter()
                .collect()
        },
        |raw| {
            raw.split(',')
                .filter_map(|item| match item.trim().to_ascii_lowercase().as_str() {
                    "registered" => Some(HistogramCategory::Registered),
                    "pending" => Some(HistogramCategory::Pending),
                    "running" => Some(HistogramCategory::Running),
                    _ => None,
                })
                .collect()
        },
    )
}

pub fn serialize_categories(categories: &BTreeSet<HistogramCategory>) -> String {
    HistogramCategory::ALL
        .into_iter()
        .filter(|category| categories.contains(category))
        .map(HistogramCategory::value)
        .collect::<Vec<_>>()
        .join(",")
}

fn task_color(task_id: &str) -> &'static str {
    if task_id == "__other__" {
        return "#cccccc";
    }
    let hash = task_id.bytes().fold(2_166_136_261_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
    });
    TASK_PALETTE[hash as usize % TASK_PALETTE.len()]
}

fn runtime_color(runtime: &str) -> &'static str {
    match runtime {
        "rust" => RUST_COLOR,
        "python" => PYTHON_COLOR,
        "external" => EXTERNAL_COLOR,
        "unknown" => UNKNOWN_COLOR,
        _ => UNKNOWN_COLOR,
    }
}

fn runtime_label(runtime: &str) -> String {
    match runtime {
        "rust" => "Rust workers".to_owned(),
        "python" => "Python workers".to_owned(),
        "external" => "External submitters".to_owned(),
        "unknown" => "Unknown runtime".to_owned(),
        other => format!("{other} workers"),
    }
}

fn runtime_short_label(runtime: &str) -> String {
    match runtime {
        "rust" => "Rust".to_owned(),
        "python" => "Python".to_owned(),
        "external" => "External".to_owned(),
        "unknown" => "Unknown".to_owned(),
        other => other.to_owned(),
    }
}

fn runtime_order(runtime: &str) -> (u8, &str) {
    match runtime {
        "rust" => (0, runtime),
        "python" => (1, runtime),
        "external" => (2, runtime),
        "unknown" => (3, runtime),
        other => (9, other),
    }
}

fn language_from_task_id(task_id: &str) -> String {
    task_id
        .split_once("::")
        .map_or("unknown", |(language, _)| language)
        .to_owned()
}

fn runner_id_for_history(history: &InvocationHistory) -> Option<&str> {
    history
        .runner_id
        .as_ref()
        .or(history.status_record.runner_id.as_ref())
        .map(rustvello_proto::identifiers::RunnerId::as_str)
}

fn task_series(data: &HistogramData) -> (Vec<String>, bool) {
    let mut totals = BTreeMap::<String, usize>::new();
    for bucket in &data.buckets {
        for (task_id, count) in &bucket.counts_by_task {
            *totals.entry(task_id.clone()).or_default() += count;
        }
    }
    let mut ordered = totals.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|(left_task, left_count), (right_task, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_task.cmp(right_task))
    });
    let has_other = ordered.len() > MAX_TASK_LEGEND;
    (
        ordered
            .into_iter()
            .take(MAX_TASK_LEGEND)
            .map(|(task_id, _)| task_id)
            .collect(),
        has_other,
    )
}

#[derive(Debug, Clone)]
struct VisibleTask {
    id: String,
    label: String,
    color: &'static str,
}

fn visible_tasks(data: &HistogramData) -> Vec<VisibleTask> {
    let (visible_task_ids, has_other) = task_series(data);
    let mut tasks = visible_task_ids
        .into_iter()
        .enumerate()
        .map(|(index, task_id)| VisibleTask {
            // Rank assignment avoids hash collisions and gives the most
            // prominent series the strongest Tableau colors.
            color: TASK_PALETTE[index % TASK_PALETTE.len()],
            label: task_id.clone(),
            id: task_id,
        })
        .collect::<Vec<_>>();
    if has_other {
        tasks.push(VisibleTask {
            id: "__other__".to_owned(),
            label: "Other".to_owned(),
            color: task_color("__other__"),
        });
    }
    tasks
}

fn histogram_groups(data: &HistogramData) -> Vec<String> {
    let mut groups = data
        .buckets
        .iter()
        .flat_map(|bucket| bucket.counts_by_runtime.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        let left_order = runtime_order(left);
        let right_order = runtime_order(right);
        left_order.cmp(&right_order)
    });
    groups
}

fn bucket_task_counts(
    counts_by_task: &BTreeMap<String, usize>,
    visible_tasks: &BTreeSet<String>,
) -> Vec<(String, usize)> {
    let mut counts = Vec::new();
    let mut other = 0;
    for (task_id, count) in counts_by_task {
        if visible_tasks.contains(task_id) {
            if *count > 0 {
                counts.push((task_id.clone(), *count));
            }
        } else {
            other += count;
        }
    }
    if other > 0 {
        counts.push(("__other__".to_owned(), other));
    }
    counts
}

fn bucket_task_counts_for_runtime(
    counts_by_task: Option<&BTreeMap<String, usize>>,
    visible_tasks: &BTreeSet<String>,
) -> Vec<(String, usize)> {
    counts_by_task.map_or_else(Vec::new, |counts_by_task| {
        bucket_task_counts(counts_by_task, visible_tasks)
    })
}

fn category_for_status(status: InvocationStatus) -> Option<HistogramCategory> {
    match status {
        InvocationStatus::Registered
        | InvocationStatus::Rerouted
        | InvocationStatus::ConcurrencyControlled => Some(HistogramCategory::Registered),
        InvocationStatus::Pending | InvocationStatus::PendingRecovery | InvocationStatus::Retry => {
            Some(HistogramCategory::Pending)
        }
        InvocationStatus::Running | InvocationStatus::RunningRecovery => {
            Some(HistogramCategory::Running)
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct HistogramEntry {
    pub invocation_id: String,
    pub task_id: String,
    pub runner_id: Option<String>,
    pub runner_language: Option<String>,
    pub status: InvocationStatus,
    pub timestamp: DateTime<Utc>,
}

impl HistogramEntry {
    pub fn from_history(history: &InvocationHistory, task_id: &str) -> Self {
        Self::from_history_with_runner_contexts(history, task_id, &HashMap::new())
    }

    pub fn from_history_with_runner_contexts(
        history: &InvocationHistory,
        task_id: &str,
        runner_contexts: &HashMap<String, StoredRunnerContext>,
    ) -> Self {
        let runner_id = runner_id_for_history(history).map(str::to_owned);
        let runner_language = runner_id
            .as_ref()
            .and_then(|id| runner_contexts.get(id))
            .map(|context| context.runner_language.to_string());
        Self {
            invocation_id: history.invocation_id.to_string(),
            task_id: task_id.to_owned(),
            runner_id,
            runner_language,
            status: history.status_record.status,
            // Occupancy must use the same timestamp that the timeline lanes
            // render. The optional history timestamp is a persistence/query
            // timestamp and can differ from the status transition timestamp.
            timestamp: history.status_record.timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub counts: [usize; 3],
    pub counts_by_task: BTreeMap<String, usize>,
    pub counts_by_runtime: BTreeMap<String, usize>,
    pub counts_by_runtime_task: BTreeMap<String, BTreeMap<String, usize>>,
    pub invocation_ids_by_runtime: BTreeMap<String, Vec<String>>,
    pub runner_ids_by_runtime: BTreeMap<String, Vec<String>>,
    pub invocation_ids_by_category: [Vec<String>; 3],
    pub invocation_ids: Vec<String>,
}

impl HistogramBucket {
    pub fn total_count(&self) -> usize {
        self.invocation_ids.len()
    }
}

#[derive(Debug, Clone)]
pub struct HistogramData {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub bucket_size: Duration,
    pub buckets: Vec<HistogramBucket>,
    pub selected: BTreeSet<HistogramCategory>,
    pub max_count: usize,
    pub empty_reason: Option<&'static str>,
}

pub fn bucket_size_for_window(duration: Duration) -> Duration {
    let milliseconds = duration.num_milliseconds().max(1);
    let seconds = (milliseconds + 999) / 1_000;
    let candidate = if milliseconds <= 5_000 {
        250
    } else if milliseconds <= 30_000 {
        500
    } else if milliseconds <= 60_000 {
        1_000
    } else if seconds <= 15 * 60 {
        5_000
    } else if seconds <= 60 * 60 {
        15_000
    } else if seconds <= 3 * 60 * 60 {
        60_000
    } else if seconds <= 12 * 60 * 60 {
        5 * 60_000
    } else if seconds <= 3 * 24 * 60 * 60 {
        30 * 60_000
    } else {
        60 * 60_000
    };
    let minimum_for_bucket_cap = (milliseconds + MAX_BUCKETS - 1) / MAX_BUCKETS;
    // Prefer temporal detail over wide bars. The display has room for roughly
    // 220 buckets; retain that resolution when the semantic default is coarser.
    let maximum_for_detail = (milliseconds / TARGET_BUCKETS).max(1);
    Duration::milliseconds(
        candidate
            .min(maximum_for_detail)
            .max(minimum_for_bucket_cap),
    )
}

pub fn build_histogram(
    entries: &[HistogramEntry],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    selected: BTreeSet<HistogramCategory>,
    explicit_bucket_size: Option<Duration>,
) -> HistogramData {
    let default_size = bucket_size_for_window(end - start);
    let empty = |reason| HistogramData {
        start,
        end,
        bucket_size: default_size,
        buckets: Vec::new(),
        selected: selected.clone(),
        max_count: 0,
        empty_reason: Some(reason),
    };
    if end <= start {
        return empty("Invalid time range.");
    }
    if selected.is_empty() {
        return empty("Select at least one status.");
    }
    if entries.is_empty() {
        return empty("No invocation history in this time range.");
    }

    let bucket_size = explicit_bucket_size.unwrap_or(default_size);
    let size_ms = bucket_size.num_milliseconds();
    assert!(size_ms > 0, "bucket size must be positive");
    let duration_ms = (end - start).num_milliseconds();
    // A sub-millisecond range still needs one drawable bucket.
    let bucket_count = ((duration_ms + size_ms - 1) / size_ms).max(1) as usize;
    let mut category_ids: Vec<[BTreeSet<String>; 3]> = (0..bucket_count)
        .map(|_| std::array::from_fn(|_| BTreeSet::new()))
        .collect();
    let mut task_ids: Vec<BTreeMap<String, BTreeSet<String>>> =
        (0..bucket_count).map(|_| BTreeMap::new()).collect();
    let mut runtime_ids: Vec<BTreeMap<String, BTreeSet<String>>> =
        (0..bucket_count).map(|_| BTreeMap::new()).collect();
    let mut runtime_task_ids: Vec<BTreeMap<String, BTreeMap<String, BTreeSet<String>>>> =
        (0..bucket_count).map(|_| BTreeMap::new()).collect();
    let mut runtime_runner_ids: Vec<BTreeMap<String, BTreeSet<String>>> =
        (0..bucket_count).map(|_| BTreeMap::new()).collect();

    let mut histories: HashMap<&str, Vec<&HistogramEntry>> = HashMap::new();
    for entry in entries {
        histories
            .entry(entry.invocation_id.as_str())
            .or_default()
            .push(entry);
    }
    for (invocation_id, history) in &mut histories {
        history.sort_by_key(|entry| entry.timestamp);
        let task_id = history
            .iter()
            .find(|entry| !entry.task_id.is_empty())
            .map_or("unknown", |entry| entry.task_id.as_str());
        for (index, entry) in history.iter().enumerate() {
            let Some(category) = category_for_status(entry.status) else {
                continue;
            };
            if !selected.contains(&category) || entry.status.is_terminal() {
                continue;
            }
            let runtime = if category == HistogramCategory::Running {
                entry
                    .runner_language
                    .clone()
                    .unwrap_or_else(|| language_from_task_id(task_id))
            } else {
                language_from_task_id(task_id)
            };
            let interval_start = entry.timestamp.max(start);
            let next = history.get(index + 1).map_or(end, |next| next.timestamp);
            let interval_end = next.min(end);
            let is_point_status =
                interval_end <= interval_start || interval_end - interval_start < bucket_size;
            if is_point_status && !(interval_start >= start && interval_start < end) {
                continue;
            }
            let occupied_start = interval_start;
            // Keep instantaneous transitions visible. The one-microsecond
            // synthetic span is only used for bucket selection; counts still
            // represent a unique invocation in that status.
            let occupied_end = if is_point_status {
                interval_start + Duration::microseconds(1)
            } else {
                interval_end
            };
            let first = ((occupied_start - start).num_milliseconds() / size_ms).max(0);
            let interval_end_ms = (occupied_end - start).num_milliseconds();
            let last = if is_point_status {
                first.min(bucket_count as i64 - 1)
            } else {
                (((interval_end_ms + size_ms - 1) / size_ms) - 1).min(bucket_count as i64 - 1)
            };
            for bucket_index in first..=last {
                let bucket_start = start + bucket_size * bucket_index as i32;
                let bucket_end = (bucket_start + bucket_size).min(end);
                let overlaps = if is_point_status {
                    interval_start >= bucket_start && interval_start < bucket_end
                } else {
                    interval_start < bucket_end && interval_end > bucket_start
                };
                if overlaps {
                    let bucket_index = bucket_index as usize;
                    category_ids[bucket_index][category.index()]
                        .insert((*invocation_id).to_owned());
                    task_ids[bucket_index]
                        .entry(task_id.to_owned())
                        .or_default()
                        .insert((*invocation_id).to_owned());
                    runtime_ids[bucket_index]
                        .entry(runtime.clone())
                        .or_default()
                        .insert((*invocation_id).to_owned());
                    runtime_task_ids[bucket_index]
                        .entry(runtime.clone())
                        .or_default()
                        .entry(task_id.to_owned())
                        .or_default()
                        .insert((*invocation_id).to_owned());
                    if category == HistogramCategory::Running {
                        if let Some(runner_id) = &entry.runner_id {
                            runtime_runner_ids[bucket_index]
                                .entry(runtime.clone())
                                .or_default()
                                .insert(runner_id.clone());
                        }
                    }
                }
            }
        }
    }

    let mut buckets = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        let bucket_start = start + bucket_size * index as i32;
        let bucket_end = (bucket_start + bucket_size).min(end);
        let counts = std::array::from_fn(|category| category_ids[index][category].len());
        let invocation_ids_by_category =
            std::array::from_fn(|category| category_ids[index][category].iter().cloned().collect());
        let invocation_ids = category_ids[index]
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        buckets.push(HistogramBucket {
            start: bucket_start,
            end: bucket_end,
            counts,
            counts_by_task: task_ids[index]
                .iter()
                .map(|(task, ids)| (task.clone(), ids.len()))
                .collect(),
            counts_by_runtime: runtime_ids[index]
                .iter()
                .map(|(runtime, ids)| (runtime.clone(), ids.len()))
                .collect(),
            counts_by_runtime_task: runtime_task_ids[index]
                .iter()
                .map(|(runtime, tasks)| {
                    (
                        runtime.clone(),
                        tasks
                            .iter()
                            .map(|(task, ids)| (task.clone(), ids.len()))
                            .collect(),
                    )
                })
                .collect(),
            invocation_ids_by_runtime: runtime_ids[index]
                .iter()
                .map(|(runtime, ids)| (runtime.clone(), ids.iter().cloned().collect::<Vec<_>>()))
                .collect(),
            runner_ids_by_runtime: runtime_runner_ids[index]
                .iter()
                .map(|(runtime, runner_ids)| {
                    (
                        runtime.clone(),
                        runner_ids.iter().cloned().collect::<Vec<_>>(),
                    )
                })
                .collect(),
            invocation_ids_by_category,
            invocation_ids,
        });
    }
    let max_count = buckets
        .iter()
        .map(HistogramBucket::total_count)
        .max()
        .unwrap_or_default();
    HistogramData {
        start,
        end,
        bucket_size,
        buckets,
        selected,
        max_count,
        empty_reason: (max_count == 0).then_some("No selected statuses occupied this time range."),
    }
}

#[derive(Debug, Clone)]
pub struct HistogramSelector {
    pub value: &'static str,
    pub label: &'static str,
    pub selected: bool,
}

#[derive(Debug, Clone)]
pub struct HistogramPanel {
    pub svg: String,
    pub legend_html: String,
    pub data_json: String,
    pub empty_reason: String,
    pub categories: Vec<HistogramSelector>,
    pub compact: bool,
    pub form_id: String,
    pub panel_id: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HistogramPanelOptions {
    pub y_axis_max: Option<usize>,
    pub plot_left: Option<f64>,
    pub plot_right: Option<f64>,
    pub relative_time: bool,
}

impl HistogramPanel {
    pub fn from_data(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
    ) -> Self {
        Self::from_data_with_options(
            data,
            common_params,
            link_path,
            compact,
            HistogramPanelOptions::default(),
        )
    }

    pub fn from_data_with_y_axis(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
        y_axis_max: Option<usize>,
    ) -> Self {
        Self::from_data_with_options(
            data,
            common_params,
            link_path,
            compact,
            HistogramPanelOptions {
                y_axis_max,
                ..HistogramPanelOptions::default()
            },
        )
    }

    /// Render a normalized comparison chart. Its time axis is elapsed time
    /// from each subject's start, so separate runs can be compared directly.
    pub fn from_data_comparison(data: &HistogramData, y_axis_max: Option<usize>) -> Self {
        Self::from_data_with_options(
            data,
            &[],
            "",
            true,
            HistogramPanelOptions {
                y_axis_max,
                relative_time: true,
                ..HistogramPanelOptions::default()
            },
        )
    }

    pub fn from_data_with_options(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
        options: HistogramPanelOptions,
    ) -> Self {
        let tasks = visible_tasks(data);
        let groups = histogram_groups(data);
        let panel_id = format!(
            "{HISTOGRAM_PANEL_PREFIX}-{}",
            NEXT_HISTOGRAM_PANEL.fetch_add(1, Ordering::Relaxed)
        );
        Self {
            svg: render_svg_with_y_axis_and_plot_bounds(SvgRenderRequest {
                data,
                common_params,
                link_path,
                compact,
                y_axis_max: options.y_axis_max,
                plot_left: options.plot_left,
                plot_right: options.plot_right,
                relative_time: options.relative_time,
                tasks: &tasks,
                groups: &groups,
            }),
            legend_html: render_legend_html(data, &tasks, &groups),
            data_json: render_client_data_json(data, &tasks, &groups, options.relative_time),
            empty_reason: data.empty_reason.unwrap_or_default().to_owned(),
            categories: HistogramCategory::ALL
                .into_iter()
                .map(|category| HistogramSelector {
                    value: category.value(),
                    label: category.label(),
                    selected: data.selected.contains(&category),
                })
                .collect(),
            compact,
            form_id: String::new(),
            panel_id,
        }
    }

    pub fn with_form_id(mut self, form_id: impl Into<String>) -> Self {
        self.form_id = form_id.into();
        self
    }
}

fn exact_statuses(selected: &BTreeSet<HistogramCategory>) -> String {
    let mut statuses = Vec::new();
    if selected.contains(&HistogramCategory::Registered) {
        statuses.extend(["Registered", "Rerouted", "ConcurrencyControlled"]);
    }
    if selected.contains(&HistogramCategory::Pending) {
        statuses.extend(["Pending", "PendingRecovery", "Retry"]);
    }
    if selected.contains(&HistogramCategory::Running) {
        statuses.extend(["Running", "RunningRecovery"]);
    }
    statuses.join(",")
}

#[derive(Serialize)]
struct HistogramClientGroup {
    id: String,
    label: String,
    color: &'static str,
    peak_workers: usize,
    worker_counts: Vec<usize>,
}

#[derive(Serialize)]
struct HistogramClientTask {
    id: String,
    label: String,
    color: &'static str,
    counts_by_runtime: BTreeMap<String, Vec<usize>>,
    totals_by_runtime: BTreeMap<String, usize>,
    total: usize,
}

#[derive(Serialize)]
struct HistogramClientData {
    labels: Vec<String>,
    groups: Vec<HistogramClientGroup>,
    tasks: Vec<HistogramClientTask>,
    totals_by_runtime: BTreeMap<String, usize>,
    total: usize,
}

fn task_count_for_runtime(
    bucket: &HistogramBucket,
    runtime: &str,
    task_id: &str,
    visible_task_set: &BTreeSet<String>,
) -> usize {
    let Some(counts_by_task) = bucket.counts_by_runtime_task.get(runtime) else {
        return 0;
    };
    if task_id == "__other__" {
        counts_by_task
            .iter()
            .filter(|(task, _)| !visible_task_set.contains(*task))
            .map(|(_, count)| count)
            .sum()
    } else {
        counts_by_task.get(task_id).copied().unwrap_or_default()
    }
}

fn task_totals_by_runtime(
    data: &HistogramData,
    groups: &[String],
    task_id: &str,
    visible_task_set: &BTreeSet<String>,
) -> BTreeMap<String, usize> {
    groups
        .iter()
        .map(|runtime| {
            let total = data
                .buckets
                .iter()
                .map(|bucket| task_count_for_runtime(bucket, runtime, task_id, visible_task_set))
                .sum();
            (runtime.clone(), total)
        })
        .collect()
}

fn runtime_total(data: &HistogramData, runtime: &str) -> usize {
    data.buckets
        .iter()
        .map(|bucket| {
            bucket
                .counts_by_runtime
                .get(runtime)
                .copied()
                .unwrap_or_default()
        })
        .sum()
}

fn peak_workers(data: &HistogramData, runtime: &str) -> usize {
    data.buckets
        .iter()
        .map(|bucket| {
            bucket
                .runner_ids_by_runtime
                .get(runtime)
                .map(Vec::len)
                .unwrap_or_default()
        })
        .max()
        .unwrap_or_default()
}

fn render_client_data_json(
    data: &HistogramData,
    tasks: &[VisibleTask],
    groups: &[String],
    relative_time: bool,
) -> String {
    let visible_task_set = tasks
        .iter()
        .filter(|task| task.id != "__other__")
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    let client = HistogramClientData {
        labels: data
            .buckets
            .iter()
            .map(|bucket| {
                if relative_time {
                    format_elapsed(bucket.start - data.start)
                } else {
                    bucket.start.format("%H:%M:%S").to_string()
                }
            })
            .collect(),
        groups: groups
            .iter()
            .map(|runtime| HistogramClientGroup {
                id: runtime.clone(),
                label: runtime_label(runtime),
                color: runtime_color(runtime),
                peak_workers: peak_workers(data, runtime),
                worker_counts: data
                    .buckets
                    .iter()
                    .map(|bucket| {
                        bucket
                            .runner_ids_by_runtime
                            .get(runtime)
                            .map(Vec::len)
                            .unwrap_or_default()
                    })
                    .collect(),
            })
            .collect(),
        tasks: tasks
            .iter()
            .map(|task| {
                let counts_by_runtime = groups
                    .iter()
                    .map(|runtime| {
                        (
                            runtime.clone(),
                            data.buckets
                                .iter()
                                .map(|bucket| {
                                    task_count_for_runtime(
                                        bucket,
                                        runtime,
                                        &task.id,
                                        &visible_task_set,
                                    )
                                })
                                .collect(),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                let totals_by_runtime =
                    task_totals_by_runtime(data, groups, &task.id, &visible_task_set);
                let total = totals_by_runtime.values().sum();
                HistogramClientTask {
                    id: task.id.clone(),
                    label: task.label.clone(),
                    color: task.color,
                    counts_by_runtime,
                    totals_by_runtime,
                    total,
                }
            })
            .collect(),
        totals_by_runtime: groups
            .iter()
            .map(|runtime| (runtime.clone(), runtime_total(data, runtime)))
            .collect(),
        total: groups
            .iter()
            .map(|runtime| runtime_total(data, runtime))
            .sum(),
    };
    serde_json::to_string(&client)
        .unwrap_or_else(|_| "{}".to_owned())
        .replace('<', "\\u003c")
        .replace('&', "\\u0026")
}

fn render_legend_html(data: &HistogramData, tasks: &[VisibleTask], groups: &[String]) -> String {
    if tasks.is_empty() || groups.is_empty() {
        return String::new();
    }
    let visible_task_set = tasks
        .iter()
        .filter(|task| task.id != "__other__")
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    let mut out = String::with_capacity(8192);
    out.push_str(r#"<div class="histogram-legend-head"><span>Hover for per-bucket detail</span><span class="histogram-legend-time" data-histogram-time></span></div>"#);
    out.push_str(r#"<table class="histogram-legend-table"><thead><tr><th class="histogram-swatch-col"></th><th class="histogram-task-col">Task</th>"#);
    for runtime in groups {
        let peak = peak_workers(data, runtime);
        let _ = write!(
            out,
            r#"<th><span class="histogram-runtime-marker" style="background:{}"></span>{}<span class="histogram-worker-badge" title="Active workers" data-histogram-worker-badge="{}" data-total="{peak}">{peak}</span></th>"#,
            runtime_color(runtime),
            xml_escape(&runtime_short_label(runtime)),
            xml_escape(runtime),
        );
    }
    out.push_str(r#"<th>Total</th></tr></thead><tbody>"#);
    for task in tasks {
        let totals_by_runtime = task_totals_by_runtime(data, groups, &task.id, &visible_task_set);
        let row_total = totals_by_runtime.values().sum::<usize>();
        let _ = write!(
            out,
            r#"<tr class="histogram-task-row" data-histogram-task-id="{}"><td><span class="histogram-swatch" style="background:{}"></span></td><td class="histogram-task-col" title="{}">{}</td>"#,
            xml_escape(&task.id),
            task.color,
            xml_escape(&task.label),
            xml_escape(&task.label),
        );
        for runtime in groups {
            let count = totals_by_runtime.get(runtime).copied().unwrap_or_default();
            let muted = if count == 0 { " histogram-muted" } else { "" };
            let _ = write!(
                out,
                r#"<td class="histogram-count{muted}" data-histogram-cell="{}" data-total="{count}">{count}</td>"#,
                xml_escape(runtime),
            );
        }
        let _ = write!(
            out,
            r#"<td class="histogram-count" data-histogram-cell="total" data-total="{row_total}">{row_total}</td></tr>"#
        );
    }
    out.push_str(r#"<tr class="histogram-footer-row"><td></td><td class="histogram-task-col histogram-muted">Column totals</td>"#);
    for runtime in groups {
        let total = runtime_total(data, runtime);
        let _ = write!(
            out,
            r#"<td class="histogram-count" data-histogram-footer="{}" data-total="{total}">{total}</td>"#,
            xml_escape(runtime),
        );
    }
    let total = groups
        .iter()
        .map(|runtime| runtime_total(data, runtime))
        .sum::<usize>();
    let _ = write!(
        out,
        r#"<td class="histogram-count" data-histogram-footer="total" data-total="{total}">{total}</td></tr></tbody></table>"#
    );
    out
}

#[cfg(test)]
fn render_svg_with_y_axis(
    data: &HistogramData,
    common_params: &[(String, String)],
    link_path: &str,
    compact: bool,
    y_axis_max: Option<usize>,
) -> String {
    render_svg_with_y_axis_and_plot_bounds(SvgRenderRequest {
        data,
        common_params,
        link_path,
        compact,
        y_axis_max,
        plot_left: None,
        plot_right: None,
        relative_time: false,
        tasks: &visible_tasks(data),
        groups: &histogram_groups(data),
    })
}

struct SvgRenderRequest<'a> {
    data: &'a HistogramData,
    common_params: &'a [(String, String)],
    link_path: &'a str,
    compact: bool,
    y_axis_max: Option<usize>,
    plot_left: Option<f64>,
    plot_right: Option<f64>,
    relative_time: bool,
    tasks: &'a [VisibleTask],
    groups: &'a [String],
}

#[allow(clippy::too_many_lines)]
fn render_svg_with_y_axis_and_plot_bounds(request: SvgRenderRequest<'_>) -> String {
    let SvgRenderRequest {
        data,
        common_params,
        link_path,
        compact,
        y_axis_max,
        plot_left,
        plot_right,
        relative_time,
        tasks,
        groups,
    } = request;

    if data.buckets.is_empty() || data.max_count == 0 || groups.is_empty() {
        return String::new();
    }
    let width = 2000.0;
    let left = plot_left.unwrap_or(420.0);
    let right = plot_right.unwrap_or(1956.0);
    let chart_height = if compact { 96.0 } else { 138.0 };
    let chart_gap = if compact { 10.0 } else { 14.0 };
    let top_margin = 6.0;
    let plot_header_height = if compact { 20.0 } else { 24.0 };
    let plot_bottom_pad = if compact { 18.0 } else { 24.0 };
    let plot_height = chart_height - plot_header_height - plot_bottom_pad;
    let svg_height =
        top_margin + groups.len() as f64 * chart_height + (groups.len() - 1) as f64 * chart_gap;
    let duration_ms = (data.end - data.start).num_milliseconds() as f64;
    let visible_task_set = tasks
        .iter()
        .filter(|task| task.id != "__other__")
        .map(|task| task.id.clone())
        .collect::<BTreeSet<_>>();
    let task_colors = tasks
        .iter()
        .map(|task| (task.id.as_str(), task.color))
        .collect::<HashMap<_, _>>();
    let max_runtime_count = data
        .buckets
        .iter()
        .flat_map(|bucket| {
            groups.iter().map(|runtime| {
                bucket
                    .counts_by_runtime
                    .get(runtime)
                    .copied()
                    .unwrap_or_default()
            })
        })
        .max()
        .unwrap_or_default();
    let scale_max = max_runtime_count.max(y_axis_max.unwrap_or_default()).max(1);
    let worker_scale_max = groups
        .iter()
        .map(|runtime| peak_workers(data, runtime))
        .max()
        .unwrap_or_default()
        .max(1);
    let selected_names = data
        .selected
        .iter()
        .map(|category| category.value())
        .collect::<Vec<_>>()
        .join(",");
    let mut out = String::with_capacity(16_384);
    let task_desc = tasks
        .iter()
        .map(|task| task.label.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let _ = write!(
        out,
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {width} {svg_height}" preserveAspectRatio="xMinYMin meet" data-histogram-start="{}" data-histogram-end="{}" data-histogram-left="{left}" data-histogram-right="{right}" data-statuses="{selected_names}" role="img" aria-label="Task occupancy by runtime"><rect width="{width}" height="{svg_height}" fill="#fff"/><desc>Tasks: {}</desc>"##,
        data.start.to_rfc3339(),
        data.end.to_rfc3339(),
        xml_escape(&task_desc),
    );
    let tick_step = ((scale_max + 3) / 4).max(1);
    let mut tick_values = (0..=scale_max).step_by(tick_step).collect::<Vec<_>>();
    if tick_values.last().copied() != Some(scale_max) {
        tick_values.push(scale_max);
    }
    let plot_width = right - left;
    for (group_index, runtime) in groups.iter().enumerate() {
        let group_top = top_margin + group_index as f64 * (chart_height + chart_gap);
        let plot_top = group_top + plot_header_height;
        let plot_bottom = group_top + chart_height - plot_bottom_pad;
        let runtime_total = runtime_total(data, runtime);
        let runtime_peak_workers = peak_workers(data, runtime);
        let runtime_color = runtime_color(runtime);
        let _ = write!(
            out,
            r##"<g class="histogram-runtime-group" data-runtime="{}"><rect x="{}" y="{:.2}" width="4" height="14" rx="2" fill="{}"/><text x="{}" y="{:.2}" font-size="11" font-weight="700" fill="#334155">{}</text><text x="{}" y="{:.2}" font-size="9" fill="#64748b">max {scale_max} tasks | peak {runtime_peak_workers} workers | samples {runtime_total}</text>"##,
            xml_escape(runtime),
            left + 2.0,
            group_top + 2.0,
            runtime_color,
            left + 12.0,
            group_top + 12.0,
            xml_escape(&runtime_label(runtime)),
            left + 112.0,
            group_top + 12.0,
        );
        let _ = write!(
            out,
            r##"<rect x="{left}" y="{plot_top:.2}" width="{plot_width:.2}" height="{plot_height:.2}" fill="#fcfcfd" stroke="#edf0f3" stroke-width="0.8"/>"##
        );
        for tick_index in 0..=8 {
            let x = left + plot_width * f64::from(tick_index) / 8.0;
            let _ = write!(
                out,
                r##"<line x1="{x:.2}" y1="{plot_top:.2}" x2="{x:.2}" y2="{plot_bottom:.2}" stroke="#f1f3f5" stroke-width="0.8"/>"##
            );
        }
        for tick in &tick_values {
            let y = plot_bottom - *tick as f64 / scale_max as f64 * plot_height;
            let _ = write!(
                out,
                r##"<line x1="{left}" y1="{y:.2}" x2="{right}" y2="{y:.2}" stroke="#e9ecef" stroke-width="0.8"/><text x="{}" y="{:.2}" text-anchor="end" font-size="9" fill="#6c757d">{tick}</text>"##,
                left - 8.0,
                y + 3.0,
            );
        }
        let _ = write!(
            out,
            r##"<text x="{}" y="{:.2}" text-anchor="end" font-size="9" fill="#6c757d">tasks</text>"##,
            left - 8.0,
            plot_top - 6.0,
        );
        let _ = write!(
            out,
            r##"<line x1="{left}" y1="{plot_bottom:.2}" x2="{right}" y2="{plot_bottom:.2}" stroke="#ced4da" stroke-width="0.9"/>"##
        );
        let worker_line_points = if runtime_peak_workers > 0 {
            let mut points = String::new();
            for bucket in &data.buckets {
                let start_ratio =
                    (bucket.start - data.start).num_milliseconds() as f64 / duration_ms;
                let end_ratio = (bucket.end - data.start).num_milliseconds() as f64 / duration_ms;
                let bucket_center = left + ((start_ratio + end_ratio) / 2.0) * plot_width;
                let worker_count = bucket
                    .runner_ids_by_runtime
                    .get(runtime)
                    .map(Vec::len)
                    .unwrap_or_default();
                let y = plot_bottom - worker_count as f64 / worker_scale_max as f64 * plot_height;
                let _ = write!(points, "{bucket_center:.2},{y:.2} ");
            }
            Some(points)
        } else {
            None
        };
        for (bucket_index, bucket) in data.buckets.iter().enumerate() {
            let runtime_bucket_total = bucket
                .counts_by_runtime
                .get(runtime)
                .copied()
                .unwrap_or_default();
            let runtime_worker_count = bucket
                .runner_ids_by_runtime
                .get(runtime)
                .map(Vec::len)
                .unwrap_or_default();
            if runtime_bucket_total == 0 && runtime_worker_count == 0 {
                continue;
            }
            let start_ratio = (bucket.start - data.start).num_milliseconds() as f64 / duration_ms;
            let end_ratio = (bucket.end - data.start).num_milliseconds() as f64 / duration_ms;
            let bucket_center = left + ((start_ratio + end_ratio) / 2.0) * plot_width;
            let bucket_width = (end_ratio - start_ratio) * plot_width;
            // Every bucket reserves the same two-pixel gutter. A proportional
            // gap and a max-width clamp made the visual rhythm change across
            // time windows, despite each bar representing the same bucket.
            let bar_width = (bucket_width - BAR_GAP_PX).max(1.0);
            let x = (bucket_center - bar_width / 2.0)
                .max(left)
                .min(right - bar_width);
            let mut y = plot_bottom;
            let mut top_tasks = bucket
                .counts_by_runtime_task
                .get(runtime)
                .map(|tasks| tasks.iter().collect::<Vec<_>>())
                .unwrap_or_default();
            top_tasks.sort_by(|(left_task, left_count), (right_task, right_count)| {
                right_count
                    .cmp(left_count)
                    .then_with(|| left_task.cmp(right_task))
            });
            let tasks_summary = top_tasks
                .into_iter()
                .take(5)
                .map(|(task, count)| format!("{task}: {count}"))
                .collect::<Vec<_>>()
                .join("\n");
            let time_label = if relative_time {
                format_elapsed(bucket.start - data.start)
            } else {
                bucket.start.format("%H:%M:%S").to_string()
            };
            let tooltip = format!(
                "{} {}\n{} tasks | {} workers{}",
                runtime_label(runtime),
                time_label,
                runtime_bucket_total,
                runtime_worker_count,
                if tasks_summary.is_empty() {
                    String::new()
                } else {
                    format!("\n{tasks_summary}")
                }
            );
            if link_path.is_empty() {
                let _ = write!(
                    out,
                    r#"<g class="histogram-bucket" tabindex="0" data-bucket-index="{bucket_index}" data-runtime="{}" data-bucket-start="{}" data-bucket-end="{}" data-statuses="{selected_names}" data-tooltip="{}">"#,
                    xml_escape(runtime),
                    bucket.start.to_rfc3339(),
                    bucket.end.to_rfc3339(),
                    xml_escape(&tooltip),
                );
            } else {
                let mut serializer = url::form_urlencoded::Serializer::new(String::new());
                for (key, value) in common_params {
                    serializer.append_pair(key, value);
                }
                let status_value = if link_path == "/invocations" {
                    exact_statuses(&data.selected)
                } else {
                    selected_names.clone()
                };
                serializer
                    .append_pair("time_range", "custom")
                    .append_pair("start_date", &bucket.start.to_rfc3339())
                    .append_pair("end_date", &bucket.end.to_rfc3339())
                    .append_pair(
                        if link_path == "/invocations" {
                            "status"
                        } else {
                            "histogram_status"
                        },
                        &status_value,
                    );
                if link_path == "/invocations" {
                    serializer.append_pair("status_mode", "history");
                }
                let scoped_invocation_ids: &[String] = bucket
                    .invocation_ids_by_runtime
                    .get(runtime)
                    .map_or(&[], Vec::as_slice);
                let has_common_invocation_scope =
                    common_params.iter().any(|(key, _)| key == "inv_ids");
                if !has_common_invocation_scope
                    && !scoped_invocation_ids.is_empty()
                    && scoped_invocation_ids.len() <= 50
                {
                    serializer.append_pair("inv_ids", &scoped_invocation_ids.join(","));
                }
                let href = format!("{link_path}?{}", serializer.finish());
                let _ = write!(
                    out,
                    r#"<a href="{}" class="histogram-bucket-link"><g class="histogram-bucket" tabindex="0" data-bucket-index="{bucket_index}" data-runtime="{}" data-bucket-start="{}" data-bucket-end="{}" data-statuses="{selected_names}" data-tooltip="{}">"#,
                    xml_escape(&href),
                    xml_escape(runtime),
                    bucket.start.to_rfc3339(),
                    bucket.end.to_rfc3339(),
                    xml_escape(&tooltip),
                );
            }
            let _ = write!(
                out,
                r##"<rect x="{x:.2}" y="{plot_top:.2}" width="{bar_width:.2}" height="{plot_height:.2}" fill="transparent"/>"##
            );
            for (task_id, count) in bucket_task_counts_for_runtime(
                bucket.counts_by_runtime_task.get(runtime),
                &visible_task_set,
            ) {
                if count == 0 {
                    continue;
                }
                let segment_height = count as f64 / scale_max as f64 * plot_height;
                y -= segment_height;
                let label = if task_id == "__other__" {
                    "Other"
                } else {
                    task_id.as_str()
                };
                let _ = write!(
                    out,
                    r#"<rect x="{x:.2}" y="{y:.2}" width="{bar_width:.2}" height="{segment_height:.2}" fill="{}" class="histogram-task-segment" data-task="{}" data-task-id="{}" data-runtime="{}"/>"#,
                    task_colors
                        .get(task_id.as_str())
                        .copied()
                        .unwrap_or_else(|| task_color(&task_id)),
                    xml_escape(label),
                    xml_escape(&task_id),
                    xml_escape(runtime),
                );
            }
            let _ = write!(out, "<title>{}</title></g>", xml_escape(&tooltip));
            if !link_path.is_empty() {
                out.push_str("</a>");
            }
        }
        // SVG uses document order for paint order. Emit the worker series after
        // every task segment so the line remains legible over tall bar stacks.
        if let Some(points) = worker_line_points {
            let _ = write!(
                out,
                r##"<line x1="{right}" y1="{plot_top:.2}" x2="{right}" y2="{plot_bottom:.2}" stroke="{runtime_color}" stroke-width="0.8" opacity="0.45"/><text x="{}" y="{:.2}" font-size="9" fill="{runtime_color}" text-anchor="start">{runtime_peak_workers}</text><text x="{}" y="{:.2}" font-size="9" fill="{runtime_color}" text-anchor="start">0</text><polyline points="{}" fill="none" stroke="{runtime_color}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" opacity="0.9" class="histogram-worker-line" data-runtime="{}"/><text x="{}" y="{:.2}" font-size="9" fill="{runtime_color}" text-anchor="end">workers</text>"##,
                right + 5.0,
                plot_top + 3.0,
                right + 5.0,
                plot_bottom + 3.0,
                xml_escape(points.trim()),
                xml_escape(runtime),
                right - 4.0,
                plot_top + 10.0,
            );
        }
        let duration = data.end - data.start;
        let duration_seconds = duration.num_milliseconds() as f64 / 1_000.0;
        for tick_index in 0..=8 {
            let ratio = f64::from(tick_index) / 8.0;
            let timestamp = data.start
                + Duration::milliseconds((duration.num_milliseconds() as f64 * ratio) as i64);
            let label = if relative_time {
                format_elapsed(timestamp - data.start)
            } else if duration_seconds <= 10.0 {
                timestamp.format("%H:%M:%S%.3f").to_string()
            } else if duration_seconds <= 3_600.0 {
                timestamp.format("%H:%M:%S").to_string()
            } else if duration_seconds <= 86_400.0 {
                timestamp.format("%H:%M").to_string()
            } else {
                timestamp.format("%m/%d %H:%M").to_string()
            };
            let x = left + plot_width * ratio;
            let anchor = if tick_index == 0 {
                "start"
            } else if tick_index == 8 {
                "end"
            } else {
                "middle"
            };
            let _ = write!(
                out,
                r##"<line x1="{x:.2}" y1="{plot_bottom:.2}" x2="{x:.2}" y2="{:.2}" stroke="#adb5bd" stroke-width="0.8"/><text x="{x:.2}" y="{:.2}" font-size="8.5" fill="#64748b" text-anchor="{anchor}">{}</text>"##,
                plot_bottom + 4.0,
                plot_bottom + 12.0,
                xml_escape(&label),
            );
        }
        out.push_str("</g>");
    }
    out.push_str("</svg>");
    out
}

fn format_elapsed(duration: Duration) -> String {
    let milliseconds = duration.num_milliseconds().max(0);
    if milliseconds < 1_000 {
        format!("+{milliseconds}ms")
    } else if milliseconds < 60_000 {
        format!("+{:.2}s", milliseconds as f64 / 1_000.0)
    } else {
        format!("+{:.1}m", milliseconds as f64 / 60_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 2, 12, 0, 0).unwrap()
    }

    fn entry(id: &str, seconds: i64, status: InvocationStatus, task: &str) -> HistogramEntry {
        HistogramEntry {
            invocation_id: id.to_owned(),
            task_id: task.to_owned(),
            runner_id: None,
            runner_language: None,
            status,
            timestamp: start() + Duration::seconds(seconds),
        }
    }

    #[test]
    fn comparison_panels_use_elapsed_time_without_invalid_drilldowns() {
        let data = build_histogram(
            &[
                entry("one", 0, InvocationStatus::Pending, "rust::test.first"),
                entry("one", 2, InvocationStatus::Running, "rust::test.first"),
            ],
            start(),
            start() + Duration::seconds(4),
            [HistogramCategory::Pending, HistogramCategory::Running]
                .into_iter()
                .collect(),
            None,
        );
        let panel = HistogramPanel::from_data_comparison(&data, Some(1));
        assert!(panel.svg.contains("+0ms"));
        assert!(!panel.svg.contains("class=\"histogram-bucket-link\""));
        assert!(panel.data_json.contains("+0ms"));
    }

    fn entry_millis(
        id: &str,
        milliseconds: i64,
        status: InvocationStatus,
        task: &str,
    ) -> HistogramEntry {
        HistogramEntry {
            invocation_id: id.to_owned(),
            task_id: task.to_owned(),
            runner_id: None,
            runner_language: None,
            status,
            timestamp: start() + Duration::milliseconds(milliseconds),
        }
    }

    fn runtime_entry(
        id: &str,
        seconds: i64,
        status: InvocationStatus,
        task: &str,
        runner_id: &str,
        runner_language: &str,
    ) -> HistogramEntry {
        HistogramEntry {
            invocation_id: id.to_owned(),
            task_id: task.to_owned(),
            runner_id: Some(runner_id.to_owned()),
            runner_language: Some(runner_language.to_owned()),
            status,
            timestamp: start() + Duration::seconds(seconds),
        }
    }

    fn all() -> BTreeSet<HistogramCategory> {
        HistogramCategory::ALL.into_iter().collect()
    }

    #[test]
    fn long_running_invocation_occupies_every_overlapping_bucket() {
        let data = build_histogram(
            &[
                entry("i1", 0, InvocationStatus::Running, "tests.task"),
                entry("i1", 12, InvocationStatus::Success, "tests.task"),
            ],
            start(),
            start() + Duration::seconds(15),
            [HistogramCategory::Running].into_iter().collect(),
            Some(Duration::seconds(5)),
        );
        assert_eq!(
            data.buckets
                .iter()
                .map(HistogramBucket::total_count)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }

    #[test]
    fn category_transitions_use_half_open_bucket_boundaries() {
        let data = build_histogram(
            &[
                entry("i1", -5, InvocationStatus::Pending, "tests.task"),
                entry("i1", 5, InvocationStatus::Running, "tests.task"),
            ],
            start(),
            start() + Duration::seconds(10),
            all(),
            Some(Duration::seconds(5)),
        );
        assert_eq!(data.buckets[0].counts, [0, 1, 0]);
        assert_eq!(data.buckets[1].counts, [0, 0, 1]);
    }

    #[test]
    fn same_category_is_counted_once_and_selection_filters_ids() {
        let data = build_histogram(
            &[
                entry("i1", 0, InvocationStatus::Registered, "alpha"),
                entry("i1", 2, InvocationStatus::Rerouted, "alpha"),
                entry("i2", 0, InvocationStatus::Running, "beta"),
            ],
            start(),
            start() + Duration::seconds(5),
            [HistogramCategory::Registered].into_iter().collect(),
            Some(Duration::seconds(5)),
        );
        assert_eq!(data.buckets[0].counts, [1, 0, 0]);
        assert_eq!(data.buckets[0].invocation_ids, vec!["i1"]);
        assert_eq!(data.buckets[0].counts_by_task.get("alpha"), Some(&1));
    }

    #[test]
    fn final_status_and_empty_selection_have_no_occupancy() {
        let final_data = build_histogram(
            &[entry("i1", 1, InvocationStatus::Success, "task")],
            start(),
            start() + Duration::seconds(10),
            all(),
            Some(Duration::seconds(5)),
        );
        assert_eq!(final_data.max_count, 0);
        let no_selection = build_histogram(
            &[entry("i1", 1, InvocationStatus::Running, "task")],
            start(),
            start() + Duration::seconds(10),
            BTreeSet::new(),
            None,
        );
        assert_eq!(
            no_selection.empty_reason,
            Some("Select at least one status.")
        );
    }

    #[test]
    fn instantaneous_and_sub_millisecond_statuses_are_visible() {
        let instant_data = build_histogram(
            &[
                entry_millis("i1", 2, InvocationStatus::Pending, "task"),
                entry_millis("i1", 2, InvocationStatus::Running, "task"),
                entry_millis("i1", 2, InvocationStatus::Success, "task"),
            ],
            start(),
            start() + Duration::milliseconds(5),
            [HistogramCategory::Pending, HistogramCategory::Running]
                .into_iter()
                .collect(),
            None,
        );
        assert_eq!(instant_data.max_count, 1);
        assert!(instant_data
            .buckets
            .iter()
            .any(|bucket| bucket.total_count() > 0));

        let sub_millisecond_data = build_histogram(
            &[
                HistogramEntry {
                    timestamp: start() + Duration::microseconds(200),
                    ..entry("i2", 0, InvocationStatus::Running, "task")
                },
                HistogramEntry {
                    timestamp: start() + Duration::microseconds(700),
                    ..entry("i2", 0, InvocationStatus::Success, "task")
                },
            ],
            start() + Duration::microseconds(100),
            start() + Duration::microseconds(900),
            [HistogramCategory::Running].into_iter().collect(),
            None,
        );
        assert_eq!(sub_millisecond_data.buckets.len(), 1);
        assert_eq!(sub_millisecond_data.max_count, 1);
    }

    #[test]
    fn automatic_resolution_is_capped_and_parser_preserves_empty() {
        assert!(bucket_size_for_window(Duration::seconds(5)) <= Duration::milliseconds(250));
        assert!(
            bucket_size_for_window(Duration::seconds(3)) <= Duration::milliseconds(14),
            "short workflow windows need fine-grained occupancy bars"
        );
        assert!(bucket_size_for_window(Duration::days(30)) >= Duration::hours(3));
        assert_eq!(parse_categories(Some("")), BTreeSet::new());
        assert_eq!(
            parse_categories(None),
            [HistogramCategory::Pending, HistogramCategory::Running]
                .into_iter()
                .collect()
        );
        assert_eq!(
            serialize_categories(
                &[HistogramCategory::Running, HistogramCategory::Registered]
                    .into_iter()
                    .collect()
            ),
            "registered,running"
        );
    }

    #[test]
    fn zoomed_svg_caps_bar_width_and_skips_empty_buckets() {
        let data = build_histogram(
            &[entry_millis("i1", 200, InvocationStatus::Running, "task")],
            start(),
            start() + Duration::milliseconds(474),
            [HistogramCategory::Running].into_iter().collect(),
            None,
        );
        assert!(data.buckets.len() >= 27);
        let svg = render_svg_with_y_axis(&data, &[], "/invocations", false, None);
        let widths = svg
            .split("<rect ")
            .filter(|fragment| fragment.contains("data-task=\""))
            .filter_map(|fragment| fragment.split("width=\"").nth(1))
            .filter_map(|fragment| fragment.split('"').next())
            .filter_map(|width| width.parse::<f64>().ok())
            .collect::<Vec<_>>();
        assert!(!widths.is_empty());
        assert!(widths.iter().all(|width| (3.0..=16.0).contains(width)));
        assert!(widths.len() < data.buckets.len());
    }

    #[test]
    fn svg_exposes_bounds_bucket_metadata_and_drilldown() {
        let data = build_histogram(
            &[entry("i1", 0, InvocationStatus::Running, "task")],
            start(),
            start() + Duration::seconds(5),
            all(),
            Some(Duration::seconds(5)),
        );
        let svg = render_svg_with_y_axis(&data, &[], "/invocations", false, None);
        assert!(svg.contains("data-histogram-start="));
        assert!(svg.contains("data-bucket-start="));
        assert!(svg.contains("time_range=custom"));
        assert!(svg.contains("inv_ids=i1"));
        assert!(svg.contains("data-task=\"task\""));
    }

    #[test]
    fn svg_stacks_multiple_task_types_with_stable_colors() {
        let data = build_histogram(
            &[
                entry("i1", 0, InvocationStatus::Running, "alpha.task"),
                entry("i2", 0, InvocationStatus::Running, "beta.task"),
            ],
            start(),
            start() + Duration::seconds(5),
            [HistogramCategory::Running].into_iter().collect(),
            Some(Duration::seconds(1)),
        );
        let svg = render_svg_with_y_axis(&data, &[], "/invocations", false, None);
        assert!(svg.contains("data-task=\"alpha.task\""));
        assert!(svg.contains("data-task=\"beta.task\""));
        assert!(svg.contains("alpha.task"));
        assert!(svg.contains("beta.task"));
    }

    #[test]
    fn svg_paints_worker_line_after_task_bars() {
        let data = build_histogram(
            &[
                runtime_entry(
                    "i1",
                    0,
                    InvocationStatus::Running,
                    "alpha.task",
                    "runner-1",
                    "rust",
                ),
                entry("i1", 3, InvocationStatus::Success, "alpha.task"),
            ],
            start(),
            start() + Duration::seconds(4),
            [HistogramCategory::Running].into_iter().collect(),
            Some(Duration::seconds(1)),
        );
        let svg = render_svg_with_y_axis(&data, &[], "/invocations", false, None);
        let bar_position = svg.find("histogram-task-segment").expect("task bar");
        let worker_line_position = svg.find("histogram-worker-line").expect("worker line");

        assert!(
            worker_line_position > bar_position,
            "the worker line must be painted over the task bars"
        );
    }

    #[test]
    fn running_occupancy_uses_runner_language_when_known() {
        let data = build_histogram(
            &[
                entry("i1", 0, InvocationStatus::Pending, "rust::demo.cross"),
                runtime_entry(
                    "i1",
                    1,
                    InvocationStatus::Running,
                    "rust::demo.cross",
                    "python-worker",
                    "python",
                ),
                entry("i1", 3, InvocationStatus::Success, "rust::demo.cross"),
            ],
            start(),
            start() + Duration::seconds(4),
            all(),
            Some(Duration::seconds(1)),
        );

        assert_eq!(data.buckets[0].counts_by_runtime.get("rust"), Some(&1));
        assert_eq!(data.buckets[1].counts_by_runtime.get("python"), Some(&1));
        assert_eq!(
            data.buckets[1].runner_ids_by_runtime.get("python"),
            Some(&vec!["python-worker".to_owned()])
        );

        let panel = HistogramPanel::from_data(&data, &[], "/invocations", false);
        assert!(panel.svg.contains("data-runtime=\"python\""));
        assert!(panel.svg.contains("data-task-id=\"rust::demo.cross\""));
        assert!(panel.legend_html.contains(">Python"));
        assert!(panel.data_json.contains("rust::demo.cross"));
    }

    #[test]
    fn other_uses_neutral_color() {
        assert_eq!(task_color("__other__"), "#cccccc");
    }

    #[test]
    fn links_use_matching_status_scope_without_duplicate_invocation_ids() {
        let data = build_histogram(
            &[entry("i1", 0, InvocationStatus::Running, "task")],
            start(),
            start() + Duration::seconds(5),
            [HistogramCategory::Running].into_iter().collect(),
            Some(Duration::seconds(1)),
        );
        let list_svg = render_svg_with_y_axis(&data, &[], "/invocations", false, None);
        let timeline_svg = render_svg_with_y_axis(
            &data,
            &[("inv_ids".to_owned(), "scope-1".to_owned())],
            "/invocations/timeline",
            false,
            None,
        );
        assert!(list_svg.contains("status_mode=history"));
        assert!(list_svg.contains("status=Running"));
        assert!(timeline_svg.contains("histogram_status=running"));
        let hrefs = timeline_svg
            .split("href=\"")
            .skip(1)
            .map(|fragment| fragment.split('"').next().unwrap_or_default())
            .collect::<Vec<_>>();
        assert!(!hrefs.is_empty());
        assert!(hrefs
            .iter()
            .all(|href| href.matches("inv_ids=").count() == 1));
    }
}
