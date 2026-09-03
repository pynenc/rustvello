//! Pure invocation-state occupancy histogram model and SVG renderer.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write;

use chrono::{DateTime, Duration, Utc};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatus;

use crate::util::escape::xml_escape;

const MAX_BUCKETS: i64 = 240;
const HISTOGRAM_PLOT_WIDTH_PX: i64 = 1_680;
const MIN_BAR_WIDTH_PX: f64 = 3.0;
// Timeline status points use a 5px radius, so occupancy bars target their 10px diameter.
const TIMELINE_POINT_DIAMETER_PX: f64 = 10.0;
const MAX_BAR_WIDTH_PX: f64 = TIMELINE_POINT_DIAMETER_PX;
const MAX_TASK_LEGEND: usize = 12;
const TASK_PALETTE: [&str; 15] = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7",
    "#9c755f", "#bab0ac", "#86bcb6", "#8cd17d", "#b6992d", "#499894", "#d37295",
];

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
    pub status: InvocationStatus,
    pub timestamp: DateTime<Utc>,
}

impl HistogramEntry {
    pub fn from_history(history: &InvocationHistory, task_id: &str) -> Self {
        Self {
            invocation_id: history.invocation_id.to_string(),
            task_id: task_id.to_owned(),
            status: history.status_record.status,
            timestamp: history
                .history_timestamp
                .unwrap_or(history.status_record.timestamp),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub counts: [usize; 3],
    pub counts_by_task: BTreeMap<String, usize>,
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
    let minimum_buckets_for_bar_width =
        (HISTOGRAM_PLOT_WIDTH_PX + MAX_BAR_WIDTH_PX as i64 - 1) / MAX_BAR_WIDTH_PX as i64;
    let maximum_for_bar_width = (milliseconds / minimum_buckets_for_bar_width).max(1);
    Duration::milliseconds(
        candidate
            .min(maximum_for_bar_width)
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
    let bucket_count = ((duration_ms + size_ms - 1) / size_ms) as usize;
    let mut category_ids: Vec<[BTreeSet<String>; 3]> = (0..bucket_count)
        .map(|_| std::array::from_fn(|_| BTreeSet::new()))
        .collect();
    let mut task_ids: Vec<BTreeMap<String, BTreeSet<String>>> =
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
            let interval_start = entry.timestamp.max(start);
            let next = history.get(index + 1).map_or(end, |next| next.timestamp);
            let interval_end = next.min(end);
            if interval_end <= interval_start {
                continue;
            }
            let first = ((interval_start - start).num_milliseconds() / size_ms).max(0);
            let interval_end_ms = (interval_end - start).num_milliseconds();
            let last =
                (((interval_end_ms + size_ms - 1) / size_ms) - 1).min(bucket_count as i64 - 1);
            for bucket_index in first..=last {
                let bucket_start = start + bucket_size * bucket_index as i32;
                let bucket_end = (bucket_start + bucket_size).min(end);
                if interval_start < bucket_end && interval_end > bucket_start {
                    let bucket_index = bucket_index as usize;
                    category_ids[bucket_index][category.index()]
                        .insert((*invocation_id).to_owned());
                    task_ids[bucket_index]
                        .entry(task_id.to_owned())
                        .or_default()
                        .insert((*invocation_id).to_owned());
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
    pub empty_reason: String,
    pub categories: Vec<HistogramSelector>,
    pub compact: bool,
    pub form_id: String,
}

impl HistogramPanel {
    pub fn from_data(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
    ) -> Self {
        Self::from_data_with_y_axis_and_plot_bounds(
            data,
            common_params,
            link_path,
            compact,
            None,
            None,
            None,
        )
    }

    pub fn from_data_with_y_axis(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
        y_axis_max: Option<usize>,
    ) -> Self {
        Self::from_data_with_y_axis_and_plot_bounds(
            data,
            common_params,
            link_path,
            compact,
            y_axis_max,
            None,
            None,
        )
    }

    pub fn from_data_with_y_axis_and_plot_bounds(
        data: &HistogramData,
        common_params: &[(String, String)],
        link_path: &str,
        compact: bool,
        y_axis_max: Option<usize>,
        plot_left: Option<f64>,
        plot_right: Option<f64>,
    ) -> Self {
        Self {
            svg: render_svg_with_y_axis_and_plot_bounds(
                data,
                common_params,
                link_path,
                compact,
                y_axis_max,
                plot_left,
                plot_right,
            ),
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

#[cfg(test)]
fn render_svg_with_y_axis(
    data: &HistogramData,
    common_params: &[(String, String)],
    link_path: &str,
    compact: bool,
    y_axis_max: Option<usize>,
) -> String {
    render_svg_with_y_axis_and_plot_bounds(
        data,
        common_params,
        link_path,
        compact,
        y_axis_max,
        None,
        None,
    )
}

fn render_svg_with_y_axis_and_plot_bounds(
    data: &HistogramData,
    common_params: &[(String, String)],
    link_path: &str,
    compact: bool,
    y_axis_max: Option<usize>,
    plot_left: Option<f64>,
    plot_right: Option<f64>,
) -> String {
    if data.buckets.is_empty() || data.max_count == 0 {
        return String::new();
    }
    let width = 2000.0;
    let height = if compact { 98.0 } else { 118.0 };
    let left = plot_left.unwrap_or(320.0);
    let right = plot_right.unwrap_or(1980.0);
    let plot_top = 8.0;
    let plot_bottom = if compact { 66.0 } else { 82.0 };
    let plot_height = plot_bottom - plot_top;
    let duration_ms = (data.end - data.start).num_milliseconds() as f64;
    let scale_max = data.max_count.max(y_axis_max.unwrap_or_default());
    let (visible_tasks, has_other_tasks) = task_series(data);
    let visible_task_set = visible_tasks.iter().cloned().collect::<BTreeSet<_>>();
    let mut legend_items = visible_tasks
        .iter()
        .map(|task_id| (task_id.clone(), task_id.clone(), task_color(task_id)))
        .collect::<Vec<_>>();
    if has_other_tasks {
        legend_items.push((
            "__other__".to_owned(),
            "Other".to_owned(),
            task_color("__other__"),
        ));
    }
    let legend_start_y = if compact { 80.0 } else { 96.0 };
    let mut legend_rows: Vec<Vec<(String, String, &str)>> = vec![Vec::new()];
    let mut legend_width = 0.0;
    for item in legend_items {
        let item_width = (item.1.len() as f64 * 7.0 + 34.0).max(110.0);
        if legend_width > 0.0 && legend_width + item_width > right - left {
            legend_rows.push(Vec::new());
            legend_width = 0.0;
        }
        legend_rows.last_mut().unwrap().push(item);
        legend_width += item_width;
    }
    let svg_height = (legend_start_y + legend_rows.len() as f64 * 16.0 + 4.0).max(height);
    let selected_names = data
        .selected
        .iter()
        .map(|category| category.value())
        .collect::<Vec<_>>()
        .join(",");
    let mut out = String::with_capacity(16_384);
    let _ = write!(
        out,
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="100%" viewBox="0 0 {width} {svg_height}" preserveAspectRatio="xMinYMin meet" data-histogram-start="{}" data-histogram-end="{}" data-histogram-left="{left}" data-histogram-right="{right}" data-statuses="{selected_names}" role="img" aria-label="Task occupancy histogram"><rect width="{width}" height="{svg_height}" fill="#fff"/><text x="10" y="22" font-size="12" font-weight="600" fill="#495057">Task occupancy</text><text x="10" y="39" font-size="10" fill="#6c757d">max {}</text>"##,
        data.start.to_rfc3339(),
        data.end.to_rfc3339(),
        scale_max,
    );
    let tick_step = ((scale_max + 3) / 4).max(1);
    let mut tick_values = (0..=scale_max).step_by(tick_step).collect::<Vec<_>>();
    if tick_values.last().copied() != Some(scale_max) {
        tick_values.push(scale_max);
    }
    for tick in tick_values {
        let y = plot_bottom - tick as f64 / scale_max as f64 * plot_height;
        let _ = write!(
            out,
            r##"<line x1="{left}" y1="{y:.2}" x2="{right}" y2="{y:.2}" stroke="#e9ecef" stroke-width="1"/><text x="{}" y="{:.2}" text-anchor="end" font-size="10" fill="#6c757d">{tick}</text>"##,
            left - 8.0,
            y + 3.0,
        );
    }
    for bucket in &data.buckets {
        let start_ratio = (bucket.start - data.start).num_milliseconds() as f64 / duration_ms;
        let end_ratio = (bucket.end - data.start).num_milliseconds() as f64 / duration_ms;
        let plot_width = right - left;
        let bucket_center = left + ((start_ratio + end_ratio) / 2.0) * plot_width;
        let bar_width = ((end_ratio - start_ratio) * plot_width - 0.7)
            .clamp(MIN_BAR_WIDTH_PX, MAX_BAR_WIDTH_PX);
        let x = (bucket_center - bar_width / 2.0)
            .max(left)
            .min(right - bar_width);
        let mut y = plot_bottom;
        if bucket.total_count() == 0 {
            continue;
        }
        let counts = HistogramCategory::ALL
            .into_iter()
            .filter(|category| data.selected.contains(category))
            .map(|category| format!("{}: {}", category.label(), bucket.counts[category.index()]))
            .collect::<Vec<_>>()
            .join(", ");
        let mut top_tasks = bucket.counts_by_task.iter().collect::<Vec<_>>();
        top_tasks.sort_by(|(left_task, left_count), (right_task, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_task.cmp(right_task))
        });
        let tasks = top_tasks
            .into_iter()
            .take(3)
            .map(|(task, count)| format!("{task}: {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let tooltip = format!(
            "{} to {} | {}{}",
            bucket.start.to_rfc3339(),
            bucket.end.to_rfc3339(),
            counts,
            if tasks.is_empty() {
                String::new()
            } else {
                format!(" | Tasks: {tasks}")
            }
        );
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
        let has_common_invocation_scope = common_params.iter().any(|(key, _)| key == "inv_ids");
        if !has_common_invocation_scope
            && !bucket.invocation_ids.is_empty()
            && bucket.invocation_ids.len() <= 50
        {
            serializer.append_pair("inv_ids", &bucket.invocation_ids.join(","));
        }
        let href = format!("{link_path}?{}", serializer.finish());
        let _ = write!(
            out,
            r#"<a href="{}" class="histogram-bucket-link"><g class="histogram-bucket" tabindex="0" data-bucket-start="{}" data-bucket-end="{}" data-invocation-ids="{}" data-statuses="{selected_names}" data-tooltip="{}">"#,
            xml_escape(&href),
            bucket.start.to_rfc3339(),
            bucket.end.to_rfc3339(),
            xml_escape(&bucket.invocation_ids.join(" ")),
            xml_escape(&tooltip),
        );
        for (task_id, count) in bucket_task_counts(&bucket.counts_by_task, &visible_task_set) {
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
                r#"<rect x="{x:.2}" y="{y:.2}" width="{bar_width:.2}" height="{segment_height:.2}" fill="{}" data-task="{}"/>"#,
                task_color(&task_id),
                xml_escape(label),
            );
        }
        let _ = write!(out, "<title>{}</title></g></a>", xml_escape(&tooltip));
    }
    for (row_index, row) in legend_rows.into_iter().enumerate() {
        let legend_y = legend_start_y + row_index as f64 * 16.0;
        let mut legend_x = left;
        for (_, label, color) in row {
            let _ = write!(
                out,
                r##"<rect x="{legend_x}" y="{}" width="10" height="10" rx="1" fill="{}"/><text x="{}" y="{}" font-size="10" fill="#495057">{}</text>"##,
                legend_y - 9.0,
                color,
                legend_x + 15.0,
                legend_y,
                xml_escape(&label),
            );
            legend_x += (label.len() as f64 * 7.0 + 34.0).max(110.0);
        }
    }
    out.push_str("</svg>");
    out
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
            status,
            timestamp: start() + Duration::seconds(seconds),
        }
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
            status,
            timestamp: start() + Duration::milliseconds(milliseconds),
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
    fn automatic_resolution_is_capped_and_parser_preserves_empty() {
        assert!(bucket_size_for_window(Duration::seconds(5)) <= Duration::milliseconds(250));
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
        assert!(widths.iter().all(|width| (3.0..=10.0).contains(width)));
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
        assert!(svg.contains("alpha.task</text>"));
        assert!(svg.contains("beta.task</text>"));
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
