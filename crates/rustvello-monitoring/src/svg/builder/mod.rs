//! Timeline data builder: processes invocation histories into lane-assigned visual elements.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatus;

use super::bounds::TimelineBounds;
use super::color::ColorScheme;
use super::config::TimelineConfig;
use super::data::TimelineData;
use super::lane_assign::{self, ElementChain};

mod global_lines;
mod lanes;
#[cfg(test)]
mod tests;

/// Builds `TimelineData` from invocation history batches.
pub struct TimelineDataBuilder {
    pub(crate) config: TimelineConfig,
    /// Collected history entries grouped by invocation_id.
    pub(crate) histories: HashMap<String, Vec<InvocationHistory>>,
    /// Task ID for each invocation (for tooltips and labels).
    pub(crate) task_ids: HashMap<String, String>,
    pub(crate) color_scheme: ColorScheme,
    /// Runner contexts for enriching runner labels.
    pub(crate) runner_contexts: HashMap<String, StoredRunnerContext>,
    /// Mapping from (worker_runner_id, original_lane_index) → lane offset within group.
    /// Built during `build_lane_groups` for use by `build_global_lines`.
    pub(crate) worker_lane_offsets: HashMap<(String, usize), usize>,
    /// Explicit time bounds (if set, overrides computed bounds).
    pub(crate) explicit_start: Option<DateTime<Utc>>,
    pub(crate) explicit_end: Option<DateTime<Utc>>,
}

impl TimelineDataBuilder {
    pub fn new(config: TimelineConfig) -> Self {
        Self {
            config,
            histories: HashMap::new(),
            task_ids: HashMap::new(),
            color_scheme: ColorScheme::default(),
            runner_contexts: HashMap::new(),
            worker_lane_offsets: HashMap::new(),
            explicit_start: None,
            explicit_end: None,
        }
    }

    /// Set explicit time bounds; entries outside this range will be filtered out.
    pub fn set_time_bounds(&mut self, start: DateTime<Utc>, end: DateTime<Utc>) {
        self.explicit_start = Some(start);
        self.explicit_end = Some(end);
    }

    /// Set runner contexts for enriching runner labels with hostname/PID.
    pub fn set_runner_contexts(&mut self, contexts: HashMap<String, StoredRunnerContext>) {
        self.runner_contexts = contexts;
    }

    /// Add a batch of history entries with their associated task ID.
    pub fn add_history_batch_for_task(&mut self, entries: Vec<InvocationHistory>, task_id: &str) {
        for entry in entries {
            let inv_id = entry.invocation_id.to_string();
            self.task_ids
                .entry(inv_id.clone())
                .or_insert_with(|| task_id.to_owned());
            self.histories.entry(inv_id).or_default().push(entry);
        }
    }

    /// Add a batch of history entries for processing (without task_id).
    pub fn add_history_batch(&mut self, entries: Vec<InvocationHistory>) {
        for entry in entries {
            let inv_id = entry.invocation_id.to_string();
            self.histories.entry(inv_id).or_default().push(entry);
        }
    }

    /// Build the timeline data, consuming the builder.
    pub fn build(mut self) -> TimelineData {
        // Filter entries by explicit time bounds if set
        if let (Some(start), Some(end)) = (self.explicit_start, self.explicit_end) {
            for entries in self.histories.values_mut() {
                entries.retain(|e| {
                    let t = e.status_record.timestamp;
                    t >= start && t <= end
                });
            }
            // Remove invocations with no entries remaining
            self.histories.retain(|_, v| !v.is_empty());
        }

        if self.histories.is_empty() {
            let (s, e) = match (self.explicit_start, self.explicit_end) {
                (Some(s), Some(e)) => (s, e),
                _ => {
                    let now = Utc::now();
                    (now, now)
                }
            };
            let bounds =
                TimelineBounds::new(s, e, self.config.left_margin, self.config.drawable_width());
            return TimelineData::new(self.config, bounds, Vec::new());
        }

        // Sort each invocation's history by timestamp
        for entries in self.histories.values_mut() {
            entries.sort_by_key(|e| e.status_record.timestamp);
        }

        // Compute time bounds (explicit bounds take precedence)
        let (computed_start, computed_end) = self.compute_time_bounds();
        let global_start = self.explicit_start.unwrap_or(computed_start);
        let global_end = self.explicit_end.unwrap_or(computed_end);
        let bounds = TimelineBounds::new(
            global_start,
            global_end,
            self.config.left_margin,
            self.config.drawable_width(),
        );

        // Build element chains for lane assignment
        let mut chains = self.build_chains();
        lane_assign::assign_lanes(&mut chains);

        // Build lane groups from assigned chains
        let groups = self.build_lane_groups(&chains, &bounds);

        let mut data = TimelineData::new(self.config.clone(), bounds, groups);

        // Generate cross-lane connecting lines for invocations spanning multiple runners
        self.build_global_lines(&chains, &mut data);

        data
    }

    /// Compute the global time bounds across all history entries.
    fn compute_time_bounds(&self) -> (DateTime<Utc>, DateTime<Utc>) {
        let mut min_time = DateTime::<Utc>::MAX_UTC;
        let mut max_time = DateTime::<Utc>::MIN_UTC;

        for entries in self.histories.values() {
            for entry in entries {
                let t = entry.status_record.timestamp;
                if t < min_time {
                    min_time = t;
                }
                if t > max_time {
                    max_time = t;
                }
            }
        }

        // Ensure non-zero span
        if min_time >= max_time {
            let now = Utc::now();
            return (now - chrono::Duration::seconds(1), now);
        }

        (min_time, max_time)
    }

    /// Build element chains from history entries.
    ///
    /// Splits invocations across runners: each group of consecutive entries
    /// sharing the same effective runner produces a separate chain.
    fn build_chains(&self) -> Vec<ElementChain> {
        let mut chains = Vec::new();

        for (inv_id, entries) in &self.histories {
            if entries.is_empty() {
                continue;
            }

            // Compute effective runner for each entry:
            //  1. entry.runner_id (the runner context that caused the status change)
            //  2. entry.status_record.runner_id (the runner that owns this status)
            //  3. "unassigned" (e.g. Registered with no runner)
            //
            // We do NOT forward-fill: entries with no runner (like Registered)
            // go to "unassigned" so the runner's chain only covers actual
            // execution time, preventing false time-span overlaps.
            let effective: Vec<String> = entries
                .iter()
                .map(|e| {
                    e.runner_id
                        .as_ref()
                        .or(e.status_record.runner_id.as_ref())
                        .map_or_else(|| "unassigned".to_owned(), ToString::to_string)
                })
                .collect();

            // Group consecutive entries by effective runner
            let mut group_start = 0;
            for i in 1..=entries.len() {
                if i == entries.len() || effective[i] != effective[group_start] {
                    let group = &entries[group_start..i];
                    let Some(first) = group.first() else {
                        group_start = i;
                        continue;
                    };
                    let start = first.status_record.timestamp;
                    let end = group.last().unwrap_or(first).status_record.timestamp;
                    let deferred = group.len() == 1
                        && group[0].status_record.status == InvocationStatus::Registered;

                    let mut chain =
                        ElementChain::new(inv_id, &effective[group_start], start, end, deferred);
                    chain.entry_start = group_start;
                    chain.entry_end = i;
                    chains.push(chain);
                    group_start = i;
                }
            }
        }

        chains
    }
}
