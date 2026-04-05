//! Lane group construction and element population.

use std::collections::HashMap;

use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatus;

use super::super::bounds::TimelineBounds;
use super::super::elements;
use super::super::lane::{LaneGroup, RunnerLane};
use super::super::lane_assign::ElementChain;
use super::super::models::StatusLine;
use super::super::runner_info::RunnerInfo;
use super::TimelineDataBuilder;
use crate::util::status_colors;

impl TimelineDataBuilder {
    /// Build lane groups with visual elements from assigned chains.
    ///
    /// Workers sharing the same `parent_runner_id` are merged into a single
    /// group under the parent, with each worker's lanes offset to create
    /// distinct sub-lanes within the group.
    pub(crate) fn build_lane_groups(
        &mut self,
        chains: &[ElementChain],
        bounds: &TimelineBounds,
    ) -> Vec<LaneGroup> {
        // Group chains by runner_id
        let mut runner_chains: HashMap<String, Vec<&ElementChain>> = HashMap::new();
        for chain in chains {
            runner_chains
                .entry(chain.runner_id.clone())
                .or_default()
                .push(chain);
        }

        // Determine the group key for each runner_id:
        // If the runner has a parent, group under the parent_runner_id.
        // Otherwise, group under its own runner_id.
        let mut group_key_for_runner: HashMap<String, String> = HashMap::new();
        for runner_id in runner_chains.keys() {
            if runner_id == "unassigned" {
                group_key_for_runner.insert(runner_id.clone(), runner_id.clone());
            } else if let Some(ctx) = self.runner_contexts.get(runner_id.as_str()) {
                if let Some(ref parent_id) = ctx.parent_runner_id {
                    group_key_for_runner.insert(runner_id.clone(), parent_id.clone());
                } else {
                    group_key_for_runner.insert(runner_id.clone(), runner_id.clone());
                }
            } else {
                group_key_for_runner.insert(runner_id.clone(), runner_id.clone());
            }
        }

        // Aggregate chains by group key, preserving the worker_id subdivision
        // group_key → Vec<(worker_runner_id, chains)>
        let mut grouped: HashMap<String, Vec<(String, Vec<&ElementChain>)>> = HashMap::new();
        for (runner_id, chains_for_runner) in &runner_chains {
            let group_key = group_key_for_runner
                .get(runner_id)
                .cloned()
                .unwrap_or_else(|| runner_id.clone());
            grouped
                .entry(group_key)
                .or_default()
                .push((runner_id.clone(), chains_for_runner.clone()));
        }

        let mut groups: Vec<LaneGroup> = Vec::new();

        // Sort group keys for deterministic order; "unassigned" always last
        let mut group_keys: Vec<String> = grouped.keys().cloned().collect();
        group_keys.sort_by(|a, b| {
            let a_unassigned = a == "unassigned";
            let b_unassigned = b == "unassigned";
            match (a_unassigned, b_unassigned) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => a.cmp(b),
            }
        });

        for group_key in &group_keys {
            let workers = &grouped[group_key];

            // Build RunnerInfo for the group header
            let runner_info = if group_key == "unassigned" {
                RunnerInfo::external_runner()
            } else if let Some(ctx) = self.runner_contexts.get(group_key.as_str()) {
                // The group key is either the parent runner or the runner itself
                RunnerInfo::from_context(ctx)
            } else {
                // Group key might be a parent that processed no invocations itself
                // but has children that did. Use the first child's parent info.
                let first_child_ctx = workers
                    .iter()
                    .filter_map(|(rid, _)| self.runner_contexts.get(rid.as_str()))
                    .find(|ctx| ctx.parent_runner_id.as_deref() == Some(group_key.as_str()));
                if let Some(child_ctx) = first_child_ctx {
                    RunnerInfo {
                        runner_cls: child_ctx
                            .parent_runner_cls
                            .clone()
                            .unwrap_or_else(|| "Runner".to_owned()),
                        runner_id: group_key.clone(),
                        hostname: child_ctx.hostname.clone(),
                        pid: child_ctx.pid,
                        thread_id: 0,
                        parent_runner_cls: None,
                        parent_runner_id: None,
                    }
                } else {
                    RunnerInfo::from_id(group_key)
                }
            };

            let mut group = LaneGroup::new(runner_info);

            // Sort workers by runner_id for deterministic lane ordering
            let mut sorted_workers: Vec<_> = workers.iter().collect();
            sorted_workers.sort_by(|a, b| a.0.cmp(&b.0));

            // Assign lanes: each worker's lanes are offset by previous workers' lane counts
            let mut lane_offset = 0usize;
            for (worker_runner_id, worker_chains) in &sorted_workers {
                let max_lane = worker_chains
                    .iter()
                    .map(|c| c.lane_index)
                    .max()
                    .unwrap_or(0);
                let num_lanes = max_lane + 1;

                // Record the offset for cross-lane line lookups
                for lane_idx in 0..num_lanes {
                    self.worker_lane_offsets.insert(
                        ((*worker_runner_id).clone(), lane_idx),
                        lane_idx + lane_offset,
                    );
                }

                // Extend the group's lanes for this worker, attaching worker info
                let worker_info = self
                    .runner_contexts
                    .get(worker_runner_id.as_str())
                    .map(RunnerInfo::from_context);
                for _ in 0..num_lanes {
                    let mut lane = RunnerLane::new();
                    lane.worker_info = worker_info.clone();
                    group.lanes.push(lane);
                }

                // Populate lanes with elements, applying the offset
                for chain in worker_chains.iter() {
                    let adjusted_lane = chain.lane_index + lane_offset;
                    let lane = &mut group.lanes[adjusted_lane];

                    let entries = self
                        .histories
                        .get(&chain.invocation_id)
                        .map(|all| all[chain.entry_start..chain.entry_end].to_vec());
                    if let Some(entries) = entries {
                        let task_id = self
                            .task_ids
                            .get(&chain.invocation_id)
                            .cloned()
                            .unwrap_or_default();
                        self.populate_lane_elements(
                            lane,
                            &chain.invocation_id,
                            &task_id,
                            &entries,
                            bounds,
                        );
                    }
                }

                lane_offset += num_lanes;
            }

            groups.push(group);
        }

        groups
    }

    /// Populate a lane with points, segments, and connecting lines from history entries.
    fn populate_lane_elements(
        &mut self,
        lane: &mut RunnerLane,
        inv_id: &str,
        task_id: &str,
        entries: &[InvocationHistory],
        bounds: &TimelineBounds,
    ) {
        let y_center = self.config.lane_height / 2.0; // center within the lane row
        let segment_statuses = status_colors::SEGMENT_STATUSES;
        let mut prev_x: Option<f64> = None;
        let mut prev_y = y_center;
        for (i, entry) in entries.iter().enumerate() {
            let status = entry.status_record.status;
            let timestamp = entry.status_record.timestamp;
            let runner_id = entry
                .status_record
                .runner_id
                .as_ref()
                .map(std::string::ToString::to_string);

            // Always create a point for every status
            let point = elements::create_status_point(
                inv_id,
                task_id,
                &status,
                timestamp,
                runner_id.as_deref(),
                bounds,
                &self.config,
                y_center,
            );

            if let Some(px) = prev_x {
                lane.lines.push(StatusLine {
                    invocation_id: inv_id.to_owned(),
                    x1: px,
                    y1: prev_y,
                    x2: point.x,
                    y2: point.y,
                    color: self.color_scheme.hex_for_runner(inv_id),
                });
            }

            prev_x = Some(point.x);
            prev_y = point.y;
            lane.points.push(point);

            // If this status should also be a segment (has a duration), create it
            let is_segment = segment_statuses.contains(&status);
            let next_entry = entries.get(i + 1);
            let next_timestamp = next_entry.map(|e| e.status_record.timestamp);

            if is_segment {
                if let Some(end_t) = next_timestamp {
                    // Determine outcome color: if next status is terminal, use its color
                    let next_status = next_entry.map(|e| e.status_record.status);
                    let color_status = match next_status {
                        Some(s @ (InvocationStatus::Success | InvocationStatus::Failed)) => s,
                        _ => status,
                    };

                    let seg = elements::create_status_segment(
                        inv_id,
                        task_id,
                        &status,
                        &color_status,
                        timestamp,
                        end_t,
                        runner_id.as_deref(),
                        bounds,
                        &self.config,
                        y_center,
                        false,
                    );

                    lane.segments.push(seg);
                } else {
                    // Ongoing segment: no next entry, extend to end of timeline
                    let seg = elements::create_status_segment(
                        inv_id,
                        task_id,
                        &status,
                        &status,
                        timestamp,
                        bounds.end,
                        runner_id.as_deref(),
                        bounds,
                        &self.config,
                        y_center,
                        true,
                    );

                    lane.segments.push(seg);
                }
            }
        }
    }
}
