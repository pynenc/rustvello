//! Cross-lane connecting lines for invocations spanning multiple runners.

use std::collections::HashMap;

use super::super::data::TimelineData;
use super::super::lane::LaneGroup;
use super::super::lane_assign::ElementChain;
use super::super::models::StatusLine;
use super::TimelineDataBuilder;

impl TimelineDataBuilder {
    /// Build cross-lane connecting lines for invocations spanning multiple runner groups.
    ///
    /// After layout is computed (so Y offsets are final), this finds the last
    /// element in each chain and the first element in the next chain of the same
    /// invocation, creating a dashed line between them when they are on different runners.
    pub(crate) fn build_global_lines(&mut self, chains: &[ElementChain], data: &mut TimelineData) {
        // Group chains by invocation, sorted by entry_start
        let mut inv_chains: HashMap<&str, Vec<&ElementChain>> = HashMap::new();
        for chain in chains {
            inv_chains
                .entry(chain.invocation_id.as_str())
                .or_default()
                .push(chain);
        }
        for chains in inv_chains.values_mut() {
            chains.sort_by_key(|c| c.entry_start);
        }

        // Build a lookup: group_runner_id + adjusted_lane_index → absolute y_offset
        let mut lane_y_lookup: HashMap<(&str, usize), f64> = HashMap::new();
        for group in &data.groups {
            for (li, lane) in group.lanes.iter().enumerate() {
                lane_y_lookup.insert((group.runner_info.runner_id.as_str(), li), lane.y_offset);
            }
        }

        // Determine group key for each runner_id (same logic as build_lane_groups)
        let mut group_key_for_runner: HashMap<&str, &str> = HashMap::new();
        for chain in chains {
            if chain.runner_id == "unassigned" {
                group_key_for_runner.insert(&chain.runner_id, "unassigned");
            } else if let Some(ctx) = self.runner_contexts.get(chain.runner_id.as_str()) {
                if let Some(ref parent_id) = ctx.parent_runner_id {
                    group_key_for_runner.insert(&chain.runner_id, parent_id.as_str());
                } else {
                    group_key_for_runner.insert(&chain.runner_id, &chain.runner_id);
                }
            } else {
                group_key_for_runner.insert(&chain.runner_id, &chain.runner_id);
            }
        }

        let y_center = data.config.lane_height / 2.0;

        for (inv_id, chains) in &inv_chains {
            if chains.len() < 2 {
                continue;
            }
            for pair in chains.windows(2) {
                let prev_chain = pair[0];
                let next_chain = pair[1];
                if prev_chain.runner_id == next_chain.runner_id
                    && prev_chain.lane_index == next_chain.lane_index
                {
                    continue; // Same lane — within-lane lines already handle this
                }

                // Map worker (runner_id, lane_index) → (group_key, adjusted_lane_index)
                let prev_adjusted = self
                    .worker_lane_offsets
                    .get(&(prev_chain.runner_id.clone(), prev_chain.lane_index))
                    .copied()
                    .unwrap_or(prev_chain.lane_index);
                let next_adjusted = self
                    .worker_lane_offsets
                    .get(&(next_chain.runner_id.clone(), next_chain.lane_index))
                    .copied()
                    .unwrap_or(next_chain.lane_index);

                let prev_group = group_key_for_runner
                    .get(prev_chain.runner_id.as_str())
                    .copied()
                    .unwrap_or(prev_chain.runner_id.as_str());
                let next_group = group_key_for_runner
                    .get(next_chain.runner_id.as_str())
                    .copied()
                    .unwrap_or(next_chain.runner_id.as_str());

                // Find absolute Y for each lane
                let prev_y_base = lane_y_lookup
                    .get(&(prev_group, prev_adjusted))
                    .copied()
                    .unwrap_or(0.0);
                let next_y_base = lane_y_lookup
                    .get(&(next_group, next_adjusted))
                    .copied()
                    .unwrap_or(0.0);

                // Find last element X in prev_chain and first element X in next_chain
                let prev_end_x =
                    self.find_chain_end_x(prev_chain, prev_group, prev_adjusted, &data.groups);
                let next_start_x =
                    self.find_chain_start_x(next_chain, next_group, next_adjusted, &data.groups);

                if let (Some(end_x), Some(start_x)) = (prev_end_x, next_start_x) {
                    data.global_lines.push(StatusLine {
                        invocation_id: (*inv_id).to_owned(),
                        x1: end_x,
                        y1: prev_y_base + y_center,
                        x2: start_x,
                        y2: next_y_base + y_center,
                        color: self.color_scheme.hex_for_runner(inv_id),
                    });
                }
            }
        }
    }

    /// Find the X coordinate of the last element in a chain.
    fn find_chain_end_x(
        &self,
        chain: &ElementChain,
        group_key: &str,
        adjusted_lane: usize,
        groups: &[LaneGroup],
    ) -> Option<f64> {
        for group in groups {
            if group.runner_info.runner_id != group_key {
                continue;
            }
            if let Some(lane) = group.lanes.get(adjusted_lane) {
                // Check segments first (they have x + width)
                let seg_end = lane
                    .segments
                    .iter()
                    .filter(|s| s.invocation_id == chain.invocation_id)
                    .map(|s| s.x + s.width)
                    .last();
                // Check points
                let pt_end = lane
                    .points
                    .iter()
                    .filter(|p| p.invocation_id == chain.invocation_id)
                    .map(|p| p.x)
                    .last();
                // Return the rightmost
                return match (seg_end, pt_end) {
                    (Some(s), Some(p)) => Some(s.max(p)),
                    (Some(s), None) => Some(s),
                    (None, Some(p)) => Some(p),
                    (None, None) => None,
                };
            }
        }
        None
    }

    /// Find the X coordinate of the first element in a chain.
    fn find_chain_start_x(
        &self,
        chain: &ElementChain,
        group_key: &str,
        adjusted_lane: usize,
        groups: &[LaneGroup],
    ) -> Option<f64> {
        for group in groups {
            if group.runner_info.runner_id != group_key {
                continue;
            }
            if let Some(lane) = group.lanes.get(adjusted_lane) {
                let seg_start = lane
                    .segments
                    .iter()
                    .filter(|s| s.invocation_id == chain.invocation_id)
                    .map(|s| s.x)
                    .next();
                let pt_start = lane
                    .points
                    .iter()
                    .filter(|p| p.invocation_id == chain.invocation_id)
                    .map(|p| p.x)
                    .next();
                return match (seg_start, pt_start) {
                    (Some(s), Some(p)) => Some(s.min(p)),
                    (Some(s), None) => Some(s),
                    (None, Some(p)) => Some(p),
                    (None, None) => None,
                };
            }
        }
        None
    }
}
