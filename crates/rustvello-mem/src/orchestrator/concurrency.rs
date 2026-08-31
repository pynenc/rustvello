use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use super::MemOrchestrator;

#[async_trait]
impl OrchestratorConcurrency for MemOrchestrator {
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
            return Ok(true);
        }

        let state = self.state.lock().await;
        let task_key = task_id.to_string();

        // Collect candidates by intersecting per-arg-pair sets
        let candidates: HashSet<&Arc<str>> = match cc_args {
            Some(args) => {
                // Arg-level CC: use per-pair index (sentinel for empty args)
                let pairs: Vec<(String, String)> = if args.0.is_empty() {
                    vec![(String::new(), String::new())]
                } else {
                    args.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                };
                let mut iter = pairs.iter();
                let Some((k, v)) = iter.next() else {
                    return Ok(true);
                };
                let key = (task_key.clone(), k.clone(), v.clone());
                let mut result: HashSet<&Arc<str>> = state
                    .cc_index
                    .get(&key)
                    .map(|s| s.iter().collect())
                    .unwrap_or_default();
                for (k, v) in iter {
                    let key = (task_key.clone(), k.clone(), v.clone());
                    let next_set = state
                        .cc_index
                        .get(&key)
                        .map(|s| s.iter().collect::<HashSet<_>>())
                        .unwrap_or_default();
                    result.retain(|id| next_set.contains(id));
                    if result.is_empty() {
                        break;
                    }
                }
                result
            }
            None => {
                // Task-level CC: all invocations for this task
                state
                    .task_invocations
                    .get(&task_key)
                    .map(|s| s.iter().collect())
                    .unwrap_or_default()
            }
        };

        // Count only non-terminal invocations (Pending or Running)
        let count = candidates
            .iter()
            .filter(|id| {
                state.status_records.get(**id).is_some_and(|r| {
                    matches!(
                        r.status,
                        InvocationStatus::Pending | InvocationStatus::Running
                    )
                })
            })
            .count();

        let limit = task_config.running_concurrency.unwrap_or(1) as usize;
        Ok(count < limit)
    }

    async fn index_for_concurrency_control(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        let task_key = task_id.to_string();

        // Index each argument pair individually (matching pynenc's per-pair design)
        // For Some(empty_args), use a sentinel pair to allow removal from cc_index
        let pairs: Vec<(String, String)> = match cc_args {
            Some(args) if !args.0.is_empty() => {
                args.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
            }
            Some(_) => vec![(String::new(), String::new())],
            None => vec![],
        };
        let mut triples = Vec::with_capacity(pairs.len());
        for (k, v) in &pairs {
            let key = (task_key.clone(), (*k).clone(), (*v).clone());
            state
                .cc_index
                .entry(key.clone())
                .or_default()
                .insert(Arc::from(invocation_id.as_str()));
            triples.push(key);
        }
        if !triples.is_empty() {
            state
                .cc_reverse
                .entry(Arc::from(invocation_id.as_str()))
                .or_default()
                .extend(triples);
        }

        Ok(())
    }

    async fn try_acquire_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
            return Ok(true);
        }

        let mut state = self.state.lock().await;
        let task_key = task_id.to_string();
        let pairs = cc_args.cloned().unwrap_or_default().cc_arg_pairs();
        let mut intersection: Option<HashSet<Arc<str>>> = None;
        for (key, value) in &pairs {
            let indexed = state
                .cc_index
                .get(&(task_key.clone(), key.clone(), value.clone()))
                .cloned()
                .unwrap_or_default();
            intersection = Some(match intersection {
                Some(previous) => previous.intersection(&indexed).cloned().collect(),
                None => indexed,
            });
        }

        let reserved = intersection.map_or(0, |members| members.len());
        let limit = task_config.running_concurrency.unwrap_or(1) as usize;
        if reserved >= limit {
            return Ok(false);
        }

        let mut triples = Vec::with_capacity(pairs.len());
        for (key, value) in pairs {
            let triple = (task_key.clone(), key, value);
            state
                .cc_index
                .entry(triple.clone())
                .or_default()
                .insert(Arc::from(invocation_id.as_str()));
            triples.push(triple);
        }
        state
            .cc_reverse
            .entry(Arc::from(invocation_id.as_str()))
            .or_default()
            .extend(triples);
        Ok(true)
    }

    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;

        if let Some(triples) = state.cc_reverse.remove(invocation_id.as_str()) {
            for key in &triples {
                if let Some(inv_set) = state.cc_index.get_mut(key) {
                    inv_set.remove(invocation_id.as_str());
                    if inv_set.is_empty() {
                        state.cc_index.remove(key);
                    }
                }
            }
        }

        Ok(())
    }
}
