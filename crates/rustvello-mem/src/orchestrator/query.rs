use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorQuery;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::MemOrchestrator;

#[async_trait]
impl OrchestratorQuery for MemOrchestrator {
    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .task_invocations
            .get(&task_id.to_string())
            .map(|ids| {
                ids.iter()
                    .map(|id| InvocationId::from_string(Arc::clone(id)))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_invocations_by_call(
        &self,
        call_id: &CallId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .call_invocations
            .get(&call_id.to_string())
            .map(|ids| {
                ids.iter()
                    .map(|id| InvocationId::from_string(Arc::clone(id)))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let ids: Vec<InvocationId> = state
            .status_records
            .iter()
            .filter(|(_, record)| record.status == status)
            .filter(|(inv_id, _)| {
                if let Some(tid) = task_id {
                    state
                        .task_invocations
                        .get(&tid.to_string())
                        .is_some_and(|s| s.contains(*inv_id))
                } else {
                    true
                }
            })
            .map(|(id, _)| InvocationId::from_string(Arc::clone(id)))
            .collect();
        Ok(ids)
    }

    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize> {
        let state = self.state.lock().await;
        let count = state
            .status_records
            .iter()
            .filter(|(inv_id, record)| {
                if let Some(ss) = statuses {
                    if !ss.contains(&record.status) {
                        return false;
                    }
                }
                if let Some(tid) = task_id {
                    if !state
                        .task_invocations
                        .get(&tid.to_string())
                        .is_some_and(|s| s.contains(*inv_id))
                    {
                        return false;
                    }
                }
                true
            })
            .count();
        Ok(count)
    }

    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let mut ids: Vec<&Arc<str>> = state
            .status_records
            .iter()
            .filter(|(inv_id, record)| {
                if let Some(ss) = statuses {
                    if !ss.contains(&record.status) {
                        return false;
                    }
                }
                if let Some(tid) = task_id {
                    if !state
                        .task_invocations
                        .get(&tid.to_string())
                        .is_some_and(|s| s.contains(*inv_id))
                    {
                        return false;
                    }
                }
                true
            })
            .map(|(id, _)| id)
            .collect();
        // Sort for deterministic pagination (HashMap iteration is non-deterministic)
        ids.sort();
        let ids: Vec<InvocationId> = ids
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|id| InvocationId::from_string(Arc::clone(id)))
            .collect();
        Ok(ids)
    }

    async fn filter_by_status(
        &self,
        invocation_ids: &[InvocationId],
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let result: Vec<InvocationId> = invocation_ids
            .iter()
            .filter(|inv_id| {
                state
                    .status_records
                    .get(inv_id.as_str())
                    .is_some_and(|r| statuses.contains(&r.status))
            })
            .cloned()
            .collect();
        Ok(result)
    }

    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        // Find invocations that have waiters, are in an available-for-run status,
        // and are NOT themselves waiting on another invocation (circular dep filter).
        let mut result = Vec::new();
        for (waited_on, waiter_ids) in &state.waiters {
            if waiter_ids.is_empty() {
                continue;
            }
            // Exclude invocations that are themselves waiting on something
            if state.waiting_for.contains_key(waited_on) {
                continue;
            }
            if let Some(record) = state.status_records.get(waited_on) {
                if record.status.is_available_for_run() {
                    result.push(InvocationId::from_string(Arc::clone(waited_on)));
                    if result.len() >= max_num {
                        break;
                    }
                }
            }
        }
        Ok(result)
    }

    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let task_key = task_id.to_string();

        // Determine candidate invocation IDs based on cc_args
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
                    return Ok(Vec::new());
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
                    .map(|ids| ids.iter().collect())
                    .unwrap_or_default()
            }
        };

        // Filter by status (empty list means no filter — return all)
        let result: Vec<InvocationId> = candidates
            .into_iter()
            .filter(|id| {
                if statuses.is_empty() {
                    return true;
                }
                state
                    .status_records
                    .get(*id)
                    .is_some_and(|r| statuses.contains(&r.status))
            })
            .map(|id| InvocationId::from_string(Arc::clone(id)))
            .collect();

        Ok(result)
    }
}
