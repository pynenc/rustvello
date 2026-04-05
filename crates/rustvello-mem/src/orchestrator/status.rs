use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::instrument;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::OrchestratorStatus;
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::MemOrchestrator;

#[async_trait]
impl OrchestratorStatus for MemOrchestrator {
    #[instrument(skip(self, call))]
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId> {
        let invocation_id = InvocationId::new();
        self.register_invocation_with_id(&invocation_id, call, None)
            .await?;
        Ok(invocation_id)
    }

    #[instrument(skip(self, call), fields(%invocation_id))]
    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let dto = InvocationDTO::new(
            invocation_id.clone(),
            call.task_id.clone(),
            call.call_id.clone(),
        );
        let status_record =
            InvocationStatusRecord::new(InvocationStatus::Registered, runner_id.cloned());

        let mut state = self.state.lock().await;
        state
            .invocations
            .insert(Arc::from(invocation_id.as_str()), dto);
        state
            .status_records
            .insert(Arc::from(invocation_id.as_str()), status_record.clone());
        state
            .task_invocations
            .entry(call.task_id.to_string())
            .or_default()
            .insert(Arc::from(invocation_id.as_str()));
        state
            .call_invocations
            .entry(call.call_id.to_string())
            .or_default()
            .insert(Arc::from(invocation_id.as_str()));

        Ok(status_record)
    }

    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32> {
        let mut state = self.state.lock().await;
        let count = state
            .retries
            .entry(Arc::from(invocation_id.as_str()))
            .or_insert(0);
        *count += 1;
        Ok(*count)
    }

    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32> {
        let state = self.state.lock().await;
        Ok(state
            .retries
            .get(invocation_id.as_str())
            .copied()
            .unwrap_or(0))
    }

    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        let inv_key = invocation_id.as_str();
        state.invocations.remove(inv_key);
        state.status_records.remove(inv_key);
        state.retries.remove(inv_key);
        for task_inv_set in state.task_invocations.values_mut() {
            task_inv_set.remove(inv_key);
        }
        for call_inv_set in state.call_invocations.values_mut() {
            call_inv_set.remove(inv_key);
        }
        if let Some(triples) = state.cc_reverse.remove(inv_key) {
            for key in &triples {
                if let Some(inv_set) = state.cc_index.get_mut(key) {
                    inv_set.remove(inv_key);
                    if inv_set.is_empty() {
                        state.cc_index.remove(key);
                    }
                }
            }
        }
        state.waiting_for.remove(inv_key);
        state.waiters.remove(inv_key);
        Ok(())
    }

    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let state = self.state.lock().await;
        state
            .status_records
            .get(invocation_id.as_str())
            .cloned()
            .ok_or_else(|| RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            })
    }

    #[instrument(skip(self), fields(%invocation_id, ?status))]
    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        use rustvello_proto::status::status_record_transition;

        let mut state = self.state.lock().await;

        let current = state
            .status_records
            .get(invocation_id.as_str())
            .ok_or_else(|| RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            })?;

        let new_record =
            status_record_transition(Some(current), status, runner_id).map_err(|e| {
                rustvello_core::error::status_machine_error_to_rustvello(
                    e,
                    invocation_id,
                    current.status,
                )
            })?;

        state
            .status_records
            .insert(Arc::from(invocation_id.as_str()), new_record.clone());

        // Update the DTO status as well
        if let Some(dto) = state.invocations.get_mut(invocation_id.as_str()) {
            dto.status = status;
            dto.updated_at = chrono::Utc::now();
        }

        // Prune terminal invocations from CC index to avoid O(n) growth
        if status.is_terminal() {
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
        }

        Ok(new_record)
    }

    fn backend_name(&self) -> &'static str {
        "In-Memory"
    }

    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        let state = self.state.lock().await;
        let mut by_status: HashMap<String, usize> = HashMap::new();
        let mut oldest: Option<DateTime<Utc>> = None;
        let mut newest: Option<DateTime<Utc>> = None;
        for record in state.status_records.values() {
            *by_status.entry(format!("{:?}", record.status)).or_default() += 1;
            let ts = record.timestamp;
            oldest = Some(oldest.map_or(ts, |o: DateTime<Utc>| o.min(ts)));
            newest = Some(newest.map_or(ts, |n: DateTime<Utc>| n.max(ts)));
        }
        let mut stats = vec![
            ("Total Invocations", state.invocations.len().to_string()),
            ("Active Runners", state.heartbeats.len().to_string()),
            (
                "Atomic Service Executions",
                state.atomic_timeline.len().to_string(),
            ),
            ("Concurrency Indexed", state.cc_reverse.len().to_string()),
            ("Blocking Waiters", state.waiting_for.len().to_string()),
        ];
        if let Some(dt) = oldest {
            stats.push(("Oldest Record", dt.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Some(dt) = newest {
            stats.push(("Newest Record", dt.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        let mut status_entries: Vec<_> = by_status.into_iter().collect();
        status_entries.sort_by_key(|(k, _)| k.clone());
        let status_summary: String = status_entries
            .iter()
            .filter(|(_, c)| *c > 0)
            .map(|(s, c)| format!("{s}: {c}"))
            .collect::<Vec<_>>()
            .join(", ");
        if !status_summary.is_empty() {
            stats.push(("Status Breakdown", status_summary));
        }
        stats
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state.invocations.clear();
        state.status_records.clear();
        state.task_invocations.clear();
        state.call_invocations.clear();
        state.waiting_for.clear();
        state.waiters.clear();
        state.cc_index.clear();
        state.cc_reverse.clear();
        state.retries.clear();
        // Intentionally keep heartbeats — runners are still alive
        // Intentionally keep atomic_timeline — monitoring history, capped at 200 entries
        state.auto_purge_queue.clear();
        Ok(())
    }

    async fn schedule_auto_purge(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .auto_purge_queue
            .insert(Arc::from(invocation_id.as_str()), Utc::now());
        Ok(())
    }

    async fn run_auto_purge(&self, max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>> {
        let threshold =
            Utc::now() - chrono::Duration::seconds(i64::try_from(max_age_secs).unwrap_or(i64::MAX));
        let expired: Vec<Arc<str>> = {
            let state = self.state.lock().await;
            state
                .auto_purge_queue
                .iter()
                .filter(|(_, &scheduled_at)| scheduled_at <= threshold)
                .map(|(id, _)| Arc::clone(id))
                .collect()
        };
        // Remove from queue first, then clean up each invocation
        {
            let mut state = self.state.lock().await;
            for id in &expired {
                state.auto_purge_queue.remove(id);
            }
        }
        let mut purged = Vec::new();
        for id_str in expired {
            let inv_id = InvocationId::from_string(id_str);
            // remove_invocation may fail if already removed — ignore errors
            if self.remove_invocation(&inv_id).await.is_ok() {
                purged.push(inv_id);
            }
        }
        Ok(purged)
    }
}
