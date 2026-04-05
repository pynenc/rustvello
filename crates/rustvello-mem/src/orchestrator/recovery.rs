use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{
    ActiveRunnerInfo, AtomicServiceExecution, OrchestratorRecovery,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use super::MemOrchestrator;

#[async_trait]
impl OrchestratorRecovery for MemOrchestrator {
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .heartbeats
            .insert(Arc::from(runner_id.as_str()), Utc::now());
        state
            .runner_created
            .entry(Arc::from(runner_id.as_str()))
            .or_insert_with(Utc::now);
        state
            .runner_atomic_eligible
            .insert(Arc::from(runner_id.as_str()), can_run_atomic_service);
        Ok(())
    }

    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let threshold = chrono::Utc::now()
            - chrono::Duration::seconds(i64::try_from(max_pending_seconds).unwrap_or(i64::MAX));

        let stale: Vec<InvocationId> = state
            .status_records
            .iter()
            .filter(|(_, record)| record.status == InvocationStatus::Pending)
            .filter(|(_, record)| record.timestamp < threshold)
            .map(|(id, _)| InvocationId::from_string(Arc::clone(id)))
            .collect();

        Ok(stale)
    }

    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let now = Utc::now();
        let dead_seconds = i64::try_from(runner_dead_after_seconds).unwrap_or(i64::MAX);

        let stale: Vec<InvocationId> = state
            .status_records
            .iter()
            .filter(|(_, record)| record.status == InvocationStatus::Running)
            .filter(|(_, record)| {
                // Check if the runner that owns this invocation is dead
                if let Some(ref runner_id) = record.runner_id {
                    match state.heartbeats.get(runner_id.as_str()) {
                        Some(last_hb) => (now - *last_hb).num_seconds() > dead_seconds,
                        None => true, // No heartbeat ever registered — consider dead
                    }
                } else {
                    // No runner_id on a Running invocation — shouldn't happen but treat as stale
                    true
                }
            })
            .map(|(id, _)| InvocationId::from_string(Arc::clone(id)))
            .collect();

        Ok(stale)
    }

    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>> {
        let state = self.state.lock().await;
        let now = Utc::now();
        let timeout = i64::try_from(timeout_seconds).unwrap_or(i64::MAX);
        let active: Vec<RunnerId> = state
            .heartbeats
            .iter()
            .filter(|(_, last_hb)| (now - **last_hb).num_seconds() <= timeout)
            .map(|(id, _)| RunnerId::from_string(Arc::clone(id)))
            .collect();
        Ok(active)
    }

    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>> {
        let state = self.state.lock().await;
        let now = Utc::now();
        let timeout = i64::try_from(timeout_seconds).unwrap_or(i64::MAX);
        let mut runners: Vec<ActiveRunnerInfo> = state
            .heartbeats
            .iter()
            .filter(|(_, last_hb)| (now - **last_hb).num_seconds() <= timeout)
            .filter(|(id, _)| {
                if let Some(filter) = can_run_atomic_service {
                    state
                        .runner_atomic_eligible
                        .get(*id)
                        .copied()
                        .unwrap_or(false)
                        == filter
                } else {
                    true
                }
            })
            .map(|(id, last_hb)| {
                let creation = state.runner_created.get(id).copied().unwrap_or(*last_hb);
                let eligible = state
                    .runner_atomic_eligible
                    .get(id)
                    .copied()
                    .unwrap_or(false);
                let last_service_start = state.runner_last_service_start.get(id).copied();
                let last_service_end = state.runner_last_service_end.get(id).copied();
                ActiveRunnerInfo {
                    runner_id: RunnerId::from_string(Arc::clone(id)),
                    creation_time: creation,
                    last_heartbeat: *last_hb,
                    can_run_atomic_service: eligible,
                    last_service_start,
                    last_service_end,
                }
            })
            .collect();
        // Sort by creation time (oldest first) for stable ordering
        runners.sort_by_key(|r| r.creation_time);
        Ok(runners)
    }

    async fn record_atomic_service_execution(
        &self,
        runner_id: &RunnerId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        let id: Arc<str> = Arc::from(runner_id.as_str());
        state
            .runner_last_service_start
            .insert(Arc::clone(&id), start);
        state.runner_last_service_end.insert(Arc::clone(&id), end);
        state.atomic_timeline.push(AtomicServiceExecution {
            runner_id: id.to_string(),
            start,
            end,
        });
        // Keep only the 200 most recent entries
        let len = state.atomic_timeline.len();
        if len > 200 {
            state.atomic_timeline.drain(0..len - 200);
        }
        Ok(())
    }

    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>> {
        let state = self.state.lock().await;
        let mut timeline = state.atomic_timeline.clone();
        timeline.sort_by(|a, b| b.start.cmp(&a.start)); // newest first
        Ok(timeline)
    }
}
