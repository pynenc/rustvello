use std::sync::Arc;

use rustvello_core::context::RunnerContext;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use tracing::Instrument;

use super::PersistentTokioRunner;
use crate::runner::executor_common::{execute_invocation_common, ExecutionDeps};

impl PersistentTokioRunner {
    /// Execute a single invocation using the given worker's identity.
    pub(super) async fn execute_invocation(
        &self,
        invocation_id: &InvocationId,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
    ) -> RustvelloResult<()> {
        let inv_span = tracing::info_span!(
            "invocation",
            invocation_id = %invocation_id,
            task_id = tracing::field::Empty,
        );
        self.execute_invocation_inner(invocation_id, worker_runner_id, worker_ctx)
            .instrument(inv_span)
            .await
    }

    async fn execute_invocation_inner(
        &self,
        invocation_id: &InvocationId,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
    ) -> RustvelloResult<()> {
        let deps = ExecutionDeps {
            lifecycle: self.control_plane.lifecycle(),
            state_backend: Arc::clone(&self.control_plane.state_backend),
            emitter: Arc::clone(&self.emitter),
            middlewares: self.middlewares.clone(),
            task_catalog: Arc::clone(&self.control_plane.task_catalog),
            worker_states: Some(Arc::clone(&self.worker_states)),
        };

        execute_invocation_common(
            &deps,
            invocation_id,
            worker_runner_id,
            "Worker",
            worker_ctx,
            &self.executor,
        )
        .await
    }

    pub(super) async fn recover_stale_invocations(&self) -> RustvelloResult<u32> {
        self.control_plane
            .recover_stale_invocations(&self.runner_id)
            .await
    }

    pub(super) async fn should_run_atomic_service(&self) -> bool {
        let timeout = self.control_plane.config.runner_dead_after_seconds;
        let runners = self
            .control_plane
            .orchestrator
            .get_active_runners(timeout, Some(true))
            .await
            .unwrap_or_default();

        if runners.is_empty() {
            return false;
        }
        if runners.len() == 1 {
            return true;
        }

        let position = runners.iter().position(|r| r.runner_id == self.runner_id);
        let position = match position {
            Some(p) => p,
            None => return false,
        };

        let total = runners.len();
        let interval_secs = self.control_plane.config.atomic_service_interval_minutes * 60.0;
        let margin_secs = self
            .control_plane
            .config
            .atomic_service_spread_margin_minutes
            * 60.0;

        let slot_size = interval_secs / total as f64;
        let slot_start = position as f64 * slot_size;
        let mut slot_end = slot_start + slot_size - margin_secs;
        if slot_end <= slot_start {
            slot_end = slot_start + slot_size / 2.0;
        }

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let time_in_cycle = now_secs % interval_secs;

        slot_start <= time_in_cycle && time_in_cycle < slot_end
    }
}
