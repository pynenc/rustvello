use std::sync::Arc;

use rustvello_core::context::{
    clear_thread_invocation_context, clear_thread_runner_context, set_thread_invocation_context,
    set_thread_runner_context, RunnerContext, INVOCATION_CTX, RUNNER_CTX,
};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};
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
            orchestrator: Arc::clone(&self.orchestrator),
            state_backend: Arc::clone(&self.state_backend),
            broker: Arc::clone(&self.broker),
            emitter: Arc::clone(&self.emitter),
            middlewares: self.middlewares.clone(),
            task_registry: Arc::clone(&self.task_registry),
            trigger_manager: self.trigger_manager.clone(),
            worker_states: Some(Arc::clone(&self.worker_states)),
        };

        execute_invocation_common(
            &deps,
            invocation_id,
            worker_runner_id,
            "Worker",
            worker_ctx,
            |task, args, inv_ctx, run_ctx| async move {
                let task_config = task.config();
                if task_config.blocking {
                    let task_clone = task;
                    let thread_ctx = run_ctx.clone();
                    let thread_inv_ctx = inv_ctx.clone();
                    INVOCATION_CTX
                        .scope(
                            inv_ctx,
                            RUNNER_CTX.scope(run_ctx, async {
                                tokio::task::spawn_blocking(move || {
                                    set_thread_runner_context(thread_ctx);
                                    set_thread_invocation_context(thread_inv_ctx);
                                    let result = std::panic::catch_unwind(
                                        std::panic::AssertUnwindSafe(|| task_clone.execute(&args)),
                                    );
                                    clear_thread_invocation_context();
                                    clear_thread_runner_context();
                                    match result {
                                        Ok(r) => r,
                                        Err(panic) => {
                                            Err(crate::runner::executor_common::unwrap_panic(panic))
                                        }
                                    }
                                })
                                .await
                                .map_err(|e| {
                                    RustvelloError::Internal {
                                        message: format!("spawn_blocking join: {e}"),
                                    }
                                })?
                            }),
                        )
                        .await
                } else {
                    let thread_ctx = run_ctx.clone();
                    let thread_inv = inv_ctx.clone();
                    INVOCATION_CTX
                        .scope(
                            inv_ctx,
                            RUNNER_CTX.scope(run_ctx, async {
                                set_thread_runner_context(thread_ctx);
                                set_thread_invocation_context(thread_inv);
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        task.execute(&args)
                                    }));
                                clear_thread_invocation_context();
                                clear_thread_runner_context();
                                match result {
                                    Ok(r) => r,
                                    Err(panic) => {
                                        Err(crate::runner::executor_common::unwrap_panic(panic))
                                    }
                                }
                            }),
                        )
                        .await
                }
            },
        )
        .await
    }

    pub(super) async fn reroute_recovered_invocation(
        &self,
        inv_id: &InvocationId,
        message: &str,
    ) -> RustvelloResult<()> {
        self.orchestrator
            .set_invocation_status(inv_id, InvocationStatus::Rerouted, Some(&self.runner_id))
            .await?;
        self.broker.route_invocation(inv_id).await?;
        self.state_backend
            .add_history(
                &InvocationHistory::new(
                    inv_id.clone(),
                    InvocationStatusRecord::new(
                        InvocationStatus::Rerouted,
                        Some(self.runner_id.clone()),
                    ),
                    Some(message.to_owned()),
                )
                .with_runner(self.runner_id.clone()),
            )
            .await?;
        Ok(())
    }

    pub(super) async fn recover_stale_invocations(&self) -> RustvelloResult<u32> {
        let mut recovered = 0u32;

        let stale_pending = self
            .orchestrator
            .get_stale_pending_invocations(self.config.max_pending_seconds)
            .await?;

        for inv_id in &stale_pending {
            match self
                .orchestrator
                .set_invocation_status(
                    inv_id,
                    InvocationStatus::PendingRecovery,
                    Some(&self.runner_id),
                )
                .await
            {
                Ok(_) => {
                    if let Err(e) = self
                        .reroute_recovered_invocation(inv_id, "Recovered from stale pending state")
                        .await
                    {
                        tracing::error!(
                            "Failed to complete status:pending_recovery for invocation:{}: {}",
                            inv_id,
                            e
                        );
                        continue;
                    }
                    recovered += 1;
                    tracing::info!("Recovered stale status:pending invocation:{}", inv_id);
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {
                    tracing::debug!("invocation:{} already recovered (race)", inv_id);
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to recover status:pending invocation:{}: {}",
                        inv_id,
                        e
                    );
                }
            }
        }

        let stale_running = self
            .orchestrator
            .get_stale_running_invocations(self.config.runner_dead_after_seconds)
            .await?;

        for inv_id in &stale_running {
            match self
                .orchestrator
                .set_invocation_status(
                    inv_id,
                    InvocationStatus::RunningRecovery,
                    Some(&self.runner_id),
                )
                .await
            {
                Ok(_) => {
                    if let Err(e) = self
                        .reroute_recovered_invocation(
                            inv_id,
                            "Recovered from stale running state (dead runner)",
                        )
                        .await
                    {
                        tracing::error!(
                            "Failed to complete status:running_recovery for invocation:{}: {}",
                            inv_id,
                            e
                        );
                        continue;
                    }
                    recovered += 1;
                    tracing::info!("Recovered stale status:running invocation:{}", inv_id);
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {
                    tracing::debug!("invocation:{} already recovered (race)", inv_id);
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to recover status:running invocation:{}: {}",
                        inv_id,
                        e
                    );
                }
            }
        }

        if recovered > 0 {
            tracing::info!(
                "Recovery cycle complete: {} invocations recovered",
                recovered
            );
        }

        Ok(recovered)
    }

    pub(super) async fn should_run_atomic_service(&self) -> bool {
        let timeout = self.config.runner_dead_after_seconds;
        let runners = self
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
        let interval_secs = self.config.atomic_service_interval_minutes * 60.0;
        let margin_secs = self.config.atomic_service_spread_margin_minutes * 60.0;

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
