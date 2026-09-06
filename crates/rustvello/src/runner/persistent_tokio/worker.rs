use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello_core::context::RunnerContext;
use rustvello_core::error::RustvelloResult;
use rustvello_core::observability::WorkerState;
use rustvello_core::runner::Runner;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::identifiers::RunnerId;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::PersistentTokioRunner;

impl PersistentTokioRunner {
    pub(super) async fn run_impl(&self) -> RustvelloResult<()> {
        tracing::info!(
            "PersistentTokioRunner starting with {} workers (app_id={}, pid={})",
            self.num_workers,
            self.control_plane.app_id,
            std::process::id()
        );
        self.emitter.on_worker_started(&self.runner_id);

        // Store main runner context for monitoring
        let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current_with_runtime(
            self.runner_id.to_string(),
            "PersistentTokioRunner",
            self.control_plane.runner_language,
            self.control_plane.executor_kind,
        );
        if let Err(e) = self
            .control_plane
            .state_backend
            .store_runner_context(&runner_ctx)
            .await
        {
            tracing::warn!("Failed to store runner context: {}", e);
        }

        // Send initial heartbeat
        if let Err(e) = self.heartbeat().await {
            tracing::warn!("Initial heartbeat failed: {}", e);
        }

        // Create the main runner context for hierarchy
        let main_ctx = RunnerContext::new_with_runtime(
            self.runner_id.clone(),
            Arc::clone(&self.control_plane.app_id),
            "PersistentTokioRunner",
            self.control_plane.runner_language,
            self.control_plane.executor_kind,
        );

        let cancel = self.control_plane.cancellation_token();

        // Spawn N workers, each with a unique UUID and child RunnerContext
        let mut worker_handles = tokio::task::JoinSet::new();
        for worker_idx in 0..self.num_workers {
            let worker_runner_id = RunnerId::new();
            let worker_ctx = main_ctx.new_child(worker_runner_id.clone());

            // Register per-worker state
            {
                let mut states = self
                    .worker_states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                states.insert(
                    worker_runner_id.clone(),
                    WorkerState::new(worker_runner_id.clone()),
                );
            }

            // Store worker context in state backend for monitoring
            let worker_sb_ctx =
                runner_ctx.new_child(worker_runner_id.to_string(), "PersistentTokioWorker");
            if let Err(e) = self
                .control_plane
                .state_backend
                .store_runner_context(&worker_sb_ctx)
                .await
            {
                tracing::warn!(
                    "Failed to store worker context for worker:{}: {}",
                    worker_runner_id,
                    e
                );
            }

            self.emitter.on_worker_started(&worker_runner_id);

            let worker = self.clone();
            let w_id = worker_runner_id.clone();
            let w_ctx = worker_ctx.clone();
            let w_cancel = cancel.clone();
            let worker_span = tracing::info_span!(
                "worker",
                worker_id = %worker_runner_id,
                worker_idx = worker_idx,
            );
            worker_handles.spawn(
                async move {
                    worker
                        .worker_loop(worker_idx, &w_id, &w_ctx, &w_cancel)
                        .await
                }
                .instrument(worker_span),
            );
        }

        // Management loop: heartbeats, recovery, triggers
        let mgmt_result = self.management_loop().await;

        self.control_plane.request_shutdown();

        while let Some(result) = worker_handles.join_next().await {
            match result {
                Ok(Err(e)) => tracing::error!("Worker error: {}", e),
                Err(e) => tracing::error!("Worker task panicked: {}", e),
                Ok(Ok(())) => {}
            }
        }

        tracing::info!("PersistentTokioRunner shutting down");
        self.emitter.on_worker_shutdown(&self.runner_id);
        mgmt_result
    }

    /// Worker loop: polls the broker and executes invocations.
    async fn worker_loop(
        &self,
        worker_idx: usize,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
        cancel: &CancellationToken,
    ) -> RustvelloResult<()> {
        tracing::debug!("Worker {} ({}) started", worker_idx, worker_runner_id);
        while !self.is_shutdown() {
            let did_work = self.run_one_as_worker(worker_runner_id, worker_ctx).await?;
            if !did_work && !self.control_plane.broker.wait_for_work(cancel).await {
                break;
            }
        }
        tracing::debug!("Worker {} ({}) stopped", worker_idx, worker_runner_id);
        self.emitter.on_worker_shutdown(worker_runner_id);
        // Remove worker state on shutdown
        if let Ok(mut states) = self.worker_states.lock() {
            states.remove(worker_runner_id);
        }
        Ok(())
    }

    /// Run one invocation as a specific worker.
    async fn run_one_as_worker(
        &self,
        worker_runner_id: &RunnerId,
        worker_ctx: &RunnerContext,
    ) -> RustvelloResult<bool> {
        match self.control_plane.claim_next().await? {
            Some(inv_id) => {
                self.execute_invocation(&inv_id, worker_runner_id, worker_ctx)
                    .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub(super) async fn management_loop(&self) -> RustvelloResult<()> {
        let heartbeat_interval =
            Duration::from_secs(self.control_plane.config.heartbeat_interval_seconds);
        let atomic_check_interval = Duration::from_secs_f64(
            self.control_plane
                .config
                .atomic_service_check_interval_minutes
                * 60.0,
        );
        let trigger_interval = Duration::from_secs(5);
        let mut last_heartbeat = Instant::now();
        let mut last_atomic_check = Instant::now();
        let mut last_trigger_eval = Instant::now();

        // OS signal handling
        let mut sigint = std::pin::pin!(tokio::signal::ctrl_c());
        #[cfg(unix)]
        let mut sigterm = {
            use tokio::signal::unix::{signal, SignalKind};
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler")
        };
        #[cfg(unix)]
        macro_rules! await_sigterm {
            () => {
                sigterm.recv()
            };
        }
        #[cfg(not(unix))]
        macro_rules! await_sigterm {
            () => {
                std::future::pending::<Option<()>>()
            };
        }

        while !self.is_shutdown() {
            if last_heartbeat.elapsed() >= heartbeat_interval {
                if let Err(e) = self.heartbeat().await {
                    tracing::warn!("Heartbeat failed: {}", e);
                }
                let worker_ids = self.active_worker_ids();
                for wid in &worker_ids {
                    if let Err(e) = self.control_plane.heartbeat(wid, false).await {
                        tracing::warn!("worker:{} heartbeat failed: {}", wid, e);
                    }
                }
                last_heartbeat = Instant::now();
            }

            if last_atomic_check.elapsed() >= atomic_check_interval {
                if self.should_run_atomic_service().await {
                    tracing::debug!(
                        "Atomic service: this runner's time slot — running recovery & triggers"
                    );
                    let svc_start = chrono::Utc::now();
                    if let Err(e) = self.recover_stale_invocations().await {
                        tracing::error!("Recovery cycle failed: {}", e);
                    }
                    if let Some(ref tm) = self.control_plane.trigger_manager {
                        if let Err(e) = self.evaluate_triggers(tm).await {
                            tracing::error!("Trigger evaluation cycle failed: {}", e);
                        }
                    }
                    let svc_end = chrono::Utc::now();
                    if let Err(e) = self
                        .control_plane
                        .orchestrator
                        .record_atomic_service_execution(&self.runner_id, svc_start, svc_end)
                        .await
                    {
                        tracing::warn!("Failed to record atomic service execution: {}", e);
                    }
                }
                last_atomic_check = Instant::now();
            }

            if let Some(ref tm) = self.control_plane.trigger_manager {
                if last_trigger_eval.elapsed() >= trigger_interval {
                    if self.should_run_atomic_service().await {
                        if let Err(e) = self.evaluate_triggers(tm).await {
                            tracing::error!("Trigger evaluation cycle failed: {}", e);
                        }
                    }
                    last_trigger_eval = Instant::now();
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = self.wait_for_shutdown() => break,
                Ok(()) = &mut sigint => {
                    tracing::info!("SIGINT received — shutting down gracefully");
                    self.control_plane.request_shutdown();
                    break;
                }
                _ = await_sigterm!() => {
                    tracing::info!("SIGTERM received — shutting down gracefully");
                    self.control_plane.request_shutdown();
                    break;
                }
            }
        }

        Ok(())
    }

    pub(super) async fn evaluate_triggers(&self, _tm: &TriggerManager) -> RustvelloResult<()> {
        self.control_plane
            .evaluate_triggers(&self.runner_id)
            .await?;
        Ok(())
    }
}
