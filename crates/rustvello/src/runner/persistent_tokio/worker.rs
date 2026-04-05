use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello_core::context::RunnerContext;
use rustvello_core::error::RustvelloResult;
use rustvello_core::observability::WorkerState;
use rustvello_core::runner::Runner;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::RunnerId;
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::PersistentTokioRunner;
use crate::runner::executor_common::retrieve_next_invocation_with_cc;

impl PersistentTokioRunner {
    pub(super) async fn run_impl(&self) -> RustvelloResult<()> {
        tracing::info!(
            "PersistentTokioRunner starting with {} workers (app_id={}, pid={})",
            self.num_workers,
            self.app_id,
            std::process::id()
        );
        self.emitter.on_worker_started(&self.runner_id);

        // Store main runner context for monitoring
        let runner_ctx = rustvello_core::state_backend::StoredRunnerContext::current(
            self.runner_id.to_string(),
            "PersistentTokioRunner",
        );
        if let Err(e) = self.state_backend.store_runner_context(&runner_ctx).await {
            tracing::warn!("Failed to store runner context: {}", e);
        }

        // Send initial heartbeat
        if let Err(e) = self.heartbeat().await {
            tracing::warn!("Initial heartbeat failed: {}", e);
        }

        // Create the main runner context for hierarchy
        let main_ctx = RunnerContext::new(
            self.runner_id.clone(),
            Arc::clone(&self.app_id),
            "PersistentTokioRunner",
        );

        // Bridge shutdown_tx → CancellationToken for wait_for_work
        let cancel = CancellationToken::new();
        {
            let cancel_clone = cancel.clone();
            let mut rx = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                if !*rx.borrow() {
                    let _ = rx.changed().await;
                }
                cancel_clone.cancel();
            });
        }

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

        let _ = self.shutdown_tx.send(true);

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
            if !did_work && !self.broker.wait_for_work(cancel).await {
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
        match retrieve_next_invocation_with_cc(
            &*self.orchestrator,
            &*self.broker,
            Some(&*self.state_backend),
            Some(&*self.task_registry),
        )
        .await?
        {
            Some(inv_id) => {
                self.execute_invocation(&inv_id, worker_runner_id, worker_ctx)
                    .await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub(super) async fn management_loop(&self) -> RustvelloResult<()> {
        let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_seconds);
        let atomic_check_interval =
            Duration::from_secs_f64(self.config.atomic_service_check_interval_minutes * 60.0);
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
                    if let Err(e) = self.orchestrator.register_heartbeat(wid, false).await {
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
                    if let Some(ref tm) = self.trigger_manager {
                        if let Err(e) = self.evaluate_triggers(tm).await {
                            tracing::error!("Trigger evaluation cycle failed: {}", e);
                        }
                    }
                    let svc_end = chrono::Utc::now();
                    if let Err(e) = self
                        .orchestrator
                        .record_atomic_service_execution(&self.runner_id, svc_start, svc_end)
                        .await
                    {
                        tracing::warn!("Failed to record atomic service execution: {}", e);
                    }
                }
                last_atomic_check = Instant::now();
            }

            if let Some(ref tm) = self.trigger_manager {
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
                    let _ = self.shutdown_tx.send(true);
                    break;
                }
                _ = await_sigterm!() => {
                    tracing::info!("SIGTERM received — shutting down gracefully");
                    let _ = self.shutdown_tx.send(true);
                    break;
                }
            }
        }

        Ok(())
    }

    pub(super) async fn evaluate_triggers(&self, tm: &TriggerManager) -> RustvelloResult<()> {
        if let Err(e) = tm.evaluate_cron_conditions().await {
            tracing::warn!("Cron condition evaluation failed: {}", e);
        }

        let to_invoke = tm.evaluate_triggers().await?;

        for (trigger_def, args) in &to_invoke {
            let task_id = &trigger_def.task_id;

            tracing::info!(
                "Trigger fired: task:{}, trigger_id={}, args={}",
                task_id,
                trigger_def.trigger_id,
                args
            );

            if self.task_registry.get_dyn(task_id).is_none() {
                tracing::error!("Triggered task:{} not found in registry, skipping", task_id);
                continue;
            }

            let mut ser_args = SerializedArguments::new();
            if let serde_json::Value::Object(map) = args {
                for (k, v) in map {
                    let serialized = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => serde_json::to_string(other).unwrap_or_else(|_| other.to_string()),
                    };
                    ser_args.insert(k, &serialized);
                }
            }

            let call_dto = CallDTO::new(task_id.clone(), ser_args);
            let inv_id = self.orchestrator.register_invocation(&call_dto).await?;
            let inv_dto =
                InvocationDTO::new(inv_id.clone(), task_id.clone(), call_dto.call_id.clone());

            self.state_backend
                .upsert_invocation(&inv_dto, &call_dto)
                .await?;

            let history = InvocationHistory::new(
                inv_id.clone(),
                InvocationStatusRecord::new(
                    InvocationStatus::Registered,
                    Some(self.runner_id.clone()),
                ),
                None,
            )
            .with_runner(self.runner_id.clone());
            self.state_backend.add_history(&history).await?;

            self.broker
                .route_invocation_for_task(&inv_id, task_id)
                .await?;

            tracing::info!("Triggered invocation:{} for task:{}", inv_id, task_id);
        }

        if !to_invoke.is_empty() {
            tracing::info!(
                "Trigger evaluation cycle: {} invocations created",
                to_invoke.len()
            );
        }

        Ok(())
    }
}
