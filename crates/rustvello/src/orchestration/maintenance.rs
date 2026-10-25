//! Recovery and runner maintenance use cases.
//!
//! A recovery pass re-queues invocations whose runner died (stale `Running`)
//! or that waited too long (stale `Pending`). Three rules keep a pass from
//! orphaning work:
//!
//! - the route is resolved **before** any status is written, so a task this
//!   process does not know is left untouched for a runner that knows it;
//! - one invocation's error is logged and the pass continues with the next;
//! - invocations left in `RUNNING_RECOVERY` / `PENDING_RECOVERY` (a recovery
//!   interrupted between its two writes) are picked up again once stale.
//!
//! An invocation recovered `max_invocation_recoveries` times fails with
//! `RecoveryLimitExceeded` instead of being re-queued forever.

use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

/// Error type stored on an invocation that reached the recovery limit.
pub(crate) const RECOVERY_LIMIT_ERROR: &str = "RecoveryLimitExceeded";

/// What a recovery attempt did with one invocation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// Re-queued.
    Rerouted,
    /// Failed with `RecoveryLimitExceeded`.
    Failed,
    /// Not stale any more, or changed by someone else; nothing done.
    Skipped,
}

impl Orchestrator {
    /// Recover stale pending/running invocations and republish them once.
    ///
    /// Returns the number of invocations re-queued. Errors on single
    /// invocations do not stop the pass; they are logged at `ERROR` in one
    /// summary line (the first few named) and the pass goes on.
    pub(crate) async fn recover_stale_invocations(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
    ) -> RustvelloResult<u32> {
        let mut candidates: Vec<(InvocationId, InvocationStatus)> = Vec::new();
        for invocation_id in self
            .backends
            .invocation_control
            .get_stale_pending_invocations(app_config.max_pending_seconds)
            .await?
        {
            candidates.push((invocation_id, InvocationStatus::PendingRecovery));
        }
        for invocation_id in self
            .backends
            .invocation_control
            .get_stale_running_invocations(app_config.runner_dead_after_seconds)
            .await?
        {
            candidates.push((invocation_id, InvocationStatus::RunningRecovery));
        }
        candidates.extend(self.interrupted_recoveries(app_config).await);

        let mut recovered = 0;
        let mut failed = 0;
        let mut errors: Vec<String> = Vec::new();
        for (invocation_id, recovery_status) in candidates {
            match self
                .recover_one(
                    app_config,
                    task_catalog,
                    runner_id,
                    &invocation_id,
                    recovery_status,
                )
                .await
            {
                Ok(RecoveryOutcome::Rerouted) => recovered += 1,
                Ok(RecoveryOutcome::Failed) => failed += 1,
                Ok(RecoveryOutcome::Skipped) => {}
                Err(error) => errors.push(format!("{invocation_id}: {error}")),
            }
        }
        if failed > 0 {
            tracing::error!(
                "recovery: {failed} invocation(s) failed with {RECOVERY_LIMIT_ERROR} after {} recoveries",
                app_config.max_invocation_recoveries
            );
        }
        if !errors.is_empty() {
            let shown: Vec<&str> = errors.iter().take(5).map(String::as_str).collect();
            tracing::error!(
                "recovery: {} invocation(s) left for another runner or the next pass: {}{}",
                errors.len(),
                shown.join("; "),
                if errors.len() > shown.len() {
                    "; ..."
                } else {
                    ""
                }
            );
        }
        Ok(recovered)
    }

    /// Invocations stuck in a recovery status for longer than the stale
    /// threshold: a recovery that wrote its first status and never its second.
    async fn interrupted_recoveries(
        &self,
        app_config: &AppConfig,
    ) -> Vec<(InvocationId, InvocationStatus)> {
        let mut found = Vec::new();
        for (status, stale_after) in [
            (
                InvocationStatus::PendingRecovery,
                app_config.max_pending_seconds,
            ),
            (
                InvocationStatus::RunningRecovery,
                app_config.runner_dead_after_seconds,
            ),
        ] {
            let ids = match self
                .backends
                .invocation_control
                .get_invocations_by_status(status, None)
                .await
            {
                Ok(ids) => ids,
                Err(error) => {
                    tracing::warn!("recovery: cannot list {status} invocations: {error}");
                    continue;
                }
            };
            let cutoff = chrono::Utc::now()
                - chrono::Duration::seconds(stale_after.min(i64::MAX as u64) as i64);
            for invocation_id in ids {
                if let Ok(record) = self
                    .backends
                    .invocation_control
                    .get_invocation_status(&invocation_id)
                    .await
                {
                    if record.status == status && record.timestamp < cutoff {
                        found.push((invocation_id, status));
                    }
                }
            }
        }
        found
    }

    /// Recoveries already recorded in the invocation's history.
    async fn recovery_count(&self, invocation_id: &InvocationId) -> usize {
        self.backends
            .state_backend
            .get_history(invocation_id)
            .await
            .map_or(0, |history| {
                history
                    .iter()
                    .filter(|entry| {
                        matches!(
                            entry.status_record.status,
                            InvocationStatus::PendingRecovery | InvocationStatus::RunningRecovery
                        )
                    })
                    .count()
            })
    }

    pub(crate) async fn recover_one(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
        invocation_id: &InvocationId,
        recovery_status: InvocationStatus,
    ) -> RustvelloResult<RecoveryOutcome> {
        let invocation = self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await?;
        let current = self
            .backends
            .invocation_control
            .get_invocation_status(invocation_id)
            .await?
            .status;
        let resuming = matches!(
            current,
            InvocationStatus::PendingRecovery | InvocationStatus::RunningRecovery
        );

        // The recovery limit does not need the route: a task no runner knows
        // still ends as FAILED instead of waiting forever.
        let limit = app_config.max_invocation_recoveries as usize;
        let previous = self.recovery_count(invocation_id).await;
        let attempts = if resuming { previous } else { previous + 1 };
        if limit > 0 && attempts > limit {
            return self
                .fail_recovery_limit(
                    invocation_id,
                    runner_id,
                    recovery_status,
                    resuming,
                    previous,
                )
                .await;
        }

        // Resolve the route before writing anything: a task this process does
        // not know stays as it is for a runner that knows it.
        let (queue, priority) = task_catalog
            .routing_for(app_config, &invocation.task_id)
            .ok_or_else(|| RustvelloError::TaskNotRegistered {
                task_id: invocation.task_id.clone(),
            })?;

        if let Some(publication) = self.publication()? {
            let route = rustvello_core::publication::PublicationRoute { queue, priority };
            let change = if resuming {
                rustvello_core::publication::PublicationChange::Reroute(route)
            } else {
                let stale_after_seconds = if recovery_status == InvocationStatus::PendingRecovery {
                    app_config.max_pending_seconds
                } else {
                    app_config.runner_dead_after_seconds
                };
                rustvello_core::publication::PublicationChange::Recover {
                    status: recovery_status,
                    stale_after_seconds,
                    route,
                }
            };
            let recovered = publication
                .change(invocation_id, runner_id, change, false)
                .await?
                .is_some();
            if recovered {
                let arguments = self.get_invocation_arguments(invocation_id).await;
                let statuses: &[InvocationStatus] = if resuming {
                    &[InvocationStatus::Rerouted]
                } else {
                    &[recovery_status, InvocationStatus::Rerouted]
                };
                for status in statuses {
                    self.report_published_status(
                        invocation_id,
                        runner_id,
                        *status,
                        &invocation.task_id,
                        arguments.clone(),
                    )
                    .await?;
                }
                return Ok(RecoveryOutcome::Rerouted);
            }
            return Ok(RecoveryOutcome::Skipped);
        }

        if !resuming {
            match self
                .set_invocation_status(invocation_id, recovery_status, runner_id)
                .await
            {
                Ok(_) => {}
                Err(RustvelloError::InvalidStatusTransition { .. }) => {
                    return Ok(RecoveryOutcome::Skipped)
                }
                Err(error) => return Err(error),
            }
        }

        self.set_invocation_status(invocation_id, InvocationStatus::Rerouted, runner_id)
            .await?;
        self.backends
            .broker
            .route_invocation_with_options(
                invocation_id,
                Some(&invocation.task_id),
                &queue,
                priority,
            )
            .await?;
        Ok(RecoveryOutcome::Rerouted)
    }

    /// Fail an invocation that reached the recovery limit: recovery status
    /// (which takes over ownership from the dead runner), stored error, FAILED.
    async fn fail_recovery_limit(
        &self,
        invocation_id: &InvocationId,
        runner_id: &RunnerId,
        recovery_status: InvocationStatus,
        resuming: bool,
        previous: usize,
    ) -> RustvelloResult<RecoveryOutcome> {
        if !resuming {
            match self
                .set_invocation_status(invocation_id, recovery_status, runner_id)
                .await
            {
                Ok(_) => {}
                Err(RustvelloError::InvalidStatusTransition { .. }) => {
                    return Ok(RecoveryOutcome::Skipped)
                }
                Err(error) => return Err(error),
            }
        }
        let error = TaskError {
            error_type: RECOVERY_LIMIT_ERROR.to_owned(),
            message: format!(
                "invocation {invocation_id} was recovered {previous} time(s) (max_invocation_recoveries); \
                 its worker died or could not run it each time. Check that the worker image imports \
                 the task module and that the task does not crash its process"
            ),
            traceback: None,
        };
        self.backends
            .state_backend
            .store_error(invocation_id, &error)
            .await?;
        self.set_invocation_status(invocation_id, InvocationStatus::Failed, runner_id)
            .await?;
        Ok(RecoveryOutcome::Failed)
    }
}
