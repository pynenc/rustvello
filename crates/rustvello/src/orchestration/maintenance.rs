//! Recovery and runner maintenance use cases.

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

impl Orchestrator {
    /// Recover stale pending/running invocations and republish them once.
    pub(crate) async fn recover_stale_invocations(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
    ) -> RustvelloResult<u32> {
        let mut recovered = 0;
        let stale_pending = self
            .backends
            .invocation_control
            .get_stale_pending_invocations(app_config.max_pending_seconds)
            .await?;
        for invocation_id in stale_pending {
            if self
                .recover_one(
                    app_config,
                    task_catalog,
                    runner_id,
                    &invocation_id,
                    InvocationStatus::PendingRecovery,
                )
                .await?
            {
                recovered += 1;
            }
        }

        let stale_running = self
            .backends
            .invocation_control
            .get_stale_running_invocations(app_config.runner_dead_after_seconds)
            .await?;
        for invocation_id in stale_running {
            if self
                .recover_one(
                    app_config,
                    task_catalog,
                    runner_id,
                    &invocation_id,
                    InvocationStatus::RunningRecovery,
                )
                .await?
            {
                recovered += 1;
            }
        }

        Ok(recovered)
    }

    async fn recover_one(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        runner_id: &RunnerId,
        invocation_id: &InvocationId,
        recovery_status: InvocationStatus,
    ) -> RustvelloResult<bool> {
        match self
            .set_invocation_status(invocation_id, recovery_status, runner_id)
            .await
        {
            Ok(_) => {}
            Err(RustvelloError::InvalidStatusTransition { .. }) => return Ok(false),
            Err(error) => return Err(error),
        }

        let invocation = self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await?;
        let (queue, priority) = task_catalog
            .routing_for(app_config, &invocation.task_id)
            .ok_or_else(|| RustvelloError::TaskNotRegistered {
                task_id: invocation.task_id.clone(),
            })?;

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
        Ok(true)
    }
}
