//! Call routing and rerouting use cases.

use std::collections::HashMap;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{CallId, InvocationId, RunnerId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use crate::task_catalog::TaskCatalog;

use super::Orchestrator;

/// Result of a `route_call` composite operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum RouteCallResult {
    /// A new invocation was created and routed.
    New(InvocationId),
    /// An existing REGISTERED invocation was reused (same call_id).
    Reused(InvocationId),
    /// An existing REGISTERED invocation was found with a different call_id.
    /// The caller decides: reuse or raise `on_diff_non_key_args_raise`.
    ReusedDifferentCall {
        invocation_id: InvocationId,
        existing_call_id: CallId,
    },
}

impl Orchestrator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn route_catalog_call(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        new_invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        registration_cc: ConcurrencyControlType,
        index_cc: bool,
        runner_id: &RunnerId,
    ) -> RustvelloResult<RouteCallResult> {
        let (queue, priority) = task_catalog
            .routing_for(app_config, &call_dto.task_id)
            .ok_or_else(|| RustvelloError::TaskNotRegistered {
                task_id: call_dto.task_id.clone(),
            })?;
        self.route_call(
            new_invocation_id,
            call_dto,
            cc_args,
            registration_cc,
            index_cc,
            runner_id,
            &queue,
            priority,
        )
        .await
    }

    pub(crate) async fn reroute_catalog_invocations(
        &self,
        app_config: &AppConfig,
        task_catalog: &TaskCatalog,
        invocation_ids: &[InvocationId],
        runner_id: &RunnerId,
    ) -> RustvelloResult<()> {
        let mut routes = HashMap::with_capacity(invocation_ids.len());
        for invocation_id in invocation_ids {
            let invocation = self
                .backends
                .state_backend
                .get_invocation(invocation_id)
                .await?;
            let route = task_catalog
                .routing_for(app_config, &invocation.task_id)
                .ok_or_else(|| RustvelloError::TaskNotRegistered {
                    task_id: invocation.task_id,
                })?;
            routes.insert(invocation_id.clone(), route);
        }
        self.reroute_invocations(invocation_ids, runner_id, &routes)
            .await
    }

    /// Route a call: check registration CC, create or reuse an invocation, route.
    ///
    /// Mirrors pynenc's `BaseOrchestrator.route_call()`:
    /// 1. If `registration_cc == Unlimited`: always create a new invocation.
    /// 2. Else: query existing REGISTERED invocations with matching CC args.
    ///    - No match -> create new.
    ///    - Match with same `call_id` -> reuse.
    ///    - Match with different `call_id` -> return `ReusedDifferentCall`.
    /// 3. For new invocations: register, persist, index CC, and publish.
    #[allow(clippy::too_many_arguments)]
    pub async fn route_call(
        &self,
        new_invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        registration_cc: ConcurrencyControlType,
        index_cc: bool,
        runner_id: &RunnerId,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<RouteCallResult> {
        if registration_cc == ConcurrencyControlType::Unlimited {
            return self
                .create_and_route_invocation(
                    new_invocation_id,
                    call_dto,
                    cc_args,
                    index_cc,
                    runner_id,
                    queue_name,
                    priority,
                )
                .await
                .map(RouteCallResult::New);
        }

        let existing = self
            .backends
            .invocation_control
            .get_existing_invocations(&call_dto.task_id, cc_args, &[InvocationStatus::Registered])
            .await?;

        if let Some(existing_inv_id) = existing.into_iter().next() {
            let existing_inv = self
                .backends
                .state_backend
                .get_invocation(&existing_inv_id)
                .await?;

            if existing_inv.call_id == call_dto.call_id {
                return Ok(RouteCallResult::Reused(existing_inv_id));
            }
            return Ok(RouteCallResult::ReusedDifferentCall {
                invocation_id: existing_inv_id,
                existing_call_id: existing_inv.call_id,
            });
        }

        self.create_and_route_invocation(
            new_invocation_id,
            call_dto,
            cc_args,
            index_cc,
            runner_id,
            queue_name,
            priority,
        )
        .await
        .map(RouteCallResult::New)
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_and_route_invocation(
        &self,
        invocation_id: &InvocationId,
        call_dto: &CallDTO,
        cc_args: Option<&SerializedArguments>,
        index_cc: bool,
        runner_id: &RunnerId,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<InvocationId> {
        let inv_dto = InvocationDTO::new(
            invocation_id.clone(),
            call_dto.task_id.clone(),
            call_dto.call_id.clone(),
        );

        self.backends
            .state_backend
            .upsert_invocation(&inv_dto, call_dto)
            .await?;

        let record = self
            .backends
            .invocation_control
            .register_invocation_with_id(invocation_id, call_dto, Some(runner_id))
            .await?;

        let history = InvocationHistory::new(invocation_id.clone(), record.clone(), None)
            .with_runner(runner_id.clone());
        self.backends.state_backend.add_history(&history).await?;

        if let Some(ref tm) = self.backends.trigger_manager {
            let ctx = rustvello_proto::trigger::StatusContext {
                invocation_id: invocation_id.clone(),
                task_id: call_dto.task_id.clone(),
                status: record.status,
                arguments: call_dto.serialized_arguments.0.clone(),
            };
            tm.report_status_change(&ctx).await?;
        }

        if index_cc {
            self.backends
                .invocation_control
                .index_for_concurrency_control(invocation_id, &call_dto.task_id, cc_args)
                .await?;
        }

        self.backends
            .broker
            .route_invocation_with_options(
                invocation_id,
                Some(&call_dto.task_id),
                queue_name,
                priority,
            )
            .await?;

        Ok(invocation_id.clone())
    }

    /// Reroute a set of invocations: transition to Rerouted, then re-enqueue.
    ///
    /// Invalid status transitions are skipped because they normally mean a
    /// racing runner already moved the invocation.
    pub async fn reroute_invocations(
        &self,
        invocation_ids: &[InvocationId],
        runner_id: &RunnerId,
        routes: &HashMap<InvocationId, (String, f64)>,
    ) -> RustvelloResult<()> {
        for inv_id in invocation_ids {
            match self
                .backends
                .invocation_control
                .set_invocation_status(inv_id, InvocationStatus::Rerouted, Some(runner_id))
                .await
            {
                Ok(record) => {
                    let history = InvocationHistory::new(inv_id.clone(), record.clone(), None)
                        .with_runner(runner_id.clone());
                    let _ = self.backends.state_backend.add_history(&history).await;

                    if let Some(ref tm) = self.backends.trigger_manager {
                        let (task_id, arguments) = self.get_trigger_context(inv_id).await;
                        let ctx = rustvello_proto::trigger::StatusContext {
                            invocation_id: inv_id.clone(),
                            task_id,
                            status: InvocationStatus::Rerouted,
                            arguments,
                        };
                        let _ = tm.report_status_change(&ctx).await;
                    }

                    let invocation = self.backends.state_backend.get_invocation(inv_id).await?;
                    let (queue_name, priority) =
                        routes.get(inv_id).ok_or_else(|| RustvelloError::Internal {
                            message: format!("missing routing for invocation {inv_id}"),
                        })?;
                    self.backends
                        .broker
                        .route_invocation_with_options(
                            inv_id,
                            Some(&invocation.task_id),
                            queue_name,
                            *priority,
                        )
                        .await?
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
