//! Invocation dispatch and concurrency admission.

use std::sync::atomic::{AtomicUsize, Ordering};

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::task::TaskRegistry;
use rustvello_proto::config::{AppConfig, QueueSelectionStrategy};
use rustvello_proto::identifiers::{InvocationId, TaskLanguage};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use super::Orchestrator;

const MAX_BLOCKING_CANDIDATES: usize = 8;
const MAX_CC_RETRIES: usize = 8;
static NEXT_QUEUE_INDEX: AtomicUsize = AtomicUsize::new(0);

impl Orchestrator {
    /// Claim an invocation eligible for this runner's immutable language.
    ///
    /// The broker lookup, concurrency admission, rejection, and rerouting are
    /// one orchestration use case. The returned invocation is admitted but is
    /// not owned until the worker performs its atomic Pending transition.
    pub(crate) async fn claim_next(
        &self,
        config: &AppConfig,
        task_registry: &TaskRegistry,
        runner_language: TaskLanguage,
    ) -> RustvelloResult<Option<InvocationId>> {
        match self
            .backends
            .invocation_control
            .get_blocking_invocations(MAX_BLOCKING_CANDIDATES)
            .await
        {
            Ok(blocking) => {
                for invocation_id in &blocking {
                    if !self
                        .candidate_matches_language(invocation_id, runner_language)
                        .await?
                    {
                        continue;
                    }
                    if self.admit_candidate(task_registry, invocation_id).await? {
                        tracing::debug!(%invocation_id, "prioritizing invocation with waiters");
                        return Ok(Some(invocation_id.clone()));
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "blocking invocation query failed; using broker queues");
            }
        }

        for _ in 0..MAX_CC_RETRIES {
            let mut candidate = None;
            for queue_name in queue_names_for_retrieval(config) {
                if let Some(invocation_id) = self
                    .backends
                    .broker
                    .retrieve_invocation_for_language_from_queue(runner_language, &queue_name)
                    .await?
                {
                    candidate = Some(invocation_id);
                    break;
                }
            }

            let Some(invocation_id) = candidate else {
                return Ok(None);
            };
            if self.admit_candidate(task_registry, &invocation_id).await? {
                return Ok(Some(invocation_id));
            }
        }

        Ok(None)
    }

    async fn candidate_matches_language(
        &self,
        invocation_id: &InvocationId,
        runner_language: TaskLanguage,
    ) -> RustvelloResult<bool> {
        let invocation = match self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await
        {
            Ok(invocation) => invocation,
            Err(_) => return Ok(true),
        };
        Ok(invocation.task_id.language() == runner_language)
    }

    async fn admit_candidate(
        &self,
        task_registry: &TaskRegistry,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<bool> {
        let invocation = match self
            .backends
            .state_backend
            .get_invocation(invocation_id)
            .await
        {
            Ok(invocation) => invocation,
            Err(_) => return Ok(true),
        };
        let task = match task_registry.get_dyn(&invocation.task_id) {
            Some(task) => task,
            None => return Ok(true),
        };
        let config = task.config();
        if config.concurrency_control == ConcurrencyControlType::Unlimited {
            return Ok(true);
        }

        let call = match self
            .backends
            .state_backend
            .get_call(&invocation.call_id)
            .await
        {
            Ok(call) => call,
            Err(_) => return Ok(true),
        };
        let cc_arguments = crate::task_config::concurrency_arguments(
            config.concurrency_control,
            &config.key_arguments,
            &call.serialized_arguments,
        );
        if self
            .backends
            .invocation_control
            .try_acquire_concurrency_slot(
                invocation_id,
                &invocation.task_id,
                config,
                cc_arguments.as_ref(),
            )
            .await?
        {
            return Ok(true);
        }

        if config.reroute_on_cc {
            match self
                .backends
                .invocation_control
                .set_invocation_status(invocation_id, InvocationStatus::ConcurrencyControlled, None)
                .await
            {
                Ok(_) => {
                    self.backends
                        .invocation_control
                        .set_invocation_status(invocation_id, InvocationStatus::Rerouted, None)
                        .await?;
                    self.backends
                        .broker
                        .route_invocation_with_options(
                            invocation_id,
                            Some(&invocation.task_id),
                            &config.queue,
                            config.priority,
                        )
                        .await?;
                }
                Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                Err(error) => return Err(error),
            }
        } else {
            match self
                .backends
                .invocation_control
                .set_invocation_status(
                    invocation_id,
                    InvocationStatus::ConcurrencyControlledFinal,
                    None,
                )
                .await
            {
                Ok(_) | Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                Err(error) => return Err(error),
            }
        }

        Ok(false)
    }
}

pub(crate) fn queue_names_for_retrieval(config: &AppConfig) -> Vec<String> {
    use rand::seq::SliceRandom;

    let mut queues = if config.runner_queues.is_empty() {
        config.broker_queues.clone()
    } else {
        config.runner_queues.clone()
    };
    match config.queue_selection_strategy {
        QueueSelectionStrategy::Ordered => {}
        QueueSelectionStrategy::RoundRobin if !queues.is_empty() => {
            let start = NEXT_QUEUE_INDEX.fetch_add(1, Ordering::Relaxed) % queues.len();
            queues.rotate_left(start);
        }
        QueueSelectionStrategy::Random => queues.shuffle(&mut rand::thread_rng()),
        _ => {}
    }
    queues
}
