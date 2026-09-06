//! Backend-backed data source implementation.

use std::sync::Arc;

use async_trait::async_trait;
use rustvello_core::broker::Broker;
use rustvello_core::error::{RustvelloResult, TaskError};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory};
use rustvello_proto::status::InvocationStatus;

use super::MonitorDataSource;

/// A [`MonitorDataSource`] backed by the standard rustvello traits.
pub struct BackendDataSource {
    orchestrator: Arc<dyn InvocationControlBackend>,
    state_backend: Arc<dyn StateBackend>,
    broker: Arc<dyn Broker>,
    task_ids: Vec<TaskId>,
}

impl BackendDataSource {
    pub fn new(
        orchestrator: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        broker: Arc<dyn Broker>,
        task_ids: Vec<TaskId>,
    ) -> Self {
        Self {
            orchestrator,
            state_backend,
            broker,
            task_ids,
        }
    }
}

#[async_trait]
impl MonitorDataSource for BackendDataSource {
    async fn get_invocation(&self, id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        self.state_backend.get_invocation(id).await
    }

    async fn get_invocation_status(&self, id: &InvocationId) -> RustvelloResult<InvocationStatus> {
        self.orchestrator
            .get_invocation_status(id)
            .await
            .map(|r| r.status)
    }

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.orchestrator
            .get_invocations_by_status(status, None)
            .await
    }

    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        self.orchestrator.get_invocations_by_task(task_id).await
    }

    async fn get_history(&self, id: &InvocationId) -> RustvelloResult<Vec<InvocationHistory>> {
        self.state_backend.get_history(id).await
    }

    async fn get_result(&self, id: &InvocationId) -> RustvelloResult<Option<String>> {
        self.state_backend.get_result(id).await
    }

    async fn get_error(&self, id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        self.state_backend.get_error(id).await
    }

    async fn get_child_invocations(&self, id: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        self.state_backend.get_child_invocations(id).await
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        self.state_backend.get_call(call_id).await
    }

    async fn count_broker_pending(&self) -> RustvelloResult<usize> {
        self.broker.count_invocations(None).await
    }

    fn get_registered_task_ids(&self) -> &[TaskId] {
        &self.task_ids
    }
}
