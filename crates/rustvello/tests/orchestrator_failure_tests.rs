//! Partial-failure tests for cross-backend orchestration use cases.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rustvello::orchestration::RouteCallResult;
use rustvello::prelude::*;
use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId, TaskLanguage};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};
use tokio_util::sync::CancellationToken;

struct FailFirstPublishBroker {
    inner: Arc<dyn Broker>,
    fail_next_publish: AtomicBool,
}

impl FailFirstPublishBroker {
    fn new(inner: Arc<dyn Broker>) -> Self {
        Self {
            inner,
            fail_next_publish: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl Broker for FailFirstPublishBroker {
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        if self.fail_next_publish.swap(false, Ordering::SeqCst) {
            return Err(RustvelloError::connection_err("injected publish failure"));
        }
        self.inner
            .route_invocation_with_options(invocation_id, task_id, queue_name, priority)
            .await
    }

    async fn retrieve_invocation_from_queue(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.inner
            .retrieve_invocation_from_queue(queue_name, task_id)
            .await
    }

    async fn retrieve_invocation_for_language_from_queue(
        &self,
        language: TaskLanguage,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.inner
            .retrieve_invocation_for_language_from_queue(language, queue_name)
            .await
    }

    async fn count_invocations_in_queues(
        &self,
        queue_names: &[String],
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize> {
        self.inner
            .count_invocations_in_queues(queue_names, task_id)
            .await
    }

    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.inner.route_invocation(invocation_id).await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        self.inner
            .route_invocation_for_task(invocation_id, task_id)
            .await
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.inner.retrieve_invocation(task_id).await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: TaskLanguage,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.inner.retrieve_invocation_for_language(language).await
    }

    async fn wait_for_work(&self, cancel: &CancellationToken) -> bool {
        self.inner.wait_for_work(cancel).await
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.inner.count_invocations(task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        self.inner.purge(task_id).await
    }
}

#[tokio::test]
async fn route_call_retries_the_same_id_after_publish_failure() {
    let inner_broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let broker: Arc<dyn Broker> = Arc::new(FailFirstPublishBroker::new(inner_broker));
    let invocation_control: Arc<dyn InvocationControlBackend> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let client_data_store = Arc::new(ClientDataStoreManager::new(
        Arc::new(rustvello_mem::client_data_store::MemClientDataStore::new()),
        ClientDataStoreConfig::default(),
    ));

    let mut app = RustvelloApp::with_backends(
        AppConfig::new("orchestrator-failure"),
        Arc::clone(&broker),
        Arc::clone(&invocation_control),
        Arc::clone(&state_backend),
        client_data_store,
    );
    let task_id = TaskId::for_language(TaskLanguage::Python, "failure", "target");
    app.register_foreign_task(task_id.clone(), TaskConfig::default())
        .unwrap();

    let invocation_id = InvocationId::new();
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let runner_id = RunnerId::from_string("failure-test-caller");

    let error = app
        .route_call(
            &invocation_id,
            &call,
            None,
            ConcurrencyControlType::Unlimited,
            false,
            &runner_id,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected publish failure"));
    assert_eq!(
        invocation_control
            .get_invocation_status(&invocation_id)
            .await
            .unwrap()
            .status,
        InvocationStatus::Registered
    );
    assert_eq!(broker.count_invocations(Some(&task_id)).await.unwrap(), 0);

    let retried = app
        .route_call(
            &invocation_id,
            &call,
            None,
            ConcurrencyControlType::Unlimited,
            false,
            &runner_id,
        )
        .await
        .unwrap();
    assert!(matches!(retried, RouteCallResult::New(ref id) if id == &invocation_id));

    assert_eq!(
        broker
            .retrieve_invocation_for_language(TaskLanguage::Rust)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        broker
            .retrieve_invocation_for_language(TaskLanguage::Python)
            .await
            .unwrap(),
        Some(invocation_id)
    );
    assert_eq!(
        broker
            .retrieve_invocation_for_language(TaskLanguage::Python)
            .await
            .unwrap(),
        None
    );
}
