use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::TriggerManager;
use rustvello_proto::config::ClientDataStoreConfig;

/// Private collection of runtime ports used by orchestration use cases.
///
/// Keeping this bundle private prevents application and runner code from
/// sequencing backend calls by reaching through the orchestration service.
#[derive(Clone)]
pub(super) struct RuntimeBackends {
    pub(super) invocation_control: Arc<dyn InvocationControlBackend>,
    pub(super) state_backend: Arc<dyn StateBackend>,
    pub(super) broker: Arc<dyn Broker>,
    pub(super) client_data_store: Arc<ClientDataStoreManager>,
    pub(super) trigger_manager: Option<TriggerManager>,
}

impl RuntimeBackends {
    pub(super) fn new(
        invocation_control: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        broker: Arc<dyn Broker>,
        client_data_store: Arc<ClientDataStoreManager>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        Self {
            invocation_control,
            state_backend,
            broker,
            client_data_store,
            trigger_manager,
        }
    }

    pub(super) fn for_runner(
        invocation_control: Arc<dyn InvocationControlBackend>,
        state_backend: Arc<dyn StateBackend>,
        broker: Arc<dyn Broker>,
        trigger_manager: Option<TriggerManager>,
    ) -> Self {
        let mut config = ClientDataStoreConfig::default();
        config.disabled = true;
        let client_data_store = Arc::new(ClientDataStoreManager::new(
            Arc::new(RunnerOnlyClientDataStore),
            config,
        ));
        Self::new(
            invocation_control,
            state_backend,
            broker,
            client_data_store,
            trigger_manager,
        )
    }
}

struct RunnerOnlyClientDataStore;

#[async_trait::async_trait]
impl ClientDataStore for RunnerOnlyClientDataStore {
    async fn store(&self, _key: &str, _value: &str) -> RustvelloResult<()> {
        Ok(())
    }

    async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        Err(RustvelloError::Internal {
            message: format!("runner-only orchestrator cannot retrieve client data {key}"),
        })
    }

    async fn purge(&self) -> RustvelloResult<()> {
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "RunnerOnly"
    }
}
