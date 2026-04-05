//! Backend extraction helpers for `from_backends`.
//!
//! Each function accepts a `&Bound<'_, PyAny>` and tries to downcast it to
//! every known PyO3 backend wrapper, returning the shared `Arc` as a trait
//! object (or concrete manager type). Feature-gated backends are only
//! attempted when the corresponding feature is enabled.

use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::trigger::TriggerManager;

use crate::broker::PyMemBroker;
use crate::client_data_store::PyMemClientDataStore;
use crate::orchestrator::PyMemOrchestrator;
use crate::state_backend::PyMemStateBackend;
use crate::trigger::PyMemTriggerStore;

pub fn extract_orchestrator(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Orchestrator>> {
    if let Ok(r) = obj.extract::<PyRef<'_, PyMemOrchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    #[cfg(feature = "sqlite")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::sqlite::PySqliteOrchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    #[cfg(feature = "postgres")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::postgres::PyPostgresOrchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    #[cfg(feature = "redis")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::redis::PyRedisOrchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    #[cfg(feature = "mongodb")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo::PyMongoOrchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    #[cfg(feature = "mongodb3")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo3::PyMongo3Orchestrator>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Orchestrator>);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "unsupported orchestrator backend type",
    ))
}

pub fn extract_state_backend(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn StateBackend>> {
    if let Ok(r) = obj.extract::<PyRef<'_, PyMemStateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    #[cfg(feature = "sqlite")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::sqlite::PySqliteStateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    #[cfg(feature = "postgres")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::postgres::PyPostgresStateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    #[cfg(feature = "redis")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::redis::PyRedisStateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    #[cfg(feature = "mongodb")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo::PyMongoStateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    #[cfg(feature = "mongodb3")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo3::PyMongo3StateBackend>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn StateBackend>);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "unsupported state backend type",
    ))
}

pub fn extract_broker(obj: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Broker>> {
    if let Ok(r) = obj.extract::<PyRef<'_, PyMemBroker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    #[cfg(feature = "sqlite")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::sqlite::PySqliteBroker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    #[cfg(feature = "postgres")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::postgres::PyPostgresBroker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    #[cfg(feature = "redis")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::redis::PyRedisBroker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    #[cfg(feature = "mongodb")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo::PyMongoBroker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    #[cfg(feature = "mongodb3")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo3::PyMongo3Broker>>() {
        return Ok(Arc::clone(&r.inner) as Arc<dyn Broker>);
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "unsupported broker backend type",
    ))
}

pub fn extract_trigger_manager(obj: &Bound<'_, PyAny>) -> PyResult<TriggerManager> {
    if let Ok(r) = obj.extract::<PyRef<'_, PyMemTriggerStore>>() {
        return Ok(r.manager.clone());
    }
    #[cfg(feature = "sqlite")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::sqlite::PySqliteTriggerStore>>() {
        return Ok(r.manager.clone());
    }
    #[cfg(feature = "postgres")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::postgres::PyPostgresTriggerStore>>() {
        return Ok(r.manager.clone());
    }
    #[cfg(feature = "redis")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::redis::PyRedisTriggerStore>>() {
        return Ok(r.manager.clone());
    }
    #[cfg(feature = "mongodb")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo::PyMongoTriggerStore>>() {
        return Ok(r.manager.clone());
    }
    #[cfg(feature = "mongodb3")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo3::PyMongo3TriggerStore>>() {
        return Ok(r.manager.clone());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "unsupported trigger store backend type",
    ))
}

pub fn extract_client_data_store(obj: &Bound<'_, PyAny>) -> PyResult<Arc<ClientDataStoreManager>> {
    if let Ok(r) = obj.extract::<PyRef<'_, PyMemClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    #[cfg(feature = "sqlite")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::sqlite::PySqliteClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    #[cfg(feature = "postgres")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::postgres::PyPostgresClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    #[cfg(feature = "redis")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::redis::PyRedisClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    #[cfg(feature = "mongodb")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo::PyMongoClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    #[cfg(feature = "mongodb3")]
    if let Ok(r) = obj.extract::<PyRef<'_, crate::mongo3::PyMongo3ClientDataStore>>() {
        return Ok(Arc::clone(&r.inner));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "unsupported client data store backend type",
    ))
}
