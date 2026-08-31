//! Integration tests using testcontainers to run suite tests against real MongoDB.
//!
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-mongo -- --ignored          # only Docker tests
//! cargo test -p rustvello-mongo -- --include-ignored   # all tests
//! ```

use std::sync::Arc;

use rustvello_mongo::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;

/// Start a Mongo container and return a `MongoPool`.
async fn mongo_pool() -> (testcontainers::ContainerAsync<Mongo>, Arc<MongoPool>) {
    let container = Mongo::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://{host}:{port}/");
    let pool = Arc::new(MongoPool::new(&uri, "rustvello_test", "test"));
    (container, pool)
}

async fn make_broker() -> (testcontainers::ContainerAsync<Mongo>, MongoBroker) {
    let (c, pool) = mongo_pool().await;
    (c, MongoBroker::new(pool))
}

async fn make_orchestrator() -> (testcontainers::ContainerAsync<Mongo>, MongoOrchestrator) {
    let (c, pool) = mongo_pool().await;
    (c, MongoOrchestrator::new(pool))
}

async fn make_state_backend() -> (testcontainers::ContainerAsync<Mongo>, MongoStateBackend) {
    let (c, pool) = mongo_pool().await;
    (c, MongoStateBackend::new(pool))
}

async fn make_trigger_store() -> (testcontainers::ContainerAsync<Mongo>, MongoTriggerStore) {
    let (c, pool) = mongo_pool().await;
    (c, MongoTriggerStore::new(pool))
}

async fn make_client_data_store() -> (testcontainers::ContainerAsync<Mongo>, MongoClientDataStore) {
    let (c, pool) = mongo_pool().await;
    (c, MongoClientDataStore::new(pool))
}

async fn make_triple() -> (testcontainers::ContainerAsync<Mongo>, BackendTriple) {
    let (container, pool) = mongo_pool().await;
    let triple = BackendTriple {
        broker: Arc::new(MongoBroker::new(Arc::clone(&pool))),
        orchestrator: Arc::new(MongoOrchestrator::new(Arc::clone(&pool))),
        state_backend: Arc::new(MongoStateBackend::new(pool)),
    };
    (container, triple)
}

mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}

mod orchestrator_suite {
    use super::*;
    rustvello_test_suite::async_orchestrator_suite!(make_orchestrator());
}

mod state_backend_suite {
    use super::*;
    rustvello_test_suite::async_state_backend_suite!(make_state_backend());
}

mod trigger_suite {
    use super::*;
    rustvello_test_suite::async_trigger_suite!(make_trigger_store());
}

mod client_data_store_suite {
    use super::*;
    rustvello_test_suite::async_client_data_store_suite!(make_client_data_store());
}

mod concurrency_suite {
    use super::*;
    rustvello_test_suite::async_concurrency_suite!(make_orchestrator());
}

mod lifecycle_suite {
    use super::*;
    rustvello_test_suite::async_lifecycle_suite!(make_triple());
}

/// Create two full backend sets with different app_ids sharing the same container.
async fn make_isolation_pair() -> (
    testcontainers::ContainerAsync<Mongo>,
    MongoBroker,
    MongoBroker,
    MongoOrchestrator,
    MongoOrchestrator,
    MongoStateBackend,
    MongoStateBackend,
    MongoTriggerStore,
    MongoTriggerStore,
    MongoClientDataStore,
    MongoClientDataStore,
) {
    let container = Mongo::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://{host}:{port}/");

    let pool_a = Arc::new(MongoPool::new(&uri, "rustvello_test", "app_a"));
    let pool_b = Arc::new(MongoPool::new(&uri, "rustvello_test", "app_b"));

    (
        container,
        MongoBroker::new(Arc::clone(&pool_a)),
        MongoBroker::new(Arc::clone(&pool_b)),
        MongoOrchestrator::new(Arc::clone(&pool_a)),
        MongoOrchestrator::new(Arc::clone(&pool_b)),
        MongoStateBackend::new(Arc::clone(&pool_a)),
        MongoStateBackend::new(Arc::clone(&pool_b)),
        MongoTriggerStore::new(Arc::clone(&pool_a)),
        MongoTriggerStore::new(Arc::clone(&pool_b)),
        MongoClientDataStore::new(Arc::clone(&pool_a)),
        MongoClientDataStore::new(Arc::clone(&pool_b)),
    )
}

mod isolation_suite {
    use super::*;
    rustvello_test_suite::async_isolation_suite!(make_isolation_pair());
}
