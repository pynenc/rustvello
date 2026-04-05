//! Integration tests using testcontainers to run suite tests against real MongoDB 3.6.
//!
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-mongo3 -- --ignored          # only Docker tests
//! cargo test -p rustvello-mongo3 -- --include-ignored   # all tests
//! ```

use std::sync::Arc;

use rustvello_mongo3::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::GenericImage;

fn mongo36_image() -> GenericImage {
    GenericImage::new("mongo", "3.6")
        .with_exposed_port(ContainerPort::Tcp(27017))
        .with_wait_for(WaitFor::message_on_stdout(
            "waiting for connections on port",
        ))
}

/// Start a Mongo 3.6 container and return a `MongoPool`.
async fn mongo_pool() -> (testcontainers::ContainerAsync<GenericImage>, Arc<MongoPool>) {
    let container = mongo36_image().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://{host}:{port}/");
    let pool = Arc::new(MongoPool::new(&uri, "rustvello_test", "test"));
    (container, pool)
}

async fn make_broker() -> (testcontainers::ContainerAsync<GenericImage>, Mongo3Broker) {
    let (c, pool) = mongo_pool().await;
    (c, Mongo3Broker::new(pool))
}

async fn make_orchestrator() -> (
    testcontainers::ContainerAsync<GenericImage>,
    Mongo3Orchestrator,
) {
    let (c, pool) = mongo_pool().await;
    (c, Mongo3Orchestrator::new(pool))
}

async fn make_state_backend() -> (
    testcontainers::ContainerAsync<GenericImage>,
    Mongo3StateBackend,
) {
    let (c, pool) = mongo_pool().await;
    (c, Mongo3StateBackend::new(pool))
}

async fn make_trigger_store() -> (
    testcontainers::ContainerAsync<GenericImage>,
    Mongo3TriggerStore,
) {
    let (c, pool) = mongo_pool().await;
    (c, Mongo3TriggerStore::new(pool))
}

async fn make_client_data_store() -> (
    testcontainers::ContainerAsync<GenericImage>,
    Mongo3ClientDataStore,
) {
    let (c, pool) = mongo_pool().await;
    (c, Mongo3ClientDataStore::new(pool))
}

async fn make_triple() -> (testcontainers::ContainerAsync<GenericImage>, BackendTriple) {
    let (container, pool) = mongo_pool().await;
    let triple = BackendTriple {
        broker: Arc::new(Mongo3Broker::new(Arc::clone(&pool))),
        orchestrator: Arc::new(Mongo3Orchestrator::new(Arc::clone(&pool))),
        state_backend: Arc::new(Mongo3StateBackend::new(pool)),
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

mod lifecycle_suite {
    use super::*;
    rustvello_test_suite::async_lifecycle_suite!(make_triple());
}

/// Create two full backend sets with different app_ids sharing the same container.
async fn make_isolation_pair() -> (
    testcontainers::ContainerAsync<GenericImage>,
    Mongo3Broker,
    Mongo3Broker,
    Mongo3Orchestrator,
    Mongo3Orchestrator,
    Mongo3StateBackend,
    Mongo3StateBackend,
    Mongo3TriggerStore,
    Mongo3TriggerStore,
    Mongo3ClientDataStore,
    Mongo3ClientDataStore,
) {
    let container = mongo36_image().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(27017).await.unwrap();
    let uri = format!("mongodb://{host}:{port}/");

    let pool_a = Arc::new(MongoPool::new(&uri, "rustvello_test", "app_a"));
    let pool_b = Arc::new(MongoPool::new(&uri, "rustvello_test", "app_b"));

    (
        container,
        Mongo3Broker::new(Arc::clone(&pool_a)),
        Mongo3Broker::new(Arc::clone(&pool_b)),
        Mongo3Orchestrator::new(Arc::clone(&pool_a)),
        Mongo3Orchestrator::new(Arc::clone(&pool_b)),
        Mongo3StateBackend::new(Arc::clone(&pool_a)),
        Mongo3StateBackend::new(Arc::clone(&pool_b)),
        Mongo3TriggerStore::new(Arc::clone(&pool_a)),
        Mongo3TriggerStore::new(Arc::clone(&pool_b)),
        Mongo3ClientDataStore::new(Arc::clone(&pool_a)),
        Mongo3ClientDataStore::new(Arc::clone(&pool_b)),
    )
}

mod isolation_suite {
    use super::*;
    rustvello_test_suite::async_isolation_suite!(make_isolation_pair());
}
