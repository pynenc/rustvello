//! Integration tests using testcontainers to run Redis suite tests against real Redis.
//!
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-redis -- --ignored          # only Docker tests
//! cargo test -p rustvello-redis -- --include-ignored   # all tests
//! ```

use std::sync::Arc;

use rustvello_redis::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

/// Start a Redis container and return the connection URI.
async fn redis_uri() -> (testcontainers::ContainerAsync<Redis>, String) {
    let container = Redis::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(6379).await.unwrap();
    let uri = format!("redis://127.0.0.1:{port}/");
    (container, uri)
}

fn make_pool(uri: &str) -> Arc<RedisPool> {
    Arc::new(RedisPool::new(uri, "test").unwrap())
}

async fn make_broker() -> (testcontainers::ContainerAsync<Redis>, RedisBroker) {
    let (c, uri) = redis_uri().await;
    (c, RedisBroker::new(make_pool(&uri)))
}

async fn make_orchestrator() -> (testcontainers::ContainerAsync<Redis>, RedisOrchestrator) {
    let (c, uri) = redis_uri().await;
    (c, RedisOrchestrator::new(make_pool(&uri)))
}

async fn make_state_backend() -> (testcontainers::ContainerAsync<Redis>, RedisStateBackend) {
    let (c, uri) = redis_uri().await;
    (c, RedisStateBackend::new(make_pool(&uri)))
}

async fn make_trigger_store() -> (testcontainers::ContainerAsync<Redis>, RedisTriggerStore) {
    let (c, uri) = redis_uri().await;
    (c, RedisTriggerStore::new(make_pool(&uri)))
}

async fn make_client_data_store() -> (testcontainers::ContainerAsync<Redis>, RedisClientDataStore) {
    let (c, uri) = redis_uri().await;
    (c, RedisClientDataStore::new(make_pool(&uri)))
}

async fn make_triple() -> (testcontainers::ContainerAsync<Redis>, BackendTriple) {
    let (container, uri) = redis_uri().await;
    let pool = make_pool(&uri);
    let triple = BackendTriple {
        broker: Arc::new(RedisBroker::new(Arc::clone(&pool))),
        orchestrator: Arc::new(RedisOrchestrator::new(Arc::clone(&pool))),
        state_backend: Arc::new(RedisStateBackend::new(pool)),
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

/// Two sets of backends sharing the same Redis instance but different app_ids.
async fn make_isolation_pair() -> (
    testcontainers::ContainerAsync<Redis>,
    RedisBroker,
    RedisBroker,
    RedisOrchestrator,
    RedisOrchestrator,
    RedisStateBackend,
    RedisStateBackend,
    RedisTriggerStore,
    RedisTriggerStore,
    RedisClientDataStore,
    RedisClientDataStore,
) {
    let (container, uri) = redis_uri().await;

    let pool_a = Arc::new(RedisPool::new(&uri, "app_a").unwrap());
    let pool_b = Arc::new(RedisPool::new(&uri, "app_b").unwrap());

    (
        container,
        RedisBroker::new(Arc::clone(&pool_a)),
        RedisBroker::new(Arc::clone(&pool_b)),
        RedisOrchestrator::new(Arc::clone(&pool_a)),
        RedisOrchestrator::new(Arc::clone(&pool_b)),
        RedisStateBackend::new(Arc::clone(&pool_a)),
        RedisStateBackend::new(Arc::clone(&pool_b)),
        RedisTriggerStore::new(Arc::clone(&pool_a)),
        RedisTriggerStore::new(Arc::clone(&pool_b)),
        RedisClientDataStore::new(Arc::clone(&pool_a)),
        RedisClientDataStore::new(Arc::clone(&pool_b)),
    )
}

mod isolation_suite {
    use super::*;
    rustvello_test_suite::async_isolation_suite!(make_isolation_pair());
}
