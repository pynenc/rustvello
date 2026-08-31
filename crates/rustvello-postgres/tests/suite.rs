//! Integration tests using testcontainers to run suite tests against real PostgreSQL.
//!
//! These tests require Docker to be running. Run with:
//!
//! ```bash
//! cargo test -p rustvello-postgres -- --ignored          # only Docker tests
//! cargo test -p rustvello-postgres -- --include-ignored   # all tests
//! ```

use std::sync::Arc;

use rustvello_postgres::prelude::*;
use rustvello_test_suite::lifecycle::BackendTriple;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return a connected `Database`.
async fn postgres_db() -> (testcontainers::ContainerAsync<Postgres>, Arc<Database>) {
    let container = Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");
    let db = Arc::new(Database::connect(&conn, "test").await.unwrap());
    (container, db)
}

async fn make_broker() -> (testcontainers::ContainerAsync<Postgres>, PostgresBroker) {
    let (c, db) = postgres_db().await;
    (c, PostgresBroker::new(db))
}

async fn make_orchestrator() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresOrchestrator,
) {
    let (c, db) = postgres_db().await;
    (c, PostgresOrchestrator::new(db))
}

async fn make_state_backend() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresStateBackend,
) {
    let (c, db) = postgres_db().await;
    (c, PostgresStateBackend::new(db))
}

async fn make_trigger_store() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresTriggerStore,
) {
    let (c, db) = postgres_db().await;
    (c, PostgresTriggerStore::new(db))
}

async fn make_client_data_store() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresClientDataStore,
) {
    let (c, db) = postgres_db().await;
    (c, PostgresClientDataStore::new(db))
}

async fn make_triple() -> (testcontainers::ContainerAsync<Postgres>, BackendTriple) {
    let (container, db) = postgres_db().await;
    let triple = BackendTriple {
        broker: Arc::new(PostgresBroker::new(Arc::clone(&db))),
        orchestrator: Arc::new(PostgresOrchestrator::new(Arc::clone(&db))),
        state_backend: Arc::new(PostgresStateBackend::new(db)),
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

/// Two sets of backends sharing the same Postgres instance but different app_ids.
async fn make_isolation_pair() -> (
    testcontainers::ContainerAsync<Postgres>,
    PostgresBroker,
    PostgresBroker,
    PostgresOrchestrator,
    PostgresOrchestrator,
    PostgresStateBackend,
    PostgresStateBackend,
    PostgresTriggerStore,
    PostgresTriggerStore,
    PostgresClientDataStore,
    PostgresClientDataStore,
) {
    let container = Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn = format!("host={host} port={port} user=postgres password=postgres dbname=postgres");

    let db_a = Arc::new(Database::connect(&conn, "app_a").await.unwrap());
    let db_b = Arc::new(Database::connect(&conn, "app_b").await.unwrap());

    (
        container,
        PostgresBroker::new(Arc::clone(&db_a)),
        PostgresBroker::new(Arc::clone(&db_b)),
        PostgresOrchestrator::new(Arc::clone(&db_a)),
        PostgresOrchestrator::new(Arc::clone(&db_b)),
        PostgresStateBackend::new(Arc::clone(&db_a)),
        PostgresStateBackend::new(Arc::clone(&db_b)),
        PostgresTriggerStore::new(Arc::clone(&db_a)),
        PostgresTriggerStore::new(Arc::clone(&db_b)),
        PostgresClientDataStore::new(Arc::clone(&db_a)),
        PostgresClientDataStore::new(Arc::clone(&db_b)),
    )
}

mod isolation_suite {
    use super::*;
    rustvello_test_suite::async_isolation_suite!(make_isolation_pair());
}
