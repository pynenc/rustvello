//! Integration tests using the shared test suite.

use rustvello_sqlite::db::Database;
use std::sync::Arc;

fn make_db() -> Arc<Database> {
    Arc::new(Database::in_memory().unwrap())
}

/// SQLite broker only supports global queue — per-task and language-specific
/// routing tests are skipped because `SqliteBroker` does not implement
/// `route_invocation_for_task` or `retrieve_invocation_for_language` overrides.
/// All invocations go through a single FIFO queue, so `test_language_routing`
/// is not applicable. However, `test_global_queue_language_fallback` passes
/// because the default `retrieve_invocation_for_language` delegates to
/// `retrieve_invocation(None)`.
mod broker_suite {
    use super::*;
    use rustvello_sqlite::broker::SqliteBroker;

    #[tokio::test]
    async fn suite_broker_route_and_retrieve() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_route_and_retrieve(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_retrieve_empty() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_retrieve_empty(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_fifo_ordering() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_fifo_ordering(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_count_invocations() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_count_invocations(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_purge_all() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_purge_all(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_batch_route() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_batch_route(&broker).await;
    }

    #[tokio::test]
    async fn suite_broker_global_queue_language_fallback() {
        let broker = SqliteBroker::new(make_db());
        rustvello_test_suite::broker::test_global_queue_language_fallback(&broker).await;
    }
}

mod orchestrator_suite {
    use super::*;
    use rustvello_sqlite::orchestrator::SqliteOrchestrator;
    rustvello_test_suite::orchestrator_suite!(SqliteOrchestrator::new(make_db()));
}

mod state_backend_suite {
    use super::*;
    use rustvello_sqlite::state_backend::SqliteStateBackend;
    rustvello_test_suite::state_backend_suite!(SqliteStateBackend::new(make_db()));
}

mod trigger_suite {
    use super::*;
    use rustvello_sqlite::trigger::SqliteTriggerStore;
    rustvello_test_suite::trigger_suite!(SqliteTriggerStore::new(make_db()));
}

mod client_data_store_suite {
    use super::*;
    use rustvello_sqlite::client_data_store::SqliteClientDataStore;
    rustvello_test_suite::client_data_store_suite!(SqliteClientDataStore::new(make_db()));
}

mod lifecycle_suite {
    use super::*;
    use rustvello_sqlite::broker::SqliteBroker;
    use rustvello_sqlite::orchestrator::SqliteOrchestrator;
    use rustvello_sqlite::state_backend::SqliteStateBackend;
    use rustvello_test_suite::lifecycle::BackendTriple;

    rustvello_test_suite::lifecycle_suite!({
        let db = make_db();
        BackendTriple {
            broker: Arc::new(SqliteBroker::new(Arc::clone(&db))),
            orchestrator: Arc::new(SqliteOrchestrator::new(Arc::clone(&db))),
            state_backend: Arc::new(SqliteStateBackend::new(db)),
        }
    });
}

/// SQLite isolation test: two file-based databases with different app_ids
/// sharing the same base path.
mod isolation_suite {
    use super::*;
    use rustvello_sqlite::broker::SqliteBroker;
    use rustvello_sqlite::client_data_store::SqliteClientDataStore;
    use rustvello_sqlite::orchestrator::SqliteOrchestrator;
    use rustvello_sqlite::state_backend::SqliteStateBackend;
    use rustvello_sqlite::trigger::SqliteTriggerStore;

    fn make_isolation_pair() -> (
        (),
        SqliteBroker,
        SqliteBroker,
        SqliteOrchestrator,
        SqliteOrchestrator,
        SqliteStateBackend,
        SqliteStateBackend,
        SqliteTriggerStore,
        SqliteTriggerStore,
        SqliteClientDataStore,
        SqliteClientDataStore,
    ) {
        // Use a unique temp dir per test run to avoid lock contention
        let id = uuid::Uuid::new_v4();
        let dir = std::env::temp_dir().join(format!("rustvello_iso_{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join("test.db");

        let db_a = Arc::new(Database::open(&base, "app_a").unwrap());
        let db_b = Arc::new(Database::open(&base, "app_b").unwrap());

        (
            (),
            SqliteBroker::new(Arc::clone(&db_a)),
            SqliteBroker::new(Arc::clone(&db_b)),
            SqliteOrchestrator::new(Arc::clone(&db_a)),
            SqliteOrchestrator::new(Arc::clone(&db_b)),
            SqliteStateBackend::new(Arc::clone(&db_a)),
            SqliteStateBackend::new(Arc::clone(&db_b)),
            SqliteTriggerStore::new(Arc::clone(&db_a)),
            SqliteTriggerStore::new(Arc::clone(&db_b)),
            SqliteClientDataStore::new(Arc::clone(&db_a)),
            SqliteClientDataStore::new(Arc::clone(&db_b)),
        )
    }

    #[tokio::test]
    async fn suite_isolation_broker() {
        let (_, ba, bb, _, _, _, _, _, _, _, _) = make_isolation_pair();
        rustvello_test_suite::isolation::test_broker_isolation(&ba, &bb).await;
    }

    #[tokio::test]
    async fn suite_isolation_orchestrator() {
        let (_, _, _, oa, ob, _, _, _, _, _, _) = make_isolation_pair();
        rustvello_test_suite::isolation::test_orchestrator_isolation(&oa, &ob).await;
    }

    #[tokio::test]
    async fn suite_isolation_state_backend() {
        let (_, _, _, _, _, sa, sb, _, _, _, _) = make_isolation_pair();
        rustvello_test_suite::isolation::test_state_backend_isolation(&sa, &sb).await;
    }

    #[tokio::test]
    async fn suite_isolation_trigger_store() {
        let (_, _, _, _, _, _, _, ta, tb, _, _) = make_isolation_pair();
        rustvello_test_suite::isolation::test_trigger_store_isolation(&ta, &tb).await;
    }

    #[tokio::test]
    async fn suite_isolation_client_data_store() {
        let (_, _, _, _, _, _, _, _, _, ca, cb) = make_isolation_pair();
        rustvello_test_suite::isolation::test_client_data_store_isolation(&ca, &cb).await;
    }
}
