//! Shared broker test definitions.
//!
//! Each function tests a specific behavior of the [`Broker`] trait.
//! Backend crates call these with their concrete implementation.

use rustvello_core::broker::Broker;
use rustvello_proto::identifiers::InvocationId;

use crate::helpers::{generate_invocation_ids, test_foreign_task_id, test_task_id};

/// Route an invocation and retrieve it.
pub async fn test_route_and_retrieve(broker: &dyn Broker) {
    let inv = InvocationId::new();
    broker.route_invocation(&inv).await.unwrap();
    let got = broker.retrieve_invocation(None).await.unwrap();
    assert_eq!(got, Some(inv));
}

/// Retrieve from an empty broker returns None.
pub async fn test_retrieve_empty(broker: &dyn Broker) {
    let got = broker.retrieve_invocation(None).await.unwrap();
    assert_eq!(got, None);
}

/// Invocations are retrieved in FIFO order.
pub async fn test_fifo_ordering(broker: &dyn Broker) {
    let ids = generate_invocation_ids(5);
    for id in &ids {
        broker.route_invocation(id).await.unwrap();
    }
    for expected in &ids {
        let got = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(got.as_ref(), Some(expected));
    }
    assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
}

/// Per-task queue isolation: tasks don't interfere with each other.
pub async fn test_per_task_isolation(broker: &dyn Broker) {
    let task_a = test_task_id("task_a");
    let task_b = test_task_id("task_b");
    let inv_a = InvocationId::new();
    let inv_b = InvocationId::new();

    broker
        .route_invocation_for_task(&inv_a, &task_a)
        .await
        .unwrap();
    broker
        .route_invocation_for_task(&inv_b, &task_b)
        .await
        .unwrap();

    let got_a = broker.retrieve_invocation(Some(&task_a)).await.unwrap();
    assert_eq!(got_a, Some(inv_a));

    let got_b = broker.retrieve_invocation(Some(&task_b)).await.unwrap();
    assert_eq!(got_b, Some(inv_b));
}

/// Count invocations accurately.
pub async fn test_count_invocations(broker: &dyn Broker) {
    assert_eq!(broker.count_invocations(None).await.unwrap(), 0);

    let ids = generate_invocation_ids(3);
    for id in &ids {
        broker.route_invocation(id).await.unwrap();
    }
    assert_eq!(broker.count_invocations(None).await.unwrap(), 3);
}

/// Count per-task invocations.
pub async fn test_count_per_task(broker: &dyn Broker) {
    let task_a = test_task_id("task_a");
    let task_b = test_task_id("task_b");

    for _ in 0..3 {
        broker
            .route_invocation_for_task(&InvocationId::new(), &task_a)
            .await
            .unwrap();
    }
    for _ in 0..2 {
        broker
            .route_invocation_for_task(&InvocationId::new(), &task_b)
            .await
            .unwrap();
    }

    assert_eq!(broker.count_invocations(Some(&task_a)).await.unwrap(), 3);
    assert_eq!(broker.count_invocations(Some(&task_b)).await.unwrap(), 2);
}

/// Purge clears all invocations.
pub async fn test_purge_all(broker: &dyn Broker) {
    let ids = generate_invocation_ids(5);
    for id in &ids {
        broker.route_invocation(id).await.unwrap();
    }
    broker.purge(None).await.unwrap();
    assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
    assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
}

/// Purge per-task only removes that task's invocations.
pub async fn test_purge_per_task(broker: &dyn Broker) {
    let task_a = test_task_id("task_a");
    let task_b = test_task_id("task_b");

    broker
        .route_invocation_for_task(&InvocationId::new(), &task_a)
        .await
        .unwrap();
    broker
        .route_invocation_for_task(&InvocationId::new(), &task_b)
        .await
        .unwrap();

    broker.purge(Some(&task_a)).await.unwrap();
    assert_eq!(broker.count_invocations(Some(&task_a)).await.unwrap(), 0);
    assert_eq!(broker.count_invocations(Some(&task_b)).await.unwrap(), 1);
}

/// Batch route multiple invocations at once.
pub async fn test_batch_route(broker: &dyn Broker) {
    let ids = generate_invocation_ids(5);
    broker.route_invocations(&ids).await.unwrap();
    assert_eq!(broker.count_invocations(None).await.unwrap(), 5);
}

/// Route a foreign task and retrieve by language.
pub async fn test_language_routing(broker: &dyn Broker) {
    let py_task = test_foreign_task_id("python", "train");
    let local_task = test_task_id("add");
    let py_inv = InvocationId::new();
    let local_inv = InvocationId::new();

    broker
        .route_invocation_for_task(&py_inv, &py_task)
        .await
        .unwrap();
    broker
        .route_invocation_for_task(&local_inv, &local_task)
        .await
        .unwrap();

    // Python worker retrieves only python tasks
    let got = broker
        .retrieve_invocation_for_language("python")
        .await
        .unwrap();
    assert_eq!(got, Some(py_inv));

    // Local worker retrieves local tasks
    let got = broker.retrieve_invocation_for_language("").await.unwrap();
    assert_eq!(got, Some(local_inv));
}

/// Global queue items are retrievable by language workers.
pub async fn test_global_queue_language_fallback(broker: &dyn Broker) {
    let inv = InvocationId::new();
    broker.route_invocation(&inv).await.unwrap();

    // Any language worker should get global queue items
    let got = broker
        .retrieve_invocation_for_language("rust")
        .await
        .unwrap();
    assert_eq!(got, Some(inv));
}

/// Macro to generate all broker suite tests for a given setup expression.
///
/// # Example
///
/// ```rust,ignore
/// use rustvello_test_suite::broker_suite;
/// use rustvello_mem::broker::MemBroker;
///
/// broker_suite!(MemBroker::new());
/// ```
#[macro_export]
macro_rules! broker_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_broker_route_and_retrieve() {
            let broker = $setup;
            $crate::broker::test_route_and_retrieve(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_retrieve_empty() {
            let broker = $setup;
            $crate::broker::test_retrieve_empty(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_fifo_ordering() {
            let broker = $setup;
            $crate::broker::test_fifo_ordering(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_per_task_isolation() {
            let broker = $setup;
            $crate::broker::test_per_task_isolation(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_count_invocations() {
            let broker = $setup;
            $crate::broker::test_count_invocations(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_count_per_task() {
            let broker = $setup;
            $crate::broker::test_count_per_task(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_purge_all() {
            let broker = $setup;
            $crate::broker::test_purge_all(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_purge_per_task() {
            let broker = $setup;
            $crate::broker::test_purge_per_task(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_batch_route() {
            let broker = $setup;
            $crate::broker::test_batch_route(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_language_routing() {
            let broker = $setup;
            $crate::broker::test_language_routing(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_global_queue_language_fallback() {
            let broker = $setup;
            $crate::broker::test_global_queue_language_fallback(&broker).await;
        }
    };
}

/// Async-setup variant of [`broker_suite!`] for backends that require
/// asynchronous initialisation (e.g. testcontainers).
///
/// `$setup` is an *async* expression that returns `(_guard, backend)` where
/// `_guard` keeps the container alive and `backend` implements [`Broker`].
///
/// Every generated test is annotated with `#[ignore = "requires Docker"]`
/// so `cargo test` skips them by default.  Run with:
///
/// ```bash
/// cargo test -- --ignored          # only Docker tests
/// cargo test -- --include-ignored  # all tests
/// ```
///
/// # Example
///
/// ```rust,ignore
/// async fn make_redis_broker() -> (impl Drop, RedisBroker) {
///     let (c, uri) = redis_uri().await;
///     (c, RedisBroker::new(make_pool(&uri)))
/// }
///
/// mod broker_suite {
///     use super::*;
///     rustvello_test_suite::async_broker_suite!(make_redis_broker());
/// }
/// ```
#[macro_export]
macro_rules! async_broker_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_route_and_retrieve() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_route_and_retrieve(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_retrieve_empty() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_retrieve_empty(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_fifo_ordering() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_fifo_ordering(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_per_task_isolation() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_per_task_isolation(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_count_invocations() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_count_invocations(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_count_per_task() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_count_per_task(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_purge_all() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_purge_all(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_purge_per_task() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_purge_per_task(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_batch_route() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_batch_route(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_language_routing() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_language_routing(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_global_queue_language_fallback() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_global_queue_language_fallback(&broker).await;
        }
    };
}
