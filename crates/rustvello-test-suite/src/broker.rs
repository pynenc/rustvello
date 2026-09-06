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

/// Logical queues are isolated and priorities apply only within one queue.
pub async fn test_named_queues_and_priorities(broker: &dyn Broker) {
    let task = test_task_id("priority_task");
    let low = InvocationId::new();
    let high_first = InvocationId::new();
    let high_second = InvocationId::new();
    let report = InvocationId::new();

    broker
        .route_invocation_with_options(&low, Some(&task), "payments", -99.5)
        .await
        .unwrap();
    broker
        .route_invocation_with_options(&high_first, Some(&task), "payments", 99.25)
        .await
        .unwrap();
    broker
        .route_invocation_with_options(&high_second, Some(&task), "payments", 99.25)
        .await
        .unwrap();
    broker
        .route_invocation_with_options(&report, Some(&task), "reports", 100.0)
        .await
        .unwrap();

    assert_eq!(
        broker
            .count_invocations_in_queues(&["payments".to_owned()], None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("payments", None)
            .await
            .unwrap(),
        Some(high_first)
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("payments", None)
            .await
            .unwrap(),
        Some(high_second)
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("payments", None)
            .await
            .unwrap(),
        Some(low)
    );
    assert_eq!(
        broker
            .retrieve_invocation_from_queue("reports", None)
            .await
            .unwrap(),
        Some(report)
    );
}

/// Invalid routing values fail before anything reaches backend storage.
pub async fn test_queue_priority_validation(broker: &dyn Broker) {
    let invocation_id = InvocationId::new();
    for priority in [-100.1, 100.1, f64::INFINITY, f64::NAN] {
        assert!(broker
            .route_invocation_with_options(&invocation_id, None, "default", priority)
            .await
            .is_err());
    }
    assert!(broker
        .route_invocation_with_options(&invocation_id, None, "invalid queue", 0.0)
        .await
        .is_err());
    assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
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
    let task = test_task_id("count_task");
    assert_eq!(broker.count_invocations(None).await.unwrap(), 0);

    let ids = generate_invocation_ids(3);
    for id in &ids {
        broker.route_invocation(id).await.unwrap();
    }
    broker
        .route_invocation_for_task(&InvocationId::new(), &task)
        .await
        .unwrap();
    assert_eq!(broker.count_invocations(None).await.unwrap(), 4);
    assert_eq!(broker.count_invocations(Some(&task)).await.unwrap(), 1);
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
    broker
        .route_invocation_for_task(&InvocationId::new(), &test_task_id("purge_task"))
        .await
        .unwrap();
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

/// Route tasks for different runtimes and retrieve each by language.
pub async fn test_language_routing(broker: &dyn Broker) {
    let py_task = test_foreign_task_id(rustvello_proto::identifiers::TaskLanguage::Python, "train");
    let rust_task = test_task_id("add");
    let py_inv = InvocationId::new();
    let rust_inv = InvocationId::new();

    broker
        .route_invocation_for_task(&py_inv, &py_task)
        .await
        .unwrap();
    broker
        .route_invocation_for_task(&rust_inv, &rust_task)
        .await
        .unwrap();

    // Python worker retrieves only python tasks
    let got = broker
        .retrieve_invocation_for_language(rustvello_proto::identifiers::TaskLanguage::Python)
        .await
        .unwrap();
    assert_eq!(got, Some(py_inv));

    // Rust worker retrieves only Rust tasks.
    let got = broker
        .retrieve_invocation_for_language(rustvello_proto::identifiers::TaskLanguage::Rust)
        .await
        .unwrap();
    assert_eq!(got, Some(rust_inv));
}

/// Legacy task-less queue items default to the Rust execution lane.
pub async fn test_global_queue_language_fallback(broker: &dyn Broker) {
    let inv = InvocationId::new();
    broker.route_invocation(&inv).await.unwrap();

    // The compatibility API predates language-qualified TaskIds and defaults to Rust.
    let got = broker
        .retrieve_invocation_for_language(rustvello_proto::identifiers::TaskLanguage::Python)
        .await
        .unwrap();
    assert_eq!(got, None);

    let got = broker
        .retrieve_invocation_for_language(rustvello_proto::identifiers::TaskLanguage::Rust)
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
        async fn suite_broker_named_queues_and_priorities() {
            let broker = $setup;
            $crate::broker::test_named_queues_and_priorities(&broker).await;
        }

        #[tokio::test]
        async fn suite_broker_queue_priority_validation() {
            let broker = $setup;
            $crate::broker::test_queue_priority_validation(&broker).await;
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
        async fn suite_broker_named_queues_and_priorities() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_named_queues_and_priorities(&broker).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_broker_queue_priority_validation() {
            let (_c, broker) = $setup.await;
            $crate::broker::test_queue_priority_validation(&broker).await;
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
