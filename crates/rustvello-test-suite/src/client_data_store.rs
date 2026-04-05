//! Shared client data store test definitions.
//!
//! Each function tests a specific behavior of the [`ClientDataStore`] trait.
//! Backend crates call these with their concrete implementation.

use rustvello_core::client_data_store::ClientDataStore;

/// Store a value and retrieve it by key.
pub async fn test_store_and_retrieve(store: &dyn ClientDataStore) {
    store.store("k1", "v1").await.unwrap();
    assert_eq!(store.retrieve("k1").await.unwrap(), "v1");
}

/// Retrieve a missing key returns an error.
pub async fn test_retrieve_missing_key_errors(store: &dyn ClientDataStore) {
    let err = store.retrieve("nonexistent").await;
    assert!(err.is_err());
}

/// Purge removes all data.
pub async fn test_purge_removes_all(store: &dyn ClientDataStore) {
    store.store("k1", "v1").await.unwrap();
    store.store("k2", "v2").await.unwrap();
    store.purge().await.unwrap();
    assert!(store.retrieve("k1").await.is_err());
    assert!(store.retrieve("k2").await.is_err());
}

/// Store with an existing key overwrites (upsert semantics).
pub async fn test_upsert_semantics(store: &dyn ClientDataStore) {
    store.store("k1", "v1").await.unwrap();
    store.store("k1", "v2").await.unwrap();
    assert_eq!(store.retrieve("k1").await.unwrap(), "v2");
}

/// Store and retrieve multiple distinct keys.
pub async fn test_multiple_keys(store: &dyn ClientDataStore) {
    for i in 0..10 {
        let key = format!("key_{i}");
        let val = format!("val_{i}");
        store.store(&key, &val).await.unwrap();
    }
    for i in 0..10 {
        let key = format!("key_{i}");
        let val = format!("val_{i}");
        assert_eq!(store.retrieve(&key).await.unwrap(), val);
    }
}

/// Store large values.
pub async fn test_large_value(store: &dyn ClientDataStore) {
    // 1MB string
    let large = "x".repeat(1_000_000);
    store.store("big", &large).await.unwrap();
    let got = store.retrieve("big").await.unwrap();
    assert_eq!(got.len(), 1_000_000);
    assert_eq!(got, large);
}

/// Backend name returns a non-empty string.
pub async fn test_backend_name(store: &dyn ClientDataStore) {
    let name = store.backend_name();
    assert!(!name.is_empty());
}

/// Macro to generate all client data store suite tests for a given setup
/// expression.
///
/// # Example
///
/// ```rust,ignore
/// use rustvello_test_suite::client_data_store_suite;
/// use rustvello_mem::client_data_store::MemClientDataStore;
///
/// client_data_store_suite!(MemClientDataStore::new());
/// ```
#[macro_export]
macro_rules! client_data_store_suite {
    ($setup:expr) => {
        #[tokio::test]
        async fn suite_cds_store_and_retrieve() {
            let store = $setup;
            $crate::client_data_store::test_store_and_retrieve(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_retrieve_missing_key_errors() {
            let store = $setup;
            $crate::client_data_store::test_retrieve_missing_key_errors(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_purge_removes_all() {
            let store = $setup;
            $crate::client_data_store::test_purge_removes_all(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_upsert_semantics() {
            let store = $setup;
            $crate::client_data_store::test_upsert_semantics(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_multiple_keys() {
            let store = $setup;
            $crate::client_data_store::test_multiple_keys(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_large_value() {
            let store = $setup;
            $crate::client_data_store::test_large_value(&store).await;
        }

        #[tokio::test]
        async fn suite_cds_backend_name() {
            let store = $setup;
            $crate::client_data_store::test_backend_name(&store).await;
        }
    };
}

/// Async-setup variant of [`client_data_store_suite!`] for testcontainers backends.
///
/// `$setup` is an async expression returning `(_guard, client_data_store)`.
/// Tests are `#[ignore = "requires Docker"]`.
#[macro_export]
macro_rules! async_client_data_store_suite {
    ($setup:expr) => {
        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_store_and_retrieve() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_store_and_retrieve(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_retrieve_missing_key_errors() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_retrieve_missing_key_errors(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_purge_removes_all() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_purge_removes_all(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_upsert_semantics() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_upsert_semantics(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_multiple_keys() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_multiple_keys(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_large_value() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_large_value(&store).await;
        }

        #[tokio::test]
        #[ignore = "requires Docker"]
        async fn suite_cds_backend_name() {
            let (_c, store) = $setup.await;
            $crate::client_data_store::test_backend_name(&store).await;
        }
    };
}
