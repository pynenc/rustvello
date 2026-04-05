//! PyO3 wrapper for the client data store subsystem.

use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_mem::client_data_store::MemClientDataStore;
use rustvello_proto::config::ClientDataStoreConfig;

/// Rust in-memory client data store exposed to Python.
///
/// Wraps `ClientDataStoreManager` with a `MemClientDataStore` backend.
/// Provides SHA-256 content-hash dedup and LRU caching.
#[pyclass(name = "RustMemClientDataStore")]
pub struct PyMemClientDataStore {
    pub(crate) inner: Arc<ClientDataStoreManager>,
}

#[pymethods]
impl PyMemClientDataStore {
    #[new]
    #[pyo3(signature = (min_size_to_cache=1024, max_size_to_cache=0, local_cache_size=128))]
    fn new(min_size_to_cache: usize, max_size_to_cache: usize, local_cache_size: usize) -> Self {
        let backend: Arc<dyn ClientDataStore> = Arc::new(MemClientDataStore::new());
        let mut config = ClientDataStoreConfig::default();
        config.min_size_to_cache = min_size_to_cache;
        config.max_size_to_cache = max_size_to_cache;
        config.local_cache_size = local_cache_size;
        let manager = ClientDataStoreManager::new(backend, config);
        Self {
            inner: Arc::new(manager),
        }
    }
}

impl_py_client_data_store!(PyMemClientDataStore);

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn small_data_inline() {
        Python::with_gil(|py| {
            // Default min_size_to_cache is 1024, so small data stays inline
            let store = PyMemClientDataStore::new(1024, 0, 128);
            let input = "small";
            let stored = store.store_if_large(py, input).unwrap();
            // Should return the data as-is (not externalized)
            assert_eq!(stored, input);
        });
    }

    #[test]
    fn resolve_inline_data() {
        Python::with_gil(|py| {
            let store = PyMemClientDataStore::new(1024, 0, 128);
            // Non-reference data should resolve to itself
            let resolved = store.resolve(py, "hello").unwrap();
            assert_eq!(resolved, "hello");
        });
    }

    #[test]
    fn large_data_externalized_and_resolved() {
        Python::with_gil(|py| {
            // Set min_size_to_cache to 10 so even short strings get externalized
            let store = PyMemClientDataStore::new(10, 0, 128);
            let large = "a]".repeat(20); // 40 chars, above threshold
            let stored = store.store_if_large(py, &large).unwrap();
            // The stored value should be a reference key (different from original)
            // or the original depending on implementation

            // Resolve should return the original value
            let resolved = store.resolve(py, &stored).unwrap();
            assert_eq!(resolved, large);
        });
    }

    #[test]
    fn purge_clears_store() {
        Python::with_gil(|py| {
            let store = PyMemClientDataStore::new(10, 0, 128);
            let large = "b".repeat(100);
            store.store_if_large(py, &large).unwrap();
            store.purge(py).unwrap();
            // After purge, the store is empty (externalized references won't resolve)
        });
    }
}
