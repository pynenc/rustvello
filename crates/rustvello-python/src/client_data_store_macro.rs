/// Generates all trait-forwarding `#[pymethods]` for a PyO3 client-data-store wrapper.
///
/// The target type must have an `inner: Arc<ClientDataStoreManager>` field.
///
/// Each backend keeps its own `#[new]` constructor in a separate
/// `#[pymethods]` block. This macro generates the remaining 6 methods,
/// all of which release the GIL via `py.allow_threads()`.
macro_rules! impl_py_client_data_store {
    ($py_type:ty) => {
        #[pyo3::pymethods]
        impl $py_type {
            /// Store a value, externalizing if above size threshold.
            /// Returns the original value (inline) or a reference key.
            fn store_if_large(
                &self,
                py: pyo3::Python<'_>,
                serialized: &str,
            ) -> pyo3::PyResult<String> {
                let mgr = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(mgr.store_if_large(serialized)))
                    .map_err(crate::error::to_py_err)
            }

            /// Resolve a value — if it's a reference key, retrieve from backend.
            /// If inline, return as-is.
            fn resolve(&self, py: pyo3::Python<'_>, data: &str) -> pyo3::PyResult<String> {
                let mgr = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(mgr.resolve(data)))
                    .map_err(crate::error::to_py_err)
            }

            /// Store a value directly by key.
            fn store(&self, py: pyo3::Python<'_>, key: &str, value: &str) -> pyo3::PyResult<()> {
                let mgr = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(mgr.store(key, value)))
                    .map_err(crate::error::to_py_err)
            }

            /// Retrieve a value directly by key.
            fn retrieve(&self, py: pyo3::Python<'_>, key: &str) -> pyo3::PyResult<String> {
                let mgr = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(mgr.retrieve(key)))
                    .map_err(crate::error::to_py_err)
            }

            /// Purge all stored data and clear the cache.
            fn purge(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<()> {
                let mgr = std::sync::Arc::clone(&self.inner);
                let rt = crate::runtime::shared_runtime()?;
                py.allow_threads(|| rt.block_on(mgr.purge()))
                    .map_err(crate::error::to_py_err)
            }

            /// Human-readable name of the underlying backend implementation.
            fn backend_name(&self) -> String {
                self.inner.backend_name().to_owned()
            }
        }
    };
}
