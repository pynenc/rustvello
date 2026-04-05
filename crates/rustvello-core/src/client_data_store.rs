//! Client data store — external storage for large serialized values.
//!
//! Transparently routes small values inline and externalizes large values
//! to a backend store with content-hash deduplication and LRU caching.
//!
//! Mirrors pynenc's `BaseClientDataStore` system.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use rustvello_proto::config::ClientDataStoreConfig;

use crate::error::{RustvelloError, RustvelloResult};

/// Reference key prefix. Any string starting with this is a CDS reference.
pub const REFERENCE_PREFIX: &str = "__rustvello__cds__:";

/// Backend storage trait for client data.
///
/// Implementations need only provide three simple operations: store, retrieve,
/// and purge. The [`ClientDataStoreManager`] handles size routing, LRU caching,
/// and key generation on top of any backend.
#[async_trait]
pub trait ClientDataStore: Send + Sync {
    /// Store a serialized value by its content-hash key.
    ///
    /// Backends should use upsert semantics (INSERT OR REPLACE) so that
    /// storing the same key twice is a no-op (content-hash deduplication).
    async fn store(&self, key: &str, value: &str) -> RustvelloResult<()>;

    /// Retrieve a serialized value by its reference key.
    ///
    /// Returns an error if the key is not found.
    async fn retrieve(&self, key: &str) -> RustvelloResult<String>;

    /// Remove all stored data.
    async fn purge(&self) -> RustvelloResult<()>;

    /// Human-readable name of this backend implementation.
    fn backend_name(&self) -> &'static str {
        "Unknown"
    }

    /// Key-value statistics about this backend's current state.
    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

/// Check whether a string is a CDS reference key.
pub fn is_reference(value: &str) -> bool {
    value.starts_with(REFERENCE_PREFIX)
}

/// Generate a content-hash reference key from serialized data.
pub fn generate_reference_key(data: &str) -> String {
    let hash = Sha256::digest(data.as_bytes());
    format!("{REFERENCE_PREFIX}{hash:x}")
}

// ---------------------------------------------------------------------------
// ClientDataStoreManager — wraps a backend with size routing + LRU cache
// ---------------------------------------------------------------------------

/// High-level client data store manager.
///
/// Wraps a [`ClientDataStore`] backend with:
/// - Size-based routing (small → inline, large → external)
/// - Content-hash key generation (SHA-256 dedup)
/// - Process-local LRU cache
///
/// This is the type that [`RustvelloApp`] owns. It corresponds to pynenc's
/// `BaseClientDataStore` public API.
pub struct ClientDataStoreManager {
    backend: Arc<dyn ClientDataStore>,
    config: ClientDataStoreConfig,
    cache: Mutex<lru::LruCache<String, String>>,
}

impl ClientDataStoreManager {
    /// Create a new manager wrapping the given backend.
    pub fn new(backend: Arc<dyn ClientDataStore>, config: ClientDataStoreConfig) -> Self {
        let cap = NonZeroUsize::new(config.local_cache_size).unwrap_or(NonZeroUsize::MIN);
        Self {
            backend,
            config,
            cache: Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Store a serialized value, externalizing it if it exceeds the size threshold.
    ///
    /// Returns either:
    /// - The original `serialized` string (inline, if small or disabled)
    /// - A reference key string (if externalized)
    pub async fn store_if_large(&self, serialized: &str) -> RustvelloResult<String> {
        if self.config.disabled {
            return Ok(serialized.to_owned());
        }

        // Already a reference? Pass through.
        if is_reference(serialized) {
            return Ok(serialized.to_owned());
        }

        let size = serialized.len();

        // Below minimum: inline
        if size < self.config.min_size_to_cache {
            return Ok(serialized.to_owned());
        }

        // Above maximum (if set): inline with warning
        if self.config.max_size_to_cache > 0 && size > self.config.max_size_to_cache {
            tracing::warn!(
                "Value size ({size} bytes) exceeds max_size_to_cache ({}). Returning inline.",
                self.config.max_size_to_cache
            );
            return Ok(serialized.to_owned());
        }

        if size > self.config.warn_threshold {
            tracing::warn!(
                "Value size ({size} bytes) exceeds warn_threshold ({}). Consider restructuring.",
                self.config.warn_threshold
            );
        }

        let key = generate_reference_key(serialized);
        self.backend.store(&key, serialized).await?;

        // Cache the value for fast resolution
        self.cache
            .lock()
            .map_err(|e| RustvelloError::Internal {
                message: format!("CDS cache lock poisoned: {e}"),
            })?
            .put(key.clone(), serialized.to_owned());

        Ok(key)
    }

    /// Resolve a value — if it's a reference key, retrieve from backend (with LRU cache).
    /// If it's an inline value, return as-is.
    pub async fn resolve(&self, data: &str) -> RustvelloResult<String> {
        if !is_reference(data) {
            return Ok(data.to_owned());
        }

        // Check LRU cache first
        {
            let mut cache = self.cache.lock().map_err(|e| RustvelloError::Internal {
                message: format!("CDS cache lock poisoned: {e}"),
            })?;
            if let Some(cached) = cache.get(data) {
                return Ok(cached.clone());
            }
        }

        // Cache miss — retrieve from backend
        let value = self.backend.retrieve(data).await?;

        // Cache the retrieved value
        self.cache
            .lock()
            .map_err(|e| RustvelloError::Internal {
                message: format!("CDS cache lock poisoned: {e}"),
            })?
            .put(data.to_owned(), value.clone());

        Ok(value)
    }

    /// Store a value directly by key (delegates to backend).
    pub async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
        self.backend.store(key, value).await
    }

    /// Retrieve a value directly by key (delegates to backend).
    pub async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
        self.backend.retrieve(key).await
    }

    /// Clear the LRU cache and purge all backend data.
    pub async fn purge(&self) -> RustvelloResult<()> {
        self.cache
            .lock()
            .map_err(|e| RustvelloError::Internal {
                message: format!("CDS cache lock poisoned: {e}"),
            })?
            .clear();
        self.backend.purge().await
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ClientDataStoreConfig {
        &self.config
    }

    /// Human-readable name of the underlying backend implementation.
    pub fn backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    /// Key-value statistics about the underlying backend's current state.
    pub async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        self.backend.usage_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::RustvelloError;
    use std::collections::HashMap;

    /// Minimal in-memory backend for testing the manager.
    struct FakeBackend {
        data: Mutex<HashMap<String, String>>,
    }

    impl FakeBackend {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl ClientDataStore for FakeBackend {
        async fn store(&self, key: &str, value: &str) -> RustvelloResult<()> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_owned(), value.to_owned());
            Ok(())
        }

        async fn retrieve(&self, key: &str) -> RustvelloResult<String> {
            self.data
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| RustvelloError::state_backend(format!("key not found: {key}")))
        }

        async fn purge(&self) -> RustvelloResult<()> {
            self.data.lock().unwrap().clear();
            Ok(())
        }
    }

    fn make_manager(config: ClientDataStoreConfig) -> ClientDataStoreManager {
        ClientDataStoreManager::new(Arc::new(FakeBackend::new()), config)
    }

    #[test]
    fn reference_key_format() {
        let key = generate_reference_key("hello world");
        assert!(key.starts_with(REFERENCE_PREFIX));
        assert!(is_reference(&key));
        assert!(!is_reference("just a normal string"));
    }

    #[test]
    fn reference_key_deterministic() {
        let k1 = generate_reference_key("same content");
        let k2 = generate_reference_key("same content");
        assert_eq!(k1, k2);
    }

    #[test]
    fn reference_key_differs_for_different_content() {
        let k1 = generate_reference_key("content A");
        let k2 = generate_reference_key("content B");
        assert_ne!(k1, k2);
    }

    #[tokio::test]
    async fn inline_below_threshold() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 100;
        let mgr = make_manager(cds_config);
        let small = "short";
        let result = mgr.store_if_large(small).await.unwrap();
        // Below threshold — returned inline
        assert_eq!(result, small);
        assert!(!is_reference(&result));
    }

    #[tokio::test]
    async fn externalize_above_threshold() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 10;
        let mgr = make_manager(cds_config);
        let large = "a]".repeat(20); // 40 bytes, above 10
        let result = mgr.store_if_large(&large).await.unwrap();
        assert!(is_reference(&result));

        // Resolve should return the original
        let resolved = mgr.resolve(&result).await.unwrap();
        assert_eq!(resolved, large);
    }

    #[tokio::test]
    async fn inline_when_disabled() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.disabled = true;
        cds_config.min_size_to_cache = 1; // Would externalize, but disabled
        let mgr = make_manager(cds_config);
        let data = "x".repeat(100);
        let result = mgr.store_if_large(&data).await.unwrap();
        assert!(!is_reference(&result));
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn inline_above_max_size() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 10;
        cds_config.max_size_to_cache = 50;
        let mgr = make_manager(cds_config);
        let huge = "x".repeat(100); // 100 > max 50
        let result = mgr.store_if_large(&huge).await.unwrap();
        assert!(!is_reference(&result));
        assert_eq!(result, huge);
    }

    #[tokio::test]
    async fn resolve_inline_passthrough() {
        let mgr = make_manager(ClientDataStoreConfig::default());
        let data = "not a reference";
        let result = mgr.resolve(data).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn content_hash_dedup() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 5;
        let mgr = make_manager(cds_config);
        let data = "deduplicate me";
        let k1 = mgr.store_if_large(data).await.unwrap();
        let k2 = mgr.store_if_large(data).await.unwrap();
        assert_eq!(k1, k2); // Same content → same key
    }

    #[tokio::test]
    async fn purge_clears_cache_and_backend() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 5;
        let mgr = make_manager(cds_config);
        let data = "some large payload here";
        let key = mgr.store_if_large(data).await.unwrap();
        assert!(is_reference(&key));

        mgr.purge().await.unwrap();

        // Backend is empty — resolve should fail
        let err = mgr.resolve(&key).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn lru_cache_hit() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 5;
        cds_config.local_cache_size = 10;
        let mgr = make_manager(cds_config);
        let data = "cached payload data";
        let key = mgr.store_if_large(data).await.unwrap();
        assert!(is_reference(&key));

        // First resolve populates cache from backend (already cached from store_if_large)
        let r1 = mgr.resolve(&key).await.unwrap();
        assert_eq!(r1, data);

        // Purge backend only (simulate offline backend) by storing then purging
        mgr.backend.purge().await.unwrap();

        // Second resolve should hit cache, not backend
        let r2 = mgr.resolve(&key).await.unwrap();
        assert_eq!(r2, data);
    }

    #[tokio::test]
    async fn passthrough_existing_reference() {
        let mut cds_config = ClientDataStoreConfig::default();
        cds_config.min_size_to_cache = 5;
        let mgr = make_manager(cds_config);
        // If the value is already a reference, pass through
        let ref_key = format!("{REFERENCE_PREFIX}abc123");
        let result = mgr.store_if_large(&ref_key).await.unwrap();
        assert_eq!(result, ref_key);
    }

    #[test]
    fn lru_cache_eviction() {
        let cap = NonZeroUsize::new(3).unwrap();
        let mut cache = lru::LruCache::<String, String>::new(cap);
        cache.put("a".into(), "1".into());
        cache.put("b".into(), "2".into());
        cache.put("c".into(), "3".into());
        assert_eq!(cache.len(), 3);

        // Adding a 4th evicts the oldest ("a")
        cache.put("d".into(), "4".into());
        assert_eq!(cache.len(), 3);
        assert!(cache.get("a").is_none());
        assert!(cache.get("d").is_some());
    }

    #[test]
    fn lru_cache_access_refreshes() {
        let cap = NonZeroUsize::new(3).unwrap();
        let mut cache = lru::LruCache::<String, String>::new(cap);
        cache.put("a".into(), "1".into());
        cache.put("b".into(), "2".into());
        cache.put("c".into(), "3".into());

        // Access "a" to move it to the end
        cache.get("a");

        // Insert "d" — should evict "b" (oldest after "a" was refreshed)
        cache.put("d".into(), "4".into());
        assert!(cache.get("a").is_some());
        assert!(cache.get("b").is_none());
    }
}
