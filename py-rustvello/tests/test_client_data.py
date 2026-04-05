"""Tests for RustMemClientDataStore."""

from rustvello import RustMemClientDataStore


class TestRustMemClientDataStore:
    def test_small_data_stays_inline(self):
        store = RustMemClientDataStore(min_size_to_cache=1024)
        result = store.store_if_large("small")
        assert result == "small"

    def test_resolve_inline_data(self):
        store = RustMemClientDataStore(min_size_to_cache=1024)
        assert store.resolve("hello") == "hello"

    def test_large_data_externalized_and_resolved(self):
        # Set threshold low so even short strings get externalized
        store = RustMemClientDataStore(min_size_to_cache=10)
        large = "a" * 100
        stored = store.store_if_large(large)
        # Resolve should return original data
        resolved = store.resolve(stored)
        assert resolved == large

    def test_purge_clears_store(self):
        store = RustMemClientDataStore(min_size_to_cache=10)
        large = "b" * 100
        store.store_if_large(large)
        store.purge()
        # After purge, store is empty
