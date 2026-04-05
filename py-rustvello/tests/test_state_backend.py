"""Tests for RustMemStateBackend."""

from rustvello import RustMemStateBackend

UUID1 = "550e8400-e29b-41d4-a716-446655440001"
UUID2 = "550e8400-e29b-41d4-a716-446655440002"


class TestRustMemStateBackend:
    def test_upsert_and_get_result(self):
        sb = RustMemStateBackend()
        sb.upsert_invocation(UUID1, "mod", "func", {"x": "42"})

        # No result yet
        assert sb.get_result(UUID1) is None

        # Store and retrieve result
        sb.store_result(UUID1, "hello")
        assert sb.get_result(UUID1) == "hello"

    def test_store_and_get_error(self):
        sb = RustMemStateBackend()
        sb.upsert_invocation(UUID1, "mod", "func", {"x": "42"})

        # No error yet
        assert sb.get_error(UUID1) is None

        # Store error with traceback
        sb.store_error(UUID1, "ValueError", "bad input", "traceback here")
        err = sb.get_error(UUID1)
        assert err is not None
        assert "ValueError" in err
        assert "bad input" in err

    def test_store_error_without_traceback(self):
        sb = RustMemStateBackend()
        sb.upsert_invocation(UUID1, "mod", "func", {})
        sb.store_error(UUID1, "RuntimeError", "oops")
        err = sb.get_error(UUID1)
        assert err is not None
        assert "RuntimeError" in err

    def test_purge_clears_data(self):
        sb = RustMemStateBackend()
        sb.upsert_invocation(UUID1, "mod", "func", {"x": "1"})
        sb.store_result(UUID1, "result")
        sb.purge()
        assert sb.get_result(UUID1) is None

    def test_multiple_invocations(self):
        sb = RustMemStateBackend()
        sb.upsert_invocation(UUID1, "mod", "func", {"x": "1"})
        sb.upsert_invocation(UUID2, "mod", "func", {"x": "2"})
        sb.store_result(UUID1, "result1")
        sb.store_result(UUID2, "result2")
        assert sb.get_result(UUID1) == "result1"
        assert sb.get_result(UUID2) == "result2"
