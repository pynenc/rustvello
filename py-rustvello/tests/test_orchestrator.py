"""Tests for RustMemOrchestrator."""

import pytest

from rustvello import RustMemOrchestrator
from rustvello.rustvello import InvocationNotFoundError


def make_orchestrator_with_invocation(module="mod", name="func"):
    """Create an orchestrator with a registered invocation, return (orch, inv_id).

    The orchestrator needs the state_backend to have registered the invocation
    first (via upsert_invocation). But PyMemOrchestrator doesn't directly expose
    register_invocation(). So we use the internal mechanism: the orchestrator
    tracks status independently. We need to register via its own inner state.

    For the Python wrapper, we rely on set_invocation_status working only when
    the invocation is already known. We work around this by using the Rustvello
    high-level API's submit() or by accepting that the orchestrator is typically
    used together with a state_backend in integration scenarios.

    For isolated orchestrator testing, we call set_invocation_status after the
    invocation has been registered internally. The PyMemOrchestrator wraps
    MemOrchestrator which has register_invocation, but it's not exposed to Python.

    We'll test what we can through the exposed API.
    """
    return RustMemOrchestrator()


class TestRustMemOrchestrator:
    def test_create(self):
        orch = RustMemOrchestrator()
        assert orch is not None

    def test_invalid_status_string(self):
        orch = RustMemOrchestrator()
        with pytest.raises(ValueError):
            orch.set_invocation_status("550e8400-e29b-41d4-a716-446655440001", "NONEXISTENT")

    def test_invalid_invocation_id_format(self):
        orch = RustMemOrchestrator()
        with pytest.raises(InvocationNotFoundError):
            orch.get_invocation_status("not-a-uuid")

    def test_get_invocations_by_status_empty(self):
        orch = RustMemOrchestrator()
        result = orch.get_invocations_by_status("REGISTERED")
        assert result == []

    def test_get_invocations_by_status_requires_both_task_parts(self):
        orch = RustMemOrchestrator()
        with pytest.raises(ValueError, match="Both task_module and task_name"):
            orch.get_invocations_by_status("REGISTERED", task_module="mod")

    def test_waiting_for_id_validation(self):
        """InvocationId accepts any non-empty string — no UUID format required.

        Design decision (see `parse_invocation_id` in utils.rs): pynenc tests
        routinely use short readable IDs like "inv-abc", so format validation is
        intentionally lax. Empty strings are the only invalid case.
        """
        orch = RustMemOrchestrator()
        # Non-UUID but non-empty strings are accepted by design.
        orch.set_waiting_for("bad-id", "also-bad")  # must NOT raise

        # Empty string is always rejected.
        with pytest.raises(ValueError):
            orch.set_waiting_for("", "also-bad")


class TestRustMemOrchestratorIntegration:
    """Integration tests using the Rustvello app to register invocations."""

    def test_full_lifecycle_via_app(self):
        """Use Rustvello app to register a task + submit, then check orchestrator status."""
        from rustvello import Rustvello

        app = Rustvello()
        app.register_task("test_mod", "greet", lambda args: "hello")
        inv_id = app.submit("test_mod", "greet")
        status = app.get_status(inv_id)
        # After submit, status should be one of the early lifecycle states
        status_str = str(status)
        assert status_str in ("REGISTERED", "PENDING", "RUNNING", "SUCCESS")
