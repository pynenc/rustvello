"""Idempotency helpers and the at-least-once contract (docs/idempotency.md).

The process-kill variant (a worker killed after its side effects) is the Rust test
``crates/rustvello/tests/idempotency_kill.rs``.
"""

import pytest

from rustvello import App, InvocationId


def test_from_key_matches_rust_and_is_scoped_by_task():
    # Golden value shared with rustvello-proto's identifiers tests: the same key
    # names the same invocation from Rust and Python.
    derived = InvocationId.from_key("python::orders.charge", "order-42")
    assert str(derived) == "ecc07e8a-6a66-50be-8251-7657613e539d"
    assert str(InvocationId.from_key("python::orders.charge", "order-42")) == str(derived)
    assert str(InvocationId.from_key("python::orders.refund", "order-42")) != str(derived)
    with pytest.raises(ValueError, match="invalid task_id"):
        InvocationId.from_key("no-separator", "order-42")


def test_submit_with_key_creates_one_invocation_per_key(tmp_path):
    app = App(app_id="keys", backend="sqlite", db_path=str(tmp_path / "keys.db"))

    @app.task
    def place_order(order: str, amount: int) -> str:
        return f"{order}:{amount}"

    first = place_order.submit_with_key("order-42", order="order-42", amount=10)
    again = place_order.submit_with_key("order-42", order="order-42", amount=10)
    assert str(first.id) == str(again.id)
    assert str(first.id) == str(InvocationId.from_key(f"python::{__name__}.place_order", "order-42"))
    with pytest.raises(Exception, match="different content"):
        place_order.submit_with_key("order-42", order="order-42", amount=11)
    other = place_order.submit_with_key("order-43", order="order-43", amount=10)
    assert str(other.id) != str(first.id)

    app.run(num_workers=1, block=False)
    try:
        assert first.result(timeout=20) == "order-42:10"
        assert other.result(timeout=20) == "order-43:10"
    finally:
        app.stop()


def test_submit_with_key_fails_closed_without_atomic_publication():
    app = App(app_id="keys-memory")

    @app.task
    def work() -> int:
        return 1

    with pytest.raises(Exception, match="does not support crash-consistent"):
        work.submit_with_key("request-1")


def test_retry_reruns_the_body_with_the_same_invocation_id(tmp_path):
    """A retry re-runs the whole body; a ledger keyed by the invocation id applies the effect once."""
    app = App(app_id="ledger", backend="sqlite", db_path=str(tmp_path / "ledger.db"))
    executions: list[str] = []
    ledger: dict[str, int] = {}

    @app.task(max_retries=1)
    def charge(amount: int) -> int:
        current = app.current_invocation()
        assert current is not None
        executions.append(current.invocation_id)
        key = f"{current.invocation_id}:charge"
        ledger.setdefault(key, amount)  # idempotent write: a repeat is a no-op
        if current.num_retries == 0:
            raise ConnectionError("lost the reply after charging")
        return ledger[key]

    invocation = charge(10)
    app.run(num_workers=1, block=False)
    try:
        assert invocation.result(timeout=20) == 10
    finally:
        app.stop()
    assert executions == [str(invocation.id), str(invocation.id)]
    assert ledger == {f"{invocation.id}:charge": 10}


def test_keyed_child_resubmitted_by_a_parent_retry_is_rejected(tmp_path):
    """Documented limitation: each attempt has its own trace span, which is part of the replay identity.

    A parent retry that re-submits a child under the same key is therefore rejected
    ("different content or lineage") instead of returning the existing child. The
    guide recommends keying the child's side effect instead (last assertion).
    """
    app = App(app_id="steps", backend="sqlite", db_path=str(tmp_path / "steps.db"))
    effects: dict[str, int] = {}
    errors: list[str] = []

    @app.task
    def step(value: int, effect_key: str) -> int:
        effects.setdefault(effect_key, value * 2)  # idempotent effect keyed by the parent
        return effects[effect_key]

    @app.workflow(max_retries=1)
    def parent(value: int) -> int:
        current = app.current_invocation()
        assert current is not None
        effect_key = f"{current.invocation_id}:step"
        try:
            child = step.submit_with_key(effect_key, value=value, effect_key=effect_key)
        except Exception as exc:  # noqa: BLE001 - the second attempt's replay is rejected
            errors.append(str(exc))
            child = step(value, effect_key)  # a new child; its effect is still applied once
        doubled = child.result(timeout=20)
        if current.num_retries == 0:
            raise ConnectionError("failed after the step")
        return doubled

    invocation = parent(21)
    app.run(num_workers=2, block=False)
    try:
        assert invocation.result(timeout=30) == 42
    finally:
        app.stop()
    assert len(errors) == 1 and "different content or lineage" in errors[0]
    assert effects == {f"{invocation.id}:step": 42}
