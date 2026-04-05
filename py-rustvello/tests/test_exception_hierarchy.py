"""Validate the Rust-defined exception inheritance chains exposed via PyO3.

This test suite must run in an environment where rustvello is installed
(py-rustvello/tests), so the import is unconditional.
"""

import pytest

import rustvello.rustvello as rv


@pytest.mark.parametrize(
    "exc_name, parent_names",
    [
        # Base
        ("RustvelloError", ["Exception"]),
        # Retry
        ("RetryError", ["RustvelloError"]),
        ("ConcurrencyRetryError", ["RetryError", "RustvelloError"]),
        # Serialization
        ("SerializationError", ["RustvelloError"]),
        # Task
        ("TaskError", ["RustvelloError"]),
        ("TaskNotFoundError", ["TaskError", "RustvelloError"]),
        ("TaskNotRegisteredError", ["TaskError", "RustvelloError"]),
        ("CycleDetectedError", ["TaskError", "RustvelloError"]),
        ("RunnerNotExecutableError", ["TaskError", "RustvelloError"]),
        ("TaskClassNotFoundError", ["TaskError", "RustvelloError"]),
        # Invocation
        ("InvocationError", ["RustvelloError"]),
        ("InvocationNotFoundError", ["InvocationError", "RustvelloError"]),
        # Status (under Invocation)
        ("InvocationStatusError", ["InvocationError", "RustvelloError"]),
        ("StatusTransitionError", ["InvocationStatusError", "RustvelloError"]),
        ("StatusOwnershipError", ["InvocationStatusError", "RustvelloError"]),
        ("StatusRaceConditionError", ["InvocationStatusError", "RustvelloError"]),
        # Infrastructure
        ("StateBackendError", ["RustvelloError"]),
        ("BrokerError", ["RustvelloError"]),
        ("RunnerError", ["RustvelloError"]),
        ("ConfigurationError", ["RustvelloError"]),
        # Internal
        ("InternalError", ["RustvelloError"]),
    ],
    ids=lambda x: x if isinstance(x, str) else None,
)
def test_rustvello_exception_hierarchy(exc_name: str, parent_names: list[str]) -> None:
    """Every Rust-defined exception must be a subclass of its documented parents."""
    exc_cls = getattr(rv, exc_name)
    for parent_name in parent_names:
        parent_cls = Exception if parent_name == "Exception" else getattr(rv, parent_name)
        assert issubclass(exc_cls, parent_cls), f"rustvello.{exc_name} is not a subclass of rustvello.{parent_name}"
