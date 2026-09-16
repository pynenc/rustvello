"""Python ABI coverage for W3C context capture and worker attachment."""

import json
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

import pytest

from rustvello import App, Rustvello, get_current_num_retries, get_current_trace_context
from rustvello.app import _current_trace_carrier, _invocation_trace_context

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def _otel_modules(events: list[object]) -> dict[str, ModuleType]:
    propagate = ModuleType("opentelemetry.propagate")
    context = ModuleType("opentelemetry.context")

    def inject(carrier: dict[str, str]) -> None:
        carrier["traceparent"] = TRACEPARENT
        carrier["tracestate"] = "ih=test"

    def extract(carrier: dict[str, str]) -> object:
        events.append(("extract", carrier.copy()))
        return "remote-context"

    def attach(value: object) -> object:
        events.append(("attach", value))
        return "token"

    def detach(token: object) -> None:
        events.append(("detach", token))

    propagate.inject = inject  # type: ignore[attr-defined]
    propagate.extract = extract  # type: ignore[attr-defined]
    context.attach = attach  # type: ignore[attr-defined]
    context.detach = detach  # type: ignore[attr-defined]
    return {
        "opentelemetry": ModuleType("opentelemetry"),
        "opentelemetry.propagate": propagate,
        "opentelemetry.context": context,
    }


def test_active_python_context_is_injected_as_w3c_carrier() -> None:
    with patch.dict(sys.modules, _otel_modules([])):
        assert _current_trace_carrier() == (TRACEPARENT, "ih=test")


def test_worker_attaches_and_detaches_persisted_context() -> None:
    events: list[object] = []
    with (
        patch.dict(sys.modules, _otel_modules(events)),
        patch("rustvello.app.get_current_trace_context", return_value=(TRACEPARENT, "ih=test")),
        _invocation_trace_context(),
    ):
        events.append("task")

    assert events == [
        ("extract", {"traceparent": TRACEPARENT, "tracestate": "ih=test"}),
        ("attach", "remote-context"),
        "task",
        ("detach", "token"),
    ]


def test_low_level_binding_accepts_valid_and_rejects_invalid_traceparent() -> None:
    app = Rustvello()
    app.register_task("trace", "task", Mock(return_value="null"))

    assert app.submit("trace", "task", {}, TRACEPARENT, "ih=test") is not None
    with pytest.raises(Exception, match="invalid W3C traceparent"):
        app.submit("trace", "task", {}, "invalid", None)


def test_real_python_context_survives_fail_once_retry_on_distinct_workers(
    tmp_path: Path,
) -> None:
    pytest.importorskip("opentelemetry")
    from opentelemetry.context import attach, detach
    from opentelemetry.trace import (
        NonRecordingSpan,
        SpanContext,
        TraceFlags,
        TraceState,
        set_span_in_context,
    )

    db_path = str(tmp_path / "python-retry.db")
    app = App(app_id="python-retry-trace", backend="sqlite", db_path=db_path)
    attempts: list[tuple[int | None, object]] = []

    @app.task(max_retries=1)
    def fail_once() -> str:
        retry = get_current_num_retries()
        attempts.append((retry, get_current_trace_context()))
        if retry == 0:
            raise RuntimeError("retry once")
        return "ok"

    span_context = SpanContext(
        trace_id=int("4bf92f3577b34da6a3ce929d0e0e4736", 16),
        span_id=int("00f067aa0ba902b7", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=TraceState((("ih", "python"),)),
    )
    token = attach(set_span_in_context(NonRecordingSpan(span_context)))
    try:
        invocation = fail_once()
    finally:
        detach(token)

    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    assert runner.run_one() is True
    assert runner.run_one() is True
    assert invocation.result(timeout=5) == "ok"

    assert [attempt for attempt, _ in attempts] == [0, 1]
    carriers = [carrier for _, carrier in attempts]
    assert carriers[0] != carriers[1]
    for traceparent, tracestate in carriers:
        assert traceparent != TRACEPARENT
        assert traceparent[3:35] == TRACEPARENT[3:35]
        assert traceparent.endswith("-01")
        assert tracestate == "ih=python"
    state_backend = app._backend_objects["state_backend"]
    stored = json.loads(state_backend.get_invocation(str(invocation.id)))
    history = json.loads(state_backend.get_history(str(invocation.id)))
    running_workers = [item["runner_id"] for item in history if item["status_record"]["status"] == "Running"]
    assert stored["trace_context"] == {
        "traceparent": TRACEPARENT,
        "tracestate": "ih=python",
    }
    assert len(running_workers) == 2
    assert running_workers[0] != running_workers[1]
    identity = json.loads(state_backend.get_workflow_data(str(invocation.id), "rustvello.execution.identity.v1"))
    assert identity["attempt"] == 1
    assert identity["execution_trace_context"] == {"traceparent": carriers[1][0], "tracestate": "ih=python"}
    assert identity["previous_attempt_trace_context"] == {"traceparent": carriers[0][0], "tracestate": "ih=python"}


def test_real_nested_python_submission_uses_execution_as_parent(tmp_path: Path) -> None:
    pytest.importorskip("opentelemetry")
    app = App(app_id="python-nested-trace", backend="sqlite", db_path=str(tmp_path / "nested.db"))
    children = []
    executions = []

    @app.task
    def child() -> str:
        assert _current_trace_carrier() == get_current_trace_context()
        return "child"

    @app.task(max_retries=1)
    def parent() -> str:
        execution = get_current_trace_context()
        assert _current_trace_carrier() == execution
        executions.append(execution)
        children.append(child())
        if get_current_num_retries() == 0:
            raise RuntimeError("retry parent")
        return "parent"

    invocation = parent()
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    for _ in range(4):
        assert runner.run_one() is True
    assert runner.run_one() is False
    assert invocation.result(timeout=5) == "parent"
    assert len(executions) == len(children) == 2
    assert executions[0] != executions[1]
    assert executions[0][0][3:35] == executions[1][0][3:35]
    backend = app._backend_objects["state_backend"]
    identity = json.loads(backend.get_workflow_data(str(invocation.id), "rustvello.execution.identity.v1"))
    assert identity["attempt"] == 1
    assert identity["execution_trace_context"]["traceparent"] == executions[1][0]
    assert identity["previous_attempt_trace_context"]["traceparent"] == executions[0][0]
    for nested, execution in zip(children, executions):
        stored = json.loads(backend.get_invocation(str(nested.id)))
        assert stored["parent_invocation_id"] == str(invocation.id)
        assert stored["trace_context"]["traceparent"] == execution[0]
        assert nested.result(timeout=5) == "child"
    assert get_current_trace_context() is None
