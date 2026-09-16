"""Python surface tests; transaction/fault semantics live in Rust's core tests."""

import json

import pytest

from rustvello import App, AppConfig, InvocationId, get_current_num_retries
from rustvello.rustvello import RustSqliteDatabase


def test_explicit_sqlite_sync_and_idempotent_high_level_submission(tmp_path):
    path = str(tmp_path / "runtime.sqlite")
    db = RustSqliteDatabase(path, "durable", synchronous="FULL", busy_timeout_ms=70)
    assert db.synchronization() == ("wal", 2, 70)
    app = App(app_id="durable", backend="sqlite", db_path=path, sqlite_synchronous="FULL")

    @app.workflow
    def work(value):
        return value

    invocation_id = InvocationId()
    first = work.submit_with_id(invocation_id, value=3)
    assert str(work.submit_with_id(invocation_id, value=3).id) == str(first.id)
    with pytest.raises(Exception, match="different content"):
        work.submit_with_id(invocation_id, value=4)
    app.run(num_workers=1, block=False)
    try:
        assert first.result(timeout=10) == 3
        assert work.submit_with_id(invocation_id, value=3).result(timeout=2) == 3
    finally:
        app.stop()


def test_unsupported_profile_and_invalid_sqlite_options_fail_closed(tmp_path):
    app = App()

    @app.task
    def work():
        return 1

    with pytest.raises(Exception, match="does not support crash-consistent"):
        work.submit_with_id(InvocationId())
    with pytest.raises(ValueError, match="FULL or NORMAL"):
        RustSqliteDatabase(str(tmp_path / "bad.db"), "bad", synchronous="OFF")
    with pytest.raises(Exception, match="busy timeout"):
        RustSqliteDatabase(str(tmp_path / "bad.db"), "bad", busy_timeout_ms=0)
    assert not list(tmp_path.iterdir())


def test_app_config_identity_and_runner_configuration():
    with pytest.raises(ValueError, match="must match"):
        App(app_id="a", config=AppConfig(app_id="b"))
    config = AppConfig(app_id="configured", heartbeat_interval_seconds=1, runner_dead_after_seconds=2)
    app = App(app_id="configured", config=config)
    assert app._config.heartbeat_interval_seconds == 1


def test_durable_replay_restores_original_w3c_context_and_rejects_drift(tmp_path):
    pytest.importorskip("opentelemetry")
    from opentelemetry.context import attach, detach
    from opentelemetry.propagate import extract

    app = App(app_id="traced-replay", backend="sqlite", db_path=str(tmp_path / "runtime.db"))

    @app.workflow
    def work():
        return 1

    request_id = InvocationId()
    carrier = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "ih=replay",
    }
    for _ in range(2):
        token = attach(extract(carrier))
        try:
            assert str(work.submit_with_id(request_id).id) == str(request_id)
        finally:
            detach(token)
    changed = {**carrier, "tracestate": "ih=changed"}
    token = attach(extract(changed))
    try:
        with pytest.raises(Exception, match="different content or lineage"):
            work.submit_with_id(request_id)
    finally:
        detach(token)
    state = app._backend_objects["state_backend"]
    stored = json.loads(state.get_invocation(str(request_id)))
    assert stored["trace_context"] == carrier
    assert len(json.loads(state.get_history(str(request_id)))) == 1


def test_python_retry_preserves_named_queue_and_priority(tmp_path):
    config = AppConfig(app_id="routing", broker_queues=["critical"], runner_queues=["critical"])
    app = App(app_id="routing", backend="sqlite", db_path=str(tmp_path / "runtime.db"), config=config)
    order = []

    @app.task(queue="critical", priority=9, max_retries=1)
    def high():
        order.append("high")
        if get_current_num_retries() == 0:
            raise RuntimeError("fail once")
        return "high"

    @app.task(queue="critical", priority=1)
    def low():
        order.append("low")
        return "low"

    low_invocation = low.submit_with_id(InvocationId())
    high_invocation = high.submit_with_id(InvocationId())
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    assert runner.run_one()
    assert runner.run_one()
    assert high_invocation.result(timeout=1) == "high"
    assert runner.run_one()
    assert low_invocation.result(timeout=1) == "low"
    assert order == ["high", "high", "low"]
