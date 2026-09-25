"""Retry backoff, execution deadlines and cancellation from the Python App (parity with Rust)."""

import time

import pytest

from rustvello import App, InvocationCancelledError, InvocationStatus, TaskConfig, get_current_num_retries


def _wait_status(invocation, wanted: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while str(invocation.status) != wanted:
        assert time.monotonic() < deadline, f"still {invocation.status}, wanted {wanted}"
        time.sleep(0.01)


class TestRetryPolicyConfig:
    def test_defaults_keep_immediate_retries_and_no_deadline(self) -> None:
        config = TaskConfig()
        assert config.retry_delay == 0.0
        assert config.retry_max_delay == 300.0
        assert config.retry_backoff == 2.0
        assert config.retry_jitter == "equal"
        assert config.timeout is None
        assert config.retry_on_timeout is True

    def test_with_retry_policy_round_trips_seconds(self) -> None:
        config = TaskConfig(max_retries=2).with_retry_policy(
            retry_delay=0.25,
            retry_max_delay=5,
            retry_backoff=3,
            retry_jitter="full",
            timeout=1.5,
            retry_on_timeout=False,
        )
        assert config.max_retries == 2
        assert config.retry_delay == 0.25
        assert config.retry_max_delay == 5.0
        assert config.retry_backoff == 3.0
        assert config.retry_jitter == "full"
        assert config.timeout == 1.5
        assert config.retry_on_timeout is False

    @pytest.mark.parametrize(
        "kwargs",
        [{"retry_delay": -1}, {"retry_backoff": 0.5}, {"retry_jitter": "decorrelated"}, {"timeout": 0}],
    )
    def test_invalid_policy_is_rejected(self, kwargs) -> None:
        with pytest.raises(ValueError):
            TaskConfig().with_retry_policy(**kwargs)

    def test_cancelled_status_is_terminal(self) -> None:
        assert InvocationStatus.cancelled().is_terminal()
        assert str(InvocationStatus.cancelled()) == "CANCELLED"


def test_retry_waits_for_backoff_delay(tmp_path) -> None:
    app = App(app_id="backoff", backend="sqlite", db_path=str(tmp_path / "backoff.db"))
    attempts: list[float] = []

    @app.task(max_retries=2, retry_delay=0.5, retry_jitter="none")
    def flaky() -> str:
        attempts.append(time.monotonic())
        if get_current_num_retries() == 0:
            raise ConnectionError("first attempt fails")
        return "ok"

    invocation = flaky()
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    assert runner.run_one()
    assert str(invocation.status) == "RETRY"
    assert not runner.run_one(), "retry must not be delivered before its delay"
    deadline = time.monotonic() + 10
    while not runner.run_one():
        assert time.monotonic() < deadline
        time.sleep(0.02)
    assert invocation.result(timeout=1) == "ok"
    assert len(attempts) == 2
    assert attempts[1] - attempts[0] >= 0.45


def test_timeout_fails_with_task_timeout_error() -> None:
    app = App(app_id="timeouts")

    @app.task(timeout=0.1)
    def slow() -> str:
        time.sleep(1.0)
        return "late"

    invocation = slow()
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    started = time.monotonic()
    assert runner.run_one()
    assert time.monotonic() - started < 0.9
    with pytest.raises(RuntimeError, match="TaskTimeoutError"):
        invocation.result(timeout=1)
    time.sleep(1.0)  # the abandoned thread finishes; its result is discarded
    assert str(invocation.status) == "FAILED"


def test_timeout_retry_can_be_disabled() -> None:
    app = App(app_id="timeouts_final")
    calls: list[int] = []

    @app.task(max_retries=3, timeout=0.05, retry_on_timeout=False)
    def slow() -> None:
        calls.append(1)
        time.sleep(0.3)

    invocation = slow()
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    while runner.run_one():
        pass
    assert str(invocation.status) == "FAILED"
    assert len(calls) == 1


def test_cancel_queued_invocation_never_runs() -> None:
    app = App(app_id="cancel_queued")
    calls: list[int] = []

    @app.task
    def work() -> int:
        calls.append(1)
        return 1

    invocation = work()
    assert invocation.cancel() is True
    assert app.cancel(invocation) is False
    assert str(invocation.status) == "CANCELLED"
    with pytest.raises(InvocationCancelledError):
        invocation.result(timeout=1)
    runner = app._build_runner(num_workers=1, idle_sleep_ms=1)
    while runner.run_one():
        pass
    assert calls == []


def test_cancel_running_invocation_discards_its_result() -> None:
    app = App(app_id="cancel_running")
    app.config.cancellation_check_interval_seconds = 0.05

    @app.task
    def slow() -> str:
        time.sleep(1.0)
        return "late"

    app.run(num_workers=1, idle_sleep_ms=5, block=False)
    try:
        invocation = slow()
        _wait_status(invocation, "RUNNING")
        assert invocation.cancel() is True
        with pytest.raises(InvocationCancelledError):
            invocation.result(timeout=2)
        time.sleep(1.2)
        assert str(invocation.status) == "CANCELLED"
    finally:
        app.stop()
