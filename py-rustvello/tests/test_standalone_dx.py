"""Tests for the standalone DX layer: App, TaskHandle, Invocation."""

import pytest

from rustvello import App, InvocationStatus

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sync_app() -> App:
    """App with dev_mode_force_sync=True — tasks run in the calling thread."""
    return App(app_id="test_standalone_dx", dev_mode_force_sync=True)


# ---------------------------------------------------------------------------
# 1. Decorator forms
# ---------------------------------------------------------------------------


class TestDecorator:
    def test_bare_decorator(self, sync_app: App) -> None:
        """@app.task without parentheses returns a TaskHandle."""
        from rustvello.app import TaskHandle

        @sync_app.task
        def bare(x: int) -> int:
            return x * 2

        assert isinstance(bare, TaskHandle)
        assert bare.__name__ == "bare"

    def test_decorator_with_args(self, sync_app: App) -> None:
        """@app.task(max_retries=3) returns a TaskHandle."""
        from rustvello.app import TaskHandle

        @sync_app.task(max_retries=3, cache_results=True)
        def resilient(x: int) -> int:
            return x + 1

        assert isinstance(resilient, TaskHandle)
        assert resilient.__name__ == "resilient"

    def test_decorator_running_concurrency(self, sync_app: App) -> None:
        """@app.task(running_concurrency=1) registers without error."""
        from rustvello.app import TaskHandle

        @sync_app.task(running_concurrency=1)
        def serial(x: int) -> int:
            return x

        assert isinstance(serial, TaskHandle)


# ---------------------------------------------------------------------------
# 2. Submit and result
# ---------------------------------------------------------------------------


class TestSubmitAndResult:
    def test_call_returns_invocation(self, sync_app: App) -> None:
        from rustvello.app import Invocation

        @sync_app.task
        def add(x: int, y: int) -> int:
            return x + y

        inv = add(1, 2)
        assert isinstance(inv, Invocation)

    def test_result_returns_value(self, sync_app: App) -> None:
        @sync_app.task
        def add(x: int, y: int) -> int:
            return x + y

        result = add(1, 2).result(timeout=5)
        assert result == 3

    def test_positional_args_match_keyword(self, sync_app: App) -> None:
        @sync_app.task
        def add(x: int, y: int) -> int:
            return x + y

        assert add(10, 20).result(timeout=5) == add(x=10, y=20).result(timeout=5)

    def test_explicit_submit(self, sync_app: App) -> None:
        @sync_app.task
        def mul(a: int, b: int) -> int:
            return a * b

        result = mul.submit(a=3, b=4).result(timeout=5)
        assert result == 12

    def test_no_arg_task(self, sync_app: App) -> None:
        @sync_app.task
        def noop() -> None:
            pass

        result = noop().result(timeout=5)
        assert result is None

    def test_default_arg_applied(self, sync_app: App) -> None:
        @sync_app.task
        def greet(name: str = "world") -> str:
            return f"hello {name}"

        result = greet().result(timeout=5)
        assert result == "hello world"


# ---------------------------------------------------------------------------
# 3. Status check
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_is_invocation_status(self, sync_app: App) -> None:
        @sync_app.task
        def value(x: int) -> int:
            return x

        inv = value(42)
        # In sync mode the task completes immediately; status should be terminal.
        status = inv.status
        assert isinstance(status, InvocationStatus)

    def test_status_terminal_after_result(self, sync_app: App) -> None:
        @sync_app.task
        def value(x: int) -> int:
            return x

        inv = value(7)
        _ = inv.result(timeout=5)
        assert inv.status.is_terminal()


# ---------------------------------------------------------------------------
# 4. JSON round-trip
# ---------------------------------------------------------------------------


class TestJsonRoundTrip:
    def test_int(self, sync_app: App) -> None:
        @sync_app.task
        def echo_int(v: int) -> int:
            return v

        assert echo_int(42).result(timeout=5) == 42

    def test_float(self, sync_app: App) -> None:
        @sync_app.task
        def echo_float(v: float) -> float:
            return v

        result = echo_float(3.14).result(timeout=5)
        assert abs(result - 3.14) < 1e-9

    def test_string(self, sync_app: App) -> None:
        @sync_app.task
        def echo_str(v: str) -> str:
            return v

        assert echo_str("hello").result(timeout=5) == "hello"

    def test_bool(self, sync_app: App) -> None:
        @sync_app.task
        def echo_bool(v: bool) -> bool:
            return v

        assert echo_bool(True).result(timeout=5) is True
        assert echo_bool(False).result(timeout=5) is False

    def test_list(self, sync_app: App) -> None:
        @sync_app.task
        def echo_list(v: list) -> list:
            return v

        assert echo_list([1, 2, 3]).result(timeout=5) == [1, 2, 3]

    def test_dict(self, sync_app: App) -> None:
        @sync_app.task
        def echo_dict(v: dict) -> dict:
            return v

        assert echo_dict({"a": 1}).result(timeout=5) == {"a": 1}

    def test_none_value(self, sync_app: App) -> None:
        @sync_app.task
        def echo_none(v: None) -> None:
            return v

        assert echo_none(None).result(timeout=5) is None


# ---------------------------------------------------------------------------
# 5. App construction
# ---------------------------------------------------------------------------


class TestAppConstruction:
    def test_default_app_id(self) -> None:
        app = App()
        assert app._engine is not None

    def test_custom_app_id(self) -> None:
        app = App(app_id="my_app")
        assert app._engine is not None

    def test_sync_mode_flag(self) -> None:
        app = App(dev_mode_force_sync=True)
        assert app._engine is not None
