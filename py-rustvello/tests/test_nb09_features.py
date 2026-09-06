"""Tests for NB_09 features: backend selection, runner, extended config, triggers."""

import pytest

from rustvello import App
from rustvello.app import ForeignTaskHandle, TaskHandle, TaskLanguage, _TriggerBuilder, _TriggerDef
from rustvello.backends import _BACKEND_NAMES, create_backends

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sync_app() -> App:
    """App with dev_mode_force_sync=True — in-memory backend."""
    return App(app_id="test_nb09", dev_mode_force_sync=True)


@pytest.fixture()
def sqlite_sync_app(tmp_path) -> App:
    """App with SQLite backend and sync mode."""
    db_path = str(tmp_path / "test.db")
    return App(
        app_id="test_nb09_sqlite",
        dev_mode_force_sync=True,
        backend="sqlite",
        db_path=db_path,
    )


# ---------------------------------------------------------------------------
# A1. Backend Selection
# ---------------------------------------------------------------------------


class TestBackendSelection:
    def test_default_is_memory(self) -> None:
        app = App(app_id="mem_test", dev_mode_force_sync=True)
        assert app.backend == "memory"

    def test_sqlite_backend(self, tmp_path) -> None:
        db_path = str(tmp_path / "test.db")
        app = App(
            app_id="sqlite_test",
            dev_mode_force_sync=True,
            backend="sqlite",
            db_path=db_path,
        )
        assert app.backend == "sqlite"

    def test_sqlite_submit_and_result(self, sqlite_sync_app: App) -> None:
        @sqlite_sync_app.task
        def double(x: int) -> int:
            return x * 2

        inv = double(21)
        assert inv.result(timeout=5) == 42

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend"):
            App(
                app_id="bad_test",
                dev_mode_force_sync=True,
                backend="nonexistent",
            )

    def test_backend_case_insensitive(self, tmp_path) -> None:
        db_path = str(tmp_path / "test.db")
        app = App(
            app_id="case_test",
            dev_mode_force_sync=True,
            backend="SQLite",
            db_path=db_path,
        )
        assert app.backend == "sqlite"


class TestCreateBackends:
    def test_known_backend_names(self) -> None:
        assert "memory" in _BACKEND_NAMES
        assert "sqlite" in _BACKEND_NAMES
        assert "redis" in _BACKEND_NAMES
        assert "postgres" in _BACKEND_NAMES
        assert "mongo" in _BACKEND_NAMES

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend"):
            create_backends("foobar", "test_app")

    def test_sqlite_returns_all_components(self, tmp_path) -> None:
        db_path = str(tmp_path / "factories.db")
        result = create_backends("sqlite", "test_app", db_path=db_path)
        assert "orchestrator" in result
        assert "state_backend" in result
        assert "broker" in result
        assert "trigger" in result
        assert "client_data_store" in result


# ---------------------------------------------------------------------------
# A2. Runner
# ---------------------------------------------------------------------------


class TestRunner:
    def test_build_runner_memory(self, sync_app: App) -> None:
        """Building a runner from a memory-backed app doesn't raise."""

        @sync_app.task
        def noop() -> str:
            return "ok"

        runner = sync_app._build_runner(num_workers=1, idle_sleep_ms=10)
        assert runner is not None

    def test_build_runner_sqlite(self, sqlite_sync_app: App) -> None:
        @sqlite_sync_app.task
        def noop() -> str:
            return "ok"

        runner = sqlite_sync_app._build_runner(num_workers=1, idle_sleep_ms=10)
        assert runner is not None

    def test_stop_without_run_is_safe(self, sync_app: App) -> None:
        """Calling stop() when no runner is active should not raise."""
        sync_app.stop()


# ---------------------------------------------------------------------------
# A3. Extended Task Config
# ---------------------------------------------------------------------------


class TestExtendedTaskConfig:
    def test_concurrency_param(self, sync_app: App) -> None:
        @sync_app.task(concurrency="task")
        def single_at_a_time(x: int) -> int:
            return x

        assert isinstance(single_at_a_time, TaskHandle)
        key = f"{single_at_a_time._language}::{single_at_a_time._module}.{single_at_a_time._name}"
        assert sync_app._task_configs[key]["concurrency"] == "task"

    def test_key_arguments(self, sync_app: App) -> None:
        @sync_app.task(concurrency="keys", key_arguments=["user_id"])
        def per_user(user_id: str, data: str) -> str:
            return f"{user_id}:{data}"

        key = f"{per_user._language}::{per_user._module}.{per_user._name}"
        assert sync_app._task_configs[key]["key_arguments"] == ["user_id"]
        assert sync_app._task_configs[key]["concurrency"] == "keys"

    def test_blocking_flag(self, sync_app: App) -> None:
        @sync_app.task(blocking=True)
        def blocker(x: int) -> int:
            return x

        key = f"{blocker._language}::{blocker._module}.{blocker._name}"
        assert sync_app._task_configs[key]["blocking"] is True

    def test_parallel_batch_size(self, sync_app: App) -> None:
        @sync_app.task(parallel_batch_size=50)
        def batchy(x: int) -> int:
            return x

        key = f"{batchy._language}::{batchy._module}.{batchy._name}"
        assert sync_app._task_configs[key]["parallel_batch_size"] == 50

    def test_reroute_on_cc(self, sync_app: App) -> None:
        @sync_app.task(reroute_on_cc=True)
        def rerouter(x: int) -> int:
            return x

        key = f"{rerouter._language}::{rerouter._module}.{rerouter._name}"
        assert sync_app._task_configs[key]["reroute_on_cc"] is True

    def test_default_config_values(self, sync_app: App) -> None:
        @sync_app.task
        def defaults(x: int) -> int:
            return x

        key = f"{defaults._language}::{defaults._module}.{defaults._name}"
        cfg = sync_app._task_configs[key]
        assert cfg["concurrency"] == "unlimited"
        assert cfg["key_arguments"] == []
        assert cfg["blocking"] is False
        assert cfg["parallel_batch_size"] == 100
        assert cfg["reroute_on_cc"] is False

    def test_extended_config_tasks_still_work(self, sync_app: App) -> None:
        """Tasks with extended config params still execute correctly."""

        @sync_app.task(concurrency="task", reroute_on_cc=True, blocking=True)
        def fancy(x: int) -> int:
            return x * 3

        inv = fancy(7)
        assert inv.result(timeout=5) == 21


# ---------------------------------------------------------------------------
# A4. Trigger Builder
# ---------------------------------------------------------------------------


class TestTriggerBuilder:
    def test_trigger_returns_builder(self, sync_app: App) -> None:
        @sync_app.task
        def cleanup() -> str:
            return "done"

        builder = sync_app.trigger(cleanup)
        assert isinstance(builder, _TriggerBuilder)

    def test_on_cron_register(self, sync_app: App) -> None:
        @sync_app.task
        def cleanup() -> str:
            return "done"

        tdef = sync_app.trigger(cleanup).on_cron("0 */5 * * * *").register()
        assert isinstance(tdef, _TriggerDef)
        assert tdef.kind == "cron"
        assert tdef.schedule == "0 */5 * * * *"
        assert tdef.task_key == f"python::{cleanup._module}.{cleanup._name}"
        assert len(sync_app._triggers) == 1

    def test_on_interval_register(self, sync_app: App) -> None:
        @sync_app.task
        def heartbeat() -> str:
            return "ping"

        tdef = sync_app.trigger(heartbeat).on_interval(30).register()
        assert tdef.kind == "interval"
        assert tdef.schedule == "30"
        assert tdef.task_key == f"python::{heartbeat._module}.{heartbeat._name}"

    def test_with_args(self, sync_app: App) -> None:
        @sync_app.task
        def process(region: str) -> str:
            return region

        tdef = sync_app.trigger(process).on_cron("0 0 * * * *").with_args(region="us-east").register()
        assert tdef.kwargs == {"region": "us-east"}

    def test_foreign_task_decorator_registers_typed_proxy(self, sync_app: App) -> None:
        @sync_app.foreign_task(TaskLanguage.Rust, module="rust_side")
        def rust_reverse(text: str) -> str:
            raise NotImplementedError

        assert isinstance(rust_reverse, ForeignTaskHandle)
        assert rust_reverse._language == "rust"
        assert rust_reverse._module == "rust_side"
        assert rust_reverse._name == "rust_reverse"

    def test_register_without_type_raises(self, sync_app: App) -> None:
        @sync_app.task
        def noop() -> str:
            return ""

        with pytest.raises(ValueError, match="Must specify a trigger type"):
            sync_app.trigger(noop).register()

    def test_multiple_triggers(self, sync_app: App) -> None:
        @sync_app.task
        def t1() -> str:
            return "a"

        @sync_app.task
        def t2() -> str:
            return "b"

        sync_app.trigger(t1).on_cron("0 0 * * * *").register()
        sync_app.trigger(t2).on_interval(60).register()
        assert len(sync_app._triggers) == 2


# ---------------------------------------------------------------------------
# Introspection
# ---------------------------------------------------------------------------


class TestIntrospection:
    def test_tasks_property(self, sync_app: App) -> None:
        @sync_app.task
        def one() -> int:
            return 1

        @sync_app.task
        def two() -> int:
            return 2

        tasks = sync_app.tasks
        assert len(tasks) == 2
        # Returns a copy, not the internal dict
        tasks["extra"] = None  # type: ignore
        assert len(sync_app.tasks) == 2

    def test_backend_property(self, sync_app: App) -> None:
        assert sync_app.backend == "memory"
