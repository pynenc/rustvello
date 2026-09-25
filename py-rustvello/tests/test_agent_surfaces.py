"""Surfaces the agent skill relies on: persisted triggers, the monitor on port 0, investigation errors."""

import json
import time
import urllib.request

import pytest

from rustvello import App, TaskLanguage


def _conditions(app: App) -> dict:
    store = app._backend_objects["trigger"]
    return {cid: json.loads(raw) for cid, raw in store.get_all_conditions()}


class TestTriggerRegistration:
    def test_register_stores_condition_and_trigger(self) -> None:
        app = App(app_id="triggers")

        @app.task
        def cleanup(region: str) -> str:
            return region

        tdef = app.trigger(cleanup).on_cron("*/5 * * * *").with_args(region="eu").register()
        store = app._backend_objects["trigger"]
        condition = _conditions(app)[tdef.condition_id]["Cron"]
        assert condition == {"cron_expression": "*/5 * * * *", "min_interval_seconds": 50}
        [trigger] = [json.loads(raw) for raw in store.get_triggers_for_condition(tdef.condition_id)]
        assert trigger["task_id"]["name"] == "cleanup"
        assert trigger["argument_template"] == {"region": "eu"}

    def test_registering_twice_is_a_no_op(self) -> None:
        app = App(app_id="triggers-twice")

        @app.task
        def tick() -> None:
            return None

        first = app.trigger(tick).on_cron("0 * * * * *").register()
        second = app.trigger(tick).on_cron("0 * * * * *").register()
        assert first.condition_id == second.condition_id
        assert len(app._backend_objects["trigger"].get_triggers_for_condition(first.condition_id)) == 1

    def test_six_field_cron_and_interval_have_no_minute_floor(self) -> None:
        app = App(app_id="triggers-fields")

        @app.task
        def tick() -> None:
            return None

        seconds = app.trigger(tick).on_cron("*/2 * * * * *").register()
        interval = app.trigger(tick).on_interval(2.5).register()
        conditions = _conditions(app)
        assert conditions[seconds.condition_id]["Cron"]["min_interval_seconds"] == 0
        assert conditions[interval.condition_id]["Cron"] == {
            "cron_expression": "* * * * * *",
            "min_interval_seconds": 3,
        }

    def test_argument_named_kind_is_passed_through(self) -> None:
        app = App(app_id="triggers-kind")

        @app.task
        def report(kind: str) -> str:
            return kind

        tdef = app.trigger(report).on_cron("0 3 * * *").with_args(kind="daily").register()
        assert tdef.kind == "cron"
        assert tdef.kwargs == {"kind": "daily"}

    def test_invalid_cron_is_rejected_at_registration(self) -> None:
        app = App(app_id="triggers-invalid")

        @app.task
        def tick() -> None:
            return None

        with pytest.raises(ValueError, match="invalid cron expression"):
            app.trigger(tick).on_cron("every five minutes").register()
        with pytest.raises(ValueError, match="at least 1 second"):
            app.trigger(tick).on_interval(0.5)

    def test_foreign_task_triggers_are_refused(self) -> None:
        app = App(app_id="triggers-foreign")

        @app.foreign_task(TaskLanguage.Rust, module="rust_side")
        def reverse(text: str) -> str:
            raise NotImplementedError

        with pytest.raises(ValueError, match="register it in the runtime"):
            app.trigger(reverse).on_cron("* * * * *").register()

    def test_cron_trigger_fires_on_a_sqlite_worker(self, tmp_path) -> None:
        app = App(app_id="triggers-fire", backend="sqlite", db_path=str(tmp_path / "t.db"))
        marker = tmp_path / "fired"

        @app.task
        def mark(name: str) -> None:
            (marker.parent / f"{name}-{time.time_ns()}").write_text(name)

        app.trigger(mark).on_cron("* * * * * *").with_args(name="fired").register()
        app.run(block=False)
        try:
            deadline = time.monotonic() + 30
            while not list(tmp_path.glob("fired-*")):
                assert time.monotonic() < deadline, "the trigger never fired"
                time.sleep(0.2)
        finally:
            app.stop()


class TestMonitorForAgents:
    def test_port_zero_reports_the_bound_port_and_serves_at_once(self) -> None:
        server = App(app_id="monitor-port").start_monitor(port=0, log_level="warn")
        try:
            host, port = server.address.rsplit(":", 1)
            assert host == "127.0.0.1"
            assert int(port) > 0
            with urllib.request.urlopen(f"http://{server.address}/api/capabilities", timeout=10) as response:
                assert json.loads(response.read())["app_id"] == "monitor-port"
        finally:
            server.stop()

    def test_bind_error_raises(self) -> None:
        first = App(app_id="monitor-a").start_monitor(port=0, log_level="warn")
        try:
            port = int(first.address.rsplit(":", 1)[1])
            with pytest.raises(OSError, match="cannot bind"):
                App(app_id="monitor-b").start_monitor(port=port, log_level="warn")
        finally:
            first.stop()

    def test_investigation_reports_status_and_error(self, tmp_path) -> None:
        app = App(app_id="investigate", backend="sqlite", db_path=str(tmp_path / "i.db"))

        @app.task
        def charge(order_id: str) -> str:
            raise PermissionError(f"declined {order_id}")

        app.run(block=False)
        try:
            invocation = charge("o-1")
            with pytest.raises(RuntimeError) as failure:
                invocation.result(timeout=30)
        finally:
            app.stop()
        # the stored message already names the type: it is not prefixed twice
        assert str(failure.value) == "Task failed: PermissionError: declined o-1"

        server = app.start_monitor(port=0, log_level="warn")
        try:
            url = f"http://{server.address}/invocations/{invocation.id}/investigation"
            with urllib.request.urlopen(url, timeout=10) as response:
                report = json.loads(response.read())
        finally:
            server.stop()
        assert report["invocation"]["status"] == "Failed"
        assert report["error"]["error_type"] == "PermissionError"
        assert "declined o-1" in report["error"]["message"]
