"""``App.send_task``: submit by name without the task's code, like Celery's ``send_task``."""

import time

import pytest

from rustvello import App, AppConfig, InvocationStatus


def _wait_terminal(invocation, timeout: float = 10.0) -> InvocationStatus:
    deadline = time.time() + timeout
    status = invocation.status
    while time.time() < deadline and not status.is_terminal():
        time.sleep(0.05)
        status = invocation.status
    return status


class TestSendTask:
    def test_registered_task_uses_its_handle(self) -> None:
        app = App(app_id="send_task_registered")

        @app.task
        def add(x: int, y: int) -> int:
            return x + y

        app.run(num_workers=1, block=False)
        try:
            inv = app.send_task(f"{add._module}.add", {"x": 2, "y": 3})
            assert inv.result(timeout=10) == 5
        finally:
            app.stop()

    def test_unknown_name_is_queued_as_a_proxy(self) -> None:
        app = App(
            app_id="send_task_proxy",
            config=AppConfig(app_id="send_task_proxy", broker_queues=["default", "hpa"]),
        )
        inv = app.send_task("jobs.calc.compute", {"n": 1}, queue="hpa", priority=2.5)
        assert app.queue_depth("hpa") == 1
        assert app.queue_depth("default") == 0
        assert not inv.status.is_terminal()
        assert app._engine.has_task("python", "jobs.calc", "compute")
        # A second submission reuses the proxy.
        app.send_task("jobs.calc.compute", {"n": 2})
        assert app.queue_depth("hpa") == 2
        # Language prefix form is accepted too.
        app.send_task("python::jobs.calc.compute", {"n": 3})
        assert app.queue_depth("hpa") == 3

    def test_unknown_task_dequeued_by_a_worker_fails_loudly(self) -> None:
        app = App(app_id="send_task_proxy_worker")
        inv = app.send_task("jobs.missing.task", {"n": 1})
        app.run(num_workers=1, block=False)
        try:
            status = _wait_terminal(inv)
            assert status == InvocationStatus.failed(), status
            with pytest.raises(RuntimeError, match="not registered in this runner"):
                inv.result(timeout=5)
        finally:
            app.stop()

    def test_bad_name_and_dev_mode_raise(self) -> None:
        app = App(app_id="send_task_bad", dev_mode_force_sync=True)
        with pytest.raises(ValueError, match="package.module.task"):
            app.send_task("nodots", {})
        with pytest.raises(ValueError, match="synchronous dev mode"):
            app.send_task("jobs.calc.compute", {})

    def test_get_task_error_points_to_send_task(self) -> None:
        app = App(app_id="send_task_get_task")
        with pytest.raises(KeyError, match="send_task"):
            app.get_task("jobs.calc.compute")
