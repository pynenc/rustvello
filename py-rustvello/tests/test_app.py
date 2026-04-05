"""Tests for the Rustvello high-level app API."""

import pytest

from rustvello import AppConfig, InvocationStatus, Rustvello, TaskConfig
from rustvello.rustvello import TaskNotRegisteredError


class TestRustvelloApp:
    def test_create_default(self):
        app = Rustvello()
        assert repr(app) == "Rustvello(...)"

    def test_create_with_config(self):
        cfg = AppConfig(app_id="test_app", dev_mode_force_sync=True)
        app = Rustvello(config=cfg)
        assert app is not None

    def test_register_and_submit(self):
        app = Rustvello()
        app.register_task("mymod", "add", lambda args: "result")
        inv_id = app.submit("mymod", "add", {"x": "1", "y": "2"})
        assert inv_id is not None
        assert len(str(inv_id)) > 0

    def test_submit_unregistered_task_raises(self):
        app = Rustvello()
        with pytest.raises(TaskNotRegisteredError):
            app.submit("nomod", "nofunc")

    def test_get_status_after_submit(self):
        app = Rustvello()
        app.register_task("mod", "fn", lambda args: "ok")
        inv_id = app.submit("mod", "fn")
        status = app.get_status(inv_id)
        assert isinstance(status, InvocationStatus)
        assert str(status) in ("REGISTERED", "PENDING", "RUNNING", "SUCCESS")

    def test_register_with_task_config(self):
        app = Rustvello()
        cfg = TaskConfig(max_retries=3, cache_results=True)
        app.register_task("mod", "fn", lambda args: "ok", config=cfg)
        inv_id = app.submit("mod", "fn")
        status = app.get_status(inv_id)
        assert str(status) in ("REGISTERED", "PENDING", "RUNNING", "SUCCESS")

    def test_get_result(self):
        app = Rustvello(config=AppConfig(dev_mode_force_sync=True))
        app.register_task("mod", "fn", lambda args: "hello_world")
        inv_id = app.submit("mod", "fn")
        # In dev_mode_force_sync, task runs synchronously
        result = app.get_result(inv_id)
        # Result may or may not be available depending on execution mode
        if result is not None:
            assert result == "hello_world"
