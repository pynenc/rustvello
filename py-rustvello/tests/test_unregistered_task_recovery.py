"""Client report: a task only the worker module registers must end FAILED, never stay RUNNING."""

import time

import pytest

from rustvello import App, InvocationStatus


@pytest.fixture
def child_only_module(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    """``import_path`` module that registers task ``t``; the runner process does not."""
    db_path = tmp_path / "unregistered.db"
    monkeypatch.setenv("RUSTVELLO_TEST_UNREGISTERED_DB", str(db_path))
    (tmp_path / "child_only_app.py").write_text(
        "import os\nfrom rustvello import App\n"
        'app = App(app_id="unregistered_task", backend="sqlite", '
        'db_path=os.environ["RUSTVELLO_TEST_UNREGISTERED_DB"], import_path="child_only_app:app")\n'
        "@app.task\ndef t(x: int) -> int:\n    return x\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    return "child_only_app:app"


def test_task_unknown_to_the_runner_fails_instead_of_staying_running(child_only_module: str, tmp_path) -> None:
    app = App(app_id="unregistered_task", backend="sqlite", db_path=str(tmp_path / "unregistered.db"))
    inv = app.send_task("child_only_app.t", {"x": 1})
    app.run(num_processes=1, block=False, import_path=child_only_module)
    try:
        deadline = time.time() + 20
        status = inv.status
        while time.time() < deadline and not status.is_terminal():
            time.sleep(0.1)
            status = inv.status
        assert status == InvocationStatus.failed(), status
        with pytest.raises(RuntimeError, match="TaskNotRegistered|not registered in this runner"):
            inv.result(timeout=5)
    finally:
        app.stop()


def test_max_invocation_recoveries_is_configurable() -> None:
    from rustvello import AppConfig

    config = AppConfig(app_id="recoveries")
    assert config.max_invocation_recoveries == 3
    config.max_invocation_recoveries = 0
    assert config.max_invocation_recoveries == 0
