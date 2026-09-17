"""Operational surface of the standalone App: config from env, queues, purge, lookup, monitor."""

import socket
import time
import urllib.request

import pytest

from rustvello import App, AppConfig, CurrentInvocation, TaskConfig


class TestConfigFromEnvironment:
    def test_from_env_reads_rustvello_variables(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RUSTVELLO__APP_ID", "from_env")
        monkeypatch.setenv("RUSTVELLO__RUNNER_QUEUES", "hpa,hyper")
        monkeypatch.setenv("RUSTVELLO__BROKER_QUEUES", "default,hpa,hyper")
        config = AppConfig.from_env()
        assert config.app_id == "from_env"
        assert config.runner_queues == ["hpa", "hyper"]
        assert config.broker_queues == ["default", "hpa", "hyper"]

    def test_from_env_app_id_override(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RUSTVELLO__APP_ID", "ignored")
        assert AppConfig.from_env(app_id="explicit").app_id == "explicit"

    def test_from_file(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        path = tmp_path / "rustvello.toml"
        path.write_text('app_id = "from_file"\nrunner_queues = ["hyper"]\nlogging_level = "debug"\n')
        config = AppConfig.from_file(str(path))
        assert config.app_id == "from_file"
        assert config.runner_queues == ["hyper"]
        assert config.logging_level == "debug"

    def test_env_switch_reaches_an_app_built_with_an_explicit_config(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path
    ) -> None:
        """A suite sets RUSTVELLO__DEV_MODE_FORCE_SYNC and every app runs inline, code untouched."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RUSTVELLO__DEV_MODE_FORCE_SYNC", "true")
        config = AppConfig.from_env(app_id="env_sync")
        assert config.dev_mode_force_sync is True
        assert App(app_id="env_sync", config=config).dev_mode_force_sync is True
        assert App(app_id="env_sync").dev_mode_force_sync is True

    def test_explicit_argument_still_wins_over_the_environment(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RUSTVELLO__DEV_MODE_FORCE_SYNC", "true")
        assert App(app_id="env_sync_off", dev_mode_force_sync=False).dev_mode_force_sync is False

    def test_setters(self) -> None:
        config = AppConfig(app_id="x")
        config.broker_queues = ["default", "hpa"]
        config.runner_queues = ["hpa"]
        config.dev_mode_force_sync = True
        config.logging_level = "warning"
        assert config.broker_queues == ["default", "hpa"]
        assert config.runner_queues == ["hpa"]
        assert config.dev_mode_force_sync is True
        assert config.logging_level == "warning"

    def test_app_picks_env_defaults(self, monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("RUSTVELLO__RUNNER_QUEUES", "hyper")
        monkeypatch.setenv("RUSTVELLO__BROKER_QUEUES", "default,hyper")
        app = App(app_id="env_app")
        assert app._config.app_id == "env_app"
        assert app._config.runner_queues == ["hyper"]

    def test_retry_for_errors_in_task_config(self) -> None:
        config = TaskConfig(max_retries=2, retry_for_errors=["ValueError"])
        assert config.retry_for_errors == ["ValueError"]


class TestTaskRegistration:
    def test_retry_for_records_class_names(self) -> None:
        app = App(app_id="retry_for_test", dev_mode_force_sync=True)

        @app.task(max_retries=2, retry_for=(ValueError, KeyError))
        def flaky(x: int) -> int:
            return x

        key = f"python::{flaky._module}.{flaky._name}"
        assert app._task_configs[key]["retry_for_errors"] == ["ValueError", "KeyError"]
        app._build_runner()  # registration with retry_for_errors must be accepted by the runner builder

    def test_replace_reregisters_a_task(self) -> None:
        app = App(app_id="replace_test", dev_mode_force_sync=True)

        @app.task
        def twice(x: int) -> int:
            return x

        with pytest.raises(Exception, match="already registered"):

            @app.task
            def twice(x: int) -> int:  # noqa: F811 - duplicate definition on purpose
                return x + 1

        @app.task(replace=True)
        def twice(x: int) -> int:  # noqa: F811
            return x + 2

        assert app.get_task(f"{twice._module}.twice") is twice
        assert twice(1).result(timeout=1) == 3

    def test_get_task_with_and_without_language(self) -> None:
        app = App(app_id="get_task_test", dev_mode_force_sync=True)

        @app.task
        def lookup(x: int) -> int:
            return x

        assert app.get_task(f"{lookup._module}.lookup") is lookup
        assert app.get_task(f"python::{lookup._module}.lookup") is lookup
        with pytest.raises(KeyError):
            app.get_task("missing.task")


class TestQueuesAndPurge:
    def test_queue_depth_and_purge(self) -> None:
        app = App(
            app_id="queue_depth_test", config=AppConfig(app_id="queue_depth_test", broker_queues=["default", "hpa"])
        )

        @app.task(queue="hpa")
        def queued(x: int) -> int:
            return x

        for value in range(3):
            queued(value)
        assert app.queue_depth("hpa") == 3
        assert app.queue_depth("default") == 0
        assert app.queue_depths() == {"default": 0, "hpa": 3}
        app.purge()
        assert app.queue_depth("hpa") == 0


class TestDevModeToggle:
    def test_toggle_runs_inline(self) -> None:
        app = App(app_id="dev_mode_toggle_test")

        @app.task
        def double(x: int) -> int:
            return x * 2

        assert app.dev_mode_force_sync is False
        app.dev_mode_force_sync = True
        assert app._config.dev_mode_force_sync is True
        assert double(4).result(timeout=1) == 8

    def test_wait_results_in_order(self) -> None:
        app = App(app_id="wait_results_test", dev_mode_force_sync=True)

        @app.task
        def identity(x: int) -> int:
            return x

        assert app.wait_results([identity(3), identity(1), identity(2)]) == [3, 1, 2]

    def test_current_invocation_outside_task(self) -> None:
        app = App(app_id="no_current_test")
        assert app.current_invocation() is None
        assert CurrentInvocation("i", "k", 0, {}).arguments == {}


class TestMonitor:
    def test_start_and_stop(self) -> None:
        app = App(app_id="monitor_test")

        @app.task
        def monitored(x: int) -> int:
            return x

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = app.start_monitor(port=port)
        try:
            assert server.is_running()
            assert server.address.endswith(f":{port}")
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as response:
                        status = response.status
                    break
                except OSError:
                    time.sleep(0.1)
            assert status == 200
        finally:
            server.stop()
        assert not server.is_running()
