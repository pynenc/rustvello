"""Operational surfaces for scripts and probes: ``App.health()``, the monitor's options, the doctor and monitor launchers."""

import json
import os
import signal
import threading
import urllib.error
import urllib.request

import pytest

from rustvello import App, MonitorServer
from rustvello import doctor as doctor_cli
from rustvello import monitor as monitor_cli
from rustvello.rustvello import RustMongo3Pool, RustMongoPool


def _get(url: str, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read().decode(errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def _post(url: str, origin: str) -> tuple[int, str]:
    request = urllib.request.Request(url, data=b"", method="POST", headers={"Origin": origin})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode(errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


class TestHealth:
    def test_health_reports_components_and_runners(self) -> None:
        app = App(app_id="health_app")
        report = app.health()
        assert report["status"] == "ok"
        assert report["app_id"] == "health_app"
        for component in ("broker", "orchestrator", "state_backend", "client_data_store", "trigger_store"):
            assert report["components"][component]["status"] == "ok", component
        assert report["components"]["state_backend"]["implementation"] == "In-Memory"
        assert report["runners"]["active"] == 0
        assert any("no active runners" in problem for problem in report["problems"])
        assert report["queues"] == {"default": 0}

    def test_health_counts_a_running_worker(self) -> None:
        app = App(app_id="health_runner")
        app.run(num_workers=1, block=False)
        try:
            deadline = 10.0
            active = 0
            while deadline > 0 and active == 0:
                active = app.health()["runners"]["active"]
                if active == 0:
                    import time

                    time.sleep(0.1)
                    deadline -= 0.1
            assert active >= 1
        finally:
            app.stop()


class TestMonitorServer:
    def test_embedded_static_assets_and_json_health(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        # No source checkout in sight: the assets must come from the extension itself.
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("RUSTVELLO_MONITORING_STATIC_DIR", raising=False)
        app = App(app_id="monitor_assets")
        server = app.start_monitor(port=0)
        try:
            assert isinstance(server, MonitorServer)
            assert server.url == f"http://{server.address}"
            assert not server.read_only
            for path in ("/static/css/rustvello.css", "/static/js/monitoring.js", "/static/logo.png"):
                status, _ = _get(server.url + path)
                assert status == 200, path
            status, body = _get(server.url + "/health")
            assert status == 200
            report = json.loads(body)
            assert report["status"] == "ok"
            assert report["components"]["state_backend"]["implementation"] == "In-Memory"
            status, body = _get(server.url + "/api/runners")
            assert status == 200 and json.loads(body)["count"] == 0
            status, body = _get(server.url + "/api/config")
            assert status == 200 and json.loads(body)["config"]["app_id"] == "monitor_assets"
        finally:
            server.stop()

    def test_read_only_refuses_purge(self) -> None:
        app = App(app_id="monitor_read_only")
        server = app.start_monitor(port=0, read_only=True)
        try:
            assert server.read_only
            status, body = _post(server.url + "/broker/purge", origin=server.url)
            assert status == 403 and "read-only" in body
            status, body = _get(server.url + "/api/capabilities")
            assert json.loads(body)["read_only"] is True
            status, body = _get(server.url + "/")
            assert "Read-only monitor" in body and "Purge Broker" not in body
        finally:
            server.stop()

    def test_read_only_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RUSTVELLO_MONITORING_READ_ONLY", "1")
        app = App(app_id="monitor_read_only_env")
        server = app.start_monitor(port=0)
        try:
            assert server.read_only
        finally:
            server.stop()

    def test_block_returns_on_sigterm(self) -> None:
        app = App(app_id="monitor_block")
        holder: dict[str, MonitorServer] = {}

        def fire() -> None:
            os.kill(os.getpid(), signal.SIGTERM)

        timer = threading.Timer(0.5, fire)
        timer.start()
        try:
            holder["server"] = app.start_monitor(port=0, block=True)
        finally:
            timer.cancel()
        assert not holder["server"].is_running()
        # The previous SIGTERM disposition is restored.
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL


class TestMongoDatabaseName:
    def test_prefix_and_verbatim(self) -> None:
        assert RustMongo3Pool("mongodb://localhost:27017", "rustvello", "shop").database_name == "rustvello_shop"
        assert (
            RustMongo3Pool("mongodb://localhost:27017", "ops_tasks", "shop", verbatim=True).database_name == "ops_tasks"
        )
        assert RustMongoPool("mongodb://localhost:27017", "rustvello", "shop").database_name == "rustvello_shop"
        assert (
            RustMongoPool("mongodb://localhost:27017", "ops_tasks", "shop", verbatim=True).database_name == "ops_tasks"
        )


@pytest.fixture
def importable_app(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    (tmp_path / "probe_app.py").write_text('from rustvello import App\napp = App(app_id="probe_app")\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    return "probe_app:app"


class TestDoctor:
    def test_exit_codes(self) -> None:
        ok = {"status": "ok", "runners": {"active": 2}}
        assert doctor_cli.exit_code(ok) == doctor_cli.EXIT_OK
        assert doctor_cli.exit_code({"status": "ok", "runners": {"active": 0}}) == doctor_cli.EXIT_NO_RUNNERS
        assert doctor_cli.exit_code({"status": "ok", "runners": {"active": 0}}, allow_no_runners=True) == 0
        assert doctor_cli.exit_code({"status": "error", "runners": {"active": 3}}) == doctor_cli.EXIT_BACKEND_ERROR

    def test_main_prints_json_report(self, importable_app: str, capsys: pytest.CaptureFixture[str]) -> None:
        assert doctor_cli.main([importable_app, "--json"]) == doctor_cli.EXIT_NO_RUNNERS
        report = json.loads(capsys.readouterr().out)
        assert report["status"] == "ok" and report["app_id"] == "probe_app"
        assert doctor_cli.main([importable_app, "--allow-no-runners"]) == 0
        text = capsys.readouterr().out
        assert "state_backend" in text and "In-Memory" in text

    def test_main_reports_import_errors_as_problems(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert doctor_cli.main(["no_such_module:app", "--json"]) == doctor_cli.EXIT_BACKEND_ERROR
        report = json.loads(capsys.readouterr().out)
        assert report["status"] == "error" and report["problems"]


class TestMonitorLauncher:
    def test_main_serves_until_sigterm(self, importable_app: str) -> None:
        timer = threading.Timer(0.8, lambda: os.kill(os.getpid(), signal.SIGTERM))
        timer.start()
        try:
            assert monitor_cli.main([importable_app, "--port", "0", "--read-only"]) == 0
        finally:
            timer.cancel()

    def test_main_fails_on_bad_import(self) -> None:
        assert monitor_cli.main(["no_such_module:app", "--port", "0"]) == 1
