"""The `python -m rustvello.worker` launcher and the child protocol."""

import io
import json

import pytest

from rustvello import App
from rustvello import worker as worker_module


@pytest.fixture()
def sync_app() -> App:
    return App(app_id="worker_cli_test", dev_mode_force_sync=True)


class TestParser:
    def test_runner_arguments(self) -> None:
        args = worker_module.build_parser().parse_args(
            ["pkg.mod:app", "--processes", "3", "--queues", "hpa", "hyper", "--loglevel", "debug"]
        )
        assert args.app == "pkg.mod:app"
        assert args.processes == 3
        assert args.queues == ["hpa", "hyper"]
        assert args.loglevel == "debug"
        assert args.child is False

    def test_child_arguments(self) -> None:
        args = worker_module.build_parser().parse_args(["--child", "--app", "pkg.mod:app"])
        assert args.child is True
        assert args.child_app == "pkg.mod:app"

    def test_app_path_required(self) -> None:
        with pytest.raises(SystemExit):
            worker_module.main([])


class TestLoadApp:
    def test_rejects_non_app_attribute(self) -> None:
        with pytest.raises(TypeError):
            worker_module.load_app("json:dumps")

    def test_rejects_malformed_path(self) -> None:
        with pytest.raises(ValueError):
            worker_module.load_app("json")


class TestProtocol:
    def _request(self, module: str, name: str, **args: object) -> dict:
        return {
            "protocol": 1,
            "invocation_id": "inv-1",
            "task_id": f"python::{module}.{name}",
            "language": "python",
            "module": module,
            "name": name,
            "args": {key: json.dumps(value) for key, value in args.items()},
            "num_retries": 2,
            "parent_invocation_id": None,
            "traceparent": None,
            "tracestate": None,
            "workflow": None,
            "is_workflow_defining": False,
        }

    def test_success_round_trip(self, sync_app: App) -> None:
        seen = {}

        @sync_app.task
        def add(x: int, y: int) -> int:
            seen["current"] = sync_app.current_invocation()
            return x + y

        response = worker_module.execute_request(sync_app, self._request(add._module, add._name, x=2, y=3))
        assert response == {"ok": True, "result": "5"}
        assert seen["current"].invocation_id == "inv-1"
        assert seen["current"].num_retries == 2
        assert sync_app.current_invocation() is None

    def test_task_error_response(self, sync_app: App) -> None:
        @sync_app.task
        def fail(message: str) -> None:
            raise KeyError(message)

        response = worker_module.execute_request(sync_app, self._request(fail._module, fail._name, message="k"))
        assert response["ok"] is False
        assert response["error_type"] == "KeyError"
        assert "k" in response["message"]
        assert "KeyError" in response["traceback"]

    def test_unknown_task_response(self, sync_app: App) -> None:
        response = worker_module.execute_request(sync_app, self._request("nowhere", "missing"))
        assert response["ok"] is False
        assert response["error_type"] == "KeyError"

    def test_protocol_mismatch(self, sync_app: App) -> None:
        response = worker_module.execute_request(sync_app, {"protocol": 99})
        assert response["ok"] is False
        assert response["error_type"] == "ValueError"

    def test_serve_prints_ready_then_answers(self, sync_app: App) -> None:
        @sync_app.task
        def echo(value: str) -> str:
            return value

        requests = io.StringIO(json.dumps(self._request(echo._module, echo._name, value="hi")) + "\n\nnot json\n")
        responses = io.StringIO()
        worker_module.serve(sync_app, requests, responses)
        lines = [json.loads(line) for line in responses.getvalue().splitlines()]
        assert lines[0]["ready"] is True
        assert lines[1] == {"ok": True, "result": '"hi"'}
        assert lines[2]["ok"] is False and lines[2]["error_type"] == "JSONDecodeError"
