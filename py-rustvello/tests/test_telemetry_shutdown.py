"""Python binding lifetime and delivery-accounting tests against a local receiver."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

import pytest

from rustvello import App

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@dataclass
class Receiver:
    """A bounded test-only OTLP response and capture surface."""

    endpoint: str = ""
    status: int = 200
    bodies: list[bytes] = field(default_factory=list)


@pytest.fixture
def receiver() -> Iterator[Receiver]:
    """Start only a literal loopback receiver, never an external collector."""
    capture = Receiver()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            """Acknowledge a bounded request with an empty protobuf response."""
            length = int(self.headers["Content-Length"])
            assert length < 4 * 1024 * 1024
            capture.bodies.append(self.rfile.read(length))
            self.send_response(capture.status)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            """Do not write authorization or request details to test output."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    capture.endpoint = f"http://127.0.0.1:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield capture
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def test_shutdown_keeps_export_open_until_python_task_finishes(receiver: Receiver, tmp_path: Path) -> None:
    """A shutdown signal must not discard the terminal or worker-stop events."""
    entered = threading.Event()
    release = threading.Event()
    app = App(
        backend="sqlite",
        db_path=str(tmp_path / "shutdown.sqlite"),
        otlp_endpoint=receiver.endpoint,
        otlp_bearer_token="local-fixture",
    )

    @app.task
    def blocked() -> str:
        entered.set()
        assert release.wait(timeout=5)
        return "done"

    invocation = blocked()
    runner = app._build_runner(num_workers=1)
    results: list[object] = []

    def run() -> None:
        try:
            results.append(runner.run_one())
        except Exception as error:
            results.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(timeout=3)
        runner.shutdown()
        assert runner.is_running()
        assert runner.telemetry_stats()["rejected_after_shutdown"] == 0
    finally:
        release.set()
        thread.join(timeout=10)
        app.stop()
    assert not thread.is_alive()
    assert results == [True]
    assert invocation.result(timeout=1) == "done"
    runner.shutdown()
    stats = runner.telemetry_stats()
    assert stats["rejected_after_shutdown"] == stats["export_failed"] == 0
    assert stats["otlp_traces_acknowledged"] == 1
    assert stats["otlp_logs_failed"] == 0
    assert any(b"task.succeeded" in body for body in receiver.bodies)
    assert any(b"worker.stopped" in body for body in receiver.bodies)
    with pytest.raises(RuntimeError, match="shutting down"):
        runner.run_one()


def test_http_rejection_is_visible_without_changing_python_task_result(
    receiver: Receiver,
    tmp_path: Path,
) -> None:
    """The queue's flush result cannot hide HTTP-level delivery failure."""
    receiver.status = 401
    app = App(
        backend="sqlite",
        db_path=str(tmp_path / "failure.sqlite"),
        otlp_endpoint=receiver.endpoint,
        otlp_bearer_token="local-fixture",
    )

    @app.task
    def succeeds() -> int:
        return 7

    invocation = succeeds()
    runner = app._build_runner(num_workers=1)
    try:
        assert runner.run_one()
        assert invocation.result(timeout=1) == 7
        stats = runner.flush_telemetry()
        assert stats["export_failed"] > 0
        for signal in ("traces", "logs", "metrics"):
            assert stats[f"otlp_{signal}_failed"] > 0
            assert stats[f"otlp_{signal}_acknowledged"] == 0
        assert runner.telemetry_stats() == stats
    finally:
        runner.shutdown()
        app.stop()
    assert app.telemetry_stats()["submission"]["otlp_logs_failed"] > 0
