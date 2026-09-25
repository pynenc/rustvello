"""``async def`` tasks: parity with the Rust async task tests.

Each scenario mirrors ``crates/rustvello/tests/async_task_tests.rs``: real
network I/O through a local TCP echo server, overlap bounded by the worker
count, retries, errors, context across ``await`` points, child submission,
workflows, dev mode and cancellation.
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from collections.abc import Iterator

import pytest

from rustvello import App, get_current_invocation_id, get_current_num_retries, workflow_root
from rustvello.app import _run_coroutine, _worker_event_loop

# ---------------------------------------------------------------------------
# A local TCP echo server (plain threads, independent of the task loops)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def echo_addr() -> Iterator[tuple[str, int]]:
    server = socket.create_server(("127.0.0.1", 0))
    server.settimeout(0.2)
    stop = threading.Event()

    def serve(conn: socket.socket) -> None:
        with conn, conn.makefile("rwb") as stream:
            for line in stream:
                stream.write(line)
                stream.flush()

    def accept() -> None:
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except (socket.timeout, OSError):
                continue
            threading.Thread(target=serve, args=(conn,), daemon=True).start()

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    try:
        yield server.getsockname()[:2]
    finally:
        stop.set()
        thread.join(timeout=2)
        server.close()


async def echo_once(host: str, port: int, message: str) -> str:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(f"{message}\n".encode())
        await writer.drain()
        return (await reader.readline()).decode().rstrip("\n")
    finally:
        writer.close()
        await writer.wait_closed()


class TransientNetworkError(Exception):
    pass


class UpstreamUnavailable(Exception):
    pass


def _running_app(app_id: str) -> App:
    return App(app_id=app_id)


# ---------------------------------------------------------------------------
# Registration and dev mode
# ---------------------------------------------------------------------------


def test_async_def_registers_and_async_generators_are_rejected() -> None:
    app = App(app_id="async_registration", dev_mode_force_sync=True)

    @app.task
    async def ping() -> str:
        await asyncio.sleep(0)
        return "pong"

    assert ping.__name__ == "ping"
    with pytest.raises(TypeError, match="async generator"):

        @app.task
        async def stream():  # type: ignore[no-untyped-def]
            yield 1


def test_dev_mode_awaits_async_body(echo_addr: tuple[str, int]) -> None:
    app = App(app_id="async_dev_mode", dev_mode_force_sync=True)

    @app.task
    async def tcp_echo(message: str) -> str:
        return await echo_once(*echo_addr, message)

    assert tcp_echo("dev").result(timeout=5) == "dev"


def test_dev_mode_call_from_running_loop_uses_helper_thread() -> None:
    app = App(app_id="async_dev_mode_nested", dev_mode_force_sync=True)

    @app.task
    async def double(x: int) -> int:
        await asyncio.sleep(0)
        return x * 2

    async def caller() -> int:
        return double(21).result(timeout=5)

    assert asyncio.run(caller()) == 42


def test_worker_loop_is_reused_per_thread_and_leftover_tasks_are_cancelled() -> None:
    cancelled = []

    async def leaves_background_task() -> int:
        async def forever() -> None:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        asyncio.get_running_loop().create_task(forever())
        await asyncio.sleep(0)
        return 1

    assert _run_coroutine(leaves_background_task()) == 1
    assert cancelled == [True]
    loop = _worker_event_loop()
    assert _worker_event_loop() is loop, "one loop per worker thread"
    other: list[object] = []
    thread = threading.Thread(target=lambda: other.append(_worker_event_loop()))
    thread.start()
    thread.join()
    assert other[0] is not loop


# ---------------------------------------------------------------------------
# Runner execution
# ---------------------------------------------------------------------------


def test_async_io_task_runs_on_worker(echo_addr: tuple[str, int]) -> None:
    app = _running_app("async_worker_io")

    @app.task
    async def tcp_echo(message: str) -> str:
        return await echo_once(*echo_addr, message)

    invocations = [tcp_echo(f"hello {i}") for i in range(5)]
    app.run(num_workers=2, block=False, idle_sleep_ms=5)
    try:
        assert app.wait_results(invocations, timeout=30) == [f"hello {i}" for i in range(5)]
    finally:
        app.stop()


def test_async_bodies_overlap_bounded_by_workers() -> None:
    app = _running_app("async_worker_concurrency")
    lock = threading.Lock()
    state = {"active": 0, "peak": 0}

    @app.task
    async def concurrent_sleep(ms: int) -> int:
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        await asyncio.sleep(ms / 1000)
        with lock:
            state["active"] -= 1
        return ms

    invocations = [concurrent_sleep(300) for _ in range(8)]
    app.run(num_workers=4, block=False, idle_sleep_ms=5)
    try:
        started = time.monotonic()
        assert app.wait_results(invocations, timeout=30) == [300] * 8
        elapsed = time.monotonic() - started
    finally:
        app.stop()
    assert state["peak"] == 4, "the worker count bounds concurrent async bodies"
    assert elapsed < 2.0, f"8 x 300ms on 4 workers took {elapsed:.2f}s (serial: 2.4s)"


def test_async_retry_then_success(echo_addr: tuple[str, int]) -> None:
    app = _running_app("async_worker_retry")

    @app.task(max_retries=2, retry_for=(TransientNetworkError,))
    async def flaky() -> int:
        retries = get_current_num_retries() or 0
        await echo_once(*echo_addr, "attempt")
        if retries == 0:
            raise TransientNetworkError("first attempt fails")
        return retries

    invocation = flaky()
    app.run(num_workers=1, block=False, idle_sleep_ms=5)
    try:
        assert invocation.result(timeout=30) == 1
    finally:
        app.stop()


def test_async_errors_and_cancellation_reach_failed() -> None:
    app = _running_app("async_worker_errors")

    @app.task
    async def always_fails() -> None:
        await asyncio.sleep(0)
        raise UpstreamUnavailable("no upstream")

    @app.task
    async def cancelled_body() -> None:
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        await asyncio.sleep(1)

    failed = always_fails()
    cancelled = cancelled_body()
    app.run(num_workers=2, block=False, idle_sleep_ms=5)
    try:
        with pytest.raises(RuntimeError, match="UpstreamUnavailable: no upstream"):
            failed.result(timeout=30)
        with pytest.raises(RuntimeError, match="CancelledError"):
            cancelled.result(timeout=30)
    finally:
        app.stop()


def test_context_survives_await_and_links_children(echo_addr: tuple[str, int]) -> None:
    app = _running_app("async_worker_context")

    @app.task
    async def tcp_echo(message: str) -> str:
        return await echo_once(*echo_addr, message)

    @app.task
    async def parent() -> dict:
        before = get_current_invocation_id()
        await echo_once(*echo_addr, "hop")
        await asyncio.sleep(0.005)
        after = get_current_invocation_id()
        current = app.current_invocation()
        child = tcp_echo("child")
        return {
            "before": before,
            "after": after,
            "task_key": current.task_key if current else None,
            "child_id": str(child.id),
            "child_result": await child.result_async(timeout=20),
        }

    invocation = parent()
    app.run(num_workers=2, block=False, idle_sleep_ms=5)
    try:
        report = invocation.result(timeout=30)
    finally:
        app.stop()
    assert report["before"] == report["after"] == str(invocation.id)
    assert report["task_key"].endswith(".parent")
    assert report["child_result"] == "child"
    assert app._backend_objects is not None
    child = json.loads(app._backend_objects["state_backend"].get_invocation(report["child_id"]))
    assert child["parent_invocation_id"] == str(invocation.id)


def test_async_workflow_root() -> None:
    app = _running_app("async_worker_workflow")

    @app.workflow
    async def order(label: str) -> str:
        root = workflow_root()
        first = root.uuid()
        await asyncio.sleep(0)
        second = root.uuid()
        return f"{label}:{first != second}"

    invocation = order("wf")
    app.run(num_workers=1, block=False, idle_sleep_ms=5)
    try:
        assert invocation.result(timeout=30) == "wf:True"
    finally:
        app.stop()
