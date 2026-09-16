"""Process-pool execution: one interpreter per worker, driven by the Rust control plane."""

import importlib
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))


@pytest.fixture(scope="module")
def pool_app(tmp_path_factory: pytest.TempPathFactory):
    db_path = tmp_path_factory.mktemp("pool") / "pool.db"
    os.environ["RUSTVELLO_TEST_POOL_DB"] = str(db_path)
    module = importlib.import_module("_pool_app")
    app = module.app
    app._config.broker_queues = ["default", "cpu"]
    app._config.runner_queues = ["cpu"]
    app.run(num_processes=3, block=False, idle_sleep_ms=10)
    try:
        yield module
    finally:
        app.stop()


def test_worker_command_targets_the_worker_module(pool_app) -> None:
    command = pool_app.app._worker_command(None)
    assert command[:3] == [sys.executable, "-m", "rustvello.worker"]
    assert command[-2:] == ["--app", "_pool_app:app"]
    env = pool_app.app._worker_env({"EXTRA": "1"})
    assert str(Path(__file__).parent) in env["PYTHONPATH"].split(os.pathsep)
    assert env["EXTRA"] == "1"


def test_tasks_run_in_separate_interpreters(pool_app) -> None:
    invocations = [pool_app.worker_pid(0.2) for _ in range(6)]
    pids = pool_app.app.wait_results(invocations, timeout=60)
    assert os.getpid() not in pids
    assert len(set(pids)) >= 2


def test_cpu_bound_tasks_run_in_parallel(pool_app) -> None:
    pool_app.app.wait_results([pool_app.spin(0.05) for _ in range(3)], timeout=60)  # warm the workers
    started = time.perf_counter()
    pids = pool_app.app.wait_results([pool_app.spin(1.0) for _ in range(3)], timeout=60)
    elapsed = time.perf_counter() - started
    assert len(set(pids)) == 3
    assert elapsed < 2.4, f"three 1s CPU tasks took {elapsed:.2f}s on three processes"


def test_task_errors_keep_type_and_message(pool_app) -> None:
    invocation = pool_app.boom("nope")
    with pytest.raises(RuntimeError) as excinfo:
        invocation.result(timeout=60)
    assert "ValueError" in str(excinfo.value)
    assert "nope" in str(excinfo.value)


def test_current_invocation_is_available_in_the_worker(pool_app) -> None:
    invocation = pool_app.who_am_i(5)
    seen = invocation.result(timeout=60)
    assert seen["invocation_id"] == str(invocation.id)
    assert seen["task_key"] == "python::_pool_app.who_am_i"
    assert seen["num_retries"] == 0
    assert seen["arguments"] == {"x": 5}
    assert seen["pid"] != os.getpid()


def test_crashed_worker_is_replaced(pool_app) -> None:
    with pytest.raises(RuntimeError) as excinfo:
        pool_app.crash_process().result(timeout=60)
    assert "WorkerProcessCrashed" in str(excinfo.value)
    assert pool_app.worker_pid(0.0).result(timeout=60) != os.getpid()
