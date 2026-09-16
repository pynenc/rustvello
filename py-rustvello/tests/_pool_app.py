"""Importable application for the process-pool tests (imported by the worker children too)."""

import os
import time

from rustvello import App

app = App(
    app_id="process_pool_test",
    backend="sqlite",
    db_path=os.environ["RUSTVELLO_TEST_POOL_DB"],
    import_path="_pool_app:app",
)


@app.task(queue="cpu")
def worker_pid(delay: float) -> int:
    time.sleep(delay)
    return os.getpid()


@app.task(queue="cpu")
def spin(seconds: float) -> int:
    # CPU-bound on purpose: with threads the GIL would serialize this, with processes it runs in parallel
    deadline = time.perf_counter() + seconds
    count = 0
    while time.perf_counter() < deadline:
        count += 1
    return os.getpid()


@app.task(queue="cpu")
def boom(message: str) -> None:
    raise ValueError(message)


@app.task(queue="cpu")
def who_am_i(x: int) -> dict:
    current = app.current_invocation()
    assert current is not None
    return {
        "invocation_id": current.invocation_id,
        "task_key": current.task_key,
        "num_retries": current.num_retries,
        "arguments": current.arguments,
        "pid": os.getpid(),
    }


@app.task(queue="cpu")
def crash_process() -> None:
    os._exit(3)
