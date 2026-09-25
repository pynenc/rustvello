"""Retries with durable backoff, a per-attempt deadline, and reading a failure."""

import asyncio
import os
import tempfile
import time

from rustvello import App

app = App(app_id="retries", backend="sqlite", db_path=os.path.join(tempfile.mkdtemp(), "retries.db"))


# Retried only for ConnectionError: 0.2 s before the first retry, doubling each
# time (equal jitter). The wait is stored in SQLite, so it survives a worker crash.
@app.task(max_retries=3, retry_for=(ConnectionError,), retry_delay=0.2, retry_backoff=2.0)
def fetch(url: str) -> str:
    current = app.current_invocation()  # same invocation id on every retry
    assert current is not None
    if current.num_retries < 2:
        raise ConnectionError(f"{url} unreachable (attempt {current.num_retries + 1})")
    return f"fetched {url} after {current.num_retries} retries"


# One attempt may take at most 0.5 s; a timeout is final here (no retry).
@app.task(timeout=0.5, max_retries=2, retry_on_timeout=False)
async def too_slow() -> str:
    await asyncio.sleep(30)  # cancelled at this await when the deadline passes
    return "never"


@app.task(max_retries=5, retry_for=(ConnectionError,))
def bad_input(value: int) -> int:
    raise ValueError(f"bad value {value}")  # not in retry_for: fails at once


if __name__ == "__main__":
    app.run(block=False)
    try:
        started = time.monotonic()
        print(fetch("https://example.org").result(timeout=60))
        assert time.monotonic() - started >= 0.2  # the backoff was honoured

        for invocation, expected in ((too_slow(), "TaskTimeoutError"), (bad_input(1), "ValueError")):
            try:
                invocation.result(timeout=60)
                raise AssertionError("expected a failure")
            except RuntimeError as error:  # "Task failed: <ErrorType>: <message>"
                assert expected in str(error), error
                print(f"{invocation.id} -> {invocation.status}: {error}")
        print("ok")
    finally:
        app.stop()
