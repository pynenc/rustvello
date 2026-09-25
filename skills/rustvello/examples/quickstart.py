"""Set up an app on SQLite, define a sync and an async task, run a worker, submit and wait."""

import asyncio
import os
import tempfile

from rustvello import App

# Producers and workers share one backend. SQLite needs no server; use a file
# path every process can reach (never ":memory:" when a worker runs elsewhere).
DB = os.path.join(tempfile.mkdtemp(), "tasks.db")
app = App(app_id="quickstart", backend="sqlite", db_path=DB)


@app.task
def add(x: int, y: int) -> int:
    return x + y


@app.task(timeout=10)  # an async body is cancelled at its next await on timeout
async def slow_double(x: int) -> int:
    await asyncio.sleep(0.1)
    return 2 * x


if __name__ == "__main__":
    app.run(block=False)  # worker in a background thread; production: its own process
    try:
        invocation = add(1, 2)  # submits and returns at once
        print("invocation id:", invocation.id)
        assert invocation.result(timeout=30) == 3  # blocks until a worker finishes it
        assert slow_double(21).result(timeout=30) == 42
        # submit many, then wait for all of them in one loop
        results = app.wait_results([add(i, i) for i in range(5)], timeout=30)
        assert results == [0, 2, 4, 6, 8]
        print("ok", results)
    finally:
        app.stop()
