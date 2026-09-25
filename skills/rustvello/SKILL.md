---
name: rustvello
description: Build, run and debug Rustvello task queues from Python (and Rust). Covers setting up an app on SQLite, sync and async tasks with retries, backoff, timeouts and cancellation, running workers, submitting and waiting, cron triggers, choosing a backend from the guarantee matrix, and investigating a failed invocation through /api/capabilities and investigation reports. Use when code imports rustvello or the rustvello crate, or when asked to add, run or debug background tasks in a project that uses it.
license: MIT
metadata:
  rustvello-version: "0.7"
---

# Rustvello

Rustvello is a task queue with a Rust core and Python bindings: producers
submit invocations of registered tasks to a shared backend, and workers execute
them. Every example in `examples/` runs in CI against the released wheel, so
the calls shown here exist in the version below.

## Requirements

- `rustvello` **0.7.x** (`pip install "rustvello>=0.7,<0.8"`), CPython 3.9+.
  Check with `python -c "import rustvello; print(rustvello.__version__)"`.
- SQLite needs nothing else. Other backends need their server (PostgreSQL,
  Redis, MongoDB, RabbitMQ).
- Optional: the Rust CLI (`cargo install rustvello-cli`) for `rustvello
investigate|status|list|cancel` against a SQLite file.

## How it fits together

- `App(app_id=..., backend="sqlite", db_path=...)` holds the backend. Every
  process of one application (producers and workers) must use the **same
  `app_id` and the same backend** (for SQLite, the same file path).
- `@app.task` registers a function under `python::<module>.<name>`. A worker
  runs a task only if it imported that module under the same name, so put
  tasks in an importable module (not only in `__main__` when a worker process
  runs them).
- Calling a task submits it and returns an `Invocation` at once.
  `invocation.result(timeout=...)` waits until a worker finishes it.
  Arguments and results are JSON (dicts, lists, str, int, float, bool, None).
- Nothing runs unless a worker runs: `app.run()` in-process, or
  `python -m rustvello.worker module:app` as its own process.
- Execution is **at least once**: a retry re-runs the whole body. Give external
  side effects an idempotency key, for example
  `app.current_invocation().invocation_id` (stable across retries).

## 1. Set up an app, define tasks, run a worker, submit and wait

<!-- readme-example: skills/rustvello/examples/quickstart.py -->

```python
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
```

Other ways to wait: `await invocation.result_async(timeout=...)` inside async
code; `invocation.status` for a non-blocking check. For unit tests without a
worker, `App(dev_mode_force_sync=True)` (or `RUSTVELLO__DEV_MODE_FORCE_SYNC=true`)
runs tasks inline in the caller.

## 2. Run a worker as its own process

The production shape: an application module (`examples/tasks.py`) imported by
both the worker and the producers, and a worker started with
`python -m rustvello.worker tasks:app`. `examples/worker_process.py` starts
one, submits three invocations from another process and stops the worker with
SIGTERM (running tasks finish first).

```bash
python -m rustvello.worker myproject.tasks:app                 # in-process worker threads
python -m rustvello.worker myproject.tasks:app --processes 8   # CPU-bound Python: one interpreter each
python -m rustvello.worker myproject.tasks:app --queues fast   # only these queues
python -m rustvello.worker myproject.tasks:app --no-triggers   # do not evaluate triggers here
```

## 3. Retries, backoff and timeouts

`examples/retries_timeouts.py` shows a task retried only for
`ConnectionError` with backoff, an async task that exceeds its deadline, and
how failures surface to the producer.

| `@app.task(...)` option            | Default                | Meaning                                                              |
| ---------------------------------- | ---------------------- | -------------------------------------------------------------------- |
| `max_retries`                      | `0`                    | Retries after the first failed attempt                               |
| `retry_for=(ConnectionError, ...)` | `()` = every exception | Exception classes that may retry (matched by class name)             |
| `retry_delay` (s)                  | `0`                    | Delay before the first retry; `0` retries at once                    |
| `retry_backoff`                    | `2.0`                  | Growth factor per retry                                              |
| `retry_max_delay` (s)              | `300`                  | Cap of the delay                                                     |
| `retry_jitter`                     | `"equal"`              | `"equal"`, `"full"` or `"none"`                                      |
| `timeout` (s)                      | `None`                 | Deadline of **one attempt**; expiry fails it with `TaskTimeoutError` |
| `retry_on_timeout`                 | `True`                 | Whether a timed-out attempt may be retried                           |

- The backoff wait is stored in the backend, not slept in a worker. It
  survives a worker crash on SQLite and PostgreSQL only (see section 6).
- On timeout an `async def` body is cancelled at its next `await`. A sync body
  cannot be interrupted: its thread keeps running and its result is discarded.
  Use `async def` or `--processes N` (the worker process is killed) for work
  that must really stop.

## 4. Cron and interval triggers

`examples/cron_trigger.py`:

```python
app.trigger(write_report).on_cron("*/5 * * * *").register()          # every 5 minutes
app.trigger(write_report).on_cron("0 0 3 * * *").with_args(kind="daily").register()  # 03:00:00
app.trigger(write_report).on_interval(300).register()                # every 300 s
```

- 5 fields = standard minute cron; 6 fields = seconds first.
- `register()` stores the trigger in the backend at once; call it at import
  time of the app module. Registering the same trigger again is a no-op.
- A running worker (not started with `--no-triggers`) evaluates triggers every
  few seconds; each slot fires once even with several workers.
- Triggers target Python tasks of this app; `with_args(...)` values must be
  JSON.

## 5. Cancellation

`examples/cancel.py`:

```python
invocation.cancel()   # True if this call cancelled it, False if already final
app.cancel(invocation)
```

Queued or backing-off invocations never run. A running attempt is abandoned
within about a second (async bodies stop at their next `await`; sync bodies
keep running but their result is discarded). `result()` then raises
`rustvello.InvocationCancelledError`. Work already done is not undone.

## 6. Choose a backend

| Need                                        | Use                                                                                                                     |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| One machine, durable, no server             | `backend="sqlite"` (a file on local disk, WAL)                                                                          |
| Several machines, durable                   | `backend="postgres"`                                                                                                    |
| Tests, throwaway scripts                    | `backend="memory"` (nothing survives the process)                                                                       |
| Redis / MongoDB / RabbitMQ already in place | allowed, but retries with backoff are **not** durable there (they retry at once) and several guarantees are best effort |

Only SQLite and PostgreSQL carry every guarantee (atomic publication, trigger
atomicity, stale-owner recovery, ordering, durability, delayed retry), each
proven by process-kill tests. Print the matrix of the installed version, or
filter it by what you need:

```bash
python scripts/guarantees.py
python scripts/guarantees.py --need delayed_retry durability
```

The same data is served by a running monitor at `/api/capabilities`
(`guarantees.active` for the app's backend, `guarantees.matrix` for all).

## 7. Investigate a failed invocation

`examples/investigate_failure.py` runs the whole flow. With an invocation id:

1. If a monitor runs (`app.start_monitor(port=8000)`), start with
   `GET /api/capabilities`: it names the app, the investigation routes, the
   page sizes and the backend's guarantees. Check `schema_version`.
2. `GET /invocations/<id>/investigation`: status, ordered history (every
   `RETRY`, runner handoff and final status), runner contexts, registration
   runner, parent and workflow, and integrity flags.
3. Without a monitor, on SQLite:
   `python scripts/investigate.py <id> --db-path ./app.db --app-id <app_id>`
   (only reads), or `rustvello investigate <id> --app-id <app_id> --db-path ./app.db --format json`.
4. The producer side: `invocation.result()` raises `RuntimeError("Task failed:
<ErrorType>: <message>")`; read `ErrorType` first (table below).

The runner that registered an invocation is not necessarily the worker that ran
it; compare the history rows before concluding. The repository's `AGENTS.md`
has recipes for timelines, workflows and triggers.

## Errors and what to do

| You see                                              | Meaning                                   | Do                                                                                                                                       |
| ---------------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `TimeoutError: Invocation … still PENDING after 30s` | No worker picked it up                    | Start a worker for the same `app_id` and backend; check the worker imported the task's module under the same name and consumes its queue |
| `RuntimeError: Task failed: TaskTimeoutError: …`     | An attempt exceeded `timeout`             | Raise `timeout`, make the body `async def` or split the work                                                                             |
| `RuntimeError: Task failed: <YourError>: …`          | The task raised after all allowed retries | Fix the cause; add the class to `retry_for` only if a retry can succeed                                                                  |
| `InvocationCancelledError`                           | Someone cancelled it                      | Not an error of the task; check who cancelled before resubmitting                                                                        |
| `ValueError` from `register()` / `on_cron()`         | Invalid cron expression or a foreign task | Fix the expression (5 or 6 fields)                                                                                                       |
| Status stays `RETRY`                                 | Waiting for its backoff                   | Expected; the not-before time is in the backend                                                                                          |

## Safe to run without asking

- `python examples/*.py` from this skill (temporary SQLite files only)
- `python scripts/guarantees.py`, `python scripts/investigate.py …`
- `GET` requests to a monitor (`/api/capabilities`, `/invocations/<id>/investigation`, `/invocations/<id>/history`)
- `rustvello investigate|status|list … --db-path …`

## Never do without the owner's approval

- `app.purge()` or `rustvello purge`: deletes every queued invocation, state and trigger.
- Delete or overwrite a production database file, or point an example at it.
- `cancel` production invocations, or start a worker against a production backend.
- Print or commit connection strings with passwords (`postgres_url`, `redis_url`, `mongo_url`).

## Rust

The same model in Rust: `#[rustvello::task(max_retries = 3, retry_delay_ms = 500,
timeout_ms = 10_000)]` on a `fn` or `async fn`, `Rustvello::builder().app_id("my-app").sqlite(db, "my-app").auto_discover_tasks().build().await?`,
`app.call(&MyTask::new(), MyTaskParams { … }).await?`, then
`invocation.wait_timeout(...)`, a worker from `.into_runner()`, cancellation
with `app.cancel(&id).await?`, and triggers with
`TriggerBuilder::new().on_cron("*/5 * * * *")?.build_and_register(&task_id, &store)`.
The runnable Rust quick start is `crates/rustvello/examples/readme_quickstart.rs`
in the repository (run in CI with the README examples).
