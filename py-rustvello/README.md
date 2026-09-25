# Rustvello for Python

Distributed task queue and workflow runtime with a Rust core. Define tasks in
Python, submit them from any process, and run them on workers that share a
backend (memory, SQLite, PostgreSQL, Redis, MongoDB or RabbitMQ). Retries,
queues and priorities, cron and event triggers, workflow roots with
deterministic replay, worker recovery and a monitoring dashboard come built in.
Rust and Python tasks can call each other through the same backend.

- **Documentation**: <https://rustvello.readthedocs.io>
- **Source code**: <https://github.com/pynenc/rustvello>
- **Changelog**: <https://rustvello.readthedocs.io/en/latest/changelog.html>

## Install

```bash
pip install rustvello
```

## Quick start

<!-- readme-example: py-rustvello/examples/quickstart.py -->

```python
from rustvello import App, workflow_root

app = App(backend="sqlite", db_path="./tasks.db")


@app.task(max_retries=2)
def add(x: int, y: int) -> int:
    return x + y


@app.workflow
def process_order(order_id: str) -> dict[str, str]:
    root = workflow_root()  # deterministic helpers, recorded for replay
    return {"order_id": order_id, "run_id": root.uuid()}


if __name__ == "__main__":
    # A worker executes what you submit. In production it is its own process:
    #   python -m rustvello.worker my_module:app
    # Here it runs in a background thread of this script.
    app.run(block=False)
    try:
        print(add(1, 2).result(timeout=30))  # 3
        print(process_order("order-1").result(timeout=30))
    finally:
        app.stop()
```

`result(timeout=...)` blocks until a worker finishes the task and raises
`TimeoutError` if none does, so something must run `app.run()` or
`python -m rustvello.worker`. For tests and local tries, run tasks inline instead:

<!-- readme-example: py-rustvello/examples/quickstart_dev_mode.py -->

```python
from rustvello import App

# Tasks run inline in the caller: no worker needed. Handy for tests and
# local tries; RUSTVELLO__DEV_MODE_FORCE_SYNC=true does the same without code.
app = App(dev_mode_force_sync=True)


@app.task
def add(x: int, y: int) -> int:
    return x + y


print(add(1, 2).result())  # 3
```

## Pynenc

[Pynenc](https://docs.pynenc.org) users do not import `rustvello` directly:
install [`pynenc-rustvello`](https://github.com/pynenc/pynenc_rustvello) to use
Rustvello backends inside a Pynenc app.

## Building from source

Requires Rust 1.85+ and [maturin](https://www.maturin.rs/). From the repository
root:

```bash
make develop                                    # or:
maturin develop --release -m py-rustvello/Cargo.toml
```
