<p align="center">
  <img src="https://raw.githubusercontent.com/pynenc/rustvello/main/docs/_static/logo.png" alt="Rustvello" width="300">
</p>
<h1 align="center">Rustvello</h1>
<p align="center">
    <em>A distributed task orchestration engine built in Rust, with Python bindings</em>
</p>
<p align="center">
    <a href="https://github.com/pynenc/rustvello/actions/workflows/main.yml">
        <img src="https://img.shields.io/github/actions/workflow/status/pynenc/rustvello/main.yml?branch=main" alt="CI">
    </a>
    <a href="https://crates.io/crates/rustvello">
        <img src="https://img.shields.io/crates/v/rustvello.svg" alt="crates.io">
    </a>
    <a href="https://pypi.org/project/rustvello/">
        <img src="https://img.shields.io/pypi/v/rustvello.svg?color=%2334D058" alt="PyPI">
    </a>
    <a href="https://rustvello.readthedocs.io">
        <img src="https://img.shields.io/readthedocs/rustvello" alt="docs">
    </a>
    <a href="https://github.com/pynenc/rustvello/blob/main/LICENSE">
        <img src="https://img.shields.io/github/license/pynenc/rustvello" alt="License">
    </a>
</p>

---

**Documentation**: <a href="https://rustvello.readthedocs.io" target="_blank">https://rustvello.readthedocs.io</a>

**Source Code**: <a href="https://github.com/pynenc/rustvello" target="_blank">https://github.com/pynenc/rustvello</a>

---

Rustvello is a distributed task orchestration engine — broker, orchestrator, state backend, trigger system, client data store, and runner — implemented in Rust for performance and safety. It works standalone from both Rust and Python (via PyO3 bindings), and also integrates with [pynenc](https://github.com/pynenc/pynenc) as an optional high-performance backend plugin.

Deciding whether it fits? Read [When to use Rustvello](docs/when-to-use.md),
[Idempotency and the at-least-once contract](docs/idempotency.md),
[Migrating from Celery](docs/migrating-from-celery.md) and the
[benchmark against Celery](docs/benchmarks.md) (reproducible, with its limits).

## Repository Structure

This is a **multi-crate Rust workspace** with Python bindings:

| Crate                                                  | Description                                                                                                                    |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| [`rustvello-proto`](crates/rustvello-proto/)           | Data transfer objects and wire types (identifiers, status FSM, config, trigger types)                                          |
| [`rustvello-core`](crates/rustvello-core/)             | Core ports (`Broker`, `InvocationControlBackend`, `StateBackend`, `TriggerStore`, `ClientDataStore`) + business logic managers |
| [`rustvello-mem`](crates/rustvello-mem/)               | In-memory backend implementations (development and testing)                                                                    |
| [`rustvello-sqlite`](crates/rustvello-sqlite/)         | SQLite-backed backend implementations (single-node production)                                                                 |
| [`rustvello-redis`](crates/rustvello-redis/)           | Redis backend implementations                                                                                                  |
| [`rustvello-postgres`](crates/rustvello-postgres/)     | PostgreSQL backend implementations                                                                                             |
| [`rustvello-mongo`](crates/rustvello-mongo/)           | MongoDB backend implementations (driver v3)                                                                                    |
| [`rustvello-mongo3`](crates/rustvello-mongo3/)         | MongoDB backend implementations (driver v2 — legacy)                                                                           |
| [`rustvello-rabbitmq`](crates/rustvello-rabbitmq/)     | RabbitMQ broker implementation                                                                                                 |
| [`rustvello-otel`](crates/rustvello-otel/)             | Bounded OTLP lifecycle exporter                                                                                                |
| [`rustvello-macros`](crates/rustvello-macros/)         | `#[rustvello::task]` proc-macro with 8 configuration attributes                                                                |
| [`rustvello`](crates/rustvello/)                       | Main library — app builder, task runner, trigger builder, auto-discovery                                                       |
| [`rustvello-cli`](crates/rustvello-cli/)               | CLI tool for running workers, inspecting status, and purging data                                                              |
| [`rustvello-monitoring`](crates/rustvello-monitoring/) | Web-based monitoring dashboard (Axum + Askama + HTMX)                                                                          |
| [`rustvello-test-suite`](crates/rustvello-test-suite/) | Shared backend compliance tests via macro-generated test suites                                                                |
| [`rustvello-python`](crates/rustvello-python/)         | PyO3 bindings exposing Rust types to Python                                                                                    |
| [`py-rustvello`](py-rustvello/)                        | Python package (cdylib + PyO3 bindings) providing the `rustvello` module                                                       |

For the full architecture, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Key Features

- **Typed Task System**: proc-macro `#[rustvello::task]` generates serializable params, deterministic call IDs, and compile-time auto-discovery via `inventory`
- **Async Tasks**: `async fn` (Rust) and `async def` (Python) task bodies awaited natively on the worker's runtime or event loop, with the same retries, results and context propagation as synchronous tasks
- **Idempotency Keys**: at-least-once execution with a stable invocation id per retry and recovery; `submit_with_key` / `submit_call_with_key` turn repeated submissions of one key into one invocation on SQLite and PostgreSQL
- **Retries, Timeouts and Cancellation**: exponential backoff with jitter stored as durable delayed retries, per-attempt execution deadlines, and cooperative cancellation of queued or running invocations
- **Invocation State Machine**: 14-state FSM with guarded transitions, ownership tracking, and automatic recovery
- **Declared Guarantees**: a per-backend guarantee matrix (atomic publication, exactly-once trigger firings, stale-owner recovery, ordering, durability, delayed retries) served at `/api/capabilities`, with every guaranteed cell backed by process-kill tests that gate releases
- **Pluggable Backends**: Swap between in-memory, SQLite, Redis, PostgreSQL, MongoDB, and RabbitMQ backends via feature flags
- **Concurrency Control**: Four levels (Unlimited, Task, Argument, None) enforced at both registration and execution time
- **Queues and Priorities**: Named logical queues, configurable runner selection, and finite float priorities with FIFO ties
- **Trigger System**: Event-driven and cron-scheduled task execution with durable event/run evidence in memory and SQLite
- **Client Data Store**: SHA-256 content-addressed external storage for large arguments/results with LRU caching
- **Workflow System**: Explicit `#[rustvello::workflow]` roots, child identity propagation, and root-scoped deterministic replay
- **Recovery & Heartbeat**: Automatic detection and re-routing of stale invocations from crashed runners
- **Monitoring Dashboard**: Browser-based UI for invocations, runners, workflows, trigger evidence, and timelines (Axum + Askama + HTMX)
- **Cross-Language Support**: Closed `TaskLanguage`, canonical `language::module.name` task IDs, typed foreign tasks, and physical language queues
- **Builder Pattern**: Fluent configuration with env var overrides (`RUSTVELLO__*`), TOML file support, and `.memory()`/`.sqlite()` presets
- **Python Bindings**: Full PyO3 bridge for standalone Python usage and optional pynenc integration
- **CLI Tool**: Run workers, inspect invocations, and purge data from the command line
- **Shared Test Suite**: Macro-generated backend compliance tests ensuring all implementations satisfy the same contracts

## Installation

### Rust

```bash
cargo add rustvello
```

Feature flags:

- `mem` (default) — in-memory backends
- `sqlite` — SQLite backends
- `redis` — Redis backends
- `mongodb` — MongoDB backends
- `mongodb3` — MongoDB backends (legacy driver v2)
- `rabbitmq` — RabbitMQ backends
- `postgres` — PostgreSQL backends
- `full` — all backends

```toml
[dependencies]
rustvello = { version = "0.8", features = ["sqlite"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

### Python

```bash
pip install rustvello
```

### CLI

```bash
cargo install rustvello-cli
```

## Quick Start (Rust)

Tasks are submitted by producers and executed by workers that share a backend.
This example runs both in one process on a local SQLite file:

<!-- readme-example: crates/rustvello/examples/readme_quickstart.rs -->

```rust
use std::time::Duration;

use rustvello::prelude::*;

// Define a task with the proc macro
#[rustvello::task(max_retries = 2, concurrency = "task", priority = 25.5)]
fn process_order(order_id: String) -> String {
    format!("processed {order_id}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Producers and workers share the same backend; here a local SQLite file
    let db = std::env::temp_dir().join(format!("rustvello-quickstart-{}.db", std::process::id()));
    let db = db.to_str().ok_or("temp path is not UTF-8")?;
    let builder = || {
        Rustvello::builder()
            .app_id("my-app")
            .sqlite(db, "my-app")
            .auto_discover_tasks()
    };

    // Start a worker. In production it is its own process (a binary of yours
    // calling `into_runner()`); here it runs in the background.
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(
        builder()
            .build()
            .await?
            .into_runner()
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            }),
    );

    // Submit the task, then wait for the worker to finish it
    let app = builder().build().await?;
    let invocation = app
        .call(
            &ProcessOrderTask::new(),
            ProcessOrderParams {
                order_id: "123".into(),
            },
        )
        .await?;
    let result: String = invocation
        .wait_timeout(Duration::from_secs(30), Duration::from_millis(50))
        .await?;
    println!("Result: {result}");
    assert_eq!(result, "processed 123");

    let _ = stop.send(());
    worker.await??;
    Ok(())
}
```

`call()` returns at once; `wait()`/`wait_timeout()` poll until a worker finishes
the invocation. `result()` only reads a finished invocation and errors while it
is still pending. For local tries without a worker, `.dev_mode(true)` on the
builder makes `call()` run the task inline.

## Quick Start (Python)

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

## Using Rustvello from an agent

[`skills/rustvello`](skills/rustvello/SKILL.md) is an agent skill (the common
`SKILL.md` format, no MCP server needed): setting up an app, workers, retries,
timeouts, triggers, cancellation, choosing a backend and investigating a failed
invocation, with examples that CI runs against the built wheel.
[`llms.txt`](llms.txt) indexes the documentation, and [`evals/`](evals/README.md)
measures how well models install, use and recommend Rustvello.

## Pynenc Integration

Rustvello also serves as an optional high-performance backend for [pynenc](https://github.com/pynenc/pynenc).
Install the plugin with `pip install pynenc-rustvello` to use Rust-powered backends inside pynenc apps:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y

result = add(1, 2).result  # 3
```

## Development

Prerequisites: Rust 1.85+, Python 3.12+, [uv](https://docs.astral.sh/uv/), [maturin](https://www.maturin.rs/)

```bash
# Install dependencies and pre-commit hooks
make install

# Run all checks (Rust + Python + pre-commit)
make check

# Run all tests (Rust + Python)
make test

# Build the Python wheel
make build

# Build and serve docs locally
make docs-serve
```

Run `make help` for the full list of targets.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on reporting bugs, submitting PRs, commit conventions, and the development workflow.

## Contact or Support

- **[GitHub Issues](https://github.com/pynenc/rustvello/issues)**: Bug reports and feature requests

## License

Rustvello is released under the [MIT License](LICENSE).
