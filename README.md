<p align="center">
  <img src="https://pynenc.org/assets/img/pynenc_logo.png" alt="Rustvello" width="300">
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
    <a href="https://codspeed.io/pynenc/rustvello?utm_source=badge">
        <img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed Badge">
    </a>
</p>

---

**Documentation**: <a href="https://rustvello.readthedocs.io" target="_blank">https://rustvello.readthedocs.io</a>

**Source Code**: <a href="https://github.com/pynenc/rustvello" target="_blank">https://github.com/pynenc/rustvello</a>

---

Rustvello is a distributed task orchestration engine — broker, orchestrator, state backend, trigger system, client data store, and runner — implemented in Rust for performance and safety. It works standalone from both Rust and Python (via PyO3 bindings), and also integrates with [pynenc](https://github.com/pynenc/pynenc) as an optional high-performance backend plugin.

## Repository Structure

This is a **multi-crate Rust workspace** with Python bindings:

| Crate                                                  | Description                                                                                                         |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| [`rustvello-proto`](crates/rustvello-proto/)           | Data transfer objects and wire types (identifiers, status FSM, config, trigger types)                               |
| [`rustvello-core`](crates/rustvello-core/)             | Core traits (`Broker`, `Orchestrator`, `StateBackend`, `TriggerStore`, `ClientDataStore`) + business logic managers |
| [`rustvello-mem`](crates/rustvello-mem/)               | In-memory backend implementations (development and testing)                                                         |
| [`rustvello-sqlite`](crates/rustvello-sqlite/)         | SQLite-backed backend implementations (single-node production)                                                      |
| [`rustvello-redis`](crates/rustvello-redis/)           | Redis backend implementations                                                                                       |
| [`rustvello-postgres`](crates/rustvello-postgres/)     | PostgreSQL backend implementations                                                                                  |
| [`rustvello-mongo`](crates/rustvello-mongo/)           | MongoDB backend implementations (driver v3)                                                                         |
| [`rustvello-mongo3`](crates/rustvello-mongo3/)         | MongoDB backend implementations (driver v2 — legacy)                                                                |
| [`rustvello-rabbitmq`](crates/rustvello-rabbitmq/)     | RabbitMQ broker implementation                                                                                      |
| [`rustvello-prometheus`](crates/rustvello-prometheus/) | Prometheus metrics exporter                                                                                         |
| [`rustvello-macros`](crates/rustvello-macros/)         | `#[rustvello::task]` proc-macro with 8 configuration attributes                                                     |
| [`rustvello`](crates/rustvello/)                       | Main library — app builder, task runner, trigger builder, auto-discovery                                            |
| [`rustvello-cli`](crates/rustvello-cli/)               | CLI tool for running workers, inspecting status, and purging data                                                   |
| [`rustvello-monitoring`](crates/rustvello-monitoring/) | Web-based monitoring dashboard (Axum + Askama + HTMX)                                                               |
| [`rustvello-test-suite`](crates/rustvello-test-suite/) | Shared backend compliance tests via macro-generated test suites                                                     |
| [`rustvello-python`](crates/rustvello-python/)         | PyO3 bindings exposing Rust types to Python                                                                         |
| [`py-rustvello`](py-rustvello/)                        | Python package (cdylib + PyO3 bindings) providing the `rustvello` module                                            |

For the full architecture, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Key Features

- **Typed Task System**: proc-macro `#[rustvello::task]` generates serializable params, deterministic call IDs, and compile-time auto-discovery via `inventory`
- **Invocation State Machine**: 13-state FSM with guarded transitions, ownership tracking, and automatic recovery
- **Pluggable Backends**: Swap between in-memory, SQLite, Redis, PostgreSQL, MongoDB, and RabbitMQ backends via feature flags
- **Concurrency Control**: Four levels (Unlimited, Task, Argument, None) enforced at both registration and execution time
- **Queues and Priorities**: Named logical queues, configurable runner selection, and finite float priorities with FIFO ties
- **Trigger System**: Event-driven and cron-scheduled task execution with durable event/run evidence in memory and SQLite
- **Client Data Store**: SHA-256 content-addressed external storage for large arguments/results with LRU caching
- **Workflow System**: Explicit `#[rustvello::workflow]` roots, child identity propagation, and root-scoped deterministic replay
- **Recovery & Heartbeat**: Automatic detection and re-routing of stale invocations from crashed runners
- **Monitoring Dashboard**: Browser-based UI for invocations, runners, workflows, trigger evidence, and timelines (Axum + Askama + HTMX)
- **Cross-Language Support**: Language-tagged `TaskId`, `ForeignTask` trait, and broker language-aware routing for Python ↔ Rust interop
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
- `prometheus` — Prometheus metrics
- `postgres` — PostgreSQL backends
- `full` — all backends

```toml
[dependencies]
rustvello = { version = "0.4.0", features = ["full"] }
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

```rust
use rustvello::prelude::*;

// Define a task with the proc macro
#[rustvello::task(max_retries = 2, concurrency = "task", queue = "orders", priority = 25.5)]
fn process_order(order_id: String) -> String {
    format!("processed {}", order_id)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build an app with in-memory backends and auto-discovered tasks
    let mut app = Rustvello::builder()
        .app_id("my-app")
        .memory()
        .auto_discover_tasks()
        .build()?;

    // Submit a task — unified call routing (sync/distributed)
    let invocation = app.call(
        &ProcessOrderTask,
        ProcessOrderParams { order_id: "123".into() },
    ).await?;

    // Get result (async for distributed, immediate for dev mode)
    let result: String = invocation.result().await?;
    println!("Result: {result}");

    Ok(())
}
```

## Quick Start (Python)

```python
from rustvello import App

app = App(backend="sqlite", db_path="./tasks.db")

@app.task(max_retries=2)
def add(x: int, y: int) -> int:
    return x + y

# Submit and wait for result
inv = add(1, 2)
result = inv.result(timeout=30)  # 3
```

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
- **[GitHub Discussions](https://github.com/pynenc/rustvello/discussions)**: Questions and ideas

## License

Rustvello is released under the [MIT License](LICENSE).
