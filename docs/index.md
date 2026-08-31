:::{image} \_static/logo.png
:alt: rustvello
:align: center
:height: 90px
:class: hero-logo
:::

```{toctree}
:hidden:
:maxdepth: 2
:caption: User Guide

installation
getting_started
architecture
workflows
configuration/index
monitoring/index
migration-0.2
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Reference

cli/index
api/index
faq
changelog
license
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Development

contributing/index
```

# rustvello

**A distributed task orchestration engine built in Rust, with Python bindings.**

Rustvello delivers the performance-critical core — broker, orchestrator, state backend,
concurrency control, workflows, trigger scheduling, and a live monitoring dashboard —
implemented in safe Rust. It works standalone from both Rust and Python (via PyO3 bindings),
and integrates with [pynenc](https://docs.pynenc.org) as an optional high-performance backend plugin.

```bash
cargo add rustvello
```

or for Python:

```bash
pip install rustvello
```

---

## Define a Task

Use the `#[rustvello::task]` macro to turn any function into a distributed task:

````{tab} Rust
```rust
use rustvello::prelude::*;

#[rustvello::task]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[tokio::main]
async fn main() -> RustvelloResult<()> {
    let app = Rustvello::builder()
        .app_id("my-app")
        .auto_discover_tasks()
        .build().await?;

    // Submit and wait for the result
    let handle = app.submit_call(&AddTask, AddParams { x: 1, y: 2 }).await?;
    let result = handle.result().await?;
    println!("{result}");  // 3
    Ok(())
}
```
````

````{tab} Python (standalone)
```python
from rustvello import App

app = App(backend="sqlite", db_path="./tasks.db")

@app.task(max_retries=2)
def add(x: int, y: int) -> int:
    return x + y

# Submit and wait for the result
inv = add(1, 2)
result = inv.result(timeout=30)  # 3
```

See {doc}`getting_started` Step 6 for backend selection, runners, triggers, and more.
````

````{tab} Python (via pynenc)
```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y

# Call it — returns an invocation; block for result
result = add(1, 2).result  # 3
```

See the [pynenc documentation](https://docs.pynenc.org) for the full Python API.
````

---

## Key Capabilities

:::::{grid} 1 2 2 3
:gutter: 3

::::{grid-item-card} Invocation Lifecycle
Every task call becomes a tracked **invocation** through a 13-state FSM:
`Registered → Pending → Running → Success/Failed/Retry`. Ownership is
recorded per runner, and recovery re-queues stale invocations automatically.

{doc}`architecture`
::::

::::{grid-item-card} Concurrency Control
Prevent duplicate work at registration and execution time. Four modes —
`Disabled`, `Task`, `Arguments`, `Keys` — match any granularity need. Set
per-task via the macro attribute or TOML config.

{doc}`configuration/index`
::::

::::{grid-item-card} Trigger System
Schedule tasks with cron expressions or fire them on invocation events.
The atomic service framework guarantees each trigger fires exactly once
across N distributed runners.

{doc}`getting_started`
::::

::::{grid-item-card} Pluggable Backends
Swap in-memory, SQLite, Redis, MongoDB, RabbitMQ, or PostgreSQL without
touching task code. Feature flags select compiled backends; the builder API
wires them together.

{doc}`installation`
::::

::::{grid-item-card} Live Monitoring
A built-in Axum web dashboard shows SVG timelines, invocation tables,
runner status, log exploration with cross-highlighting, and Prometheus
metrics export — all served from a single binary.

{doc}`monitoring/index`
::::

::::{grid-item-card} Python Bindings
Full PyO3 bridge (`py-rustvello`) with a standalone `App` class for Python-only
use, plus deep integration with pynenc for the full framework experience.
Backend selection, persistent runner, trigger scheduling — all from Python.

{doc}`getting_started`
::::

:::::

---

## Pluggable Backends

The default build includes the in-memory backend. Enable others with Cargo feature flags:

| Feature      | Crate                  | Use case                             |
| ------------ | ---------------------- | ------------------------------------ |
| `mem`        | `rustvello-mem`        | Development & unit testing (default) |
| `sqlite`     | `rustvello-sqlite`     | Single-host persistence              |
| `redis`      | `rustvello-redis`      | Distributed production               |
| `mongodb`    | `rustvello-mongo`      | Distributed production (document DB) |
| `rabbitmq`   | `rustvello-rabbitmq`   | High-throughput broker               |
| `postgres`   | `rustvello-postgres`   | PostgreSQL trigger store             |
| `prometheus` | `rustvello-prometheus` | Prometheus metrics export            |
| `rayon`      | _(rayon thread-pool)_  | CPU-bound parallel task execution    |
| `full`       | all of the above       | Everything enabled                   |

Switch backends in the builder — no code changes in task functions:

```rust
let app = Rustvello::builder()
    .app_id("my-app")
    .from_env()          // reads RUSTVELLO__* env vars
    .build().await?;
```

---

## Ecosystem

Rustvello is available as both a **Rust crate** and a **Python package**:

| Distribution                                                   | Install                        | Language | Purpose                                       |
| -------------------------------------------------------------- | ------------------------------ | -------- | --------------------------------------------- |
| `rustvello` (crate)                                            | `cargo add rustvello`          | Rust     | Distributed task engine with proc-macro       |
| `rustvello` (wheel)                                            | `pip install rustvello`        | Python   | Standalone task queue (PyO3 bindings)         |
| [`pynenc-rustvello`](https://pynenc-rustvello.readthedocs.io/) | `pip install pynenc-rustvello` | Python   | Pynenc plugin — Rust backends for pynenc apps |
| [`pynenc`](https://docs.pynenc.org)                            | `pip install pynenc`           | Python   | Full distributed task framework               |

### Capabilities by surface

| Feature                       |      Rust crate      | Python standalone |  Python via pynenc  |
| ----------------------------- | :------------------: | :---------------: | :-----------------: |
| 7 backend types               |          ✅          |        ✅         |         ✅          |
| Task registration             | `#[rustvello::task]` |    `@app.task`    |     `@app.task`     |
| Persistent runner             |          ✅          |    `app.run()`    |     runner CLI      |
| Cron/interval triggers        |   `TriggerBuilder`   |  `app.trigger()`  | Full trigger system |
| Concurrency control (4 modes) |          ✅          |        ✅         |         ✅          |
| Web monitoring dashboard      |          ✅          |         —         |          —          |
| Prometheus metrics            |          ✅          |         —         |          —          |
| CLI tooling                   |          ✅          |         —         |          —          |
| Workflow support              |          ✅          |         —         |         ✅          |
| Plugin system                 |          —           |         —         |         ✅          |

---

## Pynenc Integration

Rustvello integrates with [pynenc](https://docs.pynenc.org) as an optional high-performance backend plugin.

```text
pynenc (Python, full orchestration framework)
    └── pynenc-rustvello (plugin — stateless adapters)
            └── py-rustvello (PyO3 wheel, Python bindings)
                    └── rustvello (this repo — Rust core)
```

Python users install [`pynenc-rustvello`](https://pynenc-rustvello.readthedocs.io/) to
get Rust-powered backends. The plugin registers itself via Python entry points and adds
builder methods like `.rustvello_redis()` and `.rustvello_postgres()` to `PynencBuilder`.

Python tasks decorated with `@app.task` and Rust tasks annotated with
`#[rustvello::task]` share the same broker and orchestrator when deployed
under the same `app_id`. The cross-language architecture uses
language-qualified `TaskId`s (`py::module.name` / `rs::crate::name`)
and a canonical JSON wire format so Python workers and Rust workers can
invoke each other seamlessly.

See the [pynenc documentation](https://docs.pynenc.org) to get started from Python.

---

## Community & License

Rustvello is open source under the [MIT License](license).
Contributions welcome — see the {doc}`contributing/index` guide.
GitHub: [pynenc/rustvello](https://github.com/pynenc/rustvello)
