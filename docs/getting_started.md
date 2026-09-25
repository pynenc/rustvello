# Getting Started

:::{note}
**Using pynenc?** Install [`pynenc-rustvello`](https://github.com/pynenc/pynenc_rustvello)
to use Rust-powered backends inside your pynenc app. The plugin handles everything.
:::

:::{note}
**Python-only?** Skip to [Step 6](#step-6--use-from-python-standalone) for
the standalone Python experience — no Rust toolchain required.
:::

This guide walks you from zero to a running distributed task using rustvello's real API.
By the end you will have tasks defined with the `#[rustvello::task]` macro, a built
application, and a running worker.

---

## Concepts

| Concept          | Description                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------ |
| **Task**         | A Rust function annotated with `#[rustvello::task]` — typed, serializable, auto-registered |
| **Invocation**   | One execution request for a task + argument set; tracks status through an FSM              |
| **Broker**       | Routes invocations into queues and delivers them to workers                                |
| **Orchestrator** | Sequences complete invocation use cases across control, state, broker, and trigger ports   |
| **StateBackend** | Stores results and errors persistently                                                     |
| **TriggerStore** | Persists cron / event trigger state                                                        |
| **TaskRunner**   | Pulls invocations from the broker and executes them concurrently                           |

---

## Step 1 — Define Tasks

Create `src/tasks.rs`:

```rust
use rustvello::prelude::*;

/// Add two integers — result is i32
#[rustvello::task]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

/// Fallible task — returns RustvelloResult<T>
#[rustvello::task(max_retries = 2)]
fn divide(x: f64, y: f64) -> RustvelloResult<f64> {
    if y == 0.0 {
        return Err(RustvelloError::Runner { message: "division by zero".into() });
    }
    Ok(x / y)
}
```

The macro generates:

- `AddParams { x: i32, y: i32 }` — serializable parameter struct
- `AddTask` — struct implementing `Task` (`AddTask::new()`)
- `DivideParams` / `DivideTask` — same pattern for `divide`

---

## Step 2 — Build the Application

```rust
use rustvello::prelude::*;

#[tokio::main]
async fn main() -> RustvelloResult<()> {
    let app = Rustvello::builder()
        .app_id("my-app")
        // Reads RUSTVELLO__* environment variables (optional)
        .from_env()
        // Register all #[rustvello::task] functions found at link time
        .auto_discover_tasks()
        .build().await?;

    Ok(())
}
```

### Builder options

| Method                      | Description                                          |
| --------------------------- | ---------------------------------------------------- |
| `.app_id("name")`           | Set the application identifier                       |
| `.from_env()`               | Load config from `RUSTVELLO__*` env vars             |
| `.from_file("config.toml")` | Load from a TOML config file                         |
| `.dev_mode(true)`           | Run tasks inline (no broker/runner) — for unit tests |
| `.auto_discover_tasks()`    | Register all `#[rustvello::task]` functions          |
| `.build().await?`           | Consume the builder and produce `RustvelloApp`       |

Config priority: **programmatic > env vars > config file > defaults**

---

## Step 3 — Invoke a Task

```rust
use rustvello::prelude::*;

#[tokio::main]
async fn main() -> RustvelloResult<()> {
    let app = Rustvello::builder()
        .app_id("my-app")
        .auto_discover_tasks()
        .build().await?;

    // Submit a task — returns an InvocationHandle
    let handle = app.submit_call(&AddTask::new(), AddParams { x: 3, y: 4 }).await?;

    // Poll until a worker (Step 4) has run it
    let result = handle.wait(std::time::Duration::from_millis(50)).await?;
    println!("3 + 4 = {result}");  // 7

    Ok(())
}
```

Submitting only queues the invocation: a worker sharing the same backend must
run it (Step 4), otherwise `wait()` polls forever; use `wait_timeout()` to bound
it. `result()` reads a finished invocation and returns an error while it is
still pending. With `.dev_mode(true)`, `app.call(...)` runs the task inline and
needs no worker. The [README quick start](https://github.com/pynenc/rustvello#quick-start-rust)
shows a producer and a worker in one runnable program.

---

## Step 4 — Run a Worker

For persistent execution, start a `TaskRunner`. This is what the CLI's `run` command does:

```rust
use rustvello::prelude::*;

#[tokio::main]
async fn main() -> RustvelloResult<()> {
    let app = Rustvello::builder()
        .app_id("my-app")
        .from_env()
        .auto_discover_tasks()
        .build().await?;

    // Run until Ctrl-C, processing all queued invocations
    app.into_runner().with_graceful_shutdown(tokio::signal::ctrl_c()).await?;
    Ok(())
}
```

Or use the CLI directly (no code needed):

```bash
rustvello run --app-id my-app --db-path ./tasks.db
```

---

## Step 5 — Add a Persistent Backend

Switch from in-memory to SQLite by enabling the feature flag and using the builder:

```toml
# Cargo.toml
rustvello = { version = "0.7", features = ["sqlite"] }
```

```bash
# No code changes — configure via env var
RUSTVELLO__DB_PATH=./my_app.db rustvello run --app-id my-app
```

For Redis in production:

```toml
rustvello = { version = "0.7", features = ["redis"] }
```

```bash
RUSTVELLO__REDIS_URL=redis://localhost:6379 rustvello run --app-id my-app
```

---

## Step 6 — Use From Python (Standalone)

Rustvello ships as both a Rust crate **and** a Python package (`pip install rustvello`).
The Python package includes a lightweight `App` class for standalone use — no pynenc
required:

```python
from rustvello import App

app = App(backend="sqlite", db_path="./tasks.db")

@app.task(max_retries=2, cache_results=True)
def add(x: int, y: int) -> int:
    return x + y

# Submit a task and wait for a worker to run it
inv = add(1, 2)
result = inv.result(timeout=30)  # 3
```

`result()` waits for a worker: start one with `app.run()` (see _Running a
persistent worker_ below) or
`python -m rustvello.worker module:app`, otherwise it raises `TimeoutError`. For
tests and local tries, `App(dev_mode_force_sync=True)` (or
`RUSTVELLO__DEV_MODE_FORCE_SYNC=true`) runs every task inline in the caller.

Standalone Python tasks are functions declared with `def` or `async def`; an
`async def` task is awaited on its worker's event loop (see
[Async tasks](async_tasks.md)). Use `@app.workflow` for explicit workflow roots;
call `rustvello.workflow_root()` inside the workflow body for deterministic
`random()`, `utc_now()`, and `uuid()` operations. Use the pynenc integration when
a Python application needs framework-level import discovery. Argument binding
follows the Python function signature, and arguments and results must be JSON
serializable.

```python
from rustvello import workflow_root

@app.workflow
def process_order(order_id: str) -> dict[str, str]:
    root = workflow_root()
    return {
        "order_id": order_id,
        "run_id": root.uuid(),
        "recorded_at": root.utc_now(),
    }
```

### Backend selection

```python
app = App(backend="memory")   # default — in-process, no persistence
app = App(backend="sqlite", db_path="./tasks.db")
app = App(backend="redis", redis_url="redis://localhost:6379")
app = App(backend="postgres", postgres_url="postgresql://localhost/mydb")
app = App(backend="mongo", mongo_url="mongodb://localhost:27017", mongo_db="tasks")
app = App(backend="mongo3", mongo_url="mongodb://localhost:27017", mongo_db="tasks")  # legacy driver v2

# Mongo from connection parts, RabbitMQ as the broker on top of it
app = App(
    backend="mongo3",
    mongo_host="mongo", mongo_port=27017, mongo_username="u", mongo_password="p",
    mongo_auth_source="admin", mongo_db="tasks",
    broker="rabbitmq", rabbitmq_url="amqp://guest:guest@rabbitmq/",
)
```

When `config` is not given, `App` resolves its `AppConfig` with
`AppConfig.from_env()`: `RUSTVELLO__*` environment variables, an optional TOML
file and `[tool.rustvello.app]` in `pyproject.toml`, exactly like the Rust
builder. Explicit constructor arguments win.

### Running a persistent worker

```python
# Blocking — processes queued invocations on in-process threads (I/O-bound tasks)
app.run(num_workers=4)

# Worker processes: one interpreter (own GIL) per slot, for CPU-bound Python tasks.
# The Rust control plane stays in this process; only task code runs in the children.
app.run(num_processes=8, queues=["hpa"])

# Non-blocking — runs in a background thread
app.run(block=False)
app.stop()  # graceful shutdown
```

The same runner can be started from the command line, which is the shape a
container entrypoint or a Kubernetes manifest needs:

```bash
python -m rustvello.worker myproject.tasks:app --processes 8 --queues hpa hyper --loglevel info
```

Worker processes import the app through its import path (`package.module:app`);
pass `App(..., import_path=...)` when the app object is not discoverable through
`sys.modules`.

### Operating the app

```python
app.queue_depth("hpa")        # queued invocations in one queue (autoscaler metric)
app.queue_depths()            # per declared broker queue
app.purge()                   # drop queued work, control records and state
app.get_task("myproject.tasks.add")
app.current_invocation()      # inside a task: id, task key, retries, arguments
app.wait_results([add(1, 2), add(3, 4)], timeout=30)  # [3, 7]
server = app.start_monitor(host="0.0.0.0", port=8000)  # dashboard; server.stop()
```

### Retries by exception type

```python
@app.task(max_retries=3, retry_for=(ConnectionError, TimeoutError))
def fetch(url: str) -> str: ...
```

### Trigger scheduling

```python
@app.task
def cleanup() -> None: ...

app.trigger(cleanup).on_cron("0 */5 * * * *").register()
app.trigger(cleanup).on_interval(300).register()
```

### Extended task configuration

```python
@app.task(
    concurrency="keys",
    key_arguments=["user_id"],
    parallel_batch_size=50,
    reroute_on_cc=True,
)
def process_user(user_id: str, data: str) -> str:
    return data.upper()
```

---

## Step 7 — Use From Python via pynenc

For the full pynenc framework experience (plugins, workflows, triggers, builder pattern),
install `pynenc` and the `pynenc-rustvello` plugin:

```bash
pip install pynenc pynenc-rustvello
```

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y

result = add(1, 2).result  # 3
```

See the [pynenc documentation](https://docs.pynenc.org) and
[pynenc-rustvello documentation](https://github.com/pynenc/pynenc_rustvello) for the full guides.

---

## Step 8 — Add Monitoring

Start the live monitoring dashboard alongside your application:

```rust
use rustvello_monitoring::{start_monitor, AppInstance, MonitorConfig};

let instance = AppInstance { app_id: "my-app".into(), ..your_backends.. };
start_monitor(
    std::collections::HashMap::from([("my-app".into(), instance)]),
    "my-app",
    MonitorConfig::default(),  // binds to http://127.0.0.1:8000
).await?;
```

See {doc}`monitoring/index` for the full monitoring guide.

---

## What's Next

- {doc}`architecture` — Crate dependency graph, data model, and core trait signatures
- {doc}`configuration/index` — AppConfig, TaskConfig, env vars, TOML config file format
- {doc}`cli/index` — All CLI commands with options
- {doc}`monitoring/index` — Web dashboard features and setup
- {doc}`api/index` — API reference on docs.rs
