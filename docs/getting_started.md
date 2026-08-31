# Getting Started

:::{note}
**Using pynenc?** Install [`pynenc-rustvello`](https://pynenc-rustvello.readthedocs.io/)
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
| **Orchestrator** | Manages invocation lifecycle, concurrency control, heartbeats, and recovery                |
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
- `AddTask` — unit struct implementing `Task`
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
    let handle = app.submit_call(&AddTask, AddParams { x: 3, y: 4 }).await?;

    // Poll until the result is available
    let result = handle.result().await?;
    println!("3 + 4 = {result}");  // 7

    Ok(())
}
```

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
rustvello = { version = "0.3.1", features = ["sqlite"] }
```

```bash
# No code changes — configure via env var
RUSTVELLO__DB_PATH=./my_app.db rustvello run --app-id my-app
```

For Redis in production:

```toml
rustvello = { version = "0.3.1", features = ["redis"] }
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

# Submit a task and get the result
inv = add(1, 2)
result = inv.result(timeout=30)  # 3
```

Standalone Python tasks must be synchronous callables declared with `def`.
Use the pynenc integration when a Python workflow needs framework-level import
discovery or asynchronous task declarations. Argument binding follows the Python
function signature, and arguments and results must be JSON serializable.

### Backend selection

```python
app = App(backend="memory")   # default — in-process, no persistence
app = App(backend="sqlite", db_path="./tasks.db")
app = App(backend="redis", redis_url="redis://localhost:6379")
app = App(backend="postgres", postgres_url="postgresql://localhost/mydb")
app = App(backend="mongo", mongo_url="mongodb://localhost:27017", mongo_db="tasks")
app = App(backend="mongo3", mongo_url="mongodb://localhost:27017", mongo_db="tasks")  # legacy driver v2
```

### Running a persistent worker

```python
# Blocking — processes queued invocations
app.run(num_workers=4)

# Non-blocking — runs in a background thread
app.run(block=False)
app.stop()  # graceful shutdown
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
[pynenc-rustvello documentation](https://pynenc-rustvello.readthedocs.io/) for the full guides.

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
