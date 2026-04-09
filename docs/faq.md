# Frequently Asked Questions

## How does rustvello relate to pynenc?

Rustvello is an **independent distributed task orchestration engine** that also integrates
with [pynenc](https://docs.pynenc.org) as an optional high-performance backend plugin.
The relationship is:

```text
rustvello       — Standalone Rust engine (broker, orchestrator, state backend, monitoring)
  └── py-rustvello    — PyO3 wheel for standalone Python usage
          └── pynenc-rustvello — Optional plugin that connects rustvello backends to pynenc
                  └── pynenc — Full Python orchestration framework
```

Rustvello works standalone from both Rust and Python. The pynenc integration is additive —
not required.

---

## Can I use rustvello without pynenc?

Yes — there are two ways:

1. **Rust standalone:** Use `#[rustvello::task]` and `Rustvello::builder()` for pure-Rust
   distributed task systems — no Python required.

2. **Python standalone:** Install `pip install rustvello` and use the `App` class directly:

   ```python
   from rustvello import App

   app = App(backend="sqlite", db_path="./tasks.db")

   @app.task(max_retries=2)
   def add(x: int, y: int) -> int:
       return x + y

   inv = add(1, 2)
   result = inv.result(timeout=30)  # 3
   ```

   The standalone Python API supports backend selection (memory, sqlite, redis, postgres,
   mongo, mongo3), a persistent task runner (`app.run()`), trigger scheduling, and extended task
   configuration (concurrency control, key arguments, batch size, etc.).

The pynenc integration is additive — not required.

---

## Which backend should I use?

| Scenario                               | Recommended backend            |
| -------------------------------------- | ------------------------------ |
| Unit testing / development             | `mem` (default) — zero setup   |
| Single-host, data must survive restart | `sqlite`                       |
| Multi-host / distributed               | `redis` or `postgres`          |
| Multi-host, document-oriented data     | `mongodb`                      |
| Multi-host, document-oriented (legacy) | `mongodb3` (MongoDB driver v2) |
| High-throughput message queue          | `rabbitmq`                     |

Start with `mem` during development. When you need persistence, add `sqlite` with
one feature flag and an env var — no code changes. Scale to `redis` or `mongodb`
for multi-host deployments.

:::{note}
The `mongodb` backend uses MongoDB driver v3 and targets modern MongoDB deployments.
The `mongodb3` backend uses MongoDB driver v2 (legacy) and targets MongoDB 3.6+
without transaction support, using optimistic CAS for status transitions instead.
Both require a replica-set connection string for change-stream support.
:::

---

## What is the `#[rustvello::task]` macro and what does it generate?

The `#[rustvello::task]` attribute macro transforms a plain `fn` into a distributed
task. For a function `fn add(x: i32, y: i32) -> i32`, it generates:

- **`AddParams`** — a `serde::Serialize + Deserialize` struct with fields `x: i32, y: i32`
- **`AddTask`** — a unit struct implementing the `Task` trait (carries `TaskId`, `TaskConfig`, serialization logic)
- **A static `inventory::submit!`** — registers `AddTask` at link time so `auto_discover_tasks()` can find it

The original `add` function is preserved unchanged for direct calls.

---

## What concurrency mode should I choose?

| Mode                 | Use when                                                                       |
| -------------------- | ------------------------------------------------------------------------------ |
| `Disabled` (default) | Tasks are idempotent or you want maximum throughput                            |
| `Task`               | Only one instance of this task should run globally at a time                   |
| `Arguments`          | One instance per unique full argument set (strict dedup)                       |
| `Keys`               | One instance per subset of arguments (e.g. per `user_id`, ignoring other args) |

Apply at the macro level:

```rust
#[rustvello::task(concurrency = "keys", key_arguments = ["user_id"])]
fn update_user_profile(user_id: u64, data: String) -> String { ... }
```

Or override at runtime via TOML config or env vars without recompiling.

---

## How does recovery work?

1. Every runner periodically calls `register_heartbeat()` with its `RunnerId`.
2. The management loop checks for invocations whose runner's heartbeat is older than
   `runner_dead_after_seconds` — these transition to `RunningRecovery`.
3. Invocations stuck in `Pending` for longer than `max_pending_seconds` transition
   to `PendingRecovery`.
4. Both recovery states re-route the invocation back to `Pending` so another runner
   can pick it up.

Recovery runs in the management loop at `recovery_check_interval_seconds` cadence.
No operator intervention is needed.

---

## How do I test my tasks?

**Unit tests (no backends):** Enable `dev_mode`:

```rust
let app = Rustvello::builder()
    .app_id("test")
    .dev_mode(true)
    .build().await?;
// Tasks run inline — no runner needed
```

**Integration tests (full stack):** Use the in-memory backend and a runner:

```rust
let app = Rustvello::builder()
    .app_id("test")
    .auto_discover_tasks()   // builder method, not app method
    .build().await?;         // mem backend is default

// Run until ctrl-c (or any future that resolves when you want to stop)
app.into_runner().with_graceful_shutdown(tokio::signal::ctrl_c()).await?;
```

**Backend compliance tests:** Use `rustvello-test-suite` to validate a custom backend
implements all traits correctly:

```rust
rustvello_test_suite::suite_all!(MyBroker, MyOrchestrator, MyStateBackend);
```

---

## Can I control task execution mode with an environment variable?

Yes. Set `RUSTVELLO__DEV_MODE_FORCE_SYNC=true` (note the double underscore — all
rustvello env vars use the `RUSTVELLO__` prefix) and tasks run inline without a
worker:

```bash
# run your test suite with sync execution
RUSTVELLO__DEV_MODE_FORCE_SYNC=true cargo test
```

Or declare it in CI:

```yaml
# .github/workflows/main.yml (excerpt)
- name: Run tests
  env:
    RUSTVELLO__DEV_MODE_FORCE_SYNC: "true"
  run: cargo test
```

Any `AppConfig` field can be controlled this way — the format is
`RUSTVELLO__<FIELD_NAME>=<value>` (upper-snake-case, **double underscore** prefix). For example:

| Config field                | Environment variable                      |
| --------------------------- | ----------------------------------------- |
| `dev_mode_force_sync`       | `RUSTVELLO__DEV_MODE_FORCE_SYNC=true`     |
| `max_pending_seconds`       | `RUSTVELLO__MAX_PENDING_SECONDS=120`      |
| `runner_dead_after_seconds` | `RUSTVELLO__RUNNER_DEAD_AFTER_SECONDS=60` |

This pattern is similar to how pynenc exposes `PYNENC__DEV_MODE_FORCE_SYNC_TASKS`.

---

## How do I monitor invocations in production?

Two options:

1. **Web dashboard** (`rustvello-monitoring`): Start `start_monitor()` alongside your
   app. Browse to `http://your-host:8000` for timelines, log explorer, and tables.
   See {doc}`monitoring/index`.

2. **Prometheus** (`rustvello-prometheus`): Enable the `prometheus` feature and scrape
   `/metrics`. Standard counters for invocation counts, status transitions, and durations.

---

## Does rustvello support cron / scheduled tasks?

Yes. Register a trigger on any task via `TriggerBuilder`:

```rust
app.trigger(AddTask)
    .on_cron("0 */5 * * * *")  // every 5 minutes
    .register()?;
```

The trigger system uses an _atomic service_ algorithm that ensures exactly one runner
evaluates triggers per time slot — even with N runners running simultaneously.

---

## How does the cross-language (Python + Rust) deployment work?

Python ([pynenc](https://docs.pynenc.org)) and Rust workers share the same broker
and orchestrator when deployed under the same `app_id`. Task IDs are language-qualified
(`py::module.task` vs `rs::crate.task`) so:

- Each language's broker only delivers invocations it can execute
- Rust tasks can call Python tasks and vice versa with full argument passing
- The wire format (`BTreeMap<String, String>` with JSON values) is identical in both

See {doc}`architecture` for the full cross-language design.

---

## Why is the package called `py-rustvello` on PyPI but `rustvello` on crates.io?

- **crates.io** ships the Rust library crates (`rustvello`, `rustvello-core`, etc.)
- **PyPI** ships `py-rustvello` — the PyO3-built Python wheel (a cdylib)

This avoids a name collision, since both registries are independent and the artifacts
serve different language ecosystems. The [pynenc](https://docs.pynenc.org) package
installs `py-rustvello` as a dependency automatically.
