# Configuration

Rustvello supports four configuration sources, applied from highest to lowest priority:

1. **Programmatic** — builder method calls (always wins)
2. **Environment variables** — `RUSTVELLO__*` prefix
3. **TOML config file** — loaded via `.from_file("config.toml")`
4. **Defaults** — compile-time defaults in `AppConfig` / `TaskConfig`

:::{admonition} See also: Pynenc Docs
:class: seealso
If you are configuring Rustvello as the backend for Pynenc, see the top-level [Pynenc Configuration Docs](https://pynenc.github.io/configuration/index.html).
:::

---

## Mode Selection

When rustvello is used through pynenc, the orchestrator class controls whether
orchestration uses Python-coordinated calls (**mixed mode**) or Rust composites
(**native orchestrator**):

| Config Field       | Example Value                  | Mode   |
| ------------------ | ------------------------------ | ------ |
| `orchestrator_cls` | `RustMemOrchestrator`          | Mixed  |
| `orchestrator_cls` | `RustMemNativeOrchestrator`    | Native |
| `orchestrator_cls` | `RustSqliteNativeOrchestrator` | Native |
| `orchestrator_cls` | `RustMongo3NativeOrchestrator` | Native |

The `*NativeOrchestrator` classes enable single-call composite orchestration for
status/result/retry hot paths. See {doc}`../architecture` for details.

```{note}
Runner-loop selection is related but separate: `Pynenc.is_all_rust_native`
checks whether all configured backend class names start with `Rust`
(`orchestrator_cls`, `state_backend_cls`, `broker_cls`, `trigger_cls`,
`client_data_store_cls`).
```

### Available Class Names

| Backend    | Mixed Mode                 | Native Mode                      |
| ---------- | -------------------------- | -------------------------------- |
| In-memory  | `RustMemOrchestrator`      | `RustMemNativeOrchestrator`      |
| SQLite     | `RustSqliteOrchestrator`   | `RustSqliteNativeOrchestrator`   |
| Redis      | `RustRedisOrchestrator`    | `RustRedisNativeOrchestrator`    |
| PostgreSQL | `RustPostgresOrchestrator` | `RustPostgresNativeOrchestrator` |
| MongoDB    | `RustMongoOrchestrator`    | `RustMongoNativeOrchestrator`    |

Set via env var:

```bash
PYNENC__ORCHESTRATOR_CLS=RustSqliteNativeOrchestrator
PYNENC__BROKER_CLS=RustSqliteBroker
PYNENC__STATE_BACKEND_CLS=RustSqliteStateBackend
PYNENC__TRIGGER_CLS=RustSqliteTrigger
PYNENC__CLIENT_DATA_STORE_CLS=RustSqliteClientDataStore
```

---

## AppConfig

Application-level settings that apply to the entire `RustvelloApp`.

| Field                             | Type        | Default       | Description                                                                             |
| --------------------------------- | ----------- | ------------- | --------------------------------------------------------------------------------------- |
| `app_id`                          | `String`    | `"rustvello"` | Unique identifier for the application                                                   |
| `dev_mode_force_sync`             | `bool`      | `false`       | Execute tasks synchronously in-process (for testing)                                    |
| `max_pending_seconds`             | `f64`       | `300.0`       | Max seconds an invocation can stay `Pending` before recovery re-queues it               |
| `heartbeat_interval_seconds`      | `f64`       | `30.0`        | How often a runner publishes its heartbeat                                              |
| `runner_dead_after_seconds`       | `u64`       | `300`         | Heartbeat age threshold after which a runner is considered dead                         |
| `recovery_check_interval_seconds` | `f64`       | `60.0`        | How often the management loop scans for stale invocations                               |
| `num_workers`                     | `usize`     | CPU count     | Number of concurrent async workers per runner                                           |
| `idle_sleep_ms`                   | `u64`       | `100`         | Milliseconds a worker sleeps when the broker queue is empty                             |
| `logging_level`                   | `String`    | `"info"`      | Log level for both Rust and Python runtimes (`trace`, `debug`, `info`, `warn`, `error`) |
| `log_format`                      | `LogFormat` | `Text`        | Log output format: `Text` (human-readable) or `Json` (NDJSON)                           |

See {doc}`../monitoring/logging` for details on the unified logging format.

---

## TaskConfig

Per-task settings. Apply globally via `[task_defaults]` or per-task via `[tasks.<name>]`
in a TOML file, or via env vars.

| Field                        | Type                     | Default            | Description                                                        |
| ---------------------------- | ------------------------ | ------------------ | ------------------------------------------------------------------ |
| `max_retries`                | `u32`                    | `0`                | Maximum retry attempts on failure                                  |
| `concurrency_control`        | `ConcurrencyControlType` | `Unlimited`        | Execution-time concurrency mode                                    |
| `running_concurrency`        | `Option<u32>`            | `None` (unlimited) | Max simultaneous running instances                                 |
| `registration_concurrency`   | `ConcurrencyControlType` | `Unlimited`        | Registration-time dedup mode                                       |
| `key_arguments`              | `Vec<String>`            | `[]`               | Arg names used as the concurrency key for `Keys` mode              |
| `cache_results`              | `bool`                   | `false`            | Cache and return previous result for identical args                |
| `disable_cache_args`         | `Vec<String>`            | `[]`               | Args excluded from cache key computation                           |
| `retry_for_errors`           | `Vec<String>`            | `[]`               | Error type names that trigger a retry                              |
| `on_diff_non_key_args_raise` | `bool`                   | `false`            | Raise if a duplicate is found with different non-key args          |
| `force_new_workflow`         | `bool`                   | `false`            | Always start a new workflow scope for this task                    |
| `reroute_on_cc`              | `bool`                   | `false`            | Reroute to the existing invocation when hitting concurrency limits |
| `parallel_batch_size`        | `usize`                  | `100`              | Batch size used by `parallelize()`                                 |
| `blocking`                   | `bool`                   | `false`            | Run on a blocking (OS) thread rather than Tokio                    |

---

## Concurrency Control

`ConcurrencyControlType` controls how concurrent invocations are managed:

| Variant     | Behaviour                                           |
| ----------- | --------------------------------------------------- |
| `Unlimited` | All invocations run concurrently — no restrictions  |
| `Task`      | At most one invocation of this task runs at a time  |
| `Argument`  | At most one invocation per unique full argument set |
| `None`      | No concurrent invocations allowed                   |

Set with the macro attribute:

```rust
#[rustvello::task(concurrency = "task")]
fn exclusive_task() -> String { ... }

#[rustvello::task(concurrency = "argument")]
fn per_user_task(user_id: u64, data: String) -> String { ... }
```

Or in TOML:

```toml
[task_defaults]
concurrency_control = "task"

[tasks.per_user_task]
concurrency_control = "argument"
```

---

## Environment Variables

All `AppConfig` fields are overridable via `RUSTVELLO__` prefix + field name in
SCREAMING_SNAKE_CASE:

```bash
RUSTVELLO__APP_ID=my-app
RUSTVELLO__MAX_PENDING_SECONDS=600
RUSTVELLO__HEARTBEAT_INTERVAL_SECONDS=15
RUSTVELLO__REDIS_URL=redis://localhost:6379
RUSTVELLO__DB_PATH=./tasks.db
```

Per-task overrides use the pattern `RUSTVELLO__TASK__<TASK_NAME>__<FIELD>`:

```bash
# Override max_retries for the "send_email" task
RUSTVELLO__TASK__SEND_EMAIL__MAX_RETRIES=5
```

Enable env loading in the builder:

```rust
let app = Rustvello::builder()
    .app_id("my-app")  // programmatic — cannot be overridden by env
    .from_env()         // load remaining fields from RUSTVELLO__* vars
    .build().await?;
```

---

## TOML Config File

```toml
# config.toml
app_id = "my-app"
max_pending_seconds = 600
heartbeat_interval_seconds = 15
num_workers = 4

# Global task defaults
[task_defaults]
max_retries = 1

# Per-task overrides
[tasks.process_order]
max_retries = 3
concurrency_control = "arguments"

[tasks.send_welcome_email]
concurrency_control = "keys"
key_arguments = ["user_id"]
cache_results = true
```

Load with the builder:

```rust
let app = Rustvello::builder()
    .from_file("config.toml")?
    .from_env()   // env vars still win over file for anything in both
    .build().await?;
```

---

## Backend Connection Settings

Backend-specific URLs and paths are set via builder methods or environment variables:

````{tab} SQLite
```rust
// Programmatic
Rustvello::builder().db_path("./tasks.db").build().await?;
```
```bash
# Environment
RUSTVELLO__DB_PATH=./tasks.db rustvello run
```
````

````{tab} Redis
```rust
// Programmatic
Rustvello::builder().redis_url("redis://localhost:6379").build().await?;
```
```bash
# Environment
RUSTVELLO__REDIS_URL=redis://prod-redis:6379 rustvello run
```
````

---

## CLI Config Inspection

View the effective configuration (merged from all sources) with:

```bash
rustvello config --config ./config.toml --app-id my-app
```

This prints the resolved `AppConfig` as TOML to stdout, showing exactly which values
came from each source.
