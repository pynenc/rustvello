# API Reference

## Rust API (docs.rs)

All public Rust APIs are documented with `cargo doc` and published to docs.rs.

| Crate                  | docs.rs                                                              | Description                                     |
| ---------------------- | -------------------------------------------------------------------- | ----------------------------------------------- |
| `rustvello`            | [docs.rs/rustvello](https://docs.rs/rustvello)                       | Application layer, builder, runner              |
| `rustvello-proto`      | [docs.rs/rustvello-proto](https://docs.rs/rustvello-proto)           | DTOs, identifiers, status FSM, config           |
| `rustvello-core`       | [docs.rs/rustvello-core](https://docs.rs/rustvello-core)             | Core traits: Broker, Orchestrator, StateBackend |
| `rustvello-macros`     | [docs.rs/rustvello-macros](https://docs.rs/rustvello-macros)         | `#[rustvello::task]` proc macro                 |
| `rustvello-mem`        | [docs.rs/rustvello-mem](https://docs.rs/rustvello-mem)               | In-memory backend implementations               |
| `rustvello-sqlite`     | [docs.rs/rustvello-sqlite](https://docs.rs/rustvello-sqlite)         | SQLite backend implementations                  |
| `rustvello-redis`      | [docs.rs/rustvello-redis](https://docs.rs/rustvello-redis)           | Redis backend implementations                   |
| `rustvello-mongo`      | [docs.rs/rustvello-mongo](https://docs.rs/rustvello-mongo)           | MongoDB backend implementations                 |
| `rustvello-rabbitmq`   | [docs.rs/rustvello-rabbitmq](https://docs.rs/rustvello-rabbitmq)     | RabbitMQ broker                                 |
| `rustvello-postgres`   | [docs.rs/rustvello-postgres](https://docs.rs/rustvello-postgres)     | PostgreSQL trigger store                        |
| `rustvello-prometheus` | [docs.rs/rustvello-prometheus](https://docs.rs/rustvello-prometheus) | Prometheus EventEmitter                         |
| `rustvello-monitoring` | [docs.rs/rustvello-monitoring](https://docs.rs/rustvello-monitoring) | Web dashboard                                   |
| `rustvello-cli`        | [docs.rs/rustvello-cli](https://docs.rs/rustvello-cli)               | CLI binary                                      |
| `rustvello-test-suite` | [docs.rs/rustvello-test-suite](https://docs.rs/rustvello-test-suite) | Backend compliance tests                        |

To generate the full API docs locally:

```bash
cargo doc --workspace --no-deps --open
```

---

## PyO3-Exposed Methods

The `rustvello-python` crate exposes Rust APIs to Python via PyO3.
The methods below are the operational surface used by the pynenc adapters.

### Composite Methods (Rustvello App)

These methods execute multi-step orchestration flows in Rust (single FFI boundary)
and are used by pynenc's native orchestrator path.

| Method                                                               | Description                                                 |
| -------------------------------------------------------------------- | ----------------------------------------------------------- |
| `set_invocation_status(inv_id, status, runner_id)`                   | Status transition + history + trigger + waiter side-effects |
| `register_invocations(batch, runner_id)`                             | Batch registration + broker routing + history               |
| `set_invocation_result(inv_id, serialized_result, runner_id)`        | Persist result + success transition + downstream updates    |
| `set_invocation_exception(inv_id, error_type, error_msg, runner_id)` | Persist error + failure/retry transition                    |
| `set_invocation_retry(inv_id, runner_id)`                            | Retry transition + re-enqueue                               |
| `get_invocations_to_run(max_num, runner_id)`                         | Pull runnable invocations for execution                     |
| `route_call(call)`                                                   | Task submission in one call                                 |
| `reroute_invocations(inv_ids, runner_id)`                            | Requeue invocations for execution                           |
| `trigger_loop_iteration(runner_id)`                                  | Evaluate all trigger conditions                             |
| `check_atomic_services(runner_id, ...)`                              | Heartbeats + recovery + trigger window checks               |

### Query Methods

Methods exposed for querying invocation and runner state:

| Method                                                                 | Description                                           |
| ---------------------------------------------------------------------- | ----------------------------------------------------- |
| `get_invocation_status(inv_id)`                                        | Get current status record                             |
| `get_invocations_by_status(status)`                                    | Query invocations by status                           |
| `get_invocations_by_status(status, task_module?, task_name?)`          | Query invocations by status with optional task filter |
| `get_invocations_by_task(task_id)`                                     | Query invocations by task                             |
| `get_invocations_by_call(call_id)`                                     | Query invocations by call ID                          |
| `count_invocations(task_module?, task_name?, statuses?)`               | Count invocations matching filters                    |
| `get_invocation_ids_paginated(..., limit, offset)`                     | Paginated invocation listing                          |
| `get_existing_invocations(task_module, task_name, statuses, cc_args?)` | Existing invocation lookup for CC                     |
| `get_blocking_invocations(max_num)`                                    | Fetch invocations currently blocked on dependencies   |
| `filter_by_status(inv_ids, statuses)`                                  | Filter a set of invocation IDs by status              |

### Lifecycle Methods

| Method                                                     | Description                                       |
| ---------------------------------------------------------- | ------------------------------------------------- |
| `init_logging(...)`                                        | Initialize Rust logging subscriber from Python    |
| `get_active_runner_ids(timeout_seconds)`                   | List active runners from heartbeat data           |
| `get_stale_pending_invocations(max_pending_seconds)`       | List stale pending invocations                    |
| `get_stale_running_invocations(runner_dead_after_seconds)` | List stale running invocations                    |
| `record_atomic_service_execution(...)`                     | Persist atomic service execution windows          |
| `run_auto_purge(max_age_secs)`                             | Purge finalized invocations past retention window |
| `register_heartbeat(runner_id)`                            | Publish runner heartbeat                          |

---

## Exception Classes

PyO3-exposed exceptions that map from `RustvelloError` variants:

| Python Exception           | Rust Variant                              | Raised When                                    |
| -------------------------- | ----------------------------------------- | ---------------------------------------------- |
| `RetryError`               | `Retry`                                   | Task explicitly requests retry                 |
| `ConcurrencyRetryError`    | `ConcurrencyRetry`                        | Concurrency policy triggers retry              |
| `InvocationNotFoundError`  | `InvocationNotFound`                      | Invocation ID does not exist in the backend    |
| `StatusTransitionError`    | `InvalidStatusTransition`                 | Status transition violates FSM rules           |
| `StatusOwnershipError`     | `OwnershipViolation`                      | Runner ownership rules are violated            |
| `StatusRaceConditionError` | `StatusRaceCondition`                     | Optimistic status update raced                 |
| `SerializationError`       | `Serialization`                           | JSON serialization/deserialization failed      |
| `TaskNotFoundError`        | `TaskNotFound`                            | Task ID does not exist                         |
| `TaskNotRegisteredError`   | `TaskNotRegistered`                       | Task exists but not registered in this app     |
| `ConfigurationError`       | `Configuration`                           | Configuration parsing or validation failed     |
| `StateBackendError`        | `Infrastructure`                          | Infrastructure/backend error (non-runner kind) |
| `RunnerError`              | `TaskExecution` / `Infrastructure(Other)` | Task callback or runner-side failure           |
| `InternalError`            | `Internal`                                | Internal engine error                          |

Exceptions in the `rustvello` module inherit from `RustvelloError`.
In pynenc-facing adapters, status exceptions are translated to pynenc
exceptions (`InvocationStatusTransitionError`, `InvocationStatusOwnershipError`) with
structured fields preserved.

---

## Python API (pynenc)

:::{admonition} See also: Pynenc Docs
:class: seealso
Python users interact with Pynenc functions and decorators, not these PyO3 boundaries directly. See the [Pynenc API Reference](https://pynenc.github.io/apidocs/index.html) for public interfaces available to Python apps.
:::

Python users interact with rustvello through [pynenc](https://docs.pynenc.org).
The `py-rustvello` wheel provides thin PyO3 wrappers but the user-facing Python
API lives in pynenc.

- **[pynenc Python API](https://docs.pynenc.org)** — decorators, builder, configuration
- **[py-rustvello on PyPI](https://pypi.org/project/py-rustvello/)** — Python wheel

---

## crates.io

| Crate                  | Link                                                                                   |
| ---------------------- | -------------------------------------------------------------------------------------- |
| `rustvello`            | [crates.io/crates/rustvello](https://crates.io/crates/rustvello)                       |
| `rustvello-proto`      | [crates.io/crates/rustvello-proto](https://crates.io/crates/rustvello-proto)           |
| `rustvello-core`       | [crates.io/crates/rustvello-core](https://crates.io/crates/rustvello-core)             |
| `rustvello-macros`     | [crates.io/crates/rustvello-macros](https://crates.io/crates/rustvello-macros)         |
| `rustvello-mem`        | [crates.io/crates/rustvello-mem](https://crates.io/crates/rustvello-mem)               |
| `rustvello-sqlite`     | [crates.io/crates/rustvello-sqlite](https://crates.io/crates/rustvello-sqlite)         |
| `rustvello-redis`      | [crates.io/crates/rustvello-redis](https://crates.io/crates/rustvello-redis)           |
| `rustvello-monitoring` | [crates.io/crates/rustvello-monitoring](https://crates.io/crates/rustvello-monitoring) |
| `rustvello-cli`        | [crates.io/crates/rustvello-cli](https://crates.io/crates/rustvello-cli)               |
