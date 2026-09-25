# Changelog

For detailed information on each version, please visit the [GitHub Releases page](https://github.com/pynenc/rustvello/releases).

## Unreleased

## 0.7.0 - 2026-09-25

- Native async tasks. `#[rustvello::task]` and `#[rustvello::workflow]` accept
  `async fn`: the runner awaits the body on its Tokio runtime without a blocking
  thread, bounded by `num_workers`. The invocation, runner and W3C trace contexts
  and the worker's tracing span follow the body across `.await` points.
  `Task`/`DynTask` gain `is_async` and `run_async`/`execute_async` with defaults,
  so existing implementations are unchanged; `block_on_task_future` serves the
  synchronous entry points. A panicking or aborted body fails the attempt
  (`TaskCancelled`) instead of hanging, and a dropped worker aborts its body so
  stale recovery can re-run the invocation. `blocking = true` on an `async fn` is
  a compile error.
- Python `@app.task` accepts `async def`. Each worker thread (or worker process)
  owns one reusable event loop, so the invocation context and OpenTelemetry context
  work unchanged. Tasks a coroutine leaves running are cancelled when it returns.
  `Invocation.result_async()` awaits a child without blocking the loop, and dev
  mode awaits async bodies inline. See [Async tasks](async_tasks.md).
- Retry backoff: `retry_delay_ms`, `retry_max_delay_ms`, `retry_backoff` and
  `retry_jitter` (equal by default; full or none) on `TaskConfig`, on the task
  macros and on Python `@app.task`/`@app.workflow` (seconds). Delayed retries are
  stored durably in the backend on SQLite and PostgreSQL. The in-memory backend
  keeps them only for the life of the process. Redis, MongoDB and RabbitMQ
  declare no support and retry immediately. A worker killed during the backoff
  loses nothing, and the retry fires once when due (kill test).
- Execution deadlines: `timeout_ms` / `timeout` fail an attempt with
  `TaskTimeoutError`. `retry_on_timeout` decides whether the attempt is
  retried. Rust async bodies are aborted, Python `async def` bodies are
  cancelled on their worker event loop (`CancelledError` at the next `await`)
  and process-pool workers are killed. Sync threads are abandoned and their
  late result is discarded.
- Cancellation: new terminal status `CANCELLED`, `RustvelloApp::cancel`,
  Python `Invocation.cancel()`/`App.cancel()` and `rustvello cancel`. Running
  attempts are abandoned within `cancellation_check_interval_seconds`, with
  the same per-body rules as deadlines.
- `AttemptSignal` / `current_attempt_signal()`: a sync task body can check
  whether the runner abandoned its attempt (deadline or cancel) and stop
  cooperatively, or register an `on_abandon` hook.
- The guarantee matrix (`/api/capabilities`, `docs/guarantees.md`) gains a
  _delayed retry_ column: guaranteed on SQLite and PostgreSQL (kill-tested),
  process-local (best effort) in memory, not supported on Redis, MongoDB and
  RabbitMQ. The release gate runs the delayed-retry kill tests
  (`make test-fault`, `make test-fault-postgres`).
- Defaults keep the previous behaviour, and task configs serialized before this
  release deserialize unchanged. See the "Retries, timeouts and cancellation"
  guide for the per-backend guarantees and the side-effect semantics.

## 0.6.0 - 2026-09-25

- Trigger firings can no longer be lost. A firing is claimed into a trigger
  outbox (claim, run record with its planned invocation, and consumed
  conditions, committed together on SQLite, PostgreSQL and memory), then
  published under an invocation id derived from the run id. The atomic
  service re-publishes claimed runs that a crashed process left unpublished,
  and re-publication is idempotent, so every firing yields exactly one logical
  invocation. Proven by process-kill suites at every boundary on SQLite and
  PostgreSQL and by fault tests on the fallback path.
- Trigger-run record errors now propagate instead of being logged and ignored.
  A run whose target task is not registered on the evaluating runner stays
  pending instead of failing the iteration.
- `TriggerStore` gains `claim_trigger_runs_with_records`,
  `get_pending_trigger_runs` and `atomic_trigger_claims` (with defaults);
  `TriggerRunRecord` gains `planned_invocation_id`; `TriggerExecution` gains
  `invocation_id`. Callers of `evaluate_trigger_runs` that publish invocations
  themselves must call `complete_trigger_run`, or the trigger loop publishes
  the run as well.
- Guarantee matrix per backend (atomic publication, trigger atomicity,
  stale-owner recovery, ordering, durability), served by `/api/capabilities`
  under `guarantees` and generated into the docs; a guaranteed cell must name
  its proving tests.
- One release gate, `release-gate.yml`, defines the guarantee-matrix check,
  the fault suites and the backend suites; `release-rust.yml`,
  `release-python.yml` and `backend-and-stress.yml` (weekly and on backend
  pull requests) all call it, so every suite runs once per trigger.
  `make test-fault` now includes the trigger kill and trigger fallback suites;
  `make test-fault-postgres` runs the PostgreSQL gate against an isolated
  database.
- PostgreSQL skips its schema DDL when the schema is already current
  (`rustvello_schema_version`), so a runner starting next to busy runners no
  longer takes table locks that could deadlock them. The compliance suite can
  reuse an existing server through `RUSTVELLO_POSTGRES_DSN`.

## 0.5.3 - 2026-09-25

- The README quick starts run as written: the Python one starts a worker (and
  shows `dev_mode_force_sync` for inline runs), the Rust one starts a runner and
  waits with `wait_timeout()` instead of `result()`. They live in
  `py-rustvello/examples/` and `crates/rustvello/examples/readme_quickstart.rs`;
  CI checks the README copies and runs them against the built wheel and crate.
- `make test` and PR CI run the SQLite fault-injection suites
  (`make test-fault`). The external-backend suite, now including the PostgreSQL
  network and process-kill gates, also runs on pull requests that touch a
  backend and gates every PyPI and crates.io release.
- One documentation host, Read the Docs, in the README, `pyproject.toml`,
  `Cargo.toml` and `rustvello info`; package descriptions, keywords and
  classifiers describe the task queue and workflow runtime. Broken external
  links fixed, and a link checker (`make links`, lychee) runs in CI.
- `docs/workflows.md` documents the standalone Python workflow API
  (`@app.workflow`, `workflow_root()`).

## 0.5.2 - 2026-09-17

- `RUSTVELLO__DEV_MODE_FORCE_SYNC` reaches an `App` built with an explicit
  `AppConfig`. `dev_mode_force_sync` defaults to unset and follows the resolved
  configuration, so a test suite can run every task inline through the environment
  alone. Previously such an app raised `ValueError` whenever the variable was set.
- The published wheel is built with the `dist-release` profile, cutting the
  extension from 56 MB to 35 MB.

## 0.5.1 - 2026-09-15

- Process-pool executor for Python task code (`SubprocessExecutor`,
  `ExecutorKind::Python`): `App.run(num_processes=N, queues=[...])` and the
  `python -m rustvello.worker module:app` launcher run one interpreter per worker
  under the Rust control plane.
- Standalone `App`: `backend="mongo3"`, `broker="rabbitmq"`, Mongo connection
  parts, `retry_for`, `AppConfig.from_env()/from_file()` with settable fields,
  runtime `dev_mode_force_sync`, `purge()`, `queue_depth()`, `get_task()`,
  `current_invocation()`, `wait_results()`, `start_monitor()`, `config`.
- Bindings: `start_monitor`/`MonitorServer`, `set_current_invocation_context`,
  `clear_current_invocation_context`, `get_current_task_key`,
  `TaskConfig.retry_for_errors`, `Rustvello.set_dev_mode_force_sync`, and the
  queue-aware broker methods (`route_invocation_to_queue`, ...).
- Failed invocations report `ErrorType: message` from the stored error.
- Added SQLite atomic publication and idempotent Rust/Python submission,
  explicit FULL/NORMAL synchronization, owner-fenced execution identity, and
  native Python recovery configuration.
- Preserved Python named-queue/priority routing through retries, rejected removed
  ID reuse, and fenced stale-worker concurrency cleanup. Extended crash coverage
  to permanent errors, terminal cleanup and original-carrier submission replay.
- Persisted execution identities and known retry links across Rust/Python worker
  processes; children inherit their parent's actual execution span.
- Bounded OTLP request bytes and exporter teardown, and added adverse per-signal
  delivery accounting tests.
- Added SQLite delivery reservations/atomic claim acknowledgement, stale-owner
  payload protection, strict app IDs and bounded runner shutdown.
- Added bounded native lifecycle capture and `rustvello-otel` OTLP telemetry module.
- Removed the former Prometheus crate, feature, dependencies, and release surface.

## 0.5.0 - 2026-09-04

- Made task language a closed Rust enum and a structural part of `TaskId`, with
  canonical `language::module.name` display and physical language queue isolation.
- Added typed foreign-task declarations for Rust and Python applications, plus
  cross-language calls, waits, triggers, and wrong-worker isolation tests.
- Added standalone Python workflow roots with `@app.workflow` and
  `workflow_root()` deterministic random, time, and UUID helpers, while keeping
  Pynenc as the full Python framework integration.
- Separated the concrete cross-backend `Orchestrator` from the atomic
  `InvocationControlBackend` persistence port, extracted `TaskCatalog`, split
  orchestration modules by use case, and removed the transitional coordinator
  and core-trait compatibility names.
- Unified runner lifecycle in `RunnerControlPlane`, introduced bounded Tokio
  and Rayon executors, and persisted runner language plus executor metadata for
  monitoring. Removed redundant per-invocation and always-blocking runner
  surfaces before the 0.5 release.
- Reworked task occupancy charts to group running work by actual runner
  language, show active-worker lines per runtime, and provide a hover-linked
  Rust-rendered legend table for per-bucket Rust/Python comparison.
- Tightened monitoring timelines with single-line worker labels, denser lane
  spacing, language-coloured lane backgrounds, and stable Rayon worker-slot
  identities so CPU pools render as bounded worker groups instead of one row per
  invocation.
- Modernized the timeline dashboard with collapsed top filters, wider shared
  runner/task label rails, full task names, per-runner colours with language
  badges, synchronized scrolling and time cursors, batched backend reads, and
  one relation path per invocation for substantially lower browser overhead.
- Refined timeline hierarchy and occupancy charts with parent-derived worker
  tones, ranked task colours, readable time ticks and bar spacing, navigable
  runner labels, strict load-fixture provenance, and visible atomic-service
  execution windows correlated with each owning runner group.
- Added failure-injection coverage for retrying caller-owned invocation IDs
  after broker publication failure.
- Added an ignored cross-language monitoring load fixture and `make
monitoring-load` to generate dashboard data with mixed Rust/Python task
  languages, workflows, triggers, logical queues, Tokio workers, and Rayon CPU
  workers, including multiple runner groups and atomic service evidence.
- Expanded event monitoring with matched/triggered page summaries, event and
  trigger-run timeline actions, and JSON trace endpoints for agentic
  investigation.
- Rust crates and Python packaging are version-aligned at `0.5.0`.

## 0.4.0 - 2026-09-01

- Added named logical queues and finite float priorities to every broker
  implementation under one mandatory shared contract.
- Added task queue/priority configuration, wildcard priority rules, multi-queue
  runner selection, Python bindings, and queue-aware lifecycle rerouting.
- RabbitMQ documents and tests its adapter-only normalization from
  `-100.0..100.0` floats to native `0..255` integer priorities.
- Broker monitoring now reports configured queues non-destructively instead of
  dequeueing and re-enqueueing messages for previews.
- MongoDB and MongoDB 3 now coordinate concurrency slots on standalone
  deployments; Redis status CAS no longer retries indefinitely; RabbitMQ
  confirms publishes and synchronizes selective scan requeues before reporting
  task-specific counts.
- Hardened atomic concurrency testing and fixed the cargo-fuzz CI toolchain.
- Rust crates and Python packaging are version-aligned at `0.4.0`.

## 0.3.1 - 2026-08-31

Rustvello is version-aligned with Pynenc `v0.3.1` for the committed sync scope
from Pynenc `v0.2.0` through `v0.3.1`. The release keeps Rustvello's simplified
Rust-first surface while matching the workflow, monitoring, trigger, backend,
and test contracts that matter for current Pynenc behavior.

### Release highlights

- Rust and Python packaging now share version `0.3.1`; the Python wheel derives
  its version from the Cargo workspace through Maturin.
- Architecture documentation now covers crate dependencies, invocation
  lifecycle, backend traits, Python integration, trigger/atomic coordination,
  monitoring flow, and workflow context with static SVG diagrams.
- Backend behavior is documented as required contracts, with backend constraints
  described separately from Rustvello feature switches.

### CLI and compatibility decisions

- Kept `rustvello status <INVOCATION_ID>` as the stable invocation inspector;
  the status FSM remains a checked-in architecture artifact instead of adopting
  Pynenc's unrelated `status render` command tree.
- Kept Rust task discovery compile-time through `inventory`; no runtime Python
  app scanner or module hydration was added.
- Documented that Pynenc's `direct_task` splitter/aggregator is outside the
  simplified runner surface; native Rust and standalone Python use invocation
  handles, with synchronous development mode available for local tests.
- Added a test that the loaded Python extension version matches the Cargo
  workspace version.

### Backend contracts and reliability

- Running concurrency now uses the dedicated `running_concurrency`
  configuration instead of registration concurrency.
- Full orchestrator implementations record atomic-service timelines and
  auto-purge schedules; these are mandatory shared-suite contracts.
- Full trigger stores persist event evidence and trigger-run evidence, support
  filtered monitoring queries, and purge those records with backend data.
- Broker implementations preserve task routing, language routing, global
  fallback, batch delivery, queue counts, and purge behavior under the same
  shared tests.
- SQLite now implements the same local single-node backend behavior as the
  other full backends; RabbitMQ remains a broker-only implementation.
- Worker and runner paths include additional hardening around terminal status,
  recovery, context propagation, and signal-safe shutdown.

### Trigger and event monitoring

- Added durable event and trigger-run DTOs with condition, event, source
  invocation, and produced-invocation attribution.
- All full trigger stores now support filtered monitoring queries and purge
  their monitoring records; monitoring evidence is a required contract.
- Trigger execution now preserves unmatched events and links claimed runs and
  participating events to the invocation produced by the run.
- Added dashboard event list/detail and trigger-run detail views, including
  bounded links into the invocation timeline.
- Kept Pynmon-specific plugins, trigger mutation views, module hydration, and
  provider discovery outside Rustvello's product boundary.

### Workflow migration

- Added explicit Rust workflow roots with `#[rustvello::workflow]`.
- Ordinary top-level tasks no longer receive an implicit workflow identity.
- Replaced `force_new_workflow` with the internal `is_workflow_task` marker.
- Restricted deterministic random, time, and UUID operations to
  `WorkflowRoot::current()` in a workflow-defining invocation; invalid access
  returns typed workflow errors.
- Workflow tasks called from another workflow now define subworkflows, while
  ordinary child tasks inherit the caller's workflow.
- Monitoring labels workflow-defining invocations in lists, details, and JSON.

Rust users should replace `#[rustvello::task(force_new_workflow = true)]` with
`#[rustvello::workflow]`. Standalone Python uses `@app.workflow`, and Pynenc
adapters may set the same low-level workflow marker when translating Pynenc's
explicit workflow decorator.

### Monitoring dashboard

- Added task occupancy histograms to invocation timelines, workflow-run
  comparisons, and the Log Explorer, with aligned SVG buckets, task-colored
  stacks and legends, status selectors, hover context, and filtered invocation
  drill-down links.

- Timeline views include invocation scope, workflow type, and workflow ID
  filters against persisted invocation data.
- Drag-to-zoom links preserve active filters and use explicit UTC bounds from
  the rendered SVG.
- Event and trigger-run pages link into bounded invocation timeline windows.
- The log explorer resolves structured invocation and runner references,
  including shortened IDs from Rustvello's log context format.
- Status badges and timeline status-history rendering match current invocation
  semantics.
- Timeline SVGs use responsive intrinsic height so details appear directly
  below the graph.
- Monitoring integration tests provide a single-server `KEEP_ALIVE=1` browser
  inspection mode.

### Testing and CI

- Added backend and stress CI lanes for shared contracts and contention-heavy
  scenarios.
- Expanded shared backend test coverage for broker, orchestrator, trigger,
  atomic-service, auto-purge, and monitoring evidence behavior.
- Added SQLite stress tests for task routing and contention scenarios.
- Added Python compatibility checks for the declared `>=3.9,<4.0` support
  range, plus version-alignment tests inspired by mature Rust/Python projects.
- Kept Docker-backed Redis, PostgreSQL, MongoDB, MongoDB 3, and RabbitMQ tests
  available for backend-specific contract validation.

## 0.1.0 - 2026-04-05

Initial public release.

### Crates

- **rustvello-proto** - Data transfer objects and wire types
- **rustvello-core** - Trait definitions for broker, invocation control, state backend, runner
- **rustvello-macros** - Derive macros for task registration
- **rustvello-mem** - In-memory backend (testing / single-process)
- **rustvello-sqlite** - SQLite-backed backend
- **rustvello-postgres** - PostgreSQL-backed backend
- **rustvello-redis** - Redis-backed backend
- **rustvello-mongo** - MongoDB-backed backend (driver v3)
- **rustvello-mongo3** - MongoDB-backed backend (driver v2 legacy)
- **rustvello-rabbitmq** - RabbitMQ broker
- **rustvello-prometheus** - Prometheus metrics exporter
- **rustvello** - Main crate: runners, middleware, scheduling
- **rustvello-monitoring** - Web dashboard
- **rustvello-cli** - Command-line interface
- **rustvello-python** - PyO3 FFI bindings
- **py-rustvello** - Python wheel (maturin / PyPI)

### Highlights

- All PyO3 wrappers (`PyMem*`, `PySqlite*`, `PyPostgres*`, `PyRedis*`,
  `PyMongo*`, `PyMongo3*`) expose identical method signatures.
- Native orchestrator mode is the default.
- Per-backend extras in pynenc: `pynenc[mem]`, `pynenc[sqlite]`,
  `pynenc[postgres]`, `pynenc[redis]`, `pynenc[mongo]`, `pynenc[rabbitmq]`,
  `pynenc[all-backends]`.
- 4× test parametrization across `py-mem`, `py-sqlite`, `rust-mem`, and
  `rust-native` variants.
- Error equivalence tests (41 parametrized hierarchy tests).
- Integration test containers for PostgreSQL, Redis, MongoDB, and RabbitMQ.
- Broker per-task and language APIs exposed via PyO3 for all backends.
- Supported runner implementations: `PersistentTokioRunner` and `RayonRunner`.
- Trigger system: status, result, exception, event, and cron conditions.
- Atomic service with crash-recovery loop.
- Cross-language architecture support (Python ↔ Rust workers).
- Monitoring dashboard with SVG timelines, family tree visualization, log explorer.
- Zero clippy warnings, zero unsafe code.
