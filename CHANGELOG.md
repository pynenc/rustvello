# Changelog

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
- Split the concrete cross-backend `Orchestrator` from the atomic
  `InvocationControlBackend` persistence port, extracted `TaskCatalog`, and
  organized orchestration modules by submission, routing, dispatch, retrieval,
  maintenance, and triggers.
- Removed deprecated coordinator/core-trait compatibility names and the
  redundant per-invocation and always-blocking runner surfaces.
- Unified runner lifecycle in `RunnerControlPlane`, introduced bounded Tokio
  and Rayon executors, and persisted runner language plus executor metadata for
  monitoring.
- Expanded monitoring to show canonical task IDs, task language, runner
  language, executor kind, Python/Rust logo-colour badges, cross-language
  timelines, and richer status-history detail.
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
- Expanded event monitoring with matched/triggered page summaries, event and
  trigger-run timeline actions, and JSON trace endpoints for agentic
  investigation.
- Added failure-injection coverage for retrying caller-owned invocation IDs
  after broker publication failure.
- Added `make monitoring-load` and an ignored monitoring load fixture that
  generates mixed Rust/Python workflow, trigger, queue, Tokio, and Rayon data
  for dashboard development, including multiple runner groups and atomic
  service evidence.
- Rust crates and Python packaging are version-aligned at `0.5.0`.

## 0.4.0 - 2026-09-01

- Added monitoring occupancy histograms for invocation timelines, selected
  workflow runs, and log-scoped invocations, including status selectors and
  time-filtered drill-down links.
- Added required named-queue and finite float-priority behavior to every broker,
  with priority-first/FIFO-tie retrieval and shared backend contract tests.
- Added task queue/priority macro attributes, app and Python configuration,
  broker wildcard priority rules, and ordered, random, or round-robin runner
  queue selection.
- Kept the public priority model at `-100.0..100.0`; RabbitMQ alone maps it
  lossily to AMQP's native `0..255` integer levels.
- Made submission, retries, recovery, trigger execution, and coordinator
  dispatch queue-aware, and changed monitoring to report configured queue
  counts without dequeueing and reordering messages.
- Hardened real backend behavior: MongoDB and MongoDB 3 now coordinate
  concurrency slots on standalone deployments; Redis status CAS no longer
  retries indefinitely; RabbitMQ confirms publishes and waits for selective
  scan requeues before reporting task-specific counts.
- Made the cross-runner concurrency test deterministic and expanded the shared
  suite for atomic task-level empty-argument reservations.
- Fixed fuzz CI by installing `cargo-fuzz` with the nightly toolchain already
  used to run fuzz targets. Rust crates and the Python wheel now share `0.4.0`.

## 0.3.1 - 2026-08-31

Rustvello is now version-aligned with Pynenc `v0.3.1` for the committed sync
scope from Pynenc `v0.2.0` through `v0.3.1`. This release keeps Rustvello's
simplified Rust-first architecture while adopting the compatibility, workflow,
monitoring, trigger, backend-contract, and test-hardening work needed to match
that Pynenc line.

### Highlights

- Added explicit Rust workflow roots with `#[rustvello::workflow]`, workflow
  identity propagation, root-only deterministic operations, and migration docs
  for replacing `force_new_workflow`.
- Hardened invocation status semantics, including checked status graph docs,
  terminal concurrency-controlled states, and stricter transition coverage.
- Added durable trigger and event monitoring records, event list/detail pages,
  trigger-run detail pages, and timeline links from trigger evidence.
- Made backend contracts non-optional for full implementations: trigger
  evidence, atomic-service timelines, auto-purge, task/language broker routing,
  queue counts, and purge behavior are covered by shared suites.
- Completed SQLite's local full-backend behavior and documented backend facts
  separately from product capability switches.
- Improved monitoring timelines with invocation and workflow filters, drag
  range zoom, richer references, corrected status-history rendering, compact
  responsive SVG layout, and a single-server keep-alive test mode.
- Hardened Python packaging and bindings with Python `>=3.9,<4.0` metadata,
  version-alignment tests, typed stubs, and standalone developer-experience
  coverage.
- Expanded test and CI coverage with backend/stress workflows, shared backend
  compliance tests, Docker-backed contract tests, SQLite stress tests, Python
  compatibility checks, and full Rust/Python quality gates.
- Added project architecture documentation and static SVG diagrams for crate
  dependencies, invocation flow, backend traits, Python integration,
  trigger/atomic coordination, monitoring, and workflow context.

### Notes

- Runtime Python app/module discovery from Pynenc was not copied into
  Rustvello; Rust task discovery remains compile-time through `inventory`.
- Pynenc runner/plugin breadth and Pynmon mutation/provider views remain outside
  Rustvello's simplified product surface.
- The Python wheel version is derived from the Cargo workspace version through
  Maturin, so Rust crates and Python packaging share version `0.3.1`.

## 0.1.0 - 2026-04-05

Initial public release of the Rustvello workspace.

### Crates

- **rustvello-proto** - Data transfer objects and wire types
- **rustvello-core** - Trait definitions for broker, orchestrator, state backend, runner
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
- **rustvello-monitoring** - Web dashboard for invocations, timelines, log explorer
- **rustvello-cli** - Command-line interface
- **rustvello-python** - PyO3 FFI bindings
- **py-rustvello** - Python wheel (maturin / PyPI)

### Highlights

- Native orchestrator mode.
- In-memory, SQLite, Redis, PostgreSQL, MongoDB, MongoDB 3, and RabbitMQ backend crates.
- Trigger system with status, result, exception, event, and cron conditions.
- Atomic service with crash-recovery loop.
- Cross-language architecture support for Python and Rust workers.
- Monitoring dashboard with SVG timelines, family tree visualization, and log explorer.
- Shared backend integration tests and zero unsafe code.
