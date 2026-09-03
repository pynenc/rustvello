# Changelog

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
