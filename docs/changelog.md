# Changelog

For detailed information on each version, please visit the [GitHub Releases page](https://github.com/pynenc/rustvello/releases).

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
`#[rustvello::workflow]`. Standalone Python keeps ordinary task semantics;
Pynenc adapters may set the low-level workflow marker when translating Pynenc's
explicit workflow decorator.

### Monitoring dashboard

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
- Four runner implementations: `RayonRunner`, `PerInvocationTokioRunner`,
  `PersistentTokioRunner`, `ProcessRunner`.
- Trigger system: status, result, exception, event, and cron conditions.
- Atomic service with crash-recovery loop.
- Cross-language architecture support (Python ↔ Rust workers).
- Monitoring dashboard with SVG timelines, family tree visualization, log explorer.
- Zero clippy warnings, zero unsafe code.
