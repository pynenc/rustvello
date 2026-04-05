# Changelog

## 0.1.0 — 2026-04-05

Initial public release of the Rustvello workspace.

### Crates

- **rustvello-proto** — Data transfer objects and wire types
- **rustvello-core** — Trait definitions for broker, orchestrator, state backend, runner
- **rustvello-macros** — Derive macros for task registration
- **rustvello-mem** — In-memory backend (testing / single-process)
- **rustvello-sqlite** — SQLite-backed backend
- **rustvello-postgres** — PostgreSQL-backed backend
- **rustvello-redis** — Redis-backed backend
- **rustvello-mongo** — MongoDB-backed backend (driver v3)
- **rustvello-mongo3** — MongoDB-backed backend (driver v2 — legacy)
- **rustvello-rabbitmq** — RabbitMQ broker
- **rustvello-prometheus** — Prometheus metrics exporter
- **rustvello** — Main crate: runners (Rayon, Tokio, Process, Persistent), middleware, scheduling
- **rustvello-monitoring** — Web dashboard for invocations, timelines, log explorer
- **rustvello-cli** — Command-line interface
- **rustvello-python** — PyO3 FFI bindings
- **py-rustvello** — Python wheel (maturin / PyPI)

### Highlights

- All PyO3 wrappers (`PyMem*`, `PySqlite*`, `PyPostgres*`, `PyRedis*`,
  `PyMongo*`, `PyMongo3*`) expose identical method signatures — full backend parity.
- Native orchestrator mode is the default.
- Per-backend extras in pynenc: `pynenc[mem]`, `pynenc[sqlite]`,
  `pynenc[postgres]`, `pynenc[redis]`, `pynenc[mongo]`, `pynenc[rabbitmq]`,
  `pynenc[all-backends]`.
- 4× test parametrization across `py-mem`, `py-sqlite`, `rust-mem`, and
  `rust-native` variants.
- Error equivalence tests: 41 parametrized hierarchy tests (20 pynenc +
  21 rustvello exception classes) plus Rust↔Python error mapping validation.
- Integration test containers for PostgreSQL, Redis, MongoDB, and RabbitMQ
  using testcontainers. 61 cross-backend test methods (183 executions).
- Broker per-task and language APIs exposed via PyO3 for all backends.
- Four runner implementations: `RayonRunner`, `PerInvocationTokioRunner`,
  `PersistentTokioRunner`, `ProcessRunner`.
- Task middleware pipeline with pre/post hooks.
- Trigger system: status, result, exception, event, and cron conditions.
- Atomic service with crash-recovery loop.
- Cross-language architecture support (Python ↔ Rust workers).
- Monitoring dashboard with SVG timelines, family tree visualization, log explorer.
- `rustvello-test-suite` provides 65 shared integration tests per backend.
- Zero clippy warnings, zero unsafe code.
