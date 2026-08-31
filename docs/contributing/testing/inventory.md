# Test Inventory

Current coverage structure for Rustvello. Exact test counts are reported by the
test runner and are intentionally not duplicated here because the shared suites
expand differently for each backend implementation.

## Summary

| Category                     | Coverage                                                                                           |
| ---------------------------- | -------------------------------------------------------------------------------------------------- |
| Inline unit tests            | Internal modules across workspace crates                                                           |
| Shared compliance functions  | Broker, orchestrator, state, trigger, client-data, lifecycle, isolation, and concurrency contracts |
| Backend suite instantiations | Memory, SQLite, Redis, PostgreSQL, MongoDB, MongoDB 3, and RabbitMQ (broker-only)                  |
| Integration tests            | Cross-component runner, workflow, trigger, monitoring, CLI, and binding behavior                   |
| Property tests               | Serde, status graphs, argument/concurrency keys, trigger filters, and workflow histories           |
| Fuzz targets                 | JSON trigger and TOML configuration parsing                                                        |
| Stress/soak tests            | Fast in-memory contention plus ignored high-volume runner and SQLite persistence tests             |

## Backend Compliance Suite

Shared test functions are grouped by contract module:

| Module              | What it verifies                                                                                      |
| ------------------- | ----------------------------------------------------------------------------------------------------- |
| `broker`            | Message routing, FIFO ordering, per-task isolation, batch routing, language queues, and purge         |
| `orchestrator`      | Registration, status transitions, queries, pagination, concurrency, atomic-service history, and purge |
| `state_backend`     | State upsert/get, call retrieval, result/error storage, history, and purge                            |
| `trigger`           | Condition registration, events, cron, lifecycle, optimistic locking, deduplication, and purge         |
| `client_data_store` | Store/retrieve, missing keys, upserts, multiple keys, large values, and backend name                  |
| `lifecycle`         | Success/failure lifecycle, multiple invocations, purge-all, and component consistency                 |
| `isolation`         | Application namespace isolation for persistent/distributed backends                                   |
| `concurrency`       | Registration and running concurrency policies and atomic slot acquisition                             |

## Backend Instantiation Matrix

| Backend    | Style         | Docker | Coverage                                                                            |
| ---------- | ------------- | :----: | ----------------------------------------------------------------------------------- |
| Memory     | Sync macros   |   No   | Complete local backend contracts plus concurrency                                   |
| SQLite     | Manual wiring |   No   | Complete local backend contracts plus isolation, concurrency, and persistent stress |
| Redis      | Async macros  |  Yes   | Complete distributed backend contracts plus isolation and concurrency               |
| MongoDB    | Async macros  |  Yes   | Complete distributed backend contracts plus isolation and concurrency               |
| MongoDB 3  | Async macros  |  Yes   | Complete legacy distributed backend contracts plus isolation and concurrency        |
| PostgreSQL | Async macros  |  Yes   | Complete distributed backend contracts plus isolation and concurrency               |
| RabbitMQ   | Async macros  |  Yes   | Complete Broker contract                                                            |

## Inline Unit Tests by Crate

| Crate                  | Modules | Notable coverage                                                                            |
| ---------------------- | ------: | ------------------------------------------------------------------------------------------- |
| `rustvello-core`       |       9 | Context, serializer, call, workflow, trigger, task, client data store, error, observability |
| `rustvello-proto`      |       6 | Call, invocation, status, trigger, identifiers, config                                      |
| `rustvello-monitoring` |      13 | Parser, SVG rendering, builder, color, bounds, CSRF, time range, lane assignment            |
| `rustvello`            |       5 | Trigger builder, app builder, persistent/per-invocation tokio runners                       |
| `rustvello-mem`        |       5 | All 5 trait implementations                                                                 |
| `rustvello-sqlite`     |       5 | All 5 trait implementations                                                                 |
| `rustvello-redis`      |       3 | Broker, orchestrator, connection                                                            |
| `rustvello-mongo`      |       2 | Connection, orchestrator                                                                    |
| `rustvello-rabbitmq`   |       1 | Broker                                                                                      |
| `rustvello-prometheus` |       1 | Sink                                                                                        |
| `rustvello-python`     |       2 | Utils, error                                                                                |
| `rustvello-cli`        |       0 | Tests in integration file only                                                              |

## Integration Tests (Main Crate)

| File                        | Lines | Tests | Focus                                    |
| --------------------------- | ----: | ----: | ---------------------------------------- |
| `typed_task_tests.rs`       | 1,398 |   ~30 | Task type ergonomics, compilation checks |
| `trigger_tests.rs`          |   502 |   ~12 | Trigger system end-to-end                |
| `app_integration_tests.rs`  |   435 |    11 | Full application lifecycle               |
| `combination_tests.rs`      |   408 |     8 | Backend × runner × serializer matrix     |
| `runner_hardening_tests.rs` |   317 |     8 | Runner edge cases, error recovery        |
| `workflow_tests.rs`         |   300 |   ~14 | Multi-step workflow orchestration        |
| `runner_context_tests.rs`   |   261 |     4 | Runner context and spans                 |
| `runner_span_tests.rs`      |   190 |    ~4 | Tracing span propagation                 |
| `discovery_tests.rs`        |   174 |    ~6 | Task discovery and registration          |

## Other Integration Tests

| Crate                  | File                      | Focus                      |
| ---------------------- | ------------------------- | -------------------------- |
| `rustvello-test-suite` | `all_tests_validator.rs`  | Completeness guarantee     |
| `rustvello-monitoring` | `monitoring_dashboard.rs` | Full HTTP dashboard        |
| `rustvello-cli`        | `cli_tests.rs`            | CLI argument parsing       |
| `rustvello-proto`      | `proptest_roundtrips.rs`  | Property-based serde tests |

## Known Limits

| Area                            | Status                  | Notes                                                                                  |
| ------------------------------- | ----------------------- | -------------------------------------------------------------------------------------- |
| Docker backend tests            | Scheduled/manual        | `.github/workflows/backend-and-stress.yml` runs ignored suites against real containers |
| Python bindings                 | Default CI              | PyO3 extension is built before pytest on supported Python versions                     |
| Persistent contention           | SQLite covered          | Route/retrieve, claim, concurrency slot, and recovery tests run in the soak lane       |
| Distributed stress              | Backend compliance only | Longer contention campaigns remain follow-up work for service-backed implementations   |
| Exhaustive concurrency modeling | Not implemented         | Stress tests exercise contention, but the project does not yet use `loom`              |
| SQLite broker routing           | Covered                 | Local SQLite still exercises task-aware, language-aware, and global routing            |
| Atomic-service history          | Covered                 | Every orchestrator persists and queries bounded history through its native backend     |

See {doc}`backend-constraints` for backend facts and the normative contract rule.
