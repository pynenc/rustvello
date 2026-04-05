# Test Inventory

Current test counts and coverage status for rustvello v0.1.0.

## Summary

| Category                                  |                              Count |
| ----------------------------------------- | ---------------------------------: |
| Inline unit test modules (`#[cfg(test)]`) |                                 53 |
| Shared test functions (test-suite)        |                                 51 |
| Backend suite instantiations              |                         7 backends |
| Integration test files                    | 9 (main crate) + 11 (other crates) |
| Property-based tests (proptest)           |                                 8+ |
| Fuzz targets                              |                                  2 |
| Benchmark groups (criterion)              |                   2 (6 benchmarks) |

## Backend Compliance Suite

51 shared test functions across 7 modules:

| Module              | Tests | What it verifies                                                                                            |
| ------------------- | ----: | ----------------------------------------------------------------------------------------------------------- |
| `broker`            |    11 | Message routing, FIFO ordering, per-task isolation, batch routing, language-specific queues, purge          |
| `orchestrator`      |    10 | Invocation registration, status transitions, queries by task/status, pagination, concurrency control, purge |
| `state_backend`     |     7 | State upsert/get, call retrieval, result/error storage, history, purge                                      |
| `trigger`           |    11 | Condition registration, event/cron conditions, trigger lifecycle, optimistic locking, dedup, purge          |
| `client_data_store` |     7 | Store/retrieve, missing key errors, upsert semantics, multiple keys, large values, backend name             |
| `lifecycle`         |     5 | Full success/failure lifecycle, multiple invocations, purge-all, broker-orchestrator consistency            |

## Backend Instantiation Matrix

| Backend  | Style         | Docker |          Suites |
| -------- | ------------- | :----: | --------------: |
| mem      | Sync macros   |   No   |               6 |
| sqlite   | Manual wiring |   No   |               6 |
| redis    | Async macros  |  Yes   |               6 |
| mongo    | Async macros  |  Yes   |               6 |
| mongo3   | Async macros  |  Yes   |               6 |
| postgres | Async macros  |  Yes   |               6 |
| rabbitmq | Async macros  |  Yes   | 1 (broker only) |

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

## Known Gaps

| Area                       | Status      | Notes                                                 |
| -------------------------- | ----------- | ----------------------------------------------------- |
| Docker tests in CI         | ⚠️ Skipped  | GitHub Actions lacks Docker service containers        |
| Python FFI tests           | ⚠️ Broken   | `pyo3::prepare_freethreaded_python()` not called      |
| Multi-threaded tokio tests | ⚠️ Missing  | No `#[tokio::test(flavor = "multi_thread")]` anywhere |
| Concurrency verification   | ⚠️ Missing  | No `loom` integration                                 |
| SQLite macro migration     | ℹ️ Optional | Still uses manual wiring (works fine, just verbose)   |
