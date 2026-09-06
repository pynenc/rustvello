# Test Architecture

This document describes the design and architecture of the rustvello test suite,
covering the shared test library, macro system, test completeness validator, and
how different testing layers fit together.

## Design Principles

The test architecture follows these principles inspired by major Rust projects
(tokio, serde, diesel):

- **Backend-agnostic test logic**: Test functions are written once against trait
  interfaces (`dyn Broker`, `dyn InvocationControlBackend`, etc.), not concrete types.
- **Macro-driven instantiation**: Each backend uses a single macro call to
  generate the full test suite, eliminating copy-paste boilerplate.
- **Compile-time completeness guarantee**: A validator test ensures every shared
  test function is consumed by at least one backend or macro.
- **Layered coverage**: Unit tests inside source files, compliance suites at the
  trait level, integration tests at the application level, and property/fuzz
  tests for robustness.

## The `rustvello-test-suite` Crate

The `rustvello-test-suite` crate (`crates/rustvello-test-suite/`) is a
**dev-dependency-only** library that holds all shared test functions and macros.
It is never published or used at runtime.

### Module Structure

```text
rustvello-test-suite/src/
├── lib.rs                  # Module declarations
├── helpers.rs              # Utility functions (ID generation, test task IDs)
├── broker.rs               # 11 test functions + broker_suite! + async_broker_suite!
├── orchestrator.rs         # 10 test functions + orchestrator_suite! + async_orchestrator_suite!
├── state_backend.rs        # 7 test functions + state_backend_suite! + async_state_backend_suite!
├── trigger.rs              # 11 test functions + trigger_suite! + async_trigger_suite!
├── client_data_store.rs    # 7 test functions + client_data_store_suite! + async_client_data_store_suite!
└── lifecycle.rs            # 5 test functions + lifecycle_suite! + async_lifecycle_suite!
```

### Shared Test Functions

Each module exports `pub async fn test_*()` functions that accept a trait object
reference and exercise one specific behavior:

```rust
// In rustvello-test-suite/src/broker.rs
pub async fn test_route_and_retrieve(broker: &dyn Broker) {
    // Setup: route an invocation
    let id = InvocationId::new("test_task", "inv_1");
    broker.route_invocation(&id).await.unwrap();

    // Verify: retrieve it back
    let retrieved = broker.retrieve_invocation(None).await.unwrap();
    assert_eq!(retrieved, Some(id));
}
```

This pattern means the test logic is written **exactly once** and reused across
all 6 backend implementations (mem, sqlite, redis, mongo, postgres, rabbitmq).

### Test Suite Macros

Each module provides two macro variants:

| Macro                         | Use case                                           | Test annotation                                    |
| ----------------------------- | -------------------------------------------------- | -------------------------------------------------- |
| `broker_suite!($setup)`       | In-process backends (mem, sqlite)                  | `#[tokio::test]`                                   |
| `async_broker_suite!($setup)` | Docker backends (redis, mongo, postgres, rabbitmq) | `#[tokio::test]` + `#[ignore = "requires Docker"]` |

**Sync macros** expect `$setup` to be a synchronous expression that returns the
backend directly:

```rust
// mem/tests/suite.rs
mod broker_suite {
    use rustvello_mem::broker::MemBroker;
    rustvello_test_suite::broker_suite!(MemBroker::new());
}
```

**Async macros** expect `$setup` to be an async expression that returns
`(guard, backend)` — the guard (typically `ContainerAsync<T>`) keeps the Docker
container alive via RAII:

```rust
// redis/tests/suite.rs
mod broker_suite {
    use super::*;
    rustvello_test_suite::async_broker_suite!(make_broker());
}
```

### Macro Expansion Example

The `async_broker_suite!` macro expands to:

```rust
#[tokio::test]
#[ignore = "requires Docker"]
async fn suite_broker_route_and_retrieve() {
    let (_c, broker) = make_broker().await;
    $crate::broker::test_route_and_retrieve(&broker).await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn suite_broker_retrieve_empty() {
    let (_c, broker) = make_broker().await;
    $crate::broker::test_retrieve_empty(&broker).await;
}
// ... 9 more tests
```

The `_c` binding is critical — dropping the container guard would stop the
Docker container mid-test.

## All-Tests Validator

The file `crates/rustvello-test-suite/tests/all_tests_validator.rs` contains a
single test: `all_shared_tests_are_consumed()`.

This test:

1. **Parses** all 6 test-suite source modules to extract every `pub async fn test_*` name
2. **Scans consumers** — checks `rustvello-mem/tests/suite.rs` and
   `rustvello-sqlite/tests/suite.rs` for direct references like `::test_route_and_retrieve(`
3. **Scans macros** — checks for `$crate::module::test_*` patterns inside
   `macro_rules!` blocks
4. **Asserts** every shared function is either directly called by a consumer or
   present in a macro expansion
5. **Sanity check**: verifies at least 30 shared test functions exist

If a developer adds a new `test_*` function to a suite module but forgets to
add it to the corresponding macro, this test fails at compile time.

This approach is unique among Rust projects — most rely on convention alone. It
mirrors the Python `test_all_tests_for_plugins.py` pattern from pynenc.

## Test Layers

### Layer 1: Inline Unit Tests

```text
53 #[cfg(test)] modules across all crates
```

These test internal module logic: serialization, connection handling, query
building, etc. They live alongside the code they test:

```rust
// In rustvello-proto/src/status.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_statuses_have_no_transitions() {
        // ...
    }
}
```

### Layer 2: Backend Compliance Suites

```text
51 shared test functions × 6 backends = up to 306 test instances
```

The `rustvello-test-suite` crate ensures every backend implementation correctly
implements the trait contracts. This is the Rust equivalent of parameterized
fixtures in pytest.

### Layer 3: Integration Tests

The main `rustvello` crate has 9 integration test files (3,985 lines) testing
cross-component behavior:

| File                        | Focus                                |
| --------------------------- | ------------------------------------ |
| `typed_task_tests.rs`       | Task type ergonomics and compilation |
| `trigger_tests.rs`          | Trigger system end-to-end            |
| `app_integration_tests.rs`  | Full application lifecycle           |
| `combination_tests.rs`      | Backend × runner × serializer matrix |
| `runner_hardening_tests.rs` | Runner edge cases and error recovery |
| `workflow_tests.rs`         | Multi-step workflow orchestration    |
| `runner_context_tests.rs`   | Runner context and tracing           |
| `runner_span_tests.rs`      | Tracing span propagation             |
| `discovery_tests.rs`        | Task discovery and registration      |

### Layer 4: Property & Fuzz Tests

- **proptest** (`rustvello-proto/tests/proptest_roundtrips.rs`): Verifies serde
  roundtrip invariants and status transition graph properties
- **libfuzzer** (`fuzz/fuzz_targets/`): Fuzzes JSON trigger parsing and TOML
  config parsing for panic-freedom

### Layer 5: Benchmarks

- **criterion** (`rustvello/benches/`): Micro-benchmarks for broker and
  orchestrator operations using in-memory backends

## Test Isolation

- **In-memory backends**: Each test creates a fresh `MemBroker::new()` — no
  shared state between tests.
- **SQLite**: Uses `Database::in_memory()` — each test gets a separate in-memory
  database.
- **Docker backends**: Each test function starts its own container via
  `testcontainers`. Tests run in parallel safely because they use independent
  database instances.
- **Prometheus tests**: Use `metrics::with_local_recorder` for recorder
  isolation — no global state leakage.
- **Monitoring tests**: Bind to port 0 (OS-assigned random port) to avoid
  conflicts.

## CI Integration

The GitHub Actions workflow (`.github/workflows/main.yml`) runs:

```bash
make test  # → cargo test --workspace --exclude py-rustvello --exclude rustvello-python
```

Docker-dependent tests are **skipped** in CI because no Docker service containers
are configured. They can be enabled by adding Docker services to the workflow
and running with `--include-ignored`.

## 4× Test Parametrization (Python Integration)

The pynenc test suite uses a **4× parametrization strategy** to ensure behavioral
equivalence across all orchestration modes. Every integration test runs in four
configurations:

| Configuration     | Orchestration | Backend        | Tests                   |
| ----------------- | ------------- | -------------- | ----------------------- |
| Pure-Python       | Python-only   | Python Mem     | Baseline behavior       |
| Rust Mixed Mem    | Mixed mode    | Rust in-memory | FFI correctness         |
| Rust Mixed SQLite | Mixed mode    | Rust SQLite    | Persistence correctness |
| Rust Native Mem   | Native mode   | Rust in-memory | Composite correctness   |

### How It Works

Tests are parametrized using pytest fixtures that configure the app with different
backend and mode combinations:

```python
@pytest.fixture(params=["pure_python", "rust_mixed_mem", "rust_mixed_sqlite", "rust_native_mem"])
def app(request):
    if request.param == "pure_python":
        return PynencBuilder().memory().build()
    elif request.param == "rust_mixed_mem":
        return PynencBuilder().rustvello(backend="mem", native=False).build()
    elif request.param == "rust_mixed_sqlite":
        return PynencBuilder().rustvello(backend="sqlite", native=False).build()
    elif request.param == "rust_native_mem":
        return PynencBuilder().rustvello(backend="mem", native=True).build()
```

### Behavioral Equivalence Principle

All four configurations must produce **identical observable behavior**. If a test
passes in pure-Python mode but fails in Rust native mode, that indicates a bug in
the composite implementation or FFI bridge — not a test issue.

### Error Equivalence

Exception types and error messages must be equivalent across all modes. The same
invalid operation should raise the same exception class with the same level of
detail regardless of whether the error originates in Python or Rust.
