# Testing

```{toctree}
:hidden:
:maxdepth: 2

architecture
backend-testing
docker-tests
advanced
inventory
```

## Quick Reference

| Command                                         | What it runs                  |
| ----------------------------------------------- | ----------------------------- |
| `cargo test --workspace --exclude py-rustvello` | All Rust tests (no Docker)    |
| `cargo test -p rustvello`                       | Main crate unit + integration |
| `cargo test -p rustvello-core`                  | Core trait + type tests       |
| `cargo test -p rustvello-mem`                   | In-memory backend suite       |
| `cargo test -p rustvello-sqlite`                | SQLite backend suite          |
| `cargo test -p rustvello-redis -- --ignored`    | Redis Docker tests only       |
| `cargo test -p rustvello-mongo -- --ignored`    | MongoDB Docker tests only     |
| `cargo test -p rustvello-mongo3 -- --ignored`   | MongoDB (v2) Docker tests     |
| `cargo test -p rustvello-postgres -- --ignored` | PostgreSQL Docker tests only  |
| `cargo test -p rustvello-rabbitmq -- --ignored` | RabbitMQ Docker tests only    |
| `cargo test -p rustvello-monitoring`            | Monitoring dashboard tests    |
| `cargo test -p rustvello-prometheus`            | Prometheus sink tests         |
| `cargo test -p rustvello-proto`                 | Proto types + proptest        |
| `cargo test -p rustvello-test-suite`            | Test-suite validator          |
| `make test-rust`                                | All Rust tests via Makefile   |
| `make test-python`                              | Python binding tests          |
| `make test`                                     | All tests (Rust + Python)     |

## Running Tests

### All Rust Tests

```bash
cargo test --workspace --exclude py-rustvello
# Or:
make test-rust
```

### Docker-Dependent Tests

Tests against real Redis, MongoDB, PostgreSQL, and RabbitMQ require Docker.
They are marked `#[ignore = "requires Docker"]` and skipped by default:

```bash
# Run only Docker tests for a specific backend:
cargo test -p rustvello-redis -- --ignored

# Run ALL tests including Docker:
cargo test -p rustvello-redis -- --include-ignored

# Run Docker tests for all backends:
cargo test --workspace -- --ignored
```

### Feature-Gated Tests

```bash
# Rayon runner tests
cargo test -p rustvello --features rayon

# Full feature set
cargo test -p rustvello --features full
```

### Python Tests

```bash
make develop   # Build native extension first
make test-python
# Or: uv run pytest
```

## Test Categories

Rustvello organizes tests into five categories:

1. **Inline unit tests** (`#[cfg(test)] mod tests`) — 53 modules across all crates, testing internal logic in isolation
2. **Backend compliance suites** — shared test functions in `rustvello-test-suite` exercised against every backend implementation
3. **Integration tests** — `crates/*/tests/*.rs` files testing cross-component behavior
4. **Property-based tests** — `proptest` for serde roundtrips and state machine invariants
5. **Fuzz tests** — `libfuzzer-sys` targets for deserialization robustness
6. **Benchmarks** — `criterion` micro-benchmarks for broker and orchestrator hot paths

See {doc}`architecture` for the design rationale, {doc}`backend-testing` for writing backend tests, and {doc}`advanced` for property tests, fuzzing, and benchmarks.
