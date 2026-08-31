# Testing

```{toctree}
:hidden:
:maxdepth: 2

architecture
backend-testing
backend-constraints
docker-tests
advanced
inventory
```

## Quick Reference

| Command                                         | What it runs                          |
| ----------------------------------------------- | ------------------------------------- |
| `cargo test --workspace --exclude py-rustvello` | All Rust tests (no Docker)            |
| `cargo test -p rustvello`                       | Main crate unit + integration         |
| `cargo test -p rustvello-core`                  | Core trait + type tests               |
| `cargo test -p rustvello-mem`                   | In-memory backend suite               |
| `cargo test -p rustvello-sqlite`                | SQLite backend suite                  |
| `cargo test -p rustvello-redis -- --ignored`    | Redis Docker tests only               |
| `cargo test -p rustvello-mongo -- --ignored`    | MongoDB Docker tests only             |
| `cargo test -p rustvello-mongo3 -- --ignored`   | MongoDB (v2) Docker tests             |
| `cargo test -p rustvello-postgres -- --ignored` | PostgreSQL Docker tests only          |
| `cargo test -p rustvello-rabbitmq -- --ignored` | RabbitMQ Docker tests only            |
| `cargo test -p rustvello-monitoring`            | Monitoring dashboard tests            |
| `cargo test -p rustvello-prometheus`            | Prometheus sink tests                 |
| `cargo test -p rustvello-proto`                 | Proto types + proptest                |
| `cargo test -p rustvello-test-suite`            | Test-suite validator                  |
| `make test-rust`                                | All Rust tests via Makefile           |
| `make test-python`                              | Python binding tests                  |
| `make test`                                     | All default tests (Rust + Python)     |
| `make test-docker`                              | Ignored Docker backend suites         |
| `make test-stress`                              | Fast contention tests                 |
| `make test-soak`                                | Ignored high-volume/SQLite soak tests |

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
cargo test --workspace --exclude py-rustvello -- --ignored
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

### Python Compatibility

The supported Python versions are declared in `py-rustvello/pyproject.toml`.
The compatibility CI matrix and release wheel interpreters are derived from
the Python classifiers in that file, so adding or removing a supported minor
version updates both checks together:

```bash
make python-versions
```

Rustvello currently builds version-specific PyO3 wheels for Python 3.9 through
3.13. The CI suite installs and tests the extension on every declared version.
This is intentionally different from an `abi3` build: `abi3` can reduce the
number of wheels, but only when the extension uses PyO3's limited stable API.

## Test Categories And CI Lanes

Rustvello organizes tests into these categories:

1. **Inline unit tests** (`#[cfg(test)] mod tests`) — 53 modules across all crates, testing internal logic in isolation
2. **Backend compliance suites** — shared test functions in `rustvello-test-suite` exercised against every backend implementation
3. **Integration tests** — `crates/*/tests/*.rs` files testing cross-component behavior
4. **Property-based tests** — `proptest` for serde roundtrips and state machine invariants
5. **Fuzz tests** — `libfuzzer-sys` targets for deserialization robustness
6. **Benchmarks** — `criterion` micro-benchmarks for broker and orchestrator hot paths

Default PR CI runs unit, integration, shared compliance, Python, property, fast
contention, docs, and short fuzz checks. `.github/workflows/backend-and-stress.yml`
runs Docker-backed compliance and slower soak tests on a schedule or manual
dispatch. See {doc}`backend-constraints` for backend facts and the required
contract rule.

See {doc}`architecture` for the design rationale, {doc}`backend-testing` for writing backend tests, and {doc}`advanced` for property tests, fuzzing, and benchmarks.
