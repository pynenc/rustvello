# Advanced Testing

Beyond the standard backend compliance suites, rustvello employs property-based
testing, fuzz testing, and benchmarks.

## Property-Based Testing (proptest)

The `rustvello-proto` crate uses [proptest](https://crates.io/crates/proptest)
to verify invariants that must hold for **any** valid input, not just
hand-picked examples.

**File**: `crates/rustvello-proto/tests/proptest_roundtrips.rs`

### What Is Tested

**Serde roundtrip invariants** — for every type that crosses serialization
boundaries, encoding to JSON and decoding back produces the original value:

```rust
proptest! {
    #[test]
    fn invocation_status_roundtrip(status in arb_invocation_status()) {
        let json = serde_json::to_string(&status).unwrap();
        let decoded: InvocationStatus = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(status, decoded);
    }
}
```

Types tested: `InvocationStatus`, `ConcurrencyControlType`, `TriggerLogic`,
`InvocationStatusRecord`, `TriggerCondition`.

**Status transition graph properties**:

- Terminal statuses have no outgoing transitions
- Non-terminal statuses have at least one transition
- The transition graph is consistent (no dangling references)

### Running Property Tests

```bash
cargo test -p rustvello-proto --test proptest_roundtrips
```

Proptest runs each property 256 times by default with random inputs. On failure,
it shrinks the input to the minimal failing case.

---

## Fuzz Testing (libfuzzer)

The `fuzz/` directory contains [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html)
targets using `libfuzzer-sys`.

### Targets

| Target              | What it fuzzes                                                                 |
| ------------------- | ------------------------------------------------------------------------------ |
| `fuzz_json_trigger` | `serde_json::from_str::<TriggerCondition>()` — arbitrary JSON must never panic |
| `fuzz_toml_config`  | `toml::from_str::<AppConfig>()` — arbitrary TOML must never panic              |

Each target follows the same pattern:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<TriggerCondition>(s);
    }
});
```

### Running Fuzz Tests

```bash
# Install cargo-fuzz (requires nightly)
cargo install cargo-fuzz

# Run a fuzz target
cargo +nightly fuzz run fuzz_json_trigger

# Run for a fixed duration
cargo +nightly fuzz run fuzz_json_trigger -- -max_total_time=60

# List available targets
cargo +nightly fuzz list
```

Fuzz tests run indefinitely by default. Use `-max_total_time` to limit
duration, or Ctrl+C to stop.

### Corpus

The fuzzer builds a corpus of interesting inputs in `fuzz/corpus/<target>/`.
Commit the corpus to source control to improve future runs.

---

## Benchmarks (Criterion)

The main `rustvello` crate has [criterion](https://crates.io/crates/criterion)
benchmarks for hot-path operations.

### Available Benchmarks

**`crates/rustvello/benches/broker_bench.rs`**:

- `broker_route_single` — time to route one invocation
- `broker_route_retrieve_roundtrip` — route + retrieve cycle
- `broker_route_batch_100` — routing 100 invocations

**`crates/rustvello/benches/orchestrator_bench.rs`**:

- `orch_register_invocation` — time to register one invocation
- `orch_status_transition_cycle` — full status lifecycle
- `orch_get_invocations_by_status` — query by status filter

All benchmarks use in-memory backends (`MemBroker`, `MemOrchestrator`) to
measure pure logic overhead without I/O.

### Running Benchmarks

```bash
# Run all benchmarks
cargo bench -p rustvello

# Run a specific benchmark
cargo bench -p rustvello -- broker_route_single

# Generate HTML report (in target/criterion/)
cargo bench -p rustvello
open target/criterion/report/index.html
```

Criterion automatically detects regressions by comparing against previous runs
stored in `target/criterion/`.

---

## Monitoring Tests

The `rustvello-monitoring` crate has a specialized test setup:

```bash
# Unit tests (parser, SVG rendering, route handlers)
cargo test -p rustvello-monitoring --lib

# Integration tests (full HTTP server)
cargo test -p rustvello-monitoring --test monitoring_dashboard
```

Integration tests start a real Axum server on a random port, populate it with
synthetic data, and assert on HTML/JSON responses.

For manual browser inspection:

```bash
KEEP_ALIVE=1 cargo test -p rustvello-monitoring \
  --test monitoring_dashboard test_hierarchical_timeline -- --nocapture
```

This keeps the server running after the selected test completes so you can open
the printed dashboard URL in a browser. `RUSTVELLO_MONITOR_KEEP_ALIVE=1` is also
accepted when you want the longer namespaced variable.

## Prometheus Tests

The `rustvello-prometheus` crate uses `metrics::with_local_recorder` for test
isolation:

```bash
cargo test -p rustvello-prometheus
```

Each test installs a thread-local metrics recorder, so tests never share global
state and can run in parallel safely.
