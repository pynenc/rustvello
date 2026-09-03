# Migration Guide

This guide covers the user-visible changes introduced while aligning
Rustvello with Pynenc releases `0.2.0` through `0.3.1`. Rustvello remains a
simplified Rust-first engine, so parity means matching shared behavior rather
than reproducing every Python runner, plugin, or discovery mechanism.

## Invocation status

Rustvello has 13 invocation statuses. `ConcurrencyControlledFinal` is terminal,
and `Pending` no longer transitions directly to `Failed`. Code that reports or
filters terminal outcomes should include `ConcurrencyControlledFinal`.

The state machine is documented in {doc}`architecture`. Rustvello does not add
Pynenc's `status render` command because `rustvello status <INVOCATION_ID>` is
an existing invocation-inspection interface. The checked-in Mermaid source and
architecture diagram are the canonical documentation artifacts.

## Workflows

Replace the former implicit or forced-root pattern:

```rust
#[rustvello::task(force_new_workflow = true)]
fn reconcile() {}
```

with an explicit workflow:

```rust
#[rustvello::workflow]
fn reconcile() {}
```

Ordinary top-level tasks now have no workflow identity. An ordinary child task
inherits its caller's workflow; a workflow called from another workflow defines
a subworkflow. Deterministic random, UUID, and clock operations are available
only through `WorkflowRoot::current()` in the defining invocation. See
{doc}`workflows` for typed failure modes and replay examples.

## Trigger evidence

All full trigger stores persist emitted events and claimed trigger runs. The
dashboard can trace an event through matched conditions to the invocation
produced by the trigger. See {doc}`monitoring/triggers` and
{doc}`contributing/testing/backend-constraints` for backend storage facts; this
is mandatory for every full trigger store.

## Discovery and direct calls

Pynenc `0.2.3` added runtime scanning for a single Python `Pynenc()` instance.
Rustvello does not scan source files for application objects:

- Rust tasks use `#[rustvello::task]` or `#[rustvello::workflow]` and
  `.auto_discover_tasks()` collects link-time `inventory` entries.
- The Rustvello CLI constructs its app from `--app-id`, configuration, and the
  task inventory linked into the binary.
- Standalone Python registers decorated callables directly on an `App`; it does
  not import or hydrate modules on behalf of the user.

Pynenc's `direct_task` parallel splitter and aggregator are also not copied.
Rust callers receive an invocation handle and await `.result()`. Standalone
Python callers use `Invocation.result()`, or `dev_mode_force_sync=True` when a
local test needs immediate execution. Distribution remains explicit and does
not add Python callback aggregation to the Rust runner.

## Operational checks

After migration, run:

```bash
cargo test --workspace --exclude py-rustvello --exclude rustvello-python
cargo clippy --workspace --all-targets -- -D warnings
cd py-rustvello && uv run pytest
make docs
```

For distributed backend and contention lanes, use `make test-docker` and
`make test-soak`; they require their documented external services.
