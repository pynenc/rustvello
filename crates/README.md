# Rustvello Crates

This directory contains the Rust crates that make up the rustvello workspace.

## Crate Structure

| Crate              | Description                                                                 |
| ------------------ | --------------------------------------------------------------------------- |
| `rustvello-proto`  | Shared types: identifiers, DTOs, status enums, config, serialized arguments |
| `rustvello-core`   | Core traits: `Broker`, `Orchestrator`, `StateBackend`, `Runner`             |
| `rustvello-mem`    | In-memory implementations of core traits                                    |
| `rustvello-sqlite` | SQLite-backed implementations of core traits                                |
| `rustvello`        | Main integration crate: `App` facade, feature-flag aggregation              |
| `rustvello-cli`    | CLI binary for running workers, inspecting invocations, purging data        |
| `rustvello-python` | PyO3 bridge module (used by `py-rustvello`)                                 |

## Dependency Graph

```text
rustvello-proto
    |
rustvello-core (depends on proto)
    |          \
rustvello-mem   rustvello-sqlite  (both implement core traits)
    |          /
rustvello (facade, depends on core + mem/sqlite via features)
    |
rustvello-cli (depends on rustvello)
    |
rustvello-python (PyO3 bridge, depends on rustvello)
```

## Publishing Order

When publishing to crates.io, crates must be published in dependency order:

1. `rustvello-proto`
2. `rustvello-core`
3. `rustvello-mem`
4. `rustvello-sqlite`
5. `rustvello`
6. `rustvello-cli`

Use `make publish-rust` to publish them automatically in the correct order.
