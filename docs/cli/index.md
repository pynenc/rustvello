# Rustvello CLI

The `rustvello` binary provides command-line utilities for running workers, inspecting
invocations, and managing application data.

## Installation

```bash
cargo install rustvello-cli
```

## Commands

### `run` — Start a Worker

Start a `TaskRunner` that processes queued invocations:

```bash
rustvello run [OPTIONS]
```

| Option                 | Description                       | Default       |
| ---------------------- | --------------------------------- | ------------- |
| `-a, --app-id <ID>`    | Application ID                    | `rustvello`   |
| `-d, --db-path <PATH>` | SQLite database path              | _(in-memory)_ |
| `-c, --config <FILE>`  | TOML configuration file           | —             |
| `--memory`             | Force in-memory backends          | —             |
| `--idle-sleep-ms <MS>` | Worker idle sleep in milliseconds | `100`         |

**Examples:**

```bash
# Development — in-memory backends
rustvello run --memory

# Single-host production — SQLite persistence
rustvello run --app-id my-app --db-path ./tasks.db

# From config file (Redis, Postgres, etc.)
rustvello run --config config.toml
```

---

### `status` — Check Invocation Status

```bash
rustvello status <INVOCATION_ID> [OPTIONS]
```

| Option                 | Description             |
| ---------------------- | ----------------------- |
| `-d, --db-path <PATH>` | SQLite database path    |
| `-c, --config <FILE>`  | TOML configuration file |

**Example:**

```bash
rustvello status 550e8400-e29b-41d4-a716-446655440000 --db-path ./tasks.db
```

---

### `list` — List Invocations

```bash
rustvello list [OPTIONS]
```

| Option                  | Description                                                                        |
| ----------------------- | ---------------------------------------------------------------------------------- |
| `-s, --status <STATUS>` | Filter by status: `REGISTERED`, `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRY` |
| `-t, --task <TASK_ID>`  | Filter by task ID (format: `module.name`)                                          |
| `-d, --db-path <PATH>`  | SQLite database path                                                               |

**Example:**

```bash
rustvello list --status RUNNING --db-path ./tasks.db
rustvello list --task my_crate.process_order --db-path ./tasks.db
```

---

### `purge` — Delete All Data

```bash
rustvello purge [OPTIONS]
```

| Option                 | Description              |
| ---------------------- | ------------------------ |
| `-d, --db-path <PATH>` | SQLite database path     |
| `-y, --yes`            | Skip confirmation prompt |

```{warning}
`purge` deletes all broker queues, invocations, results, and heartbeat records.
This action is irreversible.
```

---

### `info` — System Information

```bash
rustvello info
```

Prints version, compiled feature flags, and runtime information.

---

### `config` — Show Effective Configuration

```bash
rustvello config [OPTIONS]
```

| Option                | Description             |
| --------------------- | ----------------------- |
| `-c, --config <FILE>` | TOML configuration file |
| `-a, --app-id <ID>`   | Application ID          |

Prints the resolved `AppConfig` as TOML, merging all sources (file, env, defaults).
Useful for debugging configuration priority issues.

---

## Environment Variables

All CLI options have corresponding `RUSTVELLO__*` environment variable equivalents.
Set them in your shell or a `.env` file:

```bash
export RUSTVELLO__APP_ID=my-app
export RUSTVELLO__DB_PATH=./tasks.db

rustvello run   # picks up env vars automatically
```

See {doc}`../configuration/index` for the full environment variable reference.

| `-y, --yes` | Skip confirmation prompt |

### `info` — Show System Information

```bash
rustvello-cli info
```

Displays version and build information.
