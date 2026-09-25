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

Tasks are discovered from `#[rustvello::task]` and `#[rustvello::workflow]`
registrations linked into the binary through `inventory`. The CLI does not scan
Rust or Python files for an application object. `--app-id` selects the logical
application namespace; it is not an import path.

---

### `status` — Check Invocation Status

```bash
rustvello status <INVOCATION_ID> [OPTIONS]
```

| Option                 | Description          |
| ---------------------- | -------------------- |
| `-d, --db-path <PATH>` | SQLite database path |

**Example:**

```bash
rustvello status 550e8400-e29b-41d4-a716-446655440000 --db-path ./tasks.db
```

This command inspects a persisted invocation. Rustvello intentionally keeps
state-machine rendering in {doc}`../architecture` rather than adding Pynenc's
unrelated `status render` subcommand under the same public command name.

---

### `cancel` — Cancel an Invocation

```bash
rustvello cancel <INVOCATION_ID> --db-path <PATH> [OPTIONS]
```

| Option                 | Description                                 |
| ---------------------- | ------------------------------------------- |
| `-a, --app-id <ID>`    | Application namespace (default `rustvello`) |
| `-d, --db-path <PATH>` | SQLite database path                        |

Moves an unfinished invocation (queued, backing off before a retry, or running)
to `CANCELLED`. A running attempt is abandoned by its worker within
`cancellation_check_interval_seconds`. An invocation that already finished is
left unchanged and the command exits with code `3`. See
{doc}`../retries-timeouts-cancellation`.

---

### `list` — List Invocations

```bash
rustvello list [OPTIONS]
```

| Option                  | Description                                                                                     |
| ----------------------- | ----------------------------------------------------------------------------------------------- |
| `-s, --status <STATUS>` | Filter by status: `REGISTERED`, `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRY`, `CANCELLED` |
| `-t, --task <TASK_ID>`  | Filter by task ID (format: `module.name`)                                                       |
| `-d, --db-path <PATH>`  | SQLite database path                                                                            |

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

Prints the version and the documentation and repository links.

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

## Python worker launcher

The Python package ships its own launcher for standalone `rustvello.App`
applications (the equivalent of `pynenc runner` / `celery worker`):

```bash
python -m rustvello.worker package.module:app [--processes N | --workers N] \
    [--queues q1 q2 ...] [--idle-sleep-ms MS] [--no-triggers] [--loglevel LEVEL]
```

| Option            | Description                                                    | Default |
| ----------------- | -------------------------------------------------------------- | ------- |
| `--processes N`   | Run task code in N worker processes (one interpreter each)     | —       |
| `--workers N`     | In-process worker slots when `--processes` is not given        | `4`     |
| `--queues ...`    | Broker queues this runner consumes (overrides `runner_queues`) | config  |
| `--idle-sleep-ms` | Sleep when no work is available                                | `50`    |
| `--no-triggers`   | Do not evaluate trigger conditions on this runner              | —       |
| `--loglevel`      | Python logging and runner log level                            | config  |

Configuration comes from `RUSTVELLO__*` environment variables and the optional
TOML / `pyproject.toml` sources (see Configuration), so a Kubernetes manifest
can set `RUSTVELLO__RUNNER_QUEUES=hpa` per deployment instead of passing flags.

## Environment Variables

The `run` and `config` commands load `RUSTVELLO__*` application configuration
through `RustvelloBuilder::from_env()`. Command-only flags such as `--db-path`
and `--yes` remain CLI arguments:

```bash
export RUSTVELLO__APP_ID=my-app
export RUSTVELLO__DB_PATH=./tasks.db

rustvello run   # picks up env vars automatically
```

See {doc}`../configuration/index` for the full environment variable reference.
