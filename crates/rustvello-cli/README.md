# rustvello-cli

Command-line interface for the rustvello distributed task system.

## Installation

```bash
cargo install rustvello-cli
```

## Commands

| Command                      | Description                                                      |
| ---------------------------- | ---------------------------------------------------------------- |
| `rustvello run`              | Start a task runner that processes queued invocations            |
| `rustvello status <id>`      | Show the status of an invocation                                 |
| `rustvello investigate <id>` | Print provenance, history, and runner context for one invocation |
| `rustvello list`             | List invocations (optionally filtered by status or task)         |
| `rustvello purge`            | Purge all data (broker queue, invocations, results)              |
| `rustvello info`             | Show system information (version, homepage)                      |

## Common Options

Most data commands accept `--db-path <path>` to use a SQLite database. Without
it, an in-memory SQLite database is used.

The runner discovers tasks registered in the binary through Rust `inventory`.
It does not scan files for Python application objects.

## Examples

```bash
# Start a runner
rustvello run --app-id my_app --db-path tasks.db

# List all pending invocations
rustvello list --status PENDING --db-path tasks.db

# Check status of a specific invocation
rustvello status 550e8400-e29b-41d4-a716-446655440000 --db-path tasks.db

# Investigate where an invocation was registered and executed
rustvello investigate 550e8400-e29b-41d4-a716-446655440000 \
  --app-id my_app \
  --db-path tasks.db \
  --format json

# Purge all data (with confirmation skip)
rustvello purge --db-path tasks.db --yes

# Show version and system info
rustvello info
```
