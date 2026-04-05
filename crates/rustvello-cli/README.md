# rustvello-cli

Command-line interface for the rustvello distributed task system.

## Installation

```bash
cargo install rustvello-cli
```

## Commands

| Command                 | Description                                              |
| ----------------------- | -------------------------------------------------------- |
| `rustvello run`         | Start a task runner that processes queued invocations    |
| `rustvello status <id>` | Show the status of an invocation                         |
| `rustvello list`        | List invocations (optionally filtered by status or task) |
| `rustvello purge`       | Purge all data (broker queue, invocations, results)      |
| `rustvello info`        | Show system information (version, homepage)              |

## Common Options

Most commands accept `--db_path <path>` to use a SQLite database. Without it, an in-memory database is used.

## Examples

```bash
# Start a runner
rustvello run --app_id my_app --db_path tasks.db

# List all pending invocations
rustvello list --status PENDING --db_path tasks.db

# Check status of a specific invocation
rustvello status inv_abc123 --db_path tasks.db

# Purge all data (with confirmation skip)
rustvello purge --db_path tasks.db --yes

# Show version and system info
rustvello info
```
