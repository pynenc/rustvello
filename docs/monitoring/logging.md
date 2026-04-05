# Logging Guide

Rustvello uses a unified logging model for both runtimes:

- Rust (`tracing`) emits text lines tagged with `[R]` or NDJSON with `"system": "rust"`
- Python (`logging`) emits text lines tagged with `[P]` or NDJSON with `"system": "python"`

This keeps search, parsing, and dashboards consistent across the FFI boundary.

---

## Unified Text Format (default)

Example output:

```text
2026-03-27T10:23:45.123Z INFO  [R] my_app [PTR(a86ab1f8).W(d1241003)cc6c0e34-5678-9abc-def0-123456789abc:core_tasks.recover] rustvello::runner Invocation completed
2026-03-27T10:23:45.456Z INFO  [P] my_app [PTR(a86ab1f8)cc6c0e34-5678-9abc-def0-123456789abc:tasks.add] pynenc.my_app Executing task function
```

Template:

```text
{timestamp} {level:5} [R|P] {app_id} [{context}] {logger} {message}
```

| Field       | Description                                                      |
| ----------- | ---------------------------------------------------------------- | -------------------------------------------- |
| `timestamp` | UTC ISO 8601 with milliseconds (`YYYY-MM-DDTHH:MM:SS.mmmZ`)      |
| `level`     | Padded level (`TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `CRIT`) |
| `[R         | P]`                                                              | Runtime origin tag: Rust `[R]`, Python `[P]` |
| `app_id`    | Pynenc application ID                                            |
| `context`   | Optional execution context (runner, worker, invocation, task)    |
| `logger`    | Rust target or Python logger name                                |
| `message`   | Human-readable log message                                       |

### System Tags

- `[R]`: emitted by Rust runtime code
- `[P]`: emitted by Python runtime code

If you only see `[P]`, you are likely running pure-Python components.

---

## JSON Format

Enable NDJSON output:

```bash
PYNENC__LOG_FORMAT=json pynenc --app=tasks.app runner start
```

Example line:

```json
{
  "timestamp": "2026-03-27T10:23:45.123Z",
  "severity": "INFO",
  "system": "rust",
  "logger": "rustvello::runner",
  "message": "Invocation completed",
  "runner_class": "PTR",
  "runner_id": "a86ab1f8-1111-2222-3333-444455556666",
  "worker_id": "d1241003-1111-2222-3333-444455556666",
  "invocation_id": "cc6c0e34-5678-9abc-def0-123456789abc",
  "task_id": "core_tasks.recover",
  "text": "2026-03-27T10:23:45.123Z INFO  [R] my_app [PTR(a86ab1f8).W(d1241003)cc6c0e34-5678-9abc-def0-123456789abc:core_tasks.recover] rustvello::runner Invocation completed"
}
```

Common fields:

| Field                        | Type   | Always Present | Description                                                          |
| ---------------------------- | ------ | -------------- | -------------------------------------------------------------------- |
| `timestamp`                  | string | Yes            | UTC timestamp                                                        |
| `severity`                   | string | Yes            | Log level (`TRACE`, `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) |
| `system`                     | string | Yes            | `"rust"` or `"python"`                                               |
| `logger`                     | string | Yes            | Logger/target name                                                   |
| `message`                    | string | Yes            | Message body                                                         |
| `text`                       | string | Yes            | Human-readable unified line                                          |
| `runner_class` / `runner_id` | string | No             | Runner context                                                       |
| `worker_id`                  | string | No             | Worker context                                                       |
| `invocation_id`              | string | No             | Invocation context                                                   |
| `task_id`                    | string | No             | Task context                                                         |

Python JSON logs can also include `exception` and `stack_info` when present.

---

## Configuration

### Environment Variables

| Variable                      | Values                                          | Default     | Description                            |
| ----------------------------- | ----------------------------------------------- | ----------- | -------------------------------------- |
| `PYNENC__LOGGING_LEVEL`       | `debug`, `info`, `warning`, `error`, `critical` | `info`      | Shared pynenc log level                |
| `PYNENC__LOG_FORMAT`          | `text`, `json`                                  | `text`      | Unified text or NDJSON output          |
| `PYNENC__LOG_STREAM`          | `stdout`, `stderr`                              | `stderr`    | Log destination stream                 |
| `PYNENC__LOG_USE_COLORS`      | `true`, `false`                                 | auto-detect | ANSI colors for text mode              |
| `PYNENC__COMPACT_LOG_CONTEXT` | `true`, `false`                                 | `true`      | Compact IDs and class names in context |

### Python Builder API

```python
from pynenc import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my-app")
    .rustvello(backend="sqlite")
    .logging(level="debug", fmt="json", stream="stdout", colors=False)
    .build()
)
```

### Notes

- Pynenc initializes Python logging and then calls `rustvello.init_logging(...)` for Rust.
- If `RUST_LOG` is set, Rust's `tracing` filter follows `RUST_LOG` precedence.

---

## Integration Examples

### ELK Stack

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    paths:
      - /var/log/pynenc/*.log
    json:
      keys_under_root: true
      add_error_key: true

output.logstash:
  hosts: ["logstash:5044"]
```

```ruby
# logstash filter
filter {
  if [system] == "rust" {
    mutate { add_tag => ["rust_runtime"] }
  } else if [system] == "python" {
    mutate { add_tag => ["python_runtime"] }
  }
}
```

### Datadog

```yaml
# conf.d/pynenc.d/conf.yaml
logs:
  - type: file
    path: /var/log/pynenc/*.log
    service: pynenc
    source: pynenc
```

### Grafana Loki (Promtail)

```yaml
scrape_configs:
  - job_name: pynenc
    static_configs:
      - targets: [localhost]
        labels:
          job: pynenc
          __path__: /var/log/pynenc/*.log
    pipeline_stages:
      - json:
          expressions:
            system: system
            severity: severity
            logger: logger
      - labels:
          system:
          severity:
```

---

## Troubleshooting

### Verify Effective Level

```bash
PYNENC__LOGGING_LEVEL=debug pynenc --app=tasks.app runner start 2>&1 | head -20
```

### Filter by Runtime

```bash
# Text logs
grep "\[R\]" app.log
grep "\[P\]" app.log

# JSON logs
jq 'select(.system == "rust")' app.log
jq 'select(.system == "python")' app.log
```

### Common Issues

| Symptom                           | Likely Cause                              | Fix                                                       |
| --------------------------------- | ----------------------------------------- | --------------------------------------------------------- |
| Rust and Python levels differ     | `RUST_LOG` overrides Rust filter          | Unset `RUST_LOG` or align it with `PYNENC__LOGGING_LEVEL` |
| Only `[P]` lines appear           | Running without Rust backends             | Use `PynencBuilder().rustvello(...)`                      |
| Mixed text and JSON output        | Format changed after app init             | Set `PYNENC__LOG_FORMAT` before process start             |
| JSON parser misses context fields | Log line has no runner/invocation context | Expected for app-level or startup logs                    |

:::{admonition} See also: Pynenc Docs
:class: seealso
To see how logging behaves in the higher-level Python system and runners, check out the [Pynenc Getting Started](https://pynenc.github.io/getting_started/index.html) and [Pynenc Runner Usage Guide](https://pynenc.github.io/usage_guide/runner.html).
:::
