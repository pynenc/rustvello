# Monitoring Dashboard

Rustvello ships a built-in web-based monitoring dashboard in the `rustvello-monitoring`
crate. It provides real-time visibility into invocations, runners, workers, workflows,
and task timelines — with no external dependencies beyond the Axum web server.

---

## Features

:::::{grid} 1 2 2 2
:gutter: 3

::::{grid-item-card} SVG Timeline
Visualizes invocation scheduling across runners and workers. Each runner lane shows
concurrent sub-lanes when tasks overlap, coloured by status (pending = orange, running
= green, success = grey). Workers appear as distinct sub-lanes under their parent runner.
::::

::::{grid-item-card} Log Explorer
Full-text search across structured logs. Cross-highlights related runners, workers, and
invocations when hovering a chip. Supports filtering by runner ID, worker ID,
invocation ID, task key, log level, and time range.
::::

::::{grid-item-card} Invocation Tables
Filterable tables for every invocation status. Drill into individual invocations to
see the full status history, call arguments, result, error, runner assignment, and
workflow parent/child relationships.
::::

::::{grid-item-card} Workflow View
Parent-child invocation trees rendered as interactive family graphs. Useful for
debugging fan-out patterns and understanding orchestration chains.
::::

::::{grid-item-card} Trigger Evidence
Inspect emitted events, matched conditions, trigger-run participants, and the
invocations produced by a trigger. Event rows and details link into bounded
timeline windows and expose JSON/trace endpoints for script or agent debugging.
::::

::::{grid-item-card} OpenTelemetry
Attach `rustvello-otel` to export correlated task-attempt spans, lifecycle logs,
worker resources, and low-cardinality completion metrics over OTLP/HTTP-Protobuf.
::::

::::{grid-item-card} Multi-App Support
A single monitoring server can observe multiple `RustvelloApp` instances. Switch between
apps from the navigation bar without restarting.
::::

:::::

---

### Timeline Controls

The invocation timeline supports bounded time ranges, task filters, workflow type
and workflow ID filters, and an optional comma-separated invocation scope. The
scope is useful when following one workflow or a trigger-related set of
invocations without rendering unrelated history.

Filters live in a collapsed toolbar above the timeline so the plot keeps the
full page width. Runner labels use a dedicated left rail with a language badge,
a stable per-runner colour, and one compact metadata line. Lifecycle relation
paths stay subdued until their invocation is hovered or selected.

Use the zoom button in the timeline header, then drag across the SVG time axis
to open the selected range. The selection is converted to UTC and preserves the
active filters in the URL. Invocation and event detail links can also open a
focused bounded range around their recorded timestamps.

The timeline reads history through the backend's bounded time-range query and
loads complete histories only for invocations that intersect the visible range.
This preserves status transitions that cross the left edge while avoiding an
unbounded scan of old invocation histories.

### Task Occupancy Histograms

The timeline histogram counts invocations that occupied registered, pending,
or running states during each bucket. It is aligned to the timeline's UTC bounds
and plot margins. Its task table occupies the same left rail as the runner
descriptions, and both views share a linked vertical time cursor and synchronized
horizontal scrolling, so each bar sits below the corresponding execution period.
Bars are stacked by task type with a stable task legend shared across selected
workflow runs. Status checkboxes update the URL; hovering shows exact bounds,
category counts, and task counts; selecting a bar opens the invocation list with
matching custom time, status, workflow, task, and invocation filters.

Workflow detail pages render the latest three run histograms by default and
allow up to ten selected runs. The Log Explorer uses the same model for the
invocations resolved from a pasted log block. All three surfaces use complete
status histories and half-open occupancy intervals rather than counting starts
or completions. Bars are width-capped for dense ranges and show up to 12 task
types before aggregating the tail as neutral-grey `Other`.

Rustvello does not expose Pynenc's system-task or atomic-service visibility
toggles in this view because those are not separate invocation categories in
the Rustvello monitoring model. Atomic-service execution has its own dedicated
timeline view, and event/trigger evidence has dedicated list and detail views.

### Event Investigation

The Events page shows matched and triggered counts for the current page, emitter
attribution, row-level temporal shading, and timeline actions for each event or
trigger run. Details expose machine-readable investigation URLs:

- `/events/{event_id}/api` returns the event, related trigger runs, and useful
  dashboard links.
- `/events/{event_id}/trigger-runs` returns trigger-run evidence for the event.
- `/events/{event_id}/trace` returns source and generated invocation IDs so a
  human, script, or coding agent can jump directly to the relevant timeline
  scope.

## Running the Dashboard

The monitoring server is started programmatically alongside your application:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use rustvello_monitoring::{start_monitor, AppInstance, MonitorConfig};
use std::net::SocketAddr;

// Build your app normally
let app = Rustvello::builder()
    .app_id("my-app")
    .auto_discover_tasks()
    .build().await?;

// Wrap it in an AppInstance for the monitoring server
let instance = AppInstance {
    app_id: "my-app".into(),
    config: app.config.clone(),
    broker: app.broker(),
    orchestrator: app.orchestrator(),
    state_backend: app.state_backend(),
    trigger_store: app.trigger_manager().map(|manager| Arc::clone(manager.store())),
    client_data_store: app.client_data_store(),
    task_ids: app.registered_task_ids(),
};

// Start the dashboard (runs until the process exits)
start_monitor(
    HashMap::from([("my-app".into(), instance)]),
    "my-app",
    MonitorConfig {
        bind: SocketAddr::from(([0, 0, 0, 0], 8000)),
        ..MonitorConfig::default()
    },
).await?;
```

Open a browser at `http://localhost:8000` to view the dashboard.

### From Python

```python
server = app.start_monitor(host="0.0.0.0", port=8000, read_only=True)  # MonitorServer
print(server.url, server.is_running())
server.stop()
```

`App.start_monitor(block=True)` serves until SIGTERM or Ctrl-C and then
returns, so it can be the body of a monitor process. The launcher does the
same from the command line and exits non-zero when a backend refuses the
monitor's credentials:

```bash
python -m rustvello.monitor package.module:app --host 0.0.0.0 --port 8000 --read-only
```

The dashboard's CSS, JavaScript and logo are compiled into the binary (the
wheel, the CLI), so a monitor runs anywhere without a source checkout. For
front-end work, `RUSTVELLO_MONITORING_STATIC_DIR=crates/rustvello-monitoring/static`
serves the files from disk instead.

### Read-only mode

`read_only=True`, `--read-only` or `RUSTVELLO_MONITORING_READ_ONLY=1` refuses
every request that changes data (purge, rerun, cancel answer 403) and hides
the purge buttons, so the monitor can be exposed to more people safely.
`/api/capabilities` reports `read_only`.

### Start-up check and `/health`

Before serving, the monitor reads every component once; a refused read fails
the start with the backend's own error (Python: `RuntimeError`; Rust:
`check_backends: false` in `MonitorConfig` skips it). `GET /health` (and
`/api/health`) returns the same report while serving: HTTP 200 when every
component answered, 503 otherwise, with one JSON body:

```json
{
  "status": "ok",
  "app_id": "shop",
  "components": {
    "broker": { "status": "ok", "implementation": "MongoDB 3.6" },
    "orchestrator": { "status": "ok", "implementation": "MongoDB 3.6" },
    "state_backend": {
      "status": "error",
      "implementation": "MongoDB 3.6",
      "error": "state backend error: not authorized on rustvello_shop ..."
    },
    "client_data_store": { "status": "ok", "implementation": "MongoDB 3.6" },
    "trigger_store": { "status": "ok", "implementation": "Trigger store" }
  },
  "runners": { "active": 0, "dead_after_seconds": 300 },
  "queues": { "default": 0, "hpa": 12 },
  "problems": [
    "state_backend: ...",
    "no active runners (no heartbeat in the last 300s)"
  ]
}
```

Having no runners is a `problem`, not an error: a monitor pod stays healthy
while the workers are down. `App.health()` returns the same dictionary
without a server, and `python -m rustvello.doctor module:app` prints it and
exits non-zero on a refused read (1) or no runners (2) ([CLI](../cli/index.md)).

### JSON API for scripts and agents

Everything the pages show is available as JSON; `GET /api/capabilities`
lists the routes:

| Route                                                          | Content                                                                                  |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `/api/runners`                                                 | Active runners: class, language, executor, host, pid, heartbeat age                      |
| `/api/invocations?status=Failed&task_id=...&limit=50&offset=0` | Invocations by status (newest first) with task id, timestamps and the investigation link |
| `/invocations/{id}/investigation`                              | One invocation: status, stored error, history, runners, workflow                         |
| `/api/queues`                                                  | Queued invocations per declared queue                                                    |
| `/api/config`                                                  | Effective `AppConfig`, backend implementations, registered tasks                         |
| `/api/health`                                                  | The health report above                                                                  |

---

## MonitorConfig

| Field            | Type         | Default          | Description                                                           |
| ---------------- | ------------ | ---------------- | --------------------------------------------------------------------- |
| `bind`           | `SocketAddr` | `127.0.0.1:8000` | Address and port to listen on                                         |
| `log_level`      | `String`     | `"info"`         | Monitoring server log level setting                                   |
| `read_only`      | `bool`       | `false`          | Refuse purge, rerun and cancel; also `RUSTVELLO_MONITORING_READ_ONLY` |
| `check_backends` | `bool`       | `true`           | Probe every backend before serving; a refused read is an error        |

---

## Adding to Cargo.toml

```toml
[dependencies]
rustvello-monitoring = "0.8"
```

The monitoring crate does **not** require `rustvello`'s feature flags — it depends
directly on `rustvello-core` traits and works with any backend combination.

---

## OpenTelemetry Export

Add `rustvello-otel` and attach its `BoundedAsyncEmitter` to the application and
runner lifecycle. Python applications can pass `otlp_endpoint` and
`otlp_bearer_token` to `App`.

```toml
[dependencies]
rustvello = "0.8"
rustvello-otel = "0.8"
rustvello-monitoring = "0.8"
```

The exporter maps retries as sibling execution spans under the persisted W3C parent,
logs survive unsampled or incomplete spans, and completion metrics exclude invocation,
attempt, trace, span, and workflow IDs.

---

## Structured Logging

Rustvello introduces a unified logging format that produces consistent output
from both the Rust engine and Python task code. Text logs carry `[R]` or `[P]`
system tags; JSON logs use `"system": "rust"` or `"system": "python"`.

The dashboard's log explorer reads these structured logs for cross-entity highlighting.

For the full logging guide — unified format specification, JSON output schema,
configuration, and integration with ELK/Datadog/Loki — see {doc}`logging`.

### Quick Setup

For the Rust CLI:

```bash
RUST_LOG=info rustvello run --app-id my-app
```

For Python/pynenc:

```bash
PYNENC__LOGGING_LEVEL=info PYNENC__LOG_FORMAT=json pynenc --app=tasks.app runner start
```

The log explorer parses runtime context (`runner_id`, `worker_id`,
`invocation_id`, `task_id`) from either unified text or JSON logs.

---

## Security Notes

The monitoring dashboard is intended for internal/trusted networks. It does not
implement authentication by default. For production deployments:

- Bind to `127.0.0.1` and expose via a reverse proxy (nginx, Caddy) with authentication
- Or restrict network access to the monitoring port at the firewall/VPC level
- Run it read-only (`read_only=True` / `RUSTVELLO_MONITORING_READ_ONLY=1`) when
  more people than the operators can reach it

CSRF origin validation is enforced on all state-mutating routes.

```{toctree}
:hidden:
:maxdepth: 2

logging
triggers
```
