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
invocations produced by a trigger. Event details link into a bounded timeline.
::::

::::{grid-item-card} Prometheus Metrics
When `rustvello-prometheus` is active, the dashboard exposes `/metrics` in Prometheus
text format. Counters and histograms cover invocation counts, status transitions,
broker queue depth, and runner heartbeat timings.
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

Use the zoom button in the timeline header, then drag across the SVG time axis
to open the selected range. The selection is converted to UTC and preserves the
active filters in the URL. Invocation and event detail links can also open a
focused bounded range around their recorded timestamps.

The timeline reads history through the backend's bounded time-range query and
loads complete histories only for invocations that intersect the visible range.
This preserves status transitions that cross the left edge while avoiding an
unbounded scan of old invocation histories.

Rustvello does not expose Pynenc's system-task or atomic-service visibility
toggles in this view because those are not separate invocation categories in
the Rustvello monitoring model. Atomic-service execution has its own dedicated
timeline view, and event/trigger evidence has dedicated list and detail views.

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
        log_level: "info".into(),
    },
).await?;
```

Open a browser at `http://localhost:8000` to view the dashboard.

---

## MonitorConfig

| Field       | Type         | Default          | Description                         |
| ----------- | ------------ | ---------------- | ----------------------------------- |
| `bind`      | `SocketAddr` | `127.0.0.1:8000` | Address and port to listen on       |
| `log_level` | `String`     | `"info"`         | Monitoring server log level setting |

---

## Adding to Cargo.toml

```toml
[dependencies]
rustvello-monitoring = "0.3.1"
```

The monitoring crate does **not** require `rustvello`'s feature flags — it depends
directly on `rustvello-core` traits and works with any backend combination.

---

## Prometheus Metrics

Enable the `prometheus` feature on `rustvello` and add the `rustvello-prometheus` dependency
to wire Prometheus metrics alongside the dashboard:

```toml
[dependencies]
rustvello = { version = "0.3.1", features = ["prometheus"] }
rustvello-prometheus = "0.3.1"
rustvello-monitoring = "0.3.1"
```

The dashboard automatically serves the `/metrics` endpoint when the Prometheus
`EventEmitter` is registered with the app.

Metrics exported include:

| Metric                               | Type      | Description                            |
| ------------------------------------ | --------- | -------------------------------------- |
| `rustvello_invocations_total`        | Counter   | Invocations registered by task and app |
| `rustvello_status_transitions_total` | Counter   | Status transitions by from/to state    |
| `rustvello_task_duration_seconds`    | Histogram | Task execution time by task ID         |
| `rustvello_broker_queue_depth`       | Gauge     | Current number of pending invocations  |
| `rustvello_runner_heartbeats_total`  | Counter   | Heartbeat events per runner            |

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

CSRF origin validation is enforced on all state-mutating routes.

```{toctree}
:hidden:
:maxdepth: 2

logging
triggers
```
