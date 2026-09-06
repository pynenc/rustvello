# Rustvello Agent Investigation Guide

This file is for Codex, Claude Code, Cursor, human operators, and any other
agent debugging a Rustvello application. Rustvello monitoring is not just a web
page: it is a set of query surfaces over the same broker, orchestrator, state
backend, trigger store, and runner metadata used by the app.

Use the HTTP API when a monitoring server is already running. Use the CLI when
you have direct backend access and do not want to start the monitoring web
service. Prefer bounded JSON queries before opening large HTML pages.

## First response checklist

Given an invocation id:

1. Fetch the one-shot investigation report.
2. Confirm the `Registered` event has a runner id and runner context.
3. Confirm whether registration happened inside an atomic-service window.
4. Compare the registration runner with the worker runners that recorded
   `Pending`, `Running`, and terminal status.
5. Check `parent_invocation_id`, `registered_by_invocation_id`, and `workflow`.
6. Only then open the timeline view for visual validation.

The task language is not the registration source. The worker that runs a task
is not necessarily the runner that registered it.

## CLI Investigation

The CLI can inspect SQLite-backed Rustvello data without starting the
monitoring server:

```bash
rustvello investigate <invocation-id> \
  --app-id <app-id> \
  --db-path <path-to-sqlite-db> \
  --format json
```

Useful variants:

```bash
rustvello investigate <invocation-id> --app-id prod-orders --db-path ./prod.sqlite --format text
rustvello status <invocation-id> --db-path ./prod.sqlite
rustvello list --task rust::orders.process_order --db-path ./prod.sqlite
rustvello list --status Running --db-path ./prod.sqlite
```

`investigate` returns:

- invocation identity: id, task id, call id, status, created/updated timestamps
- parent invocation id
- workflow id, workflow type, parent id, and depth
- ordered status history
- runner contexts for every runner referenced by history
- registration timestamp and registration runner
- atomic-service execution containing the registration timestamp, if any

The CLI currently targets SQLite because it can construct those backends
locally with no app code. For Mongo/Postgres/Redis/RabbitMQ production stacks,
start a monitoring instance for the app or add an equivalent backend-specific
CLI constructor.

## HTTP API

Use `BASE=http://127.0.0.1:<port>` in examples below.

Discover the API contract exposed by the running monitor before investigating:

```bash
curl -sS "$BASE/api/capabilities" | jq
```

The response identifies the active app, supported page sizes, all investigation
and list routes, accepted timeline filters, timestamp rules, and the matching
direct-backend CLI command. Treat `schema_version` as the compatibility key for
agent integrations.

### Invocation Endpoints

```bash
curl -sS "$BASE/invocations/<id>/investigation" | jq
curl -sS "$BASE/invocations/<id>/api" | jq
curl -sS "$BASE/invocations/<id>/history" | jq
```

`/invocations/{id}/investigation` is the best first query. It joins:

- core invocation data
- ordered status history
- runner context per history row
- parent and workflow references
- registration provenance
- trigger runs linked to the registration when available
- matching atomic-service window
- integrity flags
- links to related human views

`/invocations/{id}/api` is the compact invocation record. Use it when you only
need task, call, parent, workflow, and current status.

`/invocations/{id}/history` is the event sequence. Use it to verify timing,
runner handoffs, retries, and whether the same invocation moved across runners.

### Invocation List And Timeline

```bash
curl -sS "$BASE/invocations?workflow_type=<task-id>&workflow_id=<run-id>&limit=100"
curl -sS "$BASE/invocations/timeline?inv_ids=<id>"
curl -sS "$BASE/invocations/timeline?selected=<id>&inv_ids=<id>"
curl -sS "$BASE/invocations/timeline?workflow_type=<task-id>&workflow_id=<run-id>"
curl -sS "$BASE/invocations/timeline?time_range=custom&start_date=<rfc3339>&end_date=<rfc3339>"
```

Timeline filters:

- `time_range`: `1m`, `5m`, `15m`, `1h`, `3h`, `12h`, `1d`, `3d`, `1w`,
  `custom`
- `start_date`, `end_date`: RFC3339 or `YYYY-MM-DDTHH:MM:SS.sss`
- `task_id`: canonical task id, for example `python::orders.normalize`
- `workflow_type`: workflow root task id
- `workflow_id`: workflow run id, usually the main invocation id
- `selected`: invocation id highlighted and opened in the details panel
- `inv_ids`: comma/space/newline separated invocation ids to scope loading
- `runner_ids`: comma/space/newline separated runner or worker ids
- `limit`: maximum rendered invocations
- `histogram_status`: occupancy categories, comma separated

When `workflow_id` is supplied without a custom range, the timeline derives the
workflow run's min/max history window. When `selected` is supplied with
`inv_ids`, the timeline loads that invocation directly even if the range index
is sparse.

### Workflow Endpoints

```bash
curl -sS "$BASE/workflows"
curl -sS "$BASE/workflows/<workflow-type>"
curl -sS "$BASE/workflows/<workflow-type>?histogram_workflow=<run-a>,<run-b>&limit=25"
curl -sS "$BASE/workflows/<workflow-type>/<run-id>"
curl -sS "$BASE/workflows/children/<run-id>"
```

Workflow terms:

- `workflow_type`: the task id that defines the workflow
- `workflow_id`: the run id, normally the main/root invocation id
- `histogram_workflow`: comma-separated run ids selected for comparison

Use `/workflows/{type}/{run}` when a timeline or details panel links to a
specific workflow run. It redirects to the workflow page with that run selected
and jumps to the page where the run is visible.

Use `/invocations?workflow_type=...&workflow_id=...` for the detailed member
list, and `/invocations/timeline?workflow_type=...&workflow_id=...` for the
same run on the timeline.

### Runner Endpoints

```bash
curl -sS "$BASE/runners"
curl -sS "$BASE/runners/<runner-id>"
```

Use runner pages to answer:

- does the runner context exist?
- is it a parent runner or child worker?
- what language and executor does it run?
- what host, PID, and thread are recorded?
- is it atomic-service eligible?
- which invocations were processed by it or by its children?

### Atomic Service Endpoints

```bash
curl -sS "$BASE/atomic-service"
curl -sS "$BASE/atomic-service/execution?runner_id=<runner-id>&start=<rfc3339>&end=<rfc3339>"
```

Atomic-service windows are control-plane work. A triggered invocation should
show `Registered` on the runner control-plane row, while execution should show
on the worker that dequeued the task.

Use the execution detail view to inspect trigger runs claimed during a specific
atomic-service window. Use its timeline link to see the control-plane window in
context.

### Event And Trigger Endpoints

```bash
curl -sS "$BASE/events"
curl -sS "$BASE/events?event_code=<code>"
curl -sS "$BASE/events/<event-id>"
curl -sS "$BASE/trigger-runs"
curl -sS "$BASE/trigger-runs/<trigger-run-id>"
```

Use these when an invocation was created by triggering. Correlate:

- event id and condition id
- trigger definition id
- claimed/executed timestamps
- `triggered_invocation_id`
- `source_invocation_id`
- `atomic_service_runner_id`

### Logs

```bash
curl -sS "$BASE/log-explorer"
```

The log explorer resolves compact runner ids to stored runner contexts and
builds scoped timeline links for referenced invocations. Use it when the user
provides logs rather than ids.

## Investigation Recipes

### Floating Registered Point

```bash
curl -sS "$BASE/invocations/<id>/investigation" | jq '.registration, .history, .integrity'
```

Expected facts:

- `integrity.has_registered_event` is true
- `integrity.registration_runner_known` is true
- if a trigger created the invocation, `registration.atomic_service_execution`
  is present
- the first history row's runner is a parent/control-plane runner
- later `Pending`/`Running` rows may be a different worker runner

If the timeline shows the point without a runner row, check whether the visible
range contains only atomic-service data. The SVG should still create a
control-plane row for that runner.

### Workflow Run Not Shown

```bash
curl -sS "$BASE/workflows/<workflow-type>/<run-id>" -I
curl -sS "$BASE/invocations/timeline?workflow_type=<workflow-type>&workflow_id=<run-id>"
```

The workflow route should land on the workflow page with that run selected. The
timeline route should echo `workflow_id` in the filter and render only members
of that run.

### Wrong Worker Language

Check the task id and runner context separately:

```bash
curl -sS "$BASE/invocations/<id>/api" | jq '.task_id, .task_language'
curl -sS "$BASE/invocations/<id>/history" | jq '.[].runner_info'
```

A `python::...` task may be registered by a Rust runner if Rust evaluated the
trigger, but it must run on a Python worker. A `rust::...` task must run on a
Rust worker.

## Verification Commands

```bash
cargo check -p rustvello-cli -p rustvello-monitoring
cargo test -p rustvello-monitoring --lib --no-fail-fast
cargo test -p rustvello-monitoring --test monitoring_dashboard -- --nocapture
cargo test -p rustvello-monitoring --test monitoring_load -- --ignored --nocapture
```

For visual/manual debugging:

```bash
KEEP_ALIVE=1 cargo test -p rustvello-monitoring --test monitoring_load -- --ignored --nocapture
```

The command prints the monitoring URL. Query the API first, then use the HTML
dashboard to confirm visual layout.
