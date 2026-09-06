# Monitoring Investigation API

Rustvello Monitoring has an HTML dashboard for interactive use, compact JSON
endpoints for scripting, and CLI commands for direct backend inspection. They
read the same monitoring data; the machine-oriented surfaces avoid requiring a
tool to parse timeline SVG or browser markup.

## Investigate without the web service

When you have direct access to a SQLite-backed Rustvello store, query it with
the CLI instead of starting the monitoring server:

```bash
rustvello investigate <invocation-id> \
  --app-id <app-id> \
  --db-path <path-to-sqlite-db> \
  --format json
```

The CLI report includes the invocation record, ordered history, runner
contexts, registration runner, and matching atomic-service execution. Use
`--format text` for a compact operator-readable summary.

## Investigate one invocation

First discover the contract and active application:

```bash
curl -sS "http://127.0.0.1:59559/api/capabilities" | jq
```

`schema_version` changes only when the machine-facing contract changes. The
response advertises pagination limits, investigation routes, timeline filters,
and the equivalent direct-backend CLI command.

```bash
curl -sS \
  "http://127.0.0.1:59559/invocations/e2c09762-283b-482d-984a-6341163cf60e/investigation" \
  | jq
```

`GET /invocations/{invocation_id}/investigation` returns:

- Invocation identity, task, call, parent, and workflow references.
- Ordered history enriched with runner class, language, executor, host, PID,
  thread, and parent-runner context.
- The runner and timestamp that created the `Registered` event.
- The atomic-service execution containing that registration, when present.
- Trigger runs linked to the invocation, when the trigger store supports
  monitoring.
- Integrity flags that make missing registration provenance explicit.
- Relative links to the detail, history, and tightly focused timeline views.

The response is deliberately bounded: trigger evidence is limited to 50 rows
around the registration timestamp. It is an investigation view, not an
unbounded export API.

## Registration provenance

The task's execution worker is not necessarily its registration source. For
example, atomic service can evaluate a trigger on parent runner `A`, register
an invocation there, and a worker under runner `B` can dequeue and execute it.
The timeline represents this as:

1. An orange atomic-service window on runner `A`.
2. The `Registered` point on `A`'s control-plane row, above worker lanes.
3. A dashed relation from that point to the worker that moves the invocation
   through `Pending`, `Running`, and its terminal status.

This keeps control-plane provenance separate from worker execution and avoids
making the task appear to originate in the middle of an unrelated lane.

## Complementary endpoints

| Endpoint                                              | Use                                                             |
| ----------------------------------------------------- | --------------------------------------------------------------- |
| `/invocations/{id}/investigation`                     | One-shot provenance and integrity report.                       |
| `/invocations/{id}/api`                               | Core invocation record.                                         |
| `/invocations/{id}/history`                           | Ordered status history with runner contexts.                    |
| `/invocations/timeline?inv_ids={id}`                  | Interactive scoped timeline.                                    |
| `/invocations?workflow_type={task}&workflow_id={run}` | Workflow member invocation list.                                |
| `/workflows/{type}/{run}`                             | Workflow comparison page with that run selected.                |
| `/workflows/{type}?histogram_workflow={a},{b}`        | Compare multiple workflow runs.                                 |
| `/atomic-service`                                     | Recorded atomic-service execution windows.                      |
| `/atomic-service/execution?...`                       | Trigger and housekeeping detail for one service window.         |
| `/events`, `/events/{id}`                             | Trigger event monitoring.                                       |
| `/events/{id}/api`                                    | Event record, related trigger runs, and dashboard links.        |
| `/events/{id}/trigger-runs`                           | Bounded trigger-run evidence for one event.                     |
| `/events/{id}/trace`                                  | Source and generated invocation IDs for timeline investigation. |
| `/trigger-runs`, `/trigger-runs/{id}`                 | Trigger run monitoring.                                         |
| `/runners/{runner_id}`                                | Runner context, children, and activity.                         |

## Agent workflow

Any coding agent can use HTTP and `jq`; no model-specific integration is
required. Start with the investigation endpoint, inspect `integrity` and
`registration`, then open the returned timeline link only after forming a
provenance hypothesis. This works equally for Codex, Claude Code, and local
automation.

For the cross-language load fixture:

```bash
KEEP_ALIVE=1 cargo test -p rustvello-monitoring --test monitoring_load -- --ignored --nocapture
```

The command prints the monitoring URL. Query it while it remains open, then
use the human dashboard to verify visual layout.
