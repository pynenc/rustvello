# Using Rustvello from an agent

This page is for coding agents (and the people who run them) that build, run or
debug an application on Rustvello. It lists the agent skill, the commands that
are safe to run without asking, what never to do, and how to read errors and
the machine-facing monitoring API.

## Start here

| Need                                               | Where                                                                                                      |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Task-by-task instructions with runnable examples   | The agent skill, `skills/rustvello/SKILL.md` in the repository                                             |
| An index of this documentation for language models | `llms.txt` at the root of this documentation site and of the repository                                    |
| Investigate an invocation                          | `GET /api/capabilities`, then `GET /invocations/<id>/investigation` ({doc}`contributing/agent-monitoring`) |
| Which backend guarantees what                      | {doc}`guarantees` (also served in `/api/capabilities`)                                                     |
| Retried bodies, keyed submissions and side effects | {doc}`idempotency`                                                                                         |
| Whether Rustvello fits, or porting a Celery app    | {doc}`when-to-use`, {doc}`migrating-from-celery`                                                           |
| Debugging recipes for this repository's own code   | [AGENTS.md](https://github.com/pynenc/rustvello/blob/main/AGENTS.md)                                       |

## The agent skill

`skills/rustvello/` is a skill in the common `SKILL.md` format (front matter
with `name` and `description`, then instructions), with small examples and
helper scripts. It needs no MCP server: everything runs through Python, the
`rustvello` wheel and HTTP. It covers setting up an app on SQLite, sync and
async tasks with retries, backoff, timeouts and cancellation, running workers,
submitting and waiting, idempotent (keyed) submission, cron triggers, choosing
a backend from the guarantee matrix, and investigating a failed invocation.

Install it where your agent looks for skills. For Claude Code:

```bash
git clone --depth 1 https://github.com/pynenc/rustvello /tmp/rustvello
mkdir -p .claude/skills && cp -r /tmp/rustvello/skills/rustvello .claude/skills/   # this project
cp -r /tmp/rustvello/skills/rustvello ~/.claude/skills/                           # every project
```

Other agents that read `SKILL.md` folders take the same directory; an agent
without skill support can be pointed at `skills/rustvello/SKILL.md` directly.

The skill states the Rustvello version it was written for
(`metadata.rustvello-version`, and the `pip install` pin). Use the skill from
the same release as the installed wheel.

**It cannot drift.** CI copies the skill directory alone to a scratch location
and runs every example and helper script in it against the wheel built from the
same commit (`make skill-examples`, the "README examples" job). The same check
fails when the skill's version does not match the package, and when a Python
snippet in `SKILL.md` calls an API the wheel does not have.

## Commands safe to run without asking

These only read, or write temporary files of their own:

- `python -c "import rustvello; print(rustvello.__version__)"`
- the skill's examples (`python skills/rustvello/examples/<name>.py`): each uses a
  temporary SQLite file and stops its own worker
- `python skills/rustvello/scripts/guarantees.py [--need …]`: prints the
  guarantee matrix of the installed version
- `python skills/rustvello/scripts/investigate.py <id> --db-path <db> --app-id <app>`:
  serves the monitoring API for that SQLite file on a free local port while it
  runs and prints the investigation report; it changes no invocation
- `GET` requests to a running monitor: `/api/capabilities`,
  `/invocations/<id>/investigation`, `/invocations/<id>/api`,
  `/invocations/<id>/history`, the list endpoints
- `rustvello investigate|status|list … --db-path <db>` (Rust CLI)

## Never do without the owner's approval

- `app.purge()` or `rustvello purge`: deletes every queued invocation, the
  stored state and the triggers.
- Delete, move or overwrite a database file that is not a temporary one, or
  run an example against it.
- `cancel` invocations (`Invocation.cancel()`, `App.cancel()`, `rustvello
cancel`) or start a worker against a production backend: both change
  production state.
- Print, log or commit connection strings that carry passwords
  (`postgres_url`, `redis_url`, `mongo_url`, `rabbitmq_url`, `otlp_bearer_token`).

## How to read errors

On the producer side, `Invocation.result()` raises:

| Exception                                             | Meaning                                                                                                                | Next step                                                                                                             |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `RuntimeError("Task failed: <ErrorType>: <message>")` | The invocation is `FAILED` after the retries its task allows                                                           | Read `<ErrorType>`; `TaskTimeoutError` means an attempt exceeded its `timeout` ({doc}`retries-timeouts-cancellation`) |
| `rustvello.InvocationCancelledError`                  | The invocation was cancelled                                                                                           | Find out who cancelled before resubmitting                                                                            |
| `TimeoutError("Invocation … still PENDING after …s")` | No worker finished it in time; usually no worker runs for that `app_id` and backend, or it does not consume that queue | Start `python -m rustvello.worker module:app`; check the worker imported the task's module under the same name        |

`TaskHandle.submit_with_key(key, ...)` raises when the same key was already
submitted with other arguments or lineage ("different content or lineage"),
and when the backend cannot submit durably ("does not support crash-consistent
..."; use SQLite or PostgreSQL). See {doc}`idempotency`.

`App.trigger(...).register()` raises `ValueError` for an invalid cron
expression and for a trigger on a foreign (Rust) task, and `RuntimeError`
when the backend has no trigger store.

## Investigation report

`GET /invocations/<id>/investigation` returns one JSON object:

| Field          | Content                                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------------------- |
| `invocation`   | `id`, `task_id`, `call_id`, current `status` (latest history row), `parent_invocation_id`, `workflow`          |
| `error`        | `error_type`, `message` and the end of the `traceback` of a failed invocation, else `null`                     |
| `history`      | Every status change in order (`Registered`, `Pending`, `Running`, `Retry`, …) with the runner that recorded it |
| `registration` | When and by which runner the invocation was registered, its atomic-service window and trigger runs             |
| `integrity`    | Flags that must be true for a consistent record                                                                |
| `links`        | The human views (detail page, timeline, history)                                                               |

`/api/capabilities` names these routes and carries `schema_version`: fields are
only added within a schema version. `App.start_monitor(port=0)` binds a free
port; `server.address` reports it.

## Measuring how well agents use Rustvello

`evals/` in the repository holds a cross-model evaluation: fixed install,
implement and recover tasks, and discovery prompts that do not name Rustvello,
scored for task success, wrong or nonexistent API use, interventions needed and
recommendation rate. See `evals/README.md` for how to run it and record a
baseline.
