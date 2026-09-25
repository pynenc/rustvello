# Idempotency and the at-least-once contract

Rustvello runs every invocation **at least once**. It does not run a task
body exactly once, and no task runtime can do that for effects outside its
own database. This page lists when a body runs more than once, how to make
side effects safe when it does, and how Rustvello compares with Celery,
Temporal and Restate. Each Rustvello claim names the test that checks it.

## The contract

| Rustvello promises                                                                                                                                                         | Checked by                                                                                                                                                                                  |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| An invocation that was accepted is not lost when a process dies (SQLite, PostgreSQL).                                                                                      | [Guarantee matrix](guarantees.md), _atomic publication_ and _durability_ columns                                                                                                            |
| The invocation id stays the same across retries and recoveries. It is the natural idempotency key.                                                                         | `idempotency_kill.rs::killed_worker_body_runs_again_and_keyed_effect_applies_once`, `test_idempotency.py::test_retry_reruns_the_body_with_the_same_invocation_id`                           |
| An invocation records **one** final result, even when its body ran several times. A stale worker that comes back cannot overwrite it (SQLite, PostgreSQL).                 | `publication_crash_acceptance.rs::resumed_stale_worker_cannot_publish_after_replacement`, `rustvello-postgres/src/acceptance.rs::leases_competing_recovery_completion_fencing_and_identity` |
| A trigger firing publishes one invocation, whose id is derived from the trigger run, so re-publication after a crash is idempotent.                                        | `trigger_crash_acceptance.rs::sqlite_trigger_firing_survives_kill_at_every_boundary` (and the PostgreSQL variant)                                                                           |
| A client that repeats a submission with the same invocation id (or the same idempotency key) gets the same invocation; different arguments under the same id are rejected. | `publication_crash_acceptance.rs::submission_kill_at_every_boundary_and_lost_ack_replay`, `idempotency_kill.rs::keyed_submission_creates_one_invocation_per_key`, `test_idempotency.py`     |

What Rustvello does **not** promise: that the body of a task runs once. The
final _status_ and _result_ are recorded once; the _side effects_ of the body
may happen more than once.

## When a body runs more than once

| Situation                                                                                                      | What happens                                                                                                                                                                                                               | Checked by                                                                                                                                                           |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A worker dies after a side effect** and before the result is recorded (crash, OOM kill, node loss).          | The invocation stays `RUNNING`. Once the worker's heartbeat is older than `runner_dead_after_seconds`, another runner recovers it (`RUNNING_RECOVERY`) and runs the whole body again, with the same invocation id.         | `idempotency_kill.rs::killed_worker_body_runs_again_and_keyed_effect_applies_once` (SQLite, SIGKILL)                                                                 |
| **A worker stalls** (long GC pause, frozen VM, lost network) long enough to be declared dead, then resumes.    | A replacement runs the body; the stalled body also finishes. Its result is rejected by ownership fencing on SQLite and PostgreSQL, but its side effects already happened. Redis and MongoDB have no fencing kill test yet. | `publication_crash_acceptance.rs::resumed_stale_worker_cannot_publish_after_replacement`                                                                             |
| **A retry** after an error (`max_retries`), including an error raised _after_ the effect (the reply was lost). | The whole body runs again. Nothing is resumed from the middle.                                                                                                                                                             | `test_idempotency.py::test_retry_reruns_the_body_with_the_same_invocation_id`                                                                                        |
| **A timeout or cancellation of a synchronous body** (`blocking = true` Rust tasks, sync Python tasks).         | A thread cannot be killed, so the abandoned body keeps running and its result is discarded. When the timeout is retried, the retry runs **beside** the abandoned body.                                                     | `retry_timeout_cancel.rs::abandoned_blocking_body_overlaps_its_retry_with_the_same_invocation_id`                                                                    |
| **A timeout or cancellation of an async body.**                                                                | The body stops at its next `.await`/`await`. An effect split across awaits can be left half done, and the retry starts from the top.                                                                                       | `retry_timeout_cancel.rs::async_body_past_its_deadline_is_aborted_and_retried`, `test_retry_timeout_cancel.py::test_async_timeout_cancels_the_coroutine_and_retries` |
| **A client repeats a submission** after an ambiguous acknowledgement (timeout, dropped connection).            | With a plain submit, this creates a second invocation. Submit with an explicit id or an idempotency key (below) to get the same invocation back.                                                                           | `publication_crash_acceptance.rs::submission_kill_at_every_boundary_and_lost_ack_replay`                                                                             |

Two backend notes. **Delayed retries** are durable on SQLite and PostgreSQL
only; on Redis, MongoDB and RabbitMQ the backend refuses the delay and the retry
runs immediately (`suite_broker_delayed_delivery_capability`), so a
rate-limited downstream API sees the retry sooner than configured. This does
not duplicate a run, but it removes the spacing you may rely on. **Stale-owner
recovery** is heartbeat based and not kill-tested on Redis and MongoDB (see the
[guarantee matrix](guarantees.md)): expect the same duplicate runs there,
with no test that bounds them.

Recovery timing is a configuration choice. With the defaults
(`heartbeat_interval_seconds = 30`, `runner_dead_after_seconds = 300`,
`recovery_check_interval_seconds = 60`), a crashed worker's invocation is
re-run about five minutes after its last heartbeat. Lower values recover sooner, but a
worker that stalls longer than `runner_dead_after_seconds` is then treated as
dead while it is still running, which is the second row of the table above.

## Making side effects idempotent

Use the **invocation id** as the key for every external effect. It is the same
in every attempt of one invocation and different for every invocation:

````{tab} Rust
```rust
use rustvello::prelude::*;

#[rustvello::task(max_retries = 3)]
fn charge(order_id: String, cents: u64) -> RustvelloResult<String> {
    let invocation_id = get_invocation_context()
        .expect("inside a task")
        .invocation_id
        .to_string();
    // Stripe-style: the payment provider deduplicates on the key.
    let key = format!("{invocation_id}:charge");
    payments::charge(&order_id, cents, &key)?;
    Ok(key)
}
```
````

````{tab} Python
```python
@app.task(max_retries=3, retry_for=(ConnectionError,))
def charge(order_id: str, cents: int) -> str:
    key = f"{app.current_invocation().invocation_id}:charge"
    payments.charge(order_id, cents, idempotency_key=key)  # provider deduplicates
    return key
```
````

Patterns, from strongest to weakest:

1. **Let the receiver deduplicate.** Pass the key to an API that supports
   idempotency keys (most payment and messaging APIs do). Suffix the key with
   a step name (`:charge`, `:email`) when one task performs several effects.
2. **Write with a unique constraint.** Insert into a table whose primary key
   (or unique index) is the key: `INSERT ... ON CONFLICT DO NOTHING` in
   PostgreSQL and SQLite. A repeated write becomes a no-op. The kill test uses
   the file-system version of this (`create_new`).
3. **Write the effect and the key in one transaction.** When the effect is
   your own database, commit the change together with a "done" row for the key.
4. **Check, then act** only when nothing else is available: read whether the
   key is done, act, record it. Two overlapping runs (a stalled worker and its
   replacement, a timed-out sync body and its retry) can both pass the check,
   so pair it with a lock or a conditional write.

Also:

- **Keep effects late and few.** Compute first, then perform the effect as the
  last step, so a failure before it needs no cleanup.
- **Key child effects by the parent.** When a workflow is retried, its body
  runs again and submits new child invocations. Pass a key derived from the
  parent's invocation id (for example `f"{invocation_id}:charge"`) to each child
  and use it for the child's effect, so the new child's effect is a no-op.
  Re-submitting the child itself with `submit_with_key` from the retry does not
  work today: each attempt runs in its own trace span, which is part of the
  replay identity, so the second submission is rejected
  (`test_idempotency.py::test_keyed_child_resubmitted_by_a_parent_retry_is_rejected`).
- **Prefer async bodies or the process pool** for work that must stop at a
  deadline; an abandoned synchronous thread keeps running
  ([details](retries-timeouts-cancellation.md#execution-deadlines)).
- **Use the attempt signal** in long synchronous loops to stop once the runner
  gave up on the attempt (`current_attempt_signal()`).
- **Inside workflows**, use the deterministic helpers
  (`workflow_root().uuid()`, `random()`, `utc_now()`) so that a re-run derives
  the same values ([Workflows](workflows.md)).

## Deduplicating submissions: idempotency keys

Retrying a _submission_ is a separate problem: an HTTP handler that times out
while submitting does not know whether the invocation exists. Two helpers turn
a repeated submission into the same invocation. Both need a backend with
atomic publication (SQLite or PostgreSQL); other backends refuse the call
instead of pretending to deduplicate.

````{tab} Rust
```rust
// Same key + same arguments -> same invocation; same key + other arguments -> error.
let handle = app
    .submit_call_with_key(&request_id, &PlaceOrderTask::new(), params, None)
    .await?;

// Or derive the id yourself and keep it (InvocationId::from_key is a UUID v5
// of the task id and the key).
let id = InvocationId::from_key(Task::task_id(&PlaceOrderTask::new()), &request_id);
let handle = app.submit_call_with_id(id, &PlaceOrderTask::new(), params, None).await?;
```
````

````{tab} Python
```python
inv = place_order.submit_with_key(request_id, order=order_id, amount=10)

# The same id from Python and Rust:
InvocationId.from_key("python::shop.place_order", request_id)
```
````

A replay is accepted only when it matches the first submission: the arguments,
the parent/workflow lineage and the W3C trace context
(`test_durable_submission.py::test_durable_replay_restores_original_w3c_context_and_rejects_drift`).
Replaying from another trace is rejected as "different content or lineage".
In practice this means: replay from the same client context (a retry loop around
the submit call, a process that restarts without OpenTelemetry context), not
from a new traced HTTP request, and not from a later attempt of a parent task.
Keyed submissions are therefore a tool for **top-level clients**; inside tasks,
key the effects instead.

Once a finished invocation is purged (`auto_final_invocation_purge_hours`),
a replay of its id fails with "submission ID was removed" rather than running
the task again (`rustvello-sqlite/src/publication_tests.rs::replay_rejects_different_content_and_purge_is_explicit`). The key never makes the task body run once: combine it with
idempotent effects.

## Comparison

The table summarizes what each system documents. Quotes are from the linked
official pages (checked 2026-09-25).

| System    | Unit that may run twice                                                                                           | What it deduplicates for you                                                                           | What you must do                                    |
| --------- | ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------------- |
| Rustvello | The whole task body (crash, stall, retry, abandoned sync body).                                                   | Final status/result per invocation; submissions with an explicit id or key; trigger firings.           | Make each effect idempotent with the invocation id. |
| Celery    | With the default early ack, a task that started "is never executed again"; with `acks_late`, the whole task body. | Nothing by default; the result backend stores the last state.                                          | Idempotent tasks when `acks_late` is on.            |
| Temporal  | An Activity.                                                                                                      | Completed Activity results are recorded in the Workflow history; the Workflow replays them.            | Idempotent Activities; deterministic Workflow code. |
| Restate   | A `ctx.run` step whose result was not yet recorded.                                                               | Recorded step results are replayed instead of re-executed; ingress idempotency keys deduplicate calls. | Idempotent actions inside `ctx.run`.                |

**Celery.** By default "the default behavior is to acknowledge the message in
advance, just before it's executed, so that a task invocation that already
started is never executed again": at-most-once for a task that started. With
`acks_late`, "the task may be executed multiple times should the worker crash in
the middle of execution. Make sure your tasks are idempotent"
([Tasks: acks_late](https://docs.celeryq.dev/en/stable/userguide/tasks.html)).
Even then, "the worker will acknowledge tasks when the worker process executing
them abruptly exits" unless `task_reject_on_worker_lost` is enabled, which
"can cause message loops"
([Configuration](https://docs.celeryq.dev/en/stable/userguide/configuration.html)).
On Redis, a task not acknowledged within the visibility timeout (default one
hour) "will be redelivered to another worker and executed", and ETA or retry
tasks that wait longer than the timeout are "executed again, and again in a
loop" ([Redis broker](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html)).
Rustvello's model is Celery's `acks_late` plus `task_reject_on_worker_lost`:
nothing accepted is dropped, so effects must be idempotent. Its delayed
retries on SQLite and PostgreSQL are stored with their not-before time and do
not depend on a visibility timeout.

**Temporal.** "Temporal recommends that Activities be idempotent", because
"the Activity may be executed multiple times", while it "will be observed as
completed exactly once"
([Activity definition](https://docs.temporal.io/activity-definition#idempotency)).
Workflow code "must be deterministic to support replay"; external calls belong
in Activities
([Workflow definition](https://docs.temporal.io/workflow-definition#deterministic-constraints)).
A Rustvello task corresponds to an Activity: same contract, same advice. A
Rustvello workflow re-runs its body on retry and relies on child invocations
and deterministic helpers rather than a replayed event history; long-lived
durable waits (timers, signals) are not part of Rustvello today.

**Restate.** `ctx.run` wraps a non-deterministic operation and Restate
"store[s] its result in the execution log"; on replay the SDK "checks the
journal for the last recorded result. If it finds a previous result, it skips
executing that action"
([Durable steps](https://docs.restate.dev/develop/python/durable-steps),
[Request lifecycle](https://docs.restate.dev/guides/request-lifecycle)).
"Failures in `ctx.run` are treated the same as any other handler error. Restate
will retry it". Our reading, not a Restate statement: an action whose result
was not recorded yet (a crash right after the effect) is executed again, so the
action itself should still be idempotent. Restate's journal gives step-level
resumption inside a handler; Rustvello gives it at child-task granularity.

## See also

- [Guarantee matrix](guarantees.md): per-backend levels and their proving tests.
- [Retries, timeouts and cancellation](retries-timeouts-cancellation.md).
- [When to use Rustvello](when-to-use.md).
