# When to use Rustvello

Rustvello is a **task runtime**: you submit function calls, workers run them,
and a database records their status and results. It is not a durable workflow
engine in the Temporal or Restate sense. This page is meant to help you decide
quickly, including when to pick something else.

## Use Rustvello when

- **You want durable background tasks without a separate broker.** On SQLite
  (one host) or PostgreSQL (many hosts), the queue, the invocation state and
  the results live in one database and change in one transaction. A process
  that dies mid-publication loses nothing, which the
  [guarantee matrix](guarantees.md) ties to process-kill tests.
- **Your code is Python, Rust, or both.** Python tasks (`@app.task`, `async def`
  included) and Rust tasks (`#[rustvello::task]`, `async fn` included) share one
  queue and call each other through language-qualified task ids.
- **You need the task-queue basics with clear semantics**: retries with
  backoff and jitter, durable delayed retries (SQLite, PostgreSQL), execution
  deadlines, cancellation, named queues with priorities, cron and event
  triggers, concurrency limits per task, argument or key
  ([Retries, timeouts and cancellation](retries-timeouts-cancellation.md)).
- **You want to inspect what happened.** A built-in dashboard, a JSON
  investigation API (`/api/capabilities`) and a CLI answer "who registered
  this, which worker ran it, why did it retry" from the same backend.
- **You can make side effects idempotent.** Rustvello runs a task body at least
  once; see [Idempotency](idempotency.md).
- **You are migrating from Celery** and want the same task model with fewer
  moving parts: see [Migrating from Celery](migrating-from-celery.md).

## Use something else when

| You need                                                                                          | Consider                                         | Why                                                                                                                                                                                        |
| ------------------------------------------------------------------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Long-running workflows that sleep for days, wait for signals or human approval, and resume a step | Temporal, Restate                                | Rustvello has no durable timers or external signals. A workflow retry re-runs its body; it does not resume from a journal.                                                                 |
| Safe upgrades of in-flight workflow code (versioning, replay checks)                              | Temporal                                         | Not implemented in Rustvello.                                                                                                                                                              |
| Exactly-once side effects                                                                         | Nobody offers this for external effects          | Every system here, Rustvello included, is at-least-once for code that touches the outside world. Use idempotency keys ([Idempotency](idempotency.md)).                                     |
| A large, mature ecosystem: Django integration, Flower, many brokers, years of production reports  | Celery                                           | Rustvello is young (0.x). Its PostgreSQL and SQLite backends are kill-tested; there is no public record of large production deployments yet.                                               |
| Full durability on Redis, MongoDB or RabbitMQ                                                     | Rustvello on SQLite/PostgreSQL, or Celery        | On those backends Rustvello's publication, recovery and delayed retries are best effort or unsupported ([guarantee matrix](guarantees.md)).                                                |
| Very high throughput with proven numbers at scale                                                 | Measure first                                    | The [benchmark](benchmarks.md) runs at modest scale on one machine. It does not establish behaviour at thousands of tasks per second.                                                      |
| Languages other than Python and Rust                                                              | Temporal (many SDKs), a broker with many clients | Rustvello runs Python and Rust tasks only.                                                                                                                                                 |
| Sub-millisecond dispatch latency                                                                  | An in-process queue or a message broker          | Idle workers poll the database every `idle_sleep_ms` (at most 100 ms on SQLite and PostgreSQL; the Python worker defaults to 50 ms), which adds up to that much latency on an idle system. |

## Deciding between SQLite and PostgreSQL

| Situation                                                   | Backend                                             |
| ----------------------------------------------------------- | --------------------------------------------------- |
| One host (a VM, one container with a volume, a desktop app) | SQLite, file database, `synchronous=FULL`           |
| Several worker hosts, or managed database operations wanted | PostgreSQL                                          |
| Tests and local development                                 | Memory backend or `dev_mode_force_sync`             |
| Redis, MongoDB or RabbitMQ already mandated                 | Possible, with the best-effort levels in the matrix |

## What "evaluated" means today

Rustvello's durability claims are those in the [guarantee matrix](guarantees.md),
each tied to tests that run in the release gate. Nothing else is claimed: no
production-scale qualification, no multi-region setup, no formal verification.
If your decision depends on a property that is not in the matrix, test it or
ask in an issue before relying on it.
