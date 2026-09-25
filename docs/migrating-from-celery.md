# Migrating from Celery

This recipe moves a Celery application to Rustvello's Python API, concept by
concept. The complete before/after pair is runnable and checked in CI:

- before: [`py-rustvello/examples/celery_migration/celery_app.py`](../py-rustvello/examples/celery_migration/celery_app.py)
- after: [`py-rustvello/examples/celery_migration/rustvello_app.py`](../py-rustvello/examples/celery_migration/rustvello_app.py)

```bash
make migration-example   # runs both files end to end
```

Read [When to use Rustvello](when-to-use.md) first: Rustvello covers Celery's
task model, not every Celery feature (see "What does not map" below).

## Infrastructure

| Celery                                                        | Rustvello                                                                                       |
| ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Broker (RabbitMQ, Redis) + result backend (Redis, a database) | One database: SQLite on one host, PostgreSQL across hosts. Queue, state and results share it.   |
| `celery -A proj worker -Q a,b -c 4`                           | `python -m rustvello.worker proj:app --queues a b --workers 4` (threads) or `--processes 4`     |
| `celery -A proj beat` (one scheduler process)                 | Nothing extra: runners evaluate cron triggers, and one firing publishes once across all runners |
| `task_acks_late=True`, `task_reject_on_worker_lost=True`      | The default behaviour: an invocation whose worker dies is recovered and re-run                  |
| Redis `visibility_timeout`                                    | `runner_dead_after_seconds` (heartbeat based; default 300 s)                                    |

```python
# Celery
app = Celery("shop", broker="amqp://...", backend="redis://...")
app.conf.update(task_acks_late=True, task_reject_on_worker_lost=True)

# Rustvello
app = App(app_id="shop", backend="postgres", postgres_url="postgresql://...")
```

## Tasks and calls

| Celery                                                 | Rustvello                                                                |
| ------------------------------------------------------ | ------------------------------------------------------------------------ |
| `@app.task`                                            | `@app.task`                                                              |
| `add.delay(1, 2)` / `add.apply_async((1, 2))`          | `add(1, 2)` returns an `Invocation`                                      |
| `result.get(timeout=10)`                               | `invocation.result(timeout=10)`                                          |
| `result.state`, `result.ready()`                       | `invocation.status`, `invocation.status.is_terminal()`                   |
| `result.revoke()`                                      | `invocation.cancel()`                                                    |
| `bind=True`, `self.request.id`, `self.request.retries` | `app.current_invocation().invocation_id`, `.num_retries`                 |
| `add.s(1, 2)` then `.delay()` later                    | Call the handle when you want to submit; there are no signatures         |
| `task_always_eager=True` in tests                      | `App(dev_mode_force_sync=True)` or `RUSTVELLO__DEV_MODE_FORCE_SYNC=true` |

Arguments and results are JSON, like Celery's default `json` serializer.
Pickle is not supported.

## Retries

```python
# Celery
@app.task(autoretry_for=(ConnectionError,), max_retries=3, retry_backoff=True)
def fetch_price(sku): ...

# Rustvello
@app.task(
    max_retries=3,
    retry_for=(ConnectionError,),
    retry_delay=1.0,        # Celery's retry_backoff=True starts at 1 s
    retry_backoff=2.0,      # and doubles
    retry_max_delay=600.0,  # retry_backoff_max
    retry_jitter="full",    # retry_jitter=True (Rustvello defaults to "equal")
)
def fetch_price(sku: str) -> int: ...
```

Differences that matter:

- A Rustvello retry is **stored with its not-before time** in SQLite or
  PostgreSQL. A worker never holds it in memory, so no visibility timeout can
  re-deliver it early, and a worker killed during the backoff loses nothing
  (kill-tested). On Redis, MongoDB and RabbitMQ Rustvello retries immediately.
- `self.retry(countdown=...)` from inside the body has no equivalent: raise
  an exception listed in `retry_for` instead.
- Celery's `time_limit` maps to `timeout` (seconds). A synchronous Python body
  cannot be killed on timeout unless the worker uses `--processes`; see
  [Execution deadlines](retries-timeouts-cancellation.md#execution-deadlines).

## Routing and queues

```python
# Celery
app.conf.task_routes = {"shop.send_receipt": {"queue": "emails"}}

# Rustvello: declare the queues, then route per task
app = App(app_id="shop", backend="sqlite", db_path="shop.db",
          config=AppConfig(app_id="shop", broker_queues=["default", "emails"]))

@app.task(queue="emails", priority=5)
def send_receipt(amount: int, order_id: str) -> str: ...
```

A worker consumes the queues given by `--queues` (`app.run(queues=[...])`).
Higher `priority` values are claimed first within a queue.

## Results

Results are stored in the same database as the queue, with the invocation's
status history. There is no `result_expires`; the configuration value
`auto_final_invocation_purge_hours` (for example
`RUSTVELLO__AUTO_FINAL_INVOCATION_PURGE_HOURS=72`) deletes finished invocations
after a while. `app.wait_results([...])` waits for several invocations with one
polling loop.

## Beat → cron triggers

```python
# Celery
app.conf.beat_schedule = {
    "nightly-report": {"task": "shop.nightly_report", "schedule": crontab(minute=0, hour=7)},
}

# Rustvello (cron with a leading seconds field)
app.trigger(nightly_report).on_cron("0 0 7 * * *").register()
app.trigger(cleanup).on_interval(300).register()   # every 300 s
```

Every runner may evaluate triggers; the atomic service lets one runner at a
time do it, and each firing publishes one invocation even across crashes
(trigger atomicity in the [guarantee matrix](guarantees.md)).

## Canvas → workflows

Celery composes signatures (`chain`, `group`, `chord`). Rustvello composes with
ordinary code inside an `@app.workflow`, which may block on child invocations:

```python
# Celery
chain(chord(group(fetch_price.s(s) for s in skus), total.s()),
      send_receipt.s(order_id)).apply_async()

# Rustvello
@app.workflow
def checkout(order_id: str, skus: list[str]) -> str:
    prices = app.wait_results([fetch_price(sku) for sku in skus], timeout=60)  # group
    amount = sum(prices)                                                       # chord callback
    return send_receipt(amount, order_id).result(timeout=30)                   # chain
```

A workflow occupies one worker slot while it waits, so give the runner more
slots than the number of workflows that run at the same time. Children keep
the workflow's identity, and the dashboard shows the run as one unit. If the
workflow body is retried, it runs again from the top and submits its children
again, as a Celery chain re-run would. Pass each child a key derived from the
workflow's invocation id and use it for the child's side effect, so the repeat
is harmless ([Idempotency](idempotency.md)).

## What does not map

- `chord` error callbacks, `link`/`link_error`, `map`/`starmap`/`chunks`:
  write the equivalent in the workflow body.
- Rate limits (`rate_limit="10/m"`): use `running_concurrency` to cap
  simultaneous executions; there is no rate-based limit.
- ETA/countdown on submission (`apply_async(countdown=60)`): not available;
  use a trigger or a delayed retry.
- Pickle serialization, Canvas signatures stored and sent later, Celery
  signals, Flower, Django integration: not available. Use the Rustvello
  dashboard and `/api/capabilities` for monitoring.
- Result backends separate from the broker: by design, there is one database.

## Checklist

1. Choose SQLite (one host) or PostgreSQL, and create the `App`.
2. Port task decorators; replace `.delay()`/`.get()` with calls and `.result()`.
3. Port retry options and time limits.
4. Declare queues in `AppConfig(broker_queues=...)`; set `queue=` per task.
5. Replace `beat_schedule` entries with triggers.
6. Rewrite canvas compositions as workflows.
7. Make side effects idempotent, as with `acks_late` ([Idempotency](idempotency.md)).
8. Run workers with `python -m rustvello.worker module:app`.
