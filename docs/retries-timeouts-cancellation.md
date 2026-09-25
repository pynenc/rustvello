# Retries, timeouts and cancellation

This page describes how a failed attempt is retried after a delay, how an
attempt that runs too long is stopped, and how a user cancels an invocation.
It also covers what each of these does to side effects your task already
performed.

All three features are off by default. A task configured as before this page
existed behaves the same: failed attempts retry immediately (when
`max_retries` allows), and attempts have no deadline.

## Retry backoff

A task retries a failed attempt when `max_retries` allows it and the error
matches `retry_for_errors` (an empty list matches every error). The retry
policy decides **when** the retry runs:

| Option (Rust `TaskConfig` / macro) | Python `@app.task` | Default   | Meaning                                                   |
| ---------------------------------- | ------------------ | --------- | --------------------------------------------------------- |
| `retry_delay_ms`                   | `retry_delay` (s)  | `0`       | Delay before the first retry; `0` retries at once         |
| `retry_max_delay_ms`               | `retry_max_delay`  | `300000`  | Cap of the delay before jitter (5 minutes)                |
| `retry_backoff`                    | `retry_backoff`    | `2.0`     | Growth factor per retry (values below `1.0` act as `1.0`) |
| `retry_jitter`                     | `retry_jitter`     | `"equal"` | `"equal"`, `"full"` or `"none"`                           |

For retry number `n + 1` (after `n` earlier retries) the delay is:

```text
d = min(retry_max_delay, retry_delay * retry_backoff^n)

none:  d
full:  uniform in [0, d]
equal: d/2 + uniform in [0, d/2]      (default)
```

**Why equal jitter is the default.** Full jitter spreads retries best, but it
can pick a delay near zero, so a configured 30-second delay may turn into
an immediate retry. Equal jitter keeps at least half of the computed delay and
still spreads the retries of many invocations that failed together.
"Decorrelated" jitter is not offered: each delay depends on the previous
random delay, so Rustvello would have to store per-invocation state.

````{tab} Rust
```rust
#[rustvello::task(
    max_retries = 5,
    retry_for_errors = ["ConnectionError"],
    retry_delay_ms = 500,
    retry_max_delay_ms = 60_000,
    retry_backoff = 2.0,
    retry_jitter = "equal",
)]
fn fetch(url: String) -> RustvelloResult<String> {
    // ...
}
```
````

````{tab} Python
```python
@app.task(
    max_retries=5,
    retry_for=(ConnectionError,),
    retry_delay=0.5,
    retry_max_delay=60,
    retry_backoff=2.0,
    retry_jitter="equal",
)
def fetch(url: str) -> str: ...
```
````

The same fields can be set in TOML (`[tasks.<name>]`, `[task_defaults]`) and
through environment variables such as `RUSTVELLO__TASK__FETCH__RETRY_DELAY_MS`,
`RETRY_MAX_DELAY_MS`, `RETRY_BACKOFF`, `RETRY_JITTER`, `TIMEOUT_MS` and
`RETRY_ON_TIMEOUT`.

### Durable delayed retries

A worker never sleeps through a backoff. When an attempt fails, the worker
commits the `RETRY` status and a queued entry whose **not-before time is stored
in the backend**, then moves on. The entry stays invisible to every worker, and
to queue counts, until it is due. Then exactly one worker claims it: the
broker's atomic claim delivers it once, and the status machine rejects any
second claim.

If the worker that scheduled the retry dies during the backoff, nothing is
lost and nothing is duplicated. Any worker started later runs the retry once,
at or after the not-before time. The kill test
`crates/rustvello/tests/durable_retry_kill.rs` checks this: it SIGKILLs the
worker during a 2-second backoff and starts two competing replacement workers.
It then asserts that exactly one retry ran, and that it ran no earlier than
2 seconds after the failure.

| Backend                  | Delayed retry                   | Where the not-before time lives                                                                     |
| ------------------------ | ------------------------------- | --------------------------------------------------------------------------------------------------- |
| SQLite                   | Durable                         | Queue row plus an unheld delivery lease, committed in the same transaction as `RETRY` (local clock) |
| PostgreSQL               | Durable                         | Queue row `reserved_until`, set on the **database clock** in the `RETRY` transaction                |
| Memory                   | Process-local                   | The in-memory queue: survives a runner restart in the same process, not a process exit              |
| Redis, MongoDB, RabbitMQ | Not supported (immediate retry) | —                                                                                                   |

Brokers report this through `Broker::supports_delayed_delivery()`, and
transactional publications through `RuntimePublication::supports_delayed_retry()`.
A backend without support refuses delayed delivery; it never pretends to
accept it. The runner then routes the retry immediately, as in earlier
releases, and logs one warning per process. Use SQLite or PostgreSQL when you
need the backoff to hold.

A worker that picks up a retry early from a blocking-priority shortcut (a
parent waiting on the child) would bypass the delay, so that shortcut skips
`RETRY` invocations. They are only delivered through their queued entry.

## Execution deadlines

`timeout_ms` (Rust) or `timeout` in seconds (Python) sets a deadline for **one
attempt**. When it expires, the attempt fails with error type
`TaskTimeoutError`, and the invocation then follows the retry policy.
Set `retry_on_timeout = false` to make a timeout final even when `max_retries`
allows more attempts. `TaskTimeoutError` can also be listed in
`retry_for_errors`.

The deadline starts when the worker begins the attempt. It includes waiting for
a local executor slot.

What happens to the code that was running depends on how the task runs:

| Task body                                        | On expiry                                                                                                                                          |
| ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Async (native async tasks)                       | The future is dropped: execution stops at its next `.await`. Code after that point never runs.                                                     |
| Sync, `blocking = true` (and every Python task)  | A thread cannot be killed safely, so it keeps running in the background. Its result is discarded, and it keeps its executor slot until it returns. |
| Sync, `blocking = false`                         | It cannot be preempted. The deadline is checked when it returns, and a late result is discarded.                                                   |
| Subprocess executor (`App.run(num_processes=N)`) | The worker process is killed and replaced, so the body really stops.                                                                               |

Because an abandoned synchronous body keeps its slot, a task that times out
repeatedly can occupy every blocking slot. Either give such work a
cooperative stop (check a flag, bound your I/O timeouts) or run it in the
process pool.

## Cancellation

Any client can cancel an invocation that has not finished:

````{tab} Rust
```rust
match app.cancel(&invocation_id).await? {
    CancelOutcome::Cancelled => println!("cancelled"),
    CancelOutcome::AlreadyFinal(status) => println!("already {status}"),
    _ => {}
}
```
````

````{tab} Python
```python
inv = fetch("https://example.org")
inv.cancel()          # True if this call cancelled it
app.cancel(inv)       # same, from the app
```
````

````{tab} CLI
```bash
rustvello cancel <INVOCATION_ID> --app-id my-app --db-path ./tasks.db
```
````

`CANCELLED` is a terminal status. Any non-final status can move to it, and the
move is not limited to the runner that owns the invocation:

| Invocation was                                        | Effect                                                                                                                                                                                      |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Registered, pending, rerouted, concurrency-controlled | It never runs. Its queued entry is dropped, or skipped when dequeued.                                                                                                                       |
| Backing off before a retry (`RETRY`)                  | The retry never runs.                                                                                                                                                                       |
| Running                                               | The worker re-reads the status every `cancellation_check_interval_seconds` (default 1 s) and abandons the attempt. The same rules as for deadlines apply, and the late result is discarded. |
| Already finished (success, failure, cancelled)        | Nothing changes. Rust returns `CancelOutcome::AlreadyFinal`, Python returns `False`, and the CLI exits with code 3.                                                                         |

Waiters are released, and triggers see the `CANCELLED` status. Reading the
result of a cancelled invocation fails with `RustvelloError::InvocationCancelled`
(Rust) or `rustvello.InvocationCancelledError` (Python).

## Side effects and idempotency

Rustvello runs each invocation **at least once**:

- a retry re-runs the whole task body;
- a timed-out or cancelled synchronous body may keep running after the invocation
  is already `FAILED`, `RETRY` or `CANCELLED`. It can finish its side effects
  (write a row, send an email) after the status says it stopped. It can even
  overlap a retry of the same invocation;
- cancelling or timing out an async body stops it at an `.await`, so a
  side effect split across awaits can be left half done;
- cancellation never undoes work that already happened.

To make this safe, give every external side effect an idempotency key. The
invocation ID stays the same across retries: use
`get_invocation_context().invocation_id` (Rust) or
`app.current_invocation().invocation_id` (Python). Either make a repeated
write a no-op, or check whether the work is already done before doing it
again. Keep side effects late in the task and in as few steps as possible,
and prefer async I/O or the process pool for work that must stop at the
deadline.
