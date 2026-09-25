# Async tasks

A task body can be asynchronous in both languages: `async fn` under
`#[rustvello::task]` in Rust, `async def` under `@app.task` in Python. Use them
for I/O-bound work (HTTP calls, database queries, model APIs) where a
synchronous body would hold a thread while it waits.

Async tasks support everything synchronous tasks support: typed parameters and
results, retries (`max_retries`, `retry_for_errors` / `retry_for`), stored
errors, concurrency control, queues and priorities, workflow membership and
roots, child submission and invocation context.

## Rust

```rust
use rustvello::prelude::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn network_error(error: std::io::Error) -> RustvelloError {
    RustvelloError::TaskExecution {
        error_type: "TransientNetworkError".into(),
        message: error.to_string(),
        traceback: None,
    }
}

/// One round trip through a line-based echo server.
#[rustvello::task(max_retries = 2, retry_for_errors = ["TransientNetworkError"])]
async fn echo(addr: String, message: String) -> RustvelloResult<String> {
    let stream = TcpStream::connect(&addr).await.map_err(network_error)?;
    let (read, mut write) = stream.into_split();
    write.write_all(format!("{message}\n").as_bytes()).await.map_err(network_error)?;
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await.map_err(network_error)?;
    Ok(line.trim_end().to_owned())
}

/// A workflow root that awaits its children instead of blocking a thread.
#[rustvello::workflow]
async fn echo_all(addr: String, count: u32) -> RustvelloResult<Vec<String>> {
    let app = APP.get().expect("app initialised");
    let mut children = Vec::new();
    for index in 0..count {
        let params = EchoParams { addr: addr.clone(), message: format!("item {index}") };
        children.push(app.submit_call(&EchoTask::new(), params).await?);
    }
    let mut replies = Vec::new();
    for child in children {
        replies.push(child.wait(std::time::Duration::from_millis(10)).await?);
    }
    Ok(replies)
}
```

Submitting and running them is the same as for synchronous tasks. The complete
program, with a local echo server and an in-process worker, is
[`crates/rustvello/examples/async_tasks.rs`](https://github.com/pynenc/rustvello/blob/main/crates/rustvello/examples/async_tasks.rs):

```bash
cargo run -p rustvello --example async_tasks
# ["item 0", "item 1", "item 2"]
```

The macro generates the same `EchoParams` / `EchoTask` pair as for a
synchronous function. The body's future must be `Send`, because a worker may
resume it on any runtime thread.

Inside an async body, wait for other invocations with the async handle APIs
(`handle.wait(..).await`, `app.submit_call(..).await`) instead of blocking.
`#[rustvello::workflow]` also accepts an `async fn`. For deterministic replay,
use the async helpers of `WorkflowRoot` there (`random_async`, `utc_now_async`,
`uuid_async`):

```rust
#[rustvello::workflow]
async fn order_flow(order_id: String) -> RustvelloResult<String> {
    let mut root = WorkflowRoot::current()?;
    let run_id = root.uuid_async().await?;
    Ok(format!("{order_id}:{run_id}"))
}
```

### Execution model

- The runner awaits the body natively on its Tokio runtime as its own Tokio
  task. It holds no blocking-pool thread while it waits, so `blocking = true`
  is rejected on an `async fn` at compile time. Move a blocking section into
  `tokio::task::spawn_blocking` inside the body.
- Each body occupies one worker slot until it finishes. `num_workers` bounds
  how many invocations run at once, async or not, and task-level concurrency
  control applies unchanged. For concurrency inside one invocation, use
  `tokio::join!` or a `JoinSet` in the body.
- The invocation context (`get_invocation_context()`, workflow identity,
  retry count), the runner context and the W3C trace context are attached to
  the future. They hold on every poll, across every `.await`, whichever runtime
  thread resumes the body. Child submissions inherit parent and workflow
  identity as they do from synchronous tasks.
- The worker's `invocation` tracing span wraps the body, so events it logs after
  an `.await` stay inside that span.
- The Rayon runner also awaits async bodies on the Tokio runtime and keeps its
  thread pool for CPU-bound synchronous tasks.
- Synchronous entry points still work: `Task::run` and `execute_sync` drive the
  future to completion with `block_on_task_future` (with `block_in_place`
  inside a multi-threaded runtime, on a private runtime otherwise). Dev mode
  (`dev_mode_force_sync`) awaits the body inside `app.call(..)`.

### Implementing `Task` by hand

Override `is_async` and `run_async`, and point the synchronous `run` at the
future:

```rust
use rustvello_core::task::{block_on_task_future, Task, TaskFuture};

impl Task for FetchTask {
    type Params = String;
    type Result = usize;
    fn task_id(&self) -> &TaskId { &self.task_id }
    fn config(&self) -> &TaskConfig { &self.config }
    fn is_async(&self) -> bool { true }
    fn run(&self, url: String) -> RustvelloResult<usize> {
        block_on_task_future(self.run_async(url))
    }
    fn run_async(&self, url: String) -> TaskFuture<'_, usize> {
        Box::pin(async move { fetch_len(&url).await })
    }
}
```

## Python

```python
import asyncio

from rustvello import App

app = App(backend="sqlite", db_path="./tasks.db")


class TransientNetworkError(Exception):
    pass


@app.task(max_retries=2, retry_for=(TransientNetworkError,))
async def echo(host: str, port: int, message: str) -> str:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except OSError as error:
        raise TransientNetworkError(str(error)) from error
    writer.write(f"{message}\n".encode())
    await writer.drain()
    line = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return line.decode().rstrip("\n")


@app.task
async def fan_out(host: str, port: int) -> list[str]:
    children = [echo(host, port, f"item {i}") for i in range(3)]
    # Await children without blocking this worker's event loop.
    return [await child.result_async(timeout=30) for child in children]


if __name__ == "__main__":
    app.run(num_workers=4)
```

### Execution model

- Each worker owns one event loop, created on first use and reused for every
  async invocation that worker runs. That is one loop per worker thread with
  `app.run()`, one per worker process with `app.run(num_processes=N)`. Synchronous
  tasks are unchanged and run on the same workers.
- The loop runs on the worker's own thread, which is what makes the invocation
  context work unchanged: `app.current_invocation()`,
  `get_current_invocation_id()`, `workflow_root()` and child submissions from
  the body see the running invocation, and the OpenTelemetry context attached
  for the invocation is copied into the coroutine.
- **GIL:** while the loop waits on I/O it releases the GIL, so other workers
  (async or sync) keep running. CPU-bound code inside a coroutine holds the GIL
  and stalls that worker's loop; use `num_processes` for CPU-bound work.
- Each async body occupies one worker until it returns, so `num_workers` bounds
  concurrent invocations exactly as for synchronous tasks. Use
  `asyncio.gather` inside a body for concurrency within one invocation.
- Tasks that a coroutine starts and leaves running when it returns are
  cancelled before the worker takes its next invocation, as `asyncio.run` does.
  Work handed to `loop.run_in_executor` or `asyncio.to_thread` runs on another
  thread, which does not carry the invocation context.
- Inside an async body use `await invocation.result_async(...)`;
  `invocation.result(...)` blocks the loop while it polls.
- In dev mode (`dev_mode_force_sync=True`) calling an async task runs the
  coroutine to completion before the call returns. If the caller is itself
  inside a running event loop, the coroutine runs on a helper thread with its
  own loop.
- `async def` generators are rejected at registration.

## Failures and cancellation

A raised error or returned `Err` is recorded exactly as for a synchronous task:
it is retried while the retry policy allows and then stored with its error type
and message.

The following cancellation cases are covered by tests in both languages:

| Case                                                                           | Outcome                                                                                                                                                           |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The body panics (Rust)                                                         | Error `task panicked: ...`; retried or `FAILED`                                                                                                                   |
| The body is aborted on the runtime (Rust)                                      | Error type `TaskCancelled`; retried or `FAILED`                                                                                                                   |
| The coroutine is cancelled (Python)                                            | Error type `CancelledError`; retried or `FAILED`                                                                                                                  |
| The worker is dropped mid-await (Rust bounded shutdown deadline, process exit) | The body is aborted, never left running detached. The invocation stays `RUNNING` under the dead worker, and stale-runner recovery re-routes it to another worker. |

A cancelled body may have performed some of its side effects before the
cancellation, and a recovered invocation runs again from the start. Keep async
task bodies idempotent, as for any at-least-once task. Per-task time limits and
cooperative cancellation have their own API and documentation.
