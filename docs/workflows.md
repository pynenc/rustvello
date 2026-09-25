# Workflows

Rustvello distinguishes ordinary distributed tasks from explicit workflow
roots. A top-level `#[rustvello::task]` has no workflow identity. A
`#[rustvello::workflow]` invocation defines a root whose identity is persisted
with its child invocations and replay data.

## Define a workflow

```rust
use rustvello::prelude::*;

#[rustvello::workflow]
fn prepare_order(order_id: String) -> RustvelloResult<String> {
    let mut root = WorkflowRoot::current()?;
    let run_id = root.uuid()?;
    let started_at = root.utc_now()?;
    Ok(format!("{order_id}:{run_id}:{started_at}"))
}
```

The macro generates `PrepareOrderTask` and `PrepareOrderParams`, as the task
macro does. It also marks the invocation as workflow-defining and guarantees
blocking execution, which allows the synchronous function to use persistent
deterministic operations safely.

## Identity rules

| Submission                    | Result                                          |
| ----------------------------- | ----------------------------------------------- |
| Ordinary task from top level  | No workflow membership                          |
| Workflow task from top level  | New root workflow                               |
| Ordinary task from a workflow | Member of the caller's workflow                 |
| Workflow task from a workflow | New subworkflow linked to the caller's workflow |

Only the defining invocation can obtain `WorkflowRoot`. An ordinary top-level
task receives `WorkflowMembershipRequired`; an ordinary child in a workflow
receives `WorkflowRootRequired`. Calling it outside runner execution receives
`WorkflowContextUnavailable`.

## Deterministic values

Keep one root handle for the ordered sequence of operations:

```rust
let mut root = WorkflowRoot::current()?;
let first = root.random()?;
let second = root.random()?;
let id = root.uuid()?;
```

Rustvello records values by workflow ID, operation type, and sequence. Replaying
the defining invocation in the same order returns the recorded values. Changing
operation order changes the replay contract, so treat that order as persisted
workflow behavior.

The `random_async`, `utc_now_async`, and `uuid_async` variants are for async
workflow bodies (`#[rustvello::workflow] async fn`, see
[Async tasks](async_tasks.md)) and for embedded async runtimes that establish an
invocation context. Synchronous workflow functions use the blocking methods.

## Python

Standalone Python has the same model. `@app.workflow` registers a workflow
root (blocking, like the Rust macro), and `rustvello.workflow_root()` inside its
body returns the `WorkflowRoot` handle with the same deterministic operations:

```python
from rustvello import App, workflow_root

app = App(backend="sqlite", db_path="./tasks.db")


@app.workflow
def prepare_order(order_id: str) -> str:
    root = workflow_root()
    return f"{order_id}:{root.uuid()}:{root.utc_now()}"
```

`random()`, `utc_now()` and `uuid()` are recorded by workflow ID, operation type
and sequence exactly as in Rust, and the identity rules above apply unchanged:
`workflow_root()` raises `RustvelloError` outside the invocation that defines
the workflow, including in an ordinary task the workflow submitted. Tasks
submitted from the workflow body join its workflow, and
`get_current_workflow_info()` returns the workflow ID, workflow type and parent
ID of the running invocation.

Workflows need a runner (`app.run()` or `python -m rustvello.worker`). With
`dev_mode_force_sync=True` the body runs inline without an invocation context,
so `workflow_root()` raises `RustvelloError` ("workflow context is unavailable").

Pynenc integration translates Pynenc's explicit workflow marker at the adapter
boundary. Rustvello does not reintroduce implicit roots or a Python
module-discovery layer to emulate that API.

Monitoring labels a defining invocation as **Workflow root**. Ordinary members
retain the workflow link without that label.
