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

The `random_async`, `utc_now_async`, and `uuid_async` variants are available to
embedded async runtimes that establish an invocation context. Macro-generated
workflow functions use the synchronous methods.

## Python boundary

Standalone Python currently exposes distributed tasks, not the Rust-native
root handle. Pynenc integration translates Pynenc's explicit workflow marker at
the adapter boundary. Rustvello does not reintroduce implicit roots or a Python
module-discovery layer to emulate that API.

Monitoring labels a defining invocation as **Workflow root**. Ordinary members
retain the workflow link without that label.
