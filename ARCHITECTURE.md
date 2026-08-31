# Rustvello Architecture

Rustvello is a Rust-first distributed task engine with a stable wire model,
replaceable storage and transport implementations, a native runner, monitoring,
and Python bindings. This document is the repository-level map: it explains
which crate owns each responsibility and the invariants that changes must
preserve. The rendered documentation contains the detailed API and operational
guides in [`docs/architecture.md`](docs/architecture.md).

## Architectural Boundaries

Rustvello has three product surfaces:

| Surface               | Owned here               | Boundary                                                                                                                                          |
| --------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rust engine           | Yes                      | Protocol types, traits, orchestration, runners, backends, monitoring, macros, and CLI                                                             |
| Standalone Python API | Yes                      | `rustvello-python` provides PyO3 wrappers; `py-rustvello` builds and packages the extension plus its Python facade                                |
| Pynenc integration    | No, external integration | Rustvello exposes language-qualified tasks, wire-compatible DTOs, and composite operations; Pynenc-specific adapters live outside this repository |

The core crates do not import Python or Pynenc. The Python layer converts types
and delegates to the same Rust application and backend implementations used by
native Rust callers.

## Workspace Map

![Rustvello workspace crate dependency map](docs/_static/architecture-crates.svg)

The Cargo workspace contains 16 packages under `crates/` plus the
`py-rustvello` extension package:

- `rustvello-proto` owns serializable DTOs, identifiers, configuration, trigger
  conditions, and the invocation status model. It has no internal crate
  dependencies.
- `rustvello-core` owns object-safe traits and shared managers. It depends only
  on `rustvello-proto` within the workspace.
- `rustvello` owns `RustvelloApp`, its builder, task submission, orchestration
  composites, task discovery, and native runners.
- `rustvello-mem`, `rustvello-sqlite`, `rustvello-redis`, `rustvello-postgres`,
  `rustvello-mongo`, `rustvello-mongo3`, and `rustvello-rabbitmq` implement one
  or more core traits.
- `rustvello-macros` generates typed tasks and link-time `inventory`
  registrations. Generated code targets public APIs exposed by `rustvello`.
- `rustvello-monitoring`, `rustvello-prometheus`, and `rustvello-cli` are edge
  consumers of the engine.
- `rustvello-test-suite` defines reusable backend contracts.
- `rustvello-python` wraps Rust APIs with PyO3 classes; `py-rustvello` is the
  `cdylib` and Python distribution package.

Dependency direction is inward: edge crates depend on the application and
traits; backend implementations depend on traits and protocol types; core code
must never depend on a concrete backend.

## Invocation Lifecycle

![Invocation lifecycle and component ownership](docs/_static/architecture-invocation-lifecycle.svg)

Submission and execution are coordinated as follows:

1. A caller submits a registered task and serialized arguments to
   `RustvelloApp`.
2. The orchestrator registers the invocation and owns status transitions,
   runner ownership, concurrency indexes, recovery, and waiters.
3. The state backend stores the invocation DTO, call, workflow identity,
   history, result or error, and runner context.
4. The broker owns queue placement and delivery, including language-qualified
   routing where the backend supports it.
5. A runner retrieves a candidate, atomically reserves a backend concurrency
   slot, claims status ownership, executes the registered task, then records
   success, retry, or failure through the orchestration coordinator. A failed
   claim or retry releases the reservation before the invocation can be
   selected again.
6. The management loop records heartbeats and coordinates recovery and trigger
   evaluation. Terminal transitions release ownership and concurrency indexes.

The status finite-state machine is declared in
`rustvello-proto/src/status/mod.rs`; transition and ownership validation is in
`rustvello-proto/src/status/machine.rs`. Backends persist the resulting records
but do not define alternate lifecycle rules.

## Backend Contract

![Backend traits and implementations](docs/_static/architecture-backend-traits.svg)

`rustvello-core` splits each broad subsystem into focused traits. The composite
trait aliases used by the application are:

- `Broker = BrokerCore + BrokerQuery + BrokerRouting`
- `Orchestrator = OrchestratorStatus + OrchestratorLifecycle +
OrchestratorConcurrency + OrchestratorBlocking + OrchestratorQuery +
OrchestratorRecovery`
- `StateBackend = StateBackendCore + StateBackendQuery + StateBackendRunner`
- `TriggerStore` and `ClientDataStore` provide their own persistence contracts.

Concrete backends may support different combinations. Unsupported behavior
must return a typed `RustvelloError::NotSupported` or be excluded by an explicit
capability declaration; it must not silently succeed. `rustvello-test-suite`
is the executable contract shared by implementations.

The current implementation families are:

| Backend             | Primary role                                                       |
| ------------------- | ------------------------------------------------------------------ |
| Memory              | Complete local development and test stack                          |
| SQLite              | Persistent single-host stack                                       |
| Redis               | Distributed broker, orchestration, state, trigger, and client data |
| PostgreSQL          | Persistent database-backed components                              |
| MongoDB / MongoDB 3 | Current and legacy-driver database-backed components               |
| RabbitMQ            | Broker transport; other components must be supplied separately     |

## Python And Pynenc Boundaries

![Python and PyO3 integration flow](docs/_static/architecture-python.svg)

The standalone Python import path is:

```text
Python facade (`py-rustvello/rustvello`)
  -> extension module (`py-rustvello/src/lib.rs`)
  -> PyO3 wrappers (`rustvello-python`)
  -> Rust application, traits, and concrete backends
```

PyO3 wrappers own conversion, exception mapping, runtime handoff, and GIL
boundaries. They must not duplicate orchestration rules. `PyRustvello` delegates
composite operations to `RustvelloApp`, keeping hot-path coordination in Rust.

Pynenc integration is a consumer of this public Python/Rust boundary. It is not
part of the workspace and must not introduce a reverse dependency from core
Rust crates to Pynenc. Compatibility work belongs in wire types, behavior
contracts, or the external adapter, depending on which side owns the concern.

## Trigger And Atomic-Service Coordination

![Trigger and atomic-service coordination](docs/_static/architecture-atomic-trigger.svg)

`TriggerManager` evaluates persisted condition definitions through a
`TriggerStore`. The runner management loop performs global work through
`OrchestratorCoordinator::check_atomic_services`:

1. Register an eligible runner heartbeat.
2. Read active atomic-service runners.
3. Deterministically elect whether this runner may execute the service window.
4. Recover stale pending/running invocations.
5. Evaluate trigger conditions, claim each run, and persist its participants
   when the trigger store supports monitoring records.
6. Submit matching task invocations and link each invocation back to its run
   and participating events.
7. Persist the atomic-service execution interval.

Backends own atomicity for shared state. The coordinator owns sequencing and
must remain backend-independent. Trigger condition evaluation is JSON/data
driven; it does not call Python callbacks.

## Monitoring Data Flow

![Monitoring data flow](docs/_static/architecture-monitoring.svg)

`rustvello-monitoring` is a read-oriented Axum application. `AppInstance`
contains trait objects for the broker, orchestrator, state backend, optional
trigger store, and client data store. Route handlers query those interfaces and
render Askama/HTMX views, tables, event and trigger-run evidence, family trees,
and SVG timelines. Mutating routes such as rerun or purge must use the same
backend contracts as the engine; monitoring never bypasses the status machine.

Monitoring reflects persisted facts. A view must handle a backend capability
being unsupported or data being absent without inventing lifecycle state.

## Workflow Context

![Workflow identity and deterministic-operation flow](docs/_static/architecture-workflow.svg)

Every submitted invocation currently receives a `WorkflowIdentity`:

- an ordinary top-level task has no workflow identity;
- `#[rustvello::workflow]` creates a root identity at top level;
- an ordinary task submitted inside a workflow inherits its identity and
  records the parent invocation;
- a workflow task submitted inside a workflow creates a nested workflow root.

Runtime workflow context is propagated with task-local and blocking-thread
fallback context. `WorkflowRoot::current()` validates that the current
invocation defines the persisted workflow identity before exposing random,
UTC-time, or UUID operations. Its internal replay executor stores each value by
operation and sequence under the workflow ID in `StateBackendQuery`. Monitoring
derives the same root distinction from `workflow_id == invocation_id`.

## Change Rules

Changes should preserve these ownership rules:

1. Put stable serialized contracts in `rustvello-proto` and behavior traits in
   `rustvello-core`.
2. Keep orchestration sequencing in `rustvello`; do not scatter it through
   concrete backends or language bindings.
3. Add backend behavior to the shared compliance suite before relying on it in
   the application layer.
4. Make unsupported backend capabilities explicit.
5. Keep Python wrappers thin and map typed Rust errors to typed Python errors.
6. Treat status, concurrency, recovery, trigger, and workflow changes as
   cross-backend contracts and stress them under contention.
7. Update this map and the rendered architecture guide when crate ownership or
   a cross-layer flow changes.
