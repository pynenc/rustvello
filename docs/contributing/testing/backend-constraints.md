# Backend Contracts And Constraints

Rustvello backend traits are contracts, not capability menus. Every concrete
implementation must provide every required operation in the traits it exposes,
and every backend suite must exercise that contract. A backend must not turn a
required operation into a no-op, an empty result, or `NotSupported`.

The shared suites apply to Memory, SQLite, Redis, PostgreSQL, MongoDB, and
MongoDB 3.6. That includes broker routing, state, triggers and trigger
evidence, client data, orchestration recovery, and atomic-service timelines.
RabbitMQ is a deliberately specialized broker backend; it implements the full
broker contract, but it is not an orchestrator, state backend, or trigger
store.

## Backend facts

These are deployment and storage facts, not Rustvello feature switches:

| Backend     | Constraint                                                                                                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Memory      | Process-local and non-durable. Useful for tests and local development.                                                                                                                            |
| SQLite      | Local single-node storage. It still implements the complete Rustvello contracts, including task-aware and language-aware broker routing.                                                          |
| Redis       | Requires a reachable Redis service. Atomic Redis commands and scripts provide the coordination primitives used by Rustvello.                                                                      |
| PostgreSQL  | Requires a reachable PostgreSQL service. Transactions and indexes provide durable multi-record coordination.                                                                                      |
| MongoDB     | Requires a MongoDB deployment. Rustvello uses a short atomic-document mutex for concurrency admission, so full concurrency control works on standalone servers as well as replica sets.           |
| MongoDB 3.6 | Legacy server/driver path. It has atomic single-document operations but no multi-document transactions; Rustvello uses conditional updates and the same mutex coordination for its full contract. |
| RabbitMQ    | Broker-only message transport. It is suitable when queue delivery is the required component, not as a replacement for the other backend contracts.                                                |

A storage limitation must be handled in the implementation and documented as a
backend fact. It must not silently remove a required Rustvello operation.

## Verification rule

When a new required trait method is added, the change is incomplete until:

1. every concrete implementation compiles with a real implementation;
2. the shared test suite invokes the method for every applicable backend; and
3. the backend-specific tests cover its durability and concurrency semantics.

This rule is especially important for monitoring records. `EventRecord` and
`TriggerRunRecord` are durable trigger evidence and must be stored and
queryable by every trigger store. The monitoring suite is part of the normal
trigger suite; there is no "monitoring-capable backend" subset.
