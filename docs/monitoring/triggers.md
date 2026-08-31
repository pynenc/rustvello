# Trigger and Event Monitoring

Rustvello persists the evidence that caused a trigger to fire and links that
evidence to the invocation created by the trigger. Every full trigger store
implements this contract; the shared monitoring suite runs against each
backend. A broker-only component such as RabbitMQ has no trigger store.

## Data lifecycle

1. `TriggerManager::emit_event` writes an `EventRecord` before evaluating any
   condition. Unmatched events therefore remain observable.
2. Matching conditions add their condition and validation identifiers to the
   same record.
3. `evaluate_trigger_runs` atomically claims a trigger run and writes a
   `TriggerRunRecord` containing the conditions that participated.
4. Normal task submission creates the target invocation.
5. `complete_trigger_run` links the run and its event records to that
   invocation.

Monitoring persistence is part of the trigger execution contract. Backend
implementations use their native durable primitives so event matching and
trigger submission have queryable evidence after a process restart.

## Record shapes

`EventRecord` contains:

| Field                                            | Meaning                                                  |
| ------------------------------------------------ | -------------------------------------------------------- |
| `event_id`, `event_code`, `payload`, `timestamp` | Emitted event identity and data                          |
| `matched_condition_ids`                          | Event conditions satisfied by the payload                |
| `valid_condition_ids`                            | Exact persisted condition matches consumed by evaluation |
| `triggered_invocation_ids`                       | Invocations created from runs involving this event       |
| `emitted_by_*`                                   | Optional invocation, task, and runner attribution        |

`TriggerRunRecord` contains:

| Field                                              | Meaning                                                        |
| -------------------------------------------------- | -------------------------------------------------------------- |
| `trigger_run_id`, `trigger_id`, `task_id`, `logic` | Claimed run and target definition                              |
| `arguments`                                        | Arguments submitted to the target task                         |
| `participants`                                     | Condition, validation, event, and source-invocation links      |
| `claimed_at`, `executed_at`                        | Claim and submission timestamps                                |
| `triggered_invocation_id`                          | Invocation produced by the run                                 |
| `atomic_service_*`                                 | Optional service-run attribution reserved by the wire contract |

Implementations store the serialized records plus native indexes for event
code, emitter, time, produced invocation, event-to-run, and source-to-run
relationships. Queries support time bounds and limits; event queries also
filter by code or emitter, while run queries filter by event, source
invocation, or produced invocation.

## Dashboard

The **Events** view lists recent events and supports event-code filtering. An
event detail page shows its payload, condition matches, emitter, produced
invocations, related trigger runs, and a bounded invocation-timeline link.
Trigger-run detail pages show the target task, arguments, timestamps, produced
invocation, and every participating condition.

Configure `AppInstance::trigger_store` with the same store used by the running
application. Set it to `None` when the application has no trigger manager; the
dashboard then reports that event monitoring is unavailable.

## Deliberate scope

Rustvello does not copy Pynmon's plugin views, trigger mutation UI, arbitrary
event-provider discovery, or Python module hydration. It exposes only records
owned by Rustvello's trigger contracts. Status/result/exception participants
link to their source invocation; custom events additionally receive first-class
list and detail views.
