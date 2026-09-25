# Guarantee matrix

<!-- Generated from rustvello_core::guarantees; do not edit. Regenerate with
     RUSTVELLO_WRITE_GUARANTEES=1 cargo test -p rustvello-core --lib guarantees -->

What each backend promises when a process dies. The same data is served by
the monitoring API at `/api/capabilities` (`guarantees`). A backend may only
claim *guaranteed* when the listed tests prove it; those tests run in the
release gate (`make test-fault`, `make test-fault-postgres` and the backend
suites in `release-gate.yml`), which also runs on pull requests that touch
a backend, so a failing one blocks the release.

| Backend | Atomic publication | Trigger atomicity | Stale-owner recovery | Ordering | Durability | Delayed retry |
| --- | --- | --- | --- | --- | --- | --- |
| sqlite | guaranteed | guaranteed | guaranteed | guaranteed | guaranteed | guaranteed |
| postgres | guaranteed | guaranteed | guaranteed | guaranteed | guaranteed | guaranteed |
| redis | best effort | best effort | best effort | guaranteed | best effort | not supported |
| mongodb | not supported | best effort | best effort | guaranteed | best effort | not supported |
| mongodb3 | not supported | best effort | best effort | guaranteed | best effort | not supported |
| rabbitmq | not supported | n/a | n/a | guaranteed | best effort | not supported |
| memory | not supported | guaranteed | n/a | guaranteed | not supported | best effort |

*Mixed backends* (for example a RabbitMQ broker with another control
backend) never qualify for atomic publication: every port must share one
database transaction domain. *Delayed retry* follows the component that
queues the retry: the transactional publication when the backend has one,
otherwise the broker; see
[Retries, timeouts and cancellation](retries-timeouts-cancellation.md).

## sqlite

- **atomic publication**: guaranteed. one SQLite transaction per publication; kill-tested at every boundary.
  - `crates/rustvello/tests/publication_crash_acceptance.rs::submission_kill_at_every_boundary_and_lost_ack_replay`
  - `crates/rustvello/tests/publication_crash_acceptance.rs::retry_and_terminal_publication_survive_every_kill_boundary`
- **trigger atomicity**: guaranteed. claim, run record and condition clear in one transaction; publication idempotent by the run-derived invocation id.
  - `crates/rustvello/tests/trigger_crash_acceptance.rs::sqlite_trigger_firing_survives_kill_at_every_boundary`
  - `crates/rustvello/tests/trigger_crash_acceptance.rs::sqlite_concurrent_trigger_recoverers_publish_once`
- **stale owner recovery**: guaranteed. ownership-fenced recovery; a resumed stale worker cannot publish.
  - `crates/rustvello/tests/publication_crash_acceptance.rs::recovery_of_crashed_recovery_and_competing_recoverers_preserves_lineage`
  - `crates/rustvello/tests/publication_crash_acceptance.rs::resumed_stale_worker_cannot_publish_after_replacement`
  - `crates/rustvello/tests/stale_owner_concurrency.rs::duplicate_pending_claim_preserves_winner_slot`
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: guaranteed. file databases only (WAL); an in-memory database is not durable.
  - `crates/rustvello/tests/publication_crash_acceptance.rs::terminal_payload_and_cleanup_survive_every_kill_boundary`
- **delayed retry**: guaranteed. queue row with its not-before time committed with RETRY in one transaction (local clock); kill-tested during the backoff.
  - `crates/rustvello/tests/durable_retry_kill.rs::sqlite_retry_survives_worker_kill_during_backoff_and_fires_once`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## postgres

- **atomic publication**: guaranteed. one PostgreSQL transaction per publication; kill-tested at every boundary.
  - `crates/rustvello-postgres/src/acceptance.rs::process_kills_at_every_publication_boundary`
- **trigger atomicity**: guaranteed. claim, run record and condition clear in one transaction; publication idempotent by the run-derived invocation id.
  - `crates/rustvello/tests/trigger_crash_acceptance.rs::postgres_trigger_firing_survives_kill_at_every_boundary`
  - `crates/rustvello/tests/trigger_crash_acceptance.rs::postgres_concurrent_trigger_recoverers_publish_once`
- **stale owner recovery**: guaranteed. delivery leases and ownership-fenced completion.
  - `crates/rustvello-postgres/src/acceptance.rs::leases_competing_recovery_completion_fencing_and_identity`
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: guaranteed. committed transactions; relies on the server's fsync settings.
  - `crates/rustvello-postgres/src/acceptance.rs::process_kills_at_every_publication_boundary`
- **delayed retry**: guaranteed. queue row reserved until the not-before time on the database clock, committed with RETRY; kill-tested during the backoff.
  - `crates/rustvello/tests/durable_retry_kill.rs::postgres_retry_survives_worker_kill_during_backoff_and_fires_once`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## redis

- **atomic publication**: best effort. co-located publication is implemented; no process-kill suite yet.
- **trigger atomicity**: best effort. trigger outbox: claim, run record and condition clear are separate writes that the next evaluator repairs; publication is idempotent by the run-derived invocation id; no process-kill suite for this backend yet.
- **stale owner recovery**: best effort. heartbeat-based recovery; not kill-tested.
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: best effort. depends on Redis persistence (AOF with appendfsync).
- **delayed retry**: not supported. no durable delayed delivery: the backend refuses it and the runner retries immediately, logging a warning.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## mongodb

- **atomic publication**: not supported. no co-located transaction; publication uses the ordered fallback path.
- **trigger atomicity**: best effort. trigger outbox: claim, run record and condition clear are separate writes that the next evaluator repairs; publication is idempotent by the run-derived invocation id; no process-kill suite for this backend yet.
- **stale owner recovery**: best effort. heartbeat-based recovery; not kill-tested.
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: best effort. depends on the write concern.
- **delayed retry**: not supported. no durable delayed delivery: the backend refuses it and the runner retries immediately, logging a warning.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## mongodb3

- **atomic publication**: not supported. no co-located transaction; publication uses the ordered fallback path.
- **trigger atomicity**: best effort. trigger outbox: claim, run record and condition clear are separate writes that the next evaluator repairs; publication is idempotent by the run-derived invocation id; no process-kill suite for this backend yet.
- **stale owner recovery**: best effort. heartbeat-based recovery; not kill-tested.
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: best effort. depends on the write concern.
- **delayed retry**: not supported. no durable delayed delivery: the backend refuses it and the runner retries immediately, logging a warning.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## rabbitmq

- **atomic publication**: not supported. broker only; a mixed deployment is not qualified for atomic publication.
- **trigger atomicity**: n/a. broker only; triggers live in the paired store.
- **stale owner recovery**: n/a. broker only; recovery lives in the paired control backend.
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: best effort. durable queues; depends on the broker configuration.
- **delayed retry**: not supported. no durable delayed delivery: the backend refuses it and the runner retries immediately, logging a warning.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`

## memory

- **atomic publication**: not supported. in-process only; uses the ordered fallback path.
- **trigger atomicity**: guaranteed. within one process: claim, record and clear under one lock; fallback publication is exactly-once across failures at every boundary.
  - `crates/rustvello/tests/trigger_fallback_faults.rs::fallback_trigger_publication_is_exactly_once_after_failure_at_every_boundary`
- **stale owner recovery**: n/a. single process; nothing outlives the runner.
- **ordering**: guaranteed. priority, then FIFO, per queue.
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities`
- **durability**: not supported. all state is lost when the process exits.
- **delayed retry**: best effort. process-local: the delay is honoured and survives a runner restart in the same process, not a process exit.
  - `crates/rustvello-mem/src/broker.rs::delayed_delivery_is_invisible_until_due_and_delivered_once`
  - `crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability`
