//! Declared durability guarantees per backend.
//!
//! This is the single source for the guarantee matrix: `/api/capabilities`
//! serves it, `docs/guarantees.md` is generated from it, and a test checks
//! that every guaranteed cell names evidence tests that exist in the
//! repository. The release gate runs those tests, so a backend cannot claim a
//! guarantee whose fault test is not passing.

use serde::Serialize;

/// How strongly a backend provides one property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GuaranteeLevel {
    /// Provided and proven by the named fault or compliance tests, which gate releases.
    Guaranteed,
    /// Implemented, but not proven under process death, or dependent on server configuration.
    BestEffort,
    /// Not provided.
    NotSupported,
    /// The backend has no such component.
    NotApplicable,
}

impl GuaranteeLevel {
    fn label(self) -> &'static str {
        match self {
            Self::Guaranteed => "guaranteed",
            Self::BestEffort => "best effort",
            Self::NotSupported => "not supported",
            Self::NotApplicable => "n/a",
        }
    }
}

/// One property of one backend.
#[derive(Debug, Clone, Serialize)]
pub struct Guarantee {
    pub level: GuaranteeLevel,
    pub note: &'static str,
    /// Tests that prove the claim, as `path/to/file.rs::test_fn`.
    pub evidence: &'static [&'static str],
}

const fn g(
    level: GuaranteeLevel,
    note: &'static str,
    evidence: &'static [&'static str],
) -> Guarantee {
    Guarantee {
        level,
        note,
        evidence,
    }
}

/// Declared guarantees of one backend.
#[derive(Debug, Clone, Serialize)]
pub struct BackendGuarantees {
    /// Backend profile name, matching `OrchestratorStatus::guarantee_profile`.
    pub backend: &'static str,
    /// Submission, status, retry and completion effects commit in one transaction.
    pub atomic_publication: Guarantee,
    /// Each trigger firing yields exactly one logical invocation after a crash.
    pub trigger_atomicity: Guarantee,
    /// Work owned by a dead runner is recovered without a stale owner publishing over it.
    pub stale_owner_recovery: Guarantee,
    /// Priority, then FIFO, within one queue.
    pub ordering: Guarantee,
    /// Committed state survives process death.
    pub durability: Guarantee,
    /// A retry backoff is stored in the backend, so a worker killed during
    /// the delay neither loses nor duplicates the retry.
    pub delayed_retry: Guarantee,
}

impl BackendGuarantees {
    /// The properties in matrix column order.
    pub fn properties(&self) -> [(&'static str, &Guarantee); 6] {
        [
            ("atomic_publication", &self.atomic_publication),
            ("trigger_atomicity", &self.trigger_atomicity),
            ("stale_owner_recovery", &self.stale_owner_recovery),
            ("ordering", &self.ordering),
            ("durability", &self.durability),
            ("delayed_retry", &self.delayed_retry),
        ]
    }
}

use GuaranteeLevel::{BestEffort, Guaranteed, NotApplicable, NotSupported};

const ORDERING_SUITE: &[&str] = &[
    "crates/rustvello-test-suite/src/broker.rs::suite_broker_fifo_ordering",
    "crates/rustvello-test-suite/src/broker.rs::suite_broker_named_queues_and_priorities",
];
const FALLBACK_TRIGGER_FAULTS: &[&str] = &[
    "crates/rustvello/tests/trigger_fallback_faults.rs::fallback_trigger_publication_is_exactly_once_after_failure_at_every_boundary",
];
const DELAYED_DELIVERY_REFUSAL: &[&str] =
    &["crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability"];
const NO_DELAYED_RETRY_NOTE: &str =
    "no durable delayed delivery: the backend refuses it and the runner retries immediately, \
     logging a warning";
const OUTBOX_NOTE: &str =
    "trigger outbox: claim, run record and condition clear are separate writes \
     that the next evaluator repairs; publication is idempotent by the run-derived invocation id; \
     no process-kill suite for this backend yet";

/// The declared matrix, one row per backend profile.
pub fn guarantee_matrix() -> Vec<BackendGuarantees> {
    vec![
        BackendGuarantees {
            backend: "sqlite",
            atomic_publication: g(
                Guaranteed,
                "one SQLite transaction per publication; kill-tested at every boundary",
                &[
                    "crates/rustvello/tests/publication_crash_acceptance.rs::submission_kill_at_every_boundary_and_lost_ack_replay",
                    "crates/rustvello/tests/publication_crash_acceptance.rs::retry_and_terminal_publication_survive_every_kill_boundary",
                ],
            ),
            trigger_atomicity: g(
                Guaranteed,
                "claim, run record and condition clear in one transaction; publication idempotent by the run-derived invocation id",
                &[
                    "crates/rustvello/tests/trigger_crash_acceptance.rs::sqlite_trigger_firing_survives_kill_at_every_boundary",
                    "crates/rustvello/tests/trigger_crash_acceptance.rs::sqlite_concurrent_trigger_recoverers_publish_once",
                ],
            ),
            stale_owner_recovery: g(
                Guaranteed,
                "ownership-fenced recovery; a resumed stale worker cannot publish",
                &[
                    "crates/rustvello/tests/publication_crash_acceptance.rs::recovery_of_crashed_recovery_and_competing_recoverers_preserves_lineage",
                    "crates/rustvello/tests/publication_crash_acceptance.rs::resumed_stale_worker_cannot_publish_after_replacement",
                    "crates/rustvello/tests/stale_owner_concurrency.rs::duplicate_pending_claim_preserves_winner_slot",
                ],
            ),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(
                Guaranteed,
                "file databases only (WAL); an in-memory database is not durable",
                &["crates/rustvello/tests/publication_crash_acceptance.rs::terminal_payload_and_cleanup_survive_every_kill_boundary"],
            ),
            delayed_retry: g(
                Guaranteed,
                "queue row with its not-before time committed with RETRY in one transaction (local clock); kill-tested during the backoff",
                &[
                    "crates/rustvello/tests/durable_retry_kill.rs::sqlite_retry_survives_worker_kill_during_backoff_and_fires_once",
                    "crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability",
                ],
            ),
        },
        BackendGuarantees {
            backend: "postgres",
            atomic_publication: g(
                Guaranteed,
                "one PostgreSQL transaction per publication; kill-tested at every boundary",
                &["crates/rustvello-postgres/src/acceptance.rs::process_kills_at_every_publication_boundary"],
            ),
            trigger_atomicity: g(
                Guaranteed,
                "claim, run record and condition clear in one transaction; publication idempotent by the run-derived invocation id",
                &[
                    "crates/rustvello/tests/trigger_crash_acceptance.rs::postgres_trigger_firing_survives_kill_at_every_boundary",
                    "crates/rustvello/tests/trigger_crash_acceptance.rs::postgres_concurrent_trigger_recoverers_publish_once",
                ],
            ),
            stale_owner_recovery: g(
                Guaranteed,
                "delivery leases and ownership-fenced completion",
                &["crates/rustvello-postgres/src/acceptance.rs::leases_competing_recovery_completion_fencing_and_identity"],
            ),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(
                Guaranteed,
                "committed transactions; relies on the server's fsync settings",
                &["crates/rustvello-postgres/src/acceptance.rs::process_kills_at_every_publication_boundary"],
            ),
            delayed_retry: g(
                Guaranteed,
                "queue row reserved until the not-before time on the database clock, committed with RETRY; kill-tested during the backoff",
                &[
                    "crates/rustvello/tests/durable_retry_kill.rs::postgres_retry_survives_worker_kill_during_backoff_and_fires_once",
                    "crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability",
                ],
            ),
        },
        BackendGuarantees {
            backend: "redis",
            atomic_publication: g(
                BestEffort,
                "co-located publication is implemented; no process-kill suite yet",
                &[],
            ),
            trigger_atomicity: g(BestEffort, OUTBOX_NOTE, &[]),
            stale_owner_recovery: g(BestEffort, "heartbeat-based recovery; not kill-tested", &[]),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(
                BestEffort,
                "depends on Redis persistence (AOF with appendfsync)",
                &[],
            ),
            delayed_retry: g(NotSupported, NO_DELAYED_RETRY_NOTE, DELAYED_DELIVERY_REFUSAL),
        },
        BackendGuarantees {
            backend: "mongodb",
            atomic_publication: g(
                NotSupported,
                "no co-located transaction; publication uses the ordered fallback path",
                &[],
            ),
            trigger_atomicity: g(BestEffort, OUTBOX_NOTE, &[]),
            stale_owner_recovery: g(BestEffort, "heartbeat-based recovery; not kill-tested", &[]),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(BestEffort, "depends on the write concern", &[]),
            delayed_retry: g(NotSupported, NO_DELAYED_RETRY_NOTE, DELAYED_DELIVERY_REFUSAL),
        },
        BackendGuarantees {
            backend: "mongodb3",
            atomic_publication: g(
                NotSupported,
                "no co-located transaction; publication uses the ordered fallback path",
                &[],
            ),
            trigger_atomicity: g(BestEffort, OUTBOX_NOTE, &[]),
            stale_owner_recovery: g(BestEffort, "heartbeat-based recovery; not kill-tested", &[]),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(BestEffort, "depends on the write concern", &[]),
            delayed_retry: g(NotSupported, NO_DELAYED_RETRY_NOTE, DELAYED_DELIVERY_REFUSAL),
        },
        BackendGuarantees {
            backend: "rabbitmq",
            atomic_publication: g(
                NotSupported,
                "broker only; a mixed deployment is not qualified for atomic publication",
                &[],
            ),
            trigger_atomicity: g(NotApplicable, "broker only; triggers live in the paired store", &[]),
            stale_owner_recovery: g(NotApplicable, "broker only; recovery lives in the paired control backend", &[]),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(BestEffort, "durable queues; depends on the broker configuration", &[]),
            delayed_retry: g(NotSupported, NO_DELAYED_RETRY_NOTE, DELAYED_DELIVERY_REFUSAL),
        },
        BackendGuarantees {
            backend: "memory",
            atomic_publication: g(NotSupported, "in-process only; uses the ordered fallback path", &[]),
            trigger_atomicity: g(
                Guaranteed,
                "within one process: claim, record and clear under one lock; fallback publication is exactly-once across failures at every boundary",
                FALLBACK_TRIGGER_FAULTS,
            ),
            stale_owner_recovery: g(NotApplicable, "single process; nothing outlives the runner", &[]),
            ordering: g(Guaranteed, "priority, then FIFO, per queue", ORDERING_SUITE),
            durability: g(NotSupported, "all state is lost when the process exits", &[]),
            delayed_retry: g(
                BestEffort,
                "process-local: the delay is honoured and survives a runner restart in the same process, not a process exit",
                &[
                    "crates/rustvello-mem/src/broker.rs::delayed_delivery_is_invisible_until_due_and_delivered_once",
                    "crates/rustvello-test-suite/src/broker.rs::suite_broker_delayed_delivery_capability",
                ],
            ),
        },
    ]
}

/// Declared guarantees for one backend profile.
pub fn backend_guarantees(profile: &str) -> Option<BackendGuarantees> {
    guarantee_matrix()
        .into_iter()
        .find(|row| row.backend == profile)
}

/// Render the matrix as the `docs/guarantees.md` page.
pub fn render_markdown() -> String {
    let matrix = guarantee_matrix();
    let mut out = String::new();
    out.push_str("# Guarantee matrix\n\n");
    out.push_str(
        "<!-- Generated from rustvello_core::guarantees; do not edit. Regenerate with\n     \
         RUSTVELLO_WRITE_GUARANTEES=1 cargo test -p rustvello-core --lib guarantees -->\n\n",
    );
    out.push_str(
        "What each backend promises when a process dies. The same data is served by\n\
         the monitoring API at `/api/capabilities` (`guarantees`). A backend may only\n\
         claim *guaranteed* when the listed tests prove it; those tests run in the\n\
         release gate (`make test-fault`, `make test-fault-postgres` and the backend\n\
         suites in `release-gate.yml`), which also runs on pull requests that touch\n\
         a backend, so a failing one blocks the release.\n\n",
    );
    out.push_str("| Backend | Atomic publication | Trigger atomicity | Stale-owner recovery | Ordering | Durability | Delayed retry |\n");
    out.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
    for row in &matrix {
        out.push_str(&format!("| {} ", row.backend));
        for (_, guarantee) in row.properties() {
            out.push_str(&format!("| {} ", guarantee.level.label()));
        }
        out.push_str("|\n");
    }
    out.push_str(
        "\n*Mixed backends* (for example a RabbitMQ broker with another control\n\
         backend) never qualify for atomic publication: every port must share one\n\
         database transaction domain. *Delayed retry* follows the component that\n\
         queues the retry: the transactional publication when the backend has one,\n\
         otherwise the broker; see\n\
         [Retries, timeouts and cancellation](retries-timeouts-cancellation.md).\n",
    );
    for row in &matrix {
        out.push_str(&format!("\n## {}\n\n", row.backend));
        for (name, guarantee) in row.properties() {
            out.push_str(&format!(
                "- **{}**: {}. {}.\n",
                name.replace('_', " "),
                guarantee.level.label(),
                guarantee.note
            ));
            for evidence in guarantee.evidence {
                out.push_str(&format!("  - `{evidence}`\n"));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository_root() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[test]
    fn every_guaranteed_claim_names_existing_tests() {
        let root = repository_root();
        for row in guarantee_matrix() {
            for (name, guarantee) in row.properties() {
                if guarantee.level == GuaranteeLevel::Guaranteed {
                    assert!(
                        !guarantee.evidence.is_empty(),
                        "{} claims {name} without a proving test",
                        row.backend
                    );
                }
                for evidence in guarantee.evidence {
                    let (file, test) = evidence.split_once("::").expect("file::test");
                    let source = std::fs::read_to_string(root.join(file)).unwrap_or_else(|_| {
                        panic!("{}: missing evidence file {file}", row.backend)
                    });
                    assert!(
                        source.contains(&format!("fn {test}(")),
                        "{}: {name} evidence {evidence} does not exist",
                        row.backend
                    );
                }
            }
        }
    }

    #[test]
    fn backend_profiles_are_unique() {
        let matrix = guarantee_matrix();
        for row in &matrix {
            assert_eq!(
                matrix
                    .iter()
                    .filter(|other| other.backend == row.backend)
                    .count(),
                1
            );
            assert!(backend_guarantees(row.backend).is_some());
        }
    }

    #[test]
    fn docs_page_matches_declarations() {
        let path = repository_root().join("docs/guarantees.md");
        let rendered = render_markdown();
        if std::env::var_os("RUSTVELLO_WRITE_GUARANTEES").is_some() {
            std::fs::write(&path, &rendered).unwrap();
        }
        let current = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            current == rendered,
            "docs/guarantees.md is stale; regenerate with \
             RUSTVELLO_WRITE_GUARANTEES=1 cargo test -p rustvello-core --lib guarantees"
        );
    }
}
