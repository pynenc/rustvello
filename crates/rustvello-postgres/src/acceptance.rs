//! Real network/process tests. The explicit gate supplies an isolated PostgreSQL DSN.
use crate::{
    db::{Database, PostgresOptions},
    prelude::*,
};
use rustvello_core::{
    broker::Broker,
    error::TaskError,
    orchestrator::{OrchestratorConcurrency, OrchestratorRecovery, OrchestratorStatus},
    publication::{PublicationChange, PublicationRoute, RuntimePublication, SubmissionPublication},
    state_backend::StateBackendCore,
};
use rustvello_proto::{
    call::{CallDTO, SerializedArguments},
    identifiers::{InvocationId, RunnerId, TaskId},
    invocation::{InvocationDTO, TraceContextCarrier, WorkflowIdentity},
    status::InvocationStatus as Status,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn options() -> PostgresOptions {
    PostgresOptions {
        operation_timeout_ms: 2_000,
        delivery_lease_ms: 200,
        ..Default::default()
    }
}
fn dsn() -> String {
    std::env::var("RUSTVELLO_POSTGRES_DSN").expect("use make test-network-runtime-acceptance")
}
async fn connect(dsn: &str, app: &str, options: PostgresOptions) -> Arc<Database> {
    #[cfg(feature = "tls")]
    if let Ok(hostname) = std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME") {
        let ca = std::fs::read(
            std::env::var("RUSTVELLO_POSTGRES_TLS_CA")
                .expect("TLS acceptance requires private CA path"),
        )
        .unwrap();
        let tls = crate::db::PostgresTlsOptions::private_ca_pem(hostname, ca).unwrap();
        return Arc::new(
            Database::connect_tls_with_options(dsn, app, options, tls)
                .await
                .unwrap(),
        );
    }
    Arc::new(
        Database::connect_with_options(dsn, app, options)
            .await
            .unwrap(),
    )
}
fn route() -> PublicationRoute {
    PublicationRoute {
        queue: "critical".into(),
        priority: 7.0,
    }
}
fn runner() -> RunnerId {
    RunnerId::from_string("owner-a")
}
fn submission(id: &InvocationId) -> SubmissionPublication {
    let task = TaskId::new("network", "workflow");
    let call = CallDTO::new(task.clone(), SerializedArguments::default());
    let mut inv = InvocationDTO::new(id.clone(), task.clone(), call.call_id.clone());
    inv.workflow = Some(WorkflowIdentity {
        workflow_id: id.clone(),
        workflow_type: task,
        depth: 1,
        parent_id: Some(InvocationId::from_string("outer-workflow")),
    });
    inv.parent_invocation_id = Some(InvocationId::from_string("parent-invocation"));
    inv.trace_context = TraceContextCarrier {
        traceparent: Some("00-11111111111111111111111111111111-2222222222222222-01".into()),
        tracestate: Some("ih=qualified".into()),
    };
    SubmissionPublication {
        invocation: inv,
        call,
        runner_id: runner(),
        runner_context: None,
        workflow_root: true,
        cc_arguments: None,
        route: route(),
    }
}

struct Fixture {
    db: Arc<Database>,
    app: String,
    id: InvocationId,
}
impl Fixture {
    async fn open(app: String, id: InvocationId) -> Self {
        Self {
            db: connect(&dsn(), &app, options()).await,
            app,
            id,
        }
    }
    async fn new() -> Self {
        Self::open(format!("n_{}", RunnerId::new()), InvocationId::new()).await
    }
    fn control(&self) -> PostgresOrchestrator {
        PostgresOrchestrator::new(Arc::clone(&self.db))
    }
    fn publication(&self) -> Arc<dyn RuntimePublication> {
        self.control().runtime_publication().unwrap()
    }
    fn state(&self) -> PostgresStateBackend {
        PostgresStateBackend::new(Arc::clone(&self.db))
    }
    fn broker(&self) -> PostgresBroker {
        PostgresBroker::new(Arc::clone(&self.db))
    }
    async fn status(&self) -> Status {
        self.control()
            .get_invocation_status(&self.id)
            .await
            .unwrap()
            .status
    }
    async fn change(&self, change: PublicationChange) {
        self.publication()
            .change(&self.id, &runner(), change, true)
            .await
            .unwrap();
    }
    #[cfg(feature = "fault-injection")]
    async fn seed_running(&self) {
        self.publication()
            .submit(submission(&self.id))
            .await
            .unwrap();
        self.change(PublicationChange::Status(Status::Pending))
            .await;
        self.change(PublicationChange::Status(Status::Running))
            .await;
    }
    async fn count(&self, table: &str) -> i64 {
        self.db
            .conn()
            .await
            .unwrap()
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0)
    }
}

#[tokio::test]
async fn rejects_invalid_options_and_remote_plaintext_without_connecting() {
    for name in ["", "MixedCase", "has space", "x\"; DROP SCHEMA public"] {
        assert!(Database::connect("host=127.0.0.1", name).await.is_err());
    }
    for opt in [
        PostgresOptions {
            operation_timeout_ms: 0,
            ..Default::default()
        },
        PostgresOptions {
            max_pool_size: 0,
            ..Default::default()
        },
        PostgresOptions {
            delivery_lease_ms: u64::MAX,
            ..Default::default()
        },
    ] {
        assert!(
            Database::connect_with_options("host=127.0.0.1", "bounds", opt)
                .await
                .is_err()
        );
    }
    assert!(
        Database::connect("host=192.0.2.1 password=NEVER-PRINT-ME", "remote")
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("TLS")
    );
}

#[tokio::test]
#[ignore = "isolated network gate"]
async fn publication_replay_lineage_admission_and_tombstone() {
    let f = Fixture::new().await;
    let p = f.publication();
    assert!(p.submit(submission(&f.id)).await.unwrap());
    assert!(!p.submit(submission(&f.id)).await.unwrap());
    assert_eq!(f.count("broker_queue").await, 1);
    assert_eq!(f.count("history").await, 1);
    let stored = f.state().get_invocation(&f.id).await.unwrap();
    assert_eq!(stored.workflow, submission(&f.id).invocation.workflow);
    assert_eq!(
        stored.trace_context,
        submission(&f.id).invocation.trace_context
    );
    let mut changed = submission(&f.id);
    changed.route.priority = 8.0;
    assert!(p.submit(changed).await.is_err());
    f.control().remove_invocation(&f.id).await.unwrap();
    assert!(p.submit(submission(&f.id)).await.is_err());
    assert_eq!(f.count("broker_queue").await, 0);

    let policy = PostgresOptions {
        max_queue_rows: 1,
        max_payload_bytes: 1024,
        ..options()
    };
    let db = connect(&dsn(), &format!("bounded_{}", RunnerId::new()), policy).await;
    let p = PostgresOrchestrator::new(Arc::clone(&db))
        .runtime_publication()
        .unwrap();
    assert!(p.submit(submission(&InvocationId::new())).await.is_ok());
    assert!(p.submit(submission(&InvocationId::new())).await.is_err());
    let c = db.conn().await.unwrap();
    assert_eq!(
        c.query_one("SELECT count(*) FROM invocations", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let mut large = submission(&InvocationId::new());
    large.route.queue = "secret".repeat(1024);
    assert!(p.submit(large).await.is_err());
}

#[tokio::test]
#[ignore = "isolated network gate"]
async fn leases_competing_recovery_completion_fencing_and_identity() {
    let f = Fixture::new().await;
    let control = PostgresOrchestrator::new(Arc::clone(&f.db));
    control.register_heartbeat(&runner(), false).await.unwrap();
    let manager = RunnerId::from_string("manager");
    control.register_heartbeat(&manager, true).await.unwrap();
    let eligible = control.get_active_runners(10, Some(true)).await.unwrap();
    assert_eq!(eligible.len(), 1);
    assert_eq!(eligible[0].runner_id, manager);
    assert_eq!(
        control
            .get_active_runners(10, Some(false))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(control.get_active_runner_ids(10).await.unwrap().len(), 2);
    f.publication().submit(submission(&f.id)).await.unwrap();
    assert_eq!(
        f.broker()
            .retrieve_invocation_from_queue("critical", None)
            .await
            .unwrap(),
        Some(f.id.clone())
    );
    assert!(f
        .broker()
        .retrieve_invocation_from_queue("critical", None)
        .await
        .unwrap()
        .is_none());
    tokio::time::sleep(Duration::from_millis(240)).await;
    assert_eq!(
        f.broker()
            .retrieve_invocation_from_queue("critical", None)
            .await
            .unwrap(),
        Some(f.id.clone())
    );
    f.change(PublicationChange::Status(Status::Pending)).await;
    assert_eq!(f.count("broker_queue").await, 0);
    f.change(PublicationChange::Status(Status::Running)).await;
    let incoming = submission(&f.id).invocation.trace_context;
    let first = f
        .publication()
        .begin_execution(&f.id, &runner(), 0, &incoming)
        .await
        .unwrap();
    let p1 = f.publication();
    let p2 = f.publication();
    let recovery = PublicationChange::Recover {
        status: Status::RunningRecovery,
        stale_after_seconds: 0,
        route: route(),
    };
    let r1 = RunnerId::from_string("recoverer-1");
    let r2 = RunnerId::from_string("recoverer-2");
    let (a, b) = tokio::join!(
        p1.change(&f.id, &r1, recovery.clone(), false),
        p2.change(&f.id, &r2, recovery, false)
    );
    assert_eq!(
        usize::from(a.unwrap().is_some()) + usize::from(b.unwrap().is_some()),
        1
    );
    assert_eq!(f.status().await, Status::Rerouted);
    assert_eq!(f.count("broker_queue").await, 1);
    let replacement = RunnerId::from_string("replacement");
    let p = f.publication();
    p.change(
        &f.id,
        &replacement,
        PublicationChange::Status(Status::Pending),
        false,
    )
    .await
    .unwrap();
    p.change(
        &f.id,
        &replacement,
        PublicationChange::Status(Status::Running),
        false,
    )
    .await
    .unwrap();
    let second = p
        .begin_execution(&f.id, &replacement, 0, &incoming)
        .await
        .unwrap();
    assert_eq!(
        second.previous_attempt_trace_context,
        first.execution_trace_context
    );
    assert!(p
        .begin_execution(&f.id, &runner(), 0, &incoming)
        .await
        .is_err());
    assert!(p
        .change(
            &f.id,
            &runner(),
            PublicationChange::Success("stale-secret".into()),
            true
        )
        .await
        .is_err());
    assert!(f
        .state()
        .store_result_for_runner(&f.id, "stale-secret", &runner())
        .await
        .is_err());
    assert!(f
        .state()
        .store_error_for_runner(
            &f.id,
            &TaskError {
                error_type: "stale".into(),
                message: "stale-secret".into(),
                traceback: None
            },
            &runner()
        )
        .await
        .is_err());
    assert!(f.state().get_result(&f.id).await.unwrap().is_none());
    p.change(
        &f.id,
        &replacement,
        PublicationChange::Success("\"current\"".into()),
        true,
    )
    .await
    .unwrap();
    assert_eq!(
        f.state().get_result(&f.id).await.unwrap().as_deref(),
        Some("\"current\"")
    );
    assert_eq!(f.count("auto_purge_schedule").await, 1);
    assert!(!p.submit(submission(&f.id)).await.unwrap());
    assert_eq!(f.status().await, Status::Success);
}

#[tokio::test]
#[ignore = "isolated network gate"]
async fn checkout_query_and_authentication_bounds() {
    let f = Fixture::new().await;
    let started = Instant::now();
    assert!(f
        .db
        .conn()
        .await
        .unwrap()
        .query_one("SELECT pg_sleep(10)", &[])
        .await
        .is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(
        f.db.conn()
            .await
            .unwrap()
            .query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    let mut held = Vec::new();
    for _ in 0..4 {
        held.push(f.db.conn().await.unwrap());
    }
    let started = Instant::now();
    assert!(f.db.conn().await.is_err());
    assert!(started.elapsed() < Duration::from_secs(3));
    drop(held);
    let invalid = format!("{} password=secret-invalid-credential", dsn());
    let started = Instant::now();
    #[cfg(feature = "tls")]
    let err = if let Ok(hostname) = std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME") {
        let ca = std::fs::read(std::env::var("RUSTVELLO_POSTGRES_TLS_CA").unwrap()).unwrap();
        Database::connect_tls_with_options(
            &invalid,
            &format!("auth_{}", RunnerId::new()),
            options(),
            crate::db::PostgresTlsOptions::private_ca_pem(hostname, ca).unwrap(),
        )
        .await
        .err()
        .unwrap()
    } else {
        Database::connect_with_options(&invalid, &format!("auth_{}", RunnerId::new()), options())
            .await
            .err()
            .unwrap()
    };
    #[cfg(not(feature = "tls"))]
    let err =
        Database::connect_with_options(&invalid, &format!("auth_{}", RunnerId::new()), options())
            .await
            .err()
            .unwrap();
    assert!(!err.to_string().contains("secret-invalid-credential"));
    assert!(started.elapsed() < Duration::from_secs(3));
    let drift = PostgresOptions {
        max_queue_rows: 99,
        ..options()
    };
    #[cfg(feature = "tls")]
    if std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME").is_ok() {
        let hostname = std::env::var("RUSTVELLO_POSTGRES_TLS_HOSTNAME").unwrap();
        let ca = std::fs::read(std::env::var("RUSTVELLO_POSTGRES_TLS_CA").unwrap()).unwrap();
        assert!(Database::connect_tls_with_options(
            &dsn(),
            &f.app,
            drift,
            crate::db::PostgresTlsOptions::private_ca_pem(hostname, ca).unwrap(),
        )
        .await
        .is_err());
    } else {
        assert!(Database::connect_with_options(&dsn(), &f.app, drift)
            .await
            .is_err());
    }
    #[cfg(not(feature = "tls"))]
    assert!(Database::connect_with_options(&dsn(), &f.app, drift)
        .await
        .is_err());
}

#[tokio::test]
#[ignore = "isolated network gate"]
async fn concurrent_admission_redelivery_does_not_steal_slot() {
    use rustvello_proto::{config::TaskConfig, status::ConcurrencyControlType};
    let f = Fixture::new().await;
    let task = TaskId::new("network", "limited");
    let mut cfg = TaskConfig::default();
    cfg.concurrency_control = ConcurrencyControlType::Task;
    cfg.running_concurrency = Some(1);
    let a = f.control();
    let b = f.control();
    let id1 = InvocationId::new();
    let id2 = InvocationId::new();
    let (first, second) = tokio::join!(
        a.try_acquire_concurrency_slot(&id1, &task, &cfg, None),
        b.try_acquire_concurrency_slot(&id2, &task, &cfg, None)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert_ne!(first, second);
    let winner = if first { id1 } else { id2 };
    assert!(a
        .try_acquire_concurrency_slot(&winner, &task, &cfg, None)
        .await
        .unwrap());
    let mut zero = cfg;
    zero.running_concurrency = Some(0);
    assert!(!a
        .try_acquire_concurrency_slot(&winner, &task, &zero, None)
        .await
        .unwrap());
}

#[cfg(feature = "fault-injection")]
struct Process(std::process::Child);
#[cfg(feature = "fault-injection")]
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn publication_child() {
    let Ok(role) = std::env::var("NETWORK_CHILD_ROLE") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let f = Fixture::open(
            std::env::var("NETWORK_CHILD_APP").unwrap(),
            InvocationId::from_string(std::env::var("NETWORK_CHILD_ID").unwrap()),
        )
        .await;
        match role.as_str() {
            "submit" => {
                f.publication().submit(submission(&f.id)).await.unwrap();
            }
            "delivery" => {
                f.broker()
                    .retrieve_invocation_from_queue("critical", None)
                    .await
                    .unwrap();
            }
            "execution" => {
                f.publication()
                    .begin_execution(
                        &f.id,
                        &runner(),
                        0,
                        &submission(&f.id).invocation.trace_context,
                    )
                    .await
                    .unwrap();
            }
            "success" => {
                f.change(PublicationChange::Success("\"complete\"".into()))
                    .await
            }
            "failure" => {
                f.change(PublicationChange::Failure(TaskError {
                    error_type: "Failure".into(),
                    message: "exact error".into(),
                    traceback: Some("exact traceback".into()),
                }))
                .await
            }
            "retry" => f.change(PublicationChange::Retry(route())).await,
            "recover" => {
                f.change(PublicationChange::Recover {
                    status: Status::RunningRecovery,
                    stale_after_seconds: 0,
                    route: route(),
                })
                .await
            }
            "pending" => f.change(PublicationChange::Status(Status::Pending)).await,
            "running" => f.change(PublicationChange::Status(Status::Running)).await,
            _ => panic!("unknown test role"),
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated process-kill gate"]
#[cfg(feature = "fault-injection")]
async fn process_kills_at_every_publication_boundary() {
    let mut measured = Vec::new();
    for (role, stages) in [
        (
            "submit",
            vec![
                "before_begin",
                "control",
                "call",
                "workflow",
                "history",
                "queue",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "success",
            vec![
                "before_begin",
                "status_history",
                "payload",
                "terminal_effects",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "failure",
            vec![
                "before_begin",
                "status_history",
                "payload",
                "terminal_effects",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "retry",
            vec![
                "before_begin",
                "status_history",
                "counter",
                "queue",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "recover",
            vec![
                "before_begin",
                "recovery_status",
                "status_history",
                "queue",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "pending",
            vec![
                "before_begin",
                "status_history",
                "before_commit",
                "after_commit",
            ],
        ),
        (
            "running",
            vec![
                "before_begin",
                "status_history",
                "before_commit",
                "after_commit",
            ],
        ),
        ("execution", vec!["before_commit", "after_commit"]),
        ("delivery", vec!["after_commit"]),
    ] {
        for stage in stages {
            let started = Instant::now();
            let f = Fixture::new().await;
            if matches!(role, "pending" | "running" | "delivery") {
                f.publication().submit(submission(&f.id)).await.unwrap();
                if role == "running" {
                    f.change(PublicationChange::Status(Status::Pending)).await;
                }
            } else if role != "submit" {
                f.seed_running().await;
            }
            if matches!(role, "success" | "failure") {
                let conn = f.db.conn().await.unwrap();
                conn.execute(
                    "INSERT INTO waiting_for VALUES ('waiter',$1)",
                    &[&f.id.as_str()],
                )
                .await
                .unwrap();
                conn.execute(
                    "INSERT INTO cc_arg_pairs VALUES ($1,'network','key','value')",
                    &[&f.id.as_str()],
                )
                .await
                .unwrap();
            }
            let temp = tempfile::tempdir().unwrap();
            let marker = temp.path().join("barrier");
            let operation = match role {
                "success" | "failure" => "complete",
                "pending" => "status.PENDING",
                "running" => "status.RUNNING",
                _ => role,
            };
            let point = format!("{operation}.{stage}");
            let mut child = Process(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "acceptance::publication_child", "--nocapture"])
                    .env("NETWORK_CHILD_ROLE", role)
                    .env("NETWORK_CHILD_APP", &f.app)
                    .env("NETWORK_CHILD_ID", f.id.as_str())
                    .env("RUSTVELLO_POSTGRES_FAILPOINT", &point)
                    .env("RUSTVELLO_POSTGRES_BARRIER_FILE", &marker)
                    .stdout(std::process::Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            while !marker.exists() {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "child exited at {point}"
                );
                assert!(
                    started.elapsed() < Duration::from_secs(15),
                    "barrier timeout: {point}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let committed = stage == "after_commit";
            match role {
                "submit" => {
                    assert_eq!(f.count("invocations").await, i64::from(committed));
                    assert_eq!(f.count("history").await, i64::from(committed));
                    assert_eq!(f.count("calls").await, i64::from(committed));
                    assert_eq!(f.count("workflow_runs").await, i64::from(committed));
                    assert_eq!(f.count("broker_queue").await, i64::from(committed));
                    assert_eq!(
                        f.publication().submit(submission(&f.id)).await.unwrap(),
                        !committed
                    );
                    assert_eq!(f.count("broker_queue").await, 1);
                }
                "success" | "failure" => {
                    assert_eq!(
                        f.status().await,
                        if committed {
                            if role == "success" {
                                Status::Success
                            } else {
                                Status::Failed
                            }
                        } else {
                            Status::Running
                        }
                    );
                    assert_eq!(
                        f.count(if role == "success" {
                            "results"
                        } else {
                            "errors"
                        })
                        .await,
                        i64::from(committed)
                    );
                    assert_eq!(f.count("history").await, 3 + i64::from(committed));
                    assert_eq!(f.count("waiting_for").await, i64::from(!committed));
                    assert_eq!(f.count("cc_arg_pairs").await, i64::from(!committed));
                    assert_eq!(f.count("auto_purge_schedule").await, i64::from(committed));
                    if committed && role == "success" {
                        assert_eq!(
                            f.state().get_result(&f.id).await.unwrap().as_deref(),
                            Some("\"complete\"")
                        );
                    }
                    if committed && role == "failure" {
                        assert_eq!(
                            f.state().get_error(&f.id).await.unwrap().unwrap().message,
                            "exact error"
                        );
                    }
                }
                "retry" | "recover" => {
                    assert_eq!(
                        f.status().await,
                        if committed {
                            if role == "retry" {
                                Status::Retry
                            } else {
                                Status::Rerouted
                            }
                        } else {
                            Status::Running
                        }
                    );
                    assert_eq!(f.count("broker_queue").await, i64::from(committed));
                    assert_eq!(
                        f.control().get_invocation_retries(&f.id).await.unwrap(),
                        u32::from(committed && role == "retry")
                    );
                }
                "execution" => {
                    assert_eq!(f.count("workflow_data").await, i64::from(committed));
                }
                "delivery" => {
                    tokio::time::sleep(Duration::from_millis(240)).await;
                    assert_eq!(
                        f.broker()
                            .retrieve_invocation_from_queue("critical", None)
                            .await
                            .unwrap(),
                        Some(f.id.clone())
                    );
                }
                "pending" => {
                    assert_eq!(
                        f.status().await,
                        if committed {
                            Status::Pending
                        } else {
                            Status::Registered
                        }
                    );
                    assert_eq!(f.count("broker_queue").await, i64::from(!committed));
                }
                "running" => {
                    assert_eq!(
                        f.status().await,
                        if committed {
                            Status::Running
                        } else {
                            Status::Pending
                        }
                    );
                }
                _ => unreachable!(),
            }
            measured.push(serde_json::json!({"role":role,"boundary":point,"committed":committed,"elapsed_ms":started.elapsed().as_millis()}));
        }
    }
    println!(
        "NETWORK_CRASH_EVIDENCE={}",
        serde_json::to_string(&measured).unwrap()
    );
}
