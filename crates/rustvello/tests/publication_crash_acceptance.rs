//! LC-07-C: OS process death, never test-side database repair.
#![cfg(feature = "sqlite-fault-injection")]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello::prelude::*;
use rustvello_core::context::with_invocation_context;
use rustvello_core::execution::get_execution_identity;
use rustvello_proto::invocation::TraceContextCarrier;

const ID: &str = "7eaec910-1234-4567-890a-123456789abc";
const TIMEOUT: Duration = Duration::from_secs(20);

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Process {
    fn finish(&mut self) {
        let start = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "process {} failed: {status}", self.0.id());
                return;
            }
            assert!(start.elapsed() < TIMEOUT, "child deadline");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

struct Fixture(tempfile::TempDir);
impl Fixture {
    fn new() -> Self {
        Self(tempfile::tempdir().unwrap())
    }
    fn path(&self) -> &Path {
        self.0.path()
    }
    fn spawn(&self, role: &str, point: &str, fault: &str) -> Process {
        self.spawn_at(role, point, fault, "barrier")
    }
    fn spawn_at(&self, role: &str, point: &str, fault: &str, marker: &str) -> Process {
        Process(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "publication_child", "--nocapture"])
                .env("LC07C_DIR", self.path())
                .env("LC07C_ROLE", role)
                .env("RUSTVELLO_SQLITE_FAILPOINT", point)
                .env("RUSTVELLO_SQLITE_FAULT", fault)
                .env("RUSTVELLO_SQLITE_BARRIER_FILE", self.path().join(marker))
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    }
    fn clear(&self) {
        for name in [
            "barrier",
            "barrier.release",
            "executing",
            "executing.release",
        ] {
            let _ = std::fs::remove_file(self.path().join(name));
        }
    }
}

fn carrier() -> TraceContextCarrier {
    TraceContextCarrier {
        traceparent: Some("00-11111111111111111111111111111111-2222222222222222-01".into()),
        tracestate: Some("ih=qualified".into()),
    }
}
fn id() -> InvocationId {
    InvocationId::from_string(ID)
}
fn task() -> TaskId {
    TaskId::new("lc07c", "workflow")
}

fn waiter_id() -> InvocationId {
    InvocationId::from_string("7eaec910-1234-4567-890a-123456789abd")
}

fn cleanup_cc_args() -> SerializedArguments {
    let mut args = SerializedArguments::new();
    args.insert("account", "\"lc07c\"");
    args
}

fn cleanup_cc_config() -> TaskConfig {
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    config
}

async fn app(dir: &Path, role: &str) -> RustvelloApp {
    let cleanup = dir.join("cleanup-effects").exists();
    let broker = rustvello::sqlite::broker::SqliteBroker::new(Arc::new(
        rustvello::sqlite::db::Database::open(dir.join("runtime.db"), "publication").unwrap(),
    ))
    .with_reservation_lease(Duration::from_millis(100))
    .unwrap();
    let mut app = Rustvello::builder()
        .app_id("publication")
        .sqlite_with_options(
            dir.join("runtime.db").to_str().unwrap(),
            "publication",
            rustvello::sqlite::db::SqliteOptions::default(),
        )
        .broker(Arc::new(broker))
        .heartbeat_interval(1)
        .runner_dead_after_seconds(1)
        .recovery_check_interval(1)
        .max_pending_seconds(1)
        .auto_final_invocation_purge_hours(if cleanup { 24.0 } else { 0.0 })
        .build()
        .await
        .unwrap();
    app.config.atomic_service_check_interval_minutes = 0.001;
    app.config.atomic_service_interval_minutes = 0.001;
    app.config.atomic_service_spread_margin_minutes = 0.0;
    app.config.broker_queues = vec!["critical".into()];
    let dir = dir.to_path_buf();
    let pause = role == "pause";
    let mut config = TaskConfig::default();
    config.is_workflow_task = true;
    config.blocking = true;
    config.max_retries = 1;
    config.queue = "critical".into();
    config.priority = 7.0;
    if role == "reroute" {
        config.concurrency_control = ConcurrencyControlType::Task;
        config.running_concurrency = Some(0);
        config.reroute_on_cc = true;
    }
    let orchestrator = app.orchestrator();
    app.register_task(
        task(),
        config,
        Arc::new(move |_| {
            let (retries, trace) =
                with_invocation_context(|c| (c.num_retries, c.trace_context.clone())).unwrap();
            if cleanup {
                // Seed through public ports on every execution, including retry/recovery.
                tokio::runtime::Handle::current().block_on(async {
                    orchestrator
                        .set_waiting_for(&waiter_id(), &id())
                        .await
                        .unwrap();
                    assert!(orchestrator
                        .try_acquire_concurrency_slot(
                            &id(),
                            &task(),
                            &cleanup_cc_config(),
                            Some(&cleanup_cc_args()),
                        )
                        .await
                        .unwrap());
                });
            }
            if pause {
                std::fs::write(dir.join("executing"), serde_json::to_vec(&trace).unwrap()).unwrap();
                let deadline = Instant::now() + TIMEOUT;
                while !dir.join("executing.release").exists() {
                    assert!(Instant::now() < deadline, "execution barrier expired");
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
            if dir.join("fail-permanently").exists() {
                return Err(RustvelloError::TaskExecution {
                    error_type: "FixturePermanentError".into(),
                    message: "fixture permanent failure".into(),
                    traceback: None,
                });
            }
            if dir.join("fail-once").exists() && retries == 0 {
                return Err(RustvelloError::Internal {
                    message: "fixture fail once".into(),
                });
            }
            Ok(
                serde_json::json!({"pid": std::process::id(), "retries": retries, "trace": trace})
                    .to_string(),
            )
        }),
    )
    .unwrap();
    app
}

async fn wait(path: &Path) {
    tokio::time::timeout(TIMEOUT, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("missing barrier {}", path.display()));
}

#[test]
fn publication_child() {
    let Ok(dir) = std::env::var("LC07C_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let role = std::env::var("LC07C_ROLE").unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let application = app(&dir, &role).await;
            match role.as_str() {
                "submit" | "submit_busy" => {
                    let result = application
                        .submit_with_id(id(), &task(), SerializedArguments::new(), Some(carrier()))
                        .await;
                    if role == "submit_busy"
                        || std::env::var("RUSTVELLO_SQLITE_FAULT").unwrap() == "error"
                    {
                        assert!(result.is_err());
                    } else {
                        assert_eq!(result.unwrap(), id());
                    }
                }
                "pause" | "reroute" => {
                    let _ = application
                        .into_runner()
                        .with_num_workers(1)
                        .run_one()
                        .await;
                }
                "run" => {
                    let state = application.state_backend();
                    let signal = async {
                        loop {
                            if state
                                .get_invocation(&id())
                                .await
                                .unwrap()
                                .status
                                .is_terminal()
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    };
                    tokio::time::timeout(
                        TIMEOUT,
                        application
                            .into_runner()
                            .with_num_workers(1)
                            .with_bounded_shutdown(signal, Duration::from_secs(1)),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                }
                other => panic!("unknown role {other}"),
            }
        });
}

async fn assert_complete(f: &Fixture) {
    let application = app(f.path(), "inspect").await;
    let inv = application
        .state_backend()
        .get_invocation(&id())
        .await
        .unwrap();
    assert_eq!(inv.status, InvocationStatus::Success);
    assert_eq!(inv.trace_context, carrier());
    assert_eq!(inv.workflow.unwrap().workflow_id, id());
    assert_eq!(
        application
            .state_backend()
            .get_workflow_runs(&task())
            .await
            .unwrap()
            .len(),
        1
    );
    let history = application
        .state_backend()
        .get_history(&id())
        .await
        .unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Registered)
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status.is_terminal())
            .count(),
        1
    );
    assert_eq!(
        application.broker().count_invocations(None).await.unwrap(),
        0
    );
    let expected_retries = u32::from(f.path().join("fail-once").exists());
    assert_eq!(
        application
            .orchestrator()
            .get_invocation_retries(&id())
            .await
            .unwrap(),
        expected_retries
    );
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Retry)
            .count(),
        expected_retries as usize
    );
    let result: serde_json::Value =
        serde_json::from_str(&application.get_result(&id()).await.unwrap().unwrap()).unwrap();
    let identity = get_execution_identity(application.state_backend().as_ref(), &id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(identity.execution_trace_context).unwrap(),
        result["trace"]
    );
}

async fn assert_cleanup(application: &RustvelloApp, released: bool) {
    let orchestrator = application.orchestrator();
    assert_eq!(
        orchestrator.get_waiters(&id()).await.unwrap(),
        if released { vec![] } else { vec![waiter_id()] }
    );
    let probe = InvocationId::new();
    // Acquisition checks the index itself, unlike a status-filtered CC query.
    let acquired = orchestrator
        .try_acquire_concurrency_slot(
            &probe,
            &task(),
            &cleanup_cc_config(),
            Some(&cleanup_cc_args()),
        )
        .await
        .unwrap();
    assert_eq!(acquired, released, "terminal CC index release");
    if acquired {
        orchestrator
            .remove_from_concurrency_index(&probe)
            .await
            .unwrap();
    }
}

async fn assert_failed(f: &Fixture) {
    let application = app(f.path(), "inspect").await;
    let state = application.state_backend();
    let invocation = state.get_invocation(&id()).await.unwrap();
    assert_eq!(invocation.status, InvocationStatus::Failed);
    assert_eq!(invocation.trace_context, carrier());
    assert_eq!(invocation.workflow.unwrap().workflow_id, id());
    let history = state.get_history(&id()).await.unwrap();
    for status in [
        InvocationStatus::Registered,
        InvocationStatus::Retry,
        InvocationStatus::Failed,
    ] {
        assert_eq!(
            history
                .iter()
                .filter(|h| h.status_record.status == status)
                .count(),
            1
        );
    }
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status.is_terminal())
            .count(),
        1
    );
    assert!(state.get_result(&id()).await.unwrap().is_none());
    let error = state.get_error(&id()).await.unwrap().unwrap();
    assert_eq!(error.error_type, "FixturePermanentError");
    assert_eq!(error.message, "fixture permanent failure");
    assert_eq!(error.traceback, None);
    assert_eq!(
        application
            .orchestrator()
            .get_invocation_retries(&id())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        application.broker().count_invocations(None).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn terminal_payload_and_cleanup_survive_every_kill_boundary() {
    for permanent_failure in [false, true] {
        for point in [
            "complete.before_begin",
            "complete.status_history",
            "complete.payload",
            "complete.terminal_effects",
            "complete.before_commit",
            "complete.after_commit",
        ] {
            let started = Instant::now();
            let f = Fixture::new();
            std::fs::write(f.path().join("cleanup-effects"), "").unwrap();
            std::fs::write(f.path().join("fail-once"), "").unwrap();
            if permanent_failure {
                std::fs::write(f.path().join("fail-permanently"), "").unwrap();
            }
            f.spawn("submit", "", "").finish();
            let mut worker = f.spawn("run", point, "pause");
            wait(&f.path().join("barrier")).await;
            worker.kill();

            let application = app(f.path(), "inspect").await;
            let committed = point == "complete.after_commit";
            assert_cleanup(&application, committed).await;
            if !committed {
                let state = application.state_backend();
                assert_eq!(
                    application.get_status(&id()).await.unwrap(),
                    InvocationStatus::Running
                );
                assert!(state.get_result(&id()).await.unwrap().is_none());
                assert!(state.get_error(&id()).await.unwrap().is_none());
                assert!(!state
                    .get_history(&id())
                    .await
                    .unwrap()
                    .iter()
                    .any(|h| h.status_record.status.is_terminal()));
                assert!(application
                    .orchestrator()
                    .run_auto_purge(0)
                    .await
                    .unwrap()
                    .is_empty());
            } else if permanent_failure {
                assert_failed(&f).await;
            } else {
                assert_complete(&f).await;
            }

            f.spawn("run", "", "").finish();
            f.spawn("submit", "", "").finish();
            if permanent_failure {
                assert_failed(&f).await;
            } else {
                assert_complete(&f).await;
                assert!(application
                    .state_backend()
                    .get_error(&id())
                    .await
                    .unwrap()
                    .is_none());
            }
            assert_cleanup(&application, true).await;
            // Consume the schedule only after all payload/replay assertions; never repair state.
            assert_eq!(
                application.orchestrator().run_auto_purge(0).await.unwrap(),
                vec![id()]
            );
            assert!(application
                .orchestrator()
                .run_auto_purge(0)
                .await
                .unwrap()
                .is_empty());
            println!(
                "LC07C {point} permanent_failure={permanent_failure} {:?}",
                started.elapsed()
            );
        }
    }
}

const SUBMIT_POINTS: &[&str] = &[
    "submit.before_begin",
    "submit.control",
    "submit.call",
    "submit.workflow",
    "submit.history",
    "submit.queue",
    "submit.before_commit",
    "submit.after_commit",
];

#[tokio::test]
async fn busy_submitter_and_killed_writer_leave_no_unclaimable_submission() {
    let f = Fixture::new();
    let mut contender = f.spawn_at("submit_busy", "submit.before_begin", "pause", "contender");
    wait(&f.path().join("contender")).await;
    let mut writer = f.spawn("submit", "submit.queue", "pause");
    wait(&f.path().join("barrier")).await;
    std::fs::write(f.path().join("contender.release"), "").unwrap();
    contender.finish();
    writer.kill();
    assert!(app(f.path(), "inspect")
        .await
        .get_status(&id())
        .await
        .is_err());
    f.spawn("submit", "", "").finish();
    f.spawn("run", "", "").finish();
    assert_complete(&f).await;
}

#[tokio::test]
async fn submission_kill_at_every_boundary_and_lost_ack_replay() {
    for point in SUBMIT_POINTS {
        let started = Instant::now();
        let f = Fixture::new();
        let mut submitter = f.spawn("submit", point, "pause");
        wait(&f.path().join("barrier")).await;
        submitter.kill();
        let application = app(f.path(), "inspect").await;
        if *point != "submit.after_commit" {
            assert!(application.get_status(&id()).await.is_err());
            assert!(application
                .state_backend()
                .get_history(&id())
                .await
                .unwrap()
                .is_empty());
            assert!(application
                .state_backend()
                .get_workflow_runs(&task())
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                application.broker().count_invocations(None).await.unwrap(),
                0
            );
        } else {
            assert_eq!(
                application.get_status(&id()).await.unwrap(),
                InvocationStatus::Registered
            );
        }
        // Replay in a new process. An acknowledged replay must never reset terminal state.
        f.spawn("submit", "", "").finish();
        f.spawn("run", "", "").finish();
        f.spawn("submit", "", "").finish();
        assert_complete(&f).await;
        println!("LC07C {point} {:?}", started.elapsed());
    }
}

#[tokio::test]
async fn submission_write_failures_roll_back_without_orphans() {
    for point in &SUBMIT_POINTS[..SUBMIT_POINTS.len() - 1] {
        let f = Fixture::new();
        f.spawn("submit", point, "error").finish();
        assert!(app(f.path(), "inspect")
            .await
            .get_status(&id())
            .await
            .is_err());
        f.spawn("submit", "", "").finish();
        f.spawn("run", "", "").finish();
        assert_complete(&f).await;
    }
}

#[tokio::test]
async fn concurrency_reroute_does_not_strand_unclaimable_work() {
    for point in [
        "reroute.before_begin",
        "reroute.concurrency_status",
        "reroute.status_history",
        "reroute.queue",
        "reroute.before_commit",
        "reroute.after_commit",
    ] {
        let f = Fixture::new();
        f.spawn("submit", "", "").finish();
        let mut rerouter = f.spawn("reroute", point, "pause");
        wait(&f.path().join("barrier")).await;
        rerouter.kill();
        f.spawn("run", "", "").finish();
        assert_complete(&f).await;
        println!("LC07C {point} passed");
    }
}

#[tokio::test]
async fn retry_and_terminal_publication_survive_every_kill_boundary() {
    for point in [
        "retry.before_begin",
        "retry.status_history",
        "retry.counter",
        "retry.queue",
        "retry.before_commit",
        "retry.after_commit",
        "complete.before_begin",
        "complete.status_history",
        "complete.payload",
        "complete.terminal_effects",
        "complete.before_commit",
        "complete.after_commit",
        "status.PENDING.before_commit",
        "status.PENDING.after_commit",
        "status.RUNNING.before_commit",
        "status.RUNNING.after_commit",
        "execution.before_commit",
        "execution.after_commit",
    ] {
        let started = Instant::now();
        let f = Fixture::new();
        std::fs::write(f.path().join("fail-once"), "").unwrap();
        f.spawn("submit", "", "").finish();
        let mut worker = f.spawn("run", point, "pause");
        wait(&f.path().join("barrier")).await;
        worker.kill();
        f.spawn("run", "", "").finish();
        assert_complete(&f).await;
        println!("LC07C {point} {:?}", started.elapsed());
    }
}

#[tokio::test]
async fn recovery_of_crashed_recovery_and_competing_recoverers_preserves_lineage() {
    for point in [
        "recover.before_begin",
        "recover.recovery_status",
        "recover.status_history",
        "recover.queue",
        "recover.before_commit",
        "recover.after_commit",
    ] {
        let started = Instant::now();
        let f = Fixture::new();
        f.spawn("submit", "", "").finish();
        let mut first = f.spawn("pause", "", "");
        wait(&f.path().join("executing")).await;
        let old = get_execution_identity(
            app(f.path(), "inspect").await.state_backend().as_ref(),
            &id(),
        )
        .await
        .unwrap()
        .unwrap();
        first.kill();
        let mut recovery = f.spawn("run", point, "pause");
        wait(&f.path().join("barrier")).await;
        recovery.kill();
        let mut racers: Vec<_> = (0..3).map(|_| f.spawn("run", "", "")).collect();
        for racer in &mut racers {
            racer.finish();
        }
        assert_complete(&f).await;
        let new = get_execution_identity(
            app(f.path(), "inspect").await.state_backend().as_ref(),
            &id(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(new.attempt, old.attempt + 1);
        assert_eq!(
            new.previous_attempt_trace_context,
            old.execution_trace_context
        );
        println!("LC07C {point} {:?}", started.elapsed());
    }
}

#[tokio::test]
async fn resumed_stale_worker_cannot_publish_after_replacement() {
    let f = Fixture::new();
    f.spawn("submit", "", "").finish();
    let mut old = f.spawn("pause", "", "");
    wait(&f.path().join("executing")).await;
    f.spawn("run", "", "").finish();
    let application = app(f.path(), "inspect").await;
    let result = application.get_result(&id()).await.unwrap();
    std::fs::write(f.path().join("executing.release"), "").unwrap();
    old.finish();
    assert_eq!(application.get_result(&id()).await.unwrap(), result);
    assert_complete(&f).await;
    f.clear();
}
