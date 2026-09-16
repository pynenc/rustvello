//! LC-07-R: same-host SQLite qualification using independently exec'd processes.
//! Run: cargo test --offline -p rustvello --features sqlite --test runtime_backend_acceptance
#![cfg(feature = "sqlite")]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello::prelude::*;
use rustvello::runner::ShutdownOutcome;
use rustvello::sqlite::broker::SqliteBroker;
use rustvello::sqlite::db::Database;

const APP: &str = "tenant-a";
const CHILD_TIMEOUT: Duration = Duration::from_secs(20);

/// Unique on-disk fixture; child processes share paths, never Rust connections.
struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("rustvello-lc07-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn db(&self) -> PathBuf {
        self.0.join("runtime.db")
    }

    fn spawn(&self, role: &str, app: &str, name: &str) -> Process {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process_entry", "--nocapture"])
            .env("LC07_ROLE", role)
            .env("LC07_DIR", &self.0)
            .env("LC07_APP", app)
            .env("LC07_NAME", name)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Process(child)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Kill and reap children on timeout or assertion failure.
struct Process(Child);

impl Process {
    fn finish(&mut self) {
        let start = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "child {}: {status}", self.0.id());
                return;
            }
            assert!(
                start.elapsed() < CHILD_TIMEOUT,
                "child {} hung",
                self.0.id()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn wait_file(path: &Path) {
    tokio::time::timeout(CHILD_TIMEOUT, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {}", path.display()));
}

fn task_id() -> TaskId {
    TaskId::new("lc07", "work")
}

async fn app(path: &Path, app_id: &str, marker: Option<PathBuf>) -> RustvelloApp {
    app_with_lease(path, app_id, marker, Duration::from_secs(60)).await
}

async fn app_with_lease(
    path: &Path,
    app_id: &str,
    marker: Option<PathBuf>,
    lease: Duration,
) -> RustvelloApp {
    let broker = SqliteBroker::new(Arc::new(Database::open(path, app_id).unwrap()))
        .with_reservation_lease(lease)
        .unwrap();
    let mut app = Rustvello::builder()
        .app_id(app_id)
        .sqlite(path.to_str().unwrap(), app_id)
        .broker(Arc::new(broker))
        .heartbeat_interval(1)
        .runner_dead_after_seconds(2)
        .recovery_check_interval(1)
        .max_pending_seconds(1)
        .build()
        .await
        .unwrap();
    app.config.atomic_service_check_interval_minutes = 0.001;
    app.config.atomic_service_interval_minutes = 0.01;
    app.config.atomic_service_spread_margin_minutes = 0.0;
    let mut config = TaskConfig::default();
    config.blocking = true;
    app.register_task(
        task_id(),
        config,
        Arc::new(move |_| {
            if let Some(marker) = &marker {
                std::fs::write(marker, std::process::id().to_string()).unwrap();
                std::thread::sleep(Duration::from_secs(60));
            }
            Ok(std::process::id().to_string())
        }),
    )
    .unwrap();
    app
}

// Each helper starts through the OS executable loader and opens its own DB.
#[test]
fn process_entry() {
    let Ok(role) = std::env::var("LC07_ROLE") else {
        return;
    };
    let dir = PathBuf::from(std::env::var("LC07_DIR").unwrap());
    let app_id = std::env::var("LC07_APP").unwrap();
    let name = std::env::var("LC07_NAME").unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let application = app_with_lease(
                &dir.join("runtime.db"),
                &app_id,
                (role == "crash").then(|| dir.join("executing")),
                if role == "dequeued" {
                    Duration::from_secs(1)
                } else {
                    Duration::from_secs(60)
                },
            )
            .await;
            match role.as_str() {
                "dequeue" | "claim" => {
                    std::fs::write(dir.join(format!("ready-{name}")), b"").unwrap();
                    wait_file(&dir.join("go")).await;
                    let mut claimed = Vec::new();
                    if role == "dequeue" {
                        while let Some(id) = application
                            .broker()
                            .retrieve_invocation(None)
                            .await
                            .unwrap()
                        {
                            claimed.push(id.to_string());
                        }
                    } else {
                        for id in std::fs::read_to_string(dir.join("ids")).unwrap().lines() {
                            let id = InvocationId::from_string(id);
                            match application
                                .orchestrator()
                                .set_invocation_status(
                                    &id,
                                    InvocationStatus::Pending,
                                    Some(&RunnerId::from_string(name.as_str())),
                                )
                                .await
                            {
                                Ok(_) => claimed.push(id.to_string()),
                                Err(RustvelloError::InvalidStatusTransition { .. }) => {}
                                Err(error) => panic!("claim failed: {error}"),
                            }
                        }
                    }
                    std::fs::write(dir.join(format!("result-{name}")), claimed.join("\n")).unwrap();
                }
                "crash" => application
                    .into_runner()
                    .with_num_workers(1)
                    .run()
                    .await
                    .unwrap(),
                "pending" | "dequeued" => {
                    let id = application
                        .broker()
                        .retrieve_invocation(None)
                        .await
                        .unwrap()
                        .unwrap();
                    if role == "pending" {
                        application
                            .orchestrator()
                            .set_invocation_status(
                                &id,
                                InvocationStatus::Pending,
                                Some(&RunnerId::from_string("dead-pending")),
                            )
                            .await
                            .unwrap();
                    }
                    std::fs::write(dir.join("executing"), std::process::id().to_string()).unwrap();
                    std::future::pending::<()>().await;
                }
                "recover" => {
                    let state = application.state_backend();
                    let id = InvocationId::from_string(
                        std::fs::read_to_string(dir.join("ids")).unwrap(),
                    );
                    let signal = async {
                        loop {
                            if state.get_invocation(&id).await.unwrap().status
                                == InvocationStatus::Success
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    };
                    tokio::time::timeout(
                        Duration::from_secs(12),
                        application
                            .into_runner()
                            .with_num_workers(1)
                            .with_bounded_shutdown(signal, Duration::from_secs(1)),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                }
                "isolated" => {
                    let id = InvocationId::from_string(
                        std::fs::read_to_string(dir.join("ids")).unwrap(),
                    );
                    assert!(matches!(
                        application.get_status(&id).await,
                        Err(RustvelloError::InvocationNotFound { .. })
                    ));
                    assert!(matches!(
                        application.state_backend().get_invocation(&id).await,
                        Err(RustvelloError::InvocationNotFound { .. })
                    ));
                    assert_eq!(application.get_result(&id).await.unwrap(), None);
                    assert_eq!(
                        application.broker().count_invocations(None).await.unwrap(),
                        0
                    );
                    assert!(application
                        .orchestrator()
                        .get_active_runner_ids(100)
                        .await
                        .unwrap()
                        .is_empty());
                    // Identical invocation/task IDs in another app must not share ownership.
                    let call = CallDTO::new(task_id(), SerializedArguments::new());
                    application
                        .orchestrator()
                        .register_invocation_with_id(&id, &call, None)
                        .await
                        .unwrap();
                    application
                        .orchestrator()
                        .set_invocation_status(
                            &id,
                            InvocationStatus::Pending,
                            Some(&RunnerId::from_string("foreign-owner")),
                        )
                        .await
                        .unwrap();
                    application.purge().await.unwrap();
                }
                "stale_completion" => {
                    let id = InvocationId::from_string(
                        std::fs::read_to_string(dir.join("ids")).unwrap(),
                    );
                    let lifecycle = rustvello::orchestration::Orchestrator::new(
                        application.orchestrator(),
                        application.state_backend(),
                        application.broker(),
                        application.client_data_store(),
                        None,
                        0.0,
                    );
                    let old = RunnerId::from_string("old-owner");
                    assert!(matches!(
                        lifecycle
                            .set_invocation_result(&id, "stale-result", &old)
                            .await,
                        Err(RustvelloError::OwnershipViolation { .. })
                    ));
                    assert!(matches!(
                        lifecycle
                            .set_invocation_exception(&id, "StaleError", "stale-error", &old)
                            .await,
                        Err(RustvelloError::OwnershipViolation { .. })
                    ));
                }
                other => panic!("unknown role: {other}"),
            }
        });
}

async fn race(role: &str) {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let mut expected = HashSet::new();
    for _ in 0..80 {
        expected.insert(
            application
                .submit(&task_id(), SerializedArguments::new())
                .await
                .unwrap()
                .to_string(),
        );
    }
    std::fs::write(
        fixture.0.join("ids"),
        expected.iter().cloned().collect::<Vec<_>>().join("\n"),
    )
    .unwrap();
    let mut children = (0..4)
        .map(|i| fixture.spawn(role, APP, &i.to_string()))
        .collect::<Vec<_>>();
    assert_eq!(
        children
            .iter()
            .map(|p| p.0.id())
            .collect::<HashSet<_>>()
            .len(),
        4
    );
    for i in 0..4 {
        wait_file(&fixture.0.join(format!("ready-{i}"))).await;
    }
    std::fs::write(fixture.0.join("go"), b"").unwrap();
    let mut actual = HashSet::new();
    for (i, child) in children.iter_mut().enumerate() {
        child.finish();
        for id in std::fs::read_to_string(fixture.0.join(format!("result-{i}")))
            .unwrap()
            .lines()
        {
            assert!(actual.insert(id.to_owned()), "duplicate claim {id}");
        }
    }
    assert_eq!(actual, expected);
    if role == "dequeue" {
        assert_eq!(
            application.broker().count_invocations(None).await.unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn cross_process_atomic_dequeue() {
    race("dequeue").await;
}

#[tokio::test]
async fn cross_process_atomic_pending_claim() {
    race("claim").await;
}

#[tokio::test]
async fn killed_worker_is_recovered_by_a_new_process() {
    assert_recovery(
        "crash",
        InvocationStatus::Running,
        InvocationStatus::RunningRecovery,
    )
    .await;
}

#[tokio::test]
async fn killed_pending_owner_is_recovered_by_a_new_process() {
    assert_recovery(
        "pending",
        InvocationStatus::Pending,
        InvocationStatus::PendingRecovery,
    )
    .await;
}

#[tokio::test]
async fn killed_after_dequeue_before_pending_is_redelivered() {
    // Child pauses only after retrieve returns, before any Pending call.
    assert_recovery(
        "dequeued",
        InvocationStatus::Registered,
        InvocationStatus::Pending,
    )
    .await;
}

async fn assert_recovery(role: &str, initial: InvocationStatus, recovery: InvocationStatus) {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let id = application
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    std::fs::write(fixture.0.join("ids"), id.as_str()).unwrap();
    let mut killed = fixture.spawn(role, APP, "old");
    wait_file(&fixture.0.join("executing")).await;
    let old = application
        .orchestrator()
        .get_invocation_status(&id)
        .await
        .unwrap();
    assert_eq!(old.status, initial);
    assert_eq!(
        application.broker().count_invocations(None).await.unwrap(),
        0
    );
    killed.0.kill().unwrap();
    killed.0.wait().unwrap();
    let mut replacement = fixture.spawn("recover", APP, "new");
    assert_ne!(killed.0.id(), replacement.0.id());
    replacement.finish();
    assert_eq!(
        application.get_status(&id).await.unwrap(),
        InvocationStatus::Success
    );
    assert_eq!(
        application.get_result(&id).await.unwrap(),
        Some(replacement.0.id().to_string())
    );
    let history = application.state_backend().get_history(&id).await.unwrap();
    assert!(history.iter().any(|h| h.status_record.status == recovery));
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Running)
            .count(),
        if role == "crash" { 2 } else { 1 }
    );
    assert!(
        application
            .orchestrator()
            .set_invocation_status(&id, InvocationStatus::Success, old.runner_id.as_ref(),)
            .await
            .is_err(),
        "old owner must not complete the recovered invocation"
    );
}

#[tokio::test]
async fn app_separation_survives_foreign_process_purge() {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let id = application
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    application
        .state_backend()
        .store_result(&id, "tenant-a-result")
        .await
        .unwrap();
    application
        .orchestrator()
        .register_heartbeat(&RunnerId::new(), true)
        .await
        .unwrap();
    std::fs::write(fixture.0.join("ids"), id.as_str()).unwrap();
    fixture.spawn("isolated", "tenant-b", "foreign").finish();
    assert_eq!(
        application.get_status(&id).await.unwrap(),
        InvocationStatus::Registered
    );
    assert_eq!(
        application.get_result(&id).await.unwrap().as_deref(),
        Some("tenant-a-result")
    );
    assert_eq!(
        application.broker().count_invocations(None).await.unwrap(),
        1
    );
    assert!(fixture.0.join("runtime_tenant-a.db").is_file());
    assert!(fixture.0.join("runtime_tenant-b.db").is_file());
}

#[tokio::test]
async fn stale_process_cannot_overwrite_replacement_payload_or_status() {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let id = application
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    std::fs::write(fixture.0.join("ids"), id.as_str()).unwrap();
    let control = application.orchestrator();
    let old = RunnerId::from_string("old-owner");
    let new = RunnerId::from_string("replacement-owner");
    for (status, runner) in [
        (InvocationStatus::Pending, &old),
        (InvocationStatus::Running, &old),
        (InvocationStatus::RunningRecovery, &new),
        (InvocationStatus::Rerouted, &new),
        (InvocationStatus::Pending, &new),
        (InvocationStatus::Running, &new),
    ] {
        control
            .set_invocation_status(&id, status, Some(runner))
            .await
            .unwrap();
    }
    let state = application.state_backend();
    state
        .store_result_for_runner(&id, "winner", &new)
        .await
        .unwrap();
    fixture.spawn("stale_completion", APP, "old").finish();
    assert_eq!(
        state.get_result(&id).await.unwrap().as_deref(),
        Some("winner")
    );
    assert!(state.get_error(&id).await.unwrap().is_none());
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().runner_id,
        Some(new.clone())
    );
    control
        .set_invocation_status(&id, InvocationStatus::Success, Some(&new))
        .await
        .unwrap();
    fixture.spawn("stale_completion", APP, "late").finish();
    assert_eq!(
        state.get_result(&id).await.unwrap().as_deref(),
        Some("winner")
    );
    assert!(state.get_error(&id).await.unwrap().is_none());
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().status,
        InvocationStatus::Success
    );
}

#[tokio::test]
async fn cancellation_before_start_preserves_queued_work() {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let id = application
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();
    let state = application.state_backend();
    let broker = application.broker();
    let outcome = application
        .into_runner()
        .with_num_workers(1)
        .with_bounded_shutdown(std::future::ready(()), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(outcome, ShutdownOutcome::Drained);
    assert_eq!(
        state.get_invocation(&id).await.unwrap().status,
        InvocationStatus::Registered
    );
    assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
}

#[tokio::test]
async fn idle_deadline_returns_within_budget() {
    let fixture = Fixture::new();
    let application = app(&fixture.db(), APP, None).await;
    let start = Instant::now();
    let outcome = application
        .into_runner()
        .with_num_workers(1)
        .with_bounded_shutdown(
            tokio::time::sleep(Duration::from_millis(30)),
            Duration::from_millis(200),
        )
        .await
        .unwrap();
    assert_eq!(outcome, ShutdownOutcome::Drained);
    assert!(start.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_task_drain_and_deadline_are_distinct() {
    for (task_ms, budget, expected) in [
        (40, Duration::from_secs(1), ShutdownOutcome::Drained),
        (
            400,
            Duration::from_millis(30),
            ShutdownOutcome::DeadlineElapsed,
        ),
    ] {
        let fixture = Fixture::new();
        let mut application = app(&fixture.db(), APP, None).await;
        let started = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        let mut config = TaskConfig::default();
        config.blocking = true;
        let tid = TaskId::new("lc07", "slow");
        application
            .register_task(
                tid.clone(),
                config,
                Arc::new({
                    let started = Arc::clone(&started);
                    let finished = Arc::clone(&finished);
                    move |_| {
                        started.notify_one();
                        std::thread::sleep(Duration::from_millis(task_ms));
                        finished.notify_one();
                        Ok("42".to_owned())
                    }
                }),
            )
            .unwrap();
        let id = application
            .submit(&tid, SerializedArguments::new())
            .await
            .unwrap();
        let queued = application
            .submit(&task_id(), SerializedArguments::new())
            .await
            .unwrap();
        let control = application.orchestrator();
        let state = application.state_backend();
        let broker = application.broker();
        let start = Instant::now();
        let outcome = application
            .into_runner()
            .with_num_workers(1)
            .with_bounded_shutdown(started.notified(), budget)
            .await
            .unwrap();
        assert_eq!(outcome, expected);
        if expected == ShutdownOutcome::DeadlineElapsed {
            assert!(start.elapsed() < Duration::from_millis(300));
            assert_eq!(
                control.get_invocation_status(&id).await.unwrap().status,
                InvocationStatus::Running
            );
            assert_eq!(state.get_result(&id).await.unwrap(), None);
        } else {
            assert_eq!(
                control.get_invocation_status(&id).await.unwrap().status,
                InvocationStatus::Success
            );
        }
        // Await the actual blocking task too, so the fixture is never removed underneath it.
        tokio::time::timeout(Duration::from_secs(2), finished.notified())
            .await
            .unwrap();
        assert_eq!(
            control.get_invocation_status(&queued).await.unwrap().status,
            InvocationStatus::Registered
        );
        assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
        if expected == ShutdownOutcome::DeadlineElapsed {
            // Completion of an abandoned blocking call must not invent a terminal transition.
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(
                control.get_invocation_status(&id).await.unwrap().status,
                InvocationStatus::Running
            );
            assert_eq!(state.get_result(&id).await.unwrap(), None);
        }
    }
}

#[test]
fn unavailable_local_backend_fails_visibly() {
    let fixture = Fixture::new();
    assert!(Database::open(fixture.0.join("missing/runtime.db"), APP).is_err());
}

#[test]
fn sqlite_rejects_unsafe_or_aliasing_app_ids_before_file_creation() {
    let fixture = Fixture::new();
    for id in [
        "",
        "../escape",
        "a/b",
        "a\\b",
        "TENANT-A",
        ".",
        "..",
        "a.b",
        "a\0b",
        "calf\u{e9}",
        &"a".repeat(65),
    ] {
        assert!(
            matches!(
                Database::open(fixture.db(), id),
                Err(RustvelloError::Configuration { .. })
            ),
            "accepted {id:?}"
        );
    }
    assert_eq!(std::fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert!(Database::open(fixture.db(), "tenant-a_012").is_ok());
}
