//! Executor cleanup must never delete a replacement owner's concurrency slot.
#![cfg(feature = "sqlite-fault-injection")]

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello::prelude::*;
use rustvello_core::publication::{PublicationChange, PublicationRoute};

const TIMEOUT: Duration = Duration::from_secs(20);

fn task() -> TaskId {
    TaskId::new("cleanup", "limited")
}

fn config() -> TaskConfig {
    let mut config = TaskConfig::default();
    config.blocking = true;
    config.max_retries = 1;
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    config
}

async fn app(dir: &Path, pause_task: bool) -> RustvelloApp {
    let db =
        Arc::new(rustvello::sqlite::db::Database::open(dir.join("runtime.db"), "cleanup").unwrap());
    let broker = rustvello::sqlite::broker::SqliteBroker::new(db)
        .with_reservation_lease(Duration::from_millis(100))
        .unwrap();
    let mut app = Rustvello::builder()
        .app_id("cleanup")
        .sqlite_with_options(
            dir.join("runtime.db").to_str().unwrap(),
            "cleanup",
            rustvello::sqlite::db::SqliteOptions::default(),
        )
        .broker(Arc::new(broker))
        .auto_final_invocation_purge_hours(0.0)
        .build()
        .await
        .unwrap();
    let dir = dir.to_path_buf();
    app.register_task(
        task(),
        config(),
        Arc::new(move |_| {
            std::fs::write(dir.join("executed"), "yes").unwrap();
            if pause_task {
                std::fs::write(dir.join("barrier"), "executing").unwrap();
                let deadline = Instant::now() + TIMEOUT;
                while !dir.join("barrier.release").exists() {
                    assert!(Instant::now() < deadline, "task barrier expired");
                    std::thread::sleep(Duration::from_millis(5));
                }
                return Err(RustvelloError::Internal {
                    message: "retry after ownership transfer".into(),
                });
            }
            Ok("null".into())
        }),
    )
    .unwrap();
    app
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Process {
    fn start(dir: &Path, phase: &str) -> Self {
        let point = match phase {
            "pending" => "status.PENDING.before_begin",
            "running" => "status.RUNNING.before_begin",
            _ => "disabled",
        };
        Self(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "cleanup_child", "--nocapture"])
                .env("CLEANUP_TEST_DIR", dir)
                .env("CLEANUP_TEST_PHASE", phase)
                .env("RUSTVELLO_SQLITE_FAILPOINT", point)
                .env("RUSTVELLO_SQLITE_FAULT", "barrier")
                .env("RUSTVELLO_SQLITE_BARRIER_FILE", dir.join("barrier"))
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        )
    }

    async fn finish(&mut self) {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "child failed: {status}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("child deadline");
    }
}

async fn wait_for_barrier(dir: &Path) {
    tokio::time::timeout(TIMEOUT, async {
        while !dir.join("barrier").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("missing worker barrier");
}

#[test]
fn cleanup_child() {
    let Ok(dir) = std::env::var("CLEANUP_TEST_DIR") else {
        return;
    };
    let phase = std::env::var("CLEANUP_TEST_PHASE").unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let result = app(Path::new(&dir), phase == "retry")
                .await
                .into_runner()
                .run_one()
                .await;
            if phase == "pending" {
                assert!(
                    result.is_ok(),
                    "duplicate claim should be skipped: {result:?}"
                );
            } else {
                assert!(
                    matches!(
                        result,
                        Err(RustvelloError::OwnershipViolation { .. })
                            | Err(RustvelloError::InvalidStatusTransition { .. })
                    ),
                    "expected ownership fence, got {result:?}"
                );
            }
        });
}

async fn stale_cleanup(phase: &str) {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path(), false).await;
    let id = app
        .submit(&task(), SerializedArguments::new())
        .await
        .unwrap();
    let control = app.orchestrator();
    let publication = control.runtime_publication().unwrap();
    let mut child = Process::start(dir.path(), phase);
    wait_for_barrier(dir.path()).await;
    let replacement = RunnerId::new();
    if phase != "pending" {
        let recovery = if phase == "running" {
            InvocationStatus::PendingRecovery
        } else {
            InvocationStatus::RunningRecovery
        };
        assert!(publication
            .change(
                &id,
                &replacement,
                PublicationChange::Recover {
                    status: recovery,
                    stale_after_seconds: 0,
                    route: PublicationRoute {
                        queue: "default".into(),
                        priority: 0.0
                    },
                },
                false,
            )
            .await
            .unwrap()
            .is_some());
        assert!(control
            .try_acquire_concurrency_slot(
                &id,
                &task(),
                &config(),
                Some(&SerializedArguments::new())
            )
            .await
            .unwrap());
    }
    for status in [InvocationStatus::Pending, InvocationStatus::Running] {
        publication
            .change(&id, &replacement, PublicationChange::Status(status), false)
            .await
            .unwrap();
    }
    let history_len = app.state_backend().get_history(&id).await.unwrap().len();
    std::fs::write(dir.path().join("barrier.release"), "continue").unwrap();
    child.finish().await;
    let status = control.get_invocation_status(&id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Running);
    assert_eq!(status.runner_id, Some(replacement));
    assert_eq!(
        app.state_backend().get_history(&id).await.unwrap().len(),
        history_len
    );
    assert!(
        !control
            .try_acquire_concurrency_slot(
                &InvocationId::new(),
                &task(),
                &config(),
                Some(&SerializedArguments::new()),
            )
            .await
            .unwrap(),
        "stale executor deleted replacement owner's slot during {phase}"
    );
    assert_eq!(dir.path().join("executed").exists(), phase == "retry");
}

#[tokio::test]
async fn duplicate_pending_claim_preserves_winner_slot() {
    stale_cleanup("pending").await;
}

#[tokio::test]
async fn initial_running_failure_preserves_replacement_slot() {
    stale_cleanup("running").await;
}

#[tokio::test]
async fn stale_retry_preserves_replacement_slot() {
    stale_cleanup("retry").await;
}

#[tokio::test]
async fn crash_after_admission_redelivery_reuses_own_slot() {
    let dir = tempfile::tempdir().unwrap();
    let app = app(dir.path(), false).await;
    let id = app
        .submit(&task(), SerializedArguments::new())
        .await
        .unwrap();
    let control = app.orchestrator();
    let mut child = Process::start(dir.path(), "pending");
    wait_for_barrier(dir.path()).await;
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        app.into_runner().run_one().await.unwrap(),
        "orphan slot rejected its own redelivery"
    );
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().status,
        InvocationStatus::Success
    );
}

async fn retry_releases_and_keeps_effective_route(mut app: RustvelloApp) {
    app.config.broker_queues = vec!["critical".into()];
    app.config.priority_rules = vec![BrokerPriorityRule {
        task_id: "*".into(),
        priority: 9.0,
    }];
    app.set_task_config_overrides(
        Default::default(),
        rustvello::task_config::TaskConfigOverride {
            queue: Some("critical".into()),
            ..Default::default()
        },
    );
    app.register_task(
        task(),
        config(),
        Arc::new(|_| {
            if rustvello_core::context::with_invocation_context(|c| c.num_retries).unwrap() == 0 {
                Err(RustvelloError::Internal {
                    message: "retry once".into(),
                })
            } else {
                Ok("null".into())
            }
        }),
    )
    .unwrap();
    let id = app
        .submit(&task(), SerializedArguments::new())
        .await
        .unwrap();
    let control = app.orchestrator();
    let broker = app.broker();
    let runner = app.into_runner();
    assert!(runner.run_one().await.unwrap());
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().status,
        InvocationStatus::Retry
    );
    // Higher-priority retry must precede a later medium-priority message.
    let other = InvocationId::new();
    broker
        .route_invocation_with_options(&other, Some(&task()), "critical", 5.0)
        .await
        .unwrap();
    assert_eq!(
        broker
            .retrieve_invocation_for_language_from_queue(TaskLanguage::Rust, "critical")
            .await
            .unwrap(),
        Some(id.clone())
    );
    assert_eq!(
        broker
            .retrieve_invocation_for_language_from_queue(TaskLanguage::Rust, "critical")
            .await
            .unwrap(),
        Some(other)
    );
    broker
        .route_invocation_with_options(&id, Some(&task()), "critical", 9.0)
        .await
        .unwrap();
    assert!(
        runner.run_one().await.unwrap(),
        "retry did not release its slot"
    );
    assert_eq!(
        control.get_invocation_status(&id).await.unwrap().status,
        InvocationStatus::Success
    );
    assert!(control
        .try_acquire_concurrency_slot(
            &InvocationId::new(),
            &task(),
            &config(),
            Some(&SerializedArguments::new())
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn sqlite_retry_releases_atomically_and_preserves_effective_route() {
    let dir = tempfile::tempdir().unwrap();
    let app = Rustvello::builder()
        .app_id("cleanup")
        .sqlite_with_options(
            dir.path().join("runtime.db").to_str().unwrap(),
            "cleanup",
            rustvello::sqlite::db::SqliteOptions::default(),
        )
        .auto_final_invocation_purge_hours(0.0)
        .build()
        .await
        .unwrap();
    retry_releases_and_keeps_effective_route(app).await;
}

#[cfg(feature = "mem")]
#[tokio::test]
async fn generic_retry_retains_cleanup_and_effective_route() {
    retry_releases_and_keeps_effective_route(RustvelloApp::new(AppConfig::new("cleanup"))).await;
}
