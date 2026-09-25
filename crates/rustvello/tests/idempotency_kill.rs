//! Kill test for the at-least-once contract and the idempotency-key pattern
//! (roadmap M2.4, `docs/idempotency.md`).
//!
//! A worker process performs two side effects and is then killed (SIGKILL)
//! before it can record the result:
//!
//! * a plain effect (append a line to a log), which is **not** idempotent;
//! * a keyed effect guarded by the invocation id (create a ledger entry named
//!   after the invocation id only if it does not exist yet).
//!
//! A replacement worker recovers the invocation and runs the body again.
//! The test asserts what the guide promises: the body ran twice with the same
//! invocation id (at least once, not exactly once), the plain effect happened
//! twice, the keyed effect happened once, and the invocation has one result.
//!
//! A second test covers the submission side: `submit_call_with_key` turns
//! repeated submissions of one key into one invocation.
#![cfg(feature = "sqlite")]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello::app::RustvelloApp;
use rustvello::prelude::*;

const DEADLINE: Duration = Duration::from_secs(60);

fn task_id() -> TaskId {
    TaskId::new("m24_kill", "charge")
}

async fn app(db: &Path, effects_dir: Option<PathBuf>, hang: bool) -> RustvelloApp {
    let mut app = Rustvello::builder()
        .app_id("m24kill")
        .sqlite(db.to_str().unwrap(), "m24kill")
        .heartbeat_interval(1)
        .runner_dead_after_seconds(2)
        .recovery_check_interval(1)
        .build()
        .await
        .unwrap();
    let body: rustvello_core::task::TaskFn = Arc::new(move |_| {
        let context = get_invocation_context().expect("runner context");
        let invocation_id = context.invocation_id.to_string();
        if let Some(dir) = &effects_dir {
            // Plain side effect: one line per execution of the body.
            let mut log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join("plain.log"))
                .unwrap();
            writeln!(log, "{invocation_id} {}", std::process::id()).unwrap();
            log.sync_all().unwrap();
            // Keyed side effect: the invocation id is the idempotency key, and
            // "create only if absent" makes a repeated execution a no-op.
            let key = dir.join(format!("ledger-{invocation_id}-charge"));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&key)
            {
                Ok(mut entry) => {
                    writeln!(entry, "charged by {}", std::process::id()).unwrap();
                    entry.sync_all().unwrap();
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(e) => panic!("ledger write failed: {e}"),
            }
            if hang {
                // Both effects are durable; die before the result is recorded.
                std::fs::write(dir.join("effects-done"), b"").unwrap();
                std::thread::sleep(DEADLINE);
            }
        }
        Ok("\"charged\"".into())
    });
    let mut config = TaskConfig::default();
    config.blocking = true;
    app.register_task(task_id(), config, body).unwrap();
    app
}

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_worker(db: &Path, effects: &Path, hang: bool) -> Worker {
    Worker(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "idempotency_child", "--nocapture"])
            .env("M24_DB", db)
            .env("M24_EFFECTS", effects)
            .env("M24_HANG", if hang { "1" } else { "0" })
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

async fn wait_until(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let start = Instant::now();
    while !done().await {
        assert!(start.elapsed() < DEADLINE, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn killed_worker_body_runs_again_and_keyed_effect_applies_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("kill.db");
    let client = app(&db, None, false).await;
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();

    let mut first = spawn_worker(&db, dir.path(), true);
    let marker = dir.path().join("effects-done");
    wait_until("the first attempt's side effects", async || marker.exists()).await;
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    assert_eq!(
        client.get_status(&id).await.unwrap(),
        InvocationStatus::Running,
        "the killed worker never recorded a result"
    );

    let _replacement = spawn_worker(&db, dir.path(), false);
    wait_until("the recovered invocation to succeed", async || {
        client.get_status(&id).await.unwrap() == InvocationStatus::Success
    })
    .await;

    // At least once: the body ran again, with the same invocation id.
    let plain = std::fs::read_to_string(dir.path().join("plain.log")).unwrap();
    let runs: Vec<(&str, &str)> = plain
        .lines()
        .map(|line| line.split_once(' ').unwrap())
        .collect();
    assert_eq!(
        runs.len(),
        2,
        "plain effect ran once per execution: {runs:?}"
    );
    assert!(runs.iter().all(|(inv, _)| *inv == id.as_str()));
    assert_ne!(runs[0].1, runs[1].1, "two different worker processes");

    // The keyed effect was applied once, by the killed worker.
    let ledger: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            name.starts_with("ledger-").then_some(name)
        })
        .collect();
    assert_eq!(ledger, vec![format!("ledger-{id}-charge")]);
    let entry = std::fs::read_to_string(dir.path().join(&ledger[0])).unwrap();
    assert_eq!(entry.trim(), format!("charged by {}", runs[0].1));

    // One logical result, recorded after a recovery.
    assert_eq!(
        client.get_result(&id).await.unwrap().as_deref(),
        Some("\"charged\"")
    );
    let history = client.state_backend().get_history(&id).await.unwrap();
    assert!(history
        .iter()
        .any(|h| h.status_record.status == InvocationStatus::RunningRecovery));
    let successes = history
        .iter()
        .filter(|h| h.status_record.status == InvocationStatus::Success)
        .count();
    assert_eq!(successes, 1);
}

/// Child worker process body; a no-op unless spawned by the test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotency_child() {
    let Ok(db) = std::env::var("M24_DB") else {
        return;
    };
    let effects = PathBuf::from(std::env::var("M24_EFFECTS").unwrap());
    let hang = std::env::var("M24_HANG").unwrap() == "1";
    let runner = app(Path::new(&db), Some(effects), hang)
        .await
        .into_runner()
        .with_num_workers(1)
        .with_idle_sleep(20);
    runner.run().await.unwrap();
}

#[rustvello::task]
fn place_order(order: String, amount: u32) -> String {
    format!("{order}:{amount}")
}

#[tokio::test]
async fn keyed_submission_creates_one_invocation_per_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("keys.db");
    let app = Rustvello::builder()
        .app_id("m24keys")
        .sqlite(db.to_str().unwrap(), "m24keys")
        .auto_discover_tasks()
        .build()
        .await
        .unwrap();
    let task = PlaceOrderTask::new();
    let params = || PlaceOrderParams {
        order: "order-42".into(),
        amount: 10,
    };

    let first = app
        .submit_call_with_key("order-42", &task, params(), None)
        .await
        .unwrap();
    // A client retry after a lost acknowledgement, or a redelivered webhook.
    let again = app
        .submit_call_with_key("order-42", &task, params(), None)
        .await
        .unwrap();
    assert_eq!(first.invocation_id(), again.invocation_id());
    assert_eq!(
        first.invocation_id(),
        &InvocationId::from_key(Task::task_id(&task), "order-42")
    );

    // The same key with other arguments is rejected, not silently merged.
    let conflicting = app
        .submit_call_with_key(
            "order-42",
            &task,
            PlaceOrderParams {
                order: "order-42".into(),
                amount: 11,
            },
            None,
        )
        .await;
    assert!(conflicting.is_err());

    // Another key is another invocation.
    let other = app
        .submit_call_with_key("order-43", &task, params(), None)
        .await
        .unwrap();
    assert_ne!(first.invocation_id(), other.invocation_id());

    let runner = app.into_runner().with_num_workers(1);
    while runner.run_one().await.unwrap() {}
    assert_eq!(
        first.wait(Duration::from_millis(10)).await.unwrap(),
        "order-42:10"
    );
}
