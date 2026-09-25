//! Kill test for durable retry backoff (roadmap M2.2).
//!
//! A worker process fails the first attempt, commits a Retry with a backoff
//! delay and is killed (SIGKILL) during the backoff. Two fresh worker
//! processes then compete for the invocation. The retry must fire exactly
//! once, and not before the delay: the not-before time lives in the backend,
//! not in the dead worker's memory.
//!
//! SQLite runs in `make test`. PostgreSQL runs when `RUSTVELLO_POSTGRES_DSN`
//! points at a disposable database:
//! `cargo test -p rustvello --features postgres --test durable_retry_kill -- --ignored`.
#![cfg(feature = "sqlite")]

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustvello::app::RustvelloApp;
use rustvello::prelude::*;

const DELAY_MS: u64 = 2_000;
const DEADLINE: Duration = Duration::from_secs(30);

fn task_id() -> TaskId {
    TaskId::new("m2_kill", "flaky")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

fn task_config() -> TaskConfig {
    let mut config = TaskConfig::default();
    config.max_retries = 3;
    config.retry_delay_ms = DELAY_MS;
    config.retry_jitter = RetryJitter::None;
    config
}

#[derive(Clone)]
struct Target {
    backend: &'static str,
    location: String,
    app_id: String,
}

async fn app(target: &Target, attempts: Option<PathBuf>) -> RustvelloApp {
    let builder = Rustvello::builder().app_id(target.app_id.clone());
    let builder = match target.backend {
        "sqlite" => builder.sqlite(&target.location, &target.app_id),
        #[cfg(feature = "postgres")]
        "postgres" => builder.postgres_with_options(
            &target.location,
            &target.app_id,
            rustvello::postgres::db::PostgresOptions {
                // Generous budget: CI hosts may be loaded while schemas initialize.
                operation_timeout_ms: 30_000,
                ..Default::default()
            },
        ),
        other => panic!("unsupported backend {other}"),
    };
    let mut app = builder.heartbeat_interval(1).build().await.unwrap();
    let body: rustvello_core::task::TaskFn = Arc::new(move |_| {
        let context = get_invocation_context().expect("runner context");
        if let Some(path) = &attempts {
            // One line per attempt: "<num_retries> <epoch-ms>", fsynced before
            // the attempt returns so a kill cannot lose it.
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(file, "{} {}", context.num_retries, now_ms()).unwrap();
            file.sync_all().unwrap();
        }
        if context.num_retries == 0 {
            Err(RustvelloError::TaskExecution {
                error_type: "Flaky".into(),
                message: "first attempt fails".into(),
                traceback: None,
            })
        } else {
            Ok("\"recovered\"".into())
        }
    });
    app.register_task(task_id(), task_config(), body).unwrap();
    app
}

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_worker(target: &Target, attempts: &Path) -> Worker {
    Worker(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "durable_retry_child",
                "--nocapture",
                "--include-ignored",
            ])
            .env("M2_KILL_BACKEND", target.backend)
            .env("M2_KILL_LOCATION", &target.location)
            .env("M2_KILL_APP", &target.app_id)
            .env("M2_KILL_ATTEMPTS", attempts)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}

fn read_attempts(path: &Path) -> Vec<(u32, u128)> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| {
            let (retries, at) = line.split_once(' ').unwrap();
            (retries.parse().unwrap(), at.parse().unwrap())
        })
        .collect()
}

async fn wait_for(client: &RustvelloApp, id: &InvocationId, wanted: InvocationStatus) {
    let start = Instant::now();
    loop {
        if client.get_status(id).await.unwrap() == wanted {
            return;
        }
        assert!(start.elapsed() < DEADLINE, "never reached {wanted}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn kill_worker_during_backoff(target: Target, dir: &Path) {
    let attempts = dir.join("attempts.log");
    let client = app(&target, None).await;
    let id = client
        .submit(&task_id(), SerializedArguments::new())
        .await
        .unwrap();

    let mut first = spawn_worker(&target, &attempts);
    wait_for(&client, &id, InvocationStatus::Retry).await;
    first.0.kill().unwrap();
    first.0.wait().unwrap();
    let killed_at = now_ms();

    let failed = read_attempts(&attempts);
    assert_eq!(failed.len(), 1, "exactly one attempt before the kill");
    let failed_at = failed[0].1;

    // Replacement workers compete for the delayed retry.
    let _second = spawn_worker(&target, &attempts);
    let _third = spawn_worker(&target, &attempts);
    // Still backing off shortly after the kill (if the delay has not passed).
    if now_ms() + 300 < failed_at + u128::from(DELAY_MS) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            client.get_status(&id).await.unwrap(),
            InvocationStatus::Retry
        );
        assert_eq!(read_attempts(&attempts).len(), 1);
    }
    wait_for(&client, &id, InvocationStatus::Success).await;
    // Give a duplicate delivery a chance to show up before judging.
    tokio::time::sleep(Duration::from_millis(1_000)).await;

    let all = read_attempts(&attempts);
    assert_eq!(all.len(), 2, "retry fired exactly once: {all:?}");
    let (retries, retried_at) = all[1];
    assert_eq!(retries, 1);
    assert!(
        retried_at >= failed_at + u128::from(DELAY_MS),
        "retry fired {} ms after the failure, before the {DELAY_MS} ms backoff",
        retried_at - failed_at
    );
    assert!(
        retried_at <= failed_at + u128::from(DELAY_MS) + 10_000,
        "retry fired {} ms after the failure",
        retried_at - failed_at
    );
    assert!(
        killed_at < failed_at + u128::from(DELAY_MS),
        "kill happened during backoff"
    );
    let history = client.state_backend().get_history(&id).await.unwrap();
    let retries_recorded = history
        .iter()
        .filter(|h| h.status_record.status == InvocationStatus::Retry)
        .count();
    assert_eq!(retries_recorded, 1);
    let result = client.get_result(&id).await.unwrap();
    assert_eq!(result.as_deref(), Some("\"recovered\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sqlite_retry_survives_worker_kill_during_backoff_and_fires_once() {
    let dir = tempfile::tempdir().unwrap();
    let target = Target {
        backend: "sqlite",
        location: dir.path().join("kill.db").to_str().unwrap().to_owned(),
        app_id: "m2kill".into(),
    };
    kill_worker_during_backoff(target, dir.path()).await;
}

#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs RUSTVELLO_POSTGRES_DSN (disposable database)"]
async fn postgres_retry_survives_worker_kill_during_backoff_and_fires_once() {
    let dir = tempfile::tempdir().unwrap();
    let target = Target {
        backend: "postgres",
        location: std::env::var("RUSTVELLO_POSTGRES_DSN").expect("RUSTVELLO_POSTGRES_DSN"),
        app_id: format!("m2kill{}", std::process::id()),
    };
    kill_worker_during_backoff(target, dir.path()).await;
}

/// Child worker process body; a no-op unless spawned by the tests above.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_retry_child() {
    let Ok(backend) = std::env::var("M2_KILL_BACKEND") else {
        return;
    };
    let target = Target {
        backend: match backend.as_str() {
            "sqlite" => "sqlite",
            "postgres" => "postgres",
            other => panic!("unknown backend {other}"),
        },
        location: std::env::var("M2_KILL_LOCATION").unwrap(),
        app_id: std::env::var("M2_KILL_APP").unwrap(),
    };
    let attempts = PathBuf::from(std::env::var("M2_KILL_ATTEMPTS").unwrap());
    let runner = app(&target, Some(attempts))
        .await
        .into_runner()
        .with_num_workers(2)
        .with_idle_sleep(20);
    runner.run().await.unwrap();
}
