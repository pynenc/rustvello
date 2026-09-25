//! M1.1: a trigger firing survives OS process death at every boundary between
//! claim, condition clear, invocation publication and run completion.
//!
//! Each case kills a child process parked on a fault-injection barrier, then
//! lets fresh processes recover. The only accepted outcome is exactly one
//! logical invocation per firing: never zero (a lost firing), never two.
//!
//! SQLite runs with `--features sqlite-fault-injection`. PostgreSQL runs with
//! `--features postgres-fault-injection` and `RUSTVELLO_POSTGRES_DSN` set to an
//! isolated database (the cases are `#[ignore]`d without it).
#![cfg(any(
    feature = "sqlite-fault-injection",
    feature = "postgres-fault-injection"
))]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rustvello::prelude::*;
use rustvello_core::trigger::trigger_run_invocation_id;

const TIMEOUT: Duration = Duration::from_secs(30);
const EVENT: &str = "trgcrash_ready";

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Backend {
    Sqlite,
    Postgres,
}

impl Backend {
    fn from_env() -> Self {
        match std::env::var("TRGC_BACKEND").as_deref() {
            Ok("postgres") => Self::Postgres,
            _ => Self::Sqlite,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres => "postgres",
        }
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    backend: Backend,
    app_id: String,
}

impl Fixture {
    fn new(backend: Backend) -> Self {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        Self {
            dir: tempfile::tempdir().unwrap(),
            backend,
            app_id: format!("trgc_{}", &suffix[..12]),
        }
    }
    fn path(&self) -> &Path {
        self.dir.path()
    }
    fn spawn(&self, role: &str, point: &str, fault: &str) -> Process {
        let barrier = self.path().join("barrier");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "trigger_child",
                "--nocapture",
                "--include-ignored",
            ])
            .env("TRGC_DIR", self.path())
            .env("TRGC_ROLE", role)
            .env("TRGC_BACKEND", self.backend.name())
            .env("TRGC_APP", &self.app_id)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        // The same point name is armed in every failpoint family; only the
        // layer that owns the name ever reaches it.
        for family in ["RUSTVELLO", "RUSTVELLO_SQLITE", "RUSTVELLO_POSTGRES"] {
            command
                .env(format!("{family}_FAILPOINT"), point)
                .env(format!("{family}_FAULT"), fault)
                .env(format!("{family}_BARRIER_FILE"), &barrier);
        }
        Process(command.spawn().unwrap())
    }
    fn clear_barrier(&self) {
        let _ = std::fs::remove_file(self.path().join("barrier"));
        let _ = std::fs::remove_file(self.path().join("barrier.release"));
    }
}

fn target() -> TaskId {
    TaskId::new("trgcrash", "target")
}

fn runner() -> RunnerId {
    RunnerId::from_string("trgcrash-atomic-service")
}

async fn app(dir: &Path, backend: Backend, app_id: &str) -> RustvelloApp {
    let builder = Rustvello::builder().app_id(app_id);
    let builder = match backend {
        Backend::Sqlite => {
            #[cfg(feature = "sqlite-fault-injection")]
            {
                builder.sqlite(dir.join("runtime.db").to_str().unwrap(), app_id)
            }
            #[cfg(not(feature = "sqlite-fault-injection"))]
            {
                let _ = dir;
                panic!("SQLite trigger crash cases need sqlite-fault-injection")
            }
        }
        Backend::Postgres => {
            #[cfg(feature = "postgres-fault-injection")]
            {
                let _ = dir;
                builder.postgres(&std::env::var("RUSTVELLO_POSTGRES_DSN").unwrap(), app_id)
            }
            #[cfg(not(feature = "postgres-fault-injection"))]
            {
                panic!("PostgreSQL trigger crash cases need postgres-fault-injection")
            }
        }
    };
    let mut app = builder.build().await.unwrap();
    let mut config = TaskConfig::default();
    config.queue = "critical".into();
    config.priority = 3.0;
    app.register_task(target(), config, std::sync::Arc::new(|_| Ok("null".into())))
        .unwrap();
    app
}

#[test]
fn trigger_child() {
    let Ok(dir) = std::env::var("TRGC_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let role = std::env::var("TRGC_ROLE").unwrap();
    let app_id = std::env::var("TRGC_APP").unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let application = app(&dir, Backend::from_env(), &app_id).await;
            let manager = application.trigger_manager().unwrap().clone();
            match role.as_str() {
                "seed" => {
                    TriggerBuilder::new()
                        .on_event(EVENT)
                        .with_static_args(serde_json::json!({"batch": 7}))
                        .build_and_register(&target(), manager.store())
                        .await
                        .unwrap();
                    manager
                        .emit_event(EVENT, serde_json::json!({"batch": 7}))
                        .await
                        .unwrap();
                }
                "fire" => {
                    let result = application.trigger_loop_iteration(&runner()).await;
                    if std::env::var("RUSTVELLO_FAULT").as_deref() != Ok("error") {
                        result.unwrap();
                    }
                }
                other => panic!("unknown role {other}"),
            }
        });
}

async fn wait_for(path: &Path, child: &mut Process, point: &str) {
    let started = Instant::now();
    while !path.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before reaching {point}"
        );
        assert!(started.elapsed() < TIMEOUT, "barrier timeout at {point}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Every firing ends with exactly one logical invocation, linked from its run.
async fn assert_exactly_one_invocation(f: &Fixture, point: &str) {
    let application = app(f.path(), f.backend, &f.app_id).await;
    let manager = application.trigger_manager().unwrap();
    let store = manager.store();

    let invocations = application
        .orchestrator()
        .get_existing_invocations(&target(), None, ALL_STATUSES)
        .await
        .unwrap();
    assert_eq!(
        invocations.len(),
        1,
        "{point}: expected exactly one triggered invocation, found {invocations:?}"
    );
    let invocation_id = &invocations[0];

    let runs = store
        .get_trigger_runs(&TriggerRunQuery::default())
        .await
        .unwrap();
    assert_eq!(runs.len(), 1, "{point}: one claimed run per firing");
    let run = &runs[0];
    assert_eq!(
        run.triggered_invocation_id.as_ref(),
        Some(invocation_id),
        "{point}: run completion links the published invocation"
    );
    assert_eq!(
        &trigger_run_invocation_id(&run.trigger_run_id),
        invocation_id,
        "{point}: invocation identity is derived from the run id"
    );
    assert!(
        store
            .get_pending_trigger_runs(100)
            .await
            .unwrap()
            .is_empty(),
        "{point}: no run left in the outbox"
    );
    assert!(
        store.get_valid_conditions().await.unwrap().is_empty(),
        "{point}: consumed conditions are cleared"
    );

    let history = application
        .state_backend()
        .get_history(invocation_id)
        .await
        .unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|h| h.status_record.status == InvocationStatus::Registered)
            .count(),
        1,
        "{point}: one registration"
    );
    let call = application
        .state_backend()
        .get_call(
            &application
                .state_backend()
                .get_invocation(invocation_id)
                .await
                .unwrap()
                .call_id,
        )
        .await
        .unwrap();
    assert_eq!(
        call.serialized_arguments.0.get("batch").map(String::as_str),
        Some("7"),
        "{point}: trigger arguments survive recovery"
    );
    assert_eq!(
        application.broker().count_invocations(None).await.unwrap(),
        1,
        "{point}: exactly one queued delivery"
    );
}

/// Kill points in commit order: claim transaction, publication transaction,
/// run completion. Orchestrator points use the core failpoint family; the
/// others live inside the backend transaction.
const KILL_POINTS: &[&str] = &[
    "trigger.claim.before_commit",
    "trigger.claim.after_commit",
    "trigger.claimed",
    "submit.before_begin",
    "submit.queue",
    "submit.before_commit",
    "submit.after_commit",
    "trigger.published",
    "trigger.completed",
];

async fn kill_at_every_boundary(backend: Backend) {
    for point in KILL_POINTS {
        let started = Instant::now();
        let f = Fixture::new(backend);
        f.spawn("seed", "", "").finish();

        let mut firing = f.spawn("fire", point, "pause");
        wait_for(&f.path().join("barrier"), &mut firing, point).await;
        firing.kill();
        f.clear_barrier();

        // Recovery happens in fresh processes; a second pass must be a no-op.
        f.spawn("fire", "", "").finish();
        f.spawn("fire", "", "").finish();
        assert_exactly_one_invocation(&f, point).await;
        println!("TRGC {backend:?} {point} {:?}", started.elapsed());
    }
}

async fn write_failure_at_every_boundary(backend: Backend) {
    for point in KILL_POINTS {
        let f = Fixture::new(backend);
        f.spawn("seed", "", "").finish();
        f.spawn("fire", point, "error").finish();
        f.spawn("fire", "", "").finish();
        f.spawn("fire", "", "").finish();
        assert_exactly_one_invocation(&f, point).await;
    }
}

async fn concurrent_recoverers_publish_once(backend: Backend) {
    for point in [
        "trigger.claimed",
        "submit.after_commit",
        "trigger.published",
    ] {
        let f = Fixture::new(backend);
        f.spawn("seed", "", "").finish();
        let mut firing = f.spawn("fire", point, "pause");
        wait_for(&f.path().join("barrier"), &mut firing, point).await;
        firing.kill();
        f.clear_barrier();
        let mut racers: Vec<_> = (0..3).map(|_| f.spawn("fire", "", "")).collect();
        for racer in &mut racers {
            racer.finish();
        }
        assert_exactly_one_invocation(&f, point).await;
    }
}

#[cfg(feature = "sqlite-fault-injection")]
#[tokio::test]
async fn sqlite_trigger_firing_survives_kill_at_every_boundary() {
    kill_at_every_boundary(Backend::Sqlite).await;
}

#[cfg(feature = "sqlite-fault-injection")]
#[tokio::test]
async fn sqlite_trigger_firing_survives_write_failure_at_every_boundary() {
    write_failure_at_every_boundary(Backend::Sqlite).await;
}

#[cfg(feature = "sqlite-fault-injection")]
#[tokio::test]
async fn sqlite_concurrent_trigger_recoverers_publish_once() {
    concurrent_recoverers_publish_once(Backend::Sqlite).await;
}

#[cfg(feature = "postgres-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated PostgreSQL gate: set RUSTVELLO_POSTGRES_DSN"]
async fn postgres_trigger_firing_survives_kill_at_every_boundary() {
    kill_at_every_boundary(Backend::Postgres).await;
}

#[cfg(feature = "postgres-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated PostgreSQL gate: set RUSTVELLO_POSTGRES_DSN"]
async fn postgres_trigger_firing_survives_write_failure_at_every_boundary() {
    write_failure_at_every_boundary(Backend::Postgres).await;
}

#[cfg(feature = "postgres-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated PostgreSQL gate: set RUSTVELLO_POSTGRES_DSN"]
async fn postgres_concurrent_trigger_recoverers_publish_once() {
    concurrent_recoverers_publish_once(Backend::Postgres).await;
}
