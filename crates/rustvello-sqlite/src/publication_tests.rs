use std::sync::Arc;
use std::time::{Duration, Instant};

use rustvello_core::orchestrator::{
    OrchestratorConcurrency, OrchestratorQuery, OrchestratorStatus,
};
use rustvello_core::publication::{
    PublicationChange, PublicationRoute, RuntimePublication, SubmissionPublication,
};
use rustvello_core::state_backend::StateBackendCore;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, WorkflowIdentity};
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};

use crate::db::{Database, SqliteOptions, SqliteSynchronous};
use crate::orchestrator::SqliteOrchestrator;
use crate::publication::SqlitePublication;
use crate::state_backend::SqliteStateBackend;

fn submission() -> SubmissionPublication {
    let call = CallDTO::new(
        TaskId::new("publication", "work"),
        SerializedArguments::new(),
    );
    let id = InvocationId::new();
    SubmissionPublication {
        invocation: InvocationDTO::with_workflow(
            id.clone(),
            call.task_id.clone(),
            call.call_id.clone(),
            None,
            WorkflowIdentity::sub_workflow(id, call.task_id.clone(), InvocationId::new()),
        ),
        call,
        runner_id: RunnerId::from_string("caller"),
        runner_context: None,
        workflow_root: true,
        cc_arguments: None,
        route: PublicationRoute {
            queue: "critical".into(),
            priority: 8.0,
        },
    }
}

#[test]
fn explicit_sync_applies_to_every_open_without_resetting_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    for (synchronous, expected) in [
        (SqliteSynchronous::Normal, 1),
        (SqliteSynchronous::Full, 2),
        (SqliteSynchronous::Full, 2),
    ] {
        let db = Database::open_with_options(
            &path,
            "test",
            SqliteOptions {
                synchronous,
                busy_timeout: Duration::from_millis(40),
            },
        )
        .unwrap();
        assert_eq!(db.synchronization().unwrap(), ("wal".into(), expected, 40));
        let conn = db.conn.lock().unwrap();
        conn.execute("INSERT OR IGNORE INTO client_data (data_key,data_value) VALUES ('preserved','original')", []).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT data_value FROM client_data WHERE data_key='preserved'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "original"
        );
    }
    let options = SqliteOptions {
        busy_timeout: Duration::ZERO,
        ..Default::default()
    };
    assert!(Database::open_with_options(dir.path().join("invalid.db"), "test", options).is_err());
    assert!(!dir.path().join("invalid_test.db").exists());
}

#[tokio::test]
async fn busy_writer_is_bounded_and_replay_can_publish_after_lock_release() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.sqlite");
    let db = Arc::new(
        Database::open_with_options(
            &path,
            "test",
            SqliteOptions {
                busy_timeout: Duration::from_millis(30),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let locker = Database::open(&path, "test").unwrap();
    let (ready, started) = std::sync::mpsc::channel();
    let (release, released) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let conn = locker.conn.lock().unwrap();
        let tx =
            rusqlite::Transaction::new_unchecked(&conn, rusqlite::TransactionBehavior::Immediate)
                .unwrap();
        ready.send(()).unwrap();
        let _ = released.recv_timeout(Duration::from_secs(5));
        drop(tx);
    });
    started.recv_timeout(Duration::from_secs(2)).unwrap();
    let s = submission();
    let start = Instant::now();
    let outcome = SqlitePublication::new(Arc::clone(&db))
        .submit(s.clone())
        .await;
    release.send(()).unwrap();
    thread.join().unwrap();
    assert!(outcome.is_err());
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(SqliteOrchestrator::new(Arc::clone(&db))
        .get_invocation_status(&s.invocation.invocation_id)
        .await
        .is_err());
    assert!(SqlitePublication::new(Arc::clone(&db))
        .submit(s.clone())
        .await
        .unwrap());
    assert!(!SqlitePublication::new(Arc::clone(&db))
        .submit(s)
        .await
        .unwrap());
}

#[tokio::test]
async fn real_sql_write_errors_rollback_recovery_retry_completion_and_preserve_lineage() {
    for operation in ["retry", "recover", "complete"] {
        let db = Arc::new(Database::in_memory().unwrap());
        let publication = SqlitePublication::new(Arc::clone(&db));
        let s = submission();
        let id = &s.invocation.invocation_id;
        let owner = RunnerId::from_string("old-worker");
        publication.submit(s.clone()).await.unwrap();
        publication
            .change(
                id,
                &owner,
                PublicationChange::Status(InvocationStatus::Pending),
                false,
            )
            .await
            .unwrap();
        publication
            .change(
                id,
                &owner,
                PublicationChange::Status(InvocationStatus::Running),
                false,
            )
            .await
            .unwrap();
        let state = SqliteStateBackend::new(Arc::clone(&db));
        let before = state.get_history(id).await.unwrap();
        let sql = if operation == "complete" {
            "CREATE TRIGGER reject_write BEFORE INSERT ON results BEGIN SELECT RAISE(ABORT, 'disk write fixture'); END"
        } else {
            "CREATE TRIGGER reject_write BEFORE INSERT ON broker_queue BEGIN SELECT RAISE(ABORT, 'disk write fixture'); END"
        };
        db.conn.lock().unwrap().execute_batch(sql).unwrap();
        let change = match operation {
            "retry" => PublicationChange::Retry(s.route.clone()),
            "recover" => PublicationChange::Recover {
                status: InvocationStatus::RunningRecovery,
                stale_after_seconds: 0,
                route: s.route.clone(),
            },
            _ => PublicationChange::Success("ok".into()),
        };
        assert!(publication
            .change(id, &owner, change.clone(), true)
            .await
            .is_err());
        assert_eq!(state.get_history(id).await.unwrap().len(), before.len());
        assert_eq!(
            state.get_invocation(id).await.unwrap().status,
            InvocationStatus::Running
        );
        assert_eq!(
            state.get_invocation(id).await.unwrap().workflow,
            s.invocation.workflow
        );
        assert_eq!(
            SqliteOrchestrator::new(Arc::clone(&db))
                .get_invocation_retries(id)
                .await
                .unwrap(),
            0
        );
        assert!(state.get_result(id).await.unwrap().is_none());
        db.conn
            .lock()
            .unwrap()
            .execute_batch("DROP TRIGGER reject_write")
            .unwrap();
        publication.change(id, &owner, change, true).await.unwrap();
    }
}

#[tokio::test]
async fn replay_rejects_different_content_and_purge_is_explicit() {
    let db = Arc::new(Database::in_memory().unwrap());
    let p = SqlitePublication::new(Arc::clone(&db));
    let s = submission();
    p.submit(s.clone()).await.unwrap();
    let mut changed = s.clone();
    changed.route.queue = "different".into();
    assert!(p.submit(changed).await.is_err());
    let mut changed = s.clone();
    changed.invocation.workflow.as_mut().unwrap().parent_id = None;
    assert!(p.submit(changed).await.is_err());
    let control = SqliteOrchestrator::new(Arc::clone(&db));
    control
        .remove_invocation(&s.invocation.invocation_id)
        .await
        .unwrap();
    // Removal must not let a replay inherit retained results/history/attempt identity.
    assert!(p
        .submit(s)
        .await
        .unwrap_err()
        .to_string()
        .contains("was removed"));
    assert!(p.submit(submission()).await.unwrap());
}

#[tokio::test]
async fn empty_argument_publication_uses_the_shared_concurrency_sentinel() {
    let db = Arc::new(Database::in_memory().unwrap());
    let mut s = submission();
    let args = SerializedArguments::new();
    s.cc_arguments = Some(args.clone());
    SqlitePublication::new(Arc::clone(&db))
        .submit(s.clone())
        .await
        .unwrap();
    let indexed = SqliteOrchestrator::new(db)
        .get_existing_invocations(
            &s.call.task_id,
            Some(&args),
            &[InvocationStatus::Registered],
        )
        .await
        .unwrap();
    assert_eq!(indexed, vec![s.invocation.invocation_id]);
}

#[tokio::test]
async fn repeated_admission_reuses_only_its_slot_and_honors_zero_quota() {
    let control = SqliteOrchestrator::new(Arc::new(Database::in_memory().unwrap()));
    let s = submission();
    let id = &s.invocation.invocation_id;
    let mut config = rustvello_proto::config::TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    for _ in 0..2 {
        assert!(control
            .try_acquire_concurrency_slot(id, &s.call.task_id, &config, None)
            .await
            .unwrap());
    }
    assert!(!control
        .try_acquire_concurrency_slot(&InvocationId::new(), &s.call.task_id, &config, None)
        .await
        .unwrap());
    let mut disabled = config;
    disabled.running_concurrency = Some(0);
    assert!(!control
        .try_acquire_concurrency_slot(id, &s.call.task_id, &disabled, None)
        .await
        .unwrap());
}

#[tokio::test]
async fn purging_either_port_cannot_acknowledge_a_removed_submission_replay() {
    for control_only in [true, false] {
        let db = Arc::new(Database::in_memory().unwrap());
        let p = SqlitePublication::new(Arc::clone(&db));
        let s = submission();
        p.submit(s.clone()).await.unwrap();
        if control_only {
            SqliteOrchestrator::new(Arc::clone(&db))
                .purge()
                .await
                .unwrap();
        } else {
            SqliteStateBackend::new(Arc::clone(&db))
                .purge()
                .await
                .unwrap();
        }
        assert!(p
            .submit(s)
            .await
            .unwrap_err()
            .to_string()
            .contains("was removed"));
        assert!(p.submit(submission()).await.unwrap());
    }
}
