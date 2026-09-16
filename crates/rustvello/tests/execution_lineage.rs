//! Execution identity must agree across task code, persistence and lifecycle events.

use std::sync::{Arc, Mutex};

use rustvello::app::RustvelloApp;
use rustvello_core::context::get_invocation_context;
use rustvello_core::error::RustvelloError;
use rustvello_core::execution::get_execution_identity;
use rustvello_core::observability::{
    capture_w3c_trace_context, EventEmitter, EventLevel, TaskLifecycleEvent, TaskLifecycleKind,
};
use rustvello_core::runner::Runner;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::{ExecutorKind, TaskId};
use rustvello_proto::invocation::TraceContextCarrier;

const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";

#[derive(Clone, Default)]
struct RecordingEmitter(Arc<Mutex<Vec<TaskLifecycleEvent>>>);

impl EventEmitter for RecordingEmitter {
    fn on_task_lifecycle(&self, event: &TaskLifecycleEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

fn parent() -> TraceContextCarrier {
    TraceContextCarrier {
        traceparent: Some(PARENT.into()),
        tracestate: Some("ih=lineage".into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_nested_submissions_parent_to_the_running_attempt() {
    nested_submissions(ExecutorKind::Tokio, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tokio_nested_submissions_use_execution_parent() {
    nested_submissions(ExecutorKind::Tokio, false).await;
}

#[cfg(feature = "rayon")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rayon_nested_submissions_use_execution_parent() {
    nested_submissions(ExecutorKind::Rayon, false).await;
}

async fn nested_submissions(executor: ExecutorKind, blocking: bool) {
    let recording = RecordingEmitter::default();
    let mut app = RustvelloApp::new(AppConfig::new("lineage"))
        .with_event_emitter(EventLevel::TaskLifecycle, recording.clone());
    let child_id = TaskId::new("lineage", "child");
    app.register_task(
        child_id.clone(),
        TaskConfig::default(),
        Arc::new(|_| {
            assert_eq!(
                capture_w3c_trace_context(),
                get_invocation_context().unwrap().trace_context
            );
            Ok("null".into())
        }),
    )
    .unwrap();
    let mut submitter = RustvelloApp::with_backends(
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        app.client_data_store(),
    );
    submitter
        .register_foreign_task(child_id.clone(), TaskConfig::default())
        .unwrap();
    let submitter = Arc::new(submitter);
    let children = Arc::new(Mutex::new(Vec::new()));
    let parent_id = TaskId::new("lineage", "parent");
    let mut config = TaskConfig::default();
    config.blocking = blocking;
    config.max_retries = 1;
    let runtime = tokio::runtime::Handle::current();
    app.register_task(
        parent_id.clone(),
        config,
        Arc::new({
            let children = Arc::clone(&children);
            move |_| {
                let context = get_invocation_context().unwrap();
                assert_eq!(capture_w3c_trace_context(), context.trace_context);
                let submit = || {
                    runtime.block_on(async {
                        let persisted = get_execution_identity(
                            context.state_backend.as_ref().unwrap().as_ref(),
                            &context.invocation_id,
                        )
                        .await
                        .unwrap()
                        .unwrap();
                        assert_eq!(persisted.execution_trace_context, context.trace_context);
                        let child = submitter
                            .submit(&child_id, SerializedArguments::new())
                            .await
                            .unwrap();
                        children.lock().unwrap().push(child);
                    })
                };
                if executor == ExecutorKind::Tokio && !blocking {
                    tokio::task::block_in_place(submit);
                } else {
                    submit();
                }
                if context.num_retries == 0 {
                    Err(RustvelloError::runner_err("retry once"))
                } else {
                    Ok("null".into())
                }
            }
        }),
    )
    .unwrap();
    let invocation = app
        .submit_with_trace_context(&parent_id, SerializedArguments::new(), Some(parent()))
        .await
        .unwrap();
    let backend = app.state_backend();
    let runner: Box<dyn Runner> = match executor {
        #[cfg(feature = "rayon")]
        ExecutorKind::Rayon => Box::new(
            rustvello::runner::RayonRunner::new(
                app.config.app_id.clone(),
                app.config.clone(),
                app.broker(),
                app.orchestrator(),
                app.state_backend(),
                Arc::new(app.task_registry().clone()),
            )
            .unwrap()
            .with_num_threads(2)
            .unwrap()
            .with_event_emitter(EventLevel::TaskLifecycle, recording.clone()),
        ),
        _ => Box::new(app.into_runner()),
    };
    for _ in 0..4 {
        assert!(runner.run_one().await.unwrap());
    }
    assert!(!runner.run_one().await.unwrap());
    let events = recording.0.lock().unwrap().clone();
    let started: Vec<_> = events
        .iter()
        .filter(|event| {
            event.context.invocation_id == invocation
                && matches!(event.kind, TaskLifecycleKind::Started)
        })
        .collect();
    assert_eq!(started.len(), 2);
    assert_eq!(started[0].context.trace_context, parent());
    assert_eq!(started[1].context.trace_context, parent());
    assert!(started[0].context.previous_attempt_trace_context.is_empty());
    assert_eq!(
        started[1].context.previous_attempt_trace_context,
        started[0].context.execution_trace_context
    );
    assert_ne!(
        started[0].context.execution_trace_context,
        started[1].context.execution_trace_context
    );
    let children = children.lock().unwrap().clone();
    assert_eq!(children.len(), 2);
    for (index, child) in children.iter().enumerate() {
        let stored = backend.get_invocation(child).await.unwrap();
        assert_eq!(stored.parent_invocation_id.as_ref(), Some(&invocation));
        assert_eq!(
            stored.trace_context,
            started[index].context.execution_trace_context
        );
        let carrier = stored.trace_context.traceparent.unwrap();
        assert_eq!(&carrier[3..35], &PARENT[3..35]);
        assert!(
            carrier.ends_with("-00"),
            "unsampled flag must survive execution"
        );
        assert_eq!(
            stored.trace_context.tracestate.as_deref(),
            Some("ih=lineage")
        );
    }
    for started in started {
        let finished = events
            .iter()
            .find(|event| {
                event.context.invocation_id == invocation
                    && event.context.attempt == started.context.attempt
                    && matches!(
                        event.kind,
                        TaskLifecycleKind::Succeeded { .. } | TaskLifecycleKind::Failed { .. }
                    )
            })
            .unwrap();
        assert_eq!(started.context, finished.context);
    }
}

mod recovery {
    use super::*;
    use rustvello_core::error::RustvelloResult;
    use rustvello_core::middleware::TaskMiddleware;
    use rustvello_proto::identifiers::{InvocationId, RunnerId};
    use rustvello_proto::status::InvocationStatus;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Interrupt after the execution boundary, leaving a Running invocation to recover.
    struct InterruptFirst(AtomicBool);

    #[async_trait::async_trait]
    impl TaskMiddleware for InterruptFirst {
        async fn before(&self, _: &InvocationId, _: &TaskId) -> RustvelloResult<()> {
            if self.0.swap(false, Ordering::SeqCst) {
                Err(RustvelloError::runner_err("execution interrupted"))
            } else {
                Ok(())
            }
        }
        async fn after(
            &self,
            _: &InvocationId,
            _: &TaskId,
            _: &RustvelloResult<String>,
        ) -> RustvelloResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn recovered_execution_advances_identity_without_spending_retry_budget() {
        let recording = RecordingEmitter::default();
        let observed_retries = Arc::new(Mutex::new(Vec::new()));
        let mut app = RustvelloApp::new(AppConfig::new("recovery-lineage"))
            .with_event_emitter(EventLevel::TaskLifecycle, recording.clone());
        let task = TaskId::new("lineage", "recover_then_retry");
        let mut config = TaskConfig::default();
        config.max_retries = 1;
        app.register_task(
            task.clone(),
            config,
            Arc::new({
                let observed_retries = Arc::clone(&observed_retries);
                move |_| {
                    let count = get_invocation_context().unwrap().num_retries;
                    observed_retries.lock().unwrap().push(count);
                    if count == 0 {
                        Err(RustvelloError::runner_err("retry after recovery"))
                    } else {
                        Ok("true".into())
                    }
                }
            }),
        )
        .unwrap();
        let id = app
            .submit_with_trace_context(&task, SerializedArguments::new(), Some(parent()))
            .await
            .unwrap();
        let backend = app.state_backend();
        let control = app.orchestrator();
        let broker = app.broker();
        let runner = app
            .into_runner()
            .with_middleware(InterruptFirst(AtomicBool::new(true)));
        assert!(runner.run_one().await.is_err());
        assert_eq!(
            control.get_invocation_status(&id).await.unwrap().status,
            InvocationStatus::Running
        );
        let initial = get_execution_identity(backend.as_ref(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial.attempt, 0);
        let recovery_owner = RunnerId::new();
        for status in [
            InvocationStatus::RunningRecovery,
            InvocationStatus::Rerouted,
        ] {
            control
                .set_invocation_status(&id, status, Some(&recovery_owner))
                .await
                .unwrap();
        }
        broker.route_invocation_for_task(&id, &task).await.unwrap();
        assert!(runner.run_one().await.unwrap());
        assert!(runner.run_one().await.unwrap());
        assert_eq!(*observed_retries.lock().unwrap(), vec![0, 1]);
        assert_eq!(
            control.get_invocation_status(&id).await.unwrap().status,
            InvocationStatus::Success
        );
        let events = recording.0.lock().unwrap().clone();
        let starts: Vec<_> = events
            .iter()
            .filter(|event| matches!(event.kind, TaskLifecycleKind::Started))
            .collect();
        assert_eq!(
            starts
                .iter()
                .map(|event| event.context.attempt)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(starts
            .iter()
            .all(|event| event.context.trace_context == parent()));
        for pair in starts.windows(2) {
            assert_ne!(
                pair[0].context.execution_trace_context,
                pair[1].context.execution_trace_context
            );
            assert_eq!(
                pair[1].context.previous_attempt_trace_context,
                pair[0].context.execution_trace_context
            );
        }
        let scheduled = events
            .iter()
            .find(|event| matches!(event.kind, TaskLifecycleKind::RetryScheduled { .. }))
            .unwrap();
        assert_eq!(scheduled.context.attempt, 1);
        assert_eq!(
            scheduled.kind,
            TaskLifecycleKind::RetryScheduled { next_attempt: 2 }
        );
        assert_eq!(
            backend
                .get_history(&id)
                .await
                .unwrap()
                .iter()
                .filter(|entry| entry.status_record.status == InvocationStatus::Retry)
                .count(),
            1
        );
        let persisted = get_execution_identity(backend.as_ref(), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.attempt, 2);
        assert_eq!(
            persisted.execution_trace_context,
            starts[2].context.execution_trace_context
        );
    }

    #[tokio::test]
    async fn execution_counter_exhaustion_preserves_the_last_identity() {
        use rustvello_core::execution::begin_execution;
        let app = RustvelloApp::new(AppConfig::new("counter-exhaustion"));
        let backend = app.state_backend();
        let id = InvocationId::new();
        let last = begin_execution(backend.as_ref(), &id, u32::MAX, &parent())
            .await
            .unwrap();
        assert!(begin_execution(backend.as_ref(), &id, 0, &parent())
            .await
            .is_err());
        assert_eq!(
            get_execution_identity(backend.as_ref(), &id).await.unwrap(),
            Some(last)
        );
    }
}

#[cfg(feature = "sqlite")]
mod relocation {
    use super::*;
    use rustvello::builder::Rustvello;
    use rustvello_proto::status::InvocationStatus;

    async fn app(path: &str) -> RustvelloApp {
        let mut app = Rustvello::builder()
            .app_id("lineage-process")
            .sqlite(path, "lineage-process")
            .build()
            .await
            .unwrap();
        let mut config = TaskConfig::default();
        config.max_retries = 1;
        app.register_task(
            TaskId::new("lineage", "relocate"),
            config,
            Arc::new(|_| {
                let context = get_invocation_context().unwrap();
                assert_eq!(capture_w3c_trace_context(), context.trace_context);
                if context.num_retries == 0 {
                    Err(RustvelloError::runner_err("retry on next process"))
                } else {
                    Ok("true".into())
                }
            }),
        )
        .unwrap();
        app
    }

    #[tokio::test]
    async fn process_worker() {
        let Ok(path) = std::env::var("RUSTVELLO_LINEAGE_TEST_DB") else {
            return;
        };
        assert!(app(&path).await.into_runner().run_one().await.unwrap());
    }

    #[tokio::test]
    async fn retry_identity_survives_independent_processes() {
        let dir = std::env::temp_dir().join(format!("rustvello-lineage-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("shared.db");
        let path = path.to_str().unwrap();
        let app = app(path).await;
        let invocation = app
            .submit_with_trace_context(
                &TaskId::new("lineage", "relocate"),
                SerializedArguments::new(),
                Some(parent()),
            )
            .await
            .unwrap();
        let backend = app.state_backend();
        let mut identities = Vec::new();
        let mut pids = Vec::new();
        for attempt in 0..2 {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "relocation::process_worker", "--nocapture"])
                .env("RUSTVELLO_LINEAGE_TEST_DB", path)
                .spawn()
                .unwrap();
            pids.push(child.id());
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("worker timed out");
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            let identity = get_execution_identity(backend.as_ref(), &invocation)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(identity.attempt, attempt);
            identities.push(identity);
        }
        assert_ne!(pids[0], pids[1]);
        assert_eq!(
            identities[1].previous_attempt_trace_context,
            identities[0].execution_trace_context
        );
        assert_ne!(
            identities[0].execution_trace_context,
            identities[1].execution_trace_context
        );
        assert_eq!(
            backend
                .get_invocation(&invocation)
                .await
                .unwrap()
                .trace_context,
            parent()
        );
        assert_eq!(
            app.get_status(&invocation).await.unwrap(),
            InvocationStatus::Success
        );
        let history = backend.get_history(&invocation).await.unwrap();
        let workers: Vec<_> = history
            .iter()
            .filter(|entry| entry.status_record.status == InvocationStatus::Running)
            .map(|entry| entry.runner_id.as_ref().unwrap())
            .collect();
        assert_eq!(workers.len(), 2);
        for (worker, pid) in workers.iter().zip(pids) {
            assert_eq!(
                backend
                    .get_runner_context(worker.as_str())
                    .await
                    .unwrap()
                    .unwrap()
                    .pid,
                pid
            );
        }
        drop(backend);
        drop(app);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
