//! Temporary workers must report lifecycle and registration provenance truthfully.

use std::sync::{Arc, Mutex};

use rustvello::app::RustvelloApp;
use rustvello_core::context::{RunnerContext, RUNNER_CTX};
use rustvello_core::observability::{
    EventEmitter, EventLevel, TaskLifecycleEvent, WorkerLifecycleEvent, WorkerLifecycleKind,
};
use rustvello_core::runner::Runner;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

#[derive(Clone, Default)]
struct Recording {
    workers: Arc<Mutex<Vec<WorkerLifecycleEvent>>>,
    tasks: Arc<Mutex<Vec<TaskLifecycleEvent>>>,
}

impl EventEmitter for Recording {
    fn on_worker_lifecycle(&self, event: &WorkerLifecycleEvent) {
        self.workers.lock().unwrap().push(event.clone());
    }
    fn on_task_lifecycle(&self, event: &TaskLifecycleEvent) {
        self.tasks.lock().unwrap().push(event.clone());
    }
}

#[tokio::test]
async fn registration_context_uses_destination_app_and_preserves_active_worker() {
    let recording = Recording::default();
    let mut app = RustvelloApp::new(AppConfig::new("receiving-app"))
        .with_event_emitter(EventLevel::TaskLifecycle, recording.clone());
    let task = TaskId::new("lifecycle", "task");
    app.register_task(
        task.clone(),
        TaskConfig::default(),
        Arc::new(|_| Ok("null".into())),
    )
    .unwrap();
    app.submit(&task, SerializedArguments::new()).await.unwrap();
    let worker = RunnerContext::new(RunnerId::new(), Arc::from("receiving-app"), "ActualWorker");
    RUNNER_CTX
        .scope(
            worker.clone(),
            app.submit(&task, SerializedArguments::new()),
        )
        .await
        .unwrap();
    let other = RunnerContext::new(RunnerId::new(), Arc::from("another-app"), "AnotherWorker");
    RUNNER_CTX
        .scope(other, app.submit(&task, SerializedArguments::new()))
        .await
        .unwrap();
    let events = recording.tasks.lock().unwrap().clone();
    assert_eq!(events.len(), 3);
    for event in &events {
        assert_eq!(event.context.app_id, event.context.worker.app_id);
        assert_eq!(event.context.worker.app_id.as_ref(), "receiving-app");
        assert_eq!(event.context.worker.process_id, std::process::id());
    }
    assert_eq!(
        events[0].context.worker.runner_cls.as_ref(),
        "ExternalRunner"
    );
    assert_eq!(events[1].context.worker.runner_id, worker.runner_id);
    assert_eq!(events[1].context.worker.runner_cls.as_ref(), "ActualWorker");
    assert_eq!(
        events[2].context.worker.runner_cls.as_ref(),
        "ExternalRunner"
    );
    for event in &events {
        let history = app
            .state_backend()
            .get_history(&event.context.invocation_id)
            .await
            .unwrap();
        assert_eq!(
            history[0].runner_id.as_ref(),
            Some(&event.context.worker.runner_id)
        );
    }
}

#[tokio::test]
async fn tokio_run_one_pairs_lifecycle_on_empty_success_claim_and_execution_errors() {
    check_sessions(false).await;
}

#[cfg(feature = "rayon")]
#[tokio::test]
async fn rayon_run_one_pairs_lifecycle_on_empty_success_claim_and_execution_errors() {
    check_sessions(true).await;
}

async fn check_sessions(rayon: bool) {
    for outcome in ["empty", "success", "claim_error", "execution_error"] {
        let recording = Recording::default();
        let mut config = AppConfig::new("worker-lifecycle");
        if outcome == "claim_error" {
            config.runner_queues = vec!["invalid queue".into()];
        }
        let mut app = RustvelloApp::new(config)
            .with_event_emitter(EventLevel::TaskLifecycle, recording.clone());
        let task = TaskId::new("lifecycle", "task");
        app.register_task(
            task.clone(),
            TaskConfig::default(),
            Arc::new(|_| Ok("null".into())),
        )
        .unwrap();
        if outcome == "success" {
            app.submit(&task, SerializedArguments::new()).await.unwrap();
        }
        if outcome == "execution_error" {
            app.broker()
                .route_invocation(&InvocationId::new())
                .await
                .unwrap();
        }
        let backend = app.state_backend();
        let runner: Box<dyn Runner> = if rayon {
            #[cfg(feature = "rayon")]
            {
                Box::new(
                    rustvello::runner::RayonRunner::new(
                        app.config.app_id.clone(),
                        app.config.clone(),
                        app.broker(),
                        app.orchestrator(),
                        app.state_backend(),
                        Arc::new(app.task_registry().clone()),
                    )
                    .unwrap()
                    .with_num_threads(1)
                    .unwrap()
                    .with_event_emitter(EventLevel::TaskLifecycle, recording.clone()),
                )
            }
            #[cfg(not(feature = "rayon"))]
            {
                unreachable!()
            }
        } else {
            Box::new(app.into_runner())
        };
        let result = runner.run_one().await;
        match outcome {
            "empty" => assert!(!result.unwrap()),
            "success" => assert!(result.unwrap()),
            _ => assert!(result.is_err(), "{outcome}"),
        }
        assert!(
            runner.active_worker_ids().is_empty(),
            "worker state leaked on {outcome}"
        );
        let events = recording.workers.lock().unwrap().clone();
        assert_eq!(events.len(), 4, "{outcome}");
        assert_eq!(events[0].kind, WorkerLifecycleKind::Started);
        assert_eq!(events[1].kind, WorkerLifecycleKind::Started);
        assert_eq!(events[2].kind, WorkerLifecycleKind::Stopped);
        assert_eq!(events[3].kind, WorkerLifecycleKind::Stopped);
        assert_eq!(events[0].context, events[3].context);
        assert_eq!(events[1].context, events[2].context);
        assert_eq!(
            events[1].context.parent_runner_id.as_ref(),
            Some(&events[0].context.runner_id)
        );
        for event in &events {
            assert_eq!(event.context.app_id.as_ref(), "worker-lifecycle");
            let stored = backend
                .get_runner_context(event.context.runner_id.as_str())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.pid, event.context.process_id);
            assert_eq!(stored.hostname, event.context.hostname);
            assert_eq!(stored.thread_id, event.context.thread_id);
            assert_eq!(stored.runner_language, event.context.runner_language);
            assert_eq!(stored.executor_kind, event.context.executor_kind);
        }
        for event in recording.tasks.lock().unwrap().iter().skip(1) {
            assert_eq!(event.context.worker, events[1].context);
        }
    }
}
