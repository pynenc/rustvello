//! Tests that all runner types properly store runner contexts
//! (main runner + per-worker) in the state backend.
//!
//! This validates the monitoring pipeline's dependency:
//! the SVG timeline needs `get_runner_context()` to return data
//! for every `runner_id` seen in invocation history entries.

#![allow(clippy::clone_on_ref_ptr)]

use std::collections::HashSet;
use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskDefinition;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::invocation::InvocationDTO;

/// Helper: create shared backends and a runner of the given type,
/// seed one invocation, call run_one(), then return (runner_id, state_backend).
struct TestHarness {
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn Orchestrator>,
    state_backend: Arc<dyn StateBackend>,
    task_id: TaskId,
}

impl TestHarness {
    fn new() -> Self {
        let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
        let orchestrator: Arc<dyn Orchestrator> =
            Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
        let state_backend: Arc<dyn StateBackend> =
            Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
        let task_id = TaskId::new("test", "echo");
        Self {
            broker,
            orchestrator,
            state_backend,
            task_id,
        }
    }

    fn task_registry(&self) -> Arc<TaskRegistry> {
        let mut registry = TaskRegistry::new();
        registry
            .register(TaskDefinition::new(
                self.task_id.clone(),
                TaskConfig::default(),
                Arc::new(|args: String| Ok(format!("echo: {args}"))),
            ))
            .unwrap();
        Arc::new(registry)
    }

    async fn seed_invocation(&self) -> rustvello_proto::identifiers::InvocationId {
        let mut args = SerializedArguments::new();
        args.insert("msg", "hello");
        let call = CallDTO::new(self.task_id.clone(), args);
        let inv_id = self.orchestrator.register_invocation(&call).await.unwrap();
        let inv_dto =
            InvocationDTO::new(inv_id.clone(), self.task_id.clone(), call.call_id.clone());
        self.state_backend
            .upsert_invocation(&inv_dto, &call)
            .await
            .unwrap();
        self.broker.route_invocation(&inv_id).await.unwrap();
        inv_id
    }

    /// After run_one(), collect all runner_ids from history and verify
    /// each has a stored RunnerContext with correct relationships.
    async fn verify_contexts(
        &self,
        runner_id_str: &str,
        inv_id: &rustvello_proto::identifiers::InvocationId,
        expected_runner_cls: &str,
        expected_worker_cls: &str,
    ) {
        // 1. Main runner context must exist
        let main_ctx = self
            .state_backend
            .get_runner_context(runner_id_str)
            .await
            .expect("get_runner_context should not error")
            .unwrap_or_else(|| panic!("Main runner context for '{runner_id_str}' must be stored"));
        assert_eq!(main_ctx.runner_cls, expected_runner_cls);
        assert_eq!(main_ctx.runner_id, runner_id_str);
        assert!(
            main_ctx.parent_runner_id.is_none(),
            "Main runner should have no parent"
        );

        // 2. Get invocation history and extract all runner_ids
        let history = self
            .state_backend
            .get_history(inv_id)
            .await
            .expect("get_history should not error");
        assert!(
            !history.is_empty(),
            "Invocation should have history entries"
        );

        let worker_ids: HashSet<String> = history
            .iter()
            .filter_map(|h| h.runner_id.as_ref())
            .map(std::string::ToString::to_string)
            .collect();

        assert!(
            !worker_ids.is_empty(),
            "At least one history entry should have a runner_id"
        );

        // 3. Every runner_id in history must have a stored context
        for worker_id in &worker_ids {
            let ctx = self
                .state_backend
                .get_runner_context(worker_id)
                .await
                .expect("get_runner_context should not error")
                .unwrap_or_else(|| {
                    panic!("Worker runner context for '{worker_id}' must be stored")
                });

            assert_eq!(
                ctx.runner_cls, expected_worker_cls,
                "Worker '{worker_id}' should have class '{expected_worker_cls}', got '{}'",
                ctx.runner_cls
            );
            assert_eq!(
                ctx.parent_runner_id.as_deref(),
                Some(runner_id_str),
                "Worker '{worker_id}' should reference parent '{runner_id_str}'"
            );
            assert_eq!(
                ctx.parent_runner_cls.as_deref(),
                Some(expected_runner_cls),
                "Worker '{worker_id}' parent_cls should be '{expected_runner_cls}'"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// PersistentTokioRunner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persistent_tokio_runner_stores_all_contexts() {
    let h = TestHarness::new();
    let inv_id = h.seed_invocation().await;

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        h.broker.clone(),
        h.orchestrator.clone(),
        h.state_backend.clone(),
        h.task_registry(),
        None,
    );
    let runner_id_str = runner.runner_id().to_string();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    h.verify_contexts(
        &runner_id_str,
        &inv_id,
        "PersistentTokioRunner",
        "PersistentTokioWorker",
    )
    .await;
}

// ---------------------------------------------------------------------------
// PerInvocationTokioRunner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn per_invocation_runner_stores_all_contexts() {
    let h = TestHarness::new();
    let inv_id = h.seed_invocation().await;

    let runner = PerInvocationTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        h.broker.clone(),
        h.orchestrator.clone(),
        h.state_backend.clone(),
        h.task_registry(),
    );
    let runner_id_str = runner.runner_id().to_string();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    h.verify_contexts(
        &runner_id_str,
        &inv_id,
        "PerInvocationTokioRunner",
        "PerInvocationWorker",
    )
    .await;
}

// ---------------------------------------------------------------------------
// RayonRunner
// ---------------------------------------------------------------------------

#[cfg(feature = "rayon")]
#[tokio::test]
async fn rayon_runner_stores_all_contexts() {
    let h = TestHarness::new();
    let inv_id = h.seed_invocation().await;

    let runner = RayonRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        h.broker.clone(),
        h.orchestrator.clone(),
        h.state_backend.clone(),
        h.task_registry(),
    )
    .expect("test: failed to build RayonRunner");
    let runner_id_str = runner.runner_id().to_string();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    h.verify_contexts(&runner_id_str, &inv_id, "RayonRunner", "RayonWorker")
        .await;
}

// ---------------------------------------------------------------------------
// SpawnBlockingRunner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn process_runner_stores_all_contexts() {
    let h = TestHarness::new();
    let inv_id = h.seed_invocation().await;

    let runner = SpawnBlockingRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        h.broker.clone(),
        h.orchestrator.clone(),
        h.state_backend.clone(),
        h.task_registry(),
    );
    let runner_id_str = runner.runner_id().to_string();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    h.verify_contexts(
        &runner_id_str,
        &inv_id,
        "SpawnBlockingRunner",
        "ProcessWorker",
    )
    .await;
}
