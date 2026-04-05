//! Tests that runner span context is preserved in tracing output
//! when invocations are processed via the `run()` path (JoinSet::spawn).
//!
//! The PersistentTokioRunner, PerInvocationTokioRunner, and RayonRunner
//! all spawn tasks via JoinSet. This test verifies that the runner span
//! remains as a parent of the worker/invocation spans in the log output.

#![allow(clippy::clone_on_ref_ptr)]

use std::sync::{Arc, Mutex};

use rustvello::prelude::*;
use rustvello_core::broker::Broker;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskDefinition;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::invocation::InvocationDTO;

/// A tracing writer that captures output into a shared buffer.
#[derive(Clone)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for BufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
    type Writer = BufWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

struct TestSetup {
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn Orchestrator>,
    state_backend: Arc<dyn StateBackend>,
    task_id: TaskId,
    registry: Arc<TaskRegistry>,
}

impl TestSetup {
    fn new() -> Self {
        let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
        let orchestrator: Arc<dyn Orchestrator> =
            Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
        let state_backend: Arc<dyn StateBackend> =
            Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
        let task_id = TaskId::new("test", "echo");

        let mut registry = TaskRegistry::new();
        registry
            .register(TaskDefinition::new(
                task_id.clone(),
                TaskConfig::default(),
                Arc::new(|args: String| Ok(format!("echo: {args}"))),
            ))
            .unwrap();

        Self {
            broker,
            orchestrator,
            state_backend,
            task_id,
            registry: Arc::new(registry),
        }
    }

    async fn seed_invocation(&self) {
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
    }
}

/// Set up a captured tracing subscriber and return (guard, buffer).
fn setup_captured_tracing() -> (tracing::subscriber::DefaultGuard, BufWriter) {
    let buf = BufWriter(Arc::new(Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf.clone())
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, buf)
}

fn captured_output(buf: &BufWriter) -> String {
    let bytes = buf.0.lock().unwrap().clone();
    String::from_utf8(bytes).unwrap_or_default()
}

/// In current_thread mode, set_default covers all spawned tasks since
/// they all execute on the same thread.
#[tokio::test(flavor = "current_thread")]
async fn persistent_tokio_runner_preserves_runner_span_in_logs() {
    let setup = TestSetup::new();
    setup.seed_invocation().await;

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        setup.broker.clone(),
        setup.orchestrator.clone(),
        setup.state_backend.clone(),
        setup.registry.clone(),
        None,
    );

    let (_guard, buf) = setup_captured_tracing();
    let shutdown = async { tokio::time::sleep(std::time::Duration::from_millis(1000)).await };
    let _ = runner.with_graceful_shutdown(shutdown).await;
    let output = captured_output(&buf);

    // The log should show runner{...}:worker{...}:invocation{...} hierarchy
    assert!(
        output.contains("runner{") && output.contains("}:worker{"),
        "Log output should show runner span as parent of worker span.\nCaptured output:\n{output}"
    );
    assert!(
        output.contains("}:invocation{"),
        "Log output should show invocation span nested under worker span.\nCaptured output:\n{output}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn per_invocation_runner_preserves_runner_span_in_logs() {
    let setup = TestSetup::new();
    setup.seed_invocation().await;

    let runner = PerInvocationTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        setup.broker.clone(),
        setup.orchestrator.clone(),
        setup.state_backend.clone(),
        setup.registry.clone(),
    );

    let (_guard, buf) = setup_captured_tracing();
    let shutdown = async { tokio::time::sleep(std::time::Duration::from_millis(1000)).await };
    let _ = runner.with_graceful_shutdown(shutdown).await;
    let output = captured_output(&buf);

    assert!(
        output.contains("runner{") && output.contains("}:worker{"),
        "Log output should show runner span as parent of worker span.\nCaptured output:\n{output}"
    );
}

#[cfg(feature = "rayon")]
#[tokio::test(flavor = "current_thread")]
async fn rayon_runner_preserves_runner_span_in_logs() {
    let setup = TestSetup::new();
    setup.seed_invocation().await;

    let runner = RayonRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        setup.broker.clone(),
        setup.orchestrator.clone(),
        setup.state_backend.clone(),
        setup.registry.clone(),
    )
    .expect("test: failed to build RayonRunner");

    let (_guard, buf) = setup_captured_tracing();
    let shutdown = async { tokio::time::sleep(std::time::Duration::from_millis(1000)).await };
    let _ = runner.with_graceful_shutdown(shutdown).await;
    let output = captured_output(&buf);

    assert!(
        output.contains("runner{") && output.contains("}:worker{"),
        "Log output should show runner span as parent of worker span.\nCaptured output:\n{output}"
    );
}
