//! Shared test infrastructure for rustvello-monitoring integration tests.
//!
//! Provides helpers to:
//! - Build in-memory backends and seed test data
//! - Start the monitoring server on a free port
//! - Make HTTP requests against the running server
//! - Keep the server alive for browser debugging (`KEEP_ALIVE`)
//!
//! # Browser debugging
//!
//! Set `KEEP_ALIVE = true` in your test module (or the env var
//! `RUSTVELLO_MONITOR_KEEP_ALIVE=1`) to keep the server running after
//! tests complete. Open the printed URL in your browser and press
//! Ctrl-C when done.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use rustvello::prelude::*;
use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_core::context::get_or_create_runner_context;
use rustvello_core::error::RustvelloResult;
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_mem::broker::MemBroker;
use rustvello_mem::client_data_store::MemClientDataStore;
use rustvello_mem::orchestrator::MemOrchestrator;
use rustvello_mem::state_backend::MemStateBackend;
use rustvello_monitoring::AppInstance;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::{AppConfig, ClientDataStoreConfig, TaskConfig};
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

/// A running monitoring server with its URL and shutdown handle.
pub struct TestServer {
    /// Base URL of the running server, e.g. `http://127.0.0.1:12345`.
    pub url: String,
    /// Send on this channel to trigger server shutdown.
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    /// Join handle for the server task.
    server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Create an HTTP client pre-configured with the correct `Origin` header
    /// so that POST requests pass the CSRF middleware.
    pub fn client(&self) -> reqwest::Client {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ORIGIN,
            self.url.parse().expect("valid origin header"),
        );
        reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("reqwest client")
    }

    /// Shut down the server and wait for cleanup.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.server_handle.await;
    }

    /// Block until Ctrl-C for browser debugging, then shut down.
    pub async fn keep_alive_until_ctrlc(self) {
        eprintln!("\n🌐 Monitoring server running at: {}", self.url);
        eprintln!("   Open this URL in your browser to explore the dashboard.");
        eprintln!("   Press Ctrl-C to stop.\n");
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for Ctrl-C");
        eprintln!("\n👋 Stopping server...");
        self.shutdown().await;
    }
}

/// All the pieces needed for a test: an app (for submitting work), shared
/// backend `Arc`s (for both the monitoring server and the runner), and the
/// list of registered task IDs.
///
/// `into_runner()` consumes the `RustvelloApp`, so the backend `Arc`s are
/// cloned *before* that call so they can be passed to `AppInstance`.
pub struct TestAppSetup {
    pub app: RustvelloApp,
    pub config: AppConfig,
    pub broker: Arc<dyn rustvello_core::broker::Broker>,
    pub orchestrator: Arc<dyn rustvello_core::orchestrator::Orchestrator>,
    pub state_backend: Arc<dyn rustvello_core::state_backend::StateBackend>,
    pub client_data_store: Arc<ClientDataStoreManager>,
    pub task_ids: Vec<TaskId>,
}

/// Create a fresh in-memory app with a simple `process_order` task registered.
pub fn create_test_app(app_id: &str) -> TestAppSetup {
    create_test_app_with_config(AppConfig::new(app_id))
}

/// Create a fresh in-memory app with a custom config.
pub fn create_test_app_with_config(config: AppConfig) -> TestAppSetup {
    let broker: Arc<dyn rustvello_core::broker::Broker> = Arc::new(MemBroker::new());
    let orchestrator: Arc<dyn rustvello_core::orchestrator::Orchestrator> =
        Arc::new(MemOrchestrator::new());
    let state_backend: Arc<dyn rustvello_core::state_backend::StateBackend> =
        Arc::new(MemStateBackend::new());
    let cds: Arc<dyn ClientDataStore> = Arc::new(MemClientDataStore::new());
    let client_data_store = Arc::new(ClientDataStoreManager::new(
        cds,
        ClientDataStoreConfig::default(),
    ));

    let mut app = RustvelloApp::with_backends(
        config.clone(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&client_data_store),
    );

    // Register a simple task
    let task_id = TaskId::new("test", "process_order");
    app.register_task(
        task_id.clone(),
        TaskConfig::default(),
        Arc::new(|args_json: String| {
            let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| {
                rustvello_core::error::RustvelloError::Serialization {
                    message: e.to_string(),
                }
            })?;
            Ok(format!("processed: {args}"))
        }),
    )
    .expect("task registration should succeed");

    let task_ids = app.task_registry.task_ids().into_iter().cloned().collect();

    TestAppSetup {
        app,
        config,
        broker,
        orchestrator,
        state_backend,
        client_data_store,
        task_ids,
    }
}

/// Seed `count` invocations for the `test::process_order` task.
pub async fn seed_invocations(app: &RustvelloApp, count: usize) -> RustvelloResult<Vec<String>> {
    let task_id = TaskId::new("test", "process_order");
    let mut ids = Vec::with_capacity(count);
    for i in 0..count {
        let mut args = SerializedArguments::new();
        args.insert("order_id", format!("ORD-{i:04}"));
        let inv_id = app.submit(&task_id, args).await?;
        ids.push(inv_id.to_string());
    }
    Ok(ids)
}

/// Initialise `tracing_subscriber` once (subsequent calls are no-ops).
///
/// Uses the unified `RustvelloFormatter` via `init_logging` so that test
/// output matches the production `[RUST]` log format.
pub fn init_tracing() {
    rustvello::logging::init_logging(&rustvello::logging::LogConfig::default());
}

/// Start the monitoring server on a free port.
///
/// Binds to `127.0.0.1:0` so the OS assigns a random free port — this ensures
/// tests never collide, mirroring pynmon's `get_free_port()` strategy.
pub async fn start_test_server(setup: TestAppSetup) -> TestServer {
    init_tracing();
    let instance = AppInstance {
        app_id: setup.config.app_id.clone(),
        config: setup.config.clone(),
        broker: setup.broker,
        orchestrator: setup.orchestrator,
        state_backend: setup.state_backend,
        client_data_store: setup.client_data_store,
        task_ids: setup.task_ids,
    };

    let app_id = instance.app_id.clone();
    let mut apps = HashMap::new();
    apps.insert(app_id.clone(), instance);

    let state = rustvello_monitoring::state::AppState::new(apps, &app_id).expect("state creation");
    let router = rustvello_monitoring::server::build_router(state);

    // Bind to port 0 — the OS picks a free port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to free port");
    let addr: SocketAddr = listener.local_addr().expect("local addr");

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server should run cleanly");
    });

    let url = format!("http://127.0.0.1:{}", addr.port());

    // Wait for server to be ready
    let client = reqwest::Client::new();
    for attempt in 0..20 {
        match client.get(format!("{url}/health")).send().await {
            Ok(resp) if resp.status().is_success() => break,
            _ => {
                if attempt == 19 {
                    panic!("monitoring server failed to start at {url}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    eprintln!("✅ Monitoring server started at: {url}");

    TestServer {
        url,
        shutdown_tx,
        server_handle,
    }
}

/// Check whether the test should keep the server alive for debugging.
///
/// Returns `true` if the env var `RUSTVELLO_MONITOR_KEEP_ALIVE` is set to `1`,
/// `true`, or `yes` (case-insensitive).
pub fn should_keep_alive(module_keep_alive: bool) -> bool {
    if module_keep_alive {
        return true;
    }
    matches!(
        std::env::var("RUSTVELLO_MONITOR_KEEP_ALIVE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

// ---------------------------------------------------------------------------
// Hierarchical test setup — grandparent → parent → child tasks
// ---------------------------------------------------------------------------

/// Register hierarchical tasks that submit children during execution.
///
/// - `grandparent_task`: when executed, submits `num_children` parent tasks
/// - `parent_task`: when executed, submits `num_children` child tasks
/// - `child_task`: leaf task, no children
///
/// The task closures capture backend Arcs so they can call `submit_with_parent`
/// from within `spawn_blocking` / rayon threads using `Handle::block_on`.
/// Because the runner sets `THREAD_INVOCATION_CTX` + `THREAD_RUNNER_CTX` before
/// execution, `get_invocation_context()` and `get_or_create_runner_context()`
/// return the correct worker context, so child Registered entries show the
/// parent's worker — not ExternalRunner.
pub fn register_hierarchical_tasks(
    app: &mut RustvelloApp,
    orchestrator: &Arc<dyn rustvello_core::orchestrator::Orchestrator>,
    state_backend: &Arc<dyn rustvello_core::state_backend::StateBackend>,
    broker: &Arc<dyn rustvello_core::broker::Broker>,
) {
    use rustvello_core::context::get_invocation_context;

    /// Run an async future from any thread context:
    /// - tokio worker thread → `block_in_place` + `Handle::block_on`
    /// - rayon / other thread → fresh `current_thread` runtime
    fn block_on_anywhere<F: std::future::Future>(f: F) -> F::Output {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(f)),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to create temporary runtime");
                rt.block_on(f)
            }
        }
    }

    // grandparent_task — submits parent tasks when executed
    {
        let orch = Arc::clone(orchestrator);
        let sb = Arc::clone(state_backend);
        let br = Arc::clone(broker);
        app.register_task(
            TaskId::new("test", "grandparent_task"),
            TaskConfig::default(),
            Arc::new(move |args_json: String| {
                let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| {
                    rustvello_core::error::RustvelloError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                let family_id = args["family_id"].as_str().unwrap_or("unknown").to_string();
                let num_children: usize = args["num_children"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let inv_ctx =
                    get_invocation_context().expect("grandparent_task must run inside a runner");
                let gp_id = inv_ctx.invocation_id.clone();
                let workflow = inv_ctx.workflow.clone();
                let parent_tid = TaskId::new("test", "parent_task");

                for pi in 0..num_children {
                    let mut pargs = SerializedArguments::new();
                    pargs.insert("family_id", format!("{family_id}-parent-{pi}"));
                    pargs.insert("num_children", num_children.to_string());

                    let parent_workflow = WorkflowIdentity::child(
                        workflow.workflow_id.clone(),
                        workflow.workflow_type.clone(),
                        gp_id.clone(),
                        1,
                    );

                    block_on_anywhere(submit_with_parent(
                        &orch,
                        &sb,
                        &br,
                        &parent_tid,
                        pargs,
                        &gp_id,
                        parent_workflow,
                    ))
                    .map_err(|e| {
                        rustvello_core::error::RustvelloError::Internal {
                            message: e.to_string(),
                        }
                    })?;
                }

                std::thread::sleep(std::time::Duration::from_millis(5));
                Ok(format!("grandparent_task done: {args}"))
            }),
        )
        .expect("task registration should succeed");
    }

    // parent_task — submits child tasks when executed
    {
        let orch = Arc::clone(orchestrator);
        let sb = Arc::clone(state_backend);
        let br = Arc::clone(broker);
        app.register_task(
            TaskId::new("test", "parent_task"),
            TaskConfig::default(),
            Arc::new(move |args_json: String| {
                let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| {
                    rustvello_core::error::RustvelloError::Serialization {
                        message: e.to_string(),
                    }
                })?;
                let family_id = args["family_id"].as_str().unwrap_or("unknown").to_string();
                let num_children: usize = args["num_children"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let inv_ctx =
                    get_invocation_context().expect("parent_task must run inside a runner");
                let p_id = inv_ctx.invocation_id.clone();
                let workflow = inv_ctx.workflow.clone();
                let child_tid = TaskId::new("test", "child_task");

                for ci in 0..num_children {
                    let mut cargs = SerializedArguments::new();
                    cargs.insert("family_id", format!("{family_id}-child-{ci}"));

                    let child_workflow = WorkflowIdentity::child(
                        workflow.workflow_id.clone(),
                        workflow.workflow_type.clone(),
                        p_id.clone(),
                        2,
                    );

                    block_on_anywhere(submit_with_parent(
                        &orch,
                        &sb,
                        &br,
                        &child_tid,
                        cargs,
                        &p_id,
                        child_workflow,
                    ))
                    .map_err(|e| {
                        rustvello_core::error::RustvelloError::Internal {
                            message: e.to_string(),
                        }
                    })?;
                }

                std::thread::sleep(std::time::Duration::from_millis(5));
                Ok(format!("parent_task done: {args}"))
            }),
        )
        .expect("task registration should succeed");
    }

    // child_task — leaf task, no children
    app.register_task(
        TaskId::new("test", "child_task"),
        TaskConfig::default(),
        Arc::new(|args_json: String| {
            let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| {
                rustvello_core::error::RustvelloError::Serialization {
                    message: e.to_string(),
                }
            })?;
            std::thread::sleep(std::time::Duration::from_millis(5));
            Ok(format!("child_task done: {args}"))
        }),
    )
    .expect("task registration should succeed");
}

/// Create an app with three hierarchical tasks registered:
/// `test::grandparent_task`, `test::parent_task`, `test::child_task`.
///
/// The grandparent and parent tasks submit their children during execution
/// (inside the runner worker), so child Registered entries capture the
/// correct worker runner context.
pub fn create_hierarchical_test_app(app_id: &str) -> TestAppSetup {
    let config = AppConfig::new(app_id);
    let broker: Arc<dyn rustvello_core::broker::Broker> = Arc::new(MemBroker::new());
    let orchestrator: Arc<dyn rustvello_core::orchestrator::Orchestrator> =
        Arc::new(MemOrchestrator::new());
    let state_backend: Arc<dyn rustvello_core::state_backend::StateBackend> =
        Arc::new(MemStateBackend::new());
    let cds: Arc<dyn ClientDataStore> = Arc::new(MemClientDataStore::new());
    let client_data_store = Arc::new(ClientDataStoreManager::new(
        cds,
        ClientDataStoreConfig::default(),
    ));

    let mut app = RustvelloApp::with_backends(
        config.clone(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&client_data_store),
    );

    register_hierarchical_tasks(&mut app, &orchestrator, &state_backend, &broker);

    let task_ids = app.task_registry.task_ids().into_iter().cloned().collect();

    TestAppSetup {
        app,
        config,
        broker,
        orchestrator,
        state_backend,
        client_data_store,
        task_ids,
    }
}

/// Submit an invocation with an explicit parent-child relationship.
///
/// This bypasses `resolve_workflow` (which needs task-local context)
/// and directly constructs the invocation with `parent_invocation_id`
/// and `WorkflowIdentity` set.
pub async fn submit_with_parent(
    orchestrator: &Arc<dyn rustvello_core::orchestrator::Orchestrator>,
    state_backend: &Arc<dyn rustvello_core::state_backend::StateBackend>,
    broker: &Arc<dyn rustvello_core::broker::Broker>,
    task_id: &TaskId,
    args: SerializedArguments,
    parent_invocation_id: &InvocationId,
    workflow: WorkflowIdentity,
) -> RustvelloResult<InvocationId> {
    let call = CallDTO::new(task_id.clone(), args);
    let invocation_id = orchestrator.register_invocation(&call).await?;

    let inv_dto = InvocationDTO::with_workflow(
        invocation_id.clone(),
        task_id.clone(),
        call.call_id.clone(),
        Some(parent_invocation_id.clone()),
        workflow,
    );
    state_backend.upsert_invocation(&inv_dto, &call).await?;

    // Always capture the caller's runner identity — never None.
    // This mirrors app.submit(): outside a runner we get ExternalRunner context.
    let caller_ctx = get_or_create_runner_context();
    let stored = StoredRunnerContext::from_runtime(&caller_ctx);
    state_backend.store_runner_context(&stored).await?;
    let caller_runner_id = caller_ctx.runner_id.clone();

    // Record Registered history entry with runner context
    state_backend
        .add_history(
            &InvocationHistory::new(
                invocation_id.clone(),
                InvocationStatusRecord::new(
                    InvocationStatus::Registered,
                    Some(caller_runner_id.clone()),
                ),
                None,
            )
            .with_runner(caller_runner_id),
        )
        .await?;

    broker
        .route_invocation_for_task(&invocation_id, task_id)
        .await?;

    Ok(invocation_id)
}

/// Seed only grandparent invocations for runner-based tests.
///
/// Only grandparents are submitted externally — parent and child tasks
/// are created by the running grandparent/parent task bodies inside the
/// runner workers. This ensures child Registered entries capture the
/// correct worker runner context (not ExternalRunner).
///
/// Returns the grandparent invocation IDs.
#[allow(dead_code)]
pub async fn seed_grandparents_only(app: &RustvelloApp) -> RustvelloResult<Vec<InvocationId>> {
    let families: Vec<(&str, usize)> = vec![
        ("familyA", 2),
        ("familyB", 3),
        ("familyC", 4),
        ("familyD", 1),
        ("familyE", 2),
    ];

    let grandparent_tid = TaskId::new("test", "grandparent_task");
    let mut grandparent_ids = Vec::new();

    for (family, num_children) in &families {
        let mut args = SerializedArguments::new();
        args.insert("family_id", family.to_string());
        args.insert("num_children", num_children.to_string());
        let gp_id = app.submit(&grandparent_tid, args).await?;
        grandparent_ids.push(gp_id);
    }

    Ok(grandparent_ids)
}

/// Seed a full grandparent → parent → child hierarchy.
///
/// Pre-seeds the entire hierarchy externally (without runners). Useful for
/// rendering/dashboard tests that need the full data without starting runners.
///
/// Returns all invocation IDs grouped by level:
/// `(grandparent_ids, parent_ids, child_ids)`.
#[allow(dead_code)]
pub async fn seed_hierarchical_invocations(
    app: &RustvelloApp,
    orchestrator: &Arc<dyn rustvello_core::orchestrator::Orchestrator>,
    state_backend: &Arc<dyn rustvello_core::state_backend::StateBackend>,
    broker: &Arc<dyn rustvello_core::broker::Broker>,
) -> RustvelloResult<(Vec<InvocationId>, Vec<InvocationId>, Vec<InvocationId>)> {
    let families: Vec<(&str, usize)> = vec![
        ("familyA", 2),
        ("familyB", 3),
        ("familyC", 4),
        ("familyD", 1),
        ("familyE", 2),
    ];

    let grandparent_tid = TaskId::new("test", "grandparent_task");
    let parent_tid = TaskId::new("test", "parent_task");
    let child_tid = TaskId::new("test", "child_task");

    let mut grandparent_ids = Vec::new();
    let mut parent_ids = Vec::new();
    let mut child_ids = Vec::new();

    for (family, num_children) in &families {
        let mut args = SerializedArguments::new();
        args.insert("family_id", family.to_string());
        args.insert("num_children", num_children.to_string());
        let gp_id = app.submit(&grandparent_tid, args).await?;

        let root_workflow = WorkflowIdentity::root(gp_id.clone(), grandparent_tid.clone());

        for pi in 0..*num_children {
            let mut pargs = SerializedArguments::new();
            pargs.insert("family_id", format!("{family}-parent-{pi}"));
            pargs.insert("num_children", num_children.to_string());

            let parent_workflow = WorkflowIdentity::child(
                root_workflow.workflow_id.clone(),
                root_workflow.workflow_type.clone(),
                gp_id.clone(),
                1,
            );

            let p_id = submit_with_parent(
                orchestrator,
                state_backend,
                broker,
                &parent_tid,
                pargs,
                &gp_id,
                parent_workflow,
            )
            .await?;

            for ci in 0..*num_children {
                let mut cargs = SerializedArguments::new();
                cargs.insert("family_id", format!("{family}-parent-{pi}-child-{ci}"));

                let child_workflow = WorkflowIdentity::child(
                    root_workflow.workflow_id.clone(),
                    root_workflow.workflow_type.clone(),
                    p_id.clone(),
                    2,
                );

                let c_id = submit_with_parent(
                    orchestrator,
                    state_backend,
                    broker,
                    &child_tid,
                    cargs,
                    &p_id,
                    child_workflow,
                )
                .await?;

                child_ids.push(c_id);
            }
            parent_ids.push(p_id);
        }
        grandparent_ids.push(gp_id);
    }

    Ok((grandparent_ids, parent_ids, child_ids))
}
