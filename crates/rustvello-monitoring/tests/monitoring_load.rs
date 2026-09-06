//! Monitoring load fixture for cross-language Rust/Python-style workflows.
//!
//! This test is ignored by default because its purpose is exploratory: it
//! creates a realistic dashboard dataset and keeps the monitoring server open
//! when `KEEP_ALIVE=1` is set.

#![allow(dead_code)]

mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use common::{should_keep_alive, start_test_server, TestAppSetup, TestServer};
use rustvello::prelude::*;
use rustvello_core::client_data_store::{ClientDataStore, ClientDataStoreManager};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StoredRunnerContext;
use rustvello_core::task::{TaskDefinition, TaskFn, TaskRegistry};
use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_mem::broker::MemBroker;
use rustvello_mem::client_data_store::MemClientDataStore;
use rustvello_mem::orchestrator::MemOrchestrator;
use rustvello_mem::state_backend::MemStateBackend;
use rustvello_mem::trigger::MemTriggerStore;
use rustvello_proto::config::{AppConfig, ClientDataStoreConfig, TaskConfig};
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};
use rustvello_proto::status::InvocationStatus;

const KEEP_ALIVE: bool = false;
const APP_ID: &str = "monitoring-cross-language-load";
const MODULE: &str = "monitoring_load";
const DIRECT_BATCHES: u32 = 96;
const TRIGGER_VARIANTS: u32 = 24;
const INITIAL_RUST_EVENTS: u32 = 18;
const INITIAL_PYTHON_EVENTS: u32 = 15;
const LIVE_RUST_EVENTS: u32 = 12;
const LIVE_PYTHON_EVENTS: u32 = 10;
const EXPECTED_TERMINAL_INVOCATIONS: usize = 650;
const MIN_RUN_TIME: Duration = Duration::from_secs(12);

#[derive(serde::Serialize, serde::Deserialize)]
struct LoadParams {
    batch: u32,
    fanout: u32,
}
impl CrossLanguageSafe for LoadParams {}

fn rust_id(name: &str) -> TaskId {
    TaskId::for_language(TaskLanguage::Rust, MODULE, name)
}

fn python_id(name: &str) -> TaskId {
    TaskId::for_language(TaskLanguage::Python, MODULE, name)
}

fn config(queue: &str, blocking: bool, workflow: bool) -> TaskConfig {
    let mut config = TaskConfig::default();
    config.queue = queue.to_owned();
    config.blocking = blocking;
    config.is_workflow_task = workflow;
    config
}

fn args(batch: u32, fanout: u32) -> rustvello_proto::call::SerializedArguments {
    let mut args = rustvello_proto::call::SerializedArguments::new();
    args.insert("batch", batch.to_string());
    args.insert("fanout", fanout.to_string());
    args
}

fn arg_u32(args_json: &str, name: &str) -> u32 {
    let args: BTreeMap<String, String> = serde_json::from_str(args_json).unwrap();
    serde_json::from_str(args.get(name).unwrap()).unwrap()
}

fn block_on_anywhere<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("temporary runtime");
            runtime.block_on(future)
        }
    }
}

fn task_definition(task_id: TaskId, config: TaskConfig, func: TaskFn) -> TaskDefinition {
    TaskDefinition::new(task_id, config, func)
}

fn registry(
    definitions: Vec<TaskDefinition>,
    foreign_definitions: Vec<(TaskId, TaskConfig)>,
) -> Arc<TaskRegistry> {
    Arc::new({
        let mut registry = TaskRegistry::new();
        for definition in definitions {
            registry.register(definition).unwrap();
        }
        for (task_id, config) in foreign_definitions {
            registry
                .register_foreign(
                    ForeignTaskProxy::<LoadParams, String>::new(task_id).with_config(config),
                )
                .unwrap();
        }
        registry
    })
}

async fn handle_keep_alive(server: TestServer) {
    if should_keep_alive(KEEP_ALIVE) {
        server.keep_alive_until_ctrlc().await;
    } else {
        server.shutdown().await;
    }
}

fn runner_config(base: &AppConfig, queues: &[&str]) -> AppConfig {
    let mut config = base.clone();
    config.runner_queues = queues.iter().map(|queue| (*queue).to_owned()).collect();
    config
}

#[allow(clippy::too_many_lines)]
fn build_load_setup() -> (TestAppSetup, Vec<Arc<dyn Runner>>) {
    common::init_tracing();

    let broker: Arc<dyn rustvello_core::broker::Broker> = Arc::new(MemBroker::new());
    let orchestrator: Arc<dyn rustvello_core::orchestrator::InvocationControlBackend> =
        Arc::new(MemOrchestrator::new());
    let state_backend: Arc<dyn rustvello_core::state_backend::StateBackend> =
        Arc::new(MemStateBackend::new());
    let client_data_store: Arc<ClientDataStoreManager> = Arc::new(ClientDataStoreManager::new(
        Arc::new(MemClientDataStore::new()) as Arc<dyn ClientDataStore>,
        ClientDataStoreConfig::default(),
    ));
    let trigger_store: Arc<dyn TriggerStore> = Arc::new(MemTriggerStore::new());
    let trigger_manager = TriggerManager::new(Arc::clone(&trigger_store));

    let mut app_config = AppConfig::new(APP_ID);
    app_config.broker_queues = vec!["default".to_owned(), "cpu".to_owned(), "io".to_owned()];
    app_config.heartbeat_interval_seconds = 1;
    app_config.recovery_check_interval_seconds = 1;
    app_config.atomic_service_check_interval_minutes = 0.001;
    app_config.atomic_service_interval_minutes = 0.16;
    app_config.atomic_service_spread_margin_minutes = 0.002;

    let rust_seed = rust_id("seed_workflow");
    let rust_cpu = rust_id("cpu_step");
    let python_entry = python_id("entry_workflow");
    let python_prepare = python_id("prepare_step");
    let python_io = python_id("io_step");

    let app_cell: Arc<OnceLock<Arc<RustvelloApp>>> = Arc::new(OnceLock::new());

    let rust_cpu_func: TaskFn = Arc::new(|args_json| {
        let batch = arg_u32(&args_json, "batch");
        let fanout = arg_u32(&args_json, "fanout");
        let mut value = 0u64;
        for index in 0..(1_500_000 + fanout * 180_000) {
            value = std::hint::black_box(value.wrapping_add((index as u64) ^ batch as u64));
        }
        std::thread::sleep(Duration::from_millis(35 + u64::from(fanout % 5) * 15));
        Ok(serde_json::to_string(&format!("rust-cpu:{batch}:{value}")).unwrap())
    });

    let python_io_func: TaskFn = Arc::new(|args_json| {
        let batch = arg_u32(&args_json, "batch");
        let fanout = arg_u32(&args_json, "fanout");
        std::thread::sleep(Duration::from_millis(45 + u64::from(fanout % 6) * 15));
        Ok(serde_json::to_string(&format!("python-io:{batch}")).unwrap())
    });

    let rust_seed_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let rust_cpu = rust_cpu.clone();
        let python_prepare = python_prepare.clone();
        Arc::new(move |args_json| {
            let batch = arg_u32(&args_json, "batch");
            let fanout = arg_u32(&args_json, "fanout");
            let app = app_cell.get().expect("app initialized");
            let (cpu_id, prepare_id) = block_on_anywhere(async {
                let cpu_id = app.submit(&rust_cpu, args(batch, fanout)).await?;
                let prepare_id = app.submit(&python_prepare, args(batch, fanout)).await?;
                Ok::<_, RustvelloError>((cpu_id, prepare_id))
            })?;
            Ok(serde_json::to_string(&format!(
                "rust-root:{batch}:cpu={cpu_id}:prepare={prepare_id}"
            ))
            .unwrap())
        })
    };

    let python_prepare_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let rust_cpu = rust_cpu.clone();
        Arc::new(move |args_json| {
            let batch = arg_u32(&args_json, "batch");
            let fanout = arg_u32(&args_json, "fanout");
            let app = app_cell.get().expect("app initialized");
            let child_id = block_on_anywhere(app.submit(&rust_cpu, args(batch + 10_000, fanout)))?;
            Ok(serde_json::to_string(&format!("python-prepare:{batch}:cpu={child_id}")).unwrap())
        })
    };

    let python_entry_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let rust_cpu = rust_cpu.clone();
        let python_io = python_io.clone();
        Arc::new(move |args_json| {
            let batch = arg_u32(&args_json, "batch");
            let fanout = arg_u32(&args_json, "fanout");
            let app = app_cell.get().expect("app initialized");
            let (cpu_id, io_id) = block_on_anywhere(async {
                let cpu_id = app.submit(&rust_cpu, args(batch + 20_000, fanout)).await?;
                let io_id = app.submit(&python_io, args(batch, fanout)).await?;
                Ok::<_, RustvelloError>((cpu_id, io_id))
            })?;
            Ok(
                serde_json::to_string(&format!("python-root:{batch}:cpu={cpu_id}:io={io_id}"))
                    .unwrap(),
            )
        })
    };

    let rust_seed_config = config("default", true, true);
    let rust_cpu_config = config("cpu", false, false);
    let python_entry_config = config("default", true, true);
    let python_prepare_config = config("io", true, false);
    let python_io_config = config("io", true, false);

    let mut app = RustvelloApp::with_backends(
        app_config.clone(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&client_data_store),
    );
    app.set_trigger_manager(trigger_manager.clone());
    app.register_task(
        rust_seed.clone(),
        rust_seed_config.clone(),
        Arc::clone(&rust_seed_func),
    )
    .unwrap();
    app.register_task(
        rust_cpu.clone(),
        rust_cpu_config.clone(),
        Arc::clone(&rust_cpu_func),
    )
    .unwrap();
    app.register_foreign(
        ForeignTaskProxy::<LoadParams, String>::new(python_entry.clone())
            .with_config(python_entry_config.clone()),
    )
    .unwrap();
    app.register_foreign(
        ForeignTaskProxy::<LoadParams, String>::new(python_prepare.clone())
            .with_config(python_prepare_config.clone()),
    )
    .unwrap();
    app.register_foreign(
        ForeignTaskProxy::<LoadParams, String>::new(python_io.clone())
            .with_config(python_io_config.clone()),
    )
    .unwrap();

    let mut runtime_app = RustvelloApp::with_backends(
        app_config.clone(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&client_data_store),
    );
    runtime_app.set_trigger_manager(trigger_manager.clone());
    runtime_app
        .register_task(
            rust_seed.clone(),
            rust_seed_config.clone(),
            Arc::clone(&rust_seed_func),
        )
        .unwrap();
    runtime_app
        .register_task(
            rust_cpu.clone(),
            rust_cpu_config.clone(),
            Arc::clone(&rust_cpu_func),
        )
        .unwrap();
    runtime_app
        .register_foreign(
            ForeignTaskProxy::<LoadParams, String>::new(python_entry.clone())
                .with_config(python_entry_config.clone()),
        )
        .unwrap();
    runtime_app
        .register_foreign(
            ForeignTaskProxy::<LoadParams, String>::new(python_prepare.clone())
                .with_config(python_prepare_config.clone()),
        )
        .unwrap();
    runtime_app
        .register_foreign(
            ForeignTaskProxy::<LoadParams, String>::new(python_io.clone())
                .with_config(python_io_config.clone()),
        )
        .unwrap();
    assert!(
        app_cell.set(Arc::new(runtime_app)).is_ok(),
        "runtime app should be initialized once"
    );

    let rust_registry = registry(
        vec![
            task_definition(rust_seed.clone(), rust_seed_config.clone(), rust_seed_func),
            task_definition(rust_cpu.clone(), rust_cpu_config.clone(), rust_cpu_func),
        ],
        vec![
            (python_entry.clone(), python_entry_config.clone()),
            (python_prepare.clone(), python_prepare_config.clone()),
            (python_io.clone(), python_io_config.clone()),
        ],
    );
    let python_registry = registry(
        vec![
            task_definition(
                python_entry.clone(),
                python_entry_config.clone(),
                python_entry_func,
            ),
            task_definition(
                python_prepare.clone(),
                python_prepare_config.clone(),
                Arc::clone(&python_prepare_func),
            ),
            task_definition(python_io, python_io_config, python_io_func),
        ],
        vec![(rust_seed, rust_seed_config), (rust_cpu, rust_cpu_config)],
    );

    let task_ids = app
        .task_registry()
        .task_ids()
        .into_iter()
        .cloned()
        .collect();
    let setup = TestAppSetup {
        app,
        config: app_config.clone(),
        broker: Arc::clone(&broker),
        orchestrator: Arc::clone(&orchestrator),
        state_backend: Arc::clone(&state_backend),
        trigger_store,
        client_data_store,
        task_ids,
    };
    let mut runners: Vec<Arc<dyn Runner>> = Vec::new();
    for workers in [4, 3] {
        runners.push(Arc::new(
            PersistentTokioRunner::new(
                APP_ID.to_owned(),
                runner_config(&app_config, &["default"]),
                Arc::clone(&broker),
                Arc::clone(&orchestrator),
                Arc::clone(&state_backend),
                Arc::clone(&rust_registry),
                Some(trigger_manager.clone()),
            )
            .with_num_workers(workers)
            .with_idle_sleep(10),
        ));
    }
    for threads in [6, 4] {
        runners.push(Arc::new(
            RayonRunner::new(
                APP_ID.to_owned(),
                runner_config(&app_config, &["cpu"]),
                Arc::clone(&broker),
                Arc::clone(&orchestrator),
                Arc::clone(&state_backend),
                Arc::clone(&rust_registry),
            )
            .expect("rayon runner")
            .with_num_threads(threads)
            .expect("rayon threads"),
        ));
    }
    for workers in [4, 3] {
        runners.push(Arc::new(
            PersistentTokioRunner::new_python(
                APP_ID.to_owned(),
                runner_config(&app_config, &["default"]),
                Arc::clone(&broker),
                Arc::clone(&orchestrator),
                Arc::clone(&state_backend),
                Arc::clone(&python_registry),
                Some(trigger_manager.clone()),
            )
            .with_num_workers(workers)
            .with_idle_sleep(10),
        ));
    }
    for workers in [8, 6] {
        runners.push(Arc::new(
            PersistentTokioRunner::new_python(
                APP_ID.to_owned(),
                runner_config(&app_config, &["io"]),
                Arc::clone(&broker),
                Arc::clone(&orchestrator),
                Arc::clone(&state_backend),
                Arc::clone(&python_registry),
                Some(trigger_manager.clone()),
            )
            .with_num_workers(workers)
            .with_idle_sleep(10),
        ));
    }
    (setup, runners)
}

async fn populate_work(setup: &TestAppSetup) -> RustvelloResult<Vec<InvocationId>> {
    let rust_seed = rust_id("seed_workflow");
    let python_entry = python_id("entry_workflow");

    for variant in 0..TRIGGER_VARIANTS {
        TriggerBuilder::new()
            .on_event("load.rust.batch")
            .with_static_args(serde_json::json!({"batch": 9000 + variant, "fanout": 4 + variant}))
            .build_and_register(&rust_seed, &setup.trigger_store)
            .await?;
        TriggerBuilder::new()
            .on_event("load.python.batch")
            .with_static_args(serde_json::json!({"batch": 9100 + variant, "fanout": 3 + variant}))
            .build_and_register(&python_entry, &setup.trigger_store)
            .await?;
    }

    let trigger_manager = TriggerManager::new(Arc::clone(&setup.trigger_store));
    for index in 0..INITIAL_RUST_EVENTS {
        trigger_manager
            .emit_event("load.rust.batch", serde_json::json!({ "index": index }))
            .await?;
    }
    for index in 0..INITIAL_PYTHON_EVENTS {
        trigger_manager
            .emit_event("load.python.batch", serde_json::json!({ "index": index }))
            .await?;
    }

    // Trigger evaluation runs in this same external process. It should be
    // represented by the normal external context, not as a made-up second
    // runner beside direct submissions from the same process.
    let trigger_runner_id = rustvello_core::context::get_or_create_runner_id();
    setup
        .state_backend
        .store_runner_context(&StoredRunnerContext::current_with_runtime(
            trigger_runner_id.to_string(),
            "ExternalRunner",
            TaskLanguage::Rust,
            rustvello_proto::identifiers::ExecutorKind::Tokio,
        ))
        .await?;
    let mut roots = setup.app.trigger_loop_iteration(&trigger_runner_id).await?;

    for batch in 0..DIRECT_BATCHES {
        roots.push(
            setup
                .app
                .submit(&rust_seed, args(batch, 4 + batch % 3))
                .await?,
        );
        roots.push(
            setup
                .app
                .submit(&python_entry, args(batch + 100, 3 + batch % 4))
                .await?,
        );
    }

    Ok(roots)
}

async fn emit_live_trigger_events(setup: &TestAppSetup) -> RustvelloResult<()> {
    let trigger_manager = TriggerManager::new(Arc::clone(&setup.trigger_store));
    for index in 0..LIVE_RUST_EVENTS {
        trigger_manager
            .emit_event(
                "load.rust.batch",
                serde_json::json!({ "live_index": index }),
            )
            .await?;
    }
    for index in 0..LIVE_PYTHON_EVENTS {
        trigger_manager
            .emit_event(
                "load.python.batch",
                serde_json::json!({ "live_index": index }),
            )
            .await?;
    }
    Ok(())
}

async fn wait_for_dataset(
    orchestrator: &Arc<dyn rustvello_core::orchestrator::InvocationControlBackend>,
    expected_minimum: usize,
) -> RustvelloResult<usize> {
    let terminal_statuses = [
        InvocationStatus::Success,
        InvocationStatus::Failed,
        InvocationStatus::ConcurrencyControlledFinal,
    ];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let total = orchestrator.count_invocations(None, None).await?;
        let terminal = orchestrator
            .count_invocations(None, Some(&terminal_statuses))
            .await?;
        if total >= expected_minimum && terminal >= expected_minimum {
            return Ok(total);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RustvelloError::runner_err(format!(
                "load scenario timed out: {terminal}/{total} terminal, expected at least {expected_minimum}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_atomic_service(
    orchestrator: &Arc<dyn rustvello_core::orchestrator::InvocationControlBackend>,
) -> RustvelloResult<usize> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let executions = orchestrator.get_atomic_service_timeline().await?;
        if !executions.is_empty() {
            return Ok(executions.len());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RustvelloError::runner_err(
                "atomic service did not run during monitoring load fixture".to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "exploratory monitoring dataset; run via make monitoring-load"]
async fn monitoring_cross_language_load_fixture() {
    let (setup, runners) = build_load_setup();
    let roots = populate_work(&setup).await.expect("populate load work");
    assert!(
        roots.len() >= (DIRECT_BATCHES * 2) as usize,
        "direct submissions should create enough roots"
    );

    let mut handles = Vec::new();
    let run_started = tokio::time::Instant::now();
    for runner in &runners {
        let runner = Arc::clone(runner);
        handles.push(tokio::spawn(async move {
            let _ = runner.run().await;
        }));
    }
    emit_live_trigger_events(&setup)
        .await
        .expect("live trigger events should be emitted");

    let total = wait_for_dataset(&setup.orchestrator, EXPECTED_TERMINAL_INVOCATIONS)
        .await
        .expect("load fixture should create enough completed invocations");
    let atomic_count = wait_for_atomic_service(&setup.orchestrator)
        .await
        .expect("atomic service should run in the load fixture");
    let atomic_execution = setup
        .orchestrator
        .get_atomic_service_timeline()
        .await
        .expect("read recorded atomic executions")
        .into_iter()
        .next()
        .expect("at least one atomic execution was recorded");
    let elapsed = run_started.elapsed();
    if elapsed < MIN_RUN_TIME {
        tokio::time::sleep(MIN_RUN_TIME - elapsed).await;
    }
    eprintln!("Created {total} invocations across Rust and Python task languages.");
    eprintln!("Observed {atomic_count} atomic service execution(s).");
    eprintln!("Started {} runner processes/runner groups.", runners.len());

    for runner in &runners {
        runner.shutdown().await.expect("runner shutdown");
    }
    for handle in handles {
        handle.await.expect("runner task joined");
    }

    let server = start_test_server(setup).await;
    let timeline = reqwest::get(format!(
        "{}/invocations/timeline?time_range=auto&limit=50000",
        server.url
    ))
    .await
    .expect("load timeline request")
    .text()
    .await
    .expect("load timeline body");
    assert!(
        !timeline.contains("Unknown("),
        "every load runner needs provenance"
    );
    assert!(
        !timeline.contains("ExternalTriggerRunner"),
        "trigger evaluation in this process should share ExternalRunner identity"
    );
    assert!(
        timeline.contains("atomic-service-window"),
        "recorded atomic service executions should be visible"
    );
    assert!(
        timeline.contains("Origin: atomic service trigger evaluation"),
        "triggered registrations should remain on their atomic-service control-plane row"
    );
    let atomic_service = reqwest::get(format!("{}/atomic-service", server.url))
        .await
        .expect("atomic service request")
        .text()
        .await
        .expect("atomic service body");
    assert!(
        atomic_service.contains("atomic-run-track"),
        "atomic service history should render a proportional execution track"
    );
    assert!(
        atomic_service.contains("monitor-temporal-row")
            && !atomic_service.contains("Execution position"),
        "atomic service should use the shared temporal row background, not a position column"
    );
    assert!(
        atomic_service.contains("/atomic-service/execution?"),
        "each atomic execution should link to its details"
    );
    assert!(
        atomic_service.contains("/invocations/timeline?"),
        "each atomic execution should offer a focused timeline zoom"
    );
    let mut atomic_detail_query = url::form_urlencoded::Serializer::new(String::new());
    atomic_detail_query.append_pair("runner_id", &atomic_execution.runner_id);
    atomic_detail_query.append_pair("start", &atomic_execution.start.to_rfc3339());
    atomic_detail_query.append_pair("end", &atomic_execution.end.to_rfc3339());
    let atomic_detail = reqwest::get(format!(
        "{}/atomic-service/execution?{}",
        server.url,
        atomic_detail_query.finish()
    ))
    .await
    .expect("atomic execution detail request")
    .text()
    .await
    .expect("atomic execution detail body");
    assert!(
        atomic_detail.contains("Triggered Invocations"),
        "atomic execution detail should account for trigger work when available"
    );
    assert!(
        atomic_detail.contains("Timeline"),
        "atomic execution detail should link back to its focused timeline range"
    );
    eprintln!(
        "Open {}/invocations/timeline?time_range=auto for the cross-language load timeline.",
        server.url
    );
    eprintln!("Open {}/logs for Log Explorer data.", server.url);
    handle_keep_alive(server).await;
}
