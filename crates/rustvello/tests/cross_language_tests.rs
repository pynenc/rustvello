//! Cross-language workflow tests.
//!
//! These tests model two language runtimes sharing one backend. Rust tasks are
//! registered as `rust::...`, Python tasks as `python::...`, and each runner
//! only claims invocations for its own language.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use rustvello::prelude::*;
use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStoreManager;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskFn;
use rustvello_core::trigger::TriggerManager;

const APP_ID: &str = "cross-language-tests";
const MODULE: &str = "cross_language";

struct SharedBackends {
    broker: Arc<dyn Broker>,
    orchestrator: Arc<dyn InvocationControlBackend>,
    state_backend: Arc<dyn StateBackend>,
    client_data_store: Arc<ClientDataStoreManager>,
    trigger_manager: TriggerManager,
}

impl SharedBackends {
    fn new() -> Self {
        let client_data_store = Arc::new(ClientDataStoreManager::new(
            Arc::new(rustvello_mem::client_data_store::MemClientDataStore::new()),
            ClientDataStoreConfig::default(),
        ));
        let trigger_manager =
            TriggerManager::new(Arc::new(rustvello_mem::trigger::MemTriggerStore::new()));
        Self {
            broker: Arc::new(rustvello_mem::broker::MemBroker::new()),
            orchestrator: Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()),
            state_backend: Arc::new(rustvello_mem::state_backend::MemStateBackend::new()),
            client_data_store,
            trigger_manager,
        }
    }

    fn app(&self) -> RustvelloApp {
        let config = AppConfig::new(APP_ID);
        let mut app = RustvelloApp::with_backends(
            config,
            Arc::clone(&self.broker),
            Arc::clone(&self.orchestrator),
            Arc::clone(&self.state_backend),
            Arc::clone(&self.client_data_store),
        );
        app.set_trigger_manager(self.trigger_manager.clone());
        app
    }

    fn runner(&self, runner_language: TaskLanguage, registry: Arc<TaskRegistry>) -> TaskRunner {
        let config = AppConfig::new(APP_ID);
        let runner = match runner_language {
            TaskLanguage::Rust => TaskRunner::new(
                APP_ID.to_owned(),
                config,
                Arc::clone(&self.broker),
                Arc::clone(&self.orchestrator),
                Arc::clone(&self.state_backend),
                registry,
                Some(self.trigger_manager.clone()),
            ),
            TaskLanguage::Python => TaskRunner::new_python(
                APP_ID.to_owned(),
                config,
                Arc::clone(&self.broker),
                Arc::clone(&self.orchestrator),
                Arc::clone(&self.state_backend),
                registry,
                Some(self.trigger_manager.clone()),
            ),
        };
        runner.with_num_workers(1)
    }
}

fn rust_task_id(name: &str) -> TaskId {
    TaskId::new(MODULE, name)
}

fn python_task_id(name: &str) -> TaskId {
    TaskId::for_language(TaskLanguage::Python, MODULE, name)
}

fn args(items: &[(&str, &str)]) -> SerializedArguments {
    let mut args = SerializedArguments::new();
    for (key, value) in items {
        args.insert(*key, *value);
    }
    args
}

fn json_arg(args_json: &str, key: &str) -> String {
    let args: BTreeMap<String, String> = serde_json::from_str(args_json).unwrap();
    serde_json::from_str(args.get(key).unwrap()).unwrap()
}

fn task_definition(task_id: TaskId, config: TaskConfig, func: TaskFn) -> TaskDefinition {
    TaskDefinition::new(task_id, config, func)
}

fn registry(definitions: Vec<TaskDefinition>) -> Arc<TaskRegistry> {
    Arc::new({
        let mut registry = TaskRegistry::new();
        for definition in definitions {
            registry.register(definition).unwrap();
        }
        registry
    })
}

fn blocking_config() -> TaskConfig {
    let mut config = TaskConfig::default();
    config.blocking = true;
    config
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ValueParams {
    value: String,
}
impl CrossLanguageSafe for ValueParams {}

#[derive(serde::Serialize, serde::Deserialize)]
struct LabelParams {
    label: String,
}
impl CrossLanguageSafe for LabelParams {}

#[tokio::test]
async fn rust_and_python_workers_claim_only_their_language() {
    let backends = SharedBackends::new();
    let mut app = backends.app();

    let rust_id = rust_task_id("rust_echo");
    let python_id = python_task_id("python_echo");
    app.register_task(
        rust_id.clone(),
        TaskConfig::default(),
        Arc::new(|args_json| {
            Ok(serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "value"))).unwrap())
        }),
    )
    .unwrap();
    app.register_foreign(ForeignTaskProxy::<ValueParams, String>::new(
        python_id.clone(),
    ))
    .unwrap();

    let rust_runner = backends.runner(
        TaskLanguage::Rust,
        registry(vec![task_definition(
            rust_id.clone(),
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "value")))
                        .unwrap(),
                )
            }),
        )]),
    );
    let python_runner = backends.runner(
        TaskLanguage::Python,
        registry(vec![task_definition(
            python_id.clone(),
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("python:{}", json_arg(&args_json, "value")))
                        .unwrap(),
                )
            }),
        )]),
    );

    let rust_invocation = app
        .submit(&rust_id, args(&[("value", "\"one\"")]))
        .await
        .unwrap();
    let python_invocation = app
        .submit(&python_id, args(&[("value", "\"two\"")]))
        .await
        .unwrap();

    assert!(rust_runner.run_one().await.unwrap());
    assert_eq!(
        backends
            .orchestrator
            .get_invocation_status(&rust_invocation)
            .await
            .unwrap()
            .status,
        InvocationStatus::Success
    );
    assert_eq!(
        backends
            .orchestrator
            .get_invocation_status(&python_invocation)
            .await
            .unwrap()
            .status,
        InvocationStatus::Registered
    );

    assert!(python_runner.run_one().await.unwrap());
    assert_eq!(
        backends
            .state_backend
            .get_result(&python_invocation)
            .await
            .unwrap()
            .as_deref(),
        Some("\"python:two\"")
    );
}

#[tokio::test]
async fn rust_task_can_submit_and_wait_for_python_task() {
    let backends = SharedBackends::new();
    let app_cell: Arc<OnceLock<Arc<RustvelloApp>>> = Arc::new(OnceLock::new());

    let rust_entry = rust_task_id("rust_entry");
    let python_step = python_task_id("python_step");
    let rust_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let python_step = python_step.clone();
        Arc::new(move |args_json| {
            let label = json_arg(&args_json, "label");
            let app = app_cell.get().unwrap();
            let handle = tokio::runtime::Handle::current().block_on(async {
                let inv_id = app
                    .submit(
                        &python_step,
                        args(&[("label", &serde_json::to_string(&label).unwrap())]),
                    )
                    .await?;
                Ok::<_, RustvelloError>(InvocationHandle::<String>::new(
                    inv_id,
                    app.orchestrator(),
                    app.state_backend(),
                ))
            })?;
            let python_result = tokio::runtime::Handle::current()
                .block_on(handle.wait_timeout(Duration::from_secs(5), Duration::from_millis(10)))?;
            Ok(serde_json::to_string(&format!("rust saw {python_result}")).unwrap())
        })
    };

    let mut app = backends.app();
    app.register_task(
        rust_entry.clone(),
        blocking_config(),
        Arc::clone(&rust_func),
    )
    .unwrap();
    app.register_foreign(ForeignTaskProxy::<LabelParams, String>::new(
        python_step.clone(),
    ))
    .unwrap();
    let app = Arc::new(app);
    app_cell.set(Arc::clone(&app)).unwrap();

    let rust_runner = backends.runner(
        TaskLanguage::Rust,
        registry(vec![task_definition(
            rust_entry.clone(),
            blocking_config(),
            rust_func,
        )]),
    );
    let python_runner = backends.runner(
        TaskLanguage::Python,
        registry(vec![task_definition(
            python_step.clone(),
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("python:{}", json_arg(&args_json, "label")))
                        .unwrap(),
                )
            }),
        )]),
    );

    let root = app
        .submit(&rust_entry, args(&[("label", "\"alpha\"")]))
        .await
        .unwrap();
    let rust_handle = tokio::spawn(async move { rust_runner.run_one().await });

    let mut child_ran = false;
    for _ in 0..50 {
        if python_runner.run_one().await.unwrap() {
            child_ran = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(child_ran, "python child invocation was not routed");
    rust_handle.await.unwrap().unwrap();

    assert_eq!(
        backends
            .state_backend
            .get_result(&root)
            .await
            .unwrap()
            .as_deref(),
        Some("\"rust saw python:alpha\"")
    );
}

#[tokio::test]
async fn python_task_can_submit_and_wait_for_rust_task() {
    let backends = SharedBackends::new();
    let app_cell: Arc<OnceLock<Arc<RustvelloApp>>> = Arc::new(OnceLock::new());

    let python_entry = python_task_id("python_entry");
    let rust_step = rust_task_id("rust_step");
    let python_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let rust_step = rust_step.clone();
        Arc::new(move |args_json| {
            let label = json_arg(&args_json, "label");
            let app = app_cell.get().unwrap();
            let handle = tokio::runtime::Handle::current().block_on(async {
                let inv_id = app
                    .submit(
                        &rust_step,
                        args(&[("label", &serde_json::to_string(&label).unwrap())]),
                    )
                    .await?;
                Ok::<_, RustvelloError>(InvocationHandle::<String>::new(
                    inv_id,
                    app.orchestrator(),
                    app.state_backend(),
                ))
            })?;
            let rust_result = tokio::runtime::Handle::current()
                .block_on(handle.wait_timeout(Duration::from_secs(5), Duration::from_millis(10)))?;
            Ok(serde_json::to_string(&format!("python saw {rust_result}")).unwrap())
        })
    };

    let mut app = backends.app();
    app.register_task(
        python_entry.clone(),
        blocking_config(),
        Arc::clone(&python_func),
    )
    .unwrap();
    app.register_foreign(ForeignTaskProxy::<LabelParams, String>::new(
        rust_step.clone(),
    ))
    .unwrap();
    let app = Arc::new(app);
    app_cell.set(Arc::clone(&app)).unwrap();

    let python_runner = backends.runner(
        TaskLanguage::Python,
        registry(vec![task_definition(
            python_entry.clone(),
            blocking_config(),
            python_func,
        )]),
    );
    let rust_runner = backends.runner(
        TaskLanguage::Rust,
        registry(vec![task_definition(
            rust_step.clone(),
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "label")))
                        .unwrap(),
                )
            }),
        )]),
    );

    let root = app
        .submit(&python_entry, args(&[("label", "\"beta\"")]))
        .await
        .unwrap();
    let python_handle = tokio::spawn(async move { python_runner.run_one().await });

    let mut child_ran = false;
    for _ in 0..50 {
        if rust_runner.run_one().await.unwrap() {
            child_ran = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(child_ran, "rust child invocation was not routed");
    python_handle.await.unwrap().unwrap();

    assert_eq!(
        backends
            .state_backend
            .get_result(&root)
            .await
            .unwrap()
            .as_deref(),
        Some("\"python saw rust:beta\"")
    );
}

#[tokio::test]
async fn waiting_python_task_records_dependency_on_rust_task() {
    let backends = SharedBackends::new();
    let app_cell: Arc<OnceLock<Arc<RustvelloApp>>> = Arc::new(OnceLock::new());

    let python_entry = python_task_id("python_waits");
    let rust_step = rust_task_id("rust_dependency");
    let child_id_cell: Arc<OnceLock<InvocationId>> = Arc::new(OnceLock::new());
    let python_func: TaskFn = {
        let app_cell = Arc::clone(&app_cell);
        let child_id_cell = Arc::clone(&child_id_cell);
        let rust_step = rust_step.clone();
        Arc::new(move |args_json| {
            let label = json_arg(&args_json, "label");
            let app = app_cell.get().unwrap();
            let handle = tokio::runtime::Handle::current().block_on(async {
                let inv_id = app
                    .submit(
                        &rust_step,
                        args(&[("label", &serde_json::to_string(&label).unwrap())]),
                    )
                    .await?;
                let _ = child_id_cell.set(inv_id.clone());
                Ok::<_, RustvelloError>(InvocationHandle::<String>::new(
                    inv_id,
                    app.orchestrator(),
                    app.state_backend(),
                ))
            })?;
            let rust_result = tokio::runtime::Handle::current()
                .block_on(handle.wait_timeout(Duration::from_secs(5), Duration::from_millis(10)))?;
            Ok(serde_json::to_string(&rust_result).unwrap())
        })
    };

    let mut app = backends.app();
    app.register_task(
        python_entry.clone(),
        blocking_config(),
        Arc::clone(&python_func),
    )
    .unwrap();
    app.register_foreign(ForeignTaskProxy::<LabelParams, String>::new(
        rust_step.clone(),
    ))
    .unwrap();
    let app = Arc::new(app);
    app_cell.set(Arc::clone(&app)).unwrap();

    let python_runner = backends.runner(
        TaskLanguage::Python,
        registry(vec![task_definition(
            python_entry.clone(),
            blocking_config(),
            python_func,
        )]),
    );
    let rust_runner = backends.runner(
        TaskLanguage::Rust,
        registry(vec![task_definition(
            rust_step.clone(),
            TaskConfig::default(),
            Arc::new(|args_json| {
                std::thread::sleep(Duration::from_millis(50));
                Ok(
                    serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "label")))
                        .unwrap(),
                )
            }),
        )]),
    );

    let root = app
        .submit(&python_entry, args(&[("label", "\"gamma\"")]))
        .await
        .unwrap();
    let python_handle = tokio::spawn(async move { python_runner.run_one().await });

    let mut child_id = None;
    for _ in 0..50 {
        if let Some(id) = child_id_cell.get() {
            child_id = Some(id.clone());
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let child_id = child_id.expect("rust child invocation was not submitted");

    let waiters = backends.orchestrator.get_waiters(&child_id).await.unwrap();
    assert_eq!(waiters, vec![root.clone()]);

    assert!(rust_runner.run_one().await.unwrap());
    python_handle.await.unwrap().unwrap();
    assert!(backends
        .orchestrator
        .get_waiters(&child_id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn rust_trigger_routes_python_task_only_to_python_worker() {
    let backends = SharedBackends::new();
    let mut app = backends.app();
    let python_id = python_task_id("python_trigger_target");
    app.register_foreign(ForeignTaskProxy::<ValueParams, String>::new(
        python_id.clone(),
    ))
    .unwrap();

    TriggerBuilder::new()
        .on_event("rust.created")
        .with_static_args(serde_json::json!({"value": "from-rust"}))
        .build_and_register(&python_id, backends.trigger_manager.store())
        .await
        .unwrap();
    backends
        .trigger_manager
        .emit_event("rust.created", serde_json::json!({}))
        .await
        .unwrap();

    let caller = RunnerId::from_string("rust-trigger-source");
    let created = app.trigger_loop_iteration(&caller).await.unwrap();
    assert_eq!(created.len(), 1);

    let rust_runner = backends.runner(TaskLanguage::Rust, registry(vec![]));
    assert!(
        !rust_runner.run_one().await.unwrap(),
        "Rust runner must not claim a Python trigger target"
    );

    let python_runner = backends.runner(
        TaskLanguage::Python,
        registry(vec![task_definition(
            python_id,
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("python:{}", json_arg(&args_json, "value")))
                        .unwrap(),
                )
            }),
        )]),
    );
    assert!(python_runner.run_one().await.unwrap());
    assert_eq!(
        backends
            .state_backend
            .get_result(&created[0])
            .await
            .unwrap()
            .as_deref(),
        Some("\"python:from-rust\"")
    );
}

#[tokio::test]
async fn python_trigger_routes_rust_task_only_to_rust_worker() {
    let backends = SharedBackends::new();
    let mut app = backends.app();
    let rust_id = rust_task_id("rust_trigger_target");
    app.register_task(
        rust_id.clone(),
        TaskConfig::default(),
        Arc::new(|args_json| {
            Ok(serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "value"))).unwrap())
        }),
    )
    .unwrap();

    TriggerBuilder::new()
        .on_event("python.created")
        .with_static_args(serde_json::json!({"value": "from-python"}))
        .build_and_register(&rust_id, backends.trigger_manager.store())
        .await
        .unwrap();
    backends
        .trigger_manager
        .emit_event("python.created", serde_json::json!({}))
        .await
        .unwrap();

    let caller = RunnerId::from_string("python-trigger-source");
    let created = app.trigger_loop_iteration(&caller).await.unwrap();
    assert_eq!(created.len(), 1);

    let python_runner = backends.runner(TaskLanguage::Python, registry(vec![]));
    assert!(
        !python_runner.run_one().await.unwrap(),
        "Python runner must not claim a Rust trigger target"
    );

    let rust_runner = backends.runner(
        TaskLanguage::Rust,
        registry(vec![task_definition(
            rust_id,
            TaskConfig::default(),
            Arc::new(|args_json| {
                Ok(
                    serde_json::to_string(&format!("rust:{}", json_arg(&args_json, "value")))
                        .unwrap(),
                )
            }),
        )]),
    );
    assert!(rust_runner.run_one().await.unwrap());
    assert_eq!(
        backends
            .state_backend
            .get_result(&created[0])
            .await
            .unwrap()
            .as_deref(),
        Some("\"rust:from-python\"")
    );
}
