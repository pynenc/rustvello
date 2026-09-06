//! Integration tests for the typed task API and `#[rustvello::task]` macro.

use std::sync::Arc;

use rustvello::prelude::*;

// ---------------------------------------------------------------------------
// Macro-generated tasks
// ---------------------------------------------------------------------------

#[rustvello::task]
fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[rustvello::task]
fn greet(name: String) -> String {
    format!("Hello, {}!", name)
}

#[rustvello::task(max_retries = 3)]
fn flaky_double(value: i32) -> i32 {
    value * 2
}

#[rustvello::task(module = "custom_module")]
fn custom_mod_task(n: u64) -> u64 {
    n + 1
}

#[rustvello::task]
fn noop() -> String {
    "done".to_string()
}

#[rustvello::task(queue = "critical", priority = -12.5)]
fn low_priority() -> String {
    "low".to_string()
}

#[rustvello::task(queue = "critical", priority = 40)]
fn high_priority() -> String {
    "high".to_string()
}

#[rustvello::task]
fn fallible_divide(x: f64, y: f64) -> RustvelloResult<f64> {
    if y == 0.0 {
        return Err(RustvelloError::runner_err("division by zero".to_string()));
    }
    Ok(x / y)
}

// ---------------------------------------------------------------------------
// Tests: macro generates correct types and impls
// ---------------------------------------------------------------------------

#[test]
fn macro_generates_task_struct() {
    // The macro should have generated AddTask
    let _task = AddTask::new();
}

#[test]
fn macro_generates_params_struct() {
    // The macro should have generated AddParams
    let params = AddParams { x: 10, y: 20 };
    assert_eq!(params.x, 10);
    assert_eq!(params.y, 20);
}

#[test]
fn macro_task_id() {
    let task = AddTask::new();
    let id = Task::task_id(&task);
    // Module path will be this test crate's path; function name is "add"
    assert_eq!(id.language(), TaskLanguage::Rust);
    assert_eq!(id.name(), "add");
}

#[test]
fn macro_task_default_config() {
    let task = AddTask::new();
    let config = Task::config(&task);
    assert_eq!(config.max_retries, 0);
}

#[test]
fn macro_task_custom_config() {
    let task = FlakyDoubleTask::new();
    let config = Task::config(&task);
    assert_eq!(config.max_retries, 3);
}

#[test]
fn macro_task_queue_and_signed_float_priority() {
    let low = LowPriorityTask::new();
    assert_eq!(Task::config(&low).queue, "critical");
    assert_eq!(Task::config(&low).priority, -12.5);

    let high = HighPriorityTask::new();
    assert_eq!(Task::config(&high).queue, "critical");
    assert_eq!(Task::config(&high).priority, 40.0);
}

#[test]
fn macro_task_custom_module() {
    let task = CustomModTaskTask::new();
    let id = Task::task_id(&task);
    assert_eq!(id.module(), "custom_module");
    assert_eq!(id.name(), "custom_mod_task");
}

#[test]
fn macro_task_run() {
    let task = AddTask::new();
    let result = task.run(AddParams { x: 3, y: 4 }).unwrap();
    assert_eq!(result, 7);
}

#[test]
fn macro_task_run_greet() {
    let task = GreetTask::new();
    let result = task
        .run(GreetParams {
            name: "World".into(),
        })
        .unwrap();
    assert_eq!(result, "Hello, World!");
}

#[test]
fn macro_task_run_noop() {
    let task = NoopTask::new();
    let result = task.run(()).unwrap();
    assert_eq!(result, "done");
}

#[test]
fn macro_fallible_task_ok() {
    let task = FallibleDivideTask::new();
    let result = task.run(FallibleDivideParams { x: 10.0, y: 2.0 }).unwrap();
    assert!((result - 5.0).abs() < f64::EPSILON);
}

#[test]
fn macro_fallible_task_err() {
    let task = FallibleDivideTask::new();
    let result = task.run(FallibleDivideParams { x: 10.0, y: 0.0 });
    assert!(result.is_err());
}

#[test]
fn original_function_preserved() {
    // The original function should still be callable directly
    assert_eq!(add(5, 6), 11);
    assert_eq!(greet("Rust".into()), "Hello, Rust!");
    assert_eq!(noop(), "done");
}

// ---------------------------------------------------------------------------
// Tests: DynTask blanket impl works with macro-generated tasks
// ---------------------------------------------------------------------------

#[test]
fn dyn_task_execute() {
    let task = AddTask::new();
    let mut args = SerializedArguments::new();
    args.insert("x", "10");
    args.insert("y", "20");
    let result = task.execute(&args).unwrap();
    assert_eq!(result, "30");
}

#[test]
fn dyn_task_execute_greet() {
    let task = GreetTask::new();
    let mut args = SerializedArguments::new();
    args.insert("name", r#""DynTask""#);
    let result = task.execute(&args).unwrap();
    assert_eq!(result, r#""Hello, DynTask!""#);
}

// ---------------------------------------------------------------------------
// Tests: Call<T> works with macro-generated tasks
// ---------------------------------------------------------------------------

#[test]
fn call_from_macro_task() {
    let task = AddTask::new();
    let call = Call::new(&task, AddParams { x: 1, y: 2 });
    let dto = call.to_dto().unwrap();
    assert_eq!(dto.task_id, Task::task_id(&task).clone());

    // Serialized arguments should have individual keys
    assert_eq!(dto.serialized_arguments.0["x"], "1");
    assert_eq!(dto.serialized_arguments.0["y"], "2");
}

#[test]
fn call_id_deterministic_with_macro_task() {
    let task = AddTask::new();
    let call1 = Call::new(&task, AddParams { x: 1, y: 2 });
    let call2 = Call::new(&task, AddParams { x: 1, y: 2 });
    assert_eq!(call1.call_id().unwrap(), call2.call_id().unwrap());
}

#[test]
fn call_id_differs_for_different_args() {
    let task = AddTask::new();
    let call1 = Call::new(&task, AddParams { x: 1, y: 2 });
    let call2 = Call::new(&task, AddParams { x: 3, y: 4 });
    assert_ne!(call1.call_id().unwrap(), call2.call_id().unwrap());
}

// ---------------------------------------------------------------------------
// Tests: Params struct serialization
// ---------------------------------------------------------------------------

#[test]
fn params_serde_round_trip() {
    let params = AddParams { x: 42, y: -7 };
    let json = serde_json::to_string(&params).unwrap();
    let restored: AddParams = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.x, 42);
    assert_eq!(restored.y, -7);
}

// ---------------------------------------------------------------------------
// Tests: App integration with macro-generated tasks
// ---------------------------------------------------------------------------

#[test]
fn register_macro_task() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(AddTask::new()).unwrap();
    assert!(app
        .get_task(&Task::task_id(&AddTask::new()).clone())
        .is_some());
}

#[test]
fn register_duplicate_macro_task_errors() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(AddTask::new()).unwrap();
    assert!(app.register(AddTask::new()).is_err());
}

#[test]
fn execute_sync_macro_task() {
    let app = RustvelloApp::new(AppConfig::new("test"));
    let result = app
        .execute_sync(&AddTask::new(), AddParams { x: 10, y: 32 })
        .unwrap();
    assert_eq!(result, 42);
}

#[tokio::test]
async fn submit_call_macro_task() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(AddTask::new()).unwrap();

    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 5, y: 7 })
        .await
        .unwrap();

    // The invocation should be in Registered state (runner claims it later)
    let status = handle.status().await.unwrap();
    assert_eq!(status, InvocationStatus::Registered);
}

#[tokio::test]
async fn submit_call_routes_by_queue_and_priority() {
    let mut config = AppConfig::new("routing-test");
    config.broker_queues.push("critical".to_owned());
    let mut app = RustvelloApp::new(config);
    app.register(LowPriorityTask::new()).unwrap();
    app.register(HighPriorityTask::new()).unwrap();

    let low = app.submit_call(&LowPriorityTask::new(), ()).await.unwrap();
    let high = app.submit_call(&HighPriorityTask::new(), ()).await.unwrap();

    assert_eq!(
        app.broker()
            .retrieve_invocation_from_queue("critical", None)
            .await
            .unwrap()
            .as_ref(),
        Some(high.invocation_id())
    );
    assert_eq!(
        app.broker()
            .retrieve_invocation_from_queue("critical", None)
            .await
            .unwrap()
            .as_ref(),
        Some(low.invocation_id())
    );
}

#[test]
fn broker_priority_rules_override_concrete_task_priority() {
    let mut config = AppConfig::new("priority-rule-test");
    config.priority_rules = vec![
        rustvello_proto::config::BrokerPriorityRule {
            task_id: "*priority".to_owned(),
            priority: 25.0,
        },
        rustvello_proto::config::BrokerPriorityRule {
            task_id: "*low_priority".to_owned(),
            priority: 75.0,
        },
        rustvello_proto::config::BrokerPriorityRule {
            task_id: "*noop".to_owned(),
            priority: -75.0,
        },
    ];
    let app = RustvelloApp::new(config);
    let task = LowPriorityTask::new();
    let resolved = app.resolve_task_config(Task::task_id(&task), Task::config(&task));
    assert_eq!(resolved.priority, 75.0);

    let default_task = NoopTask::new();
    let resolved =
        app.resolve_task_config(Task::task_id(&default_task), Task::config(&default_task));
    assert_eq!(resolved.priority, -75.0);
}

#[tokio::test]
async fn submit_call_unregistered_task_errors() {
    let app = RustvelloApp::new(AppConfig::new("test"));
    let result = app
        .submit_call(&AddTask::new(), AddParams { x: 1, y: 2 })
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Tests: Full lifecycle — submit → run → get typed result
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_typed_lifecycle() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(AddTask::new()).unwrap();

    // Submit the call
    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 100, y: 23 })
        .await
        .unwrap();

    // Create a runner and execute one invocation
    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(AddTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();

    // Now the result should be available via the typed handle
    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 123);
}

#[tokio::test]
async fn full_typed_lifecycle_greet() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(GreetTask::new()).unwrap();

    let handle = app
        .submit_call(
            &GreetTask::new(),
            GreetParams {
                name: "Rustvello".into(),
            },
        )
        .await
        .unwrap();

    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(GreetTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();

    let result: String = handle.result().await.unwrap();
    assert_eq!(result, "Hello, Rustvello!");
}

// ---------------------------------------------------------------------------
// Tests: New macro attributes (Phase 2)
// ---------------------------------------------------------------------------

#[rustvello::task(
    concurrency = "task",
    registration_concurrency = "argument",
    key_arguments = ["order_id"],
    cache_results = true,
    reroute_on_cc = true,
    max_retries = 5,
)]
fn fully_configured(order_id: String, data: String) -> String {
    format!("{}:{}", order_id, data)
}

#[rustvello::task(concurrency = "none")]
fn no_concurrency(x: i32) -> i32 {
    x
}

#[test]
fn macro_all_config_attributes() {
    let task = FullyConfiguredTask::new();
    let config = Task::config(&task);
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.concurrency_control, ConcurrencyControlType::Task);
    assert_eq!(
        config.registration_concurrency,
        ConcurrencyControlType::Argument
    );
    assert_eq!(config.key_arguments, vec!["order_id"]);
    assert!(config.cache_results);
    assert!(!config.is_workflow_task);
    assert!(config.reroute_on_cc);
}

#[test]
fn macro_concurrency_none() {
    let task = NoConcurrencyTask::new();
    let config = Task::config(&task);
    assert_eq!(config.concurrency_control, ConcurrencyControlType::None);
    // Other fields should be defaults
    assert_eq!(config.max_retries, 0);
    assert!(!config.cache_results);
    assert!(config.key_arguments.is_empty());
    assert!(!config.is_workflow_task);
    assert!(!config.reroute_on_cc);
}

#[test]
fn macro_fully_configured_run() {
    let task = FullyConfiguredTask::new();
    let result = task
        .run(FullyConfiguredParams {
            order_id: "ORD-1".into(),
            data: "payload".into(),
        })
        .unwrap();
    assert_eq!(result, "ORD-1:payload");
}

#[rustvello::task(concurrency = "task", registration_concurrency = "unlimited")]
fn registration_unlimited(value: i32) -> i32 {
    value
}

#[rustvello::task(concurrency = "task", registration_concurrency = "argument")]
fn registration_by_argument(value: i32) -> i32 {
    value
}

#[tokio::test]
async fn registration_and_running_concurrency_are_independent() {
    let mut app = RustvelloApp::new(AppConfig::new("independent-cc"));
    let unlimited = RegistrationUnlimitedTask::new();
    let by_argument = RegistrationByArgumentTask::new();
    let unlimited_id = Task::task_id(&unlimited).clone();
    let by_argument_id = Task::task_id(&by_argument).clone();
    app.register(unlimited).unwrap();
    app.register(by_argument).unwrap();

    let mut args = SerializedArguments::new();
    args.insert("value", "1");

    let first = app
        .submit_with_cc(&unlimited_id, args.clone(), Some(&args))
        .await
        .unwrap();
    let second = app
        .submit_with_cc(&unlimited_id, args.clone(), Some(&args))
        .await
        .unwrap();
    assert_ne!(
        first, second,
        "running mode must not enable registration dedup"
    );

    let first = app
        .submit_with_cc(&by_argument_id, args.clone(), Some(&args))
        .await
        .unwrap();
    let second = app
        .submit_with_cc(&by_argument_id, args.clone(), Some(&args))
        .await
        .unwrap();
    assert_eq!(
        first, second,
        "registration mode must deduplicate matching calls"
    );
}

// ---------------------------------------------------------------------------
// Tests: Builder integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn builder_creates_working_app() {
    let mut app = Rustvello::builder()
        .app_id("builder-test")
        .dev_mode(true)
        .build()
        .await
        .unwrap();

    assert_eq!(app.config.app_id, "builder-test");
    assert!(app.config.dev_mode_force_sync);

    // Register and execute a task
    app.register(AddTask::new()).unwrap();
    let result = app
        .execute_sync(&AddTask::new(), AddParams { x: 3, y: 4 })
        .unwrap();
    assert_eq!(result, 7);
}

#[tokio::test]
async fn builder_memory_preset_works() {
    let mut app = Rustvello::builder()
        .app_id("mem-test")
        .memory()
        .build()
        .await
        .unwrap();

    app.register(GreetTask::new()).unwrap();
    let result = app
        .execute_sync(
            &GreetTask::new(),
            GreetParams {
                name: "Builder".into(),
            },
        )
        .unwrap();
    assert_eq!(result, "Hello, Builder!");
}

#[tokio::test]
async fn builder_full_lifecycle() {
    let mut app = Rustvello::builder()
        .app_id("lifecycle-test")
        .memory()
        .build()
        .await
        .unwrap();

    app.register(AddTask::new()).unwrap();

    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 50, y: 50 })
        .await
        .unwrap();

    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(AddTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();

    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 100);
}

#[tokio::test]
async fn full_typed_lifecycle_noop() {
    let mut app = RustvelloApp::new(AppConfig::new("test"));
    app.register(NoopTask::new()).unwrap();

    let handle = app.submit_call(&NoopTask::new(), ()).await.unwrap();

    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(NoopTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();

    let result: String = handle.result().await.unwrap();
    assert_eq!(result, "done");
}

// ===========================================================================
// Phase 3: Unified call, SyncInvocation, Invocation enum, CC, runner race
// ===========================================================================

// --- Tasks for Phase 3 tests ---

#[rustvello::task(concurrency = "task")]
fn task_cc(x: i32) -> i32 {
    x * 2
}

#[rustvello::task(concurrency = "argument")]
fn arg_cc(key: String, val: i32) -> String {
    format!("{}={}", key, val)
}

/// A task that always fails for retry testing.
#[rustvello::task(max_retries = 2)]
fn always_fail(_x: i32) -> RustvelloResult<i32> {
    Err(RustvelloError::runner_err("boom".to_string()))
}

// --- SyncInvocation tests ---

#[test]
fn sync_invocation_success() {
    let inv = SyncInvocation::success(InvocationId::new(), 42);
    assert_eq!(inv.status(), InvocationStatus::Success);
    assert!(inv.is_done());
    assert_eq!(*inv.invocation_id(), *inv.invocation_id()); // not consumed
}

#[test]
fn sync_invocation_failed() {
    let inv = SyncInvocation::<i32>::failed(
        InvocationId::new(),
        RustvelloError::runner_err("test error".to_string()),
    );
    assert_eq!(inv.status(), InvocationStatus::Failed);
    assert!(inv.is_done());
}

// --- Invocation enum tests ---

#[tokio::test]
async fn invocation_sync_variant() {
    let inv_id = InvocationId::new();
    let inv: Invocation<i32> = Invocation::Sync(SyncInvocation::success(inv_id.clone(), 99));

    assert!(inv.is_sync());
    assert!(!inv.is_distributed());
    assert!(inv.is_done().await.unwrap());
    assert_eq!(inv.status().await.unwrap(), InvocationStatus::Success);
    assert_eq!(inv.result().await.unwrap(), 99);
}

#[tokio::test]
async fn invocation_sync_failed_variant() {
    let inv: Invocation<i32> = Invocation::Sync(SyncInvocation::failed(
        InvocationId::new(),
        RustvelloError::runner_err("broke".to_string()),
    ));

    assert!(inv.is_sync());
    let err = inv.result().await.unwrap_err();
    assert!(err.to_string().contains("broke"));
}

// --- Unified call() tests ---

#[tokio::test]
async fn call_sync_mode_returns_sync_invocation() {
    let mut config = AppConfig::new("sync-call");
    config.dev_mode_force_sync = true;
    let cds_backend: std::sync::Arc<dyn rustvello_core::client_data_store::ClientDataStore> =
        std::sync::Arc::new(rustvello::mem::client_data_store::MemClientDataStore::new());
    let cds_mgr = std::sync::Arc::new(
        rustvello_core::client_data_store::ClientDataStoreManager::new(
            cds_backend,
            rustvello_proto::config::ClientDataStoreConfig::default(),
        ),
    );
    let mut app = RustvelloApp::with_backends(
        config,
        std::sync::Arc::new(rustvello::mem::broker::MemBroker::new()),
        std::sync::Arc::new(rustvello::mem::orchestrator::MemOrchestrator::new()),
        std::sync::Arc::new(rustvello::mem::state_backend::MemStateBackend::new()),
        cds_mgr,
    );
    app.register(AddTask::new()).unwrap();

    let inv = app
        .call(&AddTask::new(), AddParams { x: 10, y: 20 })
        .await
        .unwrap();
    assert!(inv.is_sync());
    assert_eq!(inv.result().await.unwrap(), 30);
}

#[tokio::test]
async fn call_distributed_mode_returns_distributed_invocation() {
    let mut app = RustvelloApp::new(AppConfig::new("dist-call"));
    app.register(AddTask::new()).unwrap();

    let inv = app
        .call(&AddTask::new(), AddParams { x: 5, y: 5 })
        .await
        .unwrap();
    assert!(inv.is_distributed());
    assert_eq!(inv.status().await.unwrap(), InvocationStatus::Registered);

    // Run it to completion
    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(AddTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();
    // Can't call result on the original inv since status() was already borrowed,
    // just check via state backend.
    let result_raw = app
        .state_backend()
        .get_result(inv.invocation_id())
        .await
        .unwrap()
        .unwrap();
    let result: i32 = serde_json::from_str(&result_raw).unwrap();
    assert_eq!(result, 10);
}

#[tokio::test]
async fn call_sync_retry_loop_succeeds_on_success() {
    let mut config = AppConfig::new("retry");
    config.dev_mode_force_sync = true;
    let cds_backend: std::sync::Arc<dyn rustvello_core::client_data_store::ClientDataStore> =
        std::sync::Arc::new(rustvello::mem::client_data_store::MemClientDataStore::new());
    let cds_mgr = std::sync::Arc::new(
        rustvello_core::client_data_store::ClientDataStoreManager::new(
            cds_backend,
            rustvello_proto::config::ClientDataStoreConfig::default(),
        ),
    );
    let mut app = RustvelloApp::with_backends(
        config,
        std::sync::Arc::new(rustvello::mem::broker::MemBroker::new()),
        std::sync::Arc::new(rustvello::mem::orchestrator::MemOrchestrator::new()),
        std::sync::Arc::new(rustvello::mem::state_backend::MemStateBackend::new()),
        cds_mgr,
    );
    app.register(AddTask::new()).unwrap();

    // add always succeeds; retry loop should complete on first attempt
    let inv = app
        .call(&AddTask::new(), AddParams { x: 1, y: 2 })
        .await
        .unwrap();
    assert!(inv.is_sync());
    assert_eq!(inv.result().await.unwrap(), 3);
}

#[tokio::test]
async fn call_sync_retry_loop_exhausts_retries() {
    let mut config = AppConfig::new("retry-fail");
    config.dev_mode_force_sync = true;
    let cds_backend: std::sync::Arc<dyn rustvello_core::client_data_store::ClientDataStore> =
        std::sync::Arc::new(rustvello::mem::client_data_store::MemClientDataStore::new());
    let cds_mgr = std::sync::Arc::new(
        rustvello_core::client_data_store::ClientDataStoreManager::new(
            cds_backend,
            rustvello_proto::config::ClientDataStoreConfig::default(),
        ),
    );
    let mut app = RustvelloApp::with_backends(
        config,
        std::sync::Arc::new(rustvello::mem::broker::MemBroker::new()),
        std::sync::Arc::new(rustvello::mem::orchestrator::MemOrchestrator::new()),
        std::sync::Arc::new(rustvello::mem::state_backend::MemStateBackend::new()),
        cds_mgr,
    );
    app.register(AlwaysFailTask::new()).unwrap();

    let inv = app
        .call(&AlwaysFailTask::new(), AlwaysFailParams { _x: 42 })
        .await
        .unwrap();
    assert!(inv.is_sync());
    assert_eq!(inv.status().await.unwrap(), InvocationStatus::Failed);
    let err = inv.result().await.unwrap_err();
    assert!(err.to_string().contains("boom"));
}

// --- Per-task config resolution tests ---

#[test]
fn resolve_task_config_applies_defaults_override() {
    use rustvello::task_config::TaskConfigOverride;
    let mut app = RustvelloApp::new(AppConfig::new("cfg-test"));
    app.set_task_config_overrides(
        std::collections::HashMap::new(),
        TaskConfigOverride {
            max_retries: Some(10),
            ..Default::default()
        },
    );

    let base = TaskConfig::default();
    let resolved = app.resolve_task_config(&TaskId::new("mod", "any_task"), &base);
    assert_eq!(resolved.max_retries, 10);
}

#[test]
fn resolve_task_config_per_task_overrides_defaults() {
    use rustvello::task_config::TaskConfigOverride;
    let mut overrides = std::collections::HashMap::new();
    overrides.insert(
        "my_func".to_string(),
        TaskConfigOverride {
            max_retries: Some(5),
            cache_results: Some(true),
            ..Default::default()
        },
    );

    let mut app = RustvelloApp::new(AppConfig::new("cfg-test"));
    app.set_task_config_overrides(
        overrides,
        TaskConfigOverride {
            max_retries: Some(10), // global default
            ..Default::default()
        },
    );

    let base = TaskConfig::default();
    // The per-task override (5) should take precedence over the global default (10)
    let resolved = app.resolve_task_config(&TaskId::new("mod", "my_func"), &base);
    assert_eq!(resolved.max_retries, 5);
    assert!(resolved.cache_results);
}

#[test]
fn resolve_task_config_env_overrides() {
    // Set an env var for a specific task
    std::env::set_var("RUSTVELLO__TASK__SPECIAL__MAX_RETRIES", "7");

    let app = RustvelloApp::new(AppConfig::new("env-test"));
    let base = TaskConfig::default();
    let resolved = app.resolve_task_config(&TaskId::new("mod", "special"), &base);
    assert_eq!(resolved.max_retries, 7);

    std::env::remove_var("RUSTVELLO__TASK__SPECIAL__MAX_RETRIES");
}

// --- CC Orchestrator tests ---

#[tokio::test]
async fn cc_unlimited_always_allows() {
    let orch = rustvello::mem::orchestrator::MemOrchestrator::new();
    let task_id = TaskId::new("mod", "unlimited_task");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Unlimited;

    assert!(orch
        .check_running_concurrency(&task_id, &config, None)
        .await
        .unwrap());
}

#[tokio::test]
async fn cc_task_level_blocks_second_invocation() {
    use rustvello::mem::orchestrator::MemOrchestrator;
    let orch = MemOrchestrator::new();
    let task_id = TaskId::new("mod", "cc_task");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    let inv1 = InvocationId::new();

    // Index first invocation
    orch.index_for_concurrency_control(&inv1, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();

    // Register an invocation so status check works
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_id = orch.register_invocation(&call).await.unwrap();
    // Move it to Pending (so it counts as active)
    let runner = RunnerId::from_string("cc-runner");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv_id, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();

    // Now check: it should block because there's 1 Pending invocation
    assert!(!orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap());
}

#[tokio::test]
async fn cc_remove_from_index_allows_new_invocations() {
    use rustvello::mem::orchestrator::MemOrchestrator;
    let orch = MemOrchestrator::new();
    let task_id = TaskId::new("mod", "cc_remove");
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    // Register + move to Pending + index
    let call = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let inv_id = orch.register_invocation(&call).await.unwrap();
    let runner = RunnerId::from_string("cc-runner");
    orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
        .await
        .unwrap();
    orch.index_for_concurrency_control(&inv_id, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();

    // Blocked
    assert!(!orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap());

    // Remove from index
    orch.remove_from_concurrency_index(&inv_id).await.unwrap();

    // Now allowed again
    assert!(orch
        .check_running_concurrency(&task_id, &config, Some(&SerializedArguments::new()))
        .await
        .unwrap());
}

// --- Runner race condition test ---

#[tokio::test]
async fn runner_handles_race_condition_gracefully() {
    // Set up an app with a task
    let mut app = RustvelloApp::new(AppConfig::new("race-test"));
    app.register(AddTask::new()).unwrap();

    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 1, y: 2 })
        .await
        .unwrap();

    let inv_id = handle.invocation_id().clone();

    // Create two runners
    let registry = std::sync::Arc::new({
        let mut reg = TaskRegistry::new();
        reg.register_typed(AddTask::new()).unwrap();
        reg
    });

    let runner1 = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::clone(&registry),
        None,
    );

    // First runner executes successfully
    runner1.run_one().await.unwrap();

    // Now the invocation is at Success. If a second runner somehow tried to
    // execute the same invocation, transitioning to Running would fail.
    // Let's simulate that by manually trying the transition:
    let result = app
        .orchestrator()
        .set_invocation_status(&inv_id, InvocationStatus::Running, None)
        .await;
    // Should fail because Success -> Running is not a valid transition
    assert!(result.is_err());

    // But crucially, the first run completed fine
    let result: i32 = app
        .state_backend()
        .get_result(&inv_id)
        .await
        .unwrap()
        .map(|r| serde_json::from_str(&r).unwrap())
        .unwrap();
    assert_eq!(result, 3);
}

// --- New status variants tests ---

#[test]
fn status_concurrency_controlled_final_is_terminal() {
    assert!(InvocationStatus::ConcurrencyControlledFinal.is_terminal());
}

#[test]
fn status_rerouted_is_not_terminal() {
    assert!(!InvocationStatus::Rerouted.is_terminal());
}

#[test]
fn status_transitions_to_new_cc_variants() {
    assert!(InvocationStatus::Registered
        .can_transition_to(InvocationStatus::ConcurrencyControlledFinal));
    // Registered does NOT transition to Rerouted (must go through CC first)
    assert!(!InvocationStatus::Registered.can_transition_to(InvocationStatus::Rerouted));
    // CC transitions to Rerouted (not CC_Final)
    assert!(InvocationStatus::ConcurrencyControlled.can_transition_to(InvocationStatus::Rerouted));
    // Terminal states cannot transition further
    assert!(
        !InvocationStatus::ConcurrencyControlledFinal.can_transition_to(InvocationStatus::Pending)
    );
    // Rerouted CAN transition to Pending (it's available_for_run)
    assert!(InvocationStatus::Rerouted.can_transition_to(InvocationStatus::Pending));
}

// --- serialized_args_for_concurrency_check tests (integration) ---

#[test]
fn cc_args_task_level_returns_empty_via_call() {
    let task = TaskCcTask::new();
    let call = Call::new(&task, TaskCcParams { x: 42 });
    let cc_args = call.serialized_args_for_concurrency_check().unwrap();
    // Task-level CC returns Some(empty map)
    assert!(cc_args.is_some());
    assert!(cc_args.unwrap().0.is_empty());
}

#[test]
fn cc_args_argument_level_returns_all_via_call() {
    let task = ArgCcTask::new();
    let call = Call::new(
        &task,
        ArgCcParams {
            key: "k".into(),
            val: 10,
        },
    );
    let cc_args = call.serialized_args_for_concurrency_check().unwrap();
    assert!(cc_args.is_some());
    let args = cc_args.unwrap();
    assert!(args.0.contains_key("key"));
    assert!(args.0.contains_key("val"));
}

// ---------------------------------------------------------------------------
// Client Data Store integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cds_builder_provides_accessible_manager() {
    let app = Rustvello::builder()
        .app_id("cds-test")
        .memory()
        .build()
        .await
        .unwrap();
    let cds = app.client_data_store();
    // Should be able to store and resolve
    let small = "tiny";
    let result = cds.store_if_large(small).await.unwrap();
    assert_eq!(result, small); // Below threshold → inline
}

#[tokio::test]
async fn cds_store_and_resolve_round_trip() {
    use rustvello_core::client_data_store::is_reference;

    let app = Rustvello::builder()
        .app_id("cds-round-trip")
        .memory()
        .client_data_store_config({
            let mut cfg = rustvello_proto::config::ClientDataStoreConfig::default();
            cfg.min_size_to_cache = 10;
            cfg
        })
        .build()
        .await
        .unwrap();
    let cds = app.client_data_store();

    let payload = "a]".repeat(20); // 40 bytes, above threshold of 10
    let key = cds.store_if_large(&payload).await.unwrap();
    assert!(is_reference(&key));

    let resolved = cds.resolve(&key).await.unwrap();
    assert_eq!(resolved, payload);
}

#[tokio::test]
async fn cds_disabled_returns_inline() {
    use rustvello_core::client_data_store::is_reference;

    let app = Rustvello::builder()
        .app_id("cds-disabled")
        .memory()
        .client_data_store_config({
            let mut cfg = rustvello_proto::config::ClientDataStoreConfig::default();
            cfg.disabled = true;
            cfg.min_size_to_cache = 1;
            cfg
        })
        .build()
        .await
        .unwrap();
    let cds = app.client_data_store();

    let payload = "x".repeat(200);
    let result = cds.store_if_large(&payload).await.unwrap();
    assert!(!is_reference(&result));
    assert_eq!(result, payload);
}

#[tokio::test]
async fn cds_purge_clears_data() {
    use rustvello_core::client_data_store::is_reference;

    let app = Rustvello::builder()
        .app_id("cds-purge")
        .memory()
        .client_data_store_config({
            let mut cfg = rustvello_proto::config::ClientDataStoreConfig::default();
            cfg.min_size_to_cache = 5;
            cfg
        })
        .build()
        .await
        .unwrap();
    let cds = app.client_data_store();

    let payload = "purge me";
    let key = cds.store_if_large(payload).await.unwrap();
    assert!(is_reference(&key));

    cds.purge().await.unwrap();

    let err = cds.resolve(&key).await;
    assert!(err.is_err());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn cds_sqlite_round_trip() {
    use rustvello_core::client_data_store::is_reference;

    let dir = std::env::temp_dir().join("rustvello_cds_sqlite_test");
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("cds_test.db");

    let app = Rustvello::builder()
        .app_id("cds-sqlite")
        .sqlite(db_path.to_str().unwrap(), "cds-sqlite")
        .client_data_store_config({
            let mut cfg = rustvello_proto::config::ClientDataStoreConfig::default();
            cfg.min_size_to_cache = 10;
            cfg
        })
        .build()
        .await
        .unwrap();
    let cds = app.client_data_store();

    let payload = "x".repeat(100);
    let key = cds.store_if_large(&payload).await.unwrap();
    assert!(is_reference(&key));

    let resolved = cds.resolve(&key).await.unwrap();
    assert_eq!(resolved, payload);

    cds.purge().await.unwrap();
    assert!(cds.resolve(&key).await.is_err());

    std::fs::remove_dir_all(&dir).ok();
}

// ---------------------------------------------------------------------------
// Explicit workflow assignment and context injection tests
// ---------------------------------------------------------------------------

#[rustvello::workflow]
fn order_workflow(order_id: String) -> String {
    order_id
}

#[rustvello::workflow(blocking = false)]
fn child_workflow(name: String) -> String {
    name
}

/// Ordinary top-level calls do not implicitly become workflow roots.
#[tokio::test]
async fn ordinary_top_level_submit_has_no_workflow() {
    let mut app = RustvelloApp::new(AppConfig::new("test-wf"));
    app.register(AddTask::new()).unwrap();

    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 1, y: 2 })
        .await
        .unwrap();
    let inv_id = handle.invocation_id().clone();

    let inv_dto = app.state_backend().get_invocation(&inv_id).await.unwrap();
    assert!(inv_dto.workflow.is_none());
    assert!(inv_dto.parent_invocation_id.is_none());
}

/// Untyped submission follows the same non-workflow default.
#[tokio::test]
async fn ordinary_untyped_submit_has_no_workflow() {
    let mut app = RustvelloApp::new(AppConfig::new("test-wf-untyped"));
    app.register(AddTask::new()).unwrap();

    let mut args = SerializedArguments::new();
    args.insert("x", "1");
    args.insert("y", "2");
    let inv_id = app
        .submit(&Task::task_id(&AddTask::new()).clone(), args)
        .await
        .unwrap();

    let inv_dto = app.state_backend().get_invocation(&inv_id).await.unwrap();
    assert!(inv_dto.workflow.is_none());
}

/// The workflow macro marks a task as a blocking, workflow-defining task.
#[test]
fn workflow_macro_sets_root_contract() {
    let task = ChildWorkflowTask::new();
    let config = Task::config(&task);
    assert!(config.is_workflow_task);
    assert!(
        config.blocking,
        "workflow tasks must run on the blocking path"
    );
}

/// An explicit workflow task creates and persists a root identity.
#[tokio::test]
async fn explicit_workflow_creates_root() {
    let mut app = RustvelloApp::new(AppConfig::new("test-explicit-wf"));
    app.register(OrderWorkflowTask::new()).unwrap();

    let handle = app
        .submit_call(
            &OrderWorkflowTask::new(),
            OrderWorkflowParams {
                order_id: "ORD-1".into(),
            },
        )
        .await
        .unwrap();
    let inv_id = handle.invocation_id().clone();
    let inv_dto = app.state_backend().get_invocation(&inv_id).await.unwrap();
    let workflow = inv_dto.workflow.unwrap();

    assert_eq!(workflow.workflow_id, inv_id);
    assert_eq!(
        workflow.workflow_type,
        Task::task_id(&OrderWorkflowTask::new()).clone()
    );
    assert_eq!(workflow.depth, 0);
    assert!(workflow.parent_id.is_none());

    let runs = app
        .state_backend()
        .get_workflow_runs(Task::task_id(&OrderWorkflowTask::new()))
        .await
        .unwrap();
    assert_eq!(runs.len(), 1);
}

/// Verify that the runner sets InvocationContext during task execution.
#[tokio::test]
async fn runner_sets_invocation_context() {
    let mut app = RustvelloApp::new(AppConfig::new("test-ctx"));
    app.register(AddTask::new()).unwrap();

    let handle = app
        .submit_call(&AddTask::new(), AddParams { x: 10, y: 20 })
        .await
        .unwrap();

    let runner = TaskRunner::new(
        "test".to_string(),
        AppConfig::default(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        std::sync::Arc::new({
            let mut reg = TaskRegistry::new();
            reg.register_typed(AddTask::new()).unwrap();
            reg
        }),
        None,
    );
    runner.run_one().await.unwrap();

    // Verify the task completed successfully (context was set during execution)
    let result: i32 = handle.result().await.unwrap();
    assert_eq!(result, 30);
}

/// Verify that workflow membership is queryable via state backend.
#[tokio::test]
async fn workflow_members_queryable() {
    let mut app = RustvelloApp::new(AppConfig::new("test-wf-query"));
    app.register(OrderWorkflowTask::new()).unwrap();
    app.register(ChildWorkflowTask::new()).unwrap();

    let h1 = app
        .submit_call(
            &OrderWorkflowTask::new(),
            OrderWorkflowParams {
                order_id: "one".into(),
            },
        )
        .await
        .unwrap();
    let h2 = app
        .submit_call(
            &ChildWorkflowTask::new(),
            ChildWorkflowParams { name: "two".into() },
        )
        .await
        .unwrap();

    let inv1 = h1.invocation_id().clone();
    let inv2 = h2.invocation_id().clone();

    let wf1_members = app
        .state_backend()
        .get_workflow_invocations(&inv1)
        .await
        .unwrap();
    assert_eq!(wf1_members.len(), 1);
    assert_eq!(wf1_members[0], inv1);

    let wf2_members = app
        .state_backend()
        .get_workflow_invocations(&inv2)
        .await
        .unwrap();
    assert_eq!(wf2_members.len(), 1);
    assert_eq!(wf2_members[0], inv2);
}

/// Verify child invocations inherit the parent's workflow identity.
///
/// Simulates what happens when a task calls `app.submit()` from within
/// its execution context. The child should inherit workflow_id,
/// workflow_type, and increment depth.
#[tokio::test]
async fn workflow_child_inherits_parent_workflow() {
    use rustvello_core::context::{InvocationContext, INVOCATION_CTX};

    let mut app = RustvelloApp::new(AppConfig::new("test-wf-inherit"));
    app.register(AddTask::new()).unwrap();

    app.register(OrderWorkflowTask::new()).unwrap();

    let parent_handle = app
        .submit_call(
            &OrderWorkflowTask::new(),
            OrderWorkflowParams {
                order_id: "parent".into(),
            },
        )
        .await
        .unwrap();
    let parent_inv_id = parent_handle.invocation_id().clone();
    let parent_dto = app
        .state_backend()
        .get_invocation(&parent_inv_id)
        .await
        .unwrap();
    let parent_wf = parent_dto.workflow.clone().unwrap();

    // Simulate executing inside the parent task's context
    // (this is what the runner does when it processes the parent)
    let parent_ctx = InvocationContext {
        invocation_id: parent_inv_id.clone(),
        task_id: Task::task_id(&OrderWorkflowTask::new()).clone(),
        workflow: Some(parent_wf.clone()),
        is_workflow_defining: true,
        state_backend: Some(app.state_backend()),
        parent_invocation_id: None,
        num_retries: 0,
    };

    // Submit a child task from within the parent's context
    let child_inv_id = INVOCATION_CTX
        .scope(parent_ctx, async {
            let mut args = SerializedArguments::new();
            args.insert("x", "10");
            args.insert("y", "20");
            app.submit(&Task::task_id(&AddTask::new()).clone(), args)
                .await
                .unwrap()
        })
        .await;

    let child_dto = app
        .state_backend()
        .get_invocation(&child_inv_id)
        .await
        .unwrap();
    let child_wf = child_dto.workflow.unwrap();

    // Child should inherit the SAME workflow_id and workflow_type
    assert_eq!(child_wf.workflow_id, parent_wf.workflow_id);
    assert_eq!(child_wf.workflow_type, parent_wf.workflow_type);
    // Depth should increment
    assert_eq!(child_wf.depth, parent_wf.depth + 1);
    // parent_id tracks the parent invocation within the workflow
    assert_eq!(child_wf.parent_id, Some(parent_inv_id.clone()));
    // The child should record the parent invocation ID
    assert_eq!(child_dto.parent_invocation_id, Some(parent_inv_id));
}

/// Verify that a chain of tasks all share the same workflow identity.
#[tokio::test]
async fn workflow_chain_shares_identity() {
    use rustvello_core::context::{InvocationContext, INVOCATION_CTX};

    let mut app = RustvelloApp::new(AppConfig::new("test-wf-chain"));
    app.register(AddTask::new()).unwrap();
    app.register(OrderWorkflowTask::new()).unwrap();

    let h0 = app
        .submit_call(
            &OrderWorkflowTask::new(),
            OrderWorkflowParams {
                order_id: "root".into(),
            },
        )
        .await
        .unwrap();
    let inv0 = h0.invocation_id().clone();
    let dto0 = app.state_backend().get_invocation(&inv0).await.unwrap();
    let wf0 = dto0.workflow.clone().unwrap();

    // Level 1: child of root
    let ctx0 = InvocationContext {
        invocation_id: inv0.clone(),
        task_id: Task::task_id(&OrderWorkflowTask::new()).clone(),
        workflow: Some(wf0.clone()),
        is_workflow_defining: true,
        state_backend: Some(app.state_backend()),
        parent_invocation_id: None,
        num_retries: 0,
    };

    let inv1 = INVOCATION_CTX
        .scope(ctx0, async {
            let mut args = SerializedArguments::new();
            args.insert("x", "1");
            args.insert("y", "1");
            app.submit(&Task::task_id(&AddTask::new()).clone(), args)
                .await
                .unwrap()
        })
        .await;

    let dto1 = app.state_backend().get_invocation(&inv1).await.unwrap();
    let wf1 = dto1.workflow.clone().unwrap();

    // Level 2: grandchild
    let ctx1 = InvocationContext {
        invocation_id: inv1.clone(),
        task_id: Task::task_id(&AddTask::new()).clone(),
        workflow: Some(wf1.clone()),
        is_workflow_defining: false,
        state_backend: Some(app.state_backend()),
        parent_invocation_id: Some(inv0.clone()),
        num_retries: 0,
    };

    let inv2 = INVOCATION_CTX
        .scope(ctx1, async {
            let mut args = SerializedArguments::new();
            args.insert("x", "2");
            args.insert("y", "2");
            app.submit(&Task::task_id(&AddTask::new()).clone(), args)
                .await
                .unwrap()
        })
        .await;

    let dto2 = app.state_backend().get_invocation(&inv2).await.unwrap();
    let wf2 = dto2.workflow.unwrap();

    // All three share the same workflow_id and workflow_type
    assert_eq!(wf0.workflow_id, wf1.workflow_id);
    assert_eq!(wf1.workflow_id, wf2.workflow_id);
    assert_eq!(wf0.workflow_type, wf1.workflow_type);
    assert_eq!(wf1.workflow_type, wf2.workflow_type);

    // Depths increment: 0, 1, 2
    assert_eq!(wf0.depth, 0);
    assert_eq!(wf1.depth, 1);
    assert_eq!(wf2.depth, 2);

    // Parent chain: inv2 → inv1 → inv0
    assert!(dto0.parent_invocation_id.is_none());
    assert_eq!(dto1.parent_invocation_id, Some(inv0.clone()));
    assert_eq!(dto2.parent_invocation_id, Some(inv1));
}

/// An explicit workflow submitted by a workflow creates a sub-workflow.
#[tokio::test]
async fn explicit_child_workflow_creates_sub_workflow() {
    use rustvello_core::context::{InvocationContext, INVOCATION_CTX};

    let mut app = RustvelloApp::new(AppConfig::new("test-wf-sub"));
    app.register(OrderWorkflowTask::new()).unwrap();
    app.register(ChildWorkflowTask::new()).unwrap();

    // Submit parent task (root workflow)
    let parent_handle = app
        .submit_call(
            &OrderWorkflowTask::new(),
            OrderWorkflowParams {
                order_id: "parent".into(),
            },
        )
        .await
        .unwrap();
    let parent_inv_id = parent_handle.invocation_id().clone();
    let parent_dto = app
        .state_backend()
        .get_invocation(&parent_inv_id)
        .await
        .unwrap();
    let parent_wf = parent_dto.workflow.clone().unwrap();

    // Simulate parent context
    let parent_ctx = InvocationContext {
        invocation_id: parent_inv_id.clone(),
        task_id: Task::task_id(&OrderWorkflowTask::new()).clone(),
        workflow: Some(parent_wf.clone()),
        is_workflow_defining: true,
        state_backend: Some(app.state_backend()),
        parent_invocation_id: None,
        num_retries: 0,
    };

    let child_inv_id = INVOCATION_CTX
        .scope(parent_ctx, async {
            let mut args = SerializedArguments::new();
            args.insert("name", "Alice");
            app.submit(&Task::task_id(&ChildWorkflowTask::new()).clone(), args)
                .await
                .unwrap()
        })
        .await;

    let child_dto = app
        .state_backend()
        .get_invocation(&child_inv_id)
        .await
        .unwrap();
    let child_wf = child_dto.workflow.unwrap();

    // Sub-workflow: gets its OWN workflow_id (== child invocation_id)
    assert_eq!(child_wf.workflow_id, child_inv_id);
    // Sub-workflow type == the new task's type
    assert_eq!(
        child_wf.workflow_type,
        Task::task_id(&ChildWorkflowTask::new()).clone()
    );
    // parent_id points back to the parent workflow
    assert_eq!(child_wf.parent_id, Some(parent_wf.workflow_id));
    // The child records parent invocation
    assert_eq!(child_dto.parent_invocation_id, Some(parent_inv_id));
}
