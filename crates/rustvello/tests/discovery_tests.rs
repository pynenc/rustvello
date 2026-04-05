//! Integration tests for Phase 7: task auto-discovery, TaskModule, and builder integration.

use rustvello::prelude::*;
use rustvello_core::error::RustvelloResult;
use rustvello_core::task::{TaskModule, TaskRegistry};

// ---------------------------------------------------------------------------
// Tasks registered via #[rustvello::task] — auto-submitted to inventory
// ---------------------------------------------------------------------------

#[rustvello::task]
fn disc_add(a: i32, b: i32) -> i32 {
    a + b
}

#[rustvello::task(module = "discovery")]
fn disc_greet(name: String) -> String {
    format!("Hi, {name}!")
}

#[rustvello::task]
fn disc_noop() -> String {
    "done".to_string()
}

// ---------------------------------------------------------------------------
// TaskModule implementation
// ---------------------------------------------------------------------------

struct MathModule;

impl TaskModule for MathModule {
    fn name(&self) -> &str {
        "math"
    }

    fn register(&self, registry: &mut TaskRegistry) -> RustvelloResult<()> {
        // Register the disc_add task (already defined above via macro)
        registry.register_typed(DiscAddTask::new())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auto_discover_finds_inventory_tasks() {
    // Build with auto_discover_tasks — should find all #[rustvello::task] in this binary
    let app = Rustvello::builder()
        .app_id("discover-test")
        .auto_discover_tasks()
        .build()
        .await
        .unwrap();

    // The tasks defined in THIS file should be discovered
    let tid_add = TaskId::new("discovery_tests", "disc_add");
    let tid_greet = TaskId::new("discovery", "disc_greet");
    let tid_noop = TaskId::new("discovery_tests", "disc_noop");

    assert!(
        app.get_task(&tid_add).is_some(),
        "disc_add should be auto-discovered"
    );
    assert!(
        app.get_task(&tid_greet).is_some(),
        "disc_greet should be auto-discovered"
    );
    assert!(
        app.get_task(&tid_noop).is_some(),
        "disc_noop should be auto-discovered"
    );
}

#[tokio::test]
async fn auto_discover_tasks_are_executable() {
    let app = Rustvello::builder()
        .app_id("exec-test")
        .auto_discover_tasks()
        .build()
        .await
        .unwrap();

    let tid = TaskId::new("discovery_tests", "disc_add");
    let task = app.get_task(&tid).expect("disc_add should exist");

    // Execute via DynTask interface with serialized arguments
    use rustvello_proto::call::SerializedArguments;
    let mut args = std::collections::BTreeMap::new();
    args.insert("a".to_string(), "3".to_string());
    args.insert("b".to_string(), "7".to_string());
    let serialized = SerializedArguments(args);

    let result = task.execute(&serialized).unwrap();
    assert_eq!(result, "10");
}

#[tokio::test]
async fn auto_discover_noop_task_executable() {
    let app = Rustvello::builder()
        .app_id("noop-test")
        .auto_discover_tasks()
        .build()
        .await
        .unwrap();

    let tid = TaskId::new("discovery_tests", "disc_noop");
    let task = app.get_task(&tid).expect("disc_noop should exist");

    use rustvello_proto::call::SerializedArguments;
    let mut args = std::collections::BTreeMap::new();
    args.insert("__args__".to_string(), "null".to_string());
    let serialized = SerializedArguments(args);
    let result = task.execute(&serialized).unwrap();
    assert_eq!(result, "\"done\"");
}

#[tokio::test]
async fn builder_without_auto_discover_has_no_tasks() {
    let app = Rustvello::builder()
        .app_id("no-discover-test")
        .build()
        .await
        .unwrap();

    // Without auto_discover_tasks(), no tasks should be registered
    let tid = TaskId::new("discovery_tests", "disc_add");
    assert!(app.get_task(&tid).is_none());
}

#[test]
fn task_module_registration() {
    let mut registry = TaskRegistry::new();
    let module = MathModule;

    assert_eq!(module.name(), "math");
    module.register(&mut registry).unwrap();

    let tid = TaskId::new("discovery_tests", "disc_add");
    assert!(registry.get_dyn(&tid).is_some());
}

#[tokio::test]
async fn builder_with_task_module() {
    let app = Rustvello::builder()
        .app_id("module-test")
        .task_module(Box::new(MathModule))
        .build()
        .await
        .unwrap();

    let tid = TaskId::new("discovery_tests", "disc_add");
    assert!(
        app.get_task(&tid).is_some(),
        "Task from MathModule should be registered"
    );
}

#[tokio::test]
async fn into_runner_preserves_config() {
    let app = Rustvello::builder()
        .app_id("runner-test")
        .dev_mode(true)
        .build()
        .await
        .unwrap();

    let runner = app.into_runner();
    assert_eq!(runner.runner_id().as_str().len(), 36); // UUID format
}

#[tokio::test]
async fn into_runner_with_idle_sleep() {
    let app = Rustvello::builder()
        .app_id("idle-test")
        .build()
        .await
        .unwrap();

    let runner = app.into_runner().with_idle_sleep(50);
    assert_eq!(runner.runner_id().as_str().len(), 36);
}
