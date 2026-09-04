use super::*;
use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloError;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{TaskDefinition, TaskRegistry};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::config::{AppConfig, TaskConfig};
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::InvocationDTO;
use rustvello_proto::status::{ConcurrencyControlType, InvocationStatus};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(super) fn make_runner() -> (
    PersistentTokioRunner,
    Arc<dyn Orchestrator>,
    Arc<dyn StateBackend>,
) {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            TaskId::new("test", "double"),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    (runner, orchestrator, state_backend)
}

#[tokio::test]
async fn test_run_one_no_work() {
    let (runner, _, _) = make_runner();
    let did_work = runner.run_one().await.unwrap();
    assert!(!did_work);
}

#[tokio::test]
async fn test_full_invocation_cycle() {
    let (runner, orchestrator, state_backend) = make_runner();

    let task_id = TaskId::new("test", "double");
    let mut args = SerializedArguments::new();
    args.insert("x", "21");
    let call = CallDTO::new(task_id.clone(), args);

    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    runner.broker.route_invocation(&inv_id).await.unwrap();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);

    let result = state_backend.get_result(&inv_id).await.unwrap();
    assert_eq!(result, Some("42".to_string()));
}

#[tokio::test]
async fn test_retry_on_failure() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let mut registry = TaskRegistry::new();
    registry
        .register({
            let mut config = TaskConfig::default();
            config.max_retries = 2;
            TaskDefinition::new(
                TaskId::new("test", "failing"),
                config,
                Arc::new(|_args: String| {
                    Err(RustvelloError::runner_err("always fails".to_string()))
                }),
            )
        })
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    let task_id = TaskId::new("test", "failing");
    let args = SerializedArguments::new();
    let call = CallDTO::new(task_id.clone(), args);

    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    broker.route_invocation(&inv_id).await.unwrap();

    runner.run_one().await.unwrap();
    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Retry);

    runner.run_one().await.unwrap();
    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Retry);

    runner.run_one().await.unwrap();
    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Failed);

    let error = state_backend.get_error(&inv_id).await.unwrap();
    assert!(error.is_some());
}

#[tokio::test]
async fn test_heartbeat_registers_with_orchestrator() {
    let (runner, orchestrator, _) = make_runner();
    runner.heartbeat().await.unwrap();
    let stale = orchestrator
        .get_stale_running_invocations(60)
        .await
        .unwrap();
    assert!(stale.is_empty());
}

#[tokio::test]
async fn test_recover_stale_pending_invocation() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let mem_orchestrator = Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let orchestrator: Arc<dyn Orchestrator> = Arc::clone(&mem_orchestrator) as _;
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            TaskId::new("test", "double"),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    let task_id = TaskId::new("test", "double");
    let mut args = SerializedArguments::new();
    args.insert("x", "5");
    let call = CallDTO::new(task_id.clone(), args);

    let dead_runner_id = RunnerId::from_string("dead-runner-pending");
    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&dead_runner_id))
        .await
        .unwrap();

    {
        mem_orchestrator
            .backdate_status_for_testing(&inv_id, chrono::Duration::seconds(600))
            .await;
    }

    let recovered = runner.recover_stale_invocations().await.unwrap();
    assert_eq!(recovered, 1);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Rerouted);

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);
}

#[tokio::test]
async fn test_recover_stale_running_invocation() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            TaskId::new("test", "double"),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    let dead_runner_id = RunnerId::from_string("dead-runner");

    let task_id = TaskId::new("test", "double");
    let mut args = SerializedArguments::new();
    args.insert("x", "7");
    let call = CallDTO::new(task_id.clone(), args);

    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&dead_runner_id))
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv_id, InvocationStatus::Running, Some(&dead_runner_id))
        .await
        .unwrap();

    let recovered = runner.recover_stale_invocations().await.unwrap();
    assert_eq!(recovered, 1);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Rerouted);

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);
}

#[test]
fn test_default_workers_matches_cpu_count() {
    let (runner, _, _) = make_runner();
    let expected = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    assert_eq!(runner.num_workers(), expected);
    assert!(runner.num_workers() >= 1);
}

#[test]
fn test_with_num_workers_override() {
    let (runner, _, _) = make_runner();
    let runner = runner.with_num_workers(4);
    assert_eq!(runner.num_workers(), 4);
}

#[test]
fn test_with_num_workers_clamps_to_one() {
    let (runner, _, _) = make_runner();
    let runner = runner.with_num_workers(0);
    assert_eq!(runner.num_workers(), 1);
}

#[tokio::test]
async fn test_concurrent_workers_process_invocations() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            TaskId::new("test", "double"),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    )
    .with_num_workers(4)
    .with_idle_sleep(10);

    let task_id = TaskId::new("test", "double");
    let mut inv_ids = Vec::new();
    for i in 0..8 {
        let mut args = SerializedArguments::new();
        args.insert("x", &i.to_string());
        let call = CallDTO::new(task_id.clone(), args);
        let inv_id = orchestrator.register_invocation(&call).await.unwrap();
        let inv_dto = InvocationDTO::new(inv_id.clone(), task_id.clone(), call.call_id.clone());
        state_backend
            .upsert_invocation(&inv_dto, &call)
            .await
            .unwrap();
        broker.route_invocation(&inv_id).await.unwrap();
        inv_ids.push(inv_id);
    }

    let _ = runner
        .with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        })
        .await;

    for inv_id in &inv_ids {
        let status = orchestrator.get_invocation_status(inv_id).await.unwrap();
        assert_eq!(
            status.status,
            InvocationStatus::Success,
            "Invocation {} should be Success",
            inv_id
        );
    }
}

#[test]
fn test_runner_cls() {
    let (runner, _, _) = make_runner();
    assert_eq!(runner.runner_cls(), "PersistentTokioRunner");
}

#[test]
fn test_max_parallel_slots() {
    let (runner, _, _) = make_runner();
    let runner = runner.with_num_workers(8);
    assert_eq!(runner.max_parallel_slots(), 8);
}

// ===================================================================
// Blocking priority tests
// ===================================================================

#[tokio::test]
async fn test_blocking_priority_over_fifo() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let task_id = TaskId::new("test", "double");
    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            task_id.clone(),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    let mut args1 = SerializedArguments::new();
    args1.insert("x", "10");
    let call1 = CallDTO::new(task_id.clone(), args1);
    let regular_inv = orchestrator.register_invocation(&call1).await.unwrap();
    let dto1 = InvocationDTO::new(regular_inv.clone(), task_id.clone(), call1.call_id.clone());
    state_backend
        .upsert_invocation(&dto1, &call1)
        .await
        .unwrap();
    broker.route_invocation(&regular_inv).await.unwrap();

    let mut args2 = SerializedArguments::new();
    args2.insert("x", "21");
    let call2 = CallDTO::new(task_id.clone(), args2);
    let blocking_inv = orchestrator.register_invocation(&call2).await.unwrap();
    let dto2 = InvocationDTO::new(blocking_inv.clone(), task_id.clone(), call2.call_id.clone());
    state_backend
        .upsert_invocation(&dto2, &call2)
        .await
        .unwrap();
    broker.route_invocation(&blocking_inv).await.unwrap();

    let call3 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let waiter_inv = orchestrator.register_invocation(&call3).await.unwrap();
    orchestrator
        .set_waiting_for(&waiter_inv, &blocking_inv)
        .await
        .unwrap();

    runner.run_one().await.unwrap();

    let blocking_status = orchestrator
        .get_invocation_status(&blocking_inv)
        .await
        .unwrap();
    assert_eq!(
        blocking_status.status,
        InvocationStatus::Success,
        "Blocking invocation should be prioritized and executed first"
    );

    let regular_status = orchestrator
        .get_invocation_status(&regular_inv)
        .await
        .unwrap();
    assert_eq!(
        regular_status.status,
        InvocationStatus::Registered,
        "Regular invocation should not have been executed yet"
    );

    let result = state_backend.get_result(&blocking_inv).await.unwrap();
    assert_eq!(result, Some("42".to_string()));
}

#[tokio::test]
async fn test_no_blocking_falls_back_to_fifo() {
    let (runner, orchestrator, state_backend) = make_runner();

    let task_id = TaskId::new("test", "double");
    let mut args = SerializedArguments::new();
    args.insert("x", "5");
    let call = CallDTO::new(task_id.clone(), args);

    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let inv_dto = InvocationDTO::new(inv_id.clone(), task_id, call.call_id.clone());
    state_backend
        .upsert_invocation(&inv_dto, &call)
        .await
        .unwrap();
    runner.broker.route_invocation(&inv_id).await.unwrap();

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let status = orchestrator.get_invocation_status(&inv_id).await.unwrap();
    assert_eq!(status.status, InvocationStatus::Success);
    let result = state_backend.get_result(&inv_id).await.unwrap();
    assert_eq!(result, Some("10".to_string()));
}

#[tokio::test]
async fn test_blocking_race_handled_gracefully() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let task_id = TaskId::new("test", "double");
    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            task_id.clone(),
            TaskConfig::default(),
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    let mut args = SerializedArguments::new();
    args.insert("x", "7");
    let call = CallDTO::new(task_id.clone(), args);
    let blocking_inv = orchestrator.register_invocation(&call).await.unwrap();
    let dto = InvocationDTO::new(blocking_inv.clone(), task_id.clone(), call.call_id.clone());
    state_backend.upsert_invocation(&dto, &call).await.unwrap();
    broker.route_invocation(&blocking_inv).await.unwrap();

    let call2 = CallDTO::new(task_id.clone(), SerializedArguments::new());
    let waiter = orchestrator.register_invocation(&call2).await.unwrap();
    orchestrator
        .set_waiting_for(&waiter, &blocking_inv)
        .await
        .unwrap();

    runner.run_one().await.unwrap();
    let status = orchestrator
        .get_invocation_status(&blocking_inv)
        .await
        .unwrap();
    assert_eq!(status.status, InvocationStatus::Success);

    runner.run_one().await.unwrap();
}

/// Helper: create a runner with a custom task config for the "double" task.
fn make_runner_with_config(
    task_config: TaskConfig,
) -> (
    PersistentTokioRunner,
    Arc<dyn Orchestrator>,
    Arc<dyn StateBackend>,
    Arc<dyn Broker>,
) {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());

    let task_id = TaskId::new("test", "double");
    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(
            task_id.clone(),
            task_config,
            Arc::new(|args_json: String| {
                let args: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&args_json).map_err(|e| {
                        RustvelloError::Serialization {
                            message: e.to_string(),
                        }
                    })?;
                let x: i64 = args.get("x").and_then(|v| v.parse().ok()).unwrap_or(0);
                serde_json::to_string(&(x * 2)).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            }),
        ))
        .unwrap();

    let runner = PersistentTokioRunner::new(
        "test-app".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::new(registry),
        None,
    );

    (runner, orchestrator, state_backend, broker)
}

async fn submit_invocation(
    orchestrator: &dyn Orchestrator,
    state_backend: &dyn StateBackend,
    broker: &dyn Broker,
    task_id: &TaskId,
    args: SerializedArguments,
) -> InvocationId {
    let call = CallDTO::new(task_id.clone(), args);
    let inv_id = orchestrator.register_invocation(&call).await.unwrap();
    let dto = InvocationDTO::new(inv_id.clone(), task_id.clone(), call.call_id.clone());
    state_backend.upsert_invocation(&dto, &call).await.unwrap();
    broker
        .route_invocation_for_task(&inv_id, task_id)
        .await
        .unwrap();
    inv_id
}

#[tokio::test]
async fn test_cc_task_level_blocks_second_invocation() {
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    let (runner, orchestrator, state_backend, broker) = make_runner_with_config(config);

    let task_id = TaskId::new("test", "double");
    let mut args1 = SerializedArguments::new();
    args1.insert("x", "1");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "2");

    let inv1 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args1).await;
    let inv2 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args2).await;

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let s1 = orchestrator.get_invocation_status(&inv1).await.unwrap();
    assert_eq!(s1.status, InvocationStatus::Success);

    let did_work = runner.run_one().await.unwrap();
    assert!(did_work);

    let s2 = orchestrator.get_invocation_status(&inv2).await.unwrap();
    assert_eq!(s2.status, InvocationStatus::Success);
}

#[tokio::test]
async fn test_cc_reroute_on_cc_re_enqueues() {
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    config.reroute_on_cc = true;
    let (runner, orchestrator, state_backend, broker) = make_runner_with_config(config);

    let task_id = TaskId::new("test", "double");
    let mut args1 = SerializedArguments::new();
    args1.insert("x", "5");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "6");

    let inv1 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args1).await;
    let _inv2 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args2).await;

    let fake_runner = RunnerId::from_string("fake-runner");
    orchestrator
        .index_for_concurrency_control(&inv1, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv1, InvocationStatus::Pending, Some(&fake_runner))
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv1, InvocationStatus::Running, Some(&fake_runner))
        .await
        .unwrap();

    let did_work = runner.run_one().await.unwrap();
    assert!(!did_work);

    let s2 = orchestrator.get_invocation_status(&_inv2).await.unwrap();
    assert_eq!(s2.status, InvocationStatus::Rerouted);
}

#[tokio::test]
async fn test_cc_final_rejection_without_reroute() {
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    config.reroute_on_cc = false;
    let (runner, orchestrator, state_backend, broker) = make_runner_with_config(config);

    let task_id = TaskId::new("test", "double");
    let mut args1 = SerializedArguments::new();
    args1.insert("x", "10");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "20");

    let inv1 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args1).await;
    let inv2 = submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args2).await;

    let fake_runner = RunnerId::from_string("fake-runner");
    orchestrator
        .index_for_concurrency_control(&inv1, &task_id, Some(&SerializedArguments::new()))
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv1, InvocationStatus::Pending, Some(&fake_runner))
        .await
        .unwrap();
    orchestrator
        .set_invocation_status(&inv1, InvocationStatus::Running, Some(&fake_runner))
        .await
        .unwrap();

    let did_work = runner.run_one().await.unwrap();
    assert!(!did_work);

    let s2 = orchestrator.get_invocation_status(&inv2).await.unwrap();
    assert_eq!(s2.status, InvocationStatus::ConcurrencyControlledFinal);
}

#[tokio::test]
async fn test_cc_unlimited_allows_all() {
    let config = TaskConfig::default();
    let (runner, orchestrator, state_backend, broker) = make_runner_with_config(config);

    let task_id = TaskId::new("test", "double");
    let mut args1 = SerializedArguments::new();
    args1.insert("x", "1");
    let mut args2 = SerializedArguments::new();
    args2.insert("x", "2");

    submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args1).await;
    submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args2).await;

    assert!(runner.run_one().await.unwrap());
    assert!(runner.run_one().await.unwrap());
}

#[tokio::test]
async fn test_cc_index_cleanup_on_success() {
    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);
    let (runner, orchestrator, state_backend, broker) = make_runner_with_config(config);

    let task_id = TaskId::new("test", "double");

    for i in 0..3 {
        let mut args = SerializedArguments::new();
        args.insert("x", &i.to_string());
        let inv =
            submit_invocation(&*orchestrator, &*state_backend, &*broker, &task_id, args).await;

        let did_work = runner.run_one().await.unwrap();
        assert!(did_work, "Invocation {} should have executed", i);

        let status = orchestrator.get_invocation_status(&inv).await.unwrap();
        assert_eq!(status.status, InvocationStatus::Success);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_cc_slot_is_atomic_across_runner_instances() {
    let broker: Arc<dyn Broker> = Arc::new(rustvello_mem::broker::MemBroker::new());
    let orchestrator: Arc<dyn Orchestrator> =
        Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new());
    let state_backend: Arc<dyn StateBackend> =
        Arc::new(rustvello_mem::state_backend::MemStateBackend::new());
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::new(AtomicBool::new(false));
    let release_first = Arc::new(AtomicBool::new(false));

    let mut config = TaskConfig::default();
    config.concurrency_control = ConcurrencyControlType::Task;
    config.running_concurrency = Some(1);

    let mut registry = TaskRegistry::new();
    let active_for_task = Arc::clone(&active);
    let peak_for_task = Arc::clone(&peak);
    let started_for_task = Arc::clone(&first_started);
    let release_for_task = Arc::clone(&release_first);
    registry
        .register(TaskDefinition::new(
            TaskId::new("test", "contended"),
            config,
            Arc::new(move |_args: String| {
                let now = active_for_task.fetch_add(1, Ordering::SeqCst) + 1;
                peak_for_task.fetch_max(now, Ordering::SeqCst);
                if now == 1 {
                    started_for_task.store(true, Ordering::SeqCst);
                    while !release_for_task.load(Ordering::SeqCst) {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
                active_for_task.fetch_sub(1, Ordering::SeqCst);
                Ok("null".to_string())
            }),
        ))
        .unwrap();

    let registry = Arc::new(registry);
    let runner_a = PersistentTokioRunner::new(
        "atomic-cc".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        Arc::clone(&registry),
        None,
    );
    let runner_b = PersistentTokioRunner::new(
        "atomic-cc".to_string(),
        AppConfig::default(),
        Arc::clone(&broker),
        Arc::clone(&orchestrator),
        Arc::clone(&state_backend),
        registry,
        None,
    );

    let task_id = TaskId::new("test", "contended");
    let first = submit_invocation(
        &*orchestrator,
        &*state_backend,
        &*broker,
        &task_id,
        SerializedArguments::new(),
    )
    .await;
    let second = submit_invocation(
        &*orchestrator,
        &*state_backend,
        &*broker,
        &task_id,
        SerializedArguments::new(),
    )
    .await;

    let a = tokio::spawn(async move { runner_a.run_one().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !first_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first runner did not start its invocation");
    let b = tokio::spawn(async move { runner_b.run_one().await });
    let b_result = tokio::time::timeout(Duration::from_secs(2), b).await;
    release_first.store(true, Ordering::SeqCst);
    a.await.unwrap().unwrap();
    b_result
        .expect("second runner did not complete its CC check")
        .unwrap()
        .unwrap();

    let statuses = [
        orchestrator
            .get_invocation_status(&first)
            .await
            .unwrap()
            .status,
        orchestrator
            .get_invocation_status(&second)
            .await
            .unwrap()
            .status,
    ];
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == InvocationStatus::Success)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == InvocationStatus::ConcurrencyControlledFinal)
            .count(),
        1
    );
}
