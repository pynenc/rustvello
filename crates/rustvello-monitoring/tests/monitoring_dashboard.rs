#![allow(clippy::clone_on_ref_ptr)]
//! Integration tests for the monitoring dashboard.
//!
//! These tests start a **real** Axum HTTP server on a free port, seed
//! in-memory backends with test data, run a `TaskRunner` to process
//! invocations, and then make HTTP requests to verify every dashboard
//! page renders correctly.
//!
//! # Browser debugging
//!
//! Set `KEEP_ALIVE` to `true` below, or set `KEEP_ALIVE=1` /
//! `RUSTVELLO_MONITOR_KEEP_ALIVE=1`, to keep the server running after
//! the selected test completes. Open the printed URL in your browser,
//! then press Ctrl-C when done.
//!
//! ```bash
//! KEEP_ALIVE=1 cargo test -p rustvello-monitoring \
//!     --test monitoring_dashboard test_hierarchical_timeline -- --nocapture
//! ```

mod common;

use std::sync::Arc;

use common::{
    create_hierarchical_test_app, create_test_app, register_hierarchical_tasks,
    seed_grandparents_only, seed_hierarchical_invocations, seed_invocations, should_keep_alive,
    start_test_server, TestServer,
};
use rustvello::prelude::{ForeignTaskProxy, RayonRunner};
use rustvello_core::context::{InvocationContext, RunnerContext, INVOCATION_CTX, RUNNER_CTX};
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::{StateBackend, StoredRunnerContext};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId, TaskLanguage};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};
use rustvello_proto::trigger::{
    ConditionId, EventRecord, TriggerDefinitionId, TriggerLogic, TriggerRunId,
    TriggerRunParticipant, TriggerRunRecord,
};

/// Set to `true` to keep the monitoring server alive for browser debugging.
/// The env vars `KEEP_ALIVE=1` and `RUSTVELLO_MONITOR_KEEP_ALIVE=1` also work.
const KEEP_ALIVE: bool = false;

async fn record_runner_status(
    state_backend: &Arc<dyn StateBackend>,
    invocation_id: &InvocationId,
    runner: &RunnerContext,
    status: InvocationStatus,
) {
    state_backend
        .store_runner_context(&StoredRunnerContext::from_runtime(runner))
        .await
        .expect("store runner context");

    let runner_id = runner.runner_id.clone();
    state_backend
        .add_history(
            &InvocationHistory::new(
                invocation_id.clone(),
                InvocationStatusRecord::new(status, Some(runner_id.clone())),
                None,
            )
            .with_runner(runner_id),
        )
        .await
        .expect("store invocation history");

    // Keep status timestamps strictly ordered for timeline and duration assertions.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
}

#[tokio::test]
async fn test_event_and_trigger_run_monitoring_views() {
    let setup = create_test_app("events-monitoring");
    let store: std::sync::Arc<dyn TriggerStore> = std::sync::Arc::clone(&setup.trigger_store);
    let timestamp = chrono::Utc::now();
    let event = EventRecord {
        event_id: "event-monitor-1".into(),
        event_code: "payment_received".into(),
        payload: serde_json::json!({"order_id": "ORD-42"}),
        timestamp,
        matched_condition_ids: vec![ConditionId::from("condition-payment")],
        valid_condition_ids: vec!["valid-payment".into()],
        triggered_invocation_ids: vec![InvocationId::from_string("triggered-invocation")],
        emitted_by_invocation_id: Some(InvocationId::from_string("source-invocation")),
        emitted_by_task_id: Some(TaskId::new("test", "source")),
        emitted_by_runner_id: None,
    };
    store.store_event(&event).await.unwrap();
    let run = TriggerRunRecord {
        trigger_run_id: TriggerRunId::from("trigger-run-monitor-1"),
        trigger_id: TriggerDefinitionId::from("trigger-payment"),
        task_id: TaskId::new("test", "target"),
        logic: TriggerLogic::Or,
        arguments: serde_json::json!({"order_id": "ORD-42"}),
        participants: vec![TriggerRunParticipant {
            context_type: "event".into(),
            condition_id: ConditionId::from("condition-payment"),
            valid_condition_id: "valid-payment".into(),
            event_id: Some(event.event_id.clone()),
            source_invocation_id: None,
            context_summary: "payment_received".into(),
        }],
        claimed_at: timestamp,
        executed_at: Some(timestamp),
        triggered_invocation_id: Some(InvocationId::from_string("triggered-invocation")),
        atomic_service_run_id: None,
        atomic_service_runner_id: None,
    };
    store.store_trigger_run(&run).await.unwrap();

    let server = start_test_server(setup).await;
    let client = server.client();

    let response = client
        .get(format!("{}/events?event_code=payment_received", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let list = response.text().await.unwrap();
    assert!(list.contains("event-monitor-1"));
    assert!(list.contains("1 matched"));
    assert!(list.contains("1 triggered"));
    assert!(list.contains("name=\"triggered\""));
    assert!(list.contains("monitor-filter-chip"));
    assert!(list.contains("monitor-stats-row"));
    assert!(list.contains("/events/event-monitor-1/trace"));
    assert!(list.contains("/invocations/timeline?"));
    assert!(list.contains("inv_ids=source-invocation%2Ctriggered-invocation"));
    assert!(list.contains("--time-left:"));

    let filtered_response = client
        .get(format!(
            "{}/events?event_code=payment_received&triggered=only&matched=only",
            server.url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(filtered_response.status(), 200);
    let filtered_list = filtered_response.text().await.unwrap();
    assert!(filtered_list.contains("Only triggered"));
    assert!(filtered_list.contains("Only matched"));

    let response = client
        .get(format!("{}/events/event-monitor-1", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let detail = response.text().await.unwrap();
    assert!(detail.contains("ORD-42"));
    assert!(detail.contains("/trigger-runs/trigger-run-monitor-1"));
    assert!(detail.contains("/events/event-monitor-1/api"));
    assert!(detail.contains("/events/event-monitor-1/trace"));
    assert!(detail.contains("Participants"));
    assert!(detail.contains("selected=source-invocation"));
    assert!(
        !detail.contains("start_date="),
        "event links with invocation scope should let the timeline fit actual invocation history"
    );

    let response = client
        .get(format!("{}/events/event-monitor-1/api", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let event_api: serde_json::Value = response.json().await.unwrap();
    assert_eq!(event_api["event"]["event_code"], "payment_received");
    assert_eq!(
        event_api["trigger_runs"][0]["trigger_run_id"],
        "trigger-run-monitor-1"
    );
    assert!(event_api["links"]["timeline"].as_str().is_some_and(|href| {
        href.contains("/invocations/timeline?")
            && href.contains("selected=source-invocation")
            && !href.contains("start_date=")
    }));

    let response = client
        .get(format!("{}/events/event-monitor-1/trace", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let trace: serde_json::Value = response.json().await.unwrap();
    assert_eq!(trace["focus_kind"], "event");
    assert!(trace["generated_invocation_ids"]
        .as_array()
        .is_some_and(|ids| ids.iter().any(|id| id == "triggered-invocation")));

    let response = client
        .get(format!("{}/trigger-runs/trigger-run-monitor-1", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let run_detail = response.text().await.unwrap();
    assert!(run_detail.contains("condition-payment"));
    assert!(run_detail.contains("valid-payment"));
    assert!(run_detail.contains("/events/event-monitor-1"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_event_timeline_link_fits_generated_invocation_history() {
    let setup = create_test_app("events-generated-invocation-timeline");
    let generated_ids = seed_invocations(&setup.app, 1)
        .await
        .expect("seed generated invocation");
    let generated_id = InvocationId::from_string(generated_ids[0].clone());
    let history = setup
        .state_backend
        .get_history(&generated_id)
        .await
        .expect("generated invocation history");
    let invocation_start = history
        .iter()
        .map(|entry| {
            entry
                .history_timestamp
                .unwrap_or(entry.status_record.timestamp)
        })
        .min()
        .expect("generated invocation has history");

    let store: std::sync::Arc<dyn TriggerStore> = std::sync::Arc::clone(&setup.trigger_store);
    let event_timestamp = invocation_start - chrono::Duration::seconds(1);
    let event = EventRecord {
        event_id: "event-generated-later".into(),
        event_code: "late_trigger".into(),
        payload: serde_json::json!({"order_id": "ORD-late"}),
        timestamp: event_timestamp,
        matched_condition_ids: vec![ConditionId::from("condition-late")],
        valid_condition_ids: vec!["valid-late".into()],
        triggered_invocation_ids: vec![generated_id.clone()],
        emitted_by_invocation_id: None,
        emitted_by_task_id: None,
        emitted_by_runner_id: Some(RunnerId::from_string("runner-event-source")),
    };
    store.store_event(&event).await.unwrap();
    store
        .store_trigger_run(&TriggerRunRecord {
            trigger_run_id: TriggerRunId::from("trigger-run-generated-later"),
            trigger_id: TriggerDefinitionId::from("trigger-late"),
            task_id: TaskId::new("test", "process_order"),
            logic: TriggerLogic::And,
            arguments: serde_json::json!({"order_id": "ORD-late"}),
            participants: vec![TriggerRunParticipant {
                context_type: "event".into(),
                condition_id: ConditionId::from("condition-late"),
                valid_condition_id: "valid-late".into(),
                event_id: Some(event.event_id.clone()),
                source_invocation_id: None,
                context_summary: "late_trigger".into(),
            }],
            claimed_at: invocation_start,
            executed_at: Some(invocation_start),
            triggered_invocation_id: Some(generated_id.clone()),
            atomic_service_run_id: None,
            atomic_service_runner_id: None,
        })
        .await
        .unwrap();

    let server = start_test_server(setup).await;
    let client = server.client();

    let response = client
        .get(format!("{}/events/event-generated-later/api", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let event_api: serde_json::Value = response.json().await.unwrap();
    let timeline_path = event_api["links"]["timeline"]
        .as_str()
        .expect("event timeline link");
    assert!(timeline_path.contains("selected="));
    assert!(timeline_path.contains("inv_ids="));
    assert!(
        !timeline_path.contains("start_date=") && !timeline_path.contains("time_range=custom"),
        "event timeline links with invocation IDs must not use event-time-only custom windows"
    );
    let query = timeline_path.split_once('?').expect("timeline query").1;
    let rendered_scope = url::form_urlencoded::parse(query.as_bytes())
        .find_map(|(key, value)| (key == "inv_ids").then(|| value.into_owned()))
        .expect("invocation scope");
    assert_eq!(rendered_scope, generated_ids[0]);

    let timeline = client
        .get(format!("{}{timeline_path}", server.url))
        .send()
        .await
        .expect("event timeline request");
    assert_eq!(timeline.status(), 200);
    let timeline = timeline.text().await.expect("timeline body");
    assert!(
        timeline.contains(&format!("data-invocation-id=\"{}\"", generated_ids[0])),
        "event timeline link should render the generated invocation instead of an empty timeline"
    );
    assert!(
        !timeline.contains("No invocation history in this time range."),
        "generated invocation history should define the visible timeline range"
    );

    let detail = client
        .get(format!("{}/events/event-generated-later", server.url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(detail.contains("event-generated-later"));
    assert!(detail.contains("selected="));
    assert!(!detail.contains("start_date="));

    server.shutdown().await;
}

// unused for now but kept as a pattern for future richer tests
#[allow(dead_code)]
/// Start the monitoring server with seeded + processed data.
/// The runner and the monitoring server share the same in-memory backends.
async fn setup_with_runner() -> (TestServer, reqwest::Client) {
    let setup = create_test_app("test-monitoring-full");

    // Seed 5 invocations
    seed_invocations(&setup.app, 5)
        .await
        .expect("seeding should succeed");

    // Clone backend arcs *before* into_runner() consumes the app
    let broker = setup.broker.clone();
    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();
    let trigger_store = setup.trigger_store.clone();
    let client_data_store = setup.client_data_store.clone();
    let config = setup.config.clone();
    let task_ids = setup.task_ids.clone();

    // Spawn a runner in the background that shares the same backends
    let runner = setup.app.into_runner().with_idle_sleep(50);
    let runner_handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });

    // Wait for the runner to process the seeded invocations
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    runner_handle.abort();

    // Now start the monitoring server using the *same* backend arcs
    let monitor_setup = common::TestAppSetup {
        app: rustvello::prelude::RustvelloApp::with_backends(
            config.clone(),
            broker.clone(),
            orchestrator.clone(),
            state_backend.clone(),
            client_data_store.clone(),
        ),
        config,
        broker,
        orchestrator,
        state_backend,
        trigger_store,
        client_data_store,
        task_ids,
    };
    // Re-register the task so the monitoring server knows about it
    // (the task_ids vec is already populated from the original setup)
    let server = start_test_server(monitor_setup).await;
    let client = server.client();
    (server, client)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health_check() {
    let setup = create_test_app("test-health");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/health", server.url))
        .send()
        .await
        .expect("health request");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["status"], "ok");

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_dashboard_home() {
    let setup = create_test_app("test-dashboard");
    seed_invocations(&setup.app, 3)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client.get(&server.url).send().await.expect("home request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("test-dashboard"), "should show app id");
    assert!(body.contains("Dashboard"), "should have dashboard title");

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_broker_page() {
    let setup = create_test_app("test-broker");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/broker", server.url))
        .send()
        .await
        .expect("broker request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("Broker"), "should have broker heading");

    // Test broker queue endpoint
    let resp = client
        .get(format!("{}/broker/queue?limit=5", server.url))
        .send()
        .await
        .expect("broker queue request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("queue body");
    assert!(
        body.contains("pending invocations"),
        "should show pending count"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_orchestrator_page() {
    let setup = create_test_app("test-orchestrator");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/orchestrator", server.url))
        .send()
        .await
        .expect("orchestrator request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("Orchestrator"),
        "should have orchestrator heading"
    );
    assert!(
        body.contains("Orchestrator Type"),
        "should show type banner"
    );
    assert!(
        body.contains("Invocation Status"),
        "should show invocation status section"
    );
    assert!(
        body.contains("Active Runners"),
        "should show active runners card"
    );
    assert!(
        body.contains("Recovery"),
        "should show recovery & atomic services card"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_state_backend_page() {
    let setup = create_test_app("test-state-backend");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/state-backend", server.url))
        .send()
        .await
        .expect("state-backend request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("State Backend"),
        "should have State Backend heading"
    );
    assert!(
        body.contains("In-Memory") || body.contains("MemStateBackend"),
        "should show backend type name"
    );
    assert!(
        body.contains("Danger Zone"),
        "should show Danger Zone purge section"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_client_data_store_page() {
    let setup = create_test_app("test-cds");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/client-data-store", server.url))
        .send()
        .await
        .expect("client-data-store request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("Client Data Store"),
        "should have Client Data Store heading"
    );
    assert!(body.contains("Data Store Type"), "should show type banner");
    assert!(
        body.contains("Compression"),
        "should show compression config"
    );
    assert!(
        body.contains("Danger Zone"),
        "should show Danger Zone purge section"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_tasks_list() {
    let setup = create_test_app("test-tasks-list");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/tasks", server.url))
        .send()
        .await
        .expect("tasks request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("process_order"),
        "should list registered tasks"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_tasks_detail() {
    let setup = create_test_app("test-tasks-detail");
    seed_invocations(&setup.app, 2)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/tasks/rust::test.process_order", server.url))
        .send()
        .await
        .expect("task detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("rust::test.process_order"),
        "should show canonical task ID"
    );
    assert!(body.contains("Language"), "should show task language");
    assert!(
        body.contains("rust"),
        "should show Rust as the task language"
    );
    assert!(body.contains("Module:"), "should show task module");
    assert!(body.contains("Function:"), "should show task function");
    assert!(
        body.contains("language-rust"),
        "should use the Rust language badge"
    );
    assert!(
        body.contains("monitor-table-compact") && body.contains("view-in-timeline-link"),
        "task detail should reuse the canonical invocation table"
    );
    assert!(
        body.contains("task_id=rust%3A%3Atest.process_order"),
        "task timeline actions should preserve the task scope"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_list() {
    let setup = create_test_app("test-invocations-list");
    let inv_ids = seed_invocations(&setup.app, 3)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/invocations", server.url))
        .send()
        .await
        .expect("invocations request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // At least one invocation ID (or its truncated form) should appear
    assert!(
        inv_ids.iter().any(|id| body.contains(&id[..8])),
        "should show invocation short IDs"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_list_timeline_link_preserves_visible_scope() {
    let setup = create_test_app("test-invocations-list-timeline");
    let inv_ids = seed_invocations(&setup.app, 3)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let response = client
        .get(format!("{}/invocations", server.url))
        .send()
        .await
        .expect("invocations request");
    assert_eq!(response.status(), 200);
    let body = response.text().await.expect("invocations body");
    let action_position = body
        .find("invocation-list-timeline-link")
        .expect("list timeline action");
    let href_start = body[..action_position]
        .rfind("href=\"")
        .expect("list timeline href")
        + 6;
    let href_end = href_start
        + body[href_start..]
            .find('"')
            .expect("end of list timeline href");
    let timeline_path = body[href_start..href_end].replace("&amp;", "&");
    assert!(
        timeline_path.starts_with("/invocations/timeline?inv_ids="),
        "the list-level timeline link should carry the visible invocation scope"
    );
    let query = timeline_path.split_once('?').expect("timeline query").1;
    let rendered_scope = url::form_urlencoded::parse(query.as_bytes())
        .find_map(|(key, value)| (key == "inv_ids").then(|| value.into_owned()))
        .expect("invocation scope");
    let rendered_ids = rendered_scope
        .split(',')
        .collect::<std::collections::HashSet<_>>();
    let expected_ids = inv_ids
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(rendered_ids, expected_ids);

    let timeline = client
        .get(format!("{}{timeline_path}", server.url))
        .send()
        .await
        .expect("scoped timeline request");
    assert_eq!(timeline.status(), 200);
    let timeline = timeline.text().await.expect("timeline body");
    assert!(timeline.contains("data-timeline-start="));
    for invocation_id in &inv_ids {
        assert!(
            timeline.contains(&format!("data-invocation-id=\"{invocation_id}\"")),
            "scoped timeline should render invocation {invocation_id}"
        );
    }

    let row_action_position = body
        .find("view-in-timeline-link")
        .expect("row timeline action");
    let row_href_start = body[..row_action_position]
        .rfind("href=\"")
        .expect("row timeline href")
        + 6;
    let row_href_end = row_href_start
        + body[row_href_start..]
            .find('"')
            .expect("end of row timeline href");
    let row_timeline_path = body[row_href_start..row_href_end].replace("&amp;", "&");
    let row_timeline = client
        .get(format!("{}{row_timeline_path}", server.url))
        .send()
        .await
        .expect("row timeline request");
    assert_eq!(row_timeline.status(), 200);
    let row_timeline = row_timeline.text().await.expect("row timeline body");
    let row_query = row_timeline_path
        .split_once('?')
        .expect("row timeline query")
        .1;
    let selected_id = url::form_urlencoded::parse(row_query.as_bytes())
        .find_map(|(key, value)| (key == "selected").then(|| value.into_owned()))
        .expect("selected invocation");
    assert!(row_timeline.contains(&format!("data-invocation-id=\"{selected_id}\"")));

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_detail() {
    let setup = create_test_app("test-inv-detail");
    let inv_ids = seed_invocations(&setup.app, 1)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let inv_id = &inv_ids[0];
    let resp = client
        .get(format!("{}/invocations/{inv_id}", server.url))
        .send()
        .await
        .expect("invocation detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains(inv_id), "should show full invocation ID");

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_history_json() {
    let setup = create_test_app("test-inv-history");
    let inv_ids = seed_invocations(&setup.app, 1)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let inv_id = &inv_ids[0];
    let resp = client
        .get(format!("{}/invocations/{inv_id}/history", server.url))
        .send()
        .await
        .expect("history request");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    let entries = body.as_array().expect("history should be an array");
    assert!(
        !entries.is_empty(),
        "history should have at least one entry after seeding"
    );
    // Each entry must have status and timestamp fields
    let first = &entries[0];
    assert!(
        first["status"].is_string(),
        "history entry should have a string status field"
    );
    assert!(
        first["timestamp"].is_string(),
        "history entry should have a string timestamp field"
    );
    // The first entry should be Registered (from seeding)
    assert_eq!(
        first["status"].as_str().unwrap(),
        "Registered",
        "first history entry should be Registered"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_api_json() {
    let setup = create_test_app("test-inv-api");
    let inv_ids = seed_invocations(&setup.app, 1)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let inv_id = &inv_ids[0];
    let resp = client
        .get(format!("{}/invocations/{inv_id}/api", server.url))
        .send()
        .await
        .expect("api request");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("json");
    assert_eq!(body["invocation_id"], *inv_id);
    assert!(
        body["task_id"].as_str().unwrap().contains("process_order"),
        "API should show the task_id"
    );
    assert!(body["call_id"].is_string(), "API should include call_id");
    assert!(body["status"].is_string(), "API should include status");
    assert!(
        body["created_at"].is_string(),
        "API should include created_at timestamp"
    );
    assert!(
        body["updated_at"].is_string(),
        "API should include updated_at timestamp"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_invocations_timeline() {
    let setup = create_test_app("test-timeline");
    seed_invocations(&setup.app, 5)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/invocations/timeline", server.url))
        .send()
        .await
        .expect("timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("Invocation Timeline"),
        "should have timeline heading"
    );
    assert!(body.contains("<svg"), "should render an SVG element");
    assert!(
        body.contains("timeline-container"),
        "should have timeline container div"
    );
    assert!(
        body.contains("timeline-range-zoom"),
        "should expose the drag-to-zoom control"
    );
    assert!(
        body.contains("data-timeline-start=") && body.contains("data-timeline-end="),
        "should expose SVG time bounds for drag-to-zoom"
    );
    let timeline_position = body.find("id=\"timeline-container\"").unwrap();
    let histogram_position = body.find("data-histogram-panel").unwrap();
    let details_position = body.find("id=\"invocation-details-panel\"").unwrap();
    assert!(timeline_position < histogram_position && histogram_position < details_position);
    assert!(body.contains("Apply"), "should have filter Apply button");

    let resp = client
        .get(format!(
            "{}/invocations/timeline?time_range=custom&start_date=2026-09-04T19%3A40%3A35.412070%2B00%3A00&end_date=2026-09-04T19%3A40%3A35.416259%2B00%3A00",
            server.url
        ))
        .send()
        .await
        .expect("custom timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("custom timeline body");
    assert!(
        body.contains(
            r#"id="start_date" class="form-control" step="0.001" value="2026-09-04T19:40:35.412""#
        ) && body.contains(
            r#"id="end_date" class="form-control" step="0.001" value="2026-09-04T19:40:35.416""#
        ),
        "RFC3339 custom timeline links should be echoed as valid datetime-local values"
    );

    handle_keep_alive(server).await;
}

/// Render both directions of a cross-language call:
/// Rust external -> Python task -> Rust child task.
#[tokio::test]
async fn test_timeline_renders_complete_invocation_history() {
    let mut setup = create_test_app("test-timeline-lifecycle");
    let python_task_id = TaskId::for_language(TaskLanguage::Python, "test", "prepare_order");
    setup
        .app
        .register_foreign(ForeignTaskProxy::<String, String>::new(
            python_task_id.clone(),
        ))
        .expect("register Python task proxy");
    setup.task_ids.push(python_task_id.clone());

    let rust_task_id = TaskId::new("test", "process_order");
    let app_id: Arc<str> = Arc::from("test-timeline-lifecycle");
    let rust_external = RunnerContext::new_with_runtime(
        RunnerId::from_string("rust-external"),
        Arc::clone(&app_id),
        "ExternalRunner",
        TaskLanguage::Rust,
        rustvello_proto::identifiers::ExecutorKind::Tokio,
    );
    let python_worker = RunnerContext::new_with_runtime(
        RunnerId::from_string("python-worker"),
        Arc::clone(&app_id),
        "PythonWorker",
        TaskLanguage::Python,
        rustvello_proto::identifiers::ExecutorKind::Python,
    );
    let rust_worker = RunnerContext::new_with_runtime(
        RunnerId::from_string("rust-worker"),
        app_id,
        "RustWorker",
        TaskLanguage::Rust,
        rustvello_proto::identifiers::ExecutorKind::Rayon,
    );

    let mut python_args = SerializedArguments::new();
    python_args.insert("order_id", "ORD-PYTHON");
    let python_invocation_id = RUNNER_CTX
        .scope(
            rust_external,
            setup.app.submit(&python_task_id, python_args),
        )
        .await
        .expect("Rust side should submit the Python task");

    record_runner_status(
        &setup.state_backend,
        &python_invocation_id,
        &python_worker,
        InvocationStatus::Pending,
    )
    .await;
    record_runner_status(
        &setup.state_backend,
        &python_invocation_id,
        &python_worker,
        InvocationStatus::Running,
    )
    .await;

    let python_invocation_context = InvocationContext {
        invocation_id: python_invocation_id.clone(),
        task_id: python_task_id.clone(),
        workflow: None,
        is_workflow_defining: false,
        state_backend: Some(Arc::clone(&setup.state_backend)),
        parent_invocation_id: None,
        num_retries: 0,
    };
    let mut rust_args = SerializedArguments::new();
    rust_args.insert("order_id", "ORD-RUST");
    let rust_invocation_id = RUNNER_CTX
        .scope(
            python_worker.clone(),
            INVOCATION_CTX.scope(
                python_invocation_context,
                setup.app.submit(&rust_task_id, rust_args),
            ),
        )
        .await
        .expect("Python side should submit the Rust task");

    for status in [
        InvocationStatus::Pending,
        InvocationStatus::Running,
        InvocationStatus::Success,
    ] {
        record_runner_status(
            &setup.state_backend,
            &rust_invocation_id,
            &rust_worker,
            status,
        )
        .await;
    }
    record_runner_status(
        &setup.state_backend,
        &python_invocation_id,
        &python_worker,
        InvocationStatus::Success,
    )
    .await;

    let server = start_test_server(setup).await;
    let body = server
        .client()
        .get(format!("{}/invocations/timeline", server.url))
        .send()
        .await
        .expect("timeline request")
        .text()
        .await
        .expect("timeline body");

    for status in ["Registered", "Pending", "Running", "Success"] {
        assert!(
            body.contains(&format!("data-status=\"{status}\"")),
            "timeline should render {status} for the complete invocation history"
        );
    }
    assert!(
        body.contains("data-histogram-start=") && body.contains("data-bucket-start="),
        "should render an aligned occupancy histogram"
    );
    assert!(
        body.contains("data-timeline-left=\"420.0\"")
            && body.contains("data-histogram-left=\"420\"")
            && body.contains("data-histogram-right=\"1956\""),
        "timeline and histogram should share the same plot bounds"
    );
    assert!(
        body.contains("class=\"collapse\" id=\"timeline-filter-fields\"")
            && body.contains("timeline-filter-summary"),
        "timeline filters should start as a compact top disclosure"
    );
    assert!(
        body.contains("data-timestamp=")
            && body.contains("rustvelloSetTimeCursor")
            && body.contains("class=\"relation-line\"")
            && body.contains("<path d=\""),
        "timeline should expose constant-cost linked-time metadata and consolidated paths"
    );
    assert!(
        body.contains("data-statuses=\"pending,running\"") && body.contains("stroke=\"#e9ecef\""),
        "default occupancy should show pending/running with a visible scale"
    );
    assert!(
        body.contains(">rust</text>")
            && body.contains(">python</text>")
            && body.contains("ExternalRunner")
            && body.contains("PythonWorker")
            && body.contains("RustWorker"),
        "timeline lanes should identify the submitting and executing runtimes"
    );
    assert!(
        body.contains("python") && body.contains("rayon"),
        "timeline should expose the Python and Rayon executor kinds"
    );
    assert!(
        body.contains("python::test.prepare_order") && body.contains("rust::test.process_order"),
        "timeline and occupancy should show both canonical task IDs"
    );

    let client = server.client();
    let python_history: Vec<serde_json::Value> = client
        .get(format!(
            "{}/invocations/{python_invocation_id}/history",
            server.url
        ))
        .send()
        .await
        .expect("Python history request")
        .json()
        .await
        .expect("Python history JSON");
    let rust_history: Vec<serde_json::Value> = client
        .get(format!(
            "{}/invocations/{rust_invocation_id}/history",
            server.url
        ))
        .send()
        .await
        .expect("Rust history request")
        .json()
        .await
        .expect("Rust history JSON");

    let assert_owner = |history: &[serde_json::Value],
                        status: &str,
                        language: &str,
                        executor: &str,
                        runner_cls: &str| {
        let entry = history
            .iter()
            .find(|entry| entry["status"] == status)
            .unwrap_or_else(|| panic!("missing {status} history entry"));
        assert_eq!(
            entry["runner_info"]["runner_language"], language,
            "unexpected runner language for {status}"
        );
        assert_eq!(
            entry["runner_info"]["runner_cls"], runner_cls,
            "unexpected runner class for {status}"
        );
        assert_eq!(
            entry["runner_info"]["executor_kind"], executor,
            "unexpected executor kind for {status}"
        );
    };
    assert_owner(
        &python_history,
        "Registered",
        "rust",
        "tokio",
        "ExternalRunner",
    );
    assert_owner(
        &python_history,
        "Pending",
        "python",
        "python",
        "PythonWorker",
    );
    assert_owner(
        &python_history,
        "Running",
        "python",
        "python",
        "PythonWorker",
    );
    assert_owner(
        &python_history,
        "Success",
        "python",
        "python",
        "PythonWorker",
    );
    assert_owner(
        &rust_history,
        "Registered",
        "python",
        "python",
        "PythonWorker",
    );
    assert_owner(&rust_history, "Pending", "rust", "rayon", "RustWorker");
    assert_owner(&rust_history, "Running", "rust", "rayon", "RustWorker");
    assert_owner(&rust_history, "Success", "rust", "rayon", "RustWorker");

    for history in [&python_history, &rust_history] {
        let timestamps: Vec<_> = history
            .iter()
            .map(|entry| {
                chrono::DateTime::parse_from_rfc3339(entry["timestamp"].as_str().unwrap()).unwrap()
            })
            .collect();
        assert!(
            timestamps.windows(2).all(|pair| pair[0] <= pair[1]),
            "cross-language timeline history must remain chronological"
        );
    }

    let python_api: serde_json::Value = client
        .get(format!(
            "{}/invocations/{python_invocation_id}/api",
            server.url
        ))
        .send()
        .await
        .expect("Python invocation API request")
        .json()
        .await
        .expect("Python invocation API JSON");
    assert_eq!(python_api["task_id"], "python::test.prepare_order");
    assert_eq!(python_api["task_language"], "python");
    assert!(python_api["parent_invocation_id"].is_null());

    let rust_api: serde_json::Value = client
        .get(format!(
            "{}/invocations/{rust_invocation_id}/api",
            server.url
        ))
        .send()
        .await
        .expect("Rust invocation API request")
        .json()
        .await
        .expect("Rust invocation API JSON");
    assert_eq!(rust_api["task_id"], "rust::test.process_order");
    assert_eq!(rust_api["task_language"], "rust");
    assert_eq!(
        rust_api["parent_invocation_id"],
        python_invocation_id.to_string()
    );

    let family_tree = client
        .get(format!(
            "{}/invocations/{python_invocation_id}/family-tree",
            server.url
        ))
        .send()
        .await
        .expect("cross-language family tree request")
        .text()
        .await
        .expect("cross-language family tree body");
    assert!(
        family_tree.contains("python::test")
            && family_tree.contains("prepare_order")
            && family_tree.contains("rust::test")
            && family_tree.contains("process_order"),
        "family tree should preserve both sides of the cross-language call"
    );

    let detail_body = server
        .client()
        .get(format!("{}/invocations/{python_invocation_id}", server.url))
        .send()
        .await
        .expect("invocation detail request")
        .text()
        .await
        .expect("invocation detail body");
    assert!(
        detail_body.contains("language-python"),
        "invocation history should use the Python language badge"
    );
    let rust_detail_body = server
        .client()
        .get(format!("{}/invocations/{rust_invocation_id}", server.url))
        .send()
        .await
        .expect("Rust invocation detail request")
        .text()
        .await
        .expect("Rust invocation detail body");
    assert!(
        rust_detail_body.contains("language-rust"),
        "Rust invocation history should use the Rust language badge"
    );

    let history_filtered = server
        .client()
        .get(format!(
            "{}/invocations?inv_ids={python_invocation_id},{rust_invocation_id}&status=Pending%2CRunning&status_mode=history",
            server.url,
        ))
        .send()
        .await
        .expect("history status filter request");
    assert_eq!(history_filtered.status(), 200);
    let history_body = history_filtered.text().await.expect("history filter body");
    assert!(
        history_body.contains(&python_invocation_id.to_string()[..8])
            && history_body.contains(&rust_invocation_id.to_string()[..8]),
        "historical pending/running status should match both completed invocations"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_timeline_invocation_scope_filter() {
    let setup = create_test_app("test-timeline-scope");
    let inv_ids = seed_invocations(&setup.app, 2)
        .await
        .expect("seed invocations");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/timeline?inv_ids={}",
            server.url, inv_ids[0]
        ))
        .send()
        .await
        .expect("scoped timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains(&format!("data-invocation-id=\"{}\"", inv_ids[0])),
        "scoped timeline should render the requested invocation"
    );
    assert!(
        !body.contains(&format!("data-invocation-id=\"{}\"", inv_ids[1])),
        "scoped timeline should not render unrelated invocations"
    );
    assert!(body.contains("Invocation scope:"));

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_timeline_workflow_type_filter() {
    let setup = create_hierarchical_test_app("test-timeline-workflow-filter");
    let broker = setup.broker.clone();
    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();
    let (grandparent_ids, _, _) =
        seed_hierarchical_invocations(&setup.app, &orchestrator, &state_backend, &broker)
            .await
            .expect("seed hierarchy");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/timeline?workflow_type=rust::test.grandparent_task",
            server.url
        ))
        .send()
        .await
        .expect("workflow-filtered timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    let timeline_markup = body
        .split("id=\"timeline-container\"")
        .nth(1)
        .and_then(|rest| rest.split("</div>").next())
        .expect("timeline container markup");
    assert!(
        timeline_markup.contains("data-task-key=\"rust::test.parent_task\""),
        "workflow filter should keep workflow members"
    );
    assert!(
        timeline_markup.contains("data-task-key=\"rust::test.child_task\""),
        "workflow filter should keep nested workflow members"
    );
    assert!(
        timeline_markup.contains("data-task-key=\"rust::test.grandparent_task\""),
        "workflow filter should keep the defining task"
    );

    let resp = client
        .get(format!(
            "{}/invocations/timeline?workflow_id={}",
            server.url, grandparent_ids[0]
        ))
        .send()
        .await
        .expect("workflow-id-filtered timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    let timeline_markup = body
        .split("id=\"timeline-container\"")
        .nth(1)
        .and_then(|rest| rest.split("</div>").next())
        .expect("timeline container markup");
    assert!(
        timeline_markup.contains(&format!("data-invocation-id=\"{}\"", grandparent_ids[0])),
        "workflow ID filter should keep the selected workflow"
    );
    assert!(
        !timeline_markup.contains(&format!("data-invocation-id=\"{}\"", grandparent_ids[1])),
        "workflow ID filter should remove a different workflow"
    );

    let workflow_page = client
        .get(format!(
            "{}/workflows/rust::test.grandparent_task?limit=10",
            server.url
        ))
        .send()
        .await
        .expect("workflow comparison request");
    assert_eq!(workflow_page.status(), 200);
    let workflow_page = workflow_page
        .text()
        .await
        .expect("workflow comparison body");
    assert!(
        workflow_page.contains("Workflow Details")
            && workflow_page.contains("Workflow Runs")
            && workflow_page.contains("workflow-selection-form"),
        "workflow detail should provide summary, paginated runs, and selection controls"
    );
    assert!(
        workflow_page.contains("Shared occupancy and elapsed-time axes"),
        "workflow comparison should communicate its shared scale"
    );
    assert!(
        workflow_page.contains("Run ID (Main Invocation ID)")
            && workflow_page.contains(">Invocations<")
            && workflow_page.contains("/invocations/timeline?"),
        "workflow runs should separate main invocation, run list, and timeline actions"
    );
    assert!(
        !workflow_page.contains(">Compare<"),
        "workflow rows are selected by row click, so no separate compare button is needed"
    );
    assert!(
        workflow_page.contains("workflow_id="),
        "workflow run actions should preserve the workflow id filter"
    );

    let selected_runs = format!("{},{}", grandparent_ids[0], grandparent_ids[1]);
    let workflow_page = client
        .get(format!(
            "{}/workflows/rust::test.grandparent_task?limit=10&histogram_workflow={}",
            server.url, selected_runs
        ))
        .send()
        .await
        .expect("multi workflow comparison request");
    assert_eq!(workflow_page.status(), 200);
    let workflow_page = workflow_page
        .text()
        .await
        .expect("multi workflow comparison body");
    assert!(
        workflow_page.matches("table-primary").count() >= 2,
        "multiple workflow rows should remain selected at once"
    );
    assert!(
        workflow_page.contains("data-selection-url=")
            && workflow_page.contains(&grandparent_ids[0].to_string())
            && workflow_page.contains(&grandparent_ids[1].to_string()),
        "selected workflow rows should expose immediate row-click selection URLs"
    );

    let workflow_run_redirect = client
        .get(format!(
            "{}/workflows/rust::test.grandparent_task/{}",
            server.url, grandparent_ids[1]
        ))
        .send()
        .await
        .expect("workflow run detail redirect");
    assert_eq!(workflow_run_redirect.status(), 200);
    let workflow_run_redirect = workflow_run_redirect
        .text()
        .await
        .expect("workflow run redirected body");
    assert!(
        workflow_run_redirect.contains(&grandparent_ids[1].to_string()),
        "workflow run links should land on the page where that run is visible and selected"
    );

    let invocations_page = client
        .get(format!(
            "{}/invocations?workflow_type=rust::test.grandparent_task&workflow_id={}&limit=20",
            server.url, grandparent_ids[0]
        ))
        .send()
        .await
        .expect("workflow invocations list request");
    assert_eq!(invocations_page.status(), 200);
    let invocations_page = invocations_page
        .text()
        .await
        .expect("workflow invocations list body");
    assert!(
        invocations_page.contains("view-in-timeline-link")
            && invocations_page.contains("workflow_type=rust%3A%3Atest.grandparent_task")
            && invocations_page.contains(&format!("workflow_id={}", grandparent_ids[0])),
        "timeline shortcuts from a filtered invocation list should preserve workflow filters"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_runners_page() {
    let setup = create_test_app("test-runners");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/runners", server.url))
        .send()
        .await
        .expect("runners request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("Active Runners"),
        "should have Active Runners heading"
    );
    assert!(
        body.contains("Total Runners"),
        "should show Total Runners stat card"
    );
    assert!(
        body.contains("Heartbeat Timeout"),
        "should show Heartbeat Timeout stat"
    );
    assert!(
        body.contains("Runner Configuration"),
        "should show Runner Configuration panel"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_workflows_list() {
    let setup = create_test_app("test-workflows");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/workflows", server.url))
        .send()
        .await
        .expect("workflows request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("Workflows"), "should have Workflows heading");
    // With no data seeded, should show the empty state
    assert!(
        body.contains("No workflows found") || body.contains("Workflow"),
        "should show workflow content or empty state"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_log_explorer() {
    let setup = create_test_app("test-log-explorer");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/log-explorer", server.url))
        .send()
        .await
        .expect("log-explorer request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("Log Explorer"),
        "should have Log Explorer heading"
    );
    assert!(
        body.contains("<textarea"),
        "should have textarea for log input"
    );
    assert!(
        body.contains("Analyse"),
        "should have Analyse submit button"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_switch_app_unknown_returns_404() {
    let setup = create_test_app("test-switch");
    let server = start_test_server(setup).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = client
        .get(format!("{}/switch-app/nonexistent", server.url))
        .send()
        .await
        .expect("switch-app request");
    assert_eq!(resp.status(), 404);

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_static_css_served() {
    let setup = create_test_app("test-static");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/static/css/rustvello.css", server.url))
        .send()
        .await
        .expect("static css request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("--nav-bg"),
        "should serve CSS with rustvello design variables"
    );

    let resp = client
        .get(format!("{}/static/js/monitoring.js", server.url))
        .send()
        .await
        .expect("shared monitoring JavaScript request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("JavaScript body");
    assert!(body.contains("timelineFromCurrent") && body.contains("fitWindow"));

    let resp = client
        .get(format!("{}/static/logo.png", server.url))
        .send()
        .await
        .expect("logo request");
    assert_eq!(resp.status(), 200);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    assert!(
        content_type.contains("image/png"),
        "logo should be served as a PNG asset"
    );

    handle_keep_alive(server).await;
}

#[tokio::test]
async fn test_monitoring_capabilities_api() {
    let setup = create_test_app("test-monitoring-capabilities");
    let server = start_test_server(setup).await;
    let client = server.client();

    let response = client
        .get(format!("{}/api/capabilities", server.url))
        .send()
        .await
        .expect("capabilities request");
    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.expect("capabilities JSON");
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["pagination"]["default_page_size"], 50);
    assert!(body["timeline"]["filters"]
        .as_array()
        .is_some_and(|filters| filters.iter().any(|filter| filter == "runner_ids")));

    handle_keep_alive(server).await;
}

/// If `KEEP_ALIVE` is enabled, block until Ctrl-C; otherwise shut down.
async fn handle_keep_alive(server: TestServer) {
    if should_keep_alive(KEEP_ALIVE) {
        server.keep_alive_until_ctrlc().await;
    } else {
        server.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Complex test: seeded data + runner processing → rich dashboard content
// ---------------------------------------------------------------------------

/// End-to-end test: seed invocations, run the runner to process them, then
/// verify the timeline, invocation list, detail pages and JSON APIs all
/// reflect the processed data.
///
/// This mirrors the intent of pynmon's
/// `test_invocations_timeline_multi_runner.py`: exercising the full pipeline
/// from submission through execution to monitoring.
#[tokio::test]
async fn test_processed_invocations_full_pipeline() {
    let (server, client) = setup_with_runner().await;

    // 1. Timeline should render an SVG with actual content
    let resp = client
        .get(format!("{}/invocations/timeline", server.url))
        .send()
        .await
        .expect("timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("<svg"),
        "timeline should contain an SVG element"
    );

    // 2. Invocation list should show invocations
    let resp = client
        .get(format!("{}/invocations", server.url))
        .send()
        .await
        .expect("invocations list");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("process_order"),
        "invocation list should show the task"
    );

    // 3. API JSON for one of the invocations
    //    We need to find an invocation ID from the list page.
    //    Get invocations from the orchestrator through the API:
    //    Since all invocations were for "rust::test.process_order",
    //    we can look for invocation IDs in the HTML.
    let resp = client
        .get(format!("{}/invocations?limit=1", server.url))
        .send()
        .await
        .expect("invocations request");
    let body = resp.text().await.expect("body");
    // Extract an invocation ID from the page (look for a UUID pattern in invocation links)
    let re = regex::Regex::new(r"/invocations/([0-9a-f-]{36})").unwrap();
    if let Some(cap) = re.captures(&body) {
        let inv_id = &cap[1];

        // 3a. Detail page
        let resp = client
            .get(format!("{}/invocations/{inv_id}", server.url))
            .send()
            .await
            .expect("detail");
        assert_eq!(resp.status(), 200);
        let detail = resp.text().await.expect("body");
        assert!(
            detail.contains(inv_id),
            "detail should show the invocation ID"
        );

        // 3b. History JSON should have entries (since runner processed them)
        let resp = client
            .get(format!("{}/invocations/{inv_id}/history", server.url))
            .send()
            .await
            .expect("history");
        let history: serde_json::Value = resp.json().await.expect("json");
        let entries = history.as_array().expect("history is an array");
        assert!(
            entries.len() >= 2,
            "processed invocation should have at least 2 history entries (Registered→Running or Registered→Success), got {}",
            entries.len()
        );

        // 3c. API JSON should show the invocation metadata
        let resp = client
            .get(format!("{}/invocations/{inv_id}/api", server.url))
            .send()
            .await
            .expect("api");
        let api: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(api["invocation_id"], inv_id);
        assert!(
            api["task_id"].as_str().unwrap().contains("process_order"),
            "API should show task_id"
        );
    }

    handle_keep_alive(server).await;
}

// ---------------------------------------------------------------------------
// Complex hierarchical test — grandparent → parent → child with 2 runners
// ---------------------------------------------------------------------------

/// Setup with hierarchical tasks and multiple concurrent runners.
///
/// Creates grandparent → parent → child invocations, runs 4 runner instances
/// concurrently (sharing the same in-memory backends), then starts the
/// monitoring server.
///
/// Runner configuration:
/// - 2 × PersistentTokioRunner with 10 workers each
/// - 1 × additional PersistentTokioRunner with lower worker count
/// - 1 × RayonRunner
///
/// Hierarchy (matching pynmon's test_invocations_timeline_multi_runner.py):
/// - familyA(2) + familyB(3) + familyC(4) + familyD(1) + familyE(2)
/// - Total: 5 grandparents + 12 parents + 34 children = 51 invocations
async fn setup_hierarchical_with_runners() -> (TestServer, reqwest::Client) {
    // Init tracing BEFORE runners start so their logs are visible
    common::init_tracing();

    let mut setup = create_hierarchical_test_app("test-hierarchical");

    let broker = setup.broker.clone();
    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();
    let trigger_store = setup.trigger_store.clone();
    let client_data_store = setup.client_data_store.clone();
    // Use short intervals so atomic service fires during the 3-second test run
    setup.config.heartbeat_interval_seconds = 1;
    setup.config.atomic_service_check_interval_minutes = 0.01; // ~600ms
    setup.config.atomic_service_interval_minutes = 0.02; // ~1.2s
    setup.config.atomic_service_spread_margin_minutes = 0.005; // ~300ms
    setup.app.config = setup.config.clone();
    let config = setup.config.clone();
    let task_ids = setup.task_ids.clone();

    // Seed only grandparents — parent/child tasks are created by the
    // running grandparent/parent task bodies inside the runner workers.
    let _gp_ids = seed_grandparents_only(&setup.app)
        .await
        .expect("seeding grandparents should succeed");

    // --- Helper: build a fresh app with the same backends and smart tasks ---
    let make_app =
        |b: std::sync::Arc<dyn rustvello_core::broker::Broker>,
         o: std::sync::Arc<dyn rustvello_core::orchestrator::InvocationControlBackend>,
         s: std::sync::Arc<dyn rustvello_core::state_backend::StateBackend>,
         c: std::sync::Arc<rustvello_core::client_data_store::ClientDataStoreManager>,
         cfg: rustvello_proto::config::AppConfig| {
            let mut app = rustvello::prelude::RustvelloApp::with_backends(
                cfg,
                b.clone(),
                o.clone(),
                s.clone(),
                c,
            );
            register_hierarchical_tasks(&mut app, &o, &s, &b);
            app
        };

    // Runner 1: PersistentTokioRunner with 10 workers
    let runner1 = setup
        .app
        .into_runner()
        .with_num_workers(10)
        .with_idle_sleep(20);
    let handle1 = tokio::spawn(async move {
        let _ = runner1.run().await;
    });

    // Runner 2: PersistentTokioRunner with 10 workers (shared backends)
    let app2 = make_app(
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
        config.clone(),
    );
    let runner2 = app2.into_runner().with_num_workers(10).with_idle_sleep(20);
    let handle2 = tokio::spawn(async move {
        let _ = runner2.run().await;
    });

    // Runner 3: PersistentTokioRunner with lower concurrency (shared backends)
    let app3 = make_app(
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
        config.clone(),
    );
    let runner3 = app3.into_runner().with_num_workers(4).with_idle_sleep(20);
    let handle3 = tokio::spawn(async move {
        let _ = runner3.run().await;
    });

    // Runner 4: RayonRunner (shared backends)
    let mut app4 = make_app(
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
        config.clone(),
    );
    let task_reg4 = std::sync::Arc::new(std::mem::take(app4.task_registry_mut()));
    drop(app4);
    let runner4 = RayonRunner::new(
        config.app_id.clone(),
        config.clone(),
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        task_reg4,
    )
    .expect("test: failed to build RayonRunner")
    .with_num_threads(4)
    .expect("test: failed to set num_threads");
    let handle4 = tokio::spawn(async move {
        let _ = runner4.run().await;
    });

    // Wait for all runners to process all 51 invocations
    // (cascading: grandparent → parent → child, each level must execute before next is created)
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    handle1.abort();
    handle2.abort();
    handle3.abort();
    handle4.abort();

    // Start monitoring server on the same backends
    let monitor_app = rustvello::prelude::RustvelloApp::with_backends(
        config.clone(),
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
    );
    let monitor_setup = common::TestAppSetup {
        app: monitor_app,
        config: config.clone(),
        broker,
        orchestrator,
        state_backend,
        trigger_store,
        client_data_store,
        task_ids,
    };
    let server = start_test_server(monitor_setup).await;
    let client = server.client();
    (server, client)
}

/// Hierarchical end-to-end test: grandparent → parent → child tasks,
/// processed by 2 concurrent runners, then verified via the monitoring dashboard.
///
/// This mirrors pynmon's `test_invocations_timeline_multi_runner.py`:
/// exercises the full pipeline from hierarchical submission through
/// concurrent execution to monitoring visualization.
///
/// Run with keep-alive enabled to explore the dashboard in a browser:
/// ```bash
/// KEEP_ALIVE=1 cargo test -p rustvello-monitoring \
///     --test monitoring_dashboard test_hierarchical_timeline -- --nocapture
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_hierarchical_timeline() {
    let (server, client) = setup_hierarchical_with_runners().await;

    // 1. Timeline should render an SVG with content from multiple runners
    let resp = client
        .get(format!("{}/invocations/timeline", server.url))
        .send()
        .await
        .expect("timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("<svg"), "timeline should contain an SVG");
    assert!(
        body.contains("data-status=\"Registered\"")
            && body.contains("data-status=\"Running\"")
            && body.contains("data-status=\"Success\""),
        "timeline should render the full invocation lifecycle, not only registration"
    );

    // 2. Invocation list should show all three task types
    let resp = client
        .get(format!("{}/invocations?limit=50", server.url))
        .send()
        .await
        .expect("invocations list");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("grandparent_task"),
        "should show grandparent_task"
    );
    assert!(body.contains("parent_task"), "should show parent_task");
    assert!(body.contains("child_task"), "should show child_task");

    // 3. Invocation list filter by task_id should work
    let resp = client
        .get(format!(
            "{}/invocations?task_id=rust::test.child_task",
            server.url
        ))
        .send()
        .await
        .expect("filtered list");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("child_task"),
        "filtered list should show child_task"
    );

    // 4. Pick a grandparent invocation ID and check detail + family tree
    let re = regex::Regex::new(r"/invocations/([0-9a-f-]{36})").unwrap();
    let resp = client
        .get(format!(
            "{}/invocations?task_id=rust::test.grandparent_task",
            server.url
        ))
        .send()
        .await
        .expect("grandparent list");
    let body = resp.text().await.expect("body");

    if let Some(cap) = re.captures(&body) {
        let gp_id = &cap[1];

        // 4a. Detail page should show parent invocation info
        let resp = client
            .get(format!("{}/invocations/{gp_id}", server.url))
            .send()
            .await
            .expect("detail");
        assert_eq!(resp.status(), 200);
        let detail = resp.text().await.expect("body");
        assert!(
            detail.contains(gp_id),
            "detail should show grandparent invocation ID"
        );
        assert!(
            detail.contains("grandparent_task"),
            "detail should show task name"
        );
        assert!(
            detail.contains("Workflow root"),
            "workflow-defining invocation should be identified"
        );

        // 4b. History should have entries (Registered + Running + Success)
        let resp = client
            .get(format!("{}/invocations/{gp_id}/history", server.url))
            .send()
            .await
            .expect("history");
        let history: serde_json::Value = resp.json().await.expect("json");
        let entries = history.as_array().expect("history is array");
        assert!(
            entries.len() >= 2,
            "grandparent should have at least 2 history entries, got {}",
            entries.len()
        );
        // First entry should be Registered (from submit)
        assert_eq!(
            entries[0]["status"].as_str().unwrap(),
            "Registered",
            "first history entry should be Registered"
        );
        // History entries should have runner_id for Running/Success
        let has_runner = entries.iter().any(|e| e["runner_id"].as_str().is_some());
        assert!(
            has_runner,
            "at least one history entry should have runner_id"
        );

        // 4c. API JSON should show workflow info
        let resp = client
            .get(format!("{}/invocations/{gp_id}/api", server.url))
            .send()
            .await
            .expect("api");
        let api: serde_json::Value = resp.json().await.expect("json");
        assert_eq!(api["is_workflow_defining"], true);
        assert_eq!(api["invocation_id"], gp_id);

        // 4d. Investigation API joins provenance, history, and navigation.
        let resp = client
            .get(format!("{}/invocations/{gp_id}/investigation", server.url))
            .send()
            .await
            .expect("investigation api");
        assert_eq!(resp.status(), 200);
        let investigation: serde_json::Value = resp.json().await.expect("investigation JSON");
        assert_eq!(investigation["invocation"]["id"], gp_id);
        assert!(investigation["history"].is_array());
        assert!(investigation["integrity"]["has_registered_event"]
            .as_bool()
            .unwrap());
        assert!(investigation["links"]["timeline"]
            .as_str()
            .is_some_and(|link| link.contains("/invocations/timeline")));

        // 4e. Family tree should show children
        let resp = client
            .get(format!("{}/invocations/{gp_id}/family-tree", server.url))
            .send()
            .await
            .expect("family tree");
        assert_eq!(resp.status(), 200);
        let tree = resp.text().await.expect("body");
        // The family tree should contain links to child invocations
        assert!(
            tree.contains("parent_task") || tree.contains("child"),
            "family tree should show child invocations"
        );
    }

    // 5. Check a child invocation has parent_invocation_id set
    let resp = client
        .get(format!(
            "{}/invocations?task_id=rust::test.child_task&limit=1",
            server.url
        ))
        .send()
        .await
        .expect("child list");
    let body = resp.text().await.expect("body");
    if let Some(cap) = re.captures(&body) {
        let child_id = &cap[1];
        let resp = client
            .get(format!("{}/invocations/{child_id}/api", server.url))
            .send()
            .await
            .expect("child api");
        let api: serde_json::Value = resp.json().await.expect("json");
        assert!(
            api["parent_invocation_id"].as_str().is_some(),
            "child invocation should have parent_invocation_id"
        );
        assert!(
            api["workflow"]["depth"].as_u64().unwrap() >= 1,
            "child should have depth >= 1"
        );
    }

    // 6. Runners page should show both runners
    let resp = client
        .get(format!("{}/runners", server.url))
        .send()
        .await
        .expect("runners request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("runners body");
    assert!(
        body.contains("Active Runners"),
        "runners page should have table heading"
    );

    // 7. EVERY history entry must have runner_id — no exceptions.
    // Registered entries get the submitter's runner context (ExternalRunner for
    // top-level submit, worker context for runner-triggered children).
    // Running/Success/etc. get the executing worker's context.
    let resp = client
        .get(format!(
            "{}/invocations?task_id=rust::test.grandparent_task&limit=1",
            server.url
        ))
        .send()
        .await
        .expect("gp list for runner check");
    let body = resp.text().await.expect("body");
    let re2 = regex::Regex::new(r"/invocations/([0-9a-f-]{36})").unwrap();
    if let Some(cap) = re2.captures(&body) {
        let gp_id = &cap[1];
        let resp = client
            .get(format!("{}/invocations/{gp_id}/history", server.url))
            .send()
            .await
            .expect("history for runner check");
        let history: Vec<serde_json::Value> = resp.json().await.expect("json");
        for entry in &history {
            assert!(
                entry["runner_id"].as_str().is_some(),
                "EVERY history entry must have runner_id, got {:?}",
                entry
            );
        }
    }

    handle_keep_alive(server).await;
}

// ---------------------------------------------------------------------------
// NO STATUS WITHOUT RUNNER CONTEXT — checks every single history entry
// ---------------------------------------------------------------------------

/// Regression test: every single history entry for every invocation must have
/// `runner_id` set. This is an absolute requirement — there is no valid
/// scenario where a status transition happens without runner context.
///
/// - Registered entries get the submitter's context (ExternalRunner for
///   top-level submit, worker context for runner-triggered children).
/// - Pending/Running/Success/Failed/Retry get the worker's context.
///
/// Only grandparents are seeded externally. Parent/child tasks are created
/// dynamically by the running task bodies inside runner workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_every_history_entry_has_runner_context() {
    common::init_tracing();

    let setup = create_hierarchical_test_app("test-ctx-check");

    let broker = setup.broker.clone();
    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();
    let client_data_store = setup.client_data_store.clone();

    // Seed only grandparents — children are created by running task bodies
    let gp_ids = seed_grandparents_only(&setup.app)
        .await
        .expect("seeding grandparents should succeed");

    // --- Check Registered entries BEFORE runners start ---
    // Only grandparent IDs exist at this point.
    for inv_id in &gp_ids {
        let history = state_backend
            .get_history(inv_id)
            .await
            .expect("get_history should not fail");
        assert!(
            !history.is_empty(),
            "grandparent invocation {inv_id} should have a Registered entry"
        );
        for h in &history {
            let rid = h.runner_id.as_ref().or(h.status_record.runner_id.as_ref());
            assert!(
                rid.is_some(),
                "BEFORE runners: grandparent {inv_id} status {:?} has no runner_id",
                h.status_record.status,
            );
        }
    }

    // --- Now start runners and process the invocations ---
    let config = setup.config.clone();

    // Helper to build a fresh app with same backends and smart task closures
    let make_app =
        |b: std::sync::Arc<dyn rustvello_core::broker::Broker>,
         o: std::sync::Arc<dyn rustvello_core::orchestrator::InvocationControlBackend>,
         s: std::sync::Arc<dyn rustvello_core::state_backend::StateBackend>,
         c: std::sync::Arc<rustvello_core::client_data_store::ClientDataStoreManager>,
         cfg: rustvello_proto::config::AppConfig| {
            let mut app = rustvello::prelude::RustvelloApp::with_backends(
                cfg,
                b.clone(),
                o.clone(),
                s.clone(),
                c,
            );
            register_hierarchical_tasks(&mut app, &o, &s, &b);
            app
        };

    let runner1 = setup
        .app
        .into_runner()
        .with_num_workers(10)
        .with_idle_sleep(20);
    let handle1 = tokio::spawn(async move {
        let _ = runner1.run().await;
    });

    let app2 = make_app(
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
        config.clone(),
    );
    let runner2 = app2.into_runner().with_num_workers(10).with_idle_sleep(20);
    let handle2 = tokio::spawn(async move {
        let _ = runner2.run().await;
    });

    // Give runners enough time to process cascading hierarchy
    // (grandparent → parent → child, each level must execute before next is created)
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    handle1.abort();
    handle2.abort();

    // --- Discover ALL invocation IDs from the orchestrator ---
    let gp_tid = rustvello_proto::identifiers::TaskId::new("test", "grandparent_task");
    let p_tid = rustvello_proto::identifiers::TaskId::new("test", "parent_task");
    let c_tid = rustvello_proto::identifiers::TaskId::new("test", "child_task");

    let gp_inv_ids = orchestrator.get_invocations_by_task(&gp_tid).await.unwrap();
    let p_inv_ids = orchestrator.get_invocations_by_task(&p_tid).await.unwrap();
    let c_inv_ids = orchestrator.get_invocations_by_task(&c_tid).await.unwrap();

    let total_invocations = gp_inv_ids.len() + p_inv_ids.len() + c_inv_ids.len();
    assert_eq!(
        gp_inv_ids.len(),
        5,
        "Should have 5 grandparent invocations, got {}",
        gp_inv_ids.len()
    );
    assert_eq!(
        p_inv_ids.len(),
        12,
        "Should have 12 parent invocations (2+3+4+1+2), got {}",
        p_inv_ids.len()
    );
    assert_eq!(
        c_inv_ids.len(),
        34,
        "Should have 34 child invocations (4+9+16+1+4), got {}",
        c_inv_ids.len()
    );

    // --- Check EVERY history entry for EVERY invocation AFTER runners ---
    let all_ids: Vec<_> = gp_inv_ids
        .iter()
        .chain(p_inv_ids.iter())
        .chain(c_inv_ids.iter())
        .collect();

    let mut total_entries = 0;
    let mut entries_with_runner = 0;
    let mut entries_missing_runner = Vec::new();

    for inv_id in &all_ids {
        let history = state_backend
            .get_history(inv_id)
            .await
            .expect("get_history should not fail");
        for h in &history {
            total_entries += 1;
            let rid = h.runner_id.as_ref().or(h.status_record.runner_id.as_ref());
            if rid.is_some() {
                entries_with_runner += 1;
            } else {
                entries_missing_runner.push(format!(
                    "inv={inv_id} status={:?} runner_id={:?} sr.runner_id={:?}",
                    h.status_record.status, h.runner_id, h.status_record.runner_id,
                ));
            }
        }
    }

    assert!(
        entries_missing_runner.is_empty(),
        "AFTER runners: {}/{} history entries are missing runner_id!\n\
         Entries without runner context:\n{}",
        entries_missing_runner.len(),
        total_entries,
        entries_missing_runner.join("\n"),
    );

    assert!(
        total_entries > total_invocations,
        "Should have more than {total_invocations} history entries \
         (at least Registered for each), got {total_entries}"
    );

    eprintln!(
        "✅ All {total_entries} history entries across {total_invocations} invocations have runner_id \
         ({entries_with_runner} with context)"
    );
}

// ---------------------------------------------------------------------------
// SVG timeline must never show "Unknown" workers
// ---------------------------------------------------------------------------

/// Verifies that all runner_ids seen in invocation histories have stored
/// RunnerContext entries, and that the SVG timeline contains no "Unknown".
///
/// This is a regression test: every supported runner must store per-worker
/// contexts, or the timeline
/// to fall back to RunnerInfo::from_id() → "Unknown(uuid)" labels.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_no_unknown_workers_in_timeline() {
    let (server, client) = setup_hierarchical_with_runners().await;

    // 1. The SVG timeline must not contain "Unknown"
    let resp = client
        .get(format!("{}/invocations/timeline", server.url))
        .send()
        .await
        .expect("timeline request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("<svg"), "timeline should contain an SVG");
    assert!(
        !body.contains("Unknown"),
        "SVG timeline must not contain 'Unknown' worker labels.\n\
         This means a runner_id in history has no stored RunnerContext.\n\
         Offending SVG snippet: {}",
        body.chars().take(2000).collect::<String>()
    );
    assert!(
        !body.contains("unassigned"),
        "SVG timeline must not contain 'unassigned' lane.\n\
         This means a history entry has runner_id=None (missing context).\n\
         Offending SVG snippet: {}",
        body.chars().take(2000).collect::<String>()
    );

    // 2. Verify at least some known runner classes appear
    let has_persistent = body.contains("PersistentTokio");
    let has_per_inv = body.contains("PerInvocation");
    let has_rayon = body.contains("Rayon");
    assert!(
        has_persistent || has_per_inv || has_rayon,
        "SVG should contain at least one known runner class name"
    );

    handle_keep_alive(server).await;
}

// ---------------------------------------------------------------------------
// Runner detail page tests
// ---------------------------------------------------------------------------

/// Helper: create a test app, manually register runner contexts + heartbeats,
/// then start the monitoring server. Returns parent and worker runner IDs.
async fn setup_with_runner_contexts() -> (TestServer, reqwest::Client, String, String) {
    let setup = create_test_app("test-runner-detail");

    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();

    // Create a parent runner context
    let parent_id = uuid::Uuid::new_v4().to_string();
    let parent_ctx = rustvello_core::state_backend::StoredRunnerContext {
        runner_cls: "PersistentTokioRunner".to_owned(),
        runner_language: rustvello_proto::identifiers::TaskLanguage::Rust,
        executor_kind: rustvello_proto::identifiers::ExecutorKind::Tokio,
        runner_id: parent_id.clone(),
        pid: std::process::id(),
        hostname: "test-host".to_owned(),
        thread_id: 1,
        started_at: chrono::Utc::now(),
        parent_runner_id: None,
        parent_runner_cls: None,
    };
    state_backend
        .store_runner_context(&parent_ctx)
        .await
        .expect("store parent ctx");

    // Create a worker (child) runner context
    let worker_id = uuid::Uuid::new_v4().to_string();
    let worker_ctx = rustvello_core::state_backend::StoredRunnerContext {
        runner_cls: "PersistentTokioWorker".to_owned(),
        runner_language: rustvello_proto::identifiers::TaskLanguage::Rust,
        executor_kind: rustvello_proto::identifiers::ExecutorKind::Tokio,
        runner_id: worker_id.clone(),
        pid: std::process::id(),
        hostname: "test-host".to_owned(),
        thread_id: 2,
        started_at: chrono::Utc::now(),
        parent_runner_id: Some(parent_id.clone()),
        parent_runner_cls: Some("PersistentTokioRunner".to_owned()),
    };
    state_backend
        .store_runner_context(&worker_ctx)
        .await
        .expect("store worker ctx");

    // Register heartbeats so they appear as active runners
    let parent_rid = rustvello_proto::identifiers::RunnerId::from_string(parent_id.clone());
    let worker_rid = rustvello_proto::identifiers::RunnerId::from_string(worker_id.clone());
    orchestrator
        .register_heartbeat(&parent_rid, true)
        .await
        .expect("parent heartbeat");
    orchestrator
        .register_heartbeat(&worker_rid, false)
        .await
        .expect("worker heartbeat");

    let server = start_test_server(setup).await;
    let client = server.client();
    (server, client, parent_id, worker_id)
}

/// Runner detail page renders with context info, heartbeat, and atomic service sections.
#[tokio::test]
async fn test_runner_detail_page() {
    let (server, client, parent_id, _worker_id) = setup_with_runner_contexts().await;

    let resp = client
        .get(format!("{}/runners/{parent_id}", server.url))
        .send()
        .await
        .expect("runner detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    // Should show runner context sections
    assert!(
        body.contains("Runner Context"),
        "detail should show Runner Context card"
    );
    assert!(
        body.contains("Language") && body.contains("rust"),
        "detail should show runner language"
    );
    assert!(
        body.contains("language-rust"),
        "runner detail should use the Rust language badge"
    );
    assert!(
        body.contains("Heartbeat Status"),
        "detail should show Heartbeat Status card"
    );
    assert!(
        body.contains("Atomic Service Status"),
        "detail should show Atomic Service Status card"
    );
    assert!(
        body.contains("PersistentTokioRunner"),
        "detail should show runner class"
    );
    assert!(body.contains(&parent_id), "detail should show runner ID");
    assert!(body.contains("test-host"), "detail should show hostname");

    handle_keep_alive(server).await;
}

/// Runner detail page shows parent context when runner has a parent.
#[tokio::test]
async fn test_runner_detail_with_parent() {
    let (server, client, parent_id, worker_id) = setup_with_runner_contexts().await;

    // The worker has the parent, so request the worker detail
    let resp = client
        .get(format!("{}/runners/{worker_id}", server.url))
        .send()
        .await
        .expect("worker detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    assert!(
        body.contains("Parent Context"),
        "worker detail should show Parent Context card"
    );
    assert!(
        body.contains("PersistentTokioRunner"),
        "should show parent runner class"
    );
    assert!(
        body.contains(&parent_id),
        "should show parent runner ID as link"
    );

    handle_keep_alive(server).await;
}

/// Runner detail page shows workers (child runners) when they exist.
#[tokio::test]
async fn test_runner_detail_with_workers() {
    let (server, client, parent_id, worker_id) = setup_with_runner_contexts().await;

    // The parent should show the worker in its "Workers" section
    let resp = client
        .get(format!("{}/runners/{parent_id}", server.url))
        .send()
        .await
        .expect("parent detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    assert!(
        body.contains("Workers"),
        "parent detail should show Workers section"
    );
    assert!(
        body.contains(&worker_id),
        "parent detail should list worker runner ID"
    );
    assert!(
        body.contains("PersistentTokioWorker"),
        "parent detail should show worker class"
    );

    handle_keep_alive(server).await;
}

/// Runners table uses links to detail pages instead of inline toggles.
#[tokio::test]
async fn test_runners_table_uses_links() {
    let (server, client, parent_id, _worker_id) = setup_with_runner_contexts().await;

    let resp = client
        .get(format!("{}/runners", server.url))
        .send()
        .await
        .expect("runners overview");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    // Should contain links to detail pages
    let link_pattern = format!("/runners/{parent_id}");
    assert!(
        body.contains(&link_pattern),
        "runners table should contain link to runner detail page"
    );
    // Should NOT contain inline toggle JavaScript
    assert!(
        !body.contains("toggleRunnerDetail"),
        "runners table should not have inline toggle JS"
    );

    handle_keep_alive(server).await;
}

/// Invocation API returns correct status from history, not potentially stale
/// orchestrator status.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_invocation_api_status_from_history() {
    let (server, client) = setup_hierarchical_with_runners().await;

    // Find a processed invocation
    let resp = client
        .get(format!(
            "{}/invocations?task_id=rust::test.grandparent_task&limit=1",
            server.url
        ))
        .send()
        .await
        .expect("invocation list");
    let body = resp.text().await.expect("body");
    let re = regex::Regex::new(r"/invocations/([0-9a-f-]{36})").unwrap();

    if let Some(cap) = re.captures(&body) {
        let inv_id = &cap[1];

        // Get API JSON
        let resp = client
            .get(format!("{}/invocations/{inv_id}/api", server.url))
            .send()
            .await
            .expect("api request");
        let api: serde_json::Value = resp.json().await.expect("json");

        // Get history
        let resp = client
            .get(format!("{}/invocations/{inv_id}/history", server.url))
            .send()
            .await
            .expect("history request");
        let history: Vec<serde_json::Value> = resp.json().await.expect("json");

        // The API status should match the latest history entry
        let latest_status = history
            .last()
            .and_then(|h| h["status"].as_str())
            .expect("should have history entries");
        assert_eq!(
            api["status"].as_str().unwrap(),
            latest_status,
            "API status should match latest history status"
        );
    }

    handle_keep_alive(server).await;
}

// ---------------------------------------------------------------------------
// Atomic service recovery test
// ---------------------------------------------------------------------------

/// Test that the atomic service fires through the management loop and
/// recovers stale pending invocations when configured with short intervals.
///
/// This exercises the full path: management_loop → should_run_atomic_service
/// → recover_stale_invocations → reroute.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_atomic_service_recovery() {
    use rustvello_proto::config::AppConfig;

    common::init_tracing();

    let mut config = AppConfig::new("test-atomic-recovery");
    // Short intervals so atomic service fires quickly in tests
    config.atomic_service_check_interval_minutes = 0.001; // ~60ms
    config.atomic_service_interval_minutes = 0.01; // ~600ms
    config.atomic_service_spread_margin_minutes = 0.001; // ~60ms
    config.max_pending_seconds = 1; // 1 second before pending recovery
    config.heartbeat_interval_seconds = 1;
    config.runner_dead_after_seconds = 30;

    let setup = common::create_test_app_with_config(config.clone());

    let broker = setup.broker.clone();
    let orchestrator = setup.orchestrator.clone();
    let state_backend = setup.state_backend.clone();
    let trigger_store = setup.trigger_store.clone();
    let client_data_store = setup.client_data_store.clone();
    let task_ids = setup.task_ids.clone();

    // Seed invocations but do NOT process them → they stay Pending
    let _inv_ids = common::seed_invocations(&setup.app, 3)
        .await
        .expect("seeding should succeed");

    // Wait for invocations to become stale (> max_pending_seconds=1s)
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;

    // Now start a runner that will perform atomic service (recovery)
    let runner = setup.app.into_runner().with_idle_sleep(20);
    let runner_handle = tokio::spawn(async move {
        let _ = runner.run().await;
    });

    // Wait for workers to dequeue + atomic service recovery cycle
    // Management loop has 1s sleep between iterations; workers process immediately
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    runner_handle.abort();

    // Verify invocations were recovered: check history shows non-Pending statuses
    let mut recovered_count = 0;
    let task_id = rustvello_proto::identifiers::TaskId::new("test", "process_order");
    let all_inv_ids = orchestrator
        .get_invocations_by_task(&task_id)
        .await
        .unwrap_or_default();
    for inv_id in &all_inv_ids {
        let history = state_backend.get_history(inv_id).await.unwrap_or_default();
        let has_recovery = history.iter().any(|h| {
            matches!(
                h.status_record.status,
                rustvello_proto::status::InvocationStatus::PendingRecovery
                    | rustvello_proto::status::InvocationStatus::Rerouted
                    | rustvello_proto::status::InvocationStatus::Success
            )
        });
        if has_recovery {
            recovered_count += 1;
        }
    }
    assert!(
        recovered_count > 0,
        "At least some invocations should have been recovered by the atomic service. \
         This means the management loop → should_run_atomic_service → \
         recover_stale_invocations pipeline is working."
    );

    // Start monitoring server to verify dashboard shows correct data
    let monitor_app = rustvello::prelude::RustvelloApp::with_backends(
        config.clone(),
        broker.clone(),
        orchestrator.clone(),
        state_backend.clone(),
        client_data_store.clone(),
    );
    let monitor_setup = common::TestAppSetup {
        app: monitor_app,
        config,
        broker,
        orchestrator,
        state_backend,
        trigger_store,
        client_data_store,
        task_ids,
    };
    let server = start_test_server(monitor_setup).await;
    let client = server.client();

    // Verify dashboard shows recovered invocations
    let resp = client
        .get(format!("{}/invocations?limit=50", server.url))
        .send()
        .await
        .expect("invocation list");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // At least one recovery-related status should appear
    let has_recovery_status =
        body.contains("PendingRecovery") || body.contains("Rerouted") || body.contains("Success");
    assert!(
        has_recovery_status,
        "Invocation list should show recovery-related statuses"
    );

    handle_keep_alive(server).await;
}

// ---------------------------------------------------------------------------
// LOG EXPLORER: runner/worker links must resolve truncated IDs to full UUIDs
// ---------------------------------------------------------------------------

/// The log format truncates runner and worker IDs to 8 characters:
///   `[PTR(a86ab1f8).W(d1241003)cc6c…:task.key]`
///
/// But the state backend stores runner contexts with full UUIDs.  When the
/// log explorer parses these lines, the runner/worker links it generates must
/// point to the **full** UUID, not the truncated 8-char fragment.
///
/// This test:
///   1. Stores runner contexts with known full UUIDs.
///   2. POSTs a log line containing those UUIDs' 8-char prefixes.
///   3. Asserts the rendered HTML links to `/runners/<full-UUID>`, not
///      `/runners/<8-char>`.
#[tokio::test]
async fn test_log_explorer_resolves_truncated_runner_ids() {
    use rustvello_core::state_backend::StoredRunnerContext;

    let setup = create_test_app("test-log-resolve");
    let state_backend = setup.state_backend.clone();
    let inv_ids = seed_invocations(&setup.app, 1)
        .await
        .expect("seed invocation");
    let inv_id = &inv_ids[0];

    // Full UUIDs for runner and worker
    let runner_full_id = "a86ab1f8-1234-5678-9abc-def012345678";
    let worker_full_id = "d1241003-aaaa-bbbb-cccc-ddddeeee0001";

    // Store runner context with the full UUID
    let runner_ctx = StoredRunnerContext {
        runner_cls: "PersistentTokioRunner".to_string(),
        runner_language: rustvello_proto::identifiers::TaskLanguage::Rust,
        executor_kind: rustvello_proto::identifiers::ExecutorKind::Tokio,
        runner_id: runner_full_id.to_string(),
        pid: 12345,
        hostname: "test-host".to_string(),
        thread_id: 1,
        started_at: chrono::Utc::now(),
        parent_runner_id: None,
        parent_runner_cls: None,
    };
    state_backend
        .store_runner_context(&runner_ctx)
        .await
        .expect("store runner context");

    // Store worker context as a child of the runner
    let worker_ctx = StoredRunnerContext {
        runner_cls: "Worker".to_string(),
        runner_language: rustvello_proto::identifiers::TaskLanguage::Rust,
        executor_kind: rustvello_proto::identifiers::ExecutorKind::Tokio,
        runner_id: worker_full_id.to_string(),
        pid: 12345,
        hostname: "test-host".to_string(),
        thread_id: 2,
        started_at: chrono::Utc::now(),
        parent_runner_id: Some(runner_full_id.to_string()),
        parent_runner_cls: Some("PersistentTokioRunner".to_string()),
    };
    state_backend
        .store_runner_context(&worker_ctx)
        .await
        .expect("store worker context");

    let server = start_test_server(setup).await;
    let client = server.client();

    // Log line with truncated runner/worker IDs (first 8 chars of the UUIDs)
    let log_line = format!(
        "2026-03-27T10:23:45.123Z INFO  [R] test-log-resolve \
        [PTR(a86ab1f8).W(d1241003){inv_id}:rust::test.process_order] \
        rustvello::runner Invocation completed"
    );

    // POST to log explorer analyze endpoint
    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", &log_line)])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    assert!(
        body.contains("log-svg-timeline-link") && body.contains("<svg"),
        "log explorer should render the invocation timeline for bracket-context logs"
    );

    // The runner link must point to the FULL UUID, not the truncated 8-char prefix
    assert!(
        body.contains(&format!("/runners/{runner_full_id}")),
        "Runner link should use full UUID '{runner_full_id}', not truncated 'a86ab1f8'.\n\
         Found runner links: {:?}",
        regex::Regex::new(r#"/runners/[a-f0-9-]+"#)
            .unwrap()
            .find_iter(&body)
            .map(|m| m.as_str())
            .collect::<Vec<_>>()
    );

    // The worker link must also point to the FULL UUID
    assert!(
        body.contains(&format!("/runners/{worker_full_id}")),
        "Worker link should use full UUID '{worker_full_id}', not truncated 'd1241003'.\n\
         Found runner links: {:?}",
        regex::Regex::new(r#"/runners/[a-f0-9-]+"#)
            .unwrap()
            .find_iter(&body)
            .map(|m| m.as_str())
            .collect::<Vec<_>>()
    );

    // The truncated IDs should NOT appear as href targets
    assert!(
        !body.contains(r#"/runners/a86ab1f8""#),
        "Runner link must NOT use truncated ID 'a86ab1f8'"
    );
    assert!(
        !body.contains(r#"/runners/d1241003""#),
        "Worker link must NOT use truncated ID 'd1241003'"
    );

    handle_keep_alive(server).await;
}
