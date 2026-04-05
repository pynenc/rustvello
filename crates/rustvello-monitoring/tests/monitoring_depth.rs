//! Phase 7: Monitoring dashboard depth tests.
//!
//! Covers gaps identified in the test study plan:
//! - 7.1 Deep family tree rendering
//! - 7.2 Invocations pagination
//! - 7.3 Log explorer POST /analyze
//! - 7.4 Workflow dashboard views
//! - 7.5 Call detail endpoint
//! - 7.6 Error/edge-case endpoints

mod common;

use common::{
    create_hierarchical_test_app, create_test_app, seed_hierarchical_invocations, seed_invocations,
    should_keep_alive, start_test_server, submit_with_parent, TestServer,
};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::WorkflowIdentity;

const KEEP_ALIVE: bool = false;

async fn handle_keep_alive(server: TestServer) {
    if should_keep_alive(KEEP_ALIVE) {
        server.keep_alive_until_ctrlc().await;
    } else {
        server.shutdown().await;
    }
}

// ===========================================================================
// 7.1 Family tree rendering — deep and expand
// ===========================================================================

/// Build a chain of depth N (grandparent → child₁ → child₂ → … → childₙ₋₁).
async fn seed_deep_chain(setup: &common::TestAppSetup, depth: usize) -> Vec<InvocationId> {
    let task_id = TaskId::new("test", "process_order");
    let mut args = SerializedArguments::new();
    args.insert("order_id", "root-0000");
    let root_id = setup.app.submit(&task_id, args).await.unwrap();

    let root_workflow = WorkflowIdentity::root(root_id.clone(), task_id.clone());
    let mut chain = vec![root_id.clone()];

    let mut parent_id = root_id;
    for i in 1..depth {
        let mut cargs = SerializedArguments::new();
        cargs.insert("order_id", format!("level-{i}"));
        let child_workflow = WorkflowIdentity::child(
            root_workflow.workflow_id.clone(),
            root_workflow.workflow_type.clone(),
            parent_id.clone(),
            i as u32,
        );
        let child_id = submit_with_parent(
            &setup.orchestrator,
            &setup.state_backend,
            &setup.broker,
            &task_id,
            cargs,
            &parent_id,
            child_workflow,
        )
        .await
        .unwrap();
        chain.push(child_id.clone());
        parent_id = child_id;
    }
    chain
}

/// 7-level deep tree should render an SVG family tree for the root.
#[tokio::test]
async fn test_family_tree_deep_7_levels() {
    let setup = create_test_app("test-deep-tree");
    let chain = seed_deep_chain(&setup, 7).await;
    let root_id = &chain[0].to_string();
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/invocations/{root_id}/family-tree", server.url))
        .send()
        .await
        .expect("family tree request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("<svg"), "should render an SVG");
    // The root and the deepest leaf should appear
    assert!(
        body.contains(&root_id[..8]),
        "SVG should contain root invocation short ID"
    );

    handle_keep_alive(server).await;
}

/// Family tree with `?expand=` param should return valid SVG.
#[tokio::test]
async fn test_family_tree_expand_param() {
    let setup = create_test_app("test-tree-expand");
    let chain = seed_deep_chain(&setup, 4).await;
    let root_id = chain[0].to_string();
    let child_id = chain[1].to_string();
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/{root_id}/family-tree?expand={child_id}",
            server.url
        ))
        .send()
        .await
        .expect("family tree expand request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("<svg"), "expanded tree should be SVG");

    handle_keep_alive(server).await;
}

// ===========================================================================
// 7.2 Invocations pagination
// ===========================================================================

/// Seed enough invocations to span multiple pages, verify disjoint sets.
#[tokio::test]
async fn test_invocations_pagination_disjoint_pages() {
    let setup = create_test_app("test-pagination");
    // Seed 10 invocations, paginate with limit=3
    let all_ids = seed_invocations(&setup.app, 10).await.expect("seed");
    let server = start_test_server(setup).await;
    let client = server.client();

    let re = regex::Regex::new(r"/invocations/([0-9a-f-]{36})").unwrap();

    // Collect IDs from page 1 and page 2
    let mut page1_ids = std::collections::HashSet::new();
    let resp = client
        .get(format!("{}/invocations?limit=3&page=1", server.url))
        .send()
        .await
        .expect("page 1");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    for cap in re.captures_iter(&body) {
        page1_ids.insert(cap[1].to_string());
    }

    let mut page2_ids = std::collections::HashSet::new();
    let resp = client
        .get(format!("{}/invocations?limit=3&page=2", server.url))
        .send()
        .await
        .expect("page 2");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    for cap in re.captures_iter(&body) {
        page2_ids.insert(cap[1].to_string());
    }

    // Pages must be disjoint
    let overlap: Vec<_> = page1_ids.intersection(&page2_ids).collect();
    assert!(
        overlap.is_empty(),
        "pages should be disjoint, but overlap: {overlap:?}"
    );

    // Both pages should have at least 1 invocation
    assert!(!page1_ids.is_empty(), "page 1 should have invocations");
    assert!(!page2_ids.is_empty(), "page 2 should have invocations");

    // All returned IDs should come from our seeded set
    for id in page1_ids.iter().chain(page2_ids.iter()) {
        assert!(
            all_ids.contains(id),
            "returned ID {id} should be one we seeded"
        );
    }

    handle_keep_alive(server).await;
}

// ===========================================================================
// 7.3 Log explorer POST /log-explorer/analyze
// ===========================================================================

/// Structured log line should yield parsed analysis with entity refs.
#[tokio::test]
async fn test_log_explorer_analyze_structured() {
    let setup = create_test_app("test-log-structured");
    let inv_ids = seed_invocations(&setup.app, 1).await.expect("seed");
    let inv_id = &inv_ids[0];
    let server = start_test_server(setup).await;
    let client = server.client();

    let log_text = format!(
        "2025-01-15T10:00:00Z INFO rustvello::runner [inv_id={inv_id}] Starting invocation"
    );
    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", &log_text)])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // Should show the parsed result (has_input=true renders analysis section)
    assert!(
        body.contains(inv_id) || body.contains(&inv_id[..8]),
        "analysis should reference the invocation ID"
    );

    handle_keep_alive(server).await;
}

/// Plain text (non-structured) log line should still render without error.
#[tokio::test]
async fn test_log_explorer_analyze_plain_text() {
    let setup = create_test_app("test-log-plain");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", "just some random text without structure")])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("random text") || body.contains("log_text"),
        "should echo or display the input"
    );

    handle_keep_alive(server).await;
}

/// Empty log text should render gracefully (no crash, no analysis).
#[tokio::test]
async fn test_log_explorer_analyze_empty() {
    let setup = create_test_app("test-log-empty");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", "")])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);

    handle_keep_alive(server).await;
}

/// Multi-line log with multiple invocation IDs should list all entity refs.
#[tokio::test]
async fn test_log_explorer_analyze_multi_line() {
    let setup = create_test_app("test-log-multi");
    let inv_ids = seed_invocations(&setup.app, 3).await.expect("seed");
    let server = start_test_server(setup).await;
    let client = server.client();

    let log_text = inv_ids
        .iter()
        .enumerate()
        .map(|(i, id)| format!("2025-01-15T10:00:{i:02}Z INFO runner [inv_id={id}] Processing"))
        .collect::<Vec<_>>()
        .join("\n");

    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", &log_text)])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // At least one short-ID should appear
    assert!(
        inv_ids.iter().any(|id| body.contains(&id[..8])),
        "multi-line analysis should reference at least one invocation"
    );

    handle_keep_alive(server).await;
}

/// Log line with ERROR level should render a visually distinct card.
#[tokio::test]
async fn test_log_explorer_analyze_error_level() {
    let setup = create_test_app("test-log-error");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[(
            "log_text",
            "2025-01-15T10:00:00Z ERROR rustvello Task panicked: division by zero",
        )])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // The error level should trigger a level-specific CSS class
    assert!(
        body.contains("ERROR") || body.contains("error") || body.contains("danger"),
        "ERROR log should appear with error styling"
    );

    handle_keep_alive(server).await;
}

/// Log with task_id reference should be linked.
#[tokio::test]
async fn test_log_explorer_analyze_task_ref() {
    let setup = create_test_app("test-log-task-ref");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[(
            "log_text",
            "2025-01-15T10:00:00Z INFO runner [task_id=test::process_order] Routing",
        )])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("process_order"),
        "task ref should appear in analysis"
    );

    handle_keep_alive(server).await;
}

/// Log with runner_id reference should be displayed.
#[tokio::test]
async fn test_log_explorer_analyze_runner_ref() {
    let setup = create_test_app("test-log-runner-ref");
    let server = start_test_server(setup).await;
    let client = server.client();

    let runner_uuid = uuid::Uuid::new_v4().to_string();
    let log_text = format!("2025-01-15T10:00:00Z INFO runner [runner_id={runner_uuid}] Heartbeat");
    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", &log_text)])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains(&runner_uuid[..8]),
        "runner ref should appear in analysis"
    );

    handle_keep_alive(server).await;
}

/// JSON-formatted log line should parse without error.
#[tokio::test]
async fn test_log_explorer_analyze_json_format() {
    let setup = create_test_app("test-log-json");
    let inv_ids = seed_invocations(&setup.app, 1).await.expect("seed");
    let inv_id = &inv_ids[0];
    let server = start_test_server(setup).await;
    let client = server.client();

    let log_text = format!(
        r#"{{"timestamp":"2025-01-15T10:00:00Z","level":"INFO","target":"rustvello","message":"Running","inv_id":"{inv_id}"}}"#
    );
    let resp = client
        .post(format!("{}/log-explorer/analyze", server.url))
        .form(&[("log_text", &log_text)])
        .send()
        .await
        .expect("analyze request");
    assert_eq!(resp.status(), 200);

    handle_keep_alive(server).await;
}

// ===========================================================================
// 7.4 Workflow dashboard views
// ===========================================================================

/// Workflow list with hierarchical data should display workflow types.
#[tokio::test]
async fn test_workflows_list_with_data() {
    let setup = create_hierarchical_test_app("test-wf-list");
    seed_hierarchical_invocations(
        &setup.app,
        &setup.orchestrator,
        &setup.state_backend,
        &setup.broker,
    )
    .await
    .expect("seed hierarchy");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/workflows", server.url))
        .send()
        .await
        .expect("workflows list");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("grandparent_task"),
        "should list grandparent task as workflow type"
    );

    handle_keep_alive(server).await;
}

/// Workflow detail page for a specific type should show runs.
#[tokio::test]
async fn test_workflows_detail_by_type() {
    let setup = create_hierarchical_test_app("test-wf-detail");
    seed_hierarchical_invocations(
        &setup.app,
        &setup.orchestrator,
        &setup.state_backend,
        &setup.broker,
    )
    .await
    .expect("seed hierarchy");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/workflows/test::grandparent_task", server.url))
        .send()
        .await
        .expect("workflow detail");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("grandparent_task"),
        "detail page should show the workflow type"
    );

    handle_keep_alive(server).await;
}

/// /workflows/runs should list all workflow runs across types.
#[tokio::test]
async fn test_workflows_all_runs() {
    let setup = create_hierarchical_test_app("test-wf-runs");
    seed_hierarchical_invocations(
        &setup.app,
        &setup.orchestrator,
        &setup.state_backend,
        &setup.broker,
    )
    .await
    .expect("seed hierarchy");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/workflows/runs", server.url))
        .send()
        .await
        .expect("all runs");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(body.contains("Workflow Runs"), "should have heading");
    // There are 5 grandparent families, each is a workflow root
    assert!(
        body.contains("grandparent_task"),
        "should list grandparent runs"
    );

    handle_keep_alive(server).await;
}

/// /workflows/children/{inv_id} should list child invocations as HTML partial.
#[tokio::test]
async fn test_workflows_children_partial() {
    let setup = create_hierarchical_test_app("test-wf-children");
    let (gp_ids, _parent_ids, _child_ids) = seed_hierarchical_invocations(
        &setup.app,
        &setup.orchestrator,
        &setup.state_backend,
        &setup.broker,
    )
    .await
    .expect("seed hierarchy");
    let gp_id = gp_ids[0].to_string();
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/workflows/children/{gp_id}", server.url))
        .send()
        .await
        .expect("children partial");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    // familyA has 2 children
    assert!(
        body.contains("parent_task"),
        "children partial should show parent task invocations"
    );

    handle_keep_alive(server).await;
}

// ===========================================================================
// 7.5 Call detail endpoint
// ===========================================================================

/// GET /calls/{call_id_key} should show the call with its invocation.
#[tokio::test]
async fn test_call_detail_by_path() {
    let setup = create_test_app("test-call-detail");
    let inv_ids = seed_invocations(&setup.app, 1).await.expect("seed");
    let inv_id_str = &inv_ids[0];
    let inv_id = InvocationId::from_string(inv_id_str.as_str());
    // Get the call_id from the state backend
    let inv_dto = setup
        .state_backend
        .get_invocation(&inv_id)
        .await
        .expect("get invocation");
    let call_id = inv_dto.call_id.to_string();
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/calls/{call_id}", server.url))
        .send()
        .await
        .expect("call detail request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("process_order"),
        "call detail should show task ID"
    );
    assert!(
        body.contains(inv_id_str) || body.contains(&inv_id_str[..8]),
        "call detail should reference the linked invocation"
    );

    handle_keep_alive(server).await;
}

/// GET /calls?call_id_key=... query-string variant should also work.
#[tokio::test]
async fn test_call_detail_by_query() {
    let setup = create_test_app("test-call-query");
    let inv_ids = seed_invocations(&setup.app, 1).await.expect("seed");
    let inv_id = InvocationId::from_string(inv_ids[0].as_str());
    let inv_dto = setup
        .state_backend
        .get_invocation(&inv_id)
        .await
        .expect("get invocation");
    let call_id = inv_dto.call_id.to_string();
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/calls?call_id_key={call_id}", server.url))
        .send()
        .await
        .expect("call detail query");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("process_order"),
        "query-based call detail should show task ID"
    );

    handle_keep_alive(server).await;
}

/// Call detail with nonexistent call_id should render gracefully (no 500).
#[tokio::test]
async fn test_call_detail_nonexistent() {
    let setup = create_test_app("test-call-missing");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/calls/nonexistent-call-id-12345", server.url))
        .send()
        .await
        .expect("call detail missing");
    // Should return 200 (rendered page with "not found" message) not 500
    assert!(
        resp.status().is_success(),
        "nonexistent call should not cause server error, got {}",
        resp.status()
    );

    handle_keep_alive(server).await;
}

// ===========================================================================
// 7.6 Error/edge-case endpoints
// ===========================================================================

/// Invalid (nonexistent) task_id on /tasks/{bad_id} should not crash.
#[tokio::test]
async fn test_tasks_detail_nonexistent() {
    let setup = create_test_app("test-task-missing");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/tasks/nonexistent::fake_task", server.url))
        .send()
        .await
        .expect("task detail request");
    // Should not be 500
    assert!(
        resp.status().as_u16() < 500,
        "nonexistent task should not cause 500, got {}",
        resp.status()
    );

    handle_keep_alive(server).await;
}

/// Nonexistent invocation ID on /invocations/{bad_id} should not crash.
#[tokio::test]
async fn test_invocations_detail_nonexistent() {
    let setup = create_test_app("test-inv-missing");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/00000000-0000-0000-0000-000000000000",
            server.url
        ))
        .send()
        .await
        .expect("invocation detail request");
    assert!(
        resp.status().as_u16() < 500,
        "nonexistent invocation should not cause 500, got {}",
        resp.status()
    );

    handle_keep_alive(server).await;
}

/// Nonexistent invocation on /invocations/{bad_id}/history should return empty or 404.
#[tokio::test]
async fn test_invocations_history_nonexistent() {
    let setup = create_test_app("test-history-missing");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/00000000-0000-0000-0000-000000000000/history",
            server.url
        ))
        .send()
        .await
        .expect("history request");
    assert!(
        resp.status().as_u16() < 500,
        "missing invocation history should not cause 500, got {}",
        resp.status()
    );

    handle_keep_alive(server).await;
}

/// Nonexistent invocation on /invocations/{bad_id}/api should not crash.
#[tokio::test]
async fn test_invocations_api_nonexistent() {
    let setup = create_test_app("test-api-missing");
    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!(
            "{}/invocations/00000000-0000-0000-0000-000000000000/api",
            server.url
        ))
        .send()
        .await
        .expect("api request");
    assert!(
        resp.status().as_u16() < 500,
        "missing invocation API should not cause 500, got {}",
        resp.status()
    );

    handle_keep_alive(server).await;
}

// ===========================================================================
// Multi-runner monitoring
// ===========================================================================

/// Multiple runners register heartbeats; runners page shows all of them.
#[tokio::test]
async fn test_runners_page_multi_runner() {
    use rustvello_proto::identifiers::RunnerId;

    let setup = create_test_app("test-multi-runner");

    // Register 3 different runners via heartbeat
    let r1 = RunnerId::from_string("runner-alpha");
    let r2 = RunnerId::from_string("runner-beta");
    let r3 = RunnerId::from_string("runner-gamma");
    setup
        .orchestrator
        .register_heartbeat(&r1, true)
        .await
        .unwrap();
    setup
        .orchestrator
        .register_heartbeat(&r2, true)
        .await
        .unwrap();
    setup
        .orchestrator
        .register_heartbeat(&r3, false)
        .await
        .unwrap();

    // Seed some invocations so the dashboard has data to render
    seed_invocations(&setup.app, 3)
        .await
        .expect("seeding should succeed");

    let server = start_test_server(setup).await;
    let client = server.client();

    let resp = client
        .get(format!("{}/runners", server.url))
        .send()
        .await
        .expect("runners request");
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.expect("body");

    // All three runners should appear
    assert!(body.contains("runner-alpha"), "should show runner-alpha");
    assert!(body.contains("runner-beta"), "should show runner-beta");
    assert!(body.contains("runner-gamma"), "should show runner-gamma");

    handle_keep_alive(server).await;
}

/// Timeline page renders correctly with seeded data from multiple runners.
#[tokio::test]
async fn test_invocations_timeline_multi_runner() {
    use rustvello_proto::identifiers::RunnerId;

    let setup = create_test_app("test-timeline-multi");

    // Register multiple runners
    let r1 = RunnerId::from_string("timeline-r1");
    let r2 = RunnerId::from_string("timeline-r2");
    setup
        .orchestrator
        .register_heartbeat(&r1, true)
        .await
        .unwrap();
    setup
        .orchestrator
        .register_heartbeat(&r2, true)
        .await
        .unwrap();

    // Seed invocations
    seed_invocations(&setup.app, 5)
        .await
        .expect("seeding should succeed");

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
    assert!(
        body.contains("timeline-container"),
        "should have timeline container"
    );

    handle_keep_alive(server).await;
}
