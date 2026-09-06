//! Family tree data structure and construction from invocation history.

use chrono::{DateTime, Utc};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::status::InvocationStatus;
use std::sync::Arc;

/// A node in the invocation family tree.
#[derive(Debug, Clone)]
pub struct FamilyTreeNode {
    pub invocation_id: String,
    pub task_module: String,
    pub task_func: String,
    pub status: InvocationStatus,
    pub created_at: DateTime<Utc>,
    pub elapsed_secs: f64,
    pub children: Vec<FamilyTreeNode>,
    /// True if children were truncated due to depth/budget limits.
    pub truncated: bool,
}

const MAX_DEPTH: usize = 8;
const MAX_NODES: usize = 60;

/// Build a family tree starting from the given invocation, walking to root first.
pub async fn build_family_tree(
    invocation_id: &InvocationId,
    orchestrator: &Arc<dyn InvocationControlBackend>,
    state_backend: &Arc<dyn StateBackend>,
    expand_ids: &[String],
) -> Option<FamilyTreeNode> {
    // Walk to root
    let root_id = find_root(invocation_id, state_backend).await;

    // Build tree from root
    let mut node_count = 0;
    build_node(
        &root_id,
        orchestrator,
        state_backend,
        0,
        &mut node_count,
        expand_ids,
    )
    .await
}

/// Walk parent chain to find the root invocation.
async fn find_root(
    invocation_id: &InvocationId,
    state_backend: &Arc<dyn StateBackend>,
) -> InvocationId {
    let mut current = invocation_id.clone();
    let mut visited = std::collections::HashSet::new();

    loop {
        if !visited.insert(current.to_string()) {
            break;
        }
        match state_backend.get_invocation(&current).await {
            Ok(inv) => {
                if let Some(parent_id) = inv.parent_invocation_id {
                    current = parent_id;
                } else {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    current
}

/// Recursively build a tree node with depth and budget limits.
async fn build_node(
    invocation_id: &InvocationId,
    orchestrator: &Arc<dyn InvocationControlBackend>,
    state_backend: &Arc<dyn StateBackend>,
    depth: usize,
    node_count: &mut usize,
    expand_ids: &[String],
) -> Option<FamilyTreeNode> {
    if *node_count >= MAX_NODES {
        return None;
    }
    *node_count += 1;

    let inv = state_backend.get_invocation(invocation_id).await.ok()?;
    let status = orchestrator
        .get_invocation_status(invocation_id)
        .await
        .map_or_else(
            |e| {
                tracing::warn!(
                    invocation_id = %invocation_id,
                    error = %e,
                    "Failed to get invocation status, defaulting to Registered"
                );
                InvocationStatus::Registered
            },
            |r| r.status,
        );

    let task_module = format!("{}::{}", inv.task_id.language(), inv.task_id.module());
    let task_func = inv.task_id.name().to_owned();

    let elapsed = (inv.updated_at - inv.created_at).num_milliseconds() as f64 / 1000.0;

    let mut children = Vec::new();
    let mut truncated = false;

    let inv_id_str = invocation_id.to_string();
    let should_expand = depth < MAX_DEPTH && (depth < 2 || expand_ids.contains(&inv_id_str));

    if should_expand {
        let child_ids = state_backend
            .get_child_invocations(invocation_id)
            .await
            .unwrap_or_default();

        for child_id in &child_ids {
            if *node_count >= MAX_NODES {
                truncated = true;
                break;
            }
            if let Some(child_node) = Box::pin(build_node(
                child_id,
                orchestrator,
                state_backend,
                depth + 1,
                node_count,
                expand_ids,
            ))
            .await
            {
                children.push(child_node);
            }
        }

        if child_ids.len() > children.len() {
            truncated = true;
        }
    } else if depth >= MAX_DEPTH {
        // Check if there are children we're not showing
        let child_ids = state_backend
            .get_child_invocations(invocation_id)
            .await
            .unwrap_or_default();
        truncated = !child_ids.is_empty();
    }

    Some(FamilyTreeNode {
        invocation_id: inv_id_str,
        task_module,
        task_func,
        status,
        created_at: inv.created_at,
        elapsed_secs: elapsed,
        children,
        truncated,
    })
}
