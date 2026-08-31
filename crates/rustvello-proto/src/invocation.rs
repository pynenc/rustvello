use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::identifiers::{CallId, InvocationId, RunnerId, TaskId};
use crate::status::{InvocationStatus, InvocationStatusRecord};

/// Persistence DTO for an invocation.
///
/// Contains identity and metadata but not the call arguments themselves
/// (those are in `CallDTO`, separated for efficient storage).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationDTO {
    pub invocation_id: InvocationId,
    pub task_id: TaskId,
    pub call_id: CallId,
    pub status: InvocationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// The parent invocation that spawned this one (None for top-level calls).
    pub parent_invocation_id: Option<InvocationId>,
    /// Workflow identity — present for invocations that belong to a workflow.
    pub workflow: Option<WorkflowIdentity>,
}

impl InvocationDTO {
    pub fn new(invocation_id: InvocationId, task_id: TaskId, call_id: CallId) -> Self {
        let now = Utc::now();
        Self {
            invocation_id,
            task_id,
            call_id,
            status: InvocationStatus::Registered,
            created_at: now,
            updated_at: now,
            parent_invocation_id: None,
            workflow: None,
        }
    }

    /// Create a DTO with workflow identity and optional parent.
    pub fn with_workflow(
        invocation_id: InvocationId,
        task_id: TaskId,
        call_id: CallId,
        parent_invocation_id: Option<InvocationId>,
        workflow: WorkflowIdentity,
    ) -> Self {
        let now = Utc::now();
        Self {
            invocation_id,
            task_id,
            call_id,
            status: InvocationStatus::Registered,
            created_at: now,
            updated_at: now,
            parent_invocation_id,
            workflow: Some(workflow),
        }
    }

    /// Whether this invocation defines its workflow or sub-workflow identity.
    pub fn is_workflow_defining(&self) -> bool {
        self.workflow
            .as_ref()
            .is_some_and(|workflow| workflow.workflow_id == self.invocation_id)
    }
}

/// An audit log entry for status changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationHistory {
    pub invocation_id: InvocationId,
    pub status_record: InvocationStatusRecord,
    pub message: Option<String>,
    /// The runner context that produced this status change, if any.
    pub runner_id: Option<RunnerId>,
    /// The parent invocation that registered this one (workflow lineage).
    pub registered_by_inv_id: Option<InvocationId>,
    /// The history-level timestamp (mirrors pynenc's `InvocationHistory._timestamp`).
    /// Used for time-range filtering. Defaults to `status_record.timestamp` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_timestamp: Option<DateTime<Utc>>,
}

impl InvocationHistory {
    pub fn new(
        invocation_id: InvocationId,
        status_record: InvocationStatusRecord,
        message: Option<String>,
    ) -> Self {
        Self {
            invocation_id,
            status_record,
            message,
            runner_id: None,
            registered_by_inv_id: None,
            history_timestamp: None,
        }
    }

    pub fn with_runner(mut self, runner_id: RunnerId) -> Self {
        self.runner_id = Some(runner_id);
        self
    }

    pub fn with_registered_by(mut self, inv_id: InvocationId) -> Self {
        self.registered_by_inv_id = Some(inv_id);
        self
    }
}

/// Tracks parent-child relationships in distributed workflows.
///
/// Matches pynenc's `WorkflowIdentity`. Every workflow is rooted by a
/// defining invocation; the `workflow_type` captures which task started it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowIdentity {
    /// The root workflow invocation ID
    pub workflow_id: InvocationId,
    /// The task that defines this workflow (the root task)
    pub workflow_type: TaskId,
    /// The parent workflow ID if this is a sub-workflow (None for top-level)
    pub parent_id: Option<InvocationId>,
    /// Depth in the workflow tree (0 for root)
    pub depth: u32,
}

impl WorkflowIdentity {
    /// Create a new root workflow identity.
    pub fn root(workflow_id: InvocationId, workflow_type: TaskId) -> Self {
        Self {
            workflow_id,
            workflow_type,
            parent_id: None,
            depth: 0,
        }
    }

    /// Create a child identity within the same workflow.
    pub fn child(
        workflow_id: InvocationId,
        workflow_type: TaskId,
        parent_id: InvocationId,
        depth: u32,
    ) -> Self {
        Self {
            workflow_id,
            workflow_type,
            parent_id: Some(parent_id),
            depth,
        }
    }

    /// Create a new explicit sub-workflow identity.
    pub fn sub_workflow(
        workflow_id: InvocationId,
        workflow_type: TaskId,
        parent_workflow_id: InvocationId,
    ) -> Self {
        Self {
            workflow_id,
            workflow_type,
            parent_id: Some(parent_workflow_id),
            depth: 0,
        }
    }

    /// Returns true if this workflow is nested under another.
    pub fn is_sub_workflow(&self) -> bool {
        self.parent_id.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::RunnerId;

    #[test]
    fn invocation_dto_new() {
        let task_id = TaskId::new("mod", "func");
        let call_id = CallId::new(task_id.clone(), "hash123");
        let inv_id = InvocationId::from_string("inv-1");
        let dto = InvocationDTO::new(inv_id.clone(), task_id.clone(), call_id.clone());

        assert_eq!(dto.invocation_id, inv_id);
        assert_eq!(dto.task_id, task_id);
        assert_eq!(dto.call_id, call_id);
        assert_eq!(dto.status, InvocationStatus::Registered);
        assert!(dto.created_at <= dto.updated_at);
    }

    #[test]
    fn invocation_dto_serde_round_trip() {
        let task_id = TaskId::new("mod", "func");
        let call_id = CallId::new(task_id.clone(), "hash123");
        let dto = InvocationDTO::new(InvocationId::new(), task_id, call_id);

        let json = serde_json::to_string(&dto).unwrap();
        let back: InvocationDTO = serde_json::from_str(&json).unwrap();
        assert_eq!(back.invocation_id, dto.invocation_id);
        assert_eq!(back.status, InvocationStatus::Registered);
    }

    #[test]
    fn workflow_defining_is_derived_from_persisted_identity() {
        let task_id = TaskId::new("mod", "workflow");
        let root_id = InvocationId::from_string("root-1");
        let call_id = CallId::new(task_id.clone(), "hash");
        let root = InvocationDTO::with_workflow(
            root_id.clone(),
            task_id.clone(),
            call_id.clone(),
            None,
            WorkflowIdentity::root(root_id.clone(), task_id.clone()),
        );
        let child = InvocationDTO::with_workflow(
            InvocationId::from_string("child-1"),
            task_id.clone(),
            call_id,
            Some(root_id.clone()),
            WorkflowIdentity::child(root_id, task_id, InvocationId::from_string("child-1"), 1),
        );

        assert!(root.is_workflow_defining());
        assert!(!child.is_workflow_defining());
    }

    #[test]
    fn invocation_history_new() {
        let inv_id = InvocationId::from_string("inv-1");
        let record = InvocationStatusRecord::new(
            InvocationStatus::Running,
            Some(RunnerId::from_string("runner-1")),
        );
        let history = InvocationHistory::new(inv_id.clone(), record, Some("started".to_string()));

        assert_eq!(history.invocation_id, inv_id);
        assert_eq!(history.status_record.status, InvocationStatus::Running);
        assert_eq!(history.message, Some("started".to_string()));
    }

    #[test]
    fn invocation_history_serde_round_trip() {
        let history = InvocationHistory::new(
            InvocationId::from_string("inv-1"),
            InvocationStatusRecord::new(InvocationStatus::Success, None),
            None,
        );
        let json = serde_json::to_string(&history).unwrap();
        let back: InvocationHistory = serde_json::from_str(&json).unwrap();
        assert_eq!(back.invocation_id, history.invocation_id);
        assert_eq!(back.status_record.status, InvocationStatus::Success);
    }

    #[test]
    fn workflow_identity_root() {
        let wf = WorkflowIdentity::root(
            InvocationId::from_string("wf-1"),
            TaskId::new("mod", "my_task"),
        );
        assert_eq!(wf.workflow_id.as_str(), "wf-1");
        assert_eq!(wf.workflow_type.name(), "my_task");
        assert!(wf.parent_id.is_none());
        assert_eq!(wf.depth, 0);
        assert!(!wf.is_sub_workflow());
    }

    #[test]
    fn workflow_identity_child() {
        let wf = WorkflowIdentity::child(
            InvocationId::from_string("wf-1"),
            TaskId::new("mod", "parent_task"),
            InvocationId::from_string("parent-1"),
            2,
        );
        assert_eq!(wf.workflow_id.as_str(), "wf-1");
        assert_eq!(wf.workflow_type.name(), "parent_task");
        assert_eq!(wf.parent_id.unwrap().as_str(), "parent-1");
        assert_eq!(wf.depth, 2);
    }

    #[test]
    fn workflow_identity_sub_workflow() {
        let wf = WorkflowIdentity::sub_workflow(
            InvocationId::from_string("sub-wf-1"),
            TaskId::new("mod", "sub_task"),
            InvocationId::from_string("parent-wf-1"),
        );
        assert_eq!(wf.workflow_id.as_str(), "sub-wf-1");
        assert_eq!(wf.workflow_type.name(), "sub_task");
        assert!(wf.is_sub_workflow());
        assert_eq!(wf.depth, 0);
    }

    #[test]
    fn workflow_identity_serde_round_trip() {
        let wf = WorkflowIdentity::child(
            InvocationId::from_string("wf-1"),
            TaskId::new("mod", "task"),
            InvocationId::from_string("p-1"),
            3,
        );
        let json = serde_json::to_string(&wf).unwrap();
        let back: WorkflowIdentity = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workflow_id, wf.workflow_id);
        assert_eq!(back.workflow_type, wf.workflow_type);
        assert_eq!(back.depth, 3);
    }

    #[test]
    fn invocation_dto_with_workflow() {
        let task_id = TaskId::new("mod", "func");
        let call_id = CallId::new(task_id.clone(), "hash123");
        let inv_id = InvocationId::from_string("inv-1");
        let parent_id = InvocationId::from_string("parent-1");
        let wf = WorkflowIdentity::root(inv_id.clone(), task_id.clone());

        let dto = InvocationDTO::with_workflow(
            inv_id.clone(),
            task_id.clone(),
            call_id,
            Some(parent_id.clone()),
            wf,
        );

        assert_eq!(dto.parent_invocation_id.as_ref().unwrap(), &parent_id);
        assert!(dto.workflow.is_some());
        assert_eq!(dto.workflow.as_ref().unwrap().workflow_type, task_id);
    }

    #[test]
    fn invocation_dto_without_workflow_is_none() {
        let task_id = TaskId::new("mod", "func");
        let call_id = CallId::new(task_id.clone(), "hash123");
        let dto = InvocationDTO::new(InvocationId::new(), task_id, call_id);
        assert!(dto.parent_invocation_id.is_none());
        assert!(dto.workflow.is_none());
    }

    #[test]
    fn invocation_dto_with_workflow_serde() {
        let task_id = TaskId::new("mod", "func");
        let call_id = CallId::new(task_id.clone(), "hash");
        let inv_id = InvocationId::from_string("inv-1");
        let wf = WorkflowIdentity::root(inv_id.clone(), task_id.clone());

        let dto = InvocationDTO::with_workflow(inv_id, task_id, call_id, None, wf);
        let json = serde_json::to_string(&dto).unwrap();
        let back: InvocationDTO = serde_json::from_str(&json).unwrap();
        assert!(back.workflow.is_some());
        assert_eq!(back.workflow.unwrap().workflow_type.name(), "func");
    }
}
