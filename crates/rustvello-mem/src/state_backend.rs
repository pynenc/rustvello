use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use tracing::instrument;

use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::state_backend::{
    StateBackendCore, StateBackendQuery, StateBackendRunner, StoredRunnerContext,
};
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};

struct BackendState {
    invocations: HashMap<Arc<str>, InvocationDTO>,
    calls: HashMap<String, CallDTO>,
    results: HashMap<Arc<str>, String>,
    errors: HashMap<Arc<str>, TaskError>,
    histories: HashMap<Arc<str>, Vec<InvocationHistory>>,
    /// workflow_id → member invocation IDs
    workflow_members: HashMap<Arc<str>, Vec<InvocationId>>,
    /// parent_invocation_id → child invocation IDs
    children: HashMap<Arc<str>, Vec<InvocationId>>,
    /// runner_id → StoredRunnerContext
    runner_contexts: HashMap<String, StoredRunnerContext>,
    /// runner_id → invocation IDs processed by that runner
    runner_invocations: HashMap<String, Vec<InvocationId>>,
    /// workflow_type (TaskId key) → set of WorkflowIdentity runs
    workflow_types: Vec<TaskId>,
    workflow_runs: HashMap<String, Vec<WorkflowIdentity>>,
    /// workflow_id → { key → value }
    workflow_data: HashMap<Arc<str>, HashMap<String, String>>,
    /// app_id → info_json
    app_infos: HashMap<String, String>,
    /// workflow_id → sub-invocation IDs
    workflow_sub_invocations: HashMap<Arc<str>, Vec<InvocationId>>,
}

/// In-memory state backend.
///
/// Stores invocations, calls, results, and history in process memory.
/// Suitable for testing and development only.
pub struct MemStateBackend {
    state: Mutex<BackendState>,
}

impl MemStateBackend {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BackendState {
                invocations: HashMap::with_capacity(64),
                calls: HashMap::with_capacity(64),
                results: HashMap::with_capacity(32),
                errors: HashMap::new(),
                histories: HashMap::with_capacity(64),
                workflow_members: HashMap::new(),
                children: HashMap::new(),
                runner_contexts: HashMap::new(),
                runner_invocations: HashMap::new(),
                workflow_types: Vec::new(),
                workflow_runs: HashMap::new(),
                workflow_data: HashMap::new(),
                app_infos: HashMap::new(),
                workflow_sub_invocations: HashMap::new(),
            }),
        }
    }
}

impl Default for MemStateBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateBackendCore for MemStateBackend {
    #[instrument(skip(self, invocation, call), fields(%invocation.invocation_id))]
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;

        let is_new = !state
            .invocations
            .contains_key(invocation.invocation_id.as_str());

        // Only index on first insert to avoid duplicate entries on upsert
        if is_new {
            // Index workflow membership
            if let Some(ref wf) = invocation.workflow {
                state
                    .workflow_members
                    .entry(Arc::from(wf.workflow_id.as_str()))
                    .or_default()
                    .push(invocation.invocation_id.clone());
            }

            // Index parent-child relationship
            if let Some(ref parent_id) = invocation.parent_invocation_id {
                state
                    .children
                    .entry(Arc::from(parent_id.as_str()))
                    .or_default()
                    .push(invocation.invocation_id.clone());
            }
        }

        state.invocations.insert(
            Arc::from(invocation.invocation_id.as_str()),
            invocation.clone(),
        );
        state.calls.insert(call.call_id.to_string(), call.clone());
        Ok(())
    }

    #[instrument(skip(self), fields(%invocation_id))]
    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        let state = self.state.lock().await;
        state
            .invocations
            .get(invocation_id.as_str())
            .cloned()
            .ok_or_else(|| RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            })
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        let state = self.state.lock().await;
        state
            .calls
            .get(&call_id.to_string())
            .cloned()
            .ok_or_else(|| RustvelloError::state_backend(format!("call not found: {}", call_id)))
    }

    #[instrument(skip(self, result), fields(%invocation_id))]
    async fn store_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .results
            .insert(Arc::from(invocation_id.as_str()), result.to_string());
        Ok(())
    }

    #[instrument(skip(self), fields(%invocation_id))]
    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>> {
        let state = self.state.lock().await;
        Ok(state.results.get(invocation_id.as_str()).cloned())
    }

    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .errors
            .insert(Arc::from(invocation_id.as_str()), error.clone());
        Ok(())
    }

    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        let state = self.state.lock().await;
        Ok(state.errors.get(invocation_id.as_str()).cloned())
    }

    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        // Update runner → invocation reverse index
        let rid = history
            .runner_id
            .as_ref()
            .or(history.status_record.runner_id.as_ref());
        if let Some(r) = rid {
            let inv_id = history.invocation_id.clone();
            let entries = state.runner_invocations.entry(r.to_string()).or_default();
            if !entries.iter().any(|e| e == &inv_id) {
                entries.push(inv_id);
            }
        }
        state
            .histories
            .entry(Arc::from(history.invocation_id.as_str()))
            .or_default()
            .push(history.clone());
        Ok(())
    }

    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let state = self.state.lock().await;
        Ok(state
            .histories
            .get(invocation_id.as_str())
            .cloned()
            .unwrap_or_default())
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state.invocations.clear();
        state.calls.clear();
        state.results.clear();
        state.errors.clear();
        state.histories.clear();
        state.workflow_members.clear();
        state.children.clear();
        state.runner_contexts.clear();
        state.runner_invocations.clear();
        state.workflow_types.clear();
        state.workflow_runs.clear();
        state.workflow_data.clear();
        state.app_infos.clear();
        state.workflow_sub_invocations.clear();
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "In-Memory"
    }

    async fn usage_stats(&self) -> Vec<(&'static str, String)> {
        let state = self.state.lock().await;
        let history_entries: usize = state.histories.values().map(std::vec::Vec::len).sum();
        let mut oldest: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut newest: Option<chrono::DateTime<chrono::Utc>> = None;
        for entries in state.histories.values() {
            for h in entries {
                let ts = h.status_record.timestamp;
                oldest = Some(oldest.map_or(ts, |o| o.min(ts)));
                newest = Some(newest.map_or(ts, |n| n.max(ts)));
            }
        }
        let mut stats = vec![
            ("Invocations", state.invocations.len().to_string()),
            ("Calls", state.calls.len().to_string()),
            ("Results", state.results.len().to_string()),
            ("Errors", state.errors.len().to_string()),
            ("History Entries", history_entries.to_string()),
            ("Workflows", state.workflow_members.len().to_string()),
            ("Runner Contexts", state.runner_contexts.len().to_string()),
        ];
        if let Some(dt) = oldest {
            stats.push(("Oldest Record", dt.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Some(dt) = newest {
            stats.push(("Newest Record", dt.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        stats
    }
}

#[async_trait]
impl StateBackendQuery for MemStateBackend {
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .workflow_members
            .get(workflow_id.as_str())
            .cloned()
            .unwrap_or_default())
    }

    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .children
            .get(parent_invocation_id.as_str())
            .cloned()
            .unwrap_or_default())
    }

    async fn store_workflow_run(&self, workflow: &WorkflowIdentity) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        let type_key = workflow.workflow_type.to_string();
        if !state
            .workflow_types
            .iter()
            .any(|t| t.to_string() == type_key)
        {
            state.workflow_types.push(workflow.workflow_type.clone());
        }
        let runs = state.workflow_runs.entry(type_key).or_default();
        if !runs.iter().any(|r| r.workflow_id == workflow.workflow_id) {
            runs.push(workflow.clone());
        }
        Ok(())
    }

    async fn get_all_workflow_types(&self) -> RustvelloResult<Vec<TaskId>> {
        let state = self.state.lock().await;
        Ok(state.workflow_types.clone())
    }

    async fn get_workflow_runs(
        &self,
        workflow_type: &TaskId,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let state = self.state.lock().await;
        Ok(state
            .workflow_runs
            .get(&workflow_type.to_string())
            .cloned()
            .unwrap_or_default())
    }

    async fn set_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
        value: &str,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .workflow_data
            .entry(Arc::from(workflow_id.as_str()))
            .or_default()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>> {
        let state = self.state.lock().await;
        Ok(state
            .workflow_data
            .get(workflow_id.as_str())
            .and_then(|m| m.get(key).cloned()))
    }

    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .app_infos
            .insert(app_id.to_string(), info_json.to_string());
        Ok(())
    }

    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>> {
        let state = self.state.lock().await;
        Ok(state.app_infos.get(app_id).cloned())
    }

    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
        let state = self.state.lock().await;
        Ok(state
            .app_infos
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .workflow_sub_invocations
            .entry(Arc::from(workflow_id.as_str()))
            .or_default()
            .push(sub_inv_id.clone());
        Ok(())
    }

    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .workflow_sub_invocations
            .get(workflow_id.as_str())
            .cloned()
            .unwrap_or_default())
    }

    async fn get_all_workflow_runs(&self) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let state = self.state.lock().await;
        Ok(state.workflow_runs.values().flatten().cloned().collect())
    }
}

#[async_trait]
impl StateBackendRunner for MemStateBackend {
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .runner_contexts
            .insert(context.runner_id.clone(), context.clone());
        Ok(())
    }

    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>> {
        let state = self.state.lock().await;
        Ok(state.runner_contexts.get(runner_id).cloned())
    }

    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let state = self.state.lock().await;
        Ok(state
            .runner_contexts
            .values()
            .filter(|ctx| ctx.parent_runner_id.as_deref() == Some(parent_runner_id))
            .cloned()
            .collect())
    }

    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        let ids = state
            .runner_invocations
            .get(runner_id)
            .map(|v| {
                let iter = v.iter().skip(offset);
                if limit > 0 {
                    iter.take(limit).cloned().collect()
                } else {
                    iter.cloned().collect()
                }
            })
            .unwrap_or_default();
        Ok(ids)
    }

    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize> {
        let state = self.state.lock().await;
        Ok(state
            .runner_invocations
            .get(runner_id)
            .map_or(0, std::vec::Vec::len))
    }

    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let state = self.state.lock().await;
        let mut all: Vec<&InvocationHistory> = state
            .histories
            .values()
            .flat_map(|v| v.iter())
            .filter(|h| {
                let ts = h.history_timestamp.unwrap_or(h.status_record.timestamp);
                ts >= start && ts <= end
            })
            .collect();
        // Sort by timestamp ascending
        all.sort_by_key(|h| h.history_timestamp.unwrap_or(h.status_record.timestamp));
        let result = all
            .into_iter()
            .skip(offset)
            .take(if limit > 0 { limit } else { usize::MAX })
            .cloned()
            .collect();
        Ok(result)
    }

    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let state = self.state.lock().await;
        Ok(state
            .runner_contexts
            .values()
            .filter(|ctx| ctx.runner_id.contains(partial_id))
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_proto::call::SerializedArguments;
    use rustvello_proto::identifiers::TaskId;
    use rustvello_proto::invocation::{InvocationDTO, WorkflowIdentity};
    use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

    fn make_fixtures() -> (InvocationDTO, CallDTO) {
        let task_id = TaskId::new("test.module", "my_task");
        let mut args = SerializedArguments::new();
        args.insert("x", "42");
        let call = CallDTO::new(task_id.clone(), args);
        let inv_id = InvocationId::new();
        let inv = InvocationDTO::new(inv_id, task_id, call.call_id.clone());
        (inv, call)
    }

    #[tokio::test]
    async fn test_upsert_and_get() {
        let backend = MemStateBackend::new();
        let (inv, call) = make_fixtures();

        backend.upsert_invocation(&inv, &call).await.unwrap();

        let retrieved_inv = backend.get_invocation(&inv.invocation_id).await.unwrap();
        assert_eq!(retrieved_inv.invocation_id, inv.invocation_id);

        let retrieved_call = backend.get_call(&call.call_id).await.unwrap();
        assert_eq!(retrieved_call.call_id, call.call_id);
    }

    #[tokio::test]
    async fn test_results() {
        let backend = MemStateBackend::new();
        let inv_id = InvocationId::new();

        assert!(backend.get_result(&inv_id).await.unwrap().is_none());

        backend.store_result(&inv_id, "42").await.unwrap();
        let result = backend.get_result(&inv_id).await.unwrap();
        assert_eq!(result, Some("42".to_string()));
    }

    #[tokio::test]
    async fn test_errors() {
        let backend = MemStateBackend::new();
        let inv_id = InvocationId::new();

        let error = TaskError {
            error_type: "ValueError".to_string(),
            message: "something went wrong".to_string(),
            traceback: None,
        };

        backend.store_error(&inv_id, &error).await.unwrap();
        let retrieved = backend.get_error(&inv_id).await.unwrap().unwrap();
        assert_eq!(retrieved.error_type, "ValueError");
    }

    #[tokio::test]
    async fn test_history() {
        let backend = MemStateBackend::new();
        let inv_id = InvocationId::new();

        let history = InvocationHistory::new(
            inv_id.clone(),
            InvocationStatusRecord::new(InvocationStatus::Registered, None),
            None,
        );
        backend.add_history(&history).await.unwrap();

        let histories = backend.get_history(&inv_id).await.unwrap();
        assert_eq!(histories.len(), 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let backend = MemStateBackend::new();
        let (inv, call) = make_fixtures();
        backend.upsert_invocation(&inv, &call).await.unwrap();

        backend.purge().await.unwrap();

        assert!(backend.get_invocation(&inv.invocation_id).await.is_err());
    }

    #[tokio::test]
    async fn test_workflow_invocations() {
        let backend = MemStateBackend::new();
        let task_id = TaskId::new("mod", "task");
        let mut args = SerializedArguments::new();
        args.insert("x", "1");

        // Create a root workflow invocation
        let root_inv_id = InvocationId::from_string("root-1");
        let wf = WorkflowIdentity::root(root_inv_id.clone(), task_id.clone());
        let call = CallDTO::new(task_id.clone(), args.clone());
        let inv = InvocationDTO::with_workflow(
            root_inv_id.clone(),
            task_id.clone(),
            call.call_id.clone(),
            None,
            wf.clone(),
        );
        backend.upsert_invocation(&inv, &call).await.unwrap();

        // Create a child in the same workflow
        let child_inv_id = InvocationId::from_string("child-1");
        let call2 = CallDTO::new(task_id.clone(), args);
        let inv2 = InvocationDTO::with_workflow(
            child_inv_id.clone(),
            task_id.clone(),
            call2.call_id.clone(),
            Some(root_inv_id.clone()),
            wf.clone(),
        );
        backend.upsert_invocation(&inv2, &call2).await.unwrap();

        // Query workflow members
        let members = backend
            .get_workflow_invocations(&root_inv_id)
            .await
            .unwrap();
        assert_eq!(members.len(), 2);

        // Query children of root
        let children = backend.get_child_invocations(&root_inv_id).await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0], child_inv_id);
    }

    #[tokio::test]
    async fn test_no_workflow_returns_empty() {
        let backend = MemStateBackend::new();
        let inv_id = InvocationId::from_string("nonexistent");
        let members = backend.get_workflow_invocations(&inv_id).await.unwrap();
        assert!(members.is_empty());
    }

    // --- Workflow discovery tests ---

    #[tokio::test]
    async fn test_store_and_get_workflow_runs() {
        let backend = MemStateBackend::new();
        let task_id = TaskId::new("mod", "my_workflow");
        let wf_id = InvocationId::from_string("wf-run-1");
        let wf = WorkflowIdentity::root(wf_id.clone(), task_id.clone());

        backend.store_workflow_run(&wf).await.unwrap();

        let types = backend.get_all_workflow_types().await.unwrap();
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].to_string(), task_id.to_string());

        let runs = backend.get_workflow_runs(&task_id).await.unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].workflow_id, wf_id);
    }

    #[tokio::test]
    async fn test_multiple_workflow_types() {
        let backend = MemStateBackend::new();
        let task_a = TaskId::new("mod", "workflow_a");
        let task_b = TaskId::new("mod", "workflow_b");

        let wf_a = WorkflowIdentity::root(InvocationId::from_string("wf-a"), task_a.clone());
        let wf_b = WorkflowIdentity::root(InvocationId::from_string("wf-b"), task_b.clone());

        backend.store_workflow_run(&wf_a).await.unwrap();
        backend.store_workflow_run(&wf_b).await.unwrap();

        let types = backend.get_all_workflow_types().await.unwrap();
        assert_eq!(types.len(), 2);

        let runs_a = backend.get_workflow_runs(&task_a).await.unwrap();
        assert_eq!(runs_a.len(), 1);
        let runs_b = backend.get_workflow_runs(&task_b).await.unwrap();
        assert_eq!(runs_b.len(), 1);
    }

    #[tokio::test]
    async fn test_multiple_runs_same_type() {
        let backend = MemStateBackend::new();
        let task_id = TaskId::new("mod", "my_workflow");

        for i in 0..3 {
            let wf = WorkflowIdentity::root(
                InvocationId::from_string(format!("wf-{i}")),
                task_id.clone(),
            );
            backend.store_workflow_run(&wf).await.unwrap();
        }

        let types = backend.get_all_workflow_types().await.unwrap();
        assert_eq!(types.len(), 1);

        let runs = backend.get_workflow_runs(&task_id).await.unwrap();
        assert_eq!(runs.len(), 3);
    }

    // --- Workflow data tests ---

    #[tokio::test]
    async fn test_workflow_data_set_get() {
        let backend = MemStateBackend::new();
        let wf_id = InvocationId::from_string("wf-data-1");

        // Set and get
        backend
            .set_workflow_data(&wf_id, "key1", "value1")
            .await
            .unwrap();
        let val = backend.get_workflow_data(&wf_id, "key1").await.unwrap();
        assert_eq!(val, Some("value1".to_string()));

        // Non-existent key returns None
        let val = backend.get_workflow_data(&wf_id, "missing").await.unwrap();
        assert!(val.is_none());
    }

    #[tokio::test]
    async fn test_workflow_data_update() {
        let backend = MemStateBackend::new();
        let wf_id = InvocationId::from_string("wf-data-2");

        backend
            .set_workflow_data(&wf_id, "counter", "1")
            .await
            .unwrap();
        backend
            .set_workflow_data(&wf_id, "counter", "2")
            .await
            .unwrap();
        let val = backend.get_workflow_data(&wf_id, "counter").await.unwrap();
        assert_eq!(val, Some("2".to_string()));
    }

    #[tokio::test]
    async fn test_workflow_data_isolation() {
        let backend = MemStateBackend::new();
        let wf1 = InvocationId::from_string("wf-iso-1");
        let wf2 = InvocationId::from_string("wf-iso-2");

        backend
            .set_workflow_data(&wf1, "key", "val1")
            .await
            .unwrap();
        backend
            .set_workflow_data(&wf2, "key", "val2")
            .await
            .unwrap();

        assert_eq!(
            backend.get_workflow_data(&wf1, "key").await.unwrap(),
            Some("val1".to_string())
        );
        assert_eq!(
            backend.get_workflow_data(&wf2, "key").await.unwrap(),
            Some("val2".to_string())
        );
    }

    #[tokio::test]
    async fn test_workflow_data_purge() {
        let backend = MemStateBackend::new();
        let wf_id = InvocationId::from_string("wf-purge");

        backend
            .set_workflow_data(&wf_id, "key", "val")
            .await
            .unwrap();
        backend.purge().await.unwrap();

        let val = backend.get_workflow_data(&wf_id, "key").await.unwrap();
        assert!(val.is_none());
    }
}
