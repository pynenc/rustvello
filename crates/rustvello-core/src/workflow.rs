//! Explicit workflow-root operations and deterministic replay support.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::context::get_invocation_context;
use crate::error::{RustvelloError, RustvelloResult};
use crate::state_backend::StateBackend;
use rustvello_proto::identifiers::InvocationId;

/// Root-scoped deterministic operations for an explicit workflow invocation.
///
/// Obtain this from [`WorkflowRoot::current`] inside a `#[rustvello::workflow]`
/// function. Ordinary tasks, including child tasks that belong to a workflow,
/// cannot construct this root handle.
pub struct WorkflowRoot {
    executor: DeterministicExecutor,
}

impl WorkflowRoot {
    /// Resolve the current explicit workflow root.
    pub fn current() -> RustvelloResult<Self> {
        let ctx = get_invocation_context().ok_or(RustvelloError::WorkflowContextUnavailable)?;
        let workflow = ctx
            .workflow
            .ok_or_else(|| RustvelloError::WorkflowMembershipRequired {
                invocation_id: ctx.invocation_id.clone(),
            })?;

        if !ctx.is_workflow_defining || workflow.workflow_id != ctx.invocation_id {
            return Err(RustvelloError::WorkflowRootRequired {
                invocation_id: ctx.invocation_id,
                workflow_id: workflow.workflow_id,
            });
        }

        let state_backend = ctx
            .state_backend
            .ok_or(RustvelloError::WorkflowContextUnavailable)?;
        Ok(Self {
            executor: DeterministicExecutor::new(workflow.workflow_id, state_backend),
        })
    }

    /// Generate the next deterministic random value in `[0, 1)`.
    pub fn random(&mut self) -> RustvelloResult<f64> {
        let handle = current_runtime_handle()?;
        handle.block_on(self.executor.random())
    }

    /// Return the next deterministic UTC timestamp.
    pub fn utc_now(&mut self) -> RustvelloResult<DateTime<Utc>> {
        let handle = current_runtime_handle()?;
        handle.block_on(self.executor.utc_now())
    }

    /// Return the next deterministic UUID string.
    pub fn uuid(&mut self) -> RustvelloResult<String> {
        let handle = current_runtime_handle()?;
        handle.block_on(self.executor.uuid())
    }

    /// Async variant of [`WorkflowRoot::random`] for embedded runtimes.
    pub async fn random_async(&mut self) -> RustvelloResult<f64> {
        self.executor.random().await
    }

    /// Async variant of [`WorkflowRoot::utc_now`] for embedded runtimes.
    pub async fn utc_now_async(&mut self) -> RustvelloResult<DateTime<Utc>> {
        self.executor.utc_now().await
    }

    /// Async variant of [`WorkflowRoot::uuid`] for embedded runtimes.
    pub async fn uuid_async(&mut self) -> RustvelloResult<String> {
        self.executor.uuid().await
    }
}

fn current_runtime_handle() -> RustvelloResult<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().map_err(|error| RustvelloError::Internal {
        message: format!("workflow operation requires a Tokio runtime: {error}"),
    })
}

/// Internal replay engine used by [`WorkflowRoot`].
///
/// Mirrors pynenc's `DeterministicExecutor`. Ensures that operations like
/// random number generation, time functions, and UUIDs behave deterministically
/// across workflow replays by using deterministic seeds and storing results
/// in the state backend.
pub(crate) struct DeterministicExecutor {
    workflow_id: InvocationId,
    state_backend: Arc<dyn StateBackend>,
    operation_counters: HashMap<String, u64>,
}

impl DeterministicExecutor {
    /// Create a new deterministic executor for a workflow.
    fn new(workflow_id: InvocationId, state_backend: Arc<dyn StateBackend>) -> Self {
        Self {
            workflow_id,
            state_backend,
            operation_counters: HashMap::new(),
        }
    }

    /// Get the next sequence number for an operation type.
    pub fn get_next_sequence(&mut self, operation: &str) -> u64 {
        let counter = self
            .operation_counters
            .entry(operation.to_string())
            .or_insert(0);
        *counter += 1;
        *counter
    }

    /// Get the current count for an operation type.
    pub fn get_operation_count(&self, operation: &str) -> u64 {
        self.operation_counters.get(operation).copied().unwrap_or(0)
    }

    /// Execute an operation with deterministic results.
    ///
    /// Checks if a value exists for this operation+sequence in the state backend.
    /// If found, returns the stored value (replay mode). Otherwise, generates
    /// a new value using the provided generator and stores it.
    pub async fn deterministic_operation<F>(
        &mut self,
        operation: &str,
        generator: F,
    ) -> RustvelloResult<String>
    where
        F: FnOnce() -> String,
    {
        let sequence = self.get_next_sequence(operation);
        let operation_key = format!("{operation}:{sequence}");

        // Check for stored value (replay)
        if let Some(value) = self
            .state_backend
            .get_workflow_data(&self.workflow_id, &operation_key)
            .await?
        {
            return Ok(value);
        }

        // Generate new value and store
        let value = generator();
        self.state_backend
            .set_workflow_data(&self.workflow_id, &operation_key, &value)
            .await?;

        // Update total count
        let total_count_key = format!("counter:{operation}");
        let current_total = self
            .state_backend
            .get_workflow_data(&self.workflow_id, &total_count_key)
            .await?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        self.state_backend
            .set_workflow_data(
                &self.workflow_id,
                &total_count_key,
                &current_total.max(sequence).to_string(),
            )
            .await?;

        Ok(value)
    }

    /// Get or establish the workflow base time for deterministic timestamps.
    pub async fn get_base_time(&self) -> RustvelloResult<DateTime<Utc>> {
        let base_time_key = "workflow:base_time";
        if let Some(stored) = self
            .state_backend
            .get_workflow_data(&self.workflow_id, base_time_key)
            .await?
        {
            let dt = DateTime::parse_from_rfc3339(&stored)
                .map_or_else(|_| Utc::now(), |dt| dt.with_timezone(&Utc));
            return Ok(dt);
        }

        let base_time = Utc::now();
        self.state_backend
            .set_workflow_data(&self.workflow_id, base_time_key, &base_time.to_rfc3339())
            .await?;
        Ok(base_time)
    }

    /// Generate a deterministic random number (0.0..1.0) using workflow-specific seed.
    pub async fn random(&mut self) -> RustvelloResult<f64> {
        let wf_id = self.workflow_id.as_str().to_owned();
        let seq = self.get_operation_count("random") + 1;

        let value_str = self
            .deterministic_operation("random", move || {
                let seed_string = format!("{wf_id}:random:{seq}");
                let hash = Sha256::digest(seed_string.as_bytes());
                // Use first 8 bytes as u64 seed, then map to [0, 1)
                let bytes: [u8; 8] = hash[..8]
                    .try_into()
                    .expect("SHA-256 always produces ≥8 bytes");
                let seed = u64::from_le_bytes(bytes);
                let random_val = (seed as f64) / (u64::MAX as f64);
                random_val.to_string()
            })
            .await?;

        Ok(value_str.parse::<f64>().unwrap_or(0.0))
    }

    /// Get current time deterministically by advancing from base time.
    pub async fn utc_now(&mut self) -> RustvelloResult<DateTime<Utc>> {
        let base_time = self.get_base_time().await?;
        let seq = self.get_operation_count("time") + 1;

        let value_str = self
            .deterministic_operation("time", move || {
                let current_time = base_time + chrono::Duration::seconds(seq as i64);
                current_time.to_rfc3339()
            })
            .await?;

        let dt = DateTime::parse_from_rfc3339(&value_str)
            .map_or_else(|_| Utc::now(), |dt| dt.with_timezone(&Utc));
        Ok(dt)
    }

    /// Generate a deterministic UUID using workflow-specific seed.
    pub async fn uuid(&mut self) -> RustvelloResult<String> {
        let wf_id = self.workflow_id.as_str().to_owned();
        let seq = self.get_operation_count("uuid") + 1;

        self.deterministic_operation("uuid", move || {
            let seed_string = format!("{wf_id}:uuid:{seq}");
            let hash = Sha256::digest(seed_string.as_bytes());
            // Use first 16 bytes to construct a UUID
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&hash[..16]);
            // Set version 4 and variant bits for valid UUID format
            bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
            bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 1
            let u = uuid::Uuid::from_bytes(bytes);
            u.to_string()
        })
        .await
    }
}

#[cfg(test)]
#[allow(clippy::clone_on_ref_ptr)]
mod tests {
    use super::*;
    use crate::state_backend::{StateBackendCore, StateBackendQuery, StateBackendRunner};

    // Helper to create an in-memory state backend for testing.
    // We use a simple inline implementation to avoid depending on rustvello-mem
    // in the core crate.
    struct TestStateBackend {
        data: std::sync::Mutex<HashMap<String, HashMap<String, String>>>,
    }

    impl TestStateBackend {
        fn new() -> Self {
            Self {
                data: std::sync::Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl StateBackendCore for TestStateBackend {
        async fn upsert_invocation(
            &self,
            _inv: &rustvello_proto::invocation::InvocationDTO,
            _call: &rustvello_proto::call::CallDTO,
        ) -> RustvelloResult<()> {
            Ok(())
        }
        async fn get_invocation(
            &self,
            id: &InvocationId,
        ) -> RustvelloResult<rustvello_proto::invocation::InvocationDTO> {
            Err(crate::error::RustvelloError::InvocationNotFound {
                invocation_id: id.clone(),
            })
        }
        async fn get_call(
            &self,
            id: &rustvello_proto::identifiers::CallId,
        ) -> RustvelloResult<rustvello_proto::call::CallDTO> {
            Err(crate::error::RustvelloError::Internal {
                message: format!("call not found: {id}"),
            })
        }
        async fn store_result(&self, _id: &InvocationId, _r: &str) -> RustvelloResult<()> {
            Ok(())
        }
        async fn get_result(&self, _id: &InvocationId) -> RustvelloResult<Option<String>> {
            Ok(None)
        }
        async fn store_error(
            &self,
            _id: &InvocationId,
            _e: &crate::error::TaskError,
        ) -> RustvelloResult<()> {
            Ok(())
        }
        async fn get_error(
            &self,
            _id: &InvocationId,
        ) -> RustvelloResult<Option<crate::error::TaskError>> {
            Ok(None)
        }
        async fn add_history(
            &self,
            _h: &rustvello_proto::invocation::InvocationHistory,
        ) -> RustvelloResult<()> {
            Ok(())
        }
        async fn get_history(
            &self,
            _id: &InvocationId,
        ) -> RustvelloResult<Vec<rustvello_proto::invocation::InvocationHistory>> {
            Ok(Vec::new())
        }
        async fn purge(&self) -> RustvelloResult<()> {
            self.data.lock().unwrap().clear();
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl StateBackendQuery for TestStateBackend {
        async fn set_workflow_data(
            &self,
            workflow_id: &InvocationId,
            key: &str,
            value: &str,
        ) -> RustvelloResult<()> {
            self.data
                .lock()
                .unwrap()
                .entry(workflow_id.as_str().to_string())
                .or_default()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }

        async fn get_workflow_data(
            &self,
            workflow_id: &InvocationId,
            key: &str,
        ) -> RustvelloResult<Option<String>> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(&workflow_id.as_str().to_string())
                .and_then(|m| m.get(key).cloned()))
        }

        async fn get_workflow_invocations(
            &self,
            _workflow_id: &InvocationId,
        ) -> RustvelloResult<Vec<InvocationId>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_workflow_invocations".into(),
            })
        }
        async fn get_child_invocations(
            &self,
            _parent_invocation_id: &InvocationId,
        ) -> RustvelloResult<Vec<InvocationId>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_child_invocations".into(),
            })
        }
        async fn store_workflow_run(
            &self,
            _workflow: &rustvello_proto::invocation::WorkflowIdentity,
        ) -> RustvelloResult<()> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "store_workflow_run".into(),
            })
        }
        async fn get_all_workflow_types(
            &self,
        ) -> RustvelloResult<Vec<rustvello_proto::identifiers::TaskId>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_all_workflow_types".into(),
            })
        }
        async fn get_workflow_runs(
            &self,
            _workflow_type: &rustvello_proto::identifiers::TaskId,
        ) -> RustvelloResult<Vec<rustvello_proto::invocation::WorkflowIdentity>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_workflow_runs".into(),
            })
        }
        async fn store_app_info(&self, _app_id: &str, _info_json: &str) -> RustvelloResult<()> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "store_app_info".into(),
            })
        }
        async fn get_app_info(&self, _app_id: &str) -> RustvelloResult<Option<String>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_app_info".into(),
            })
        }
        async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_all_app_infos".into(),
            })
        }
        async fn store_workflow_sub_invocation(
            &self,
            _workflow_id: &InvocationId,
            _sub_inv_id: &InvocationId,
        ) -> RustvelloResult<()> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "store_workflow_sub_invocation".into(),
            })
        }
        async fn get_workflow_sub_invocations(
            &self,
            _workflow_id: &InvocationId,
        ) -> RustvelloResult<Vec<InvocationId>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_workflow_sub_invocations".into(),
            })
        }
    }

    #[async_trait::async_trait]
    impl StateBackendRunner for TestStateBackend {
        async fn store_runner_context(
            &self,
            _context: &crate::state_backend::StoredRunnerContext,
        ) -> RustvelloResult<()> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "store_runner_context".into(),
            })
        }
        async fn get_runner_context(
            &self,
            _runner_id: &str,
        ) -> RustvelloResult<Option<crate::state_backend::StoredRunnerContext>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_runner_context".into(),
            })
        }
        async fn get_runner_contexts_by_parent(
            &self,
            _parent_runner_id: &str,
        ) -> RustvelloResult<Vec<crate::state_backend::StoredRunnerContext>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_runner_contexts_by_parent".into(),
            })
        }
        async fn get_invocation_ids_by_runner(
            &self,
            _runner_id: &str,
            _limit: usize,
            _offset: usize,
        ) -> RustvelloResult<Vec<InvocationId>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_invocation_ids_by_runner".into(),
            })
        }
        async fn count_invocations_by_runner(&self, _runner_id: &str) -> RustvelloResult<usize> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "count_invocations_by_runner".into(),
            })
        }
        async fn get_history_in_timerange(
            &self,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
            _limit: usize,
            _offset: usize,
        ) -> RustvelloResult<Vec<rustvello_proto::invocation::InvocationHistory>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_history_in_timerange".into(),
            })
        }
        async fn get_matching_runner_contexts(
            &self,
            _partial_id: &str,
        ) -> RustvelloResult<Vec<crate::state_backend::StoredRunnerContext>> {
            Err(crate::error::RustvelloError::NotSupported {
                backend: "TestStateBackend".into(),
                method: "get_matching_runner_contexts".into(),
            })
        }
    }

    fn make_executor() -> (DeterministicExecutor, InvocationId) {
        let wf_id = InvocationId::from_string("test-workflow-001".to_string());
        let backend = Arc::new(TestStateBackend::new());
        let executor = DeterministicExecutor::new(wf_id.clone(), backend);
        (executor, wf_id)
    }

    // --- Sequence tracking tests ---

    #[test]
    fn sequence_increments_correctly() {
        let (mut executor, _) = make_executor();
        assert_eq!(executor.get_next_sequence("test_op"), 1);
        assert_eq!(executor.get_next_sequence("test_op"), 2);
        assert_eq!(executor.get_next_sequence("other_op"), 1);
        assert_eq!(executor.get_next_sequence("test_op"), 3);
    }

    #[test]
    fn operation_count_retrieval() {
        let (mut executor, _) = make_executor();
        assert_eq!(executor.get_operation_count("test_op"), 0);
        executor.get_next_sequence("test_op");
        executor.get_next_sequence("test_op");
        assert_eq!(executor.get_operation_count("test_op"), 2);
    }

    #[test]
    fn operation_count_per_instance() {
        let (mut exec1, wf_id) = make_executor();
        exec1.get_next_sequence("test");
        exec1.get_next_sequence("test");
        assert_eq!(exec1.get_operation_count("test"), 2);

        let exec2 = DeterministicExecutor::new(wf_id, Arc::new(TestStateBackend::new()));
        assert_eq!(exec2.get_operation_count("test"), 0);
    }

    #[test]
    fn operation_count_isolated_by_type() {
        let (mut executor, _) = make_executor();
        executor.get_next_sequence("random");
        executor.get_next_sequence("random");
        executor.get_next_sequence("time");
        assert_eq!(executor.get_operation_count("random"), 2);
        assert_eq!(executor.get_operation_count("time"), 1);
        assert_eq!(executor.get_operation_count("uuid"), 0);
    }

    // --- Deterministic operation tests ---

    #[tokio::test]
    async fn stores_and_retrieves_values() {
        let (mut executor, wf_id) = make_executor();
        let backend = executor.state_backend.clone();

        let result = executor
            .deterministic_operation("test", || "generated_value_1".to_string())
            .await
            .unwrap();
        assert_eq!(result, "generated_value_1");

        let stored = backend.get_workflow_data(&wf_id, "test:1").await.unwrap();
        assert_eq!(stored, Some("generated_value_1".to_string()));
    }

    #[tokio::test]
    async fn creates_unique_sequences() {
        let (mut executor, wf_id) = make_executor();
        let backend = executor.state_backend.clone();
        let mut counter = 0u32;

        let r1 = executor
            .deterministic_operation("test", || {
                counter += 1;
                format!("value_{counter}")
            })
            .await
            .unwrap();
        let r2 = executor
            .deterministic_operation("test", || {
                counter += 1;
                format!("value_{counter}")
            })
            .await
            .unwrap();
        let r3 = executor
            .deterministic_operation("test", || {
                counter += 1;
                format!("value_{counter}")
            })
            .await
            .unwrap();

        assert_eq!(r1, "value_1");
        assert_eq!(r2, "value_2");
        assert_eq!(r3, "value_3");

        assert_eq!(
            backend.get_workflow_data(&wf_id, "test:1").await.unwrap(),
            Some("value_1".to_string())
        );
        assert_eq!(
            backend.get_workflow_data(&wf_id, "test:2").await.unwrap(),
            Some("value_2".to_string())
        );
        assert_eq!(
            backend.get_workflow_data(&wf_id, "test:3").await.unwrap(),
            Some("value_3".to_string())
        );
    }

    #[tokio::test]
    async fn replays_stored_values() {
        let wf_id = InvocationId::from_string("test-wf-replay".to_string());
        let backend = Arc::new(TestStateBackend::new());

        // First executor: generate values
        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let r1 = exec1
            .deterministic_operation("test", || "fresh_1".to_string())
            .await
            .unwrap();
        let r2 = exec1
            .deterministic_operation("test", || "fresh_2".to_string())
            .await
            .unwrap();
        assert_eq!(r1, "fresh_1");
        assert_eq!(r2, "fresh_2");

        // Second executor: replay without calling generators
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        let mut gen_called = false;
        let replay1 = exec2
            .deterministic_operation("test", || {
                gen_called = true;
                "should_not_appear".to_string()
            })
            .await
            .unwrap();
        assert_eq!(replay1, "fresh_1");
        assert!(!gen_called, "Generator should not be called during replay");

        let replay2 = exec2
            .deterministic_operation("test", || {
                gen_called = true;
                "should_not_appear".to_string()
            })
            .await
            .unwrap();
        assert_eq!(replay2, "fresh_2");
        assert!(!gen_called);
    }

    #[tokio::test]
    async fn handles_partial_replay_then_generation() {
        let wf_id = InvocationId::from_string("test-wf-partial".to_string());
        let backend = Arc::new(TestStateBackend::new());

        // First executor: generate 2 values
        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let r1 = exec1
            .deterministic_operation("test", || "value_1".to_string())
            .await
            .unwrap();
        let r2 = exec1
            .deterministic_operation("test", || "value_2".to_string())
            .await
            .unwrap();

        // Second executor: replay 2, then generate 1 new
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        let replay1 = exec2
            .deterministic_operation("test", || "new_1".to_string())
            .await
            .unwrap();
        let replay2 = exec2
            .deterministic_operation("test", || "new_2".to_string())
            .await
            .unwrap();
        let new_val = exec2
            .deterministic_operation("test", || "value_3".to_string())
            .await
            .unwrap();

        assert_eq!(replay1, r1);
        assert_eq!(replay2, r2);
        assert_eq!(new_val, "value_3");
    }

    #[tokio::test]
    async fn isolated_by_operation_type() {
        let (mut executor, wf_id) = make_executor();
        let backend = executor.state_backend.clone();

        executor
            .deterministic_operation("type_a", || "a_value".to_string())
            .await
            .unwrap();
        executor
            .deterministic_operation("type_b", || "b_value".to_string())
            .await
            .unwrap();
        executor
            .deterministic_operation("type_a", || "a_value_2".to_string())
            .await
            .unwrap();

        assert_eq!(
            backend.get_workflow_data(&wf_id, "type_a:1").await.unwrap(),
            Some("a_value".to_string())
        );
        assert_eq!(
            backend.get_workflow_data(&wf_id, "type_b:1").await.unwrap(),
            Some("b_value".to_string())
        );
        assert_eq!(
            backend.get_workflow_data(&wf_id, "type_a:2").await.unwrap(),
            Some("a_value_2".to_string())
        );
    }

    // --- Built-in function tests ---

    #[tokio::test]
    async fn base_time_establishment() {
        let (executor, _) = make_executor();
        let base1 = executor.get_base_time().await.unwrap();
        assert!(base1.timezone() == Utc);
        let base2 = executor.get_base_time().await.unwrap();
        assert_eq!(base1, base2);
    }

    #[tokio::test]
    async fn deterministic_random_generation() {
        let wf_id = InvocationId::from_string("test-wf-random".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let randoms: Vec<f64> = {
            let mut v = Vec::new();
            for _ in 0..5 {
                v.push(exec1.random().await.unwrap());
            }
            v
        };

        // All in range [0, 1)
        assert!(randoms.iter().all(|&r| (0.0..=1.0).contains(&r)));
        // All unique
        let unique: std::collections::HashSet<u64> = randoms.iter().map(|r| r.to_bits()).collect();
        assert_eq!(unique.len(), 5);

        // Replay produces same values
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        for (i, &original) in randoms.iter().enumerate() {
            let replayed = exec2.random().await.unwrap();
            assert_eq!(original, replayed, "random mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn deterministic_time_progression() {
        let wf_id = InvocationId::from_string("test-wf-time".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let times: Vec<DateTime<Utc>> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(exec1.utc_now().await.unwrap());
            }
            v
        };

        // All UTC
        assert!(times.iter().all(|t| t.timezone() == Utc));
        // Strictly ascending
        assert!(times.windows(2).all(|w| w[0] < w[1]));
        // At or after base time
        let base = exec1.get_base_time().await.unwrap();
        assert!(times[0] >= base);

        // Replay produces same values
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        for (i, &original) in times.iter().enumerate() {
            let replayed = exec2.utc_now().await.unwrap();
            assert_eq!(original, replayed, "time mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn deterministic_uuid_generation() {
        let wf_id = InvocationId::from_string("test-wf-uuid".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let uuids: Vec<String> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(exec1.uuid().await.unwrap());
            }
            v
        };

        // Valid UUID format
        assert!(uuids
            .iter()
            .all(|u| u.len() == 36 && u.chars().filter(|&c| c == '-').count() == 4));
        // All unique
        let unique: std::collections::HashSet<&String> = uuids.iter().collect();
        assert_eq!(unique.len(), 3);

        // Replay produces same values
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        for (i, original) in uuids.iter().enumerate() {
            let replayed = exec2.uuid().await.unwrap();
            assert_eq!(original, &replayed, "uuid mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn mixed_deterministic_operations_sequence() {
        let wf_id = InvocationId::from_string("test-wf-mixed".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let random1 = exec1.random().await.unwrap();
        let time1 = exec1.utc_now().await.unwrap();
        let uuid1 = exec1.uuid().await.unwrap();
        let random2 = exec1.random().await.unwrap();
        let time2 = exec1.utc_now().await.unwrap();

        assert_ne!(random1, random2);
        assert_ne!(time1, time2);

        // Replay
        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        assert_eq!(random1, exec2.random().await.unwrap());
        assert_eq!(time1, exec2.utc_now().await.unwrap());
        assert_eq!(uuid1, exec2.uuid().await.unwrap());
        assert_eq!(random2, exec2.random().await.unwrap());
        assert_eq!(time2, exec2.utc_now().await.unwrap());
    }

    // --- Replay behavior tests ---

    #[tokio::test]
    async fn complete_workflow_replay() {
        let wf_id = InvocationId::from_string("test-wf-complete".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        let original_random = exec1.random().await.unwrap();
        let original_time = exec1.utc_now().await.unwrap();
        let original_uuid = exec1.uuid().await.unwrap();
        let original_custom = exec1
            .deterministic_operation("custom", || "custom_1".to_string())
            .await
            .unwrap();

        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        assert_eq!(original_random, exec2.random().await.unwrap());
        assert_eq!(original_time, exec2.utc_now().await.unwrap());
        assert_eq!(original_uuid, exec2.uuid().await.unwrap());
        let replay_custom = exec2
            .deterministic_operation("custom", || "should_not_appear".to_string())
            .await
            .unwrap();
        assert_eq!(original_custom, replay_custom);
    }

    #[tokio::test]
    async fn counter_consistency_across_replay() {
        let wf_id = InvocationId::from_string("test-wf-counter".to_string());
        let backend = Arc::new(TestStateBackend::new());

        let mut exec1 = DeterministicExecutor::new(wf_id.clone(), backend.clone());
        for _ in 0..3 {
            exec1
                .deterministic_operation("test", || "val".to_string())
                .await
                .unwrap();
        }
        assert_eq!(exec1.get_operation_count("test"), 3);

        let mut exec2 = DeterministicExecutor::new(wf_id, backend);
        for _ in 0..3 {
            exec2
                .deterministic_operation("test", || "val".to_string())
                .await
                .unwrap();
        }
        assert_eq!(exec2.get_operation_count("test"), 3);

        exec2
            .deterministic_operation("test", || "val_4".to_string())
            .await
            .unwrap();
        assert_eq!(exec2.get_operation_count("test"), 4);
    }

    // --- Workflow isolation tests ---

    #[tokio::test]
    async fn workflow_isolation() {
        let backend = Arc::new(TestStateBackend::new());
        let wf1_id = InvocationId::from_string("workflow-1".to_string());
        let wf2_id = InvocationId::from_string("workflow-2".to_string());

        let mut exec1 = DeterministicExecutor::new(wf1_id.clone(), backend.clone());
        let mut exec2 = DeterministicExecutor::new(wf2_id, backend.clone());

        let randoms1: Vec<f64> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(exec1.random().await.unwrap());
            }
            v
        };
        let randoms2: Vec<f64> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(exec2.random().await.unwrap());
            }
            v
        };

        // Different workflows produce different values
        assert_ne!(randoms1, randoms2);

        // Same workflow replays same values
        let mut exec1_replay = DeterministicExecutor::new(wf1_id, backend);
        let replayed: Vec<f64> = {
            let mut v = Vec::new();
            for _ in 0..3 {
                v.push(exec1_replay.random().await.unwrap());
            }
            v
        };
        assert_eq!(randoms1, replayed);
    }

    #[tokio::test]
    async fn state_backend_basic_operations() {
        let wf_id = InvocationId::from_string("test-wf-basic".to_string());
        let backend = Arc::new(TestStateBackend::new());

        backend
            .set_workflow_data(&wf_id, "test_key", "test_value")
            .await
            .unwrap();

        let retrieved = backend.get_workflow_data(&wf_id, "test_key").await.unwrap();
        assert_eq!(retrieved, Some("test_value".to_string()));

        let non_existent = backend
            .get_workflow_data(&wf_id, "non_existent")
            .await
            .unwrap();
        assert_eq!(non_existent, None);
    }
}
