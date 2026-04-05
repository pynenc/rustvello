//! Shared test helpers.

use rustvello_proto::identifiers::{InvocationId, TaskId};

/// Create a deterministic TaskId for tests.
pub fn test_task_id(name: &str) -> TaskId {
    TaskId::new("test_module", name)
}

/// Create a foreign TaskId for cross-language tests.
pub fn test_foreign_task_id(language: &str, name: &str) -> TaskId {
    TaskId::foreign(language, "test_module", name)
}

/// Generate a batch of InvocationIds.
pub fn generate_invocation_ids(count: usize) -> Vec<InvocationId> {
    (0..count).map(|_| InvocationId::new()).collect()
}
