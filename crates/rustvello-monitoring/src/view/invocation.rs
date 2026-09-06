//! Canonical invocation row used by every invocation list context.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationRowView {
    pub invocation_id: String,
    pub short_id: String,
    pub task_id: String,
    pub call_id: String,
    pub status: String,
    pub status_class: String,
    pub num_retries: usize,
    pub is_workflow_defining: bool,
    pub timeline_url: String,
    pub detail_url: String,
}
