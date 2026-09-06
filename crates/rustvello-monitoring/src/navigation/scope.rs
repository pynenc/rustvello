//! Filters that can be carried between monitoring surfaces.

use super::TimeWindow;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MonitoringScope {
    pub task_id: Option<String>,
    pub workflow_type: Option<String>,
    pub workflow_id: Option<String>,
    pub invocation_ids: Vec<String>,
    pub runner_ids: Vec<String>,
    pub statuses: Vec<String>,
    pub status_mode: Option<String>,
    pub time: Option<TimeWindow>,
}

impl MonitoringScope {
    pub fn with_invocation(mut self, invocation_id: impl Into<String>) -> Self {
        self.invocation_ids = vec![invocation_id.into()];
        self
    }

    pub fn with_workflow(
        mut self,
        workflow_type: impl Into<String>,
        workflow_id: impl Into<String>,
    ) -> Self {
        self.workflow_type = Some(workflow_type.into());
        self.workflow_id = Some(workflow_id.into());
        self
    }

    pub fn with_time(mut self, time: TimeWindow) -> Self {
        self.time = Some(time);
        self
    }
}
