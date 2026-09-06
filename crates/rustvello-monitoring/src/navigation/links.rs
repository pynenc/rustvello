//! Typed links between monitoring views.

use super::MonitoringScope;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonitoringDestination {
    InvocationList,
    InvocationDetail(String),
    Timeline,
    WorkflowRun {
        workflow_type: String,
        workflow_id: String,
    },
    RunnerDetail(String),
    AtomicExecution {
        runner_id: String,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    },
}

#[derive(Debug, Clone)]
pub struct MonitoringLink {
    pub destination: MonitoringDestination,
    pub scope: MonitoringScope,
    pub selected_invocation: Option<String>,
    pub limit: Option<usize>,
}

impl MonitoringLink {
    pub fn new(destination: MonitoringDestination) -> Self {
        Self {
            destination,
            scope: MonitoringScope::default(),
            selected_invocation: None,
            limit: None,
        }
    }

    pub fn with_scope(mut self, scope: MonitoringScope) -> Self {
        self.scope = scope;
        self
    }

    pub fn with_selected_invocation(mut self, invocation_id: impl Into<String>) -> Self {
        self.selected_invocation = Some(invocation_id.into());
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn href(&self) -> String {
        match &self.destination {
            MonitoringDestination::InvocationList => {
                append_query("/invocations", self.scope_pairs(true, false))
            }
            MonitoringDestination::InvocationDetail(invocation_id) => {
                format!("/invocations/{invocation_id}")
            }
            MonitoringDestination::Timeline => {
                append_query("/invocations/timeline", self.scope_pairs(true, true))
            }
            MonitoringDestination::WorkflowRun {
                workflow_type,
                workflow_id,
            } => format!("/workflows/{workflow_type}/{workflow_id}"),
            MonitoringDestination::RunnerDetail(runner_id) => format!("/runners/{runner_id}"),
            MonitoringDestination::AtomicExecution {
                runner_id,
                start,
                end,
            } => append_query(
                "/atomic-service/execution",
                vec![
                    ("runner_id", runner_id.clone()),
                    ("start", start.to_rfc3339()),
                    ("end", end.to_rfc3339()),
                ],
            ),
        }
    }

    fn scope_pairs(
        &self,
        include_invocations: bool,
        include_selection: bool,
    ) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if include_selection {
            push_option(&mut pairs, "selected", self.selected_invocation.as_deref());
        }
        if include_invocations && !self.scope.invocation_ids.is_empty() {
            pairs.push(("inv_ids", self.scope.invocation_ids.join(",")));
        }
        if !self.scope.runner_ids.is_empty() {
            pairs.push(("runner_ids", self.scope.runner_ids.join(",")));
        }
        push_option(&mut pairs, "task_id", self.scope.task_id.as_deref());
        push_option(
            &mut pairs,
            "workflow_type",
            self.scope.workflow_type.as_deref(),
        );
        push_option(&mut pairs, "workflow_id", self.scope.workflow_id.as_deref());
        if !self.scope.statuses.is_empty() {
            pairs.push(("status", self.scope.statuses.join(",")));
        }
        push_option(&mut pairs, "status_mode", self.scope.status_mode.as_deref());
        if let Some(time) = self.scope.time {
            pairs.push(("time_range", "custom".to_owned()));
            pairs.push(("start_date", time.start.to_rfc3339()));
            pairs.push(("end_date", time.end.to_rfc3339()));
        }
        if let Some(limit) = self.limit {
            pairs.push(("limit", limit.to_string()));
        }
        pairs
    }
}

fn push_option(pairs: &mut Vec<(&'static str, String)>, key: &'static str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        pairs.push((key, value.to_owned()));
    }
}

fn append_query(path: &str, pairs: Vec<(&str, String)>) -> String {
    if pairs.is_empty() {
        return path.to_owned();
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        serializer.append_pair(key, &value);
    }
    format!("{path}?{}", serializer.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::navigation::{parse_datetime, TimeWindow};

    #[test]
    fn timeline_link_encodes_scope_and_rfc3339_offsets() {
        let start = parse_datetime("2026-09-04T19:40:35.412070+00:00").unwrap();
        let end = parse_datetime("2026-09-04T19:40:35.416259+00:00").unwrap();
        let scope = MonitoringScope::default()
            .with_invocation("abc")
            .with_workflow("rust::orders.run", "run-id")
            .with_time(TimeWindow::fit_default(start, end));
        let href = MonitoringLink::new(MonitoringDestination::Timeline)
            .with_scope(scope)
            .with_selected_invocation("abc")
            .href();
        assert!(href.contains("selected=abc"));
        assert!(href.contains("inv_ids=abc"));
        assert!(href.contains("workflow_type=rust%3A%3Aorders.run"));
        assert!(href.contains("start_date=2026-09-04T19%3A40%3A35"));
        assert!(href.contains("%2B00%3A00"));
    }

    #[test]
    fn invocation_list_preserves_workflow_scope() {
        let href = MonitoringLink::new(MonitoringDestination::InvocationList)
            .with_scope(MonitoringScope::default().with_workflow("python::flow", "run"))
            .with_limit(50)
            .href();
        assert_eq!(
            href,
            "/invocations?workflow_type=python%3A%3Aflow&workflow_id=run&limit=50"
        );
    }
}
