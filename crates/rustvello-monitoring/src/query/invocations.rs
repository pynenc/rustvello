//! Bounded invocation row enrichment shared by monitoring routes.

use futures_util::{stream, StreamExt};
use rustvello_proto::identifiers::InvocationId;
use rustvello_proto::status::InvocationStatus;

use crate::navigation::{MonitoringDestination, MonitoringLink, MonitoringScope, TimeWindow};
use crate::util::{formatting, status_colors};
use crate::view::InvocationRowView;
use crate::AppInstance;

pub async fn load_invocation_rows(
    app: &AppInstance,
    invocation_ids: Vec<InvocationId>,
    base_scope: MonitoringScope,
) -> Vec<InvocationRowView> {
    let backend = &app.state_backend;
    let orchestrator = &app.orchestrator;
    stream::iter(invocation_ids.into_iter().map(|invocation_id| {
        let base_scope = base_scope.clone();
        async move {
            let (invocation, history) = tokio::join!(
                backend.get_invocation(&invocation_id),
                backend.get_history(&invocation_id)
            );
            let invocation = invocation.ok()?;
            let history = history.unwrap_or_default();
            let status = if let Some(last) = history.last() {
                last.status_record.status
            } else {
                orchestrator
                    .get_invocation_status(&invocation_id)
                    .await
                    .map(|record| record.status)
                    .unwrap_or(InvocationStatus::Registered)
            };
            let full_id = invocation_id.to_string();
            let mut scope = base_scope.with_invocation(full_id.clone());
            let extent = history
                .iter()
                .map(|entry| {
                    entry
                        .history_timestamp
                        .unwrap_or(entry.status_record.timestamp)
                })
                .min()
                .zip(
                    history
                        .iter()
                        .map(|entry| {
                            entry
                                .history_timestamp
                                .unwrap_or(entry.status_record.timestamp)
                        })
                        .max(),
                );
            if let Some((start, end)) = extent {
                scope = scope.with_time(TimeWindow::fit_default(start, end));
            }
            Some(InvocationRowView {
                short_id: formatting::truncate_id(&full_id),
                timeline_url: MonitoringLink::new(MonitoringDestination::Timeline)
                    .with_scope(scope)
                    .with_selected_invocation(full_id.clone())
                    .href(),
                detail_url: MonitoringLink::new(MonitoringDestination::InvocationDetail(
                    full_id.clone(),
                ))
                .href(),
                invocation_id: full_id,
                task_id: invocation.task_id.to_string(),
                call_id: invocation.call_id.to_string(),
                status: format!("{status:?}"),
                status_class: status_colors::badge_class(&status).to_owned(),
                num_retries: history
                    .iter()
                    .filter(|entry| entry.status_record.status == InvocationStatus::Retry)
                    .count(),
                is_workflow_defining: invocation.is_workflow_defining(),
            })
        }
    }))
    .buffered(32)
    .filter_map(|row| async move { row })
    .collect()
    .await
}
