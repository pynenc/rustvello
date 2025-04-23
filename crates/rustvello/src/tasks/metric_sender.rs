use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::timing::metric_sending_interval;
use rustvello_core::{time, MetricBuffer, MuseResult, State, Transport};
use rustvello_proto::MetricPayload;

pub async fn start_metric_sender_task(
    cancellation_token: CancellationToken,
    client: Arc<dyn Transport + Send + Sync>,
    state: Arc<State>,
    metric_buffer: Arc<MetricBuffer>,
) {
    loop {
        let interval = metric_sending_interval(state.get_finest_resolution());
        select! {
            _ = cancellation_token.cancelled() => {
                println!("Metric sender task was cancelled.");
                break;
            }
            _ = sleep(interval) => {
                if let Err(e) = send_metrics(
                    &client,
                    &state,
                    &metric_buffer,
                ).await {
                    eprintln!("Error during metric sending: {:?}", e);
                }
            }
        }
    }
}

async fn send_metrics(
    client: &Arc<dyn Transport + Send + Sync>,
    state: &Arc<State>,
    buffer: &Arc<MetricBuffer>,
) -> MuseResult<()> {
    let buffered_metrics = buffer.get_and_clear().await;
    if buffered_metrics.is_empty() {
        log::debug!("No metrics to send. Exiting.");
        return Ok(());
    }
    log::debug!("Processing metrics for {} elements", buffered_metrics.len());
    let metric_order = state.get_metric_order();
    let timestamp = time::utc_now_i64();
    let mut metrics_per_node: HashMap<Option<SocketAddr>, Vec<MetricPayload>> = HashMap::new();
    for (local_elem_id, metrics) in buffered_metrics {
        if let Some(element_id) = state.get_element_id(&local_elem_id) {
            let node_addr = state.find_element_node_addr(element_id);
            let metric_ids = metric_order.iter().map(|def| def.id).collect();
            let values = metric_order
                .iter()
                .map(|def| metrics.get(&def.code).cloned())
                .collect();

            let payload = MetricPayload {
                time: timestamp,
                element_id,
                metric_ids,
                values,
            };
            metrics_per_node.entry(node_addr).or_default().push(payload);
        } else {
            log::warn!(
                "Skipping metrics for not registered Element {:?}.",
                local_elem_id
            );
        }
    }
    for (node_addr, payloads) in metrics_per_node {
        log::debug!(
            "Sending {} metrics to node {:?}.",
            payloads.len(),
            node_addr
        );
        client.send_metrics(payloads, node_addr).await?;
    }

    Ok(())
}
