// crates/rustvello/src/tasks/cluster_monitor.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::select;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use rustvello_core::{MuseResult, State, Transport};
use rustvello_proto::*;

pub async fn start_cluster_monitor(
    cancellation_token: CancellationToken,
    client: Arc<dyn Transport + Send + Sync>,
    state: Arc<State>,
    is_initialized: Arc<AtomicBool>,
    interval_duration: Duration,
) {
    let mut interval = interval(interval_duration);
    let mut old_max_element_id: Option<ElementId> = None;
    loop {
        select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Cluster monitor task was cancelled.");
                break;
            }
            _ = interval.tick() => {
                if is_initialized.load(Ordering::SeqCst) {
                    if let Err(e) = perform_cluster_monitoring(&client, &state, &mut old_max_element_id).await {
                        eprintln!("Error during cluster monitoring: {:?}", e);
                    }
                }
            }
        }
    }
}

async fn perform_cluster_monitoring(
    client: &Arc<dyn Transport + Send + Sync>,
    state: &Arc<State>,
    old_max_element_id: &mut Option<ElementId>,
) -> MuseResult<()> {
    // 1. Update nodes
    let node_state = client.get_node_state().await?;
    state.update_nodes(node_state.available_nodes.into()).await;

    // 2. Check and update finest_resolution
    let new_finest_resolution = client.get_finest_resolution().await?;
    state.update_finest_resolution(new_finest_resolution).await;

    // 3. Check min/max element IDs and update ranges
    if let (Some(min_element_id), Some(current_max_element_id)) = state.get_element_id_range().await
    {
        let should_request = match *old_max_element_id {
            None => true,
            Some(old_max) => current_max_element_id > old_max,
        };
        if should_request {
            let ini = match *old_max_element_id {
                Some(old_max) => old_max + 1,
                None => min_element_id,
            };
            let ranges = client
                .get_node_elem_ranges(Some(ini), Some(current_max_element_id))
                .await?;
            state.update_node_elem_ranges(&ranges).await;
            *old_max_element_id = Some(current_max_element_id);
        }
    }

    Ok(())
}
