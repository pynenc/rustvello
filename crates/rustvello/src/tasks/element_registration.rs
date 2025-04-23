// crates/rustvello/src/tasks/element_registration.rs

use std::sync::Arc;

use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::timing::element_registration_interval;
use rustvello_core::{ElementBuffer, MuseResult, State, Transport};
use rustvello_proto::*;

pub async fn start_element_registration_task(
    cancellation_token: CancellationToken,
    client: Arc<dyn Transport + Send + Sync>,
    state: Arc<State>,
    element_buffer: Arc<ElementBuffer>,
) {
    loop {
        let interval = element_registration_interval(state.get_finest_resolution());
        select! {
            _ = cancellation_token.cancelled() => {
                println!("Element registration task was cancelled.");
                break;
            }
            _ = sleep(interval) => {
                if let Err(e) = process_pending_elements(
                    &client,
                    &state,
                    &element_buffer,
                ).await {
                    eprintln!("Error during element registration: {:?}", e);
                }
            }
        }
    }
}

async fn process_pending_elements(
    client: &Arc<dyn Transport + Send + Sync>,
    state: &Arc<State>,
    buffer: &Arc<ElementBuffer>,
) -> MuseResult<()> {
    // Get pending elements from the buffer
    let pending_entries = buffer.get_pending_elements().await;
    if pending_entries.is_empty() {
        return Ok(());
    }
    // Prepare the elements for registration
    let elements_to_register: Vec<ElementRegistration> = pending_entries
        .iter()
        .map(|entry| entry.registration.clone())
        .collect();

    // Attempt to register elements
    let results = client.register_elements(&elements_to_register).await?;

    // Process the results
    for (entry, result) in pending_entries.into_iter().zip(results.into_iter()) {
        match result {
            Ok(element_id) => {
                state.update_min_max_element_id(element_id).await;
                state.update_element_id(entry.id, element_id).await;
                buffer.mark_succeeded(&entry.id).await;
            }
            Err(e) => {
                log::error!("Element {:?} registration error: {:?}", entry.id, e);
                buffer.mark_failed(entry).await;
            }
        }
    }

    Ok(())
}
