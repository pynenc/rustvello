use std::collections::HashMap;

use rustvello_proto::*;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct BufferEntry {
    pub id: LocalElementId,
    pub registration: ElementRegistration,
}

impl BufferEntry {
    pub fn new(id: LocalElementId, registration: ElementRegistration) -> Self {
        Self { id, registration }
    }
}

// Buffer to manage element registration attempts
pub struct ElementBuffer {
    pending: Mutex<Vec<BufferEntry>>,
    retry_counts: Mutex<HashMap<LocalElementId, usize>>,
    max_retries: usize,
}

impl ElementBuffer {
    pub fn new(max_retries: usize) -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            retry_counts: Mutex::new(HashMap::new()),
            max_retries,
        }
    }

    /// Adds an element to the pending queue.
    pub async fn add_element(
        &self,
        element_id: LocalElementId,
        element_registration: ElementRegistration,
    ) {
        let mut pending = self.pending.lock().await;
        pending.push(BufferEntry::new(element_id, element_registration));
    }

    /// Retrieves and removes all pending elements.
    pub async fn get_pending_elements(&self) -> Vec<BufferEntry> {
        let mut pending = self.pending.lock().await;
        pending.drain(..).collect()
    }

    /// Marks an element as failed and handles retries.
    /// Returns `true` if the element will be retried, `false` otherwise.
    pub async fn mark_failed(&self, element: BufferEntry) {
        let mut retry_counts = self.retry_counts.lock().await;
        let count = retry_counts.entry(element.id).or_insert(0);
        *count += 1;

        if *count >= self.max_retries {
            retry_counts.remove(&element.id);
            log::warn!(
                "Element {:?} won't be retried after {} failed attempts",
                element.id,
                self.max_retries
            );
        } else {
            let mut pending = self.pending.lock().await;
            pending.push(element);
        }
    }

    /// Marks an element as successfully registered.
    pub async fn mark_succeeded(&self, element_id: &LocalElementId) {
        let mut retry_counts = self.retry_counts.lock().await;
        retry_counts.remove(element_id);
    }
}
