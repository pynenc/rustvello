use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorBlocking;
use rustvello_proto::identifiers::InvocationId;

use super::MemOrchestrator;

#[async_trait]
impl OrchestratorBlocking for MemOrchestrator {
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut state = self.state.lock().await;
        state
            .waiting_for
            .insert(Arc::from(waiter.as_str()), Arc::from(waited_on.as_str()));
        state
            .waiters
            .entry(Arc::from(waited_on.as_str()))
            .or_default()
            .insert(Arc::from(waiter.as_str()));
        Ok(())
    }

    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        let state = self.state.lock().await;
        Ok(state
            .waiters
            .get(waited_on.as_str())
            .map(|ids| {
                ids.iter()
                    .map(|id| InvocationId::from_string(Arc::clone(id)))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn release_waiters(
        &self,
        completed: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut state = self.state.lock().await;
        let waiter_ids = state.waiters.remove(completed.as_str()).unwrap_or_default();
        let released: Vec<InvocationId> = waiter_ids
            .iter()
            .map(|id| {
                state.waiting_for.remove(id);
                InvocationId::from_string(Arc::clone(id))
            })
            .collect();
        Ok(released)
    }
}
