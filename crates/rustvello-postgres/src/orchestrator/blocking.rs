use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorBlocking;
use rustvello_proto::identifiers::InvocationId;

use super::PostgresOrchestrator;
use crate::db::pg_err;

#[async_trait]
impl OrchestratorBlocking for PostgresOrchestrator {
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO waiting_for (waiter_id, waited_on_id) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
                &[&waiter.as_str(), &waited_on.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT waiter_id FROM waiting_for WHERE waited_on_id = $1",
                &[&waited_on.as_str()],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn release_waiters(
        &self,
        completed: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;

        // Atomically select and delete using a CTE.
        let rows = client
            .query(
                "DELETE FROM waiting_for WHERE waited_on_id = $1 RETURNING waiter_id",
                &[&completed.as_str()],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }
}
