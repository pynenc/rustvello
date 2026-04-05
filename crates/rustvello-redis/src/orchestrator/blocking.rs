use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorBlocking;
use rustvello_proto::identifiers::InvocationId;

use super::{prefixed_key, RedisOrchestrator};
use crate::connection::redis_err;

#[async_trait]
impl OrchestratorBlocking for RedisOrchestrator {
    async fn set_waiting_for(
        &self,
        waiter: &InvocationId,
        waited_on: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.sadd::<_, _, ()>(
            prefixed_key(&self.waiters_prefix, waited_on.as_str()),
            waiter.as_str(),
        )
        .await
        .map_err(redis_err)
    }

    async fn get_waiters(&self, waited_on: &InvocationId) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.waiters_prefix, waited_on.as_str()))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn release_waiters(
        &self,
        completed: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let waiters = self.get_waiters(completed).await?;
        if !waiters.is_empty() {
            let mut conn = self.pool.conn().await?;
            conn.del::<_, ()>(prefixed_key(&self.waiters_prefix, completed.as_str()))
                .await
                .map_err(redis_err)?;
        }
        Ok(waiters)
    }
}
