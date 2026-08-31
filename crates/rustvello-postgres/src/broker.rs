//! PostgreSQL-backed [`Broker`] implementation.

use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::db::{pg_err, Database};

/// PostgreSQL-backed broker implementation.
///
/// Persists the queue to a PostgreSQL database, suitable for multi-node deployments.
pub struct PostgresBroker {
    db: Arc<Database>,
}

impl PostgresBroker {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl Broker for PostgresBroker {
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO broker_queue (invocation_id) VALUES ($1)",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO broker_queue (invocation_id, task_id) VALUES ($1, $2)",
                &[&invocation_id.as_str(), &task_id.to_string()],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let client = self.db.conn().await?;

        // Atomically select and delete using a CTE for crash safety.
        let row = if let Some(tid) = task_id {
            client
                .query_opt(
                    "DELETE FROM broker_queue
                     WHERE id = (
                         SELECT bq.id FROM broker_queue bq
                         WHERE bq.task_id = $1 OR (bq.task_id IS NULL AND EXISTS (
                             SELECT 1 FROM invocations inv
                             WHERE inv.invocation_id = bq.invocation_id AND inv.task_id = $1
                         ))
                         ORDER BY bq.id ASC LIMIT 1
                         FOR UPDATE OF bq SKIP LOCKED
                     )
                     RETURNING invocation_id",
                    &[&tid.to_string()],
                )
                .await
                .map_err(pg_err)?
        } else {
            client
                .query_opt(
                    "DELETE FROM broker_queue
                     WHERE id = (SELECT id FROM broker_queue ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED)
                     RETURNING invocation_id",
                    &[],
                )
                .await
                .map_err(pg_err)?
        };

        Ok(row.map(|r| InvocationId::from_string(r.get::<_, String>(0))))
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let client = self.db.conn().await?;
        let row = if let Some(tid) = task_id {
            client
                .query_one(
                    "SELECT COUNT(*) FROM broker_queue bq \
                     WHERE bq.task_id = $1 OR (bq.task_id IS NULL AND EXISTS (\
                         SELECT 1 FROM invocations inv \
                         WHERE inv.invocation_id = bq.invocation_id AND inv.task_id = $1\
                     ))",
                    &[&tid.to_string()],
                )
                .await
                .map_err(pg_err)?
        } else {
            client
                .query_one("SELECT COUNT(*) FROM broker_queue", &[])
                .await
                .map_err(pg_err)?
        };
        let count: i64 = row.get(0);
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        if let Some(tid) = task_id {
            client
                .execute(
                    "DELETE FROM broker_queue \
                     WHERE task_id = $1 OR (task_id IS NULL AND invocation_id IN (\
                         SELECT inv.invocation_id FROM invocations inv WHERE inv.task_id = $1\
                     ))",
                    &[&tid.to_string()],
                )
                .await
                .map_err(pg_err)?;
        } else {
            client
                .execute("DELETE FROM broker_queue", &[])
                .await
                .map_err(pg_err)?;
        }
        Ok(())
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let client = self.db.conn().await?;
        let prefix = format!("{language}::");
        let global = client
            .query_opt(
                "DELETE FROM broker_queue
                 WHERE id = (
                     SELECT id FROM broker_queue
                     WHERE task_id IS NULL
                     ORDER BY id ASC LIMIT 1
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING invocation_id",
                &[],
            )
            .await
            .map_err(pg_err)?;
        let row = match global {
            Some(row) => Some(row),
            None if language.is_empty() => client
                .query_opt(
                    "DELETE FROM broker_queue
                     WHERE id = (
                         SELECT id FROM broker_queue
                         WHERE task_id NOT LIKE '%::%'
                         ORDER BY id ASC LIMIT 1
                         FOR UPDATE SKIP LOCKED
                     )
                     RETURNING invocation_id",
                    &[],
                )
                .await
                .map_err(pg_err)?,
            None => client
                .query_opt(
                    "DELETE FROM broker_queue
                     WHERE id = (
                         SELECT id FROM broker_queue
                         WHERE task_id LIKE $1 || '%'
                         ORDER BY id ASC LIMIT 1
                         FOR UPDATE SKIP LOCKED
                     )
                     RETURNING invocation_id",
                    &[&prefix],
                )
                .await
                .map_err(pg_err)?,
        };
        Ok(row.map(|r| InvocationId::from_string(r.get::<_, String>(0))))
    }
}
