//! PostgreSQL-backed [`Broker`] implementation.

use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};

use crate::db::{pg_err, Database};

/// PostgreSQL-backed broker with atomic priority dequeue via `SKIP LOCKED`.
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
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let client = self.db.conn().await?;
        let task_id = task_id.map(ToString::to_string);
        client
            .execute(
                "INSERT INTO broker_queue (invocation_id, task_id, queue_name, priority) \
                 VALUES ($1, $2, $3, $4)",
                &[&invocation_id.as_str(), &task_id, &queue_name, &priority],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, None, DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, Some(task_id), DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn retrieve_invocation_from_queue(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        let client = self.db.conn().await?;
        let row = match task_id {
            Some(task_id) => {
                let task_id = task_id.to_string();
                client
                    .query_opt(
                        "DELETE FROM broker_queue WHERE id = (\
                           SELECT id FROM broker_queue \
                           WHERE queue_name = $1 AND task_id = $2 \
                           ORDER BY priority DESC, id ASC LIMIT 1 \
                           FOR UPDATE SKIP LOCKED\
                         ) RETURNING invocation_id",
                        &[&queue_name, &task_id],
                    )
                    .await
                    .map_err(pg_err)?
            }
            None => client
                .query_opt(
                    "DELETE FROM broker_queue WHERE id = (\
                       SELECT id FROM broker_queue WHERE queue_name = $1 \
                       ORDER BY priority DESC, id ASC LIMIT 1 \
                       FOR UPDATE SKIP LOCKED\
                     ) RETURNING invocation_id",
                    &[&queue_name],
                )
                .await
                .map_err(pg_err)?,
        };
        Ok(row.map(|row| InvocationId::from_string(row.get::<_, String>(0))))
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_from_queue(DEFAULT_QUEUE, task_id)
            .await
    }

    async fn retrieve_invocation_for_language_from_queue(
        &self,
        language: TaskLanguage,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        let client = self.db.conn().await?;
        let language = language.to_string();
        let prefix = format!("{language}::%");
        let row = client
            .query_opt(
                "DELETE FROM broker_queue WHERE id = (\
                   SELECT id FROM broker_queue \
                   WHERE queue_name = $1 \
                     AND ((task_id IS NULL AND $2 = 'rust') OR task_id LIKE $3) \
                   ORDER BY priority DESC, id ASC LIMIT 1 \
                   FOR UPDATE SKIP LOCKED\
                 ) RETURNING invocation_id",
                &[&queue_name, &language, &prefix],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map(|row| InvocationId::from_string(row.get::<_, String>(0))))
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: TaskLanguage,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_for_language_from_queue(language, DEFAULT_QUEUE)
            .await
    }

    async fn count_invocations_in_queues(
        &self,
        queue_names: &[String],
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize> {
        for queue_name in queue_names {
            validate_routing(queue_name, 0.0)?;
        }
        let client = self.db.conn().await?;
        let task_id = task_id.map(ToString::to_string);
        let mut count = 0i64;
        if queue_names.is_empty() {
            let row = match task_id {
                Some(task_id) => client
                    .query_one(
                        "SELECT COUNT(*) FROM broker_queue WHERE task_id = $1",
                        &[&task_id],
                    )
                    .await
                    .map_err(pg_err)?,
                None => client
                    .query_one("SELECT COUNT(*) FROM broker_queue", &[])
                    .await
                    .map_err(pg_err)?,
            };
            count = row.get(0);
        } else {
            for queue_name in queue_names {
                let row = match &task_id {
                    Some(task_id) => client
                        .query_one(
                            "SELECT COUNT(*) FROM broker_queue \
                             WHERE queue_name = $1 AND task_id = $2",
                            &[queue_name, task_id],
                        )
                        .await
                        .map_err(pg_err)?,
                    None => client
                        .query_one(
                            "SELECT COUNT(*) FROM broker_queue WHERE queue_name = $1",
                            &[queue_name],
                        )
                        .await
                        .map_err(pg_err)?,
                };
                count += row.get::<_, i64>(0);
            }
        }
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        match task_id {
            Some(task_id) => {
                client
                    .execute(
                        "DELETE FROM broker_queue WHERE task_id = $1",
                        &[&task_id.to_string()],
                    )
                    .await
                    .map_err(pg_err)?;
            }
            None => {
                client
                    .execute("DELETE FROM broker_queue", &[])
                    .await
                    .map_err(pg_err)?;
            }
        }
        Ok(())
    }
}
