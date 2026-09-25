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
    fn publication_domain(&self) -> Option<rustvello_core::publication::PublicationDomain> {
        Some(Arc::clone(&self.db.domain))
    }
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await?;
        let task_id = task_id.map(ToString::to_string);
        crate::publication::publish(
            &tx,
            invocation_id.as_str(),
            task_id.as_deref(),
            &rustvello_core::publication::PublicationRoute {
                queue: queue_name.into(),
                priority,
            },
            self.db.options.max_queue_rows,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    fn supports_delayed_delivery(&self) -> bool {
        true
    }

    /// Inserts the entry and its not-before time (database clock) in one
    /// transaction; retrieval and counts skip it until it is due.
    async fn route_invocation_after(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
        delay: std::time::Duration,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await?;
        let task_id = task_id.map(ToString::to_string);
        crate::publication::publish(
            &tx,
            invocation_id.as_str(),
            task_id.as_deref(),
            &rustvello_core::publication::PublicationRoute {
                queue: queue_name.into(),
                priority,
            },
            self.db.options.max_queue_rows,
        )
        .await?;
        crate::publication::delay_delivery(&tx, invocation_id.as_str(), delay).await?;
        tx.commit().await?;
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
                        "UPDATE broker_queue SET reserved_until=clock_timestamp() + $3 * interval '1 millisecond' WHERE id = (\
                           SELECT id FROM broker_queue \
                           WHERE queue_name = $1 AND task_id = $2 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp()) \
                           ORDER BY priority DESC, id ASC LIMIT 1 \
                           FOR UPDATE SKIP LOCKED\
                         ) RETURNING invocation_id",
                        &[&queue_name, &task_id, &(self.db.options.delivery_lease_ms as f64)],
                    )
                    .await
                    .map_err(pg_err)?
            }
            None => client
                .query_opt(
                    "UPDATE broker_queue SET reserved_until=clock_timestamp() + $2 * interval '1 millisecond' WHERE id = (\
                       SELECT id FROM broker_queue WHERE queue_name = $1 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp()) \
                       ORDER BY priority DESC, id ASC LIMIT 1 \
                       FOR UPDATE SKIP LOCKED\
                     ) RETURNING invocation_id",
                    &[&queue_name, &(self.db.options.delivery_lease_ms as f64)],
                )
                .await
                .map_err(pg_err)?,
        };
        let id = row.map(|row| InvocationId::from_string(row.get::<_, String>(0)));
        if let Some(id) = &id {
            crate::failpoints::boundary("delivery.after_commit", id.as_str()).await?;
        }
        Ok(id)
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
                "UPDATE broker_queue SET reserved_until=clock_timestamp() + $4 * interval '1 millisecond' WHERE id = (\
                   SELECT id FROM broker_queue \
                   WHERE queue_name = $1 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp()) \
                     AND ((task_id IS NULL AND $2 = 'rust') OR task_id LIKE $3) \
                   ORDER BY priority DESC, id ASC LIMIT 1 \
                   FOR UPDATE SKIP LOCKED\
                 ) RETURNING invocation_id",
                &[&queue_name, &language, &prefix, &(self.db.options.delivery_lease_ms as f64)],
            )
            .await
            .map_err(pg_err)?;
        let id = row.map(|row| InvocationId::from_string(row.get::<_, String>(0)));
        if let Some(id) = &id {
            crate::failpoints::boundary("delivery.after_commit", id.as_str()).await?;
        }
        Ok(id)
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
                        "SELECT COUNT(*) FROM broker_queue WHERE task_id = $1 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp())",
                        &[&task_id],
                    )
                    .await
                    .map_err(pg_err)?,
                None => client
                    .query_one("SELECT COUNT(*) FROM broker_queue WHERE reserved_until IS NULL OR reserved_until <= clock_timestamp()", &[])
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
                             WHERE queue_name = $1 AND task_id = $2 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp())",
                            &[queue_name, task_id],
                        )
                        .await
                        .map_err(pg_err)?,
                    None => client
                        .query_one(
                            "SELECT COUNT(*) FROM broker_queue WHERE queue_name = $1 AND (reserved_until IS NULL OR reserved_until <= clock_timestamp())",
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
