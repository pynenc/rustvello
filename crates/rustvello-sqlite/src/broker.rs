use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::db::{blocking, lock_err, sql_err, Database};

/// SQLite-backed broker with atomic named-queue priority retrieval.
pub struct SqliteBroker {
    db: Arc<Database>,
}

impl SqliteBroker {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl Broker for SqliteBroker {
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let task_id = task_id.map(ToString::to_string);
        let queue_name = queue_name.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT INTO broker_queue (invocation_id, task_id, queue_name, priority) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![invocation_id.as_str(), task_id, queue_name, priority],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
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
        let db = Arc::clone(&self.db);
        let queue_name = queue_name.to_owned();
        let task_id = task_id.map(ToString::to_string);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let row: Option<(i64, String)> = match task_id {
                Some(task_id) => tx
                    .query_row(
                        "SELECT id, invocation_id FROM broker_queue \
                         WHERE queue_name = ?1 AND task_id = ?2 \
                         ORDER BY priority DESC, id ASC LIMIT 1",
                        rusqlite::params![queue_name, task_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .ok(),
                None => tx
                    .query_row(
                        "SELECT id, invocation_id FROM broker_queue \
                         WHERE queue_name = ?1 \
                         ORDER BY priority DESC, id ASC LIMIT 1",
                        [queue_name],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .ok(),
            };
            if let Some((row_id, invocation_id)) = row {
                tx.execute("DELETE FROM broker_queue WHERE id = ?1", [row_id])
                    .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                Ok(Some(InvocationId::from_string(invocation_id)))
            } else {
                Ok(None)
            }
        })
        .await
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
        language: &str,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        let db = Arc::clone(&self.db);
        let language = language.to_owned();
        let queue_name = queue_name.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let row: Option<(i64, String)> = if language.is_empty() {
                tx.query_row(
                    "SELECT id, invocation_id FROM broker_queue \
                     WHERE queue_name = ?1 AND (task_id IS NULL OR task_id NOT LIKE '%::%') \
                     ORDER BY priority DESC, id ASC LIMIT 1",
                    [queue_name],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            } else {
                let prefix = format!("{language}::%");
                tx.query_row(
                    "SELECT id, invocation_id FROM broker_queue \
                     WHERE queue_name = ?1 AND (task_id IS NULL OR task_id LIKE ?2) \
                     ORDER BY priority DESC, id ASC LIMIT 1",
                    rusqlite::params![queue_name, prefix],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            };
            if let Some((row_id, invocation_id)) = row {
                tx.execute("DELETE FROM broker_queue WHERE id = ?1", [row_id])
                    .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                Ok(Some(InvocationId::from_string(invocation_id)))
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
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
        let db = Arc::clone(&self.db);
        let queue_names = queue_names.to_vec();
        let task_id = task_id.map(ToString::to_string);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let count: i64 = match (queue_names.is_empty(), task_id) {
                (true, None) => conn
                    .query_row("SELECT COUNT(*) FROM broker_queue", [], |row| row.get(0))
                    .map_err(sql_err)?,
                (true, Some(task_id)) => conn
                    .query_row(
                        "SELECT COUNT(*) FROM broker_queue WHERE task_id = ?1",
                        [task_id],
                        |row| row.get(0),
                    )
                    .map_err(sql_err)?,
                (false, task_id) => {
                    let mut count = 0i64;
                    for queue_name in queue_names {
                        count += match &task_id {
                            Some(task_id) => conn
                                .query_row(
                                    "SELECT COUNT(*) FROM broker_queue \
                                     WHERE queue_name = ?1 AND task_id = ?2",
                                    rusqlite::params![queue_name, task_id],
                                    |row| row.get::<_, i64>(0),
                                )
                                .map_err(sql_err)?,
                            None => conn
                                .query_row(
                                    "SELECT COUNT(*) FROM broker_queue WHERE queue_name = ?1",
                                    [queue_name],
                                    |row| row.get::<_, i64>(0),
                                )
                                .map_err(sql_err)?,
                        };
                    }
                    count
                }
            };
            Ok(count as usize)
        })
        .await
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.map(ToString::to_string);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            match task_id {
                Some(task_id) => {
                    conn.execute("DELETE FROM broker_queue WHERE task_id = ?1", [task_id])
                        .map_err(sql_err)?;
                }
                None => {
                    conn.execute("DELETE FROM broker_queue", [])
                        .map_err(sql_err)?;
                }
            }
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_broker() -> SqliteBroker {
        let db = Arc::new(Database::in_memory().unwrap());
        SqliteBroker::new(db)
    }

    #[tokio::test]
    async fn test_route_and_retrieve() {
        let broker = make_broker();
        let id1 = InvocationId::new();
        let id2 = InvocationId::new();
        broker.route_invocation(&id1).await.unwrap();
        broker.route_invocation(&id2).await.unwrap();
        assert_eq!(broker.retrieve_invocation(None).await.unwrap(), Some(id1));
        assert_eq!(broker.retrieve_invocation(None).await.unwrap(), Some(id2));
    }

    #[tokio::test]
    async fn test_purge() {
        let broker = make_broker();
        broker.route_invocation(&InvocationId::new()).await.unwrap();
        broker.purge(None).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
    }
}
