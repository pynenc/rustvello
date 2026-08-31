use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::db::{blocking, lock_err, sql_err, Database};

/// SQLite-backed broker implementation.
///
/// Persists the queue to a SQLite database, surviving process restarts.
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
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT INTO broker_queue (invocation_id) VALUES (?1)",
                [id.as_str()],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let id = invocation_id.clone();
        let task_id = task_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT INTO broker_queue (invocation_id, task_id) VALUES (?1, ?2)",
                rusqlite::params![id.as_str(), task_id.to_string()],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;

            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            let result: Option<(i64, String)> = if let Some(ref tid) = task_id {
                tx.query_row(
                    "SELECT bq.id, bq.invocation_id FROM broker_queue bq \
                     LEFT JOIN invocations inv ON bq.invocation_id = inv.invocation_id \
                     WHERE (bq.task_id = ?1 OR (bq.task_id IS NULL AND inv.task_id = ?1)) \
                     ORDER BY bq.id ASC LIMIT 1",
                    [&tid.to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            } else {
                tx.query_row(
                    "SELECT id, invocation_id FROM broker_queue ORDER BY id ASC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            };

            if let Some((row_id, inv_id)) = result {
                tx.execute("DELETE FROM broker_queue WHERE id = ?1", [row_id])
                    .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                Ok(Some(InvocationId::from_string(inv_id)))
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let count: i64 = if let Some(ref tid) = task_id {
                conn.query_row(
                    "SELECT COUNT(*) FROM broker_queue bq \
                     LEFT JOIN invocations inv ON bq.invocation_id = inv.invocation_id \
                     WHERE (bq.task_id = ?1 OR (bq.task_id IS NULL AND inv.task_id = ?1))",
                    [&tid.to_string()],
                    |row| row.get(0),
                )
                .map_err(sql_err)?
            } else {
                conn.query_row("SELECT COUNT(*) FROM broker_queue", [], |row| row.get(0))
                    .map_err(sql_err)?
            };
            Ok(count as usize)
        })
        .await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            if let Some(ref tid) = task_id {
                conn.execute(
                    "DELETE FROM broker_queue WHERE task_id = ?1 OR (task_id IS NULL AND invocation_id IN (\
                     SELECT inv.invocation_id FROM invocations inv WHERE inv.task_id = ?1))",
                    [&tid.to_string()],
                )
                .map_err(sql_err)?;
            } else {
                conn.execute("DELETE FROM broker_queue", [])
                    .map_err(sql_err)?;
            }
            Ok(())
        })
        .await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let db = Arc::clone(&self.db);
        let language = language.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            // Global routing is available to every language worker.
            let global: Option<(i64, String)> = tx
                .query_row(
                    "SELECT bq.id, bq.invocation_id FROM broker_queue bq \
                     WHERE bq.task_id IS NULL \
                     ORDER BY bq.id ASC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            let result = if global.is_some() {
                global
            } else if language.is_empty() {
                tx.query_row(
                    "SELECT id, invocation_id FROM broker_queue \
                     WHERE task_id IS NOT NULL AND task_id NOT LIKE '%::%' \
                     ORDER BY id ASC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            } else {
                let prefix = format!("{language}::");
                tx.query_row(
                    "SELECT id, invocation_id FROM broker_queue \
                     WHERE task_id LIKE ?1 || '%' \
                     ORDER BY id ASC LIMIT 1",
                    [&prefix],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()
            };

            if let Some((row_id, inv_id)) = result {
                tx.execute("DELETE FROM broker_queue WHERE id = ?1", [row_id])
                    .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                Ok(Some(InvocationId::from_string(inv_id)))
            } else {
                Ok(None)
            }
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

        assert_eq!(broker.count_invocations(None).await.unwrap(), 2);

        let r1 = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(r1.unwrap().as_str(), id1.as_str());

        let r2 = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(r2.unwrap().as_str(), id2.as_str());

        assert!(broker.retrieve_invocation(None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_purge() {
        let broker = make_broker();
        broker.route_invocation(&InvocationId::new()).await.unwrap();
        broker.route_invocation(&InvocationId::new()).await.unwrap();

        broker.purge(None).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
    }
}
