use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::OptionalExtension;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};

use crate::db::{blocking, lock_err, sql_err, Database};

/// SQLite-backed broker with atomic named-queue priority retrieval.
pub struct SqliteBroker {
    db: Arc<Database>,
    reservation_lease: Duration,
}

impl SqliteBroker {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            reservation_lease: Duration::from_secs(60),
        }
    }

    /// Set the durable dequeue lease (100ms..=1h; default 60s).
    ///
    /// All consumers should use the same setting. The SQLite orchestrator
    /// acknowledges delivery atomically when Pending commits. Without that
    /// acknowledgment, a later broker poll redelivers after this wall-clock
    /// lease expires. Lease expiry never changes invocation status or owner.
    pub fn with_reservation_lease(mut self, lease: Duration) -> RustvelloResult<Self> {
        if !(Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&lease) {
            return Err(RustvelloError::Configuration {
                message: "SQLite reservation lease must be between 100ms and 1h".into(),
            });
        }
        self.reservation_lease = lease;
        Ok(self)
    }

    async fn reserve(
        &self,
        queue: &str,
        task: Option<&TaskId>,
        language: Option<TaskLanguage>,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue, 0.0)?;
        let db = Arc::clone(&self.db);
        let queue = queue.to_owned();
        let task = task.map(ToString::to_string);
        let language = language.map(|l| l.to_string());
        let lease_ms = self.reservation_lease.as_millis() as i64;
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = rusqlite::Transaction::new_unchecked(
                &conn,
                rusqlite::TransactionBehavior::Immediate,
            )
            .map_err(sql_err)?;
            // Bound cleanup per poll. A duplicate publication for an owned or
            // terminal invocation must not redeliver forever after lease expiry.
            tx.execute(
                "DELETE FROM broker_queue WHERE id IN (
                    SELECT q.id FROM broker_queue q JOIN status_records s
                        ON s.invocation_id = q.invocation_id
                    WHERE q.queue_name = ?1
                      AND s.status NOT IN ('REGISTERED', 'RETRY', 'REROUTED')
                    LIMIT 128)",
                [&queue],
            )
            .map_err(sql_err)?;
            let now = chrono::Utc::now().timestamp_millis();
            let row: Option<(i64, String)> = tx
                .query_row(
                    "SELECT q.id, q.invocation_id FROM broker_queue q
                 LEFT JOIN broker_reservations r ON r.queue_id = q.id
                 WHERE q.queue_name = ?1
                   AND (?2 IS NULL OR q.task_id = ?2)
                   AND (?3 IS NULL OR (q.task_id IS NULL AND ?3 = 'rust')
                        OR q.task_id LIKE ?3 || '::%')
                   AND (r.queue_id IS NULL OR r.expires_at_ms <= ?4)
                   AND NOT EXISTS (SELECT 1 FROM status_records s
                        WHERE s.invocation_id = q.invocation_id
                          AND s.status NOT IN ('REGISTERED', 'RETRY', 'REROUTED'))
                 ORDER BY q.priority DESC, q.id ASC LIMIT 1",
                    rusqlite::params![queue, task, language, now],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(sql_err)?;
            if let Some((row_id, invocation_id)) = row {
                tx.execute(
                    "INSERT INTO broker_reservations (queue_id, expires_at_ms) VALUES (?1, ?2)
                     ON CONFLICT(queue_id) DO UPDATE SET expires_at_ms = excluded.expires_at_ms",
                    rusqlite::params![row_id, now + lease_ms],
                )
                .map_err(sql_err)?;
                tx.commit().map_err(sql_err)?;
                Ok(Some(InvocationId::from_string(invocation_id)))
            } else {
                tx.commit().map_err(sql_err)?;
                Ok(None)
            }
        })
        .await
    }
}

/// Wall-clock milliseconds at which a delayed entry becomes deliverable.
pub(crate) fn not_before_ms(delay: Duration) -> i64 {
    let delay = i64::try_from(delay.as_millis()).unwrap_or(i64::MAX);
    chrono::Utc::now().timestamp_millis().saturating_add(delay)
}

#[async_trait]
impl Broker for SqliteBroker {
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

    fn supports_delayed_delivery(&self) -> bool {
        true
    }

    /// The entry is inserted with a delivery lease that nobody holds and that
    /// expires at the not-before time, so retrieval and counts skip it until
    /// then. Both rows commit in one transaction in the database file.
    async fn route_invocation_after(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
        delay: Duration,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let task_id = task_id.map(ToString::to_string);
        let queue_name = queue_name.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = rusqlite::Transaction::new_unchecked(
                &conn,
                rusqlite::TransactionBehavior::Immediate,
            )
            .map_err(sql_err)?;
            tx.execute(
                "INSERT INTO broker_queue (invocation_id, task_id, queue_name, priority) \
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![invocation_id.as_str(), task_id, queue_name, priority],
            )
            .map_err(sql_err)?;
            let row_id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO broker_reservations (queue_id, expires_at_ms) VALUES (?1, ?2)",
                rusqlite::params![row_id, not_before_ms(delay)],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
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
        self.reserve(queue_name, task_id, None).await
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
        self.reserve(queue_name, None, Some(language)).await
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
        let db = Arc::clone(&self.db);
        let queue_names = queue_names.to_vec();
        let task_id = task_id.map(ToString::to_string);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let queues = if queue_names.is_empty() {
                vec![None]
            } else {
                queue_names.into_iter().map(Some).collect()
            };
            let now = chrono::Utc::now().timestamp_millis();
            let mut count = 0i64;
            for queue in queues {
                count += conn
                    .query_row(
                        "SELECT COUNT(*) FROM broker_queue q
                     LEFT JOIN broker_reservations r ON r.queue_id = q.id
                     WHERE (?1 IS NULL OR q.queue_name = ?1)
                       AND (?2 IS NULL OR q.task_id = ?2)
                       AND (r.queue_id IS NULL OR r.expires_at_ms <= ?3)
                       AND NOT EXISTS (SELECT 1 FROM status_records s
                            WHERE s.invocation_id = q.invocation_id
                              AND s.status NOT IN ('REGISTERED', 'RETRY', 'REROUTED'))",
                        rusqlite::params![queue, task_id, now],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(sql_err)?;
            }
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
    async fn delayed_entry_is_hidden_until_due_then_delivered_once() {
        let broker = make_broker();
        assert!(broker.supports_delayed_delivery());
        let id = InvocationId::new();
        broker
            .route_invocation_after(&id, None, DEFAULT_QUEUE, 0.0, Duration::from_millis(300))
            .await
            .unwrap();
        assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
        assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
        assert_eq!(
            broker.retrieve_invocation(None).await.unwrap(),
            Some(id.clone())
        );
        // Delivered once: the new lease hides it from other consumers.
        assert_eq!(broker.retrieve_invocation(None).await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_purge() {
        let broker = make_broker();
        broker.route_invocation(&InvocationId::new()).await.unwrap();
        broker.purge(None).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn file_lock_errors_are_visible_and_app_local() {
        let dir = std::env::temp_dir().join(format!("sqlite-lock-{}", InvocationId::new()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("backend.db");
        let locked = Database::open(&path, "a").unwrap();
        let same_app = Arc::new(Database::open(&path, "a").unwrap());
        let other_app = Arc::new(Database::open(&path, "b").unwrap());
        same_app
            .conn
            .lock()
            .unwrap()
            .busy_timeout(std::time::Duration::from_millis(20))
            .unwrap();
        locked
            .conn
            .lock()
            .unwrap()
            .execute_batch("BEGIN IMMEDIATE")
            .unwrap();

        let same = SqliteBroker::new(Arc::clone(&same_app));
        let other = SqliteBroker::new(Arc::clone(&other_app));
        let id = InvocationId::new();
        let task = TaskId::new("locked", "task");
        assert!(same.route_invocation(&id).await.is_err());
        assert!(same.retrieve_invocation(None).await.is_err());
        assert!(same.retrieve_invocation(Some(&task)).await.is_err());
        assert!(same
            .retrieve_invocation_for_language(TaskLanguage::Rust)
            .await
            .is_err());
        other.route_invocation(&id).await.unwrap();
        assert_eq!(
            other.retrieve_invocation(None).await.unwrap(),
            Some(id.clone())
        );

        locked
            .conn
            .lock()
            .unwrap()
            .execute_batch("ROLLBACK")
            .unwrap();
        same.route_invocation(&id).await.unwrap();
        assert_eq!(same.retrieve_invocation(None).await.unwrap(), Some(id));
        drop((same, other, same_app, other_app, locked));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
