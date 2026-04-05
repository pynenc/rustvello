use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::state_backend::{StateBackendRunner, StoredRunnerContext};

use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::invocation::InvocationHistory;
use rustvello_proto::status::InvocationStatusRecord;

use crate::db::{blocking, lock_err, parse_status, parse_timestamp, sql_err};

use super::SqliteStateBackend;

#[async_trait]
impl StateBackendRunner for SqliteStateBackend {
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let context = context.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO runner_contexts
                 (runner_id, runner_cls, pid, hostname, thread_id, started_at, parent_runner_id, parent_runner_cls)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    &context.runner_id,
                    &context.runner_cls,
                    context.pid as i64,
                    &context.hostname,
                    context.thread_id as i64,
                    context.started_at.to_rfc3339(),
                    &context.parent_runner_id,
                    &context.parent_runner_cls,
                ],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>> {
        let db = Arc::clone(&self.db);
        let runner_id = runner_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let result = conn
                .query_row(
                    "SELECT runner_id, runner_cls, pid, hostname, thread_id, started_at,
                            parent_runner_id, parent_runner_cls
                     FROM runner_contexts WHERE runner_id = ?1",
                    rusqlite::params![runner_id],
                    parse_runner_context,
                )
                .ok();
            Ok(result)
        })
        .await
    }

    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let db = Arc::clone(&self.db);
        let parent_runner_id = parent_runner_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT runner_id, runner_cls, pid, hostname, thread_id, started_at,
                            parent_runner_id, parent_runner_cls
                     FROM runner_contexts WHERE parent_runner_id = ?1",
                )
                .map_err(sql_err)?;
            let contexts = stmt
                .query_map([parent_runner_id], parse_runner_context)
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            Ok(contexts)
        })
        .await
    }

    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let runner_id = runner_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let sql = if limit > 0 {
                "SELECT DISTINCT invocation_id FROM history WHERE runner_id = ?1 LIMIT ?2 OFFSET ?3"
            } else {
                "SELECT DISTINCT invocation_id FROM history WHERE runner_id = ?1 LIMIT -1 OFFSET ?3"
            };
            let mut stmt = conn.prepare(sql).map_err(sql_err)?;
            let ids = stmt
                .query_map(
                    rusqlite::params![runner_id, limit as i64, offset as i64],
                    |row| {
                        let id: String = row.get(0)?;
                        Ok(InvocationId::from_string(id))
                    },
                )
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            Ok(ids)
        })
        .await
    }

    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize> {
        let db = Arc::clone(&self.db);
        let runner_id = runner_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(DISTINCT invocation_id) FROM history WHERE runner_id = ?1",
                    rusqlite::params![runner_id],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            Ok(count as usize)
        })
        .await
    }

    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let start_str = start.to_rfc3339();
            let end_str = end.to_rfc3339();
            let effective_ts = "COALESCE(history_timestamp, timestamp)";
            let sql = format!(
                "SELECT invocation_id, status, runner_id, timestamp, message, history_timestamp
                 FROM history WHERE {effective_ts} >= ?1 AND {effective_ts} <= ?2
                 ORDER BY {effective_ts} ASC LIMIT ?3 OFFSET ?4"
            );
            let limit_val: i64 = if limit > 0 { limit as i64 } else { -1 };
            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let histories = stmt
                .query_map(
                    rusqlite::params![&start_str, &end_str, limit_val, offset as i64],
                    |row| {
                        let inv_id: String = row.get(0)?;
                        let status_str: String = row.get(1)?;
                        let runner_id: Option<String> = row.get(2)?;
                        let ts_str: String = row.get(3)?;
                        let message: Option<String> = row.get(4)?;
                        let hist_ts_str: Option<String> = row.get(5)?;
                        Ok((inv_id, status_str, runner_id, ts_str, message, hist_ts_str))
                    },
                )
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?
                .into_iter()
                .map(
                    |(inv_id, status_str, runner_id, ts_str, message, hist_ts_str)| {
                        let status = parse_status(&status_str)?;
                        let timestamp = parse_timestamp(&ts_str)?;
                        let history_timestamp =
                            hist_ts_str.map(|s| parse_timestamp(&s)).transpose()?;
                        Ok(InvocationHistory {
                            invocation_id: InvocationId::from_string(inv_id),
                            status_record: InvocationStatusRecord {
                                status,
                                runner_id: runner_id.clone().map(RunnerId::from_string),
                                timestamp,
                            },
                            message,
                            runner_id: runner_id.map(RunnerId::from_string),
                            registered_by_inv_id: None,
                            history_timestamp,
                        })
                    },
                )
                .collect::<RustvelloResult<Vec<_>>>()?;
            Ok(histories)
        })
        .await
    }

    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let db = Arc::clone(&self.db);
        let partial_id = partial_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let pattern = format!("%{partial_id}%");
            let mut stmt = conn
                .prepare(
                    "SELECT runner_id, runner_cls, pid, hostname, thread_id, started_at,
                            parent_runner_id, parent_runner_cls
                     FROM runner_contexts WHERE runner_id LIKE ?1",
                )
                .map_err(sql_err)?;
            let contexts = stmt
                .query_map([&pattern], parse_runner_context)
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            Ok(contexts)
        })
        .await
    }
}

/// Parse a `runner_contexts` row into a `StoredRunnerContext`.
fn parse_runner_context(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredRunnerContext> {
    let runner_id: String = row.get(0)?;
    let runner_cls: String = row.get(1)?;
    let pid: i64 = row.get(2)?;
    let hostname: String = row.get(3)?;
    let thread_id: i64 = row.get(4)?;
    let started_at_str: String = row.get(5)?;
    let parent_runner_id: Option<String> = row.get(6)?;
    let parent_runner_cls: Option<String> = row.get(7)?;

    let started_at = chrono::DateTime::parse_from_rfc3339(&started_at_str)
        .map_or_else(|_| chrono::Utc::now(), |dt| dt.with_timezone(&chrono::Utc));

    Ok(StoredRunnerContext {
        runner_id,
        runner_cls,
        pid: u32::try_from(pid).unwrap_or(0),
        hostname,
        thread_id: u64::try_from(thread_id).unwrap_or(0),
        started_at,
        parent_runner_id,
        parent_runner_cls,
    })
}
