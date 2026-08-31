use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorRecovery;
use rustvello_core::orchestrator::{ActiveRunnerInfo, AtomicServiceExecution};
use rustvello_proto::identifiers::{InvocationId, RunnerId};

use crate::db::{blocking, lock_err, parse_timestamp, sql_err};

use super::SqliteOrchestrator;

#[async_trait]
impl OrchestratorRecovery for SqliteOrchestrator {
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let runner_id = runner_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let now = Utc::now().to_rfc3339();
            let can_run = if can_run_atomic_service { 1i32 } else { 0i32 };

            conn.execute(
                "INSERT INTO runner_heartbeats (runner_id, creation_time, last_heartbeat, can_run_atomic_service)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(runner_id) DO UPDATE SET last_heartbeat = ?3, can_run_atomic_service = ?4",
                rusqlite::params![runner_id.as_str(), &now, &now, can_run],
            )
            .map_err(sql_err)?;

            Ok(())

        })
        .await
    }

    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let threshold = (Utc::now()
                - chrono::Duration::seconds(
                    i64::try_from(max_pending_seconds).unwrap_or(i64::MAX),
                ))
            .to_rfc3339();

            let mut stmt = conn
                .prepare(
                    "SELECT invocation_id FROM status_records
                     WHERE status = 'PENDING' AND timestamp < ?1",
                )
                .map_err(sql_err)?;

            let ids: Vec<InvocationId> = stmt
                .query_map([&threshold], |row| {
                    let id: String = row.get(0)?;
                    Ok(InvocationId::from_string(id))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            Ok(ids)
        })
        .await
    }

    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let threshold = (Utc::now()
                - chrono::Duration::seconds(
                    i64::try_from(runner_dead_after_seconds).unwrap_or(i64::MAX),
                ))
            .to_rfc3339();

            let mut stmt = conn
                .prepare(
                    "SELECT sr.invocation_id FROM status_records sr
                     LEFT JOIN runner_heartbeats rh ON sr.runner_id = rh.runner_id
                     WHERE sr.status = 'RUNNING'
                       AND (rh.last_heartbeat IS NULL OR rh.last_heartbeat < ?1)",
                )
                .map_err(sql_err)?;

            let ids: Vec<InvocationId> = stmt
                .query_map([&threshold], |row| {
                    let id: String = row.get(0)?;
                    Ok(InvocationId::from_string(id))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            Ok(ids)
        })
        .await
    }

    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let threshold = (Utc::now()
                - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX)))
            .to_rfc3339();

            let mut stmt = conn
                .prepare("SELECT runner_id FROM runner_heartbeats WHERE last_heartbeat >= ?1")
                .map_err(sql_err)?;

            let ids: Vec<RunnerId> = stmt
                .query_map([&threshold], |row| {
                    let id: String = row.get(0)?;
                    Ok(RunnerId::from_string(id))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            Ok(ids)
        })
        .await
    }

    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>> {
        let db = Arc::clone(&self.db);
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let threshold = (Utc::now()
                - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX)))
            .to_rfc3339();

            let sql = match can_run_atomic_service {
                Some(true) => {
                    "SELECT runner_id, creation_time, last_heartbeat, can_run_atomic_service, last_service_start, last_service_end
                     FROM runner_heartbeats WHERE last_heartbeat >= ?1 AND can_run_atomic_service = 1"
                }
                Some(false) => {
                    "SELECT runner_id, creation_time, last_heartbeat, can_run_atomic_service, last_service_start, last_service_end
                     FROM runner_heartbeats WHERE last_heartbeat >= ?1 AND can_run_atomic_service = 0"
                }
                None => {
                    "SELECT runner_id, creation_time, last_heartbeat, can_run_atomic_service, last_service_start, last_service_end
                     FROM runner_heartbeats WHERE last_heartbeat >= ?1"
                }
            };

            let mut stmt = conn.prepare(sql).map_err(sql_err)?;
            let runners: Vec<ActiveRunnerInfo> = stmt
                .query_map([&threshold], |row| {
                    let runner_id: String = row.get(0)?;
                    let creation_time_str: String = row.get(1)?;
                    let last_heartbeat_str: String = row.get(2)?;
                    let can_run: i32 = row.get(3)?;
                    let last_service_start_str: Option<String> = row.get(4)?;
                    let last_service_end_str: Option<String> = row.get(5)?;
                    Ok((
                        runner_id,
                        creation_time_str,
                        last_heartbeat_str,
                        can_run,
                        last_service_start_str,
                        last_service_end_str,
                    ))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?
                .into_iter()
                .filter_map(|(rid, ct, lh, can_run, lss, lse)| {
                    let creation_time = parse_timestamp(&ct).ok()?;
                    let last_heartbeat = parse_timestamp(&lh).ok()?;
                    let last_service_start = lss.as_deref().and_then(|s| parse_timestamp(s).ok());
                    let last_service_end = lse.as_deref().and_then(|s| parse_timestamp(s).ok());
                    Some(ActiveRunnerInfo {
                        runner_id: RunnerId::from_string(rid),
                        creation_time,
                        last_heartbeat,
                        can_run_atomic_service: can_run != 0,
                        last_service_start,
                        last_service_end,
                    })
                })
                .collect();

            Ok(runners)

        })
        .await
    }

    async fn record_atomic_service_execution(
        &self,
        runner_id: &RunnerId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let runner_id = runner_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let start_str = start.to_rfc3339();
            let end_str = end.to_rfc3339();

            conn.execute(
                "INSERT INTO runner_heartbeats (
                     runner_id, creation_time, last_heartbeat,
                     can_run_atomic_service, last_service_start, last_service_end
                 ) VALUES (?1, ?2, ?3, 1, ?2, ?3)
                 ON CONFLICT(runner_id) DO UPDATE SET
                     last_service_start = excluded.last_service_start,
                     last_service_end = excluded.last_service_end",
                rusqlite::params![runner_id.as_str(), &start_str, &end_str],
            )
            .map_err(sql_err)?;

            Ok(())
        })
        .await
    }

    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT runner_id, last_service_start, last_service_end
                     FROM runner_heartbeats
                     WHERE last_service_start IS NOT NULL AND last_service_end IS NOT NULL
                     ORDER BY last_service_start DESC",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sql_err)?;

            rows.map(|row| {
                let (runner_id, start, end) = row.map_err(sql_err)?;
                Ok(AtomicServiceExecution {
                    runner_id,
                    start: parse_timestamp(&start)?,
                    end: parse_timestamp(&end)?,
                })
            })
            .collect()
        })
        .await
    }
}
