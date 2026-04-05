use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::OrchestratorStatus;
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::db::{blocking, lock_err, parse_status, parse_timestamp, sql_err};

use super::SqliteOrchestrator;

#[async_trait]
impl OrchestratorStatus for SqliteOrchestrator {
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId> {
        let invocation_id = InvocationId::new();
        self.register_invocation_with_id(&invocation_id, call, None)
            .await?;
        Ok(invocation_id)
    }

    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let call = call.clone();
        let runner_id = runner_id.cloned();
        blocking(move || {

            let now = Utc::now();
            let now_str = now.to_rfc3339();
            let status = InvocationStatus::Registered;
            let status_str = status.to_string();
            let task_id_str = call.task_id.to_string();
            let call_id_str = call.call_id.to_string();
            let runner_id_str = runner_id.as_ref().map(|r| r.as_str().to_owned());

            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            tx.execute(
                "INSERT OR IGNORE INTO invocations (invocation_id, task_id, call_id, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    invocation_id.as_str(),
                    &task_id_str,
                    &call_id_str,
                    &status_str,
                    &now_str,
                    &now_str,
                ],
            )
            .map_err(sql_err)?;

            tx.execute(
                "INSERT OR REPLACE INTO status_records (invocation_id, status, runner_id, timestamp)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![invocation_id.as_str(), &status_str, &runner_id_str, &now_str],
            )
            .map_err(sql_err)?;

            tx.commit().map_err(sql_err)?;

            Ok(InvocationStatusRecord {
                status,
                runner_id,
                timestamp: now,
            })

        })
        .await
    }

    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            tx.execute(
                "INSERT INTO retries (invocation_id, retry_count) VALUES (?1, 1)
                 ON CONFLICT(invocation_id) DO UPDATE SET retry_count = retry_count + 1",
                [invocation_id.as_str()],
            )
            .map_err(sql_err)?;
            let count: u32 = tx
                .query_row(
                    "SELECT retry_count FROM retries WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(count)
        })
        .await
    }

    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let count: u32 = conn
                .query_row(
                    "SELECT retry_count FROM retries WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            Ok(count)
        })
        .await
    }

    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let id = invocation_id.as_str();
            tx.execute("DELETE FROM status_records WHERE invocation_id = ?1", [id])
                .map_err(sql_err)?;
            tx.execute("DELETE FROM cc_arg_pairs WHERE invocation_id = ?1", [id])
                .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM waiting_for WHERE waiter_id = ?1 OR waited_on_id = ?1",
                [id],
            )
            .map_err(sql_err)?;
            tx.execute("DELETE FROM retries WHERE invocation_id = ?1", [id])
                .map_err(sql_err)?;
            tx.execute("DELETE FROM invocations WHERE invocation_id = ?1", [id])
                .map_err(sql_err)?;
            tx.execute(
                "DELETE FROM auto_purge_schedule WHERE invocation_id = ?1",
                [id],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;

            let (status_str, runner_id_opt, timestamp_str): (String, Option<String>, String) = conn
                .query_row(
                    "SELECT status, runner_id, timestamp FROM status_records WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|_| RustvelloError::InvocationNotFound {
                    invocation_id: invocation_id.clone(),
                })?;

            Ok(InvocationStatusRecord {
                status: parse_status(&status_str)?,
                runner_id: runner_id_opt.map(RunnerId::from_string),
                timestamp: parse_timestamp(&timestamp_str)?,
            })

        })
        .await
    }

    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let runner_id = runner_id.cloned();
        blocking(move || {

            use rustvello_proto::status::status_record_transition;

            let conn = db.conn.lock().map_err(lock_err)?;

            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            let (current_status_str, current_runner_id_str, current_ts_str): (
                String,
                Option<String>,
                String,
            ) = tx
                .query_row(
                    "SELECT status, runner_id, timestamp FROM status_records WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .map_err(|_| RustvelloError::InvocationNotFound {
                    invocation_id: invocation_id.clone(),
                })?;
            let current_status = parse_status(&current_status_str)?;
            let current_record = InvocationStatusRecord {
                status: current_status,
                runner_id: current_runner_id_str.map(RunnerId::from_string),
                timestamp: chrono::DateTime::parse_from_rfc3339(&current_ts_str)
                    .map_or_else(|_| Utc::now(), |dt| dt.with_timezone(&Utc)),
            };

            let new_record = status_record_transition(Some(&current_record), status, runner_id.as_ref())
                .map_err(|e| {
                    rustvello_core::error::status_machine_error_to_rustvello(
                        e,
                        &invocation_id,
                        current_status,
                    )
                })?;

            let now_str = new_record.timestamp.to_rfc3339();
            let status_str = status.to_string();
            let runner_id_str = new_record.runner_id.as_ref().map(|r| r.as_str().to_owned());

            tx.execute(
                "UPDATE status_records SET status = ?1, runner_id = ?2, timestamp = ?3 WHERE invocation_id = ?4",
                rusqlite::params![&status_str, &runner_id_str, &now_str, invocation_id.as_str()],
            )
            .map_err(sql_err)?;

            tx.execute(
                "UPDATE invocations SET status = ?1, updated_at = ?2 WHERE invocation_id = ?3",
                rusqlite::params![&status_str, &now_str, invocation_id.as_str()],
            )
            .map_err(sql_err)?;

            tx.commit().map_err(sql_err)?;

            Ok(new_record)

        })
        .await
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute_batch(
                "DELETE FROM cc_arg_pairs;
                 DELETE FROM waiting_for;
                 DELETE FROM status_records;
                 DELETE FROM retries;
                 DELETE FROM runner_heartbeats;
                 DELETE FROM auto_purge_schedule;
                 DELETE FROM invocations;",
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn schedule_auto_purge(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {

            let now_str = Utc::now().to_rfc3339();
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO auto_purge_schedule (invocation_id, scheduled_at) VALUES (?1, ?2)",
                rusqlite::params![invocation_id.as_str(), &now_str],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn run_auto_purge(&self, max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let expired: Vec<String> = blocking(move || {
            let threshold = Utc::now()
                - chrono::Duration::seconds(i64::try_from(max_age_secs).unwrap_or(i64::MAX));
            let threshold_str = threshold.to_rfc3339();

            let conn = db.conn.lock().map_err(lock_err)?;
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let mut stmt = tx
                .prepare("SELECT invocation_id FROM auto_purge_schedule WHERE scheduled_at <= ?1")
                .map_err(sql_err)?;
            let rows: Vec<String> = stmt
                .query_map([&threshold_str], |row| row.get(0))
                .map_err(sql_err)?
                .collect::<Result<Vec<String>, _>>()
                .map_err(sql_err)?;
            drop(stmt);
            tx.execute(
                "DELETE FROM auto_purge_schedule WHERE scheduled_at <= ?1",
                [&threshold_str],
            )
            .map_err(sql_err)?;
            tx.commit().map_err(sql_err)?;
            Ok(rows)
        })
        .await?;

        let mut purged = Vec::new();
        for id_str in expired {
            let inv_id = InvocationId::from_string(id_str);
            if self.remove_invocation(&inv_id).await.is_ok() {
                purged.push(inv_id);
            }
        }
        Ok(purged)
    }
}
