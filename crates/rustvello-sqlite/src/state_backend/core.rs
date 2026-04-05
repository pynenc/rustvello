use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::state_backend::StateBackendCore;

use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{CallId, InvocationId, RunnerId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use crate::db::{blocking, lock_err, parse_status, parse_timestamp, sql_err};

use super::SqliteStateBackend;

#[async_trait]
impl StateBackendCore for SqliteStateBackend {
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation = invocation.clone();
        let call = call.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;

            let tx = conn.unchecked_transaction().map_err(sql_err)?;

            let args_json = serde_json::to_string(&call.serialized_arguments.0)
                .map_err(|e| RustvelloError::Serialization { message: e.to_string() })?;

            let (parent_inv_id, wf_id, wf_type, wf_depth) = match &invocation.workflow {
                Some(wf) => (
                    invocation
                        .parent_invocation_id
                        .as_ref()
                        .map(|id| id.as_str().to_owned()),
                    Some(wf.workflow_id.as_str().to_owned()),
                    Some(wf.workflow_type.to_string()),
                    Some(wf.depth as i64),
                ),
                None => (
                    invocation
                        .parent_invocation_id
                        .as_ref()
                        .map(|id| id.as_str().to_owned()),
                    None,
                    None,
                    None,
                ),
            };

            tx.execute(
                "INSERT OR REPLACE INTO invocations (invocation_id, task_id, call_id, status, created_at, updated_at, parent_invocation_id, workflow_id, workflow_type, workflow_depth)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                rusqlite::params![
                    &invocation.invocation_id.as_str(),
                    &invocation.task_id.to_string(),
                    &invocation.call_id.to_string(),
                    &invocation.status.to_string(),
                    &invocation.created_at.to_rfc3339(),
                    &invocation.updated_at.to_rfc3339(),
                    &parent_inv_id,
                    &wf_id,
                    &wf_type,
                    &wf_depth,
                ],
            )
            .map_err(sql_err)?;

            tx.execute(
                "INSERT OR REPLACE INTO calls (call_id, task_id, serialized_arguments) VALUES (?1, ?2, ?3)",
                rusqlite::params![
                    &call.call_id.to_string(),
                    &call.task_id.to_string(),
                    &args_json,
                ],
            )
            .map_err(sql_err)?;

            tx.commit().map_err(sql_err)?;

            Ok(())

        })
        .await
    }

    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;

            let (task_id_str, call_id_str, status_str, created_str, updated_str, parent_inv_id, wf_id, wf_type, wf_depth): (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
            ) = conn
                .query_row(
                    "SELECT task_id, call_id, status, created_at, updated_at, parent_invocation_id, workflow_id, workflow_type, workflow_depth FROM invocations WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?)),
                )
                .map_err(|_| RustvelloError::InvocationNotFound { invocation_id: invocation_id.clone() })?;

            let task_id: TaskId = task_id_str
                .parse()
                .map_err(|e| RustvelloError::state_backend(format!("invalid task_id in database: {e}")))?;

            let args_id = call_id_str
                .rsplit_once(':')
                .map_or(call_id_str.as_str(), |(_, a)| a);
            let call_id = CallId::new(task_id.clone(), args_id);

            let created_at = parse_timestamp(&created_str)?;
            let updated_at = parse_timestamp(&updated_str)?;

            let parent_invocation_id = parent_inv_id.map(InvocationId::from_string);

            let workflow = match (wf_id, wf_type) {
                (Some(wf_id_str), Some(wf_type_str)) => {
                    let wf_task_id: TaskId = wf_type_str.parse().map_err(|e| {
                        RustvelloError::state_backend(format!("invalid workflow task_id in database: {e}"))
                    })?;
                    Some(WorkflowIdentity {
                        workflow_id: InvocationId::from_string(wf_id_str),
                        workflow_type: wf_task_id,
                        parent_id: None,
                        depth: u32::try_from(wf_depth.unwrap_or(0)).unwrap_or(0),
                    })
                }
                _ => None,
            };

            Ok(InvocationDTO {
                invocation_id: invocation_id.clone(),
                task_id,
                call_id,
                status: parse_status(&status_str)?,
                created_at,
                updated_at,
                parent_invocation_id,
                workflow,
            })

        })
        .await
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        let db = Arc::clone(&self.db);
        let call_id = call_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let call_id_str = call_id.to_string();

            let (task_id_str, args_json): (String, String) = conn
                .query_row(
                    "SELECT task_id, serialized_arguments FROM calls WHERE call_id = ?1",
                    [&call_id_str],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| {
                    RustvelloError::state_backend(format!("call not found: {}", call_id_str))
                })?;

            let task_id: TaskId = task_id_str.parse().map_err(|e| {
                RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
            })?;

            let args_map: std::collections::BTreeMap<String, String> =
                serde_json::from_str(&args_json).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;

            let args = SerializedArguments(args_map);

            Ok(CallDTO {
                call_id: call_id.clone(),
                task_id,
                serialized_arguments: args,
            })
        })
        .await
    }

    async fn store_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let result = result.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO results (invocation_id, result) VALUES (?1, ?2)",
                rusqlite::params![invocation_id.as_str(), result],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let result: Option<String> = conn
                .query_row(
                    "SELECT result FROM results WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| row.get(0),
                )
                .ok();
            Ok(result)
        })
        .await
    }

    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let error = error.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO errors (invocation_id, error_type, message, traceback) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    invocation_id.as_str(),
                    &error.error_type,
                    &error.message,
                    &error.traceback,
                ],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let result: Option<(String, String, Option<String>)> = conn
                .query_row(
                    "SELECT error_type, message, traceback FROM errors WHERE invocation_id = ?1",
                    [invocation_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .ok();

            Ok(result.map(|(error_type, message, traceback)| TaskError {
                error_type,
                message,
                traceback,
            }))
        })
        .await
    }

    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let history = history.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let hist_ts = history.history_timestamp.map(|ts| ts.to_rfc3339());
            conn.execute(
                "INSERT INTO history (invocation_id, status, runner_id, timestamp, message, history_timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    &history.invocation_id.as_str(),
                    &history.status_record.status.to_string(),
                    &history.status_record.runner_id.as_ref().map(|r| r.as_str().to_string()),
                    &history.status_record.timestamp.to_rfc3339(),
                    &history.message,
                    &hist_ts,
                ],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;

            let mut stmt = conn
                .prepare(
                    "SELECT status, runner_id, timestamp, message, history_timestamp FROM history WHERE invocation_id = ?1 ORDER BY id",
                )
                .map_err(sql_err)?;

            let histories: Vec<InvocationHistory> = stmt
                .query_map([invocation_id.as_str()], |row| {
                    let status_str: String = row.get(0)?;
                    let runner_id: Option<String> = row.get(1)?;
                    let timestamp_str: String = row.get(2)?;
                    let message: Option<String> = row.get(3)?;
                    let hist_ts_str: Option<String> = row.get(4)?;

                    let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())),
                            )
                        })?;

                    let history_timestamp = hist_ts_str
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&chrono::Utc));

                    let status = status_str.parse::<InvocationStatus>().map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                        )
                    })?;

                    Ok(InvocationHistory {
                        invocation_id: invocation_id.clone(),
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
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;

            Ok(histories)

        })
        .await
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute_batch(
                "DELETE FROM invocations;
                 DELETE FROM calls;
                 DELETE FROM results;
                 DELETE FROM errors;
                 DELETE FROM history;
                 DELETE FROM status_records;
                 DELETE FROM waiting_for;
                 DELETE FROM broker_queue;
                 DELETE FROM workflow_runs;
                 DELETE FROM workflow_data;
                 DELETE FROM app_infos;
                 DELETE FROM workflow_sub_invocations;
                 DELETE FROM runner_contexts;",
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }
}
