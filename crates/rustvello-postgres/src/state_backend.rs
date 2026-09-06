//! PostgreSQL-backed [`StateBackend`] implementation.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use rustvello_core::error::{RustvelloError, RustvelloResult, TaskError};
use rustvello_core::state_backend::{
    StateBackendCore, StateBackendQuery, StateBackendRunner, StoredRunnerContext,
};
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{
    CallId, ExecutorKind, InvocationId, RunnerId, TaskId, TaskLanguage,
};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};
use rustvello_proto::status::InvocationStatusRecord;

use crate::db::{parse_status, pg_err, Database};

/// PostgreSQL-backed state backend implementation.
pub struct PostgresStateBackend {
    db: Arc<Database>,
}

impl PostgresStateBackend {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl StateBackendCore for PostgresStateBackend {
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()> {
        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        let args_json = serde_json::to_string(&call.serialized_arguments.0).map_err(|e| {
            RustvelloError::Serialization {
                message: e.to_string(),
            }
        })?;

        let (parent_inv_id, wf_id, wf_type, wf_depth) = match &invocation.workflow {
            Some(wf) => (
                invocation
                    .parent_invocation_id
                    .as_ref()
                    .map(|id| id.as_str().to_string()),
                Some(wf.workflow_id.as_str().to_string()),
                Some(wf.workflow_type.to_string()),
                Some(wf.depth as i32),
            ),
            None => (
                invocation
                    .parent_invocation_id
                    .as_ref()
                    .map(|id| id.as_str().to_string()),
                None,
                None,
                None,
            ),
        };

        tx
            .execute(
                "INSERT INTO invocations (invocation_id, task_id, call_id, status, created_at, updated_at,
                    parent_invocation_id, workflow_id, workflow_type, workflow_depth)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (invocation_id) DO UPDATE SET
                    task_id = $2, call_id = $3, status = $4, updated_at = $6,
                    parent_invocation_id = $7, workflow_id = $8, workflow_type = $9, workflow_depth = $10",
                &[
                    &invocation.invocation_id.as_str(),
                    &invocation.task_id.to_string(),
                    &invocation.call_id.to_string(),
                    &invocation.status.to_string(),
                    &invocation.created_at,
                    &invocation.updated_at,
                    &parent_inv_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    &wf_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    &wf_type as &(dyn tokio_postgres::types::ToSql + Sync),
                    &wf_depth as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(pg_err)?;

        tx.execute(
            "INSERT INTO calls (call_id, task_id, serialized_arguments) VALUES ($1, $2, $3)
                 ON CONFLICT (call_id) DO UPDATE SET task_id = $2, serialized_arguments = $3",
            &[
                &call.call_id.to_string(),
                &call.task_id.to_string(),
                &args_json,
            ],
        )
        .await
        .map_err(pg_err)?;

        tx.commit().await.map_err(pg_err)?;

        Ok(())
    }

    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        let client = self.db.conn().await?;

        let row = client
            .query_opt(
                "SELECT task_id, call_id, status, created_at, updated_at,
                        parent_invocation_id, workflow_id, workflow_type, workflow_depth
                 FROM invocations WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?
            .ok_or_else(|| RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            })?;

        let task_id_str: String = row.get(0);
        let call_id_str: String = row.get(1);
        let status_str: String = row.get(2);
        let created_at: chrono::DateTime<Utc> = row.get(3);
        let updated_at: chrono::DateTime<Utc> = row.get(4);
        let parent_inv_id: Option<String> = row.get(5);
        let wf_id: Option<String> = row.get(6);
        let wf_type: Option<String> = row.get(7);
        let wf_depth: Option<i32> = row.get(8);

        let task_id: TaskId = task_id_str.parse().map_err(|e| {
            RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
        })?;

        let call_id: CallId = call_id_str.parse().map_err(|e| {
            RustvelloError::state_backend(format!("invalid call_id in database: {e}"))
        })?;

        let parent_invocation_id = parent_inv_id.map(InvocationId::from_string);

        let workflow = match (wf_id, wf_type) {
            (Some(wf_id_str), Some(wf_type_str)) => {
                let wf_task_id: TaskId = wf_type_str.parse().map_err(|e| {
                    RustvelloError::state_backend(format!(
                        "invalid workflow task_id in database: {e}"
                    ))
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
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        let client = self.db.conn().await?;
        let call_id_str = call_id.to_string();

        let row = client
            .query_opt(
                "SELECT task_id, serialized_arguments FROM calls WHERE call_id = $1",
                &[&call_id_str],
            )
            .await
            .map_err(pg_err)?
            .ok_or_else(|| {
                RustvelloError::state_backend(format!("call not found: {}", call_id_str))
            })?;

        let task_id_str: String = row.get(0);
        let args_json: String = row.get(1);

        let task_id: TaskId = task_id_str.parse().map_err(|e| {
            RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
        })?;

        let args_map: std::collections::BTreeMap<String, String> = serde_json::from_str(&args_json)
            .map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;

        let args = SerializedArguments(args_map);

        Ok(CallDTO {
            call_id: call_id.clone(),
            task_id,
            serialized_arguments: args,
        })
    }

    async fn store_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO results (invocation_id, result) VALUES ($1, $2)
                 ON CONFLICT (invocation_id) DO UPDATE SET result = $2",
                &[&invocation_id.as_str(), &result],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>> {
        let client = self.db.conn().await?;
        let row = client
            .query_opt(
                "SELECT result FROM results WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO errors (invocation_id, error_type, message, traceback) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (invocation_id) DO UPDATE SET error_type = $2, message = $3, traceback = $4",
                &[
                    &invocation_id.as_str(),
                    &error.error_type,
                    &error.message,
                    &error.traceback as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        let client = self.db.conn().await?;
        let row = client
            .query_opt(
                "SELECT error_type, message, traceback FROM errors WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        Ok(row.map(|r| TaskError {
            error_type: r.get(0),
            message: r.get(1),
            traceback: r.get(2),
        }))
    }

    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        let runner_id_str = history
            .status_record
            .runner_id
            .as_ref()
            .map(|r| r.as_str().to_string());

        client
            .execute(
                "INSERT INTO history (invocation_id, status, runner_id, timestamp, message, history_timestamp)
                 VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    &history.invocation_id.as_str(),
                    &history.status_record.status.to_string(),
                    &runner_id_str as &(dyn tokio_postgres::types::ToSql + Sync),
                    &history.status_record.timestamp,
                    &history.message as &(dyn tokio_postgres::types::ToSql + Sync),
                    &history.history_timestamp as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let client = self.db.conn().await?;

        let rows = client
            .query(
                "SELECT status, runner_id, timestamp, message, history_timestamp FROM history
                 WHERE invocation_id = $1 ORDER BY id",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        let mut histories = Vec::with_capacity(rows.len());
        for row in &rows {
            let status_str: String = row.get(0);
            let runner_id: Option<String> = row.get(1);
            let timestamp: chrono::DateTime<Utc> = row.get(2);
            let message: Option<String> = row.get(3);
            let history_timestamp: Option<chrono::DateTime<Utc>> = row.get(4);

            histories.push(InvocationHistory {
                invocation_id: invocation_id.clone(),
                status_record: InvocationStatusRecord {
                    status: parse_status(&status_str)?,
                    runner_id: runner_id.clone().map(RunnerId::from_string),
                    timestamp,
                },
                message,
                runner_id: runner_id.map(RunnerId::from_string),
                registered_by_inv_id: None,
                history_timestamp,
            });
        }
        Ok(histories)
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .batch_execute(
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
            .await
            .map_err(pg_err)?;
        Ok(())
    }
}

#[async_trait]
impl StateBackendQuery for PostgresStateBackend {
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT invocation_id FROM invocations WHERE workflow_id = $1",
                &[&workflow_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT invocation_id FROM invocations WHERE parent_invocation_id = $1",
                &[&parent_invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn store_workflow_run(&self, workflow: &WorkflowIdentity) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        let parent_id = workflow
            .parent_id
            .as_ref()
            .map(|id| id.as_str().to_string());
        client
            .execute(
                "INSERT INTO workflow_runs (workflow_id, workflow_type, parent_workflow_id, depth)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (workflow_id) DO UPDATE SET
                    workflow_type = $2, parent_workflow_id = $3, depth = $4",
                &[
                    &workflow.workflow_id.as_str(),
                    &workflow.workflow_type.to_string(),
                    &parent_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    &(workflow.depth as i32),
                ],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_all_workflow_types(&self) -> RustvelloResult<Vec<TaskId>> {
        let client = self.db.conn().await?;
        let rows = client
            .query("SELECT DISTINCT workflow_type FROM workflow_runs", &[])
            .await
            .map_err(pg_err)?;
        rows.iter()
            .map(|r| {
                let s: String = r.get(0);
                s.parse::<TaskId>().map_err(|e| {
                    RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
                })
            })
            .collect()
    }

    async fn get_workflow_runs(
        &self,
        workflow_type: &TaskId,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT workflow_id, workflow_type, parent_workflow_id, depth
                 FROM workflow_runs WHERE workflow_type = $1",
                &[&workflow_type.to_string()],
            )
            .await
            .map_err(pg_err)?;
        rows.iter()
            .map(|r| {
                let wf_id: String = r.get(0);
                let wf_type: String = r.get(1);
                let parent_id: Option<String> = r.get(2);
                let depth: i32 = r.get(3);
                let task_id = wf_type.parse::<TaskId>().map_err(|e| {
                    RustvelloError::state_backend(format!(
                        "invalid workflow task_id in database: {e}"
                    ))
                })?;
                Ok(WorkflowIdentity {
                    workflow_id: InvocationId::from_string(wf_id),
                    workflow_type: task_id,
                    parent_id: parent_id.map(InvocationId::from_string),
                    depth: u32::try_from(depth).unwrap_or(0),
                })
            })
            .collect()
    }

    async fn count_workflow_runs(&self, workflow_type: &TaskId) -> RustvelloResult<usize> {
        let client = self.db.conn().await?;
        let row = client
            .query_one(
                "SELECT COUNT(*) FROM workflow_runs WHERE workflow_type = $1",
                &[&workflow_type.to_string()],
            )
            .await
            .map_err(pg_err)?;
        let count: i64 = row.get(0);
        Ok(usize::try_from(count).unwrap_or(usize::MAX))
    }

    async fn get_workflow_runs_paginated(
        &self,
        workflow_type: &TaskId,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT workflow_id, workflow_type, parent_workflow_id, depth
                 FROM workflow_runs WHERE workflow_type = $1
                 ORDER BY workflow_id DESC LIMIT $2 OFFSET $3",
                &[
                    &workflow_type.to_string(),
                    &i64::try_from(limit).unwrap_or(i64::MAX),
                    &i64::try_from(offset).unwrap_or(i64::MAX),
                ],
            )
            .await
            .map_err(pg_err)?;
        rows.iter()
            .map(|row| {
                let workflow_id: String = row.get(0);
                let workflow_type: String = row.get(1);
                let parent_id: Option<String> = row.get(2);
                let depth: i32 = row.get(3);
                Ok(WorkflowIdentity {
                    workflow_id: InvocationId::from_string(workflow_id),
                    workflow_type: workflow_type.parse::<TaskId>().map_err(|error| {
                        RustvelloError::state_backend(format!(
                            "invalid workflow task_id in database: {error}"
                        ))
                    })?,
                    parent_id: parent_id.map(InvocationId::from_string),
                    depth: u32::try_from(depth).unwrap_or(0),
                })
            })
            .collect()
    }

    async fn set_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
        value: &str,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        let key_s = key.to_string();
        let value_s = value.to_string();
        client
            .execute(
                "INSERT INTO workflow_data (workflow_id, data_key, data_value)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (workflow_id, data_key) DO UPDATE SET data_value = $3",
                &[&workflow_id.as_str(), &key_s, &value_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>> {
        let client = self.db.conn().await?;
        let key_s = key.to_string();
        let row = client
            .query_opt(
                "SELECT data_value FROM workflow_data WHERE workflow_id = $1 AND data_key = $2",
                &[&workflow_id.as_str(), &key_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        let app_id_s = app_id.to_string();
        let info_s = info_json.to_string();
        client
            .execute(
                "INSERT INTO app_infos (app_id, info_json) VALUES ($1, $2)
                 ON CONFLICT (app_id) DO UPDATE SET info_json = $2",
                &[&app_id_s, &info_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>> {
        let client = self.db.conn().await?;
        let app_id_s = app_id.to_string();
        let row = client
            .query_opt(
                "SELECT info_json FROM app_infos WHERE app_id = $1",
                &[&app_id_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map(|r| r.get(0)))
    }

    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
        let client = self.db.conn().await?;
        let rows = client
            .query("SELECT app_id, info_json FROM app_infos", &[])
            .await
            .map_err(pg_err)?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO workflow_sub_invocations (workflow_id, sub_invocation_id)
                 VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&workflow_id.as_str(), &sub_inv_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT sub_invocation_id FROM workflow_sub_invocations WHERE workflow_id = $1",
                &[&workflow_id.as_str()],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_all_workflow_runs(&self) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT workflow_id, workflow_type, parent_workflow_id, depth FROM workflow_runs",
                &[],
            )
            .await
            .map_err(pg_err)?;
        rows.iter()
            .map(|r| {
                let task_id = r.get::<_, String>(1).parse::<TaskId>().map_err(|e| {
                    RustvelloError::state_backend(format!(
                        "invalid workflow task_id in database: {e}"
                    ))
                })?;
                Ok(WorkflowIdentity {
                    workflow_id: InvocationId::from_string(r.get::<_, String>(0)),
                    workflow_type: task_id,
                    parent_id: r.get::<_, Option<String>>(2).map(InvocationId::from_string),
                    depth: u32::try_from(r.get::<_, i32>(3)).unwrap_or(0),
                })
            })
            .collect()
    }
}

#[async_trait]
impl StateBackendRunner for PostgresStateBackend {
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()> {
        let client = self.db.conn().await?;
        client
            .execute(
                "INSERT INTO runner_contexts
                 (runner_id, runner_cls, runner_language, executor_kind, pid, hostname, thread_id, started_at,
                  parent_runner_id, parent_runner_cls)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (runner_id) DO UPDATE SET
                    runner_cls = $2, runner_language = $3, executor_kind = $4, pid = $5, hostname = $6,
                    thread_id = $7, started_at = $8, parent_runner_id = $9,
                    parent_runner_cls = $10",
                &[
                    &context.runner_id,
                    &context.runner_cls,
                    &context.runner_language.as_str(),
                    &context.executor_kind.as_str(),
                    &i32::try_from(context.pid).unwrap_or(0),
                    &context.hostname,
                    &(context.thread_id as i64),
                    &context.started_at,
                    &context.parent_runner_id as &(dyn tokio_postgres::types::ToSql + Sync),
                    &context.parent_runner_cls as &(dyn tokio_postgres::types::ToSql + Sync),
                ],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }

    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>> {
        let client = self.db.conn().await?;
        let runner_id_s = runner_id.to_string();
        let row = client
            .query_opt(
                "SELECT runner_id, runner_cls, runner_language, executor_kind, pid, hostname, thread_id, started_at,
                        parent_runner_id, parent_runner_cls
                 FROM runner_contexts WHERE runner_id = $1",
                &[&runner_id_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(row.map(|r| parse_pg_runner_row(&r)))
    }

    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let client = self.db.conn().await?;
        let parent_id_s = parent_runner_id.to_string();
        let rows = client
            .query(
                "SELECT runner_id, runner_cls, runner_language, executor_kind, pid, hostname, thread_id, started_at,
                        parent_runner_id, parent_runner_cls
                 FROM runner_contexts WHERE parent_runner_id = $1",
                &[&parent_id_s],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows.iter().map(parse_pg_runner_row).collect())
    }

    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let runner_id_s = runner_id.to_string();
        let rows = if limit > 0 {
            client
                .query(
                    "SELECT DISTINCT invocation_id FROM history
                     WHERE runner_id = $1 LIMIT $2 OFFSET $3",
                    &[&runner_id_s, &(limit as i64), &(offset as i64)],
                )
                .await
                .map_err(pg_err)?
        } else {
            client
                .query(
                    "SELECT DISTINCT invocation_id FROM history
                     WHERE runner_id = $1 OFFSET $2",
                    &[&runner_id_s, &(offset as i64)],
                )
                .await
                .map_err(pg_err)?
        };
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize> {
        let client = self.db.conn().await?;
        let runner_id_s = runner_id.to_string();
        let row = client
            .query_one(
                "SELECT COUNT(DISTINCT invocation_id) FROM history WHERE runner_id = $1",
                &[&runner_id_s],
            )
            .await
            .map_err(pg_err)?;
        let count: i64 = row.get(0);
        Ok(count as usize)
    }

    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let client = self.db.conn().await?;
        let rows = if limit > 0 {
            client
                .query(
                    "SELECT invocation_id, status, runner_id, timestamp, message, history_timestamp
                     FROM history
                     WHERE COALESCE(history_timestamp, timestamp) >= $1
                       AND COALESCE(history_timestamp, timestamp) <= $2
                     ORDER BY COALESCE(history_timestamp, timestamp) ASC
                     LIMIT $3 OFFSET $4",
                    &[&start, &end, &(limit as i64), &(offset as i64)],
                )
                .await
                .map_err(pg_err)?
        } else {
            client
                .query(
                    "SELECT invocation_id, status, runner_id, timestamp, message, history_timestamp
                     FROM history
                     WHERE COALESCE(history_timestamp, timestamp) >= $1
                       AND COALESCE(history_timestamp, timestamp) <= $2
                     ORDER BY COALESCE(history_timestamp, timestamp) ASC
                     OFFSET $3",
                    &[&start, &end, &(offset as i64)],
                )
                .await
                .map_err(pg_err)?
        };
        rows.iter()
            .map(|r| {
                let inv_id: String = r.get(0);
                let status_str: String = r.get(1);
                let runner_id: Option<String> = r.get(2);
                let timestamp: chrono::DateTime<Utc> = r.get(3);
                let message: Option<String> = r.get(4);
                let history_timestamp: Option<chrono::DateTime<Utc>> = r.get(5);
                Ok(InvocationHistory {
                    invocation_id: InvocationId::from_string(inv_id),
                    status_record: InvocationStatusRecord {
                        status: parse_status(&status_str)?,
                        runner_id: runner_id.clone().map(RunnerId::from_string),
                        timestamp,
                    },
                    message,
                    runner_id: runner_id.map(RunnerId::from_string),
                    registered_by_inv_id: None,
                    history_timestamp,
                })
            })
            .collect()
    }

    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let client = self.db.conn().await?;
        let pattern = format!("%{partial_id}%");
        let rows = client
            .query(
                "SELECT runner_id, runner_cls, runner_language, executor_kind, pid, hostname, thread_id, started_at,
                        parent_runner_id, parent_runner_cls
                 FROM runner_contexts WHERE runner_id LIKE $1",
                &[&pattern],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows.iter().map(parse_pg_runner_row).collect())
    }
}

fn parse_pg_runner_row(row: &tokio_postgres::Row) -> StoredRunnerContext {
    StoredRunnerContext {
        runner_id: row.get(0),
        runner_cls: row.get(1),
        runner_language: row
            .get::<_, String>(2)
            .parse()
            .unwrap_or(TaskLanguage::Rust),
        executor_kind: row
            .get::<_, String>(3)
            .parse()
            .unwrap_or(ExecutorKind::Tokio),
        pid: u32::try_from(row.get::<_, i32>(4)).unwrap_or(0),
        hostname: row.get(5),
        thread_id: u64::try_from(row.get::<_, i64>(6)).unwrap_or(0),
        started_at: row.get(7),
        parent_runner_id: row.get(8),
        parent_runner_cls: row.get(9),
    }
}
