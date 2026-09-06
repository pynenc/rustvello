use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::state_backend::StateBackendQuery;

use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::invocation::WorkflowIdentity;

use crate::db::{blocking, lock_err, sql_err};

use super::SqliteStateBackend;

#[async_trait]
impl StateBackendQuery for SqliteStateBackend {
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let workflow_id = workflow_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT invocation_id FROM invocations WHERE workflow_id = ?1")
                .map_err(sql_err)?;
            let ids: Vec<InvocationId> = stmt
                .query_map([workflow_id.as_str()], |row| {
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

    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let parent_invocation_id = parent_invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT invocation_id FROM invocations WHERE parent_invocation_id = ?1")
                .map_err(sql_err)?;
            let ids: Vec<InvocationId> = stmt
                .query_map([parent_invocation_id.as_str()], |row| {
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

    async fn store_workflow_run(&self, workflow: &WorkflowIdentity) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let workflow = workflow.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO workflow_runs (workflow_id, workflow_type, parent_workflow_id, depth) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    &workflow.workflow_id.as_str(),
                    &workflow.workflow_type.to_string(),
                    &workflow.parent_id.as_ref().map(|id| id.as_str().to_owned()),
                    workflow.depth as i64,
                ],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_all_workflow_types(&self) -> RustvelloResult<Vec<TaskId>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT DISTINCT workflow_type FROM workflow_runs")
                .map_err(sql_err)?;
            let types: Vec<TaskId> = stmt
                .query_map([], |row| {
                    let type_str: String = row.get(0)?;
                    Ok(type_str)
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?
                .into_iter()
                .map(|s| {
                    s.parse::<TaskId>().map_err(|e| {
                        RustvelloError::state_backend(format!("invalid task_id in database: {e}"))
                    })
                })
                .collect::<RustvelloResult<Vec<_>>>()?;
            Ok(types)
        })
        .await
    }

    async fn get_workflow_runs(
        &self,
        workflow_type: &TaskId,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let db = Arc::clone(&self.db);
        let workflow_type = workflow_type.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let type_key = workflow_type.to_string();
            let mut stmt = conn
                .prepare(
                    "SELECT workflow_id, workflow_type, parent_workflow_id, depth FROM workflow_runs WHERE workflow_type = ?1",
                )
                .map_err(sql_err)?;
            let runs: Vec<WorkflowIdentity> = stmt
                .query_map([&type_key], |row| {
                    let wf_id: String = row.get(0)?;
                    let wf_type: String = row.get(1)?;
                    let parent_id: Option<String> = row.get(2)?;
                    let depth: i64 = row.get(3)?;
                    Ok((wf_id, wf_type, parent_id, depth))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?
                .into_iter()
                .map(|(wf_id, wf_type, parent_id, depth)| {
                    let task_id = wf_type.parse::<TaskId>()
                        .map_err(|e| RustvelloError::state_backend(format!("invalid workflow task_id in database: {e}")))?;
                    Ok(WorkflowIdentity {
                        workflow_id: InvocationId::from_string(wf_id),
                        workflow_type: task_id,
                        parent_id: parent_id.map(InvocationId::from_string),
                        depth: u32::try_from(depth).unwrap_or(0),
                    })
                })
                .collect::<RustvelloResult<Vec<_>>>()?;
            Ok(runs)

        })
        .await
    }

    async fn count_workflow_runs(&self, workflow_type: &TaskId) -> RustvelloResult<usize> {
        let db = Arc::clone(&self.db);
        let type_key = workflow_type.to_string();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM workflow_runs WHERE workflow_type = ?1",
                    [&type_key],
                    |row| row.get(0),
                )
                .map_err(sql_err)?;
            Ok(usize::try_from(count).unwrap_or(usize::MAX))
        })
        .await
    }

    async fn get_workflow_runs_paginated(
        &self,
        workflow_type: &TaskId,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let db = Arc::clone(&self.db);
        let type_key = workflow_type.to_string();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT workflow_id, workflow_type, parent_workflow_id, depth
                     FROM workflow_runs WHERE workflow_type = ?1
                     ORDER BY workflow_id DESC LIMIT ?2 OFFSET ?3",
                )
                .map_err(sql_err)?;
            let rows = stmt
                .query_map(
                    rusqlite::params![
                        type_key,
                        i64::try_from(limit).unwrap_or(i64::MAX),
                        i64::try_from(offset).unwrap_or(i64::MAX)
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    },
                )
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            rows.into_iter()
                .map(|(workflow_id, workflow_type, parent_id, depth)| {
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
        })
        .await
    }

    async fn set_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
        value: &str,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let workflow_id = workflow_id.clone();
        let key = key.to_owned();
        let value = value.to_owned();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO workflow_data (workflow_id, data_key, data_value) VALUES (?1, ?2, ?3)",
                rusqlite::params![workflow_id.as_str(), key, value],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>> {
        let db = Arc::clone(&self.db);
        let workflow_id = workflow_id.clone();
        let key = key.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let result: Option<String> = conn
                .query_row(
                    "SELECT data_value FROM workflow_data WHERE workflow_id = ?1 AND data_key = ?2",
                    rusqlite::params![workflow_id.as_str(), key],
                    |row| row.get(0),
                )
                .ok();
            Ok(result)
        })
        .await
    }

    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let app_id = app_id.to_owned();
        let info_json = info_json.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR REPLACE INTO app_infos (app_id, info_json) VALUES (?1, ?2)",
                rusqlite::params![app_id, info_json],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }

    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>> {
        let db = Arc::clone(&self.db);
        let app_id = app_id.to_owned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let result: Option<String> = conn
                .query_row(
                    "SELECT info_json FROM app_infos WHERE app_id = ?1",
                    rusqlite::params![app_id],
                    |row| row.get(0),
                )
                .ok();
            Ok(result)
        })
        .await
    }

    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare("SELECT app_id, info_json FROM app_infos")
                .map_err(sql_err)?;
            let infos = stmt
                .query_map([], |row| {
                    let app_id: String = row.get(0)?;
                    let info_json: String = row.get(1)?;
                    Ok((app_id, info_json))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?;
            Ok(infos)
        })
        .await
    }

    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let workflow_id = workflow_id.clone();
        let sub_inv_id = sub_inv_id.clone();
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            conn.execute(
                "INSERT OR IGNORE INTO workflow_sub_invocations (workflow_id, sub_invocation_id) VALUES (?1, ?2)",
                rusqlite::params![workflow_id.as_str(), sub_inv_id.as_str()],
            )
            .map_err(sql_err)?;
            Ok(())

        })
        .await
    }

    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let workflow_id = workflow_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT sub_invocation_id FROM workflow_sub_invocations WHERE workflow_id = ?1",
                )
                .map_err(sql_err)?;
            let ids = stmt
                .query_map([workflow_id.as_str()], |row| {
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

    async fn get_all_workflow_runs(&self) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let db = Arc::clone(&self.db);
        blocking(move || {

            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT workflow_id, workflow_type, parent_workflow_id, depth FROM workflow_runs",
                )
                .map_err(sql_err)?;
            let runs = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .map_err(sql_err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(sql_err)?
                .into_iter()
                .map(|(wf_id, wf_type, parent_id, depth)| {
                    let task_id = wf_type.parse::<TaskId>()
                        .map_err(|e| RustvelloError::state_backend(format!("invalid workflow task_id in database: {e}")))?;
                    Ok(WorkflowIdentity {
                        workflow_id: InvocationId::from_string(wf_id),
                        workflow_type: task_id,
                        parent_id: parent_id.map(InvocationId::from_string),
                        depth: u32::try_from(depth).unwrap_or(0),
                    })
                })
                .collect::<RustvelloResult<Vec<_>>>()?;
            Ok(runs)

        })
        .await
    }
}
