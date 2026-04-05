use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorQuery;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use crate::db::{blocking, lock_err, sql_err};

use super::SqliteOrchestrator;

#[async_trait]
impl OrchestratorQuery for SqliteOrchestrator {
    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let task_id_str = task_id.to_string();

            let mut stmt = conn
                .prepare("SELECT invocation_id FROM invocations WHERE task_id = ?1")
                .map_err(sql_err)?;

            let ids: Vec<InvocationId> = stmt
                .query_map([&task_id_str], |row| {
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

    async fn get_invocations_by_call(
        &self,
        call_id: &CallId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let call_id = call_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let call_id_str = call_id.to_string();

            let mut stmt = conn
                .prepare("SELECT invocation_id FROM invocations WHERE call_id = ?1")
                .map_err(sql_err)?;

            let ids: Vec<InvocationId> = stmt
                .query_map([&call_id_str], |row| {
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

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let status_str = status.to_string();

            let ids: Vec<InvocationId> = if let Some(tid) = task_id {
                let task_id_str = tid.to_string();
                let mut stmt = conn
                    .prepare(
                        "SELECT invocation_id FROM invocations WHERE status = ?1 AND task_id = ?2",
                    )
                    .map_err(sql_err)?;
                let result: Vec<InvocationId> = stmt
                    .query_map(rusqlite::params![&status_str, &task_id_str], |row| {
                        let id: String = row.get(0)?;
                        Ok(InvocationId::from_string(id))
                    })
                    .map_err(sql_err)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_err)?;
                result
            } else {
                let mut stmt = conn
                    .prepare("SELECT invocation_id FROM invocations WHERE status = ?1")
                    .map_err(sql_err)?;
                let result: Vec<InvocationId> = stmt
                    .query_map([&status_str], |row| {
                        let id: String = row.get(0)?;
                        Ok(InvocationId::from_string(id))
                    })
                    .map_err(sql_err)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_err)?;
                result
            };

            Ok(ids)
        })
        .await
    }

    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        let statuses = statuses.map(<[InvocationStatus]>::to_vec);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;

            let mut sql = String::from("SELECT COUNT(*) FROM status_records sr");
            let mut params: Vec<String> = Vec::new();
            let mut where_clauses = Vec::new();

            if let Some(tid) = task_id {
                sql.push_str(" JOIN invocations inv ON sr.invocation_id = inv.invocation_id");
                where_clauses.push(format!("inv.task_id = ?{}", params.len() + 1));
                params.push(tid.to_string());
            }

            if let Some(ss) = statuses {
                if !ss.is_empty() {
                    let placeholders: Vec<String> = (0..ss.len())
                        .map(|i| format!("?{}", params.len() + i + 1))
                        .collect();
                    where_clauses.push(format!("sr.status IN ({})", placeholders.join(",")));
                    for s in ss {
                        params.push(s.to_string());
                    }
                }
            }

            if !where_clauses.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clauses.join(" AND "));
            }

            let count: usize = conn
                .query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                    row.get(0)
                })
                .map_err(sql_err)?;
            Ok(count)
        })
        .await
    }

    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.cloned();
        let statuses = statuses.map(<[InvocationStatus]>::to_vec);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut sql = String::from("SELECT sr.invocation_id FROM status_records sr");
            let mut params: Vec<String> = Vec::new();
            let mut where_clauses = Vec::new();

            if let Some(tid) = task_id {
                sql.push_str(" JOIN invocations inv ON sr.invocation_id = inv.invocation_id");
                where_clauses.push(format!("inv.task_id = ?{}", params.len() + 1));
                params.push(tid.to_string());
            }

            if let Some(ss) = statuses {
                if !ss.is_empty() {
                    let placeholders: Vec<String> = (0..ss.len())
                        .map(|i| format!("?{}", params.len() + i + 1))
                        .collect();
                    where_clauses.push(format!("sr.status IN ({})", placeholders.join(",")));
                    for s in ss {
                        params.push(s.to_string());
                    }
                }
            }

            if !where_clauses.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&where_clauses.join(" AND "));
            }

            sql.push_str(&format!(
                " LIMIT ?{} OFFSET ?{}",
                params.len() + 1,
                params.len() + 2
            ));
            params.push(limit.to_string());
            params.push(offset.to_string());

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let ids: Vec<InvocationId> = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |row| {
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

    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let mut stmt = conn
                .prepare(
                    "SELECT DISTINCT wf.waited_on_id FROM waiting_for wf
                     JOIN status_records sr ON wf.waited_on_id = sr.invocation_id
                     WHERE sr.status IN ('REGISTERED', 'PENDING', 'RUNNING')
                       AND NOT EXISTS (
                           SELECT 1 FROM waiting_for wf2
                           WHERE wf2.waiter_id = wf.waited_on_id
                       )
                     LIMIT ?1",
                )
                .map_err(sql_err)?;
            let ids: Vec<InvocationId> = stmt
                .query_map([max_num as i64], |row| {
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

    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.clone();
        let cc_args = cc_args.cloned();
        let statuses = statuses.to_vec();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;
            let task_key = task_id.to_string();

            let mut params: Vec<String> = statuses
                .iter()
                .map(std::string::ToString::to_string)
                .collect();
            let status_clause = if statuses.is_empty() {
                String::new()
            } else {
                let placeholders: Vec<String> =
                    (0..statuses.len()).map(|i| format!("?{}", i + 1)).collect();
                format!(" AND i.status IN ({})", placeholders.join(","))
            };

            let sql = match cc_args {
                Some(ref args) => {
                    let pairs = args.cc_arg_pairs();
                    let n_pairs = pairs.len();
                    let task_idx = params.len() + 1;
                    params.push(task_key);
                    let mut pair_conds = Vec::with_capacity(n_pairs);
                    for (k, v) in &pairs {
                        let ki = params.len() + 1;
                        let vi = params.len() + 2;
                        params.push(k.clone());
                        params.push(v.clone());
                        pair_conds.push(format!("(cp.arg_key = ?{ki} AND cp.arg_value = ?{vi})"));
                    }
                    let where_pairs = pair_conds.join(" OR ");
                    format!(
                        "SELECT cp.invocation_id FROM cc_arg_pairs cp
                         JOIN invocations i ON cp.invocation_id = i.invocation_id
                         WHERE cp.task_id = ?{task_idx} AND ({where_pairs}){status_clause}
                         GROUP BY cp.invocation_id
                         HAVING COUNT(*) = {n_pairs}"
                    )
                }
                None => {
                    let task_idx = params.len() + 1;
                    params.push(task_key);
                    if statuses.is_empty() {
                        format!(
                            "SELECT invocation_id FROM invocations
                             WHERE task_id = ?{task_idx}"
                        )
                    } else {
                        format!(
                            "SELECT invocation_id FROM invocations i
                             WHERE i.task_id = ?{task_idx}{status_clause}"
                        )
                    }
                }
            };

            let mut stmt = conn.prepare(&sql).map_err(sql_err)?;
            let ids: Vec<InvocationId> = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |row| {
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
}
