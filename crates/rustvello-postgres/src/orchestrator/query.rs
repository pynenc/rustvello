use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorQuery;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::PostgresOrchestrator;
use crate::db::pg_err;

#[async_trait]
impl OrchestratorQuery for PostgresOrchestrator {
    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let task_id_str = task_id.to_string();

        let rows = client
            .query(
                "SELECT invocation_id FROM invocations WHERE task_id = $1",
                &[&task_id_str],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_invocations_by_call(
        &self,
        call_id: &CallId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let call_id_str = call_id.to_string();

        let rows = client
            .query(
                "SELECT invocation_id FROM invocations WHERE call_id = $1",
                &[&call_id_str],
            )
            .await
            .map_err(pg_err)?;

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let status_str = status.to_string();

        let rows = if let Some(tid) = task_id {
            let task_id_str = tid.to_string();
            client
                .query(
                    "SELECT invocation_id FROM invocations WHERE status = $1 AND task_id = $2",
                    &[&status_str, &task_id_str],
                )
                .await
                .map_err(pg_err)?
        } else {
            client
                .query(
                    "SELECT invocation_id FROM invocations WHERE status = $1",
                    &[&status_str],
                )
                .await
                .map_err(pg_err)?
        };

        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize> {
        let client = self.db.conn().await?;
        let mut query = "SELECT COUNT(*) FROM invocations WHERE 1=1".to_string();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut idx = 1;

        if let Some(tid) = task_id {
            query.push_str(&format!(" AND task_id = ${idx}"));
            params.push(Box::new(tid.to_string()));
            idx += 1;
        }
        if let Some(statuses) = statuses {
            if !statuses.is_empty() {
                let placeholders: Vec<String> = statuses
                    .iter()
                    .map(|s| {
                        let p = format!("${idx}");
                        params.push(Box::new(s.to_string()));
                        idx += 1;
                        p
                    })
                    .collect();
                query.push_str(&format!(" AND status IN ({})", placeholders.join(",")));
            }
        }

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let row = client
            .query_one(&query, &param_refs)
            .await
            .map_err(pg_err)?;
        let count: i64 = row.get(0);
        Ok(count as usize)
    }

    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let mut query = "SELECT invocation_id FROM invocations WHERE 1=1".to_string();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut idx = 1;

        if let Some(tid) = task_id {
            query.push_str(&format!(" AND task_id = ${idx}"));
            params.push(Box::new(tid.to_string()));
            idx += 1;
        }
        if let Some(statuses) = statuses {
            if !statuses.is_empty() {
                let placeholders: Vec<String> = statuses
                    .iter()
                    .map(|s| {
                        let p = format!("${idx}");
                        params.push(Box::new(s.to_string()));
                        idx += 1;
                        p
                    })
                    .collect();
                query.push_str(&format!(" AND status IN ({})", placeholders.join(",")));
            }
        }
        query.push_str(&format!(
            " ORDER BY created_at LIMIT ${idx} OFFSET ${}",
            idx + 1
        ));
        params.push(Box::new(limit as i64));
        params.push(Box::new(offset as i64));

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = client.query(&query, &param_refs).await.map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let rows = client
            .query(
                "SELECT DISTINCT w.waited_on_id FROM waiting_for w
                 JOIN status_records sr ON w.waited_on_id = sr.invocation_id
                 WHERE sr.status IN ('REGISTERED', 'PENDING', 'RUNNING')
                   AND NOT EXISTS (
                       SELECT 1 FROM waiting_for w2
                       WHERE w2.waiter_id = w.waited_on_id
                   )
                 LIMIT $1",
                &[&(max_num as i64)],
            )
            .await
            .map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }

    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        let client = self.db.conn().await?;
        let task_key = task_id.to_string();

        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut idx = 1;

        // Empty statuses means "no filter — return all" (matches mem/sqlite).
        let status_clause = if statuses.is_empty() {
            String::new()
        } else {
            let placeholders: Vec<String> = statuses
                .iter()
                .map(|s| {
                    let p = format!("${idx}");
                    params.push(Box::new(s.to_string()));
                    idx += 1;
                    p
                })
                .collect();
            format!(" AND i.status IN ({})", placeholders.join(","))
        };

        let query = match cc_args {
            Some(args) => {
                let pairs = args.cc_arg_pairs();
                let n_pairs = pairs.len();
                let task_p = format!("${idx}");
                params.push(Box::new(task_key));
                idx += 1;
                let pair_conds: Vec<String> = pairs
                    .iter()
                    .map(|(k, v)| {
                        let kp = format!("${idx}");
                        params.push(Box::new(k.clone()));
                        idx += 1;
                        let vp = format!("${idx}");
                        params.push(Box::new(v.clone()));
                        idx += 1;
                        format!("(cp.arg_key = {kp} AND cp.arg_value = {vp})")
                    })
                    .collect();
                let where_pairs = pair_conds.join(" OR ");
                format!(
                    "SELECT cp.invocation_id FROM cc_arg_pairs cp
                     JOIN invocations i ON cp.invocation_id = i.invocation_id
                     WHERE cp.task_id = {task_p} AND ({where_pairs}){status_clause}
                     GROUP BY cp.invocation_id
                     HAVING COUNT(*) = {n_pairs}"
                )
            }
            None => {
                let task_p = format!("${idx}");
                params.push(Box::new(task_key));
                if statuses.is_empty() {
                    format!(
                        "SELECT invocation_id FROM invocations
                         WHERE task_id = {task_p}"
                    )
                } else {
                    format!(
                        "SELECT i.invocation_id FROM invocations i
                         WHERE i.task_id = {task_p}{status_clause}"
                    )
                }
            }
        };

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| &**p as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        let rows = client.query(&query, &param_refs).await.map_err(pg_err)?;
        Ok(rows
            .iter()
            .map(|r| InvocationId::from_string(r.get::<_, String>(0)))
            .collect())
    }
}
