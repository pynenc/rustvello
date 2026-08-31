use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::ConcurrencyControlType;

use super::PostgresOrchestrator;
use crate::db::pg_err;

#[async_trait]
impl OrchestratorConcurrency for PostgresOrchestrator {
    /// **Note:** This check-and-decide pattern is inherently subject to
    /// TOCTOU races in multi-node PostgreSQL deployments. Two concurrent
    /// callers may both read the same count and both admit a new invocation,
    /// briefly exceeding the concurrency limit. An advisory lock or
    /// strict enforcement. Runners use `try_acquire_concurrency_slot`, which
    /// serializes reservations with a transaction-scoped advisory lock.
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
            return Ok(true);
        }

        let client = self.db.conn().await?;
        let task_key = task_id.to_string();

        let count: i64 = match cc_args {
            Some(args) => {
                let pairs = args.cc_arg_pairs();
                let n_pairs = pairs.len();
                let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
                    Vec::new();
                let mut idx = 1;
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
                let sql = format!(
                    "SELECT COUNT(*) FROM (
                         SELECT cp.invocation_id FROM cc_arg_pairs cp
                         JOIN invocations i ON cp.invocation_id = i.invocation_id
                         WHERE cp.task_id = {task_p} AND ({where_pairs})
                           AND i.status IN ('PENDING', 'RUNNING')
                         GROUP BY cp.invocation_id
                         HAVING COUNT(*) = {n_pairs}
                     ) sub"
                );
                let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    params.iter().map(|p| &**p as _).collect();
                let row = client.query_one(&sql, &param_refs).await.map_err(pg_err)?;
                row.get(0)
            }
            None => {
                let row = client
                    .query_one(
                        "SELECT COUNT(*) FROM invocations
                         WHERE task_id = $1 AND status IN ('PENDING', 'RUNNING')",
                        &[&task_key],
                    )
                    .await
                    .map_err(pg_err)?;
                row.get(0)
            }
        };

        let limit = task_config.running_concurrency.unwrap_or(1) as i64;
        Ok(count < limit)
    }

    async fn index_for_concurrency_control(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<()> {
        let Some(args) = cc_args else {
            return Ok(());
        };
        let client = self.db.conn().await?;
        let task_key = task_id.to_string();
        let pairs = args.cc_arg_pairs();
        let inv_str = invocation_id.as_str();

        for (k, v) in &pairs {
            client
                .execute(
                    "INSERT INTO cc_arg_pairs (invocation_id, task_id, arg_key, arg_value)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (invocation_id, arg_key, arg_value) DO NOTHING",
                    &[&inv_str, &task_key, k, v],
                )
                .await
                .map_err(pg_err)?;
        }

        Ok(())
    }

    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let client = self.db.conn().await?;

        client
            .execute(
                "DELETE FROM cc_arg_pairs WHERE invocation_id = $1",
                &[&invocation_id.as_str()],
            )
            .await
            .map_err(pg_err)?;

        Ok(())
    }

    /// Atomic check-and-index via a single INSERT … SELECT … WHERE count < limit.
    ///
    /// For per-pair CC, indexes each arg pair atomically. Checks all pairs
    /// collectively (GROUP BY/HAVING intersection) before allowing the insert.
    async fn try_acquire_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
            self.index_for_concurrency_control(invocation_id, task_id, cc_args)
                .await?;
            return Ok(true);
        }

        let Some(args) = cc_args else {
            // Task-level CC: no per-pair index, just check invocations directly
            return self
                .check_running_concurrency(task_id, task_config, cc_args)
                .await;
        };

        let mut client = self.db.conn().await?;
        let tx = client.transaction().await.map_err(pg_err)?;
        let task_key = task_id.to_string();
        let pairs = args.cc_arg_pairs();
        let n_pairs = pairs.len();
        let limit = task_config.running_concurrency.unwrap_or(1) as i64;

        // Serialize reservations for a task. The pair-level predicate below
        // still decides whether unrelated argument keys may proceed.
        tx.query_one("SELECT pg_advisory_xact_lock(hashtext($1))", &[&task_key])
            .await
            .map_err(pg_err)?;

        // Build the per-pair count check
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut idx = 1;
        let task_p = format!("${idx}");
        params.push(Box::new(task_key.clone()));
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
        let limit_p = format!("${idx}");
        params.push(Box::new(limit));
        let check_sql = format!(
            "SELECT (SELECT COUNT(*) FROM (
                 SELECT cp.invocation_id FROM cc_arg_pairs cp
                 WHERE cp.task_id = {task_p} AND ({where_pairs})
                 GROUP BY cp.invocation_id
                 HAVING COUNT(*) = {n_pairs}
             ) sub) < {limit_p}"
        );
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| &**p as _).collect();
        let row = tx
            .query_one(&check_sql, &param_refs)
            .await
            .map_err(pg_err)?;
        let allowed: bool = row.get(0);

        if allowed {
            let inv_str = invocation_id.as_str();
            for (k, v) in &pairs {
                tx.execute(
                    "INSERT INTO cc_arg_pairs (invocation_id, task_id, arg_key, arg_value)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (invocation_id, arg_key, arg_value) DO NOTHING",
                    &[&inv_str, &task_key, k, v],
                )
                .await
                .map_err(pg_err)?;
            }
        }

        tx.commit().await.map_err(pg_err)?;
        Ok(allowed)
    }
}
