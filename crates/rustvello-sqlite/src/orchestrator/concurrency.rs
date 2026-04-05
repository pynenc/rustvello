use std::sync::Arc;

use async_trait::async_trait;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::ConcurrencyControlType;

use crate::db::{blocking, lock_err, sql_err};

use super::SqliteOrchestrator;

#[async_trait]
impl OrchestratorConcurrency for SqliteOrchestrator {
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        let db = Arc::clone(&self.db);
        let task_id = task_id.clone();
        let task_config = task_config.clone();
        let cc_args = cc_args.cloned();
        blocking(move || {
            if task_config.concurrency_control == ConcurrencyControlType::Unlimited {
                return Ok(true);
            }

            let conn = db.conn.lock().map_err(lock_err)?;
            let task_key = task_id.to_string();

            let count: i64 = match cc_args {
                Some(args) => {
                    // Arg-level CC: per-pair intersection via GROUP BY/HAVING
                    let pairs = args.cc_arg_pairs();
                    let n_pairs = pairs.len();
                    let pair_conds: Vec<String> = (0..pairs.len())
                        .map(|i| {
                            format!(
                                "(cp.arg_key = ?{} AND cp.arg_value = ?{})",
                                i * 2 + 2,
                                i * 2 + 3
                            )
                        })
                        .collect();
                    let where_pairs = pair_conds.join(" OR ");
                    let sql = format!(
                        "SELECT COUNT(*) FROM (
                             SELECT cp.invocation_id FROM cc_arg_pairs cp
                             JOIN invocations i ON cp.invocation_id = i.invocation_id
                             WHERE cp.task_id = ?1 AND ({where_pairs})
                               AND i.status IN ('PENDING', 'RUNNING')
                             GROUP BY cp.invocation_id
                             HAVING COUNT(*) = {n_pairs}
                         )"
                    );
                    let mut params: Vec<String> = vec![task_key];
                    for (k, v) in &pairs {
                        params.push(k.clone());
                        params.push(v.clone());
                    }
                    conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |row| {
                        row.get(0)
                    })
                    .map_err(sql_err)?
                }
                None => {
                    // Task-level CC: count from invocations table directly
                    conn.query_row(
                        "SELECT COUNT(*) FROM invocations
                         WHERE task_id = ?1 AND status IN ('PENDING', 'RUNNING')",
                        rusqlite::params![&task_key],
                        |row| row.get(0),
                    )
                    .map_err(sql_err)?
                }
            };

            let limit = task_config.running_concurrency.unwrap_or(1) as i64;
            Ok(count < limit)
        })
        .await
    }

    async fn index_for_concurrency_control(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        let task_id = task_id.clone();
        let cc_args = cc_args.cloned();
        blocking(move || {

            let Some(args) = cc_args else {
                return Ok(());
            };
            let conn = db.conn.lock().map_err(lock_err)?;
            let task_key = task_id.to_string();
            let pairs = args.cc_arg_pairs();

            for (k, v) in &pairs {
                conn.execute(
                    "INSERT OR REPLACE INTO cc_arg_pairs (invocation_id, task_id, arg_key, arg_value)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![invocation_id.as_str(), &task_key, k, v],
                )
                .map_err(sql_err)?;
            }

            Ok(())

        })
        .await
    }

    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let db = Arc::clone(&self.db);
        let invocation_id = invocation_id.clone();
        blocking(move || {
            let conn = db.conn.lock().map_err(lock_err)?;

            conn.execute(
                "DELETE FROM cc_arg_pairs WHERE invocation_id = ?1",
                [invocation_id.as_str()],
            )
            .map_err(sql_err)?;

            Ok(())
        })
        .await
    }
}
