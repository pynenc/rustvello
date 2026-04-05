use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::state_backend::{
    StateBackendCore, StateBackendQuery, StateBackendRunner, StoredRunnerContext,
};
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::invocation::{InvocationDTO, InvocationHistory, WorkflowIdentity};

use crate::connection::{redis_err, scan_keys, RedisPool};
use rustvello_core::error::TaskError;

fn prefixed_key(prefix: &str, suffix: &str) -> String {
    let mut s = String::with_capacity(prefix.len() + suffix.len());
    s.push_str(prefix);
    s.push_str(suffix);
    s
}

/// Redis-backed state backend.
///
/// Stores invocations, calls, results, errors, and history as JSON strings
/// in Redis keys with structured prefixes.
#[non_exhaustive]
pub struct RedisStateBackend {
    pool: Arc<RedisPool>,
    inv_prefix: String,
    call_prefix: String,
    result_prefix: String,
    error_prefix: String,
    history_prefix: String,
    wf_prefix: String,
    child_prefix: String,
    wf_types_key: String,
    wf_runs_prefix: String,
    wf_data_prefix: String,
    app_infos_key: String,
    wf_sub_prefix: String,
    runner_prefix: String,
    runner_inv_prefix: String,
    history_ts_key: String,
}

impl RedisStateBackend {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let p = pool.prefix();
        Self {
            inv_prefix: format!("{p}state:inv:"),
            call_prefix: format!("{p}state:call:"),
            result_prefix: format!("{p}state:result:"),
            error_prefix: format!("{p}state:error:"),
            history_prefix: format!("{p}state:history:"),
            wf_prefix: format!("{p}state:wf:"),
            child_prefix: format!("{p}state:child:"),
            wf_types_key: format!("{p}state:wf_types"),
            wf_runs_prefix: format!("{p}state:wf_runs:"),
            wf_data_prefix: format!("{p}state:wf_data:"),
            app_infos_key: format!("{p}state:app_infos"),
            wf_sub_prefix: format!("{p}state:wf_sub:"),
            runner_prefix: format!("{p}state:runner:"),
            runner_inv_prefix: format!("{p}state:runner_inv:"),
            history_ts_key: format!("{p}state:history_ts"),
            pool,
        }
    }
}

#[async_trait]
impl StateBackendCore for RedisStateBackend {
    async fn upsert_invocation(
        &self,
        invocation: &InvocationDTO,
        call: &CallDTO,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let inv_json =
            serde_json::to_string(invocation).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })?;
        let call_json = serde_json::to_string(call).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;

        conn.set::<_, _, ()>(
            prefixed_key(&self.inv_prefix, invocation.invocation_id.as_ref()),
            &inv_json,
        )
        .await
        .map_err(redis_err)?;

        conn.set::<_, _, ()>(
            prefixed_key(&self.call_prefix, &call.call_id.to_string()),
            &call_json,
        )
        .await
        .map_err(redis_err)?;

        // Index by workflow if present
        if let Some(wf) = &invocation.workflow {
            conn.sadd::<_, _, ()>(
                prefixed_key(&self.wf_prefix, wf.workflow_id.as_ref()),
                invocation.invocation_id.as_str(),
            )
            .await
            .map_err(redis_err)?;
        }

        // Index by parent
        if let Some(parent_id) = &invocation.parent_invocation_id {
            conn.sadd::<_, _, ()>(
                prefixed_key(&self.child_prefix, parent_id.as_ref()),
                invocation.invocation_id.as_str(),
            )
            .await
            .map_err(redis_err)?;
        }

        Ok(())
    }

    async fn get_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<InvocationDTO> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(prefixed_key(&self.inv_prefix, invocation_id.as_ref()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            }),
            None => Err(RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            }),
        }
    }

    async fn get_call(&self, call_id: &CallId) -> RustvelloResult<CallDTO> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(prefixed_key(&self.call_prefix, &call_id.to_string()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            }),
            None => Err(RustvelloError::state_backend(format!(
                "call not found: {}",
                call_id
            ))),
        }
    }

    async fn store_result(
        &self,
        invocation_id: &InvocationId,
        result: &str,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.set::<_, _, ()>(
            prefixed_key(&self.result_prefix, invocation_id.as_ref()),
            result,
        )
        .await
        .map_err(redis_err)
    }

    async fn get_result(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<String>> {
        let mut conn = self.pool.conn().await?;
        conn.get(prefixed_key(&self.result_prefix, invocation_id.as_ref()))
            .await
            .map_err(redis_err)
    }

    async fn store_error(
        &self,
        invocation_id: &InvocationId,
        error: &TaskError,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(error).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(
            prefixed_key(&self.error_prefix, invocation_id.as_ref()),
            &json,
        )
        .await
        .map_err(redis_err)
    }

    async fn get_error(&self, invocation_id: &InvocationId) -> RustvelloResult<Option<TaskError>> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(prefixed_key(&self.error_prefix, invocation_id.as_ref()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => {
                let err: TaskError =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(err))
            }
            None => Ok(None),
        }
    }

    async fn add_history(&self, history: &InvocationHistory) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(history).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        // Append to invocation-scoped list
        conn.rpush::<_, _, ()>(
            prefixed_key(&self.history_prefix, history.invocation_id.as_ref()),
            &json,
        )
        .await
        .map_err(redis_err)?;
        // Index by timestamp for time-range queries
        let ts = history
            .history_timestamp
            .unwrap_or(history.status_record.timestamp);
        conn.zadd::<_, _, _, ()>(&self.history_ts_key, &json, ts.timestamp_millis() as f64)
            .await
            .map_err(redis_err)?;
        // Maintain runner → invocation reverse index
        let rid = history
            .runner_id
            .as_ref()
            .or(history.status_record.runner_id.as_ref());
        if let Some(r) = rid {
            conn.sadd::<_, _, ()>(
                prefixed_key(&self.runner_inv_prefix, r.as_str()),
                history.invocation_id.as_str(),
            )
            .await
            .map_err(redis_err)?;
        }
        Ok(())
    }

    async fn get_history(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let mut conn = self.pool.conn().await?;
        let vals: Vec<String> = conn
            .lrange(
                prefixed_key(&self.history_prefix, invocation_id.as_ref()),
                0,
                -1,
            )
            .await
            .map_err(redis_err)?;
        vals.into_iter()
            .map(|s| {
                serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            })
            .collect()
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let prefixes = [
            &self.inv_prefix,
            &self.call_prefix,
            &self.result_prefix,
            &self.error_prefix,
            &self.history_prefix,
            &self.wf_prefix,
            &self.child_prefix,
            &self.wf_runs_prefix,
            &self.wf_data_prefix,
            &self.wf_sub_prefix,
            &self.runner_prefix,
            &self.runner_inv_prefix,
        ];
        let mut conn = self.pool.conn().await?;
        for prefix in prefixes {
            let keys = scan_keys(&mut conn, &format!("{}*", prefix)).await?;
            if !keys.is_empty() {
                conn.del::<_, ()>(keys).await.map_err(redis_err)?;
            }
        }
        for key in [
            &self.wf_types_key,
            &self.app_infos_key,
            &self.history_ts_key,
        ] {
            conn.del::<_, ()>(key).await.map_err(redis_err)?;
        }
        Ok(())
    }
}

#[async_trait]
impl StateBackendQuery for RedisStateBackend {
    async fn get_workflow_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.wf_prefix, workflow_id.as_ref()))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn get_child_invocations(
        &self,
        parent_invocation_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(format!("{}{}", &self.child_prefix, parent_invocation_id))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn store_workflow_run(&self, workflow: &WorkflowIdentity) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let type_key = workflow.workflow_type.to_string();
        // Track the workflow type
        conn.sadd::<_, _, ()>(&self.wf_types_key, &type_key)
            .await
            .map_err(redis_err)?;
        // Store the run under its type (keyed by workflow_id to avoid dupes)
        let json = serde_json::to_string(workflow).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.hset::<_, _, _, ()>(
            prefixed_key(&self.wf_runs_prefix, &type_key),
            workflow.workflow_id.as_str(),
            &json,
        )
        .await
        .map_err(redis_err)?;
        Ok(())
    }

    async fn get_all_workflow_types(&self) -> RustvelloResult<Vec<TaskId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn.smembers(&self.wf_types_key).await.map_err(redis_err)?;
        members
            .into_iter()
            .map(|s| {
                s.parse::<TaskId>()
                    .map_err(|e| RustvelloError::state_backend(format!("invalid task_id: {e}")))
            })
            .collect()
    }

    async fn get_workflow_runs(
        &self,
        workflow_type: &TaskId,
    ) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let mut conn = self.pool.conn().await?;
        let vals: Vec<String> = conn
            .hvals(prefixed_key(
                &self.wf_runs_prefix,
                &workflow_type.to_string(),
            ))
            .await
            .map_err(redis_err)?;
        vals.into_iter()
            .map(|s| {
                serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
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
        let mut conn = self.pool.conn().await?;
        conn.hset::<_, _, _, ()>(
            prefixed_key(&self.wf_data_prefix, workflow_id.as_ref()),
            key,
            value,
        )
        .await
        .map_err(redis_err)?;
        Ok(())
    }

    async fn get_workflow_data(
        &self,
        workflow_id: &InvocationId,
        key: &str,
    ) -> RustvelloResult<Option<String>> {
        let mut conn = self.pool.conn().await?;
        conn.hget(
            prefixed_key(&self.wf_data_prefix, workflow_id.as_ref()),
            key,
        )
        .await
        .map_err(redis_err)
    }

    async fn store_app_info(&self, app_id: &str, info_json: &str) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.hset::<_, _, _, ()>(&self.app_infos_key, app_id, info_json)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_app_info(&self, app_id: &str) -> RustvelloResult<Option<String>> {
        let mut conn = self.pool.conn().await?;
        conn.hget(&self.app_infos_key, app_id)
            .await
            .map_err(redis_err)
    }

    async fn get_all_app_infos(&self) -> RustvelloResult<Vec<(String, String)>> {
        let mut conn = self.pool.conn().await?;
        let map: Vec<(String, String)> =
            conn.hgetall(&self.app_infos_key).await.map_err(redis_err)?;
        Ok(map)
    }

    async fn store_workflow_sub_invocation(
        &self,
        workflow_id: &InvocationId,
        sub_inv_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.sadd::<_, _, ()>(
            prefixed_key(&self.wf_sub_prefix, workflow_id.as_ref()),
            sub_inv_id.as_str(),
        )
        .await
        .map_err(redis_err)?;
        Ok(())
    }

    async fn get_workflow_sub_invocations(
        &self,
        workflow_id: &InvocationId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.wf_sub_prefix, workflow_id.as_ref()))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn get_all_workflow_runs(&self) -> RustvelloResult<Vec<WorkflowIdentity>> {
        let mut conn = self.pool.conn().await?;
        let types: Vec<String> = conn.smembers(&self.wf_types_key).await.map_err(redis_err)?;
        let mut all = Vec::new();
        for t in &types {
            let vals: Vec<String> = conn
                .hvals(prefixed_key(&self.wf_runs_prefix, t))
                .await
                .map_err(redis_err)?;
            for s in vals {
                let wf: WorkflowIdentity =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                all.push(wf);
            }
        }
        Ok(all)
    }
}

#[async_trait]
impl StateBackendRunner for RedisStateBackend {
    async fn store_runner_context(&self, context: &StoredRunnerContext) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(context).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(prefixed_key(&self.runner_prefix, &context.runner_id), &json)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_runner_context(
        &self,
        runner_id: &str,
    ) -> RustvelloResult<Option<StoredRunnerContext>> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(prefixed_key(&self.runner_prefix, runner_id))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => {
                let ctx: StoredRunnerContext =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(ctx))
            }
            None => Ok(None),
        }
    }

    async fn get_runner_contexts_by_parent(
        &self,
        parent_runner_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.runner_prefix)).await?;
        let mut result = Vec::new();
        for key in keys {
            let val: Option<String> = conn.get(&key).await.map_err(redis_err)?;
            if let Some(s) = val {
                let ctx: StoredRunnerContext =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                if ctx.parent_runner_id.as_deref() == Some(parent_runner_id) {
                    result.push(ctx);
                }
            }
        }
        Ok(result)
    }

    async fn get_invocation_ids_by_runner(
        &self,
        runner_id: &str,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.runner_inv_prefix, runner_id))
            .await
            .map_err(redis_err)?;
        let iter = members.into_iter().skip(offset);
        let ids: Vec<InvocationId> = if limit > 0 {
            iter.take(limit).map(InvocationId::from_string).collect()
        } else {
            iter.map(InvocationId::from_string).collect()
        };
        Ok(ids)
    }

    async fn count_invocations_by_runner(&self, runner_id: &str) -> RustvelloResult<usize> {
        let mut conn = self.pool.conn().await?;
        let count: usize = conn
            .scard(prefixed_key(&self.runner_inv_prefix, runner_id))
            .await
            .map_err(redis_err)?;
        Ok(count)
    }

    async fn get_history_in_timerange(
        &self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationHistory>> {
        let mut conn = self.pool.conn().await?;
        let min = start.timestamp_millis() as f64;
        let max = end.timestamp_millis() as f64;
        // Fetch all in range, then paginate
        let vals: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&self.history_ts_key)
            .arg(min)
            .arg(max)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let iter = vals.into_iter().skip(offset);
        let selected: Vec<String> = if limit > 0 {
            iter.take(limit).collect()
        } else {
            iter.collect()
        };
        selected
            .into_iter()
            .map(|s| {
                serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })
            })
            .collect()
    }

    async fn get_matching_runner_contexts(
        &self,
        partial_id: &str,
    ) -> RustvelloResult<Vec<StoredRunnerContext>> {
        let mut conn = self.pool.conn().await?;
        let pattern = format!("{}*{}*", &self.runner_prefix, partial_id);
        let keys = scan_keys(&mut conn, &pattern).await?;
        let mut result = Vec::new();
        for key in keys {
            let val: Option<String> = conn.get(&key).await.map_err(redis_err)?;
            if let Some(s) = val {
                let ctx: StoredRunnerContext =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                result.push(ctx);
            }
        }
        Ok(result)
    }
}
