use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::OrchestratorConcurrency;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::identifiers::{InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::{cc_pair_redis_key, deserialize_status_record, prefixed_key, RedisOrchestrator};
use crate::connection::redis_err;

#[async_trait]
impl OrchestratorConcurrency for RedisOrchestrator {
    /// **Note:** This check-and-decide pattern is inherently subject to TOCTOU
    /// races in multi-process deployments. Two concurrent callers may both read
    /// the same count and both admit a new invocation, briefly exceeding the
    /// concurrency limit. Prefer `try_acquire_concurrency_slot` which is atomic.
    async fn check_running_concurrency(
        &self,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        let task_str = task_id.to_string();
        let mut conn = self.pool.conn().await?;

        // Get candidate invocation IDs via per-pair intersection
        let candidates: Vec<String> = match cc_args {
            Some(args) => {
                let pairs = args.cc_arg_pairs();
                let mut result: Option<std::collections::HashSet<String>> = None;
                for (k, v) in &pairs {
                    let key = cc_pair_redis_key(&self.cc_prefix, &task_str, k, v);
                    let members: Vec<String> = conn.smembers(&key).await.map_err(redis_err)?;
                    let set: std::collections::HashSet<String> = members.into_iter().collect();
                    result = Some(match result {
                        Some(prev) => prev.intersection(&set).cloned().collect(),
                        None => set,
                    });
                    if result
                        .as_ref()
                        .is_some_and(std::collections::HashSet::is_empty)
                    {
                        break;
                    }
                }
                result.map(|s| s.into_iter().collect()).unwrap_or_default()
            }
            None => conn
                .smembers(prefixed_key(&self.task_inv_prefix, &task_str))
                .await
                .map_err(redis_err)?,
        };

        if candidates.is_empty() {
            let limit = task_config.running_concurrency.unwrap_or(1) as usize;
            return Ok(0 < limit);
        }

        // Batch-fetch all status records with MGET
        let mut cmd = redis::cmd("MGET");
        for inv_str in &candidates {
            cmd.arg(prefixed_key(&self.status_prefix, inv_str));
        }
        let values: Vec<Option<String>> = cmd.query_async(&mut conn).await.map_err(redis_err)?;

        // Count only non-terminal (Pending/Running) invocations
        let count = values
            .iter()
            .filter(|val| {
                val.as_deref()
                    .and_then(|s| deserialize_status_record(s).ok())
                    .is_some_and(|record| {
                        matches!(
                            record.status,
                            InvocationStatus::Pending | InvocationStatus::Running
                        )
                    })
            })
            .count();

        let limit = task_config.running_concurrency.unwrap_or(1) as usize;
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
        let task_str = task_id.to_string();
        let pairs = args.cc_arg_pairs();
        let mut conn = self.pool.conn().await?;
        let rev_key = prefixed_key(&self.cc_rev_prefix, invocation_id.as_str());

        for (k, v) in &pairs {
            let redis_key = cc_pair_redis_key(&self.cc_prefix, &task_str, k, v);
            conn.sadd::<_, _, ()>(&redis_key, invocation_id.as_str())
                .await
                .map_err(redis_err)?;
            // Store the redis key in the reverse SET for O(1) removal
            conn.sadd::<_, _, ()>(&rev_key, &redis_key)
                .await
                .map_err(redis_err)?;
        }
        Ok(())
    }

    async fn remove_from_concurrency_index(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let rev_key = prefixed_key(&self.cc_rev_prefix, invocation_id.as_str());
        // Look up ALL CC pair keys from the reverse SET
        let cc_keys: Vec<String> = conn.smembers(&rev_key).await.map_err(redis_err)?;
        for key in &cc_keys {
            conn.srem::<_, _, ()>(key, invocation_id.as_str())
                .await
                .map_err(redis_err)?;
        }
        if !cc_keys.is_empty() {
            conn.del::<_, ()>(&rev_key).await.map_err(redis_err)?;
        }
        Ok(())
    }

    /// Atomic check-and-index using a Lua script to prevent TOCTOU races.
    ///
    /// The script reads the CC set members per pair, intersects them,
    /// counts non-terminal invocations, and if under the limit, atomically
    /// adds the new invocation to all pair sets and the reverse mapping.
    async fn try_acquire_concurrency_slot(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
        task_config: &TaskConfig,
        cc_args: Option<&SerializedArguments>,
    ) -> RustvelloResult<bool> {
        if task_config.concurrency_control
            == rustvello_proto::status::ConcurrencyControlType::Unlimited
        {
            self.index_for_concurrency_control(invocation_id, task_id, cc_args)
                .await?;
            return Ok(true);
        }

        let Some(args) = cc_args else {
            return self
                .check_running_concurrency(task_id, task_config, cc_args)
                .await;
        };

        let task_str = task_id.to_string();
        let pairs = args.cc_arg_pairs();
        let rev_key = prefixed_key(&self.cc_rev_prefix, invocation_id.as_str());
        let limit = task_config.running_concurrency.unwrap_or(1) as i64;
        let inv_str = invocation_id.as_str();

        // Build per-pair Redis keys
        let pair_keys: Vec<String> = pairs
            .iter()
            .map(|(k, v)| cc_pair_redis_key(&self.cc_prefix, &task_str, k, v))
            .collect();
        let n_pair_keys = pair_keys.len();

        // Lua script: atomically check all pairs + index
        // KEYS = [pair_key_1, ..., pair_key_N, rev_key]
        // ARGV = [invocation_id, limit, status_prefix, n_pair_keys]
        let script = redis::Script::new(
            r#"
            local n_pairs = tonumber(ARGV[4])
            local inv_id = ARGV[1]
            local limit = tonumber(ARGV[2])
            local status_prefix = ARGV[3]
            local rev_key = KEYS[n_pairs + 1]

            -- Intersect all per-pair sets
            local intersection = nil
            for i = 1, n_pairs do
                local members = redis.call('SMEMBERS', KEYS[i])
                if intersection == nil then
                    intersection = {}
                    for _, m in ipairs(members) do
                        intersection[m] = true
                    end
                else
                    local new_set = {}
                    for _, m in ipairs(members) do
                        if intersection[m] then
                            new_set[m] = true
                        end
                    end
                    intersection = new_set
                end
            end

            -- Every indexed member owns or reserves one execution slot.
            local active = 0
            if intersection then
                for id, _ in pairs(intersection) do
                    active = active + 1
                end
            end

            if active < limit then
                for i = 1, n_pairs do
                    redis.call('SADD', KEYS[i], inv_id)
                    redis.call('SADD', rev_key, KEYS[i])
                end
                return 1
            else
                return 0
            end
            "#,
        );

        let mut conn = self.pool.conn().await?;
        let mut invocation = script.prepare_invoke();
        for pk in &pair_keys {
            invocation.key(pk);
        }
        invocation.key(&rev_key);
        invocation.arg(inv_str);
        invocation.arg(limit);
        invocation.arg(&self.status_prefix);
        invocation.arg(n_pair_keys as i64);

        let result: i32 = invocation
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(result == 1)
    }
}
