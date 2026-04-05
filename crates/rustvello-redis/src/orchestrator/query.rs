use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{OrchestratorQuery, OrchestratorStatus};
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{CallId, InvocationId, TaskId};
use rustvello_proto::status::InvocationStatus;

use super::{cc_pair_redis_key, deserialize_status_record, prefixed_key, RedisOrchestrator};
use crate::connection::{redis_err, scan_keys};

#[async_trait]
impl OrchestratorQuery for RedisOrchestrator {
    async fn get_invocations_by_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.task_inv_prefix, &task_id.to_string()))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn get_invocations_by_call(
        &self,
        call_id: &CallId,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(prefixed_key(&self.call_inv_prefix, &call_id.to_string()))
            .await
            .map_err(redis_err)?;
        Ok(members.into_iter().map(InvocationId::from_string).collect())
    }

    async fn get_invocations_by_status(
        &self,
        status: InvocationStatus,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        // Get candidate invocation IDs
        let candidates = match task_id {
            Some(tid) => self.get_invocations_by_task(tid).await?,
            None => {
                // Full scan — get all status keys
                let mut conn = self.pool.conn().await?;
                let keys = scan_keys(&mut conn, &format!("{}*", &self.status_prefix)).await?;
                keys.into_iter()
                    .map(|k| InvocationId::from_string(k[self.status_prefix.len()..].to_string()))
                    .collect()
            }
        };

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-fetch all status records with MGET
        let mut conn = self.pool.conn().await?;
        let keys: Vec<String> = candidates.iter().map(|id| self.status_key(id)).collect();
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut result = Vec::new();
        for (inv_id, val) in candidates.into_iter().zip(values) {
            if let Some(s) = val {
                if let Ok(record) = deserialize_status_record(&s) {
                    if record.status == status {
                        result.push(inv_id);
                    }
                }
            }
        }
        Ok(result)
    }

    async fn count_invocations(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
    ) -> RustvelloResult<usize> {
        let candidates = match task_id {
            Some(tid) => self.get_invocations_by_task(tid).await?,
            None => {
                let mut conn = self.pool.conn().await?;
                let keys = scan_keys(&mut conn, &format!("{}*", &self.status_prefix)).await?;
                keys.into_iter()
                    .map(|k| InvocationId::from_string(k[self.status_prefix.len()..].to_string()))
                    .collect()
            }
        };
        let Some(statuses) = statuses else {
            return Ok(candidates.len());
        };
        if candidates.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.conn().await?;
        let keys: Vec<String> = candidates.iter().map(|id| self.status_key(id)).collect();
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let count = values
            .iter()
            .filter(|v| {
                v.as_deref()
                    .and_then(|s| deserialize_status_record(s).ok())
                    .is_some_and(|r| statuses.contains(&r.status))
            })
            .count();
        Ok(count)
    }

    async fn get_invocation_ids_paginated(
        &self,
        task_id: Option<&TaskId>,
        statuses: Option<&[InvocationStatus]>,
        limit: usize,
        offset: usize,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let candidates = match task_id {
            Some(tid) => self.get_invocations_by_task(tid).await?,
            None => {
                let mut conn = self.pool.conn().await?;
                let keys = scan_keys(&mut conn, &format!("{}*", &self.status_prefix)).await?;
                keys.into_iter()
                    .map(|k| InvocationId::from_string(k[self.status_prefix.len()..].to_string()))
                    .collect()
            }
        };
        let filtered = if let Some(statuses) = statuses {
            let mut conn = self.pool.conn().await?;
            let keys: Vec<String> = candidates.iter().map(|id| self.status_key(id)).collect();
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let values: Vec<Option<String>> = redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?;
            candidates
                .into_iter()
                .zip(values)
                .filter(|(_, v)| {
                    v.as_deref()
                        .and_then(|s| deserialize_status_record(s).ok())
                        .is_some_and(|r| statuses.contains(&r.status))
                })
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        } else {
            candidates
        };
        Ok(filtered.into_iter().skip(offset).take(limit).collect())
    }

    async fn get_blocking_invocations(&self, max_num: usize) -> RustvelloResult<Vec<InvocationId>> {
        // Scan waiter keys, find waited-on invocations in a runnable status
        // that are NOT themselves waiting on another invocation.
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.waiters_prefix)).await?;

        // Collect all IDs that are themselves waiters (appear as members in any waiter set)
        let mut self_waiting: std::collections::HashSet<String> = std::collections::HashSet::new();
        for key in &keys {
            let members: Vec<String> = conn.smembers(key).await.map_err(redis_err)?;
            self_waiting.extend(members);
        }

        let mut result = Vec::new();
        for key in keys {
            if result.len() >= max_num {
                break;
            }
            let inv_id_str = &key[self.waiters_prefix.len()..];
            // Skip invocations that are themselves waiting on something
            if self_waiting.contains(inv_id_str) {
                continue;
            }
            let inv_id = InvocationId::from_string(inv_id_str.to_string());
            if let Ok(record) = self.get_invocation_status(&inv_id).await {
                if matches!(
                    record.status,
                    InvocationStatus::Registered | InvocationStatus::Pending
                ) {
                    result.push(inv_id);
                }
            }
        }
        Ok(result)
    }

    async fn get_existing_invocations(
        &self,
        task_id: &TaskId,
        cc_args: Option<&SerializedArguments>,
        statuses: &[InvocationStatus],
    ) -> RustvelloResult<Vec<InvocationId>> {
        // Empty statuses means "no filter — return all" (matches mem/sqlite).
        let task_str = task_id.to_string();
        let mut conn = self.pool.conn().await?;

        let candidates: Vec<String> = match cc_args {
            Some(args) => {
                // Arg-level CC: per-pair intersection
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
            None => {
                // Task-level CC: all invocations for this task
                conn.smembers(prefixed_key(&self.task_inv_prefix, &task_str))
                    .await
                    .map_err(redis_err)?
            }
        };

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-fetch statuses with MGET
        let mut conn = self.pool.conn().await?;
        let keys: Vec<String> = candidates
            .iter()
            .map(|id| prefixed_key(&self.status_prefix, id))
            .collect();
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let result: Vec<InvocationId> = candidates
            .into_iter()
            .zip(values)
            .filter(|(_, val)| {
                if statuses.is_empty() {
                    // No status filter — accept any candidate that has a record
                    val.is_some()
                } else {
                    val.as_deref()
                        .and_then(|s| deserialize_status_record(s).ok())
                        .is_some_and(|r| statuses.contains(&r.status))
                }
            })
            .map(|(id, _)| InvocationId::from_string(id))
            .collect();

        Ok(result)
    }
}
