use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;

use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{
    ActiveRunnerInfo, AtomicServiceExecution, OrchestratorQuery, OrchestratorRecovery,
};
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::InvocationStatus;

use super::{deserialize_status_record, prefixed_key, RedisOrchestrator};
use crate::connection::{redis_err, scan_keys};

#[async_trait]
impl OrchestratorRecovery for RedisOrchestrator {
    async fn register_heartbeat(
        &self,
        runner_id: &RunnerId,
        _can_run_atomic_service: bool,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let now = Utc::now().to_rfc3339();
        conn.set::<_, _, ()>(
            prefixed_key(&self.heartbeat_prefix, runner_id.as_str()),
            &now,
        )
        .await
        .map_err(redis_err)
    }

    async fn get_stale_pending_invocations(
        &self,
        max_pending_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(max_pending_seconds).unwrap_or(i64::MAX));

        let candidates = self
            .get_invocations_by_status(InvocationStatus::Pending, None)
            .await?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-fetch all status records with MGET
        let mut conn = self.pool.conn().await?;
        let status_keys: Vec<String> = candidates.iter().map(|id| self.status_key(id)).collect();
        let status_values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&status_keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut stale = Vec::new();
        for (inv_id, val) in candidates.into_iter().zip(status_values) {
            if let Some(s) = val {
                if let Ok(record) = deserialize_status_record(&s) {
                    if record.timestamp < threshold {
                        stale.push(inv_id);
                    }
                }
            }
        }
        Ok(stale)
    }

    async fn get_stale_running_invocations(
        &self,
        runner_dead_after_seconds: u64,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(
                i64::try_from(runner_dead_after_seconds).unwrap_or(i64::MAX),
            );

        let candidates = self
            .get_invocations_by_status(InvocationStatus::Running, None)
            .await?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-fetch all status records with MGET
        let mut conn = self.pool.conn().await?;
        let status_keys: Vec<String> = candidates.iter().map(|id| self.status_key(id)).collect();
        let status_values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&status_keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        // Collect (inv_id, runner_id) pairs for running invocations
        let mut runner_pairs: Vec<(InvocationId, String)> = Vec::new();
        for (inv_id, val) in candidates.into_iter().zip(status_values) {
            if let Some(s) = val {
                if let Ok(record) = deserialize_status_record(&s) {
                    if let Some(runner_id) = record.runner_id {
                        runner_pairs.push((inv_id, runner_id.to_string()));
                    }
                }
            }
        }

        if runner_pairs.is_empty() {
            return Ok(Vec::new());
        }

        // Batch-fetch all heartbeats with MGET
        let hb_keys: Vec<String> = runner_pairs
            .iter()
            .map(|(_, rid)| prefixed_key(&self.heartbeat_prefix, rid))
            .collect();
        let hb_values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&hb_keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut stale = Vec::new();
        for ((inv_id, _), hb_val) in runner_pairs.into_iter().zip(hb_values) {
            let is_stale = match hb_val {
                Some(ts) => chrono::DateTime::parse_from_rfc3339(&ts)
                    .map(|dt| dt < threshold)
                    .unwrap_or(true),
                None => true,
            };
            if is_stale {
                stale.push(inv_id);
            }
        }
        Ok(stale)
    }

    async fn get_active_runner_ids(&self, timeout_seconds: u64) -> RustvelloResult<Vec<RunnerId>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.heartbeat_prefix)).await?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let mut result = Vec::new();
        for (key, val) in keys.into_iter().zip(values) {
            if let Some(ts) = val {
                let is_active = chrono::DateTime::parse_from_rfc3339(&ts)
                    .map(|dt| dt >= threshold)
                    .unwrap_or(false);
                if is_active {
                    let runner_id_str = &key[self.heartbeat_prefix.len()..];
                    result.push(RunnerId::from_string(runner_id_str.to_string()));
                }
            }
        }
        Ok(result)
    }

    async fn get_active_runners(
        &self,
        timeout_seconds: u64,
        _can_run_atomic_service: Option<bool>,
    ) -> RustvelloResult<Vec<ActiveRunnerInfo>> {
        let threshold = Utc::now()
            - chrono::Duration::seconds(i64::try_from(timeout_seconds).unwrap_or(i64::MAX));
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.heartbeat_prefix)).await?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let mut result = Vec::new();
        for (key, val) in keys.into_iter().zip(values) {
            if let Some(ts_str) = val {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&ts_str) {
                    let dt_utc: DateTime<Utc> = dt.into();
                    if dt_utc >= threshold {
                        let runner_id_str = &key[self.heartbeat_prefix.len()..];
                        result.push(ActiveRunnerInfo {
                            runner_id: RunnerId::from_string(runner_id_str.to_string()),
                            creation_time: dt_utc,
                            last_heartbeat: dt_utc,
                            can_run_atomic_service: true,
                            last_service_start: None,
                            last_service_end: None,
                        });
                    }
                }
            }
        }
        Ok(result)
    }

    async fn record_atomic_service_execution(
        &self,
        runner_id: &RunnerId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> RustvelloResult<()> {
        let execution = AtomicServiceExecution {
            runner_id: runner_id.to_string(),
            start,
            end,
        };
        let value = serde_json::to_string(&execution).map_err(|e| {
            rustvello_core::error::RustvelloError::Serialization {
                message: format!("atomic service execution: {e}"),
            }
        })?;

        let mut conn = self.pool.conn().await?;
        // The sequence suffix keeps identical executions distinct. The score
        // gives reads deterministic newest-first ordering by execution start.
        let sequence: i64 = conn
            .incr(&self.atomic_timeline_sequence_key, 1i64)
            .await
            .map_err(redis_err)?;
        let member = format!("{sequence}:{value}");
        let score = start.timestamp_millis();

        // ZADD + ZREMRANGEBYRANK is one transaction so concurrent runners
        // cannot leave the bounded timeline over its retention limit.
        redis::pipe()
            .atomic()
            .cmd("ZADD")
            .arg(&self.atomic_timeline_key)
            .arg(score)
            .arg(member)
            .cmd("ZREMRANGEBYRANK")
            .arg(&self.atomic_timeline_key)
            .arg(0)
            .arg(-201)
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)
    }

    async fn get_atomic_service_timeline(&self) -> RustvelloResult<Vec<AtomicServiceExecution>> {
        let mut conn = self.pool.conn().await?;
        let values: Vec<String> = conn
            .zrevrange(&self.atomic_timeline_key, 0, 199)
            .await
            .map_err(redis_err)?;
        values
            .iter()
            .map(|member| {
                let (_, value) = member.split_once(':').ok_or_else(|| {
                    rustvello_core::error::RustvelloError::Serialization {
                        message: "invalid atomic service timeline member".into(),
                    }
                })?;
                serde_json::from_str(value).map_err(|e| {
                    rustvello_core::error::RustvelloError::Serialization {
                        message: format!("atomic service execution: {e}"),
                    }
                })
            })
            .collect()
    }
}
