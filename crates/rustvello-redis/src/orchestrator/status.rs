use async_trait::async_trait;
use chrono::Utc;
use redis::AsyncCommands;

use rustvello_core::error::RustvelloError;
use rustvello_core::error::RustvelloResult;
use rustvello_core::orchestrator::{OrchestratorConcurrency, OrchestratorStatus};
use rustvello_proto::call::CallDTO;
use rustvello_proto::identifiers::{InvocationId, RunnerId};
use rustvello_proto::status::{InvocationStatus, InvocationStatusRecord};

use super::{deserialize_status_record, prefixed_key, serialize_status_record, RedisOrchestrator};
use crate::connection::redis_err;

#[async_trait]
impl OrchestratorStatus for RedisOrchestrator {
    async fn register_invocation(&self, call: &CallDTO) -> RustvelloResult<InvocationId> {
        let inv_id = InvocationId::new();
        let record = InvocationStatusRecord {
            status: InvocationStatus::Registered,
            timestamp: Utc::now(),
            runner_id: None,
        };
        let mut conn = self.pool.conn().await?;

        redis::pipe()
            .atomic()
            .set(self.status_key(&inv_id), serialize_status_record(&record)?)
            .sadd(
                prefixed_key(&self.task_inv_prefix, &call.task_id.to_string()),
                inv_id.as_str(),
            )
            .sadd(
                prefixed_key(&self.call_inv_prefix, &call.call_id.to_string()),
                inv_id.as_str(),
            )
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)?;

        Ok(inv_id)
    }

    async fn get_invocation_status(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(self.status_key(invocation_id))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => deserialize_status_record(&s),
            None => Err(RustvelloError::InvocationNotFound {
                invocation_id: invocation_id.clone(),
            }),
        }
    }

    async fn set_invocation_status(
        &self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        use rustvello_proto::status::status_record_transition;

        let status_key = self.status_key(invocation_id);

        // Lua CAS script: atomically compare-and-set the status record.
        // KEYS[1] = status key
        // ARGV[1] = expected current status name (e.g. "Pending")
        // ARGV[2] = new JSON record to write
        // Returns: 1 = success, 0 = key not found, -1 = status conflict
        let cas_script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if current == false then
                return 0
            end
            local current_status = string.match(current, '^{"status":"([^"]*)"')
            if current_status ~= ARGV[1] then
                return -1
            end
            redis.call('SET', KEYS[1], ARGV[2])
            return 1
            "#,
        );

        // CAS retry loop: read → validate in Rust → atomic write in Lua
        loop {
            let current_record = self.get_invocation_status(invocation_id).await?;
            let expected_status = current_record.status.to_string();

            let new_record = status_record_transition(Some(&current_record), status, runner_id)
                .map_err(|e| {
                    rustvello_core::error::status_machine_error_to_rustvello(
                        e,
                        invocation_id,
                        current_record.status,
                    )
                })?;

            let new_record_json = serialize_status_record(&new_record)?;

            let mut conn = self.pool.conn().await?;
            let result: i32 = cas_script
                .key(&status_key)
                .arg(&expected_status)
                .arg(&new_record_json)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;

            match result {
                1 => return Ok(new_record),
                0 => {
                    return Err(RustvelloError::InvocationNotFound {
                        invocation_id: invocation_id.clone(),
                    })
                }
                _ => continue, // CAS conflict — retry
            }
        }
    }

    async fn register_invocation_with_id(
        &self,
        invocation_id: &InvocationId,
        call: &CallDTO,
        runner_id: Option<&RunnerId>,
    ) -> RustvelloResult<InvocationStatusRecord> {
        let record = InvocationStatusRecord {
            status: InvocationStatus::Registered,
            timestamp: Utc::now(),
            runner_id: runner_id.cloned(),
        };
        let mut conn = self.pool.conn().await?;
        // Only set if not already present (NX = set if not exists)
        // SET ... NX returns OK (Some) when set, nil (None) when key exists.
        // Parse as Option<String> to propagate real connection errors.
        let result: Option<String> = redis::cmd("SET")
            .arg(self.status_key(invocation_id))
            .arg(serialize_status_record(&record)?)
            .arg("NX")
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;
        let was_set = result.is_some();
        if was_set {
            // Index by task and call
            redis::pipe()
                .atomic()
                .sadd(
                    prefixed_key(&self.task_inv_prefix, &call.task_id.to_string()),
                    invocation_id.as_str(),
                )
                .sadd(
                    prefixed_key(&self.call_inv_prefix, &call.call_id.to_string()),
                    invocation_id.as_str(),
                )
                .query_async::<()>(&mut conn)
                .await
                .map_err(redis_err)?;
        }
        Ok(record)
    }

    async fn increment_invocation_retries(
        &self,
        invocation_id: &InvocationId,
    ) -> RustvelloResult<u32> {
        let key = prefixed_key(&self.retries_prefix, invocation_id.as_str());
        let mut conn = self.pool.conn().await?;
        let count: i64 = conn.incr(&key, 1i64).await.map_err(redis_err)?;
        Ok(u32::try_from(count).unwrap_or(0))
    }

    async fn get_invocation_retries(&self, invocation_id: &InvocationId) -> RustvelloResult<u32> {
        let key = prefixed_key(&self.retries_prefix, invocation_id.as_str());
        let mut conn = self.pool.conn().await?;
        let val: Option<i64> = conn.get(&key).await.map_err(redis_err)?;
        Ok(u32::try_from(val.unwrap_or(0)).unwrap_or(0))
    }

    async fn remove_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        // Remove status record
        conn.del::<_, ()>(self.status_key(invocation_id))
            .await
            .map_err(redis_err)?;
        // Remove from CC index
        self.remove_from_concurrency_index(invocation_id).await?;
        // Remove retries
        conn.del::<_, ()>(prefixed_key(&self.retries_prefix, invocation_id.as_str()))
            .await
            .map_err(redis_err)?;
        // Remove waiters (both directions)
        conn.del::<_, ()>(prefixed_key(&self.waiters_prefix, invocation_id.as_str()))
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let patterns = [
            format!("{}*", &self.status_prefix),
            format!("{}*", &self.task_inv_prefix),
            format!("{}*", &self.call_inv_prefix),
            format!("{}*", &self.waiters_prefix),
            format!("{}*", &self.cc_prefix),
            format!("{}*", &self.cc_rev_prefix),
            format!("{}*", &self.heartbeat_prefix),
            format!("{}*", &self.retries_prefix),
        ];
        for pattern in &patterns {
            let keys = crate::connection::scan_keys(&mut conn, pattern).await?;
            if !keys.is_empty() {
                redis::cmd("DEL")
                    .arg(&keys)
                    .query_async::<()>(&mut conn)
                    .await
                    .map_err(redis_err)?;
            }
        }
        Ok(())
    }

    async fn schedule_auto_purge(&self, _invocation_id: &InvocationId) -> RustvelloResult<()> {
        Err(RustvelloError::NotSupported {
            backend: "Redis".into(),
            method: "schedule_auto_purge".into(),
        })
    }

    async fn run_auto_purge(&self, _max_age_secs: u64) -> RustvelloResult<Vec<InvocationId>> {
        Err(RustvelloError::NotSupported {
            backend: "Redis".into(),
            method: "run_auto_purge".into(),
        })
    }
}
