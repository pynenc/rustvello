use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use tracing;

use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::trigger::{
    ConditionId, EventQuery, EventRecord, TriggerCondition, TriggerDefinitionDTO,
    TriggerDefinitionId, TriggerRunId, TriggerRunQuery, TriggerRunRecord, ValidCondition,
};

use crate::connection::{redis_err, scan_keys, RedisPool};

/// Batch-fetch conditions by ID using MGET instead of N individual GETs.
async fn batch_get_conditions(
    conn: &mut redis::aio::MultiplexedConnection,
    member_ids: &[String],
    cond_prefix: &str,
) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
    if member_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = member_ids
        .iter()
        .map(|id| {
            let mut k = String::with_capacity(cond_prefix.len() + id.len());
            k.push_str(cond_prefix);
            k.push_str(id);
            k
        })
        .collect();
    let values: Vec<Option<String>> = redis::cmd("MGET")
        .arg(&keys)
        .query_async(conn)
        .await
        .map_err(redis_err)?;
    let mut result = Vec::with_capacity(member_ids.len());
    for (cid_str, val) in member_ids.iter().zip(values) {
        if let Some(json) = val {
            match serde_json::from_str::<TriggerCondition>(&json) {
                Ok(cond) => result.push((ConditionId::from(cid_str.clone()), cond)),
                Err(e) => {
                    tracing::warn!("Failed to deserialize condition {}: {}", cid_str, e);
                }
            }
        }
    }
    Ok(result)
}

/// Redis-backed trigger store.
#[non_exhaustive]
pub struct RedisTriggerStore {
    pool: Arc<RedisPool>,
    cond_prefix: String,
    cond_task_prefix: String,
    trigger_prefix: String,
    cond_trigger_prefix: String,
    valid_cond_prefix: String,
    cron_exec_prefix: String,
    run_prefix: String,
    trigger_task_prefix: String,
    cron_index: String,
    event_index_prefix: String,
    event_record_prefix: String,
    trigger_run_record_prefix: String,
}

impl RedisTriggerStore {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let p = pool.prefix();
        Self {
            cond_prefix: format!("{p}trg:cond:"),
            cond_task_prefix: format!("{p}trg:cond_task:"),
            trigger_prefix: format!("{p}trg:def:"),
            cond_trigger_prefix: format!("{p}trg:cond_trg:"),
            valid_cond_prefix: format!("{p}trg:valid:"),
            cron_exec_prefix: format!("{p}trg:cron_exec:"),
            run_prefix: format!("{p}trg:run:"),
            trigger_task_prefix: format!("{p}trg:trg_task:"),
            cron_index: format!("{p}trg:cron_ids"),
            event_index_prefix: format!("{p}trg:event:"),
            event_record_prefix: format!("{p}trg:event_record:"),
            trigger_run_record_prefix: format!("{p}trg:run_record:"),
            pool,
        }
    }
}

#[async_trait]
impl TriggerStore for RedisTriggerStore {
    async fn register_condition(
        &self,
        condition: &TriggerCondition,
    ) -> RustvelloResult<ConditionId> {
        let cond_id = condition.condition_id();
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(condition).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(format!("{}{}", &self.cond_prefix, cond_id.as_str()), &json)
            .await
            .map_err(redis_err)?;

        // Index by task if the condition references one
        for task_id in condition.source_task_ids() {
            conn.sadd::<_, _, ()>(
                format!("{}{}", &self.cond_task_prefix, task_id),
                cond_id.as_str().to_owned(),
            )
            .await
            .map_err(redis_err)?;
        }

        // Secondary index by condition type for efficient lookup
        if matches!(condition, TriggerCondition::Cron(_)) {
            conn.sadd::<_, _, ()>(&self.cron_index, cond_id.as_str().to_owned())
                .await
                .map_err(redis_err)?;
        }
        if let TriggerCondition::Event(ev) = condition {
            conn.sadd::<_, _, ()>(
                format!("{}{}", &self.event_index_prefix, ev.event_code),
                cond_id.as_str().to_owned(),
            )
            .await
            .map_err(redis_err)?;
        }

        Ok(cond_id)
    }

    async fn get_condition(&self, id: &ConditionId) -> RustvelloResult<Option<TriggerCondition>> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(format!("{}{}", &self.cond_prefix, id.as_str()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => {
                let c: TriggerCondition =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }

    async fn get_conditions_for_task(
        &self,
        task_id: &TaskId,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(format!("{}{}", &self.cond_task_prefix, task_id))
            .await
            .map_err(redis_err)?;

        batch_get_conditions(&mut conn, &members, &self.cond_prefix).await
    }

    async fn get_cron_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let mut conn = self.pool.conn().await?;
        // Use secondary index instead of full keyspace SCAN
        let members: Vec<String> = conn.smembers(&self.cron_index).await.map_err(redis_err)?;

        batch_get_conditions(&mut conn, &members, &self.cond_prefix).await
    }

    async fn get_event_conditions(
        &self,
        event_code: &str,
    ) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let mut conn = self.pool.conn().await?;
        // Use per-event-code secondary index instead of full keyspace SCAN
        let members: Vec<String> = conn
            .smembers(format!("{}{}", &self.event_index_prefix, event_code))
            .await
            .map_err(redis_err)?;

        batch_get_conditions(&mut conn, &members, &self.cond_prefix).await
    }

    async fn register_trigger(&self, trigger: &TriggerDefinitionDTO) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(trigger).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(
            format!("{}{}", &self.trigger_prefix, trigger.trigger_id.as_str()),
            &json,
        )
        .await
        .map_err(redis_err)?;

        // Index by condition
        for cid in &trigger.condition_ids {
            conn.sadd::<_, _, ()>(
                format!("{}{}", &self.cond_trigger_prefix, cid.as_str()),
                trigger.trigger_id.as_str().to_owned(),
            )
            .await
            .map_err(redis_err)?;
        }

        // Index by task
        conn.sadd::<_, _, ()>(
            format!("{}{}", &self.trigger_task_prefix, trigger.task_id),
            trigger.trigger_id.as_str().to_owned(),
        )
        .await
        .map_err(redis_err)?;

        Ok(())
    }

    async fn get_trigger(
        &self,
        id: &TriggerDefinitionId,
    ) -> RustvelloResult<Option<TriggerDefinitionDTO>> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(format!("{}{}", &self.trigger_prefix, id.as_str()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => {
                let t: TriggerDefinitionDTO =
                    serde_json::from_str(&s).map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    async fn get_triggers_for_condition(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Vec<TriggerDefinitionDTO>> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(format!("{}{}", &self.cond_trigger_prefix, cond_id.as_str()))
            .await
            .map_err(redis_err)?;

        if members.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<String> = members
            .iter()
            .map(|tid| format!("{}{}", &self.trigger_prefix, tid))
            .collect();
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut result = Vec::new();
        for val in values.into_iter().flatten() {
            let t: TriggerDefinitionDTO =
                serde_json::from_str(&val).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push(t);
        }
        Ok(result)
    }

    async fn remove_triggers_for_task(&self, task_id: &TaskId) -> RustvelloResult<u32> {
        let mut conn = self.pool.conn().await?;
        let members: Vec<String> = conn
            .smembers(format!("{}{}", &self.trigger_task_prefix, task_id))
            .await
            .map_err(redis_err)?;

        let count = u32::try_from(members.len()).unwrap_or(u32::MAX);
        for tid_str in &members {
            // Read trigger data to find its condition_ids so we can clean up
            // the reverse index (&self.cond_trigger_prefix:{cond_id} sets)
            let val: Option<String> = conn
                .get(format!("{}{}", &self.trigger_prefix, tid_str))
                .await
                .map_err(redis_err)?;
            if let Some(json) = val {
                if let Ok(trigger) = serde_json::from_str::<TriggerDefinitionDTO>(&json) {
                    for cid in &trigger.condition_ids {
                        conn.srem::<_, _, ()>(
                            format!("{}{}", &self.cond_trigger_prefix, cid.as_str()),
                            tid_str.as_str(),
                        )
                        .await
                        .map_err(redis_err)?;
                    }
                }
            }
            conn.del::<_, ()>(format!("{}{}", &self.trigger_prefix, tid_str))
                .await
                .map_err(redis_err)?;
        }
        conn.del::<_, ()>(format!("{}{}", &self.trigger_task_prefix, task_id))
            .await
            .map_err(redis_err)?;
        Ok(count)
    }

    async fn record_valid_condition(&self, vc: &ValidCondition) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(vc).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        let key = format!("{}{}", &self.valid_cond_prefix, vc.valid_condition_id);
        conn.set::<_, _, ()>(&key, &json).await.map_err(redis_err)
    }

    async fn get_valid_conditions(&self) -> RustvelloResult<Vec<ValidCondition>> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.valid_cond_prefix)).await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut conn)
            .await
            .map_err(redis_err)?;

        let mut result = Vec::new();
        for val in values.into_iter().flatten() {
            let vc: ValidCondition =
                serde_json::from_str(&val).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            result.push(vc);
        }
        Ok(result)
    }

    async fn clear_valid_conditions(&self, ids: &[String]) -> RustvelloResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.conn().await?;
        let keys: Vec<String> = ids
            .iter()
            .map(|id| format!("{}{}", &self.valid_cond_prefix, id))
            .collect();
        conn.del::<_, ()>(keys).await.map_err(redis_err)
    }

    async fn get_last_cron_execution(
        &self,
        cond_id: &ConditionId,
    ) -> RustvelloResult<Option<DateTime<Utc>>> {
        let mut conn = self.pool.conn().await?;
        let val: Option<String> = conn
            .get(format!("{}{}", &self.cron_exec_prefix, cond_id.as_str()))
            .await
            .map_err(redis_err)?;
        match val {
            Some(s) => {
                let dt = DateTime::parse_from_rfc3339(&s)
                    .map(|d| d.with_timezone(&Utc))
                    .map_err(|e| RustvelloError::Serialization {
                        message: format!("cron timestamp: {}", e),
                    })?;
                Ok(Some(dt))
            }
            None => Ok(None),
        }
    }

    async fn store_cron_execution(
        &self,
        cond_id: &ConditionId,
        time: DateTime<Utc>,
        expected_last: Option<DateTime<Utc>>,
    ) -> RustvelloResult<bool> {
        let key = format!("{}{}", &self.cron_exec_prefix, cond_id.as_str());
        let mut conn = self.pool.conn().await?;

        // Atomic compare-and-swap via Lua script
        let expected_val = match expected_last {
            Some(dt) => dt.to_rfc3339(),
            None => String::new(), // empty string sentinel for "no previous value"
        };
        let new_val = time.to_rfc3339();

        // Lua CAS: if expected is "" (no previous), check key doesn't exist;
        // otherwise check current value matches expected. Set new value on match.
        let script = redis::Script::new(
            r"
            local current = redis.call('GET', KEYS[1])
            local expected = ARGV[1]
            if expected == '' then
                if current == false then
                    redis.call('SET', KEYS[1], ARGV[2])
                    return 1
                else
                    return 0
                end
            else
                if current == expected then
                    redis.call('SET', KEYS[1], ARGV[2])
                    return 1
                else
                    return 0
                end
            end
            ",
        );
        let result: i32 = script
            .key(&key)
            .arg(&expected_val)
            .arg(&new_val)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        Ok(result == 1)
    }

    async fn claim_trigger_run(&self, run_id: &TriggerRunId) -> RustvelloResult<bool> {
        let key = format!("{}{}", &self.run_prefix, run_id.as_str());
        let mut conn = self.pool.conn().await?;
        // SETNX — returns true if the key was set (i.e. we claimed it)
        let set: bool = conn.set_nx(&key, "1").await.map_err(redis_err)?;
        if set {
            // Auto-expire after 1 hour to prevent leaks
            conn.expire::<_, ()>(&key, 3600).await.map_err(redis_err)?;
        }
        Ok(set)
    }

    async fn store_event(&self, event: &EventRecord) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(event).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(
            format!("{}{}", &self.event_record_prefix, event.event_id),
            json,
        )
        .await
        .map_err(redis_err)
    }

    async fn get_event(&self, event_id: &str) -> RustvelloResult<Option<EventRecord>> {
        let mut conn = self.pool.conn().await?;
        let json: Option<String> = conn
            .get(format!("{}{}", &self.event_record_prefix, event_id))
            .await
            .map_err(redis_err)?;
        json.map(|value| {
            serde_json::from_str(&value).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })
        })
        .transpose()
    }

    async fn get_events(&self, query: &EventQuery) -> RustvelloResult<Vec<EventRecord>> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.event_record_prefix)).await?;
        let values: Vec<Option<String>> = if keys.is_empty() {
            Vec::new()
        } else {
            redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?
        };
        let mut events = Vec::new();
        for value in values.into_iter().flatten() {
            let event: EventRecord =
                serde_json::from_str(&value).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            if query
                .event_code
                .as_ref()
                .is_some_and(|code| code != &event.event_code)
                || query
                    .emitted_by_invocation_id
                    .as_ref()
                    .is_some_and(|id| event.emitted_by_invocation_id.as_ref() != Some(id))
                || query.start.is_some_and(|start| event.timestamp < start)
                || query.end.is_some_and(|end| event.timestamp > end)
            {
                continue;
            }
            events.push(event);
        }
        events.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
        if let Some(limit) = query.limit {
            events.truncate(limit);
        }
        Ok(events)
    }

    async fn store_trigger_run(&self, run: &TriggerRunRecord) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let json = serde_json::to_string(run).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })?;
        conn.set::<_, _, ()>(
            format!(
                "{}{}",
                &self.trigger_run_record_prefix,
                run.trigger_run_id.as_str()
            ),
            json,
        )
        .await
        .map_err(redis_err)
    }

    async fn get_trigger_run(
        &self,
        run_id: &TriggerRunId,
    ) -> RustvelloResult<Option<TriggerRunRecord>> {
        let mut conn = self.pool.conn().await?;
        let json: Option<String> = conn
            .get(format!(
                "{}{}",
                &self.trigger_run_record_prefix,
                run_id.as_str()
            ))
            .await
            .map_err(redis_err)?;
        json.map(|value| {
            serde_json::from_str(&value).map_err(|e| RustvelloError::Serialization {
                message: e.to_string(),
            })
        })
        .transpose()
    }

    async fn get_trigger_runs(
        &self,
        query: &TriggerRunQuery,
    ) -> RustvelloResult<Vec<TriggerRunRecord>> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.trigger_run_record_prefix)).await?;
        let values: Vec<Option<String>> = if keys.is_empty() {
            Vec::new()
        } else {
            redis::cmd("MGET")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(redis_err)?
        };
        let mut runs = Vec::new();
        for value in values.into_iter().flatten() {
            let run: TriggerRunRecord =
                serde_json::from_str(&value).map_err(|e| RustvelloError::Serialization {
                    message: e.to_string(),
                })?;
            if query
                .event_id
                .as_ref()
                .is_some_and(|event_id| !run.event_ids().contains(&event_id.as_str()))
                || query
                    .source_invocation_id
                    .as_ref()
                    .is_some_and(|invocation_id| {
                        !run.source_invocation_ids().contains(&invocation_id)
                    })
                || query
                    .triggered_invocation_id
                    .as_ref()
                    .is_some_and(|invocation_id| {
                        run.triggered_invocation_id.as_ref() != Some(invocation_id)
                    })
                || query.start.is_some_and(|start| run.claimed_at < start)
                || query.end.is_some_and(|end| run.claimed_at > end)
            {
                continue;
            }
            runs.push(run);
        }
        runs.sort_by(|left, right| right.claimed_at.cmp(&left.claimed_at));
        if let Some(limit) = query.limit {
            runs.truncate(limit);
        }
        Ok(runs)
    }

    async fn purge(&self) -> RustvelloResult<()> {
        let prefixes = [
            &self.cond_prefix,
            &self.cond_task_prefix,
            &self.trigger_prefix,
            &self.cond_trigger_prefix,
            &self.valid_cond_prefix,
            &self.cron_exec_prefix,
            &self.run_prefix,
            &self.trigger_task_prefix,
            &self.event_index_prefix,
            &self.event_record_prefix,
            &self.trigger_run_record_prefix,
        ];
        let mut conn = self.pool.conn().await?;
        for prefix in prefixes {
            let keys = scan_keys(&mut conn, &format!("{}*", prefix)).await?;
            if !keys.is_empty() {
                conn.del::<_, ()>(keys).await.map_err(redis_err)?;
            }
        }
        // Also delete the singleton cron index key
        conn.del::<_, ()>(&self.cron_index)
            .await
            .map_err(redis_err)?;
        Ok(())
    }

    async fn get_all_conditions(&self) -> RustvelloResult<Vec<(ConditionId, TriggerCondition)>> {
        let mut conn = self.pool.conn().await?;
        let keys = scan_keys(&mut conn, &format!("{}*", &self.cond_prefix)).await?;
        let ids: Vec<String> = keys
            .iter()
            .filter_map(|k| k.strip_prefix(&self.cond_prefix).map(String::from))
            .collect();
        batch_get_conditions(&mut conn, &ids, &self.cond_prefix).await
    }
}
