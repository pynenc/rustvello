use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};

use crate::connection::{redis_err, scan_keys, RedisPool};

const MAX_SEQUENCE: u64 = i64::MAX as u64;

/// Redis broker backed by one priority sorted set per logical queue.
#[non_exhaustive]
pub struct RedisBroker {
    pool: Arc<RedisPool>,
    queue_prefix: String,
    metadata_prefix: String,
    sequence_key: String,
    queued_by_inv_key: String,
    leases_key: String,
    lease_payload_key: String,
    leased_by_inv_key: String,
    count_key: String,
}

impl RedisBroker {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let prefix = pool.prefix();
        Self {
            queue_prefix: format!("{prefix}broker:queue:"),
            metadata_prefix: format!("{prefix}broker:metadata:"),
            sequence_key: format!("{prefix}broker:sequence"),
            queued_by_inv_key: format!("{prefix}broker:queued_by_inv"),
            leases_key: format!("{prefix}broker:leases"),
            lease_payload_key: format!("{prefix}broker:lease_payload"),
            leased_by_inv_key: format!("{prefix}broker:leased_by_inv"),
            count_key: format!("{prefix}broker:count"),
            pool,
        }
    }

    fn queue_key(&self, queue_name: &str) -> String {
        format!("{}{queue_name}", self.queue_prefix)
    }

    fn metadata_key(&self, queue_name: &str) -> String {
        format!("{}{queue_name}", self.metadata_prefix)
    }

    fn queue_member(sequence: u64, invocation_id: &InvocationId) -> RustvelloResult<String> {
        if sequence > MAX_SEQUENCE {
            return Err(RustvelloError::broker_err(
                "Redis broker sequence exhausted",
            ));
        }
        Ok(format!(
            "{:019}:{}",
            MAX_SEQUENCE - sequence,
            invocation_id.as_str()
        ))
    }

    fn invocation_id_from_member(member: &str) -> RustvelloResult<InvocationId> {
        let (_, invocation_id) = member.split_once(':').ok_or_else(|| {
            RustvelloError::broker_err(format!("invalid Redis broker member: {member}"))
        })?;
        Ok(InvocationId::from_string(invocation_id.to_owned()))
    }

    async fn pop_matching(
        &self,
        queue_name: &str,
        mode: &str,
        value: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let script = redis::Script::new(
            r#"
            local expired = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', ARGV[3])
            for _, leased_member in ipairs(expired) do
                local payload = redis.call('HGET', KEYS[4], leased_member)
                if payload then
                    local fields = {}
                    for field in string.gmatch(payload, '([^\31]+)') do table.insert(fields, field) end
                    if #fields == 5 and redis.call('HGET', KEYS[5], fields[1]) == leased_member then
                        redis.call('ZADD', ARGV[4] .. fields[2], tonumber(fields[5]), fields[3])
                        redis.call('HSET', ARGV[5] .. fields[2], fields[3], fields[4])
                        redis.call('HSET', KEYS[6], fields[1], fields[2] .. '\31' .. fields[3])
                        redis.call('HDEL', KEYS[5], fields[1])
                    end
                end
                redis.call('ZREM', KEYS[3], leased_member)
                redis.call('HDEL', KEYS[4], leased_member)
            end
            local members = redis.call('ZREVRANGE', KEYS[1], 0, -1)
            for _, member in ipairs(members) do
                local task = redis.call('HGET', KEYS[2], member) or ''
                local matches = ARGV[1] == 'all'
                if ARGV[1] == 'task' then
                    matches = task == ARGV[2]
                elseif ARGV[1] == 'language' then
                    if task == '' then
                        matches = ARGV[2] == 'rust'
                    else
                        matches = string.sub(task, 1, string.len(ARGV[2]) + 2) == ARGV[2] .. '::'
                    end
                end
                if matches then
                    local priority = redis.call('ZSCORE', KEYS[1], member) or '0'
                    redis.call('ZREM', KEYS[1], member)
                    redis.call('HDEL', KEYS[2], member)
                    local split = string.find(member, ':', 1, true)
                    local id = string.sub(member, split + 1)
                    redis.call('HDEL', KEYS[6], id)
                    redis.call('ZADD', KEYS[3], tonumber(ARGV[3]) + tonumber(ARGV[6]), member)
                    redis.call('HSET', KEYS[4], member,
                        id .. '\31' .. ARGV[7] .. '\31' .. member .. '\31' .. task .. '\31' .. priority)
                    redis.call('HSET', KEYS[5], id, member)
                    return member
                end
            end
            return nil
            "#,
        );
        let mut conn = self.pool.conn().await?;
        let member: Option<String> = script
            .key(self.queue_key(queue_name))
            .key(self.metadata_key(queue_name))
            .key(&self.leases_key)
            .key(&self.lease_payload_key)
            .key(&self.leased_by_inv_key)
            .key(&self.queued_by_inv_key)
            .arg(mode)
            .arg(value)
            .arg(chrono::Utc::now().timestamp_millis())
            .arg(&self.queue_prefix)
            .arg(&self.metadata_prefix)
            .arg(self.pool.options.delivery_lease_ms)
            .arg(queue_name)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        member
            .as_deref()
            .map(Self::invocation_id_from_member)
            .transpose()
    }

    async fn count_matching(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize> {
        let mut conn = self.pool.conn().await?;
        if task_id.is_none() {
            return conn
                .zcard::<_, usize>(self.queue_key(queue_name))
                .await
                .map_err(redis_err);
        }
        let script = redis::Script::new(
            r#"
            local count = 0
            local members = redis.call('ZRANGE', KEYS[1], 0, -1)
            for _, member in ipairs(members) do
                if redis.call('HGET', KEYS[2], member) == ARGV[1] then
                    count = count + 1
                end
            end
            return count
            "#,
        );
        script
            .key(self.queue_key(queue_name))
            .key(self.metadata_key(queue_name))
            .arg(task_id.expect("checked above").to_string())
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    fn publication_domain(&self) -> Option<rustvello_core::publication::PublicationDomain> {
        Some(Arc::clone(&self.pool.domain))
    }

    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        let mut conn = self.pool.conn().await?;
        let sequence: u64 = conn
            .incr(&self.sequence_key, 1u64)
            .await
            .map_err(redis_err)?;
        let member = Self::queue_member(sequence, invocation_id)?;
        let task_id = task_id.map_or_else(String::new, ToString::to_string);
        let script = redis::Script::new(
            r#"
            local id, queue, member = ARGV[1], ARGV[2], ARGV[3]
            local queued = redis.call('HGET', KEYS[3], id)
            local leased = redis.call('HGET', KEYS[6], id)
            local had = queued or leased
            local count = tonumber(redis.call('GET', KEYS[7]) or '0')
            if not had and count >= tonumber(ARGV[6]) then return 0 end
            if queued then
                local split = string.find(queued, '\31', 1, true)
                local old_queue, old_member = string.sub(queued, 1, split - 1), string.sub(queued, split + 1)
                redis.call('ZREM', ARGV[7] .. old_queue, old_member)
                redis.call('HDEL', ARGV[8] .. old_queue, old_member)
            end
            if leased then
                redis.call('ZREM', KEYS[4], leased)
                redis.call('HDEL', KEYS[5], leased)
                redis.call('HDEL', KEYS[6], id)
            end
            redis.call('ZADD', KEYS[1], ARGV[5], member)
            redis.call('HSET', KEYS[2], member, ARGV[4])
            redis.call('HSET', KEYS[3], id, queue .. '\31' .. member)
            if not had then redis.call('SET', KEYS[7], count + 1) end
            return 1
            "#,
        );
        let accepted: i32 = script
            .key(self.queue_key(queue_name))
            .key(self.metadata_key(queue_name))
            .key(&self.queued_by_inv_key)
            .key(&self.leases_key)
            .key(&self.lease_payload_key)
            .key(&self.leased_by_inv_key)
            .key(&self.count_key)
            .arg(invocation_id.as_str())
            .arg(queue_name)
            .arg(&member)
            .arg(task_id)
            .arg(priority)
            .arg(self.pool.options.max_queue_rows)
            .arg(&self.queue_prefix)
            .arg(&self.metadata_prefix)
            .invoke_async(&mut conn)
            .await
            .map_err(redis_err)?;
        if accepted == 1 {
            Ok(())
        } else {
            Err(RustvelloError::Configuration {
                message: "Redis queue admission capacity reached".into(),
            })
        }
    }

    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, None, DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        self.route_invocation_with_options(invocation_id, Some(task_id), DEFAULT_QUEUE, 0.0)
            .await
    }

    async fn retrieve_invocation_from_queue(
        &self,
        queue_name: &str,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        match task_id {
            Some(task_id) => {
                self.pop_matching(queue_name, "task", &task_id.to_string())
                    .await
            }
            None => self.pop_matching(queue_name, "all", "").await,
        }
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_from_queue(DEFAULT_QUEUE, task_id)
            .await
    }

    async fn retrieve_invocation_for_language_from_queue(
        &self,
        language: TaskLanguage,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        self.pop_matching(queue_name, "language", language.as_str())
            .await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: TaskLanguage,
    ) -> RustvelloResult<Option<InvocationId>> {
        self.retrieve_invocation_for_language_from_queue(language, DEFAULT_QUEUE)
            .await
    }

    async fn count_invocations_in_queues(
        &self,
        queue_names: &[String],
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<usize> {
        let queues = if queue_names.is_empty() {
            let mut conn = self.pool.conn().await?;
            scan_keys(&mut conn, &format!("{}*", self.queue_prefix))
                .await?
                .into_iter()
                .filter_map(|key| key.strip_prefix(&self.queue_prefix).map(str::to_owned))
                .collect()
        } else {
            queue_names.to_vec()
        };
        let mut total = 0;
        for queue_name in queues {
            validate_routing(&queue_name, 0.0)?;
            total += self.count_matching(&queue_name, task_id).await?;
        }
        Ok(total)
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        let queue_keys = scan_keys(&mut conn, &format!("{}*", self.queue_prefix)).await?;
        if let Some(task_id) = task_id {
            let task_id = task_id.to_string();
            let script = redis::Script::new(
                r#"
                local removed = 0
                local members = redis.call('ZRANGE', KEYS[1], 0, -1)
                for _, member in ipairs(members) do
                    if redis.call('HGET', KEYS[2], member) == ARGV[1] then
                        redis.call('ZREM', KEYS[1], member)
                        redis.call('HDEL', KEYS[2], member)
                        local split = string.find(member, ':', 1, true)
                        redis.call('HDEL', KEYS[3], string.sub(member, split + 1))
                        removed = removed + 1
                    end
                end
                local count = tonumber(redis.call('GET', KEYS[4]) or '0')
                redis.call('SET', KEYS[4], math.max(0, count - removed))
                return removed
                "#,
            );
            for queue_key in queue_keys {
                let queue_name = queue_key
                    .strip_prefix(&self.queue_prefix)
                    .unwrap_or_default();
                let _: i64 = script
                    .key(&queue_key)
                    .key(self.metadata_key(queue_name))
                    .key(&self.queued_by_inv_key)
                    .key(&self.count_key)
                    .arg(&task_id)
                    .invoke_async(&mut conn)
                    .await
                    .map_err(redis_err)?;
            }
            let leased_script = redis::Script::new(
                r#"
                local removed = 0
                local entries = redis.call('HGETALL', KEYS[2])
                for index = 1, #entries, 2 do
                    local member, payload = entries[index], entries[index + 1]
                    local fields = {}
                    for field in string.gmatch(payload, '([^\31]+)') do table.insert(fields, field) end
                    if #fields == 5 and fields[4] == ARGV[1] then
                        redis.call('ZREM', KEYS[1], member)
                        redis.call('HDEL', KEYS[2], member)
                        redis.call('HDEL', KEYS[3], fields[1])
                        removed = removed + 1
                    end
                end
                local count = tonumber(redis.call('GET', KEYS[4]) or '0')
                redis.call('SET', KEYS[4], math.max(0, count - removed))
                return removed
                "#,
            );
            let _: i64 = leased_script
                .key(&self.leases_key)
                .key(&self.lease_payload_key)
                .key(&self.leased_by_inv_key)
                .key(&self.count_key)
                .arg(&task_id)
                .invoke_async(&mut conn)
                .await
                .map_err(redis_err)?;
        } else {
            let mut keys = queue_keys;
            keys.extend(scan_keys(&mut conn, &format!("{}*", self.metadata_prefix)).await?);
            keys.push(self.sequence_key.clone());
            keys.push(self.queued_by_inv_key.clone());
            keys.push(self.leases_key.clone());
            keys.push(self.lease_payload_key.clone());
            keys.push(self.leased_by_inv_key.clone());
            keys.push(self.count_key.clone());
            if !keys.is_empty() {
                conn.del::<_, ()>(keys).await.map_err(redis_err)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Arc<RedisPool> {
        Arc::new(RedisPool::new("redis://localhost/", "test_app").unwrap())
    }

    #[test]
    fn queue_name_format() {
        let broker = RedisBroker::new(test_pool());
        assert_eq!(
            broker.queue_key("payments"),
            "rustvello:test_app:broker:queue:payments"
        );
    }
}
