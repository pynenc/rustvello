use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::connection::{redis_err, scan_keys, RedisPool};

const MAX_SEQUENCE: u64 = i64::MAX as u64;

/// Redis broker backed by one priority sorted set per logical queue.
#[non_exhaustive]
pub struct RedisBroker {
    pool: Arc<RedisPool>,
    queue_prefix: String,
    metadata_prefix: String,
    sequence_key: String,
}

impl RedisBroker {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let prefix = pool.prefix();
        Self {
            queue_prefix: format!("{prefix}broker:queue:"),
            metadata_prefix: format!("{prefix}broker:metadata:"),
            sequence_key: format!("{prefix}broker:sequence"),
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
            local members = redis.call('ZREVRANGE', KEYS[1], 0, -1)
            for _, member in ipairs(members) do
                local task = redis.call('HGET', KEYS[2], member) or ''
                local matches = ARGV[1] == 'all'
                if ARGV[1] == 'task' then
                    matches = task == ARGV[2]
                elseif ARGV[1] == 'language' then
                    if task == '' then
                        matches = true
                    elseif ARGV[2] == '' then
                        matches = string.find(task, '::', 1, true) == nil
                    else
                        matches = string.sub(task, 1, string.len(ARGV[2]) + 2) == ARGV[2] .. '::'
                    end
                end
                if matches then
                    redis.call('ZREM', KEYS[1], member)
                    redis.call('HDEL', KEYS[2], member)
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
            .arg(mode)
            .arg(value)
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
        redis::pipe()
            .atomic()
            .zadd(self.queue_key(queue_name), &member, priority)
            .hset(self.metadata_key(queue_name), &member, task_id)
            .query_async::<()>(&mut conn)
            .await
            .map_err(redis_err)
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
        language: &str,
        queue_name: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        validate_routing(queue_name, 0.0)?;
        self.pop_matching(queue_name, "language", language).await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
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
            let script = redis::Script::new(
                r#"
                local members = redis.call('ZRANGE', KEYS[1], 0, -1)
                for _, member in ipairs(members) do
                    if redis.call('HGET', KEYS[2], member) == ARGV[1] then
                        redis.call('ZREM', KEYS[1], member)
                        redis.call('HDEL', KEYS[2], member)
                    end
                end
                return 1
                "#,
            );
            for queue_key in queue_keys {
                let queue_name = queue_key
                    .strip_prefix(&self.queue_prefix)
                    .unwrap_or_default();
                script
                    .key(&queue_key)
                    .key(self.metadata_key(queue_name))
                    .arg(task_id.to_string())
                    .invoke_async::<()>(&mut conn)
                    .await
                    .map_err(redis_err)?;
            }
        } else {
            let mut keys = queue_keys;
            keys.extend(scan_keys(&mut conn, &format!("{}*", self.metadata_prefix)).await?);
            keys.push(self.sequence_key.clone());
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
