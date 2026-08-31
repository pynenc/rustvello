use std::sync::Arc;

use async_trait::async_trait;
use redis::AsyncCommands;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::connection::{redis_err, scan_keys, RedisPool};

/// Redis-backed broker using RPUSH/LPOP (FIFO) on Redis lists.
#[non_exhaustive]
pub struct RedisBroker {
    pool: Arc<RedisPool>,
    global_queue: String,
    task_prefix: String,
}

impl RedisBroker {
    pub fn new(pool: Arc<RedisPool>) -> Self {
        let p = pool.prefix();
        Self {
            global_queue: format!("{p}broker:global"),
            task_prefix: format!("{p}broker:task:"),
            pool,
        }
    }

    fn task_queue(&self, task_id: &TaskId) -> String {
        format!("{}{}", self.task_prefix, task_id)
    }
}

#[async_trait]
impl Broker for RedisBroker {
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.rpush::<_, _, ()>(&self.global_queue, invocation_id.as_str())
            .await
            .map_err(redis_err)
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        conn.rpush::<_, _, ()>(&self.task_queue(task_id), invocation_id.as_str())
            .await
            .map_err(redis_err)
    }

    async fn route_invocations(&self, ids: &[InvocationId]) -> RustvelloResult<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.pool.conn().await?;
        let values: Vec<&str> = ids.iter().map(InvocationId::as_str).collect();
        conn.rpush::<_, _, ()>(&self.global_queue, values)
            .await
            .map_err(redis_err)
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        let queue = match task_id {
            Some(tid) => self.task_queue(tid),
            None => self.global_queue.clone(),
        };
        let val: Option<String> = conn.lpop(&queue, None).await.map_err(redis_err)?;
        Ok(val.map(InvocationId::from_string))
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let mut conn = self.pool.conn().await?;
        if let Some(value) = conn
            .lpop::<_, Option<String>>(&self.global_queue, None)
            .await
            .map_err(redis_err)?
        {
            return Ok(Some(InvocationId::from_string(value)));
        }

        let pattern = format!("{}*", self.task_prefix);
        let keys = scan_keys(&mut conn, &pattern).await?;
        for key in keys {
            let task = key.strip_prefix(&self.task_prefix).unwrap_or_default();
            let matches = if language.is_empty() {
                !task.contains("::")
            } else {
                task.starts_with(&format!("{language}::"))
            };
            if !matches {
                continue;
            }
            let val: Option<String> = conn.lpop(&key, None).await.map_err(redis_err)?;
            if let Some(v) = val {
                return Ok(Some(InvocationId::from_string(v)));
            }
        }
        Ok(None)
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let mut conn = self.pool.conn().await?;
        if let Some(tid) = task_id {
            return conn
                .llen::<_, usize>(&self.task_queue(tid))
                .await
                .map_err(redis_err);
        }
        let mut total = conn
            .llen::<_, usize>(&self.global_queue)
            .await
            .map_err(redis_err)?;
        for key in scan_keys(&mut conn, &format!("{}*", self.task_prefix)).await? {
            total += conn.llen::<_, usize>(&key).await.map_err(redis_err)?;
        }
        Ok(total)
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let mut conn = self.pool.conn().await?;
        if let Some(tid) = task_id {
            return conn
                .del::<_, ()>(&self.task_queue(tid))
                .await
                .map_err(redis_err);
        }
        conn.del::<_, ()>(&self.global_queue)
            .await
            .map_err(redis_err)?;
        for key in scan_keys(&mut conn, &format!("{}*", self.task_prefix)).await? {
            conn.del::<_, ()>(&key).await.map_err(redis_err)?;
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
    fn global_queue_name() {
        let b = RedisBroker::new(test_pool());
        assert_eq!(b.global_queue, "rustvello:test_app:broker:global");
    }

    #[test]
    fn task_queue_name_format() {
        let b = RedisBroker::new(test_pool());
        let task_id = TaskId::new("my_module", "my_task");
        let queue = b.task_queue(&task_id);
        assert!(queue.starts_with("rustvello:test_app:broker:task:"));
        assert!(queue.contains("my_module"));
        assert!(queue.contains("my_task"));
    }
}
