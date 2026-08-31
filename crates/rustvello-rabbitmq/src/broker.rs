use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use lapin::options::{
    BasicGetOptions, BasicPublishOptions, QueueDeclareOptions, QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::BasicProperties;

use rustvello_core::broker::Broker;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::identifiers::{InvocationId, TaskId};

use crate::connection::AmqpConnection;

const GLOBAL_QUEUE: &str = "rustvello_broker_global";

fn queue_name_for_task(task_id: &TaskId) -> String {
    format!("rustvello_broker_{}", task_id)
}

fn broker_err(e: lapin::Error) -> RustvelloError {
    RustvelloError::broker_err(format!("RabbitMQ error: {}", e))
}

/// RabbitMQ-backed broker for Rustvello.
///
/// Uses AMQP queues for invocation routing:
/// - Global queue for task-agnostic routing
/// - Per-task queues for filtered retrieval
#[non_exhaustive]
pub struct RabbitMqBroker {
    conn: AmqpConnection,
    prefix: String,
    /// Cached global queue name (built once at construction)
    cached_global_queue: String,
    task_queues: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl RabbitMqBroker {
    /// Create a new broker connected to the given AMQP URI.
    ///
    /// `prefix` is prepended to queue names to allow namespace isolation
    /// between different applications sharing the same RabbitMQ instance.
    pub fn new(uri: &str, prefix: &str) -> Self {
        let cached_global_queue = format!("{}_{}", prefix, GLOBAL_QUEUE);
        Self {
            conn: AmqpConnection::new(uri),
            prefix: prefix.to_string(),
            cached_global_queue,
            task_queues: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }

    fn global_queue(&self) -> &str {
        &self.cached_global_queue
    }

    fn task_queue(&self, task_id: &TaskId) -> String {
        format!("{}_{}", self.prefix, queue_name_for_task(task_id))
    }

    fn language_queue(&self, language: &str) -> String {
        format!("{}_{}_{}", self.prefix, GLOBAL_QUEUE, language)
    }

    fn queue_for_task(&self, task_id: &TaskId) -> String {
        if task_id.language().is_empty() {
            self.task_queue(task_id)
        } else {
            self.language_queue(task_id.language())
        }
    }

    async fn ensure_queue(&self, queue: &str) -> RustvelloResult<()> {
        let ch = self.conn.channel().await.map_err(broker_err)?;
        ch.queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
            .await
            .map_err(broker_err)?;
        Ok(())
    }

    async fn publish(&self, queue: &str, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.ensure_queue(queue).await?;
        let ch = self.conn.channel().await.map_err(broker_err)?;
        ch.basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            invocation_id.as_str().as_bytes(),
            BasicProperties::default(),
        )
        .await
        .map_err(broker_err)?
        .await
        .map_err(broker_err)?;
        Ok(())
    }

    async fn publish_many(
        &self,
        queue: &str,
        invocation_ids: &[InvocationId],
    ) -> RustvelloResult<()> {
        self.ensure_queue(queue).await?;
        let ch = self.conn.channel().await.map_err(broker_err)?;
        for invocation_id in invocation_ids {
            ch.basic_publish(
                "",
                queue,
                BasicPublishOptions::default(),
                invocation_id.as_str().as_bytes(),
                BasicProperties::default(),
            )
            .await
            .map_err(broker_err)?
            .await
            .map_err(broker_err)?;
        }
        Ok(())
    }

    async fn retrieve_from_queue(&self, queue: &str) -> RustvelloResult<Option<InvocationId>> {
        self.ensure_queue(queue).await?;
        let ch = self.conn.channel().await.map_err(broker_err)?;
        // At-most-once semantics: `no_ack: true` means RabbitMQ removes the
        // message on delivery. This matches the other broker implementations.
        let msg = ch
            .basic_get(queue, BasicGetOptions { no_ack: true })
            .await
            .map_err(broker_err)?;

        match msg {
            Some(delivery) => {
                let id_str = String::from_utf8(delivery.delivery.data).map_err(|e| {
                    RustvelloError::broker_err(format!("non-UTF-8 invocation ID: {}", e))
                })?;
                Ok(Some(InvocationId::from_string(id_str)))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl Broker for RabbitMqBroker {
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        self.publish(self.global_queue(), invocation_id).await
    }

    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let queue = self.queue_for_task(task_id);
        self.task_queues.lock().await.insert(queue.clone());
        self.publish(&queue, invocation_id).await
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let queue = match task_id {
            Some(tid) => self.queue_for_task(tid),
            None => self.global_queue().to_owned(),
        };
        self.retrieve_from_queue(&queue).await
    }

    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        if let Some(invocation_id) = self.retrieve_from_queue(self.global_queue()).await? {
            return Ok(Some(invocation_id));
        }
        if !language.is_empty() {
            return self
                .retrieve_from_queue(&self.language_queue(language))
                .await;
        }
        let language_prefix = format!("{}_{}_", self.prefix, GLOBAL_QUEUE);
        let queues: Vec<String> = self
            .task_queues
            .lock()
            .await
            .iter()
            .filter(|queue| !queue.starts_with(&language_prefix))
            .cloned()
            .collect();
        for queue in queues {
            if let Some(invocation_id) = self.retrieve_from_queue(&queue).await? {
                return Ok(Some(invocation_id));
            }
        }
        Ok(None)
    }

    async fn route_invocations(&self, ids: &[InvocationId]) -> RustvelloResult<()> {
        self.publish_many(self.global_queue(), ids).await
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        if let Some(tid) = task_id {
            let queue = self.queue_for_task(tid);
            self.ensure_queue(&queue).await?;
            let ch = self.conn.channel().await.map_err(broker_err)?;
            let state = ch
                .queue_declare(
                    &queue,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(broker_err)?;
            return Ok(state.message_count() as usize);
        }
        let mut queues = vec![self.global_queue().to_owned()];
        queues.extend(self.task_queues.lock().await.iter().cloned());
        let ch = self.conn.channel().await.map_err(broker_err)?;
        let mut total = 0;
        for queue in queues {
            let state = ch
                .queue_declare(
                    &queue,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(broker_err)?;
            total += state.message_count() as usize;
        }
        Ok(total)
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let ch = self.conn.channel().await.map_err(broker_err)?;
        let queues = match task_id {
            Some(tid) => vec![self.queue_for_task(tid)],
            None => {
                let mut queues = vec![self.global_queue().to_owned()];
                queues.extend(self.task_queues.lock().await.drain());
                queues
            }
        };
        for queue in queues {
            self.ensure_queue(&queue).await?;
            ch.queue_purge(&queue, QueuePurgeOptions::default())
                .await
                .map_err(broker_err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_for_task_includes_task_id() {
        let task_id = TaskId::new("my_module", "my_task");
        let name = queue_name_for_task(&task_id);
        assert!(name.starts_with("rustvello_broker_"));
        assert!(name.contains("my_module"));
        assert!(name.contains("my_task"));
    }

    #[test]
    fn broker_global_queue_uses_prefix() {
        let broker = RabbitMqBroker::new("amqp://localhost", "test_prefix");
        let queue = broker.global_queue();
        assert!(queue.starts_with("test_prefix_"));
        assert!(queue.contains(GLOBAL_QUEUE));
    }

    #[test]
    fn broker_task_queue_uses_prefix() {
        let broker = RabbitMqBroker::new("amqp://localhost", "test_prefix");
        let task_id = TaskId::new("mod", "func");
        let queue = broker.task_queue(&task_id);
        assert!(queue.starts_with("test_prefix_"));
    }

    #[test]
    fn broker_err_maps_to_broker_error() {
        let err = broker_err(lapin::Error::InvalidChannel(0));
        assert!(
            matches!(err, RustvelloError::Infrastructure { .. }),
            "expected Infrastructure, got {:?}",
            err
        );
    }
}
