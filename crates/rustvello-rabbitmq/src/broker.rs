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
        }
    }

    fn global_queue(&self) -> &str {
        &self.cached_global_queue
    }

    fn task_queue(&self, task_id: &TaskId) -> String {
        format!("{}_{}", self.prefix, queue_name_for_task(task_id))
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
        self.publish(&self.task_queue(task_id), invocation_id).await
    }

    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let queue = match task_id {
            Some(tid) => self.task_queue(tid),
            None => self.global_queue().to_owned(),
        };
        self.ensure_queue(&queue).await?;
        let ch = self.conn.channel().await.map_err(broker_err)?;
        // At-most-once semantics: `no_ack: true` means RabbitMQ removes the
        // message on delivery. If the process crashes before processing, the
        // message is permanently lost. This is consistent with the other broker
        // implementations (in-memory, SQLite, Redis) which are also
        // at-most-once. For at-least-once guarantees, switch to manual ack
        // with `no_ack: false` and call `basic_ack` after successful processing.
        let msg = ch
            .basic_get(&queue, BasicGetOptions { no_ack: true })
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

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let queue = match task_id {
            Some(tid) => self.task_queue(tid),
            None => self.global_queue().to_owned(),
        };
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
        Ok(state.message_count() as usize)
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let queue = match task_id {
            Some(tid) => self.task_queue(tid),
            None => self.global_queue().to_owned(),
        };
        self.ensure_queue(&queue).await?;
        let ch = self.conn.channel().await.map_err(broker_err)?;
        ch.queue_purge(&queue, QueuePurgeOptions::default())
            .await
            .map_err(broker_err)?;
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
