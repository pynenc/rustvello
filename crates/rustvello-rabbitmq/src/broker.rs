use std::collections::HashSet;

use async_trait::async_trait;
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicGetOptions, BasicNackOptions, BasicPublishOptions, QueueDeclareOptions,
    QueuePurgeOptions,
};
use lapin::types::{AMQPValue, FieldTable, ShortString};
use lapin::BasicProperties;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_proto::config::{MAX_PRIORITY, MIN_PRIORITY};
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};

use crate::connection::AmqpConnection;

const MAX_RABBITMQ_PRIORITY: u8 = 255;

fn broker_err(error: lapin::Error) -> RustvelloError {
    RustvelloError::broker_err(format!("RabbitMQ error: {error}"))
}

#[derive(Debug, Serialize, Deserialize)]
struct MessageEnvelope {
    invocation_id: String,
    task_id: Option<String>,
    priority: f64,
}

/// RabbitMQ broker using one native priority queue per language and logical queue.
///
/// Pynenc/Rustvello float priorities are normalized into RabbitMQ's 256
/// integer levels. Language routing happens when publishing, so a worker reads
/// directly from its own physical queue without scanning or requeueing work
/// owned by another runtime.
#[non_exhaustive]
pub struct RabbitMqBroker {
    conn: AmqpConnection,
    prefix: String,
    logical_queues: tokio::sync::Mutex<HashSet<String>>,
}

impl RabbitMqBroker {
    pub fn new(uri: &str, prefix: &str) -> Self {
        Self {
            conn: AmqpConnection::new(uri),
            prefix: prefix.to_owned(),
            logical_queues: tokio::sync::Mutex::new(HashSet::from([DEFAULT_QUEUE.to_owned()])),
        }
    }

    fn queue_name(&self, logical_queue: &str, language: TaskLanguage) -> String {
        format!(
            "{}_rustvello_broker_{}_{}",
            self.prefix, language, logical_queue
        )
    }

    fn priority_rank(priority: f64) -> u8 {
        let normalized = (priority - MIN_PRIORITY) / (MAX_PRIORITY - MIN_PRIORITY);
        (normalized * f64::from(MAX_RABBITMQ_PRIORITY)).round() as u8
    }

    async fn ensure_queue(
        &self,
        logical_queue: &str,
        language: TaskLanguage,
    ) -> RustvelloResult<u32> {
        let queue_name = self.queue_name(logical_queue, language);
        let mut arguments = FieldTable::default();
        arguments.insert(
            ShortString::from("x-max-priority"),
            AMQPValue::ShortShortUInt(MAX_RABBITMQ_PRIORITY),
        );
        let channel = self.conn.channel().await.map_err(broker_err)?;
        let state = channel
            .queue_declare(&queue_name, QueueDeclareOptions::default(), arguments)
            .await
            .map_err(broker_err)?;
        self.logical_queues
            .lock()
            .await
            .insert(logical_queue.to_owned());
        Ok(state.message_count())
    }

    async fn requeue(deliveries: Vec<Delivery>) -> RustvelloResult<()> {
        for delivery in deliveries {
            delivery
                .nack(BasicNackOptions {
                    multiple: false,
                    requeue: true,
                })
                .await
                .map_err(broker_err)?;
        }
        Ok(())
    }

    async fn wait_for_requeued_messages(
        &self,
        logical_queue: &str,
        language: TaskLanguage,
        expected_count: u32,
    ) -> RustvelloResult<()> {
        // Basic.Nack has no synchronous server acknowledgement. A short,
        // bounded poll avoids exposing a transient empty queue after a
        // task/language scan has requeued deliveries.
        for _ in 0..20 {
            if self.ensure_queue(logical_queue, language).await? >= expected_count {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    async fn take_matching<F>(
        &self,
        logical_queue: &str,
        language: TaskLanguage,
        matches: F,
    ) -> RustvelloResult<Option<InvocationId>>
    where
        F: Fn(&MessageEnvelope) -> bool + Send,
    {
        let message_count = self.ensure_queue(logical_queue, language).await?;
        let channel = self.conn.channel().await.map_err(broker_err)?;
        let queue_name = self.queue_name(logical_queue, language);
        let mut held = Vec::new();
        for _ in 0..message_count {
            let Some(message) = channel
                .basic_get(&queue_name, BasicGetOptions { no_ack: false })
                .await
                .map_err(broker_err)?
            else {
                break;
            };
            let delivery = message.delivery;
            let envelope: MessageEnvelope = match serde_json::from_slice(&delivery.data) {
                Ok(envelope) => envelope,
                Err(error) => {
                    delivery
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: true,
                        })
                        .await
                        .map_err(broker_err)?;
                    Self::requeue(held).await?;
                    return Err(RustvelloError::broker_err(format!(
                        "invalid RabbitMQ broker envelope: {error}"
                    )));
                }
            };
            if matches(&envelope) {
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .map_err(broker_err)?;
                Self::requeue(held).await?;
                self.wait_for_requeued_messages(logical_queue, language, message_count - 1)
                    .await?;
                return Ok(Some(InvocationId::from_string(envelope.invocation_id)));
            }
            held.push(delivery);
        }
        Self::requeue(held).await?;
        self.wait_for_requeued_messages(logical_queue, language, message_count)
            .await?;
        Ok(None)
    }

    async fn count_matching<F>(
        &self,
        logical_queue: &str,
        language: TaskLanguage,
        matches: F,
    ) -> RustvelloResult<usize>
    where
        F: Fn(&MessageEnvelope) -> bool + Send,
    {
        let message_count = self.ensure_queue(logical_queue, language).await?;
        let channel = self.conn.channel().await.map_err(broker_err)?;
        let queue_name = self.queue_name(logical_queue, language);
        let mut deliveries = Vec::new();
        let mut count = 0;
        for _ in 0..message_count {
            let Some(message) = channel
                .basic_get(&queue_name, BasicGetOptions { no_ack: false })
                .await
                .map_err(broker_err)?
            else {
                break;
            };
            let delivery = message.delivery;
            let envelope: MessageEnvelope = match serde_json::from_slice(&delivery.data) {
                Ok(envelope) => envelope,
                Err(error) => {
                    delivery
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: true,
                        })
                        .await
                        .map_err(broker_err)?;
                    Self::requeue(deliveries).await?;
                    return Err(RustvelloError::broker_err(format!(
                        "invalid RabbitMQ broker envelope: {error}"
                    )));
                }
            };
            count += usize::from(matches(&envelope));
            deliveries.push(delivery);
        }
        Self::requeue(deliveries).await?;
        self.wait_for_requeued_messages(logical_queue, language, message_count)
            .await?;
        Ok(count)
    }
}

#[async_trait]
impl Broker for RabbitMqBroker {
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        // The legacy task-less API predates language-qualified TaskIds. Keep it
        // on the Rust lane; normal application submission always supplies a task.
        let language = task_id.map_or(TaskLanguage::Rust, TaskId::language);
        self.ensure_queue(queue_name, language).await?;
        let envelope = serde_json::to_vec(&MessageEnvelope {
            invocation_id: invocation_id.to_string(),
            task_id: task_id.map(ToString::to_string),
            priority,
        })
        .map_err(|error| RustvelloError::broker_err(error.to_string()))?;
        let channel = self.conn.channel().await.map_err(broker_err)?;
        channel
            .basic_publish(
                "",
                &self.queue_name(queue_name, language),
                BasicPublishOptions::default(),
                &envelope,
                BasicProperties::default().with_priority(Self::priority_rank(priority)),
            )
            .await
            .map_err(broker_err)?
            .await
            .map_err(broker_err)?;
        Ok(())
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
        if let Some(task_id) = task_id {
            let language = task_id.language();
            let task_id = task_id.to_string();
            return self
                .take_matching(queue_name, language, move |envelope| {
                    envelope.task_id.as_ref() == Some(&task_id)
                })
                .await;
        }

        // Language-agnostic administrative retrieval has no cross-language
        // ordering guarantee. Worker paths always use the language API below.
        for language in TaskLanguage::ALL {
            if let Some(invocation_id) = self.take_matching(queue_name, language, |_| true).await? {
                return Ok(Some(invocation_id));
            }
        }
        Ok(None)
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
        self.take_matching(queue_name, language, |_| true).await
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
            self.logical_queues.lock().await.iter().cloned().collect()
        } else {
            queue_names.to_vec()
        };
        let mut total = 0;
        for queue_name in queues {
            validate_routing(&queue_name, 0.0)?;
            if let Some(task_id) = task_id {
                let language = task_id.language();
                let task_id = task_id.to_string();
                total += self
                    .count_matching(&queue_name, language, move |envelope| {
                        envelope.task_id.as_ref() == Some(&task_id)
                    })
                    .await?;
            } else {
                for language in TaskLanguage::ALL {
                    total += self.ensure_queue(&queue_name, language).await? as usize;
                }
            }
        }
        Ok(total)
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let queues: Vec<String> = self.logical_queues.lock().await.iter().cloned().collect();
        if let Some(task_id) = task_id {
            for queue_name in queues {
                while self
                    .retrieve_invocation_from_queue(&queue_name, Some(task_id))
                    .await?
                    .is_some()
                {}
            }
            return Ok(());
        }
        let channel = self.conn.channel().await.map_err(broker_err)?;
        for queue_name in queues {
            for language in TaskLanguage::ALL {
                self.ensure_queue(&queue_name, language).await?;
                channel
                    .queue_purge(
                        &self.queue_name(&queue_name, language),
                        QueuePurgeOptions::default(),
                    )
                    .await
                    .map_err(broker_err)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_normalization_boundaries() {
        assert_eq!(RabbitMqBroker::priority_rank(-100.0), 0);
        assert_eq!(RabbitMqBroker::priority_rank(0.0), 128);
        assert_eq!(RabbitMqBroker::priority_rank(100.0), 255);
    }

    #[test]
    fn physical_queue_includes_language_and_logical_queue() {
        let broker = RabbitMqBroker::new("amqp://localhost", "test");
        assert_eq!(
            broker.queue_name("payments", TaskLanguage::Rust),
            "test_rustvello_broker_rust_payments"
        );
        assert_eq!(
            broker.queue_name("payments", TaskLanguage::Python),
            "test_rustvello_broker_python_payments"
        );
    }
}
