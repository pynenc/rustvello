use std::collections::VecDeque;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use async_trait::async_trait;
use tracing::instrument;

use rustvello_core::broker::{validate_routing, Broker, DEFAULT_QUEUE};
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId, TaskLanguage};
/// In-memory broker with a global queue and per-task queues.
///
/// Not suitable for production — all data is lost on process exit.
/// Useful for unit tests and local development.
///
/// # Queue semantics
///
/// - [`route_invocation`]: pushes to the global queue (task ID unknown at call site).
/// - [`route_invocation_for_task`]: pushes to a task-specific queue; used by callers
///   that know the task ID (e.g. `RustvelloApp::submit_call`).
/// - [`retrieve_invocation`] with `None`: drains the global queue first, then falls
///   back to any non-empty task queue (round-robin); ensures that invocations routed
///   via the task-aware path are also visible to runners that poll without a filter.
/// - [`retrieve_invocation`] with `Some(task_id)`: drains only the task-specific queue.
///
/// # Notify-based wakeup
///
/// Workers can call [`wait_for_work`] instead of polling with sleep.
/// When new work is routed, one waiting worker is woken via `tokio::sync::Notify`.
pub struct MemBroker {
    queue: Mutex<VecDeque<QueuedInvocation>>,
    /// Notification channel for waking idle workers.
    notify: tokio::sync::Notify,
}

#[derive(Clone)]
struct QueuedInvocation {
    invocation_id: InvocationId,
    task_id: Option<TaskId>,
    queue_name: String,
    priority: f64,
}

impl MemBroker {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            notify: tokio::sync::Notify::new(),
        }
    }
}

impl Default for MemBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Broker for MemBroker {
    #[instrument(skip(self), fields(%invocation_id, queue = queue_name, priority))]
    async fn route_invocation_with_options(
        &self,
        invocation_id: &InvocationId,
        task_id: Option<&TaskId>,
        queue_name: &str,
        priority: f64,
    ) -> RustvelloResult<()> {
        validate_routing(queue_name, priority)?;
        self.queue.lock().await.push_back(QueuedInvocation {
            invocation_id: invocation_id.clone(),
            task_id: task_id.cloned(),
            queue_name: queue_name.to_owned(),
            priority,
        });
        self.notify.notify_one();
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
        let mut queue = self.queue.lock().await;
        let selected = queue
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                item.queue_name == queue_name
                    && task_id.is_none_or(|task_id| item.task_id.as_ref() == Some(task_id))
            })
            .max_by(|(left_index, left), (right_index, right)| {
                left.priority
                    .total_cmp(&right.priority)
                    .then_with(|| right_index.cmp(left_index))
            })
            .map(|(index, _)| index);
        Ok(selected.and_then(|index| queue.remove(index).map(|item| item.invocation_id)))
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
        let mut queue = self.queue.lock().await;
        let selected = queue
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                if item.queue_name != queue_name {
                    return false;
                }
                item.task_id
                    .as_ref()
                    .map_or(language == TaskLanguage::Rust, |task_id| {
                        task_id.language() == language
                    })
            })
            .max_by(|(left_index, left), (right_index, right)| {
                left.priority
                    .total_cmp(&right.priority)
                    .then_with(|| right_index.cmp(left_index))
            })
            .map(|(index, _)| index);
        Ok(selected.and_then(|index| queue.remove(index).map(|item| item.invocation_id)))
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
        for queue_name in queue_names {
            validate_routing(queue_name, 0.0)?;
        }
        let queue = self.queue.lock().await;
        Ok(queue
            .iter()
            .filter(|item| {
                (queue_names.is_empty() || queue_names.contains(&item.queue_name))
                    && task_id.is_none_or(|task_id| item.task_id.as_ref() == Some(task_id))
            })
            .count())
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        self.count_invocations_in_queues(&[], task_id).await
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let mut queue = self.queue.lock().await;
        match task_id {
            Some(task_id) => queue.retain(|item| item.task_id.as_ref() != Some(task_id)),
            None => queue.clear(),
        }
        Ok(())
    }

    /// Zero-cost wait: blocks until new work is routed or cancelled.
    async fn wait_for_work(&self, cancel: &CancellationToken) -> bool {
        tokio::select! {
            _ = cancel.cancelled() => false,
            _ = self.notify.notified() => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustvello_proto::identifiers::TaskLanguage;

    #[tokio::test]
    async fn test_route_and_retrieve() {
        let broker = MemBroker::new();
        let id1 = InvocationId::new();
        let id2 = InvocationId::new();

        broker.route_invocation(&id1).await.unwrap();
        broker.route_invocation(&id2).await.unwrap();

        assert_eq!(broker.count_invocations(None).await.unwrap(), 2);

        let retrieved1 = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(retrieved1, Some(id1));

        let retrieved2 = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(retrieved2, Some(id2));

        let retrieved3 = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(retrieved3, None);
    }

    #[tokio::test]
    async fn test_per_task_routing() {
        let broker = MemBroker::new();
        let task_a = TaskId::new("mod", "task_a");
        let task_b = TaskId::new("mod", "task_b");
        let id_a = InvocationId::new();
        let id_b = InvocationId::new();

        broker
            .route_invocation_for_task(&id_a, &task_a)
            .await
            .unwrap();
        broker
            .route_invocation_for_task(&id_b, &task_b)
            .await
            .unwrap();

        // Per-task retrieval should return only the matching task's invocation
        let got_a = broker.retrieve_invocation(Some(&task_a)).await.unwrap();
        assert_eq!(got_a, Some(id_a));
        // task_b's queue still has one item
        assert_eq!(broker.count_invocations(Some(&task_b)).await.unwrap(), 1);
        // Total = 1 (only task_b remains)
        assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
        // Global retrieve should pick up the task_b item from the task queue fallback
        let got_b = broker.retrieve_invocation(None).await.unwrap();
        assert_eq!(got_b, Some(id_b));
    }

    #[tokio::test]
    async fn test_per_task_purge() {
        let broker = MemBroker::new();
        let task_a = TaskId::new("mod", "task_a");
        let task_b = TaskId::new("mod", "task_b");
        broker
            .route_invocation_for_task(&InvocationId::new(), &task_a)
            .await
            .unwrap();
        broker
            .route_invocation_for_task(&InvocationId::new(), &task_b)
            .await
            .unwrap();

        assert_eq!(broker.count_invocations(None).await.unwrap(), 2);
        broker.purge(Some(&task_a)).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 1);
        assert_eq!(broker.count_invocations(Some(&task_a)).await.unwrap(), 0);
        assert_eq!(broker.count_invocations(Some(&task_b)).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let broker = MemBroker::new();
        broker.route_invocation(&InvocationId::new()).await.unwrap();
        broker.route_invocation(&InvocationId::new()).await.unwrap();

        assert_eq!(broker.count_invocations(None).await.unwrap(), 2);

        broker.purge(None).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_batch_route() {
        let broker = MemBroker::new();
        let ids: Vec<InvocationId> = (0..5).map(|_| InvocationId::new()).collect();

        broker.route_invocations(&ids).await.unwrap();
        assert_eq!(broker.count_invocations(None).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_language_routing_foreign_task() {
        let broker = MemBroker::new();
        let py_task = TaskId::for_language(TaskLanguage::Python, "analytics.tasks", "train");
        let rs_task = TaskId::new("math", "add");
        let py_inv = InvocationId::new();
        let rs_inv = InvocationId::new();

        broker
            .route_invocation_for_task(&py_inv, &py_task)
            .await
            .unwrap();
        broker
            .route_invocation_for_task(&rs_inv, &rs_task)
            .await
            .unwrap();

        // Python worker should get only the python invocation
        let got = broker
            .retrieve_invocation_for_language(TaskLanguage::Python)
            .await
            .unwrap();
        assert_eq!(got, Some(py_inv));

        // Python queue is now empty
        let got = broker
            .retrieve_invocation_for_language(TaskLanguage::Python)
            .await
            .unwrap();
        assert_eq!(got, None);

        // Rust worker should get only the Rust invocation.
        let got = broker
            .retrieve_invocation_for_language(TaskLanguage::Rust)
            .await
            .unwrap();
        assert_eq!(got, Some(rs_inv));
    }

    #[tokio::test]
    async fn test_task_less_routing_defaults_to_rust() {
        let broker = MemBroker::new();
        let inv = InvocationId::new();

        // Route through the legacy API with no task identity.
        broker.route_invocation(&inv).await.unwrap();

        let got = broker
            .retrieve_invocation_for_language(TaskLanguage::Python)
            .await
            .unwrap();
        assert_eq!(got, None);

        let got = broker
            .retrieve_invocation_for_language(TaskLanguage::Rust)
            .await
            .unwrap();
        assert_eq!(got, Some(inv));
    }
}
