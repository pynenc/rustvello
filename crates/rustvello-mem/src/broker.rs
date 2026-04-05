use std::collections::{BTreeMap, VecDeque};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use async_trait::async_trait;
use tracing::instrument;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::{InvocationId, TaskId};
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
    /// Queues keyed by queue name.
    /// GLOBAL_QUEUE is used for invocations routed without a task_id.
    /// Each TaskId string maps to its own per-task queue.
    queues: Mutex<BTreeMap<String, VecDeque<InvocationId>>>,
    /// Notification channel for waking idle workers.
    notify: tokio::sync::Notify,
}

const GLOBAL_QUEUE: &str = "__global__";

impl MemBroker {
    pub fn new() -> Self {
        Self {
            queues: Mutex::new(BTreeMap::new()),
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
    /// Route to the global queue (task ID unknown at this call site).
    #[instrument(skip(self), fields(%invocation_id))]
    async fn route_invocation(&self, invocation_id: &InvocationId) -> RustvelloResult<()> {
        let mut queues = self.queues.lock().await;
        queues
            .entry(GLOBAL_QUEUE.to_owned())
            .or_default()
            .push_back(invocation_id.clone());
        drop(queues);
        self.notify.notify_one();
        Ok(())
    }

    /// Route to the task-specific queue.
    ///
    /// Callers that know the task ID should prefer this over `route_invocation`
    /// so that `retrieve_invocation(Some(task_id))` can return a filtered result.
    #[instrument(skip(self), fields(%invocation_id, %task_id))]
    async fn route_invocation_for_task(
        &self,
        invocation_id: &InvocationId,
        task_id: &TaskId,
    ) -> RustvelloResult<()> {
        let mut queues = self.queues.lock().await;
        queues
            .entry(task_id.to_string())
            .or_default()
            .push_back(invocation_id.clone());
        drop(queues);
        self.notify.notify_one();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn retrieve_invocation(
        &self,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Option<InvocationId>> {
        let mut queues = self.queues.lock().await;
        if let Some(tid) = task_id {
            // Task-filtered retrieval: pop from the task-specific queue only.
            return Ok(queues
                .get_mut(&tid.to_string())
                .and_then(VecDeque::pop_front));
        }
        // Global retrieval: drain global queue first, then any task queue.
        if let Some(id) = queues.get_mut(GLOBAL_QUEUE).and_then(VecDeque::pop_front) {
            return Ok(Some(id));
        }
        // Fall back to the first non-empty task queue (in iteration order).
        for (key, queue) in queues.iter_mut() {
            if key == GLOBAL_QUEUE {
                continue;
            }
            if let Some(id) = queue.pop_front() {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// Retrieve from queues matching a specific language.
    ///
    /// **Behavior:** First checks the global queue, then per-task queues
    /// whose keys start with `"language::"`. Because the global queue is
    /// checked first, a single-language worker can drain globally-routed
    /// invocations before language-agnostic workers see them.
    ///
    /// Queue keys for foreign tasks use the format `"language::module.name"`,
    /// so we match keys that start with `"language::"`. For local tasks
    /// (no language prefix), they are only retrieved if `language` is empty.
    async fn retrieve_invocation_for_language(
        &self,
        language: &str,
    ) -> RustvelloResult<Option<InvocationId>> {
        let mut queues = self.queues.lock().await;
        // First check the global queue (serves all languages).
        if let Some(id) = queues.get_mut(GLOBAL_QUEUE).and_then(VecDeque::pop_front) {
            return Ok(Some(id));
        }
        let prefix = format!("{language}::");
        for (key, queue) in queues.iter_mut() {
            if key == GLOBAL_QUEUE {
                continue;
            }
            // Match: foreign task keys start with "language::"; local keys have no "::"
            let matches = if language.is_empty() {
                !key.contains("::")
            } else {
                key.starts_with(&prefix)
            };
            if matches {
                if let Some(id) = queue.pop_front() {
                    return Ok(Some(id));
                }
            }
        }
        Ok(None)
    }

    async fn count_invocations(&self, task_id: Option<&TaskId>) -> RustvelloResult<usize> {
        let queues = self.queues.lock().await;
        if let Some(tid) = task_id {
            return Ok(queues.get(&tid.to_string()).map_or(0, VecDeque::len));
        }
        Ok(queues.values().map(VecDeque::len).sum())
    }

    async fn purge(&self, task_id: Option<&TaskId>) -> RustvelloResult<()> {
        let mut queues = self.queues.lock().await;
        if let Some(tid) = task_id {
            queues.remove(&tid.to_string());
            return Ok(());
        }
        queues.clear();
        Ok(())
    }

    /// Batch retrieval: single lock acquisition drains up to `max` items.
    async fn retrieve_invocations(
        &self,
        max: usize,
        task_id: Option<&TaskId>,
    ) -> RustvelloResult<Vec<InvocationId>> {
        let mut queues = self.queues.lock().await;
        let capped = max.min(10_000);
        let mut results = Vec::with_capacity(capped);
        for _ in 0..capped {
            let item = if let Some(tid) = task_id {
                queues
                    .get_mut(&tid.to_string())
                    .and_then(VecDeque::pop_front)
            } else {
                // Global first, then any task queue
                let global = queues.get_mut(GLOBAL_QUEUE).and_then(VecDeque::pop_front);
                if global.is_some() {
                    global
                } else {
                    let mut found = None;
                    for (key, queue) in queues.iter_mut() {
                        if key == GLOBAL_QUEUE {
                            continue;
                        }
                        if let Some(id) = queue.pop_front() {
                            found = Some(id);
                            break;
                        }
                    }
                    found
                }
            };
            match item {
                Some(id) => results.push(id),
                None => break,
            }
        }
        Ok(results)
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
        let py_task = TaskId::foreign("python", "analytics.tasks", "train");
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
            .retrieve_invocation_for_language("python")
            .await
            .unwrap();
        assert_eq!(got, Some(py_inv));

        // Python queue is now empty
        let got = broker
            .retrieve_invocation_for_language("python")
            .await
            .unwrap();
        assert_eq!(got, None);

        // Local (empty lang) worker should get the rust task (no "::" in key)
        let got = broker.retrieve_invocation_for_language("").await.unwrap();
        assert_eq!(got, Some(rs_inv));
    }

    #[tokio::test]
    async fn test_language_routing_global_queue_serves_all() {
        let broker = MemBroker::new();
        let inv = InvocationId::new();

        // Route via global queue (no task ID)
        broker.route_invocation(&inv).await.unwrap();

        // Any language worker should be able to get it
        let got = broker
            .retrieve_invocation_for_language("python")
            .await
            .unwrap();
        assert_eq!(got, Some(inv));
    }
}
