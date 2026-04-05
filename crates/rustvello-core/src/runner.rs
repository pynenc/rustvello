use async_trait::async_trait;

use rustvello_proto::identifiers::RunnerId;

use crate::error::RustvelloResult;

/// Runner interface — the task execution engine.
///
/// Mirrors pynenc's `BaseRunner`. A runner:
/// 1. Polls the broker for queued invocations
/// 2. Loads invocation data from the state backend
/// 3. Executes the task function
/// 4. Stores results and updates status
#[async_trait]
pub trait Runner: Send + Sync {
    /// Get this runner's unique identifier.
    fn runner_id(&self) -> &RunnerId;

    /// Human-readable runner class name (e.g., "PersistentTokioRunner").
    fn runner_cls(&self) -> &str;

    /// Maximum number of parallel execution slots.
    fn max_parallel_slots(&self) -> usize;

    /// Get runner_ids of currently active workers.
    fn active_worker_ids(&self) -> Vec<RunnerId>;

    /// Start the runner's main execution loop.
    /// This is a blocking call that runs until shutdown.
    async fn run(&self) -> RustvelloResult<()>;

    /// Perform a single iteration of the runner loop.
    /// Returns `true` if work was done, `false` if idle.
    async fn run_one(&self) -> RustvelloResult<bool>;

    /// Graceful shutdown signal.
    async fn shutdown(&self) -> RustvelloResult<()>;

    /// Send a heartbeat to indicate the runner is alive.
    async fn heartbeat(&self) -> RustvelloResult<()>;
}
