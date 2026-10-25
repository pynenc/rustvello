mod native_async;
mod subprocess;
mod tokio;

#[cfg(feature = "rayon")]
mod rayon;

use std::sync::Arc;

use async_trait::async_trait;
use rustvello_core::context::{InvocationContext, RunnerContext};
use rustvello_core::error::RustvelloResult;
use rustvello_core::task::DynTask;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::ExecutorKind;

pub(crate) use native_async::execute_native_async;
#[cfg(feature = "rayon")]
pub(crate) use rayon::RayonExecutor;
pub(crate) use subprocess::SubprocessExecutor;
pub use subprocess::SubprocessSpec;
pub(crate) use tokio::TokioExecutor;

/// Local mechanism used to invoke task code after distributed work is claimed.
#[async_trait]
pub(crate) trait TaskExecutor: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    /// Prepare the executor before the runner takes work, so a configuration
    /// mistake (a worker process that cannot start or registers fewer tasks
    /// than the runner) stops the runner at start-up instead of failing
    /// every invocation.
    async fn start(&self) -> RustvelloResult<()> {
        Ok(())
    }

    async fn execute(
        &self,
        task: Arc<dyn DynTask>,
        args: SerializedArguments,
        invocation_context: InvocationContext,
        runner_context: RunnerContext,
    ) -> RustvelloResult<String>;
}
