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

#[cfg(feature = "rayon")]
pub(crate) use rayon::RayonExecutor;
pub(crate) use tokio::TokioExecutor;

/// Local mechanism used to invoke task code after distributed work is claimed.
#[async_trait]
pub(crate) trait TaskExecutor: Send + Sync {
    fn kind(&self) -> ExecutorKind;

    async fn execute(
        &self,
        task: Arc<dyn DynTask>,
        args: SerializedArguments,
        invocation_context: InvocationContext,
        runner_context: RunnerContext,
    ) -> RustvelloResult<String>;
}
