use rustvello_core::error::RustvelloResult;
use rustvello_proto::identifiers::InvocationId;

use super::control_plane::RunnerControlPlane;

/// Claim the next invocation from this process's language and logical queues.
pub(crate) async fn claim_next(
    control_plane: &RunnerControlPlane,
) -> RustvelloResult<Option<InvocationId>> {
    control_plane
        .lifecycle()
        .claim_next(
            &control_plane.config,
            control_plane.task_registry(),
            control_plane.runner_language,
        )
        .await
}
