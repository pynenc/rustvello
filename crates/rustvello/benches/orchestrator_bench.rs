use criterion::{criterion_group, criterion_main, Criterion};
use rustvello_core::orchestrator::{OrchestratorQuery, OrchestratorStatus};
use rustvello_mem::orchestrator::MemOrchestrator;
use rustvello_proto::call::{CallDTO, SerializedArguments};
use rustvello_proto::identifiers::{RunnerId, TaskId};
use rustvello_proto::status::InvocationStatus;

fn make_call(task_module: &str, task_name: &str) -> CallDTO {
    let task_id = TaskId::new(task_module, task_name);
    let args = SerializedArguments::new();
    CallDTO::new(task_id, args)
}

fn bench_orchestrator(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let orch = MemOrchestrator::new();
    let runner = RunnerId::from_string("bench-runner");

    c.bench_function("orch_register_invocation", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let call = make_call("bench_mod", &format!("task_{i}"));
            i += 1;
            rt.block_on(orch.register_invocation(&call)).unwrap();
        });
    });

    c.bench_function("orch_status_transition_cycle", |b| {
        b.iter(|| {
            let call = make_call("bench_mod", "cycle_task");
            rt.block_on(async {
                let inv_id = orch.register_invocation(&call).await.unwrap();
                orch.set_invocation_status(&inv_id, InvocationStatus::Pending, Some(&runner))
                    .await
                    .unwrap();
                orch.set_invocation_status(&inv_id, InvocationStatus::Running, Some(&runner))
                    .await
                    .unwrap();
                orch.set_invocation_status(&inv_id, InvocationStatus::Success, Some(&runner))
                    .await
                    .unwrap();
            });
        });
    });

    c.bench_function("orch_get_invocations_by_status", |b| {
        // Pre-populate
        let call = make_call("bench_mod", "query_task");
        for _ in 0..100 {
            rt.block_on(orch.register_invocation(&call)).unwrap();
        }
        b.iter(|| {
            rt.block_on(orch.get_invocations_by_status(InvocationStatus::Registered, None))
                .unwrap();
        });
    });
}

criterion_group!(benches, bench_orchestrator);
criterion_main!(benches);
