use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rustvello::prelude::*;
use rustvello_core::runner::Runner;
use rustvello_core::task::TaskFn;

static NEXT_APP: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
enum RunnerKind {
    Persistent,
    #[cfg(feature = "rayon")]
    Rayon,
}

#[derive(Clone, Copy)]
enum Workload {
    Short,
    BlockingIo,
    CpuBound,
    Mixed,
}

fn task_for(workload: Workload) -> (TaskConfig, TaskFn) {
    let mut config = TaskConfig::default();
    config.blocking = matches!(workload, Workload::BlockingIo | Workload::Mixed);
    let call_count = Arc::new(AtomicU64::new(0));
    let task = Arc::new(move |_| {
        match workload {
            Workload::Short => {}
            Workload::BlockingIo => std::thread::sleep(Duration::from_millis(1)),
            Workload::CpuBound => {
                let mut value = 0u64;
                for index in 0..25_000 {
                    value = std::hint::black_box(value.wrapping_add(index));
                }
                std::hint::black_box(value);
            }
            Workload::Mixed => {
                if call_count.fetch_add(1, Ordering::Relaxed) % 2 == 0 {
                    std::thread::sleep(Duration::from_millis(1));
                } else {
                    let mut value = 0u64;
                    for index in 0..25_000 {
                        value = std::hint::black_box(value.wrapping_add(index));
                    }
                    std::hint::black_box(value);
                }
            }
        }
        Ok("null".to_owned())
    });
    (config, task)
}

async fn run_batch(runner_kind: RunnerKind, workload: Workload) {
    let app_id = format!("runner-bench-{}", NEXT_APP.fetch_add(1, Ordering::Relaxed));
    let task_id = TaskId::new("bench", "work");
    let (task_config, task_fn) = task_for(workload);
    let mut app = RustvelloApp::new(AppConfig::new(&app_id));
    app.register_task(task_id.clone(), task_config.clone(), Arc::clone(&task_fn))
        .unwrap();

    let mut handles = Vec::with_capacity(16);
    for _ in 0..16 {
        handles.push(
            app.submit(&task_id, SerializedArguments::new())
                .await
                .unwrap(),
        );
    }

    let mut registry = TaskRegistry::new();
    registry
        .register(TaskDefinition::new(task_id, task_config, task_fn))
        .unwrap();
    let registry = Arc::new(registry);

    match runner_kind {
        RunnerKind::Persistent => {
            let runner = PersistentTokioRunner::new(
                app_id,
                app.config.clone(),
                app.broker(),
                app.orchestrator(),
                app.state_backend(),
                registry,
                None,
            )
            .with_num_workers(4);
            let running = runner.clone();
            let join = tokio::spawn(async move { running.run().await });
            wait_for_all(&app, &handles).await;
            runner.shutdown().await.unwrap();
            join.await.unwrap().unwrap();
        }
        #[cfg(feature = "rayon")]
        RunnerKind::Rayon => {
            let runner = RayonRunner::new(
                app_id,
                app.config.clone(),
                app.broker(),
                app.orchestrator(),
                app.state_backend(),
                registry,
            )
            .unwrap()
            .with_num_threads(4)
            .unwrap();
            let running = runner.clone();
            let join = tokio::spawn(async move { running.run().await });
            wait_for_all(&app, &handles).await;
            runner.shutdown().await.unwrap();
            join.await.unwrap().unwrap();
        }
    }
}

async fn wait_for_all(app: &RustvelloApp, invocation_ids: &[InvocationId]) {
    loop {
        let mut complete = true;
        for invocation_id in invocation_ids {
            let status = app.get_status(invocation_id).await.unwrap();
            complete &= status.is_terminal();
        }
        if complete {
            return;
        }
        tokio::task::yield_now().await;
    }
}

fn runner_benchmarks(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let mut group = criterion.benchmark_group("runner_batch_16");

    for (workload_name, workload) in [
        ("short", Workload::Short),
        ("blocking_io", Workload::BlockingIo),
        ("cpu_bound", Workload::CpuBound),
        ("mixed", Workload::Mixed),
    ] {
        #[cfg(feature = "rayon")]
        let runner_cases = [
            ("persistent", RunnerKind::Persistent),
            ("rayon", RunnerKind::Rayon),
        ];
        #[cfg(not(feature = "rayon"))]
        let runner_cases = [("persistent", RunnerKind::Persistent)];
        for (runner_name, runner) in runner_cases {
            group.bench_with_input(
                BenchmarkId::new(workload_name, runner_name),
                &(runner, workload),
                |bencher, &(runner, workload)| {
                    bencher.iter(|| runtime.block_on(run_batch(runner, workload)));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, runner_benchmarks);
criterion_main!(benches);
