use criterion::{criterion_group, criterion_main, Criterion};
use rustvello_core::broker::Broker;
use rustvello_mem::broker::MemBroker;
use rustvello_proto::identifiers::InvocationId;

fn bench_route_and_retrieve(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let broker = MemBroker::new();

    c.bench_function("broker_route_single", |b| {
        b.iter(|| {
            let id = InvocationId::new();
            rt.block_on(broker.route_invocation(&id)).unwrap();
        });
    });

    c.bench_function("broker_route_retrieve_roundtrip", |b| {
        b.iter(|| {
            let id = InvocationId::new();
            rt.block_on(async {
                broker.route_invocation(&id).await.unwrap();
                broker.retrieve_invocation(None).await.unwrap();
            });
        });
    });

    c.bench_function("broker_route_batch_100", |b| {
        b.iter(|| {
            let ids: Vec<InvocationId> = (0..100).map(|_| InvocationId::new()).collect();
            rt.block_on(broker.route_invocations(&ids)).unwrap();
            // Drain the queue
            rt.block_on(async {
                while broker.retrieve_invocation(None).await.unwrap().is_some() {}
            });
        });
    });
}

criterion_group!(benches, bench_route_and_retrieve);
criterion_main!(benches);
