//! The worker's `invocation` span follows an async body across `.await`.
//!
//! Kept in its own test binary: tracing caches callsite interest globally, so
//! tests running in parallel without a subscriber can disable the span here.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustvello::prelude::*;

#[rustvello::task(module = "async_span")]
async fn traced_async() -> String {
    tokio::time::sleep(Duration::from_millis(5)).await;
    tracing::info!("async body resumed after await");
    "traced".to_owned()
}

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tracing_span_follows_async_body_across_await() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(captured.clone())
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut app = RustvelloApp::new(AppConfig::new("async-span"));
    app.register(TracedAsyncTask::new()).unwrap();
    let handle = app.submit_call(&TracedAsyncTask::new(), ()).await.unwrap();
    let id = handle.invocation_id().clone();
    let runner = PersistentTokioRunner::new(
        app.config.app_id.clone(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
        None,
    )
    .with_num_workers(1)
    .with_idle_sleep(5);
    runner.run_one().await.unwrap();
    assert_eq!(handle.result().await.unwrap(), "traced");

    let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let line = output
        .lines()
        .find(|line| line.contains("async body resumed after await"))
        .unwrap_or_else(|| panic!("body event missing:\n{output}"));
    assert!(
        line.contains("invocation{") && line.contains(&id.to_string()),
        "body event outside the invocation span: {line}"
    );
}
