use std::time::Duration;

use rustvello::prelude::*;

// Define a task with the proc macro
#[rustvello::task(max_retries = 2, concurrency = "task", priority = 25.5)]
fn process_order(order_id: String) -> String {
    format!("processed {order_id}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Producers and workers share the same backend; here a local SQLite file
    let db = std::env::temp_dir().join(format!("rustvello-quickstart-{}.db", std::process::id()));
    let db = db.to_str().ok_or("temp path is not UTF-8")?;
    let builder = || {
        Rustvello::builder()
            .app_id("my-app")
            .sqlite(db, "my-app")
            .auto_discover_tasks()
    };

    // Start a worker. In production it is its own process (a binary of yours
    // calling `into_runner()`); here it runs in the background.
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let worker = tokio::spawn(
        builder()
            .build()
            .await?
            .into_runner()
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            }),
    );

    // Submit the task, then wait for the worker to finish it
    let app = builder().build().await?;
    let invocation = app
        .call(
            &ProcessOrderTask::new(),
            ProcessOrderParams {
                order_id: "123".into(),
            },
        )
        .await?;
    let result: String = invocation
        .wait_timeout(Duration::from_secs(30), Duration::from_millis(50))
        .await?;
    println!("Result: {result}");
    assert_eq!(result, "processed 123");

    let _ = stop.send(());
    worker.await??;
    Ok(())
}
