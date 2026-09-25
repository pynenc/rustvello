//! Async tasks doing real network I/O.
//!
//! Starts a local TCP echo server, submits async tasks that talk to it, and
//! runs a worker in the same process until every result is in.
//!
//! ```bash
//! cargo run -p rustvello --example async_tasks
//! ```

use std::sync::Arc;
use std::time::Duration;

use rustvello::prelude::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

fn network_error(error: std::io::Error) -> RustvelloError {
    RustvelloError::TaskExecution {
        error_type: "TransientNetworkError".into(),
        message: error.to_string(),
        traceback: None,
    }
}

/// One round trip through a line-based echo server.
#[rustvello::task(max_retries = 2, retry_for_errors = ["TransientNetworkError"])]
async fn echo(addr: String, message: String) -> RustvelloResult<String> {
    let stream = TcpStream::connect(&addr).await.map_err(network_error)?;
    let (read, mut write) = stream.into_split();
    write
        .write_all(format!("{message}\n").as_bytes())
        .await
        .map_err(network_error)?;
    let mut line = String::new();
    BufReader::new(read)
        .read_line(&mut line)
        .await
        .map_err(network_error)?;
    Ok(line.trim_end().to_owned())
}

/// A workflow root that awaits its children instead of blocking a thread.
#[rustvello::workflow]
async fn echo_all(addr: String, count: u32) -> RustvelloResult<Vec<String>> {
    let app = APP.get().expect("app initialised");
    let mut children = Vec::new();
    for index in 0..count {
        let params = EchoParams {
            addr: addr.clone(),
            message: format!("item {index}"),
        };
        children.push(app.submit_call(&EchoTask::new(), params).await?);
    }
    let mut replies = Vec::new();
    for child in children {
        replies.push(child.wait(Duration::from_millis(10)).await?);
    }
    Ok(replies)
}

static APP: std::sync::OnceLock<Arc<RustvelloApp>> = std::sync::OnceLock::new();

async fn start_echo_server() -> std::io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let mut lines = BufReader::new(read).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if write
                        .write_all(format!("{line}\n").as_bytes())
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
    });
    Ok(addr)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = start_echo_server().await?;

    let mut app = Rustvello::builder().app_id("async-echo").build().await?;
    app.register(EchoTask::new())?;
    app.register(EchoAllTask::new())?;
    let app = Arc::new(app);
    APP.set(Arc::clone(&app)).ok();

    let root = app
        .submit_call(&EchoAllTask::new(), EchoAllParams { addr, count: 3 })
        .await?;

    // A worker in this process for the example; production workers run
    // `app.into_runner().run()` (or `rustvello run`) in their own processes.
    let runner = TaskRunner::new(
        app.config.app_id.clone(),
        app.config.clone(),
        app.broker(),
        app.orchestrator(),
        app.state_backend(),
        Arc::new(app.task_registry().clone()),
        None,
    )
    .with_num_workers(4);
    let replies = runner
        .with_graceful_shutdown(async {
            while !root.is_done().await.unwrap_or(false) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map(|()| root)?
        .result()
        .await?;

    println!("{replies:?}");
    Ok(())
}
