// crates/rustvello/src/tasks/flush_task.rs

use std::sync::Arc;

use tokio::select;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;

use rustvello_record::Recorder;

pub async fn start_recorder_flush_task(
    cancellation_token: CancellationToken,
    recorder: Arc<Mutex<dyn Recorder + Send + Sync>>,
    flush_interval: Duration,
) {
    let mut interval = interval(flush_interval);

    loop {
        select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Recorder flush task was cancelled.");
                break;
            }
            _ = interval.tick() => {
                let mut recorder = recorder.lock().await;
                if let Err(e) = recorder.flush().await {
                    eprintln!("Error while flushing recorder: {:?}", e);
                }
            }
        }
    }
}
