//! Caller deadlines include teardown, even for a badly behaved exporter Drop.

use rustvello_core::observability::{
    AsyncExportConfig, BoundedAsyncEmitter, LifecycleEvent, LifecycleExporter,
};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

struct BlockingDropExporter {
    dropping: Sender<()>,
    release: Receiver<()>,
}

impl LifecycleExporter for BlockingDropExporter {
    fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
        Ok(())
    }
}

impl Drop for BlockingDropExporter {
    fn drop(&mut self) {
        let _ = self.dropping.send(());
        let _ = self.release.recv();
    }
}

#[test]
fn shutdown_deadline_includes_exporter_destructor() {
    let (dropping, dropped) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let emitter = BoundedAsyncEmitter::new(
        AsyncExportConfig::default(),
        BlockingDropExporter {
            dropping,
            release: blocked,
        },
    );
    let control = emitter.clone();
    let (done, result) = mpsc::channel();
    let caller = std::thread::spawn(move || {
        let _ = done.send(control.shutdown(Duration::from_millis(100)));
    });
    let entered = dropped.recv_timeout(Duration::from_secs(2));
    let bounded = result.recv_timeout(Duration::from_millis(500));
    // Release before assertions so a failing regression cannot leak test threads.
    release.send(()).unwrap();
    caller.join().unwrap();
    entered.unwrap();
    assert!(bounded.unwrap().unwrap_err().contains("timed out"));
    emitter.shutdown(Duration::from_secs(1)).unwrap();
}

struct ShutdownNotification(Sender<()>);

impl LifecycleExporter for ShutdownNotification {
    fn export(&mut self, _: &[LifecycleEvent]) -> Result<(), String> {
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), String> {
        self.0.send(()).map_err(|error| error.to_string())
    }
}

#[test]
fn dropping_last_emitter_finalizes_adapter_accounting() {
    let (shutdown, done) = mpsc::channel();
    let emitter =
        BoundedAsyncEmitter::new(AsyncExportConfig::default(), ShutdownNotification(shutdown));
    drop(emitter);
    done.recv_timeout(Duration::from_secs(1)).unwrap();
}
