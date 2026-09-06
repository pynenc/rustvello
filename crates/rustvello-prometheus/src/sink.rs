//! Prometheus sink implementing [`EventEmitter`].

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::observability::EventEmitter;
use rustvello_proto::identifiers::{InvocationId, RunnerId, TaskId};

/// Guard ensuring the global metrics recorder is installed at most once.
static RECORDER_INSTALLED: OnceLock<()> = OnceLock::new();

/// Prometheus metrics sink for Rustvello.
///
/// Registers standard metrics and exposes them via an HTTP `/metrics` endpoint.
/// The sink spawns a background tokio task for the HTTP server.
///
/// **Singleton:** Only one `PrometheusSink` can be created per process because
/// the `metrics` crate uses a global recorder. A second call to [`new`] will
/// return an error.
///
/// # Metric Naming Convention
///
/// `rustvello_<subsystem>_<metric>_<unit>`
///
/// # Example
///
/// ```rust,no_run
/// use rustvello_prometheus::PrometheusSink;
/// use std::net::SocketAddr;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let sink = PrometheusSink::new("0.0.0.0:9090".parse()?)?;
/// # Ok(())
/// # }
/// ```
pub struct PrometheusSink {
    _handle: metrics_exporter_prometheus::PrometheusHandle,
}

impl PrometheusSink {
    /// Create a new Prometheus sink and start the HTTP exporter on the given address.
    ///
    /// # Errors
    ///
    /// Returns an error if the global metrics recorder has already been installed
    /// (only one `PrometheusSink` per process).
    pub fn new(bind: SocketAddr) -> RustvelloResult<Self> {
        if RECORDER_INSTALLED.get().is_some() {
            return Err(RustvelloError::Configuration {
                message:
                    "Prometheus recorder already installed — only one PrometheusSink per process"
                        .to_owned(),
            });
        }
        let builder = PrometheusBuilder::new().with_http_listener(bind);
        let handle = builder
            .install_recorder()
            .map_err(|e| RustvelloError::Configuration {
                message: format!("failed to install Prometheus recorder: {e}"),
            })?;
        let _ = RECORDER_INSTALLED.set(());
        Ok(Self { _handle: handle })
    }
}

impl EventEmitter for PrometheusSink {
    fn on_worker_started(&self, runner_id: &RunnerId) {
        gauge!("rustvello_workers_active", "runner_id" => runner_id.as_str().to_owned())
            .increment(1.0);
    }

    fn on_worker_shutdown(&self, runner_id: &RunnerId) {
        gauge!("rustvello_workers_active", "runner_id" => runner_id.as_str().to_owned())
            .decrement(1.0);
    }

    fn on_task_submitted(&self, task_id: &TaskId, _inv_id: &InvocationId) {
        counter!("rustvello_tasks_submitted_total", "task_id" => task_id.to_string()).increment(1);
    }

    fn on_task_started(&self, task_id: &TaskId, _inv_id: &InvocationId) {
        counter!("rustvello_tasks_started_total", "task_id" => task_id.to_string()).increment(1);
    }

    fn on_task_succeeded(&self, task_id: &TaskId, _inv_id: &InvocationId, duration: Duration) {
        let tid = task_id.to_string();
        counter!("rustvello_tasks_succeeded_total", "task_id" => tid.clone()).increment(1);
        histogram!("rustvello_task_duration_seconds", "task_id" => tid)
            .record(duration.as_secs_f64());
    }

    fn on_task_failed(
        &self,
        task_id: &TaskId,
        _inv_id: &InvocationId,
        _error: &str,
        duration: Duration,
    ) {
        let tid = task_id.to_string();
        counter!("rustvello_tasks_failed_total", "task_id" => tid.clone()).increment(1);
        histogram!("rustvello_task_duration_seconds", "task_id" => tid)
            .record(duration.as_secs_f64());
    }

    fn on_task_retried(&self, task_id: &TaskId, _inv_id: &InvocationId, _attempt: u32) {
        counter!("rustvello_tasks_retried_total", "task_id" => task_id.to_string()).increment(1);
    }

    fn on_queue_depth(&self, queue: &str, depth: usize) {
        gauge!("rustvello_queue_depth", "queue" => queue.to_owned()).set(depth as f64);
    }

    fn on_cc_rejected(&self, task_id: &TaskId) {
        counter!("rustvello_cc_rejected_total", "task_id" => task_id.to_string()).increment(1);
    }

    fn on_cc_slot_acquired(&self, task_id: &TaskId) {
        let tid = task_id.to_string();
        counter!("rustvello_cc_slot_acquired_total", "task_id" => tid.clone()).increment(1);
        gauge!("rustvello_cc_slots_active", "task_id" => tid).increment(1.0);
    }

    fn on_cc_slot_released(&self, task_id: &TaskId) {
        let tid = task_id.to_string();
        counter!("rustvello_cc_slot_released_total", "task_id" => tid.clone()).increment(1);
        gauge!("rustvello_cc_slots_active", "task_id" => tid).decrement(1.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::Recorder;
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// Build an isolated recorder + handle pair for testing (no HTTP server).
    fn test_recorder() -> (impl Recorder, metrics_exporter_prometheus::PrometheusHandle) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        (recorder, handle)
    }

    fn task_id(s: &str) -> TaskId {
        s.parse().unwrap()
    }

    fn inv_id(s: &str) -> InvocationId {
        InvocationId::from(s)
    }

    fn runner_id(s: &str) -> RunnerId {
        RunnerId::from(s)
    }

    #[test]
    fn worker_started_increments_gauge() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_worker_started(&runner_id("r1"));
        });

        let output = handle.render();
        assert!(
            output.contains("rustvello_workers_active"),
            "missing gauge:\n{output}"
        );
        assert!(
            output.contains("runner_id=\"r1\""),
            "missing label:\n{output}"
        );
    }

    #[test]
    fn worker_shutdown_decrements_gauge() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_worker_started(&runner_id("r1"));
            sink.on_worker_shutdown(&runner_id("r1"));
        });

        let output = handle.render();
        // After +1 then -1 the gauge should be 0
        assert!(output.contains("rustvello_workers_active{runner_id=\"r1\"} 0"));
    }

    #[test]
    fn task_submitted_increments_counter() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_task_submitted(&task_id("my_app.add"), &inv_id("inv-001"));
            sink.on_task_submitted(&task_id("my_app.add"), &inv_id("inv-002"));
        });

        let output = handle.render();
        assert!(
            output.contains("rustvello_tasks_submitted_total"),
            "missing counter:\n{output}"
        );
        // Two increments → value 2
        assert!(output.contains("} 2"), "expected count 2:\n{output}");
    }

    #[test]
    fn task_started_increments_counter() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_task_started(&task_id("my_app.run"), &inv_id("inv-001"));
        });

        let output = handle.render();
        assert!(output.contains("rustvello_tasks_started_total"));
    }

    #[test]
    fn task_succeeded_records_counter_and_histogram() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_task_succeeded(
                &task_id("my_app.calc"),
                &inv_id("inv-001"),
                Duration::from_millis(150),
            );
        });

        let output = handle.render();
        assert!(
            output.contains("rustvello_tasks_succeeded_total"),
            "missing succeeded counter:\n{output}"
        );
        assert!(
            output.contains("rustvello_task_duration_seconds"),
            "missing duration histogram:\n{output}"
        );
    }

    #[test]
    fn task_failed_records_counter_and_histogram() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_task_failed(
                &task_id("my_app.calc"),
                &inv_id("inv-001"),
                "boom",
                Duration::from_secs(1),
            );
        });

        let output = handle.render();
        assert!(output.contains("rustvello_tasks_failed_total"));
        assert!(output.contains("rustvello_task_duration_seconds"));
    }

    #[test]
    fn task_retried_increments_counter() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_task_retried(&task_id("my_app.flaky"), &inv_id("inv-001"), 3);
        });

        let output = handle.render();
        assert!(output.contains("rustvello_tasks_retried_total"));
    }

    #[test]
    fn queue_depth_sets_gauge() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_queue_depth("default", 42);
        });

        let output = handle.render();
        assert!(output.contains("rustvello_queue_depth{queue=\"default\"} 42"));
    }

    #[test]
    fn cc_rejected_increments_counter() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_cc_rejected(&task_id("my_app.heavy"));
        });

        let output = handle.render();
        assert!(output.contains("rustvello_cc_rejected_total"));
    }

    #[test]
    fn cc_slot_acquired_increments_counter_and_gauge() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_cc_slot_acquired(&task_id("my_app.limited"));
        });

        let output = handle.render();
        assert!(
            output.contains("rustvello_cc_slot_acquired_total"),
            "missing acquired counter:\n{output}"
        );
        assert!(
            output.contains("rustvello_cc_slots_active"),
            "missing active gauge:\n{output}"
        );
    }

    #[test]
    fn cc_slot_released_decrements_gauge() {
        let (rec, handle) = test_recorder();
        let sink = PrometheusSink {
            _handle: handle.clone(),
        };

        metrics::with_local_recorder(&rec, || {
            sink.on_cc_slot_acquired(&task_id("my_app.limited"));
            sink.on_cc_slot_released(&task_id("my_app.limited"));
        });

        let output = handle.render();
        assert!(
            output.contains("rustvello_cc_slot_released_total"),
            "missing released counter:\n{output}"
        );
        // After acquire + release, gauge should be 0
        assert!(
            output.contains("rustvello_cc_slots_active{task_id=\"rust::my_app.limited\"} 0"),
            "expected gauge at 0:\n{output}"
        );
    }
}
