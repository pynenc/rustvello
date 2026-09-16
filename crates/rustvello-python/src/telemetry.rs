use std::collections::BTreeMap;
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::PyResult;
use rustvello_core::observability::{
    AsyncExportConfig, AsyncExportStats, BoundedAsyncEmitter, EventEmitter, TaskLifecycleEvent,
    WorkerLifecycleEvent,
};
use rustvello_otel::{
    OtlpExportAccounting, OtlpLifecycleConfig, OtlpLifecycleExporter, SignalExportStats,
};

#[derive(Clone)]
pub(crate) struct TelemetryEmitter {
    queue: BoundedAsyncEmitter,
    delivery: OtlpExportAccounting,
}

impl EventEmitter for TelemetryEmitter {
    fn on_worker_lifecycle(&self, event: &WorkerLifecycleEvent) {
        self.queue.on_worker_lifecycle(event);
    }

    fn on_task_lifecycle(&self, event: &TaskLifecycleEvent) {
        self.queue.on_task_lifecycle(event);
    }
}

pub(crate) fn emitter(endpoint: &str, bearer_token: &str) -> PyResult<TelemetryEmitter> {
    let exporter = OtlpLifecycleExporter::new(OtlpLifecycleConfig::new(endpoint, bearer_token))
        .map_err(PyValueError::new_err)?;
    let delivery = exporter.accounting();
    Ok(TelemetryEmitter {
        queue: BoundedAsyncEmitter::new(AsyncExportConfig::default(), exporter),
        delivery,
    })
}

pub(crate) fn flush(
    emitter: &TelemetryEmitter,
    timeout_ms: u64,
) -> PyResult<BTreeMap<&'static str, u64>> {
    emitter
        .queue
        .flush(Duration::from_millis(timeout_ms))
        .map(|stats| stats_map(stats, emitter))
        .map_err(PyRuntimeError::new_err)
}

pub(crate) fn shutdown(
    emitter: &TelemetryEmitter,
    timeout_ms: u64,
) -> PyResult<BTreeMap<&'static str, u64>> {
    emitter
        .queue
        .shutdown(Duration::from_millis(timeout_ms))
        .map(|stats| stats_map(stats, emitter))
        .map_err(PyRuntimeError::new_err)
}

pub(crate) fn snapshot(emitter: &TelemetryEmitter) -> BTreeMap<&'static str, u64> {
    stats_map(emitter.queue.stats(), emitter)
}

fn stats_map(stats: AsyncExportStats, emitter: &TelemetryEmitter) -> BTreeMap<&'static str, u64> {
    let mut result = BTreeMap::from([
        ("accepted", stats.accepted),
        ("dropped", stats.dropped),
        ("export_failed", stats.export_failed),
        ("exported", stats.exported),
        ("enqueue_nanos_max", stats.enqueue_nanos_max),
        ("enqueue_nanos_total", stats.enqueue_nanos_total),
        ("rejected_after_shutdown", stats.rejected_after_shutdown),
    ]);
    let delivery = emitter.delivery.stats();
    for (keys, signal) in [
        (
            [
                "otlp_traces_attempted",
                "otlp_traces_acknowledged",
                "otlp_traces_rejected",
                "otlp_traces_failed",
                "otlp_traces_not_sent",
            ],
            delivery.traces,
        ),
        (
            [
                "otlp_logs_attempted",
                "otlp_logs_acknowledged",
                "otlp_logs_rejected",
                "otlp_logs_failed",
                "otlp_logs_not_sent",
            ],
            delivery.logs,
        ),
        (
            [
                "otlp_metrics_attempted",
                "otlp_metrics_acknowledged",
                "otlp_metrics_rejected",
                "otlp_metrics_failed",
                "otlp_metrics_not_sent",
            ],
            delivery.metrics,
        ),
    ] {
        append_signal(&mut result, keys, signal);
    }
    result.extend([
        ("otlp_failed_events", delivery.failed_events),
        ("otlp_incomplete_attempts", delivery.incomplete_attempts),
        ("otlp_duplicate_events", delivery.duplicate_events),
        ("otlp_unsampled_attempts", delivery.unsampled_attempts),
        ("otlp_response_warnings", delivery.response_warnings),
        ("otlp_unprocessed_events", delivery.unprocessed_events),
    ]);
    result
}

fn append_signal(
    result: &mut BTreeMap<&'static str, u64>,
    keys: [&'static str; 5],
    signal: SignalExportStats,
) {
    result.extend(keys.into_iter().zip([
        signal.attempted,
        signal.acknowledged,
        signal.rejected,
        signal.failed,
        signal.not_sent,
    ]));
}
