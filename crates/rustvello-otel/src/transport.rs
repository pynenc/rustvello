//! Bounded, acknowledged HTTP export using the SDK's OTLP protobuf conversion.

use std::io::Read;
use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::collector::{logs, metrics, trace};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, AggregationTemporality, Metric, NumberDataPoint, ResourceMetrics,
    ScopeMetrics, Sum,
};
use opentelemetry_proto::transform::common::tonic::{Attributes, ResourceAttributesWithSchema};
use prost::Message;

use super::{
    instrumentation_scope, terminal_outcome, Duration, Instant, KeyValue, OtlpLifecycleConfig,
    SystemTime, TaskLifecycleEvent, UNIX_EPOCH,
};

const MAX_RESPONSE_BYTES: u64 = 64 * 1024;

/// Counts records acknowledged, explicitly rejected, or of unknown delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SignalExportStats {
    pub attempted: u64,
    pub acknowledged: u64,
    pub rejected: u64,
    /// Transport, HTTP, timeout, malformed, and oversized response failures.
    /// A lost acknowledgement cannot prove the receiver did not commit.
    pub failed: u64,
    /// Mapped records not transmitted because of request size, deadline, or
    /// client initialization failure. These are known local losses.
    pub not_sent: u64,
}

/// Signal-level delivery and lifecycle-integrity counters, independent of queues.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OtlpExportStats {
    pub traces: SignalExportStats,
    pub logs: SignalExportStats,
    pub metrics: SignalExportStats,
    /// Native lifecycle validation, state, or mapping-deadline failures.
    /// Transport failures and request-size rejection are counted per signal.
    pub failed_events: u64,
    pub incomplete_attempts: u64,
    pub duplicate_events: u64,
    pub unsampled_attempts: u64,
    pub response_warnings: u64,
    /// Lifecycle events not mapped because the shared export budget expired.
    /// Their possible signal records are unknown, not counted as HTTP losses.
    pub unprocessed_events: u64,
}

/// Shared accounting remains readable after moving or shutting down the exporter.
#[derive(Clone, Debug, Default)]
pub struct OtlpExportAccounting(Arc<Mutex<OtlpExportStats>>);

impl OtlpExportAccounting {
    pub fn stats(&self) -> OtlpExportStats {
        *self.0.lock().expect("OTLP accounting lock")
    }

    pub(super) fn update(&self, update: impl FnOnce(&mut OtlpExportStats)) {
        update(&mut self.0.lock().expect("OTLP accounting lock"));
    }
}

#[derive(Clone, Copy)]
pub(super) enum Signal {
    Traces,
    Logs,
    Metrics,
}

impl Signal {
    fn path(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }

    fn stats(self, stats: &mut OtlpExportStats) -> &mut SignalExportStats {
        match self {
            Self::Traces => &mut stats.traces,
            Self::Logs => &mut stats.logs,
            Self::Metrics => &mut stats.metrics,
        }
    }

    fn acknowledgement(self, body: &[u8], count: u64) -> Result<(u64, bool), String> {
        // Never retry partial success, including the acknowledged subset.
        let partial = match self {
            Self::Traces => trace::v1::ExportTraceServiceResponse::decode(body).map(|r| {
                r.partial_success
                    .map(|p| (p.rejected_spans, !p.error_message.is_empty()))
            }),
            Self::Logs => logs::v1::ExportLogsServiceResponse::decode(body).map(|r| {
                r.partial_success
                    .map(|p| (p.rejected_log_records, !p.error_message.is_empty()))
            }),
            Self::Metrics => metrics::v1::ExportMetricsServiceResponse::decode(body).map(|r| {
                r.partial_success
                    .map(|p| (p.rejected_data_points, !p.error_message.is_empty()))
            }),
        }
        .map_err(|_| "malformed OTLP acknowledgement".to_owned())?;
        let (rejected, warning) = partial.unwrap_or_default();
        if rejected < 0 || rejected as u64 > count {
            return Err("invalid OTLP rejection count".to_owned());
        }
        Ok((rejected as u64, warning))
    }
}

/// One outer batch; records retain their own worker resource and scope.
#[derive(Default)]
pub(super) struct Pending {
    pub logs: Vec<opentelemetry_proto::tonic::logs::v1::ResourceLogs>,
    pub traces: Vec<opentelemetry_proto::tonic::trace::v1::ResourceSpans>,
    pub metrics: Vec<ResourceMetrics>,
}

/// Lazily created on the bounded emitter thread; no secondary SDK worker queue.
pub(super) struct Transport {
    client: Option<reqwest::blocking::Client>,
    pub accounting: OtlpExportAccounting,
}

impl Transport {
    pub fn new() -> Self {
        Self {
            client: None,
            accounting: OtlpExportAccounting::default(),
        }
    }

    pub fn flush(
        &mut self,
        config: &OtlpLifecycleConfig,
        pending: Pending,
        deadline: Instant,
    ) -> Result<(), String> {
        let mut first_error = None;
        let results = [
            self.send(
                config,
                Signal::Logs,
                pending.logs.len() as u64,
                logs::v1::ExportLogsServiceRequest {
                    resource_logs: pending.logs,
                },
                deadline,
            ),
            self.send(
                config,
                Signal::Traces,
                pending.traces.len() as u64,
                trace::v1::ExportTraceServiceRequest {
                    resource_spans: pending.traces,
                },
                deadline,
            ),
            self.send(
                config,
                Signal::Metrics,
                pending.metrics.len() as u64,
                metrics::v1::ExportMetricsServiceRequest {
                    resource_metrics: pending.metrics,
                },
                deadline,
            ),
        ];
        for result in results {
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn send(
        &mut self,
        config: &OtlpLifecycleConfig,
        signal: Signal,
        count: u64,
        request: impl Message,
        deadline: Instant,
    ) -> Result<(), String> {
        if count == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            self.accounting
                .update(|stats| signal.stats(stats).not_sent += count);
            return Err("OTLP export deadline exhausted before transmission".to_owned());
        }
        let encoded_len = request.encoded_len();
        if encoded_len > config.max_request_bytes {
            self.accounting
                .update(|stats| signal.stats(stats).not_sent += count);
            return Err(format!(
                "OTLP {} request body exceeds byte bound ({} > {})",
                signal.path(),
                encoded_len,
                config.max_request_bytes
            ));
        }
        let body = request.encode_to_vec();
        if self.client.is_none() {
            self.client = Some(
                reqwest::blocking::Client::builder()
                    .timeout(config.export_timeout)
                    .connect_timeout(config.export_timeout)
                    .redirect(reqwest::redirect::Policy::none())
                    .retry(reqwest::retry::never())
                    .build()
                    .map_err(|_| {
                        self.accounting
                            .update(|stats| signal.stats(stats).not_sent += count);
                        "cannot build OTLP HTTP client".to_owned()
                    })?,
            );
        }
        let Some(remaining) = deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
        else {
            self.accounting
                .update(|stats| signal.stats(stats).not_sent += count);
            return Err("OTLP export deadline exhausted before transmission".to_owned());
        };
        self.accounting
            .update(|stats| signal.stats(stats).attempted += count);
        let result = self.request(config, signal, body, count, remaining);
        self.accounting.update(|stats| match &result {
            Ok((rejected, warning)) => {
                let counts = signal.stats(stats);
                counts.acknowledged += count - rejected;
                counts.rejected += rejected;
                stats.response_warnings += u64::from(*warning);
            }
            Err(_) => signal.stats(stats).failed += count,
        });
        match result {
            Ok((0, _)) => Ok(()),
            Ok(_) => Err(format!("OTLP {} record rejected", signal.path())),
            Err(error) => Err(error),
        }
    }

    fn request(
        &mut self,
        config: &OtlpLifecycleConfig,
        signal: Signal,
        body: Vec<u8>,
        count: u64,
        remaining: Duration,
    ) -> Result<(u64, bool), String> {
        let response = self
            .client
            .as_ref()
            .expect("client initialized")
            .post(format!("{}/v1/{}", config.endpoint, signal.path()))
            .bearer_auth(&config.bearer_token)
            .header("Content-Type", "application/x-protobuf")
            .timeout(remaining)
            .body(body)
            .send()
            .map_err(|_| "OTLP transport failed or timed out".to_owned())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(format!("OTLP HTTP status {}", response.status().as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES)
        {
            return Err("OTLP acknowledgement exceeds response bound".to_owned());
        }
        if response.headers().get("content-type").is_none_or(|value| {
            value.to_str().map_or(true, |value| {
                value.split(';').next().unwrap_or("").trim() != "application/x-protobuf"
            })
        }) {
            return Err("OTLP acknowledgement has invalid content type".to_owned());
        }
        let mut body = Vec::new();
        response
            .take(MAX_RESPONSE_BYTES + 1)
            .read_to_end(&mut body)
            .map_err(|_| "OTLP response read failed or timed out".to_owned())?;
        if body.len() as u64 > MAX_RESPONSE_BYTES {
            return Err("OTLP acknowledgement exceeds response bound".to_owned());
        }
        signal.acknowledgement(&body, count)
    }
}

/// Delta sums avoid an unbounded SDK metric-series cache and shutdown re-export.
pub(super) fn completion_metric(
    resource: &ResourceAttributesWithSchema,
    event: &TaskLifecycleEvent,
    start: SystemTime,
    end: SystemTime,
) -> Result<metrics::v1::ExportMetricsServiceRequest, String> {
    // Count observations in disjoint worker intervals, independent of overlapping
    // execution durations. Original event timestamps remain on logs and spans.
    let end = end
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "invalid metric timestamp".to_owned())?;
    let start = start
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "invalid metric timestamp".to_owned())?;
    let end_ns =
        u64::try_from(end.as_nanos()).map_err(|_| "metric timestamp overflow".to_owned())?;
    let start_ns =
        u64::try_from(start.as_nanos()).map_err(|_| "metric timestamp overflow".to_owned())?;
    let attributes = Attributes::from(vec![
        KeyValue::new("rustvello.task.id", event.context.task_id.to_string()),
        KeyValue::new("rustvello.queue", event.context.queue.to_string()),
        KeyValue::new(
            "outcome",
            terminal_outcome(&event.kind).expect("terminal event"),
        ),
    ])
    .0;
    Ok(metrics::v1::ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(opentelemetry_proto::tonic::resource::v1::Resource {
                attributes: resource.attributes.0.clone(),
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some((&instrumentation_scope(), None).into()),
                metrics: vec![Metric {
                    name: "rustvello.task.completed".to_owned(),
                    description: "Completed Rustvello task attempts".to_owned(),
                    unit: "{attempt}".to_owned(),
                    data: Some(metric::Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            attributes,
                            start_time_unix_nano: start_ns,
                            time_unix_nano: end_ns,
                            value: Some(number_data_point::Value::AsInt(1)),
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Delta as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            schema_url: resource.schema_url.clone().unwrap_or_default(),
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fails immediately if an oversized message reaches allocation or encoding.
    struct OversizedMessage;

    impl Message for OversizedMessage {
        fn encode_raw(&self, _: &mut impl prost::bytes::BufMut) {
            panic!("oversized message must not be encoded");
        }

        fn encode_to_vec(&self) -> Vec<u8> {
            panic!("oversized message must not allocate an encoding buffer");
        }

        fn merge_field(
            &mut self,
            _: u32,
            _: prost::encoding::WireType,
            _: &mut impl prost::bytes::Buf,
            _: prost::encoding::DecodeContext,
        ) -> Result<(), prost::DecodeError> {
            unreachable!("test message is never decoded")
        }

        fn encoded_len(&self) -> usize {
            usize::MAX
        }

        fn clear(&mut self) {}
    }

    #[test]
    fn oversized_request_is_counted_before_encoding_or_client_creation() {
        let config = OtlpLifecycleConfig::new("http://127.0.0.1:1", "token");
        let mut transport = Transport::new();
        for signal in [Signal::Logs, Signal::Traces, Signal::Metrics] {
            let error = transport
                .send(
                    &config,
                    signal,
                    7,
                    OversizedMessage,
                    Instant::now() + Duration::from_secs(1),
                )
                .unwrap_err();
            assert!(error.contains("request body exceeds byte bound"));
            assert!(transport.client.is_none());
            let stats = transport.accounting.stats();
            let counts = *signal.stats(&mut stats.clone());
            assert_eq!(
                counts,
                SignalExportStats {
                    not_sent: 7,
                    ..Default::default()
                }
            );
            assert_eq!(stats.failed_events, 0);
            assert_eq!(stats.unprocessed_events, 0);
        }
    }
}
