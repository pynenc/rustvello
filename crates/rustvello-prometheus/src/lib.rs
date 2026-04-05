//! Prometheus metrics sink for Rustvello observability.
//!
//! Implements [`EventEmitter`] using the `metrics` crate with a Prometheus
//! exporter. Metrics are exposed via an HTTP `/metrics` endpoint.

pub mod sink;

pub use sink::PrometheusSink;
