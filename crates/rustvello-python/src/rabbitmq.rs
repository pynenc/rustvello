//! PyO3 wrapper for RabbitMQ broker.
//!
//! RabbitMQ only provides a broker implementation — orchestrator,
//! state backend, trigger, and client data store must use another backend.

use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_rabbitmq::prelude::RabbitMqBroker;

/// RabbitMQ broker exposed to Python.
///
/// Wraps ``RabbitMqBroker`` for durable, distributed task queuing.
/// Must be paired with another backend for orchestration, state, etc.
#[pyclass(name = "RustRabbitmqBroker")]
pub struct PyRabbitmqBroker {
    pub(crate) inner: Arc<RabbitMqBroker>,
}

#[pymethods]
impl PyRabbitmqBroker {
    /// Create a new RabbitMQ broker.
    ///
    /// Args:
    ///   uri: AMQP connection URI (e.g. ``amqp://guest:guest@localhost:5672``)
    ///   prefix: Queue name prefix for namespacing
    #[new]
    fn new(uri: &str, prefix: &str) -> Self {
        Self {
            inner: Arc::new(RabbitMqBroker::new(uri, prefix)),
        }
    }
}

impl_py_broker!(PyRabbitmqBroker);

#[cfg(test)]
mod tests {}
