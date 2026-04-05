use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_mem::broker::MemBroker;

/// Rust in-memory broker exposed to Python.
///
/// Wraps `MemBroker` — a VecDeque-based FIFO queue.
#[pyclass(name = "RustMemBroker")]
pub struct PyMemBroker {
    pub(crate) inner: Arc<MemBroker>,
}

#[pymethods]
impl PyMemBroker {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(MemBroker::new()),
        }
    }
}

impl_py_broker!(PyMemBroker);

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    const UUID1: &str = "550e8400-e29b-41d4-a716-446655440001";
    const UUID2: &str = "550e8400-e29b-41d4-a716-446655440002";
    const UUID3: &str = "550e8400-e29b-41d4-a716-446655440003";

    #[test]
    fn new_broker_is_empty() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            assert_eq!(broker.count_invocations(py).unwrap(), 0);
        });
    }

    #[test]
    fn route_and_retrieve_single() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            broker.route_invocation(py, UUID1).unwrap();
            assert_eq!(broker.count_invocations(py).unwrap(), 1);

            let retrieved = broker.retrieve_invocation(py).unwrap();
            assert_eq!(retrieved.as_deref(), Some(UUID1));
            assert_eq!(broker.count_invocations(py).unwrap(), 0);
        });
    }

    #[test]
    fn fifo_ordering() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            broker.route_invocation(py, UUID1).unwrap();
            broker.route_invocation(py, UUID2).unwrap();
            broker.route_invocation(py, UUID3).unwrap();
            assert_eq!(broker.count_invocations(py).unwrap(), 3);

            assert_eq!(
                broker.retrieve_invocation(py).unwrap().as_deref(),
                Some(UUID1)
            );
            assert_eq!(
                broker.retrieve_invocation(py).unwrap().as_deref(),
                Some(UUID2)
            );
            assert_eq!(
                broker.retrieve_invocation(py).unwrap().as_deref(),
                Some(UUID3)
            );
            assert_eq!(broker.retrieve_invocation(py).unwrap(), None);
        });
    }

    #[test]
    fn route_batch() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            broker
                .route_invocations(py, vec![UUID1.into(), UUID2.into()])
                .unwrap();
            assert_eq!(broker.count_invocations(py).unwrap(), 2);
        });
    }

    #[test]
    fn retrieve_from_empty() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            assert_eq!(broker.retrieve_invocation(py).unwrap(), None);
        });
    }

    #[test]
    fn purge_clears_queue() {
        Python::with_gil(|py| {
            let broker = PyMemBroker::new();
            broker.route_invocation(py, UUID1).unwrap();
            broker.route_invocation(py, UUID2).unwrap();
            broker.purge(py).unwrap();
            assert_eq!(broker.count_invocations(py).unwrap(), 0);
        });
    }
}
