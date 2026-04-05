//! PyO3 wrapper for the trigger subsystem.

use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_mem::trigger::MemTriggerStore;

/// Parse an optional JSON string into an optional `BTreeMap` for argument/payload filters.
pub(crate) fn parse_optional_filter(
    json: Option<&str>,
) -> PyResult<Option<std::collections::BTreeMap<String, serde_json::Value>>> {
    match json {
        None => Ok(None),
        Some(s) => {
            let map: std::collections::BTreeMap<String, serde_json::Value> =
                serde_json::from_str(s).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("invalid filter JSON: {e}"))
                })?;
            if map.is_empty() {
                Ok(None)
            } else {
                Ok(Some(map))
            }
        }
    }
}

/// Rust in-memory trigger store + manager exposed to Python.
///
/// Wraps `MemTriggerStore` and `TriggerManager` for condition registration,
/// trigger evaluation, and event reporting.
#[pyclass(name = "RustMemTriggerStore")]
pub struct PyMemTriggerStore {
    pub(crate) store: Arc<MemTriggerStore>,
    pub(crate) manager: TriggerManager,
}

#[pymethods]
impl PyMemTriggerStore {
    #[new]
    fn new() -> Self {
        let store = Arc::new(MemTriggerStore::new());
        let manager = TriggerManager::new(Arc::clone(&store) as Arc<dyn TriggerStore>);
        Self { store, manager }
    }
}

impl_py_trigger_store!(PyMemTriggerStore);

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn new_trigger_store_is_empty() {
        Python::with_gil(|py| {
            let store = PyMemTriggerStore::new();
            let triggers = store.evaluate_triggers(py).unwrap();
            assert!(triggers.is_empty());
        });
    }

    #[test]
    fn register_and_get_condition() {
        Python::with_gil(|py| {
            let store = PyMemTriggerStore::new();
            let condition_json = r#"{"Event":{"event_code":"order_placed"}}"#;
            let cid = store.register_condition(py, condition_json).unwrap();
            assert!(!cid.is_empty());

            let retrieved = store.get_condition(py, &cid).unwrap();
            assert!(retrieved.is_some());
        });
    }

    #[test]
    fn get_nonexistent_condition_returns_none() {
        Python::with_gil(|py| {
            let store = PyMemTriggerStore::new();
            let result = store.get_condition(py, "nonexistent-id").unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn purge_clears_all() {
        Python::with_gil(|py| {
            let store = PyMemTriggerStore::new();
            let condition_json = r#"{"Event":{"event_code":"test"}}"#;
            let cid = store.register_condition(py, condition_json).unwrap();
            assert!(store.get_condition(py, &cid).unwrap().is_some());

            store.purge(py).unwrap();
            assert!(store.get_condition(py, &cid).unwrap().is_none());
        });
    }

    #[test]
    fn get_conditions_for_unknown_task_is_empty() {
        Python::with_gil(|py| {
            let store = PyMemTriggerStore::new();
            let conditions = store.get_conditions_for_task(py, "mod", "func").unwrap();
            assert!(conditions.is_empty());
        });
    }
}
