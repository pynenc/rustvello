mod composites;

use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use rustvello::app::RustvelloApp;
use rustvello_core::error::RustvelloError;
use rustvello_proto::call::SerializedArguments;
use rustvello_proto::identifiers::{TaskId, TaskLanguage};

use crate::config::{PyAppConfig, PyTaskConfig};
use crate::error::to_py_err;
use crate::identifiers::PyInvocationId;
use crate::runtime::shared_runtime;
use crate::status::PyInvocationStatus;
use crate::utils::parse_task_id;

/// The main Rustvello application exposed to Python.
///
/// Provides task registration, submission and result retrieval.
///
/// Uses the process-global `shared_runtime()` — the same tokio runtime that
/// all other PyO3 wrapper objects (`PyMemOrchestrator`, `PyMemStateBackend`,
/// etc.) use. This ensures a single thread pool services all async operations
/// across all objects, preventing divergent runtime-fence issues.
#[pyclass(name = "Rustvello")]
pub struct PyRustvello {
    pub(crate) inner: Arc<tokio::sync::Mutex<RustvelloApp>>,
}

#[pymethods]
impl PyRustvello {
    #[new]
    #[pyo3(signature = (config=None))]
    fn new(config: Option<PyAppConfig>) -> PyResult<Self> {
        // Eagerly initialise the shared runtime so any failure surfaces here
        // rather than on the first async call.
        shared_runtime()?;
        let app_config = config.map_or_else(python_app_config, |c| c.inner);
        let app = RustvelloApp::new(app_config);
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(app)),
        })
    }

    /// Register a Python callable as a task.
    #[pyo3(signature = (module, name, func, config=None))]
    fn register_task(
        &self,
        py: Python<'_>,
        module: &str,
        name: &str,
        func: PyObject,
        config: Option<PyTaskConfig>,
    ) -> PyResult<()> {
        let task_id = TaskId::try_for_language(TaskLanguage::Python, module, name)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let task_config = config.map(|c| c.inner).unwrap_or_default();

        // Wrap the Python function in a Rust closure
        let py_func = func.clone_ref(py);
        let task_fn: rustvello_core::task::TaskFn = Arc::new(move |args_json: String| {
            Python::with_gil(|py| {
                let result = py_func
                    .call1(py, (args_json,))
                    .map_err(|e| RustvelloError::runner_err(e.to_string()))?;
                result
                    .extract::<String>(py)
                    .map_err(|e| RustvelloError::Serialization {
                        message: e.to_string(),
                    })
            })
        });

        // Release the GIL while waiting on the async lock so other Python
        // threads are not blocked during startup task registration.
        py.allow_threads(|| {
            shared_runtime()?.block_on(async {
                let mut app = self.inner.lock().await;
                app.register_task(task_id, task_config, task_fn)
                    .map_err(to_py_err)
            })
        })
    }

    /// Register a task implemented by another language runtime.
    #[pyo3(signature = (language, module, name, config=None))]
    fn register_foreign_task(
        &self,
        py: Python<'_>,
        language: &str,
        module: &str,
        name: &str,
        config: Option<PyTaskConfig>,
    ) -> PyResult<()> {
        if language == "python" {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "register Python tasks with register_task()",
            ));
        }
        let task_id = parse_task_id(language, module, name)?;
        let task_config = config.map(|c| c.inner).unwrap_or_default();

        py.allow_threads(|| {
            shared_runtime()?.block_on(async {
                let mut app = self.inner.lock().await;
                app.register_foreign_task(task_id, task_config)
                    .map_err(to_py_err)
            })
        })
    }

    /// Submit a task for asynchronous execution.
    #[pyo3(signature = (module, name, kwargs=None))]
    fn submit(
        &self,
        py: Python<'_>,
        module: &str,
        name: &str,
        kwargs: Option<BTreeMap<String, String>>,
    ) -> PyResult<PyInvocationId> {
        let task_id = TaskId::try_for_language(TaskLanguage::Python, module, name)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let mut args = SerializedArguments::new();
        if let Some(kw) = kwargs {
            for (k, v) in kw {
                args.insert(k, v);
            }
        }

        let app = Arc::clone(&self.inner);
        let inv_id = py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.submit(&task_id, args).await
                })
                .map_err(to_py_err)
        })?;

        Ok(PyInvocationId { inner: inv_id })
    }

    /// Submit a task by fully qualified language/module/name identity.
    #[pyo3(signature = (language, module, name, kwargs=None))]
    fn submit_task(
        &self,
        py: Python<'_>,
        language: &str,
        module: &str,
        name: &str,
        kwargs: Option<BTreeMap<String, String>>,
    ) -> PyResult<PyInvocationId> {
        let task_id = parse_task_id(language, module, name)?;
        let mut args = SerializedArguments::new();
        if let Some(kw) = kwargs {
            for (k, v) in kw {
                args.insert(k, v);
            }
        }

        let app = Arc::clone(&self.inner);
        let inv_id = py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.submit(&task_id, args).await
                })
                .map_err(to_py_err)
        })?;

        Ok(PyInvocationId { inner: inv_id })
    }

    /// Mark one invocation as waiting for another invocation.
    fn set_waiting_for(
        &self,
        py: Python<'_>,
        waiter: &PyInvocationId,
        waited_on: &PyInvocationId,
    ) -> PyResult<()> {
        let app = Arc::clone(&self.inner);
        let waiter_id = waiter.inner.clone();
        let waited_on_id = waited_on.inner.clone();
        py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.set_waiting_for(&waiter_id, &waited_on_id).await
                })
                .map_err(to_py_err)
        })
    }

    /// Execute a task synchronously, bypassing the broker and runner.
    ///
    /// Calls the registered task function directly in the current thread and
    /// returns the raw JSON result string. Used by the standalone DX layer
    /// (`App`) when `dev_mode_force_sync=True`.
    ///
    /// Unlike `submit()`, this does not create an invocation record, store
    /// history, or route through the broker. It is a direct synchronous call.
    #[pyo3(signature = (module, name, kwargs=None))]
    fn call_sync(
        &self,
        py: Python<'_>,
        module: &str,
        name: &str,
        kwargs: Option<BTreeMap<String, String>>,
    ) -> PyResult<Option<String>> {
        let task_id = TaskId::try_for_language(TaskLanguage::Python, module, name)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let mut args = SerializedArguments::new();
        if let Some(kw) = kwargs {
            for (k, v) in kw {
                args.insert(k, v);
            }
        }

        let app = Arc::clone(&self.inner);
        let result = py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.submit_sync(&task_id, args).await
                })
                .map_err(to_py_err)
        })?;

        Ok(Some(result))
    }

    /// Get the current status of an invocation.
    fn get_status(
        &self,
        py: Python<'_>,
        invocation_id: &PyInvocationId,
    ) -> PyResult<PyInvocationStatus> {
        let app = Arc::clone(&self.inner);
        let inv_id = invocation_id.inner.clone();
        let status = py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.get_status(&inv_id).await
                })
                .map_err(to_py_err)
        })?;

        Ok(PyInvocationStatus { inner: status })
    }

    /// Get the result of a completed invocation.
    fn get_result(
        &self,
        py: Python<'_>,
        invocation_id: &PyInvocationId,
    ) -> PyResult<Option<String>> {
        let app = Arc::clone(&self.inner);
        let inv_id = invocation_id.inner.clone();
        py.allow_threads(|| {
            shared_runtime()?
                .block_on(async {
                    let app = app.lock().await;
                    app.get_result(&inv_id).await
                })
                .map_err(to_py_err)
        })
    }

    /// Create a `Rustvello` that shares backends with existing PyO3 wrapper objects.
    ///
    /// This allows the composite hot-path operations to read/write the same
    /// state as the individual backend adapters, enabling zero-copy native mode.
    ///
    /// Accepts **any** backend variant (mem, sqlite, postgres, redis, mongo).
    /// The backends are extracted via PyO3 downcasting and their inner `Arc`s
    /// are shared with the new `RustvelloApp`.
    ///
    /// The `RustvelloApp` created here has an **empty task registry**, so
    /// `get_invocations_to_run_with_context()` cannot resolve per-task
    /// `TaskConfig` for concurrency-control checks.  The Python-side
    /// `_RustvelloNativeOrchestrator` therefore does **not** override
    /// `get_invocations_to_run` and falls back to pynenc's Python CC
    /// logic.
    #[staticmethod]
    #[pyo3(signature = (orchestrator, state_backend, broker, trigger, client_data_store, config=None))]
    fn from_backends(
        orchestrator: &Bound<'_, PyAny>,
        state_backend: &Bound<'_, PyAny>,
        broker: &Bound<'_, PyAny>,
        trigger: &Bound<'_, PyAny>,
        client_data_store: &Bound<'_, PyAny>,
        config: Option<PyAppConfig>,
    ) -> PyResult<Self> {
        use crate::backend_extract::{
            extract_broker, extract_client_data_store, extract_orchestrator, extract_state_backend,
            extract_trigger_manager,
        };

        let app_config = config.map_or_else(python_app_config, |c| c.inner);
        let orch = extract_orchestrator(orchestrator)?;
        let sb = extract_state_backend(state_backend)?;
        let br = extract_broker(broker)?;
        let cds = extract_client_data_store(client_data_store)?;
        let tm = Some(extract_trigger_manager(trigger)?);

        shared_runtime()?;
        let app = RustvelloApp::with_backends_and_triggers(app_config, br, orch, sb, cds, tm);
        Ok(Self {
            inner: Arc::new(tokio::sync::Mutex::new(app)),
        })
    }

    fn __repr__(&self) -> String {
        "Rustvello(...)".to_string()
    }
}

fn python_app_config() -> rustvello_proto::config::AppConfig {
    rustvello_proto::config::AppConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn new_without_config() {
        Python::with_gil(|_py| {
            let app = PyRustvello::new(None).unwrap();
            assert_eq!(app.__repr__(), "Rustvello(...)");
        });
    }

    #[test]
    fn new_with_default_config() {
        Python::with_gil(|_py| {
            let config = PyAppConfig {
                inner: rustvello_proto::config::AppConfig::default(),
            };
            let app = PyRustvello::new(Some(config)).unwrap();
            assert_eq!(app.__repr__(), "Rustvello(...)");
        });
    }

    #[test]
    fn submit_unknown_task_returns_error() {
        Python::with_gil(|py| {
            let app = PyRustvello::new(None).unwrap();
            let result = app.submit(py, "unknown_module", "unknown_func", None);
            assert!(result.is_err());
        });
    }
}
