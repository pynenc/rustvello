//! PyO3 wrapper for the task runner subsystem.

use pyo3::prelude::*;
use std::sync::Arc;

use rustvello_core::broker::Broker;
use rustvello_core::error::RustvelloError;
use rustvello_core::orchestrator::Orchestrator;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{TaskDefinition, TaskFn, TaskRegistry};
use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::TaskId;
use rustvello_proto::status::ConcurrencyControlType;

use crate::config::PyAppConfig;
use crate::error::to_py_err;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map a Python CC type string to the Rust enum.
///
/// Accepts pynenc's `ConcurrencyControlType` `.value` strings (lowercase
/// from `StrEnum(auto())`) and common aliases.
fn parse_cc_type(s: &str) -> PyResult<ConcurrencyControlType> {
    s.parse::<ConcurrencyControlType>()
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

// Re-use the canonical backend extraction functions from backend_extract.rs.
// This ensures all backends (including Mongo3) are supported.
use crate::backend_extract::{
    extract_broker, extract_orchestrator, extract_state_backend, extract_trigger_manager,
};

// ---------------------------------------------------------------------------
// PyTaskRunner — wraps PersistentTokioRunner
// ---------------------------------------------------------------------------

/// Rust task runner exposed to Python.
///
/// Wraps `TaskRunner` — processes invocations from the broker, executes tasks,
/// manages heartbeats, recovery, and trigger evaluation.
#[pyclass(name = "RustTaskRunner")]
pub struct PyTaskRunner {
    runner: Arc<rustvello::runner::TaskRunner>,
}

#[pymethods]
impl PyTaskRunner {
    /// Get the runner's unique ID.
    fn runner_id(&self) -> String {
        self.runner.runner_id().to_string()
    }

    /// Return currently active invocations as (worker_runner_id, invocation_id).
    fn active_invocations(&self) -> Vec<(String, String)> {
        self.runner
            .worker_state()
            .into_values()
            .filter_map(|state| {
                state
                    .current_invocation
                    .map(|inv_id| (state.runner_id.to_string(), inv_id.to_string()))
            })
            .collect()
    }

    /// Process a single invocation from the broker.
    /// Returns True if work was done, False if the queue was empty.
    /// Releases the GIL so Rust can call back into Python (via TaskFn)
    /// from spawn_blocking threads.
    fn run_one(&self, py: Python<'_>) -> PyResult<bool> {
        let runner = Arc::clone(&self.runner);
        py.allow_threads(move || {
            crate::runtime::shared_runtime()?
                .block_on(runner.run_one())
                .map_err(to_py_err)
        })
    }

    /// Run the runner loop. This blocks until shutdown is called.
    /// Releases the GIL so other Python threads can run.
    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let runner = Arc::clone(&self.runner);
        py.allow_threads(move || {
            crate::runtime::shared_runtime()?
                .block_on(runner.run())
                .map_err(to_py_err)
        })
    }

    /// Signal the runner to shut down gracefully.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let runner = Arc::clone(&self.runner);
        py.allow_threads(move || {
            crate::runtime::shared_runtime()?
                .block_on(runner.shutdown())
                .map_err(to_py_err)
        })
    }
}

// ---------------------------------------------------------------------------
// PyTaskRunnerBuilder — constructs a runner with backends + tasks
// ---------------------------------------------------------------------------

/// Builder for creating a runner from existing backends.
///
/// Backends can be set via `.memory()` for testing, or via
/// `.with_backends()` to reuse backends from Python adapters.
/// Tasks must be registered via `.register_task()` before `.build()`.
#[pyclass(name = "RustTaskRunnerBuilder")]
pub struct PyTaskRunnerBuilder {
    app_id: String,
    config: AppConfig,
    broker: Option<Arc<dyn Broker>>,
    orchestrator: Option<Arc<dyn Orchestrator>>,
    state_backend: Option<Arc<dyn StateBackend>>,
    trigger_manager: Option<TriggerManager>,
    task_registry: TaskRegistry,
    num_workers: Option<usize>,
    idle_sleep_ms: Option<u64>,
}

#[pymethods]
impl PyTaskRunnerBuilder {
    #[new]
    #[pyo3(signature = (app_id="rustvello"))]
    fn new(app_id: &str) -> Self {
        Self {
            app_id: app_id.to_string(),
            config: AppConfig::default(),
            broker: None,
            orchestrator: None,
            state_backend: None,
            trigger_manager: None,
            task_registry: TaskRegistry::new(),
            num_workers: None,
            idle_sleep_ms: None,
        }
    }

    /// Use in-memory backends (for testing/development).
    fn memory(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.broker = Some(Arc::new(rustvello_mem::broker::MemBroker::new()));
        slf.orchestrator = Some(Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()));
        slf.state_backend = Some(Arc::new(
            rustvello_mem::state_backend::MemStateBackend::new(),
        ));
        let store = Arc::new(rustvello_mem::trigger::MemTriggerStore::new());
        slf.trigger_manager = Some(TriggerManager::new(store as Arc<dyn TriggerStore>));
        slf
    }

    /// Set backends from existing Rust PyO3 backend objects.
    ///
    /// This shares the same backend instances that the Python adapters use,
    /// so the runner operates on the same state as the orchestrator/broker/etc.
    #[pyo3(signature = (broker, orchestrator, state_backend, trigger_store=None))]
    fn with_backends<'a>(
        mut slf: PyRefMut<'a, Self>,
        broker: &Bound<'_, PyAny>,
        orchestrator: &Bound<'_, PyAny>,
        state_backend: &Bound<'_, PyAny>,
        trigger_store: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'a, Self>> {
        slf.broker = Some(extract_broker(broker)?);
        slf.orchestrator = Some(extract_orchestrator(orchestrator)?);
        slf.state_backend = Some(extract_state_backend(state_backend)?);
        if let Some(ts) = trigger_store {
            slf.trigger_manager = Some(extract_trigger_manager(ts)?);
        }
        Ok(slf)
    }

    /// Set number of worker threads.
    fn with_num_workers(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.num_workers = Some(n.max(1));
        slf
    }

    /// Set the idle sleep interval in milliseconds.
    ///
    /// Controls how long the runner sleeps when no work is available.
    fn with_idle_sleep(mut slf: PyRefMut<'_, Self>, ms: u64) -> PyRefMut<'_, Self> {
        slf.idle_sleep_ms = Some(ms);
        slf
    }

    /// Replace the builder's `AppConfig` with a pre-configured one.
    fn with_config(mut slf: PyRefMut<'_, Self>, config: PyAppConfig) -> PyRefMut<'_, Self> {
        slf.app_id = config.inner.app_id.clone();
        slf.config = config.inner;
        slf
    }

    /// Register a Python callable as a task in the runner's task registry.
    ///
    /// The callable receives a JSON string (serialized arguments dict)
    /// and must return a JSON string (serialized result).
    /// On error, the Python exception type name is captured for retry matching.
    #[pyo3(signature = (module, name, func, *,
        concurrency_control = "Unlimited",
        key_arguments = vec![],
        reroute_on_cc = false,
        max_retries = 0,
        retry_for_errors = vec![],
        registration_concurrency = "Unlimited",
        cache_results = false,
        disable_cache_args = vec![],
        on_diff_non_key_args_raise = false,
        parallel_batch_size = 100,
        force_new_workflow = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn register_task(
        &mut self,
        py: Python<'_>,
        module: &str,
        name: &str,
        func: PyObject,
        concurrency_control: &str,
        key_arguments: Vec<String>,
        reroute_on_cc: bool,
        max_retries: u32,
        retry_for_errors: Vec<String>,
        registration_concurrency: &str,
        cache_results: bool,
        disable_cache_args: Vec<String>,
        on_diff_non_key_args_raise: bool,
        parallel_batch_size: usize,
        force_new_workflow: bool,
    ) -> PyResult<()> {
        let task_id = TaskId::try_new(module, name)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        let py_func = func.clone_ref(py);
        let task_fn: TaskFn = Arc::new(move |args_json: String| {
            Python::with_gil(|py| match py_func.call1(py, (args_json,)) {
                Ok(result) => {
                    result
                        .extract::<String>(py)
                        .map_err(|e| RustvelloError::Serialization {
                            message: e.to_string(),
                        })
                }
                Err(py_err) => {
                    let error_type = py_err
                        .get_type_bound(py)
                        .name()
                        .map_or_else(|_| "UnknownError".to_string(), |n| n.to_string());
                    let message = py_err.to_string();
                    let traceback = py_err
                        .traceback_bound(py)
                        .map(|tb| tb.format().unwrap_or_default());
                    Err(RustvelloError::TaskExecution {
                        error_type,
                        message,
                        traceback,
                    })
                }
            })
        });

        let mut config = rustvello_proto::config::TaskConfig::default();
        config.blocking = true;
        config.concurrency_control = parse_cc_type(concurrency_control)?;
        config.key_arguments = key_arguments;
        config.reroute_on_cc = reroute_on_cc;
        config.max_retries = max_retries;
        config.retry_for_errors = retry_for_errors;
        config.registration_concurrency = parse_cc_type(registration_concurrency)?;
        config.cache_results = cache_results;
        config.disable_cache_args = disable_cache_args;
        config.on_diff_non_key_args_raise = on_diff_non_key_args_raise;
        config.parallel_batch_size = parallel_batch_size;
        config.force_new_workflow = force_new_workflow;

        self.task_registry
            .register(TaskDefinition::new(task_id, config, task_fn))
            .map_err(to_py_err)
    }

    /// Build the runner. All backends must be configured.
    fn build(&mut self) -> PyResult<PyTaskRunner> {
        let broker = self
            .broker
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("broker not configured"))?;
        let orchestrator = self.orchestrator.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("orchestrator not configured")
        })?;
        let state_backend = self.state_backend.clone().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("state_backend not configured")
        })?;

        // Take ownership of the populated task registry
        let registry = std::mem::take(&mut self.task_registry);

        let mut runner = rustvello::runner::TaskRunner::new(
            self.app_id.clone(),
            self.config.clone(),
            broker,
            orchestrator,
            state_backend,
            Arc::new(registry),
            self.trigger_manager.clone(),
        );

        if let Some(n) = self.num_workers {
            runner = runner.with_num_workers(n);
        }

        if let Some(ms) = self.idle_sleep_ms {
            runner = runner.with_idle_sleep(ms);
        }

        Ok(PyTaskRunner {
            runner: Arc::new(runner),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn builder_new_default_app_id() {
        Python::with_gil(|_py| {
            let builder = PyTaskRunnerBuilder::new("rustvello");
            assert_eq!(builder.app_id, "rustvello");
        });
    }

    #[test]
    fn builder_custom_app_id() {
        Python::with_gil(|_py| {
            let builder = PyTaskRunnerBuilder::new("my-app");
            assert_eq!(builder.app_id, "my-app");
        });
    }

    #[test]
    fn builder_memory_then_build() {
        Python::with_gil(|py| {
            let builder = pyo3::Py::new(py, PyTaskRunnerBuilder::new("test")).unwrap();
            builder.borrow_mut(py).broker = Some(Arc::new(rustvello_mem::broker::MemBroker::new()));
            builder.borrow_mut(py).orchestrator =
                Some(Arc::new(rustvello_mem::orchestrator::MemOrchestrator::new()));
            builder.borrow_mut(py).state_backend = Some(Arc::new(
                rustvello_mem::state_backend::MemStateBackend::new(),
            ));
            let store = Arc::new(rustvello_mem::trigger::MemTriggerStore::new());
            builder.borrow_mut(py).trigger_manager = Some(TriggerManager::new(
                store as Arc<dyn rustvello_core::trigger::TriggerStore>,
            ));
            let runner = builder.borrow_mut(py).build().unwrap();
            // runner_id should be a valid UUID string
            let id = runner.runner_id();
            assert!(!id.is_empty());
            assert!(uuid::Uuid::parse_str(&id).is_ok());
        });
    }

    #[test]
    fn builder_without_backends_fails() {
        Python::with_gil(|_py| {
            let mut builder = PyTaskRunnerBuilder::new("test");
            let result = builder.build();
            assert!(result.is_err());
        });
    }
}
