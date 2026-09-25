//! PyO3 wrapper for the task runner subsystem.

use pyo3::prelude::*;
use std::sync::{Arc, Mutex};

use crate::telemetry::TelemetryEmitter;
use rustvello_core::broker::{validate_routing, Broker};
use rustvello_core::error::RustvelloError;
use rustvello_core::observability::EventLevel;
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::{TaskDefinition, TaskFn, TaskRegistry};
use rustvello_core::trigger::{TriggerManager, TriggerStore};
use rustvello_proto::config::AppConfig;
use rustvello_proto::identifiers::{TaskId, TaskLanguage};
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
    telemetry: Option<TelemetryEmitter>,
    calls: Mutex<RunnerCalls>,
}

#[derive(Default)]
struct RunnerCalls {
    active: usize,
    closing: bool,
}

struct RunningCall<'a>(&'a PyTaskRunner);

impl Drop for RunningCall<'_> {
    fn drop(&mut self) {
        let closing = {
            let mut calls = self.0.calls.lock().expect("runner calls lock");
            calls.active -= 1;
            calls.closing && calls.active == 0
        };
        if closing {
            if let Some(emitter) = &self.0.telemetry {
                // Runs outside the GIL after terminal and worker-stop events.
                // Counters remain readable even if the bounded shutdown fails.
                let _ = crate::telemetry::shutdown(emitter, 5_000);
            }
        }
    }
}

impl PyTaskRunner {
    fn begin_call(&self) -> PyResult<RunningCall<'_>> {
        let mut calls = self.calls.lock().expect("runner calls lock");
        if calls.closing {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "runner is shutting down",
            ));
        }
        calls.active += 1;
        Ok(RunningCall(self))
    }
}

#[pymethods]
impl PyTaskRunner {
    /// Get the runner's unique ID.
    fn runner_id(&self) -> String {
        self.runner.runner_id().to_string()
    }

    /// Whether an execution call is still draining after a shutdown signal.
    fn is_running(&self) -> bool {
        self.calls.lock().expect("runner calls lock").active != 0
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
        py.allow_threads(|| {
            let _call = self.begin_call()?;
            crate::runtime::shared_runtime()?
                .block_on(runner.run_one())
                .map_err(to_py_err)
        })
    }

    /// Run the runner loop. This blocks until shutdown is called.
    /// Releases the GIL so other Python threads can run.
    fn run(&self, py: Python<'_>) -> PyResult<()> {
        let runner = Arc::clone(&self.runner);
        py.allow_threads(|| {
            let _call = self.begin_call()?;
            crate::runtime::shared_runtime()?
                .block_on(runner.run())
                .map_err(to_py_err)
        })
    }

    /// Signal the runner to shut down gracefully.
    fn shutdown(&self, py: Python<'_>) -> PyResult<()> {
        let idle = {
            let mut calls = self.calls.lock().expect("runner calls lock");
            calls.closing = true;
            calls.active == 0
        };
        let runner = Arc::clone(&self.runner);
        py.allow_threads(move || {
            crate::runtime::shared_runtime()?
                .block_on(runner.shutdown())
                .map_err(to_py_err)
        })?;
        if idle {
            if let Some(emitter) = &self.telemetry {
                py.allow_threads(|| crate::telemetry::shutdown(emitter, 5_000))?;
            }
        }
        Ok(())
    }

    /// Read delivery counters without waiting, including after an export timeout.
    fn telemetry_stats(&self) -> PyResult<std::collections::BTreeMap<&'static str, u64>> {
        self.telemetry
            .as_ref()
            .map(crate::telemetry::snapshot)
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err("OTLP telemetry is not enabled")
            })
    }

    #[pyo3(signature = (timeout_ms=5000))]
    fn flush_telemetry(
        &self,
        py: Python<'_>,
        timeout_ms: u64,
    ) -> PyResult<std::collections::BTreeMap<&'static str, u64>> {
        let emitter = self.telemetry.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("OTLP telemetry is not enabled")
        })?;
        py.allow_threads(|| crate::telemetry::flush(emitter, timeout_ms))
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
/// Worker command plus extra environment for the subprocess executor.
type ProcessPoolSpec = (Vec<String>, Vec<(String, String)>);

#[pyclass(name = "RustTaskRunnerBuilder")]
pub struct PyTaskRunnerBuilder {
    app_id: String,
    config: AppConfig,
    broker: Option<Arc<dyn Broker>>,
    orchestrator: Option<Arc<dyn InvocationControlBackend>>,
    state_backend: Option<Arc<dyn StateBackend>>,
    trigger_manager: Option<TriggerManager>,
    task_registry: TaskRegistry,
    num_workers: Option<usize>,
    /// Worker command and extra env for the subprocess executor (one interpreter per worker).
    process_pool: Option<ProcessPoolSpec>,
    idle_sleep_ms: Option<u64>,
    telemetry: Option<TelemetryEmitter>,
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
            process_pool: None,
            idle_sleep_ms: None,
            telemetry: None,
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

    /// Execute Python tasks in a pool of worker processes started with `command`.
    ///
    /// The pool size is `with_num_workers`. Each worker runs one interpreter, so this is
    /// how CPU-bound Python tasks use several cores; the control plane stays in this
    /// process. `env` adds environment variables to the workers.
    #[pyo3(signature = (command, env=None))]
    fn with_process_pool(
        mut slf: PyRefMut<'_, Self>,
        command: Vec<String>,
        env: Option<Vec<(String, String)>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if command.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "process pool command must not be empty",
            ));
        }
        slf.process_pool = Some((command, env.unwrap_or_default()));
        Ok(slf)
    }

    /// Set the idle sleep interval in milliseconds.
    ///
    /// Controls how long the runner sleeps when no work is available.
    fn with_idle_sleep(mut slf: PyRefMut<'_, Self>, ms: u64) -> PyRefMut<'_, Self> {
        slf.idle_sleep_ms = Some(ms);
        slf
    }

    /// Enable bounded OTLP/HTTP-Protobuf lifecycle export for this runner.
    fn enable_otlp<'a>(
        mut slf: PyRefMut<'a, Self>,
        endpoint: &str,
        bearer_token: &str,
    ) -> PyResult<PyRefMut<'a, Self>> {
        if slf.telemetry.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "OTLP telemetry is already enabled",
            ));
        }
        slf.telemetry = Some(crate::telemetry::emitter(endpoint, bearer_token)?);
        Ok(slf)
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
        running_concurrency = None,
        key_arguments = vec![],
        reroute_on_cc = false,
        max_retries = 0,
        retry_for_errors = vec![],
        registration_concurrency = "Unlimited",
        cache_results = false,
        disable_cache_args = vec![],
        on_diff_non_key_args_raise = false,
        parallel_batch_size = 100,
        is_workflow_task = false,
        queue = "default",
        priority = 0.0,
        retry_delay = 0.0,
        retry_max_delay = 300.0,
        retry_backoff = 2.0,
        retry_jitter = "equal",
        timeout = None,
        retry_on_timeout = true,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn register_task(
        &mut self,
        py: Python<'_>,
        module: &str,
        name: &str,
        func: PyObject,
        concurrency_control: &str,
        running_concurrency: Option<u32>,
        key_arguments: Vec<String>,
        reroute_on_cc: bool,
        max_retries: u32,
        retry_for_errors: Vec<String>,
        registration_concurrency: &str,
        cache_results: bool,
        disable_cache_args: Vec<String>,
        on_diff_non_key_args_raise: bool,
        parallel_batch_size: usize,
        is_workflow_task: bool,
        queue: &str,
        priority: f64,
        retry_delay: f64,
        retry_max_delay: f64,
        retry_backoff: f64,
        retry_jitter: &str,
        timeout: Option<f64>,
        retry_on_timeout: bool,
    ) -> PyResult<()> {
        validate_routing(queue, priority)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        let task_id = TaskId::try_for_language(TaskLanguage::Python, module, name)
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
        config.running_concurrency = running_concurrency;
        config.key_arguments = key_arguments;
        config.reroute_on_cc = reroute_on_cc;
        config.max_retries = max_retries;
        config.retry_for_errors = retry_for_errors;
        config.registration_concurrency = parse_cc_type(registration_concurrency)?;
        config.cache_results = cache_results;
        config.disable_cache_args = disable_cache_args;
        config.on_diff_non_key_args_raise = on_diff_non_key_args_raise;
        config.parallel_batch_size = parallel_batch_size;
        config.is_workflow_task = is_workflow_task;
        config.queue = queue.to_owned();
        config.priority = priority;
        crate::config::apply_retry_policy(
            &mut config,
            retry_delay,
            retry_max_delay,
            retry_backoff,
            retry_jitter,
            timeout,
            retry_on_timeout,
        )?;

        self.task_registry
            .register(TaskDefinition::new(task_id, config, task_fn))
            .map_err(to_py_err)
    }

    /// Register a task implemented by another language runtime.
    #[pyo3(signature = (language, module, name, *, queue = "default", priority = 0.0))]
    fn register_foreign_task(
        &mut self,
        language: &str,
        module: &str,
        name: &str,
        queue: &str,
        priority: f64,
    ) -> PyResult<()> {
        let task_id = crate::utils::parse_task_id(language, module, name)?;
        let mut config = rustvello_proto::config::TaskConfig::default();
        config.queue = queue.to_owned();
        config.priority = priority;

        self.task_registry
            .register_task_proxy(task_id, config)
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

        let mut runner = rustvello::runner::TaskRunner::new_python(
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

        if let Some((command, env)) = &self.process_pool {
            runner = runner.with_subprocess_executor(rustvello::runner::SubprocessSpec {
                command: command.clone(),
                env: env.clone(),
                kind: rustvello_proto::identifiers::ExecutorKind::Python,
            });
        }

        if let Some(emitter) = &self.telemetry {
            runner = runner.with_event_emitter(EventLevel::TaskLifecycle, emitter.clone());
        }

        Ok(PyTaskRunner {
            runner: Arc::new(runner),
            telemetry: self.telemetry.clone(),
            calls: Mutex::new(RunnerCalls::default()),
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
