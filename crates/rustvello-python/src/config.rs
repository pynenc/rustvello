use pyo3::prelude::*;

use cistell_core::Resolver;

use rustvello_core::broker::validate_routing;
use rustvello_proto::config::{
    AppConfig, BrokerPriorityRule, QueueSelectionStrategy, RetryJitter, TaskConfig,
};
use rustvello_proto::status::ConcurrencyControlType;

fn seconds_to_ms(name: &str, seconds: f64) -> PyResult<u64> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "{name} must be a finite number of seconds >= 0"
        )));
    }
    Ok((seconds * 1000.0).round() as u64)
}

/// Apply the retry backoff and execution deadline options (seconds) to a config.
///
/// Shared by `TaskConfig.with_retry_policy` and the runner's `register_task`,
/// so both Python registration paths validate identically.
pub(crate) fn apply_retry_policy(
    config: &mut TaskConfig,
    retry_delay: f64,
    retry_max_delay: f64,
    retry_backoff: f64,
    retry_jitter: &str,
    timeout: Option<f64>,
    retry_on_timeout: bool,
) -> PyResult<()> {
    if !retry_backoff.is_finite() || retry_backoff < 1.0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "retry_backoff must be a finite number >= 1.0",
        ));
    }
    config.retry_delay_ms = seconds_to_ms("retry_delay", retry_delay)?;
    config.retry_max_delay_ms = seconds_to_ms("retry_max_delay", retry_max_delay)?;
    config.retry_backoff = retry_backoff;
    config.retry_jitter = retry_jitter
        .parse::<RetryJitter>()
        .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
    config.timeout_ms = match timeout {
        None => None,
        Some(seconds) if seconds.is_finite() && seconds > 0.0 => {
            Some(((seconds * 1000.0).round() as u64).max(1))
        }
        Some(_) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive number of seconds (or None for no deadline)",
            ))
        }
    };
    config.retry_on_timeout = retry_on_timeout;
    Ok(())
}

/// Python wrapper for TaskConfig.
#[pyclass(name = "TaskConfig")]
#[derive(Clone)]
pub struct PyTaskConfig {
    pub inner: TaskConfig,
}

#[pymethods]
impl PyTaskConfig {
    #[new]
    #[pyo3(signature = (max_retries=0, concurrency_control="unlimited", running_concurrency=None, cache_results=false, queue="default", priority=0.0, is_workflow_task=false, retry_for_errors=vec![]))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_retries: u32,
        concurrency_control: &str,
        running_concurrency: Option<u32>,
        cache_results: bool,
        queue: &str,
        priority: f64,
        is_workflow_task: bool,
        retry_for_errors: Vec<String>,
    ) -> PyResult<Self> {
        validate_routing(queue, priority)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        let cc = match concurrency_control {
            "unlimited" => ConcurrencyControlType::Unlimited,
            "task" => ConcurrencyControlType::Task,
            "argument" => ConcurrencyControlType::Argument,
            "none" => ConcurrencyControlType::None,
            other => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "unknown concurrency control type: '{}'. Expected 'unlimited', 'task', 'argument', or 'none'",
                    other
                )))
            }
        };
        let mut inner = TaskConfig::default();
        inner.max_retries = max_retries;
        inner.concurrency_control = cc;
        inner.running_concurrency = running_concurrency;
        inner.cache_results = cache_results;
        inner.queue = queue.to_owned();
        inner.priority = priority;
        inner.is_workflow_task = is_workflow_task;
        inner.retry_for_errors = retry_for_errors;
        Ok(Self { inner })
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.max_retries
    }

    /// Exception type names that trigger a retry; empty means every error retries.
    #[getter]
    fn retry_for_errors(&self) -> Vec<String> {
        self.inner.retry_for_errors.clone()
    }

    #[getter]
    fn cache_results(&self) -> bool {
        self.inner.cache_results
    }

    #[getter]
    fn running_concurrency(&self) -> Option<u32> {
        self.inner.running_concurrency
    }

    #[getter]
    fn queue(&self) -> &str {
        &self.inner.queue
    }

    #[getter]
    fn priority(&self) -> f64 {
        self.inner.priority
    }

    #[getter]
    fn is_workflow_task(&self) -> bool {
        self.inner.is_workflow_task
    }

    /// Return a copy with retry backoff and an execution deadline (seconds).
    ///
    /// Defaults keep today's behaviour: immediate retries, no deadline.
    #[pyo3(signature = (*, retry_delay=0.0, retry_max_delay=300.0, retry_backoff=2.0, retry_jitter="equal", timeout=None, retry_on_timeout=true))]
    fn with_retry_policy(
        &self,
        retry_delay: f64,
        retry_max_delay: f64,
        retry_backoff: f64,
        retry_jitter: &str,
        timeout: Option<f64>,
        retry_on_timeout: bool,
    ) -> PyResult<Self> {
        let mut inner = self.inner.clone();
        apply_retry_policy(
            &mut inner,
            retry_delay,
            retry_max_delay,
            retry_backoff,
            retry_jitter,
            timeout,
            retry_on_timeout,
        )?;
        Ok(Self { inner })
    }

    /// Delay before the first retry, in seconds (0 retries immediately).
    #[getter]
    fn retry_delay(&self) -> f64 {
        self.inner.retry_delay_ms as f64 / 1000.0
    }

    /// Upper bound of the retry delay before jitter, in seconds.
    #[getter]
    fn retry_max_delay(&self) -> f64 {
        self.inner.retry_max_delay_ms as f64 / 1000.0
    }

    /// Growth factor of the retry delay per attempt.
    #[getter]
    fn retry_backoff(&self) -> f64 {
        self.inner.retry_backoff
    }

    /// Jitter strategy: ``"equal"``, ``"full"`` or ``"none"``.
    #[getter]
    fn retry_jitter(&self) -> String {
        self.inner.retry_jitter.to_string()
    }

    /// Execution deadline of one attempt in seconds, or ``None``.
    #[getter]
    fn timeout(&self) -> Option<f64> {
        self.inner.timeout_ms.map(|ms| ms as f64 / 1000.0)
    }

    /// Whether a timed-out attempt may be retried.
    #[getter]
    fn retry_on_timeout(&self) -> bool {
        self.inner.retry_on_timeout
    }

    fn __repr__(&self) -> String {
        format!(
            "TaskConfig(max_retries={}, cache_results={}, queue='{}', priority={}, is_workflow_task={})",
            self.inner.max_retries,
            self.inner.cache_results,
            self.inner.queue,
            self.inner.priority,
            self.inner.is_workflow_task,
        )
    }
}

/// Python wrapper for AppConfig.
#[pyclass(name = "AppConfig")]
#[derive(Clone)]
pub struct PyAppConfig {
    pub inner: AppConfig,
}

#[pymethods]
impl PyAppConfig {
    /// Configure the native recovery/trigger scheduler, in minutes.
    #[pyo3(signature = (*, interval_minutes, check_interval_minutes, spread_margin_minutes=0.0))]
    fn with_atomic_services(
        mut slf: PyRefMut<'_, Self>,
        interval_minutes: f64,
        check_interval_minutes: f64,
        spread_margin_minutes: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if !interval_minutes.is_finite()
            || !check_interval_minutes.is_finite()
            || !spread_margin_minutes.is_finite()
            || interval_minutes <= 0.0
            || check_interval_minutes <= 0.0
            || spread_margin_minutes < 0.0
            || spread_margin_minutes >= interval_minutes
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "atomic service intervals must be positive and finite, with 0 <= margin < interval",
            ));
        }
        slf.inner.atomic_service_interval_minutes = interval_minutes;
        slf.inner.atomic_service_check_interval_minutes = check_interval_minutes;
        slf.inner.atomic_service_spread_margin_minutes = spread_margin_minutes;
        Ok(slf)
    }

    #[new]
    #[pyo3(signature = (
        app_id = "rustvello",
        dev_mode_force_sync = false,
        max_pending_seconds = None,
        heartbeat_interval_seconds = None,
        runner_dead_after_seconds = None,
        recovery_check_interval_seconds = None,
        scheduler_interval_seconds = None,
        enable_scheduler = None,
        blocking_control = None,
        broker_queues = None,
        runner_queues = None,
        queue_selection_strategy = None,
        priority_rules = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        app_id: &str,
        dev_mode_force_sync: bool,
        max_pending_seconds: Option<u64>,
        heartbeat_interval_seconds: Option<u64>,
        runner_dead_after_seconds: Option<u64>,
        recovery_check_interval_seconds: Option<u64>,
        scheduler_interval_seconds: Option<u64>,
        enable_scheduler: Option<bool>,
        blocking_control: Option<bool>,
        broker_queues: Option<Vec<String>>,
        runner_queues: Option<Vec<String>>,
        queue_selection_strategy: Option<&str>,
        priority_rules: Option<Vec<(String, f64)>>,
    ) -> PyResult<Self> {
        let mut inner = AppConfig::default();
        inner.app_id = app_id.to_string();
        inner.dev_mode_force_sync = dev_mode_force_sync;
        if let Some(v) = max_pending_seconds {
            inner.max_pending_seconds = v;
        }
        if let Some(v) = heartbeat_interval_seconds {
            inner.heartbeat_interval_seconds = v;
        }
        if let Some(v) = runner_dead_after_seconds {
            inner.runner_dead_after_seconds = v;
        }
        if let Some(v) = recovery_check_interval_seconds {
            inner.recovery_check_interval_seconds = v;
        }
        if let Some(v) = scheduler_interval_seconds {
            inner.scheduler_interval_seconds = v;
        }
        if let Some(v) = enable_scheduler {
            inner.enable_scheduler = v;
        }
        if let Some(v) = blocking_control {
            inner.blocking_control = v;
        }
        if let Some(queues) = broker_queues {
            for queue in &queues {
                validate_routing(queue, 0.0)
                    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            }
            inner.broker_queues = queues;
        }
        if let Some(queues) = runner_queues {
            for queue in &queues {
                validate_routing(queue, 0.0)
                    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            }
            inner.runner_queues = queues;
        }
        if let Some(strategy) = queue_selection_strategy {
            inner.queue_selection_strategy = strategy
                .parse::<QueueSelectionStrategy>()
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        }
        if let Some(rules) = priority_rules {
            for (task_id, priority) in &rules {
                if task_id.is_empty() || glob::Pattern::new(task_id).is_err() {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid priority rule pattern: {task_id:?}"
                    )));
                }
                validate_routing("default", *priority)
                    .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
            }
            inner.priority_rules = rules
                .into_iter()
                .map(|(task_id, priority)| BrokerPriorityRule { task_id, priority })
                .collect();
        }
        Ok(Self { inner })
    }

    /// Resolve the configuration like the Rust builder does: `RUSTVELLO__*` environment
    /// variables, an optional TOML file, and `[tool.rustvello.app]` in `./pyproject.toml`,
    /// over the defaults. Programmatic values set afterwards win.
    #[staticmethod]
    #[pyo3(signature = (file=None, app_id=None))]
    fn from_env(file: Option<&str>, app_id: Option<&str>) -> PyResult<Self> {
        let configuration = |error: String| pyo3::exceptions::PyValueError::new_err(error);
        let mut builder = Resolver::builder().env();
        if let Some(path) = file {
            builder = builder
                .file(path)
                .map_err(|error| configuration(error.to_string()))?;
        }
        builder = builder
            .pyproject_toml("rustvello", "app")
            .map_err(|error| configuration(error.to_string()))?;
        let resolved = builder
            .build()
            .resolve::<AppConfig>()
            .map_err(|error| configuration(error.to_string()))?;
        let mut inner = resolved.value;
        if let Some(app_id) = app_id {
            inner.app_id = app_id.to_owned();
        }
        Ok(Self { inner })
    }

    /// Resolve the configuration from a TOML file (plus env and `pyproject.toml`).
    #[staticmethod]
    #[pyo3(signature = (path, app_id=None))]
    fn from_file(path: &str, app_id: Option<&str>) -> PyResult<Self> {
        Self::from_env(Some(path), app_id)
    }

    #[getter]
    fn app_id(&self) -> &str {
        &self.inner.app_id
    }

    #[setter]
    fn set_app_id(&mut self, value: String) {
        self.inner.app_id = value;
    }

    #[getter]
    fn dev_mode_force_sync(&self) -> bool {
        self.inner.dev_mode_force_sync
    }

    #[setter]
    fn set_dev_mode_force_sync(&mut self, value: bool) {
        self.inner.dev_mode_force_sync = value;
    }

    #[getter]
    fn logging_level(&self) -> &str {
        &self.inner.logging_level
    }

    #[setter]
    fn set_logging_level(&mut self, value: String) {
        self.inner.logging_level = value;
    }

    #[setter]
    fn set_broker_queues(&mut self, queues: Vec<String>) -> PyResult<()> {
        for queue in &queues {
            validate_routing(queue, 0.0)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        }
        self.inner.broker_queues = queues;
        Ok(())
    }

    #[setter]
    fn set_runner_queues(&mut self, queues: Vec<String>) -> PyResult<()> {
        for queue in &queues {
            validate_routing(queue, 0.0)
                .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        }
        self.inner.runner_queues = queues;
        Ok(())
    }

    #[setter]
    fn set_queue_selection_strategy(&mut self, strategy: &str) -> PyResult<()> {
        self.inner.queue_selection_strategy = strategy
            .parse::<QueueSelectionStrategy>()
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        Ok(())
    }

    /// How often a worker checks whether its running invocation was cancelled (seconds).
    #[getter]
    fn cancellation_check_interval_seconds(&self) -> f64 {
        self.inner.cancellation_check_interval_seconds
    }

    #[setter]
    fn set_cancellation_check_interval_seconds(&mut self, value: f64) -> PyResult<()> {
        if !value.is_finite() || value < 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "cancellation_check_interval_seconds must be finite and >= 0",
            ));
        }
        self.inner.cancellation_check_interval_seconds = value;
        Ok(())
    }

    #[getter]
    fn max_pending_seconds(&self) -> u64 {
        self.inner.max_pending_seconds
    }

    #[getter]
    fn heartbeat_interval_seconds(&self) -> u64 {
        self.inner.heartbeat_interval_seconds
    }

    #[getter]
    fn runner_dead_after_seconds(&self) -> u64 {
        self.inner.runner_dead_after_seconds
    }

    #[getter]
    fn recovery_check_interval_seconds(&self) -> u64 {
        self.inner.recovery_check_interval_seconds
    }

    #[getter]
    fn scheduler_interval_seconds(&self) -> u64 {
        self.inner.scheduler_interval_seconds
    }

    #[getter]
    fn enable_scheduler(&self) -> bool {
        self.inner.enable_scheduler
    }

    #[getter]
    fn blocking_control(&self) -> bool {
        self.inner.blocking_control
    }

    #[getter]
    fn broker_queues(&self) -> Vec<String> {
        self.inner.broker_queues.clone()
    }

    #[getter]
    fn runner_queues(&self) -> Vec<String> {
        self.inner.runner_queues.clone()
    }

    #[getter]
    fn queue_selection_strategy(&self) -> &'static str {
        match self.inner.queue_selection_strategy {
            QueueSelectionStrategy::RoundRobin => "round_robin",
            QueueSelectionStrategy::Ordered => "ordered",
            QueueSelectionStrategy::Random => "random",
            _ => "unknown",
        }
    }

    #[getter]
    fn priority_rules(&self) -> Vec<(String, f64)> {
        self.inner
            .priority_rules
            .iter()
            .map(|rule| (rule.task_id.clone(), rule.priority))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "AppConfig(app_id='{}', heartbeat={}s, dead_after={}s)",
            self.inner.app_id,
            self.inner.heartbeat_interval_seconds,
            self.inner.runner_dead_after_seconds,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    // ── TaskConfig ───────────────────────────────────────────────

    #[test]
    fn task_config_defaults() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(0, "unlimited", None, false, "default", 0.0, false, vec![])
                .unwrap();
            assert_eq!(cfg.max_retries(), 0);
            assert!(!cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), None);
            assert!(!cfg.is_workflow_task());
        });
    }

    #[test]
    fn task_config_custom_values() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(
                3,
                "task",
                Some(5),
                true,
                "critical",
                12.5,
                true,
                vec!["ValueError".to_owned()],
            )
            .unwrap();
            assert_eq!(cfg.max_retries(), 3);
            assert!(cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), Some(5));
            assert_eq!(cfg.queue(), "critical");
            assert_eq!(cfg.priority(), 12.5);
            assert!(cfg.is_workflow_task());
        });
    }

    #[test]
    fn task_config_all_concurrency_types() {
        Python::with_gil(|_py| {
            for cc in &["unlimited", "task", "argument", "none"] {
                assert!(
                    PyTaskConfig::new(0, cc, None, false, "default", 0.0, false, vec![]).is_ok()
                );
            }
        });
    }

    #[test]
    fn task_config_invalid_concurrency_type() {
        Python::with_gil(|_py| {
            let result =
                PyTaskConfig::new(0, "invalid", None, false, "default", 0.0, false, vec![]);
            assert!(result.is_err());
        });
    }

    #[test]
    fn task_config_repr() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(2, "unlimited", None, true, "default", 0.0, true, vec![])
                .unwrap();
            let repr = cfg.__repr__();
            assert!(repr.contains("max_retries=2"));
            assert!(repr.contains("cache_results=true"));
            assert!(repr.contains("is_workflow_task=true"));
        });
    }

    // ── AppConfig ────────────────────────────────────────────────

    #[test]
    fn app_config_defaults() {
        let cfg = PyAppConfig::new(
            "rustvello",
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(cfg.app_id(), "rustvello");
        assert!(!cfg.dev_mode_force_sync());
        assert_eq!(cfg.max_pending_seconds(), 300);
        assert_eq!(cfg.heartbeat_interval_seconds(), 30);
        assert_eq!(cfg.runner_dead_after_seconds(), 300);
        assert_eq!(cfg.recovery_check_interval_seconds(), 60);
        assert_eq!(cfg.scheduler_interval_seconds(), 60);
        assert!(cfg.enable_scheduler());
        assert!(cfg.blocking_control());
    }

    #[test]
    fn app_config_custom() {
        let cfg = PyAppConfig::new(
            "my_app", true, None, None, None, None, None, None, None, None, None, None, None,
        )
        .unwrap();
        assert_eq!(cfg.app_id(), "my_app");
        assert!(cfg.dev_mode_force_sync());
    }

    #[test]
    fn app_config_custom_fields() {
        let cfg = PyAppConfig::new(
            "test",
            false,
            Some(600),
            Some(15),
            Some(120),
            Some(30),
            Some(120),
            Some(false),
            Some(false),
            Some(vec!["default".to_owned(), "critical".to_owned()]),
            Some(vec!["critical".to_owned()]),
            Some("ordered"),
            Some(vec![("billing.*".to_owned(), 50.0)]),
        )
        .unwrap();
        assert_eq!(cfg.max_pending_seconds(), 600);
        assert_eq!(cfg.heartbeat_interval_seconds(), 15);
        assert_eq!(cfg.runner_dead_after_seconds(), 120);
        assert_eq!(cfg.recovery_check_interval_seconds(), 30);
        assert_eq!(cfg.scheduler_interval_seconds(), 120);
        assert!(!cfg.enable_scheduler());
        assert!(!cfg.blocking_control());
        assert_eq!(cfg.broker_queues(), vec!["default", "critical"]);
        assert_eq!(cfg.runner_queues(), vec!["critical"]);
        assert_eq!(cfg.queue_selection_strategy(), "ordered");
        assert_eq!(cfg.priority_rules(), vec![("billing.*".to_owned(), 50.0)]);
    }

    #[test]
    fn app_config_repr() {
        let cfg = PyAppConfig::new(
            "test_app", false, None, None, None, None, None, None, None, None, None, None, None,
        )
        .unwrap();
        let repr = cfg.__repr__();
        assert!(repr.contains("test_app"));
        assert!(repr.contains("heartbeat=30s"));
    }
}
