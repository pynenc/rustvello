use pyo3::prelude::*;

use rustvello_core::broker::validate_routing;
use rustvello_proto::config::{AppConfig, BrokerPriorityRule, QueueSelectionStrategy, TaskConfig};
use rustvello_proto::status::ConcurrencyControlType;

/// Python wrapper for TaskConfig.
#[pyclass(name = "TaskConfig")]
#[derive(Clone)]
pub struct PyTaskConfig {
    pub inner: TaskConfig,
}

#[pymethods]
impl PyTaskConfig {
    #[new]
    #[pyo3(signature = (max_retries=0, concurrency_control="unlimited", running_concurrency=None, cache_results=false, queue="default", priority=0.0))]
    fn new(
        max_retries: u32,
        concurrency_control: &str,
        running_concurrency: Option<u32>,
        cache_results: bool,
        queue: &str,
        priority: f64,
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
        Ok(Self { inner })
    }

    #[getter]
    fn max_retries(&self) -> u32 {
        self.inner.max_retries
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

    fn __repr__(&self) -> String {
        format!(
            "TaskConfig(max_retries={}, cache_results={}, queue='{}', priority={})",
            self.inner.max_retries, self.inner.cache_results, self.inner.queue, self.inner.priority,
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

    #[getter]
    fn app_id(&self) -> &str {
        &self.inner.app_id
    }

    #[getter]
    fn dev_mode_force_sync(&self) -> bool {
        self.inner.dev_mode_force_sync
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
            let cfg = PyTaskConfig::new(0, "unlimited", None, false, "default", 0.0).unwrap();
            assert_eq!(cfg.max_retries(), 0);
            assert!(!cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), None);
        });
    }

    #[test]
    fn task_config_custom_values() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(3, "task", Some(5), true, "critical", 12.5).unwrap();
            assert_eq!(cfg.max_retries(), 3);
            assert!(cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), Some(5));
            assert_eq!(cfg.queue(), "critical");
            assert_eq!(cfg.priority(), 12.5);
        });
    }

    #[test]
    fn task_config_all_concurrency_types() {
        Python::with_gil(|_py| {
            for cc in &["unlimited", "task", "argument", "none"] {
                assert!(PyTaskConfig::new(0, cc, None, false, "default", 0.0).is_ok());
            }
        });
    }

    #[test]
    fn task_config_invalid_concurrency_type() {
        Python::with_gil(|_py| {
            let result = PyTaskConfig::new(0, "invalid", None, false, "default", 0.0);
            assert!(result.is_err());
        });
    }

    #[test]
    fn task_config_repr() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(2, "unlimited", None, true, "default", 0.0).unwrap();
            let repr = cfg.__repr__();
            assert!(repr.contains("max_retries=2"));
            assert!(repr.contains("cache_results=true"));
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
