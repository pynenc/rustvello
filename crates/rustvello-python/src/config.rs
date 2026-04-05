use pyo3::prelude::*;

use rustvello_proto::config::{AppConfig, TaskConfig};
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
    #[pyo3(signature = (max_retries=0, concurrency_control="unlimited", running_concurrency=None, cache_results=false))]
    fn new(
        max_retries: u32,
        concurrency_control: &str,
        running_concurrency: Option<u32>,
        cache_results: bool,
    ) -> PyResult<Self> {
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

    fn __repr__(&self) -> String {
        format!(
            "TaskConfig(max_retries={}, cache_results={})",
            self.inner.max_retries, self.inner.cache_results
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
    ) -> Self {
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
        Self { inner }
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
            let cfg = PyTaskConfig::new(0, "unlimited", None, false).unwrap();
            assert_eq!(cfg.max_retries(), 0);
            assert!(!cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), None);
        });
    }

    #[test]
    fn task_config_custom_values() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(3, "task", Some(5), true).unwrap();
            assert_eq!(cfg.max_retries(), 3);
            assert!(cfg.cache_results());
            assert_eq!(cfg.running_concurrency(), Some(5));
        });
    }

    #[test]
    fn task_config_all_concurrency_types() {
        Python::with_gil(|_py| {
            for cc in &["unlimited", "task", "argument", "none"] {
                assert!(PyTaskConfig::new(0, cc, None, false).is_ok());
            }
        });
    }

    #[test]
    fn task_config_invalid_concurrency_type() {
        Python::with_gil(|_py| {
            let result = PyTaskConfig::new(0, "invalid", None, false);
            assert!(result.is_err());
        });
    }

    #[test]
    fn task_config_repr() {
        Python::with_gil(|_py| {
            let cfg = PyTaskConfig::new(2, "unlimited", None, true).unwrap();
            let repr = cfg.__repr__();
            assert!(repr.contains("max_retries=2"));
            assert!(repr.contains("cache_results=true"));
        });
    }

    // ── AppConfig ────────────────────────────────────────────────

    #[test]
    fn app_config_defaults() {
        let cfg = PyAppConfig::new("rustvello", false, None, None, None, None, None, None, None);
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
        let cfg = PyAppConfig::new("my_app", true, None, None, None, None, None, None, None);
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
        );
        assert_eq!(cfg.max_pending_seconds(), 600);
        assert_eq!(cfg.heartbeat_interval_seconds(), 15);
        assert_eq!(cfg.runner_dead_after_seconds(), 120);
        assert_eq!(cfg.recovery_check_interval_seconds(), 30);
        assert_eq!(cfg.scheduler_interval_seconds(), 120);
        assert!(!cfg.enable_scheduler());
        assert!(!cfg.blocking_control());
    }

    #[test]
    fn app_config_repr() {
        let cfg = PyAppConfig::new("test_app", false, None, None, None, None, None, None, None);
        let repr = cfg.__repr__();
        assert!(repr.contains("test_app"));
        assert!(repr.contains("heartbeat=30s"));
    }
}
