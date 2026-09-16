//! Start the rustvello-monitoring dashboard from Python.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use pyo3::prelude::*;
use rustvello_monitoring::{start_monitor as serve, AppInstance, MonitorConfig};
use rustvello_proto::config::AppConfig;

use crate::backend_extract::{
    extract_broker, extract_client_data_store, extract_orchestrator, extract_state_backend,
    extract_trigger_manager,
};
use crate::config::PyAppConfig;
use crate::runtime::shared_runtime;
use crate::utils::parse_task_id;

/// Handle on a running monitoring server.
#[pyclass(name = "MonitorServer")]
pub struct PyMonitorServer {
    handle: Option<tokio::task::JoinHandle<()>>,
    address: String,
}

#[pymethods]
impl PyMonitorServer {
    /// `host:port` the server was bound to.
    #[getter]
    fn address(&self) -> &str {
        &self.address
    }

    fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Stop serving; the socket closes when the task is aborted.
    fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "MonitorServer(address={:?}, running={})",
            self.address,
            self.is_running()
        )
    }
}

/// Serve the monitoring dashboard for one app over the given backend objects.
///
/// Returns immediately; the server runs on the shared runtime until `stop()`.
#[pyfunction]
#[pyo3(signature = (app_id, broker, orchestrator, state_backend, client_data_store, trigger=None, task_ids=vec![], host="127.0.0.1", port=8000, log_level="info", config=None))]
#[allow(clippy::too_many_arguments)]
pub fn start_monitor(
    app_id: &str,
    broker: &Bound<'_, PyAny>,
    orchestrator: &Bound<'_, PyAny>,
    state_backend: &Bound<'_, PyAny>,
    client_data_store: &Bound<'_, PyAny>,
    trigger: Option<&Bound<'_, PyAny>>,
    task_ids: Vec<(String, String)>,
    host: &str,
    port: u16,
    log_level: &str,
    config: Option<PyAppConfig>,
) -> PyResult<PyMonitorServer> {
    let bind: SocketAddr = format!("{host}:{port}").parse().map_err(|error| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "invalid bind address {host}:{port}: {error}"
        ))
    })?;
    let mut app_config = config.map_or_else(AppConfig::default, |config| config.inner);
    app_config.app_id = app_id.to_owned();
    let trigger_store = match trigger {
        Some(trigger) => Some(Arc::clone(extract_trigger_manager(trigger)?.store())),
        None => None,
    };
    let instance = AppInstance {
        app_id: app_id.to_owned(),
        config: app_config,
        broker: extract_broker(broker)?,
        orchestrator: extract_orchestrator(orchestrator)?,
        state_backend: extract_state_backend(state_backend)?,
        trigger_store,
        client_data_store: extract_client_data_store(client_data_store)?,
        task_ids: task_ids
            .iter()
            .map(|(module, name)| parse_task_id("python", module, name))
            .collect::<PyResult<Vec<_>>>()?,
    };
    let mut apps = HashMap::new();
    apps.insert(app_id.to_owned(), instance);
    let monitor_config = MonitorConfig {
        bind,
        log_level: log_level.to_owned(),
    };
    let selected = app_id.to_owned();
    let handle = shared_runtime()?.spawn(async move {
        if let Err(error) = serve(apps, &selected, monitor_config).await {
            eprintln!("rustvello monitoring server stopped: {error}");
        }
    });
    Ok(PyMonitorServer {
        handle: Some(handle),
        address: bind.to_string(),
    })
}
