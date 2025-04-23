// tests/it/common.rs

use std::collections::HashMap;
use std::env;

use tokio::time::{sleep, Duration};

use crate::logger::init_logger;
use rustvello::{timing, Muse};
use rustvello_core::{ClientType, Config};
use rustvello_proto::*;

pub const TEST_ENDPOINT: &str = "http://localhost:8000";

/// Fetch the client type from the `rustvello_CLIENT_TYPE` environment variable.
pub fn client_type_from_env() -> ClientType {
    match env::var("rustvello_CLIENT_TYPE")
        .unwrap_or_else(|_| "Mock".to_string())
        .to_lowercase()
        .as_str()
    {
        "poet" => ClientType::Poet,
        _ => ClientType::Mock, // Default to Mock
    }
}

pub struct TestContext {
    pub config: Config,
    pub muse: Muse,
}

pub fn default_config(client_type: Option<ClientType>) -> Config {
    Config {
        endpoints: vec![TEST_ENDPOINT.to_string()],
        client_type: client_type.unwrap_or_else(client_type_from_env),
        recording_enabled: false,
        recording_path: None,
        recording_flush_interval: None,
        default_resolution: TimestampResolution::Milliseconds,
        element_kinds: vec![ElementKindRegistration::new(
            "server",
            None,
            "Server",
            "A server element kind",
        )],
        metric_definitions: vec![MetricDefinition::new(
            "cpu_usage",
            "CPU Usage",
            "The CPU usage of a server",
        )],
        initialization_interval: Some(Duration::from_millis(100)),
        cluster_monitor_interval: Some(Duration::from_millis(100)),
        max_reg_elem_retries: 3,
        max_endpoint_retries: None,
    }
}

impl TestContext {
    pub async fn new_with_config(config: Config) -> Self {
        init_logger();
        let mut muse = Muse::new(&config).expect("Failed to create the Muse");
        muse.initialize(Some(timing::adjust_duration_by_modifier(
            config
                .initialization_interval
                .unwrap_or(timing::INITIALIZATION_INTERVAL),
            10.0,
        )))
        .await
        .expect("Initialization issues");
        Self { config, muse }
    }

    pub async fn new(client_type: Option<ClientType>) -> Self {
        TestContext::new_with_config(default_config(client_type)).await
    }

    pub async fn new_recording(client_type: Option<ClientType>, record_path: String) -> Self {
        let mut config = default_config(client_type);
        config.recording_enabled = true;
        config.recording_path = Some(record_path);
        config.recording_flush_interval = Some(Duration::from_millis(1));
        TestContext::new_with_config(config).await
    }

    /// Waits until the metrics sending tasks ran at least once
    /// * sleeps 50% of the sending task interval
    pub async fn wait_for_metrics_sending_task(&self) {
        let metric_send_interval =
            timing::metric_sending_interval(self.muse.get_finest_resolution());
        let waiting = timing::adjust_duration_by_modifier(metric_send_interval, 2.0);
        sleep(waiting).await;
    }

    /// Waits until the cluster monitoring tasks ran at least once
    /// * sleeps 50% of the sending task interval
    pub async fn wait_for_cluster_monitoring_task(&self) {
        let interval = self
            .config
            .cluster_monitor_interval
            .unwrap_or(timing::CLUSTER_MONITOR_INTERVAL);
        timing::adjust_duration_by_modifier(interval, 1.5);
        let waiting = timing::adjust_duration_by_modifier(interval, 1.5);
        sleep(waiting).await;
    }

    pub async fn register_test_element(&self) -> LocalElementId {
        let local_elem_id = self
            .muse
            .register_element("server", "TestServer".to_string(), HashMap::new(), None)
            .await
            .expect("Failed to register element");

        let state = self.muse.get_state();
        let start_time = tokio::time::Instant::now();
        let elem_reg_duration =
            timing::element_registration_interval(self.muse.get_finest_resolution());
        let timeout = timing::adjust_duration_by_modifier(elem_reg_duration, 2.0);
        while state.get_element_id(&local_elem_id).is_none() && start_time.elapsed() < timeout {
            sleep(timing::element_registration_interval(
                self.muse.get_finest_resolution(),
            ))
            .await;
        }

        local_elem_id
    }
}
