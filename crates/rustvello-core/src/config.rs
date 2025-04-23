// crates/rustvello/src/config.rs

use serde::{Deserialize, Serialize};

use super::{MuseError, MuseResult};
use rustvello_proto::{ElementKindRegistration, MetricDefinition, TimestampResolution};
use tokio::time::Duration;

/// Specifies the type of client to use with the Muse system.
///
/// - `Poet`: Communicates with the Poet service.
/// - `Mock`: Uses a mock client for testing purposes.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub enum ClientType {
    /// Communicates with the Poet service.
    Poet,
    /// Uses a mock client for testing.
    Mock,
}

/// Configuration settings for the Muse client.
///
/// The `Config` struct contains all necessary parameters to initialize the Muse client.
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq)]
pub struct Config {
    /// List of endpoint URLs for the Muse client.
    pub endpoints: Vec<String>,
    /// The type of client to use (`Poet` or `Mock`).
    pub client_type: ClientType,
    /// Enables event recording if set to `true`.
    pub recording_enabled: bool,
    /// File path for recording events (required if `recording_enabled` is `true`).
    pub recording_path: Option<String>,
    /// Interval for flushing the recorder when recording is enabled.
    pub recording_flush_interval: Option<Duration>,
    /// Default timestamp resolution for metrics.
    pub default_resolution: TimestampResolution,
    /// List of element kinds to register upon initialization.
    pub element_kinds: Vec<ElementKindRegistration>,
    /// List of metric definitions available for reporting.
    pub metric_definitions: Vec<MetricDefinition>,
    /// Interval for initialization tasks (optional).
    pub initialization_interval: Option<Duration>,
    /// Interval for cluster monitoring tasks (optional).
    pub cluster_monitor_interval: Option<Duration>,
    /// Maximum number of retries for element registration.
    pub max_reg_elem_retries: usize,
    /// Maximum number of Poet endpoints retries, None for no limit.
    pub max_endpoint_retries: Option<usize>,
}

impl Config {
    /// Creates a new `Config` instance with the provided settings.
    ///
    /// # Arguments
    ///
    /// - `endpoints`: A vector of endpoint URLs.
    /// - `client_type`: The client type to use.
    /// - `recording_enabled`: Enables event recording.
    /// - `recording_path`: File path for recording events.
    /// - `recording_flush_interval`: Interval to flush recording events.
    /// - `default_resolution`: Default timestamp resolution.
    /// - `element_kinds`: Element kinds to register.
    /// - `metric_definitions`: Metric definitions for reporting.
    /// - `initialization_interval`: Interval for the initialization task.
    /// - `cluster_monitor_interval`: Interval for cluster monitoring.
    /// - `max_reg_elem_retries`: Max retries for element registration.
    /// - `max_endpoint_retries`: Max retries for Poet endpoints.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Configuration`] if validation fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use rustvello_core::prelude::*;
    /// use rustvello_proto::prelude::*;
    ///
    /// let config = Config::new(
    ///     vec!["http://localhost:8080".to_string()],
    ///     ClientType::Poet,
    ///     false,
    ///     None,
    ///     None,
    ///     TimestampResolution::Milliseconds,
    ///     vec![ElementKindRegistration::new("kind_code", Some("parent_code"), "kind_name", "description")],
    ///     vec![MetricDefinition::new("metric_code", "metric_name", "description")],
    ///     Some(std::time::Duration::from_secs(60)),
    ///     Some(std::time::Duration::from_secs(60)),
    ///     3,
    ///     None,
    /// ).expect("Failed to create config");
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoints: Vec<String>,
        client_type: ClientType,
        recording_enabled: bool,
        recording_path: Option<String>,
        recording_flush_interval: Option<Duration>,
        default_resolution: TimestampResolution,
        element_kinds: Vec<ElementKindRegistration>,
        metric_definitions: Vec<MetricDefinition>,
        initialization_interval: Option<Duration>,
        cluster_monitor_interval: Option<Duration>,
        max_reg_elem_retries: usize,
        max_endpoint_retries: Option<usize>,
    ) -> MuseResult<Self> {
        let config = Self {
            endpoints,
            client_type,
            recording_enabled,
            recording_path,
            recording_flush_interval,
            default_resolution,
            element_kinds,
            metric_definitions,
            initialization_interval,
            cluster_monitor_interval,
            max_reg_elem_retries,
            max_endpoint_retries,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validates the configuration settings.
    ///
    /// Ensures all required fields are properly set.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Configuration`] if any validation check fails.
    pub fn validate(&self) -> MuseResult<()> {
        if self.client_type == ClientType::Poet && self.endpoints.is_empty() {
            return Err(MuseError::Configuration(
                "At least one endpoint needs to be specified for Poet client.".to_string(),
            ));
        }
        if self.element_kinds.is_empty() {
            return Err(MuseError::Configuration(
                "Element kinds cannot be empty.".to_string(),
            ));
        }
        if self.metric_definitions.is_empty() {
            return Err(MuseError::Configuration(
                "Metric definitions cannot be empty.".to_string(),
            ));
        }
        if self.recording_enabled && self.recording_path.is_none() {
            return Err(MuseError::Configuration(
                "Recording enabled without a file path.".to_string(),
            ));
        }
        Ok(())
    }

    /// Compares two configurations and returns a vector of strings describing the differences.
    ///
    /// # Arguments
    /// * `other` - The configuration to compare with.
    ///
    /// # Returns
    /// A vector of strings where each entry describes a difference between the two configurations.
    pub fn diff(&self, other: &Config) -> Vec<String> {
        let mut differences = Vec::new();

        if self.endpoints != other.endpoints {
            differences.push(format!(
                "endpoints: {:?} != {:?}",
                self.endpoints, other.endpoints
            ));
        }

        if self.client_type != other.client_type {
            differences.push(format!(
                "client_type: {:?} != {:?}",
                self.client_type, other.client_type
            ));
        }

        if self.recording_enabled != other.recording_enabled {
            differences.push(format!(
                "recording_enabled: {} != {}",
                self.recording_enabled, other.recording_enabled
            ));
        }

        if self.recording_path != other.recording_path {
            differences.push(format!(
                "recording_path: {:?} != {:?}",
                self.recording_path, other.recording_path
            ));
        }

        if self.default_resolution != other.default_resolution {
            differences.push(format!(
                "default_resolution: {:?} != {:?}",
                self.default_resolution, other.default_resolution
            ));
        }

        if self.element_kinds != other.element_kinds {
            differences.push(format!(
                "element_kinds: {:?} != {:?}",
                self.element_kinds, other.element_kinds
            ));
        }

        if self.metric_definitions != other.metric_definitions {
            differences.push(format!(
                "metric_definitions: {:?} != {:?}",
                self.metric_definitions, other.metric_definitions
            ));
        }

        if self.cluster_monitor_interval != other.cluster_monitor_interval {
            differences.push(format!(
                "cluster_monitor_interval: {:?} != {:?}",
                self.cluster_monitor_interval, other.cluster_monitor_interval
            ));
        }

        if self.max_reg_elem_retries != other.max_reg_elem_retries {
            differences.push(format!(
                "max_reg_elem_retries: {} != {}",
                self.max_reg_elem_retries, other.max_reg_elem_retries
            ));
        }

        if self.max_endpoint_retries != other.max_endpoint_retries {
            differences.push(format!(
                "max_endpoint_retries: {:?} != {:?}",
                self.max_endpoint_retries, other.max_endpoint_retries
            ));
        }

        differences
    }

    /// Helper function to pretty-print the differences as a single string.
    ///
    /// # Arguments
    /// * `other` - The configuration to compare with.
    ///
    /// # Returns
    /// A single string containing all differences separated by newlines.
    pub fn pretty_diff(&self, other: &Config) -> String {
        self.diff(other).join("\n")
    }

    /// Checks if two Config instances are equivalent based on relevant fields,
    /// ignoring `recording_enabled` and `recording_path`.
    ///
    /// # Arguments
    /// - `other`: The `Config` instance to compare with.
    ///
    /// # Returns
    /// `true` if the relevant fields are equal, `false` otherwise.
    pub fn is_relevantly_equal(&self, other: &Config) -> bool {
        self.endpoints == other.endpoints
            && self.client_type == other.client_type
            && self.default_resolution == other.default_resolution
            && self.element_kinds == other.element_kinds
            && self.metric_definitions == other.metric_definitions
            && self.cluster_monitor_interval == other.cluster_monitor_interval
            && self.max_reg_elem_retries == other.max_reg_elem_retries
            && self.max_endpoint_retries == other.max_endpoint_retries
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_relevant_config_comparison() {
        let config1 = Config {
            endpoints: vec!["http://localhost:8000".to_string()],
            client_type: ClientType::Poet,
            recording_enabled: true, // Ignored for comparison
            recording_path: Some("recording.json".to_string()), // Ignored for comparison
            recording_flush_interval: Some(Duration::from_millis(1)),
            default_resolution: TimestampResolution::Milliseconds,
            element_kinds: vec![ElementKindRegistration::new(
                "kind_code",
                None,
                "kind_name",
                "desc",
            )],
            metric_definitions: vec![MetricDefinition::new("metric_code", "metric_name", "desc")],
            initialization_interval: Some(Duration::from_secs(60)),
            cluster_monitor_interval: Some(Duration::from_secs(60)),
            max_reg_elem_retries: 3,
            max_endpoint_retries: None,
        };

        let config2 = Config {
            endpoints: vec!["http://localhost:8000".to_string()],
            client_type: ClientType::Poet,
            recording_enabled: false,
            recording_path: None,
            recording_flush_interval: None,
            default_resolution: TimestampResolution::Milliseconds,
            element_kinds: vec![ElementKindRegistration::new(
                "kind_code",
                None,
                "kind_name",
                "desc",
            )],
            metric_definitions: vec![MetricDefinition::new("metric_code", "metric_name", "desc")],
            initialization_interval: Some(Duration::from_secs(60)),
            cluster_monitor_interval: Some(Duration::from_secs(60)),
            max_reg_elem_retries: 3,
            max_endpoint_retries: None,
        };

        assert!(
            config1.is_relevantly_equal(&config2),
            "Configs should be considered equal based on relevant fields"
        );
    }
}
