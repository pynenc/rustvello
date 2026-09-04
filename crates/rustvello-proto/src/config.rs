use cistell_core::Config;
use serde::{Deserialize, Serialize};

use crate::status::ConcurrencyControlType;

pub const DEFAULT_QUEUE: &str = "default";
pub const MIN_PRIORITY: f64 = -100.0;
pub const MAX_PRIORITY: f64 = 100.0;
pub const DEFAULT_PRIORITY: f64 = 0.0;

/// Broker priority override matched against a task ID with shell-style wildcards.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BrokerPriorityRule {
    pub task_id: String,
    pub priority: f64,
}

impl std::str::FromStr for BrokerPriorityRule {
    type Err = ConfigParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (task_id, priority) = value.rsplit_once('=').ok_or_else(|| {
            ConfigParseError(format!(
                "invalid broker priority rule {value:?}; expected task-pattern=priority"
            ))
        })?;
        let priority = priority
            .parse::<f64>()
            .map_err(|_| ConfigParseError(format!("invalid priority in broker rule {value:?}")))?;
        if task_id.is_empty()
            || !priority.is_finite()
            || !(MIN_PRIORITY..=MAX_PRIORITY).contains(&priority)
        {
            return Err(ConfigParseError(format!(
                "invalid broker priority rule {value:?}; priority must be between {MIN_PRIORITY} and {MAX_PRIORITY}"
            )));
        }
        Ok(Self {
            task_id: task_id.to_owned(),
            priority,
        })
    }
}

/// Error type for parsing config enum variants from strings.
#[derive(Debug)]
pub struct ConfigParseError(pub String);

impl std::fmt::Display for ConfigParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ConfigParseError {}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum LogFormat {
    /// Human-readable text with optional ANSI colors.
    Text,
    /// Structured JSON (one object per line).
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self::Text
    }
}

impl std::str::FromStr for LogFormat {
    type Err = ConfigParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(ConfigParseError(format!("invalid LogFormat: {s}"))),
        }
    }
}

/// Controls how task arguments are displayed in logs and monitoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ArgumentPrintMode {
    /// Show complete argument values.
    Full,
    /// Show only argument names (keys).
    Keys,
    /// Show truncated argument values (up to `truncate_arguments_length`).
    Truncated,
    /// Hide all argument values.
    Hidden,
}

/// Strategy used by runners when consuming multiple logical queues.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueueSelectionStrategy {
    RoundRobin,
    Ordered,
    Random,
}

impl Default for QueueSelectionStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

impl std::str::FromStr for QueueSelectionStrategy {
    type Err = ConfigParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "round_robin" => Ok(Self::RoundRobin),
            "ordered" => Ok(Self::Ordered),
            "random" => Ok(Self::Random),
            _ => Err(ConfigParseError(format!(
                "invalid QueueSelectionStrategy: {value}"
            ))),
        }
    }
}

impl Default for ArgumentPrintMode {
    fn default() -> Self {
        Self::Truncated
    }
}

impl std::str::FromStr for ArgumentPrintMode {
    type Err = ConfigParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "keys" => Ok(Self::Keys),
            "truncated" => Ok(Self::Truncated),
            "hidden" => Ok(Self::Hidden),
            _ => Err(ConfigParseError(format!("invalid ArgumentPrintMode: {s}"))),
        }
    }
}

/// Per-task configuration options.
///
/// Mirrors pynenc's `ConfigTask` with settings for retries, concurrency,
/// and execution behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TaskConfig {
    /// Logical broker queue used for this task.
    pub queue: String,
    /// Broker priority within the task queue. Higher values run first.
    pub priority: f64,
    /// Maximum number of retry attempts (0 = no retries)
    pub max_retries: u32,
    /// Error type names that should trigger a retry.
    /// Empty means only explicit `RetryError` triggers retries.
    pub retry_for_errors: Vec<String>,
    /// Concurrency control strategy for running invocations
    pub concurrency_control: ConcurrencyControlType,
    /// Maximum number of concurrent invocations (when using Task concurrency)
    pub running_concurrency: Option<u32>,
    /// Concurrency control strategy applied at registration time
    pub registration_concurrency: ConcurrencyControlType,
    /// Whether to cache results for identical arguments
    pub cache_results: bool,
    /// Parameter names used as concurrency keys (when using Task concurrency)
    pub key_arguments: Vec<String>,
    /// Argument names to exclude from cache key computation
    pub disable_cache_args: Vec<String>,
    /// Raise an error when a call with matching key args but different
    /// non-key args is registered (prevents silent overwrites)
    pub on_diff_non_key_args_raise: bool,
    /// Batch size for `parallelize()` — how many calls to submit at once
    pub parallel_batch_size: usize,
    /// Whether this task explicitly defines a workflow or sub-workflow root.
    #[serde(default, alias = "force_new_workflow")]
    pub is_workflow_task: bool,
    /// Reroute an invocation when it hits concurrency control limits
    pub reroute_on_cc: bool,
    /// Whether to run this task on a blocking thread (`tokio::task::spawn_blocking`).
    /// Use for CPU-bound or synchronous I/O tasks that could starve the async executor.
    pub blocking: bool,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            queue: DEFAULT_QUEUE.to_owned(),
            priority: DEFAULT_PRIORITY,
            max_retries: 0,
            retry_for_errors: Vec::new(),
            concurrency_control: ConcurrencyControlType::Unlimited,
            running_concurrency: None,
            registration_concurrency: ConcurrencyControlType::Unlimited,
            cache_results: false,
            key_arguments: Vec::new(),
            disable_cache_args: Vec::new(),
            on_diff_non_key_args_raise: false,
            parallel_batch_size: 100,
            is_workflow_task: false,
            reroute_on_cc: false,
            blocking: false,
        }
    }
}

/// Global application configuration.
#[derive(Config, Debug, Clone, Serialize, Deserialize)]
#[config(prefix = "RUSTVELLO", group = "app")]
#[non_exhaustive]
pub struct AppConfig {
    /// Unique identifier for this application instance
    #[config(default = "rustvello")]
    pub app_id: String,
    /// Logical queues accepted by the broker. `default` is always present.
    #[config(default = vec!["default".to_owned()])]
    pub broker_queues: Vec<String>,
    /// Queues consumed by runners. Empty means all broker queues.
    #[config(default = vec![])]
    pub runner_queues: Vec<String>,
    /// Selection strategy when a runner consumes multiple queues.
    #[config(default = QueueSelectionStrategy::RoundRobin)]
    pub queue_selection_strategy: QueueSelectionStrategy,
    /// Task-ID wildcard rules. The highest matching priority overrides task config.
    #[config(default = vec![])]
    pub priority_rules: Vec<BrokerPriorityRule>,
    /// Force synchronous task execution (for development/testing)
    #[config(default = false)]
    pub dev_mode_force_sync: bool,
    /// Maximum time an invocation can be pending before recovery (seconds)
    #[config(default = 300u64)]
    pub max_pending_seconds: u64,
    /// Heartbeat interval for runners (seconds)
    #[config(default = 30u64)]
    pub heartbeat_interval_seconds: u64,
    /// Runners without a heartbeat for this long are considered dead (seconds)
    #[config(default = 300u64)]
    pub runner_dead_after_seconds: u64,
    /// How often to scan for stale invocations (seconds)
    #[config(default = 60u64)]
    pub recovery_check_interval_seconds: u64,
    /// Whether to display arguments in logs
    #[config(default = true)]
    pub print_arguments: bool,
    /// How arguments are displayed in logs/monitoring
    #[config(default = ArgumentPrintMode::Truncated)]
    pub argument_print_mode: ArgumentPrintMode,
    /// Maximum length for truncated argument display
    #[config(default = 32usize)]
    pub truncate_arguments_length: usize,
    /// Cron expression for recovering pending invocations (empty = disabled)
    #[config(default = "*/5 * * * *")]
    pub recover_pending_cron: String,
    /// Cron expression for recovering running invocations from dead runners (empty = disabled)
    #[config(default = "*/15 * * * *")]
    pub recover_running_cron: String,
    /// Module paths to scan for trigger task registration
    #[config(default = vec![])]
    pub trigger_task_modules: Vec<String>,
    /// TTL for cached invocation status checks (seconds, 0 = no cache)
    #[config(default = 0.0f64)]
    pub cached_status_time_seconds: f64,
    /// Logging level (trace, debug, info, warn, error)
    #[config(default = "info")]
    pub logging_level: String,
    /// Log output format (Text or Json)
    #[config(default = LogFormat::Text)]
    pub log_format: LogFormat,
    /// Whether to use ANSI colors in text format (None = auto-detect TTY)
    pub log_use_colors: Option<bool>,
    /// Whether to show compact context (abbreviated names, truncated IDs)
    #[config(default = true)]
    pub compact_log_context: bool,
    /// Blocking control — whether orchestrator uses blocking/waiting semantics
    #[config(default = true)]
    pub blocking_control: bool,
    /// Hours after which final invocations can be auto-purged (0 = disabled)
    #[config(default = 0.0f64)]
    pub auto_final_invocation_purge_hours: f64,
    /// Scheduler evaluation interval in seconds (trigger scheduler)
    #[config(default = 60u64)]
    pub scheduler_interval_seconds: u64,
    /// Whether the scheduler is enabled
    #[config(default = true)]
    pub enable_scheduler: bool,
    /// Total cycle interval for atomic global services (triggers, recovery, etc.)
    /// in minutes. The interval is divided equally among all active runners.
    #[config(default = 5.0f64)]
    pub atomic_service_interval_minutes: f64,
    /// Safety margin (minutes) subtracted from each runner's time slot to prevent
    /// overlapping execution of atomic services across distributed runners.
    #[config(default = 1.0f64)]
    pub atomic_service_spread_margin_minutes: f64,
    /// How frequently an individual runner checks if it should execute atomic
    /// global services (minutes). Should be less than `atomic_service_interval_minutes`.
    #[config(default = 0.5f64)]
    pub atomic_service_check_interval_minutes: f64,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            app_id: "rustvello".to_string(),
            broker_queues: vec![DEFAULT_QUEUE.to_owned()],
            runner_queues: Vec::new(),
            queue_selection_strategy: QueueSelectionStrategy::RoundRobin,
            priority_rules: Vec::new(),
            dev_mode_force_sync: false,
            max_pending_seconds: 300,
            heartbeat_interval_seconds: 30,
            runner_dead_after_seconds: 300,
            recovery_check_interval_seconds: 60,
            print_arguments: true,
            argument_print_mode: ArgumentPrintMode::Truncated,
            truncate_arguments_length: 32,
            recover_pending_cron: "*/5 * * * *".to_owned(),
            recover_running_cron: "*/15 * * * *".to_owned(),
            trigger_task_modules: Vec::new(),
            cached_status_time_seconds: 0.0,
            logging_level: "info".to_string(),
            log_format: LogFormat::Text,
            log_use_colors: None,
            compact_log_context: true,
            blocking_control: true,
            auto_final_invocation_purge_hours: 0.0,
            scheduler_interval_seconds: 60,
            enable_scheduler: true,
            atomic_service_interval_minutes: 5.0,
            atomic_service_spread_margin_minutes: 1.0,
            atomic_service_check_interval_minutes: 0.5,
        }
    }
}

impl AppConfig {
    pub fn new(app_id: impl Into<String>) -> Self {
        Self {
            app_id: app_id.into(),
            ..Default::default()
        }
    }
}

/// Configuration for the client data store subsystem.
///
/// Controls when serialized values are stored externally vs. returned inline,
/// local LRU caching behavior, and size monitoring thresholds.
///
/// Mirrors pynenc's `ConfigClientDataStore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ClientDataStoreConfig {
    /// Bypass entirely — always return inline serialized strings.
    pub disabled: bool,
    /// Minimum serialized string length (bytes) to store externally.
    /// Values shorter than this are returned inline.
    pub min_size_to_cache: usize,
    /// Maximum serialized string length (bytes) to store externally.
    /// Values longer than this are returned inline with a warning.
    /// 0 = unlimited (no maximum).
    pub max_size_to_cache: usize,
    /// Maximum number of entries in the process-local LRU cache.
    pub local_cache_size: usize,
    /// Log a warning when any single value exceeds this size (bytes).
    pub warn_threshold: usize,
}

impl Default for ClientDataStoreConfig {
    fn default() -> Self {
        Self {
            disabled: false,
            min_size_to_cache: 1024,
            max_size_to_cache: 0,
            local_cache_size: 1024,
            warn_threshold: 10_485_760,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_config_defaults() {
        let tc = TaskConfig::default();
        assert_eq!(tc.queue, DEFAULT_QUEUE);
        assert_eq!(tc.priority, DEFAULT_PRIORITY);
        assert_eq!(tc.max_retries, 0);
        assert_eq!(tc.concurrency_control, ConcurrencyControlType::Unlimited);
        assert!(tc.running_concurrency.is_none());
        assert_eq!(
            tc.registration_concurrency,
            ConcurrencyControlType::Unlimited
        );
        assert!(!tc.cache_results);
        assert!(tc.key_arguments.is_empty());
        assert!(!tc.is_workflow_task);
        assert!(!tc.reroute_on_cc);
    }

    #[test]
    fn app_config_defaults() {
        let ac = AppConfig::default();
        assert_eq!(ac.app_id, "rustvello");
        assert!(!ac.dev_mode_force_sync);
        assert_eq!(ac.max_pending_seconds, 300);
        assert_eq!(ac.heartbeat_interval_seconds, 30);
    }

    #[test]
    fn app_config_new() {
        let ac = AppConfig::new("my-app");
        assert_eq!(ac.app_id, "my-app");
        assert!(!ac.dev_mode_force_sync);
    }

    #[test]
    fn serde_round_trip_task_config() {
        let tc = TaskConfig {
            queue: "critical".to_string(),
            priority: 12.5,
            max_retries: 3,
            retry_for_errors: vec!["TimeoutError".to_string()],
            concurrency_control: ConcurrencyControlType::Task,
            running_concurrency: Some(5),
            registration_concurrency: ConcurrencyControlType::Argument,
            cache_results: true,
            key_arguments: vec!["order_id".to_string()],
            disable_cache_args: vec!["timestamp".to_string()],
            on_diff_non_key_args_raise: true,
            parallel_batch_size: 50,
            is_workflow_task: true,
            reroute_on_cc: true,
            blocking: false,
        };
        let json = serde_json::to_string(&tc).unwrap();
        let back: TaskConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.queue, "critical");
        assert_eq!(back.priority, 12.5);
        assert_eq!(back.max_retries, 3);
        assert_eq!(back.concurrency_control, ConcurrencyControlType::Task);
        assert_eq!(back.running_concurrency, Some(5));
        assert_eq!(
            back.registration_concurrency,
            ConcurrencyControlType::Argument
        );
        assert!(back.cache_results);
        assert_eq!(back.key_arguments, vec!["order_id"]);
        assert!(back.is_workflow_task);
        assert!(back.reroute_on_cc);
        assert!(!back.blocking);
    }

    #[test]
    fn serde_round_trip_app_config() {
        let ac = AppConfig::new("test");
        let json = serde_json::to_string(&ac).unwrap();
        let back: AppConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.app_id, "test");
    }

    #[test]
    fn client_data_store_config_defaults() {
        let c = ClientDataStoreConfig::default();
        assert!(!c.disabled);
        assert_eq!(c.min_size_to_cache, 1024);
        assert_eq!(c.max_size_to_cache, 0);
        assert_eq!(c.local_cache_size, 1024);
        assert_eq!(c.warn_threshold, 10_485_760);
    }

    #[test]
    fn serde_round_trip_client_data_store_config() {
        let c = ClientDataStoreConfig {
            disabled: true,
            min_size_to_cache: 512,
            max_size_to_cache: 1_000_000,
            local_cache_size: 256,
            warn_threshold: 5_000_000,
        };
        let json = serde_json::to_string(&c).unwrap();
        let back: ClientDataStoreConfig = serde_json::from_str(&json).unwrap();
        assert!(back.disabled);
        assert_eq!(back.min_size_to_cache, 512);
        assert_eq!(back.max_size_to_cache, 1_000_000);
        assert_eq!(back.local_cache_size, 256);
        assert_eq!(back.warn_threshold, 5_000_000);
    }
}
