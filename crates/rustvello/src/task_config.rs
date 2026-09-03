//! Task configuration overrides and resolution helpers.
//!
//! Extracted from `app.rs` to keep the application shell focused on
//! lifecycle management and task registration.

use rustvello_proto::call::SerializedArguments;
use rustvello_proto::config::TaskConfig;
use rustvello_proto::status::ConcurrencyControlType;

/// Partial task config overrides (each field is optional).
///
/// Applied in layers: global defaults → per-task TOML → per-task env vars.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskConfigOverride {
    pub queue: Option<String>,
    pub priority: Option<f64>,
    pub max_retries: Option<u32>,
    pub concurrency_control: Option<ConcurrencyControlType>,
    pub running_concurrency: Option<Option<u32>>,
    pub registration_concurrency: Option<ConcurrencyControlType>,
    pub cache_results: Option<bool>,
    pub key_arguments: Option<Vec<String>>,
    pub retry_for_errors: Option<Vec<String>>,
    pub disable_cache_args: Option<Vec<String>>,
    pub on_diff_non_key_args_raise: Option<bool>,
    pub parallel_batch_size: Option<usize>,
    pub is_workflow_task: Option<bool>,
    pub reroute_on_cc: Option<bool>,
    pub blocking: Option<bool>,
}

pub(crate) fn concurrency_arguments(
    mode: ConcurrencyControlType,
    key_arguments: &[String],
    args: &SerializedArguments,
) -> Option<SerializedArguments> {
    match mode {
        ConcurrencyControlType::Unlimited => None,
        ConcurrencyControlType::Task => Some(SerializedArguments::new()),
        ConcurrencyControlType::Argument if key_arguments.is_empty() => Some(args.clone()),
        ConcurrencyControlType::Argument => {
            let mut filtered = SerializedArguments::new();
            for key in key_arguments {
                if let Some(value) = args.0.get(key) {
                    filtered.insert(key, value.clone());
                }
            }
            Some(filtered)
        }
        ConcurrencyControlType::None => Some(args.clone()),
        _ => Some(args.clone()),
    }
}

impl TaskConfigOverride {
    /// Apply non-None fields of this override onto the given config.
    pub fn apply_to(&self, config: &mut TaskConfig) {
        if let Some(ref v) = self.queue {
            config.queue.clone_from(v);
        }
        if let Some(v) = self.priority {
            config.priority = v;
        }
        if let Some(v) = self.max_retries {
            config.max_retries = v;
        }
        if let Some(v) = self.concurrency_control {
            config.concurrency_control = v;
        }
        if let Some(v) = self.running_concurrency {
            config.running_concurrency = v;
        }
        if let Some(v) = self.registration_concurrency {
            config.registration_concurrency = v;
        }
        if let Some(v) = self.cache_results {
            config.cache_results = v;
        }
        if let Some(ref v) = self.key_arguments {
            config.key_arguments = v.clone();
        }
        if let Some(ref v) = self.retry_for_errors {
            config.retry_for_errors = v.clone();
        }
        if let Some(ref v) = self.disable_cache_args {
            config.disable_cache_args = v.clone();
        }
        if let Some(v) = self.on_diff_non_key_args_raise {
            config.on_diff_non_key_args_raise = v;
        }
        if let Some(v) = self.parallel_batch_size {
            config.parallel_batch_size = v;
        }
        if let Some(v) = self.is_workflow_task {
            config.is_workflow_task = v;
        }
        if let Some(v) = self.reroute_on_cc {
            config.reroute_on_cc = v;
        }
        if let Some(v) = self.blocking {
            config.blocking = v;
        }
    }
}

/// Parse a concurrency control type from a string (env vars, TOML values).
pub(crate) fn parse_concurrency_control_type(s: &str) -> Option<ConcurrencyControlType> {
    match s.to_lowercase().as_str() {
        "unlimited" => Some(ConcurrencyControlType::Unlimited),
        "task" => Some(ConcurrencyControlType::Task),
        "argument" => Some(ConcurrencyControlType::Argument),
        "none" => Some(ConcurrencyControlType::None),
        _ => Option::None,
    }
}

/// Apply task config overrides from environment variables with the given prefix.
///
/// Reads `{prefix}MAX_RETRIES`, `{prefix}CONCURRENCY_CONTROL`, etc.
pub(crate) fn apply_task_env_overrides(prefix: &str, config: &mut TaskConfig) {
    fn env(prefix: &str, key: &str) -> Option<String> {
        std::env::var(format!("{prefix}{key}")).ok()
    }

    if let Some(val) = env(prefix, "MAX_RETRIES") {
        if let Ok(n) = val.parse::<u32>() {
            config.max_retries = n;
        }
    }
    if let Some(val) = env(prefix, "QUEUE") {
        config.queue = val;
    }
    if let Some(val) = env(prefix, "PRIORITY") {
        if let Ok(priority) = val.parse::<f64>() {
            config.priority = priority;
        }
    }
    if let Some(val) = env(prefix, "CONCURRENCY_CONTROL") {
        if let Some(cc) = parse_concurrency_control_type(&val) {
            config.concurrency_control = cc;
        }
    }
    if let Some(val) = env(prefix, "RUNNING_CONCURRENCY") {
        config.running_concurrency = val.parse::<u32>().ok();
    }
    if let Some(val) = env(prefix, "CACHE_RESULTS") {
        if let Ok(b) = val.parse::<bool>() {
            config.cache_results = b;
        }
    }
    if let Some(val) = env(prefix, "IS_WORKFLOW_TASK") {
        if let Ok(b) = val.parse::<bool>() {
            config.is_workflow_task = b;
        }
    }
    if let Some(val) = env(prefix, "REROUTE_ON_CC") {
        if let Ok(b) = val.parse::<bool>() {
            config.reroute_on_cc = b;
        }
    }
}
