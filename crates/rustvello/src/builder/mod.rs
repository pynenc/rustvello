mod build;
mod presets;
pub mod sub_builders;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use cistell_core::{ConfigValue, MapSource};
use rustvello_core::broker::Broker;
use rustvello_core::client_data_store::ClientDataStore;
use rustvello_core::error::{RustvelloError, RustvelloResult};
use rustvello_core::orchestrator::InvocationControlBackend;
use rustvello_core::state_backend::StateBackend;
use rustvello_core::task::TaskModule;
use rustvello_core::trigger::TriggerStore;
use rustvello_proto::config::{ArgumentPrintMode, ClientDataStoreConfig, LogFormat};

use crate::task_config::TaskConfigOverride;

/// Deferred backend configuration — stores connection parameters without
/// establishing any connections.  Actual I/O happens in [`RustvelloBuilder::build()`].
#[derive(Debug, Clone)]
pub(super) enum BackendPreset {
    Memory,
    #[cfg(feature = "sqlite")]
    Sqlite {
        path: String,
        app_id: String,
    },
    #[cfg(feature = "redis")]
    Redis {
        uri: String,
        app_id: String,
    },
    #[cfg(feature = "postgres")]
    Postgres {
        connection_string: String,
        app_id: String,
    },
    #[cfg(all(feature = "postgres", feature = "tls"))]
    PostgresTls {
        connection_string: String,
        app_id: String,
    },
    #[cfg(feature = "mongodb")]
    MongoDB {
        uri: String,
        db_name: String,
        app_id: String,
    },
    #[cfg(feature = "mongodb3")]
    Mongo3 {
        uri: String,
        db_name: String,
        app_id: String,
    },
}

/// Deferred RabbitMQ broker configuration (broker-only preset).
#[derive(Debug, Clone)]
#[cfg(feature = "rabbitmq")]
pub(super) struct RabbitMqConfig {
    pub uri: String,
    pub prefix: String,
}

/// Fluent builder for constructing a [`RustvelloApp`].
///
/// Provides an ergonomic API for configuring backends, loading settings
/// from environment variables or TOML files, and applying dev-mode presets.
///
/// # Priority order (highest wins)
///
/// 1. Programmatic — explicit builder method calls
/// 2. Environment variables — always read automatically
/// 3. Config file — via [`from_file()`](Self::from_file)
/// 4. pyproject.toml — `[tool.rustvello.app]`
/// 5. Defaults
///
/// # Examples
///
/// ```rust,no_run
/// use rustvello::prelude::*;
///
/// // Minimal in-memory defaults
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let app = Rustvello::builder()
///     .app_id("my-app")
///     .build().await?;
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct RustvelloBuilder {
    /// Programmatic field overrides — highest config priority (rank 10).
    pub(super) programmatic: MapSource,
    /// Paths of TOML config files to load (in order of `.from_file()` calls).
    pub(super) file_paths: Vec<std::path::PathBuf>,
    /// Whether to include env vars in the resolver chain (opt-in via `from_env()`).
    pub(super) use_env: bool,
    pub(super) broker: Option<Arc<dyn Broker>>,
    pub(super) orchestrator: Option<Arc<dyn InvocationControlBackend>>,
    pub(super) state_backend: Option<Arc<dyn StateBackend>>,
    pub(super) client_data_store: Option<Arc<dyn ClientDataStore>>,
    pub(super) trigger_store: Option<Arc<dyn TriggerStore>>,
    pub(super) client_data_store_config: ClientDataStoreConfig,
    /// Per-task config overrides from TOML [tasks.<name>] sections.
    pub(super) task_config_overrides: HashMap<String, TaskConfigOverride>,
    /// Global task defaults from TOML [task_defaults] section.
    pub(super) task_defaults_override: TaskConfigOverride,
    /// Task modules to register during build.
    pub(super) task_modules: Vec<Box<dyn TaskModule>>,
    /// Whether auto_discover_tasks was called.
    pub(super) auto_discover: bool,
    /// Deferred backend preset (connection created in `.build()`).
    pub(super) backend_preset: Option<BackendPreset>,
    /// Deferred RabbitMQ config — broker-only, combined with another preset.
    #[cfg(feature = "rabbitmq")]
    pub(super) rabbitmq_config: Option<RabbitMqConfig>,
}

/// Namespace type for `Rustvello::builder()`.
pub struct Rustvello;

impl Rustvello {
    /// Create a new [`RustvelloBuilder`] with default settings.
    pub fn builder() -> RustvelloBuilder {
        RustvelloBuilder::new()
    }
}

impl std::fmt::Debug for RustvelloBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustvelloBuilder")
            .field("auto_discover", &self.auto_discover)
            .finish_non_exhaustive()
    }
}

impl RustvelloBuilder {
    fn new() -> Self {
        Self {
            programmatic: MapSource::new("programmatic"),
            file_paths: Vec::new(),
            use_env: false,
            broker: None,
            orchestrator: None,
            state_backend: None,
            client_data_store: None,
            trigger_store: None,
            client_data_store_config: ClientDataStoreConfig::default(),
            task_config_overrides: HashMap::new(),
            task_defaults_override: TaskConfigOverride::default(),
            task_modules: Vec::new(),
            auto_discover: false,
            backend_preset: None,
            #[cfg(feature = "rabbitmq")]
            rabbitmq_config: None,
        }
    }

    // -----------------------------------------------------------------------
    // Config field setters
    // -----------------------------------------------------------------------

    /// Set the application identifier.
    pub fn app_id(mut self, id: impl Into<String>) -> Self {
        self.programmatic
            .insert("app_id", ConfigValue::String(id.into()));
        self
    }

    /// Enable or disable synchronous dev-mode execution.
    pub fn dev_mode(mut self, enabled: bool) -> Self {
        self.programmatic
            .insert("dev_mode_force_sync", ConfigValue::Bool(enabled));
        self
    }

    /// Set the maximum pending time for invocations (in seconds).
    pub fn max_pending_seconds(mut self, seconds: u64) -> Self {
        self.programmatic
            .insert("max_pending_seconds", ConfigValue::Integer(seconds as i64));
        self
    }

    /// Set the heartbeat interval for runners (in seconds).
    pub fn heartbeat_interval(mut self, seconds: u64) -> Self {
        self.programmatic.insert(
            "heartbeat_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set the argument print mode (Full, Keys, Truncated, Hidden).
    pub fn argument_print_mode(mut self, mode: ArgumentPrintMode) -> Self {
        let s = argument_print_mode_str(mode);
        self.programmatic
            .insert("argument_print_mode", ConfigValue::String(s.to_string()));
        self.programmatic
            .insert("print_arguments", ConfigValue::Bool(true));
        self
    }

    /// Hide all argument values in logs and display.
    pub fn hide_arguments(mut self) -> Self {
        self.programmatic
            .insert("print_arguments", ConfigValue::Bool(false));
        self
    }

    /// Set the maximum length for truncated argument display.
    pub fn truncate_arguments_length(mut self, length: usize) -> Self {
        self.programmatic.insert(
            "truncate_arguments_length",
            ConfigValue::Integer(length as i64),
        );
        self
    }

    /// Set how long (seconds) before a runner without heartbeats is considered dead.
    pub fn runner_dead_after_seconds(mut self, seconds: u64) -> Self {
        self.programmatic.insert(
            "runner_dead_after_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set how often (seconds) to scan for stale invocations.
    pub fn recovery_check_interval(mut self, seconds: u64) -> Self {
        self.programmatic.insert(
            "recovery_check_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set the TTL for cached invocation status checks (seconds, 0 = no cache).
    pub fn cached_status_time(mut self, seconds: f64) -> Self {
        self.programmatic
            .insert("cached_status_time_seconds", ConfigValue::Float(seconds));
        self
    }

    /// Set the cron expression for recovering pending invocations.
    pub fn recover_pending_cron(mut self, cron: impl Into<String>) -> Self {
        self.programmatic
            .insert("recover_pending_cron", ConfigValue::String(cron.into()));
        self
    }

    /// Set the cron expression for recovering running invocations from dead runners.
    pub fn recover_running_cron(mut self, cron: impl Into<String>) -> Self {
        self.programmatic
            .insert("recover_running_cron", ConfigValue::String(cron.into()));
        self
    }

    /// Set module paths to scan for trigger task registration.
    pub fn trigger_task_modules(mut self, modules: Vec<String>) -> Self {
        let arr = modules.into_iter().map(ConfigValue::String).collect();
        self.programmatic
            .insert("trigger_task_modules", ConfigValue::Array(arr));
        self
    }

    /// Set the logging level (trace, debug, info, warn, error).
    pub fn logging_level(mut self, level: impl Into<String>) -> Self {
        self.programmatic
            .insert("logging_level", ConfigValue::String(level.into()));
        self
    }

    /// Set the log output format (Text or Json).
    pub fn log_format(mut self, format: LogFormat) -> Self {
        let s = log_format_str(format);
        self.programmatic
            .insert("log_format", ConfigValue::String(s.to_string()));
        self
    }

    /// Set whether to use ANSI colors in text format (None = auto-detect TTY).
    pub fn log_use_colors(mut self, use_colors: Option<bool>) -> Self {
        if let Some(b) = use_colors {
            self.programmatic
                .insert("log_use_colors", ConfigValue::Bool(b));
        }
        self
    }

    /// Set whether to show compact context in logs.
    pub fn compact_log_context(mut self, compact: bool) -> Self {
        self.programmatic
            .insert("compact_log_context", ConfigValue::Bool(compact));
        self
    }

    /// Set whether the orchestrator uses blocking/waiting semantics.
    pub fn blocking_control(mut self, enabled: bool) -> Self {
        self.programmatic
            .insert("blocking_control", ConfigValue::Bool(enabled));
        self
    }

    /// Set hours after which final invocations can be auto-purged (0 = disabled).
    pub fn auto_final_invocation_purge_hours(mut self, hours: f64) -> Self {
        self.programmatic.insert(
            "auto_final_invocation_purge_hours",
            ConfigValue::Float(hours),
        );
        self
    }

    /// Set the scheduler evaluation interval in seconds.
    pub fn scheduler_interval(mut self, seconds: u64) -> Self {
        self.programmatic.insert(
            "scheduler_interval_seconds",
            ConfigValue::Integer(seconds as i64),
        );
        self
    }

    /// Set whether the trigger scheduler is enabled.
    pub fn enable_scheduler(mut self, enabled: bool) -> Self {
        self.programmatic
            .insert("enable_scheduler", ConfigValue::Bool(enabled));
        self
    }

    // -----------------------------------------------------------------------
    // Backend setters
    // -----------------------------------------------------------------------

    /// Use a custom broker.
    pub fn broker(mut self, broker: Arc<dyn Broker>) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Use a custom orchestrator.
    pub fn orchestrator(mut self, orch: Arc<dyn InvocationControlBackend>) -> Self {
        self.orchestrator = Some(orch);
        self
    }

    /// Use a custom state backend.
    pub fn state_backend(mut self, sb: Arc<dyn StateBackend>) -> Self {
        self.state_backend = Some(sb);
        self
    }

    /// Use a custom client data store backend.
    pub fn client_data_store(mut self, cds: Arc<dyn ClientDataStore>) -> Self {
        self.client_data_store = Some(cds);
        self
    }

    /// Configure the client data store manager.
    pub fn client_data_store_config(mut self, config: ClientDataStoreConfig) -> Self {
        self.client_data_store_config = config;
        self
    }

    // -----------------------------------------------------------------------
    // Task loading
    // -----------------------------------------------------------------------

    /// Auto-discover all tasks registered via `#[rustvello::task]` at compile time.
    ///
    /// Uses the `inventory` crate to collect all [`TaskEntry`] instances
    /// submitted by the proc macro. This is the Rust equivalent of pynenc's
    /// `_load_plugins()` via entry_points.
    pub fn auto_discover_tasks(mut self) -> Self {
        self.auto_discover = true;
        self
    }

    /// Register a [`TaskModule`] — a group of related tasks.
    ///
    /// Equivalent to pynenc's `_register_plugin_from_entry_point()`.
    pub fn task_module(mut self, module: Box<dyn TaskModule>) -> Self {
        self.task_modules.push(module);
        self
    }

    /// Set a custom trigger store backend.
    pub fn trigger_store(mut self, store: Arc<dyn TriggerStore>) -> Self {
        self.trigger_store = Some(store);
        self
    }

    // -----------------------------------------------------------------------
    // Sub-builders (grouped configuration)
    // -----------------------------------------------------------------------

    /// Open the **logging & display** sub-builder.
    ///
    /// ```rust,no_run
    /// # use rustvello::prelude::*;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let app = Rustvello::builder()
    ///     .logging()
    ///         .level("debug")
    ///         .compact_context(true)
    ///         .done()
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn logging(self) -> sub_builders::LoggingBuilder {
        sub_builders::LoggingBuilder::new(self)
    }

    /// Open the **performance & scheduling** sub-builder.
    pub fn performance(self) -> sub_builders::PerformanceBuilder {
        sub_builders::PerformanceBuilder::new(self)
    }

    /// Open the **reliability & recovery** sub-builder.
    pub fn reliability(self) -> sub_builders::ReliabilityBuilder {
        sub_builders::ReliabilityBuilder::new(self)
    }

    // Preset methods and build are in submodules:
    // - presets.rs: memory(), sqlite(), redis(), mongodb(), rabbitmq()
    // - build.rs: build()

    // -----------------------------------------------------------------------
    // Config loaders
    // -----------------------------------------------------------------------

    /// Enable reading environment variables (`RUSTVELLO__*`) during [`build()`](Self::build).
    ///
    /// When called, environment variables are added to the priority chain:
    /// programmatic > env > file > defaults.
    pub fn from_env(mut self) -> Self {
        self.use_env = true;
        self
    }

    /// Load task-section config from a TOML file and register the path for
    /// the cistell resolver to read `[app]` fields during [`build()`](Self::build).
    ///
    /// Reads `[task_defaults]` and `[tasks.<name>]` sections immediately.
    /// The `[app]` section is resolved later via cistell's `FileSource`.
    pub fn from_file(mut self, path: impl AsRef<Path>) -> RustvelloResult<Self> {
        let content =
            std::fs::read_to_string(path.as_ref()).map_err(|e| RustvelloError::Configuration {
                message: format!(
                    "failed to read config file '{}': {}",
                    path.as_ref().display(),
                    e
                ),
            })?;

        let table: std::collections::HashMap<String, toml::Value> = toml::from_str(&content)
            .map_err(|e| RustvelloError::Configuration {
                message: format!("invalid TOML: {e}"),
            })?;

        if let Some(toml::Value::Table(defaults)) = table.get("task_defaults") {
            self.task_defaults_override = parse_task_config_override(defaults);
        }
        if let Some(toml::Value::Table(tasks)) = table.get("tasks") {
            for (name, val) in tasks {
                if let toml::Value::Table(task_table) = val {
                    self.task_config_overrides
                        .insert(name.clone(), parse_task_config_override(task_table));
                }
            }
        }

        self.file_paths.push(path.as_ref().to_owned());
        Ok(self)
    }
}

// ---------------------------------------------------------------------------
// Helpers for enum ↔ string conversion (used by setters and sub-builders)
// ---------------------------------------------------------------------------

pub(super) fn argument_print_mode_str(mode: ArgumentPrintMode) -> &'static str {
    match mode {
        ArgumentPrintMode::Full => "full",
        ArgumentPrintMode::Keys => "keys",
        ArgumentPrintMode::Truncated => "truncated",
        ArgumentPrintMode::Hidden => "hidden",
        _ => "truncated",
    }
}

pub(super) fn log_format_str(format: LogFormat) -> &'static str {
    match format {
        LogFormat::Text => "text",
        LogFormat::Json => "json",
        _ => "text",
    }
}

/// Parse a `[task_defaults]` or `[tasks.<name>]` TOML table into a `TaskConfigOverride`.
pub(super) fn parse_task_config_override(
    table: &toml::map::Map<String, toml::Value>,
) -> TaskConfigOverride {
    let mut o = TaskConfigOverride::default();

    if let Some(toml::Value::String(v)) = table.get("queue") {
        o.queue = Some(v.clone());
    }
    if let Some(value) = table.get("priority").and_then(toml::Value::as_float) {
        o.priority = Some(value);
    } else if let Some(toml::Value::Integer(value)) = table.get("priority") {
        o.priority = Some(*value as f64);
    }

    if let Some(toml::Value::Integer(v)) = table.get("max_retries") {
        if let Ok(n) = u32::try_from(*v) {
            o.max_retries = Some(n);
        }
    }
    if let Some(toml::Value::String(v)) = table.get("concurrency_control") {
        o.concurrency_control = crate::task_config::parse_concurrency_control_type(v);
    }
    if let Some(toml::Value::Integer(v)) = table.get("running_concurrency") {
        if let Ok(n) = u32::try_from(*v) {
            o.running_concurrency = Some(Some(n));
        }
    }
    if let Some(toml::Value::String(v)) = table.get("registration_concurrency") {
        o.registration_concurrency = crate::task_config::parse_concurrency_control_type(v);
    }
    if let Some(toml::Value::Boolean(v)) = table.get("cache_results") {
        o.cache_results = Some(*v);
    }
    if let Some(toml::Value::Array(arr)) = table.get("key_arguments") {
        let keys: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        if !keys.is_empty() {
            o.key_arguments = Some(keys);
        }
    }
    if let Some(toml::Value::Boolean(v)) = table.get("is_workflow_task") {
        o.is_workflow_task = Some(*v);
    }
    if let Some(toml::Value::Boolean(v)) = table.get("reroute_on_cc") {
        o.reroute_on_cc = Some(*v);
    }
    if let Some(toml::Value::Boolean(v)) = table.get("blocking") {
        o.blocking = Some(*v);
    }
    if let Some(toml::Value::Array(arr)) = table.get("retry_for_errors") {
        let errs: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        if !errs.is_empty() {
            o.retry_for_errors = Some(errs);
        }
    }
    if let Some(toml::Value::Array(arr)) = table.get("disable_cache_args") {
        let args: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        if !args.is_empty() {
            o.disable_cache_args = Some(args);
        }
    }
    if let Some(toml::Value::Boolean(v)) = table.get("on_diff_non_key_args_raise") {
        o.on_diff_non_key_args_raise = Some(*v);
    }
    if let Some(toml::Value::Integer(v)) = table.get("parallel_batch_size") {
        if let Ok(n) = usize::try_from(*v) {
            o.parallel_batch_size = Some(n);
        }
    }

    o
}
#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialize all tests that touch environment variables.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env_vars() {
        std::env::remove_var("RUSTVELLO__APP_ID");
        std::env::remove_var("RUSTVELLO__DEV_MODE_FORCE_SYNC");
        std::env::remove_var("RUSTVELLO__MAX_PENDING_SECONDS");
        std::env::remove_var("RUSTVELLO__HEARTBEAT_INTERVAL_SECONDS");
    }

    #[tokio::test]
    async fn builder_default_config() {
        let app = Rustvello::builder().build().await.unwrap();
        assert_eq!(app.config.app_id, "rustvello");
        assert!(!app.config.dev_mode_force_sync);
        assert_eq!(app.config.max_pending_seconds, 300);
        assert_eq!(app.config.heartbeat_interval_seconds, 30);
    }

    #[tokio::test]
    async fn builder_fluent_config() {
        let app = Rustvello::builder()
            .app_id("test-app")
            .dev_mode(true)
            .max_pending_seconds(600)
            .heartbeat_interval(60)
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "test-app");
        assert!(app.config.dev_mode_force_sync);
        assert_eq!(app.config.max_pending_seconds, 600);
        assert_eq!(app.config.heartbeat_interval_seconds, 60);
    }

    #[tokio::test]
    async fn builder_memory_preset() {
        let app = Rustvello::builder()
            .app_id("mem-app")
            .memory()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "mem-app");
    }

    #[tokio::test]
    async fn builder_env_overrides() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env_vars();

        // Set env vars
        std::env::set_var("RUSTVELLO__APP_ID", "env-app");
        std::env::set_var("RUSTVELLO__DEV_MODE_FORCE_SYNC", "true");
        std::env::set_var("RUSTVELLO__MAX_PENDING_SECONDS", "999");
        std::env::set_var("RUSTVELLO__HEARTBEAT_INTERVAL_SECONDS", "15");

        let app = Rustvello::builder().from_env().build().await.unwrap();
        assert_eq!(app.config.app_id, "env-app");
        assert!(app.config.dev_mode_force_sync);
        assert_eq!(app.config.max_pending_seconds, 999);
        assert_eq!(app.config.heartbeat_interval_seconds, 15);

        clear_env_vars();
    }

    #[tokio::test]
    async fn builder_programmatic_wins_over_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env_vars();

        std::env::set_var("RUSTVELLO__APP_ID", "env-app");

        let app = Rustvello::builder()
            .app_id("programmatic-app")
            .from_env()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "programmatic-app");

        clear_env_vars();
    }

    #[tokio::test]
    async fn builder_toml_loading() {
        let toml = r#"
[app]
app_id = "toml-app"
dev_mode_force_sync = true
max_pending_seconds = 500
heartbeat_interval_seconds = 20
"#;
        let dir = std::env::temp_dir().join("rustvello_test_toml");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.toml");
        std::fs::write(&path, toml).unwrap();

        let app = Rustvello::builder()
            .from_file(&path)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "toml-app");
        assert!(app.config.dev_mode_force_sync);
        assert_eq!(app.config.max_pending_seconds, 500);
        assert_eq!(app.config.heartbeat_interval_seconds, 20);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn builder_programmatic_wins_over_toml() {
        let toml = r#"
[app]
app_id = "toml-app"
max_pending_seconds = 500
"#;
        let dir = std::env::temp_dir().join("rustvello_test_toml_priority");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("priority.toml");
        std::fs::write(&path, toml).unwrap();

        let app = Rustvello::builder()
            .app_id("programmatic-app")
            .from_file(&path)
            .unwrap()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "programmatic-app");
        // max_pending_seconds not set programmatically, so TOML wins
        assert_eq!(app.config.max_pending_seconds, 500);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn builder_invalid_toml_file() {
        let result = Rustvello::builder().from_file("/nonexistent/path.toml");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn builder_priority_programmatic_over_env_over_toml() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env_vars();

        let toml = r#"
[app]
app_id = "toml-app"
dev_mode_force_sync = true
max_pending_seconds = 111
heartbeat_interval_seconds = 11
"#;
        let dir = std::env::temp_dir().join("rustvello_test_full_priority");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("full.toml");
        std::fs::write(&path, toml).unwrap();

        std::env::set_var("RUSTVELLO__APP_ID", "env-app");
        std::env::set_var("RUSTVELLO__MAX_PENDING_SECONDS", "222");

        let app = Rustvello::builder()
            .app_id("code-app") // programmatic — highest priority
            .from_file(&path)   // TOML — lowest priority
            .unwrap()
            .from_env()         // env — middle priority
            .build()
            .await
            .unwrap();

        // app_id: programmatic wins over both env and TOML
        assert_eq!(app.config.app_id, "code-app");
        // dev_mode_force_sync: not set programmatically or env, so TOML wins
        assert!(app.config.dev_mode_force_sync);
        // max_pending_seconds: not set programmatically, env wins over TOML
        assert_eq!(app.config.max_pending_seconds, 222);
        // heartbeat: not set programmatically or env, so TOML wins
        assert_eq!(app.config.heartbeat_interval_seconds, 11);

        clear_env_vars();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn builder_hide_arguments() {
        let app = Rustvello::builder().hide_arguments().build().await.unwrap();
        assert!(!app.config.print_arguments);
    }

    #[tokio::test]
    async fn builder_truncate_arguments() {
        let app = Rustvello::builder()
            .truncate_arguments_length(50)
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.truncate_arguments_length, 50);
    }

    #[tokio::test]
    async fn builder_runner_dead_after_seconds() {
        let app = Rustvello::builder()
            .runner_dead_after_seconds(120)
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.runner_dead_after_seconds, 120);
    }

    #[tokio::test]
    async fn builder_recovery_check_interval() {
        let app = Rustvello::builder()
            .recovery_check_interval(45)
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.recovery_check_interval_seconds, 45);
    }

    #[tokio::test]
    async fn builder_blocking_control() {
        let app = Rustvello::builder()
            .blocking_control(true)
            .build()
            .await
            .unwrap();
        assert!(app.config.blocking_control);
    }

    #[tokio::test]
    async fn builder_sub_builder_logging() {
        let app = Rustvello::builder()
            .app_id("sub-log")
            .logging()
            .level("debug")
            .compact_context(true)
            .hide_arguments()
            .truncate_arguments_length(42)
            .done()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "sub-log");
        assert_eq!(app.config.logging_level, "debug");
        assert!(app.config.compact_log_context);
        assert!(!app.config.print_arguments);
        assert_eq!(app.config.truncate_arguments_length, 42);
    }

    #[tokio::test]
    async fn builder_sub_builder_performance() {
        let app = Rustvello::builder()
            .performance()
            .cached_status_time(5.0)
            .scheduler_interval(120)
            .enable_scheduler(false)
            .auto_final_invocation_purge_hours(48.0)
            .done()
            .build()
            .await
            .unwrap();
        assert!((app.config.cached_status_time_seconds - 5.0).abs() < f64::EPSILON);
        assert_eq!(app.config.scheduler_interval_seconds, 120);
        assert!(!app.config.enable_scheduler);
        assert!((app.config.auto_final_invocation_purge_hours - 48.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn builder_sub_builder_reliability() {
        let app = Rustvello::builder()
            .reliability()
            .max_pending_seconds(900)
            .heartbeat_interval(10)
            .runner_dead_after(200)
            .recovery_check_interval(45)
            .recover_pending_cron("0 */5 * * * *")
            .blocking_control(true)
            .done()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.max_pending_seconds, 900);
        assert_eq!(app.config.heartbeat_interval_seconds, 10);
        assert_eq!(app.config.runner_dead_after_seconds, 200);
        assert_eq!(app.config.recovery_check_interval_seconds, 45);
        assert_eq!(app.config.recover_pending_cron, "0 */5 * * * *");
        assert!(app.config.blocking_control);
    }

    #[tokio::test]
    async fn builder_sub_builder_mixed_with_flat() {
        let app = Rustvello::builder()
            .app_id("mixed")
            .logging()
            .level("warn")
            .done()
            .max_pending_seconds(999)
            .reliability()
            .heartbeat_interval(15)
            .done()
            .build()
            .await
            .unwrap();
        assert_eq!(app.config.app_id, "mixed");
        assert_eq!(app.config.logging_level, "warn");
        assert_eq!(app.config.max_pending_seconds, 999);
        assert_eq!(app.config.heartbeat_interval_seconds, 15);
    }

    // -----------------------------------------------------------------------
    // Cistell integration: env key compatibility + defaults verification
    // -----------------------------------------------------------------------

    /// Verify that AppConfig::default() matches the expected hardcoded defaults,
    /// providing a regression test for #[config(default = ...)] annotations.
    #[test]
    fn test_app_config_defaults_match() {
        use rustvello_proto::config::{AppConfig, ArgumentPrintMode, LogFormat};
        let cfg = AppConfig::default();
        assert_eq!(cfg.app_id, "rustvello");
        assert!(!cfg.dev_mode_force_sync);
        assert_eq!(cfg.max_pending_seconds, 300);
        assert_eq!(cfg.heartbeat_interval_seconds, 30);
        assert_eq!(cfg.runner_dead_after_seconds, 300);
        assert_eq!(cfg.recovery_check_interval_seconds, 60);
        assert!(cfg.print_arguments);
        assert_eq!(cfg.argument_print_mode, ArgumentPrintMode::Truncated);
        assert_eq!(cfg.truncate_arguments_length, 32);
        assert_eq!(cfg.recover_pending_cron, "*/5 * * * *");
        assert_eq!(cfg.recover_running_cron, "*/15 * * * *");
        assert!(cfg.trigger_task_modules.is_empty());
        assert_eq!(cfg.cached_status_time_seconds, 0.0);
        assert_eq!(cfg.logging_level, "info");
        assert_eq!(cfg.log_format, LogFormat::Text);
        assert!(cfg.log_use_colors.is_none());
        assert!(cfg.compact_log_context);
        assert!(cfg.blocking_control);
        assert_eq!(cfg.auto_final_invocation_purge_hours, 0.0);
        assert_eq!(cfg.scheduler_interval_seconds, 60);
        assert!(cfg.enable_scheduler);
    }

    /// Verify that the new class-specific env key format (RUSTVELLO__APP__*)
    /// works in addition to the legacy format (RUSTVELLO__*).
    #[tokio::test]
    async fn test_app_config_new_env_key_format() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env_vars();

        // New class-specific env key: RUSTVELLO__APP__<FIELD>
        std::env::set_var("RUSTVELLO__APP__APP_ID", "new-format-app");
        std::env::set_var("RUSTVELLO__APP__MAX_PENDING_SECONDS", "777");

        let app = Rustvello::builder().from_env().build().await.unwrap();
        assert_eq!(app.config.app_id, "new-format-app");
        assert_eq!(app.config.max_pending_seconds, 777);

        std::env::remove_var("RUSTVELLO__APP__APP_ID");
        std::env::remove_var("RUSTVELLO__APP__MAX_PENDING_SECONDS");
    }

    /// Verify that the legacy flat env key format (RUSTVELLO__*) still works
    /// for backward compatibility with existing deployments.
    #[tokio::test]
    async fn test_app_config_legacy_env_key_format() {
        let _lock = ENV_LOCK.lock().unwrap();
        clear_env_vars();

        // Legacy format: RUSTVELLO__<FIELD> (no group segment)
        std::env::set_var("RUSTVELLO__APP_ID", "legacy-format-app");
        std::env::set_var("RUSTVELLO__MAX_PENDING_SECONDS", "888");

        let app = Rustvello::builder().from_env().build().await.unwrap();
        assert_eq!(app.config.app_id, "legacy-format-app");
        assert_eq!(app.config.max_pending_seconds, 888);

        clear_env_vars();
    }

    /// Verify that explain() returns provenance output describing all resolved fields.
    #[tokio::test]
    async fn test_provenance_explain_output() {
        use cistell_core::Resolver;
        use rustvello_proto::config::AppConfig;

        let resolved = Resolver::builder()
            .build()
            .resolve::<AppConfig>()
            .expect("resolution should not fail");

        let explanation = resolved.explain();
        // Should contain the field name and source information
        assert!(
            explanation.contains("app_id"),
            "explain() should mention field names"
        );
        assert!(
            explanation.contains("default") || explanation.contains("Default"),
            "explain() should mention source: {explanation}"
        );
    }
}
