use clap::{Parser, Subcommand};
use std::sync::Arc;

use rustvello::builder::Rustvello;
use rustvello::logging::{init_logging, LogConfig};
use rustvello::prelude::*;
use rustvello_core::runner::Runner;
use rustvello_core::state_backend::{StateBackendCore, StateBackendRunner};
use uuid::Uuid;

#[derive(Parser)]
#[command(
    name = "rustvello",
    about = "Rustvello distributed task system CLI",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a task runner that processes queued invocations
    Run {
        /// Application ID
        #[arg(short, long, default_value = "rustvello")]
        app_id: String,

        /// SQLite database path (uses in-memory if not specified)
        #[arg(short, long)]
        db_path: Option<String>,

        /// TOML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// Use in-memory backends (for development)
        #[arg(long)]
        memory: bool,

        /// Idle sleep time in milliseconds
        #[arg(long, default_value = "100")]
        idle_sleep_ms: u64,
    },

    /// Show the status of an invocation
    Status {
        /// Invocation ID to check
        invocation_id: String,

        /// SQLite database path
        #[arg(short, long)]
        db_path: Option<String>,
    },

    /// List invocations, optionally filtered by status
    List {
        /// Filter by status (REGISTERED, PENDING, RUNNING, SUCCESS, FAILED, RETRY)
        #[arg(short, long)]
        status: Option<String>,

        /// Filter by task ID (format: language::module.name)
        #[arg(short, long)]
        task: Option<String>,

        /// SQLite database path
        #[arg(short, long)]
        db_path: Option<String>,
    },

    /// Investigate an invocation and print provenance, history, and runner context
    Investigate {
        /// Invocation ID to investigate
        invocation_id: String,

        /// Application ID used to namespace the backend data
        #[arg(short, long, default_value = "rustvello")]
        app_id: String,

        /// SQLite database path
        #[arg(short, long)]
        db_path: Option<String>,

        /// Output format
        #[arg(long, default_value = "json")]
        format: InvestigationFormat,
    },

    /// Purge all data (broker queue, invocations, results)
    Purge {
        /// SQLite database path
        #[arg(short, long)]
        db_path: Option<String>,

        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,
    },

    /// Show system information
    Info,

    /// Show effective configuration
    Config {
        /// TOML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// Application ID
        #[arg(short, long, default_value = "rustvello")]
        app_id: String,
    },
}

#[derive(Clone, clap::ValueEnum)]
enum InvestigationFormat {
    Json,
    Text,
}

fn parse_status(s: &str) -> Option<InvocationStatus> {
    s.parse::<InvocationStatus>().ok()
}

type SqliteBackends = (
    Arc<rustvello_sqlite::broker::SqliteBroker>,
    Arc<rustvello_sqlite::orchestrator::SqliteOrchestrator>,
    Arc<rustvello_sqlite::state_backend::SqliteStateBackend>,
);

fn make_sqlite_backends(
    db_path: &Option<String>,
) -> Result<SqliteBackends, Box<dyn std::error::Error>> {
    make_sqlite_backends_for(db_path, "rustvello")
}

fn make_sqlite_backends_for(
    db_path: &Option<String>,
    app_id: &str,
) -> Result<SqliteBackends, Box<dyn std::error::Error>> {
    let db = Arc::new(match db_path {
        Some(path) => rustvello_sqlite::db::Database::open(path, app_id)?,
        None => rustvello_sqlite::db::Database::in_memory()?,
    });
    Ok((
        Arc::new(rustvello_sqlite::broker::SqliteBroker::new(Arc::clone(&db))),
        Arc::new(rustvello_sqlite::orchestrator::SqliteOrchestrator::new(
            Arc::clone(&db),
        )),
        Arc::new(rustvello_sqlite::state_backend::SqliteStateBackend::new(db)),
    ))
}

async fn print_investigation(
    invocation_id: &str,
    app_id: &str,
    db_path: &Option<String>,
    format: InvestigationFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_, orchestrator, state_backend) = make_sqlite_backends_for(db_path, app_id)?;
    let inv_id = InvocationId::try_from_string(invocation_id.to_owned())?;
    let invocation = state_backend.get_invocation(&inv_id).await?;
    let history = state_backend.get_history(&inv_id).await.unwrap_or_default();
    let mut runner_contexts = serde_json::Map::new();
    for entry in &history {
        if let Some(runner_id) = entry
            .runner_id
            .as_ref()
            .or(entry.status_record.runner_id.as_ref())
        {
            let runner_id = runner_id.to_string();
            if runner_contexts.contains_key(&runner_id) {
                continue;
            }
            if let Ok(Some(context)) = state_backend.get_runner_context(&runner_id).await {
                runner_contexts.insert(
                    runner_id,
                    serde_json::json!({
                        "class": context.runner_cls,
                        "language": context.runner_language.to_string(),
                        "executor": context.executor_kind.to_string(),
                        "hostname": context.hostname,
                        "pid": context.pid,
                        "thread_id": context.thread_id,
                        "parent_runner_id": context.parent_runner_id,
                        "parent_runner_class": context.parent_runner_cls,
                    }),
                );
            }
        }
    }
    let latest_status = orchestrator.get_invocation_status(&inv_id).await.ok();
    let registration = history
        .iter()
        .find(|entry| entry.status_record.status == InvocationStatus::Registered);
    let registration_runner_id = registration.and_then(|entry| {
        entry
            .runner_id
            .as_ref()
            .or(entry.status_record.runner_id.as_ref())
            .map(std::string::ToString::to_string)
    });
    let registration_time = registration.map(|entry| entry.status_record.timestamp);
    let atomic_execution = if let (Some(runner_id), Some(timestamp)) =
        (registration_runner_id.as_ref(), registration_time)
    {
        orchestrator
            .get_atomic_service_timeline()
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|execution| {
                execution.runner_id == *runner_id
                    && execution.start <= timestamp
                    && timestamp <= execution.end
            })
    } else {
        None
    };
    let history_json = history
        .iter()
        .map(|entry| {
            let runner_id = entry
                .runner_id
                .as_ref()
                .or(entry.status_record.runner_id.as_ref())
                .map(std::string::ToString::to_string);
            serde_json::json!({
                "status": entry.status_record.status.to_string(),
                "timestamp": entry.status_record.timestamp.to_rfc3339(),
                "runner_id": runner_id,
                "message": entry.message,
                "registered_by_invocation_id": entry.registered_by_inv_id.as_ref().map(std::string::ToString::to_string),
            })
        })
        .collect::<Vec<_>>();
    let report = serde_json::json!({
        "app_id": app_id,
        "invocation": {
            "id": invocation.invocation_id.to_string(),
            "task_id": invocation.task_id.to_string(),
            "call_id": invocation.call_id.to_string(),
            "status": latest_status.as_ref().map_or_else(|| invocation.status.to_string(), |status| status.status.to_string()),
            "created_at": invocation.created_at.to_rfc3339(),
            "updated_at": invocation.updated_at.to_rfc3339(),
            "parent_invocation_id": invocation.parent_invocation_id.as_ref().map(std::string::ToString::to_string),
            "workflow": invocation.workflow.as_ref().map(|workflow| serde_json::json!({
                "workflow_id": workflow.workflow_id.to_string(),
                "workflow_type": workflow.workflow_type.to_string(),
                "parent_id": workflow.parent_id.as_ref().map(std::string::ToString::to_string),
                "depth": workflow.depth,
            })),
        },
        "registration": {
            "timestamp": registration_time.map(|time| time.to_rfc3339()),
            "runner_id": registration_runner_id,
            "atomic_service_execution": atomic_execution.map(|execution| serde_json::json!({
                "runner_id": execution.runner_id,
                "start": execution.start.to_rfc3339(),
                "end": execution.end.to_rfc3339(),
                "duration_seconds": execution.duration_secs(),
            })),
        },
        "runner_contexts": runner_contexts,
        "history": history_json,
    });

    match format {
        InvestigationFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        InvestigationFormat::Text => {
            println!(
                "Invocation: {}",
                report["invocation"]["id"].as_str().unwrap_or("")
            );
            println!(
                "Task:       {}",
                report["invocation"]["task_id"].as_str().unwrap_or("")
            );
            println!(
                "Status:     {}",
                report["invocation"]["status"].as_str().unwrap_or("")
            );
            if let Some(time) = report["registration"]["timestamp"].as_str() {
                println!("Registered: {time}");
            }
            if let Some(runner_id) = report["registration"]["runner_id"].as_str() {
                println!("Runner:     {runner_id}");
            }
            println!("History:");
            if let Some(history) = report["history"].as_array() {
                for entry in history {
                    println!(
                        "  {}  {}  {}",
                        entry["timestamp"].as_str().unwrap_or(""),
                        entry["status"].as_str().unwrap_or(""),
                        entry["runner_id"].as_str().unwrap_or("")
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize tracing with structured span fields in log output.
    // Span fields (runner_id, app_id, invocation_id, task_id) are
    // automatically included in every log line within their scope.
    init_logging(&LogConfig::default());
    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            app_id,
            db_path,
            config,
            memory,
            idle_sleep_ms,
        } => {
            println!("Starting Rustvello runner (app_id: {})", app_id);

            let mut builder = Rustvello::builder()
                .from_env()
                .app_id(&app_id)
                .auto_discover_tasks();

            if let Some(ref config_path) = config {
                builder = builder.from_file(config_path).unwrap_or_else(|e| {
                    eprintln!("Failed to load config file: {}", e);
                    std::process::exit(1);
                });
            }

            if memory {
                builder = builder.memory();
            } else if let Some(ref path) = db_path {
                builder = builder.sqlite(path, "rustvello");
            } else {
                // Default: in-memory SQLite
                builder = builder.sqlite(":memory:", "rustvello");
            }

            let app = builder.build().await.unwrap_or_else(|e| {
                eprintln!("Failed to build application: {}", e);
                std::process::exit(1);
            });

            let runner = app.into_runner().with_idle_sleep(idle_sleep_ms);

            println!(
                "Runner {} started, waiting for tasks...",
                runner.runner_id()
            );

            if let Err(e) = runner.run().await {
                eprintln!("Runner error: {}", e);
                std::process::exit(1);
            }
        }

        Commands::Status {
            invocation_id,
            db_path,
        } => {
            let (_, orchestrator, state_backend) =
                make_sqlite_backends(&db_path).unwrap_or_else(|e| {
                    eprintln!("Failed to open database: {}", e);
                    std::process::exit(1);
                });
            let inv_id = match Uuid::parse_str(&invocation_id) {
                Ok(uuid) => InvocationId::from_string(uuid.to_string()),
                Err(_) => {
                    eprintln!("Invalid invocation ID: not a valid UUID");
                    std::process::exit(1);
                }
            };

            match orchestrator.get_invocation_status(&inv_id).await {
                Ok(record) => {
                    println!("Invocation: {}", inv_id);
                    println!("Status:     {}", record.status);
                    println!("Timestamp:  {}", record.timestamp);
                    if let Some(runner) = &record.runner_id {
                        println!("Runner:     {}", runner);
                    }

                    // Show result or error if terminal
                    if record.status == InvocationStatus::Success {
                        match state_backend.get_result(&inv_id).await {
                            Ok(Some(result)) => println!("Result:     {}", result),
                            Ok(None) => {}
                            Err(e) => eprintln!("Warning: failed to fetch result: {}", e),
                        }
                    } else if record.status == InvocationStatus::Failed {
                        match state_backend.get_error(&inv_id).await {
                            Ok(Some(error)) => println!("Error:      {}", error),
                            Ok(None) => {}
                            Err(e) => eprintln!("Warning: failed to fetch error: {}", e),
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::List {
            status,
            task,
            db_path,
        } => {
            let (_, orchestrator, _) = make_sqlite_backends(&db_path).unwrap_or_else(|e| {
                eprintln!("Failed to open database: {}", e);
                std::process::exit(1);
            });

            let task_id = task.map(|t| {
                t.parse::<TaskId>().unwrap_or_else(|e| {
                    eprintln!("Invalid task ID '{}': {}", t, e);
                    std::process::exit(1);
                })
            });

            if let Some(status_str) = status {
                let status = parse_status(&status_str).unwrap_or_else(|| {
                    eprintln!("Invalid status: {}", status_str);
                    std::process::exit(1);
                });

                match orchestrator
                    .get_invocations_by_status(status, task_id.as_ref())
                    .await
                {
                    Ok(ids) => {
                        println!("Found {} invocations with status {}:", ids.len(), status);
                        for id in &ids {
                            println!("  {}", id);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            } else if let Some(tid) = task_id {
                match orchestrator.get_invocations_by_task(&tid).await {
                    Ok(ids) => {
                        println!("Found {} invocations for task {}:", ids.len(), tid);
                        for id in &ids {
                            println!("  {}", id);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            } else {
                println!("Specify --status or --task to filter invocations");
            }
        }

        Commands::Investigate {
            invocation_id,
            app_id,
            db_path,
            format,
        } => {
            if let Err(error) = print_investigation(&invocation_id, &app_id, &db_path, format).await
            {
                eprintln!("Failed to investigate invocation: {error}");
                std::process::exit(1);
            }
        }

        Commands::Purge { db_path, yes } => {
            if !yes {
                println!("This will delete ALL data. Use --yes to confirm.");
                std::process::exit(0);
            }

            let (broker, orchestrator, state_backend) = make_sqlite_backends(&db_path)
                .unwrap_or_else(|e| {
                    eprintln!("Failed to open database: {}", e);
                    std::process::exit(1);
                });
            if let Err(e) = broker.purge(None).await {
                eprintln!("Failed to purge broker: {}", e);
                std::process::exit(1);
            }
            if let Err(e) = orchestrator.purge().await {
                eprintln!("Failed to purge orchestrator: {}", e);
                std::process::exit(1);
            }
            if let Err(e) = state_backend.purge().await {
                eprintln!("Failed to purge state backend: {}", e);
                std::process::exit(1);
            }
            println!("All data purged.");
        }

        Commands::Info => {
            println!("Rustvello v{}", env!("CARGO_PKG_VERSION"));
            println!("Distributed task system for Rust");
            println!("Homepage: https://pynenc.org");
            println!("Repository: https://github.com/pynenc/rustvello");
        }

        Commands::Config { config, app_id } => {
            let mut builder = Rustvello::builder().from_env().app_id(&app_id);

            if let Some(ref config_path) = config {
                builder = builder.from_file(config_path).unwrap_or_else(|e| {
                    eprintln!("Failed to load config file: {}", e);
                    std::process::exit(1);
                });
            }

            let app = builder.memory().build().await.unwrap_or_else(|e| {
                eprintln!("Failed to build application: {}", e);
                std::process::exit(1);
            });

            println!("=== Effective Configuration ===");
            println!();
            println!("[app]");
            println!("  app_id                       = {:?}", app.config.app_id);
            println!(
                "  dev_mode_force_sync          = {}",
                app.config.dev_mode_force_sync
            );
            println!(
                "  max_pending_seconds          = {}",
                app.config.max_pending_seconds
            );
            println!(
                "  heartbeat_interval_seconds   = {}",
                app.config.heartbeat_interval_seconds
            );
            println!(
                "  runner_dead_after_seconds     = {}",
                app.config.runner_dead_after_seconds
            );
            println!(
                "  recovery_check_interval_sec  = {}",
                app.config.recovery_check_interval_seconds
            );
            println!(
                "  print_arguments              = {}",
                app.config.print_arguments
            );
            println!(
                "  argument_print_mode          = {:?}",
                app.config.argument_print_mode
            );
            println!(
                "  truncate_arguments_length    = {}",
                app.config.truncate_arguments_length
            );
            println!(
                "  recover_pending_cron         = {:?}",
                app.config.recover_pending_cron
            );
            println!(
                "  recover_running_cron         = {:?}",
                app.config.recover_running_cron
            );
            println!(
                "  cached_status_time_seconds   = {}",
                app.config.cached_status_time_seconds
            );
            println!(
                "  logging_level                = {:?}",
                app.config.logging_level
            );
            println!(
                "  log_format                   = {:?}",
                app.config.log_format
            );
            println!(
                "  log_use_colors               = {:?}",
                app.config.log_use_colors
            );
            println!(
                "  compact_log_context          = {}",
                app.config.compact_log_context
            );
            println!(
                "  blocking_control             = {}",
                app.config.blocking_control
            );
            println!(
                "  auto_final_inv_purge_hours   = {}",
                app.config.auto_final_invocation_purge_hours
            );
            println!(
                "  scheduler_interval_seconds   = {}",
                app.config.scheduler_interval_seconds
            );
            println!(
                "  enable_scheduler             = {}",
                app.config.enable_scheduler
            );
        }
    }
}
