// crates/rustvello-cli/src/main.rs

use clap::{Parser, Subcommand};

mod commands;
mod common;

use commands::{
    get_metric_order::GetMetricOrderArgs, get_metrics::GetMetricsArgs,
    get_node_elem_ranges::GetNodeElemRangesArgs, get_node_state::GetNodeStateArgs,
    get_resolution::GetFinestResolutionArgs, is_ready::IsReadyArgs, record::RecordArgs,
    register_element::RegisterElementArgs, register_element_kind::RegisterElementKindArgs,
    register_metric::RegisterMetricArgs, replay::ReplayArgs, send_metric::SendMetricArgs,
};

#[derive(Parser)]
#[command(name = "rustvello-cli", version = "0.1.0", about = "CLI for rustvello")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check if the poet server is up and healthy
    IsReady(IsReadyArgs),
    /// Register an element kind with the poet server
    GetNodeState(GetNodeStateArgs),
    /// Register an element kind with the poet server
    GetFinestResolution(GetFinestResolutionArgs),
    /// Register an element kind with the poet server
    RegisterElementKind(RegisterElementKindArgs),
    /// Register an element with the poet server
    RegisterElement(RegisterElementArgs),
    /// Get the Element Ranges Assigned to each node
    GetNodeElemRanges(GetNodeElemRangesArgs),
    /// Register a metric definition
    RegisterMetric(RegisterMetricArgs),
    /// Get Global Metrics Order
    GetMetricOrder(GetMetricOrderArgs),
    /// Register a metric with the poet server
    SendMetric(SendMetricArgs),
    /// Get registered metrics with the poet server
    GetMetrics(GetMetricsArgs),
    /// Record a session
    Record(RecordArgs),
    /// Replay a recorded session
    Replay(ReplayArgs),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::IsReady(args) => commands::is_ready::execute(args).await,
        Commands::GetNodeState(args) => commands::get_node_state::execute(args).await,
        Commands::GetFinestResolution(args) => commands::get_resolution::execute(args).await,
        Commands::RegisterElement(args) => commands::register_element::execute(args).await,
        Commands::GetNodeElemRanges(args) => commands::get_node_elem_ranges::execute(args).await,
        Commands::RegisterElementKind(args) => commands::register_element_kind::execute(args).await,
        Commands::RegisterMetric(args) => commands::register_metric::execute(args).await,
        Commands::GetMetricOrder(args) => commands::get_metric_order::execute(args).await,
        Commands::SendMetric(args) => commands::send_metric::execute(args).await,
        Commands::GetMetrics(args) => commands::get_metrics::execute(args).await,
        Commands::Record(args) => commands::record::execute(args).await,
        Commands::Replay(args) => commands::replay::execute(args).await,
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
