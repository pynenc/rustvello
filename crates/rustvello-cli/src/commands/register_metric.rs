// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;
use rustvello_core::{MuseResult, Transport};
use rustvello_proto::MetricDefinition;

use crate::common::CommonArgs;

#[derive(Args)]
pub struct RegisterMetricArgs {
    #[clap(flatten)]
    pub common: CommonArgs,

    /// Metric code
    #[arg(short, long)]
    pub code: String,

    /// Metric name
    #[arg(short, long)]
    pub name: String,

    /// Metric description
    #[arg(short, long)]
    pub description: String,
}

pub async fn execute(args: RegisterMetricArgs) -> MuseResult<()> {
    let client = super::utils::create_poet_client(&args.common.poet_url);

    let payload = [MetricDefinition::new(
        &args.code,
        &args.name,
        &args.description,
    )];

    client.register_metrics(&payload).await?;
    println!("Metric registered successfully");
    Ok(())
}
