// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;

use super::utils::create_poet_client;
use crate::common::CommonArgs;
use rustvello_core::{MuseResult, Transport};

#[derive(Args)]
pub struct GetMetricOrderArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn execute(args: GetMetricOrderArgs) -> MuseResult<()> {
    let client = create_poet_client(&args.common.poet_url);

    match client.get_metric_order().await {
        Ok(metric_order) => {
            println!("Getting Global Metric Order:");
            for metric_def in metric_order {
                println!("  - {:?}", metric_def);
            }
        }
        Err(e) => {
            eprintln!("Failed to get metric order: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
