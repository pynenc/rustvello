// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;

use super::utils::create_poet_client;
use crate::common::CommonArgs;
use rustvello_core::{MuseResult, Transport};
use rustvello_proto::MetricQuery;

#[derive(Args)]
pub struct GetMetricsArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn execute(args: GetMetricsArgs) -> MuseResult<()> {
    let client = create_poet_client(&args.common.poet_url);

    match client.get_metrics(&MetricQuery::default(), None).await {
        Ok(metrics) => {
            println!("Getting all Metrics:");
            for metric in metrics {
                println!("  - {:?}", metric);
            }
        }
        Err(e) => {
            eprintln!("Failed to get metrics: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
