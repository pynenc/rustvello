// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;
use rustvello_core::{MuseResult, Transport};

use crate::common::CommonArgs;

#[derive(Args)]
pub struct GetNodeElemRangesArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn execute(args: GetNodeElemRangesArgs) -> MuseResult<()> {
    let client = super::utils::create_poet_client(&args.common.poet_url);
    match client.get_node_elem_ranges(None, None).await {
        Ok(node_elem_ranges) => {
            println!("All nodes element ranges:");
            for node_elem_range in node_elem_ranges {
                println!("  - {:?}", node_elem_range);
            }
        }
        Err(e) => {
            eprintln!("Failed to get all node element ranges: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
