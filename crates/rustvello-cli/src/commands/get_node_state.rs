// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;
use rustvello_core::{MuseResult, Transport};

use crate::common::CommonArgs;

#[derive(Args)]
pub struct GetNodeStateArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn execute(args: GetNodeStateArgs) -> MuseResult<()> {
    let client = super::utils::create_poet_client(&args.common.poet_url);
    let node_state = client.get_node_state().await?;
    println!("{node_state}");
    Ok(())
}
