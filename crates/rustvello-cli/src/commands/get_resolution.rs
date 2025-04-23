// crates/rustvello-cli/src/commands/register_metric.rs

use clap::Args;
use rustvello_core::{MuseResult, Transport};

use crate::common::CommonArgs;

#[derive(Args)]
pub struct GetFinestResolutionArgs {
    #[clap(flatten)]
    pub common: CommonArgs,
}

pub async fn execute(args: GetFinestResolutionArgs) -> MuseResult<()> {
    let client = super::utils::create_poet_client(&args.common.poet_url);
    let finest_resolution = client.get_finest_resolution().await?;
    println!("Finest Resolution: {finest_resolution}");
    Ok(())
}
