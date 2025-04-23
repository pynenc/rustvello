// crates/rustvello-cli/src/commands/is_ready.rs

use clap::Args;

use super::utils::create_poet_client;
use rustvello_core::{MuseResult, Transport};

#[derive(Args)]
pub struct IsReadyArgs {
    /// Poet server URL
    #[arg(short, long, default_value = "http://localhost:8000")]
    pub poet_url: String,
}

pub async fn execute(args: IsReadyArgs) -> MuseResult<()> {
    let client = create_poet_client(&args.poet_url);
    client.health_check().await
}
