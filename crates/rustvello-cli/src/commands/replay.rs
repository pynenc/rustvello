// crates/rustvello-cli/src/commands/replay.rs

use std::path::PathBuf;

use clap::Args;

use crate::common::CommonArgs;
use rustvello::prelude::*;

#[derive(Args)]
pub struct ReplayArgs {
    #[clap(flatten)]
    pub common: CommonArgs,

    /// Input file path
    #[arg(short, long)]
    pub input: PathBuf,

    /// Muse endpoint
    #[arg(short, long)]
    pub endpoint: Option<String>,
}

pub async fn execute(args: ReplayArgs) -> MuseResult<()> {
    // Check and replay using the replay file
    if args.input.exists() {
        println!("Replay file found: {:?}", args.input);

        // Use the `from_replay` constructor to initialize Muse
        let muse = Muse::from_replay(&args.input).await?;

        // Start replaying events from the replay file
        muse.replay(&args.input).await?;

        println!("Replay completed successfully.");
    } else {
        println!("Replay file not found at: {:?}", args.input);
        return Err(MuseError::Configuration(
            "Replay file does not exist.".to_string(),
        ));
    }

    Ok(())
}
