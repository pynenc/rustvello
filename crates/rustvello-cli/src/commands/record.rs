// crates/rustvello-cli/src/commands/record.rs

use std::path::PathBuf;

use clap::Args;

use rustvello::prelude::*;

#[derive(Args, Debug)]
pub struct RecordArgs {
    /// Output file path
    #[arg(short, long)]
    pub output: PathBuf,
    // Additional arguments...
}

pub async fn execute(args: RecordArgs) -> MuseResult<()> {
    // TODO probably not necessary
    println!("Called record with {:?}: Not implemented", args);
    Ok(())
}
