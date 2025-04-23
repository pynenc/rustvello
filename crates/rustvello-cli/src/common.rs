// crates/rustvello-cli/src/common.rs
use clap::Args;

#[derive(Args, Clone)]
pub struct CommonArgs {
    /// Poet server URL
    #[arg(short = 'u', long, default_value = "http://localhost:8000")]
    pub poet_url: String,
}
