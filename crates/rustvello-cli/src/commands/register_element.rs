// crates/rustvello-cli/src/commands/register_element.rs

use std::collections::HashMap;

use clap::Args;
use serde_json::from_str;

use super::utils::create_poet_client;
use rustvello_core::{MuseError, MuseResult, Transport};
use rustvello_proto::{types::*, ElementRegistration};

#[derive(Args)]
pub struct RegisterElementArgs {
    /// Poet server URL
    #[arg(short = 'u', long, default_value = "http://localhost:8000")]
    pub poet_url: String,
    /// Element kind
    #[arg(short, long)]
    pub kind: String,
    /// Element name
    #[arg(short, long)]
    pub name: String,
    /// Parent element ID (optional)
    #[arg(short = 'p', long)]
    pub parent_id: Option<ElementId>,
    /// Metadata as JSON string
    #[arg(short, long, default_value = "{}")]
    pub metadata: Option<String>,
}

pub async fn execute(args: RegisterElementArgs) -> MuseResult<()> {
    let client = create_poet_client(&args.poet_url);

    let metadata: HashMap<String, String> = match &args.metadata {
        Some(metadata_str) => from_str(metadata_str)
            .map_err(|e| MuseError::Client(format!("Failed to parse metadata JSON: {}", e)))?,
        None => HashMap::new(),
    };

    let elements = [ElementRegistration::new(
        &args.kind,
        args.name,
        metadata,
        args.parent_id,
    )];

    let result = client.register_elements(&elements).await?;
    println!("Element registered with IDs: {:?}", result);
    Ok(())
}
