// crates/rustvello-cli/src/commands/utils.rs

use rustvello_client::PoetClient;

pub fn create_poet_client(poet_url: &str) -> PoetClient {
    PoetClient::new(&[poet_url.to_string()], None)
}
