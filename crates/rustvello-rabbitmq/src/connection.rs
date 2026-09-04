use lapin::{options::ConfirmSelectOptions, Channel, Connection, ConnectionProperties};
use tokio::sync::Mutex;

/// Manages a persistent AMQP connection and channel.
///
/// Reconnects lazily when a channel operation fails.
pub(crate) struct AmqpConnection {
    uri: String,
    state: Mutex<Option<(Connection, Channel)>>,
}

impl AmqpConnection {
    pub fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_string(),
            state: Mutex::new(None),
        }
    }

    /// Get or create a channel, reconnecting if necessary.
    pub async fn channel(&self) -> Result<Channel, lapin::Error> {
        let mut guard = self.state.lock().await;
        if let Some((conn, ch)) = guard.as_ref() {
            if conn.status().connected() && ch.status().connected() {
                return Ok(ch.clone());
            }
        }
        let conn = Connection::connect(&self.uri, ConnectionProperties::default()).await?;
        let ch = conn.create_channel().await?;
        ch.confirm_select(ConfirmSelectOptions::default()).await?;
        *guard = Some((conn, ch.clone()));
        Ok(ch)
    }
}
