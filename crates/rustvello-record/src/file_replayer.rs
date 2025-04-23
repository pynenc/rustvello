// crates/rustvello-record/src/file_replayer.rs

//! Provides the [`FileReplayer`] for replaying recorded events.
//!
//! The recording files should have a specific extension to determine
//! the encoding/serialization format. Supported extensions are:
//!
//! - `.bin` for Bincode serialization
//! - `.json` for JSON serialization

use std::path::Path;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use super::SerializationFormat;
use crate::{RecordedEventWithTime, Replayer};
use rustvello_core::{MuseError, MuseResult};

/// A replayer that reads events from a recording file.
///
/// The serialization format is determined by the file extension.
/// Supported extensions are `.bin` for Bincode and `.json` for JSON.
pub struct FileReplayer {
    reader: BufReader<File>,
    format: SerializationFormat,
}

impl FileReplayer {
    /// Creates a new `FileReplayer`.
    ///
    /// # Arguments
    ///
    /// - `path`: The file path to read recordings from.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Replaying`] if the file cannot be opened.
    pub async fn new(path: &Path) -> MuseResult<Self> {
        let ext = path.extension().and_then(|e| e.to_str());
        let format = SerializationFormat::from_extension(ext)?;

        let file = File::open(path)
            .await
            .map_err(|e| MuseError::Replaying(format!("Failed to open file: {}", e)))?;
        log::info!("Using {:?} format for replaying.", format);
        Ok(Self {
            reader: BufReader::new(file),
            format,
        })
    }
}

#[async_trait]
impl Replayer for FileReplayer {
    async fn next_event(&mut self) -> MuseResult<Option<RecordedEventWithTime>> {
        match self.format {
            SerializationFormat::Bincode => {
                // Bincode doesn't support async read directly; read the entire file into memory
                let mut buffer = Vec::new();
                let bytes_read = self
                    .reader
                    .read_to_end(&mut buffer)
                    .await
                    .map_err(|e| MuseError::Replaying(e.to_string()))?;
                if bytes_read == 0 {
                    return Ok(None); // EOF
                }
                let mut cursor = std::io::Cursor::new(buffer);
                match bincode::deserialize_from::<_, RecordedEventWithTime>(&mut cursor) {
                    Ok(timed_event) => Ok(Some(timed_event)),
                    Err(e) => Err(MuseError::Replaying(format!(
                        "Failed to deserialize event: {}",
                        e
                    ))),
                }
            }
            SerializationFormat::Json => {
                let mut line = String::new();
                match self.reader.read_line(&mut line).await {
                    Ok(0) => Ok(None), // EOF
                    Ok(_) => {
                        let timed_event: RecordedEventWithTime = serde_json::from_str(&line)
                            .map_err(|e| {
                                MuseError::Replaying(format!("Failed to deserialize event: {}", e))
                            })?;
                        Ok(Some(timed_event))
                    }
                    Err(e) => Err(MuseError::Replaying(format!("Failed to read line: {}", e))),
                }
            }
        }
    }
}
