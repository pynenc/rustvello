// crates/rustvello-record/src/file_recorder.rs

//! Implements the [`FileRecorder`] for recording events to a file.
//!
//! The recording files should have a specific extension to determine
//! the encoding/serialization format. Supported extensions are:
//!
//! - `.bin` for Bincode serialization
//! - `.json` for JSON serialization

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use async_trait::async_trait;

use super::SerializationFormat;
use crate::{RecordedEventWithTime, Recorder};
use rustvello_core::{MuseError, MuseResult};

/// A recorder that writes events to a file.
///
/// The serialization format is determined by the file extension.
/// Supported extensions are `.bin` for Bincode and `.json` for JSON.
pub struct FileRecorder {
    writer: BufWriter<File>,
    format: SerializationFormat,
}

impl FileRecorder {
    /// Creates a new `FileRecorder`.
    ///
    /// # Arguments
    ///
    /// - `path`: The file path to write recordings to.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Recording`] if the file cannot be opened.
    pub fn new(path: &Path) -> MuseResult<Self> {
        let ext = path.extension().and_then(|e| e.to_str());
        let format = SerializationFormat::from_extension(ext)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .map_err(|e| MuseError::Recording(format!("Failed to open file: {}", e)))?;
        log::info!("Using {:?} format for recording.", format);
        Ok(Self {
            writer: BufWriter::new(file),
            format,
        })
    }
}

#[async_trait]
impl Recorder for FileRecorder {
    /// Records an event to the file.
    ///
    /// # Arguments
    ///
    /// - `event`: The [`RecordedEvent`] to record.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Recording`] if serialization fails.
    async fn record(&mut self, event: RecordedEventWithTime) -> MuseResult<()> {
        match self.format {
            SerializationFormat::Bincode => bincode::serialize_into(&mut self.writer, &event)
                .map_err(|e| MuseError::Recording(format!("Failed to record event: {}", e))),
            SerializationFormat::Json => {
                serde_json::to_writer(&mut self.writer, &event)
                    .map_err(|e| MuseError::Recording(format!("Failed to record event: {}", e)))?;
                self.writer
                    .write_all(b"\n")
                    .map_err(|e| MuseError::Recording(format!("Failed to write newline: {}", e)))
            }
        }
    }

    /// Flushes the writer.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Recording`] if flushing fails.
    async fn flush(&mut self) -> MuseResult<()> {
        self.writer
            .flush()
            .map_err(|e| MuseError::Recording(format!("Failed to flush file: {}", e)))
    }

    /// Closes the recorder by flushing the writer.
    ///
    /// # Errors
    ///
    /// Returns a [`MuseError::Recording`] if flushing fails.
    async fn close(&mut self) -> MuseResult<()> {
        self.flush().await
    }
}
