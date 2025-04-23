// crates/rustvello-core/src/errors.rs

use std::time::Duration;

use chrono::OutOfRangeError;
use thiserror::Error;

use rustvello_proto::prelude::*;

pub type MuseResult<T> = Result<T, MuseError>;

#[derive(Error, Debug)]
pub enum MuseError {
    #[error("Configuration error {0}")]
    Configuration(String),
    #[error("Error on Poet client {0}")]
    Client(String),
    #[error("Muse initialization timeout {0:?}")]
    MuseInitializationTimeout(Duration),
    #[error("Recording error {0}")]
    Recording(String),
    #[error("Replaying error {0}")]
    Replaying(String),
    #[error("File has an invalid extension {0:?}")]
    InvalidFileExtension(Option<String>),
    #[error("Invalid Element Kind Code {0}")]
    InvalidElementKindCode(String),
    #[error("Do not exists a remote element id for {0}")]
    NotAvailableRemoteElementId(LocalElementId),
    #[error("Invalid Metric Code {0}")]
    InvalidMetricCode(String),
    #[error("Duration conversion error: {0}")]
    DurationConversion(String),
}

impl From<OutOfRangeError> for MuseError {
    fn from(err: OutOfRangeError) -> Self {
        MuseError::DurationConversion(err.to_string())
    }
}
