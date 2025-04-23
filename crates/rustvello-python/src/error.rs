use std::fmt::{Debug, Formatter};
use std::io::Error;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use thiserror::Error;

use crate::exceptions::*;
use rustvello::MuseError;

#[derive(Error)]
pub enum PyMusesErr {
    #[error(transparent)]
    Muses(#[from] MuseError),
    #[error("{0}")]
    Uuid(String),
    #[error("{0}")]
    Chrono(String),
    #[error("{0}")]
    Other(String),
}

impl std::convert::From<std::io::Error> for PyMusesErr {
    fn from(value: Error) -> Self {
        PyMusesErr::Other(format!("{value:?}"))
    }
}

impl std::convert::From<uuid::Error> for PyMusesErr {
    fn from(err: uuid::Error) -> Self {
        PyMusesErr::Uuid(err.to_string())
    }
}

impl From<chrono::OutOfRangeError> for PyMusesErr {
    fn from(err: chrono::OutOfRangeError) -> Self {
        PyMusesErr::Chrono(err.to_string())
    }
}

impl std::convert::From<PyMusesErr> for PyErr {
    fn from(err: PyMusesErr) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));

        use PyMusesErr::*;
        match err {
            Muses(err) => match err {
                MuseError::Configuration(err) => ConfigurationError::new_err(err.to_string()),
                MuseError::MuseInitializationTimeout(d) => {
                    MuseInitializationTimeoutError::new_err(format!("Duration: {:?}", d))
                }
                MuseError::Client(err) => ClientError::new_err(err.to_string()),
                MuseError::Recording(err) => RecordingError::new_err(err.to_string()),
                MuseError::Replaying(err) => ReplayingError::new_err(err.to_string()),
                MuseError::InvalidFileExtension(ext) => {
                    let msg = if let Some(msg) = ext {
                        msg.to_string()
                    } else {
                        "File extension not specified".to_string()
                    };
                    InvalidFileExtensionError::new_err(msg)
                }
                MuseError::InvalidElementKindCode(name) => {
                    InvalidElementKindCodeError::new_err(name.to_string())
                }
                MuseError::NotAvailableRemoteElementId(id) => {
                    NotAvailableRemoteElementIdError::new_err(id.to_string())
                }
                MuseError::InvalidMetricCode(name) => {
                    InvalidMetricCodeError::new_err(name.to_string())
                }
                MuseError::DurationConversion(err) => {
                    DurationConversionError::new_err(err.to_string())
                }
            },
            _ => default(),
        }
    }
}

impl Debug for PyMusesErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use PyMusesErr::*;
        match self {
            Muses(err) => write!(f, "{err:?}"),
            PyMusesErr::Uuid(err) => write!(f, "UUID error {err:?}"),
            PyMusesErr::Chrono(err) => write!(f, "Chrono error {err:?}"),
            Other(err) => write!(f, "BindingsError: {err:?}"),
        }
    }
}

#[macro_export]
macro_rules! raise_err(
    ($msg:expr, $err:ident) => {{
        Err(MuseError::$err($msg.into())).map_err(PyMusesErr::from)?;
        unreachable!()
    }}
);
