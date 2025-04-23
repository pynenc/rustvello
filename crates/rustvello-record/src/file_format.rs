//! Defines the serialization formats for recording files.
//!
//! The recording files should have a specific extension to determine
//! the encoding/serialization format. Supported extensions are:
//!
//! - `.bin` for Bincode serialization
//! - `.json` for JSON serialization
//!
//! The serialization format is used internally by the [`FileRecorder`] and [`FileReplayer`].

use rustvello_core::{MuseError, MuseResult};

/// Enum representing the supported serialization formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SerializationFormat {
    /// Bincode serialization format.
    Bincode,
    /// JSON serialization format.
    Json,
}

impl SerializationFormat {
    /// Determines the serialization format from a file extension.
    ///
    /// # Arguments
    ///
    /// - `ext`: The file extension (without the dot).
    ///
    /// # Returns
    ///
    /// A `SerializationFormat` corresponding to the file extension.
    ///
    /// # Errors
    ///
    /// Returns [`MuseError::InvalidFileExtension`] if the extension is not supported.
    pub fn from_extension(ext: Option<&str>) -> MuseResult<Self> {
        match ext.map(|s| s.to_lowercase()) {
            Some(ref s) if s == "bin" => Ok(SerializationFormat::Bincode),
            Some(ref s) if s == "json" => Ok(SerializationFormat::Json),
            other => Err(MuseError::InvalidFileExtension(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_extensions() {
        assert_eq!(
            SerializationFormat::from_extension(Some("bin")).unwrap(),
            SerializationFormat::Bincode
        );
        assert_eq!(
            SerializationFormat::from_extension(Some("json")).unwrap(),
            SerializationFormat::Json
        );
    }

    #[test]
    fn test_invalid_extension() {
        let result = SerializationFormat::from_extension(Some("xml"));
        assert!(result.is_err());
        if let Err(MuseError::InvalidFileExtension(Some(ext))) = result {
            assert_eq!(ext, "xml");
        } else {
            panic!("Expected InvalidFileExtension error");
        }
    }

    #[test]
    fn test_empty_extension() {
        let result = SerializationFormat::from_extension(None);
        assert!(result.is_err());
        if let Err(MuseError::InvalidFileExtension(None)) = result {
            // Test passed
        } else {
            panic!("Expected InvalidFileExtension error for None");
        }
    }

    #[test]
    fn test_case_insensitivity() {
        assert_eq!(
            SerializationFormat::from_extension(Some("BIN")).unwrap(),
            SerializationFormat::Bincode
        );
        assert_eq!(
            SerializationFormat::from_extension(Some("Json")).unwrap(),
            SerializationFormat::Json
        );
    }
}
