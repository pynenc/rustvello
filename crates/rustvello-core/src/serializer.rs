use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{RustvelloError, RustvelloResult};

/// Serializer interface for converting values to/from strings.
///
/// Provides a uniform interface for the system, enabling custom
/// serialization strategies (e.g., for Python interop via PyO3).
///
/// In Rust, the default [`SerdeSerializer`] uses `serde_json` directly.
/// Python backends can implement this trait with pickle or custom codecs.
pub trait Serializer: Send + Sync {
    /// Serialize a value to its string representation.
    fn serialize<T: Serialize>(&self, value: &T) -> RustvelloResult<String>;

    /// Deserialize a string into the target type.
    fn deserialize<T: DeserializeOwned>(&self, data: &str) -> RustvelloResult<T>;
}

/// Default JSON serializer backed by serde_json.
///
/// This is the standard serializer for pure-Rust usage. It works with
/// any type implementing `Serialize`/`Deserialize`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SerdeSerializer;

impl Serializer for SerdeSerializer {
    fn serialize<T: Serialize>(&self, value: &T) -> RustvelloResult<String> {
        serde_json::to_string(value).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })
    }

    fn deserialize<T: DeserializeOwned>(&self, data: &str) -> RustvelloResult<T> {
        serde_json::from_str(data).map_err(|e| RustvelloError::Serialization {
            message: e.to_string(),
        })
    }
}

/// Type alias preserving backward compatibility.
pub type JsonSerializer = SerdeSerializer;

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Point {
        x: i32,
        y: i32,
    }

    #[test]
    fn serialize_struct() {
        let s = SerdeSerializer;
        let val = Point { x: 1, y: 2 };
        let result = s.serialize(&val).unwrap();
        assert!(result.contains("\"x\":1"));
        assert!(result.contains("\"y\":2"));
    }

    #[test]
    fn serialize_primitive() {
        let s = SerdeSerializer;
        assert_eq!(s.serialize(&42i32).unwrap(), "42");
        assert_eq!(s.serialize(&"hello").unwrap(), "\"hello\"");
        assert_eq!(s.serialize(&true).unwrap(), "true");
    }

    #[test]
    fn serialize_json_value() {
        let s = SerdeSerializer;
        let val = serde_json::json!({"key": "value", "num": 42});
        let result = s.serialize(&val).unwrap();
        assert!(result.contains("key"));
        assert!(result.contains("42"));
    }

    #[test]
    fn deserialize_struct() {
        let s = SerdeSerializer;
        let data = r#"{"x":10,"y":20}"#;
        let point: Point = s.deserialize(data).unwrap();
        assert_eq!(point, Point { x: 10, y: 20 });
    }

    #[test]
    fn deserialize_json_value() {
        let s = SerdeSerializer;
        let val: serde_json::Value = s.deserialize(r#"{"key":"value"}"#).unwrap();
        assert_eq!(val["key"], "value");
    }

    #[test]
    fn deserialize_invalid_json_errors() {
        let s = SerdeSerializer;
        let result: RustvelloResult<serde_json::Value> = s.deserialize("not json {{{");
        assert!(result.is_err());
    }

    #[test]
    fn round_trip_struct() {
        let s = SerdeSerializer;
        let original = Point { x: 42, y: -7 };
        let serialized = s.serialize(&original).unwrap();
        let back: Point = s.deserialize(&serialized).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn round_trip_json_value() {
        let s = SerdeSerializer;
        let original = serde_json::json!({"a": [1, 2, 3], "b": true});
        let serialized = s.serialize(&original).unwrap();
        let back: serde_json::Value = s.deserialize(&serialized).unwrap();
        assert_eq!(original, back);
    }
}
