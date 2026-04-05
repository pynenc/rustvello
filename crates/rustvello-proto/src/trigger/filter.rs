//! Filter and matching helper functions for trigger conditions.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Compute the filter-id string for an optional argument filter.
///
/// Matches pynenc's `StaticArgumentFilter.filter_id`:
/// - `None` or empty map → `"static_no_args"`
/// - non-empty map → `"static_"` + SHA-256 of JSON-serialized arguments
pub(super) fn argument_filter_id(filter: &Option<BTreeMap<String, serde_json::Value>>) -> String {
    match filter {
        None => "static_no_args".to_string(),
        Some(f) if f.is_empty() => "static_no_args".to_string(),
        Some(f) => {
            // Match Python's json.dumps(sorted_dict, sort_keys=True, default=str)
            // which uses ", " and ": " separators. BTreeMap iteration is sorted by key.
            let json = python_compatible_json(f);
            let hash = Sha256::digest(json.as_bytes());
            format!("static_{:x}", hash)
        }
    }
}

/// Compute the result-filter-id string.
///
/// Matches pynenc's `NoResultFilter.filter_id` = `"no_result_filter"`.
pub(super) fn result_filter_id(filter: &Option<serde_json::Value>) -> String {
    match filter {
        None => "no_result_filter".to_string(),
        Some(v) => {
            let json = serde_json::to_string(v).unwrap_or_default();
            let hash = Sha256::digest(json.as_bytes());
            format!("{:x}", hash)
        }
    }
}

/// Produce JSON matching Python's `json.dumps(obj, sort_keys=True)` format.
///
/// Python's default separators are `", "` and `": "` (with spaces).
/// Rust's `serde_json::to_string` produces compact JSON (no spaces).
/// We need to match Python's format for the SHA-256 hash to be identical.
fn python_compatible_json(map: &BTreeMap<String, serde_json::Value>) -> String {
    fn value_to_python_json(v: &serde_json::Value) -> String {
        match v {
            serde_json::Value::Null => "null".to_string(),
            serde_json::Value::Bool(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => {
                format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
            }
            serde_json::Value::Array(arr) => {
                let items: Vec<String> = arr.iter().map(value_to_python_json).collect();
                format!("[{}]", items.join(", "))
            }
            serde_json::Value::Object(obj) => {
                // BTreeMap from serde_json::Map is already sorted
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "\"{}\": {}",
                            k.replace('\\', "\\\\").replace('"', "\\\""),
                            value_to_python_json(v)
                        )
                    })
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
    // Top-level is always an object (BTreeMap)
    let items: Vec<String> = map
        .iter()
        .map(|(k, v)| {
            format!(
                "\"{}\": {}",
                k.replace('\\', "\\\\").replace('"', "\\\""),
                value_to_python_json(v)
            )
        })
        .collect();
    format!("{{{}}}", items.join(", "))
}

/// Check whether serialized arguments satisfy an optional subset-match filter.
///
/// Each value in `filter` is compared to the corresponding arg after parsing
/// it from its JSON string representation.  `None` filter matches everything.
pub(super) fn check_argument_filter(
    filter: &Option<BTreeMap<String, serde_json::Value>>,
    args: &BTreeMap<String, String>,
) -> bool {
    match filter {
        None => true,
        Some(expected) => expected.iter().all(|(k, v)| {
            args.get(k)
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .is_some_and(|actual| &actual == v)
        }),
    }
}

/// Check whether a JSON result value matches an optional exact-equality filter.
pub(super) fn check_result_filter(
    filter: &Option<serde_json::Value>,
    result: &serde_json::Value,
) -> bool {
    match filter {
        None => true,
        Some(expected) => expected == result,
    }
}

/// Check whether an event payload satisfies an optional subset-match filter.
///
/// Works on the payload `serde_json::Value` directly (not on serialized strings).
pub(super) fn check_payload_filter(
    filter: &Option<BTreeMap<String, serde_json::Value>>,
    payload: &serde_json::Value,
) -> bool {
    match filter {
        None => true,
        Some(expected) => {
            let obj = match payload.as_object() {
                Some(o) => o,
                None => return expected.is_empty(),
            };
            expected.iter().all(|(k, v)| obj.get(k) == Some(v))
        }
    }
}
