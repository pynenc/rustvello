// crates/rustvello-proto/src/element_kind.rs

use serde::{Deserialize, Serialize};

use crate::types::*;
use crate::utils::deterministic_u64_from_str;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ElementKindRegistration {
    id: ElementKindId,
    pub code: String,
    parent_id: Option<ElementKindId>,
    pub name: String,
    pub description: String,
    forwarded: bool,
}

impl ElementKindRegistration {
    pub fn new(code: &str, parent_code: Option<&str>, name: &str, description: &str) -> Self {
        Self {
            id: deterministic_u64_from_str(code),
            code: code.to_string(),
            parent_id: parent_code.map(deterministic_u64_from_str),
            name: name.to_string(),
            description: description.to_string(),
            forwarded: false,
        }
    }
}
