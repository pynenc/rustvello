// crates/rustvello-proto/src/element.rs

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::*;
use crate::utils::deterministic_u64_from_str;

pub fn generate_local_element_id() -> Uuid {
    Uuid::new_v4()
}

// src/lib.rs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ElementRegistration {
    kind_id: ElementKindId,
    pub name: String,
    pub metadata: HashMap<String, String>,
    pub parent_id: Option<ElementId>,
}

impl ElementRegistration {
    pub fn new(
        kind_code: &str,
        name: String,
        metadata: HashMap<String, String>,
        parent_id: Option<ElementId>,
    ) -> Self {
        Self {
            kind_id: deterministic_u64_from_str(kind_code),
            name,
            metadata,
            parent_id,
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct NewElementsResponse {
    // Using String for errors instead of Error
    pub results: Vec<Result<u64, String>>,
}
