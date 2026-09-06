//! Base fields shared by typed monitoring rows.

use super::{RowAction, TemporalExtent};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowLink {
    pub label: String,
    pub href: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Badge {
    pub label: String,
    pub class: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowBase {
    pub key: String,
    pub primary: RowLink,
    pub badges: Vec<Badge>,
    pub actions: Vec<RowAction>,
    pub temporal_extent: Option<TemporalExtent>,
}

/// A compact, removable representation of one active filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilterSummaryItem {
    pub label: String,
    pub value: String,
    pub remove_url: String,
    pub removable: bool,
}
