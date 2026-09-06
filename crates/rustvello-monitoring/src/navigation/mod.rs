//! Canonical monitoring scope and navigation links.

mod links;
mod scope;
mod time_window;

pub use links::{MonitoringDestination, MonitoringLink};
pub use scope::MonitoringScope;
pub use time_window::{parse_datetime, TimeWindow, DEFAULT_TARGET_FILL};
