//! SVG Timeline Engine for invocation visualization.
//!
//! Renders invocation history batches into an SVG timeline showing
//! status transitions across runners and time.

pub mod bounds;
pub mod builder;
pub mod color;
pub mod config;
pub mod data;
pub mod elements;
pub mod lane;
pub mod lane_assign;
pub mod models;
pub mod render;
pub mod render_atomic_service;
pub mod render_axis;
pub mod render_elements;
pub mod render_lanes;
pub mod runner_info;
pub mod tooltip;

pub use builder::TimelineDataBuilder;
pub use config::TimelineConfig;
pub use render::TimelineSvgRenderer;
