//! # rustvello: *The Common rustvello Library for Rust*
//!
//! rustvello is a Rust library providing functionality to interact with the IH system,
//! including element registration, metric reporting, and recording/replaying for testing.
//! It's designed to be efficient, scalable, and easy to integrate into your applications.
//!
//! ## **Quickstart**
//!
//! Here's a simple example demonstrating how to use rustvello:
//!
//! ```rust,no_run
//! use rustvello::prelude::*;
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> MuseResult<()> {
//!     let config = Config::new(
//!         vec!["http://localhost:8080".to_string()],
//!         ClientType::Poet,
//!         false,
//!         None,
//!         None,
//!         TimestampResolution::Milliseconds,
//!         vec![ElementKindRegistration::new("kind_code", Some("parent_code"), "kind_name", "description")],
//!         vec![MetricDefinition::new("metric_code", "metric_name", "description")],
//!         Some(std::time::Duration::from_secs(60)),
//!         Some(std::time::Duration::from_secs(60)),
//!         3,
//!         None,
//!     )?;
//!
//!     let mut muse = Muse::new(&config)?;
//!     muse.initialize(Some(std::time::Duration::from_secs(5))).await?;
//!
//!     let local_elem_id = muse
//!         .register_element(
//!             "kind_code",
//!             "Element Name".to_string(),
//!             HashMap::new(),
//!             None,
//!         )
//!         .await?;
//!
//!     muse.send_metric(local_elem_id, "metric_code", MetricValue::from(42.0))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## **Features**
//!
//! - **Element Registration:** Easily register elements with specific kinds and metadata.
//! - **Metric Reporting:** Send metrics associated with elements to the Muse system.
//! - **Event Recording:** Record events for later analysis or replaying.
//! - **Client Configuration:** Support for different client types (`Poet`, `Mock`).
//!
//! ## **Modules**
//!
//! - [`config`]: Contains configuration structures and enums.
//! - [`muse`]: Main module containing the `Muse` struct.
//! - [`tasks`]: Internal tasks handling background operations.
//!
//! ## **License**
//!
//! This project is licensed under the MIT License.
//!
//! [`config`]: crate::config
//! [`muse`]: crate::muse
//! [`tasks`]: crate::tasks

mod muse;
pub mod prelude;
mod tasks;
pub mod timing;

pub use rustvello_core::MuseError;
pub use muse::Muse;

/// Polars crate version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
