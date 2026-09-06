//! Shared presentation primitives for monitoring pages.

mod actions;
mod invocation;
mod list;
mod pagination;
mod temporal;

pub use actions::{ActionKind, RowAction};
pub use invocation::InvocationRowView;
pub use list::{Badge, FilterSummaryItem, RowBase, RowLink};
pub use pagination::PaginationView;
pub use temporal::{page_time_range, TemporalExtent, TemporalPosition};
