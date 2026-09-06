//! Bounded query contracts shared by HTML and machine-readable monitoring.

mod invocations;
mod page;

pub use invocations::load_invocation_rows;
pub use page::{Page, PageRequest, TotalCount, DEFAULT_PAGE_SIZE, PAGE_SIZES};
