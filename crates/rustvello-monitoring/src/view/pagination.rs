//! One pagination view model for monitoring templates.

use crate::query::{PageRequest, TotalCount};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaginationView {
    pub page: usize,
    pub limit: usize,
    pub total_count: Option<usize>,
    pub total_pages: Option<usize>,
    pub has_prev: bool,
    pub has_next: bool,
}

impl PaginationView {
    pub fn new(request: PageRequest, total: TotalCount, has_next: bool) -> Self {
        let total_count = match total {
            TotalCount::Exact(total) => Some(total),
            TotalCount::AtLeast(_) | TotalCount::Unknown => None,
        };
        Self {
            page: request.page,
            limit: request.limit,
            total_count,
            total_pages: total_count.map(|total| total.div_ceil(request.limit).max(1)),
            has_prev: request.page > 1,
            has_next,
        }
    }
}
