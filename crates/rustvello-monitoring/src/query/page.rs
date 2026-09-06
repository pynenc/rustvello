//! Common pagination types.

pub const PAGE_SIZES: [usize; 4] = [25, 50, 100, 200];
pub const DEFAULT_PAGE_SIZE: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageRequest {
    pub page: usize,
    pub limit: usize,
}

impl PageRequest {
    pub fn new(page: Option<usize>, limit: Option<usize>) -> Self {
        Self {
            page: page.unwrap_or(1).max(1),
            limit: limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, 200),
        }
    }

    pub fn offset(self) -> usize {
        (self.page - 1).saturating_mul(self.limit)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TotalCount {
    Exact(usize),
    AtLeast(usize),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub request: PageRequest,
    pub total: TotalCount,
    pub has_next: bool,
}

impl<T> Page<T> {
    pub fn from_offset_items(mut items: Vec<T>, request: PageRequest, total: usize) -> Self {
        let start = request.offset().min(items.len());
        let end = start.saturating_add(request.limit).min(items.len());
        let page_items = items.drain(start..end).collect();
        Self {
            items: page_items,
            request,
            total: TotalCount::Exact(total),
            has_next: end < total,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_clamps_invalid_page_sizes() {
        assert_eq!(PageRequest::new(Some(0), Some(50)).page, 1);
        assert_eq!(PageRequest::new(Some(2), Some(1000)).limit, 200);
    }

    #[test]
    fn offset_page_has_stable_boundaries() {
        let page = Page::from_offset_items((0..8).collect(), PageRequest::new(Some(2), Some(3)), 8);
        assert_eq!(page.items, vec![3, 4, 5]);
        assert!(page.has_next);
    }
}
