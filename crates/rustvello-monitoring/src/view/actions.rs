//! Consistent labels and icons for monitoring actions.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    Timeline,
    Zoom,
    Details,
    Relationships,
    Back,
}

impl ActionKind {
    pub const fn icon(self) -> &'static str {
        match self {
            Self::Timeline => "timeline",
            Self::Zoom => "zoom_in",
            Self::Details => "info",
            Self::Relationships => "account_tree",
            Self::Back => "arrow_back",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Timeline => "Open in timeline",
            Self::Zoom => "Zoom current view",
            Self::Details => "View details",
            Self::Relationships => "View relationships",
            Self::Back => "Back",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowAction {
    pub kind: ActionKind,
    pub href: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_and_zoom_have_distinct_icons() {
        assert_eq!(ActionKind::Timeline.icon(), "timeline");
        assert_eq!(ActionKind::Zoom.icon(), "zoom_in");
        assert_ne!(ActionKind::Timeline.icon(), ActionKind::Zoom.icon());
    }
}
