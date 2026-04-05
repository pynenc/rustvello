//! Status-to-color mappings for invocation statuses.

use rustvello_proto::status::InvocationStatus;

/// Return a hex color string for a given invocation status.
/// Colors match the pynenc/pynmon palette for visual consistency.
pub fn hex_color(status: &InvocationStatus) -> &'static str {
    match status {
        InvocationStatus::Registered => "#95a5a6",
        InvocationStatus::Pending => "#f39c12",
        InvocationStatus::Running => "#3498db",
        InvocationStatus::Success => "#27ae60",
        InvocationStatus::Failed => "#e74c3c",
        InvocationStatus::Retry => "#9b59b6",
        InvocationStatus::ConcurrencyControlled => "#e67e22",
        InvocationStatus::ConcurrencyControlledFinal => "#d35400",
        InvocationStatus::Rerouted => "#16a085",
        InvocationStatus::PendingRecovery => "#e67e22",
        InvocationStatus::RunningRecovery => "#e67e22",
        InvocationStatus::Paused => "#1abc9c",
        InvocationStatus::Resumed => "#2980b9",
        InvocationStatus::Killed => "#c0392b",
        _ => "#95a5a6",
    }
}

/// Return a Bootstrap badge CSS class for a given status.
pub fn badge_class(status: &InvocationStatus) -> &'static str {
    match status {
        InvocationStatus::Registered => "bg-secondary",
        InvocationStatus::Pending => "bg-warning text-dark",
        InvocationStatus::Running => "bg-primary",
        InvocationStatus::Success => "bg-success",
        InvocationStatus::Failed => "bg-danger",
        InvocationStatus::Retry => "bg-warning",
        InvocationStatus::ConcurrencyControlled => "bg-info",
        InvocationStatus::ConcurrencyControlledFinal => "bg-info",
        InvocationStatus::Rerouted => "bg-secondary",
        InvocationStatus::PendingRecovery => "bg-warning text-dark",
        InvocationStatus::RunningRecovery => "bg-primary",
        InvocationStatus::Paused => "bg-secondary",
        InvocationStatus::Resumed => "bg-primary",
        InvocationStatus::Killed => "bg-danger",
        _ => "bg-secondary",
    }
}

/// Statuses that are shown as segments (bars) in the timeline.
pub const SEGMENT_STATUSES: &[InvocationStatus] = &[
    InvocationStatus::Pending,
    InvocationStatus::Running,
    InvocationStatus::Paused,
    InvocationStatus::Resumed,
];

/// Statuses that represent terminal outcomes.
pub const OUTCOME_STATUSES: &[InvocationStatus] = &[
    InvocationStatus::Success,
    InvocationStatus::Failed,
    InvocationStatus::Retry,
    InvocationStatus::ConcurrencyControlledFinal,
    InvocationStatus::Rerouted,
];

/// Default color for unknown statuses.
pub const DEFAULT_COLOR: &str = "#7f8c8d";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_colors_match_pynenc_palette() {
        // Verify each status color matches pynmon/util/status_colors.py exactly
        assert_eq!(hex_color(&InvocationStatus::Registered), "#95a5a6");
        assert_eq!(hex_color(&InvocationStatus::Pending), "#f39c12");
        assert_eq!(hex_color(&InvocationStatus::Running), "#3498db");
        assert_eq!(hex_color(&InvocationStatus::Success), "#27ae60");
        assert_eq!(hex_color(&InvocationStatus::Failed), "#e74c3c");
        assert_eq!(hex_color(&InvocationStatus::Retry), "#9b59b6");
        assert_eq!(
            hex_color(&InvocationStatus::ConcurrencyControlled),
            "#e67e22"
        );
        assert_eq!(
            hex_color(&InvocationStatus::ConcurrencyControlledFinal),
            "#d35400"
        );
        assert_eq!(hex_color(&InvocationStatus::Rerouted), "#16a085");
        assert_eq!(hex_color(&InvocationStatus::PendingRecovery), "#e67e22");
        assert_eq!(hex_color(&InvocationStatus::RunningRecovery), "#e67e22");
        assert_eq!(hex_color(&InvocationStatus::Paused), "#1abc9c");
        assert_eq!(hex_color(&InvocationStatus::Resumed), "#2980b9");
        assert_eq!(hex_color(&InvocationStatus::Killed), "#c0392b");
    }

    #[test]
    fn test_default_color() {
        assert_eq!(DEFAULT_COLOR, "#7f8c8d");
    }

    #[test]
    fn test_segment_statuses_are_duration_statuses() {
        assert!(SEGMENT_STATUSES.contains(&InvocationStatus::Pending));
        assert!(SEGMENT_STATUSES.contains(&InvocationStatus::Running));
        assert!(SEGMENT_STATUSES.contains(&InvocationStatus::Paused));
        assert!(SEGMENT_STATUSES.contains(&InvocationStatus::Resumed));
        // Points-only statuses should NOT be in SEGMENT_STATUSES
        assert!(!SEGMENT_STATUSES.contains(&InvocationStatus::Success));
        assert!(!SEGMENT_STATUSES.contains(&InvocationStatus::Failed));
        assert!(!SEGMENT_STATUSES.contains(&InvocationStatus::Registered));
    }
}
