mod concurrency;
mod machine;
#[cfg(test)]
mod tests;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::sync::LazyLock;

use crate::identifiers::RunnerId;

pub use concurrency::ConcurrencyControlType;
pub use machine::{
    compute_new_owner, status_record_transition, validate_ownership, validate_transition,
    OwnershipError, StatusMachineError, StatusTransitionError,
};

// ============================================================================
// InvocationStatus enum
// ============================================================================

/// The lifecycle status of an invocation.
///
/// Follows a strict state machine — not all transitions are valid.
/// Mirrors pynenc's `InvocationStatus` enum exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum InvocationStatus {
    /// Invocation created and registered
    Registered,
    /// Paused due to concurrency control (transient — will be rerouted)
    ConcurrencyControlled,
    /// Permanently blocked by concurrency control
    ConcurrencyControlledFinal,
    /// Re-queued in the broker for another execution attempt
    Rerouted,
    /// Queued in the broker, waiting for a runner
    Pending,
    /// Pending recovery after runner failure (timeout exceeded)
    PendingRecovery,
    /// Currently being executed by a runner
    Running,
    /// Being re-executed during recovery (owner runner inactive)
    RunningRecovery,
    /// Task execution is paused
    Paused,
    /// Task execution has been resumed after pause
    Resumed,
    /// Task execution has been killed
    Killed,
    /// Completed successfully
    Success,
    /// Failed with an error
    Failed,
    /// Marked for retry after a failure
    Retry,
}

/// All status variants, for iteration.
pub const ALL_STATUSES: &[InvocationStatus] = &[
    InvocationStatus::Registered,
    InvocationStatus::ConcurrencyControlled,
    InvocationStatus::ConcurrencyControlledFinal,
    InvocationStatus::Rerouted,
    InvocationStatus::Pending,
    InvocationStatus::PendingRecovery,
    InvocationStatus::Running,
    InvocationStatus::RunningRecovery,
    InvocationStatus::Paused,
    InvocationStatus::Resumed,
    InvocationStatus::Killed,
    InvocationStatus::Success,
    InvocationStatus::Failed,
    InvocationStatus::Retry,
];

impl InvocationStatus {
    /// Returns true if this is a terminal (final) status.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        STATUS_CONFIG.definition(*self).is_final
    }

    /// Returns true if this status means the invocation can be picked up by a runner.
    #[inline]
    pub fn is_available_for_run(&self) -> bool {
        STATUS_CONFIG.definition(*self).available_for_run
    }

    /// Returns the set of valid next states from this status.
    #[inline]
    pub fn valid_transitions(&self) -> &[InvocationStatus] {
        &STATUS_CONFIG.definition(*self).allowed_transitions
    }

    /// Check if transitioning to `next` is valid.
    #[inline]
    pub fn can_transition_to(&self, next: InvocationStatus) -> bool {
        self.valid_transitions().contains(&next)
    }

    /// Returns all terminal statuses.
    pub fn final_statuses() -> &'static [InvocationStatus] {
        &STATUS_CONFIG.final_statuses
    }

    /// Returns all statuses where invocations can be picked up by runners.
    pub fn available_for_run_statuses() -> &'static [InvocationStatus] {
        &STATUS_CONFIG.available_for_run_statuses
    }
}

impl fmt::Display for InvocationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registered => write!(f, "REGISTERED"),
            Self::ConcurrencyControlled => write!(f, "CONCURRENCY_CONTROLLED"),
            Self::ConcurrencyControlledFinal => write!(f, "CONCURRENCY_CONTROLLED_FINAL"),
            Self::Rerouted => write!(f, "REROUTED"),
            Self::Pending => write!(f, "PENDING"),
            Self::PendingRecovery => write!(f, "PENDING_RECOVERY"),
            Self::Running => write!(f, "RUNNING"),
            Self::RunningRecovery => write!(f, "RUNNING_RECOVERY"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Resumed => write!(f, "RESUMED"),
            Self::Killed => write!(f, "KILLED"),
            Self::Success => write!(f, "SUCCESS"),
            Self::Failed => write!(f, "FAILED"),
            Self::Retry => write!(f, "RETRY"),
        }
    }
}

impl FromStr for InvocationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "REGISTERED" => Ok(Self::Registered),
            "CONCURRENCY_CONTROLLED" => Ok(Self::ConcurrencyControlled),
            "CONCURRENCY_CONTROLLED_FINAL" => Ok(Self::ConcurrencyControlledFinal),
            "REROUTED" => Ok(Self::Rerouted),
            "PENDING" => Ok(Self::Pending),
            "PENDING_RECOVERY" => Ok(Self::PendingRecovery),
            "RUNNING" => Ok(Self::Running),
            "RUNNING_RECOVERY" => Ok(Self::RunningRecovery),
            "PAUSED" => Ok(Self::Paused),
            "RESUMED" => Ok(Self::Resumed),
            "KILLED" => Ok(Self::Killed),
            "SUCCESS" => Ok(Self::Success),
            "FAILED" => Ok(Self::Failed),
            "RETRY" => Ok(Self::Retry),
            other => Err(format!("unknown invocation status: {other}")),
        }
    }
}

// ============================================================================
// InvocationStatusRecord
// ============================================================================

/// A status change record with ownership and timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationStatusRecord {
    pub status: InvocationStatus,
    pub runner_id: Option<RunnerId>,
    pub timestamp: DateTime<Utc>,
}

impl InvocationStatusRecord {
    pub fn new(status: InvocationStatus, runner_id: Option<RunnerId>) -> Self {
        Self {
            status,
            runner_id,
            timestamp: Utc::now(),
        }
    }
}

// ============================================================================
// StatusDefinition — declarative rules for each status
// ============================================================================

/// Declarative definition of status behavior and ownership rules.
///
/// Mirrors pynenc's `StatusDefinition` dataclass exactly.
#[derive(Debug, Clone)]
pub struct StatusDefinition {
    /// Valid next statuses from this status.
    pub allowed_transitions: Vec<InvocationStatus>,
    /// Terminates invocation lifecycle.
    pub is_final: bool,
    /// Can be picked up by runners via broker.
    pub available_for_run: bool,
    /// Only the owning runner can modify when in this status.
    pub requires_ownership: bool,
    /// Claims ownership on entry (sets runner_id).
    pub acquires_ownership: bool,
    /// Releases ownership on entry (clears runner_id).
    pub releases_ownership: bool,
    /// Bypasses ownership validation (for recovery scenarios).
    pub overrides_ownership: bool,
}

impl StatusDefinition {
    const fn new() -> Self {
        Self {
            allowed_transitions: Vec::new(),
            is_final: false,
            available_for_run: false,
            requires_ownership: false,
            acquires_ownership: false,
            releases_ownership: false,
            overrides_ownership: false,
        }
    }
}

// ============================================================================
// StatusConfiguration — complete config for all statuses
// ============================================================================

/// Complete configuration for invocation status behavior.
pub(super) struct StatusConfiguration {
    /// Definition for the "no status yet" (None → Registered) transition.
    pub(super) initial: StatusDefinition,
    /// Definitions indexed by status.
    definitions: Vec<(InvocationStatus, StatusDefinition)>,
    /// Cached: all terminal statuses.
    pub(super) final_statuses: Vec<InvocationStatus>,
    /// Cached: all available-for-run statuses.
    pub(super) available_for_run_statuses: Vec<InvocationStatus>,
}

impl StatusConfiguration {
    pub(super) fn definition(&self, status: InvocationStatus) -> &StatusDefinition {
        self.definitions
            .iter()
            .find(|(s, _)| *s == status)
            .map_or_else(
                || panic!("missing StatusDefinition for {status:?}"),
                |(_, d)| d,
            )
    }
}

/// Build the static status configuration. Mirrors pynenc's `_CONFIG` exactly.
fn build_config() -> StatusConfiguration {
    use InvocationStatus::*;

    let initial = StatusDefinition {
        allowed_transitions: vec![Registered],
        ..StatusDefinition::new()
    };

    let definitions = vec![
        (
            Registered,
            StatusDefinition {
                allowed_transitions: vec![
                    Pending,
                    ConcurrencyControlled,
                    ConcurrencyControlledFinal,
                ],
                available_for_run: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            ConcurrencyControlled,
            StatusDefinition {
                allowed_transitions: vec![Rerouted],
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Rerouted,
            StatusDefinition {
                allowed_transitions: vec![Pending, ConcurrencyControlled],
                available_for_run: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Pending,
            StatusDefinition {
                // An invocation can FAIL without running by the CYCLE-CONTROL mechanism
                // to avoid deadlocks.
                // PENDING_RECOVERY is for timeout recovery without ownership validation.
                allowed_transitions: vec![Running, Killed, Rerouted, Failed, PendingRecovery],
                requires_ownership: true,
                acquires_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            PendingRecovery,
            StatusDefinition {
                allowed_transitions: vec![Rerouted],
                releases_ownership: true,
                overrides_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Running,
            StatusDefinition {
                allowed_transitions: vec![Paused, Killed, Retry, Success, Failed, RunningRecovery],
                requires_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            RunningRecovery,
            StatusDefinition {
                allowed_transitions: vec![Rerouted],
                releases_ownership: true,
                overrides_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Paused,
            StatusDefinition {
                allowed_transitions: vec![Resumed, Killed],
                requires_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Resumed,
            StatusDefinition {
                allowed_transitions: vec![Paused, Killed, Retry, Success, Failed],
                requires_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Killed,
            StatusDefinition {
                allowed_transitions: vec![Rerouted],
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Retry,
            StatusDefinition {
                allowed_transitions: vec![Pending],
                available_for_run: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Success,
            StatusDefinition {
                is_final: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            Failed,
            StatusDefinition {
                is_final: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
        (
            ConcurrencyControlledFinal,
            StatusDefinition {
                is_final: true,
                releases_ownership: true,
                ..StatusDefinition::new()
            },
        ),
    ];

    let final_statuses: Vec<_> = definitions
        .iter()
        .filter(|(_, d)| d.is_final)
        .map(|(s, _)| *s)
        .collect();
    let available_for_run_statuses: Vec<_> = definitions
        .iter()
        .filter(|(_, d)| d.available_for_run)
        .map(|(s, _)| *s)
        .collect();

    StatusConfiguration {
        initial,
        definitions,
        final_statuses,
        available_for_run_statuses,
    }
}

pub(super) static STATUS_CONFIG: LazyLock<StatusConfiguration> = LazyLock::new(build_config);

/// Get the status definition for a given status.
pub fn get_status_definition(status: InvocationStatus) -> &'static StatusDefinition {
    STATUS_CONFIG.definition(status)
}

/// Get the status definition for the initial (None) state.
pub fn get_initial_definition() -> &'static StatusDefinition {
    &STATUS_CONFIG.initial
}
