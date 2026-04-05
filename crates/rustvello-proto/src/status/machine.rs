use std::fmt;

use crate::identifiers::RunnerId;

use super::{InvocationStatus, InvocationStatusRecord, STATUS_CONFIG};

// ============================================================================
// State Machine Error Types
// ============================================================================

/// Error returned when a status transition is invalid.
#[derive(Debug, Clone)]
pub struct StatusTransitionError {
    pub from: Option<InvocationStatus>,
    pub to: InvocationStatus,
    pub allowed: Vec<InvocationStatus>,
}

impl fmt::Display for StatusTransitionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.from {
            Some(from) => write!(f, "invalid status transition: {} -> {}", from, self.to),
            None => write!(f, "invalid initial status: {}", self.to),
        }
    }
}

/// Error returned when ownership rules are violated.
#[derive(Debug, Clone)]
pub struct OwnershipError {
    pub from_status: InvocationStatus,
    pub to_status: InvocationStatus,
    pub current_owner: Option<String>,
    pub attempted_owner: Option<String>,
    pub reason: String,
}

impl fmt::Display for OwnershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ownership violation ({} -> {}): {}",
            self.from_status, self.to_status, self.reason
        )
    }
}

/// Combined error type for the state machine.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum StatusMachineError {
    Transition(StatusTransitionError),
    Ownership(OwnershipError),
}

impl fmt::Display for StatusMachineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transition(e) => write!(f, "{e}"),
            Self::Ownership(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for StatusTransitionError {}
impl std::error::Error for OwnershipError {}
impl std::error::Error for StatusMachineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transition(e) => Some(e),
            Self::Ownership(e) => Some(e),
        }
    }
}

// ============================================================================
// State Machine Functions
// ============================================================================

/// Validate state transition or return error.
pub fn validate_transition(
    from_status: Option<InvocationStatus>,
    to_status: InvocationStatus,
) -> Result<(), StatusTransitionError> {
    let definition = match from_status {
        Some(s) => STATUS_CONFIG.definition(s),
        None => &STATUS_CONFIG.initial,
    };
    if definition.allowed_transitions.contains(&to_status) {
        Ok(())
    } else {
        Err(StatusTransitionError {
            from: from_status,
            to: to_status,
            allowed: definition.allowed_transitions.clone(),
        })
    }
}

/// Validate ownership requirements for a transition.
pub fn validate_ownership(
    current_record: Option<&InvocationStatusRecord>,
    new_status: InvocationStatus,
    runner_id: Option<&RunnerId>,
) -> Result<(), OwnershipError> {
    let current_record = match current_record {
        Some(r) => r,
        None => return Ok(()),
    };

    let new_def = STATUS_CONFIG.definition(new_status);

    // Allow transitions to statuses that override ownership validation
    if new_def.overrides_ownership {
        return Ok(());
    }

    let current_def = STATUS_CONFIG.definition(current_record.status);

    if current_def.requires_ownership {
        let current_owner = current_record.runner_id.as_ref().map(RunnerId::as_str);
        let requester = runner_id.map(RunnerId::as_str);
        if requester != current_owner {
            return Err(OwnershipError {
                from_status: current_record.status,
                to_status: new_status,
                current_owner: current_owner.map(String::from),
                attempted_owner: requester.map(String::from),
                reason: format!(
                    "status requires ownership by runner '{}'",
                    current_owner.unwrap_or("<none>")
                ),
            });
        }
    }

    if new_def.acquires_ownership && runner_id.is_none() {
        return Err(OwnershipError {
            from_status: current_record.status,
            to_status: new_status,
            current_owner: current_record
                .runner_id
                .as_ref()
                .map(|r| r.as_str().to_string()),
            attempted_owner: None,
            reason: format!("status {new_status} requires a runner_id to acquire ownership"),
        });
    }

    Ok(())
}

/// Compute new owner based on status transition.
pub fn compute_new_owner(
    current_record: Option<&InvocationStatusRecord>,
    new_status: InvocationStatus,
    runner_id: Option<RunnerId>,
) -> Option<RunnerId> {
    let new_def = STATUS_CONFIG.definition(new_status);
    if new_def.releases_ownership {
        None
    } else if new_def.acquires_ownership {
        runner_id
    } else {
        current_record.and_then(|r| r.runner_id.clone())
    }
}

/// Execute a status change with full validation (transition + ownership).
///
/// Returns a new `InvocationStatusRecord` with the correct owner.
pub fn status_record_transition(
    current_record: Option<&InvocationStatusRecord>,
    new_status: InvocationStatus,
    runner_id: Option<&RunnerId>,
) -> Result<InvocationStatusRecord, StatusMachineError> {
    let from_status = current_record.map(|r| r.status);

    validate_transition(from_status, new_status).map_err(StatusMachineError::Transition)?;
    validate_ownership(current_record, new_status, runner_id)
        .map_err(StatusMachineError::Ownership)?;

    let new_owner = compute_new_owner(current_record, new_status, runner_id.cloned());
    Ok(InvocationStatusRecord::new(new_status, new_owner))
}
