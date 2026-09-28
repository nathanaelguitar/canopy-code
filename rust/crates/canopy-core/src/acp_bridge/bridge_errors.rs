//! Typed errors shared by the ACP bridge primitives.

use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BridgeError {
    #[error("No session with id \"{session_id}\"")]
    SessionNotFound { session_id: String },
    #[error("Session \"{session_id}\" is archived. Unarchive it before loading.")]
    SessionArchived { session_id: String },
    #[error("Session \"{session_id}\" exists in both active and archived directories.")]
    SessionConflict { session_id: String },
    #[error("Session limit reached ({limit})")]
    SessionLimitExceeded { limit: usize },
    #[error("prompt exceeded the {deadline_ms}ms deadline")]
    PromptDeadlineExceeded { deadline_ms: u64 },
    #[error(
        "Workspace mismatch: runtime is bound to \"{bound}\" but request asked for \"{requested}\""
    )]
    WorkspaceMismatch { bound: String, requested: String },
    #[error(
        "Permission {request_id}: optionId \"{option_id}\" is not in the set of options the agent offered."
    )]
    InvalidPermissionOption {
        request_id: String,
        option_id: String,
    },
    #[error(
        "Permission {request_id}: agent-declared optionId set contains the cancel-vote sentinel \"{sentinel}\""
    )]
    CancelSentinelCollision {
        request_id: String,
        sentinel: String,
    },
    #[error(
        "Permission request {request_id} on session {session_id} was rejected by policy ({reason})"
    )]
    PermissionForbidden {
        request_id: String,
        session_id: String,
        reason: String,
    },
}

/// Matches the harmless idle-cancel wording accepted by the TS bridge.
pub fn is_not_currently_generating_cancel(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not currently generating")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_cancel_match_is_narrow_and_case_insensitive() {
        assert!(is_not_currently_generating_cancel(
            "Not currently generating (idle)"
        ));
        assert!(!is_not_currently_generating_cancel("cancel failed"));
    }
}
