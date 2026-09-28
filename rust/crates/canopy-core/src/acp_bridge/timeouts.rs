//! ACP bridge operation deadlines and retry hints.

use thiserror::Error;

pub const DEFAULT_SESSION_RESTORE_TIMEOUT_MS: u64 = 60_000;
pub const MAX_SESSION_RESTORE_TIMEOUT_MS: u64 = 2_147_483_647;
pub const MIN_RESTORE_RETRY_AFTER_SECONDS: u64 = 5;
pub const MAX_RESTORE_RETRY_AFTER_SECONDS: u64 = 120;
pub const MCP_RESTART_SERVER_DEADLINE_MS: u64 = 300_000;
pub const MCP_RESTART_CLIENT_HEADROOM_MS: u64 = 30_000;
pub const MAX_DAEMON_WORKSPACES: u64 = 25;
pub const CHANNEL_WORKER_STARTUP_TIMEOUT_MS: u64 = 30_000;
pub const CHANNEL_WORKER_STOP_GRACE_MS: u64 = 10_000;
pub const CHANNEL_WORKER_KILL_GRACE_MS: u64 = 2_000;
pub const CHANNEL_CONTROL_CLIENT_HEADROOM_MS: u64 = 30_000;

pub const CHANNEL_CONTROL_DEFAULT_TIMEOUT_MS: u64 = 2
    * MAX_DAEMON_WORKSPACES
    * (CHANNEL_WORKER_STOP_GRACE_MS
        + CHANNEL_WORKER_KILL_GRACE_MS
        + CHANNEL_WORKER_STARTUP_TIMEOUT_MS)
    + CHANNEL_CONTROL_CLIENT_HEADROOM_MS;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionRestoreTimeoutOptions {
    pub session_restore_timeout_ms: Option<u64>,
    pub initialize_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TimeoutConfigError {
    #[error("Invalid {field}: {value}. Must be a positive integer no greater than {maximum}.")]
    Invalid {
        field: &'static str,
        value: u64,
        maximum: u64,
    },
}

pub fn resolve_session_restore_timeout_ms(
    options: SessionRestoreTimeoutOptions,
) -> Result<u64, TimeoutConfigError> {
    if let Some(timeout) = options.session_restore_timeout_ms {
        validate_timeout("sessionRestoreTimeoutMs", timeout)?;
        return Ok(timeout);
    }
    if let Some(timeout) = options.initialize_timeout_ms {
        validate_timeout("initializeTimeoutMs", timeout)?;
        return Ok(timeout.max(DEFAULT_SESSION_RESTORE_TIMEOUT_MS));
    }
    Ok(DEFAULT_SESSION_RESTORE_TIMEOUT_MS)
}

fn validate_timeout(field: &'static str, value: u64) -> Result<(), TimeoutConfigError> {
    if value == 0 || value > MAX_SESSION_RESTORE_TIMEOUT_MS {
        return Err(TimeoutConfigError::Invalid {
            field,
            value,
            maximum: MAX_SESSION_RESTORE_TIMEOUT_MS,
        });
    }
    Ok(())
}

pub fn restore_retry_after_seconds(timeout_ms: u64) -> u64 {
    timeout_ms.div_ceil(1_000).clamp(
        MIN_RESTORE_RETRY_AFTER_SECONDS,
        MAX_RESTORE_RETRY_AFTER_SECONDS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_budget_explicit_wins_and_initialize_only_raises_default() {
        assert_eq!(
            resolve_session_restore_timeout_ms(SessionRestoreTimeoutOptions {
                session_restore_timeout_ms: Some(10_000),
                initialize_timeout_ms: Some(120_000),
            }),
            Ok(10_000)
        );
        assert_eq!(
            resolve_session_restore_timeout_ms(SessionRestoreTimeoutOptions {
                session_restore_timeout_ms: None,
                initialize_timeout_ms: Some(10_000),
            }),
            Ok(DEFAULT_SESSION_RESTORE_TIMEOUT_MS)
        );
        assert_eq!(
            resolve_session_restore_timeout_ms(SessionRestoreTimeoutOptions {
                session_restore_timeout_ms: None,
                initialize_timeout_ms: Some(120_000),
            }),
            Ok(120_000)
        );
    }

    #[test]
    fn timeout_validation_and_retry_hints_keep_the_source_bounds() {
        assert!(
            resolve_session_restore_timeout_ms(SessionRestoreTimeoutOptions {
                session_restore_timeout_ms: Some(0),
                initialize_timeout_ms: None,
            })
            .is_err()
        );
        assert!(
            resolve_session_restore_timeout_ms(SessionRestoreTimeoutOptions {
                session_restore_timeout_ms: Some(MAX_SESSION_RESTORE_TIMEOUT_MS + 1),
                initialize_timeout_ms: None,
            })
            .is_err()
        );
        assert_eq!(restore_retry_after_seconds(1), 5);
        assert_eq!(restore_retry_after_seconds(61_000), 61);
        assert_eq!(restore_retry_after_seconds(u64::MAX), 120);
    }
}
