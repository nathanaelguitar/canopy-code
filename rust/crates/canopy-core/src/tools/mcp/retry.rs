//! Bounded retry policy for transient MCP transport failures.
//!
//! This mirrors `packages/core/src/tools/mcp-retry.ts`. It stays separate
//! from provider retry classification because MCP retries intentionally use a
//! much smaller set of transient conditions and a short fixed backoff.

use crate::utils::cancellation::CancellationToken;
use regex::Regex;
use std::error::Error;
use std::fmt::{self, Display};
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

pub const DEFAULT_MAX_RETRIES: usize = 2;
pub const DEFAULT_BASE_DELAY: Duration = Duration::from_millis(200);

const TRANSIENT_ERROR_CODES: [&str; 8] = [
    "ECONNRESET",
    "ETIMEDOUT",
    "ENOTFOUND",
    "ECONNREFUSED",
    "EAI_AGAIN",
    "EPIPE",
    "EHOSTUNREACH",
    "ENETUNREACH",
];

static TRANSIENT_HTTP_STATUS_PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();

fn transient_http_status_patterns() -> &'static [Regex] {
    TRANSIENT_HTTP_STATUS_PATTERNS.get_or_init(|| {
        [
            r"(?i)\b(502|503|504)\s+(Bad Gateway|Service Unavailable|Gateway Timeout)",
            r"(?i)\bstatus\s*(?:code)?\s*[:=]?\s*(502|503|504)\b",
            r"(?i)\bHTTP/\S+\s+(502|503|504)\b",
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("valid MCP retry status regex"))
        .collect()
    })
}

/// Return whether an error message and optional JSON-RPC code identify a
/// transient MCP failure. The caller supplies the message using its normal
/// error formatter; `json_rpc_code` mirrors the source's `.code` check.
pub fn is_transient_network_error(message: &str, json_rpc_code: Option<f64>) -> bool {
    if json_rpc_code.is_some_and(|code| code == -32601.0 || code == -32600.0 || code == -32602.0) {
        return false;
    }
    if message.contains("401") || message.contains("403") {
        return false;
    }
    if TRANSIENT_ERROR_CODES
        .iter()
        .any(|code| message.contains(code))
    {
        return true;
    }
    if transient_http_status_patterns()
        .iter()
        .any(|pattern| pattern.is_match(message))
    {
        return true;
    }
    message.contains("Connection closed")
        || message.contains("transport error")
        || message.contains("Streamable HTTP connection")
}

/// MCP-specific retry settings. Defaults match the TypeScript helper: two
/// retries after the initial request and a 200 ms exponential base delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpRetryOptions {
    pub max_retries: usize,
    pub base_delay: Duration,
}

impl Default for McpRetryOptions {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            base_delay: DEFAULT_BASE_DELAY,
        }
    }
}

/// A retry operation either exhausted its retry budget / hit a permanent
/// failure, or its backoff was cancelled.
#[derive(Debug)]
pub enum McpRetryError<E> {
    Operation(E),
    Aborted,
}

impl<E: Display> Display for McpRetryError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => Display::fmt(error, formatter),
            Self::Aborted => formatter.write_str("Retry aborted"),
        }
    }
}

impl<E: Error + 'static> Error for McpRetryError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            Self::Aborted => None,
        }
    }
}

/// Retry a fallible async operation with bounded exponential backoff.
///
/// `should_retry` adapts the caller's MCP error type to
/// [`is_transient_network_error`]. The original final error is returned
/// unchanged. Cancellation is observed while waiting between attempts, as in
/// the TypeScript helper.
pub async fn retry_with_backoff<T, E, F, Fut, C>(
    mut operation: F,
    options: McpRetryOptions,
    cancellation: Option<&CancellationToken>,
    should_retry: C,
) -> Result<T, McpRetryError<E>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: Fn(&E) -> bool,
{
    for retry_index in 0..=options.max_retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if retry_index == options.max_retries || !should_retry(&error) => {
                return Err(McpRetryError::Operation(error));
            }
            Err(_) => {
                let delay = exponential_backoff(options.base_delay, retry_index);
                if !wait_with_abort(delay, cancellation).await {
                    return Err(McpRetryError::Aborted);
                }
            }
        }
    }
    unreachable!("inclusive retry range always returns or errors")
}

fn exponential_backoff(base_delay: Duration, retry_index: usize) -> Duration {
    let multiplier = u32::try_from(retry_index)
        .ok()
        .and_then(|shift| 1_u32.checked_shl(shift))
        .unwrap_or(u32::MAX);
    base_delay.saturating_mul(multiplier)
}

async fn wait_with_abort(delay: Duration, cancellation: Option<&CancellationToken>) -> bool {
    if let Some(cancellation) = cancellation {
        if cancellation.is_cancelled() {
            return false;
        }
        tokio::select! {
            _ = cancellation.cancelled() => false,
            _ = tokio::time::sleep(delay) => true,
        }
    } else {
        tokio::time::sleep(delay).await;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_BASE_DELAY, DEFAULT_MAX_RETRIES, McpRetryError, McpRetryOptions,
        exponential_backoff, is_transient_network_error, retry_with_backoff,
    };
    use crate::utils::cancellation::CancellationToken;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[derive(Debug, Eq, PartialEq)]
    struct TestError(&'static str);

    impl std::fmt::Display for TestError {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl std::error::Error for TestError {}

    #[test]
    fn default_retry_bounds_and_exponential_delays_match_source() {
        assert_eq!(DEFAULT_MAX_RETRIES, 2);
        assert_eq!(DEFAULT_BASE_DELAY, Duration::from_millis(200));
        assert_eq!(
            exponential_backoff(Duration::from_millis(50), 0),
            Duration::from_millis(50)
        );
        assert_eq!(
            exponential_backoff(Duration::from_millis(50), 1),
            Duration::from_millis(100)
        );
        assert_eq!(
            exponential_backoff(Duration::from_millis(50), 2),
            Duration::from_millis(200)
        );
    }

    #[test]
    fn classifies_transient_network_codes_and_http_context() {
        for code in [
            "ECONNRESET",
            "ETIMEDOUT",
            "ENOTFOUND",
            "ECONNREFUSED",
            "EAI_AGAIN",
            "EPIPE",
            "EHOSTUNREACH",
            "ENETUNREACH",
        ] {
            assert!(is_transient_network_error(code, None), "{code}");
        }
        for message in [
            "502 Bad Gateway",
            "503 Service Unavailable",
            "504 Gateway Timeout",
            "Request failed with status code 502",
            "status: 503",
            "HTTP/1.1 504 Gateway Timeout",
            "Connection closed",
            "transport error",
            "Streamable HTTP connection failed",
        ] {
            assert!(is_transient_network_error(message, None), "{message}");
        }
    }

    #[test]
    fn rejects_permanent_auth_rpc_and_bare_status_numbers() {
        assert!(!is_transient_network_error("401 Unauthorized", None));
        assert!(!is_transient_network_error("403 Forbidden", None));
        for code in [-32601.0, -32600.0, -32602.0] {
            assert!(!is_transient_network_error("ECONNRESET", Some(code)));
        }
        assert!(!is_transient_network_error("processed 502 items", None));
        assert!(!is_transient_network_error("timeout after 503ms", None));
        assert!(!is_transient_network_error("unknown failure", None));
    }

    #[tokio::test]
    async fn retries_only_transient_failures_and_returns_the_last_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let operation_calls = Arc::clone(&calls);
        let result: Result<&str, McpRetryError<TestError>> = retry_with_backoff(
            move || {
                let attempt = operation_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt < 2 {
                        Err(TestError("ECONNRESET"))
                    } else {
                        Ok("recovered")
                    }
                }
            },
            McpRetryOptions {
                max_retries: 2,
                base_delay: Duration::ZERO,
            },
            None,
            |error| is_transient_network_error(error.0, None),
        )
        .await;
        assert_eq!(result.unwrap(), "recovered");
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let calls = Arc::new(AtomicUsize::new(0));
        let operation_calls = Arc::clone(&calls);
        let exhausted: Result<(), McpRetryError<TestError>> = retry_with_backoff(
            move || {
                let attempt = operation_calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    Err(TestError(if attempt < 2 {
                        "ECONNRESET"
                    } else {
                        "last transient failure"
                    }))
                }
            },
            McpRetryOptions {
                max_retries: 2,
                base_delay: Duration::ZERO,
            },
            None,
            |error| is_transient_network_error(error.0, None),
        )
        .await;
        assert!(matches!(
            exhausted,
            Err(McpRetryError::Operation(TestError(
                "last transient failure"
            )))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let calls = Arc::new(AtomicUsize::new(0));
        let operation_calls = Arc::clone(&calls);
        let result: Result<(), McpRetryError<TestError>> = retry_with_backoff(
            move || {
                operation_calls.fetch_add(1, Ordering::SeqCst);
                async { Err(TestError("401 Unauthorized")) }
            },
            McpRetryOptions::default(),
            None,
            |error| is_transient_network_error(error.0, None),
        )
        .await;
        assert!(matches!(
            result,
            Err(McpRetryError::Operation(TestError("401 Unauthorized")))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancellation_interrupts_backoff_and_pre_cancelled_signal_aborts() {
        let token = CancellationToken::new();
        let cancel_token = token.clone();
        let cancel = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel_token.cancel();
        });
        let result: Result<(), McpRetryError<TestError>> = retry_with_backoff(
            || async { Err(TestError("ETIMEDOUT")) },
            McpRetryOptions {
                max_retries: 1,
                base_delay: Duration::from_secs(30),
            },
            Some(&token),
            |error| is_transient_network_error(error.0, None),
        )
        .await;
        assert!(matches!(result, Err(McpRetryError::Aborted)));
        cancel.await.unwrap();

        let pre_cancelled = CancellationToken::new();
        pre_cancelled.cancel();
        let result: Result<(), McpRetryError<TestError>> = retry_with_backoff(
            || async { Err(TestError("ETIMEDOUT")) },
            McpRetryOptions {
                max_retries: 1,
                base_delay: Duration::from_secs(30),
            },
            Some(&pre_cancelled),
            |error| is_transient_network_error(error.0, None),
        )
        .await;
        assert!(matches!(result, Err(McpRetryError::Aborted)));
    }
}
