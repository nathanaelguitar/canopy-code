//! Provider-neutral retry orchestration with injected classification and I/O.
//!
//! Callers supply error classification, the default retry predicate, sleep,
//! clock, randomness, and optional logging hooks. The retry loop owns attempt
//! accounting, delay policy, persistent retry behavior, cancellation during
//! waits, content retries, and per-attempt timing context.

use crate::utils::cancellation::CancellationToken;
use serde_json::Value;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const DEFAULT_MAX_ATTEMPTS: usize = 7;
pub const DEFAULT_INITIAL_DELAY_MS: f64 = 1_500.0;
pub const DEFAULT_MAX_DELAY_MS: f64 = 30_000.0;
pub const DEFAULT_PERSISTENT_MAX_BACKOFF_MS: f64 = 5.0 * 60.0 * 1_000.0;
pub const DEFAULT_PERSISTENT_CAP_MS: f64 = 6.0 * 60.0 * 60.0 * 1_000.0;
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: f64 = 30_000.0;
const MAX_TIMEOUT_MS: f64 = 2_147_483_647.0;

pub type RetrySleepFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type RetrySleepHook = dyn Fn(f64) -> RetrySleepFuture + Send + Sync;
pub type RetryAttemptHook<'a, E> =
    dyn for<'error> Fn(RetryAttemptInfo<'error, E>) + Send + Sync + 'a;
pub type RetryHeartbeatHook<'a, E> =
    dyn for<'error> Fn(&RetryHeartbeatInfo<'error, E>) + Send + Sync + 'a;
pub type RetryLogHook<'a, E> =
    dyn for<'error> Fn(&RetryLogEntry, Option<&'error E>) + Send + Sync + 'a;

/// Minimal provider-neutral facts needed by the retry loop.
///
/// An adapter can translate its provider error classification and parsed
/// `Retry-After` value into this structure. `diagnostics` is passed to logging;
/// the generic loop does not inspect provider-specific fields.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RetryFailureInfo {
    pub status_code: Option<f64>,
    pub retry_after_ms: Option<f64>,
    pub is_abort: bool,
    /// Keeps permanent business failures out of the unbounded persistent loop.
    pub is_fail_fast: bool,
    /// Optional replacement message for special quota or policy failures.
    /// The original error remains attached to [`RetryWithBackoffError::FastFail`].
    pub fast_fail_message: Option<String>,
    pub diagnostics: Option<Value>,
}

/// Explicit context passed to each operation invocation.
///
/// This provides the retry telemetry that the TypeScript source propagates
/// through AsyncLocalStorage, without requiring ambient thread/task state.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RetryAttemptContext {
    pub attempt: u64,
    pub retry_total_delay_ms: f64,
    pub request_setup_ms: f64,
}

#[derive(Debug)]
pub struct RetryAttemptInfo<'a, E> {
    pub attempt: u64,
    pub error: &'a E,
    pub error_status: Option<f64>,
    pub delay_ms: f64,
}

#[derive(Debug)]
pub struct RetryHeartbeatInfo<'a, E> {
    pub attempt: u64,
    pub remaining_ms: f64,
    pub error: &'a E,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryLogLevel {
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetryLogEntry {
    pub level: RetryLogLevel,
    pub message: String,
    pub attempt: u64,
    pub error_status: Option<f64>,
    pub delay_ms: Option<f64>,
    pub persistent: bool,
    pub diagnostics: Option<Value>,
}

/// Configuration and callbacks for [`retry_with_backoff`].
pub struct RetryOptions<'a, E, T> {
    pub max_attempts: usize,
    pub initial_delay_ms: f64,
    pub max_delay_ms: f64,
    /// If supplied, overrides the default predicate provided by `RetryHooks`.
    pub should_retry_on_error: Option<&'a (dyn for<'error> Fn(&'error E) -> bool + Send + Sync)>,
    pub should_retry_on_content: Option<&'a (dyn Fn(&T) -> bool + Send + Sync)>,
    pub persistent_mode: bool,
    pub persistent_max_backoff_ms: f64,
    pub persistent_cap_ms: f64,
    pub heartbeat_interval_ms: f64,
    pub heartbeat_fn: Option<&'a RetryHeartbeatHook<'a, E>>,
    pub signal: Option<&'a CancellationToken>,
    pub on_retry: Option<&'a RetryAttemptHook<'a, E>>,
}

impl<'a, E, T> RetryOptions<'a, E, T> {
    pub fn new() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_delay_ms: DEFAULT_INITIAL_DELAY_MS,
            max_delay_ms: DEFAULT_MAX_DELAY_MS,
            should_retry_on_error: None,
            should_retry_on_content: None,
            persistent_mode: false,
            persistent_max_backoff_ms: DEFAULT_PERSISTENT_MAX_BACKOFF_MS,
            persistent_cap_ms: DEFAULT_PERSISTENT_CAP_MS,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            heartbeat_fn: None,
            signal: None,
            on_retry: None,
        }
    }
}

impl<'a, E, T> Default for RetryOptions<'a, E, T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Injected environment for provider-neutral retry behavior.
pub struct RetryHooks<'a, E> {
    /// Translate an error to status, abort/fail-fast, Retry-After, and log data.
    pub classify_error: &'a (dyn Fn(&E) -> RetryFailureInfo + Send + Sync),
    /// Source-compatible default predicate (rate limits, 5xx, transport).
    pub default_should_retry: &'a (dyn Fn(&E) -> bool + Send + Sync),
    pub now_ms: &'a (dyn Fn() -> f64 + Send + Sync),
    pub random: &'a mut (dyn FnMut() -> f64 + Send),
    pub sleep: &'a RetrySleepHook,
    pub logger: Option<&'a RetryLogHook<'a, E>>,
}

/// Error returned by the retry loop. `FastFail` preserves the original error
/// while exposing the caller-provided replacement guidance message.
#[derive(Debug)]
pub enum RetryWithBackoffError<E> {
    Operation(E),
    FastFail { original: E, message: String },
    Cancelled,
    InvalidMaxAttempts,
    AttemptsExhausted,
}

impl<E: std::fmt::Display> std::fmt::Display for RetryWithBackoffError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Operation(error) => std::fmt::Display::fmt(error, formatter),
            Self::FastFail { message, .. } => formatter.write_str(message),
            Self::Cancelled => formatter.write_str("Retry aborted by signal"),
            Self::InvalidMaxAttempts => {
                formatter.write_str("maxAttempts must be a positive number.")
            }
            Self::AttemptsExhausted => formatter.write_str("Retry attempts exhausted"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RetryWithBackoffError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            Self::FastFail { original, .. } => Some(original),
            Self::Cancelled | Self::InvalidMaxAttempts | Self::AttemptsExhausted => None,
        }
    }
}

/// Retry an operation with capped exponential backoff and jitter.
///
/// The operation receives explicit attempt telemetry. Error classification,
/// default retryability, parsed Retry-After, and provider-specific quota
/// handling remain injectable so this module has no provider transport
/// dependency.
pub async fn retry_with_backoff<T, E, F, Fut>(
    mut operation: F,
    options: &RetryOptions<'_, E, T>,
    hooks: &mut RetryHooks<'_, E>,
) -> Result<T, RetryWithBackoffError<E>>
where
    T: Send,
    E: Send,
    F: FnMut(RetryAttemptContext) -> Fut + Send,
    Fut: Future<Output = Result<T, E>> + Send,
{
    if options.max_attempts == 0 {
        return Err(RetryWithBackoffError::InvalidMaxAttempts);
    }

    let request_entry_time = (hooks.now_ms)();
    let mut attempt = 0_usize;
    let mut persistent_attempt = 0_u64;
    let mut iteration_count = 0_u64;
    let mut retry_total_delay_ms = 0.0;
    let mut current_delay = options.initial_delay_ms;
    let mut last_content_result = None;
    let mut had_content_retry = false;

    while attempt < options.max_attempts {
        attempt += 1;
        iteration_count = iteration_count.saturating_add(1);
        let request_setup_ms = (hooks.now_ms)() - request_entry_time;
        let context = RetryAttemptContext {
            attempt: iteration_count,
            retry_total_delay_ms,
            request_setup_ms,
        };

        match operation(context).await {
            Ok(result) => {
                if options
                    .should_retry_on_content
                    .is_some_and(|predicate| predicate(&result))
                {
                    last_content_result = Some(result);
                    had_content_retry = true;
                    let delay_ms = get_capped_exponential_retry_delay_ms(
                        1,
                        current_delay,
                        options.max_delay_ms,
                        0.3,
                        &mut *hooks.random,
                    );
                    log(
                        hooks,
                        RetryLogEntry {
                            level: RetryLogLevel::Warn,
                            message: format!(
                                "Attempt {iteration_count}: response rejected by content check. Retrying with backoff in {}s...",
                                (delay_ms / 1_000.0).ceil()
                            ),
                            attempt: iteration_count,
                            error_status: None,
                            delay_ms: Some(delay_ms),
                            persistent: false,
                            diagnostics: None,
                        },
                        None,
                    );
                    sleep_abortable(delay_ms, options.signal, hooks.sleep)
                        .await
                        .map_err(|()| RetryWithBackoffError::Cancelled)?;
                    retry_total_delay_ms += delay_ms;
                    current_delay = options.max_delay_ms.min(current_delay * 2.0);
                    continue;
                }
                return Ok(result);
            }
            Err(error) => {
                let failure = (hooks.classify_error)(&error);

                // An abort from the operation is authoritative, even when a
                // custom retry predicate would otherwise accept the error.
                if failure.is_abort {
                    return Err(RetryWithBackoffError::Operation(error));
                }

                if let Some(message) = failure.fast_fail_message.clone() {
                    log(
                        hooks,
                        RetryLogEntry {
                            level: RetryLogLevel::Error,
                            message: message.clone(),
                            attempt: iteration_count,
                            error_status: failure.status_code,
                            delay_ms: None,
                            persistent: false,
                            diagnostics: failure.diagnostics.clone(),
                        },
                        Some(&error),
                    );
                    return Err(RetryWithBackoffError::FastFail {
                        original: error,
                        message,
                    });
                }

                let caller_allows_retry = match options.should_retry_on_error {
                    Some(predicate) => predicate(&error),
                    None => (hooks.default_should_retry)(&error),
                };
                let is_transient_capacity = is_transient_capacity_status(failure.status_code);
                let should_persist = options.persistent_mode
                    && is_transient_capacity
                    && caller_allows_retry
                    && !failure.is_fail_fast;

                if !should_persist && (attempt >= options.max_attempts || !caller_allows_retry) {
                    return Err(RetryWithBackoffError::Operation(error));
                }

                if should_persist {
                    persistent_attempt = persistent_attempt.saturating_add(1);
                    let retry_after = applicable_retry_after(&failure);
                    let delay_ms = if let Some(retry_after_ms) = retry_after.filter(|ms| *ms > 0.0)
                    {
                        retry_after_ms.min(options.persistent_cap_ms)
                    } else {
                        get_capped_exponential_retry_delay_ms(
                            persistent_attempt,
                            options.initial_delay_ms,
                            options
                                .persistent_max_backoff_ms
                                .min(options.persistent_cap_ms),
                            0.25,
                            &mut *hooks.random,
                        )
                    };

                    let reported_attempt = persistent_attempt;
                    let status = format_status(failure.status_code);
                    log(
                        hooks,
                        RetryLogEntry {
                            level: RetryLogLevel::Warn,
                            message: format!(
                                "[Persistent] Attempt {reported_attempt} failed with status {status}. Retrying in {}s...",
                                (delay_ms / 1_000.0).ceil()
                            ),
                            attempt: reported_attempt,
                            error_status: failure.status_code,
                            delay_ms: Some(delay_ms),
                            persistent: true,
                            diagnostics: failure.diagnostics.clone(),
                        },
                        Some(&error),
                    );

                    if options.signal.is_none_or(|signal| !signal.is_cancelled()) {
                        fire_on_retry(options, hooks, &error, &failure, iteration_count, delay_ms);
                    }

                    sleep_with_heartbeat(delay_ms, reported_attempt, &error, options, hooks.sleep)
                        .await
                        .map_err(|()| RetryWithBackoffError::Cancelled)?;
                    retry_total_delay_ms += delay_ms;

                    // Match the source's bounded counter plus monotonic
                    // iteration counter: persistent mode keeps looping after
                    // maxAttempts without changing telemetry attempt numbers.
                    if attempt >= options.max_attempts {
                        attempt = options.max_attempts - 1;
                    }
                } else {
                    let retry_after = applicable_retry_after(&failure);
                    let delay_ms = if let Some(retry_after_ms) = retry_after.filter(|ms| *ms > 0.0)
                    {
                        // The source parser already clamps at Node's timer
                        // ceiling; preserve that bound while bypassing
                        // maxDelayMs for normal provider-directed waits.
                        let delay_ms = retry_after_ms.min(MAX_TIMEOUT_MS);
                        current_delay = options.initial_delay_ms;
                        let status = format_status(failure.status_code);
                        log(
                            hooks,
                            RetryLogEntry {
                                level: if is_server_error(failure.status_code) {
                                    RetryLogLevel::Error
                                } else {
                                    RetryLogLevel::Warn
                                },
                                message: format!(
                                    "Attempt {attempt} failed with status {status}. Retrying after explicit delay of {delay_ms}ms..."
                                ),
                                attempt: iteration_count,
                                error_status: failure.status_code,
                                delay_ms: Some(delay_ms),
                                persistent: false,
                                diagnostics: failure.diagnostics.clone(),
                            },
                            Some(&error),
                        );
                        delay_ms
                    } else {
                        let delay_ms = get_capped_exponential_retry_delay_ms(
                            1,
                            current_delay,
                            options.max_delay_ms,
                            0.3,
                            &mut *hooks.random,
                        );
                        current_delay = options.max_delay_ms.min(current_delay * 2.0);
                        let status = format_status(failure.status_code);
                        log(
                            hooks,
                            RetryLogEntry {
                                level: if is_server_error(failure.status_code) {
                                    RetryLogLevel::Error
                                } else {
                                    RetryLogLevel::Warn
                                },
                                message: format!(
                                    "Attempt {attempt} failed with status {status}. Retrying with backoff in {}s...",
                                    (delay_ms / 1_000.0).ceil()
                                ),
                                attempt: iteration_count,
                                error_status: failure.status_code,
                                delay_ms: Some(delay_ms),
                                persistent: false,
                                diagnostics: failure.diagnostics.clone(),
                            },
                            Some(&error),
                        );
                        delay_ms
                    };

                    if options.signal.is_none_or(|signal| !signal.is_cancelled()) {
                        fire_on_retry(options, hooks, &error, &failure, iteration_count, delay_ms);
                    }

                    sleep_abortable(delay_ms, options.signal, hooks.sleep)
                        .await
                        .map_err(|()| RetryWithBackoffError::Cancelled)?;
                    retry_total_delay_ms += delay_ms;
                }
            }
        }
    }

    if had_content_retry {
        return last_content_result.ok_or(RetryWithBackoffError::AttemptsExhausted);
    }
    Err(RetryWithBackoffError::AttemptsExhausted)
}

fn fire_on_retry<E, T>(
    options: &RetryOptions<'_, E, T>,
    hooks: &RetryHooks<'_, E>,
    error: &E,
    failure: &RetryFailureInfo,
    attempt: u64,
    delay_ms: f64,
) {
    let Some(callback) = options.on_retry else {
        return;
    };
    let info = RetryAttemptInfo {
        attempt,
        error,
        error_status: failure.status_code,
        delay_ms,
    };
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| callback(info))) {
        let callback_error = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                payload
                    .downcast_ref::<&'static str>()
                    .map(|message| (*message).to_owned())
            })
            .unwrap_or_else(|| "non-string panic payload".to_owned());
        log(
            hooks,
            RetryLogEntry {
                level: RetryLogLevel::Warn,
                message: format!("onRetry callback panicked (swallowed): {callback_error}"),
                attempt,
                error_status: failure.status_code,
                delay_ms: Some(delay_ms),
                persistent: false,
                diagnostics: failure.diagnostics.clone(),
            },
            None,
        );
    }
}

fn log<E>(hooks: &RetryHooks<'_, E>, entry: RetryLogEntry, error: Option<&E>) {
    if let Some(logger) = hooks.logger {
        logger(&entry, error);
    }
}

fn applicable_retry_after(failure: &RetryFailureInfo) -> Option<f64> {
    if failure.status_code == Some(429.0) || failure.status_code == Some(503.0) {
        failure
            .retry_after_ms
            .map(|delay| delay.min(MAX_TIMEOUT_MS))
    } else {
        None
    }
}

fn is_transient_capacity_status(status: Option<f64>) -> bool {
    status == Some(429.0) || status == Some(529.0)
}

fn is_server_error(status: Option<f64>) -> bool {
    status.is_some_and(|status| (500.0..600.0).contains(&status))
}

fn format_status(status: Option<f64>) -> String {
    status.map_or_else(
        || "unknown".to_owned(),
        |status| {
            if status.fract() == 0.0 {
                format!("{status:.0}")
            } else {
                status.to_string()
            }
        },
    )
}

/// Shared pure exponential-backoff calculation for retry callers.
///
/// This applies the source's attempt normalization, exponent cap, Node timer
/// ceiling, and symmetric injected jitter. It deliberately does not interpret
/// provider headers; callers pass only the exponential inputs.
pub fn get_capped_exponential_retry_delay_ms(
    attempt: u64,
    initial_delay_ms: f64,
    max_delay_ms: f64,
    jitter_ratio: f64,
    random: &mut dyn FnMut() -> f64,
) -> f64 {
    let exponent = attempt.max(1).saturating_sub(1).min(31) as i32;
    let delay_ceiling_ms = max_delay_ms.min(MAX_TIMEOUT_MS);
    let exponential_delay_ms = (initial_delay_ms * 2_f64.powi(exponent)).min(delay_ceiling_ms);
    if jitter_ratio <= 0.0 {
        return exponential_delay_ms;
    }
    let jitter = exponential_delay_ms * jitter_ratio * (random() * 2.0 - 1.0);
    (exponential_delay_ms + jitter)
        .max(0.0)
        .min(delay_ceiling_ms)
}

async fn sleep_abortable(
    delay_ms: f64,
    signal: Option<&CancellationToken>,
    sleep: &RetrySleepHook,
) -> Result<(), ()> {
    if signal.is_some_and(CancellationToken::is_cancelled) {
        return Err(());
    }
    if let Some(signal) = signal {
        tokio::select! {
            biased;
            _ = signal.cancelled() => Err(()),
            _ = sleep(delay_ms) => {
                if signal.is_cancelled() { Err(()) } else { Ok(()) }
            }
        }
    } else {
        sleep(delay_ms).await;
        Ok(())
    }
}

async fn sleep_with_heartbeat<E, T>(
    total_ms: f64,
    attempt: u64,
    error: &E,
    options: &RetryOptions<'_, E, T>,
    sleep: &RetrySleepHook,
) -> Result<(), ()> {
    let mut remaining = total_ms;
    while remaining > 0.0 {
        if options.signal.is_some_and(CancellationToken::is_cancelled) {
            return Err(());
        }
        let chunk_ms = remaining.min(options.heartbeat_interval_ms).max(1.0);
        sleep_abortable(chunk_ms, options.signal, sleep).await?;
        remaining -= chunk_ms;
        if remaining > 0.0 {
            if let Some(heartbeat_fn) = options.heartbeat_fn {
                heartbeat_fn(&RetryHeartbeatInfo {
                    attempt,
                    remaining_ms: remaining,
                    error,
                });
            }
        }
    }
    Ok(())
}

/// Default wall clock helper for [`RetryHooks::now_ms`].
pub fn system_time_ms() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs_f64() * 1_000.0,
        Err(error) => -(error.duration().as_secs_f64() * 1_000.0),
    }
}

/// Default Tokio sleep adapter for [`RetryHooks::sleep`].
pub fn tokio_retry_sleep(delay_ms: f64) -> RetrySleepFuture {
    let delay_ms = if delay_ms.is_finite() {
        delay_ms.max(0.0)
    } else {
        0.0
    };
    Box::pin(tokio::time::sleep(Duration::from_secs_f64(
        delay_ms / 1_000.0,
    )))
}

/// Whether a provider status is eligible for persistent retry.
pub fn is_transient_capacity_error(status_code: Option<f64>) -> bool {
    is_transient_capacity_status(status_code)
}

/// Strict parser matching the source's `CANOPY_CODE_UNATTENDED_RETRY` values.
pub fn is_unattended_mode_value(value: Option<&str>) -> bool {
    matches!(value, Some("true" | "1"))
}

/// Check the unattended retry environment flag. `CI=true` alone is ignored.
pub fn is_unattended_mode() -> bool {
    is_unattended_mode_value(
        std::env::var("CANOPY_CODE_UNATTENDED_RETRY")
            .ok()
            .as_deref(),
    )
}

/// Extract a valid HTTP status from common JSON SDK error shapes or an SSE
/// `HTTP_STATUS/NNN` message. Numeric fields follow source priority order.
pub fn get_error_status(error: &Value) -> Option<f64> {
    let object = error.as_object()?;
    let candidate = object
        .get("status")
        .filter(|value| !value.is_null())
        .or_else(|| object.get("statusCode").filter(|value| !value.is_null()))
        .or_else(|| {
            object
                .get("response")
                .and_then(Value::as_object)
                .and_then(|response| response.get("status"))
                .filter(|value| !value.is_null())
        })
        .or_else(|| {
            object
                .get("error")
                .and_then(Value::as_object)
                .and_then(|nested| nested.get("code"))
                .filter(|value| !value.is_null())
        });

    if let Some(status) = candidate
        .and_then(Value::as_f64)
        .filter(|status| (100.0..=599.0).contains(status))
    {
        return Some(status);
    }

    object
        .get("message")
        .and_then(Value::as_str)
        .and_then(parse_sse_status)
}

fn parse_sse_status(message: &str) -> Option<f64> {
    const MARKER: &str = "HTTP_STATUS/";
    for (start, _) in message.match_indices(MARKER) {
        let digits_start = start + MARKER.len();
        let digits_end = digits_start.checked_add(3)?;
        let digits = message.get(digits_start..digits_end)?;
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let next = message.as_bytes().get(digits_end).copied();
        // JavaScript's `\b` after three digits rejects adjacent word chars,
        // including another digit, letter, or underscore.
        if next.is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
            continue;
        }
        if let Ok(status) = digits.parse::<u16>() {
            return (100..=599).contains(&status).then_some(f64::from(status));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, PartialEq)]
    struct Failure {
        message: &'static str,
        info: RetryFailureInfo,
        retryable: bool,
    }

    fn failure(status: f64, retryable: bool) -> Failure {
        Failure {
            message: "transient",
            info: RetryFailureInfo {
                status_code: Some(status),
                ..RetryFailureInfo::default()
            },
            retryable,
        }
    }

    fn classify(error: &Failure) -> RetryFailureInfo {
        error.info.clone()
    }

    fn default_retry(error: &Failure) -> bool {
        error.retryable
    }

    fn never_retry(_: &Failure) -> bool {
        false
    }

    fn always_retry(_: &Failure) -> bool {
        true
    }

    fn immediate_sleep(_: f64) -> RetrySleepFuture {
        Box::pin(async {})
    }

    fn run_hooks<'a>(
        random: &'a mut (dyn FnMut() -> f64 + Send),
        now_ms: &'a (dyn Fn() -> f64 + Send + Sync),
        sleep: &'a RetrySleepHook,
        logger: Option<&'a RetryLogHook<'a, Failure>>,
    ) -> RetryHooks<'a, Failure> {
        RetryHooks {
            classify_error: &classify,
            default_should_retry: &default_retry,
            now_ms,
            random,
            sleep,
            logger,
        }
    }

    #[tokio::test]
    async fn retries_with_default_limits_and_monotonic_attempt_context() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::clone(&calls);
        let mut operation_attempt = 0;
        let operation = move |context| {
            observed.lock().unwrap().push(context);
            operation_attempt += 1;
            let result = if operation_attempt <= 2 {
                Err(failure(500.0, true))
            } else {
                Ok("success")
            };
            std::future::ready(result)
        };

        let mut options = RetryOptions::new();
        options.max_attempts = 3;
        options.initial_delay_ms = 10.0;
        options.max_delay_ms = 40.0;
        let now = || 1_000.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);

        let retry = retry_with_backoff(operation, &options, &mut hooks);
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&retry);
        let result = retry.await;
        assert!(matches!(result, Ok("success")));
        let contexts = calls.lock().unwrap();
        assert_eq!(
            contexts.iter().map(|ctx| ctx.attempt).collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(contexts[0].request_setup_ms, 0.0);
        assert_eq!(contexts[0].retry_total_delay_ms, 0.0);
        assert_eq!(contexts[1].retry_total_delay_ms, 10.0);
        assert_eq!(contexts[2].retry_total_delay_ms, 30.0);
    }

    #[tokio::test]
    async fn validates_attempt_limit_and_returns_last_rejected_content() {
        let mut zero = RetryOptions::<Failure, &'static str>::new();
        zero.max_attempts = 0;
        let mut random = || 0.5;
        let now = || 0.0;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        let not_called = |_| std::future::ready(Ok::<_, Failure>("unused"));
        assert!(matches!(
            retry_with_backoff(not_called, &zero, &mut hooks).await,
            Err(RetryWithBackoffError::InvalidMaxAttempts)
        ));

        let always_bad = |_| std::future::ready(Ok::<_, Failure>("bad"));
        let bad_content = |value: &&str| *value == "bad";
        let mut options = RetryOptions::new();
        options.max_attempts = 2;
        options.initial_delay_ms = 2.0;
        options.max_delay_ms = 10.0;
        options.should_retry_on_content = Some(&bad_content);
        assert!(matches!(
            retry_with_backoff(always_bad, &options, &mut hooks).await,
            Ok("bad")
        ));
    }

    #[tokio::test]
    async fn content_retry_succeeds_without_error_callback() {
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            std::future::ready(Ok::<_, Failure>(if calls == 1 { "bad" } else { "good" }))
        };
        let bad_content = |value: &&str| *value == "bad";
        let on_retry =
            |_: RetryAttemptInfo<'_, Failure>| panic!("content retries do not emit onRetry");
        let mut options = RetryOptions::new();
        options.max_attempts = 3;
        options.initial_delay_ms = 2.0;
        options.max_delay_ms = 10.0;
        options.should_retry_on_content = Some(&bad_content);
        options.on_retry = Some(&on_retry);
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("good")
        ));
    }

    #[tokio::test]
    async fn custom_retry_predicate_overrides_default_and_abort_is_authoritative() {
        let mut custom_calls = 0;
        let operation = |_| {
            custom_calls += 1;
            std::future::ready(Err::<(), _>(failure(429.0, true)))
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 4;
        options.should_retry_on_error = Some(&never_retry);
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Err(RetryWithBackoffError::Operation(_))
        ));
        assert_eq!(custom_calls, 1);

        let aborting = |_| {
            let mut error = failure(500.0, true);
            error.info.is_abort = true;
            std::future::ready(Err::<(), _>(error))
        };
        options.should_retry_on_error = Some(&always_retry);
        assert!(matches!(
            retry_with_backoff(aborting, &options, &mut hooks).await,
            Err(RetryWithBackoffError::Operation(error)) if error.info.is_abort
        ));
    }

    #[tokio::test]
    async fn persistent_retries_only_capacity_statuses_and_obeys_fail_fast() {
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            let result = if calls <= 3 {
                Err(failure(429.0, true))
            } else {
                Ok("ok")
            };
            std::future::ready(result)
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 2;
        options.initial_delay_ms = 10.0;
        options.persistent_mode = true;
        options.persistent_max_backoff_ms = 100.0;
        options.heartbeat_interval_ms = 50.0;
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
        assert_eq!(calls, 4);

        let mut failfast_calls = 0;
        let failfast = |_| {
            failfast_calls += 1;
            let mut error = failure(429.0, true);
            error.info.is_fail_fast = true;
            std::future::ready(Err::<&str, _>(error))
        };
        options.max_attempts = 2;
        assert!(matches!(
            retry_with_backoff(failfast, &options, &mut hooks).await,
            Err(RetryWithBackoffError::Operation(_))
        ));
        assert_eq!(
            failfast_calls, 2,
            "fail-fast remains bounded, not zero-retry"
        );
    }

    #[tokio::test]
    async fn persistent_retry_after_uses_absolute_cap_not_backoff_cap_or_jitter() {
        let mut error = failure(429.0, true);
        error.info.retry_after_ms = Some(600_000.0);
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Err::<&str, _>(error.clone())
            } else {
                Ok("ok")
            })
        };
        let delays = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&delays);
        let sleep = move |delay: f64| {
            recorded.lock().unwrap().push(delay);
            Box::pin(async {}) as RetrySleepFuture
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 1;
        options.initial_delay_ms = 10.0;
        options.persistent_mode = true;
        options.persistent_max_backoff_ms = 5_000.0;
        options.persistent_cap_ms = 50_000.0;
        options.heartbeat_interval_ms = 100_000.0;
        let now = || 0.0;
        let random_calls = Arc::new(AtomicUsize::new(0));
        let random_counter = Arc::clone(&random_calls);
        let mut random = move || {
            random_counter.fetch_add(1, Ordering::SeqCst);
            0.0
        };
        let sleep: &RetrySleepHook = &sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
        assert_eq!(delays.lock().unwrap().as_slice(), [50_000.0]);
        assert_eq!(
            random_calls.load(Ordering::SeqCst),
            0,
            "Retry-After receives no jitter"
        );
    }

    #[tokio::test]
    async fn retries_normal_retry_after_without_max_delay_clamp_and_resets_exponential_floor() {
        let mut first_error = failure(429.0, true);
        first_error.info.retry_after_ms = Some(60_000.0);
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            let result = match calls {
                1 => Err(first_error.clone()),
                2 => Err(failure(500.0, true)),
                _ => Ok("ok"),
            };
            std::future::ready(result)
        };
        let delays = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&delays);
        let sleep = move |delay: f64| {
            recorded.lock().unwrap().push(delay);
            Box::pin(async {}) as RetrySleepFuture
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 3;
        options.initial_delay_ms = 100.0;
        options.max_delay_ms = 1_000.0;
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
        assert_eq!(delays.lock().unwrap().as_slice(), [60_000.0, 100.0]);
    }

    #[tokio::test]
    async fn cancellation_interrupts_waits_and_preaborted_tokens_still_run_first_attempt() {
        let cancellation = CancellationToken::new();
        let cancel_on_sleep = cancellation.clone();
        let sleep = move |_delay: f64| {
            let cancellation = cancel_on_sleep.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                cancellation.cancel();
            });
            Box::pin(async move {
                std::future::pending::<()>().await;
            }) as RetrySleepFuture
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 3;
        options.initial_delay_ms = 100.0;
        options.signal = Some(&cancellation);
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        let operation = |_| std::future::ready(Err::<&str, _>(failure(500.0, true)));
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Err(RetryWithBackoffError::Cancelled)
        ));

        let preaborted = CancellationToken::new();
        preaborted.cancel();
        options.signal = Some(&preaborted);
        let mut ran = false;
        let operation = |_| {
            ran = true;
            std::future::ready(Ok::<_, Failure>("ok"))
        };
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
        assert!(ran);
    }

    #[tokio::test]
    async fn persistent_sleep_emits_heartbeat_chunks_and_zero_interval_progresses() {
        let heartbeats = Arc::new(Mutex::new(Vec::new()));
        let recorded_heartbeats = Arc::clone(&heartbeats);
        let heartbeat = move |info: &RetryHeartbeatInfo<'_, Failure>| {
            recorded_heartbeats
                .lock()
                .unwrap()
                .push((info.attempt, info.remaining_ms));
        };
        let mut error = failure(429.0, true);
        error.info.retry_after_ms = Some(5.0);
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Err::<&str, _>(error.clone())
            } else {
                Ok("ok")
            })
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 1;
        options.persistent_mode = true;
        options.persistent_cap_ms = 20.0;
        options.heartbeat_interval_ms = 2.0;
        options.heartbeat_fn = Some(&heartbeat);
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, None);
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
        assert_eq!(heartbeats.lock().unwrap().as_slice(), [(1, 3.0), (1, 1.0)]);

        options.heartbeat_interval_ms = 0.0;
        let mut calls = 0;
        let operation = |_| {
            calls += 1;
            std::future::ready(if calls == 1 {
                Err::<&str, _>(failure(429.0, true))
            } else {
                Ok("ok")
            })
        };
        assert!(matches!(
            retry_with_backoff(operation, &options, &mut hooks).await,
            Ok("ok")
        ));
    }

    #[tokio::test]
    async fn callback_errors_are_swallowed_and_abort_suppresses_retry_event() {
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&callback_calls);
        let callback = move |_info: RetryAttemptInfo<'_, Failure>| {
            counted.fetch_add(1, Ordering::SeqCst);
            panic!("telemetry failed");
        };
        let logs = Arc::new(Mutex::new(Vec::new()));
        let recorded_logs = Arc::clone(&logs);
        let logger = move |entry: &RetryLogEntry, _error: Option<&Failure>| {
            recorded_logs.lock().unwrap().push(entry.clone());
        };
        let mut options = RetryOptions::new();
        options.max_attempts = 2;
        options.initial_delay_ms = 1.0;
        options.on_retry = Some(&callback);
        let operation = |_| std::future::ready(Err::<(), _>(failure(500.0, true)));
        let now = || 0.0;
        let mut random = || 0.5;
        let sleep: &RetrySleepHook = &immediate_sleep;
        let mut hooks = run_hooks(&mut random, &now, sleep, Some(&logger));
        let _ = retry_with_backoff(operation, &options, &mut hooks).await;
        assert_eq!(callback_calls.load(Ordering::SeqCst), 1);
        assert!(logs.lock().unwrap().iter().any(|entry| {
            entry
                .message
                .contains("onRetry callback panicked (swallowed): telemetry failed")
        }));

        let cancellation = CancellationToken::new();
        let cancel_during_operation = cancellation.clone();
        let operation = |_| {
            cancel_during_operation.cancel();
            std::future::ready(Err::<(), _>(failure(500.0, true)))
        };
        options.signal = Some(&cancellation);
        callback_calls.store(0, Ordering::SeqCst);
        let _ = retry_with_backoff(operation, &options, &mut hooks).await;
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn recognizes_transient_capacity_and_strict_unattended_flag() {
        assert!(is_transient_capacity_error(Some(429.0)));
        assert!(is_transient_capacity_error(Some(529.0)));
        assert!(!is_transient_capacity_error(Some(500.0)));
        assert!(!is_transient_capacity_error(None));
        assert!(is_unattended_mode_value(Some("1")));
        assert!(is_unattended_mode_value(Some("true")));
        assert!(!is_unattended_mode_value(Some("TRUE")));
        assert!(!is_unattended_mode_value(Some("yes")));
        assert!(!is_unattended_mode_value(None));
    }

    #[test]
    fn shared_backoff_helper_caps_exponential_and_injected_jitter() {
        let mut midpoint = || 0.5;
        assert_eq!(
            get_capped_exponential_retry_delay_ms(0, 60_000.0, 300_000.0, 0.0, &mut midpoint),
            60_000.0
        );
        assert_eq!(
            get_capped_exponential_retry_delay_ms(2, 60_000.0, 300_000.0, 0.0, &mut midpoint),
            120_000.0
        );
        assert_eq!(
            get_capped_exponential_retry_delay_ms(10, 60_000.0, 300_000.0, 0.0, &mut midpoint),
            300_000.0
        );

        let mut high = || 1.0;
        assert_eq!(
            get_capped_exponential_retry_delay_ms(2, 100.0, 250.0, 0.3, &mut high),
            250.0
        );
        let mut low = || 0.0;
        assert_eq!(
            get_capped_exponential_retry_delay_ms(2, 100.0, 250.0, 0.3, &mut low),
            140.0
        );
        assert_eq!(
            get_capped_exponential_retry_delay_ms(5_000, 1_000.0, 5e9, 0.3, &mut high),
            MAX_TIMEOUT_MS
        );
    }

    #[test]
    fn extracts_error_status_in_source_priority_order_and_from_sse_messages() {
        assert_eq!(
            get_error_status(&serde_json::json!({"status":429})),
            Some(429.0)
        );
        assert_eq!(
            get_error_status(&serde_json::json!({
                "status":429,
                "statusCode":500,
                "response":{"status":502},
                "error":{"code":503}
            })),
            Some(429.0)
        );
        assert_eq!(
            get_error_status(&serde_json::json!({"message":"id:1\nevent:error\n:HTTP_STATUS/503"})),
            Some(503.0)
        );
        assert_eq!(
            get_error_status(&serde_json::json!({"message":"HTTP_STATUS/4291"})),
            None
        );
        assert_eq!(get_error_status(&serde_json::json!({"status":700})), None);
        assert_eq!(get_error_status(&serde_json::json!(429)), None);
    }
}
