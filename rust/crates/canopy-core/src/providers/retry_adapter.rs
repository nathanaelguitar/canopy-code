//! Bridge provider errors into the provider-neutral retry loop.

use std::collections::HashMap;

use crate::providers::openai_compatible::ProviderError;
use crate::providers::retry_error_classification::{
    RetryErrorClassification, RetryErrorClassificationContext, RetryErrorDiagnosis, RetryErrorKind,
    RetryErrorReason, classify_provider_error,
    default_should_retry as provider_default_should_retry,
};
use crate::providers::retry_policy::{
    RetryAfterHeaderSource, RetryAfterHeaders, get_retry_after_delay_ms,
};
use crate::utils::retry::RetryFailureInfo;

/// Provider-specific inputs needed to adapt errors for `utils::retry`.
///
/// The clock is consulted each time an error is classified so HTTP-date
/// `Retry-After` values are relative to the current attempt, not adapter
/// construction time.
#[derive(Clone, Copy)]
pub struct ProviderRetryAdapter<'a> {
    context: RetryErrorClassificationContext<'a>,
    now_ms: &'a (dyn Fn() -> f64 + Send + Sync),
}

impl<'a> ProviderRetryAdapter<'a> {
    pub fn new(
        context: RetryErrorClassificationContext<'a>,
        now_ms: &'a (dyn Fn() -> f64 + Send + Sync),
    ) -> Self {
        Self { context, now_ms }
    }

    /// Convert a provider failure into the generic retry loop's minimal facts.
    pub fn classify(&self, error: &ProviderError) -> RetryFailureInfo {
        let classification = classify_provider_error(error, self.context);
        let retry_after_ms = parse_retry_after(&classification, (self.now_ms)());
        let fast_fail_message = fast_fail_message(error, &classification);

        RetryFailureInfo {
            status_code: classification.status_code.map(f64::from),
            retry_after_ms,
            is_abort: classification.kind == RetryErrorKind::Abort,
            is_fail_fast: classification.diagnosis == RetryErrorDiagnosis::FailFast,
            fast_fail_message,
            diagnostics: serde_json::to_value(classification).ok(),
        }
    }

    /// Use the source-compatible bounded-retry predicate.
    ///
    /// This intentionally follows `default_should_retry`, rather than the
    /// classifier diagnosis: allocated-quota 429 errors are diagnosed as
    /// fail-fast for persistent mode but still receive the normal bounded
    /// retry budget in the source wrapper.
    pub fn should_retry_by_default(&self, error: &ProviderError) -> bool {
        provider_default_should_retry(error, self.context)
    }
}

fn parse_retry_after(classification: &RetryErrorClassification, now_ms: f64) -> Option<f64> {
    let value = classification.retry_after.as_ref()?;
    let headers = HashMap::from([("retry-after".to_owned(), value.clone())]);
    get_retry_after_delay_ms(
        RetryAfterHeaders {
            direct: Some(RetryAfterHeaderSource::StringMap(&headers)),
            response: None,
        },
        now_ms_to_i64(now_ms),
    )
}

fn now_ms_to_i64(now_ms: f64) -> i64 {
    if !now_ms.is_finite() {
        return 0;
    }
    now_ms.floor().clamp(i64::MIN as f64, i64::MAX as f64) as i64
}

fn fast_fail_message(
    error: &ProviderError,
    classification: &RetryErrorClassification,
) -> Option<String> {
    if classification.reason == RetryErrorReason::CanopyOauthFreeTierQuota {
        return Some(CANOPY_OAUTH_QUOTA_MESSAGE.to_owned());
    }

    let message = classification
        .provider_message
        .as_deref()
        .or_else(|| provider_error_message(error))?;
    is_quota_exhausted_message(message).then(|| format_quota_exhausted_message(message))
}

fn provider_error_message(error: &ProviderError) -> Option<&str> {
    match error {
        ProviderError::HttpStatus { body, .. } => Some(body),
        ProviderError::Transport { message, .. } => Some(message),
        _ => None,
    }
}

fn is_quota_exhausted_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("quota")
        && (message.contains("exhausted") || message.contains("exceeded"))
        && (message.contains("will reset") || message.contains("reset at"))
}

fn format_quota_exhausted_message(message: &str) -> String {
    let bytes = message.as_bytes();
    let message = if bytes.len() >= 4
        && bytes[..3].iter().all(u8::is_ascii_digit)
        && bytes[3].is_ascii_whitespace()
    {
        message[3..].trim_start()
    } else {
        message.trim()
    };
    let message = if message.is_empty() {
        "quota has been exhausted"
    } else {
        message
    };
    format!(
        "Quota exhausted: {message}\n\nPlease retry after the reset time, or switch to another API key / auth method."
    )
}

const CANOPY_OAUTH_QUOTA_MESSAGE: &str = "Canopy OAuth free tier has been discontinued as of 2026-04-15.\n\nTo continue using Canopy Code, try one of these alternatives:\n  - OpenRouter:    https://openrouter.ai/docs/quickstart\n  - Fireworks AI:  https://docs.fireworks.ai/api-reference/introduction\n  - ModelStudio:   https://help.aliyun.com/zh/model-studio/coding-plan\n\nAfter setting up your API key, run /auth to configure your provider.";

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    fn http_error(status: u16, body: &str, retry_after: Option<&str>) -> ProviderError {
        ProviderError::HttpStatus {
            status: StatusCode::from_u16(status).expect("valid test status"),
            body: body.to_owned(),
            truncated: "",
            retry_after: retry_after.map(str::to_owned),
        }
    }

    #[test]
    fn adapts_provider_classification_and_parses_retry_after_with_injected_clock() {
        let now = || 1_767_225_600_000.0;
        let adapter = ProviderRetryAdapter::new(RetryErrorClassificationContext::default(), &now);

        let seconds = http_error(
            429,
            r#"{"error":{"code":429,"message":"Rate limit exceeded"}}"#,
            Some("2.5"),
        );
        let failure = adapter.classify(&seconds);
        assert_eq!(failure.status_code, Some(429.0));
        assert_eq!(failure.retry_after_ms, Some(2_500.0));
        assert!(!failure.is_abort);
        assert!(!failure.is_fail_fast);
        assert_eq!(failure.diagnostics.as_ref().unwrap()["retryAfter"], "2.5");

        let http_date = http_error(
            503,
            "upstream unavailable",
            Some("Thu, 01 Jan 2026 00:00:10 GMT"),
        );
        assert_eq!(adapter.classify(&http_date).retry_after_ms, Some(10_000.0));
    }

    #[test]
    fn default_retry_uses_bounded_predicate_separately_from_fail_fast_diagnosis() {
        let now = || 0.0;
        let adapter = ProviderRetryAdapter::new(RetryErrorClassificationContext::default(), &now);
        let allocated_quota = http_error(
            429,
            r#"{"error":{"code":"Throttling.AllocationQuota","message":"Allocation quota exceeded"}}"#,
            None,
        );
        let failure = adapter.classify(&allocated_quota);
        assert!(failure.is_fail_fast);
        assert!(failure.fast_fail_message.is_none());
        assert!(adapter.should_retry_by_default(&allocated_quota));

        let server_error = http_error(500, "temporary server failure", None);
        assert!(adapter.should_retry_by_default(&server_error));

        let client_error = http_error(400, "invalid request", None);
        assert!(!adapter.should_retry_by_default(&client_error));
    }

    #[test]
    fn maps_source_quota_fast_fails_to_user_messages() {
        let now = || 0.0;
        let context = RetryErrorClassificationContext {
            auth_type: Some("canopy-oauth"),
            ..RetryErrorClassificationContext::default()
        };
        let adapter = ProviderRetryAdapter::new(context, &now);
        let canopy_quota = http_error(
            429,
            r#"{"error":{"code":"insufficient_quota","message":"Free allocated quota exceeded"}}"#,
            None,
        );
        assert!(
            adapter
                .classify(&canopy_quota)
                .fast_fail_message
                .is_some_and(|message| message.contains("free tier has been discontinued"))
        );

        let reset_quota = http_error(
            429,
            "429 Your token-plan quota has been exhausted. The quota will reset at 07-27 09:25:00 UTC.",
            None,
        );
        let message = adapter.classify(&reset_quota).fast_fail_message.unwrap();
        assert!(message.starts_with("Quota exhausted: Your token-plan"));
        assert!(message.contains("retry after the reset time"));
    }
}
