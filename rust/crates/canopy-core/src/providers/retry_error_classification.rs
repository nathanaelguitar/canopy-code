//! Retry diagnostics for the native provider error types.
//!
//! This ports the classification and status/provider-payload extraction from
//! `packages/core/src/utils/retryErrorClassification.ts` and `rateLimit.ts`.
//! Delay calculation stays in `retry_policy`; this module only describes an
//! error and answers the default retry question.

use std::error::Error;

use serde::Serialize;
use serde_json::Value;

use crate::providers::openai_compatible::ProviderError;

const BUILTIN_RATE_LIMIT_CODES: &[i64] = &[429, 503, 1302, 1305];
const FALLBACK_ELIGIBLE_STATUSES: &[u16] = &[429, 503, 529];
const MAX_TRANSPORT_CAUSE_DEPTH: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetryErrorKind {
    Http,
    SseProvider,
    Provider,
    Transport,
    Abort,
    ProviderBusiness,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetryErrorDiagnosis {
    Retryable,
    FailFast,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetryErrorReason {
    Aborted,
    CanopyOauthFreeTierQuota,
    AllocatedQuotaExceeded,
    RateLimit,
    TransportError,
    CapacityOverload,
    AuthError,
    ClientError,
    ServerError,
    HttpStatus,
    Unclassified,
}

/// Optional knobs that match the source classifier's auth and custom-code
/// inputs. `auth_type` uses Canopy's wire name, such as `canopy-oauth`.
#[derive(Clone, Copy, Debug, Default)]
pub struct RetryErrorClassificationContext<'a> {
    pub auth_type: Option<&'a str>,
    pub extra_retry_error_codes: &'a [i64],
}

/// Stable, serializable diagnostics for a provider failure. Optional values
/// are omitted from JSON, matching the source helper's object shape.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryErrorClassification {
    pub kind: RetryErrorKind,
    pub diagnosis: RetryErrorDiagnosis,
    pub reason: RetryErrorReason,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport_code: Option<String>,
    /// Raw `Retry-After` response header, for the retry policy to interpret.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<String>,
}

impl RetryErrorClassification {
    /// The diagnostic classification itself says this failure may be retried.
    /// This is distinct from [`default_should_retry`]: quota 429s are
    /// diagnosed as fail-fast but still receive the normal bounded retry
    /// budget in the TypeScript implementation.
    pub fn is_retryable(&self) -> bool {
        self.diagnosis == RetryErrorDiagnosis::Retryable
    }

    /// Whether this classification qualifies for model fallback after
    /// same-model retries are exhausted.
    pub fn is_fallback_eligible(&self) -> bool {
        self.kind != RetryErrorKind::Transport
            && self
                .status_code
                .is_some_and(|status| FALLBACK_ELIGIBLE_STATUSES.contains(&status))
            && !matches!(
                self.diagnosis,
                RetryErrorDiagnosis::FailFast | RetryErrorDiagnosis::Unknown
            )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProviderErrorTransport {
    Http,
    Sse,
    #[default]
    Unknown,
}

/// Extracted fields used by classification and retry logging.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderErrorDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<String>,
    pub transport: ProviderErrorTransport,
}

/// Extract status and provider details from a typed provider error.
///
/// HTTP status takes precedence over an `HTTP_STATUS/NNN` marker. Otherwise,
/// the marker is parsed from the transport message or rendered error text.
/// SSE `data:` frames and ordinary JSON error bodies are both inspected for
/// provider code, message, and request ID.
pub fn extract_provider_error_details(error: &ProviderError) -> ProviderErrorDetails {
    let raw_message = raw_error_message(error);
    let status_code = match error {
        ProviderError::HttpStatus { status, .. } => Some(status.as_u16()),
        _ => parse_http_status_marker(&raw_message),
    };
    let retry_after = match error {
        ProviderError::HttpStatus { retry_after, .. } => retry_after.clone(),
        _ => None,
    };
    let transport = if raw_message.contains("event:error") || raw_message.contains("HTTP_STATUS/") {
        ProviderErrorTransport::Sse
    } else if status_code.is_some() {
        ProviderErrorTransport::Http
    } else {
        ProviderErrorTransport::Unknown
    };
    let provider = extract_provider_payload_fields(&raw_message);

    ProviderErrorDetails {
        status_code,
        provider_code: provider.code,
        provider_message: provider.message,
        request_id: provider.request_id,
        retry_after,
        transport,
    }
}

/// Classify a provider error using the source's ordered business, throttling,
/// transport, and HTTP rules.
pub fn classify_provider_error(
    error: &ProviderError,
    context: RetryErrorClassificationContext<'_>,
) -> RetryErrorClassification {
    let details = extract_provider_error_details(error);
    let common = |kind, diagnosis, reason, transport_code| RetryErrorClassification {
        kind,
        diagnosis,
        reason,
        status_code: details.status_code,
        provider_code: details.provider_code.clone(),
        provider_message: details.provider_message.clone(),
        request_id: details.request_id.clone(),
        transport_code,
        retry_after: details.retry_after.clone(),
    };

    if matches!(error, ProviderError::Cancelled) {
        return common(
            RetryErrorKind::Abort,
            RetryErrorDiagnosis::FailFast,
            RetryErrorReason::Aborted,
            None,
        );
    }

    if context.auth_type == Some("canopy-oauth")
        && details.status_code == Some(429)
        && details.provider_code.as_deref() == Some("insufficient_quota")
        && details.provider_message.as_deref().is_some_and(|message| {
            message
                .to_ascii_lowercase()
                .contains("free allocated quota exceeded")
        })
    {
        return common(
            RetryErrorKind::ProviderBusiness,
            RetryErrorDiagnosis::FailFast,
            RetryErrorReason::CanopyOauthFreeTierQuota,
            None,
        );
    }

    if details.provider_code.as_deref() == Some("Throttling.AllocationQuota") {
        return common(
            RetryErrorKind::ProviderBusiness,
            RetryErrorDiagnosis::FailFast,
            RetryErrorReason::AllocatedQuotaExceeded,
            None,
        );
    }

    if is_rate_limit_error(error, &details, context.extra_retry_error_codes) {
        let kind = match details.transport {
            ProviderErrorTransport::Sse => RetryErrorKind::SseProvider,
            ProviderErrorTransport::Http => RetryErrorKind::Http,
            ProviderErrorTransport::Unknown if details.status_code.is_some() => {
                RetryErrorKind::Http
            }
            ProviderErrorTransport::Unknown => RetryErrorKind::Provider,
        };
        return common(
            kind,
            RetryErrorDiagnosis::Retryable,
            RetryErrorReason::RateLimit,
            None,
        );
    }

    if let Some(transport_code) = get_transport_code(error) {
        if details.status_code.is_none_or(|status| status >= 500) {
            return common(
                RetryErrorKind::Transport,
                RetryErrorDiagnosis::Retryable,
                RetryErrorReason::TransportError,
                Some(transport_code),
            );
        }
    }

    // ProviderError's transport and timeout variants are already typed, even
    // when the platform does not expose a stable symbolic socket code.
    if matches!(
        error,
        ProviderError::Transport { .. }
            | ProviderError::StreamIdleTimeout(_)
            | ProviderError::StreamLifetimeExceeded(_)
    ) {
        return common(
            RetryErrorKind::Transport,
            RetryErrorDiagnosis::Retryable,
            RetryErrorReason::TransportError,
            None,
        );
    }

    if let Some(status) = details.status_code {
        let kind = if details.transport == ProviderErrorTransport::Sse {
            RetryErrorKind::SseProvider
        } else {
            RetryErrorKind::Http
        };

        if status == 529 {
            return common(
                kind,
                RetryErrorDiagnosis::Retryable,
                RetryErrorReason::CapacityOverload,
                None,
            );
        }
        if status == 401 || status == 403 {
            return common(
                kind,
                RetryErrorDiagnosis::FailFast,
                RetryErrorReason::AuthError,
                None,
            );
        }
        if (400..500).contains(&status) {
            return common(
                kind,
                RetryErrorDiagnosis::FailFast,
                RetryErrorReason::ClientError,
                None,
            );
        }
        if (500..600).contains(&status) {
            return common(
                kind,
                RetryErrorDiagnosis::Retryable,
                RetryErrorReason::ServerError,
                None,
            );
        }
        return common(
            kind,
            RetryErrorDiagnosis::Unknown,
            RetryErrorReason::HttpStatus,
            None,
        );
    }

    common(
        RetryErrorKind::Unknown,
        RetryErrorDiagnosis::Unknown,
        RetryErrorReason::Unclassified,
        None,
    )
}

/// Match `retry.ts`'s default bounded-retry predicate. Rate limits, all 5xx
/// statuses, and typed transport failures may retry. Business-quota errors
/// with a 429 still return true here; their fail-fast diagnosis separately
/// prevents them from entering the persistent retry loop.
pub fn default_should_retry(
    error: &ProviderError,
    context: RetryErrorClassificationContext<'_>,
) -> bool {
    let details = extract_provider_error_details(error);
    is_rate_limit_error(error, &details, context.extra_retry_error_codes)
        || details
            .status_code
            .is_some_and(|status| (500..600).contains(&status))
        || matches!(
            error,
            ProviderError::Transport { .. }
                | ProviderError::StreamIdleTimeout(_)
                | ProviderError::StreamLifetimeExceeded(_)
        )
}

/// The no-extra-codes form of [`default_should_retry`].
pub fn is_retryable_by_default(error: &ProviderError) -> bool {
    default_should_retry(error, RetryErrorClassificationContext::default())
}

/// Determine whether fallback is allowed by the classified capacity status.
pub fn is_fallback_eligible(classification: &RetryErrorClassification) -> bool {
    classification.is_fallback_eligible()
}

/// Extract the HTTP status carried by a provider error, including an
/// SSE-embedded `HTTP_STATUS/NNN` marker.
pub fn extract_provider_error_status(error: &ProviderError) -> Option<u16> {
    extract_provider_error_details(error).status_code
}

#[derive(Default)]
struct ProviderPayloadFields {
    code: Option<String>,
    message: Option<String>,
    request_id: Option<String>,
}

fn raw_error_message(error: &ProviderError) -> String {
    match error {
        ProviderError::HttpStatus { body, .. } => body.clone(),
        ProviderError::Transport { message, .. } => message.clone(),
        _ => error.to_string(),
    }
}

fn extract_provider_payload_fields(message: &str) -> ProviderPayloadFields {
    for payload in json_payloads(message) {
        let Some(direct) = payload.as_object() else {
            continue;
        };
        let nested_error = payload.get("error");
        let source = nested_error
            .filter(|value| value.is_object() || value.is_array())
            .unwrap_or(&payload);

        let code = source.get("code").and_then(json_code_string);
        let provider_message = source
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let request_id = source
            .get("request_id")
            .and_then(Value::as_str)
            .or_else(|| source.get("requestId").and_then(Value::as_str))
            .or_else(|| direct.get("request_id").and_then(Value::as_str))
            .or_else(|| direct.get("requestId").and_then(Value::as_str))
            .map(str::to_owned);

        if code.is_some() || provider_message.is_some() || request_id.is_some() {
            return ProviderPayloadFields {
                code,
                message: provider_message,
                request_id,
            };
        }
    }
    ProviderPayloadFields::default()
}

fn json_payloads(message: &str) -> Vec<Value> {
    let mut payloads = Vec::new();
    for line in message.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        if let Ok(payload) = serde_json::from_str::<Value>(data) {
            payloads.push(payload);
        }
    }
    if !payloads.is_empty() {
        return payloads;
    }

    let Some(start) = message.find('{') else {
        return payloads;
    };
    let Some(end) = message.rfind('}') else {
        return payloads;
    };
    if end <= start {
        return payloads;
    }
    if let Ok(payload) = serde_json::from_str::<Value>(&message[start..=end]) {
        payloads.push(payload);
    }
    payloads
}

fn json_code_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn get_api_error_numeric_code(message: &str) -> Option<f64> {
    for payload in json_payloads(message) {
        let Some(error) = payload.get("error").and_then(Value::as_object) else {
            continue;
        };
        // quotaErrorDetection's ApiError guard only requires an `error`
        // object with a `message` property; the value need not be a string.
        if !error.contains_key("message") {
            continue;
        }
        if let Some(number) = error.get("code").and_then(js_number) {
            if number.is_finite() && number > 0.0 {
                return Some(number);
            }
        }
    }
    None
}

fn js_number(value: &Value) -> Option<f64> {
    match value {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64(),
        Value::String(value) => parse_js_number(value),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn parse_js_number(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(0.0);
    }
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        return u64::from_str_radix(hex, 16)
            .ok()
            .map(|number| number as f64);
    }
    if let Some(binary) = trimmed
        .strip_prefix("0b")
        .or_else(|| trimmed.strip_prefix("0B"))
    {
        return u64::from_str_radix(binary, 2)
            .ok()
            .map(|number| number as f64);
    }
    if let Some(octal) = trimmed
        .strip_prefix("0o")
        .or_else(|| trimmed.strip_prefix("0O"))
    {
        return u64::from_str_radix(octal, 8)
            .ok()
            .map(|number| number as f64);
    }
    trimmed.parse::<f64>().ok()
}

fn is_rate_limit_error(
    error: &ProviderError,
    details: &ProviderErrorDetails,
    extra_codes: &[i64],
) -> bool {
    let raw_message = raw_error_message(error);
    let numeric_code =
        get_api_error_numeric_code(&raw_message).or_else(|| details.status_code.map(f64::from));
    let Some(code) = numeric_code else {
        return false;
    };
    if BUILTIN_RATE_LIMIT_CODES
        .iter()
        .any(|known| code == *known as f64)
    {
        return true;
    }
    extra_codes.iter().any(|extra| code == *extra as f64)
}

fn parse_http_status_marker(message: &str) -> Option<u16> {
    for (start, _) in message.match_indices("HTTP_STATUS/") {
        let digits_start = start + "HTTP_STATUS/".len();
        let digits_end = digits_start + 3;
        let Some(digits) = message.get(digits_start..digits_end) else {
            continue;
        };
        if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let next = message
            .get(digits_end..)
            .and_then(|rest| rest.chars().next());
        if next.is_some_and(|character| character.is_ascii_alphanumeric() || character == '_') {
            continue;
        }
        let Ok(status) = digits.parse::<u16>() else {
            continue;
        };
        if (100..=599).contains(&status) {
            return Some(status);
        }
    }
    None
}

fn get_transport_code(error: &ProviderError) -> Option<String> {
    match error {
        ProviderError::Transport { source, .. } => transport_code_from_reqwest(source),
        ProviderError::StreamIdleTimeout(_) => Some("ETIMEDOUT".to_owned()),
        _ => None,
    }
}

fn transport_code_from_reqwest(error: &reqwest::Error) -> Option<String> {
    if error.is_timeout() {
        return Some("ETIMEDOUT".to_owned());
    }
    let mut current: Option<&(dyn Error + 'static)> = Some(error);
    for _ in 0..=MAX_TRANSPORT_CAUSE_DEPTH {
        let cause = current?;
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            let code = match io_error.kind() {
                std::io::ErrorKind::ConnectionAborted => "ECONNABORTED",
                std::io::ErrorKind::ConnectionRefused => "ECONNREFUSED",
                std::io::ErrorKind::ConnectionReset => "ECONNRESET",
                std::io::ErrorKind::BrokenPipe => "EPIPE",
                std::io::ErrorKind::TimedOut => "ETIMEDOUT",
                _ => return None,
            };
            return Some(code.to_owned());
        }
        current = cause.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::StatusCode;

    use crate::providers::openai_compatible::ProviderError;

    use super::{
        ProviderErrorTransport, RetryErrorClassificationContext, RetryErrorDiagnosis,
        RetryErrorKind, RetryErrorReason, classify_provider_error, default_should_retry,
        extract_provider_error_details, extract_provider_error_status, is_fallback_eligible,
        is_retryable_by_default,
    };

    fn http_error(status: StatusCode, body: &str) -> ProviderError {
        ProviderError::HttpStatus {
            status,
            body: body.to_owned(),
            truncated: "",
            retry_after: None,
        }
    }

    #[test]
    fn classifies_rate_limit_and_server_http_statuses() {
        let limited = classify_provider_error(
            &http_error(StatusCode::TOO_MANY_REQUESTS, "Too many requests"),
            RetryErrorClassificationContext::default(),
        );
        assert_eq!(limited.kind, RetryErrorKind::Http);
        assert_eq!(limited.diagnosis, RetryErrorDiagnosis::Retryable);
        assert_eq!(limited.reason, RetryErrorReason::RateLimit);
        assert_eq!(limited.status_code, Some(429));
        assert!(is_retryable_by_default(&http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "Too many requests"
        )));

        let server = classify_provider_error(
            &http_error(StatusCode::INTERNAL_SERVER_ERROR, "Internal error"),
            RetryErrorClassificationContext::default(),
        );
        assert_eq!(server.diagnosis, RetryErrorDiagnosis::Retryable);
        assert_eq!(server.reason, RetryErrorReason::ServerError);
        assert!(is_fallback_eligible(&classify_provider_error(
            &http_error(StatusCode::SERVICE_UNAVAILABLE, "Unavailable"),
            RetryErrorClassificationContext::default(),
        )));
    }

    #[test]
    fn preserves_retry_after_for_the_retry_policy() {
        let error = ProviderError::HttpStatus {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: "limited".to_owned(),
            truncated: "",
            retry_after: Some("2.5".to_owned()),
        };

        let details = extract_provider_error_details(&error);
        assert_eq!(details.retry_after.as_deref(), Some("2.5"));

        let classification =
            classify_provider_error(&error, RetryErrorClassificationContext::default());
        assert_eq!(classification.retry_after.as_deref(), Some("2.5"));
    }

    #[test]
    fn extracts_nested_provider_fields_from_http_and_sse_payloads() {
        let http = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"rate_limit_exceeded","message":"Try later","request_id":"req-http"}}"#,
        );
        let details = extract_provider_error_details(&http);
        assert_eq!(
            details.provider_code.as_deref(),
            Some("rate_limit_exceeded")
        );
        assert_eq!(details.provider_message.as_deref(), Some("Try later"));
        assert_eq!(details.request_id.as_deref(), Some("req-http"));
        assert_eq!(details.transport, ProviderErrorTransport::Http);

        let sse = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "id:1\nevent:error\n:HTTP_STATUS/503\ndata:{\"request_id\":\"req-sse\",\"code\":\"Throttling.RateLimit\",\"message\":\"Rate limit exceeded\"}",
        );
        let classification =
            classify_provider_error(&sse, RetryErrorClassificationContext::default());
        assert_eq!(classification.kind, RetryErrorKind::SseProvider);
        assert_eq!(classification.status_code, Some(429));
        assert_eq!(classification.reason, RetryErrorReason::RateLimit);
        assert_eq!(
            classification.provider_code.as_deref(),
            Some("Throttling.RateLimit")
        );
        assert_eq!(classification.request_id.as_deref(), Some("req-sse"));
        // The real source prefers its direct HTTP status over an embedded
        // marker. This valid failed response pins that priority (429 wins over
        // the conflicting SSE marker at 503).
        assert_eq!(extract_provider_error_status(&sse), Some(429));
    }

    #[test]
    fn allocation_quota_is_fail_fast_but_still_gets_bounded_default_retries() {
        let error = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"Throttling.AllocationQuota","message":"Allocated quota exceeded"}}"#,
        );
        let classification =
            classify_provider_error(&error, RetryErrorClassificationContext::default());
        assert_eq!(classification.kind, RetryErrorKind::ProviderBusiness);
        assert_eq!(classification.diagnosis, RetryErrorDiagnosis::FailFast);
        assert_eq!(
            classification.reason,
            RetryErrorReason::AllocatedQuotaExceeded
        );
        assert!(!classification.is_retryable());
        assert!(is_retryable_by_default(&error));
        assert!(!classification.is_fallback_eligible());
    }

    #[test]
    fn recognizes_canopy_oauth_free_quota_only_with_matching_context() {
        let error = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"insufficient_quota","message":"Free allocated quota exceeded"}}"#,
        );
        let ordinary = classify_provider_error(&error, RetryErrorClassificationContext::default());
        assert_eq!(ordinary.reason, RetryErrorReason::RateLimit);
        let canopy_oauth = classify_provider_error(
            &error,
            RetryErrorClassificationContext {
                auth_type: Some("canopy-oauth"),
                extra_retry_error_codes: &[],
            },
        );
        assert_eq!(canopy_oauth.kind, RetryErrorKind::ProviderBusiness);
        assert_eq!(canopy_oauth.diagnosis, RetryErrorDiagnosis::FailFast);
        assert_eq!(
            canopy_oauth.reason,
            RetryErrorReason::CanopyOauthFreeTierQuota
        );
    }

    #[test]
    fn recognizes_known_and_extra_numeric_provider_rate_limit_codes() {
        let glm = http_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"error":{"code":"1302","message":"Rate limited"}}"#,
        );
        let glm_classification =
            classify_provider_error(&glm, RetryErrorClassificationContext::default());
        assert_eq!(glm_classification.reason, RetryErrorReason::RateLimit);
        assert_eq!(glm_classification.provider_code.as_deref(), Some("1302"));

        let custom = http_error(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"code":4999,"message":"Custom throttle"}}"#,
        );
        let context = RetryErrorClassificationContext {
            auth_type: None,
            extra_retry_error_codes: &[4999],
        };
        assert_eq!(
            classify_provider_error(&custom, context).reason,
            RetryErrorReason::RateLimit
        );
        assert!(default_should_retry(&custom, context));
        assert!(!default_should_retry(
            &custom,
            RetryErrorClassificationContext::default()
        ));
    }

    #[test]
    fn valid_api_error_code_takes_precedence_over_a_rate_limit_status() {
        let error = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":400,"message":"Bad request"}}"#,
        );
        let classification =
            classify_provider_error(&error, RetryErrorClassificationContext::default());
        assert_eq!(classification.diagnosis, RetryErrorDiagnosis::FailFast);
        assert_eq!(classification.reason, RetryErrorReason::ClientError);
        assert!(!default_should_retry(
            &error,
            RetryErrorClassificationContext::default()
        ));
    }

    #[test]
    fn status_classification_pins_auth_client_capacity_and_unknown_ranges() {
        for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
            let classification = classify_provider_error(
                &http_error(status, "authentication failed"),
                RetryErrorClassificationContext::default(),
            );
            assert_eq!(classification.reason, RetryErrorReason::AuthError);
            assert_eq!(classification.diagnosis, RetryErrorDiagnosis::FailFast);
        }
        for status in [StatusCode::BAD_REQUEST, StatusCode::from_u16(408).unwrap()] {
            let classification = classify_provider_error(
                &http_error(status, "client error"),
                RetryErrorClassificationContext::default(),
            );
            assert_eq!(classification.reason, RetryErrorReason::ClientError);
            assert_eq!(classification.diagnosis, RetryErrorDiagnosis::FailFast);
        }
        let capacity = classify_provider_error(
            &http_error(StatusCode::from_u16(529).unwrap(), "Overloaded"),
            RetryErrorClassificationContext::default(),
        );
        assert_eq!(capacity.reason, RetryErrorReason::CapacityOverload);
        assert!(capacity.is_fallback_eligible());

        let redirect = classify_provider_error(
            &http_error(StatusCode::FOUND, "redirect"),
            RetryErrorClassificationContext::default(),
        );
        assert_eq!(redirect.diagnosis, RetryErrorDiagnosis::Unknown);
        assert_eq!(redirect.reason, RetryErrorReason::HttpStatus);
        assert!(!is_retryable_by_default(&http_error(
            StatusCode::FOUND,
            "redirect"
        )));
    }

    #[test]
    fn typed_transport_and_stream_timeouts_are_retryable() {
        let timeout = ProviderError::StreamIdleTimeout(Duration::from_secs(30));
        let classification =
            classify_provider_error(&timeout, RetryErrorClassificationContext::default());
        assert_eq!(classification.kind, RetryErrorKind::Transport);
        assert_eq!(classification.diagnosis, RetryErrorDiagnosis::Retryable);
        assert_eq!(classification.reason, RetryErrorReason::TransportError);
        assert_eq!(classification.transport_code.as_deref(), Some("ETIMEDOUT"));
        assert!(is_retryable_by_default(&timeout));

        let invalid = ProviderError::InvalidRequestShape;
        let classification =
            classify_provider_error(&invalid, RetryErrorClassificationContext::default());
        assert_eq!(classification.kind, RetryErrorKind::Unknown);
        assert_eq!(classification.diagnosis, RetryErrorDiagnosis::Unknown);
        assert_eq!(classification.reason, RetryErrorReason::Unclassified);
        assert!(!is_retryable_by_default(&invalid));
    }

    #[test]
    fn structured_classification_serializes_with_camel_case_optional_fields() {
        let classification = classify_provider_error(
            &http_error(StatusCode::TOO_MANY_REQUESTS, "limited"),
            RetryErrorClassificationContext::default(),
        );
        let diagnostic = serde_json::to_value(classification).unwrap();
        assert_eq!(diagnostic["kind"], "http");
        assert_eq!(diagnostic["diagnosis"], "retryable");
        assert_eq!(diagnostic["reason"], "rate-limit");
        assert_eq!(diagnostic["statusCode"], 429);
        assert!(diagnostic.get("providerCode").is_none());
    }
}
