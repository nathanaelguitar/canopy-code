// Copyright 2025 Google LLC
// SPDX-License-Identifier: Apache-2.0
//
//! Provider API error formatting ported from
//! `packages/core/src/utils/errorParsing.ts`.

use std::collections::HashSet;

use serde_json::Value;

const API_ERROR_PREFIX: &str = "[API Error: ";
const QUOTA_EXHAUSTED_PREFIX: &str = "Quota exhausted: ";
const ERROR_MESSAGE_MAX_LENGTH: usize = 1000;
const RATE_LIMIT_GEMINI: &str = "\nPlease wait and try again later. To increase your limits, request a quota increase through AI Studio, or switch to another /auth method";
const RATE_LIMIT_VERTEX: &str = "\nPlease wait and try again later. To increase your limits, request a quota increase through Vertex, or switch to another /auth method";
const RATE_LIMIT_DEFAULT: &str = "\nPossible quota limitations in place or slow response times detected. Please wait and try again later.";

/// Supported authentication paths, mirroring the TypeScript `AuthType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthType {
    OpenAi,
    CanopyOauth,
    ChatgptOauth,
    Gemini,
    VertexAi,
    Anthropic,
}

impl AuthType {
    /// The TypeScript wire value for this authentication path.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::CanopyOauth => "canopy-oauth",
            Self::ChatgptOauth => "chatgpt-oauth",
            Self::Gemini => "gemini",
            Self::VertexAi => "vertex-ai",
            Self::Anthropic => "anthropic",
        }
    }
}

/// The object-shaped error recognized by `isStructuredError`.
#[derive(Clone, Debug, PartialEq)]
pub struct StructuredError {
    pub message: String,
    pub status: Option<f64>,
}

impl StructuredError {
    pub fn new(message: impl Into<String>, status: Option<f64>) -> Self {
        Self {
            message: message.into(),
            status,
        }
    }
}

/// A Rust representation of an `Error` instance and the fields used by
/// `getErrorMessage` when formatting its cause chain.
#[derive(Clone, Debug, PartialEq)]
pub struct ErrorDetails {
    pub message: String,
    pub status: Option<f64>,
    pub cause: Option<ErrorCause>,
}

impl ErrorDetails {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
            cause: None,
        }
    }

    pub fn with_status(mut self, status: f64) -> Self {
        self.status = Some(status);
        self
    }

    pub fn with_cause(mut self, cause: ErrorCause) -> Self {
        self.cause = Some(cause);
        self
    }
}

/// Cause values supported by the source error-message formatter.
#[derive(Clone, Debug, PartialEq)]
pub enum ErrorCause {
    Error {
        message: String,
        name: String,
        code: Option<String>,
        cause: Option<Box<ErrorCause>>,
    },
    Object {
        message: Option<String>,
        code: Option<String>,
        cause: Option<Box<ErrorCause>>,
    },
    Aggregate(Vec<ErrorCause>),
    Value(Value),
}

impl ErrorCause {
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
            name: "Error".to_owned(),
            code: None,
            cause: None,
        }
    }

    pub fn coded_error(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self::Error {
            message: message.into(),
            name: "Error".to_owned(),
            code: Some(code.into()),
            cause: None,
        }
    }

    pub fn aggregate(errors: impl IntoIterator<Item = ErrorCause>) -> Self {
        Self::Aggregate(errors.into_iter().collect())
    }
}

/// Dynamic input forms accepted by [`parse_and_format_api_error`]. JSON values
/// preserve the source's structural detection for plain error-like objects;
/// `ErrorDetails` carries the richer cause data unavailable in `serde_json`.
#[derive(Clone, Debug, PartialEq)]
pub enum ErrorInput {
    String(String),
    Structured(StructuredError),
    Error(ErrorDetails),
    Value(Value),
    Unknown,
}

impl From<&str> for ErrorInput {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for ErrorInput {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Value> for ErrorInput {
    fn from(value: Value) -> Self {
        match value {
            Value::String(message) => Self::String(message),
            other => Self::Value(other),
        }
    }
}

impl From<StructuredError> for ErrorInput {
    fn from(value: StructuredError) -> Self {
        Self::Structured(value)
    }
}

impl From<ErrorDetails> for ErrorInput {
    fn from(value: ErrorDetails) -> Self {
        Self::Error(value)
    }
}

/// Convert an unknown provider error into the user-facing API error string.
///
/// Strings may contain a JSON API error after a prefix. Structured errors
/// include status-specific quota guidance; API JSON includes the provider's
/// status text. Already-formatted API/quota messages are returned unchanged.
pub fn parse_and_format_api_error(
    error: impl Into<ErrorInput>,
    auth_type: Option<AuthType>,
) -> String {
    match error.into() {
        ErrorInput::String(message) => parse_string_error(&message, auth_type),
        ErrorInput::Structured(error) => {
            format_structured_error(&error.message, error.status, None, false, auth_type)
        }
        ErrorInput::Error(error) => format_structured_error(
            &error.message,
            error.status,
            error.cause.as_ref(),
            true,
            auth_type,
        ),
        ErrorInput::Value(Value::String(message)) => parse_string_error(&message, auth_type),
        ErrorInput::Value(value) => {
            if let Some((message, status)) = structured_error_fields(&value) {
                format_structured_error(message, status, None, false, auth_type)
            } else {
                unknown_error()
            }
        }
        ErrorInput::Unknown => unknown_error(),
    }
}

fn parse_string_error(error: &str, auth_type: Option<AuthType>) -> String {
    if is_already_formatted(error) {
        return error.to_owned();
    }

    let Some(json_start) = error.find('{') else {
        return wrap_api_error(error);
    };
    let Ok(parsed_error) = serde_json::from_str::<Value>(&error[json_start..]) else {
        return wrap_api_error(error);
    };
    let Some(api_error) = api_error_fields(&parsed_error) else {
        return wrap_api_error(error);
    };

    let mut final_message = api_error.message.clone();
    if let Some(message) = final_message.as_str()
        && let Ok(nested_error) = serde_json::from_str::<Value>(message)
        && let Some(nested_api_error) = api_error_fields(&nested_error)
    {
        final_message = nested_api_error.message.clone();
    }

    let status_text = api_error
        .status
        .filter(|status| js_truthy(status))
        .map(|status| format!(" (Status: {})", js_string(status)))
        .unwrap_or_default();
    let mut text = format!(
        "{API_ERROR_PREFIX}{}{status_text}]",
        js_string(&final_message)
    );
    if api_error
        .code
        .is_some_and(|code| json_number_equals(code, 429.0))
    {
        text.push_str(rate_limit_message(auth_type));
    }
    text
}

struct ApiErrorFields<'a> {
    code: Option<&'a Value>,
    message: &'a Value,
    status: Option<&'a Value>,
}

fn api_error_fields(value: &Value) -> Option<ApiErrorFields<'_>> {
    let error = value.get("error")?.as_object()?;
    let message = error.get("message")?;
    Some(ApiErrorFields {
        code: error.get("code"),
        message,
        status: error.get("status"),
    })
}

fn structured_error_fields(value: &Value) -> Option<(&str, Option<f64>)> {
    let object = value.as_object()?;
    let message = object.get("message")?.as_str()?;
    let status = object.get("status").and_then(Value::as_f64);
    Some((message, status))
}

fn format_structured_error(
    message: &str,
    status: Option<f64>,
    cause: Option<&ErrorCause>,
    is_error_instance: bool,
    auth_type: Option<AuthType>,
) -> String {
    if message.starts_with("Canopy OAuth quota exceeded:")
        || message.starts_with("Canopy OAuth free tier has been discontinued")
    {
        return message.to_owned();
    }
    if is_already_formatted(message) {
        return message.to_owned();
    }

    let message = match is_error_instance {
        true => get_error_message(message, cause),
        false => message.to_owned(),
    };
    let mut text = wrap_api_error(&message);
    if status == Some(429.0) {
        text.push_str(rate_limit_message(auth_type));
    }
    text
}

fn get_error_message(message: &str, cause: Option<&ErrorCause>) -> String {
    let detail = cause.and_then(describe_error_cause);
    if let Some(detail) = detail.filter(|detail| detail != message) {
        truncate_error_message(&format!("{message} (cause: {detail})"))
    } else {
        truncate_error_message(message)
    }
}

fn describe_error_cause(cause: &ErrorCause) -> Option<String> {
    if matches!(cause, ErrorCause::Value(Value::Null)) {
        return None;
    }
    describe_coded_cause(cause, 0).or_else(|| describe_cause_fallback(cause))
}

fn describe_coded_cause(cause: &ErrorCause, depth: usize) -> Option<String> {
    if depth >= 8 {
        return None;
    }
    if let ErrorCause::Aggregate(errors) = cause {
        let details = errors
            .iter()
            .filter_map(|error| describe_coded_cause(error, depth + 1));
        return join_unique(details);
    }

    if let Some(nested) = nested_cause(cause) {
        if let Some(detail) = describe_coded_cause(&nested, depth + 1) {
            return Some(detail);
        }
    }
    if cause_code(cause).is_some_and(|code| !code.is_empty()) {
        return describe_single_error(cause);
    }
    None
}

fn describe_cause_fallback(cause: &ErrorCause) -> Option<String> {
    if let ErrorCause::Aggregate(errors) = cause {
        return join_unique(errors.iter().filter_map(describe_single_error))
            .or_else(|| describe_single_error(cause));
    }
    describe_single_error(cause)
}

fn nested_cause(cause: &ErrorCause) -> Option<ErrorCause> {
    match cause {
        ErrorCause::Error { cause, .. } | ErrorCause::Object { cause, .. } => {
            cause.as_deref().cloned()
        }
        ErrorCause::Value(Value::Object(object)) => {
            object.get("cause").cloned().map(ErrorCause::Value)
        }
        _ => None,
    }
}

fn cause_code(cause: &ErrorCause) -> Option<String> {
    match cause {
        ErrorCause::Error { code, .. } | ErrorCause::Object { code, .. } => code.clone(),
        ErrorCause::Value(Value::Object(object)) => object.get("code").and_then(value_code),
        _ => None,
    }
}

fn describe_single_error(cause: &ErrorCause) -> Option<String> {
    match cause {
        ErrorCause::Error {
            message,
            name,
            code,
            ..
        } => {
            let message = nonempty_trimmed(message);
            if let Some(message) = message {
                if let Some(code) = code.as_deref().filter(|code| !message.contains(code)) {
                    return Some(format!("{code}: {message}"));
                }
                return Some(message.to_owned());
            }
            code.clone()
                .or_else(|| (name != "Error").then(|| name.clone()))
        }
        ErrorCause::Object { message, code, .. } => {
            let message = message.as_deref().and_then(nonempty_trimmed);
            match (message, code.as_deref()) {
                (Some(message), Some(code)) if !message.contains(code) => {
                    Some(format!("{code}: {message}"))
                }
                (Some(message), _) => Some(message.to_owned()),
                (None, Some(code)) if !code.is_empty() => Some(code.to_owned()),
                _ => None,
            }
        }
        ErrorCause::Aggregate(_) => Some("AggregateError".to_owned()),
        ErrorCause::Value(Value::Object(object)) => {
            let message = object
                .get("message")
                .and_then(Value::as_str)
                .and_then(nonempty_trimmed);
            let code = object.get("code").and_then(value_code);
            match (message, code.as_deref()) {
                (Some(message), Some(code)) if !message.contains(code) => {
                    Some(format!("{code}: {message}"))
                }
                (Some(message), _) => Some(message.to_owned()),
                (None, Some(code)) => Some(code.to_owned()),
                _ => None,
            }
        }
        ErrorCause::Value(value) => {
            let string = js_string(value);
            (string != "[object Object]").then_some(string)
        }
    }
}

fn nonempty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn value_code(value: &Value) -> Option<String> {
    match value {
        Value::String(code) if !code.is_empty() => Some(code.clone()),
        Value::Number(code) => Some(code.to_string()),
        _ => None,
    }
}

fn join_unique(values: impl IntoIterator<Item = String>) -> Option<String> {
    let mut seen = HashSet::new();
    let unique: Vec<String> = values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect();
    (!unique.is_empty()).then(|| unique.join("; "))
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn json_number_equals(value: &Value, expected: f64) -> bool {
    value.as_f64() == Some(expected)
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn truncate_error_message(message: &str) -> String {
    if message.encode_utf16().count() <= ERROR_MESSAGE_MAX_LENGTH {
        return message.to_owned();
    }
    let mut truncated = String::new();
    let mut code_units = 0;
    for character in message.chars() {
        let character_units = character.len_utf16();
        if code_units + character_units > ERROR_MESSAGE_MAX_LENGTH - 3 {
            break;
        }
        truncated.push(character);
        code_units += character_units;
    }
    truncated.push_str("...");
    truncated
}

fn is_already_formatted(value: &str) -> bool {
    let trimmed = value.trim_end();
    if trimmed.starts_with(QUOTA_EXHAUSTED_PREFIX) {
        return true;
    }
    if !trimmed.starts_with(API_ERROR_PREFIX) {
        return false;
    }
    if trimmed.ends_with(']') {
        return true;
    }
    [RATE_LIMIT_GEMINI, RATE_LIMIT_VERTEX, RATE_LIMIT_DEFAULT]
        .iter()
        .any(|suffix| trimmed.contains(&format!("]{suffix}")))
}

fn rate_limit_message(auth_type: Option<AuthType>) -> &'static str {
    match auth_type {
        Some(AuthType::Gemini) => RATE_LIMIT_GEMINI,
        Some(AuthType::VertexAi) => RATE_LIMIT_VERTEX,
        _ => RATE_LIMIT_DEFAULT,
    }
}

fn wrap_api_error(message: &str) -> String {
    format!("{API_ERROR_PREFIX}{message}]")
}

fn unknown_error() -> String {
    format!("{API_ERROR_PREFIX}An unknown error occurred.]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GEMINI_GUIDANCE: &str = "request a quota increase through AI Studio";
    const VERTEX_GUIDANCE: &str = "request a quota increase through Vertex";

    fn api_error_json(code: i64, message: &str, status: &str) -> String {
        format!(
            "got status: {code}. {{\"error\":{{\"code\":{code},\"message\":{},\"status\":{}}}}}",
            serde_json::to_string(message).unwrap(),
            serde_json::to_string(status).unwrap()
        )
    }

    #[test]
    fn formats_provider_api_json_and_preserves_status_text() {
        let error = "got status: 400 Bad Request. {\"error\":{\"code\":400,\"message\":\"API key not valid. Please pass a valid API key.\",\"status\":\"INVALID_ARGUMENT\"}}";
        assert_eq!(
            parse_and_format_api_error(error, None),
            "[API Error: API key not valid. Please pass a valid API key. (Status: INVALID_ARGUMENT)]"
        );
        assert_eq!(
            parse_and_format_api_error(
                "{\"error\":{\"code\":1302,\"message\":\"账户已达到速率限制\"}}",
                None
            ),
            "[API Error: 账户已达到速率限制]"
        );
    }

    #[test]
    fn formats_429_api_json_with_auth_specific_guidance() {
        let error = api_error_json(429, "Rate limit exceeded", "RESOURCE_EXHAUSTED");
        let default = parse_and_format_api_error(error.as_str(), None);
        assert!(
            default.starts_with("[API Error: Rate limit exceeded (Status: RESOURCE_EXHAUSTED)]")
        );
        assert!(default.contains("Possible quota limitations in place"));

        let gemini = parse_and_format_api_error(error.as_str(), Some(AuthType::Gemini));
        assert!(gemini.contains(GEMINI_GUIDANCE));
        let vertex = parse_and_format_api_error(error.as_str(), Some(AuthType::VertexAi));
        assert!(vertex.contains(VERTEX_GUIDANCE));
        let openai = parse_and_format_api_error(error, Some(AuthType::OpenAi));
        assert!(openai.contains("Possible quota limitations in place"));
    }

    #[test]
    fn maps_every_auth_type_and_uses_default_guidance_for_non_google_auth() {
        assert_eq!(AuthType::OpenAi.as_str(), "openai");
        assert_eq!(AuthType::CanopyOauth.as_str(), "canopy-oauth");
        assert_eq!(AuthType::ChatgptOauth.as_str(), "chatgpt-oauth");
        assert_eq!(AuthType::Gemini.as_str(), "gemini");
        assert_eq!(AuthType::VertexAi.as_str(), "vertex-ai");
        assert_eq!(AuthType::Anthropic.as_str(), "anthropic");
        for auth_type in [
            Some(AuthType::OpenAi),
            Some(AuthType::CanopyOauth),
            Some(AuthType::ChatgptOauth),
            Some(AuthType::Anthropic),
            None,
        ] {
            assert_eq!(rate_limit_message(auth_type), RATE_LIMIT_DEFAULT);
        }
    }

    #[test]
    fn wraps_plain_malformed_and_non_api_strings() {
        for raw in [
            "This is a plain old error message",
            "[Stream Error: {\"error\": \"malformed}]",
            "[Stream Error: {\"not_an_error\": \"some other json\"}]",
        ] {
            assert_eq!(
                parse_and_format_api_error(raw, None),
                format!("[API Error: {raw}]")
            );
        }
        assert_eq!(
            parse_and_format_api_error(ErrorInput::Value(json!(12345)), None),
            "[API Error: An unknown error occurred.]"
        );
    }

    #[test]
    fn unwraps_one_nested_api_error_and_adds_auth_guidance() {
        let nested = json!({
            "error": {
                "code": 429,
                "message": "Gemini 2.5 Pro Preview does not have a free quota tier.",
                "status": "RESOURCE_EXHAUSTED"
            }
        });
        let outer = json!({
            "error": {
                "code": 429,
                "message": nested.to_string(),
                "status": "Too Many Requests"
            }
        });
        let result = parse_and_format_api_error(outer.to_string(), Some(AuthType::Gemini));
        assert!(result.contains("Gemini 2.5 Pro Preview"));
        assert!(result.contains(GEMINI_GUIDANCE));
        assert!(result.contains("(Status: Too Many Requests)"));
    }

    #[test]
    fn structured_errors_preserve_special_quota_text_and_status_guidance() {
        let ordinary = StructuredError::new("A structured error occurred", Some(500.0));
        assert_eq!(
            parse_and_format_api_error(ordinary, None),
            "[API Error: A structured error occurred]"
        );

        let rate_limit = StructuredError::new("Rate limit exceeded", Some(429.0));
        let result = parse_and_format_api_error(rate_limit, Some(AuthType::VertexAi));
        assert!(result.starts_with("[API Error: Rate limit exceeded]"));
        assert!(result.contains(VERTEX_GUIDANCE));

        let native_rate_limit = ErrorDetails::new("Rate limit exceeded").with_status(429.0);
        assert!(
            parse_and_format_api_error(native_rate_limit, None)
                .contains("Possible quota limitations in place")
        );

        for message in [
            "Canopy OAuth quota exceeded: retry after 12:00 UTC",
            "Canopy OAuth free tier has been discontinued for this model",
        ] {
            assert_eq!(
                parse_and_format_api_error(
                    StructuredError::new(message, Some(429.0)),
                    Some(AuthType::Gemini)
                ),
                message
            );
        }
        let plain_string = "Canopy OAuth quota exceeded: retry after 12:00 UTC";
        assert_eq!(
            parse_and_format_api_error(plain_string, None),
            format!("[API Error: {plain_string}]")
        );
    }

    #[test]
    fn surfaces_native_error_cause_and_aggregate_details() {
        let coded = ErrorCause::coded_error("", "ECONNREFUSED");
        let error = ErrorDetails::new("Connection error.").with_cause(coded);
        assert_eq!(
            parse_and_format_api_error(error, None),
            "[API Error: Connection error. (cause: ECONNREFUSED)]"
        );

        let aggregate = ErrorCause::aggregate([
            ErrorCause::Error {
                message: "fetch failed".to_owned(),
                name: "TypeError".to_owned(),
                code: None,
                cause: Some(Box::new(ErrorCause::coded_error(
                    "connect ECONNREFUSED ::1:29900",
                    "ECONNREFUSED",
                ))),
            },
            ErrorCause::Error {
                message: "fetch failed".to_owned(),
                name: "TypeError".to_owned(),
                code: None,
                cause: Some(Box::new(ErrorCause::coded_error(
                    "connect ETIMEDOUT 127.0.0.1:29900",
                    "ETIMEDOUT",
                ))),
            },
        ]);
        let error = ErrorDetails::new("Connection error.").with_cause(aggregate);
        let result = parse_and_format_api_error(error, None);
        assert!(result.contains("ECONNREFUSED"));
        assert!(result.contains("ETIMEDOUT"));

        let object_cause = ErrorCause::Object {
            message: Some("connection refused".to_owned()),
            code: Some("-32603".to_owned()),
            cause: None,
        };
        let object_error = ErrorDetails::new("fetch failed").with_cause(object_cause);
        assert_eq!(
            parse_and_format_api_error(object_error, None),
            "[API Error: fetch failed (cause: -32603: connection refused)]"
        );

        let value_cause = ErrorCause::Value(json!({ "cause": { "code": "ENOTFOUND" } }));
        let value_error = ErrorDetails::new("fetch failed").with_cause(value_cause);
        assert_eq!(
            parse_and_format_api_error(value_error, None),
            "[API Error: fetch failed (cause: ENOTFOUND)]"
        );
    }

    #[test]
    fn returns_friendly_quota_exhaustion_messages_verbatim() {
        let message = "Quota exhausted: Your token-plan 1-week quota has been exhausted. The quota will reset at 07-27 09:25:00 UTC.\n\nPlease retry after the reset time, or switch to another API key / auth method.";
        assert_eq!(parse_and_format_api_error(message, None), message);
        assert_eq!(
            parse_and_format_api_error(
                StructuredError::new(message, Some(429.0)),
                Some(AuthType::OpenAi)
            ),
            message
        );
    }

    #[test]
    fn idempotency_preserves_final_strings_structured_errors_and_provider_suffixes() {
        let formatted = "[API Error: 402 Model X is not available for billing.]";
        assert_eq!(parse_and_format_api_error(formatted, None), formatted);
        assert_eq!(
            parse_and_format_api_error(StructuredError::new(formatted, Some(402.0)), None),
            formatted
        );

        for (suffix, auth) in [
            (RATE_LIMIT_DEFAULT, None),
            (RATE_LIMIT_GEMINI, Some(AuthType::Gemini)),
            (RATE_LIMIT_VERTEX, Some(AuthType::VertexAi)),
        ] {
            let formatted = format!("[API Error: Rate limit exceeded]{suffix}");
            assert_eq!(
                parse_and_format_api_error(formatted.as_str(), auth),
                formatted
            );
            assert_eq!(
                parse_and_format_api_error(
                    StructuredError::new(formatted.as_str(), Some(429.0)),
                    auth
                ),
                formatted
            );
        }

        let mention = "see [API Error: 502] in the upstream log for details";
        assert_eq!(
            parse_and_format_api_error(mention, None),
            format!("[API Error: {mention}]")
        );
    }

    #[test]
    fn only_numeric_api_code_429_adds_rate_guidance() {
        let string_code = r#"{"error":{"code":"429","message":"Rate limit"}}"#;
        let result = parse_and_format_api_error(string_code, None);
        assert_eq!(result, "[API Error: Rate limit]");
    }

    #[test]
    fn raw_json_object_with_message_uses_structured_path() {
        let error = json!({"message": "already a structured error", "status": 429});
        let result = parse_and_format_api_error(error, Some(AuthType::Gemini));
        assert!(result.starts_with("[API Error: already a structured error]"));
        assert!(result.contains(GEMINI_GUIDANCE));
    }

    #[test]
    fn error_message_cause_equal_to_message_is_not_repeated() {
        let cause = ErrorCause::error("same");
        let error = ErrorDetails::new("same").with_cause(cause);
        assert_eq!(parse_and_format_api_error(error, None), "[API Error: same]");
    }

    #[test]
    fn api_error_shape_requires_an_error_object_with_a_message_field() {
        let missing_message = r#"{"error":{"code":400,"status":"BAD_REQUEST"}}"#;
        assert_eq!(
            parse_and_format_api_error(missing_message, None),
            format!("[API Error: {missing_message}]")
        );
        let null_error = r#"{"error":null}"#;
        assert_eq!(
            parse_and_format_api_error(null_error, None),
            format!("[API Error: {null_error}]")
        );
        assert_eq!(
            parse_and_format_api_error(ErrorInput::Unknown, None),
            "[API Error: An unknown error occurred.]"
        );
    }

    #[test]
    fn only_native_error_instances_apply_the_shared_message_length_cap() {
        let long_message = "x".repeat(1200);
        let structured =
            parse_and_format_api_error(StructuredError::new(long_message.clone(), None), None);
        assert!(structured.len() > 1000);

        let native = parse_and_format_api_error(ErrorDetails::new(long_message), None);
        assert_eq!(native, format!("{API_ERROR_PREFIX}{}...]", "x".repeat(997)));
    }
}
