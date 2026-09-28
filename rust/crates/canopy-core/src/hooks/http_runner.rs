//! HTTP hook execution.
//!
//! Port of `packages/core/src/hooks/httpHookRunner.ts`. The transport and DNS
//! resolver are injected so the host can select its networking stack while
//! this module owns request projection, timeout/cancellation, output
//! normalization, and URL/SSRF policy.
//!
//! The timeout and cancellation window ends when response headers arrive,
//! matching `fetch()` in the TypeScript runner. Reading/parsing the response
//! body happens after that window. DNS is checked immediately before the
//! request, but the default transport still performs its own resolution, so
//! there is a small DNS-rebinding window just as in the source runtime.

use std::collections::HashMap;
use std::future::Future;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use indexmap::IndexMap;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Client, Method, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::hooks::env_interpolator::{interpolate_headers, interpolate_url};
use crate::hooks::planner::HookEventName;
use crate::hooks::ssrf_guard::{is_blocked_address, is_metadata_address};
use crate::hooks::url_validator::UrlValidator;
use crate::utils::cancellation::{CancellationToken, combine_cancellation_tokens};

/// Source-compatible default timeout (ten minutes).
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(10 * 60);
/// Maximum length of each user-facing output string, measured as UTF-16 code
/// units to match JavaScript's `String.length` and `substring` behavior.
pub const MAX_OUTPUT_LENGTH: usize = 10_000;

pub type HttpHookFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// HTTP hook fields used by the runner. Unknown source fields are retained so
/// the execution result can return the original config object unchanged.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HttpHookConfig {
    pub url: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub headers: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_env_vars: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub once: bool,
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
    #[serde(skip)]
    original_config: Option<Value>,
}

impl HttpHookConfig {
    /// Parse a source hook-config object while retaining its original JSON
    /// representation for `HttpHookExecutionResult.hook_config`.
    pub fn from_value(value: Value) -> Result<Self, serde_json::Error> {
        let mut config: Self = serde_json::from_value(value.clone())?;
        config.original_config = Some(value);
        Ok(config)
    }

    fn source_value(&self) -> Value {
        self.original_config
            .clone()
            .unwrap_or_else(|| serde_json::to_value(self).unwrap_or(Value::Null))
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One HTTP request after hook input projection and environment interpolation.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpHookRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Response body abstraction. The runner consumes it only after the fetch
/// timeout/cancellation window has ended, matching the TypeScript behavior.
pub trait HttpHookResponseBody: Send {
    fn read_all<'a>(&'a mut self) -> HttpHookFuture<'a, Result<Vec<u8>, String>>;
}

/// Response from the transport. Header names are treated case-insensitively
/// when the runner looks up `content-type`.
pub struct HttpHookResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    body: Option<Box<dyn HttpHookResponseBody>>,
}

impl HttpHookResponse {
    pub fn new(
        status: u16,
        headers: Vec<(String, String)>,
        body: impl HttpHookResponseBody + 'static,
    ) -> Self {
        Self {
            status,
            headers,
            body: Some(Box::new(body)),
        }
    }

    /// Build a buffered response, useful for host adapters that already own
    /// the response bytes.
    pub fn from_bytes(status: u16, headers: Vec<(String, String)>, bytes: Vec<u8>) -> Self {
        Self::new(status, headers, BufferedResponseBody(Some(bytes)))
    }

    fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    async fn read_body(&mut self) -> Result<Vec<u8>, String> {
        let Some(mut body) = self.body.take() else {
            return Ok(Vec::new());
        };
        body.read_all().await
    }
}

struct BufferedResponseBody(Option<Vec<u8>>);

impl HttpHookResponseBody for BufferedResponseBody {
    fn read_all<'a>(&'a mut self) -> HttpHookFuture<'a, Result<Vec<u8>, String>> {
        Box::pin(async move { Ok(self.0.take().unwrap_or_default()) })
    }
}

/// Injectable transport. `send` resolves once status and headers are
/// available; response-body reads remain on [`HttpHookResponseBody`].
pub trait HttpHookTransport: Send + Sync {
    fn send<'a>(
        &'a self,
        request: HttpHookRequest,
    ) -> HttpHookFuture<'a, Result<HttpHookResponse, String>>;
}

/// Injectable async DNS resolver used for the preflight SSRF check.
pub trait HttpHookDnsResolver: Send + Sync {
    fn resolve_all<'a>(
        &'a self,
        hostname: &'a str,
    ) -> HttpHookFuture<'a, Result<Vec<IpAddr>, String>>;
}

/// Tokio DNS implementation used by the default runner.
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioHttpHookDnsResolver;

impl HttpHookDnsResolver for TokioHttpHookDnsResolver {
    fn resolve_all<'a>(
        &'a self,
        hostname: &'a str,
    ) -> HttpHookFuture<'a, Result<Vec<IpAddr>, String>> {
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((hostname, 0))
                .await
                .map_err(|error| error.to_string())?;
            let mut unique = Vec::new();
            for address in addresses {
                if !unique.contains(&address.ip()) {
                    unique.push(address.ip());
                }
            }
            Ok(unique)
        })
    }
}

/// Reqwest-backed production transport.
pub struct ReqwestHttpHookTransport {
    client: Client,
}

impl ReqwestHttpHookTransport {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub fn with_default_client() -> Self {
        Self::new(Client::new())
    }
}

impl HttpHookTransport for ReqwestHttpHookTransport {
    fn send<'a>(
        &'a self,
        request: HttpHookRequest,
    ) -> HttpHookFuture<'a, Result<HttpHookResponse, String>> {
        Box::pin(async move {
            let method =
                Method::from_bytes(request.method.as_bytes()).map_err(|error| error.to_string())?;
            let mut builder = self.client.request(method, &request.url);
            for (name, value) in request.headers {
                let name =
                    HeaderName::from_bytes(name.as_bytes()).map_err(|error| error.to_string())?;
                let value = HeaderValue::from_str(&value).map_err(|error| error.to_string())?;
                builder = builder.header(name, value);
            }
            let response = builder
                .body(request.body)
                .send()
                .await
                .map_err(|error| error.to_string())?;
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_owned(),
                        value.to_str().unwrap_or_default().to_owned(),
                    )
                })
                .collect();
            Ok(HttpHookResponse::new(
                status,
                headers,
                ReqwestResponseBody(Some(response)),
            ))
        })
    }
}

struct ReqwestResponseBody(Option<reqwest::Response>);

impl HttpHookResponseBody for ReqwestResponseBody {
    fn read_all<'a>(&'a mut self) -> HttpHookFuture<'a, Result<Vec<u8>, String>> {
        Box::pin(async move {
            let response = self
                .0
                .take()
                .ok_or_else(|| "HTTP hook response body was already consumed".to_owned())?;
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| error.to_string())
        })
    }
}

pub type StatusMessageCallback = Arc<dyn Fn(&str) + Send + Sync + 'static>;

/// Result of executing one HTTP hook. `error` is populated only for blocking
/// failures; transport errors, timeouts, aborts, and non-2xx statuses return a
/// successful non-blocking result with `{"continue": true}`.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpHookExecutionResult {
    pub hook_config: Value,
    pub event_name: HookEventName,
    pub success: bool,
    pub error: Option<String>,
    pub output: Option<Value>,
    pub duration_ms: u64,
}

/// HTTP hook runner with independently injectable DNS and HTTP boundaries.
pub struct HttpHookRunner {
    allow_private_network_hosts: bool,
    validator: RwLock<UrlValidator>,
    executed_once_hooks: Mutex<std::collections::HashSet<String>>,
    status_message_callback: RwLock<Option<StatusMessageCallback>>,
    transport: Arc<dyn HttpHookTransport>,
    resolver: Arc<dyn HttpHookDnsResolver>,
}

impl HttpHookRunner {
    pub fn new(
        allowed_urls: Option<Vec<String>>,
        allow_private_network_hosts: bool,
    ) -> Result<Self, regex::Error> {
        Self::with_components(
            allowed_urls,
            allow_private_network_hosts,
            Arc::new(ReqwestHttpHookTransport::with_default_client()),
            Arc::new(TokioHttpHookDnsResolver),
        )
    }

    pub fn with_components(
        allowed_urls: Option<Vec<String>>,
        allow_private_network_hosts: bool,
        transport: Arc<dyn HttpHookTransport>,
        resolver: Arc<dyn HttpHookDnsResolver>,
    ) -> Result<Self, regex::Error> {
        let validator = UrlValidator::new(
            allowed_urls.unwrap_or_default(),
            allow_private_network_hosts,
        )?;
        Ok(Self {
            allow_private_network_hosts,
            validator: RwLock::new(validator),
            executed_once_hooks: Mutex::new(std::collections::HashSet::new()),
            status_message_callback: RwLock::new(None),
            transport,
            resolver,
        })
    }

    pub fn set_status_message_callback<F>(&self, callback: F)
    where
        F: Fn(&str) + Send + Sync + 'static,
    {
        *self
            .status_message_callback
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(callback));
    }

    pub fn clear_status_message_callback(&self) {
        *self
            .status_message_callback
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Execute a hook. An already-cancelled external token is a blocking
    /// failure, matching the source's preflight abort result. Cancellation or
    /// timeout after the request starts is non-blocking.
    pub async fn execute(
        &self,
        hook_config: &HttpHookConfig,
        event_name: HookEventName,
        input: &Value,
        signal: Option<&CancellationToken>,
    ) -> HttpHookExecutionResult {
        let started = Instant::now();
        let hook_id = hook_config
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
            .unwrap_or(&hook_config.url)
            .to_owned();
        let source_config = hook_config.source_value();

        if signal.is_some_and(CancellationToken::is_cancelled) {
            return HttpHookExecutionResult {
                hook_config: source_config,
                event_name,
                success: false,
                error: Some(format!(
                    "HTTP hook execution cancelled (aborted): {hook_id}"
                )),
                output: None,
                duration_ms: 0,
            };
        }

        if hook_config.once {
            let once_key = format!("{}:{}", hook_config.url, event_name_text(event_name));
            let mut executed = self
                .executed_once_hooks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !executed.insert(once_key) {
                return HttpHookExecutionResult {
                    hook_config: source_config,
                    event_name,
                    success: true,
                    error: None,
                    output: Some(continue_output()),
                    duration_ms: 0,
                };
            }
        }

        if let Some(status_message) = hook_config
            .status_message
            .as_deref()
            .filter(|message| !message.is_empty())
        {
            if let Some(callback) = self
                .status_message_callback
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
            {
                callback(status_message);
            }
        }

        let allowed_env_vars = hook_config
            .allowed_env_vars
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let environment = allowed_env_vars
            .iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| ((*name).to_owned(), value))
            })
            .collect::<HashMap<_, _>>();
        let url = interpolate_url(&hook_config.url, &allowed_env_vars, &environment);

        let validation = self
            .validator
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .validate(&url);
        if !validation.allowed {
            let reason = validation
                .reason
                .unwrap_or_else(|| "URL was rejected".to_owned());
            return failed_result(
                source_config,
                event_name,
                format!("URL validation failed: {reason}"),
                started,
            );
        }

        let parsed_url = match Url::parse(&url) {
            Ok(parsed) => parsed,
            Err(error) => {
                return failed_result(
                    source_config,
                    event_name,
                    format!("URL validation failed: {error}"),
                    started,
                );
            }
        };
        if let Err(error) = self.validate_resolved_host(&parsed_url).await {
            return failed_result(source_config, event_name, error, started);
        }

        let header_pairs = hook_config
            .headers
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let interpolated = interpolate_headers(
            &merge_default_headers(header_pairs),
            &allowed_env_vars,
            &environment,
        );
        let body = match make_request_body(input, event_name) {
            Ok(body) => body,
            Err(error) => {
                return failed_result(
                    source_config,
                    event_name,
                    format!("Failed to serialize HTTP hook request: {error}"),
                    started,
                );
            }
        };
        let request = HttpHookRequest {
            method: "POST".to_owned(),
            url,
            headers: interpolated,
            body,
        };

        let combined =
            combine_cancellation_tokens([signal], timeout_for_config(hook_config.timeout));
        if combined.token.is_cancelled() {
            combined.cleanup.cleanup();
            return non_blocking_result(source_config, event_name, started);
        }

        let send_future = self.transport.send(request);
        tokio::pin!(send_future);
        let send_result = tokio::select! {
            biased;
            _reason = combined.token.cancelled() => {
                combined.cleanup.cleanup();
                return non_blocking_result(source_config, event_name, started);
            }
            result = &mut send_future => result,
        };
        combined.cleanup.cleanup();

        let mut response = match send_result {
            Ok(response) => response,
            Err(_error) => return non_blocking_result(source_config, event_name, started),
        };
        // The source captures duration as soon as fetch resolves, before it
        // consumes or parses the response body.
        let response_duration_ms = elapsed_ms(started);
        if !response.ok() {
            return HttpHookExecutionResult {
                hook_config: source_config,
                event_name,
                success: true,
                error: None,
                output: Some(continue_output()),
                duration_ms: response_duration_ms,
            };
        }

        let output = match parse_response(&mut response, event_name).await {
            Ok(output) => output,
            Err(_error) => return non_blocking_result(source_config, event_name, started),
        };
        HttpHookExecutionResult {
            hook_config: source_config,
            event_name,
            success: true,
            error: None,
            output: Some(output),
            duration_ms: response_duration_ms,
        }
    }

    /// Clear the once set for a new session or host-level reset.
    pub fn reset_once_hooks(&self) {
        self.executed_once_hooks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// Replace URL allowlist patterns. Invalid regular expressions return an
    /// error, matching constructor validation in the TypeScript validator.
    pub fn update_allowed_urls(&self, allowed_urls: Vec<String>) -> Result<(), regex::Error> {
        let validator = UrlValidator::new(allowed_urls, self.allow_private_network_hosts)?;
        *self
            .validator
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = validator;
        Ok(())
    }

    async fn validate_resolved_host(&self, parsed_url: &Url) -> Result<(), String> {
        let Some(host) = parsed_url.host_str() else {
            return Ok(());
        };
        let hostname = host
            .strip_prefix('[')
            .and_then(|value| value.strip_suffix(']'))
            .unwrap_or(host);

        if is_metadata_address(hostname) {
            return Err(format!(
                "HTTP hook blocked: {hostname} is a cloud metadata endpoint"
            ));
        }
        if hostname.parse::<IpAddr>().is_ok() {
            if !self.allow_private_network_hosts && is_blocked_address(hostname) {
                return Err(format!(
                    "HTTP hook blocked: {hostname} is in a private/link-local range"
                ));
            }
            return Ok(());
        }

        // TypeScript intentionally lets fetch handle DNS lookup errors. Keep
        // that behavior; successful resolutions still validate every answer.
        let Ok(addresses) = self.resolver.resolve_all(hostname).await else {
            return Ok(());
        };
        for address in addresses {
            let address = address.to_string();
            if is_metadata_address(&address) {
                return Err(format!(
                    "HTTP hook blocked: {hostname} resolves to {address} (cloud metadata endpoint)"
                ));
            }
            if !self.allow_private_network_hosts && is_blocked_address(&address) {
                return Err(format!(
                    "HTTP hook blocked: {hostname} resolves to {address} (private/link-local). Loopback (127.0.0.1, ::1) is allowed."
                ));
            }
        }
        Ok(())
    }
}

fn merge_default_headers(headers: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut merged = IndexMap::from([("Content-Type".to_owned(), "application/json".to_owned())]);
    for (name, value) in headers {
        // This follows the source object spread: an identically-spelled key
        // replaces the default while a differently-cased spelling remains a
        // separate entry and is normalized by the HTTP implementation.
        merged.insert(name, value);
    }
    merged.into_iter().collect()
}

fn make_request_body(
    input: &Value,
    event_name: HookEventName,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut object = match input {
        Value::Object(object) => object.clone(),
        _ => Map::new(),
    };
    object.insert("hook_event_name".to_owned(), event_name_value(event_name));
    serde_json::to_vec(&Value::Object(object))
}

fn event_name_value(event_name: HookEventName) -> Value {
    serde_json::to_value(event_name).unwrap_or(Value::Null)
}

fn event_name_text(event_name: HookEventName) -> String {
    event_name_value(event_name)
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

fn continue_output() -> Value {
    serde_json::json!({ "continue": true })
}

fn non_blocking_result(
    hook_config: Value,
    event_name: HookEventName,
    started: Instant,
) -> HttpHookExecutionResult {
    HttpHookExecutionResult {
        hook_config,
        event_name,
        success: true,
        error: None,
        output: Some(continue_output()),
        duration_ms: elapsed_ms(started),
    }
}

fn failed_result(
    hook_config: Value,
    event_name: HookEventName,
    error: String,
    started: Instant,
) -> HttpHookExecutionResult {
    HttpHookExecutionResult {
        hook_config,
        event_name,
        success: false,
        error: Some(error),
        output: None,
        duration_ms: elapsed_ms(started),
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

fn timeout_for_config(timeout_seconds: Option<f64>) -> Option<Duration> {
    let seconds = timeout_seconds
        .filter(|seconds| !seconds.is_nan() && *seconds != 0.0)
        .unwrap_or(DEFAULT_HTTP_TIMEOUT.as_secs_f64());
    let millis = seconds * 1_000.0;
    if millis <= 0.0 || millis.is_nan() {
        return None;
    }
    // Node clamps timer values above 2^31-1 ms and positive sub-ms values to
    // 1 ms. Preserve that behavior instead of overflowing Duration conversion.
    if !millis.is_finite() || millis > f64::from(i32::MAX) {
        return Some(Duration::from_millis(1));
    }
    Some(Duration::from_millis((millis.floor() as u64).max(1)))
}

async fn parse_response(
    response: &mut HttpHookResponse,
    event_name: HookEventName,
) -> Result<Value, String> {
    let content_type = response.header("content-type").unwrap_or_default();
    if content_type.contains("application/json") {
        // `response.json()` is inside a local try/catch in the source, so a
        // body read failure here falls back to an empty continue output.
        let Ok(bytes) = response.read_body().await else {
            return Ok(continue_output());
        };
        let text = decode_response_text(&bytes);
        let Ok(json) = serde_json::from_str::<Value>(&text) else {
            return Ok(continue_output());
        };
        return Ok(normalize_output(json, event_name).unwrap_or_else(continue_output));
    }

    let bytes = response.read_body().await?;
    let decoded = decode_response_text(&bytes);
    let text = trim_javascript_whitespace(&decoded);
    if text.is_empty() {
        return Ok(continue_output());
    }
    Ok(serde_json::json!({
        "continue": true,
        "systemMessage": truncate_output(text),
    }))
}

fn decode_response_text(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.strip_prefix('\u{feff}').unwrap_or(&text).to_owned()
}

fn trim_javascript_whitespace(value: &str) -> &str {
    value.trim_matches(is_javascript_whitespace)
}

fn is_javascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn normalize_output(json: Value, event_name: HookEventName) -> Option<Value> {
    let Value::Object(object) = json else {
        // Arrays are objects in JavaScript and normalize to an empty HookOutput;
        // primitive and null values make the `in` checks throw and are caught
        // by parseResponse as an empty continue result.
        return if matches!(json, Value::Array(_)) {
            Some(Value::Object(Map::new()))
        } else {
            None
        };
    };
    let mut output = Map::new();
    if let Some(Value::Bool(value)) = object.get("continue") {
        output.insert("continue".to_owned(), Value::Bool(*value));
    }
    if let Some(Value::String(value)) = object.get("stopReason") {
        output.insert(
            "stopReason".to_owned(),
            Value::String(truncate_output(value)),
        );
    }
    if let Some(Value::Bool(value)) = object.get("suppressOutput") {
        output.insert("suppressOutput".to_owned(), Value::Bool(*value));
    }
    if let Some(Value::String(value)) = object.get("systemMessage") {
        output.insert(
            "systemMessage".to_owned(),
            Value::String(truncate_output(value)),
        );
    }
    if let Some(Value::String(value)) = object.get("decision") {
        output.insert("decision".to_owned(), Value::String(value.clone()));
    }
    if let Some(Value::String(value)) = object.get("reason") {
        output.insert("reason".to_owned(), Value::String(truncate_output(value)));
    }
    if let Some(Value::Object(hook_output)) = object.get("hookSpecificOutput") {
        let mut hook_output = hook_output.clone();
        if let Some(Value::String(additional_context)) = hook_output.get_mut("additionalContext") {
            *additional_context = truncate_output(additional_context);
        }
        hook_output
            .entry("hookEventName".to_owned())
            .or_insert_with(|| event_name_value(event_name));
        output.insert("hookSpecificOutput".to_owned(), Value::Object(hook_output));
    } else if let Some(Value::Array(hook_output)) = object.get("hookSpecificOutput") {
        // Arrays satisfy JavaScript's `typeof value === 'object'` check. A
        // named property added to an array is omitted by JSON serialization.
        output.insert(
            "hookSpecificOutput".to_owned(),
            Value::Array(hook_output.clone()),
        );
    }
    Some(Value::Object(output))
}

fn truncate_output(value: &str) -> String {
    let original_length = value.encode_utf16().count();
    if original_length <= MAX_OUTPUT_LENGTH {
        return value.to_owned();
    }
    let mut truncated = String::with_capacity(value.len().min(MAX_OUTPUT_LENGTH));
    let mut length = 0;
    for character in value.chars() {
        let units = character.len_utf16();
        if length + units > MAX_OUTPUT_LENGTH {
            break;
        }
        truncated.push(character);
        length += units;
    }
    format!(
        "{truncated}\n... [truncated, {} more characters]",
        original_length.saturating_sub(MAX_OUTPUT_LENGTH)
    )
}
