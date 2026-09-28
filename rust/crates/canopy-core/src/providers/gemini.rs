//! Native Google Gemini GenerateContent transport and Gemini-shaped request
//! preparation for the Rust agent runtime.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, RETRY_AFTER};
use reqwest::{Client, Response, Url};
use serde_json::{Map, Value, json};

use crate::providers::openai_compatible::{
    BoundedSseParser, MAX_REQUEST_BODY_BYTES, MAX_RESPONSE_BODY_BYTES, MAX_SSE_EVENT_BYTES,
    ProviderError, decode_sse_frame, estimated_json_size, insert_header, read_bounded_body,
    read_error_body,
};
use crate::utils::cancellation::CancellationToken;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STREAM_MAX_LIFETIME: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct GeminiProviderConfig {
    pub model: String,
    /// Defaults to Google's public Generative Language API. Custom base URLs
    /// may include a path prefix for gateways.
    pub base_url: String,
    pub api_key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub proxy: Option<String>,
    pub user_agent: Option<String>,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// `None` disables the per-read watchdog.
    pub stream_idle_timeout: Option<Duration>,
    /// `None` disables the total provider-stream lifetime cap.
    pub stream_max_lifetime: Option<Duration>,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_sse_event_bytes: usize,
}

impl Default for GeminiProviderConfig {
    fn default() -> Self {
        Self {
            model: String::new(),
            base_url: "https://generativelanguage.googleapis.com".to_owned(),
            api_key: None,
            headers: BTreeMap::new(),
            proxy: None,
            user_agent: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            stream_idle_timeout: Some(DEFAULT_STREAM_IDLE_TIMEOUT),
            stream_max_lifetime: Some(DEFAULT_STREAM_MAX_LIFETIME),
            max_request_body_bytes: MAX_REQUEST_BODY_BYTES,
            max_response_body_bytes: MAX_RESPONSE_BODY_BYTES,
            max_sse_event_bytes: MAX_SSE_EVENT_BYTES,
        }
    }
}

impl fmt::Debug for GeminiProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeminiProviderConfig")
            .field("model", &self.model)
            .field("base_url", &"[redacted]")
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("headers", &"[redacted]")
            .field("proxy", &self.proxy.as_ref().map(|_| "[redacted]"))
            .field("user_agent", &self.user_agent)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("stream_idle_timeout", &self.stream_idle_timeout)
            .field("stream_max_lifetime", &self.stream_max_lifetime)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("max_response_body_bytes", &self.max_response_body_bytes)
            .field("max_sse_event_bytes", &self.max_sse_event_bytes)
            .finish_non_exhaustive()
    }
}

pub struct GeminiNativeClient {
    client: Client,
    config: GeminiProviderConfig,
    request_limit: usize,
    response_limit: usize,
    event_limit: usize,
}

impl fmt::Debug for GeminiNativeClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeminiNativeClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GeminiNativeClient {
    pub fn new(config: GeminiProviderConfig) -> Result<Self, ProviderError> {
        let mut default_headers = HeaderMap::new();
        if let Some(user_agent) = &config.user_agent {
            insert_header(&mut default_headers, "user-agent", user_agent)?;
        }
        for (name, value) in &config.headers {
            insert_header(&mut default_headers, name, value)?;
        }
        if let Some(api_key) = config.api_key.as_deref().filter(|key| !key.is_empty()) {
            insert_header(&mut default_headers, "x-goog-api-key", api_key)?;
        }

        let mut builder = Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .default_headers(default_headers);
        if let Some(proxy) = &config.proxy {
            let proxy = reqwest::Proxy::all(proxy).map_err(map_transport_error)?;
            builder = builder.proxy(proxy);
        }
        let client = builder.build().map_err(map_transport_error)?;
        Ok(Self {
            client,
            request_limit: config
                .max_request_body_bytes
                .clamp(1, MAX_REQUEST_BODY_BYTES),
            response_limit: config
                .max_response_body_bytes
                .clamp(1, MAX_RESPONSE_BODY_BYTES),
            event_limit: config.max_sse_event_bytes.clamp(1, MAX_SSE_EVENT_BYTES),
            config,
        })
    }

    pub fn config(&self) -> &GeminiProviderConfig {
        &self.config
    }

    pub async fn complete(
        &self,
        request: &Value,
        sampling_params: Option<&Value>,
        reasoning: Option<&Value>,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Value, ProviderError> {
        let model = request
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.trim().is_empty())
            .unwrap_or(&self.config.model);
        let body = build_gemini_request(request, sampling_params, reasoning)?;
        let response = self
            .send(model, "generateContent", body, false, cancellation)
            .await?;
        let read = read_bounded_body(response, self.response_limit);
        let bytes = if let Some(token) = cancellation {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(ProviderError::Cancelled),
                result = read => result?,
            }
        } else {
            read.await?
        };
        serde_json::from_slice(&bytes).map_err(ProviderError::InvalidResponseJson)
    }

    pub async fn stream_gemini(
        &self,
        request: &Value,
        sampling_params: Option<&Value>,
        reasoning: Option<&Value>,
        cancellation: Option<CancellationToken>,
    ) -> Result<GeminiEventStream, ProviderError> {
        let model = request
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.trim().is_empty())
            .unwrap_or(&self.config.model);
        let body = build_gemini_request(request, sampling_params, reasoning)?;
        let response = self
            .send(
                model,
                "streamGenerateContent",
                body,
                true,
                cancellation.as_ref(),
            )
            .await?;
        Ok(GeminiEventStream {
            response: Some(response),
            parser: BoundedSseParser::new(self.event_limit),
            idle_timeout: self
                .config
                .stream_idle_timeout
                .filter(|timeout| !timeout.is_zero()),
            max_lifetime: self
                .config
                .stream_max_lifetime
                .filter(|timeout| !timeout.is_zero()),
            upstream_waited: Duration::ZERO,
            cancellation,
            done: false,
        })
    }

    async fn send(
        &self,
        model: &str,
        method: &str,
        body: Value,
        stream: bool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Response, ProviderError> {
        if estimated_json_size(&body) > self.request_limit {
            return Err(ProviderError::RequestTooLarge {
                limit: self.request_limit,
            });
        }
        let endpoint = generate_content_endpoint(&self.config.base_url, model, method, stream)?;
        let body = serde_json::to_vec(&body).map_err(|_| ProviderError::InvalidRequestShape)?;
        let request = self
            .client
            .post(endpoint)
            .header(CONTENT_TYPE, "application/json")
            .header(
                ACCEPT,
                if stream {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            )
            .body(body);
        let response = if let Some(token) = cancellation {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(ProviderError::Cancelled),
                result = request.send() => result.map_err(map_transport_error)?,
            }
        } else {
            request.send().await.map_err(map_transport_error)?
        };
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(|value| value.chars().take(1_000).collect::<String>());
        let read_error = read_error_body(response);
        let (body, truncated) = if let Some(token) = cancellation {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(ProviderError::Cancelled),
                result = read_error => result,
            }
        } else {
            read_error.await
        };
        Err(ProviderError::HttpStatus {
            status,
            body: self.redact_secrets(body),
            truncated: if truncated { " (body truncated)" } else { "" },
            retry_after,
        })
    }

    fn redact_secrets(&self, mut body: String) -> String {
        if let Some(api_key) = self.config.api_key.as_deref().filter(|key| !key.is_empty()) {
            body = body.replace(api_key, "[redacted]");
        }
        for (name, value) in &self.config.headers {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "authorization" | "x-goog-api-key" | "x-api-key" | "api-key"
            ) && !value.is_empty()
            {
                body = body.replace(value, "[redacted]");
            }
        }
        body
    }
}

pub struct GeminiEventStream {
    response: Option<Response>,
    parser: BoundedSseParser,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    upstream_waited: Duration,
    cancellation: Option<CancellationToken>,
    done: bool,
}

impl fmt::Debug for GeminiEventStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeminiEventStream")
            .field("idle_timeout", &self.idle_timeout)
            .field("max_lifetime", &self.max_lifetime)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl GeminiEventStream {
    pub async fn next_chunk(&mut self) -> Result<Option<Value>, ProviderError> {
        if self.done {
            return Ok(None);
        }
        loop {
            if let Some(frame) = self.parser.next_frame()? {
                if let Some(event) = decode_sse_frame(frame)? {
                    if event.data == Value::String("[DONE]".to_owned()) {
                        self.done = true;
                        self.response = None;
                        return Ok(None);
                    }
                    return Ok(Some(event.data));
                }
                continue;
            }
            let Some(response) = self.response.as_mut() else {
                self.done = true;
                return Ok(None);
            };
            if self.done {
                return Ok(None);
            }

            let remaining_lifetime = self
                .max_lifetime
                .map(|limit| limit.saturating_sub(self.upstream_waited));
            if remaining_lifetime == Some(Duration::ZERO) {
                self.done = true;
                return Err(ProviderError::StreamLifetimeExceeded(
                    self.max_lifetime.unwrap_or_default(),
                ));
            }
            let watchdog = match (self.idle_timeout, remaining_lifetime) {
                (Some(idle), Some(lifetime)) => Some(idle.min(lifetime)),
                (Some(idle), None) => Some(idle),
                (None, Some(lifetime)) => Some(lifetime),
                (None, None) => None,
            };
            let wait_started = Instant::now();
            let result = if let Some(token) = self.cancellation.as_ref() {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        self.done = true;
                        return Err(ProviderError::Cancelled);
                    }
                    result = read_next_chunk(response, watchdog) => result,
                }
            } else {
                read_next_chunk(response, watchdog).await
            };
            self.upstream_waited = self.upstream_waited.saturating_add(wait_started.elapsed());
            let chunk = match result {
                Ok(chunk) => chunk,
                Err(NextChunkError::TimedOut) => {
                    self.done = true;
                    if let Some(limit) = remaining_lifetime
                        && watchdog == Some(limit)
                    {
                        return Err(ProviderError::StreamLifetimeExceeded(
                            self.max_lifetime.unwrap_or_default(),
                        ));
                    }
                    return Err(ProviderError::StreamIdleTimeout(
                        self.idle_timeout.unwrap_or_default(),
                    ));
                }
                Err(NextChunkError::Transport(error)) => {
                    self.done = true;
                    return Err(map_transport_error(error));
                }
            };
            if let Some(chunk) = chunk {
                self.parser.push_chunk(chunk);
            } else {
                self.response = None;
                self.done = true;
                if let Some(frame) = self.parser.finish()?
                    && let Some(event) = decode_sse_frame(frame)?
                {
                    if event.data == Value::String("[DONE]".to_owned()) {
                        return Ok(None);
                    }
                    return Ok(Some(event.data));
                }
                return Ok(None);
            }
        }
    }
}

enum NextChunkError {
    TimedOut,
    Transport(reqwest::Error),
}

async fn read_next_chunk(
    response: &mut Response,
    timeout: Option<Duration>,
) -> Result<Option<Bytes>, NextChunkError> {
    let read = response.chunk();
    let result = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| NextChunkError::TimedOut)?
    } else {
        read.await
    };
    result.map_err(NextChunkError::Transport)
}

/// Prepare the SDK-shaped request consumed by the native Gemini REST API.
/// The runtime request remains Gemini-shaped; only provider config flattening,
/// generation defaults, and unsupported tool-result media are adjusted here.
pub fn build_gemini_request(
    request: &Value,
    sampling_params: Option<&Value>,
    reasoning: Option<&Value>,
) -> Result<Value, ProviderError> {
    let mut request = request
        .as_object()
        .cloned()
        .ok_or(ProviderError::InvalidRequestShape)?;
    request.remove("model");
    let mut config = request
        .remove("config")
        .and_then(|config| config.as_object().cloned())
        .unwrap_or_default();
    if let Some(contents) = request.get_mut("contents") {
        strip_unsupported_content_fields(contents);
    }

    let mut generation = config
        .remove("generationConfig")
        .and_then(|config| config.as_object().cloned())
        .unwrap_or_default();
    for (wire_key, setting_key, request_key, default) in [
        ("temperature", "temperature", "temperature", Some(json!(1))),
        ("topP", "top_p", "topP", Some(json!(0.95))),
        ("topK", "top_k", "topK", Some(json!(64))),
        ("maxOutputTokens", "max_tokens", "maxOutputTokens", None),
        (
            "presencePenalty",
            "presence_penalty",
            "presencePenalty",
            None,
        ),
        (
            "frequencyPenalty",
            "frequency_penalty",
            "frequencyPenalty",
            None,
        ),
    ] {
        if let Some(value) =
            select_generation_value(sampling_params, setting_key, &config, request_key, default)
        {
            generation.insert(wire_key.to_owned(), value);
        }
        config.remove(request_key);
    }
    let request_thinking_config = config.remove("thinkingConfig");
    let thinking_config = build_thinking_config(reasoning, request_thinking_config.as_ref());
    generation.insert("thinkingConfig".to_owned(), thinking_config);

    const TOP_LEVEL_CONFIG_FIELDS: &[&str] = &[
        "systemInstruction",
        "tools",
        "toolConfig",
        "safetySettings",
        "cachedContent",
    ];
    for field in TOP_LEVEL_CONFIG_FIELDS {
        if let Some(value) = config.remove(*field) {
            request.insert((*field).to_owned(), value);
        }
    }
    generation.extend(config);
    if !generation.is_empty() {
        request.insert("generationConfig".to_owned(), Value::Object(generation));
    }
    Ok(Value::Object(request))
}

fn select_generation_value(
    sampling: Option<&Value>,
    sampling_key: &str,
    request_config: &Map<String, Value>,
    request_key: &str,
    default: Option<Value>,
) -> Option<Value> {
    sampling
        .and_then(Value::as_object)
        .and_then(|sampling| sampling.get(sampling_key))
        .or_else(|| request_config.get(request_key))
        .cloned()
        .or(default)
}

fn build_thinking_config(reasoning: Option<&Value>, request: Option<&Value>) -> Value {
    match reasoning {
        Some(Value::Bool(false)) => json!({"includeThoughts":false}),
        Some(Value::Null) | None => request.cloned().unwrap_or_else(
            || json!({"includeThoughts":true,"thinkingLevel":"THINKING_LEVEL_UNSPECIFIED"}),
        ),
        Some(reasoning) => {
            let thinking_level = match reasoning
                .get("effort")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str()
            {
                "low" => "LOW",
                "medium" => "MEDIUM",
                "high" | "xhigh" | "max" => "HIGH",
                _ => "THINKING_LEVEL_UNSPECIFIED",
            };
            json!({"includeThoughts":true,"thinkingLevel":thinking_level})
        }
    }
}

fn strip_unsupported_content_fields(contents: &mut Value) {
    match contents {
        Value::Array(contents) => {
            for content in contents {
                strip_content_fields(content);
            }
        }
        Value::Object(_) => strip_content_fields(contents),
        _ => {}
    }
}

fn strip_content_fields(content: &mut Value) {
    let is_content = content
        .as_object()
        .is_some_and(|object| object.contains_key("role") || object.contains_key("parts"));
    if is_content {
        if let Some(parts) = content.get_mut("parts").and_then(Value::as_array_mut) {
            for part in parts {
                strip_part_fields(part);
            }
        }
    } else {
        strip_part_fields(content);
    }
}

fn strip_part_fields(part: &mut Value) {
    let Some(object) = part.as_object_mut() else {
        return;
    };
    for media_key in ["inlineData", "fileData"] {
        if let Some(media) = object.get_mut(media_key).and_then(Value::as_object_mut) {
            media.remove("displayName");
        }
    }
    if let Some(response_parts) = object
        .get_mut("functionResponse")
        .and_then(Value::as_object_mut)
        .and_then(|response| response.get_mut("parts"))
        .and_then(Value::as_array_mut)
    {
        for nested_part in response_parts {
            convert_unsupported_tool_media(nested_part);
            strip_part_fields(nested_part);
        }
    }
}

fn convert_unsupported_tool_media(part: &mut Value) {
    let inline_mime = part.pointer("/inlineData/mimeType").and_then(Value::as_str);
    let file_mime = part.pointer("/fileData/mimeType").and_then(Value::as_str);
    let (mime, display_name) = if let Some(mime) = inline_mime {
        (
            mime,
            part.pointer("/inlineData/displayName")
                .and_then(Value::as_str),
        )
    } else if let Some(mime) = file_mime {
        (
            mime,
            part.pointer("/fileData/displayName")
                .and_then(Value::as_str),
        )
    } else {
        return;
    };
    if mime.starts_with("audio/") || mime.starts_with("video/") {
        let suffix = display_name
            .filter(|name| !name.is_empty())
            .map(|name| format!(" ({name})"))
            .unwrap_or_default();
        *part = json!({"text":format!("Unsupported media type for Gemini: {mime}{suffix}.")});
    }
}

fn generate_content_endpoint(
    base_url: &str,
    model: &str,
    method: &str,
    stream: bool,
) -> Result<Url, ProviderError> {
    if model.trim().is_empty() {
        return Err(ProviderError::InvalidRequestShape);
    }
    let mut url = Url::parse(base_url).map_err(|_| ProviderError::InvalidBaseUrl)?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ProviderError::InvalidBaseUrl);
    }
    let path = url.path().trim_end_matches('/');
    let version_prefix = if path.ends_with("/v1beta") {
        path.to_owned()
    } else {
        format!("{path}/v1beta")
    };
    let model = model.strip_prefix("models/").unwrap_or(model);
    url.set_path(&format!("{version_prefix}/models/{model}:{method}"));
    if stream {
        url.query_pairs_mut().append_pair("alt", "sse");
    }
    Ok(url)
}

fn map_transport_error(error: reqwest::Error) -> ProviderError {
    let error = error.without_url();
    let message = error.to_string();
    ProviderError::Transport {
        source: error,
        message,
    }
}
