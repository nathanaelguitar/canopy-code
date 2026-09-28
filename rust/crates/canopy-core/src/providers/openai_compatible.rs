//! Bounded transport for OpenAI Chat Completions compatible services.
//!
//! This is the HTTP boundary used by OpenAI, OpenRouter, local OpenAI-compatible
//! servers, and the provider-specific request converters. Bodies and individual
//! SSE events have hard upper bounds so a malformed or hostile endpoint cannot
//! grow a response buffer for the lifetime of a session.

use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Response, StatusCode, Url};
use serde_json::Value;
use thiserror::Error;

pub const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;
pub const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STREAM_MAX_LIFETIME: Duration = Duration::from_secs(600);

#[derive(Clone)]
pub struct OpenAiCompatibleConfig {
    /// Provider base URL, such as `https://api.openai.com/v1`.
    pub base_url: String,
    /// API key. Debug output always redacts this value.
    pub api_key: Option<String>,
    /// Additional request headers. Debug output redacts the full map because it
    /// may contain credential material.
    pub headers: BTreeMap<String, String>,
    /// Optional HTTP proxy URL. When absent, reqwest's normal proxy environment
    /// handling is retained.
    pub proxy: Option<String>,
    pub user_agent: Option<String>,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// `None` disables the per-read idle watchdog.
    pub stream_idle_timeout: Option<Duration>,
    /// `None` disables the total stream lifetime cap.
    pub stream_max_lifetime: Option<Duration>,
    /// Effective limits are clamped to the public hard caps above.
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_sse_event_bytes: usize,
}

impl Default for OpenAiCompatibleConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.openai.com/v1".to_owned(),
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

impl fmt::Debug for OpenAiCompatibleConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompatibleConfig")
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
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider base URL must be an absolute HTTP or HTTPS URL")]
    InvalidBaseUrl,
    #[error("provider request URL could not be constructed")]
    InvalidRequestUrl,
    #[error("invalid provider HTTP header name: {name}")]
    InvalidHeaderName { name: String },
    #[error("invalid provider HTTP header value for {name}")]
    InvalidHeaderValue { name: String },
    #[error("provider request must be a JSON object")]
    InvalidRequestShape,
    #[error("provider request body exceeds the {limit}-byte limit")]
    RequestTooLarge { limit: usize },
    #[error("provider response body exceeds the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
    #[error("provider SSE event exceeds the {limit}-byte limit")]
    SseEventTooLarge { limit: usize },
    #[error("provider returned an invalid UTF-8 SSE event")]
    InvalidSseUtf8,
    #[error("provider returned a malformed JSON SSE event")]
    InvalidSseJson(#[source] serde_json::Error),
    #[error("provider returned a non-JSON response")]
    InvalidResponseJson(#[source] serde_json::Error),
    #[error("provider returned HTTP {status}: {body}{truncated}")]
    HttpStatus {
        status: StatusCode,
        body: String,
        truncated: &'static str,
        retry_after: Option<String>,
    },
    #[error("provider stream was idle longer than {0:?}")]
    StreamIdleTimeout(Duration),
    #[error("provider stream exceeded its {0:?} lifetime limit")]
    StreamLifetimeExceeded(Duration),
    #[error("provider request was cancelled")]
    Cancelled,
    #[error("provider request failed: {message}")]
    Transport {
        #[source]
        source: reqwest::Error,
        message: String,
    },
}

/// One decoded server-sent event. `data` is the JSON value from its `data:`
/// fields; OpenAI's `[DONE]` sentinel ends the stream and is not returned.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenAiSseEvent {
    pub event: Option<String>,
    pub data: Value,
}

pub struct OpenAiCompatibleClient {
    client: Client,
    config: OpenAiCompatibleConfig,
    endpoint: Url,
    request_limit: usize,
    response_limit: usize,
    event_limit: usize,
}

impl fmt::Debug for OpenAiCompatibleClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompatibleClient")
            .field("config", &self.config)
            .field("endpoint", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl OpenAiCompatibleClient {
    pub fn new(config: OpenAiCompatibleConfig) -> Result<Self, ProviderError> {
        let endpoint = chat_completions_endpoint(&config.base_url)?;
        let request_limit = config
            .max_request_body_bytes
            .clamp(1, MAX_REQUEST_BODY_BYTES);
        let response_limit = config
            .max_response_body_bytes
            .clamp(1, MAX_RESPONSE_BODY_BYTES);
        let event_limit = config.max_sse_event_bytes.clamp(1, MAX_SSE_EVENT_BYTES);

        let mut default_headers = HeaderMap::new();
        for (name, value) in &config.headers {
            insert_header(&mut default_headers, name, value)?;
        }
        if let Some(user_agent) = &config.user_agent {
            insert_header(&mut default_headers, "user-agent", user_agent)?;
        }

        let mut builder = Client::builder()
            .timeout(config.request_timeout)
            .connect_timeout(config.connect_timeout)
            .default_headers(default_headers);
        if let Some(proxy) = &config.proxy {
            let proxy = reqwest::Proxy::all(proxy).map_err(|error| {
                let error = error.without_url();
                ProviderError::Transport {
                    message: error.to_string(),
                    source: error,
                }
            })?;
            builder = builder.proxy(proxy);
        }
        let client = builder.build().map_err(|error| {
            let error = error.without_url();
            ProviderError::Transport {
                message: error.to_string(),
                source: error,
            }
        })?;

        Ok(Self {
            client,
            config,
            endpoint,
            request_limit,
            response_limit,
            event_limit,
        })
    }

    /// Send a non-streaming request, limiting both the serialized request and
    /// decoded response body before parsing JSON.
    pub async fn complete(&self, request: &Value) -> Result<Value, ProviderError> {
        let body = self.encode_request(request, false)?;
        let response = self.send(body, false).await?;
        let bytes = read_bounded_body(response, self.response_limit).await?;
        serde_json::from_slice(&bytes).map_err(ProviderError::InvalidResponseJson)
    }

    /// Start a streaming Chat Completions request. Each event is decoded on
    /// demand; no list of events or full response transcript is retained.
    pub async fn stream(&self, request: &Value) -> Result<OpenAiEventStream, ProviderError> {
        let body = self.encode_request(request, true)?;
        let response = self.send(body, true).await?;
        Ok(OpenAiEventStream {
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
            event_limit: self.event_limit,
            done: false,
        })
    }

    fn encode_request(&self, request: &Value, stream: bool) -> Result<Vec<u8>, ProviderError> {
        if estimated_json_size(request) > self.request_limit {
            return Err(ProviderError::RequestTooLarge {
                limit: self.request_limit,
            });
        }
        let mut request = request.clone();
        let object = request
            .as_object_mut()
            .ok_or(ProviderError::InvalidRequestShape)?;
        object.insert("stream".to_owned(), Value::Bool(stream));

        if estimated_json_size(&request) > self.request_limit {
            return Err(ProviderError::RequestTooLarge {
                limit: self.request_limit,
            });
        }
        serde_json::to_vec(&request).map_err(|_| ProviderError::InvalidRequestShape)
    }

    async fn send(&self, body: Vec<u8>, stream: bool) -> Result<Response, ProviderError> {
        let mut request = self
            .client
            .post(self.endpoint.clone())
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
        if let Some(api_key) = &self.config.api_key {
            request = request.header(AUTHORIZATION, format!("Bearer {api_key}"));
        }
        let response = request.send().await.map_err(map_transport_error)?;
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.chars().take(1_000).collect::<String>());
            let (body, truncated) = read_error_body(response).await;
            let body = redact_configured_secrets(body, &self.config);
            Err(ProviderError::HttpStatus {
                status,
                body,
                truncated: if truncated { " (body truncated)" } else { "" },
                retry_after,
            })
        }
    }
}

pub struct OpenAiEventStream {
    response: Option<Response>,
    parser: BoundedSseParser,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    /// Sum of time blocked waiting for provider transport chunks. Consumer
    /// pauses between `next_event` calls do not spend this budget.
    upstream_waited: Duration,
    event_limit: usize,
    done: bool,
}

impl fmt::Debug for OpenAiEventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiEventStream")
            .field("idle_timeout", &self.idle_timeout)
            .field("max_lifetime", &self.max_lifetime)
            .field("event_limit", &self.event_limit)
            .field("done", &self.done)
            .finish_non_exhaustive()
    }
}

impl OpenAiEventStream {
    pub async fn next_event(&mut self) -> Result<Option<OpenAiSseEvent>, ProviderError> {
        if self.done {
            return Ok(None);
        }

        loop {
            if let Some(frame) = self.parser.next_frame()? {
                let Some(event) = decode_sse_frame(frame)? else {
                    continue;
                };
                if event.data == Value::String("[DONE]".to_owned()) {
                    self.done = true;
                    self.response = None;
                    return Ok(None);
                }
                return Ok(Some(event));
            }

            let Some(response) = self.response.as_mut() else {
                self.done = true;
                return Ok(None);
            };

            let remaining_lifetime = self
                .max_lifetime
                .map(|max_lifetime| max_lifetime.saturating_sub(self.upstream_waited));
            if let Some(max_lifetime) = self.max_lifetime {
                if remaining_lifetime == Some(Duration::ZERO) {
                    self.done = true;
                    return Err(ProviderError::StreamLifetimeExceeded(max_lifetime));
                }
            }

            let wait_for_next_chunk = response.chunk();
            let wait_started = Instant::now();
            let result = match (self.idle_timeout, remaining_lifetime) {
                (Some(idle_timeout), Some(max_lifetime)) => {
                    let max_wait = idle_timeout.min(max_lifetime);
                    match tokio::time::timeout(max_wait, wait_for_next_chunk).await {
                        Ok(result) => result,
                        Err(_) if max_lifetime <= idle_timeout => {
                            self.done = true;
                            return Err(ProviderError::StreamLifetimeExceeded(max_lifetime));
                        }
                        Err(_) => {
                            self.done = true;
                            return Err(ProviderError::StreamIdleTimeout(idle_timeout));
                        }
                    }
                }
                (Some(idle_timeout), None) => {
                    match tokio::time::timeout(idle_timeout, wait_for_next_chunk).await {
                        Ok(result) => result,
                        Err(_) => {
                            self.done = true;
                            return Err(ProviderError::StreamIdleTimeout(idle_timeout));
                        }
                    }
                }
                (None, Some(max_lifetime)) => {
                    match tokio::time::timeout(max_lifetime, wait_for_next_chunk).await {
                        Ok(result) => result,
                        Err(_) => {
                            self.done = true;
                            return Err(ProviderError::StreamLifetimeExceeded(max_lifetime));
                        }
                    }
                }
                (None, None) => wait_for_next_chunk.await,
            }
            .map_err(map_transport_error);
            self.upstream_waited = self.upstream_waited.saturating_add(wait_started.elapsed());
            let result = result?;

            match result {
                Some(chunk) => self.parser.push_chunk(chunk),
                None => {
                    self.response = None;
                    self.done = true;
                    if let Some(frame) = self.parser.finish()? {
                        if let Some(event) = decode_sse_frame(frame)? {
                            if event.data == Value::String("[DONE]".to_owned()) {
                                return Ok(None);
                            }
                            return Ok(Some(event));
                        }
                    }
                    return Ok(None);
                }
            }
        }
    }
}

pub(super) struct BoundedSseParser {
    max_event_bytes: usize,
    chunk: Bytes,
    offset: usize,
    frame: Vec<u8>,
}

impl BoundedSseParser {
    pub(super) fn new(max_event_bytes: usize) -> Self {
        Self {
            max_event_bytes,
            chunk: Bytes::new(),
            offset: 0,
            frame: Vec::new(),
        }
    }

    pub(super) fn push_chunk(&mut self, chunk: Bytes) {
        self.chunk = chunk;
        self.offset = 0;
    }

    pub(super) fn next_frame(&mut self) -> Result<Option<Vec<u8>>, ProviderError> {
        while self.offset < self.chunk.len() {
            let byte = self.chunk[self.offset];
            self.offset += 1;
            self.frame.push(byte);

            if let Some(delimiter_len) = sse_delimiter_len(&self.frame) {
                self.frame.truncate(self.frame.len() - delimiter_len);
                return Ok(Some(std::mem::take(&mut self.frame)));
            }

            if self.frame.len() > self.max_event_bytes.saturating_add(4) {
                return Err(ProviderError::SseEventTooLarge {
                    limit: self.max_event_bytes,
                });
            }
        }
        Ok(None)
    }

    pub(super) fn finish(&mut self) -> Result<Option<Vec<u8>>, ProviderError> {
        if self.frame.len() > self.max_event_bytes {
            return Err(ProviderError::SseEventTooLarge {
                limit: self.max_event_bytes,
            });
        }
        if self.frame.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(&mut self.frame)))
        }
    }
}

fn sse_delimiter_len(frame: &[u8]) -> Option<usize> {
    [
        b"\r\n\r\n".as_slice(),
        b"\r\n\n",
        b"\n\r\n",
        b"\n\n",
        b"\r\r",
    ]
    .iter()
    .find_map(|delimiter| frame.ends_with(delimiter).then_some(delimiter.len()))
}

pub(super) fn decode_sse_frame(frame: Vec<u8>) -> Result<Option<OpenAiSseEvent>, ProviderError> {
    let frame = String::from_utf8(frame).map_err(|_| ProviderError::InvalidSseUtf8)?;
    let normalized = frame.replace("\r\n", "\n").replace('\r', "\n");
    let mut event_name = None;
    let mut data_lines = Vec::new();

    for line in normalized.split('\n') {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (field, mut value) = line.split_once(':').unwrap_or((line, ""));
        if let Some(without_space) = value.strip_prefix(' ') {
            value = without_space;
        }
        match field {
            "event" if !value.contains('\0') => event_name = Some(value.to_owned()),
            "data" => data_lines.push(value),
            _ => {}
        }
    }

    if data_lines.is_empty() {
        return Ok(None);
    }
    let data = data_lines.join("\n");
    if data == "[DONE]" {
        return Ok(Some(OpenAiSseEvent {
            event: event_name,
            data: Value::String(data),
        }));
    }
    let data = serde_json::from_str(&data).map_err(ProviderError::InvalidSseJson)?;
    Ok(Some(OpenAiSseEvent {
        event: event_name,
        data,
    }))
}

fn chat_completions_endpoint(base_url: &str) -> Result<Url, ProviderError> {
    let mut url = Url::parse(base_url).map_err(|_| ProviderError::InvalidBaseUrl)?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ProviderError::InvalidBaseUrl);
    }

    let path = url.path().trim_end_matches('/');
    let endpoint_path = if path.ends_with("/chat/completions") {
        path.to_owned()
    } else if path.ends_with("/v1") {
        format!("{path}/chat/completions")
    } else {
        format!("{path}/v1/chat/completions")
    };
    url.set_path(&endpoint_path);
    Ok(url)
}

pub(super) fn insert_header(
    headers: &mut HeaderMap,
    name: &str,
    value: &str,
) -> Result<(), ProviderError> {
    let name =
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| ProviderError::InvalidHeaderName {
            name: name.to_owned(),
        })?;
    let value = HeaderValue::from_str(value).map_err(|_| ProviderError::InvalidHeaderValue {
        name: name.as_str().to_owned(),
    })?;
    headers.insert(name, value);
    Ok(())
}

pub(super) fn estimated_json_size(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(true) => 4,
        Value::Bool(false) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(value) => estimated_string_size(value),
        Value::Array(values) => values.iter().fold(2usize, |total, value| {
            total
                .saturating_add(1)
                .saturating_add(estimated_json_size(value))
        }),
        Value::Object(values) => values.iter().fold(2usize, |total, (key, value)| {
            total
                .saturating_add(1)
                .saturating_add(estimated_string_size(key))
                .saturating_add(1)
                .saturating_add(estimated_json_size(value))
        }),
    }
}

fn estimated_string_size(value: &str) -> usize {
    value.chars().fold(2usize, |total, character| {
        total.saturating_add(match character {
            '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => 2,
            character if character <= '\u{001f}' => 6,
            character => character.len_utf8(),
        })
    })
}

pub(super) async fn read_bounded_body(
    mut response: Response,
    limit: usize,
) -> Result<Vec<u8>, ProviderError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ProviderError::ResponseTooLarge { limit });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(map_transport_error)? {
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(ProviderError::ResponseTooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) async fn read_error_body(mut response: Response) -> (String, bool) {
    let mut body = Vec::with_capacity(MAX_ERROR_BODY_BYTES.min(4096));
    let mut truncated = response
        .content_length()
        .is_some_and(|length| length > MAX_ERROR_BODY_BYTES as u64);
    while body.len() < MAX_ERROR_BODY_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = MAX_ERROR_BODY_BYTES.saturating_sub(body.len());
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    truncated = true;
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
    let body = String::from_utf8_lossy(&body).into_owned();
    (body, truncated)
}

fn map_transport_error(error: reqwest::Error) -> ProviderError {
    let error = error.without_url();
    let message = error.to_string();
    ProviderError::Transport {
        source: error,
        message,
    }
}

fn redact_configured_secrets(mut body: String, config: &OpenAiCompatibleConfig) -> String {
    if let Some(api_key) = &config.api_key {
        if !api_key.is_empty() {
            body = body.replace(api_key, "[redacted]");
        }
    }
    for (name, value) in &config.headers {
        let is_credential_header = matches!(
            name.to_ascii_lowercase().as_str(),
            "authorization" | "x-api-key" | "api-key" | "anthropic-api-key"
        );
        if is_credential_header && !value.is_empty() {
            body = body.replace(value, "[redacted]");
        }
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn spawn_http_server(
        status: &'static str,
        content_type: &'static str,
        extra_headers: &'static str,
        response_body: Vec<u8>,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test request");
            let mut request = Vec::new();
            let mut read_buffer = [0_u8; 4096];
            let mut expected_request_bytes = None;

            loop {
                let count = socket
                    .read(&mut read_buffer)
                    .await
                    .expect("read test request");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&read_buffer[..count]);

                if expected_request_bytes.is_none() {
                    if let Some(header_end) =
                        request.windows(4).position(|part| part == b"\r\n\r\n")
                    {
                        let header_text = String::from_utf8_lossy(&request[..header_end]);
                        let content_length = header_text
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find_map(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .unwrap_or_default();
                        expected_request_bytes = Some(header_end + 4 + content_length);
                    }
                }
                if expected_request_bytes.is_some_and(|expected| request.len() >= expected) {
                    break;
                }
            }

            let response_headers = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{extra_headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            socket
                .write_all(response_headers.as_bytes())
                .await
                .expect("write test response headers");
            socket
                .write_all(&response_body)
                .await
                .expect("write test response body");
            String::from_utf8_lossy(&request).into_owned()
        });
        (format!("http://{address}"), server)
    }

    #[test]
    fn builds_chat_completions_urls_without_losing_custom_prefixes() {
        assert_eq!(
            chat_completions_endpoint("https://api.example.test/v1/")
                .expect("valid endpoint")
                .as_str(),
            "https://api.example.test/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("http://localhost:1234/proxy")
                .expect("valid endpoint")
                .as_str(),
            "http://localhost:1234/proxy/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_endpoint("https://api.example.test/v1/chat/completions")
                .expect("valid endpoint")
                .as_str(),
            "https://api.example.test/v1/chat/completions"
        );
        assert!(matches!(
            chat_completions_endpoint("file:///tmp/provider"),
            Err(ProviderError::InvalidBaseUrl)
        ));
    }

    #[test]
    fn parses_sse_events_across_arbitrary_transport_chunks() {
        let mut parser = BoundedSseParser::new(256);
        parser.push_chunk(Bytes::from_static(b"event: chunk\r\ndata: {\"choices\":["));
        assert_eq!(parser.next_frame().expect("partial frame"), None);
        parser.push_chunk(Bytes::from_static(b"{}]}\r\n\r\ndata: [DONE]\n\n"));
        let first = parser
            .next_frame()
            .expect("complete frame")
            .expect("event frame");
        assert_eq!(
            decode_sse_frame(first).expect("valid SSE JSON"),
            Some(OpenAiSseEvent {
                event: Some("chunk".to_owned()),
                data: json!({"choices": [{}]}),
            })
        );
        let second = parser
            .next_frame()
            .expect("sentinel frame")
            .expect("sentinel");
        assert_eq!(
            decode_sse_frame(second).expect("done sentinel"),
            Some(OpenAiSseEvent {
                event: None,
                data: Value::String("[DONE]".to_owned()),
            })
        );
    }

    #[test]
    fn rejects_oversized_unterminated_sse_frame_before_retaining_more() {
        let mut parser = BoundedSseParser::new(4);
        parser.push_chunk(Bytes::from_static(b"data:123456789"));
        assert!(matches!(
            parser.next_frame(),
            Err(ProviderError::SseEventTooLarge { limit: 4 })
        ));
    }

    #[test]
    fn json_size_estimator_covers_escaping_and_nested_values() {
        let value = json!({"text": "a\n\u{0001}"});
        let encoded = serde_json::to_vec(&value).expect("serializes");
        assert!(estimated_json_size(&value) >= encoded.len());
        assert!(estimated_json_size(&json!([true, null, 1])) >= 12);
    }

    #[test]
    fn debug_output_redacts_credentials_and_endpoint() {
        let config = OpenAiCompatibleConfig {
            base_url: "https://user:password@provider.test/v1?secret=token".to_owned(),
            api_key: Some("secret-key".to_owned()),
            headers: BTreeMap::from([("authorization".to_owned(), "hidden".to_owned())]),
            ..OpenAiCompatibleConfig::default()
        };
        let output = format!("{config:?}");
        assert!(!output.contains("password"));
        assert!(!output.contains("secret-key"));
        assert!(!output.contains("hidden"));
        assert!(!output.contains("provider.test"));
    }

    #[tokio::test]
    async fn streams_openai_events_from_http_without_collecting_the_response() {
        let response =
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
        let (base_url, server) =
            spawn_http_server("200 OK", "text/event-stream", "", response.to_vec()).await;
        let client = OpenAiCompatibleClient::new(OpenAiCompatibleConfig {
            base_url: format!("{base_url}/v1"),
            api_key: Some("unit-test-key".to_owned()),
            ..OpenAiCompatibleConfig::default()
        })
        .expect("valid client config");

        let mut stream = client
            .stream(&json!({"model": "test-model", "messages": []}))
            .await
            .expect("start provider stream");
        assert_eq!(
            stream.next_event().await.expect("read provider event"),
            Some(OpenAiSseEvent {
                event: None,
                data: json!({"choices": [{"delta": {"content": "hi"}}]}),
            })
        );
        assert_eq!(stream.next_event().await.expect("read done"), None);

        let request = server.await.expect("test server completed");
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer unit-test-key")
        );
        assert!(request.contains("\"stream\":true"));
    }

    #[tokio::test]
    async fn stream_lifetime_budget_excludes_time_between_consumer_polls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let (release_second_event, wait_for_release) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept test request");
            let mut request = Vec::new();
            let mut read_buffer = [0_u8; 2048];
            let mut expected_request_bytes = None;
            loop {
                let count = socket
                    .read(&mut read_buffer)
                    .await
                    .expect("read test request");
                if count == 0 {
                    break;
                }
                request.extend_from_slice(&read_buffer[..count]);
                if expected_request_bytes.is_none() {
                    if let Some(header_end) =
                        request.windows(4).position(|part| part == b"\r\n\r\n")
                    {
                        let header_text = String::from_utf8_lossy(&request[..header_end]);
                        let content_length = header_text
                            .lines()
                            .filter_map(|line| line.split_once(':'))
                            .find_map(|(name, value)| {
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().ok())
                                    .flatten()
                            })
                            .unwrap_or_default();
                        expected_request_bytes = Some(header_end + 4 + content_length);
                    }
                }
                if expected_request_bytes.is_some_and(|expected| request.len() >= expected) {
                    break;
                }
            }

            let first = "data: {\"choices\":[{\"delta\":{\"content\":\"first\"},\"finish_reason\":null}]}\n\n";
            let second = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";
            let response_length = first.len() + second.len();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {response_length}\r\nConnection: close\r\n\r\n"
            );
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("write response headers");
            socket
                .write_all(first.as_bytes())
                .await
                .expect("write first event");
            socket.flush().await.expect("flush first event");
            let _ = wait_for_release.await;
            socket
                .write_all(second.as_bytes())
                .await
                .expect("write second event");
        });

        let client = OpenAiCompatibleClient::new(OpenAiCompatibleConfig {
            base_url: format!("http://{address}/v1"),
            stream_idle_timeout: None,
            stream_max_lifetime: Some(Duration::from_millis(500)),
            ..Default::default()
        })
        .expect("create test client");
        let mut stream = client
            .stream(&json!({"model":"test","messages":[]}))
            .await
            .expect("start test stream");
        assert_eq!(
            stream
                .next_event()
                .await
                .expect("first event")
                .expect("event present")
                .data["choices"][0]["delta"]["content"],
            "first"
        );
        tokio::time::sleep(Duration::from_millis(600)).await;
        release_second_event.send(()).expect("release second event");
        assert_eq!(
            stream
                .next_event()
                .await
                .expect("second event after consumer pause")
                .expect("second event present")
                .data["choices"][0]["finish_reason"],
            "stop"
        );
        server.await.expect("test server task");
    }

    #[tokio::test]
    async fn error_responses_are_capped_and_credentials_are_redacted() {
        let body = format!(
            "provider echoed unit-test-key {}",
            "x".repeat(MAX_ERROR_BODY_BYTES)
        );
        let (base_url, server) = spawn_http_server(
            "401 Unauthorized",
            "application/json",
            "",
            body.into_bytes(),
        )
        .await;
        let client = OpenAiCompatibleClient::new(OpenAiCompatibleConfig {
            base_url: format!("{base_url}/v1"),
            api_key: Some("unit-test-key".to_owned()),
            ..OpenAiCompatibleConfig::default()
        })
        .expect("valid client config");

        let error = client
            .complete(&json!({"model": "test-model", "messages": []}))
            .await
            .expect_err("unauthorized provider call");
        match error {
            ProviderError::HttpStatus {
                body,
                truncated,
                status,
                retry_after,
            } => {
                assert_eq!(status, StatusCode::UNAUTHORIZED);
                assert!(body.len() <= MAX_ERROR_BODY_BYTES);
                assert!(!body.contains("unit-test-key"));
                assert_eq!(truncated, " (body truncated)");
                assert_eq!(retry_after, None);
            }
            other => panic!("unexpected provider error: {other}"),
        }
        let _ = server.await.expect("test server completed");
    }

    #[tokio::test]
    async fn status_errors_preserve_bounded_retry_after_metadata() {
        let (base_url, server) = spawn_http_server(
            "429 Too Many Requests",
            "application/json",
            "Retry-After: 2.5\r\n",
            br#"{"error":{"message":"slow down","code":429}}"#.to_vec(),
        )
        .await;
        let client = OpenAiCompatibleClient::new(OpenAiCompatibleConfig {
            base_url: format!("{base_url}/v1"),
            ..OpenAiCompatibleConfig::default()
        })
        .expect("valid client config");

        let error = client
            .complete(&json!({"model":"test-model","messages":[]}))
            .await
            .expect_err("rate limited provider call");
        assert!(matches!(
            error,
            ProviderError::HttpStatus {
                status: StatusCode::TOO_MANY_REQUESTS,
                retry_after: Some(ref value),
                ..
            } if value == "2.5"
        ));
        let _ = server.await.expect("test server completed");
    }

    #[tokio::test]
    async fn rejects_non_streaming_response_body_above_limit_from_header() {
        let (base_url, server) =
            spawn_http_server("200 OK", "application/json", "", b"12345".to_vec()).await;
        let client = OpenAiCompatibleClient::new(OpenAiCompatibleConfig {
            base_url: format!("{base_url}/v1"),
            max_response_body_bytes: 4,
            ..OpenAiCompatibleConfig::default()
        })
        .expect("valid client config");

        let error = client
            .complete(&json!({"model": "test-model", "messages": []}))
            .await
            .expect_err("oversized response");
        assert!(matches!(
            error,
            ProviderError::ResponseTooLarge { limit: 4 }
        ));
        let _ = server.await.expect("test server completed");
    }
}
