//! Native Anthropic Messages API request, transport, and response conversion.
//!
//! This provider module can be used independently. The agent runtime still
//! selects the OpenAI-compatible transport; provider selection remains a
//! separate migration step.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, RETRY_AFTER};
use reqwest::{Client, Response, Url};
use serde_json::{Map, Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::providers::openai_compatible::{
    BoundedSseParser, MAX_REQUEST_BODY_BYTES, MAX_RESPONSE_BODY_BYTES, MAX_SSE_EVENT_BYTES,
    OpenAiSseEvent, ProviderError, decode_sse_frame, estimated_json_size, insert_header,
    read_bounded_body, read_error_body,
};
use crate::providers::openai_request::normalize_mcp_tool_name;
use crate::providers::openai_response::GenAiUsageProvenance;
use crate::providers::retry_adapter::ProviderRetryAdapter;
use crate::providers::retry_error_classification::RetryErrorClassificationContext;
use crate::providers::schema::{SchemaComplianceMode, convert_schema};
use crate::providers::streaming_converter::{ConvertedStreamChunk, ToolCallPreparation};
use crate::token_limits::{
    TokenLimitType, default_output_ceiling, has_explicit_output_limit,
    parse_positive_integer_env_value, reconcile_max_tokens, token_limit,
};
use crate::turn::{Turn, TurnEvent, TurnResponseError};
use crate::utils::cancellation::CancellationToken;
use crate::utils::retry::{
    RetryHooks, RetryOptions, RetryWithBackoffError, is_unattended_mode, retry_with_backoff,
    system_time_ms, tokio_retry_sleep,
};

pub const ANTHROPIC_VERSION: &str = "2023-06-01";

#[derive(Clone)]
pub struct AnthropicProviderConfig {
    pub model: String,
    /// Defaults to Anthropic's Messages API. A custom URL may point at a
    /// compatible gateway; non-Anthropic hosts use Bearer auth by default.
    pub base_url: String,
    pub api_key: Option<String>,
    /// Overrides host-based auth selection for compatible gateways.
    pub bearer_auth: Option<bool>,
    pub headers: BTreeMap<String, String>,
    pub proxy: Option<String>,
    pub sampling_params: Option<Value>,
    /// `false` disables extended thinking; an object can supply `effort` and
    /// `budget_tokens`, matching the TypeScript content-generator config.
    pub reasoning: Option<Value>,
    pub schema_compliance: SchemaComplianceMode,
    pub enable_cache_control: bool,
    pub cache_retention_1h: bool,
    /// Per-cache-anchor override (`system`, `tool`, or `user.last`).
    pub cache_retention_by_block: BTreeMap<String, bool>,
    /// Add cross-session cache scope when the endpoint accepts the Anthropic
    /// prompt-caching-scope beta. Native Anthropic endpoints enable this
    /// automatically; gateways require this explicit opt-in.
    pub force_global_cache_scope: bool,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub stream_idle_timeout: Option<Duration>,
    pub stream_max_lifetime: Option<Duration>,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub max_sse_event_bytes: usize,
    pub cli_version: Option<String>,
}

impl Default for AnthropicProviderConfig {
    fn default() -> Self {
        Self {
            model: String::new(),
            base_url: "https://api.anthropic.com".to_owned(),
            api_key: None,
            bearer_auth: None,
            headers: BTreeMap::new(),
            proxy: None,
            sampling_params: None,
            reasoning: None,
            schema_compliance: SchemaComplianceMode::Auto,
            enable_cache_control: true,
            cache_retention_1h: false,
            cache_retention_by_block: BTreeMap::new(),
            force_global_cache_scope: false,
            request_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(15),
            stream_idle_timeout: Some(Duration::from_secs(60)),
            stream_max_lifetime: Some(Duration::from_secs(600)),
            max_request_body_bytes: MAX_REQUEST_BODY_BYTES,
            max_response_body_bytes: MAX_RESPONSE_BODY_BYTES,
            max_sse_event_bytes: MAX_SSE_EVENT_BYTES,
            cli_version: None,
        }
    }
}

impl fmt::Debug for AnthropicProviderConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicProviderConfig")
            .field("model", &self.model)
            .field("base_url", &"[redacted]")
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("bearer_auth", &self.bearer_auth)
            .field("headers", &"[redacted]")
            .field("proxy", &self.proxy.as_ref().map(|_| "[redacted]"))
            .field("sampling_params", &self.sampling_params)
            .field("reasoning", &self.reasoning)
            .field("schema_compliance", &self.schema_compliance)
            .field("enable_cache_control", &self.enable_cache_control)
            .field("cache_retention_1h", &self.cache_retention_1h)
            .field("cache_retention_by_block", &self.cache_retention_by_block)
            .field("force_global_cache_scope", &self.force_global_cache_scope)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("stream_idle_timeout", &self.stream_idle_timeout)
            .field("stream_max_lifetime", &self.stream_max_lifetime)
            .finish_non_exhaustive()
    }
}

pub struct AnthropicMessagesClient {
    client: Client,
    config: AnthropicProviderConfig,
    endpoint: Url,
    request_limit: usize,
    response_limit: usize,
    event_limit: usize,
    bearer_auth: bool,
}

impl Clone for AnthropicMessagesClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            config: self.config.clone(),
            endpoint: self.endpoint.clone(),
            request_limit: self.request_limit,
            response_limit: self.response_limit,
            event_limit: self.event_limit,
            bearer_auth: self.bearer_auth,
        }
    }
}

impl fmt::Debug for AnthropicMessagesClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicMessagesClient")
            .field("config", &self.config)
            .field("endpoint", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl AnthropicMessagesClient {
    pub fn new(config: AnthropicProviderConfig) -> Result<Self, ProviderError> {
        let endpoint = messages_endpoint(&config.base_url)?;
        let native_endpoint = is_anthropic_native_base_url(&config.base_url);
        let bearer_auth = config.bearer_auth.unwrap_or(!native_endpoint);
        let request_limit = config
            .max_request_body_bytes
            .clamp(1, MAX_REQUEST_BODY_BYTES);
        let response_limit = config
            .max_response_body_bytes
            .clamp(1, MAX_RESPONSE_BODY_BYTES);
        let event_limit = config.max_sse_event_bytes.clamp(1, MAX_SSE_EVENT_BYTES);

        let mut default_headers = HeaderMap::new();
        insert_header(&mut default_headers, "anthropic-version", ANTHROPIC_VERSION)?;
        insert_header(
            &mut default_headers,
            "user-agent",
            &user_agent(&config, bearer_auth),
        )?;
        if bearer_auth {
            insert_header(&mut default_headers, "x-app", "cli")?;
        }
        for (name, value) in &config.headers {
            if name.eq_ignore_ascii_case("anthropic-beta")
                || name.eq_ignore_ascii_case("authorization")
                || name.eq_ignore_ascii_case("x-api-key")
                || name.eq_ignore_ascii_case("anthropic-api-key")
            {
                continue;
            }
            insert_header(&mut default_headers, name, value)?;
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
            config,
            endpoint,
            request_limit,
            response_limit,
            event_limit,
            bearer_auth,
        })
    }

    /// Send a non-streaming Messages API request and cap the decoded body.
    pub async fn complete(&self, request: &Value) -> Result<Value, ProviderError> {
        self.complete_with_cancellation(request, None).await
    }

    pub async fn complete_with_cancellation(
        &self,
        request: &Value,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Value, ProviderError> {
        let body = self.encode_request(request, false)?;
        let response = self.send(body, request, false, cancellation).await?;
        let read_body = read_bounded_body(response, self.response_limit);
        let bytes = if let Some(cancellation) = cancellation {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(ProviderError::Cancelled),
                bytes = read_body => bytes?,
            }
        } else {
            read_body.await?
        };
        serde_json::from_slice(&bytes).map_err(ProviderError::InvalidResponseJson)
    }

    pub async fn complete_gemini(
        &self,
        request: &Value,
    ) -> Result<ConvertedGeminiResponse, ProviderError> {
        let wire_request = build_anthropic_request(request, &self.config)?;
        let response = self.complete(&wire_request).await?;
        Ok(convert_anthropic_message_to_gemini(&response))
    }

    /// Generate a Gemini-shaped response for a prompt-hook request while
    /// enforcing its deterministic sampling and disabled-reasoning policy.
    pub async fn complete_gemini_with_cancellation(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
    ) -> Result<ConvertedGeminiResponse, ProviderError> {
        let request_config = request.get("config").unwrap_or(&Value::Null);
        let mut config = self.config.clone();
        if request_config
            .pointer("/thinkingConfig/includeThoughts")
            .and_then(Value::as_bool)
            == Some(false)
        {
            config.reasoning = Some(Value::Bool(false));
        }
        if let Some(temperature) = request_config.get("temperature") {
            let sampling = config
                .sampling_params
                .get_or_insert_with(|| serde_json::json!({}));
            if let Some(sampling) = sampling.as_object_mut() {
                sampling.insert("temperature".to_owned(), temperature.clone());
            }
        }
        let wire_request = build_anthropic_request(request, &config)?;
        let response = self
            .complete_with_cancellation(&wire_request, Some(cancellation))
            .await?;
        Ok(convert_anthropic_message_to_gemini(&response))
    }

    /// Start a bounded, demand-driven Messages API SSE stream.
    pub async fn stream(
        &self,
        request: &Value,
        cancellation: Option<CancellationToken>,
    ) -> Result<AnthropicEventStream, ProviderError> {
        let body = self.encode_request(request, true)?;
        let response = self
            .send(body, request, true, cancellation.as_ref())
            .await?;
        Ok(AnthropicEventStream {
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

    /// Build and stream a Gemini-shaped request, preserving Anthropic's
    /// incremental text, thinking, tool-input, usage, and finish events.
    pub async fn stream_gemini(
        &self,
        request: &Value,
        cancellation: Option<CancellationToken>,
    ) -> Result<AnthropicGeminiEventStream, ProviderError> {
        let wire_request = build_anthropic_request(request, &self.config)?;
        let source = self.stream(&wire_request, cancellation.clone()).await?;
        Ok(AnthropicGeminiEventStream {
            source: Some(source),
            client: self.clone(),
            fallback_request: wire_request,
            cancellation,
            blocks: HashMap::new(),
            usage: AnthropicUsage::default(),
            message_id: None,
            model: None,
            finish_reason: None,
            message_start_usage_pending: false,
            any_usage_reported: false,
            assistant_payload: false,
            fallback_attempted: false,
            done: false,
        })
    }

    /// Retry only request setup failures, using Canopy's shared provider
    /// policy. Once the response stream has begun, its reader returns errors
    /// to the turn instead of replaying already-visible output.
    pub async fn stream_gemini_with_retries(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
    ) -> Result<AnthropicGeminiEventStream, RetryWithBackoffError<ProviderError>> {
        let now_ms = system_time_ms;
        let adapter =
            ProviderRetryAdapter::new(RetryErrorClassificationContext::default(), &now_ms);
        let classify_error = |error: &ProviderError| adapter.classify(error);
        let default_should_retry = |error: &ProviderError| adapter.should_retry_by_default(error);
        let sleep = |delay_ms| tokio_retry_sleep(delay_ms);
        let mut random = || {
            const MASK_53_BITS: u128 = (1_u128 << 53) - 1;
            (Uuid::new_v4().as_u128() & MASK_53_BITS) as f64 / (1_u64 << 53) as f64
        };
        let mut options = RetryOptions::<ProviderError, AnthropicGeminiEventStream>::new();
        options.persistent_mode = is_unattended_mode();
        options.signal = Some(cancellation);
        let mut hooks = RetryHooks {
            classify_error: &classify_error,
            default_should_retry: &default_should_retry,
            now_ms: &now_ms,
            random: &mut random,
            sleep: &sleep,
            logger: None,
        };
        let operation = |_attempt| async {
            self.stream_gemini(request, Some(cancellation.clone()))
                .await
        };
        retry_with_backoff(operation, &options, &mut hooks).await
    }

    pub async fn complete_gemini_with_retries(
        &self,
        request: &Value,
        cancellation: &CancellationToken,
    ) -> Result<ConvertedGeminiResponse, RetryWithBackoffError<ProviderError>> {
        let wire_request = build_anthropic_request(request, &self.config)
            .map_err(RetryWithBackoffError::Operation)?;
        let now_ms = system_time_ms;
        let adapter =
            ProviderRetryAdapter::new(RetryErrorClassificationContext::default(), &now_ms);
        let classify_error = |error: &ProviderError| adapter.classify(error);
        let default_should_retry = |error: &ProviderError| adapter.should_retry_by_default(error);
        let sleep = |delay_ms| tokio_retry_sleep(delay_ms);
        let mut random = || {
            const MASK_53_BITS: u128 = (1_u128 << 53) - 1;
            (Uuid::new_v4().as_u128() & MASK_53_BITS) as f64 / (1_u64 << 53) as f64
        };
        let mut options = RetryOptions::<ProviderError, ConvertedGeminiResponse>::new();
        options.persistent_mode = is_unattended_mode();
        options.signal = Some(cancellation);
        let mut hooks = RetryHooks {
            classify_error: &classify_error,
            default_should_retry: &default_should_retry,
            now_ms: &now_ms,
            random: &mut random,
            sleep: &sleep,
            logger: None,
        };
        let operation = |_attempt| async {
            let response = self
                .complete_with_cancellation(&wire_request, Some(cancellation))
                .await?;
            Ok::<_, ProviderError>(convert_anthropic_message_to_gemini(&response))
        };
        retry_with_backoff(operation, &options, &mut hooks).await
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
        if stream {
            object.insert("stream".to_owned(), Value::Bool(true));
        } else {
            object.remove("stream");
        }
        if estimated_json_size(&request) > self.request_limit {
            return Err(ProviderError::RequestTooLarge {
                limit: self.request_limit,
            });
        }
        serde_json::to_vec(&request).map_err(|_| ProviderError::InvalidRequestShape)
    }

    async fn send(
        &self,
        body: Vec<u8>,
        body_value: &Value,
        stream: bool,
        cancellation: Option<&CancellationToken>,
    ) -> Result<Response, ProviderError> {
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
            request = request.header(
                if self.bearer_auth {
                    "authorization"
                } else {
                    "x-api-key"
                },
                if self.bearer_auth {
                    format!("Bearer {api_key}")
                } else {
                    api_key.clone()
                },
            );
        }
        request = request.headers(self.per_request_headers(body_value)?);
        let send = request.send();
        let response = if let Some(cancellation) = cancellation {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(ProviderError::Cancelled),
                response = send => response.map_err(map_transport_error)?,
            }
        } else {
            send.await.map_err(map_transport_error)?
        };
        if response.status().is_success() {
            Ok(response)
        } else {
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.chars().take(1_000).collect::<String>());
            let read_error = read_error_body(response);
            let (body, truncated) = if let Some(cancellation) = cancellation {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => return Err(ProviderError::Cancelled),
                    result = read_error => result,
                }
            } else {
                read_error.await
            };
            let body = redact_secrets(body, &self.config);
            Err(ProviderError::HttpStatus {
                status,
                body,
                truncated: if truncated { " (body truncated)" } else { "" },
                retry_after,
            })
        }
    }

    fn per_request_headers(&self, request: &Value) -> Result<HeaderMap, ProviderError> {
        let mut headers = HeaderMap::new();
        let mut betas = Vec::new();
        for (name, value) in &self.config.headers {
            if name.eq_ignore_ascii_case("anthropic-beta") {
                betas.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_owned),
                );
            }
        }
        if request.get("thinking").is_some() {
            betas.push("interleaved-thinking-2025-05-14".to_owned());
        }
        if request.get("output_config").is_some() {
            betas.push("effort-2025-11-24".to_owned());
        }
        if contains_one_hour_cache_control(request) {
            betas.push("extended-cache-ttl-2025-04-11".to_owned());
        }
        if contains_global_cache_scope(request) {
            betas.push("prompt-caching-scope-2026-01-05".to_owned());
        }
        if !betas.is_empty() {
            let mut seen = HashSet::new();
            betas.retain(|beta| seen.insert(beta.clone()));
            insert_header(&mut headers, "anthropic-beta", &betas.join(","))?;
        }
        Ok(headers)
    }
}

pub struct AnthropicEventStream {
    response: Option<Response>,
    parser: BoundedSseParser,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    upstream_waited: Duration,
    cancellation: Option<CancellationToken>,
    done: bool,
}

impl AnthropicEventStream {
    pub async fn next_event(&mut self) -> Result<Option<OpenAiSseEvent>, ProviderError> {
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
                    return Ok(Some(event));
                }
                continue;
            }
            let Some(response) = self.response.as_mut() else {
                self.done = true;
                return Ok(None);
            };
            let remaining_lifetime = self
                .max_lifetime
                .map(|duration| duration.saturating_sub(self.upstream_waited));
            if let Some(max_lifetime) = self.max_lifetime {
                if remaining_lifetime == Some(Duration::ZERO) {
                    self.done = true;
                    return Err(ProviderError::StreamLifetimeExceeded(max_lifetime));
                }
            }
            let wait_started = Instant::now();
            let result = if let Some(cancellation) = &self.cancellation {
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(ProviderError::Cancelled),
                    result = wait_for_chunk(response, self.idle_timeout, remaining_lifetime) => result,
                }
            } else {
                wait_for_chunk(response, self.idle_timeout, remaining_lifetime).await
            };
            let result = match result {
                Ok(result) => result,
                Err(error) => {
                    self.done = true;
                    return Err(error);
                }
            };
            self.upstream_waited = self.upstream_waited.saturating_add(wait_started.elapsed());
            match result {
                Some(chunk) => self.parser.push_chunk(chunk),
                None => {
                    self.response = None;
                    self.done = true;
                    if let Some(frame) = self.parser.finish()? {
                        let Some(event) = decode_sse_frame(frame)? else {
                            return Ok(None);
                        };
                        if event.data == Value::String("[DONE]".to_owned()) {
                            return Ok(None);
                        }
                        return Ok(Some(event));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

async fn wait_for_chunk(
    response: &mut Response,
    idle_timeout: Option<Duration>,
    remaining_lifetime: Option<Duration>,
) -> Result<Option<Bytes>, ProviderError> {
    match (idle_timeout, remaining_lifetime) {
        (Some(idle), Some(lifetime)) => {
            let timeout = idle.min(lifetime);
            match tokio::time::timeout(timeout, response.chunk()).await {
                Ok(result) => result.map_err(map_transport_error),
                Err(_) if lifetime <= idle => Err(ProviderError::StreamLifetimeExceeded(lifetime)),
                Err(_) => Err(ProviderError::StreamIdleTimeout(idle)),
            }
        }
        (Some(idle), None) => tokio::time::timeout(idle, response.chunk())
            .await
            .map_err(|_| ProviderError::StreamIdleTimeout(idle))?
            .map_err(map_transport_error),
        (None, Some(lifetime)) => tokio::time::timeout(lifetime, response.chunk())
            .await
            .map_err(|_| ProviderError::StreamLifetimeExceeded(lifetime))?
            .map_err(map_transport_error),
        (None, None) => response.chunk().await.map_err(map_transport_error),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConvertedGeminiResponse {
    pub response: Value,
    pub usage_provenance: Option<GenAiUsageProvenance>,
}

pub fn convert_anthropic_message_to_gemini(message: &Value) -> ConvertedGeminiResponse {
    let mut parts = Vec::new();
    if let Some(blocks) = message.get("content").and_then(Value::as_array) {
        for block in blocks {
            let Some(kind) = block.get("type").and_then(Value::as_str) else {
                continue;
            };
            match kind {
                "text" => {
                    if let Some(text) = block
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|text| !text.is_empty())
                    {
                        parts.push(json!({"text":text}));
                    }
                }
                "tool_use" => {
                    let mut call = Map::new();
                    if let Some(id) = block.get("id").and_then(Value::as_str) {
                        call.insert("id".to_owned(), Value::String(id.to_owned()));
                    }
                    if let Some(name) = block.get("name").and_then(Value::as_str) {
                        call.insert("name".to_owned(), Value::String(name.to_owned()));
                    }
                    call.insert("args".to_owned(), safe_tool_args(block.get("input")));
                    parts.push(json!({"functionCall":Value::Object(call)}));
                }
                "thinking" => {
                    let thinking = block.get("thinking").and_then(Value::as_str).unwrap_or("");
                    let signature = block.get("signature").and_then(Value::as_str).unwrap_or("");
                    if !thinking.is_empty() || !signature.is_empty() {
                        parts.push(
                            json!({"text":thinking,"thought":true,"thoughtSignature":signature}),
                        );
                    }
                }
                "redacted_thinking" => parts.push(json!({"text":"","thought":true})),
                _ => {}
            }
        }
    }
    let stop_reason = message.get("stop_reason").and_then(Value::as_str);
    let mut candidate = json!({
        "content":{"parts":parts,"role":"model"},
        "index":0,
        "safetyRatings":[],
    });
    if let Some(reason) = finish_reason(stop_reason) {
        candidate["finishReason"] = Value::String(reason.to_owned());
    }
    let mut response = json!({
        "candidates":[candidate],
        "createTime":now_millis().to_string(),
        "promptFeedback":{"safetyRatings":[]},
    });
    if let Some(id) = message.get("id").and_then(Value::as_str) {
        response["responseId"] = Value::String(id.to_owned());
    }
    if let Some(model) = message.get("model").and_then(Value::as_str) {
        response["modelVersion"] = Value::String(model.to_owned());
    }
    let usage = message.get("usage").filter(|usage| usage.is_object());
    let usage_provenance = usage.map(|usage| {
        let input = usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cache_read = usage
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cache_creation = usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let output = usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let (metadata, provenance) = anthropic_usage_metadata(
            input,
            cache_read,
            cache_creation,
            Some(output),
            usage
                .get("cache_read_input_tokens")
                .and_then(Value::as_u64)
                .is_some(),
            usage
                .get("cache_creation_input_tokens")
                .and_then(Value::as_u64)
                .is_some(),
        );
        response["usageMetadata"] = metadata;
        provenance
    });
    ConvertedGeminiResponse {
        response,
        usage_provenance,
    }
}

pub fn build_anthropic_request(
    request: &Value,
    config: &AnthropicProviderConfig,
) -> Result<Value, ProviderError> {
    if !request.is_object() || config.model.trim().is_empty() {
        return Err(ProviderError::InvalidRequestShape);
    }
    let request_config = request.get("config").unwrap_or(&Value::Null);
    let mut ids = ToolIdResolver::default();
    let mut messages = Vec::new();
    let contents = request.get("contents").unwrap_or(&Value::Null);
    if let Some(contents) = contents.as_array() {
        for content in contents {
            convert_content(content, &mut messages, &mut ids);
        }
    } else if !contents.is_null() {
        convert_content(contents, &mut messages, &mut ids);
    }
    messages = normalize_anthropic_history(messages, request_config, config)?;
    let system_text = extract_system_text(request_config.get("systemInstruction"));
    let tools = convert_tools(
        request_config.get("tools"),
        config.schema_compliance,
        config.enable_cache_control,
        resolved_cache_retention(config, "tool"),
        use_global_cache_scope(config),
    );
    if config.enable_cache_control {
        add_user_cache_anchor(
            &mut messages,
            resolved_cache_retention(config, "user.last"),
            false,
        );
    }

    let max_tokens = max_output_tokens(request_config, config);
    let mut body = Map::new();
    body.insert("model".to_owned(), Value::String(config.model.clone()));
    body.insert("max_tokens".to_owned(), json!(max_tokens));
    body.insert("messages".to_owned(), Value::Array(messages));
    if !system_text.is_empty() {
        let mut system_block = json!({"type":"text","text":system_text});
        if config.enable_cache_control {
            system_block["cache_control"] = cache_control(
                resolved_cache_retention(config, "system"),
                use_global_cache_scope(config),
            );
        }
        body.insert("system".to_owned(), Value::Array(vec![system_block]));
    }
    if !tools.is_empty() {
        body.insert("tools".to_owned(), Value::Array(tools));
        if request_config
            .pointer("/toolConfig/functionCallingConfig/mode")
            .and_then(Value::as_str)
            == Some("ANY")
        {
            body.insert("tool_choice".to_owned(), json!({"type":"any"}));
        }
    }
    add_sampling_parameters(&mut body, request_config, config);
    add_thinking_parameters(&mut body, request_config, config, max_tokens);
    Ok(Value::Object(body))
}

fn add_sampling_parameters(
    body: &mut Map<String, Value>,
    request_config: &Value,
    config: &AnthropicProviderConfig,
) {
    let sampling = config.sampling_params.as_ref().and_then(Value::as_object);
    for (wire, request_key) in [
        ("temperature", "temperature"),
        ("top_p", "topP"),
        ("top_k", "topK"),
    ] {
        let value = sampling
            .and_then(|sampling| sampling.get(wire))
            .filter(|value| !value.is_null())
            .or_else(|| {
                request_config
                    .get(request_key)
                    .filter(|value| !value.is_null())
            });
        if let Some(value) = value {
            if wire != "temperature" || !model_rejects_temperature(&config.model) {
                body.insert(wire.to_owned(), value.clone());
            }
        } else if wire == "temperature" && !model_rejects_temperature(&config.model) {
            body.insert(wire.to_owned(), json!(1));
        }
    }
}

fn max_output_tokens(request_config: &Value, config: &AnthropicProviderConfig) -> u64 {
    let configured = config
        .sampling_params
        .as_ref()
        .and_then(|sampling| sampling.get("max_tokens"))
        .and_then(Value::as_f64);
    let requested = request_config
        .get("maxOutputTokens")
        .and_then(Value::as_f64);
    let explicit = reconcile_max_tokens(configured, requested)
        .or(configured)
        .or(requested);
    let is_known = has_explicit_output_limit(&config.model);
    if let Some(value) = explicit.filter(|value| value.is_finite() && *value > 0.0) {
        let value = value.floor() as u64;
        return if is_known {
            value.min(token_limit(&config.model, TokenLimitType::Output))
        } else {
            value
        };
    }
    parse_positive_integer_env_value(
        std::env::var("CANOPY_CODE_MAX_OUTPUT_TOKENS")
            .ok()
            .as_deref(),
    )
    .map(|value| {
        if is_known {
            value.min(token_limit(&config.model, TokenLimitType::Output))
        } else {
            value
        }
    })
    .unwrap_or_else(|| default_output_ceiling(&config.model))
}

fn add_thinking_parameters(
    body: &mut Map<String, Value>,
    request_config: &Value,
    config: &AnthropicProviderConfig,
    max_tokens: u64,
) {
    if !anthropic_thinking_enabled(request_config, config) {
        return;
    }
    let reasoning = config.reasoning.as_ref().and_then(Value::as_object);
    let effort = reasoning
        .and_then(|reasoning| reasoning.get("effort"))
        .and_then(Value::as_str);
    let adaptive = model_supports_adaptive_thinking(&config.model);
    let thinking = if adaptive {
        json!({"type":"adaptive","display":"summarized"})
    } else {
        let default_budget = match effort {
            Some("low") => 16_000,
            Some("high") => 64_000,
            Some("xhigh") => 96_000,
            Some("max") => 128_000,
            _ => 32_000,
        };
        let configured_budget = reasoning
            .and_then(|reasoning| reasoning.get("budget_tokens"))
            .and_then(Value::as_u64);
        let request_budget = request_config
            .pointer("/thinkingConfig/thinkingBudget")
            .and_then(Value::as_u64)
            .filter(|budget| *budget > 0);
        let budget = configured_budget
            .or(Some(default_budget))
            .map(|budget| request_budget.map_or(budget, |cap| budget.min(cap)))
            .unwrap_or(default_budget);
        json!({"type":"enabled","budget_tokens":budget.min(max_tokens.saturating_sub(1).max(1))})
    };
    body.insert("thinking".to_owned(), thinking);
    if let Some(effort) = effective_effort(&config.model, effort) {
        body.insert("output_config".to_owned(), json!({"effort":effort}));
    }
}

fn effective_effort(model: &str, effort: Option<&str>) -> Option<&'static str> {
    let effort = effort?;
    let version = claude_version(model)?;
    let five_x = version.0 >= 5;
    let supports_xhigh = five_x || (version.1 == "opus" && (version.0, version.2) >= (4, 7));
    let supports_max = five_x
        || ((version.1 == "opus" || version.1 == "sonnet") && (version.0, version.2) >= (4, 6));
    match effort {
        "low" | "medium" | "high" => Some(match effort {
            "low" => "low",
            "medium" => "medium",
            _ => "high",
        }),
        "xhigh" if supports_xhigh => Some("xhigh"),
        "max" if supports_max => Some("max"),
        "xhigh" | "max" => Some("high"),
        _ => None,
    }
}

fn claude_version(model: &str) -> Option<(u32, &str, u32)> {
    let lower = model.to_ascii_lowercase();
    let marker = lower.find("claude-")? + "claude-".len();
    let tail = &lower[marker..];
    let (family, version) = tail.split_once('-')?;
    let family = match family {
        "opus" => "opus",
        "sonnet" => "sonnet",
        "haiku" => "haiku",
        "fable" => "fable",
        "mythos" => "mythos",
        _ => return None,
    };
    let mut numbers = version.split(['-', '.']);
    let major = numbers.next()?.parse().ok()?;
    let minor = numbers
        .next()
        .and_then(|part| {
            let digits = part
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            (digits.len() <= 2).then(|| digits.parse().ok()).flatten()
        })
        .unwrap_or(0);
    Some((major, family, minor))
}

fn model_supports_adaptive_thinking(model: &str) -> bool {
    claude_version(model).is_some_and(|(major, _, minor)| major > 4 || (major == 4 && minor >= 6))
}

fn model_rejects_temperature(model: &str) -> bool {
    claude_version(model).is_some_and(|(major, _, minor)| major > 4 || (major == 4 && minor >= 8))
}

fn convert_content(content: &Value, messages: &mut Vec<Value>, ids: &mut ToolIdResolver) {
    if let Some(text) = content.as_str() {
        messages.push(json!({"role":"user","content":[{"type":"text","text":text}]}));
        return;
    }
    let Some(content_object) = content.as_object() else {
        return;
    };
    let role = if content_object.get("role").and_then(Value::as_str) == Some("model") {
        "assistant"
    } else {
        "user"
    };
    let Some(parts) = content_object.get("parts").and_then(Value::as_array) else {
        return;
    };
    let mut blocks = Vec::new();
    for part in parts {
        if let Some(text) = part.as_str() {
            blocks.push(json!({"type":"text","text":text}));
            continue;
        }
        if let Some(part) = part.as_object() {
            if role == "assistant" && part.get("thought").and_then(Value::as_bool) == Some(true) {
                let mut thinking = json!({"type":"thinking","thinking":part.get("text").and_then(Value::as_str).unwrap_or("")});
                if let Some(signature) = part.get("thoughtSignature").and_then(Value::as_str) {
                    thinking["signature"] = Value::String(signature.to_owned());
                }
                blocks.push(thinking);
            } else if let Some(text) = part
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                blocks.push(json!({"type":"text","text":text}));
            }
            if let Some(media) = media_block(part) {
                blocks.push(media);
            }
            if role == "assistant" {
                if let Some(call) = part.get("functionCall").and_then(Value::as_object) {
                    let id = ids.resolve(call.get("id").and_then(Value::as_str));
                    let name = normalize_mcp_tool_name(
                        call.get("name").and_then(Value::as_str).unwrap_or(""),
                    );
                    let input = call
                        .get("args")
                        .filter(|args| is_json_truthy(args))
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    blocks.push(json!({"type":"tool_use","id":id,"name":name,"input":input}));
                }
            }
            if role == "user" {
                if let Some(response) = part.get("functionResponse").and_then(Value::as_object) {
                    let response_value = response.get("response").unwrap_or(&Value::Null);
                    let result_text = function_response_text(response_value);
                    let media = response
                        .get("parts")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_object)
                        .filter_map(media_block)
                        .collect::<Vec<_>>();
                    let result_content = if media.is_empty() {
                        Value::String(result_text)
                    } else {
                        let mut blocks = Vec::with_capacity(media.len() + 1);
                        if !result_text.is_empty() {
                            blocks.push(json!({"type":"text","text":result_text}));
                        }
                        blocks.extend(media);
                        Value::Array(blocks)
                    };
                    let mut result = json!({
                        "type":"tool_result",
                        "tool_use_id":ids.resolve(response.get("id").and_then(Value::as_str)),
                        "content":result_content,
                    });
                    if response_value.get("error").is_some() {
                        result["is_error"] = Value::Bool(true);
                    }
                    blocks.push(result);
                }
            }
        }
    }
    if role == "user"
        && blocks
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
    {
        blocks.sort_by_key(|block| {
            usize::from(block.get("type").and_then(Value::as_str) != Some("tool_result"))
        });
    }
    if !blocks.is_empty() {
        messages.push(json!({"role":role,"content":blocks}));
    }
}

fn media_block(part: &Map<String, Value>) -> Option<Value> {
    let (mime, display_name, source) =
        if let Some(data) = part.get("inlineData").and_then(Value::as_object) {
            let mime = data.get("mimeType")?.as_str()?;
            let base64 = data.get("data")?.as_str()?;
            (
                mime,
                data.get("displayName").and_then(Value::as_str),
                Some(("base64", base64)),
            )
        } else if let Some(data) = part.get("fileData").and_then(Value::as_object) {
            let mime = data.get("mimeType")?.as_str()?;
            let url = data.get("fileUri")?.as_str()?;
            (
                mime,
                data.get("displayName").and_then(Value::as_str),
                Some(("url", url)),
            )
        } else {
            return None;
        };
    let (source_type, data) = source?;
    let display = display_name
        .map(|name| format!(" ({name})"))
        .unwrap_or_default();
    if matches!(
        mime,
        "image/jpeg" | "image/png" | "image/gif" | "image/webp"
    ) {
        Some(json!({"type":"image","source":image_source(source_type, mime, data)}))
    } else if mime == "application/pdf" {
        Some(json!({"type":"document","source":image_source(source_type, mime, data)}))
    } else {
        Some(
            json!({"type":"text","text":format!("Unsupported {} media type: {}{}.", if source_type == "url" { "file" } else { "inline" }, mime, display)}),
        )
    }
}

fn image_source(source_type: &str, mime: &str, data: &str) -> Value {
    if source_type == "url" {
        json!({"type":"url","url":data})
    } else {
        json!({"type":"base64","media_type":mime,"data":data})
    }
}

fn contains_one_hour_cache_control(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_one_hour_cache_control),
        Value::Object(values) => {
            values
                .get("cache_control")
                .and_then(|control| control.get("ttl"))
                .and_then(Value::as_str)
                == Some("1h")
                || values.values().any(contains_one_hour_cache_control)
        }
        _ => false,
    }
}

fn contains_global_cache_scope(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_global_cache_scope),
        Value::Object(values) => {
            values
                .get("cache_control")
                .and_then(|control| control.get("scope"))
                .and_then(Value::as_str)
                == Some("global")
                || values.values().any(contains_global_cache_scope)
        }
        _ => false,
    }
}

fn function_response_text(response: &Value) -> String {
    if response.is_null() {
        return String::new();
    }
    if let Some(text) = response.as_str() {
        return text.to_owned();
    }
    if let Some(output) = response.get("output").and_then(Value::as_str) {
        return output.to_owned();
    }
    if let Some(error) = response.get("error").and_then(Value::as_str) {
        return error.to_owned();
    }
    serde_json::to_string(response).unwrap_or_default()
}

fn normalize_anthropic_history(
    mut messages: Vec<Value>,
    request_config: &Value,
    config: &AnthropicProviderConfig,
) -> Result<Vec<Value>, ProviderError> {
    let deepseek = is_deepseek_provider(config);
    let thinking_enabled = anthropic_thinking_enabled(request_config, config);
    let deepseek_thinking_on = deepseek && thinking_enabled;
    let strip_assistant_thinking = deepseek && !thinking_enabled;

    if strip_assistant_thinking {
        strip_thinking_from_assistant_messages(&mut messages);
    }
    if deepseek_thinking_on {
        fill_missing_thinking_signatures(&mut messages);
        inject_empty_thinking_on_tool_use_turns(&mut messages);
    }

    messages = merge_consecutive_assistant_messages(messages);
    messages = clean_orphaned_tool_calls(messages);
    messages = merge_consecutive_assistant_messages(messages);

    let drop_unsigned_thinking = !deepseek
        && thinking_enabled
        && model_supports_adaptive_thinking(&config.model)
        && !is_anthropic_native_base_url(&config.base_url);
    if drop_unsigned_thinking {
        messages = drop_unsigned_thinking_from_assistant_messages(messages)?;
    }
    if !deepseek_thinking_on {
        messages = drop_empty_text_thinking_blocks(messages);
    }
    if strip_assistant_thinking {
        strip_thinking_from_assistant_messages(&mut messages);
    }

    messages = merge_consecutive_user_messages(messages);
    if model_supports_adaptive_thinking(&config.model) {
        strip_trailing_assistant_prefill(&mut messages);
    }
    Ok(messages)
}

fn anthropic_thinking_enabled(request_config: &Value, config: &AnthropicProviderConfig) -> bool {
    request_config
        .pointer("/thinkingConfig/includeThoughts")
        .and_then(Value::as_bool)
        != Some(false)
        && config.reasoning.as_ref() != Some(&Value::Bool(false))
}

fn is_deepseek_provider(config: &AnthropicProviderConfig) -> bool {
    let hostname_matches = Url::parse(&config.base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == "api.deepseek.com" || host.ends_with(".api.deepseek.com"));
    hostname_matches || config.model.to_ascii_lowercase().contains("deepseek")
}

fn merge_consecutive_assistant_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut merged = Vec::<Value>::new();
    for message in messages {
        let current_is_assistant = message.get("role").and_then(Value::as_str) == Some("assistant");
        let current_blocks = message.get("content").and_then(Value::as_array);
        if current_is_assistant
            && let Some(last) = merged.last_mut()
            && last.get("role").and_then(Value::as_str) == Some("assistant")
            && let (Some(last_blocks), Some(current_blocks)) = (
                last.get("content").and_then(Value::as_array),
                current_blocks,
            )
        {
            let is_thinking = |block: &Value| {
                matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("thinking" | "redacted_thinking")
                )
            };
            let last_blocks = last_blocks.clone();
            let mut combined = last_blocks
                .iter()
                .filter(|block| is_thinking(block))
                .chain(current_blocks.iter().filter(|block| is_thinking(block)))
                .chain(last_blocks.iter().filter(|block| !is_thinking(block)))
                .chain(current_blocks.iter().filter(|block| !is_thinking(block)))
                .cloned()
                .collect::<Vec<_>>();
            let mut seen_tool_use_ids = HashSet::<String>::new();
            combined.retain(|block| {
                if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                    return true;
                }
                let Some(id) = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                else {
                    return true;
                };
                seen_tool_use_ids.insert(id.to_owned())
            });
            last["content"] = Value::Array(combined);
            continue;
        }
        merged.push(message);
    }
    merged
}

fn clean_orphaned_tool_calls(messages: Vec<Value>) -> Vec<Value> {
    let mut valid_tool_use_blocks = HashSet::<(usize, usize)>::new();
    let mut valid_tool_result_blocks = HashSet::<(usize, usize)>::new();

    for (assistant_index, message) in messages.iter().enumerate() {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(blocks) = message.get("content").and_then(Value::as_array) else {
            continue;
        };
        let mut tool_use_blocks = HashMap::<String, usize>::new();
        for (block_index, block) in blocks.iter().enumerate() {
            if block.get("type").and_then(Value::as_str) == Some("tool_use")
                && let Some(id) = block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
            {
                tool_use_blocks.entry(id.to_owned()).or_insert(block_index);
            }
        }
        if tool_use_blocks.is_empty() {
            continue;
        }
        if assistant_index + 1 == messages.len() {
            valid_tool_use_blocks.extend(
                tool_use_blocks
                    .values()
                    .map(|block_index| (assistant_index, *block_index)),
            );
            continue;
        }

        for (user_index, user_message) in messages.iter().enumerate().skip(assistant_index + 1) {
            if user_message.get("role").and_then(Value::as_str) != Some("user") {
                break;
            }
            let Some(user_blocks) = user_message.get("content").and_then(Value::as_array) else {
                break;
            };
            let mut seen_non_tool_result = false;
            for (block_index, block) in user_blocks.iter().enumerate() {
                if block.get("type").and_then(Value::as_str) == Some("tool_result") {
                    let Some(id) = block.get("tool_use_id").and_then(Value::as_str) else {
                        continue;
                    };
                    if !seen_non_tool_result && let Some(tool_use_index) = tool_use_blocks.get(id) {
                        valid_tool_use_blocks.insert((assistant_index, *tool_use_index));
                        valid_tool_result_blocks.insert((user_index, block_index));
                    }
                } else {
                    seen_non_tool_result = true;
                }
            }
        }
    }

    let mut cleaned = Vec::with_capacity(messages.len());
    for (message_index, mut message) in messages.into_iter().enumerate() {
        let Some(blocks) = message.get("content").and_then(Value::as_array).cloned() else {
            cleaned.push(message);
            continue;
        };
        let has_tool_use = blocks
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"));
        let has_tool_result = blocks
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"));
        if !has_tool_use && !has_tool_result {
            cleaned.push(message);
            continue;
        }

        let mut tool_use_removed = false;
        let mut seen_tool_results = HashSet::<String>::new();
        let filtered = blocks
            .into_iter()
            .enumerate()
            .filter_map(
                |(block_index, block)| match block.get("type").and_then(Value::as_str) {
                    Some("tool_use") => {
                        let id = block
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty());
                        let keep = id.is_none()
                            || valid_tool_use_blocks.contains(&(message_index, block_index));
                        tool_use_removed |= !keep;
                        keep.then_some(block)
                    }
                    Some("tool_result") => {
                        let id = block
                            .get("tool_use_id")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty());
                        let Some(id) = id else {
                            return Some(block);
                        };
                        if !valid_tool_result_blocks.contains(&(message_index, block_index))
                            || !seen_tool_results.insert(id.to_owned())
                        {
                            None
                        } else {
                            Some(block)
                        }
                    }
                    _ => Some(block),
                },
            )
            .collect::<Vec<_>>();

        let surviving_tool_use = filtered
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"));
        let final_blocks = if tool_use_removed
            && !surviving_tool_use
            && message.get("role").and_then(Value::as_str) == Some("assistant")
        {
            filtered
                .into_iter()
                .filter(|block| {
                    !matches!(
                        block.get("type").and_then(Value::as_str),
                        Some("thinking" | "redacted_thinking")
                    )
                })
                .collect::<Vec<_>>()
        } else {
            filtered
        };
        if !final_blocks.is_empty() {
            message["content"] = Value::Array(final_blocks);
            cleaned.push(message);
        }
    }
    cleaned
}

fn strip_thinking_from_assistant_messages(messages: &mut [Value]) {
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(blocks) = message.get("content").and_then(Value::as_array).cloned() else {
            continue;
        };
        let filtered = blocks
            .iter()
            .filter(|block| {
                !matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("thinking" | "redacted_thinking")
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        if !filtered.is_empty() && filtered.len() != blocks.len() {
            message["content"] = Value::Array(filtered);
        }
    }
}

fn fill_missing_thinking_signatures(messages: &mut [Value]) {
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(blocks) = message.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        for block in blocks {
            if block.get("type").and_then(Value::as_str) == Some("thinking")
                && !block.get("signature").is_some_and(Value::is_string)
            {
                block["signature"] = Value::String(String::new());
            }
        }
    }
}

fn inject_empty_thinking_on_tool_use_turns(messages: &mut [Value]) {
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            continue;
        }
        let Some(blocks) = message.get_mut("content").and_then(Value::as_array_mut) else {
            continue;
        };
        let has_tool_use = blocks
            .iter()
            .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"));
        let has_thinking = blocks.iter().any(|block| {
            matches!(
                block.get("type").and_then(Value::as_str),
                Some("thinking" | "redacted_thinking")
            )
        });
        if has_tool_use && !has_thinking {
            blocks.insert(0, json!({"type":"thinking","thinking":"","signature":""}));
        }
    }
}

fn drop_unsigned_thinking_from_assistant_messages(
    messages: Vec<Value>,
) -> Result<Vec<Value>, ProviderError> {
    let mut active_tool_use_turns = HashSet::<usize>::new();
    let mut cursor = messages.len();
    while cursor > 0 {
        let mut has_tool_result = false;
        while cursor > 0 && messages[cursor - 1].get("role").and_then(Value::as_str) == Some("user")
        {
            let has_result = messages[cursor - 1]
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        block.get("type").and_then(Value::as_str) == Some("tool_result")
                    })
                });
            has_tool_result |= has_result;
            cursor -= 1;
        }
        if !has_tool_result || cursor == 0 {
            break;
        }
        let assistant_index = cursor - 1;
        let assistant = &messages[assistant_index];
        let is_assistant_tool_use = assistant.get("role").and_then(Value::as_str)
            == Some("assistant")
            && assistant
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|blocks| {
                    blocks
                        .iter()
                        .any(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
                });
        if !is_assistant_tool_use {
            break;
        }
        active_tool_use_turns.insert(assistant_index);
        cursor = assistant_index;
    }

    let mut cleaned = Vec::with_capacity(messages.len());
    for (index, mut message) in messages.into_iter().enumerate() {
        if message.get("role").and_then(Value::as_str) != Some("assistant") {
            cleaned.push(message);
            continue;
        }
        let Some(blocks) = message.get("content").and_then(Value::as_array).cloned() else {
            cleaned.push(message);
            continue;
        };
        let has_unsigned_thinking = blocks.iter().any(|block| {
            block.get("type").and_then(Value::as_str) == Some("thinking")
                && block
                    .get("signature")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
        });
        if !has_unsigned_thinking {
            cleaned.push(message);
            continue;
        }
        if active_tool_use_turns.contains(&index) {
            return Err(ProviderError::InvalidRequestShape);
        }
        let filtered = blocks
            .into_iter()
            .filter(|block| {
                block.get("type").and_then(Value::as_str) != Some("thinking")
                    || block
                        .get("signature")
                        .and_then(Value::as_str)
                        .is_some_and(|signature| !signature.is_empty())
            })
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            message["content"] = Value::Array(filtered);
            cleaned.push(message);
        }
    }
    Ok(cleaned)
}

fn drop_empty_text_thinking_blocks(messages: Vec<Value>) -> Vec<Value> {
    let latest_assistant_index = messages
        .iter()
        .rposition(|message| message.get("role").and_then(Value::as_str) == Some("assistant"));
    let mut cleaned = Vec::with_capacity(messages.len());
    for (index, mut message) in messages.into_iter().enumerate() {
        if message.get("role").and_then(Value::as_str) != Some("assistant")
            || Some(index) == latest_assistant_index
        {
            cleaned.push(message);
            continue;
        }
        let Some(blocks) = message.get("content").and_then(Value::as_array).cloned() else {
            cleaned.push(message);
            continue;
        };
        let filtered = blocks
            .into_iter()
            .filter(|block| {
                block.get("type").and_then(Value::as_str) != Some("thinking")
                    || block
                        .get("thinking")
                        .and_then(Value::as_str)
                        .is_some_and(|thinking| !thinking.is_empty())
            })
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            message["content"] = Value::Array(filtered);
            cleaned.push(message);
        }
    }
    cleaned
}

fn merge_consecutive_user_messages(messages: Vec<Value>) -> Vec<Value> {
    let mut merged = Vec::<Value>::new();
    for message in messages {
        let current_is_user = message.get("role").and_then(Value::as_str) == Some("user");
        let current_blocks = message.get("content").and_then(Value::as_array);
        if current_is_user
            && let Some(last) = merged.last_mut()
            && last.get("role").and_then(Value::as_str) == Some("user")
            && let (Some(last_blocks), Some(current_blocks)) = (
                last.get("content").and_then(Value::as_array),
                current_blocks,
            )
        {
            let combined = last_blocks
                .iter()
                .chain(current_blocks.iter())
                .cloned()
                .collect::<Vec<_>>();
            let mut seen_tool_results = HashSet::<String>::new();
            let tool_results = combined
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_result"))
                .filter(|block| {
                    let Some(id) = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                    else {
                        return true;
                    };
                    seen_tool_results.insert(id.to_owned())
                })
                .cloned();
            let non_tool_results = combined
                .iter()
                .filter(|block| block.get("type").and_then(Value::as_str) != Some("tool_result"))
                .cloned();
            last["content"] = Value::Array(tool_results.chain(non_tool_results).collect());
            continue;
        }
        merged.push(message);
    }
    merged
}

fn strip_trailing_assistant_prefill(messages: &mut Vec<Value>) {
    while messages
        .last()
        .is_some_and(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        && messages.last().is_some_and(is_empty_assistant_message)
    {
        messages.pop();
    }
    if messages
        .last()
        .is_some_and(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
    {
        messages.push(json!({"role":"user","content":[{"type":"text","text":"Continue."}]}));
    }
}

fn is_empty_assistant_message(message: &Value) -> bool {
    let Some(content) = message.get("content") else {
        return true;
    };
    if let Some(text) = content.as_str() {
        return text.trim().is_empty();
    }
    let Some(blocks) = content.as_array() else {
        return true;
    };
    for block in blocks {
        if block.get("type").and_then(Value::as_str) == Some("text") {
            if block
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| !text.trim().is_empty())
            {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

fn convert_tools(
    tools: Option<&Value>,
    mode: SchemaComplianceMode,
    cache: bool,
    one_hour: bool,
    global_scope: bool,
) -> Vec<Value> {
    let Some(tools) = tools.and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut converted = Vec::new();
    for tool in tools {
        let Some(declarations) = tool.get("functionDeclarations").and_then(Value::as_array) else {
            continue;
        };
        for declaration in declarations {
            let (Some(name), Some(description)) = (
                declaration
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.is_empty()),
                declaration
                    .get("description")
                    .and_then(Value::as_str)
                    .filter(|description| !description.is_empty()),
            ) else {
                continue;
            };
            let schema = declaration
                .get("parametersJsonSchema")
                .or_else(|| declaration.get("parameters"));
            let input_schema = schema
                .map(|schema| convert_schema(schema, mode))
                .unwrap_or_else(|| json!({"type":"object","properties":{}}));
            let input_schema = if input_schema.get("type").and_then(Value::as_str).is_none() {
                let mut schema = input_schema;
                schema["type"] = Value::String("object".to_owned());
                schema
            } else {
                input_schema
            };
            converted
                .push(json!({"name":name,"description":description,"input_schema":input_schema}));
        }
    }
    if cache && let Some(last) = converted.last_mut() {
        last["cache_control"] = cache_control(one_hour, global_scope);
    }
    converted
}

fn cache_control(one_hour: bool, global_scope: bool) -> Value {
    let mut control = Map::from_iter([("type".to_owned(), json!("ephemeral"))]);
    if one_hour {
        control.insert("ttl".to_owned(), json!("1h"));
    }
    if global_scope {
        control.insert("scope".to_owned(), json!("global"));
    }
    Value::Object(control)
}

fn resolved_cache_retention(config: &AnthropicProviderConfig, anchor: &str) -> bool {
    const WIRE_ORDER: [&str; 3] = ["tool", "system", "user.last"];
    let Some(anchor_index) = WIRE_ORDER.iter().position(|candidate| *candidate == anchor) else {
        return config.cache_retention_1h;
    };
    WIRE_ORDER[anchor_index..].iter().rev().any(|candidate| {
        config
            .cache_retention_by_block
            .get(*candidate)
            .copied()
            .unwrap_or(config.cache_retention_1h)
    })
}

fn use_global_cache_scope(config: &AnthropicProviderConfig) -> bool {
    config.enable_cache_control
        && (is_anthropic_native_base_url(&config.base_url) || config.force_global_cache_scope)
}

fn add_user_cache_anchor(messages: &mut [Value], one_hour: bool, global_scope: bool) {
    for message in messages.iter_mut().rev() {
        if message.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let Some(content) = message.get_mut("content").and_then(Value::as_array_mut) else {
            break;
        };
        if let Some(last) = content.last_mut() {
            let kind = last.get("type").and_then(Value::as_str);
            if kind == Some("tool_result")
                || (kind == Some("text")
                    && last
                        .get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.is_empty()))
            {
                last["cache_control"] = cache_control(one_hour, global_scope);
            }
        }
        break;
    }
}

fn extract_system_text(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| extract_system_text(Some(value)))
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Some(value) => value
            .get("parts")
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter_map(|part| {
                        part.as_str()
                            .or_else(|| part.get("text").and_then(Value::as_str))
                    })
                    .filter(|text| !text.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default(),
        None => String::new(),
    }
}

#[derive(Default)]
struct ToolIdResolver {
    source_to_wire: HashMap<String, String>,
    used: HashSet<String>,
    generated: usize,
}

impl ToolIdResolver {
    fn resolve(&mut self, source: Option<&str>) -> String {
        let source = source.unwrap_or_default().trim();
        if !source.is_empty() {
            if let Some(existing) = self.source_to_wire.get(source) {
                return existing.clone();
            }
        }
        let base = if source.is_empty() {
            let id = format!("tool_{}", self.generated);
            self.generated += 1;
            id
        } else {
            let sanitized = source
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect::<String>();
            if sanitized.is_empty() {
                let id = format!("tool_{}", self.generated);
                self.generated += 1;
                id
            } else {
                sanitized
            }
        };
        let mut wire = base.clone();
        let mut suffix = 1;
        while !self.used.insert(wire.clone()) {
            wire = format!("{base}_{suffix}");
            suffix += 1;
        }
        if !source.is_empty() {
            self.source_to_wire.insert(source.to_owned(), wire.clone());
        }
        wire
    }
}

#[derive(Default)]
struct AnthropicUsage {
    input: u64,
    output: u64,
    cache_read: u64,
    cache_creation: u64,
    input_reported: bool,
    output_reported: bool,
    cache_read_reported: bool,
    cache_creation_reported: bool,
}

impl AnthropicUsage {
    fn update(&mut self, usage: &Value) {
        update_usage_field(
            usage,
            "input_tokens",
            &mut self.input,
            &mut self.input_reported,
        );
        update_usage_field(
            usage,
            "output_tokens",
            &mut self.output,
            &mut self.output_reported,
        );
        update_usage_field(
            usage,
            "cache_read_input_tokens",
            &mut self.cache_read,
            &mut self.cache_read_reported,
        );
        update_usage_field(
            usage,
            "cache_creation_input_tokens",
            &mut self.cache_creation,
            &mut self.cache_creation_reported,
        );
    }

    fn metadata(&self) -> (Value, GenAiUsageProvenance) {
        anthropic_usage_metadata(
            self.input,
            self.cache_read,
            self.cache_creation,
            self.output_reported.then_some(self.output),
            self.cache_read_reported,
            self.cache_creation_reported,
        )
    }

    fn any_reported(&self) -> bool {
        self.input_reported
            || self.output_reported
            || self.cache_read_reported
            || self.cache_creation_reported
    }
}

fn update_usage_field(value: &Value, field: &str, target: &mut u64, reported: &mut bool) {
    if let Some(value) = value.get(field).and_then(Value::as_u64) {
        *target = value;
        *reported = true;
    }
}

fn anthropic_usage_metadata(
    input: u64,
    cache_read: u64,
    cache_creation: u64,
    output: Option<u64>,
    cache_read_reported: bool,
    cache_creation_reported: bool,
) -> (Value, GenAiUsageProvenance) {
    let looks_like_openai =
        !cache_creation_reported && cache_creation == 0 && cache_read > 0 && input >= cache_read;
    let prompt = if looks_like_openai {
        input
    } else {
        input
            .saturating_add(cache_read)
            .saturating_add(cache_creation)
    };
    let mut metadata = json!({
        "promptTokenCount":prompt,
        "cachedContentTokenCount":cache_read,
    });
    if let Some(output) = output {
        metadata["candidatesTokenCount"] = json!(output);
        metadata["totalTokenCount"] = json!(prompt.saturating_add(output));
    }
    let provenance = GenAiUsageProvenance {
        cached_input_tokens_reported: cache_read_reported,
        cache_creation_input_tokens: cache_creation_reported.then_some(cache_creation),
    };
    (metadata, provenance)
}

struct StreamingBlockState {
    kind: String,
    id: Option<String>,
    name: Option<String>,
    input_json: String,
    signature: String,
}

pub struct AnthropicGeminiEventStream {
    source: Option<AnthropicEventStream>,
    client: AnthropicMessagesClient,
    fallback_request: Value,
    cancellation: Option<CancellationToken>,
    blocks: HashMap<usize, StreamingBlockState>,
    usage: AnthropicUsage,
    message_id: Option<String>,
    model: Option<String>,
    finish_reason: Option<String>,
    message_start_usage_pending: bool,
    any_usage_reported: bool,
    assistant_payload: bool,
    fallback_attempted: bool,
    done: bool,
}

#[derive(Debug, Error)]
pub enum AnthropicGeminiStreamError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("Anthropic stream returned an error event: {message}")]
    StreamContent { message: String },
}

#[derive(Debug, Error)]
pub enum AnthropicTurnError {
    #[error(transparent)]
    Stream(#[from] AnthropicGeminiStreamError),
    #[error(transparent)]
    Turn(#[from] TurnResponseError),
}

impl AnthropicGeminiEventStream {
    pub async fn next_chunk(
        &mut self,
    ) -> Result<Option<ConvertedStreamChunk>, AnthropicGeminiStreamError> {
        if self.done {
            return Ok(None);
        }
        loop {
            let event = match self.source.as_mut() {
                Some(source) => source.next_event().await?,
                None => None,
            };
            let Some(event) = event else {
                self.source = None;
                if !self.assistant_payload
                    && self.finish_reason.is_none()
                    && !self.fallback_attempted
                {
                    self.fallback_attempted = true;
                    let message = self
                        .client
                        .complete_with_cancellation(
                            &self.fallback_request,
                            self.cancellation.as_ref(),
                        )
                        .await?;
                    self.done = true;
                    return Ok(Some(
                        convert_anthropic_message_to_gemini(&message).into_chunk(),
                    ));
                }
                self.done = true;
                return Ok(None);
            };
            if let Some(message) = anthropic_stream_error(&event) {
                self.done = true;
                return Err(AnthropicGeminiStreamError::StreamContent { message });
            }
            if let Some(chunk) = self.convert_event(event)? {
                return Ok(Some(chunk));
            }
        }
    }

    pub async fn next_turn_events(
        &mut self,
        turn: &mut Turn,
    ) -> Result<Option<Vec<TurnEvent>>, AnthropicTurnError> {
        loop {
            let Some(chunk) = self.next_chunk().await? else {
                return Ok(None);
            };
            let events = turn.accept_response(&chunk.response, false)?;
            if !events.is_empty() {
                return Ok(Some(events));
            }
        }
    }

    fn convert_event(
        &mut self,
        event: OpenAiSseEvent,
    ) -> Result<Option<ConvertedStreamChunk>, ProviderError> {
        match event
            .data
            .get("type")
            .and_then(Value::as_str)
            .or(event.event.as_deref())
            .unwrap_or("")
        {
            "message_start" => {
                let message = event.data.get("message").unwrap_or(&Value::Null);
                self.message_id = message
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or(self.message_id.take());
                self.model = message
                    .get("model")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or(self.model.take());
                if let Some(usage) = message.get("usage") {
                    self.usage.update(usage);
                    self.any_usage_reported |= self.usage.any_reported();
                }
                self.message_start_usage_pending = self.usage.any_reported();
            }
            "content_block_start" => {
                let index = event.data.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                let block = event.data.get("content_block").unwrap_or(&Value::Null);
                let kind = block
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("text")
                    .to_owned();
                let initial = if kind == "tool_use" {
                    block
                        .get("input")
                        .filter(|input| *input != &json!({}))
                        .and_then(|input| serde_json::to_string(input).ok())
                        .unwrap_or_default()
                } else {
                    String::new()
                };
                let state = StreamingBlockState {
                    kind: kind.clone(),
                    id: block.get("id").and_then(Value::as_str).map(str::to_owned),
                    name: block.get("name").and_then(Value::as_str).map(str::to_owned),
                    input_json: initial,
                    signature: block
                        .get("signature")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                };
                if kind == "tool_use" {
                    if let (Some(id), Some(name)) = (
                        state.id.as_ref().filter(|value| !value.is_empty()),
                        state.name.as_ref().filter(|value| !value.is_empty()),
                    ) {
                        let usage = self.take_start_usage();
                        let mut chunk = self.empty_chunk(usage);
                        chunk.tool_call_preparations.push(ToolCallPreparation {
                            call_id: id.clone(),
                            tool_name: name.clone(),
                        });
                        self.blocks.insert(index, state);
                        return Ok(Some(chunk));
                    }
                }
                self.blocks.insert(index, state);
            }
            "content_block_delta" => {
                let index = event.data.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                let delta = event.data.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text_delta" => {
                        let text = delta.get("text").and_then(Value::as_str).unwrap_or("");
                        if !text.is_empty() {
                            self.assistant_payload = true;
                            let usage = self.take_start_usage();
                            return Ok(Some(self.part_chunk(json!({"text":text}), usage)));
                        }
                    }
                    "thinking_delta" => {
                        let text = delta.get("thinking").and_then(Value::as_str).unwrap_or("");
                        if !text.is_empty() {
                            self.assistant_payload = true;
                            let usage = self.take_start_usage();
                            return Ok(Some(
                                self.part_chunk(json!({"text":text,"thought":true}), usage),
                            ));
                        }
                    }
                    "signature_delta" => {
                        let signature =
                            delta.get("signature").and_then(Value::as_str).unwrap_or("");
                        if !signature.is_empty() {
                            if let Some(block) = self.blocks.get_mut(&index) {
                                block.signature.push_str(signature);
                            }
                            self.assistant_payload = true;
                            let usage = self.take_start_usage();
                            return Ok(Some(self.part_chunk(
                                json!({"thought":true,"thoughtSignature":signature}),
                                usage,
                            )));
                        }
                    }
                    "input_json_delta" => {
                        if let Some(block) = self.blocks.get_mut(&index) {
                            let delta = delta
                                .get("partial_json")
                                .and_then(Value::as_str)
                                .unwrap_or("");
                            if block.input_json.len().saturating_add(delta.len())
                                > MAX_RESPONSE_BODY_BYTES
                            {
                                return Err(ProviderError::ResponseTooLarge {
                                    limit: MAX_RESPONSE_BODY_BYTES,
                                });
                            }
                            block.input_json.push_str(delta);
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                let index = event.data.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                if let Some(block) = self.blocks.remove(&index) {
                    if block.kind == "tool_use" {
                        let args = serde_json::from_str::<Value>(if block.input_json.is_empty() {
                            "{}"
                        } else {
                            &block.input_json
                        })
                        .ok()
                        .unwrap_or_else(|| json!({}));
                        self.assistant_payload = true;
                        let usage = self.take_start_usage();
                        return Ok(Some(self.part_chunk(
                            json!({"functionCall":{
                                "id":block.id,
                                "name":block.name,
                                "args":args,
                            }}),
                            usage,
                        )));
                    }
                }
            }
            "message_delta" => {
                if let Some(reason) = event
                    .data
                    .pointer("/delta/stop_reason")
                    .and_then(Value::as_str)
                {
                    self.finish_reason = Some(reason.to_owned());
                }
                if let Some(usage) = event.data.get("usage") {
                    self.usage.update(usage);
                    self.any_usage_reported |= self.usage.any_reported();
                }
                if self.finish_reason.is_some()
                    || event
                        .data
                        .get("usage")
                        .is_some_and(|usage| !usage.is_null())
                {
                    self.message_start_usage_pending = false;
                    let (metadata, provenance) = self.usage.metadata();
                    let mut chunk = self.empty_chunk(None);
                    if self.any_usage_reported || event.data.get("usage").is_some() {
                        chunk.response["usageMetadata"] = metadata;
                        chunk.usage_provenance = Some(provenance);
                    }
                    if let Some(reason) = finish_reason(self.finish_reason.as_deref()) {
                        chunk.response["candidates"][0]["finishReason"] =
                            Value::String(reason.to_owned());
                    }
                    return Ok(Some(chunk));
                }
            }
            "message_stop" => {
                if self.any_usage_reported {
                    self.message_start_usage_pending = false;
                    let (metadata, provenance) = self.usage.metadata();
                    let mut chunk = self.empty_chunk(None);
                    chunk.response["usageMetadata"] = metadata;
                    chunk.usage_provenance = Some(provenance);
                    if let Some(reason) = finish_reason(self.finish_reason.as_deref()) {
                        chunk.response["candidates"][0]["finishReason"] =
                            Value::String(reason.to_owned());
                    }
                    return Ok(Some(chunk));
                }
            }
            _ => {}
        }
        Ok(None)
    }

    fn take_start_usage(&mut self) -> Option<Value> {
        if !self.message_start_usage_pending {
            return None;
        }
        self.message_start_usage_pending = false;
        Some(self.usage.metadata().0)
    }

    fn empty_chunk(&self, usage: Option<Value>) -> ConvertedStreamChunk {
        let mut chunk = ConvertedStreamChunk {
            response: json!({
                "createTime":now_millis().to_string(),
                "promptFeedback":{"safetyRatings":[]},
                "candidates":[{"content":{"parts":[],"role":"model"},"index":0,"safetyRatings":[]}],
            }),
            ..ConvertedStreamChunk::default()
        };
        if let Some(message_id) = &self.message_id {
            chunk.response["responseId"] = Value::String(message_id.clone());
        }
        if let Some(model) = &self.model {
            chunk.response["modelVersion"] = Value::String(model.clone());
        }
        if let Some(usage) = usage {
            let (_, provenance) = self.usage.metadata();
            chunk.response["usageMetadata"] = usage;
            chunk.usage_provenance = Some(provenance);
        }
        chunk
    }

    fn part_chunk(&self, part: Value, usage: Option<Value>) -> ConvertedStreamChunk {
        let mut chunk = self.empty_chunk(usage);
        chunk.response["candidates"][0]["content"]["parts"] = json!([part]);
        chunk
    }
}

impl ConvertedGeminiResponse {
    fn into_chunk(self) -> ConvertedStreamChunk {
        ConvertedStreamChunk {
            response: self.response,
            usage_provenance: self.usage_provenance,
            ..ConvertedStreamChunk::default()
        }
    }
}

fn safe_tool_args(input: Option<&Value>) -> Value {
    match input {
        Some(Value::Object(_) | Value::Array(_)) => input.cloned().unwrap_or_else(|| json!({})),
        Some(Value::String(text)) => {
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({}))
        }
        _ => json!({}),
    }
}

fn is_json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn finish_reason(reason: Option<&str>) -> Option<&'static str> {
    match reason? {
        "end_turn" | "stop_sequence" | "tool_use" => Some("STOP"),
        "max_tokens" => Some("MAX_TOKENS"),
        "content_filter" => Some("SAFETY"),
        _ => Some("FINISH_REASON_UNSPECIFIED"),
    }
}

fn anthropic_stream_error(event: &OpenAiSseEvent) -> Option<String> {
    let is_error = event.event.as_deref() == Some("error")
        || event.data.get("type").and_then(Value::as_str) == Some("error");
    if !is_error {
        return None;
    }
    event
        .data
        .pointer("/error/message")
        .and_then(Value::as_str)
        .or_else(|| event.data.get("message").and_then(Value::as_str))
        .map(str::to_owned)
        .or_else(|| serde_json::to_string(&event.data).ok())
        .or_else(|| Some("Unknown Anthropic stream error".to_owned()))
}

fn messages_endpoint(base_url: &str) -> Result<Url, ProviderError> {
    let mut url = Url::parse(base_url).map_err(|_| ProviderError::InvalidBaseUrl)?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(ProviderError::InvalidBaseUrl);
    }
    let path = url.path().trim_end_matches('/');
    let endpoint = if path.ends_with("/messages") {
        path.to_owned()
    } else if path.ends_with("/v1") {
        format!("{path}/messages")
    } else {
        format!("{path}/v1/messages")
    };
    url.set_path(&endpoint);
    Ok(url)
}

fn is_anthropic_native_base_url(base_url: &str) -> bool {
    Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| host == "api.anthropic.com" || host.ends_with(".anthropic.com"))
}

fn user_agent(config: &AnthropicProviderConfig, bearer_auth: bool) -> String {
    let version = config
        .cli_version
        .as_deref()
        .filter(|version| !version.is_empty())
        .unwrap_or("unknown");
    if bearer_auth {
        return format!("claude-cli/{version} (external, cli)");
    }
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    format!(
        "CanopyCode/{version} ({platform}; {})",
        std::env::consts::ARCH
    )
}

fn redact_secrets(mut body: String, config: &AnthropicProviderConfig) -> String {
    if let Some(api_key) = config.api_key.as_deref().filter(|key| !key.is_empty()) {
        body = body.replace(api_key, "[redacted]");
    }
    for (name, value) in &config.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "authorization" | "x-api-key" | "api-key" | "anthropic-api-key"
        ) && !value.is_empty()
        {
            body = body.replace(value, "[redacted]");
        }
    }
    body
}

fn map_transport_error(error: reqwest::Error) -> ProviderError {
    let error = error.without_url();
    let message = error.to_string();
    ProviderError::Transport {
        source: error,
        message,
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
