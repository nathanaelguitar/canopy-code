//! Native MCP transport adapters. WebSocket, Streamable HTTP, SSE, and stdio
//! are concrete transports; OAuth providers and embedded SDK control remain
//! injected adapters.

use super::client_runtime::{
    McpClientError, McpOAuthRecoveryState, McpServerRequestHandler, McpTransport, McpTransportAuth,
    McpTransportError, McpTransportErrorHandler, McpTransportFactory, McpTransportKind,
    McpTransportSpec, StreamableHttpFallback, streamable_http_get_sse_fallback,
};
use crate::utils::cancellation::CancellationToken;
use crate::utils::sanitize_child_env::sanitize_child_env;
use futures_util::{SinkExt, StreamExt};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, WWW_AUTHENTICATE};
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode};
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message as WebSocketMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite};

const MAX_STDIO_LINE_BYTES: usize = 64 * 1024 * 1024;
const MAX_RETAINED_STDIO_LINE_CAPACITY: usize = 1024 * 1024;
const MAX_HTTP_SSE_EVENT_BYTES: usize = 16 * 1024 * 1024;
const ERROR_BODY_EXCERPT_BYTES: usize = 512;
const GRACEFUL_CHILD_EXIT_MS: u64 = 2_000;

type AuthFuture<'a> = futures_util::future::BoxFuture<'a, Result<HashMap<String, String>, String>>;

/// OAuth, Google ADC, and service-account impersonation implementations can
/// provide their request headers without coupling the client protocol to a
/// cloud SDK. Returned Authorization replaces a configured Authorization.
pub trait McpTransportAuthResolver: Send + Sync {
    fn resolve_headers<'a>(&'a self, spec: &'a McpTransportSpec) -> AuthFuture<'a>;
}

/// Concrete factory for stdio, WebSocket, SSE, and Streamable HTTP transports.
/// OAuth providers and cloud credential acquisition remain injected adapters.
pub struct NativeMcpTransportFactory {
    proxy_url: Option<String>,
    tls_insecure: bool,
    oauth_recovery: Option<Arc<McpOAuthRecoveryState>>,
    auth_resolver: Option<Arc<dyn McpTransportAuthResolver>>,
    sdk_transport_factory: Option<Arc<dyn McpTransportFactory>>,
}

impl NativeMcpTransportFactory {
    pub fn new() -> Self {
        let tls_insecure = std::env::var("CANOPY_TLS_INSECURE")
            .ok()
            .is_some_and(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            || std::env::var("NODE_TLS_REJECT_UNAUTHORIZED").as_deref() == Ok("0");
        Self {
            proxy_url: None,
            tls_insecure,
            oauth_recovery: None,
            auth_resolver: None,
            sdk_transport_factory: None,
        }
    }

    pub fn with_proxy(mut self, proxy_url: Option<String>) -> Self {
        self.proxy_url = proxy_url;
        self
    }

    pub fn with_tls_insecure(mut self, tls_insecure: bool) -> Self {
        self.tls_insecure = tls_insecure;
        self
    }

    pub fn with_oauth_recovery(mut self, state: Arc<McpOAuthRecoveryState>) -> Self {
        self.oauth_recovery = Some(state);
        self
    }

    pub fn with_auth_resolver(mut self, resolver: Arc<dyn McpTransportAuthResolver>) -> Self {
        self.auth_resolver = Some(resolver);
        self
    }

    pub fn with_sdk_transport_factory(mut self, factory: Arc<dyn McpTransportFactory>) -> Self {
        self.sdk_transport_factory = Some(factory);
        self
    }

    fn build_http_client(&self, spec: &McpTransportSpec) -> Result<Client, McpTransportError> {
        let redirect = if spec.stop_agent_plugin_redirects {
            Policy::none()
        } else {
            Policy::limited(10)
        };
        let mut builder = Client::builder().redirect(redirect);
        if self.tls_insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(proxy_url) = self.proxy_url.as_deref() {
            let proxy = reqwest::Proxy::all(proxy_url)
                .map_err(|error| McpTransportError::Transport(error.to_string()))?;
            builder = builder.proxy(proxy);
        }
        builder
            .build()
            .map_err(|error| McpTransportError::Transport(error.to_string()))
    }

    async fn resolve_headers(
        &self,
        spec: &McpTransportSpec,
    ) -> Result<HeaderMap, McpTransportError> {
        let mut headers = headers_from_map(&spec.headers)?;
        match &spec.auth {
            McpTransportAuth::None | McpTransportAuth::BearerToken(_) => {}
            McpTransportAuth::GoogleCredentials { .. }
            | McpTransportAuth::ServiceAccountImpersonation { .. } => {
                let Some(resolver) = self.auth_resolver.as_ref() else {
                    return Err(McpTransportError::Transport(format!(
                        "MCP auth provider {:?} needs an installed Rust credential adapter",
                        spec.auth
                    )));
                };
                let extra = resolver
                    .resolve_headers(spec)
                    .await
                    .map_err(McpTransportError::Transport)?;
                for (name, value) in extra {
                    let name = HeaderName::from_bytes(name.as_bytes())
                        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                    let value = HeaderValue::from_str(&value)
                        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                    headers.insert(name, value);
                }
            }
        }
        Ok(headers)
    }
}

impl Default for NativeMcpTransportFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl McpTransportFactory for NativeMcpTransportFactory {
    fn create<'a>(
        &'a self,
        spec: McpTransportSpec,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<Arc<dyn McpTransport>, McpTransportError>> {
        Box::pin(async move {
            if let Some(state) = self.oauth_recovery.as_ref() {
                state.clear(&spec.server_name, &oauth_recovery_config(&spec));
            }
            match spec.kind {
                McpTransportKind::Stdio => {
                    if cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        return Err(McpTransportError::Cancelled);
                    }
                    let transport = StdioTransport::spawn(spec)?;
                    if cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        let _ = transport.close().await;
                        return Err(McpTransportError::Cancelled);
                    }
                    Ok(transport as Arc<dyn McpTransport>)
                }
                McpTransportKind::StreamableHttp => {
                    let client = self.build_http_client(&spec)?;
                    let headers = self.resolve_headers(&spec).await?;
                    let url = spec.endpoint.as_deref().ok_or_else(|| {
                        McpTransportError::Transport("Streamable HTTP URL is missing".to_owned())
                    })?;
                    let url = reqwest::Url::parse(url)
                        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                    let shared = Arc::new(HttpShared::new(client, url, headers));
                    if let Some(state) = self.oauth_recovery.as_ref() {
                        shared.set_oauth_observer(
                            Arc::clone(state),
                            spec.server_name.clone(),
                            oauth_recovery_config(&spec),
                        );
                    }
                    Ok(Arc::new(StreamableHttpTransport { shared }) as Arc<dyn McpTransport>)
                }
                McpTransportKind::Sse => {
                    let client = self.build_http_client(&spec)?;
                    let headers = self.resolve_headers(&spec).await?;
                    let endpoint = spec.endpoint.as_deref().ok_or_else(|| {
                        McpTransportError::Transport("SSE URL is missing".to_owned())
                    })?;
                    let endpoint = reqwest::Url::parse(endpoint)
                        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                    let oauth_observer = self.oauth_recovery.as_ref().map(|state| {
                        (
                            Arc::clone(state),
                            spec.server_name.clone(),
                            oauth_recovery_config(&spec),
                        )
                    });
                    let shared = LegacySseShared::connect(
                        client,
                        endpoint,
                        headers,
                        oauth_observer,
                        cancellation,
                    )
                    .await?;
                    Ok(Arc::new(LegacySseTransport { shared }) as Arc<dyn McpTransport>)
                }
                McpTransportKind::WebSocket => {
                    let headers = self.resolve_headers(&spec).await?;
                    let endpoint = spec.endpoint.as_deref().ok_or_else(|| {
                        McpTransportError::Transport("WebSocket URL is missing".to_owned())
                    })?;
                    let transport =
                        WebSocketTransport::connect(endpoint, headers, cancellation).await?;
                    Ok(transport as Arc<dyn McpTransport>)
                }
                McpTransportKind::Sdk => {
                    let factory = self.sdk_transport_factory.as_ref().ok_or_else(|| {
                        McpTransportError::Transport(
                            "SDK MCP server requires an installed control-plane transport adapter"
                                .to_owned(),
                        )
                    })?;
                    factory.create(spec, cancellation).await
                }
            }
        })
    }
}

fn headers_from_map(
    values: &std::collections::BTreeMap<String, String>,
) -> Result<HeaderMap, McpTransportError> {
    let mut headers = HeaderMap::new();
    for (name, value) in values {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
        let value = HeaderValue::from_str(value)
            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn oauth_recovery_config(spec: &McpTransportSpec) -> Value {
    let mut config = serde_json::json!({});
    if let Some(oauth) = spec.oauth_config.as_ref() {
        config["oauth"] = oauth.clone();
    }
    if let Some(endpoint) = spec.endpoint.as_deref() {
        let field = match spec.kind {
            McpTransportKind::StreamableHttp => "httpUrl",
            McpTransportKind::Sse => "url",
            _ => return config,
        };
        config[field] = Value::String(endpoint.to_owned());
    }
    config["authProviderType"] = Value::String(
        match &spec.auth {
            McpTransportAuth::GoogleCredentials { .. } => "google_credentials",
            McpTransportAuth::ServiceAccountImpersonation { .. } => "service_account_impersonation",
            _ => "dynamic_discovery",
        }
        .to_owned(),
    );
    config
}

fn record_oauth_transport_error(
    observer: Option<(Arc<McpOAuthRecoveryState>, String, Value)>,
    error: &McpTransportError,
) {
    if let Some((state, name, config)) = observer {
        state.record_connect_error(&name, &config, &McpClientError::from(error.clone()));
    }
}

struct HttpShared {
    client: Client,
    url: reqwest::Url,
    headers: HeaderMap,
    session_id: RwLock<Option<String>>,
    protocol_version: RwLock<Option<String>>,
    server_request_handler: RwLock<Option<McpServerRequestHandler>>,
    error_handler: RwLock<Option<McpTransportErrorHandler>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpTransportError>>>>,
    event_task: AsyncMutex<Option<JoinHandle<()>>>,
    closing: AtomicBool,
    connect_phase: AtomicBool,
    oauth_observer: Mutex<Option<(Arc<McpOAuthRecoveryState>, String, Value)>>,
}

impl HttpShared {
    fn new(client: Client, url: reqwest::Url, headers: HeaderMap) -> Self {
        Self {
            client,
            url,
            headers,
            session_id: RwLock::new(None),
            protocol_version: RwLock::new(None),
            server_request_handler: RwLock::new(None),
            error_handler: RwLock::new(None),
            pending: Mutex::new(HashMap::new()),
            event_task: AsyncMutex::new(None),
            closing: AtomicBool::new(false),
            connect_phase: AtomicBool::new(true),
            oauth_observer: Mutex::new(None),
        }
    }

    fn set_oauth_observer(&self, state: Arc<McpOAuthRecoveryState>, name: String, config: Value) {
        *self
            .oauth_observer
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some((state, name, config));
    }

    fn remember_session(&self, response: &Response) {
        if let Some(session) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
        {
            *self.session_id.write().unwrap_or_else(|e| e.into_inner()) = Some(session.to_owned());
        }
    }

    fn notify_error(&self, error: McpTransportError) {
        if let Some(handler) = self
            .error_handler
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            handler(error);
        }
    }

    fn record_connect_error(&self, error: &McpTransportError) {
        if self.connect_phase.load(Ordering::Acquire) {
            record_oauth_transport_error(
                self.oauth_observer
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone(),
                error,
            );
        }
    }

    fn request_headers(&self, accept: &'static str) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(ACCEPT, HeaderValue::from_static(accept));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(session) = self
            .session_id
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            if let Ok(value) = HeaderValue::from_str(session) {
                headers.insert(HeaderName::from_static("mcp-session-id"), value);
            }
        }
        if let Some(version) = self
            .protocol_version
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            if let Ok(value) = HeaderValue::from_str(version) {
                headers.insert(HeaderName::from_static("mcp-protocol-version"), value);
            }
        }
        headers
    }
}

struct StreamableHttpTransport {
    shared: Arc<HttpShared>,
}

impl StreamableHttpTransport {
    async fn send_post(&self, message: &Value) -> Result<Response, McpTransportError> {
        let response = self
            .shared
            .client
            .post(self.shared.url.clone())
            .headers(
                self.shared
                    .request_headers("application/json, text/event-stream"),
            )
            .json(message)
            .send()
            .await
            .map_err(|error| {
                let error = McpTransportError::Transport(error.to_string());
                self.shared.record_connect_error(&error);
                error
            })?;
        self.shared.remember_session(&response);
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let challenge = response
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let excerpt = read_error_excerpt(response).await.unwrap_or_default();
            let error = McpTransportError::HttpStatus {
                status,
                message: excerpt,
                www_authenticate: challenge,
            };
            self.shared.record_connect_error(&error);
            return Err(error);
        }
        Ok(response)
    }

    async fn start_optional_get_stream(&self) {
        let mut task = self.shared.event_task.lock().await;
        if task.is_some() || self.shared.closing.load(Ordering::Acquire) {
            return;
        }
        let shared = Arc::clone(&self.shared);
        *task = Some(tokio::spawn(async move {
            if let Err(error) = run_optional_get_stream(shared.clone()).await {
                fail_pending(&shared.pending, "Connection closed");
                if !shared.closing.load(Ordering::Acquire) {
                    shared.notify_error(error);
                }
            }
        }));
    }
}

impl McpTransport for StreamableHttpTransport {
    fn request<'a>(
        &'a self,
        request: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, McpTransportError>> {
        Box::pin(async move {
            let id = request.get("id").and_then(Value::as_u64).ok_or_else(|| {
                McpTransportError::Transport(
                    "MCP request ID must be an unsigned integer".to_owned(),
                )
            })?;
            let (sender, receiver) = oneshot::channel();
            self.shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, sender);
            let _pending = PendingGuard::new(Arc::downgrade(&self.shared), id);
            let response = self.send_post(&request).await?;
            if response.status() == StatusCode::ACCEPTED {
                // The matching response arrives on the initialized GET SSE stream.
            } else if is_event_stream(&response) {
                read_event_stream(response, Arc::clone(&self.shared), None).await?;
            } else {
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                let message: Value = serde_json::from_slice(&bytes).map_err(|error| {
                    McpTransportError::Transport(format!(
                        "invalid Streamable HTTP JSON response: {error}"
                    ))
                })?;
                route_message(Arc::clone(&self.shared), message).await;
            }
            await_pending(receiver, cancellation).await
        })
    }

    fn notify<'a>(
        &'a self,
        notification: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(McpTransportError::Cancelled);
            }
            let method = notification
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let response = self.send_post(&notification).await?;
            if response.status() != StatusCode::ACCEPTED {
                if is_event_stream(&response) {
                    read_event_stream(response, Arc::clone(&self.shared), None).await?;
                } else if response.content_length().unwrap_or(0) > 0 {
                    let _ = response.bytes().await;
                }
            }
            if method == "notifications/initialized" {
                self.shared.connect_phase.store(false, Ordering::Release);
                self.start_optional_get_stream().await;
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            self.shared.closing.store(true, Ordering::Release);
            if let Some(task) = self.shared.event_task.lock().await.take() {
                task.abort();
            }
            let Some(session) = self
                .shared
                .session_id
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            else {
                return Ok(());
            };
            let mut headers = self
                .shared
                .request_headers("application/json, text/event-stream");
            if let Ok(value) = HeaderValue::from_str(&session) {
                headers.insert(HeaderName::from_static("mcp-session-id"), value);
            }
            match self
                .shared
                .client
                .delete(self.shared.url.clone())
                .headers(headers)
                .send()
                .await
            {
                Ok(response)
                    if response.status() == StatusCode::METHOD_NOT_ALLOWED
                        || response.status() == StatusCode::NOT_FOUND =>
                {
                    Ok(())
                }
                Ok(response) if response.status().is_success() => Ok(()),
                Ok(response) => Err(http_status_error(response).await),
                Err(error) => Err(McpTransportError::Transport(error.to_string())),
            }
        })
    }

    fn set_server_request_handler(&self, handler: McpServerRequestHandler) {
        *self
            .shared
            .server_request_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_error_handler(&self, handler: McpTransportErrorHandler) {
        *self
            .shared
            .error_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_protocol_version(&self, version: &str) {
        *self
            .shared
            .protocol_version
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(version.to_owned());
    }
}

struct PendingGuard {
    shared: Weak<HttpShared>,
    id: u64,
}

impl PendingGuard {
    fn new(shared: Weak<HttpShared>, id: u64) -> Self {
        Self { shared, id }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
    }
}

async fn run_optional_get_stream(shared: Arc<HttpShared>) -> Result<(), McpTransportError> {
    let mut headers = shared.request_headers("text/event-stream");
    headers.remove(CONTENT_TYPE);
    let response = shared
        .client
        .get(shared.url.clone())
        .headers(headers)
        .send()
        .await
        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
    shared.remember_session(&response);
    if response.status() == StatusCode::BAD_REQUEST {
        let excerpt = read_error_excerpt(response).await.unwrap_or_default();
        let relevant_headers = vec![("Accept".to_owned(), "text/event-stream".to_owned())];
        let _fallback: Option<StreamableHttpFallback> = streamable_http_get_sse_fallback(
            Some("GET"),
            &relevant_headers,
            400,
            excerpt.as_bytes(),
        );
        return Ok(());
    }
    if response.status() == StatusCode::METHOD_NOT_ALLOWED {
        return Ok(());
    }
    if !response.status().is_success() {
        return Err(http_status_error(response).await);
    }
    if is_event_stream(&response) {
        read_event_stream(response, shared, None).await?;
    }
    Ok(())
}

fn is_event_stream(response: &Response) -> bool {
    response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"))
}

async fn read_event_stream(
    response: Response,
    shared: Arc<HttpShared>,
    cancellation: Option<CancellationToken>,
) -> Result<(), McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut event_data = Vec::<String>::new();
    loop {
        let next = if let Some(token) = cancellation.as_ref() {
            if token.is_cancelled() {
                return Err(McpTransportError::Cancelled);
            }
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(McpTransportError::Cancelled),
                value = stream.next() => value,
            }
        } else {
            stream.next().await
        };
        let Some(chunk) = next else {
            process_sse_line(&mut event_data, &mut buffer, Arc::clone(&shared)).await?;
            if shared.closing.load(Ordering::Acquire) {
                return Ok(());
            }
            return Err(McpTransportError::Transport(
                "Streamable HTTP connection closed".to_owned(),
            ));
        };
        let chunk = chunk.map_err(|error| McpTransportError::Transport(error.to_string()))?;
        buffer.extend_from_slice(&chunk);
        if buffer.len() > MAX_HTTP_SSE_EVENT_BYTES {
            return Err(McpTransportError::Transport(format!(
                "SSE event exceeded {MAX_HTTP_SSE_EVENT_BYTES} bytes"
            )));
        }
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line);
            if line.is_empty() {
                if !event_data.is_empty() {
                    let data = event_data.join("\n");
                    event_data.clear();
                    let message: Value = serde_json::from_str(&data).map_err(|error| {
                        McpTransportError::Transport(format!(
                            "invalid JSON in MCP SSE event: {error}"
                        ))
                    })?;
                    route_message(Arc::clone(&shared), message).await;
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                event_data.push(data.strip_prefix(' ').unwrap_or(data).to_owned());
            }
        }
    }
}

async fn process_sse_line(
    event_data: &mut Vec<String>,
    _buffer: &mut Vec<u8>,
    shared: Arc<HttpShared>,
) -> Result<(), McpTransportError> {
    if !event_data.is_empty() {
        let data = event_data.join("\n");
        event_data.clear();
        let message: Value = serde_json::from_str(&data).map_err(|error| {
            McpTransportError::Transport(format!("invalid JSON in MCP SSE event: {error}"))
        })?;
        route_message(shared, message).await;
    }
    Ok(())
}

async fn route_message(shared: Arc<HttpShared>, message: Value) {
    if message.get("method").and_then(Value::as_str).is_some() {
        let handler = shared
            .server_request_handler
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let response = handler.map_or_else(
            || serde_json::json!({"jsonrpc":"2.0","id":message.get("id").cloned().unwrap_or(Value::Null),"error":{"code":-32601,"message":"Method not found"}}),
            |handler| handler(&message),
        );
        if message.get("id").is_some() {
            let _ = shared
                .client
                .post(shared.url.clone())
                .headers(shared.request_headers("application/json, text/event-stream"))
                .json(&response)
                .send()
                .await;
        }
        return;
    }
    if let Some(id) = message.get("id").and_then(Value::as_u64) {
        if let Some(sender) = shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        {
            let _ = sender.send(Ok(message));
        }
    }
}

async fn await_pending(
    receiver: oneshot::Receiver<Result<Value, McpTransportError>>,
    cancellation: Option<CancellationToken>,
) -> Result<Value, McpTransportError> {
    if let Some(token) = cancellation {
        if token.is_cancelled() {
            return Err(McpTransportError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(McpTransportError::Cancelled),
            result = receiver => result.unwrap_or_else(|_| Err(McpTransportError::Transport("request channel closed".to_owned()))),
        }
    } else {
        receiver.await.unwrap_or_else(|_| {
            Err(McpTransportError::Transport(
                "request channel closed".to_owned(),
            ))
        })
    }
}

async fn read_error_excerpt(response: Response) -> Result<String, McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while bytes.len() < ERROR_BODY_EXCERPT_BYTES {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk.map_err(|error| McpTransportError::Transport(error.to_string()))?;
        let remaining = ERROR_BODY_EXCERPT_BYTES - bytes.len();
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    let value = String::from_utf8_lossy(&bytes).trim().to_owned();
    Ok(if value.is_empty() {
        value
    } else if bytes.len() == ERROR_BODY_EXCERPT_BYTES {
        format!("{value}...")
    } else {
        value
    })
}

async fn http_status_error(response: Response) -> McpTransportError {
    let status = response.status().as_u16();
    let challenge = response
        .headers()
        .get(WWW_AUTHENTICATE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let excerpt = read_error_excerpt(response).await.unwrap_or_default();
    McpTransportError::HttpStatus {
        status,
        message: excerpt,
        www_authenticate: challenge,
    }
}

struct LegacySseShared {
    client: Client,
    sse_url: reqwest::Url,
    headers: HeaderMap,
    message_url: RwLock<Option<reqwest::Url>>,
    protocol_version: RwLock<Option<String>>,
    server_request_handler: RwLock<Option<McpServerRequestHandler>>,
    error_handler: RwLock<Option<McpTransportErrorHandler>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpTransportError>>>>,
    endpoint_ready: Mutex<Option<oneshot::Sender<Result<(), McpTransportError>>>>,
    reader_task: AsyncMutex<Option<JoinHandle<()>>>,
    closing: AtomicBool,
    connect_phase: AtomicBool,
    oauth_observer: Option<(Arc<McpOAuthRecoveryState>, String, Value)>,
}

impl LegacySseShared {
    async fn connect(
        client: Client,
        sse_url: reqwest::Url,
        mut headers: HeaderMap,
        oauth_observer: Option<(Arc<McpOAuthRecoveryState>, String, Value)>,
        cancellation: Option<CancellationToken>,
    ) -> Result<Arc<Self>, McpTransportError> {
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let shared = Arc::new(Self {
            client,
            sse_url,
            headers,
            message_url: RwLock::new(None),
            protocol_version: RwLock::new(None),
            server_request_handler: RwLock::new(None),
            error_handler: RwLock::new(None),
            pending: Mutex::new(HashMap::new()),
            endpoint_ready: Mutex::new(None),
            reader_task: AsyncMutex::new(None),
            closing: AtomicBool::new(false),
            connect_phase: AtomicBool::new(true),
            oauth_observer,
        });
        let (sender, receiver) = oneshot::channel();
        *shared
            .endpoint_ready
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(sender);
        let reader_shared = Arc::clone(&shared);
        let reader = tokio::spawn(async move {
            if let Err(error) = legacy_sse_read_loop(reader_shared.clone()).await {
                reader_shared.record_connect_error(&error);
                fail_pending(&reader_shared.pending, "Connection closed");
                if let Some(sender) = reader_shared
                    .endpoint_ready
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                {
                    let _ = sender.send(Err(error.clone()));
                }
                if !reader_shared.closing.load(Ordering::Acquire) {
                    if let Some(handler) = reader_shared
                        .error_handler
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                    {
                        handler(error);
                    }
                }
            }
        });
        *shared.reader_task.lock().await = Some(reader);
        match await_sse_endpoint(receiver, cancellation).await {
            Ok(()) => Ok(shared),
            Err(error) => {
                shared.closing.store(true, Ordering::Release);
                if let Some(task) = shared.reader_task.lock().await.take() {
                    task.abort();
                }
                Err(error)
            }
        }
    }

    fn message_headers(&self) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(version) = self
            .protocol_version
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            if let Ok(value) = HeaderValue::from_str(version) {
                headers.insert(HeaderName::from_static("mcp-protocol-version"), value);
            }
        }
        headers
    }

    fn record_connect_error(&self, error: &McpTransportError) {
        if self.connect_phase.load(Ordering::Acquire) {
            record_oauth_transport_error(self.oauth_observer.clone(), error);
        }
    }

    fn set_endpoint(&self, endpoint: &str) -> Result<(), McpTransportError> {
        let endpoint = self.sse_url.join(endpoint).map_err(|error| {
            McpTransportError::Transport(format!("invalid SSE message endpoint: {error}"))
        })?;
        *self.message_url.write().unwrap_or_else(|e| e.into_inner()) = Some(endpoint);
        if let Some(sender) = self
            .endpoint_ready
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = sender.send(Ok(()));
        }
        Ok(())
    }
}

struct LegacySseTransport {
    shared: Arc<LegacySseShared>,
}

impl LegacySseTransport {
    async fn post(&self, message: &Value) -> Result<Response, McpTransportError> {
        let endpoint = self
            .shared
            .message_url
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| {
                McpTransportError::Transport("SSE message endpoint is not ready".to_owned())
            })?;
        let response = self
            .shared
            .client
            .post(endpoint)
            .headers(self.shared.message_headers())
            .json(message)
            .send()
            .await
            .map_err(|error| {
                let error = McpTransportError::Transport(error.to_string());
                self.shared.record_connect_error(&error);
                error
            })?;
        if !response.status().is_success() {
            let error = http_status_error(response).await;
            self.shared.record_connect_error(&error);
            return Err(error);
        }
        Ok(response)
    }
}

impl McpTransport for LegacySseTransport {
    fn request<'a>(
        &'a self,
        request: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, McpTransportError>> {
        Box::pin(async move {
            let id = request.get("id").and_then(Value::as_u64).ok_or_else(|| {
                McpTransportError::Transport(
                    "MCP request ID must be an unsigned integer".to_owned(),
                )
            })?;
            let (sender, receiver) = oneshot::channel();
            self.shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, sender);
            let _pending = SsePendingGuard {
                shared: Arc::downgrade(&self.shared),
                id,
            };
            let response = self.post(&request).await?;
            if is_event_stream(&response) {
                let shared = Arc::clone(&self.shared);
                tokio::spawn(async move {
                    if let Err(error) = legacy_sse_message_stream(response, shared.clone()).await {
                        fail_pending(&shared.pending, "Connection closed");
                        if let Some(handler) = shared
                            .error_handler
                            .read()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_ref()
                        {
                            handler(error);
                        }
                    }
                });
            } else if response.content_length().unwrap_or(0) > 0 {
                let bytes = response
                    .bytes()
                    .await
                    .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                if let Ok(message) = serde_json::from_slice::<Value>(&bytes) {
                    route_legacy_message(Arc::clone(&self.shared), message).await;
                }
            }
            await_pending(receiver, cancellation).await
        })
    }

    fn notify<'a>(
        &'a self,
        notification: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(McpTransportError::Cancelled);
            }
            let method = notification
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let response = self.post(&notification).await?;
            if is_event_stream(&response) {
                let shared = Arc::clone(&self.shared);
                tokio::spawn(async move {
                    if let Err(error) = legacy_sse_message_stream(response, shared.clone()).await {
                        fail_pending(&shared.pending, "Connection closed");
                        if let Some(handler) = shared
                            .error_handler
                            .read()
                            .unwrap_or_else(|e| e.into_inner())
                            .as_ref()
                        {
                            handler(error);
                        }
                    }
                });
            }
            if method == "notifications/initialized" {
                self.shared.connect_phase.store(false, Ordering::Release);
            }
            Ok(())
        })
    }

    fn close<'a>(&'a self) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            self.shared.closing.store(true, Ordering::Release);
            if let Some(task) = self.shared.reader_task.lock().await.take() {
                task.abort();
            }
            fail_pending(&self.shared.pending, "Connection closed");
            Ok(())
        })
    }

    fn set_server_request_handler(&self, handler: McpServerRequestHandler) {
        *self
            .shared
            .server_request_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_error_handler(&self, handler: McpTransportErrorHandler) {
        *self
            .shared
            .error_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_protocol_version(&self, version: &str) {
        *self
            .shared
            .protocol_version
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(version.to_owned());
    }
}

async fn await_sse_endpoint(
    receiver: oneshot::Receiver<Result<(), McpTransportError>>,
    cancellation: Option<CancellationToken>,
) -> Result<(), McpTransportError> {
    if let Some(token) = cancellation {
        if token.is_cancelled() {
            return Err(McpTransportError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => Err(McpTransportError::Cancelled),
            result = receiver => result.unwrap_or_else(|_| Err(McpTransportError::Transport("SSE endpoint channel closed".to_owned()))),
        }
    } else {
        receiver.await.unwrap_or_else(|_| {
            Err(McpTransportError::Transport(
                "SSE endpoint channel closed".to_owned(),
            ))
        })
    }
}

async fn legacy_sse_read_loop(shared: Arc<LegacySseShared>) -> Result<(), McpTransportError> {
    let response = shared
        .client
        .get(shared.sse_url.clone())
        .headers(shared.headers.clone())
        .send()
        .await
        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
    if !response.status().is_success() {
        let error = http_status_error(response).await;
        shared.record_connect_error(&error);
        return Err(error);
    }
    read_legacy_sse_stream(response, shared).await
}

async fn read_legacy_sse_stream(
    response: Response,
    shared: Arc<LegacySseShared>,
) -> Result<(), McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut event_name = String::new();
    let mut event_data = Vec::<String>::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| McpTransportError::Transport(error.to_string()))?;
        buffer.extend_from_slice(&chunk);
        if buffer.len() > MAX_HTTP_SSE_EVENT_BYTES {
            return Err(McpTransportError::Transport(format!(
                "SSE event exceeded {MAX_HTTP_SSE_EVENT_BYTES} bytes"
            )));
        }
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line);
            if line.is_empty() {
                dispatch_legacy_sse_event(&shared, &event_name, &mut event_data).await?;
                event_name.clear();
            } else if let Some(value) = line.strip_prefix("event:") {
                event_name = value.trim().to_owned();
            } else if let Some(value) = line.strip_prefix("data:") {
                event_data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
            }
        }
    }
    Err(McpTransportError::Transport(
        "SSE connection closed".to_owned(),
    ))
}

async fn legacy_sse_message_stream(
    response: Response,
    shared: Arc<LegacySseShared>,
) -> Result<(), McpTransportError> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut event_data = Vec::<String>::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| McpTransportError::Transport(error.to_string()))?;
        buffer.extend_from_slice(&chunk);
        if buffer.len() > MAX_HTTP_SSE_EVENT_BYTES {
            return Err(McpTransportError::Transport(format!(
                "SSE event exceeded {MAX_HTTP_SSE_EVENT_BYTES} bytes"
            )));
        }
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let mut line = buffer.drain(..=newline).collect::<Vec<_>>();
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line);
            if line.is_empty() && !event_data.is_empty() {
                dispatch_legacy_sse_event(&shared, "message", &mut event_data).await?;
            } else if let Some(value) = line.strip_prefix("data:") {
                event_data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
            }
        }
    }
    Ok(())
}

async fn dispatch_legacy_sse_event(
    shared: &Arc<LegacySseShared>,
    event_name: &str,
    event_data: &mut Vec<String>,
) -> Result<(), McpTransportError> {
    if event_data.is_empty() {
        return Ok(());
    }
    let data = event_data.join("\n");
    event_data.clear();
    if event_name == "endpoint" {
        return shared.set_endpoint(&data);
    }
    let message: Value = serde_json::from_str(&data).map_err(|error| {
        McpTransportError::Transport(format!("invalid JSON in MCP SSE event: {error}"))
    })?;
    route_legacy_message(Arc::clone(shared), message).await;
    Ok(())
}

async fn route_legacy_message(shared: Arc<LegacySseShared>, message: Value) {
    if message.get("method").and_then(Value::as_str).is_some() {
        let handler = shared
            .server_request_handler
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let response = handler.map_or_else(
            || serde_json::json!({"jsonrpc":"2.0","id":message.get("id").cloned().unwrap_or(Value::Null),"error":{"code":-32601,"message":"Method not found"}}),
            |handler| handler(&message),
        );
        if message.get("id").is_some() {
            let endpoint = {
                shared
                    .message_url
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            };
            if let Some(endpoint) = endpoint {
                let _ = shared
                    .client
                    .post(endpoint)
                    .headers(shared.message_headers())
                    .json(&response)
                    .send()
                    .await;
            }
        }
        return;
    }
    if let Some(id) = message.get("id").and_then(Value::as_u64) {
        if let Some(sender) = shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        {
            let _ = sender.send(Ok(message));
        }
    }
}

struct SsePendingGuard {
    shared: Weak<LegacySseShared>,
    id: u64,
}

impl Drop for SsePendingGuard {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
    }
}

struct WebSocketShared {
    outbound: mpsc::UnboundedSender<WebSocketMessage>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpTransportError>>>>,
    server_request_handler: RwLock<Option<McpServerRequestHandler>>,
    error_handler: RwLock<Option<McpTransportErrorHandler>>,
    closing: AtomicBool,
    alive: AtomicBool,
    io_task: AsyncMutex<Option<JoinHandle<()>>>,
}

struct WebSocketTransport {
    shared: Arc<WebSocketShared>,
}

impl WebSocketTransport {
    async fn connect(
        endpoint: &str,
        headers: HeaderMap,
        cancellation: Option<CancellationToken>,
    ) -> Result<Arc<Self>, McpTransportError> {
        let endpoint = normalize_websocket_url(endpoint)?;
        let mut request = endpoint
            .into_client_request()
            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
        for (name, value) in &headers {
            let name = name
                .as_str()
                .parse::<tungstenite::http::header::HeaderName>()
                .map_err(|error| McpTransportError::Transport(error.to_string()))?;
            let value = value
                .to_str()
                .map_err(|error| McpTransportError::Transport(error.to_string()))?
                .parse::<tungstenite::http::header::HeaderValue>()
                .map_err(|error| McpTransportError::Transport(error.to_string()))?;
            request.headers_mut().insert(name, value);
        }

        let connect = async {
            connect_async(request)
                .await
                .map(|(socket, _response)| socket)
                .map_err(websocket_error)
        };
        let socket = if let Some(token) = cancellation {
            if token.is_cancelled() {
                return Err(McpTransportError::Cancelled);
            }
            tokio::select! {
                biased;
                _ = token.cancelled() => return Err(McpTransportError::Cancelled),
                result = connect => result?,
            }
        } else {
            connect.await?
        };

        let (mut writer, mut reader) = socket.split();
        let (outbound, mut outbound_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(WebSocketShared {
            outbound,
            pending: Mutex::new(HashMap::new()),
            server_request_handler: RwLock::new(None),
            error_handler: RwLock::new(None),
            closing: AtomicBool::new(false),
            alive: AtomicBool::new(true),
            io_task: AsyncMutex::new(None),
        });
        let io_shared = Arc::clone(&shared);
        let io_task = tokio::spawn(async move {
            let mut terminal_error = None;
            let mut close_received = false;
            loop {
                tokio::select! {
                    outgoing = outbound_rx.recv() => match outgoing {
                        Some(message) => {
                            if let Err(error) = writer.send(message).await {
                                terminal_error = Some(websocket_error(error));
                                break;
                            }
                        }
                        None => break,
                    },
                    incoming = reader.next() => match incoming {
                        Some(Ok(WebSocketMessage::Text(text))) => {
                            match serde_json::from_str::<Value>(text.as_str()) {
                                Ok(message) => route_websocket_message(&io_shared, message),
                                Err(error) => {
                                    terminal_error = Some(McpTransportError::Transport(
                                        format!("invalid JSON from MCP WebSocket server: {error}"),
                                    ));
                                    break;
                                }
                            }
                        }
                        Some(Ok(WebSocketMessage::Binary(bytes))) => {
                            match serde_json::from_slice::<Value>(&bytes) {
                                Ok(message) => route_websocket_message(&io_shared, message),
                                Err(error) => {
                                    terminal_error = Some(McpTransportError::Transport(
                                        format!("invalid JSON from MCP WebSocket server: {error}"),
                                    ));
                                    break;
                                }
                            }
                        }
                        Some(Ok(WebSocketMessage::Ping(payload))) => {
                            if io_shared.outbound.send(WebSocketMessage::Pong(payload)).is_err() {
                                break;
                            }
                        }
                        Some(Ok(WebSocketMessage::Close(_))) => {
                            if !close_received {
                                close_received = true;
                                if !io_shared.closing.load(Ordering::Acquire) {
                                    let _ = io_shared
                                        .outbound
                                        .send(WebSocketMessage::Close(None));
                                    terminal_error = Some(McpTransportError::Transport(
                                        "Connection closed".to_owned(),
                                    ));
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            terminal_error = Some(websocket_error(error));
                            break;
                        }
                        None => {
                            terminal_error = Some(McpTransportError::Transport(
                                "Connection closed".to_owned(),
                            ));
                            break;
                        }
                    }
                }
            }

            io_shared.alive.store(false, Ordering::Release);
            fail_pending(&io_shared.pending, "Connection closed");
            if !io_shared.closing.load(Ordering::Acquire) {
                if let Some(error) = terminal_error {
                    if let Some(handler) = io_shared
                        .error_handler
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                    {
                        handler(error);
                    }
                }
            }
        });
        *shared.io_task.lock().await = Some(io_task);
        Ok(Arc::new(Self { shared }))
    }
}

impl McpTransport for WebSocketTransport {
    fn request<'a>(
        &'a self,
        request: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, McpTransportError>> {
        Box::pin(async move {
            if !self.shared.alive.load(Ordering::Acquire) {
                return Err(McpTransportError::Transport("Connection closed".to_owned()));
            }
            let id = request.get("id").and_then(Value::as_u64).ok_or_else(|| {
                McpTransportError::Transport(
                    "MCP request ID must be an unsigned integer".to_owned(),
                )
            })?;
            let encoded = serde_json::to_string(&request)
                .map_err(|error| McpTransportError::Transport(error.to_string()))?;
            let (sender, receiver) = oneshot::channel();
            self.shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, sender);
            let _pending = WebSocketPendingGuard {
                shared: Arc::downgrade(&self.shared),
                id,
            };
            self.shared
                .outbound
                .send(WebSocketMessage::Text(encoded.into()))
                .map_err(|_| McpTransportError::Transport("Connection closed".to_owned()))?;
            await_pending(receiver, cancellation).await
        })
    }

    fn notify<'a>(
        &'a self,
        notification: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(McpTransportError::Cancelled);
            }
            if !self.shared.alive.load(Ordering::Acquire) {
                return Err(McpTransportError::Transport("Connection closed".to_owned()));
            }
            let encoded = serde_json::to_string(&notification)
                .map_err(|error| McpTransportError::Transport(error.to_string()))?;
            self.shared
                .outbound
                .send(WebSocketMessage::Text(encoded.into()))
                .map_err(|_| McpTransportError::Transport("Connection closed".to_owned()))
        })
    }

    fn close<'a>(&'a self) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if self.shared.closing.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.shared.alive.store(false, Ordering::Release);
            fail_pending(&self.shared.pending, "Connection closed");
            let _ = self.shared.outbound.send(WebSocketMessage::Close(None));
            if let Some(mut task) = self.shared.io_task.lock().await.take() {
                if tokio::time::timeout(Duration::from_millis(GRACEFUL_CHILD_EXIT_MS), &mut task)
                    .await
                    .is_err()
                {
                    task.abort();
                }
            }
            Ok(())
        })
    }

    fn set_server_request_handler(&self, handler: McpServerRequestHandler) {
        *self
            .shared
            .server_request_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_error_handler(&self, handler: McpTransportErrorHandler) {
        *self
            .shared
            .error_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }
}

struct WebSocketPendingGuard {
    shared: Weak<WebSocketShared>,
    id: u64,
}

impl Drop for WebSocketPendingGuard {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
    }
}

fn normalize_websocket_url(endpoint: &str) -> Result<String, McpTransportError> {
    let normalized = if let Some(rest) = endpoint.strip_prefix("tcp://") {
        format!("ws://{rest}")
    } else {
        endpoint.to_owned()
    };
    if !normalized.starts_with("ws://") && !normalized.starts_with("wss://") {
        return Err(McpTransportError::Transport(format!(
            "invalid MCP WebSocket URL '{endpoint}': expected ws://, wss://, or tcp://"
        )));
    }
    Ok(normalized)
}

fn websocket_error(error: tungstenite::Error) -> McpTransportError {
    match error {
        tungstenite::Error::Http(response) => {
            let response = *response;
            let status = response.status().as_u16();
            let challenge = response
                .headers()
                .get(WWW_AUTHENTICATE.as_str())
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let message = response
                .body()
                .as_ref()
                .map(|body| String::from_utf8_lossy(body).trim().to_owned())
                .unwrap_or_default();
            McpTransportError::HttpStatus {
                status,
                message,
                www_authenticate: challenge,
            }
        }
        error => McpTransportError::Transport(error.to_string()),
    }
}

fn route_websocket_message(shared: &Arc<WebSocketShared>, message: Value) {
    if message.get("method").and_then(Value::as_str).is_some() {
        let handler = shared
            .server_request_handler
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if message.get("id").is_some() {
            let response = handler.map_or_else(
                || serde_json::json!({"jsonrpc":"2.0","id":message.get("id").cloned().unwrap_or(Value::Null),"error":{"code":-32601,"message":"Method not found"}}),
                |handler| handler(&message),
            );
            if let Ok(encoded) = serde_json::to_string(&response) {
                let _ = shared.outbound.send(WebSocketMessage::Text(encoded.into()));
            }
        }
        return;
    }
    if let Some(id) = message.get("id").and_then(Value::as_u64) {
        if let Some(sender) = shared
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        {
            let _ = sender.send(Ok(message));
        }
    }
}

struct StdioShared {
    stdin: AsyncMutex<Option<ChildStdin>>,
    child: AsyncMutex<Option<Child>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpTransportError>>>>,
    server_request_handler: RwLock<Option<McpServerRequestHandler>>,
    error_handler: RwLock<Option<McpTransportErrorHandler>>,
    pid: Option<u32>,
    alive: AtomicBool,
    closing: AtomicBool,
    closed: AtomicBool,
    reader_task: Mutex<Option<JoinHandle<()>>>,
}

struct StdioTransport {
    shared: Arc<StdioShared>,
}

impl StdioTransport {
    fn spawn(spec: McpTransportSpec) -> Result<Arc<Self>, McpTransportError> {
        let Some(command_name) = spec.command.as_deref() else {
            return Err(McpTransportError::Transport(
                "stdio MCP command is missing".to_owned(),
            ));
        };
        let child_env = sanitize_child_env(&spec.env);
        let mut command = Command::new(command_name);
        command
            .args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .env_clear()
            .envs(&child_env);
        if let Some(cwd) = spec.cwd.as_ref() {
            command.current_dir(cwd);
        }
        let mut child = command
            .spawn()
            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
        let pid = child.id();
        let stdin = child.stdin.take().ok_or_else(|| {
            McpTransportError::Transport("stdio MCP stdin was not piped".to_owned())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            McpTransportError::Transport("stdio MCP stdout was not piped".to_owned())
        })?;
        let stderr = child.stderr.take();
        let shared = Arc::new(StdioShared {
            stdin: AsyncMutex::new(Some(stdin)),
            child: AsyncMutex::new(Some(child)),
            pending: Mutex::new(HashMap::new()),
            server_request_handler: RwLock::new(None),
            error_handler: RwLock::new(None),
            pid,
            alive: AtomicBool::new(true),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reader_task: Mutex::new(None),
        });
        let reader_shared = Arc::clone(&shared);
        let reader = tokio::spawn(async move {
            if let Err(error) = stdio_read_loop(stdout, reader_shared.clone()).await {
                let reason = error.to_string();
                fail_pending(&reader_shared.pending, &reason);
                if !reader_shared.closing.load(Ordering::Acquire) {
                    if let Some(handler) = reader_shared
                        .error_handler
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .as_ref()
                    {
                        handler(McpTransportError::Transport(reason));
                    }
                }
                let mut child = reader_shared.child.lock().await;
                if let Some(child) = child.as_mut() {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                }
            }
            reader_shared.alive.store(false, Ordering::Release);
        });
        *shared
            .reader_task
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(reader);
        if let Some(stderr) = stderr {
            tokio::spawn(drain_stderr(stderr));
        }
        Ok(Arc::new(Self { shared }))
    }
}

impl McpTransport for StdioTransport {
    fn request<'a>(
        &'a self,
        request: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<Value, McpTransportError>> {
        Box::pin(async move {
            if !self.shared.alive.load(Ordering::Acquire) {
                return Err(McpTransportError::Transport("Connection closed".to_owned()));
            }
            let id = request.get("id").and_then(Value::as_u64).ok_or_else(|| {
                McpTransportError::Transport(
                    "MCP request ID must be an unsigned integer".to_owned(),
                )
            })?;
            let (sender, receiver) = oneshot::channel();
            self.shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id, sender);
            let _pending = StdioPendingGuard {
                shared: Arc::downgrade(&self.shared),
                id,
            };
            write_stdio_message(&self.shared, &request).await?;
            await_pending(receiver, cancellation).await
        })
    }

    fn notify<'a>(
        &'a self,
        notification: Value,
        cancellation: Option<CancellationToken>,
    ) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(McpTransportError::Cancelled);
            }
            write_stdio_message(&self.shared, &notification).await
        })
    }

    fn close<'a>(&'a self) -> futures_util::future::BoxFuture<'a, Result<(), McpTransportError>> {
        Box::pin(async move {
            if self.shared.closing.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            if let Some(mut stdin) = self.shared.stdin.lock().await.take() {
                let _ = stdin.shutdown().await;
            }
            if let Some(mut child) = self.shared.child.lock().await.take() {
                match tokio::time::timeout(
                    Duration::from_millis(GRACEFUL_CHILD_EXIT_MS),
                    child.wait(),
                )
                .await
                {
                    Ok(result) => {
                        let _ = result
                            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                    }
                    Err(_) => {
                        child
                            .kill()
                            .await
                            .map_err(|error| McpTransportError::Transport(error.to_string()))?;
                        let _ = child.wait().await;
                    }
                }
            }
            if let Some(reader) = self
                .shared
                .reader_task
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                reader.abort();
            }
            fail_pending(&self.shared.pending, "Connection closed");
            self.shared.alive.store(false, Ordering::Release);
            self.shared.closed.store(true, Ordering::Release);
            Ok(())
        })
    }

    fn set_server_request_handler(&self, handler: McpServerRequestHandler) {
        *self
            .shared
            .server_request_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn set_error_handler(&self, handler: McpTransportErrorHandler) {
        *self
            .shared
            .error_handler
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(handler);
    }

    fn pid(&self) -> Option<u32> {
        self.shared
            .alive
            .load(Ordering::Acquire)
            .then_some(self.shared.pid)
            .flatten()
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        if self.shared.closed.load(Ordering::Acquire) {
            return;
        }
        self.shared.closing.store(true, Ordering::Release);
        if let Ok(mut child) = self.shared.child.try_lock() {
            if let Some(child) = child.as_mut() {
                let _ = child.start_kill();
            }
        }
        if let Some(reader) = self
            .shared
            .reader_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            reader.abort();
        }
        fail_pending(&self.shared.pending, "Connection closed");
        self.shared.alive.store(false, Ordering::Release);
    }
}

struct StdioPendingGuard {
    shared: Weak<StdioShared>,
    id: u64,
}

impl Drop for StdioPendingGuard {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
    }
}

async fn write_stdio_message(
    shared: &StdioShared,
    message: &Value,
) -> Result<(), McpTransportError> {
    let encoded = serde_json::to_vec(message)
        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
    let mut stdin = shared.stdin.lock().await;
    let Some(stdin) = stdin.as_mut() else {
        return Err(McpTransportError::Transport("Connection closed".to_owned()));
    };
    stdin
        .write_all(&encoded)
        .await
        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
    stdin
        .write_all(b"\n")
        .await
        .map_err(|error| McpTransportError::Transport(error.to_string()))?;
    stdin
        .flush()
        .await
        .map_err(|error| McpTransportError::Transport(error.to_string()))
}

async fn stdio_read_loop(
    stdout: ChildStdout,
    shared: Arc<StdioShared>,
) -> Result<(), McpTransportError> {
    let mut reader = BufReader::new(stdout);
    let mut line = Vec::new();
    loop {
        if line.capacity() > MAX_RETAINED_STDIO_LINE_CAPACITY {
            // Large tool results (for example, inline screenshots) are parsed
            // into owned JSON values. Release the wire buffer before waiting
            // for another message instead of retaining its high-water capacity
            // for the lifetime of this MCP process.
            line = Vec::new();
        } else {
            line.clear();
        }
        match read_bounded_stdio_line(&mut reader, &mut line)
            .await
            .map_err(|error| McpTransportError::Transport(error.to_string()))?
        {
            None => {
                fail_pending(&shared.pending, "Connection closed");
                return Err(McpTransportError::Transport("Connection closed".to_owned()));
            }
            Some(BoundedStdioLine::TooLarge) => {
                return Err(McpTransportError::Transport(format!(
                    "stdio MCP JSON-RPC line exceeded {MAX_STDIO_LINE_BYTES} bytes"
                )));
            }
            Some(BoundedStdioLine::Complete) => {}
        }
        let message: Value = match serde_json::from_slice(&line) {
            Ok(message) => message,
            Err(error) => {
                return Err(McpTransportError::Transport(format!(
                    "invalid JSON from stdio MCP server: {error}"
                )));
            }
        };
        if message.get("method").and_then(Value::as_str).is_some() {
            let handler = shared
                .server_request_handler
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let response = handler.map_or_else(
                || serde_json::json!({"jsonrpc":"2.0","id":message.get("id").cloned().unwrap_or(Value::Null),"error":{"code":-32601,"message":"Method not found"}}),
                |handler| handler(&message),
            );
            if message.get("id").is_some() {
                write_stdio_message(&shared, &response).await?;
            }
            continue;
        }
        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            if let Some(sender) = shared
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id)
            {
                let _ = sender.send(Ok(message));
            }
        }
    }
}

enum BoundedStdioLine {
    Complete,
    TooLarge,
}

/// Read a JSONL frame without allowing a child process to grow the retained
/// line buffer past `MAX_STDIO_LINE_BYTES`. An oversized frame immediately
/// fails the transport; the reader task then terminates the child.
async fn read_bounded_stdio_line<R>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<Option<BoundedStdioLine>>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(BoundedStdioLine::Complete))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(consumed) > MAX_STDIO_LINE_BYTES {
            return Ok(Some(BoundedStdioLine::TooLarge));
        }
        line.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);

        if newline.is_some() {
            return Ok(Some(BoundedStdioLine::Complete));
        }
    }
}

fn fail_pending(
    pending: &Mutex<HashMap<u64, oneshot::Sender<Result<Value, McpTransportError>>>>,
    reason: &str,
) {
    let mut pending = pending.lock().unwrap_or_else(|e| e.into_inner());
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(McpTransportError::Transport(reason.to_owned())));
    }
}

async fn drain_stderr(stderr: ChildStderr) {
    let mut reader = BufReader::new(stderr);
    let mut buffer = [0_u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }
}
