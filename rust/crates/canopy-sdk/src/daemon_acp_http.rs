//! ACP-over-HTTP transport for the daemon SDK.
//!
//! The transport sends ACP JSON-RPC over `POST /acp`, receives replies and
//! notifications from connection/session `GET /acp` SSE streams, and maps the
//! TypeScript SDK's URL-shaped daemon routes to ACP methods. Session reply
//! pumps are shared per session so the background request path never races an
//! active event subscriber for the daemon's single-reader session stream.

use std::collections::HashMap;
use std::future::pending;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Map, Number, Value, json};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

use crate::daemon_event_denormalizer::denormalize_acp_notification;
use crate::daemon_sse::{
    DEFAULT_SSE_IDLE_TIMEOUT, SseError, parse_sse_frames, parse_sse_json_stream,
};

/// Hard cap for outstanding ACP requests, including requests waiting on SSE.
pub const MAX_PENDING_ACP_REQUESTS: usize = 1024;
/// Maximum buffered event count for each public session subscription.
pub const DEFAULT_ACP_EVENT_QUEUE_CAPACITY: usize = 64;

const MAX_SAFE_EVENT_ID: u64 = 9_007_199_254_740_991;
const SESSION_REPLY_METHODS: &[&str] = &[
    "session/prompt",
    "session/cancel",
    "session/set_config_option",
    "session/set_mode",
    "session/set_model",
];

/// ACP-over-HTTP request or stream failure.
#[derive(Debug, Error)]
pub enum AcpHttpError {
    #[error("ACP HTTP transport is closed")]
    Closed,
    #[error("ACP HTTP operation was cancelled")]
    Cancelled,
    #[error("invalid ACP daemon base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("ACP HTTP request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("ACP initialize failed: {0}")]
    Initialize(String),
    #[error("ACP SSE stream failed: {0}")]
    Sse(#[from] SseError),
    #[error("ACP SSE response had HTTP status {status}: {detail}")]
    Http {
        status: u16,
        detail: String,
        body: Option<Value>,
    },
    #[error("ACP SSE expected content-type text/event-stream, got {content_type:?}")]
    InvalidContentType { content_type: String },
    #[error("ACP request queue is full (limit {MAX_PENDING_ACP_REQUESTS})")]
    TooManyPending,
    #[error("an ACP event subscription already owns session {0:?}")]
    SessionAlreadySubscribed(String),
    #[error("ACP response stream ended before the request was answered")]
    ReplyStreamClosed,
    #[error("ACP reply stream failed: {0}")]
    ReplyStreamFailed(String),
    #[error("initial ACP SSE connection timed out")]
    ConnectTimeout,
    #[error("ACP reply stream task ended unexpectedly")]
    ReplyTaskEnded,
    #[error("ACP response payload did not contain a valid numeric JSON-RPC id")]
    InvalidResponse,
    #[error("invalid HTTP header {name}: {source}")]
    InvalidHeader {
        name: &'static str,
        #[source]
        source: reqwest::header::InvalidHeaderValue,
    },
}

/// Caller-owned cancellation handle for ACP requests or event subscriptions.
#[derive(Clone, Debug)]
pub struct AcpCancellation {
    sender: watch::Sender<bool>,
}

impl Default for AcpCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpCancellation {
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }
}

/// Options for a resumable ACP session stream.
#[derive(Clone, Debug, Default)]
pub struct AcpSubscribeOptions {
    pub last_event_id: Option<u64>,
    pub epoch: Option<String>,
    pub connect_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub queue_capacity: Option<usize>,
    pub cancellation: Option<AcpCancellation>,
}

/// ACP RPC error object returned by the daemon.
#[derive(Clone, Debug)]
pub struct AcpRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

/// A JSON-RPC response with raw JSON retained for future fields.
#[derive(Clone, Debug)]
pub struct AcpRpcResponse {
    pub id: u64,
    pub result: Option<Value>,
    pub error: Option<AcpRpcError>,
    pub raw: Value,
}

/// HTTP-shaped result synthesized from an ACP route call.
#[derive(Clone, Debug)]
pub struct AcpHttpResponse {
    pub status: u16,
    pub body: Option<Value>,
}

/// URL route mapping produced from the TypeScript SDK's `acpRouteTable`.
#[derive(Clone, Debug)]
pub struct AcpRouteCall {
    pub method: String,
    pub params: Value,
    pub notification: bool,
}

/// Metadata learned from the accepted session stream.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AcpStreamMetadata {
    pub epoch: Option<String>,
}

/// Active event subscription. Dropping `events` cancels its session reader.
pub struct AcpEventSubscription {
    pub metadata: AcpStreamMetadata,
    pub events: AcpEventStream,
}

/// Bounded event stream whose drop releases its single-reader session lease.
pub struct AcpEventStream {
    receiver: mpsc::Receiver<Result<DaemonEvent, AcpHttpError>>,
    cancel: watch::Sender<bool>,
    lease: Arc<SessionLease>,
    _caller_cancellation: Option<AcpCancellation>,
}

impl Stream for AcpEventStream {
    type Item = Result<DaemonEvent, AcpHttpError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for AcpEventStream {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        self.lease.release();
    }
}

use crate::daemon_sse::DaemonEvent;

/// ACP-over-HTTP transport with one connection reply pump and at most one
/// session reader per session. The module exposes the request/reply transport
/// and route matcher independently of the TypeScript `DaemonClient` wrapper.
#[derive(Clone)]
pub struct AcpHttpTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    base_url: Url,
    client: reqwest::Client,
    token: Option<String>,
    idle_timeout: Option<Duration>,
    disposed: AtomicBool,
    disposed_sender: watch::Sender<bool>,
    init_lock: tokio::sync::Mutex<()>,
    state: Mutex<TransportState>,
    next_generation: AtomicU64,
}

struct TransportState {
    initialized: bool,
    init_result: Option<Value>,
    connection_id: Option<String>,
    next_request_id: u64,
    pending: HashMap<u64, PendingRequest>,
    connection_pump: Option<PumpEntry>,
    session_pumps: HashMap<String, PumpEntry>,
    active_session_subscriptions: HashMap<String, usize>,
    active_session_cancels: HashMap<String, watch::Sender<bool>>,
}

struct PendingRequest {
    scope: ReplyScope,
    sender: oneshot::Sender<Result<Value, AcpHttpError>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplyScope {
    Connection,
    Session(String),
}

struct PumpEntry {
    generation: u64,
    cancel: watch::Sender<bool>,
    ready: watch::Receiver<Option<Result<(), String>>>,
    references: usize,
    task: Option<JoinHandle<()>>,
}

struct SessionLease {
    inner: Arc<TransportInner>,
    session_id: String,
    generation: u64,
    released: AtomicBool,
}

impl SessionLease {
    fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut state = lock_recover(&self.inner.state);
        if let Some(count) = state.active_session_subscriptions.get_mut(&self.session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active_session_subscriptions.remove(&self.session_id);
                if let Some(cancel) = state.active_session_cancels.remove(&self.session_id) {
                    cancel.send_replace(true);
                }
                if let Some(entry) = state.session_pumps.remove(&self.session_id) {
                    if entry.generation == self.generation {
                        entry.cancel.send_replace(true);
                        if let Some(task) = entry.task {
                            task.abort();
                        }
                    } else {
                        state.session_pumps.insert(self.session_id.clone(), entry);
                    }
                }
                if !self.inner.disposed.load(Ordering::Acquire) {
                    reject_pending_for_session(
                        &mut state,
                        &self.session_id,
                        AcpHttpError::ReplyStreamClosed,
                    );
                }
            }
        }
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.release();
    }
}

struct PendingGuard {
    inner: Arc<TransportInner>,
    id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        lock_recover(&self.inner.state).pending.remove(&self.id);
    }
}

struct SessionPumpLease {
    inner: Arc<TransportInner>,
    session_id: String,
    generation: u64,
    released: bool,
}

impl Drop for SessionPumpLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let mut state = lock_recover(&self.inner.state);
        let should_stop = state
            .session_pumps
            .get_mut(&self.session_id)
            .filter(|entry| entry.generation == self.generation)
            .is_some_and(|entry| {
                entry.references = entry.references.saturating_sub(1);
                entry.references == 0
            });
        if should_stop {
            if let Some(entry) = state.session_pumps.remove(&self.session_id) {
                entry.cancel.send_replace(true);
                if let Some(task) = entry.task {
                    task.abort();
                }
            }
        }
    }
}

impl AcpHttpTransport {
    /// Construct an ACP transport with the default HTTP client and 45-second
    /// SSE idle timeout.
    pub fn new(base_url: impl AsRef<str>, token: Option<String>) -> Result<Self, AcpHttpError> {
        Self::with_options(
            base_url,
            token,
            reqwest::Client::new(),
            Some(DEFAULT_SSE_IDLE_TIMEOUT),
        )
    }

    /// Construct with an explicit client and SSE idle timeout (`None` disables
    /// the idle timer).
    pub fn with_options(
        base_url: impl AsRef<str>,
        token: Option<String>,
        client: reqwest::Client,
        idle_timeout: Option<Duration>,
    ) -> Result<Self, AcpHttpError> {
        let raw = base_url.as_ref();
        let base_url =
            Url::parse(raw).map_err(|error| AcpHttpError::InvalidBaseUrl(error.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(AcpHttpError::InvalidBaseUrl(format!(
                "unsupported URL scheme in {raw:?}"
            )));
        }
        let (disposed_sender, _disposed_receiver) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(TransportInner {
                base_url,
                client,
                token,
                idle_timeout: idle_timeout.filter(|value| !value.is_zero()),
                disposed: AtomicBool::new(false),
                disposed_sender,
                init_lock: tokio::sync::Mutex::new(()),
                state: Mutex::new(TransportState {
                    initialized: false,
                    init_result: None,
                    connection_id: None,
                    next_request_id: 1,
                    pending: HashMap::new(),
                    connection_pump: None,
                    session_pumps: HashMap::new(),
                    active_session_subscriptions: HashMap::new(),
                    active_session_cancels: HashMap::new(),
                }),
                next_generation: AtomicU64::new(1),
            }),
        })
    }

    pub fn connected(&self) -> bool {
        !self.inner.disposed.load(Ordering::Acquire) && lock_recover(&self.inner.state).initialized
    }

    /// Perform the one-time ACP initialize handshake. The first request path
    /// also attempts `GET /capabilities`, matching the TypeScript transport.
    pub async fn ensure_initialized(&self) -> Result<(), AcpHttpError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpHttpError::Closed);
        }
        let _guard = self.inner.init_lock.lock().await;
        if lock_recover(&self.inner.state).initialized {
            return Ok(());
        }
        self.initialize().await
    }

    async fn initialize(&self) -> Result<(), AcpHttpError> {
        let id = {
            let mut state = lock_recover(&self.inner.state);
            let id = state.next_request_id;
            state.next_request_id = state.next_request_id.saturating_add(1);
            id
        };
        let initialize_request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "initialize",
            "params": { "clientInfo": { "name": "qwen-code-sdk", "version": "1.0.0" } }
        });
        let headers = json_headers(&self.inner.token)?;
        let mut dispose_receiver = self.inner.disposed_sender.subscribe();
        let send = self
            .inner
            .client
            .post(endpoint(&self.inner.base_url, "acp")?)
            .headers(headers)
            .json(&initialize_request)
            .send();
        let response = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_receiver) => return Err(AcpHttpError::Closed),
            response = send => response.map_err(AcpHttpError::Request)?,
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text = read_response_text(response, &mut dispose_receiver, None).await?;
            return Err(AcpHttpError::Initialize(format!("HTTP {status}: {text}")));
        }
        let response_headers = response.headers().clone();
        let json = response.json::<Value>();
        let response: Value = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_receiver) => return Err(AcpHttpError::Closed),
            result = json => result.map_err(AcpHttpError::Request)?,
        };
        if let Some(error) = response.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("ACP initialize returned an error");
            return Err(AcpHttpError::Initialize(message.to_owned()));
        }
        let result = response.get("result").cloned().unwrap_or(Value::Null);
        let connection_id = response_headers
            .get("acp-connection-id")
            .and_then(|header| header.to_str().ok())
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                extract_string_path(
                    &result,
                    &["agentCapabilities", "_meta", "qwen", "connectionId"],
                )
            })
            .or_else(|| extract_string_path(&result, &["_meta", "qwen", "connectionId"]));
        {
            let mut state = lock_recover(&self.inner.state);
            state.connection_id = connection_id;
            state.init_result = Some(result);
            state.initialized = true;
        }

        if let Ok(url) = endpoint(&self.inner.base_url, "capabilities") {
            if let Ok(headers) = json_headers(&self.inner.token) {
                let send = self.inner.client.get(url).headers(headers).send();
                if let Ok(response) = tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut dispose_receiver) => return Err(AcpHttpError::Closed),
                    result = send => result,
                } {
                    if response.status().is_success() {
                        let json = response.json::<Value>();
                        if let Ok(capabilities) = tokio::select! {
                            biased;
                            _ = wait_for_cancel(&mut dispose_receiver) => return Err(AcpHttpError::Closed),
                            result = json => result,
                        } {
                            lock_recover(&self.inner.state).init_result = Some(capabilities);
                        }
                    }
                }
            }
        }
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpHttpError::Closed);
        }
        Ok(())
    }

    /// Send a JSON-RPC request and wait for the matching inline or SSE reply.
    pub async fn send_request(
        &self,
        method: impl Into<String>,
        params: Value,
        cancellation: Option<AcpCancellation>,
    ) -> Result<AcpRpcResponse, AcpHttpError> {
        self.ensure_initialized().await?;
        let method = method.into();
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let scope = session_id
            .as_ref()
            .filter(|_| SESSION_REPLY_METHODS.contains(&method.as_str()))
            .cloned()
            .map(ReplyScope::Session)
            .unwrap_or(ReplyScope::Connection);
        let (id, response_rx, pending_guard) = self.register_pending(scope.clone())?;
        let _pending_guard = pending_guard;
        let mut dispose_rx = self.inner.disposed_sender.subscribe();
        let mut caller_rx = cancellation.as_ref().map(|token| token.sender.subscribe());

        let _session_lease = if let ReplyScope::Session(sid) = &scope {
            Some(tokio::select! {
                biased;
                _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
                _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
                result = self.ensure_session_reply_pump(sid) => result?,
            })
        } else {
            None
        };
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
            result = self.ensure_connection_pump() => result?,
        }

        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": object_value(params),
        });
        let headers = self.request_headers(None)?;
        let post = self
            .inner
            .client
            .post(endpoint(&self.inner.base_url, "acp")?)
            .headers(headers)
            .json(&request)
            .send();
        let response = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
            response = post => response.map_err(AcpHttpError::Request)?,
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text = read_response_text(response, &mut dispose_rx, caller_rx.as_mut()).await?;
            let raw = json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -(status as i64),
                    "message": format!("HTTP {status}: {text}"),
                    "data": { "httpStatus": status }
                }
            });
            return parse_rpc_response(raw);
        }
        let status = response.status();
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|header| header.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if status.as_u16() == 200 && content_type.contains("application/json") {
            let json = response.json::<Value>();
            let raw: Value = tokio::select! {
                biased;
                _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
                _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
                result = json => result.map_err(AcpHttpError::Request)?,
            };
            return parse_rpc_response(raw);
        }

        let raw = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
            result = response_rx => result.map_err(|_| AcpHttpError::ReplyStreamClosed)??,
        };
        parse_rpc_response(raw)
    }

    /// Send an ACP notification. Notifications have no JSON-RPC id and do not
    /// wait for an SSE reply.
    pub async fn send_notification(
        &self,
        method: impl Into<String>,
        params: Value,
        cancellation: Option<AcpCancellation>,
    ) -> Result<(), AcpHttpError> {
        self.ensure_initialized().await?;
        let mut dispose_rx = self.inner.disposed_sender.subscribe();
        let mut caller_rx = cancellation.as_ref().map(|token| token.sender.subscribe());
        let body = json!({
            "jsonrpc": "2.0",
            "method": method.into(),
            "params": object_value(params),
        });
        let response = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_rx) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(caller_rx.as_mut()) => return Err(AcpHttpError::Cancelled),
            response = self.inner.client
                .post(endpoint(&self.inner.base_url, "acp")?)
                .headers(self.request_headers(None)?)
                .json(&body)
                .send() => response.map_err(AcpHttpError::Request)?,
        };
        drop(response);
        Ok(())
    }

    /// Map an HTTP-shaped SDK route and dispatch it through ACP. This covers
    /// the shared TypeScript `acpRouteTable`; callers that use `DaemonClient`
    /// still need the small URL/Response adapter described in PORT_STATUS.
    pub async fn dispatch_route(
        &self,
        http_method: &str,
        path: &str,
        query: &str,
        body: Value,
        cancellation: Option<AcpCancellation>,
    ) -> Result<AcpHttpResponse, AcpHttpError> {
        self.ensure_initialized().await?;
        let Some(route) = map_route(http_method, path, query, &body) else {
            return Ok(AcpHttpResponse {
                status: 404,
                body: Some(json!({ "error": format!("No ACP mapping for {http_method} {path}") })),
            });
        };
        if route.method == "_capabilities" {
            return Ok(AcpHttpResponse {
                status: 200,
                body: lock_recover(&self.inner.state)
                    .init_result
                    .clone()
                    .or(Some(json!({ "v": 1 }))),
            });
        }
        if route.notification {
            self.send_notification(route.method, route.params, cancellation)
                .await?;
            return Ok(AcpHttpResponse {
                status: 204,
                body: None,
            });
        }
        let response = self
            .send_request(route.method, route.params, cancellation)
            .await?;
        if let Some(error) = response.error {
            let status = error
                .data
                .as_ref()
                .and_then(|data| data.get("httpStatus"))
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok())
                .unwrap_or_else(|| rpc_error_http_status(error.code, error.data.as_ref()));
            let mut body = Map::new();
            body.insert("error".into(), Value::String(error.message));
            if let Some(data) = error.data.filter(|data| !data.is_null()) {
                body.insert("data".into(), data);
            }
            return Ok(AcpHttpResponse {
                status,
                body: Some(Value::Object(body)),
            });
        }
        Ok(AcpHttpResponse {
            status: 200,
            body: response.result,
        })
    }

    /// Open the resumable session `/acp` event stream. The stream reader also
    /// resolves replies routed by the daemon to this session and projects
    /// permission requests to `permission_request` events.
    pub async fn subscribe_events(
        &self,
        session_id: impl Into<String>,
        options: AcpSubscribeOptions,
    ) -> Result<AcpEventSubscription, AcpHttpError> {
        self.ensure_initialized().await?;
        let session_id = session_id.into();
        let (generation, cancellation, mut cancel_receiver, lease) = {
            let mut state = lock_recover(&self.inner.state);
            if state
                .active_session_subscriptions
                .get(&session_id)
                .copied()
                .unwrap_or_default()
                > 0
            {
                return Err(AcpHttpError::SessionAlreadySubscribed(session_id));
            }
            if let Some(pump) = state.session_pumps.remove(&session_id) {
                pump.cancel.send_replace(true);
                if let Some(task) = pump.task {
                    task.abort();
                }
            }
            state
                .active_session_subscriptions
                .insert(session_id.clone(), 1);
            let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
            let (cancellation, cancel_receiver) = watch::channel(false);
            state
                .active_session_cancels
                .insert(session_id.clone(), cancellation.clone());
            let lease = Arc::new(SessionLease {
                inner: Arc::clone(&self.inner),
                session_id: session_id.clone(),
                generation,
                released: AtomicBool::new(false),
            });
            (generation, cancellation, cancel_receiver, lease)
        };
        let mut external_receiver = options
            .cancellation
            .as_ref()
            .map(|token| token.sender.subscribe());
        if options
            .cancellation
            .as_ref()
            .is_some_and(AcpCancellation::is_cancelled)
        {
            lease.release();
            return Err(AcpHttpError::Cancelled);
        }
        let mut dispose_receiver = self.inner.disposed_sender.subscribe();
        let mut headers = self.stream_headers(
            Some(&session_id),
            options.last_event_id,
            options.epoch.as_deref(),
        )?;
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let connect_timeout = options
            .connect_timeout
            .filter(|duration| !duration.is_zero());
        let get = self
            .inner
            .client
            .get(endpoint(&self.inner.base_url, "acp")?)
            .headers(headers)
            .send();
        let response = match connect_timeout {
            Some(duration) => tokio::select! {
                biased;
                _ = wait_for_cancel(&mut dispose_receiver) => { lease.release(); return Err(AcpHttpError::Closed); },
                _ = wait_for_cancel(&mut cancel_receiver) => { lease.release(); return Err(AcpHttpError::Cancelled); },
                _ = wait_optional_cancel(external_receiver.as_mut()) => { lease.release(); return Err(AcpHttpError::Cancelled); },
                _ = sleep(duration) => { lease.release(); return Err(AcpHttpError::ConnectTimeout); },
                response = get => response.map_err(AcpHttpError::Request)?,
            },
            None => tokio::select! {
                biased;
                _ = wait_for_cancel(&mut dispose_receiver) => { lease.release(); return Err(AcpHttpError::Closed); },
                _ = wait_for_cancel(&mut cancel_receiver) => { lease.release(); return Err(AcpHttpError::Cancelled); },
                _ = wait_optional_cancel(external_receiver.as_mut()) => { lease.release(); return Err(AcpHttpError::Cancelled); },
                response = get => response.map_err(AcpHttpError::Request)?,
            },
        };
        let response = match validate_event_response(
            response,
            "GET /acp (session stream)",
            &mut dispose_receiver,
            Some(&mut cancel_receiver),
            external_receiver.as_mut(),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                lease.release();
                return Err(error);
            }
        };
        let metadata = AcpStreamMetadata {
            epoch: response
                .headers()
                .get("x-qwen-event-epoch")
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
        };
        let (sender, receiver) = mpsc::channel(
            options
                .queue_capacity
                .unwrap_or(DEFAULT_ACP_EVENT_QUEUE_CAPACITY)
                .clamp(1, 4096),
        );
        let idle_timeout = options
            .idle_timeout
            .or(self.inner.idle_timeout)
            .filter(|duration| !duration.is_zero());
        let inner = Arc::clone(&self.inner);
        let caller_cancellation = options.cancellation.clone();
        let stream_caller_cancellation = caller_cancellation.clone();
        let task_lease = Arc::clone(&lease);
        tokio::spawn(async move {
            run_session_stream(
                inner,
                response,
                session_id,
                sender,
                cancel_receiver,
                external_receiver,
                caller_cancellation,
                dispose_receiver,
                idle_timeout,
            )
            .await;
            task_lease.release();
        });
        let _ = generation;
        Ok(AcpEventSubscription {
            metadata,
            events: AcpEventStream {
                receiver,
                cancel: cancellation,
                lease,
                _caller_cancellation: stream_caller_cancellation,
            },
        })
    }

    /// Idempotently cancel all reply streams and reject pending requests.
    pub fn dispose(&self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.disposed_sender.send_replace(true);
        let mut state = lock_recover(&self.inner.state);
        if let Some(pump) = state.connection_pump.take() {
            pump.cancel.send_replace(true);
            if let Some(task) = pump.task {
                task.abort();
            }
        }
        for (_, pump) in state.session_pumps.drain() {
            pump.cancel.send_replace(true);
            if let Some(task) = pump.task {
                task.abort();
            }
        }
        for (_, cancel) in state.active_session_cancels.drain() {
            cancel.send_replace(true);
        }
        for (_, pending) in state.pending.drain() {
            let _ = pending.sender.send(Err(AcpHttpError::Closed));
        }
        state.initialized = false;
    }

    fn register_pending(
        &self,
        scope: ReplyScope,
    ) -> Result<
        (
            u64,
            oneshot::Receiver<Result<Value, AcpHttpError>>,
            PendingGuard,
        ),
        AcpHttpError,
    > {
        let mut state = lock_recover(&self.inner.state);
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpHttpError::Closed);
        }
        if state.pending.len() >= MAX_PENDING_ACP_REQUESTS {
            return Err(AcpHttpError::TooManyPending);
        }
        let id = state.next_request_id;
        state.next_request_id = if id >= MAX_SAFE_EVENT_ID { 1 } else { id + 1 };
        let (sender, receiver) = oneshot::channel();
        state.pending.insert(id, PendingRequest { scope, sender });
        Ok((
            id,
            receiver,
            PendingGuard {
                inner: Arc::clone(&self.inner),
                id,
            },
        ))
    }

    async fn ensure_connection_pump(&self) -> Result<(), AcpHttpError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpHttpError::Closed);
        }
        let (generation, cancel, mut ready_rx, start_task) = {
            let mut state = lock_recover(&self.inner.state);
            if let Some(entry) = &mut state.connection_pump {
                (
                    entry.generation,
                    entry.cancel.clone(),
                    entry.ready.clone(),
                    None,
                )
            } else {
                let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
                let (cancel, _cancel_rx) = watch::channel(false);
                let (ready_tx, ready_rx) = watch::channel(None);
                state.connection_pump = Some(PumpEntry {
                    generation,
                    cancel: cancel.clone(),
                    ready: ready_rx.clone(),
                    references: 0,
                    task: None,
                });
                (generation, cancel, ready_rx, Some(ready_tx))
            }
        };
        if let Some(ready_tx) = start_task {
            let inner = Arc::clone(&self.inner);
            let cancel_rx = cancel.subscribe();
            let task = tokio::spawn(async move {
                let result = run_connection_pump(inner.clone(), cancel_rx, ready_tx).await;
                finish_connection_pump(&inner, generation, result);
            });
            let mut state = lock_recover(&self.inner.state);
            if let Some(entry) = state
                .connection_pump
                .as_mut()
                .filter(|entry| entry.generation == generation)
            {
                entry.task = Some(task);
            } else {
                task.abort();
            }
        }
        await_pump_ready(&mut ready_rx, &self.inner.disposed_sender).await
    }

    async fn ensure_session_reply_pump(
        &self,
        session_id: &str,
    ) -> Result<SessionPumpLease, AcpHttpError> {
        let (generation, cancel, mut ready_rx, start_task) = {
            let mut state = lock_recover(&self.inner.state);
            if state
                .active_session_subscriptions
                .get(session_id)
                .copied()
                .unwrap_or_default()
                > 0
            {
                return Ok(SessionPumpLease {
                    inner: Arc::clone(&self.inner),
                    session_id: session_id.to_owned(),
                    generation: 0,
                    released: true,
                });
            }
            if let Some(entry) = state.session_pumps.get_mut(session_id) {
                entry.references += 1;
                (
                    entry.generation,
                    entry.cancel.clone(),
                    entry.ready.clone(),
                    None,
                )
            } else {
                let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
                let (cancel, _cancel_rx) = watch::channel(false);
                let (ready_tx, ready_rx) = watch::channel(None);
                state.session_pumps.insert(
                    session_id.to_owned(),
                    PumpEntry {
                        generation,
                        cancel: cancel.clone(),
                        ready: ready_rx.clone(),
                        references: 1,
                        task: None,
                    },
                );
                (generation, cancel, ready_rx, Some(ready_tx))
            }
        };
        let lease = SessionPumpLease {
            inner: Arc::clone(&self.inner),
            session_id: session_id.to_owned(),
            generation,
            released: false,
        };
        if let Some(ready_tx) = start_task {
            let inner = Arc::clone(&self.inner);
            let sid = session_id.to_owned();
            let cancel_rx = cancel.subscribe();
            let task = tokio::spawn(async move {
                let result = run_session_reply_pump(inner.clone(), &sid, cancel_rx, ready_tx).await;
                finish_session_pump(&inner, &sid, generation, result);
            });
            let mut state = lock_recover(&self.inner.state);
            if let Some(entry) = state
                .session_pumps
                .get_mut(session_id)
                .filter(|entry| entry.generation == generation)
            {
                entry.task = Some(task);
            } else {
                task.abort();
            }
        }
        if let Err(error) = await_pump_ready(&mut ready_rx, &self.inner.disposed_sender).await {
            drop(lease);
            return Err(error);
        }
        Ok(lease)
    }

    fn request_headers(&self, extra: Option<HeaderMap>) -> Result<HeaderMap, AcpHttpError> {
        let mut headers = json_headers(&self.inner.token)?;
        if let Some(connection_id) = lock_recover(&self.inner.state).connection_id.as_deref() {
            headers.insert(
                "acp-connection-id",
                checked_header("acp-connection-id", connection_id)?,
            );
        }
        if let Some(extra) = extra {
            for (name, value) in extra.iter() {
                headers.insert(name.clone(), value.clone());
            }
        }
        Ok(headers)
    }

    fn stream_headers(
        &self,
        session_id: Option<&str>,
        last_event_id: Option<u64>,
        epoch: Option<&str>,
    ) -> Result<HeaderMap, AcpHttpError> {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        if let Some(token) = self.inner.token.as_ref().filter(|token| !token.is_empty()) {
            headers.insert(
                AUTHORIZATION,
                checked_header("authorization", &format!("Bearer {token}"))?,
            );
        }
        if let Some(connection_id) = lock_recover(&self.inner.state).connection_id.as_deref() {
            headers.insert(
                "acp-connection-id",
                checked_header("acp-connection-id", connection_id)?,
            );
        }
        if let Some(session_id) = session_id {
            headers.insert(
                "acp-session-id",
                checked_header("acp-session-id", session_id)?,
            );
        }
        if let Some(id) = last_event_id {
            headers.insert(
                "last-event-id",
                checked_header("last-event-id", &id.to_string())?,
            );
            if let Some(epoch) = epoch {
                headers.insert(
                    "x-qwen-event-epoch",
                    checked_header("x-qwen-event-epoch", epoch)?,
                );
            }
        }
        Ok(headers)
    }
}

async fn run_connection_pump(
    inner: Arc<TransportInner>,
    mut cancel_receiver: watch::Receiver<bool>,
    ready_sender: watch::Sender<Option<Result<(), String>>>,
) -> Result<(), String> {
    let response =
        match open_sse_response(&inner, None, None, None, &mut cancel_receiver, None).await {
            Ok(Some(response)) => response,
            Ok(None) => return Ok(()),
            Err(error) => {
                let message = error.to_string();
                ready_sender.send_replace(Some(Err(message.clone())));
                return Err(message);
            }
        };
    ready_sender.send_replace(Some(Ok(())));
    let mut frames = Box::pin(parse_sse_json_stream(
        response.bytes_stream(),
        inner.idle_timeout,
    ));
    loop {
        let mut disposed = inner.disposed_sender.subscribe();
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut cancel_receiver) => return Ok(()),
            _ = wait_for_cancel(&mut disposed) => return Ok(()),
            frame = frames.next() => match frame {
                Some(Ok(value)) => {
                    deliver_response(&inner, value, None);
                }
                Some(Err(error)) => return Err(error.to_string()),
                None => return Err("connection ACP SSE stream closed unexpectedly".into()),
            }
        }
    }
}

async fn run_session_reply_pump(
    inner: Arc<TransportInner>,
    session_id: &str,
    mut cancel_receiver: watch::Receiver<bool>,
    ready_sender: watch::Sender<Option<Result<(), String>>>,
) -> Result<(), String> {
    let response = match open_sse_response(
        &inner,
        Some(session_id),
        None,
        None,
        &mut cancel_receiver,
        None,
    )
    .await
    {
        Ok(Some(response)) => response,
        Ok(None) => return Ok(()),
        Err(error) => {
            let message = error.to_string();
            ready_sender.send_replace(Some(Err(message.clone())));
            return Err(message);
        }
    };
    ready_sender.send_replace(Some(Ok(())));
    let mut frames = Box::pin(parse_sse_json_stream(
        response.bytes_stream(),
        inner.idle_timeout,
    ));
    loop {
        let mut disposed = inner.disposed_sender.subscribe();
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut cancel_receiver) => return Ok(()),
            _ = wait_for_cancel(&mut disposed) => return Ok(()),
            frame = frames.next() => match frame {
                Some(Ok(value)) => {
                    deliver_response(&inner, value, Some(session_id));
                }
                Some(Err(error)) => return Err(error.to_string()),
                None => return Err("session reply ACP SSE stream closed unexpectedly".into()),
            }
        }
    }
}

async fn run_session_stream(
    inner: Arc<TransportInner>,
    response: reqwest::Response,
    session_id: String,
    sender: mpsc::Sender<Result<DaemonEvent, AcpHttpError>>,
    mut cancel_receiver: watch::Receiver<bool>,
    mut external_receiver: Option<watch::Receiver<bool>>,
    caller_cancellation: Option<AcpCancellation>,
    mut dispose_receiver: watch::Receiver<bool>,
    idle_timeout: Option<Duration>,
) {
    // Retain caller token until the reader exits. Dropping the caller's handle
    // is not itself cancellation.
    let _caller_cancellation = caller_cancellation;
    let mut frames = Box::pin(parse_sse_frames(response.bytes_stream(), idle_timeout));
    loop {
        let next = tokio::select! {
            biased;
            _ = wait_for_cancel(&mut cancel_receiver) => return,
            _ = wait_optional_cancel(external_receiver.as_mut()) => {
                let _ = sender.try_send(Err(AcpHttpError::Cancelled));
                return;
            }
            _ = wait_for_cancel(&mut dispose_receiver) => return,
            frame = frames.next() => frame,
        };
        let Some(next) = next else {
            let _ = sender.try_send(Err(AcpHttpError::ReplyStreamClosed));
            return;
        };
        let frame = match next {
            Ok(frame) => frame,
            Err(error) => {
                let _ = sender.try_send(Err(AcpHttpError::Sse(error)));
                return;
            }
        };
        if deliver_response(&inner, frame.data.clone(), Some(&session_id)) {
            continue;
        }
        let Some(object) = frame.data.as_object() else {
            continue;
        };
        let method = object.get("method").and_then(Value::as_str);
        if method == Some("session/request_permission") {
            if let Some(event) = permission_request_event(object, frame.id) {
                let sent = tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut cancel_receiver) => return,
                    _ = wait_optional_cancel(external_receiver.as_mut()) => return,
                    _ = wait_for_cancel(&mut dispose_receiver) => return,
                    result = sender.send(Ok(event)) => result,
                };
                if sent.is_err() {
                    return;
                }
            }
            continue;
        }
        if object.get("id").is_none() {
            if let Some(method) = method {
                if let Some(mut event) = denormalize_acp_notification(method, object.get("params"))
                {
                    // The generic ACP denormalizer assigns a local ordering
                    // token. On this resumable stream, the SSE bus cursor is
                    // authoritative and must be the ID consumers persist.
                    event.id = frame.id;
                    if let Some(raw) = event.raw.as_object_mut() {
                        if let Some(id) = frame.id {
                            raw.insert("id".into(), Value::from(id));
                        } else {
                            raw.remove("id");
                        }
                    }
                    let sent = tokio::select! {
                        biased;
                        _ = wait_for_cancel(&mut cancel_receiver) => return,
                        _ = wait_optional_cancel(external_receiver.as_mut()) => return,
                        _ = wait_for_cancel(&mut dispose_receiver) => return,
                        result = sender.send(Ok(event)) => result,
                    };
                    if sent.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

async fn open_sse_response(
    inner: &TransportInner,
    session_id: Option<&str>,
    last_event_id: Option<u64>,
    epoch: Option<&str>,
    cancel_receiver: &mut watch::Receiver<bool>,
    connect_timeout: Option<Duration>,
) -> Result<Option<reqwest::Response>, AcpHttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    if let Some(token) = inner.token.as_ref().filter(|token| !token.is_empty()) {
        headers.insert(
            AUTHORIZATION,
            checked_header("authorization", &format!("Bearer {token}"))?,
        );
    }
    if let Some(connection_id) = lock_recover(&inner.state).connection_id.as_deref() {
        headers.insert(
            "acp-connection-id",
            checked_header("acp-connection-id", connection_id)?,
        );
    }
    if let Some(session_id) = session_id {
        headers.insert(
            "acp-session-id",
            checked_header("acp-session-id", session_id)?,
        );
    }
    if let Some(id) = last_event_id {
        headers.insert(
            "last-event-id",
            checked_header("last-event-id", &id.to_string())?,
        );
        if let Some(epoch) = epoch {
            headers.insert(
                "x-qwen-event-epoch",
                checked_header("x-qwen-event-epoch", epoch)?,
            );
        }
    }
    let request = inner
        .client
        .get(endpoint(&inner.base_url, "acp")?)
        .headers(headers)
        .send();
    let response = if let Some(duration) = connect_timeout.filter(|duration| !duration.is_zero()) {
        tokio::select! {
            biased;
            _ = wait_for_cancel(cancel_receiver) => return Ok(None),
            _ = sleep(duration) => return Err(AcpHttpError::ConnectTimeout),
            result = request => result.map_err(AcpHttpError::Request)?,
        }
    } else {
        tokio::select! {
            biased;
            _ = wait_for_cancel(cancel_receiver) => return Ok(None),
            result = request => result.map_err(AcpHttpError::Request)?,
        }
    };
    let mut dispose_receiver = inner.disposed_sender.subscribe();
    validate_event_response(
        response,
        "GET /acp",
        &mut dispose_receiver,
        Some(cancel_receiver),
        None,
    )
    .await
    .map(Some)
}

async fn validate_event_response(
    response: reqwest::Response,
    label: &str,
    dispose_receiver: &mut watch::Receiver<bool>,
    cancellation_receiver: Option<&mut watch::Receiver<bool>>,
    external_receiver: Option<&mut watch::Receiver<bool>>,
) -> Result<reqwest::Response, AcpHttpError> {
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        let body = read_http_error_body(
            response,
            dispose_receiver,
            cancellation_receiver,
            external_receiver,
        )
        .await?;
        let detail = body
            .as_ref()
            .and_then(|value| value.get("error"))
            .map(js_string)
            .unwrap_or_else(|| format!("HTTP {status}"));
        return Err(AcpHttpError::Http {
            status,
            detail: format!("{label}: {detail}"),
            body,
        });
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    if !content_type
        .to_ascii_lowercase()
        .contains("text/event-stream")
    {
        return Err(AcpHttpError::InvalidContentType { content_type });
    }
    Ok(response)
}

async fn read_http_error_body(
    response: reqwest::Response,
    dispose_receiver: &mut watch::Receiver<bool>,
    cancellation_receiver: Option<&mut watch::Receiver<bool>>,
    external_receiver: Option<&mut watch::Receiver<bool>>,
) -> Result<Option<Value>, AcpHttpError> {
    const MAX_BODY: usize = 1024 * 1024;
    let mut cancellation_receiver = cancellation_receiver;
    let mut external_receiver = external_receiver;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = wait_for_cancel(dispose_receiver) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(cancellation_receiver.as_deref_mut()) => return Err(AcpHttpError::Cancelled),
            _ = wait_optional_cancel(external_receiver.as_deref_mut()) => return Err(AcpHttpError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let Ok(chunk) = chunk else {
            return Ok(None);
        };
        let remaining = MAX_BODY.saturating_sub(bytes.len());
        if chunk.len() > remaining {
            bytes.extend_from_slice(&chunk[..remaining]);
            bytes.extend_from_slice(b"\n...[truncated]");
            break;
        }
        bytes.extend_from_slice(&chunk);
        if bytes.len() == MAX_BODY {
            bytes.extend_from_slice(b"\n...[truncated]");
            break;
        }
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();
    Ok(Some(
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
}

async fn read_response_text(
    response: reqwest::Response,
    dispose_receiver: &mut watch::Receiver<bool>,
    cancellation_receiver: Option<&mut watch::Receiver<bool>>,
) -> Result<String, AcpHttpError> {
    const MAX_BODY: usize = 1024 * 1024;
    let mut cancellation_receiver = cancellation_receiver;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    let mut truncated = false;
    loop {
        let next = tokio::select! {
            biased;
            _ = wait_for_cancel(dispose_receiver) => return Err(AcpHttpError::Closed),
            _ = wait_optional_cancel(cancellation_receiver.as_deref_mut()) => return Err(AcpHttpError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(AcpHttpError::Request)?;
        let remaining = MAX_BODY.saturating_sub(bytes.len());
        if chunk.len() > remaining {
            bytes.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        bytes.extend_from_slice(&chunk);
        if bytes.len() == MAX_BODY {
            truncated = true;
            break;
        }
    }
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str("\n...[truncated]");
    }
    Ok(text)
}

fn deliver_response(inner: &TransportInner, value: Value, source_session: Option<&str>) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.get("method").and_then(Value::as_str).is_some() {
        return false;
    }
    let Some(id) = object.get("id").and_then(Value::as_u64) else {
        return false;
    };
    let mut state = lock_recover(&inner.state);
    let can_resolve =
        state
            .pending
            .get(&id)
            .is_some_and(|pending| match (&pending.scope, source_session) {
                (ReplyScope::Connection, None) => true,
                (ReplyScope::Connection, Some(_)) => true,
                (ReplyScope::Session(expected), Some(actual)) => expected == actual,
                (ReplyScope::Session(_), None) => false,
            });
    if !can_resolve {
        return false;
    }
    if let Some(pending) = state.pending.remove(&id) {
        let _ = pending.sender.send(Ok(value));
        return true;
    }
    false
}

fn finish_connection_pump(inner: &TransportInner, generation: u64, result: Result<(), String>) {
    let mut state = lock_recover(&inner.state);
    if state
        .connection_pump
        .as_ref()
        .is_some_and(|entry| entry.generation == generation)
    {
        state.connection_pump.take();
    }
    if let Err(reason) = result {
        if !inner.disposed.load(Ordering::Acquire) {
            reject_pending_matching(
                &mut state,
                |scope| matches!(scope, ReplyScope::Connection),
                || AcpHttpError::ReplyStreamFailed(reason.clone()),
            );
        }
    }
}

fn finish_session_pump(
    inner: &TransportInner,
    session_id: &str,
    generation: u64,
    result: Result<(), String>,
) {
    let mut state = lock_recover(&inner.state);
    if state
        .session_pumps
        .get(session_id)
        .is_some_and(|entry| entry.generation == generation)
    {
        state.session_pumps.remove(session_id);
    }
    if let Err(reason) = result {
        if !inner.disposed.load(Ordering::Acquire) {
            reject_pending_for_session(
                &mut state,
                session_id,
                AcpHttpError::ReplyStreamFailed(reason),
            );
        }
    }
}

fn reject_pending_for_session(state: &mut TransportState, session_id: &str, error: AcpHttpError) {
    let message = error.to_string();
    reject_pending_matching(
        state,
        |scope| matches!(scope, ReplyScope::Session(id) if id == session_id),
        || AcpHttpError::ReplyStreamFailed(message.clone()),
    );
}

fn reject_pending_matching(
    state: &mut TransportState,
    mut predicate: impl FnMut(&ReplyScope) -> bool,
    make_error: impl Fn() -> AcpHttpError,
) {
    let ids: Vec<_> = state
        .pending
        .iter()
        .filter_map(|(id, pending)| predicate(&pending.scope).then_some(*id))
        .collect();
    for id in ids {
        if let Some(pending) = state.pending.remove(&id) {
            let _ = pending.sender.send(Err(make_error()));
        }
    }
}

async fn await_pump_ready(
    receiver: &mut watch::Receiver<Option<Result<(), String>>>,
    disposed: &watch::Sender<bool>,
) -> Result<(), AcpHttpError> {
    let mut disposed_receiver = disposed.subscribe();
    loop {
        if let Some(result) = receiver.borrow().clone() {
            return result.map_err(AcpHttpError::ReplyStreamFailed);
        }
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut disposed_receiver) => return Err(AcpHttpError::Closed),
            changed = receiver.changed() => {
                if changed.is_err() {
                    return Err(AcpHttpError::ReplyTaskEnded);
                }
            }
        }
    }
}

async fn wait_for_cancel(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_optional_cancel(receiver: Option<&mut watch::Receiver<bool>>) {
    match receiver {
        Some(receiver) => wait_for_cancel(receiver).await,
        None => pending::<()>().await,
    }
}

fn endpoint(base_url: &Url, suffix: &str) -> Result<Url, AcpHttpError> {
    let mut url = base_url.clone();
    url.set_fragment(None);
    let mut segments = url.path_segments_mut().map_err(|_| {
        AcpHttpError::InvalidBaseUrl("base URL cannot contain path segments".into())
    })?;
    segments.pop_if_empty().push(suffix);
    drop(segments);
    Ok(url)
}

fn json_headers(token: &Option<String>) -> Result<HeaderMap, AcpHttpError> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(token) = token.as_ref().filter(|token| !token.is_empty()) {
        headers.insert(
            AUTHORIZATION,
            checked_header("authorization", &format!("Bearer {token}"))?,
        );
    }
    Ok(headers)
}

fn checked_header(name: &'static str, value: &str) -> Result<HeaderValue, AcpHttpError> {
    HeaderValue::from_str(value).map_err(|source| AcpHttpError::InvalidHeader { name, source })
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn parse_rpc_response(raw: Value) -> Result<AcpRpcResponse, AcpHttpError> {
    let id = raw
        .get("id")
        .and_then(Value::as_u64)
        .ok_or(AcpHttpError::InvalidResponse)?;
    let error = raw
        .get("error")
        .and_then(Value::as_object)
        .map(|error| AcpRpcError {
            code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("ACP JSON-RPC error")
                .to_owned(),
            data: error.get("data").cloned(),
        });
    Ok(AcpRpcResponse {
        id,
        result: raw.get("result").cloned(),
        error,
        raw,
    })
}

fn extract_string_path(root: &Value, path: &[&str]) -> Option<String> {
    let mut current = root;
    for part in path {
        current = current.get(*part)?;
    }
    current.as_str().map(str::to_owned)
}

fn object_value(value: Value) -> Value {
    if value.is_object() {
        value
    } else {
        Value::Object(Map::new())
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "null".into(),
        Value::Object(_) => "[object Object]".into(),
        Value::Array(values) => values.iter().map(js_string).collect::<Vec<_>>().join(","),
        _ => value.to_string(),
    }
}

pub(crate) fn rpc_error_http_status(code: i64, data: Option<&Value>) -> u16 {
    if data
        .and_then(|value| value.get("errorKind"))
        .and_then(Value::as_str)
        .is_some_and(|kind| {
            matches!(
                kind,
                "session_archived" | "session_conflict" | "session_archiving"
            )
        })
    {
        return 409;
    }
    match code {
        -32601 => 404,
        -32600 | -32602 | -32700 => 400,
        _ => 500,
    }
}

/// Map an HTTP method/path/query/body to the same ACP method and parameter
/// object used by the TypeScript SDK's `acpRouteTable`.
pub fn map_route(http_method: &str, path: &str, query: &str, body: &Value) -> Option<AcpRouteCall> {
    let method = http_method.to_ascii_uppercase();
    let (path, embedded_query) = if let Ok(url) = Url::parse(path) {
        (
            url.path().to_owned(),
            url.query().unwrap_or_default().to_owned(),
        )
    } else {
        (
            path_without_query(path).to_owned(),
            path_query(path).to_owned(),
        )
    };
    let query = if query.is_empty() {
        embedded_query.as_str()
    } else {
        query.strip_prefix('?').unwrap_or(query)
    };
    let segments = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(decode_uri_component)
        .collect::<Option<Vec<_>>>()?;
    let q = QueryValues::parse(query);

    if method == "POST" && path_matches(&segments, &["session"]) {
        return route("session/new", session_new_params(body), false);
    }
    if segments.len() == 3
        && segments[0] == "session"
        && segments[2] == "prompt"
        && method == "POST"
    {
        return route(
            "session/prompt",
            path_session_body(body, &segments[1]),
            false,
        );
    }
    if segments.len() == 3
        && segments[0] == "session"
        && segments[2] == "cancel"
        && method == "POST"
    {
        return route("session/cancel", json!({ "sessionId": segments[1] }), true);
    }
    if segments.len() == 2 && segments[0] == "session" && method == "DELETE" {
        return route("session/close", json!({ "sessionId": segments[1] }), false);
    }
    if segments.len() == 3 && segments[0] == "session" && method == "POST" {
        let method = match segments[2].as_str() {
            "load" => "session/load",
            "resume" => "session/resume",
            "model" => "session/set_model",
            "heartbeat" => "_qwen/session/heartbeat",
            "artifacts" => "_qwen/session/artifacts/add",
            "recap" => "_qwen/session/recap",
            "btw" => "_qwen/session/btw",
            "shell" => "_qwen/session/shell",
            "branch" => "session/fork",
            "detach" => "_qwen/session/detach",
            _ => "",
        };
        if !method.is_empty() {
            return route(method, path_session_body(body, &segments[1]), false);
        }
    }
    if segments.len() == 3 && segments[0] == "session" && method == "PATCH" {
        let rpc_method = match segments[2].as_str() {
            "metadata" => "_qwen/session/update_metadata",
            "organization" => "_qwen/session/update_organization",
            _ => "",
        };
        if !rpc_method.is_empty() {
            return route(rpc_method, path_session_body(body, &segments[1]), false);
        }
    }
    if segments.len() == 3 && segments[0] == "session" && method == "GET" {
        let mut params = json!({ "sessionId": segments[1] });
        match segments[2].as_str() {
            "artifacts" => return route("_qwen/session/artifacts", params, false),
            "context" => return route("_qwen/session/context", params, false),
            "context-usage" => {
                if let Some(value) = q.boolean("detail") {
                    params["detail"] = Value::Bool(value);
                }
                return route("_qwen/session/context_usage", params, false);
            }
            "supported-commands" => {
                return route("_qwen/session/supported_commands", params, false);
            }
            "tasks" => return route("_qwen/session/tasks", params, false),
            "lsp" => return route("_qwen/session/lsp", params, false),
            _ => {}
        }
    }
    if segments.len() == 4
        && segments[0] == "session"
        && segments[2] == "permission"
        && method == "POST"
    {
        let mut params = body_record(body);
        params.insert("sessionId".into(), Value::String(segments[1].clone()));
        params.insert("requestId".into(), Value::String(segments[3].clone()));
        return route("session/permission", Value::Object(params), false);
    }
    if segments.len() == 2 && segments[0] == "permission" && method == "POST" {
        let mut params = body_record(body);
        params.insert("requestId".into(), Value::String(segments[1].clone()));
        return route("session/permission", Value::Object(params), false);
    }
    if segments.len() == 3
        && segments[0] == "session"
        && segments[2] == "artifacts"
        && method == "POST"
    {
        return route(
            "_qwen/session/artifacts/add",
            path_session_body(body, &segments[1]),
            false,
        );
    }
    if segments.len() == 4
        && segments[0] == "session"
        && segments[2] == "artifacts"
        && method == "DELETE"
    {
        let mut params = json!({ "sessionId": segments[1], "artifactId": segments[3] });
        if let Some(client_id) = body.get("clientId").and_then(Value::as_str) {
            params["clientId"] = Value::String(client_id.to_owned());
        }
        return route("_qwen/session/artifacts/remove", params, false);
    }
    if segments.len() == 1 && segments[0] == "capabilities" && method == "GET" {
        return route("_capabilities", json!({}), false);
    }
    if segments.len() == 1 && segments[0] == "health" && method == "GET" {
        return route("_qwen/health", json!({}), false);
    }

    if segments.len() >= 2 && segments[0] == "workspace" {
        let tail = &segments[1..];
        if tail == ["mcp", "initialize"] && method == "POST" {
            return route(
                "_qwen/workspace/mcp/initialize",
                body_record_value(body),
                false,
            );
        }
        if tail == ["mcp", "reload"] && method == "POST" {
            return route("_qwen/workspace/mcp/reload", body_record_value(body), false);
        }
        if tail == ["mcp"] && method == "GET" {
            return route("_qwen/workspace/mcp", json!({}), false);
        }
        if tail == ["skills"] && method == "GET" {
            return route("_qwen/workspace/skills", json!({}), false);
        }
        if tail == ["providers"] && method == "GET" {
            return route("_qwen/workspace/providers", json!({}), false);
        }
        if tail == ["env"] && method == "GET" {
            return route("_qwen/workspace/env", json!({}), false);
        }
        if tail == ["preflight"] && method == "GET" {
            return route("_qwen/workspace/preflight", json!({}), false);
        }
        if tail == ["init"] && method == "POST" {
            return route("_qwen/workspace/init", body_record_value(body), false);
        }
        if tail == ["trust"] && method == "GET" {
            return route("_qwen/workspace/trust", json!({}), false);
        }
        if tail == ["trust", "request"] && method == "POST" {
            return route(
                "_qwen/workspace/trust/request",
                body_record_value(body),
                false,
            );
        }
        if tail == ["permissions"] && method == "GET" {
            return route("_qwen/workspace/permissions", json!({}), false);
        }
        if tail == ["permissions"] && method == "POST" {
            return route(
                "_qwen/workspace/permissions/set",
                body_record_value(body),
                false,
            );
        }
        if tail == ["voice"] && method == "GET" {
            return route("_qwen/workspace/voice", json!({}), false);
        }
        if tail == ["voice"] && method == "POST" {
            return route("_qwen/workspace/voice/set", body_record_value(body), false);
        }
        if tail == ["setup-github"] && method == "POST" {
            return route(
                "_qwen/workspace/setup-github",
                body_record_value(body),
                false,
            );
        }
        if tail == ["tools"] && method == "GET" {
            return route("_qwen/workspace/tools", json!({}), false);
        }
        if tail == ["memory"] && method == "GET" {
            return route("_qwen/workspace/memory", json!({}), false);
        }
        if tail == ["memory"] && method == "POST" {
            return route(
                "_qwen/workspace/memory/write",
                body_record_value(body),
                false,
            );
        }
        if tail == ["memory", "remember"] && method == "POST" {
            return route(
                "_qwen/workspace/memory/remember",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "memory" && tail[1] == "remember" && method == "GET" {
            return route(
                "_qwen/workspace/memory/remember/get",
                json!({ "taskId": tail[2] }),
                false,
            );
        }
        if tail == ["memory", "forget"] && method == "POST" {
            return route(
                "_qwen/workspace/memory/forget",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "memory" && tail[1] == "forget" && method == "GET" {
            return route(
                "_qwen/workspace/memory/forget/get",
                json!({ "taskId": tail[2] }),
                false,
            );
        }
        if tail == ["memory", "dream"] && method == "POST" {
            return route("_qwen/workspace/memory/dream", json!({}), false);
        }
        if tail.len() == 3 && tail[0] == "memory" && tail[1] == "dream" && method == "GET" {
            return route(
                "_qwen/workspace/memory/dream/get",
                json!({ "taskId": tail[2] }),
                false,
            );
        }
        if tail == ["agents"] && method == "GET" {
            return route("_qwen/workspace/agents/list", json!({}), false);
        }
        if tail == ["agents"] && method == "POST" {
            return route(
                "_qwen/workspace/agents/create",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 2 && tail[0] == "agents" && method == "GET" {
            return route(
                "_qwen/workspace/agents/get",
                json!({ "agentType": tail[1] }),
                false,
            );
        }
        if tail.len() == 2 && tail[0] == "agents" && method == "DELETE" {
            let mut params = body_record(body);
            params.insert("agentType".into(), Value::String(tail[1].clone()));
            return route(
                "_qwen/workspace/agents/delete",
                Value::Object(params),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "mcp" && tail[2] == "tools" && method == "GET" {
            return route(
                "_qwen/workspace/mcp/tools",
                json!({ "serverName": tail[1] }),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "mcp" && tail[2] == "resources" && method == "GET" {
            return route(
                "_qwen/workspace/mcp/resources",
                json!({ "serverName": tail[1] }),
                false,
            );
        }
        if tail == ["mcp", "servers"] && method == "POST" {
            return route(
                "_qwen/workspace/mcp/servers/add",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "mcp" && tail[1] == "servers" && method == "DELETE" {
            let mut params = body_record(body);
            params.insert("name".into(), Value::String(tail[2].clone()));
            return route(
                "_qwen/workspace/mcp/servers/remove",
                Value::Object(params),
                false,
            );
        }
        if tail == ["set-tool-enabled"] && method == "POST" {
            return route(
                "_qwen/workspace/set_tool_enabled",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "mcp" && tail[2] == "restart" && method == "POST" {
            let mut params = body_record(body);
            params.insert("serverName".into(), Value::String(tail[1].clone()));
            return route(
                "_qwen/workspace/restart_mcp_server",
                Value::Object(params),
                false,
            );
        }
        if tail == ["auth", "status"] && method == "GET" {
            return route("_qwen/workspace/auth/status", json!({}), false);
        }
        if tail == ["auth", "device-flow"] && method == "POST" {
            return route(
                "_qwen/workspace/auth/device_flow/start",
                body_record_value(body),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "auth" && tail[1] == "device-flow" && method == "GET" {
            return route(
                "_qwen/workspace/auth/device_flow/get",
                json!({ "id": tail[2] }),
                false,
            );
        }
        if tail.len() == 3 && tail[0] == "auth" && tail[1] == "device-flow" && method == "DELETE" {
            return route(
                "_qwen/workspace/auth/device_flow/cancel",
                json!({ "id": tail[2] }),
                false,
            );
        }

        // Workspace cwd routes match greedily before the generic workspace
        // catch-all, and allow slashes inside the captured cwd.
        if let Some(cwd) = workspace_suffix(&path, "/sessions") {
            if method == "GET" {
                let mut params = Map::new();
                params.insert("workspaceCwd".into(), Value::String(cwd));
                for key in [
                    "cursor",
                    "archiveState",
                    "view",
                    "group",
                    "parentSessionId",
                    "sourceType",
                    "sourceId",
                ] {
                    if let Some(value) = q.string(key) {
                        params.insert(key.into(), Value::String(value));
                    }
                }
                if let Some(value) = q.number("size") {
                    params.insert("_meta".into(), json!({ "size": value }));
                }
                return route("session/list", Value::Object(params), false);
            }
        }
        if let Some(cwd) = workspace_suffix(&path, "/session-groups") {
            if method == "GET" {
                return route(
                    "_qwen/workspace/session_groups/list",
                    json!({ "workspaceCwd": cwd }),
                    false,
                );
            }
            if method == "POST" {
                let mut params = body_record(body);
                params.insert("workspaceCwd".into(), Value::String(cwd));
                return route(
                    "_qwen/workspace/session_groups/create",
                    Value::Object(params),
                    false,
                );
            }
        }
        if let Some((cwd, group_id)) = workspace_group_suffix(&path) {
            if method == "PATCH" {
                let mut params = body_record(body);
                params.insert("workspaceCwd".into(), Value::String(cwd));
                params.insert("groupId".into(), Value::String(group_id));
                return route(
                    "_qwen/workspace/session_groups/update",
                    Value::Object(params),
                    false,
                );
            }
            if method == "DELETE" {
                return route(
                    "_qwen/workspace/session_groups/delete",
                    json!({ "workspaceCwd": cwd, "groupId": group_id }),
                    false,
                );
            }
        }
        if matches!(method.as_str(), "GET" | "POST") {
            let wildcard = path.strip_prefix("/workspace/").unwrap_or_default();
            if !wildcard.is_empty() {
                let decoded = decode_uri_component(wildcard)?;
                let mut params = if method == "POST" {
                    body_record(body)
                } else {
                    Map::new()
                };
                params.insert("path".into(), Value::String(decoded));
                return route("_qwen/workspace", Value::Object(params), false);
            }
        }
    }

    if segments == ["file"] && method == "GET" {
        let mut params = Map::new();
        add_string_query(&mut params, &q, "path");
        add_number_query(&mut params, &q, "maxBytes");
        add_number_query(&mut params, &q, "line");
        add_number_query(&mut params, &q, "limit");
        add_string_query(&mut params, &q, "cursor");
        return route("_qwen/file/read", Value::Object(params), false);
    }
    if segments == ["file", "bytes"] && method == "GET" {
        let mut params = Map::new();
        add_string_query(&mut params, &q, "path");
        add_number_query(&mut params, &q, "offset");
        add_number_query(&mut params, &q, "maxBytes");
        return route("_qwen/file/read_bytes", Value::Object(params), false);
    }
    if segments == ["stat"] && method == "GET" {
        return route("_qwen/file/stat", q_object(&q, &["path"]), false);
    }
    if segments == ["list"] && method == "GET" {
        return route("_qwen/file/list", q_object(&q, &["path"]), false);
    }
    if segments == ["glob"] && method == "GET" {
        return route("_qwen/file/glob", q_object(&q, &["pattern"]), false);
    }
    if segments == ["file", "write"] && method == "POST" {
        return route("_qwen/file/write", body_record_value(body), false);
    }
    if segments == ["file", "edit"] && method == "POST" {
        return route("_qwen/file/edit", body_record_value(body), false);
    }
    if segments == ["sessions", "delete"] && method == "POST" {
        return route("_qwen/sessions/delete", body_record_value(body), false);
    }
    if segments == ["sessions", "archive"] && method == "POST" {
        return route("_qwen/sessions/archive", body_record_value(body), false);
    }
    if segments == ["sessions", "unarchive"] && method == "POST" {
        return route("_qwen/sessions/unarchive", body_record_value(body), false);
    }
    None
}

fn route(method: &str, params: Value, notification: bool) -> Option<AcpRouteCall> {
    Some(AcpRouteCall {
        method: method.to_owned(),
        params,
        notification,
    })
}

fn path_matches(actual: &[String], expected: &[&str]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual == expected)
}

fn path_without_query(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

fn path_query(path: &str) -> &str {
    path.split_once('?')
        .map(|(_, query)| query)
        .unwrap_or_default()
}

fn session_new_params(body: &Value) -> Value {
    let Some(object) = body.as_object() else {
        return json!({});
    };
    let requested_session_id = object.get("sessionId").cloned();
    let mut result = object.clone();
    result.remove("sessionScope");
    result.remove("sessionId");
    let meta = result.remove("_meta");
    if let Some(session_id) = requested_session_id {
        let mut meta = meta
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        meta.insert("qwen-code/sessionId".into(), session_id);
        result.insert("_meta".into(), Value::Object(meta));
    } else if let Some(meta) = meta {
        result.insert("_meta".into(), meta);
    }
    Value::Object(result)
}

fn path_session_body(body: &Value, session_id: &str) -> Value {
    let mut params = body_record(body);
    params.insert("sessionId".into(), Value::String(session_id.to_owned()));
    Value::Object(params)
}

fn body_record(body: &Value) -> Map<String, Value> {
    body.as_object().cloned().unwrap_or_default()
}

fn body_record_value(body: &Value) -> Value {
    Value::Object(body_record(body))
}

fn workspace_suffix(path: &str, suffix: &str) -> Option<String> {
    let path = path.strip_suffix('/').unwrap_or(path);
    let workspace = path.strip_prefix("/workspace/")?;
    let cwd = workspace.strip_suffix(suffix)?;
    if cwd.is_empty() {
        None
    } else {
        decode_uri_component(cwd)
    }
}

fn workspace_group_suffix(path: &str) -> Option<(String, String)> {
    let path = path.strip_suffix('/').unwrap_or(path);
    let workspace = path.strip_prefix("/workspace/")?;
    let (cwd, group) = workspace.rsplit_once("/session-groups/")?;
    if cwd.is_empty() || group.is_empty() || group.contains('/') {
        return None;
    }
    Some((decode_uri_component(cwd)?, decode_uri_component(group)?))
}

fn decode_uri_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = hex_value(*bytes.get(index + 1)?)?;
            let low = hex_value(*bytes.get(index + 2)?)?;
            decoded.push(high * 16 + low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

struct QueryValues(Vec<(String, String)>);

impl QueryValues {
    fn parse(query: &str) -> Self {
        Self(
            url::form_urlencoded::parse(query.as_bytes())
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect(),
        )
    }

    fn string(&self, name: &str) -> Option<String> {
        self.0
            .iter()
            .find_map(|(key, value)| (key == name).then(|| value.clone()))
    }

    fn number(&self, name: &str) -> Option<Value> {
        let value = self.string(name)?;
        if value.is_empty() {
            return None;
        }
        // TypeScript's `Number(value)` returns NaN for an invalid non-empty
        // value; JSON serialization turns that into `null`. Preserve that
        // route behavior so the daemon sees and rejects the malformed field.
        let number = js_number(&value).unwrap_or(f64::NAN);
        Some(
            Number::from_f64(number)
                .map(Value::Number)
                .unwrap_or(Value::Null),
        )
    }

    fn boolean(&self, name: &str) -> Option<bool> {
        let value = self.string(name)?;
        if value.is_empty() {
            None
        } else {
            Some(value == "true")
        }
    }
}

fn js_number(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(0.0);
    }
    let (radix, digits) = if let Some(digits) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        (16, digits)
    } else if let Some(digits) = trimmed
        .strip_prefix("0b")
        .or_else(|| trimmed.strip_prefix("0B"))
    {
        (2, digits)
    } else if let Some(digits) = trimmed
        .strip_prefix("0o")
        .or_else(|| trimmed.strip_prefix("0O"))
    {
        (8, digits)
    } else {
        return trimmed.parse::<f64>().ok();
    };
    u64::from_str_radix(digits, radix)
        .ok()
        .map(|number| number as f64)
}

fn add_string_query(params: &mut Map<String, Value>, query: &QueryValues, name: &str) {
    if let Some(value) = query.string(name) {
        params.insert(name.to_owned(), Value::String(value));
    }
}

fn add_number_query(params: &mut Map<String, Value>, query: &QueryValues, name: &str) {
    if let Some(value) = query.number(name) {
        params.insert(name.to_owned(), value);
    }
}

fn q_object(query: &QueryValues, names: &[&str]) -> Value {
    let mut params = Map::new();
    for name in names {
        add_string_query(&mut params, query, name);
    }
    Value::Object(params)
}

fn permission_request_event(
    message: &Map<String, Value>,
    bus_id: Option<u64>,
) -> Option<DaemonEvent> {
    let params = message.get("params").and_then(Value::as_object);
    let metadata = params
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
        .cloned();
    let request_id = metadata
        .as_ref()?
        .get("qwen")?
        .as_object()?
        .get("requestId")?
        .as_str()?;
    if request_id.is_empty() {
        return None;
    }
    let params = params.cloned().unwrap_or_default();
    let mut data = Map::new();
    data.insert("requestId".into(), Value::String(request_id.to_owned()));
    if let Some(session_id) = params.get("sessionId").and_then(Value::as_str) {
        data.insert("sessionId".into(), Value::String(session_id.to_owned()));
    }
    for key in ["toolCall", "options"] {
        if let Some(value) = params.get(key) {
            data.insert(key.into(), value.clone());
        }
    }
    Some(daemon_event(
        bus_id,
        "permission_request",
        Value::Object(data),
        metadata,
        None,
    ))
}

fn daemon_event(
    id: Option<u64>,
    event_type: &str,
    data: Value,
    metadata: Option<Map<String, Value>>,
    originator_client_id: Option<String>,
) -> DaemonEvent {
    let mut raw = Map::new();
    if let Some(id) = id {
        raw.insert("id".into(), Value::from(id));
    }
    raw.insert("v".into(), Value::from(1));
    raw.insert("type".into(), Value::String(event_type.to_owned()));
    raw.insert("data".into(), data.clone());
    if let Some(metadata) = &metadata {
        raw.insert("_meta".into(), Value::Object(metadata.clone()));
    }
    if let Some(originator) = &originator_client_id {
        raw.insert(
            "originatorClientId".into(),
            Value::String(originator.clone()),
        );
    }
    DaemonEvent {
        id,
        version: 1,
        event_type: event_type.to_owned(),
        data: Some(data),
        prompt_id: None,
        metadata,
        originator_client_id,
        raw: Value::Object(raw),
    }
}
