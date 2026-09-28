//! ACP-over-WebSocket transport for the daemon SDK.
//!
//! A single authenticated WebSocket carries initialization, JSON-RPC requests,
//! responses, and notifications. Request IDs are correlated on a bounded
//! pending map; each event subscriber has its own bounded drop-oldest queue.
//! URL-shaped calls reuse [`crate::daemon_acp_http::map_route`] so the HTTP and
//! WebSocket transports share the TypeScript ACP route table.

use std::collections::{HashMap, VecDeque};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{SinkExt, Stream, StreamExt};
use reqwest::header::AUTHORIZATION;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use url::Url;

use crate::daemon_acp_http::{AcpHttpResponse, map_route, rpc_error_http_status};
use crate::daemon_event_denormalizer::denormalize_acp_notification;
use crate::daemon_sse::DaemonEvent;

/// Maximum outstanding JSON-RPC requests on one WebSocket.
pub const MAX_PENDING_ACP_WS_REQUESTS: usize = 1024;
/// Maximum queued events per subscriber; full queues drop the oldest event.
pub const MAX_ACP_WS_EVENT_QUEUE_CAPACITY: usize = 256;
/// Maximum encoded or received JSON-RPC message size.
pub const MAX_ACP_WS_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

const CONNECT_AND_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(30);
const OUTBOUND_QUEUE_CAPACITY: usize = 128;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Failure from the ACP WebSocket transport.
#[derive(Debug, Error)]
pub enum AcpWsError {
    #[error("ACP WebSocket transport is closed")]
    Closed,
    #[error("ACP WebSocket operation was cancelled")]
    Cancelled,
    #[error("invalid ACP WebSocket URL: {0}")]
    InvalidUrl(String),
    #[error("ACP WebSocket connection failed: {0}")]
    Connect(String),
    #[error("ACP WebSocket closed: {0}")]
    Connection(String),
    #[error("ACP WebSocket initialize timed out after 30 seconds")]
    InitializeTimeout,
    #[error("ACP WebSocket initialize failed: {0}")]
    Initialize(String),
    #[error("ACP WebSocket request queue is full (limit {MAX_PENDING_ACP_WS_REQUESTS})")]
    TooManyPending,
    #[error("ACP WebSocket outbound queue is full")]
    OutboundQueueFull,
    #[error("ACP WebSocket message exceeds the {MAX_ACP_WS_MESSAGE_BYTES}-byte limit")]
    MessageTooLarge,
    #[error("ACP WebSocket request ID space is exhausted")]
    RequestIdExhausted,
    #[error("ACP WebSocket response was malformed")]
    InvalidResponse,
    #[error("invalid Authorization header for ACP WebSocket: {0}")]
    InvalidAuthorizationHeader(String),
    #[error("ACP WebSocket event stream closed: {0}")]
    StreamClosed(String),
}

/// Caller-owned cancellation handle for requests and event subscriptions.
#[derive(Clone, Debug)]
pub struct AcpWsCancellation {
    sender: watch::Sender<bool>,
}

impl Default for AcpWsCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl AcpWsCancellation {
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

/// Per-route options. The TypeScript transport forwards only the client ID
/// request header into JSON-RPC `_meta`; other HTTP headers are not portable.
#[derive(Clone, Debug, Default)]
pub struct AcpWsRequestOptions {
    pub cancellation: Option<AcpWsCancellation>,
    pub client_id: Option<String>,
}

/// Capabilities learned during initialize, retained for an ACP-only fallback.
#[derive(Clone, Debug, Default)]
pub struct AcpWsMetadata {
    pub initialize_result: Option<Value>,
}

/// ACP WebSocket transport with lazy authenticated connection and handshake.
#[derive(Clone)]
pub struct AcpWsTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    ws_url: Url,
    token: Option<String>,
    rest_client: reqwest::Client,
    disposed: AtomicBool,
    disposed_sender: watch::Sender<bool>,
    initialize_lock: AsyncMutex<()>,
    state: Mutex<TransportState>,
    next_generation: std::sync::atomic::AtomicU64,
}

struct TransportState {
    generation: u64,
    connected: bool,
    init_result: Option<Value>,
    next_request_id: u64,
    next_subscriber_id: u64,
    pending: HashMap<u64, oneshot::Sender<Result<Value, String>>>,
    subscribers: HashMap<u64, Subscriber>,
    outbound: Option<mpsc::Sender<Message>>,
    worker: Option<JoinHandle<()>>,
}

struct Subscriber {
    session_id: String,
    queue: Arc<EventQueue>,
}

struct EventQueue {
    state: Mutex<EventQueueState>,
    notify: Arc<Notify>,
    capacity: usize,
}

struct EventQueueState {
    events: VecDeque<DaemonEvent>,
    closed: bool,
    close_error: Option<String>,
    error_reported: bool,
}

impl EventQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(EventQueueState {
                events: VecDeque::new(),
                closed: false,
                close_error: None,
                error_reported: false,
            }),
            notify: Arc::new(Notify::new()),
            capacity: capacity.clamp(1, MAX_ACP_WS_EVENT_QUEUE_CAPACITY),
        }
    }

    fn push(&self, event: DaemonEvent) {
        let mut state = lock_recover(&self.state);
        if state.closed {
            return;
        }
        if state.events.len() == self.capacity {
            state.events.pop_front();
        }
        state.events.push_back(event);
        self.notify.notify_one();
    }

    fn close(&self, error: Option<String>) {
        let mut state = lock_recover(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        state.events.clear();
        state.close_error = error;
        self.notify.notify_one();
    }
}

/// Bounded event stream. Dropping it unregisters its notification listener.
pub struct AcpWsEventStream {
    queue: Arc<EventQueue>,
    inner: Weak<TransportInner>,
    subscriber_id: u64,
    cancel_sender: watch::Sender<bool>,
    cancel_task: Option<JoinHandle<()>>,
    waiter: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl Stream for AcpWsEventStream {
    type Item = Result<DaemonEvent, AcpWsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            {
                let mut state = lock_recover(&this.queue.state);
                if let Some(event) = state.events.pop_front() {
                    this.waiter = None;
                    return Poll::Ready(Some(Ok(event)));
                }
                if state.closed {
                    this.waiter = None;
                    let item = if !state.error_reported {
                        if let Some(error) = state.close_error.take() {
                            state.error_reported = true;
                            Some(Err(AcpWsError::StreamClosed(error)))
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    drop(state);
                    this.finish_subscription();
                    return Poll::Ready(item);
                }
            }

            if this.waiter.is_none() {
                let notify = Arc::clone(&this.queue.notify);
                this.waiter = Some(Box::pin(async move { notify.notified().await }));
            }
            match this
                .waiter
                .as_mut()
                .expect("waiter was set")
                .as_mut()
                .poll(cx)
            {
                Poll::Ready(()) => this.waiter = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AcpWsEventStream {
    fn finish_subscription(&mut self) {
        self.cancel_sender.send_replace(true);
        if let Some(task) = self.cancel_task.take() {
            task.abort();
        }
        if let Some(inner) = self.inner.upgrade() {
            lock_recover(&inner.state)
                .subscribers
                .remove(&self.subscriber_id);
        }
    }
}

impl Drop for AcpWsEventStream {
    fn drop(&mut self) {
        self.finish_subscription();
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

impl AcpWsTransport {
    /// Create a transport. `ws_url` must use `ws:` or `wss:`; the matching
    /// `http:` or `https:` origin is used for the capabilities fallback.
    pub fn new(ws_url: impl AsRef<str>, token: Option<String>) -> Result<Self, AcpWsError> {
        Self::with_rest_client(ws_url, token, reqwest::Client::new())
    }

    /// Create a transport with an explicit HTTP client for capability lookup.
    pub fn with_rest_client(
        ws_url: impl AsRef<str>,
        token: Option<String>,
        rest_client: reqwest::Client,
    ) -> Result<Self, AcpWsError> {
        let raw = ws_url.as_ref();
        let ws_url = Url::parse(raw).map_err(|error| AcpWsError::InvalidUrl(error.to_string()))?;
        if !matches!(ws_url.scheme(), "ws" | "wss") {
            return Err(AcpWsError::InvalidUrl(format!(
                "unsupported URL scheme in {raw:?}"
            )));
        }
        let (disposed_sender, _) = watch::channel(false);
        Ok(Self {
            inner: Arc::new(TransportInner {
                ws_url,
                token,
                rest_client,
                disposed: AtomicBool::new(false),
                disposed_sender,
                initialize_lock: AsyncMutex::new(()),
                state: Mutex::new(TransportState {
                    generation: 0,
                    connected: false,
                    init_result: None,
                    next_request_id: 1,
                    next_subscriber_id: 1,
                    pending: HashMap::new(),
                    subscribers: HashMap::new(),
                    outbound: None,
                    worker: None,
                }),
                next_generation: std::sync::atomic::AtomicU64::new(1),
            }),
        })
    }

    pub fn connected(&self) -> bool {
        !self.inner.disposed.load(Ordering::Acquire) && lock_recover(&self.inner.state).connected
    }

    pub fn metadata(&self) -> AcpWsMetadata {
        AcpWsMetadata {
            initialize_result: lock_recover(&self.inner.state).init_result.clone(),
        }
    }

    /// Ensure a WebSocket is open and the ACP initialize request has completed.
    pub async fn ensure_initialized(&self) -> Result<(), AcpWsError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpWsError::Closed);
        }
        let _guard = self.inner.initialize_lock.lock().await;
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AcpWsError::Closed);
        }
        let initialized = {
            let state = lock_recover(&self.inner.state);
            state.connected && state.init_result.is_some()
        };
        if initialized {
            return Ok(());
        }
        self.connect_and_initialize().await
    }

    async fn connect_and_initialize(&self) -> Result<(), AcpWsError> {
        let mut request = self
            .inner
            .ws_url
            .as_str()
            .into_client_request()
            .map_err(|error| AcpWsError::Connect(error.to_string()))?;
        if let Some(token) = self.inner.token.as_ref().filter(|token| !token.is_empty()) {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|error| AcpWsError::InvalidAuthorizationHeader(error.to_string()))?;
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_ACP_WS_MESSAGE_BYTES);
        config.max_frame_size = Some(MAX_ACP_WS_MESSAGE_BYTES);
        let (socket, _) = match timeout(
            CONNECT_AND_INITIALIZE_TIMEOUT,
            connect_async_with_config(request, Some(config), false),
        )
        .await
        {
            Err(_) => return Err(AcpWsError::InitializeTimeout),
            Ok(Err(error)) => return Err(AcpWsError::Connect(error.to_string())),
            Ok(Ok(connection)) => connection,
        };
        if self.inner.disposed.load(Ordering::Acquire) {
            drop(socket);
            return Err(AcpWsError::Closed);
        }

        let generation = self.inner.next_generation.fetch_add(1, Ordering::Relaxed);
        let (outbound, outbound_receiver) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        {
            let mut state = lock_recover(&self.inner.state);
            state.generation = generation;
            state.connected = true;
            state.outbound = Some(outbound.clone());
        }
        let inner = Arc::clone(&self.inner);
        let worker = tokio::spawn(run_socket(socket, outbound_receiver, inner, generation));
        lock_recover(&self.inner.state).worker = Some(worker);

        let handshake = timeout(
            CONNECT_AND_INITIALIZE_TIMEOUT,
            self.send_request_connected(
                "initialize",
                json!({ "clientInfo": { "name": "qwen-code-sdk", "version": "1.0.0" } }),
                None,
                None,
            ),
        )
        .await;
        let response = match handshake {
            Err(_) => {
                self.close_generation(generation, "initialize timed out".into());
                return Err(AcpWsError::InitializeTimeout);
            }
            Ok(Err(error)) => {
                self.close_generation(generation, error.to_string());
                return Err(AcpWsError::Initialize(error.to_string()));
            }
            Ok(Ok(response)) => response,
        };
        let result = response.get("result").cloned().unwrap_or(Value::Null);
        let mut state = lock_recover(&self.inner.state);
        if state.generation != generation || !state.connected {
            return Err(AcpWsError::Connect(
                "socket closed during initialize".into(),
            ));
        }
        state.init_result = Some(result);
        Ok(())
    }

    /// Send one ACP JSON-RPC request and wait for its matching response.
    pub async fn send_request(
        &self,
        method: impl Into<String>,
        params: Value,
        cancellation: Option<AcpWsCancellation>,
    ) -> Result<Value, AcpWsError> {
        self.ensure_initialized().await?;
        let method = method.into();
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self.send_request_connected(&method, params, cancellation, session_id)
            .await
    }

    async fn send_request_connected(
        &self,
        method: &str,
        params: Value,
        cancellation: Option<AcpWsCancellation>,
        session_id: Option<String>,
    ) -> Result<Value, AcpWsError> {
        let (id, receiver, outbound) = {
            let mut state = lock_recover(&self.inner.state);
            if self.inner.disposed.load(Ordering::Acquire) || !state.connected {
                return Err(AcpWsError::Closed);
            }
            if state.pending.len() >= MAX_PENDING_ACP_WS_REQUESTS {
                return Err(AcpWsError::TooManyPending);
            }
            let id = state.next_request_id;
            state.next_request_id = id.checked_add(1).ok_or(AcpWsError::RequestIdExhausted)?;
            let outbound = state.outbound.clone().ok_or(AcpWsError::Closed)?;
            let (sender, receiver) = oneshot::channel();
            state.pending.insert(id, sender);
            (id, receiver, outbound)
        };
        let _pending = PendingGuard {
            inner: Arc::clone(&self.inner),
            id,
        };
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": object_value(params),
        });
        let encoded = serde_json::to_string(&request).map_err(|_| AcpWsError::InvalidResponse)?;
        if encoded.len() > MAX_ACP_WS_MESSAGE_BYTES {
            return Err(AcpWsError::MessageTooLarge);
        }
        let mut dispose_receiver = self.inner.disposed_sender.subscribe();
        let mut caller_receiver = cancellation.as_ref().map(|token| token.sender.subscribe());
        if caller_receiver
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return Err(AcpWsError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_receiver) => return Err(AcpWsError::Closed),
            _ = wait_optional_cancel(caller_receiver.as_mut()) => return Err(AcpWsError::Cancelled),
            result = outbound.send(Message::Text(encoded.into())) => {
                result.map_err(|_| AcpWsError::Closed)?;
            }
        }
        tokio::select! {
            biased;
            _ = wait_for_cancel(&mut dispose_receiver) => Err(AcpWsError::Closed),
            _ = wait_optional_cancel(caller_receiver.as_mut()) => {
                if method == "session/prompt" {
                    if let Some(session_id) = session_id {
                        let _ = self.send_notification_connected("session/cancel", json!({ "sessionId": session_id }));
                    }
                }
                Err(AcpWsError::Cancelled)
            },
            result = receiver => match result {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(reason)) => Err(AcpWsError::Connection(reason)),
                Err(_) => Err(AcpWsError::Closed),
            }
        }
    }

    /// Send a JSON-RPC notification. A notification that races a close is
    /// dropped, matching the TypeScript transport's best-effort `send` path.
    pub async fn send_notification(
        &self,
        method: impl Into<String>,
        params: Value,
    ) -> Result<(), AcpWsError> {
        self.ensure_initialized().await?;
        self.send_notification_connected(&method.into(), params)
    }

    fn send_notification_connected(&self, method: &str, params: Value) -> Result<(), AcpWsError> {
        let sender = lock_recover(&self.inner.state).outbound.clone();
        let Some(sender) = sender else { return Ok(()) };
        let message = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": object_value(params),
        });
        let encoded = serde_json::to_string(&message).map_err(|_| AcpWsError::InvalidResponse)?;
        if encoded.len() > MAX_ACP_WS_MESSAGE_BYTES {
            return Err(AcpWsError::MessageTooLarge);
        }
        match sender.try_send(Message::Text(encoded.into())) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(AcpWsError::OutboundQueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(()),
        }
    }

    /// Map and dispatch a URL-shaped daemon route through the shared ACP route
    /// table. `path` may include its query string; pass `query` separately when
    /// it is already available.
    pub async fn dispatch_route(
        &self,
        http_method: &str,
        path: &str,
        query: &str,
        body: Value,
        cancellation: Option<AcpWsCancellation>,
    ) -> Result<AcpHttpResponse, AcpWsError> {
        self.dispatch_route_with_options(
            http_method,
            path,
            query,
            body,
            AcpWsRequestOptions {
                cancellation,
                client_id: None,
            },
        )
        .await
    }

    pub async fn dispatch_route_with_options(
        &self,
        http_method: &str,
        path: &str,
        query: &str,
        body: Value,
        options: AcpWsRequestOptions,
    ) -> Result<AcpHttpResponse, AcpWsError> {
        self.ensure_initialized().await?;
        let Some(mut route) = map_route(http_method, path, query, &body) else {
            return Ok(AcpHttpResponse {
                status: 404,
                body: Some(json!({ "error": format!("No ACP mapping for {http_method} {path}") })),
            });
        };
        if let Some(client_id) = options.client_id {
            let params = ensure_object(&mut route.params);
            let meta = params
                .entry("_meta")
                .or_insert_with(|| Value::Object(Map::new()));
            ensure_object(meta).insert("clientId".into(), Value::String(client_id));
        }
        if route.method == "_capabilities" {
            return self.capabilities_fallback().await;
        }
        if route.notification {
            self.send_notification_connected(&route.method, route.params)?;
            return Ok(AcpHttpResponse {
                status: 204,
                body: None,
            });
        }
        let response = self
            .send_request_connected(
                &route.method,
                route.params.clone(),
                options.cancellation,
                route
                    .params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            )
            .await?;
        if let Some(error) = response.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
            let data = error.get("data").filter(|value| !value.is_null());
            let status = data
                .and_then(|value| value.get("httpStatus"))
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok())
                .unwrap_or_else(|| rpc_error_http_status(code, data));
            let mut body = Map::new();
            body.insert(
                "error".into(),
                error
                    .get("message")
                    .cloned()
                    .unwrap_or_else(|| Value::String("ACP request failed".into())),
            );
            if let Some(data) = data {
                body.insert("data".into(), data.clone());
            }
            return Ok(AcpHttpResponse {
                status,
                body: Some(Value::Object(body)),
            });
        }
        Ok(AcpHttpResponse {
            status: 200,
            body: response.get("result").cloned(),
        })
    }

    async fn capabilities_fallback(&self) -> Result<AcpHttpResponse, AcpWsError> {
        let mut url = self.inner.ws_url.clone();
        url.set_scheme(match url.scheme() {
            "ws" => "http",
            _ => "https",
        })
        .map_err(|_| AcpWsError::InvalidUrl("could not derive REST URL".into()))?;
        url.set_path("/capabilities");
        url.set_query(None);
        url.set_fragment(None);
        let mut request = self.inner.rest_client.get(url);
        if let Some(token) = self.inner.token.as_ref().filter(|token| !token.is_empty()) {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|error| AcpWsError::InvalidAuthorizationHeader(error.to_string()))?;
            request = request.header(AUTHORIZATION, value);
        }
        if let Ok(response) = request.send().await {
            let status = response.status().as_u16();
            if !response.status().is_success() && status != 404 {
                let body = response.json::<Value>().await.ok();
                return Ok(AcpHttpResponse { status, body });
            }
            if response.status().is_success()
                && let Ok(envelope) = response.json::<Value>().await
                && envelope.get("features").is_some_and(Value::is_array)
            {
                return Ok(AcpHttpResponse {
                    status,
                    body: Some(envelope),
                });
            }
        }
        let mut fallback = lock_recover(&self.inner.state)
            .init_result
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        fallback.insert("v".into(), Value::from(1));
        fallback.insert("features".into(), Value::Array(Vec::new()));
        Ok(AcpHttpResponse {
            status: 200,
            body: Some(Value::Object(fallback)),
        })
    }

    /// Subscribe to live notifications filtered for `session_id`. ACP WS does
    /// not replay; REST-only cursor and epoch fields therefore have no effect.
    pub async fn subscribe_events(
        &self,
        session_id: impl Into<String>,
        cancellation: Option<AcpWsCancellation>,
    ) -> Result<AcpWsEventStream, AcpWsError> {
        self.ensure_initialized().await?;
        let session_id = session_id.into();
        let queue = Arc::new(EventQueue::new(MAX_ACP_WS_EVENT_QUEUE_CAPACITY));
        let (subscriber_id, local_cancel_sender, local_cancel_receiver) = {
            let mut state = lock_recover(&self.inner.state);
            if !state.connected {
                return Err(AcpWsError::Closed);
            }
            let id = state.next_subscriber_id;
            state.next_subscriber_id = id.saturating_add(1);
            state.subscribers.insert(
                id,
                Subscriber {
                    session_id,
                    queue: Arc::clone(&queue),
                },
            );
            let (sender, receiver) = watch::channel(false);
            (id, sender, receiver)
        };

        let mut external_receiver = cancellation.as_ref().map(|token| token.sender.subscribe());
        let mut local_receiver = local_cancel_receiver;
        let mut dispose_receiver = self.inner.disposed_sender.subscribe();
        let watcher_queue = Arc::clone(&queue);
        let weak_inner = Arc::downgrade(&self.inner);
        let watcher = tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = wait_for_cancel(&mut local_receiver) => return,
                _ = wait_optional_cancel(external_receiver.as_mut()) => {
                    watcher_queue.close(None);
                }
                _ = wait_for_cancel(&mut dispose_receiver) => return,
            }
            if let Some(inner) = weak_inner.upgrade() {
                lock_recover(&inner.state)
                    .subscribers
                    .remove(&subscriber_id);
            }
        });
        if cancellation
            .as_ref()
            .is_some_and(AcpWsCancellation::is_cancelled)
        {
            queue.close(None);
        }
        Ok(AcpWsEventStream {
            queue,
            inner: Arc::downgrade(&self.inner),
            subscriber_id,
            cancel_sender: local_cancel_sender,
            cancel_task: Some(watcher),
            waiter: None,
        })
    }

    /// Close the socket and release every pending request and subscriber.
    pub fn dispose(&self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        self.inner.disposed_sender.send_replace(true);
        let (outbound, worker, pending, subscribers) = {
            let mut state = lock_recover(&self.inner.state);
            state.connected = false;
            let outbound = state.outbound.take();
            let worker = state.worker.take();
            let pending = std::mem::take(&mut state.pending);
            let subscribers = std::mem::take(&mut state.subscribers);
            (outbound, worker, pending, subscribers)
        };
        if let Some(outbound) = outbound {
            let _ = outbound.try_send(Message::Close(None));
        }
        if let Some(worker) = worker {
            worker.abort();
        }
        for (_, sender) in pending {
            let _ = sender.send(Err("transport disposed".into()));
        }
        for (_, subscriber) in subscribers {
            subscriber.queue.close(Some("transport disposed".into()));
        }
    }

    fn close_generation(&self, generation: u64, reason: String) {
        fail_connection(&self.inner, generation, reason);
    }
}

impl Drop for TransportInner {
    fn drop(&mut self) {
        self.disposed.store(true, Ordering::Release);
        self.disposed_sender.send_replace(true);
    }
}

async fn run_socket(
    socket: Socket,
    mut outbound: mpsc::Receiver<Message>,
    inner: Arc<TransportInner>,
    generation: u64,
) {
    let (mut sink, mut stream) = socket.split();
    let reason = loop {
        tokio::select! {
            biased;
            message = outbound.recv() => match message {
                Some(message @ Message::Close(_)) => {
                    let _ = sink.send(message).await;
                    break "transport disposed".to_owned();
                }
                Some(message) => {
                    if let Err(error) = sink.send(message).await {
                        break error.to_string();
                    }
                }
                None => break "transport closed".to_owned(),
            },
            incoming = stream.next() => match incoming {
                Some(Ok(Message::Text(text))) => handle_text(&inner, text.as_str(), generation),
                Some(Ok(Message::Binary(bytes))) => {
                    if bytes.len() > MAX_ACP_WS_MESSAGE_BYTES {
                        break "message exceeds size limit".into();
                    }
                    let text = String::from_utf8_lossy(&bytes);
                    handle_text(&inner, &text, generation);
                }
                Some(Ok(Message::Ping(payload))) => {
                    if let Err(error) = sink.send(Message::Pong(payload)).await {
                        break error.to_string();
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(frame))) => {
                    let detail = frame.map(|frame| format!("{} {}", frame.code, frame.reason)).unwrap_or_default();
                    break format!("WebSocket closed: {detail}");
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => break error.to_string(),
                None => break "WebSocket stream ended".into(),
            }
        }
    };
    fail_connection(&inner, generation, reason);
}

fn handle_text(inner: &Arc<TransportInner>, text: &str, generation: u64) {
    if text.len() > MAX_ACP_WS_MESSAGE_BYTES {
        fail_connection(inner, generation, "message exceeds size limit".into());
        return;
    }
    let Ok(message) = serde_json::from_str::<Value>(text) else {
        return;
    };
    if let Some(id) = message.get("id").and_then(Value::as_u64) {
        let sender = {
            let mut state = lock_recover(&inner.state);
            if state.generation != generation {
                return;
            }
            state.pending.remove(&id)
        };
        if let Some(sender) = sender {
            let _ = sender.send(Ok(message));
        }
        return;
    }
    if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return;
    }
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return;
    };
    let Some(event) = denormalize_acp_notification(method, message.get("params")) else {
        return;
    };
    let state = lock_recover(&inner.state);
    if state.generation != generation {
        return;
    }
    for subscriber in state.subscribers.values() {
        let event_session = event
            .data
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|object| object.get("sessionId"))
            .and_then(Value::as_str)
            .filter(|session_id| !session_id.is_empty());
        if event_session.is_some_and(|event_session| event_session != subscriber.session_id) {
            continue;
        }
        subscriber.queue.push(event.clone());
    }
}

fn fail_connection(inner: &Arc<TransportInner>, generation: u64, reason: String) {
    let (pending, subscribers) = {
        let mut state = lock_recover(&inner.state);
        if state.generation != generation {
            return;
        }
        state.connected = false;
        state.outbound = None;
        state.init_result = None;
        state.worker = None;
        (
            std::mem::take(&mut state.pending),
            std::mem::take(&mut state.subscribers),
        )
    };
    for (_, sender) in pending {
        let _ = sender.send(Err(reason.clone()));
    }
    for (_, subscriber) in subscribers {
        subscriber.queue.close(Some(reason.clone()));
    }
}

async fn wait_for_cancel(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            pending::<()>().await;
        }
    }
}

async fn wait_optional_cancel(receiver: Option<&mut watch::Receiver<bool>>) {
    match receiver {
        Some(receiver) => wait_for_cancel(receiver).await,
        None => pending::<()>().await,
    }
}

fn object_value(value: Value) -> Value {
    if value.is_object() {
        value
    } else {
        Value::Object(Map::new())
    }
}

fn ensure_object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    value.as_object_mut().expect("object was installed")
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
