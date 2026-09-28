//! Automatic transport recovery for daemon SDK backends.
//!
//! The wrapper mirrors the TypeScript `AutoReconnectTransport`: it retries
//! only a transport-closed error, with no backoff, serializes recovery, tries
//! the preferred backend factory once, falls back to REST/SSE, and retries a
//! failed fetch or event subscription once. Session reattachment remains the
//! caller's responsibility.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{Stream, StreamExt, stream};
use reqwest::header::HeaderMap;
use reqwest::{Method, Response};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, oneshot, watch};
use url::Url;

use crate::daemon_acp_http::{
    AcpCancellation, AcpHttpError, AcpHttpResponse, AcpHttpTransport, AcpSubscribeOptions,
};
use crate::daemon_acp_ws::{AcpWsCancellation, AcpWsError, AcpWsRequestOptions, AcpWsTransport};
use crate::daemon_rest::{
    RestSseCancellation, RestSseError, RestSseTransport, SseConnectReason, SubscribeEventsOptions,
};
use crate::daemon_sse::DaemonEvent;

/// Transport family requested from a [`TransportFactory`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonTransportType {
    Rest,
    AcpHttp,
    AcpWs,
}

impl DaemonTransportType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::AcpHttp => "acp-http",
            Self::AcpWs => "acp-ws",
        }
    }
}

/// Errors surfaced by the reconnecting transport.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AutoReconnectError {
    #[error("transport connection closed")]
    Closed,
    #[error("event subscription was cancelled")]
    Cancelled,
    #[error("invalid daemon URL: {0}")]
    InvalidUrl(String),
    #[error("daemon transport operation failed: {0}")]
    Operation(String),
}

/// HTTP-shaped fetch input. It is cloneable so one failed request can be
/// retried once without consuming its URL, headers, or body.
#[derive(Clone, Debug)]
pub struct ReconnectFetchRequest {
    pub url: String,
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
    /// Per-call timeout; `None` and zero both disable it.
    pub timeout: Option<Duration>,
}

/// URL-shaped request used by ACP transports, which synthesize an HTTP status
/// and JSON body instead of returning a raw `reqwest::Response`.
#[derive(Clone, Debug)]
pub struct AutoReconnectRouteRequest {
    pub http_method: String,
    pub path: String,
    /// Query string without a leading `?`.
    pub query: String,
    pub body: Value,
    /// ACP WebSocket maps this to `_meta.clientId`; ACP HTTP and REST ignore it.
    pub client_id: Option<String>,
    /// Optional wrapper cancellation, forwarded into the ACP request.
    pub cancellation: Option<RestSseCancellation>,
}

/// Event stream returned from one backend subscription attempt.
pub type ReconnectEventStream =
    Pin<Box<dyn Stream<Item = Result<DaemonEvent, AutoReconnectError>> + Send>>;

/// Metadata from an accepted SSE subscription, used to invoke acceptance
/// callbacks after the initial connect and after any restart.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconnectStreamMetadata {
    pub stream_id: Option<String>,
    pub epoch: Option<String>,
}

/// One backend's accepted event stream and response metadata.
pub struct ReconnectEventSubscription {
    pub metadata: ReconnectStreamMetadata,
    pub events: ReconnectEventStream,
}

/// Native transport backend boundary used by [`AutoReconnectTransport`].
/// Implementations should return [`AutoReconnectError::Closed`] only when the
/// transport itself has closed; network and protocol errors are propagated
/// without triggering reconnection, matching the TypeScript wrapper.
pub trait AutoReconnectBackend: Send + Sync {
    fn transport_type(&self) -> DaemonTransportType;
    fn supports_replay(&self) -> bool;
    fn connected(&self) -> bool;

    fn fetch<'a>(
        &'a self,
        request: ReconnectFetchRequest,
    ) -> BoxFuture<'a, Result<Response, AutoReconnectError>>;

    /// Dispatch a URL-shaped daemon route. REST-compatible implementors return
    /// the upstream HTTP response projected into an ACP-shaped result. ACP
    /// backends override this with the shared ACP route table.
    fn dispatch_route<'a>(
        &'a self,
        _request: AutoReconnectRouteRequest,
    ) -> BoxFuture<'a, Result<AcpHttpResponse, AutoReconnectError>> {
        Box::pin(async {
            Err(AutoReconnectError::Operation(
                "route dispatch is unsupported by this backend".into(),
            ))
        })
    }

    fn subscribe_events<'a>(
        &'a self,
        session_id: &'a str,
        options: AutoReconnectSubscribeOptions,
    ) -> BoxFuture<'a, Result<ReconnectEventSubscription, AutoReconnectError>>;

    fn dispose(&self);
}

/// Factory for a preferred transport implementation. Factory failures are
/// intentionally swallowed during recovery; the wrapper then creates REST/SSE.
pub type TransportFactory = Arc<
    dyn Fn(
            DaemonTransportType,
        ) -> BoxFuture<'static, Result<Arc<dyn AutoReconnectBackend>, AutoReconnectError>>
        + Send
        + Sync,
>;

/// Subscribe options. Cursor, query, cancellation, and callback values are
/// forwarded to each replacement stream unchanged.
#[derive(Clone, Default)]
pub struct AutoReconnectSubscribeOptions {
    pub last_event_id: Option<u64>,
    pub epoch: Option<String>,
    pub max_queued: Option<u64>,
    pub client_id: Option<String>,
    pub connect_reason: Option<SseConnectReason>,
    pub previous_stream_id: Option<String>,
    pub connect_timeout: Option<Duration>,
    pub cancellation: Option<RestSseCancellation>,
    pub on_epoch: Option<Arc<dyn Fn(String) + Send + Sync>>,
    pub on_sse_stream_accepted: Option<Arc<dyn Fn(Option<String>) + Send + Sync>>,
}

/// Construction settings for a reconnecting transport.
pub struct AutoReconnectOptions {
    pub base_url: String,
    pub token: Option<String>,
    pub preferred_type: Option<DaemonTransportType>,
    pub factory: Option<TransportFactory>,
    pub initial: Option<Arc<dyn AutoReconnectBackend>>,
    pub rest_client: Option<reqwest::Client>,
}

impl AutoReconnectOptions {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token: None,
            preferred_type: None,
            factory: None,
            initial: None,
            rest_client: None,
        }
    }
}

/// Optional reconnect wrapper for native daemon transport backends.
#[derive(Clone)]
pub struct AutoReconnectTransport {
    inner: Arc<ReconnectInner>,
}

struct ReconnectInner {
    base_url: String,
    token: Option<String>,
    rest_client: reqwest::Client,
    preferred_type: DaemonTransportType,
    factory: Option<TransportFactory>,
    initial_supports_replay: bool,
    disposed: AtomicBool,
    backend: Mutex<BackendSlot>,
    reconnect_lock: AsyncMutex<()>,
}

struct BackendSlot {
    backend: Arc<dyn AutoReconnectBackend>,
    generation: u64,
}

impl AutoReconnectTransport {
    /// Construct with the default REST/SSE backend and no preferred factory.
    pub fn new(
        base_url: impl Into<String>,
        token: Option<String>,
    ) -> Result<Self, AutoReconnectError> {
        let mut options = AutoReconnectOptions::new(base_url);
        options.token = token;
        Self::with_options(options)
    }

    pub fn with_options(options: AutoReconnectOptions) -> Result<Self, AutoReconnectError> {
        let rest_client = options.rest_client.unwrap_or_default();
        let backend = match options.initial {
            Some(initial) => initial,
            None => Arc::new(RestSseBackend::with_client(
                &options.base_url,
                options.token.clone(),
                rest_client.clone(),
            )?) as Arc<dyn AutoReconnectBackend>,
        };
        let initial_supports_replay = backend.supports_replay();
        Ok(Self {
            inner: Arc::new(ReconnectInner {
                base_url: options.base_url,
                token: options.token,
                rest_client,
                preferred_type: options.preferred_type.unwrap_or(DaemonTransportType::Rest),
                factory: options.factory,
                initial_supports_replay,
                disposed: AtomicBool::new(false),
                backend: Mutex::new(BackendSlot {
                    backend,
                    generation: 0,
                }),
                reconnect_lock: AsyncMutex::new(()),
            }),
        })
    }

    /// The current backend's transport family.
    pub fn transport_type(&self) -> DaemonTransportType {
        lock_recover(&self.inner.backend).backend.transport_type()
    }

    /// Matches TypeScript's snapshot: this reports the initial backend's
    /// replay capability even if recovery later changes transport families.
    pub fn supports_replay(&self) -> bool {
        self.inner.initial_supports_replay
    }

    pub fn connected(&self) -> bool {
        !self.inner.disposed.load(Ordering::Acquire)
            && lock_recover(&self.inner.backend).backend.connected()
    }

    /// Delegate a request, recovering and retrying once only on `Closed`.
    pub async fn fetch(
        &self,
        request: ReconnectFetchRequest,
    ) -> Result<Response, AutoReconnectError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AutoReconnectError::Closed);
        }
        let (backend, generation) = backend_snapshot(&self.inner);
        match backend.fetch(request.clone()).await {
            Err(AutoReconnectError::Closed) => {
                self.reconnect_if_current(generation).await?;
                if self.inner.disposed.load(Ordering::Acquire) {
                    return Err(AutoReconnectError::Closed);
                }
                let (backend, _) = backend_snapshot(&self.inner);
                backend.fetch(request).await
            }
            result => result,
        }
    }

    /// Dispatch a URL-shaped route through REST or an ACP backend. This is the
    /// ACP counterpart to [`Self::fetch`], whose return type remains a raw HTTP
    /// response for compatibility with the existing Rust SDK API.
    pub async fn dispatch_route(
        &self,
        request: AutoReconnectRouteRequest,
    ) -> Result<AcpHttpResponse, AutoReconnectError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AutoReconnectError::Closed);
        }
        let (backend, generation) = backend_snapshot(&self.inner);
        match backend.dispatch_route(request.clone()).await {
            Err(AutoReconnectError::Closed) => {
                if request
                    .cancellation
                    .as_ref()
                    .is_some_and(RestSseCancellation::is_cancelled)
                {
                    return Err(AutoReconnectError::Cancelled);
                }
                self.reconnect_if_current(generation).await?;
                if self.inner.disposed.load(Ordering::Acquire) {
                    return Err(AutoReconnectError::Closed);
                }
                let (backend, _) = backend_snapshot(&self.inner);
                backend.dispatch_route(request).await
            }
            result => result,
        }
    }

    /// Open a stream and restart it once if it ends with a transport-closed
    /// error. Normal end-of-stream and other errors are returned as-is.
    pub async fn subscribe_events(
        &self,
        session_id: impl Into<String>,
        options: AutoReconnectSubscribeOptions,
    ) -> Result<ReconnectEventStream, AutoReconnectError> {
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(AutoReconnectError::Closed);
        }
        let session_id = session_id.into();
        let (backend, generation) = backend_snapshot(&self.inner);
        let initial = backend.subscribe_events(&session_id, options.clone()).await;
        let (subscription, generation, restarted) = match initial {
            Ok(subscription) => (subscription, generation, false),
            Err(AutoReconnectError::Closed) => {
                if options
                    .cancellation
                    .as_ref()
                    .is_some_and(RestSseCancellation::is_cancelled)
                {
                    return Err(AutoReconnectError::Cancelled);
                }
                self.reconnect_if_current(generation).await?;
                if self.inner.disposed.load(Ordering::Acquire) {
                    return Err(AutoReconnectError::Closed);
                }
                let (backend, generation) = backend_snapshot(&self.inner);
                let subscription = backend
                    .subscribe_events(&session_id, options.clone())
                    .await?;
                (subscription, generation, true)
            }
            Err(error) => return Err(error),
        };
        notify_stream_callbacks(&options, &subscription.metadata);
        let state = ReconnectingEventState {
            inner: Arc::clone(&self.inner),
            session_id,
            options,
            events: subscription.events,
            generation,
            restarted,
            finished: false,
        };
        Ok(Box::pin(stream::unfold(state, |mut state| async move {
            let event = next_reconnecting_event(&mut state).await?;
            Some((event, state))
        })))
    }

    /// Idempotently dispose the current backend. A backend created by an
    /// in-flight recovery is also disposed before it can be installed.
    pub fn dispose(&self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        lock_recover(&self.inner.backend).backend.dispose();
    }

    async fn reconnect_if_current(&self, generation: u64) -> Result<u64, AutoReconnectError> {
        reconnect_if_current(&self.inner, generation).await
    }
}

struct ReconnectingEventState {
    inner: Arc<ReconnectInner>,
    session_id: String,
    options: AutoReconnectSubscribeOptions,
    events: ReconnectEventStream,
    generation: u64,
    restarted: bool,
    finished: bool,
}

async fn next_reconnecting_event(
    state: &mut ReconnectingEventState,
) -> Option<Result<DaemonEvent, AutoReconnectError>> {
    if state.finished {
        return None;
    }
    loop {
        match state.events.next().await {
            Some(Ok(event)) => return Some(Ok(event)),
            Some(Err(AutoReconnectError::Closed)) if !state.restarted => {
                if state.inner.disposed.load(Ordering::Acquire) {
                    state.finished = true;
                    return Some(Err(AutoReconnectError::Closed));
                }
                if state
                    .options
                    .cancellation
                    .as_ref()
                    .is_some_and(RestSseCancellation::is_cancelled)
                {
                    state.finished = true;
                    return Some(Err(AutoReconnectError::Cancelled));
                }
                let observed_generation = state.generation;
                if let Err(error) = reconnect_if_current(&state.inner, observed_generation).await {
                    state.finished = true;
                    return Some(Err(error));
                }
                if state.inner.disposed.load(Ordering::Acquire) {
                    state.finished = true;
                    return Some(Err(AutoReconnectError::Closed));
                }
                let (backend, generation) = backend_snapshot(&state.inner);
                match backend
                    .subscribe_events(&state.session_id, state.options.clone())
                    .await
                {
                    Ok(subscription) => {
                        notify_stream_callbacks(&state.options, &subscription.metadata);
                        state.events = subscription.events;
                        state.generation = generation;
                        state.restarted = true;
                    }
                    Err(error) => {
                        state.finished = true;
                        return Some(Err(error));
                    }
                }
            }
            Some(Err(AutoReconnectError::Cancelled)) => {
                state.finished = true;
                return None;
            }
            Some(Err(error)) => {
                state.finished = true;
                return Some(Err(error));
            }
            None => {
                state.finished = true;
                return None;
            }
        }
    }
}

async fn reconnect_if_current(
    inner: &Arc<ReconnectInner>,
    observed_generation: u64,
) -> Result<u64, AutoReconnectError> {
    let inner = Arc::clone(inner);
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        let result = reconnect_if_current_owned(inner, observed_generation).await;
        let _ = sender.send(result);
    });
    receiver.await.unwrap_or(Err(AutoReconnectError::Closed))
}

async fn reconnect_if_current_owned(
    inner: Arc<ReconnectInner>,
    observed_generation: u64,
) -> Result<u64, AutoReconnectError> {
    let _guard = inner.reconnect_lock.lock().await;
    if inner.disposed.load(Ordering::Acquire) {
        return Err(AutoReconnectError::Closed);
    }
    let (old_backend, current_generation) = {
        let slot = lock_recover(&inner.backend);
        (Arc::clone(&slot.backend), slot.generation)
    };
    if current_generation != observed_generation {
        return Ok(current_generation);
    }
    old_backend.dispose();

    let replacement = if let Some(factory) = &inner.factory {
        match factory(inner.preferred_type).await {
            Ok(backend) => Some(backend),
            Err(_) => None,
        }
    } else {
        None
    };
    let replacement = match replacement {
        Some(backend) => backend,
        None => Arc::new(RestSseBackend::with_client(
            &inner.base_url,
            inner.token.clone(),
            inner.rest_client.clone(),
        )?) as Arc<dyn AutoReconnectBackend>,
    };
    if inner.disposed.load(Ordering::Acquire) {
        replacement.dispose();
        return Err(AutoReconnectError::Closed);
    }
    let mut slot = lock_recover(&inner.backend);
    if slot.generation != observed_generation {
        replacement.dispose();
        return Ok(slot.generation);
    }
    slot.backend = replacement;
    slot.generation = slot.generation.saturating_add(1);
    Ok(slot.generation)
}

fn backend_snapshot(inner: &ReconnectInner) -> (Arc<dyn AutoReconnectBackend>, u64) {
    let slot = lock_recover(&inner.backend);
    (Arc::clone(&slot.backend), slot.generation)
}

fn notify_stream_callbacks(
    options: &AutoReconnectSubscribeOptions,
    metadata: &ReconnectStreamMetadata,
) {
    if let Some(callback) = &options.on_sse_stream_accepted {
        callback(metadata.stream_id.clone());
    }
    if let (Some(callback), Some(epoch)) = (&options.on_epoch, &metadata.epoch) {
        callback(epoch.clone());
    }
}

/// Concrete backend that combines `RestSseTransport` subscriptions with
/// `reqwest` fetch calls. It is also the mandatory fallback after factory
/// failure.
pub struct RestSseBackend {
    transport: RestSseTransport,
    client: reqwest::Client,
    base_url: Url,
    token: Option<String>,
}

impl RestSseBackend {
    pub fn new(
        base_url: impl AsRef<str>,
        token: Option<String>,
    ) -> Result<Self, AutoReconnectError> {
        Self::with_client(base_url, token, reqwest::Client::new())
    }

    pub fn with_client(
        base_url: impl AsRef<str>,
        token: Option<String>,
        client: reqwest::Client,
    ) -> Result<Self, AutoReconnectError> {
        let transport = RestSseTransport::with_options(
            base_url.as_ref(),
            token.clone(),
            client.clone(),
            Some(crate::daemon_sse::DEFAULT_SSE_IDLE_TIMEOUT),
        )
        .map_err(map_rest_error)?;
        let base_url = Url::parse(base_url.as_ref())
            .map_err(|error| AutoReconnectError::InvalidUrl(error.to_string()))?;
        Ok(Self {
            transport,
            client,
            base_url,
            token,
        })
    }
}

impl AutoReconnectBackend for RestSseBackend {
    fn transport_type(&self) -> DaemonTransportType {
        DaemonTransportType::Rest
    }

    fn supports_replay(&self) -> bool {
        true
    }

    fn connected(&self) -> bool {
        self.transport.connected()
    }

    fn fetch<'a>(
        &'a self,
        request: ReconnectFetchRequest,
    ) -> BoxFuture<'a, Result<Response, AutoReconnectError>> {
        Box::pin(async move {
            if !self.connected() {
                return Err(AutoReconnectError::Closed);
            }
            let url = Url::parse(&request.url)
                .map_err(|error| AutoReconnectError::InvalidUrl(error.to_string()))?;
            let mut builder = self
                .client
                .request(request.method, url)
                .headers(request.headers);
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            if let Some(timeout) = request.timeout.filter(|timeout| !timeout.is_zero()) {
                builder = builder.timeout(timeout);
            }
            builder
                .send()
                .await
                .map_err(|error| AutoReconnectError::Operation(error.to_string()))
        })
    }

    fn dispatch_route<'a>(
        &'a self,
        request: AutoReconnectRouteRequest,
    ) -> BoxFuture<'a, Result<AcpHttpResponse, AutoReconnectError>> {
        Box::pin(async move {
            if !self.connected() {
                return Err(AutoReconnectError::Closed);
            }
            let raw_url = format!(
                "{}/{}",
                self.base_url.as_str().trim_end_matches('/'),
                request.path.trim_start_matches('/')
            );
            let mut url = Url::parse(&raw_url)
                .map_err(|error| AutoReconnectError::InvalidUrl(error.to_string()))?;
            if !request.query.is_empty() {
                url.set_query(Some(request.query.trim_start_matches('?')));
            }
            let method = Method::from_bytes(request.http_method.as_bytes())
                .map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
            let mut builder = self.client.request(method, url);
            if let Some(token) = self.token.as_ref().filter(|token| !token.is_empty()) {
                builder = builder.bearer_auth(token);
            }
            if !request.body.is_null() {
                builder = builder.json(&request.body);
            }
            let response = builder
                .send()
                .await
                .map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
            let status = response.status().as_u16();
            let mut body_stream = response.bytes_stream();
            let mut bytes = Vec::new();
            const MAX_ROUTE_RESPONSE_BYTES: usize = 1024 * 1024;
            while let Some(chunk) = body_stream.next().await {
                let chunk =
                    chunk.map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
                let remaining = MAX_ROUTE_RESPONSE_BYTES.saturating_sub(bytes.len());
                if chunk.len() > remaining {
                    bytes.extend_from_slice(&chunk[..remaining]);
                    bytes.extend_from_slice(b"\n...[truncated]");
                    break;
                }
                bytes.extend_from_slice(&chunk);
                if bytes.len() == MAX_ROUTE_RESPONSE_BYTES {
                    bytes.extend_from_slice(b"\n...[truncated]");
                    break;
                }
            }
            let body = if bytes.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_slice(&bytes)
                        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into())),
                )
            };
            Ok(AcpHttpResponse { status, body })
        })
    }

    fn subscribe_events<'a>(
        &'a self,
        session_id: &'a str,
        options: AutoReconnectSubscribeOptions,
    ) -> BoxFuture<'a, Result<ReconnectEventSubscription, AutoReconnectError>> {
        Box::pin(async move {
            let subscription = self
                .transport
                .subscribe_events(session_id, to_rest_options(&options))
                .await
                .map_err(map_rest_error)?;
            let metadata = ReconnectStreamMetadata {
                stream_id: subscription.metadata.stream_id,
                epoch: subscription.metadata.epoch,
            };
            let events = subscription
                .events
                .map(|event| event.map_err(map_rest_error));
            Ok(ReconnectEventSubscription {
                metadata,
                events: Box::pin(events),
            })
        })
    }

    fn dispose(&self) {
        self.transport.dispose();
    }
}

/// ACP-over-HTTP implementation for [`AutoReconnectBackend`]. Use
/// [`AutoReconnectTransport::dispatch_route`] for route calls; ACP transports
/// synthesize status/body values and cannot return a raw HTTP response.
pub struct AcpHttpBackend {
    transport: AcpHttpTransport,
}

impl AcpHttpBackend {
    pub fn new(transport: AcpHttpTransport) -> Self {
        Self { transport }
    }
}

impl AutoReconnectBackend for AcpHttpBackend {
    fn transport_type(&self) -> DaemonTransportType {
        DaemonTransportType::AcpHttp
    }

    fn supports_replay(&self) -> bool {
        true
    }

    fn connected(&self) -> bool {
        self.transport.connected()
    }

    fn fetch<'a>(
        &'a self,
        _request: ReconnectFetchRequest,
    ) -> BoxFuture<'a, Result<Response, AutoReconnectError>> {
        Box::pin(async {
            Err(AutoReconnectError::Operation(
                "ACP HTTP does not expose raw fetch responses; use dispatch_route".into(),
            ))
        })
    }

    fn dispatch_route<'a>(
        &'a self,
        request: AutoReconnectRouteRequest,
    ) -> BoxFuture<'a, Result<AcpHttpResponse, AutoReconnectError>> {
        Box::pin(async move {
            let (cancellation, bridge) =
                bridge_rest_cancellation(request.cancellation.clone(), AcpCancellation::new());
            let response = self
                .transport
                .dispatch_route(
                    &request.http_method,
                    &request.path,
                    &request.query,
                    request.body,
                    Some(cancellation),
                )
                .await
                .map_err(map_acp_http_error);
            drop(bridge);
            response
        })
    }

    fn subscribe_events<'a>(
        &'a self,
        session_id: &'a str,
        options: AutoReconnectSubscribeOptions,
    ) -> BoxFuture<'a, Result<ReconnectEventSubscription, AutoReconnectError>> {
        Box::pin(async move {
            let (cancellation, bridge) =
                bridge_rest_cancellation(options.cancellation.clone(), AcpCancellation::new());
            let subscription = self
                .transport
                .subscribe_events(
                    session_id,
                    AcpSubscribeOptions {
                        last_event_id: options.last_event_id,
                        epoch: options.epoch.clone(),
                        connect_timeout: options.connect_timeout,
                        cancellation: Some(cancellation),
                        ..AcpSubscribeOptions::default()
                    },
                )
                .await
                .map_err(map_acp_http_error)?;
            let metadata = ReconnectStreamMetadata {
                stream_id: None,
                epoch: subscription.metadata.epoch,
            };
            let events = subscription.events.map(move |event| {
                let _keep_cancellation_forwarder_alive = &bridge;
                event.map_err(map_acp_http_error)
            });
            Ok(ReconnectEventSubscription {
                metadata,
                events: Box::pin(events),
            })
        })
    }

    fn dispose(&self) {
        self.transport.dispose();
    }
}

/// ACP-over-WebSocket implementation for [`AutoReconnectBackend`]. Use
/// [`AutoReconnectTransport::dispatch_route`] for route calls; raw fetch is
/// specific to REST-backed routes.
pub struct AcpWsBackend {
    transport: AcpWsTransport,
}

impl AcpWsBackend {
    pub fn new(transport: AcpWsTransport) -> Self {
        Self { transport }
    }
}

impl AutoReconnectBackend for AcpWsBackend {
    fn transport_type(&self) -> DaemonTransportType {
        DaemonTransportType::AcpWs
    }

    fn supports_replay(&self) -> bool {
        false
    }

    fn connected(&self) -> bool {
        self.transport.connected()
    }

    fn fetch<'a>(
        &'a self,
        _request: ReconnectFetchRequest,
    ) -> BoxFuture<'a, Result<Response, AutoReconnectError>> {
        Box::pin(async {
            Err(AutoReconnectError::Operation(
                "ACP WebSocket does not expose raw fetch responses; use dispatch_route".into(),
            ))
        })
    }

    fn dispatch_route<'a>(
        &'a self,
        request: AutoReconnectRouteRequest,
    ) -> BoxFuture<'a, Result<AcpHttpResponse, AutoReconnectError>> {
        Box::pin(async move {
            let (cancellation, bridge) =
                bridge_rest_cancellation(request.cancellation.clone(), AcpWsCancellation::new());
            let response = self
                .transport
                .dispatch_route_with_options(
                    &request.http_method,
                    &request.path,
                    &request.query,
                    request.body,
                    AcpWsRequestOptions {
                        cancellation: Some(cancellation),
                        client_id: request.client_id,
                    },
                )
                .await
                .map_err(map_acp_ws_error);
            drop(bridge);
            response
        })
    }

    fn subscribe_events<'a>(
        &'a self,
        session_id: &'a str,
        options: AutoReconnectSubscribeOptions,
    ) -> BoxFuture<'a, Result<ReconnectEventSubscription, AutoReconnectError>> {
        Box::pin(async move {
            let (cancellation, bridge) =
                bridge_rest_cancellation(options.cancellation.clone(), AcpWsCancellation::new());
            let events = self
                .transport
                .subscribe_events(session_id, Some(cancellation))
                .await
                .map_err(map_acp_ws_error)?
                .map(move |event| {
                    let _keep_cancellation_forwarder_alive = &bridge;
                    event.map_err(map_acp_ws_error)
                });
            Ok(ReconnectEventSubscription {
                metadata: ReconnectStreamMetadata::default(),
                events: Box::pin(events),
            })
        })
    }

    fn dispose(&self) {
        self.transport.dispose();
    }
}

trait CancellationTarget: Clone + Send + 'static {
    fn cancel(&self);
}

impl CancellationTarget for AcpCancellation {
    fn cancel(&self) {
        AcpCancellation::cancel(self);
    }
}

impl CancellationTarget for AcpWsCancellation {
    fn cancel(&self) {
        AcpWsCancellation::cancel(self);
    }
}

struct CancellationBridge {
    stop_sender: watch::Sender<bool>,
}

impl Drop for CancellationBridge {
    fn drop(&mut self) {
        self.stop_sender.send_replace(true);
    }
}

fn bridge_rest_cancellation<T: CancellationTarget>(
    external: Option<RestSseCancellation>,
    target: T,
) -> (T, Option<Arc<CancellationBridge>>) {
    let Some(external) = external else {
        return (target, None);
    };
    if external.is_cancelled() {
        target.cancel();
        return (target, None);
    }
    let mut external_receiver = external.subscribe_cancelled();
    let (stop_sender, mut stop_receiver) = watch::channel(false);
    let task_target = target.clone();
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = async {
                loop {
                    if *external_receiver.borrow() {
                        break;
                    }
                    if external_receiver.changed().await.is_err() {
                        break;
                    }
                }
            } => task_target.cancel(),
            _ = stop_receiver.changed() => {}
        }
    });
    (target, Some(Arc::new(CancellationBridge { stop_sender })))
}

fn map_acp_http_error(error: AcpHttpError) -> AutoReconnectError {
    match error {
        AcpHttpError::Closed => AutoReconnectError::Closed,
        AcpHttpError::Cancelled => AutoReconnectError::Cancelled,
        AcpHttpError::InvalidBaseUrl(message) => AutoReconnectError::InvalidUrl(message),
        other => AutoReconnectError::Operation(other.to_string()),
    }
}

fn map_acp_ws_error(error: AcpWsError) -> AutoReconnectError {
    match error {
        AcpWsError::Closed
        | AcpWsError::Connect(_)
        | AcpWsError::Connection(_)
        | AcpWsError::InitializeTimeout
        | AcpWsError::StreamClosed(_) => AutoReconnectError::Closed,
        AcpWsError::Cancelled => AutoReconnectError::Cancelled,
        AcpWsError::InvalidUrl(message) => AutoReconnectError::InvalidUrl(message),
        other => AutoReconnectError::Operation(other.to_string()),
    }
}

fn to_rest_options(options: &AutoReconnectSubscribeOptions) -> SubscribeEventsOptions {
    SubscribeEventsOptions {
        last_event_id: options.last_event_id,
        epoch: options.epoch.clone(),
        max_queued: options.max_queued,
        client_id: options.client_id.clone(),
        connect_reason: options.connect_reason,
        previous_stream_id: options.previous_stream_id.clone(),
        connect_timeout: options.connect_timeout,
        cancellation: options.cancellation.clone(),
    }
}

fn map_rest_error(error: RestSseError) -> AutoReconnectError {
    match error {
        RestSseError::Closed => AutoReconnectError::Closed,
        RestSseError::Cancelled => AutoReconnectError::Cancelled,
        RestSseError::InvalidBaseUrl(message) => AutoReconnectError::InvalidUrl(message),
        other => AutoReconnectError::Operation(other.to_string()),
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
