//! REST/SSE transport for the daemon SDK.
//!
//! This module opens `GET /session/:id/events`, maps the TypeScript transport's
//! authentication, resume, and diagnostic headers/query parameters, and feeds
//! the response body into [`crate::daemon_sse::parse_sse_stream`]. It is a
//! transport foundation; it does not implement the complete `DaemonClient`
//! REST API or ACP transports.

use std::collections::HashMap;
use std::future::pending;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{Stream, StreamExt, stream};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::sleep;
use url::Url;

use crate::daemon_sse::{DEFAULT_SSE_IDLE_TIMEOUT, DaemonEvent, SseError, parse_sse_stream};

/// Upper bound for an HTTP error response captured in [`RestSseError::Http`].
/// A broken or hostile peer cannot make an error path buffer an unlimited body.
const MAX_HTTP_ERROR_BODY_BYTES: usize = 1024 * 1024;

/// Connect or stream failure from [`RestSseTransport`].
#[derive(Debug, Error)]
pub enum RestSseError {
    #[error("REST/SSE transport has been disposed")]
    Closed,
    #[error("REST/SSE subscription was cancelled")]
    Cancelled,
    #[error("invalid daemon base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("REST/SSE request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("initial REST/SSE connection timed out")]
    ConnectTimeout,
    #[error("invalid value for HTTP header {name}: {source}")]
    InvalidHeader {
        name: &'static str,
        #[source]
        source: reqwest::header::InvalidHeaderValue,
    },
    #[error("GET /session/:id/events: {detail} (HTTP {status})")]
    Http {
        status: u16,
        body: Option<Value>,
        detail: String,
    },
    #[error(
        "GET /session/:id/events: expected content-type text/event-stream, got \"{content_type}\" (HTTP {status})"
    )]
    InvalidContentType { status: u16, content_type: String },
    #[error("SSE stream failed: {0}")]
    Sse(#[from] SseError),
}

/// Diagnostic reason attached to a daemon SSE subscription request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SseConnectReason {
    Initial,
    Resume,
    PromptRestart,
    StreamEnd,
    TransportError,
    StateResync,
    Unknown,
}

impl SseConnectReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Resume => "resume",
            Self::PromptRestart => "prompt_restart",
            Self::StreamEnd => "stream_end",
            Self::TransportError => "transport_error",
            Self::StateResync => "state_resync",
            Self::Unknown => "unknown",
        }
    }
}

/// Cloneable cancellation handle for a pending connection or active stream.
/// Dropping the handle does not cancel; call [`Self::cancel`] explicitly.
#[derive(Clone, Debug)]
pub struct RestSseCancellation {
    sender: watch::Sender<bool>,
}

impl Default for RestSseCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl RestSseCancellation {
    /// Make a fresh, not-yet-cancelled handle.
    pub fn new() -> Self {
        let (sender, _receiver) = watch::channel(false);
        Self { sender }
    }

    /// Cancel all subscriptions using a clone of this handle.
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    /// Whether this handle has already been cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    pub(crate) fn subscribe_cancelled(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
    }
}

/// Options used to open one session event subscription.
#[derive(Clone, Debug, Default)]
pub struct SubscribeEventsOptions {
    /// Resume after this event. The epoch header is only sent with this cursor.
    pub last_event_id: Option<u64>,
    /// Epoch associated with `last_event_id`.
    pub epoch: Option<String>,
    /// Per-subscriber event backlog cap (`?maxQueued=`).
    pub max_queued: Option<u64>,
    /// Optional daemon-side client identity (`X-Qwen-Client-Id`).
    pub client_id: Option<String>,
    /// Diagnostic-only connect reason (`?connectReason=`).
    pub connect_reason: Option<SseConnectReason>,
    /// Previous accepted stream identifier (`?previousStreamId=`).
    pub previous_stream_id: Option<String>,
    /// Initial request-to-headers timeout. The long-lived body is not covered.
    pub connect_timeout: Option<Duration>,
    /// Optional caller cancellation, observed during connect and while reading.
    pub cancellation: Option<RestSseCancellation>,
}

/// Server metadata accepted with an SSE response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SseStreamMetadata {
    /// Validated, lowercase UUID supplied in `X-Qwen-SSE-Stream-Id`.
    pub stream_id: Option<String>,
    /// Current daemon bus epoch from `X-Qwen-Event-Epoch`.
    pub epoch: Option<String>,
}

/// Accepted response metadata and the bounded, idle-timed SSE event stream.
pub struct EventSubscription {
    pub metadata: SseStreamMetadata,
    pub events: Pin<Box<dyn Stream<Item = Result<DaemonEvent, RestSseError>> + Send>>,
}

/// Daemon REST/SSE transport with bounded event parsing and explicit teardown.
///
/// `dispose()` is idempotent and cancels both in-flight connects and active
/// response bodies. Dropping an `EventSubscription::events` stream also drops
/// its response body and removes it from the transport's active set. The
/// default idle-read timeout is 45 seconds, matching the TypeScript SDK; pass
/// `None` to [`Self::with_options`] to disable that timeout.
#[derive(Clone)]
pub struct RestSseTransport {
    inner: Arc<TransportInner>,
}

struct TransportInner {
    base_url: Url,
    client: reqwest::Client,
    token: Option<String>,
    idle_timeout: Option<Duration>,
    disposed: AtomicBool,
    next_id: AtomicU64,
    active: Mutex<HashMap<u64, watch::Sender<bool>>>,
}

impl RestSseTransport {
    /// Construct a transport using the default HTTP client and 45-second idle
    /// timeout. `base_url` may include a path prefix such as `/api`.
    pub fn new(base_url: impl AsRef<str>, token: Option<String>) -> Result<Self, RestSseError> {
        Self::with_options(
            base_url,
            token,
            reqwest::Client::new(),
            Some(DEFAULT_SSE_IDLE_TIMEOUT),
        )
    }

    /// Construct a transport with an explicit HTTP client and idle timeout.
    /// `None` disables idle detection; a zero duration also disables it.
    pub fn with_options(
        base_url: impl AsRef<str>,
        token: Option<String>,
        client: reqwest::Client,
        idle_timeout: Option<Duration>,
    ) -> Result<Self, RestSseError> {
        let raw_base_url = base_url.as_ref();
        let base_url = Url::parse(raw_base_url)
            .map_err(|error| RestSseError::InvalidBaseUrl(error.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(RestSseError::InvalidBaseUrl(format!(
                "unsupported URL scheme in {raw_base_url:?}"
            )));
        }
        Ok(Self {
            inner: Arc::new(TransportInner {
                base_url,
                client,
                token,
                idle_timeout: idle_timeout.filter(|duration| !duration.is_zero()),
                disposed: AtomicBool::new(false),
                next_id: AtomicU64::new(1),
                active: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Whether the transport still accepts new requests.
    pub fn connected(&self) -> bool {
        !self.inner.disposed.load(Ordering::Acquire)
    }

    /// Open the event stream for one session. Response metadata is available
    /// as soon as the server accepts the subscription; events are then
    /// delivered lazily without an unbounded intermediate queue.
    pub async fn subscribe_events(
        &self,
        session_id: &str,
        options: SubscribeEventsOptions,
    ) -> Result<EventSubscription, RestSseError> {
        if !self.connected() {
            return Err(RestSseError::Closed);
        }

        let (cancel_sender, mut cancel_receiver) = watch::channel(false);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut active = lock_recover(&self.inner.active);
            if !self.connected() {
                return Err(RestSseError::Closed);
            }
            active.insert(id, cancel_sender);
        }
        let guard = ActiveStreamGuard {
            inner: Arc::clone(&self.inner),
            id,
        };

        let external_token = options.cancellation.clone();
        let mut external_receiver = external_token
            .as_ref()
            .map(|cancellation| cancellation.sender.subscribe());
        if external_token
            .as_ref()
            .is_some_and(RestSseCancellation::is_cancelled)
        {
            return Err(RestSseError::Cancelled);
        }

        let url = build_event_url(&self.inner.base_url, session_id, &options)?;
        let request = self
            .inner
            .client
            .get(url)
            .headers(build_headers(&self.inner.token, &options)?);

        let connect_timeout = options
            .connect_timeout
            .filter(|duration| !duration.is_zero());
        let response = match connect_timeout {
            Some(duration) => {
                tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut cancel_receiver) => return Err(RestSseError::Closed),
                    _ = wait_for_optional_cancel(external_receiver.as_mut()) => return Err(RestSseError::Cancelled),
                    _ = sleep(duration) => return Err(RestSseError::ConnectTimeout),
                    result = request.send() => result.map_err(RestSseError::Request)?,
                }
            }
            None => {
                tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut cancel_receiver) => return Err(RestSseError::Closed),
                    _ = wait_for_optional_cancel(external_receiver.as_mut()) => return Err(RestSseError::Cancelled),
                    result = request.send() => result.map_err(RestSseError::Request)?,
                }
            }
        };

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body =
                read_error_body(response, &mut cancel_receiver, external_receiver.as_mut()).await?;
            let detail = body
                .as_ref()
                .and_then(|body| body.get("error"))
                .map(js_string)
                .unwrap_or_else(|| format!("HTTP {status}"));
            return Err(RestSseError::Http {
                status,
                body,
                detail,
            });
        }

        let status = response.status().as_u16();
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
            return Err(RestSseError::InvalidContentType {
                status,
                content_type,
            });
        }

        let metadata = response_metadata(response.headers());
        let body = response.bytes_stream();
        let parser = Box::pin(parse_sse_stream(body, self.inner.idle_timeout));
        let events = stream::unfold(
            EventStreamState {
                parser: Some(parser),
                cancel_receiver,
                external_receiver,
                _external_token: external_token,
                _guard: Some(guard),
                finished: false,
            },
            |mut state| async move {
                if state.finished {
                    return None;
                }
                tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut state.cancel_receiver) => {
                        state.finished = true;
                        state.parser.take();
                        state._guard.take();
                        Some((Err(RestSseError::Closed), state))
                    }
                    _ = wait_for_optional_cancel(state.external_receiver.as_mut()) => {
                        state.finished = true;
                        state.parser.take();
                        state._guard.take();
                        Some((Err(RestSseError::Cancelled), state))
                    }
                    event = state.parser.as_mut().expect("active parser").next() => match event {
                        Some(Ok(event)) => Some((Ok(event), state)),
                        Some(Err(error)) => {
                            state.finished = true;
                            state.parser.take();
                            state._guard.take();
                            Some((Err(RestSseError::Sse(error)), state))
                        }
                        None => {
                            None
                        }
                    }
                }
            },
        );

        Ok(EventSubscription {
            metadata,
            events: Box::pin(events),
        })
    }

    /// Cancel pending connects and active event streams. Safe to call more
    /// than once; disposed transports reject later subscriptions.
    pub fn dispose(&self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        let active = lock_recover(&self.inner.active);
        for sender in active.values() {
            sender.send_replace(true);
        }
    }
}

struct EventStreamState {
    parser: Option<Pin<Box<dyn Stream<Item = Result<DaemonEvent, SseError>> + Send>>>,
    cancel_receiver: watch::Receiver<bool>,
    external_receiver: Option<watch::Receiver<bool>>,
    // Keep the caller's sender alive for the lifetime of its subscription;
    // dropping a cancellation handle is not itself a cancellation request.
    _external_token: Option<RestSseCancellation>,
    _guard: Option<ActiveStreamGuard>,
    finished: bool,
}

struct ActiveStreamGuard {
    inner: Arc<TransportInner>,
    id: u64,
}

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        let mut active = lock_recover(&self.inner.active);
        if let Some(sender) = active.remove(&self.id) {
            sender.send_replace(true);
        }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

async fn wait_for_optional_cancel(receiver: Option<&mut watch::Receiver<bool>>) {
    match receiver {
        Some(receiver) => wait_for_cancel(receiver).await,
        None => pending::<()>().await,
    }
}

fn build_headers(
    token: &Option<String>,
    options: &SubscribeEventsOptions,
) -> Result<HeaderMap, RestSseError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    if let Some(token) = token.as_ref().filter(|token| !token.is_empty()) {
        headers.insert(
            AUTHORIZATION,
            header_value("authorization", &format!("Bearer {token}"))?,
        );
    }
    if let Some(client_id) = options.client_id.as_ref().filter(|id| !id.is_empty()) {
        headers.insert(
            "x-qwen-client-id",
            header_value("x-qwen-client-id", client_id)?,
        );
    }
    if let Some(last_event_id) = options.last_event_id {
        headers.insert(
            "last-event-id",
            header_value("last-event-id", &last_event_id.to_string())?,
        );
        if let Some(epoch) = &options.epoch {
            headers.insert(
                "x-qwen-event-epoch",
                header_value("x-qwen-event-epoch", epoch)?,
            );
        }
    }
    Ok(headers)
}

fn header_value(name: &'static str, value: &str) -> Result<HeaderValue, RestSseError> {
    HeaderValue::from_str(value).map_err(|source| RestSseError::InvalidHeader { name, source })
}

fn build_event_url(
    base_url: &Url,
    session_id: &str,
    options: &SubscribeEventsOptions,
) -> Result<Url, RestSseError> {
    let mut url = base_url.clone();
    url.set_fragment(None);
    {
        let mut path = url.path_segments_mut().map_err(|_| {
            RestSseError::InvalidBaseUrl("base URL cannot contain path segments".into())
        })?;
        path.pop_if_empty()
            .push("session")
            .push(session_id)
            .push("events");
    }
    {
        let mut query = url.query_pairs_mut();
        if let Some(max_queued) = options.max_queued {
            query.append_pair("maxQueued", &max_queued.to_string());
        }
        if let Some(reason) = options.connect_reason {
            query.append_pair("connectReason", reason.as_str());
        }
        if let Some(previous_stream_id) = &options.previous_stream_id {
            query.append_pair("previousStreamId", previous_stream_id);
        }
    }
    Ok(url)
}

fn response_metadata(headers: &HeaderMap) -> SseStreamMetadata {
    let stream_id = headers
        .get("x-qwen-sse-stream-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_supported_stream_id(value))
        .map(str::to_ascii_lowercase);
    let epoch = headers
        .get("x-qwen-event-epoch")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    SseStreamMetadata { stream_id, epoch }
}

fn is_supported_stream_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) || byte.is_ascii_hexdigit())
    {
        return false;
    }
    matches!(bytes[14].to_ascii_lowercase(), b'1'..=b'8')
        && matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b')
}

async fn read_error_body(
    response: reqwest::Response,
    cancel_receiver: &mut watch::Receiver<bool>,
    external_receiver: Option<&mut watch::Receiver<bool>>,
) -> Result<Option<Value>, RestSseError> {
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    let mut truncated = false;
    let mut external_receiver = external_receiver;
    loop {
        let next = tokio::select! {
            biased;
            _ = wait_for_cancel(cancel_receiver) => return Err(RestSseError::Closed),
            _ = wait_for_optional_cancel(external_receiver.as_deref_mut()) => return Err(RestSseError::Cancelled),
            next = stream.next() => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(_) => return Ok(None),
        };
        let remaining = MAX_HTTP_ERROR_BODY_BYTES.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        body.extend_from_slice(&chunk);
        if body.len() == MAX_HTTP_ERROR_BODY_BYTES {
            // Stop promptly at the cap. A marker makes the returned text clear
            // that the peer may have supplied more bytes.
            truncated = true;
            break;
        }
    }
    if truncated {
        body.extend_from_slice(b"\n...[truncated]");
    }
    let text = String::from_utf8_lossy(&body).into_owned();
    Ok(Some(
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    ))
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
