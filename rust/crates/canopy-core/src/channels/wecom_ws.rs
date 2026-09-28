//! WeCom smart-bot WebSocket client protocol.
//!
//! Port of the MIT `@wecom/aibot-node-sdk` 1.0.7 wire behavior used by
//! `packages/channels/wecom/src/WeComAdapter.ts`. This module owns the WSS
//! connection, auth and heartbeat frames, correlated acknowledgements, callback
//! delivery, reconnect policy, and the proactive text/media commands. A host
//! still owns inbound authorization, prompt dispatch, and adapter watchdogs.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use md5::{Digest, Md5};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval_at, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use super::wecom::{WECOM_MAX_MEDIA_BYTES, WeComConfig, WeComMediaType};

/// Maximum UTF-8 bytes accepted for one inbound or outbound JSON text frame.
/// The upload protocol's 512 KiB raw chunks fit after base64 encoding.
pub const WECOM_WS_MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum concurrently outstanding request acknowledgements on a connection.
pub const WECOM_WS_MAX_PENDING_REQUESTS: usize = 512;
/// Source SDK limit for queued replies sharing one callback `req_id`.
pub const WECOM_WS_MAX_REPLY_QUEUE_SIZE: usize = 500;
/// Upload chunk size before base64 encoding, matching the SDK.
pub const WECOM_WS_UPLOAD_CHUNK_BYTES: usize = 512 * 1024;

const DEFAULT_WS_URL: &str = "wss://openws.work.weixin.qq.com";
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_RECONNECT_BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_REQUEST_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const OUTBOUND_QUEUE_CAPACITY: usize = 128;
const EVENT_QUEUE_CAPACITY: usize = 256;
const MAX_AUTH_FAILURE_ATTEMPTS: usize = 5;
const MAX_RECONNECT_ATTEMPTS: usize = 10;
const MAX_MISSED_HEARTBEATS: u8 = 2;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type SocketWriter = SplitSink<Socket, Message>;

/// Connection state observable by a WeCom channel host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WeComWsStatus {
    Idle,
    Connecting,
    Connected,
    Authenticated,
    Disconnected,
    Closed,
}

/// Callback and lifecycle events emitted by the SDK-compatible client.
#[derive(Clone, Debug, PartialEq)]
pub enum WeComWsEvent {
    Connected,
    Authenticated,
    /// The SDK emits a common message event and a type-specific event. The
    /// host receives one event with `msgtype` and can route either convention.
    Message {
        msgtype: String,
        frame: Value,
    },
    /// The SDK emits both `event` and `event.<eventtype>` for event callbacks.
    Event {
        event_type: Option<String>,
        frame: Value,
    },
    Disconnected {
        reason: String,
    },
    Reconnecting {
        attempt: usize,
        authentication: bool,
    },
    ServerDisconnected {
        reason: String,
    },
    Error {
        message: String,
    },
}

/// Runtime options corresponding to the SDK's `WSClientOptions` controls that
/// affect this adapter's wire behavior.
#[derive(Clone, Debug)]
pub struct WeComWsOptions {
    pub heartbeat_interval: Duration,
    pub reconnect_base_delay: Duration,
    /// `None` means unlimited, matching the SDK's `-1` option.
    pub max_reconnect_attempts: Option<usize>,
    /// `None` means unlimited, matching the SDK's `-1` option.
    pub max_auth_failure_attempts: Option<usize>,
    pub request_ack_timeout: Duration,
    pub max_pending_requests: usize,
    pub max_reply_queue_size: usize,
    pub scene: Option<String>,
    pub plug_version: Option<String>,
}

impl Default for WeComWsOptions {
    fn default() -> Self {
        Self {
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            reconnect_base_delay: DEFAULT_RECONNECT_BASE_DELAY,
            max_reconnect_attempts: Some(MAX_RECONNECT_ATTEMPTS),
            max_auth_failure_attempts: Some(MAX_AUTH_FAILURE_ATTEMPTS),
            request_ack_timeout: DEFAULT_REQUEST_ACK_TIMEOUT,
            max_pending_requests: WECOM_WS_MAX_PENDING_REQUESTS,
            max_reply_queue_size: WECOM_WS_MAX_REPLY_QUEUE_SIZE,
            scene: None,
            plug_version: None,
        }
    }
}

/// Failure from the WeCom WSS client.
#[derive(Debug, Error)]
pub enum WeComWsError {
    #[error("WeCom WebSocket client requires an active Tokio runtime")]
    RuntimeUnavailable,
    #[error("WeCom WebSocket client is not connected")]
    NotConnected,
    #[error("WeCom WebSocket client is not authenticated")]
    NotAuthenticated,
    #[error("WeCom WebSocket client is closed")]
    Closed,
    #[error("WeCom WebSocket connection failed: {0}")]
    Connection(String),
    #[error("WeCom WebSocket operation timed out")]
    Timeout,
    #[error("WeCom WebSocket acknowledgement failed: {0}")]
    Acknowledgement(String),
    #[error("WeCom WebSocket outbound queue is full")]
    OutboundQueueFull,
    #[error("WeCom WebSocket pending request limit ({WECOM_WS_MAX_PENDING_REQUESTS}) reached")]
    TooManyPending,
    #[error("WeCom WebSocket reply queue for this request is full")]
    ReplyQueueFull,
    #[error("WeCom WebSocket frame is {actual} bytes; maximum is {WECOM_WS_MAX_FRAME_BYTES}")]
    FrameTooLarge { actual: usize },
    #[error("WeCom WebSocket message must be a JSON object")]
    InvalidMessage,
    #[error("WeCom WebSocket authentication failed: {0}")]
    AuthenticationFailed(String),
    #[error("WeCom WebSocket authentication failed after {0} attempts")]
    AuthenticationExhausted(usize),
    #[error("WeCom WebSocket reconnect limit ({0}) reached")]
    ReconnectExhausted(usize),
    #[error("WeCom media upload failed: {0}")]
    Upload(String),
}

#[derive(Clone)]
pub struct WeComWsClient {
    inner: Arc<Inner>,
}

impl fmt::Debug for WeComWsClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WeComWsClient")
            .field("bot_id", &self.inner.config.bot_id)
            .field("ws_url", &self.ws_url())
            .field("status", &*self.inner.status.borrow())
            .finish()
    }
}

struct Inner {
    config: WeComConfig,
    options: WeComWsOptions,
    status: watch::Sender<WeComWsStatus>,
    events: broadcast::Sender<WeComWsEvent>,
    shutdown: watch::Sender<bool>,
    lifecycle: Mutex<Option<JoinHandle<()>>>,
    outbound: Mutex<Option<mpsc::Sender<OutboundFrame>>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>,
    reply_queues: Mutex<HashMap<String, Weak<ReplyQueue>>>,
}

struct OutboundFrame {
    req_id: String,
    frame: Value,
}

struct ReplyQueue {
    lock: Arc<AsyncMutex<()>>,
    queued: AtomicUsize,
}

struct ReplyQueuePermit(Arc<ReplyQueue>);

impl Drop for ReplyQueuePermit {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::AcqRel);
    }
}

struct PendingGuard {
    inner: Arc<Inner>,
    req_id: String,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        lock_recover(&self.inner.pending).remove(&self.req_id);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ConnectionExit {
    Manual,
    ServerDisconnected(String),
    AuthenticationFailed,
    ConnectionLost(String),
}

impl WeComWsClient {
    pub fn new(config: WeComConfig) -> Self {
        Self::with_options(config, WeComWsOptions::default())
    }

    pub fn with_options(config: WeComConfig, options: WeComWsOptions) -> Self {
        let (status, _) = watch::channel(WeComWsStatus::Idle);
        let (events, _) = broadcast::channel(EVENT_QUEUE_CAPACITY);
        let (shutdown, _) = watch::channel(false);
        Self {
            inner: Arc::new(Inner {
                config,
                options,
                status,
                events,
                shutdown,
                lifecycle: Mutex::new(None),
                outbound: Mutex::new(None),
                pending: Mutex::new(HashMap::new()),
                reply_queues: Mutex::new(HashMap::new()),
            }),
        }
    }

    pub fn ws_url(&self) -> &str {
        self.inner
            .config
            .ws_url
            .as_deref()
            .unwrap_or(DEFAULT_WS_URL)
    }

    pub fn status(&self) -> WeComWsStatus {
        *self.inner.status.borrow()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WeComWsEvent> {
        self.inner.events.subscribe()
    }

    /// Start the SDK-style reconnecting connection worker. Network connection
    /// and authentication proceed in the background; use [`wait_authenticated`]
    /// when the caller needs the adapter's 30-second auth gate.
    pub fn connect(&self) -> Result<(), WeComWsError> {
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| WeComWsError::RuntimeUnavailable)?;
        let mut lifecycle = lock_recover(&self.inner.lifecycle);
        if lifecycle.as_ref().is_some_and(|task| !task.is_finished()) {
            return Ok(());
        }
        *lifecycle = None;
        self.inner.shutdown.send_replace(false);
        let (outbound, receiver) = mpsc::channel(OUTBOUND_QUEUE_CAPACITY);
        *lock_recover(&self.inner.outbound) = Some(outbound);
        let inner = Arc::clone(&self.inner);
        let shutdown = self.inner.shutdown.subscribe();
        *lifecycle = Some(runtime.spawn(run_client(inner, receiver, shutdown)));
        Ok(())
    }

    /// Stop the worker, cancel outstanding requests, and release its socket.
    pub async fn disconnect(&self) {
        self.inner.shutdown.send_replace(true);
        self.inner.status.send_replace(WeComWsStatus::Closed);
        fail_all_pending(&self.inner, "connection manually closed");
        let task = lock_recover(&self.inner.lifecycle).take();
        if let Some(task) = task {
            let _ = task.await;
        }
        *lock_recover(&self.inner.outbound) = None;
    }

    /// Wait until authentication succeeds or the timeout expires. The SDK
    /// adapter uses a 30-second authentication timeout.
    pub async fn wait_authenticated(&self, wait: Duration) -> Result<(), WeComWsError> {
        let mut status = self.inner.status.subscribe();
        let wait_status = async {
            loop {
                match *status.borrow_and_update() {
                    WeComWsStatus::Authenticated => return Ok(()),
                    WeComWsStatus::Closed => return Err(WeComWsError::Closed),
                    _ => {}
                }
                status.changed().await.map_err(|_| WeComWsError::Closed)?;
            }
        };
        timeout(wait, wait_status)
            .await
            .map_err(|_| WeComWsError::Timeout)?
    }

    /// Send one proactive text/markdown message and await its server ack.
    pub async fn send_message(&self, chat_id: &str, body: Value) -> Result<Value, WeComWsError> {
        let mut full_body = Map::new();
        full_body.insert("chatid".to_owned(), Value::String(chat_id.to_owned()));
        let provided = body.as_object().ok_or(WeComWsError::InvalidMessage)?;
        full_body.extend(provided.clone());
        let req_id = generate_req_id("aibot_send_msg");
        self.send_correlated(req_id, "aibot_send_msg", Some(Value::Object(full_body)))
            .await
    }

    /// Send a passive reply on the callback's `req_id`. Replies sharing one
    /// callback ID are serialized, as in the SDK's stream reply queue.
    pub async fn send_reply(
        &self,
        req_id: &str,
        body: Value,
        command: &str,
    ) -> Result<Value, WeComWsError> {
        let (_permit, _guard) = self.reply_guard(req_id).await?;
        self.send_correlated(req_id.to_owned(), command, Some(body))
            .await
    }

    pub async fn reply_welcome(&self, req_id: &str, body: Value) -> Result<Value, WeComWsError> {
        self.send_reply(req_id, body, "aibot_respond_welcome_msg")
            .await
    }

    pub async fn send_media_message(
        &self,
        chat_id: &str,
        media_type: WeComMediaType,
        media_id: &str,
    ) -> Result<Value, WeComWsError> {
        let mut media_body = Map::new();
        media_body.insert("media_id".to_owned(), Value::String(media_id.to_owned()));
        let kind = media_type.as_str();
        let body = json!({
            "msgtype": kind,
            (kind): Value::Object(media_body),
        });
        self.send_message(chat_id, body).await
    }

    /// Upload a media file with the SDK's init → bounded 512 KiB chunks →
    /// finish protocol. Chunk retries and the SDK's size ceiling are preserved;
    /// the channel adapter currently applies its stricter 20 MiB limit first.
    pub async fn upload_media(
        &self,
        bytes: &[u8],
        media_type: WeComMediaType,
        filename: &str,
    ) -> Result<Value, WeComWsError> {
        let chunk_count = bytes.len().div_ceil(WECOM_WS_UPLOAD_CHUNK_BYTES);
        if bytes.len() > WECOM_MAX_MEDIA_BYTES || chunk_count > 100 {
            return Err(WeComWsError::Upload(format!(
                "file exceeds the adapter limit of {WECOM_MAX_MEDIA_BYTES} bytes"
            )));
        }
        let digest = Md5::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let init = self
            .send_correlated(
                generate_req_id("aibot_upload_media_init"),
                "aibot_upload_media_init",
                Some(json!({
                    "type": media_type.as_str(),
                    "filename": filename,
                    "total_size": bytes.len(),
                    "total_chunks": chunk_count,
                    "md5": digest,
                })),
            )
            .await?;
        let upload_id = init
            .pointer("/body/upload_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| WeComWsError::Upload("init ack omitted upload_id".to_owned()))?
            .to_owned();

        let data: Arc<[u8]> = Arc::from(bytes);
        let work_chunk_count = chunk_count.max(1);
        let concurrency = match chunk_count {
            0..=4 => chunk_count.max(1),
            5..=10 => 3,
            _ => 2,
        };
        let mut pending = futures_util::stream::FuturesUnordered::new();
        let mut next_index = 0_usize;
        let mut first_error = None;
        while next_index < work_chunk_count && pending.len() < concurrency {
            pending.push(self.upload_chunk(Arc::clone(&data), upload_id.clone(), next_index));
            next_index += 1;
        }
        while let Some(result) = pending.next().await {
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(error.to_string());
                }
            }
            if next_index < work_chunk_count {
                pending.push(self.upload_chunk(Arc::clone(&data), upload_id.clone(), next_index));
                next_index += 1;
            }
        }
        if let Some(error) = first_error {
            return Err(WeComWsError::Upload(format!(
                "one or more media chunks failed: {error}"
            )));
        }

        let finish = self
            .send_correlated(
                generate_req_id("aibot_upload_media_finish"),
                "aibot_upload_media_finish",
                Some(json!({ "upload_id": upload_id })),
            )
            .await?;
        let body = finish
            .get("body")
            .and_then(Value::as_object)
            .ok_or_else(|| WeComWsError::Upload("finish ack omitted body".to_owned()))?;
        let media_id = body
            .get("media_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| WeComWsError::Upload("finish ack omitted media_id".to_owned()))?;
        Ok(json!({
            "type": body.get("type").and_then(Value::as_str).unwrap_or(media_type.as_str()),
            "media_id": media_id,
            "created_at": body.get("created_at").cloned().unwrap_or_else(|| {
                Value::String(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            }),
        }))
    }

    async fn upload_chunk(
        &self,
        bytes: Arc<[u8]>,
        upload_id: String,
        chunk_index: usize,
    ) -> Result<(), WeComWsError> {
        let start = chunk_index * WECOM_WS_UPLOAD_CHUNK_BYTES;
        let end = (start + WECOM_WS_UPLOAD_CHUNK_BYTES).min(bytes.len());
        let encoded = BASE64_STANDARD.encode(&bytes[start..end]);
        let mut last_error = None;
        for attempt in 0..=2 {
            let result = self
                .send_correlated(
                    generate_req_id("aibot_upload_media_chunk"),
                    "aibot_upload_media_chunk",
                    Some(json!({
                        "upload_id": upload_id,
                        // The SDK's documented types say one-based, while its
                        // implementation sends zero-based array offsets.
                        "chunk_index": chunk_index,
                        "base64_data": encoded,
                    })),
                )
                .await;
            match result {
                Ok(_) => return Ok(()),
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 2 {
                        sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }
        Err(WeComWsError::Upload(format!(
            "chunk {chunk_index} failed after 3 attempts: {}",
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown error".to_owned())
        )))
    }

    async fn send_correlated(
        &self,
        req_id: String,
        command: &str,
        body: Option<Value>,
    ) -> Result<Value, WeComWsError> {
        match self.status() {
            WeComWsStatus::Authenticated => {}
            WeComWsStatus::Closed => return Err(WeComWsError::Closed),
            WeComWsStatus::Connected => return Err(WeComWsError::NotAuthenticated),
            _ => return Err(WeComWsError::NotConnected),
        }
        let outbound = lock_recover(&self.inner.outbound)
            .clone()
            .ok_or(WeComWsError::NotConnected)?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = lock_recover(&self.inner.pending);
            if pending.len() >= self.inner.options.max_pending_requests {
                return Err(WeComWsError::TooManyPending);
            }
            if pending.contains_key(&req_id) {
                return Err(WeComWsError::Connection(
                    "duplicate request ID is already pending".to_owned(),
                ));
            }
            pending.insert(req_id.clone(), sender);
        }
        let _guard = PendingGuard {
            inner: Arc::clone(&self.inner),
            req_id: req_id.clone(),
        };
        let mut frame = Map::new();
        frame.insert("cmd".to_owned(), Value::String(command.to_owned()));
        frame.insert("headers".to_owned(), json!({ "req_id": req_id }));
        if let Some(body) = body {
            frame.insert("body".to_owned(), body);
        }
        let frame = Value::Object(frame);
        check_frame_size(&frame)?;
        outbound
            .try_send(OutboundFrame { req_id, frame })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => WeComWsError::OutboundQueueFull,
                mpsc::error::TrySendError::Closed(_) => WeComWsError::NotConnected,
            })?;
        match timeout(self.inner.options.request_ack_timeout, receiver).await {
            Err(_) => Err(WeComWsError::Timeout),
            Ok(Err(_)) => Err(WeComWsError::NotConnected),
            Ok(Ok(Err(message))) => Err(WeComWsError::Acknowledgement(message)),
            Ok(Ok(Ok(frame))) => Ok(frame),
        }
    }

    async fn reply_guard(
        &self,
        req_id: &str,
    ) -> Result<(ReplyQueuePermit, tokio::sync::OwnedMutexGuard<()>), WeComWsError> {
        let queue = {
            let mut queues = lock_recover(&self.inner.reply_queues);
            queues.retain(|_, queue| queue.strong_count() > 0);
            if let Some(queue) = queues.get(req_id).and_then(Weak::upgrade) {
                queue
            } else {
                if queues.len() >= WECOM_WS_MAX_PENDING_REQUESTS * 2 {
                    return Err(WeComWsError::ReplyQueueFull);
                }
                let queue = Arc::new(ReplyQueue {
                    lock: Arc::new(AsyncMutex::new(())),
                    queued: AtomicUsize::new(0),
                });
                queues.insert(req_id.to_owned(), Arc::downgrade(&queue));
                queue
            }
        };
        let mut queued = queue.queued.load(Ordering::Acquire);
        loop {
            if queued >= self.inner.options.max_reply_queue_size {
                return Err(WeComWsError::ReplyQueueFull);
            }
            match queue.queued.compare_exchange_weak(
                queued,
                queued + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => queued = actual,
            }
        }
        let permit = ReplyQueuePermit(Arc::clone(&queue));
        let guard = Arc::clone(&queue.lock).lock_owned().await;
        Ok((permit, guard))
    }
}

async fn run_client(
    inner: Arc<Inner>,
    mut outbound: mpsc::Receiver<OutboundFrame>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut reconnect_attempts = 0_usize;
    let mut auth_failure_attempts = 0_usize;
    loop {
        if *shutdown.borrow() {
            break;
        }
        inner.status.send_replace(WeComWsStatus::Connecting);
        let mut websocket_config = WebSocketConfig::default();
        websocket_config.max_message_size = Some(WECOM_WS_MAX_FRAME_BYTES);
        websocket_config.max_frame_size = Some(WECOM_WS_MAX_FRAME_BYTES);
        let connected = tokio::select! {
            result = timeout(
                CONNECT_TIMEOUT,
                connect_async_with_config(
                    inner.config.ws_url.as_deref().unwrap_or(DEFAULT_WS_URL),
                    Some(websocket_config),
                    false,
                ),
            ) => Some(result),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                None
            },
        };
        let Some(connected) = connected else {
            continue;
        };
        let socket = match connected {
            Err(_) => {
                let retry =
                    schedule_retry(&inner, &mut shutdown, false, &mut reconnect_attempts).await;
                if !retry {
                    break;
                }
                continue;
            }
            Ok(Err(error)) => {
                emit_error(&inner, format!("WebSocket connect failed: {error}"));
                let retry =
                    schedule_retry(&inner, &mut shutdown, false, &mut reconnect_attempts).await;
                if !retry {
                    break;
                }
                continue;
            }
            Ok(Ok((socket, _response))) => socket,
        };
        inner.status.send_replace(WeComWsStatus::Connected);
        let _ = inner.events.send(WeComWsEvent::Connected);
        let (mut writer, mut reader) = socket.split();
        let auth_req_id = generate_req_id("aibot_subscribe");
        let mut auth_body = Map::new();
        auth_body.insert(
            "bot_id".to_owned(),
            Value::String(inner.config.bot_id.clone()),
        );
        auth_body.insert(
            "secret".to_owned(),
            Value::String(inner.config.secret.clone()),
        );
        if let Some(scene) = inner.options.scene.as_ref() {
            auth_body.insert("scene".to_owned(), Value::String(scene.clone()));
        }
        if let Some(version) = inner.options.plug_version.as_ref() {
            auth_body.insert("plug_version".to_owned(), Value::String(version.clone()));
        }
        let auth_frame = json!({
            "cmd": "aibot_subscribe",
            "headers": { "req_id": auth_req_id },
            "body": auth_body,
        });
        if let Err(error) = send_frame(&mut writer, &auth_frame).await {
            emit_error(
                &inner,
                format!("failed to send authentication frame: {error}"),
            );
            fail_all_pending(&inner, "authentication send failed");
            let retry = schedule_retry(&inner, &mut shutdown, false, &mut reconnect_attempts).await;
            if !retry {
                break;
            }
            continue;
        }

        let mut authenticated = false;
        let mut missed_heartbeats = 0_u8;
        let interval = inner
            .options
            .heartbeat_interval
            .max(Duration::from_millis(1));
        let mut heartbeat = interval_at(Instant::now() + interval, interval);
        let exit = loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break ConnectionExit::Manual;
                    }
                }
                _ = heartbeat.tick(), if authenticated => {
                    if missed_heartbeats >= MAX_MISSED_HEARTBEATS {
                        emit_error(&inner, format!("no heartbeat acknowledgement for {missed_heartbeats} consecutive pings"));
                        break ConnectionExit::ConnectionLost(format!("heartbeat acknowledgement timed out after {missed_heartbeats} pings"));
                    }
                    missed_heartbeats += 1;
                    let ping = json!({
                        "cmd": "ping",
                        "headers": { "req_id": generate_req_id("ping") },
                    });
                    if let Err(error) = send_frame(&mut writer, &ping).await {
                        emit_error(&inner, format!("failed to send heartbeat: {error}"));
                        break ConnectionExit::ConnectionLost(format!("failed to send heartbeat: {error}"));
                    }
                }
                message = reader.next() => {
                    let Some(message) = message else {
                        break ConnectionExit::ConnectionLost("server closed the connection".to_owned());
                    };
                    match message {
                        Ok(Message::Text(text)) => {
                            match parse_server_frame(text.as_bytes()) {
                                Some(frame) => match handle_server_frame(&inner, &frame, &mut authenticated, &mut missed_heartbeats) {
                                    FrameAction::Continue => {},
                                    FrameAction::AuthenticationFailed(reason) => {
                                        emit_error(&inner, format!("authentication failed: {reason}"));
                                        break ConnectionExit::AuthenticationFailed;
                                    }
                                    FrameAction::ServerDisconnected(reason) => {
                                        break ConnectionExit::ServerDisconnected(reason);
                                    }
                                },
                                None => emit_error(&inner, "ignored malformed or oversized WebSocket JSON frame".to_owned()),
                            }
                        }
                        Ok(Message::Binary(bytes)) => {
                            match parse_server_frame(bytes.as_ref()) {
                                Some(frame) => match handle_server_frame(&inner, &frame, &mut authenticated, &mut missed_heartbeats) {
                                    FrameAction::Continue => {},
                                    FrameAction::AuthenticationFailed(reason) => {
                                        emit_error(&inner, format!("authentication failed: {reason}"));
                                        break ConnectionExit::AuthenticationFailed;
                                    }
                                    FrameAction::ServerDisconnected(reason) => {
                                        break ConnectionExit::ServerDisconnected(reason);
                                    }
                                },
                                None => emit_error(&inner, "ignored malformed or oversized WebSocket binary frame".to_owned()),
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            if let Err(error) = writer.send(Message::Pong(data)).await {
                                emit_error(&inner, format!("failed to answer WebSocket ping: {error}"));
                                break ConnectionExit::ConnectionLost(format!("failed to answer WebSocket ping: {error}"));
                            }
                        }
                        Ok(Message::Pong(_)) => {}
                        Ok(Message::Close(frame)) => {
                            let reason = frame.map(|frame| frame.reason.to_string()).filter(|reason| !reason.is_empty()).unwrap_or_else(|| "server closed the connection".to_owned());
                            break ConnectionExit::ConnectionLost(reason);
                        }
                        Ok(Message::Frame(_)) => {}
                        Err(error) => {
                            let reason = format!("WebSocket read failed: {error}");
                            emit_error(&inner, reason.clone());
                            break ConnectionExit::ConnectionLost(reason);
                        }
                    }
                }
                outbound_frame = outbound.recv() => {
                    let Some(outbound_frame) = outbound_frame else {
                        break ConnectionExit::Manual;
                    };
                    if !authenticated {
                        fail_pending_req(&inner, &outbound_frame.req_id, "WeCom client is not authenticated".to_owned());
                        continue;
                    }
                    if let Err(error) = send_frame(&mut writer, &outbound_frame.frame).await {
                        fail_pending_req(&inner, &outbound_frame.req_id, error.to_string());
                        emit_error(&inner, format!("failed to send WebSocket command: {error}"));
                        break ConnectionExit::ConnectionLost(error.to_string());
                    }
                }
            }
        };

        if !matches!(&exit, ConnectionExit::ServerDisconnected(_)) {
            let reason = match &exit {
                ConnectionExit::Manual => "connection manually closed".to_owned(),
                ConnectionExit::AuthenticationFailed => "authentication failed".to_owned(),
                ConnectionExit::ConnectionLost(reason) => reason.clone(),
                ConnectionExit::ServerDisconnected(_) => String::new(),
            };
            let _ = inner.events.send(WeComWsEvent::Disconnected { reason });
        }
        if !matches!(&exit, ConnectionExit::Manual) {
            inner.status.send_replace(WeComWsStatus::Disconnected);
        }
        fail_all_pending(&inner, "WebSocket connection closed");

        match exit {
            ConnectionExit::Manual => break,
            ConnectionExit::ServerDisconnected(reason) => {
                let _ = inner.events.send(WeComWsEvent::ServerDisconnected {
                    reason: reason.clone(),
                });
                let _ = inner.events.send(WeComWsEvent::Disconnected { reason });
                break;
            }
            ConnectionExit::AuthenticationFailed => {
                let retry =
                    schedule_retry(&inner, &mut shutdown, true, &mut auth_failure_attempts).await;
                if !retry {
                    break;
                }
            }
            ConnectionExit::ConnectionLost(_) => {
                let retry =
                    schedule_retry(&inner, &mut shutdown, false, &mut reconnect_attempts).await;
                if !retry {
                    break;
                }
            }
        }
    }
    if *inner.status.borrow() != WeComWsStatus::Closed {
        inner.status.send_replace(WeComWsStatus::Disconnected);
    }
    *lock_recover(&inner.outbound) = None;
}

enum FrameAction {
    Continue,
    AuthenticationFailed(String),
    ServerDisconnected(String),
}

fn handle_server_frame(
    inner: &Arc<Inner>,
    frame: &Value,
    authenticated: &mut bool,
    missed_heartbeats: &mut u8,
) -> FrameAction {
    let command = frame.get("cmd").and_then(Value::as_str).unwrap_or_default();
    let req_id = frame
        .pointer("/headers/req_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if command == "aibot_msg_callback" {
        if let Some(msgtype) = frame
            .pointer("/body/msgtype")
            .and_then(Value::as_str)
            .filter(|msgtype| !msgtype.is_empty())
        {
            let _ = inner.events.send(WeComWsEvent::Message {
                msgtype: msgtype.to_owned(),
                frame: frame.clone(),
            });
        }
        return FrameAction::Continue;
    }
    if command == "aibot_event_callback" {
        if frame
            .pointer("/body/msgtype")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return FrameAction::Continue;
        }
        let event_type = frame
            .pointer("/body/event/eventtype")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let is_server_disconnect = event_type.as_deref() == Some("disconnected_event");
        let _ = inner.events.send(WeComWsEvent::Event {
            event_type: event_type.clone(),
            frame: frame.clone(),
        });
        if is_server_disconnect {
            fail_all_pending(inner, "server disconnected this connection");
            return FrameAction::ServerDisconnected(
                "new connection established; server disconnected this connection".to_owned(),
            );
        }
        return FrameAction::Continue;
    }

    // The SDK identifies auth and heartbeat replies by req_id prefix, then
    // resolves ordinary acks by the exact request ID.
    if req_id.starts_with("aibot_subscribe") {
        if frame.get("errcode").and_then(Value::as_i64) != Some(0) {
            let reason = format_ack_error(frame);
            return FrameAction::AuthenticationFailed(reason);
        }
        *authenticated = true;
        inner.status.send_replace(WeComWsStatus::Authenticated);
        let _ = inner.events.send(WeComWsEvent::Authenticated);
        return FrameAction::Continue;
    }
    if req_id.starts_with("ping") {
        if frame.get("errcode").and_then(Value::as_i64) == Some(0) {
            *missed_heartbeats = 0;
        } else {
            emit_error(
                inner,
                format!(
                    "heartbeat acknowledgement failed: {}",
                    format_ack_error(frame)
                ),
            );
        }
        return FrameAction::Continue;
    }
    if !req_id.is_empty() {
        if let Some(sender) = lock_recover(&inner.pending).remove(req_id) {
            let response = if frame.get("errcode").and_then(Value::as_i64) == Some(0) {
                Ok(frame.clone())
            } else {
                Err(format_ack_error(frame))
            };
            let _ = sender.send(response);
        }
    }
    FrameAction::Continue
}

async fn schedule_retry(
    inner: &Arc<Inner>,
    shutdown: &mut watch::Receiver<bool>,
    authentication: bool,
    attempts: &mut usize,
) -> bool {
    let limit = if authentication {
        inner.options.max_auth_failure_attempts
    } else {
        inner.options.max_reconnect_attempts
    };
    if limit.is_some_and(|limit| *attempts >= limit) {
        let error = if authentication {
            WeComWsError::AuthenticationExhausted(*attempts)
        } else {
            WeComWsError::ReconnectExhausted(*attempts)
        };
        emit_error(inner, error.to_string());
        inner.status.send_replace(WeComWsStatus::Disconnected);
        return false;
    }
    *attempts = (*attempts).saturating_add(1);
    let exponent = u32::try_from((*attempts).saturating_sub(1)).unwrap_or(u32::MAX);
    let delay = inner
        .options
        .reconnect_base_delay
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(MAX_RECONNECT_DELAY);
    let _ = inner.events.send(WeComWsEvent::Reconnecting {
        attempt: *attempts,
        authentication,
    });
    tokio::select! {
        _ = sleep(delay) => !*shutdown.borrow(),
        changed = shutdown.changed() => changed.is_ok() && !*shutdown.borrow(),
    }
}

async fn send_frame(writer: &mut SocketWriter, frame: &Value) -> Result<(), WeComWsError> {
    let encoded = serde_json::to_string(frame)
        .map_err(|error| WeComWsError::Connection(error.to_string()))?;
    if encoded.len() > WECOM_WS_MAX_FRAME_BYTES {
        return Err(WeComWsError::FrameTooLarge {
            actual: encoded.len(),
        });
    }
    writer
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|error| WeComWsError::Connection(error.to_string()))
}

fn check_frame_size(frame: &Value) -> Result<(), WeComWsError> {
    let actual = serde_json::to_vec(frame)
        .map_err(|error| WeComWsError::Connection(error.to_string()))?
        .len();
    if actual > WECOM_WS_MAX_FRAME_BYTES {
        return Err(WeComWsError::FrameTooLarge { actual });
    }
    Ok(())
}

fn parse_server_frame(bytes: &[u8]) -> Option<Value> {
    if bytes.len() > WECOM_WS_MAX_FRAME_BYTES {
        return None;
    }
    let text = String::from_utf8_lossy(bytes);
    // Match the SDK's removal of raw control bytes before JSON.parse.
    let cleaned = text
        .chars()
        .filter(|character| !matches!(*character as u32, 0x00..=0x08 | 0x0b..=0x0c | 0x0e..=0x1f))
        .collect::<String>();
    serde_json::from_str(&cleaned).ok()
}

fn fail_pending_req(inner: &Arc<Inner>, req_id: &str, message: String) {
    if let Some(sender) = lock_recover(&inner.pending).remove(req_id) {
        let _ = sender.send(Err(message));
    }
}

fn fail_all_pending(inner: &Arc<Inner>, reason: &str) {
    let pending = std::mem::take(&mut *lock_recover(&inner.pending));
    for (req_id, sender) in pending {
        let _ = sender.send(Err(format!("{reason}; request {req_id} cancelled")));
    }
}

fn format_ack_error(frame: &Value) -> String {
    let code = frame
        .get("errcode")
        .map(Value::to_string)
        .unwrap_or_else(|| "missing".to_owned());
    let message = frame
        .get("errmsg")
        .and_then(Value::as_str)
        .unwrap_or("unknown server error");
    format!("errcode={code}, errmsg={message}")
}

fn emit_error(inner: &Arc<Inner>, message: String) {
    let message = message.chars().take(256).collect::<String>();
    let _ = inner.events.send(WeComWsEvent::Error { message });
}

fn generate_req_id(prefix: &str) -> String {
    let millis = chrono::Utc::now().timestamp_millis();
    let random = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}_{millis}_{random}")
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
