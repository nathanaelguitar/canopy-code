//! JSON-RPC half of the ACP bridge: routes child requests to the host and
//! correlates daemon requests with child responses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use super::channel::{AcpChannelExitInfo, ChannelFailure};
use super::ndjson::{NdJsonDecoder, NdJsonError, NdJsonStreamLimits, encode_frame};
use super::process_registry::TrackedChildProcess;
use super::spawn_channel::{BoundedByteSender, SpawnedAcpChannel};

#[derive(Clone, Debug, PartialEq)]
pub struct AcpRpcRequest {
    pub id: Value,
    pub method: String,
    pub params: Value,
    pub frame_bytes: usize,
    pub queue_charge_bytes: usize,
}
#[derive(Clone, Debug, PartialEq)]
pub struct AcpRpcNotification {
    pub method: String,
    pub params: Value,
    pub frame_bytes: usize,
    pub queue_charge_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AcpRpcInbound {
    Request(AcpRpcRequest),
    Notification(AcpRpcNotification),
}
#[derive(Clone, Debug, PartialEq)]
pub struct AcpRpcErrorResponse {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

#[derive(Debug, Error, Clone)]
pub enum AcpRpcError {
    #[error("ACP transport failed: {0}")]
    Transport(String),
    #[error("ACP request timed out after {0}ms")]
    Timeout(u64),
    #[error("ACP request failed ({code}): {message}")]
    Remote {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("ACP request was canceled because the transport closed")]
    Closed,
    #[error(transparent)]
    Protocol(#[from] NdJsonError),
}

struct RpcState {
    writer: BoundedByteSender,
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, AcpRpcError>>>>,
    inbound_charges: Mutex<HashMap<String, usize>>,
    decoder: tokio::sync::Mutex<NdJsonDecoder>,
    next_id: AtomicU64,
    request_tx: mpsc::Sender<AcpRpcRequest>,
    notification_tx: mpsc::Sender<AcpRpcNotification>,
    failure_tx: tokio::sync::watch::Sender<Option<ChannelFailure>>,
    process: TrackedChildProcess,
}

/// Single-writer multiplexed ACP JSON-RPC connection. The child-to-host
/// request and notification receivers have one consumer each, preserving
/// ordering from the child stream.
pub struct AcpRpcConnection {
    state: Arc<RpcState>,
    requests: mpsc::Receiver<AcpRpcRequest>,
    notifications: mpsc::Receiver<AcpRpcNotification>,
    transport_failed: tokio::sync::watch::Receiver<Option<ChannelFailure>>,
    exited: tokio::sync::watch::Receiver<Option<AcpChannelExitInfo>>,
}

/// Cloneable request/response side of a connection. The receive queues stay
/// owned by `AcpRpcConnection`, while request handlers can hold this handle to
/// reply concurrently without serializing through the stream pump.
#[derive(Clone)]
pub struct AcpRpcHandle {
    state: Arc<RpcState>,
}

impl AcpRpcConnection {
    pub fn new(
        channel: SpawnedAcpChannel,
        limits: NdJsonStreamLimits,
    ) -> Result<Self, NdJsonError> {
        let decoder = NdJsonDecoder::new(limits)?;
        let (request_tx, requests) = mpsc::channel(limits.max_queued_messages);
        let (notification_tx, notifications) = mpsc::channel(limits.max_queued_messages);
        let source_failed = channel.transport_failed.clone();
        let (failure_tx, transport_failed) = tokio::sync::watch::channel(None);
        let incoming = channel.incoming;
        let outgoing = channel.outgoing;
        let exited = channel.exited;
        let process = channel.process;
        let state = Arc::new(RpcState {
            writer: outgoing,
            pending: Mutex::new(HashMap::new()),
            inbound_charges: Mutex::new(HashMap::new()),
            decoder: tokio::sync::Mutex::new(decoder),
            next_id: AtomicU64::new(1),
            request_tx,
            notification_tx,
            failure_tx,
            process,
        });
        let reader_state = state.clone();
        tokio::spawn(async move {
            read_loop(reader_state, incoming).await;
        });
        let state_for_failure = state.clone();
        let mut failed = source_failed.clone();
        let combined_failure_tx = state.failure_tx.clone();
        tokio::spawn(async move {
            while failed.changed().await.is_ok() {
                if let Some(error) = failed.borrow().clone() {
                    let _ = combined_failure_tx.send(Some(error.clone()));
                    fail_pending(
                        &state_for_failure,
                        AcpRpcError::Transport(format!("{error:?}")),
                    );
                    break;
                }
            }
        });
        Ok(Self {
            state,
            requests,
            notifications,
            transport_failed,
            exited,
        })
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, AcpRpcError> {
        self.handle().request(method, params, timeout_ms).await
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), AcpRpcError> {
        self.handle().notify(method, params).await
    }

    pub async fn respond(
        &self,
        id: Value,
        result: Result<Value, AcpRpcErrorResponse>,
    ) -> Result<(), AcpRpcError> {
        self.handle().respond(id, result).await
    }

    pub async fn release_notification(&self, notification: &AcpRpcNotification) {
        self.handle().release_notification(notification).await;
    }

    pub fn handle(&self) -> AcpRpcHandle {
        AcpRpcHandle {
            state: self.state.clone(),
        }
    }

    pub async fn next_request(&mut self) -> Option<AcpRpcRequest> {
        self.requests.recv().await
    }
    pub async fn next_notification(&mut self) -> Option<AcpRpcNotification> {
        self.notifications.recv().await
    }
    pub async fn next_inbound(&mut self) -> Option<AcpRpcInbound> {
        let mut requests_open = true;
        let mut notifications_open = true;
        loop {
            match (requests_open, notifications_open) {
                (false, false) => return None,
                (true, false) => match self.requests.recv().await {
                    Some(request) => return Some(AcpRpcInbound::Request(request)),
                    None => requests_open = false,
                },
                (false, true) => match self.notifications.recv().await {
                    Some(notification) => {
                        return Some(AcpRpcInbound::Notification(notification));
                    }
                    None => notifications_open = false,
                },
                (true, true) => {
                    tokio::select! {
                        request = self.requests.recv() => match request {
                            Some(request) => return Some(AcpRpcInbound::Request(request)),
                            None => requests_open = false,
                        },
                        notification = self.notifications.recv() => match notification {
                            Some(notification) => return Some(AcpRpcInbound::Notification(notification)),
                            None => notifications_open = false,
                        },
                    }
                }
            }
        }
    }
    pub fn transport_failed(&self) -> tokio::sync::watch::Receiver<Option<ChannelFailure>> {
        self.transport_failed.clone()
    }
    pub fn exited(&self) -> tokio::sync::watch::Receiver<Option<AcpChannelExitInfo>> {
        self.exited.clone()
    }
    pub async fn kill(&self) -> Result<(), String> {
        self.state.process.terminate().await
    }
    pub fn kill_sync(&self) {
        self.state.process.kill_sync();
    }
}

impl AcpRpcHandle {
    pub async fn kill(&self) -> Result<(), String> {
        self.state.process.terminate().await
    }

    pub fn kill_sync(&self) {
        self.state.process.kill_sync();
    }

    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout_ms: u64,
    ) -> Result<Value, AcpRpcError> {
        let id = Value::String(format!(
            "acp-{}",
            self.state.next_id.fetch_add(1, Ordering::Relaxed)
        ));
        let id_key = serde_json::to_string(&id).unwrap_or_default();
        let message = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
        let frame = encode_frame(&message, DAEMON_MAX_FRAME_BYTES)?;
        {
            let mut decoder = self.state.decoder.lock().await;
            decoder.admit_outbound_request(
                message.get("id").expect("request id exists"),
                frame.len(),
            )?;
        }
        let (sender, receiver) = oneshot::channel();
        lock(&self.state.pending).insert(id_key.clone(), sender);
        if let Err(error) = self.state.writer.send(Bytes::from(frame)).await {
            lock(&self.state.pending).remove(&id_key);
            self.state
                .decoder
                .lock()
                .await
                .discard_outbound_request(message.get("id").expect("request id exists"));
            return Err(AcpRpcError::Transport(error.to_string()));
        }
        if timeout_ms == 0 {
            receiver.await.map_err(|_| AcpRpcError::Closed)?
        } else {
            match tokio::time::timeout(Duration::from_millis(timeout_ms), receiver).await {
                Ok(result) => result.map_err(|_| AcpRpcError::Closed)?,
                Err(_) => {
                    lock(&self.state.pending).remove(&id_key);
                    self.state
                        .decoder
                        .lock()
                        .await
                        .discard_outbound_request(message.get("id").expect("request id exists"));
                    Err(AcpRpcError::Timeout(timeout_ms))
                }
            }
        }
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), AcpRpcError> {
        let frame = encode_frame(
            &json!({"jsonrpc":"2.0","method":method,"params":params}),
            DAEMON_MAX_FRAME_BYTES,
        )?;
        self.state
            .writer
            .send(Bytes::from(frame))
            .await
            .map_err(|error| AcpRpcError::Transport(error.to_string()))
    }

    pub async fn respond(
        &self,
        id: Value,
        result: Result<Value, AcpRpcErrorResponse>,
    ) -> Result<(), AcpRpcError> {
        let id_key = serde_json::to_string(&id).unwrap_or_default();
        let message = match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err(error) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":error.code,"message":error.message,"data":error.data}})
            }
        };
        let frame = encode_frame(&message, DAEMON_MAX_FRAME_BYTES)?;
        self.state
            .writer
            .send(Bytes::from(frame))
            .await
            .map_err(|error| AcpRpcError::Transport(error.to_string()))?;
        self.state
            .decoder
            .lock()
            .await
            .release_inbound_response(&serde_json::from_str(&id_key).unwrap_or(Value::Null));
        let charge = { lock(&self.state.inbound_charges).remove(&id_key) };
        if let Some(charge) = charge {
            self.state.decoder.lock().await.release_queued(1, charge);
        }
        Ok(())
    }

    pub async fn release_notification(&self, notification: &AcpRpcNotification) {
        self.release_notification_charge(notification.queue_charge_bytes)
            .await;
    }

    pub async fn release_notification_charge(&self, queue_charge_bytes: usize) {
        self.state
            .decoder
            .lock()
            .await
            .release_queued(1, queue_charge_bytes);
    }
}

const DAEMON_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

async fn read_loop(
    state: Arc<RpcState>,
    mut incoming: mpsc::Receiver<Result<Bytes, ChannelFailure>>,
) {
    while let Some(chunk) = incoming.recv().await {
        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(error) => {
                fail_pending(&state, AcpRpcError::Transport(format!("{error:?}")));
                break;
            }
        };
        let messages = {
            let mut decoder = state.decoder.lock().await;
            decoder.feed_detailed(&bytes)
        };
        let messages = match messages {
            Ok(messages) => messages,
            Err(error) => {
                let failure = ChannelFailure::Transport(error.to_string());
                let _ = state.failure_tx.send(Some(failure));
                let _ = state.process.terminate().await;
                fail_pending(&state, error.into());
                return;
            }
        };
        for decoded in messages {
            let message = decoded.value;
            let frame_bytes = decoded.frame_bytes;
            if let Some(id) = message.get("id") {
                if message.get("method").is_none() {
                    let key = serde_json::to_string(id).unwrap_or_default();
                    state.decoder.lock().await.discard_outbound_request(id);
                    if let Some(sender) = lock(&state.pending).remove(&key) {
                        let result = if let Some(value) = message.get("result") {
                            Ok(value.clone())
                        } else {
                            let error = &message["error"];
                            Err(AcpRpcError::Remote {
                                code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
                                message: error
                                    .get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or("ACP request failed")
                                    .to_owned(),
                                data: error.get("data").cloned(),
                            })
                        };
                        let _ = sender.send(result);
                    }
                    state
                        .decoder
                        .lock()
                        .await
                        .release_queued(1, decoded.queue_charge_bytes);
                    continue;
                }
                if let Some(method) = message.get("method").and_then(Value::as_str) {
                    let request = AcpRpcRequest {
                        id: id.clone(),
                        method: method.to_owned(),
                        params: message.get("params").cloned().unwrap_or(Value::Null),
                        frame_bytes,
                        queue_charge_bytes: decoded.queue_charge_bytes,
                    };
                    let key = serde_json::to_string(id).unwrap_or_default();
                    lock(&state.inbound_charges).insert(key.clone(), decoded.queue_charge_bytes);
                    if state.request_tx.send(request).await.is_err() {
                        lock(&state.inbound_charges).remove(&key);
                        state
                            .decoder
                            .lock()
                            .await
                            .release_queued(1, decoded.queue_charge_bytes);
                        return;
                    }
                    continue;
                }
            }
            if let Some(method) = message.get("method").and_then(Value::as_str) {
                let notification = AcpRpcNotification {
                    method: method.to_owned(),
                    params: message.get("params").cloned().unwrap_or(Value::Null),
                    frame_bytes,
                    queue_charge_bytes: decoded.queue_charge_bytes,
                };
                if state.notification_tx.send(notification).await.is_err() {
                    return;
                }
                continue;
            }
            state
                .decoder
                .lock()
                .await
                .release_queued(1, decoded.queue_charge_bytes);
        }
    }
    let mut decoder = state.decoder.lock().await;
    if let Err(error) = decoder.finish(true) {
        let _ = state
            .failure_tx
            .send(Some(ChannelFailure::Transport(error.to_string())));
        fail_pending(&state, error.into());
    } else {
        fail_pending(&state, AcpRpcError::Closed);
    }
}

fn fail_pending(state: &RpcState, error: AcpRpcError) {
    let mut pending = lock(&state.pending);
    for (_, sender) in pending.drain() {
        let _ = sender.send(Err(error.clone()));
    }
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_messages_have_standard_json_rpc_shapes() {
        let frame = encode_frame(
            &json!({"jsonrpc":"2.0","method":"session/cancel","params":{"sessionId":"s"}}),
            1024,
        )
        .unwrap();
        assert!(frame.ends_with(b"\n"));
        assert_eq!(
            serde_json::from_slice::<Value>(&frame[..frame.len() - 1]).unwrap()["method"],
            "session/cancel"
        );
    }
}
