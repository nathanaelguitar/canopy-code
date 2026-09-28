//! Dispatches ACP child callbacks to the host-side bridge client.
//!
//! The TypeScript SDK owns JSON-RPC demultiplexing and invokes `Client`
//! methods concurrently. This module provides the same seam for the native
//! transport: standard client callbacks and extension callbacks are routed
//! separately, request failures are made log-safe before crossing the wire,
//! and active callback work is bounded by both count and estimated bytes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use super::bridge_client::{AcpRpcConnection, AcpRpcErrorResponse, AcpRpcHandle, AcpRpcInbound};

pub type BridgeClientFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Typed surface corresponding to the ACP SDK `Client` callbacks.
///
/// `Extension` is used for ACP extension methods such as `qwen/...` and
/// `craft/...`; unknown standard requests are rejected before reaching the
/// host. The host implementation can bind this interface to permission,
/// filesystem, event-bus, and extension services independently.
pub trait AcpBridgeClient: Send + Sync + 'static {
    fn request<'a>(
        &'a self,
        method: ClientRequestMethod,
        params: Value,
    ) -> BridgeClientFuture<'a, Result<Value, ClientRequestFailure>>;

    fn notification<'a>(
        &'a self,
        method: ClientNotificationMethod,
        params: Value,
    ) -> BridgeClientFuture<'a, Result<(), ClientRequestFailure>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientRequestMethod {
    RequestPermission,
    ReadTextFile,
    WriteTextFile,
    Extension(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientNotificationMethod {
    SessionUpdate,
    Extension(String),
}

#[derive(Clone, Debug, Error)]
#[error("ACP client callback failed ({code}): {message}")]
pub struct ClientRequestFailure {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl ClientRequestFailure {
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("Method not found: {method}"),
            data: None,
        }
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AcpClientDispatchOptions {
    pub max_active_handlers: usize,
    pub max_active_handler_bytes: usize,
}

impl Default for AcpClientDispatchOptions {
    fn default() -> Self {
        Self {
            max_active_handlers: 256,
            max_active_handler_bytes: 32 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Error)]
pub enum AcpClientDispatchError {
    #[error("ACP client dispatch limits must be positive")]
    InvalidLimits,
    #[error(
        "ACP inbound handler capacity exceeded (active handlers or bytes exhausted; required {required_bytes} bytes)"
    )]
    CapacityExceeded { required_bytes: usize },
    #[error("ACP client callback task failed: {0}")]
    CallbackTask(String),
}

/// Run callback dispatch until the child transport closes. The connection's
/// NDJSON decoder already accounts queued frames; this second budget covers
/// decoded parameters and callback results retained while host work is live.
/// Exceeding either limit closes the child transport, matching the source
/// bridge's fail-closed admission guard.
pub async fn run_client_dispatch(
    connection: &mut AcpRpcConnection,
    client: Arc<dyn AcpBridgeClient>,
    options: AcpClientDispatchOptions,
) -> Result<(), AcpClientDispatchError> {
    if options.max_active_handlers == 0
        || options.max_active_handler_bytes == 0
        || options.max_active_handler_bytes > u32::MAX as usize
    {
        return Err(AcpClientDispatchError::InvalidLimits);
    }
    let active_handlers = Arc::new(Semaphore::new(options.max_active_handlers));
    let active_bytes = Arc::new(Semaphore::new(options.max_active_handler_bytes));
    let handle = connection.handle();
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            inbound = connection.next_inbound() => {
                let Some(inbound) = inbound else { break; };
                let params = match &inbound {
                    AcpRpcInbound::Request(request) => &request.params,
                    AcpRpcInbound::Notification(notification) => &notification.params,
                };
                let estimated = estimate_handler_bytes(params, options.max_active_handler_bytes);
                let required_bytes = estimated.saturating_add(2_048.min(options.max_active_handler_bytes));
                let count_permit = match active_handlers.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let _ = connection.kill().await;
                        return Err(AcpClientDispatchError::CapacityExceeded { required_bytes });
                    }
                };
                let byte_count = required_bytes.max(1).min(u32::MAX as usize) as u32;
                let bytes_permit = match active_bytes.clone().try_acquire_many_owned(byte_count) {
                    Ok(permit) => permit,
                    Err(_) => {
                        drop(count_permit);
                        let _ = connection.kill().await;
                        return Err(AcpClientDispatchError::CapacityExceeded { required_bytes });
                    }
                };
                let client = client.clone();
                let handle = handle.clone();
                tasks.spawn(async move {
                    dispatch_one(client, handle, inbound, count_permit, bytes_permit).await
                });
            }
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    let _ = connection.kill().await;
                    return Err(AcpClientDispatchError::CallbackTask(error.to_string()));
                }
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            return Err(AcpClientDispatchError::CallbackTask(error.to_string()));
        }
    }
    Ok(())
}

async fn dispatch_one(
    client: Arc<dyn AcpBridgeClient>,
    connection: AcpRpcHandle,
    inbound: AcpRpcInbound,
    _count_permit: OwnedSemaphorePermit,
    _bytes_permit: OwnedSemaphorePermit,
) {
    match inbound {
        AcpRpcInbound::Request(request) => {
            let result = match route_request(&request.method) {
                Some(method) => client.request(method, request.params).await,
                None => Err(ClientRequestFailure::method_not_found(&request.method)),
            };
            let result = result.map_err(log_safe_request_error);
            if connection.respond(request.id, result).await.is_err() {
                connection.kill_sync();
            }
        }
        AcpRpcInbound::Notification(notification) => {
            let method = route_notification(&notification.method);
            let charge_bytes = notification.queue_charge_bytes;
            if let Some(method) = method {
                let _ = client.notification(method, notification.params).await;
            }
            connection.release_notification_charge(charge_bytes).await;
        }
    }
}

fn route_request(method: &str) -> Option<ClientRequestMethod> {
    match method {
        "session/request_permission" => Some(ClientRequestMethod::RequestPermission),
        "fs/read_text_file" => Some(ClientRequestMethod::ReadTextFile),
        "fs/write_text_file" => Some(ClientRequestMethod::WriteTextFile),
        _ if is_extension_method(method) => Some(ClientRequestMethod::Extension(method.to_owned())),
        _ => None,
    }
}

fn route_notification(method: &str) -> Option<ClientNotificationMethod> {
    match method {
        "session/update" => Some(ClientNotificationMethod::SessionUpdate),
        _ if is_extension_method(method) => {
            Some(ClientNotificationMethod::Extension(method.to_owned()))
        }
        _ => None,
    }
}

fn is_extension_method(method: &str) -> bool {
    method.starts_with("qwen/") || method.starts_with("craft/")
}

/// Mirror the source `logSafeAcpError` boundary: known JSON-RPC codes map to
/// fixed messages, and only a small allow-list of structured error data can
/// cross back to the child. Arbitrary callback messages remain diagnostic-only.
fn log_safe_request_error(error: ClientRequestFailure) -> AcpRpcErrorResponse {
    let code = error.code;
    AcpRpcErrorResponse {
        code,
        message: match code {
            -32700 => "Parse error",
            -32600 => "Invalid request",
            -32601 => "Method not found",
            -32602 => "Invalid params",
            -32603 => "Internal error",
            -32000 => "Authentication required",
            -32002 => "Resource not found",
            _ => "ACP client request failed",
        }
        .to_owned(),
        data: log_safe_error_data(error.data.as_ref()),
    }
}

fn log_safe_error_data(data: Option<&Value>) -> Option<Value> {
    let Value::Object(object) = data? else {
        return None;
    };
    let error_kind = object.get("errorKind")?.as_str()?;
    let mut safe = Map::new();
    safe.insert(
        "errorKind".to_owned(),
        json!(truncate_chars(error_kind, 128)),
    );
    if let Some(status) = object.get("status").and_then(Value::as_i64) {
        safe.insert("status".to_owned(), json!(status));
    }
    if let Some(hint) = object.get("hint").and_then(Value::as_str) {
        safe.insert("hint".to_owned(), json!(truncate_chars(hint, 512)));
    }
    Some(Value::Object(safe))
}

fn truncate_chars(value: &str, limit: usize) -> String {
    let mut chars = value.chars();
    let prefix: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        format!("{prefix}…")
    } else {
        prefix
    }
}

fn estimate_handler_bytes(params: &Value, limit_bytes: usize) -> usize {
    // JSON params cannot contain cycles. Serialized size is a conservative,
    // stable estimate of the shape retained by the callback.
    serde_json::to_vec(params)
        .map(|encoded| encoded.len().min(limit_bytes.saturating_add(1)))
        .unwrap_or(limit_bytes.saturating_add(1))
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_callbacks_and_extensions_are_routed_separately() {
        assert_eq!(
            route_request("session/request_permission"),
            Some(ClientRequestMethod::RequestPermission)
        );
        assert_eq!(
            route_request("fs/read_text_file"),
            Some(ClientRequestMethod::ReadTextFile)
        );
        assert_eq!(
            route_request("qwen/control/create-sub-session"),
            Some(ClientRequestMethod::Extension(
                "qwen/control/create-sub-session".into()
            ))
        );
        assert_eq!(route_request("session/prompt"), None);
        assert_eq!(
            route_notification("session/update"),
            Some(ClientNotificationMethod::SessionUpdate)
        );
        assert_eq!(
            route_notification("craft/drainMidTurnQueue"),
            Some(ClientNotificationMethod::Extension(
                "craft/drainMidTurnQueue".into()
            ))
        );
    }

    #[test]
    fn errors_are_sanitized_and_only_structured_filesystem_fields_survive() {
        let mapped = log_safe_request_error(ClientRequestFailure {
            code: -32602,
            message: "secret path /private/token".into(),
            data: Some(json!({
                "errorKind":"untrusted_workspace",
                "status":403,
                "hint":"safe hint",
                "details":"secret"
            })),
        });
        assert_eq!(mapped.code, -32602);
        assert_eq!(mapped.message, "Invalid params");
        assert_eq!(
            mapped.data,
            Some(json!({"errorKind":"untrusted_workspace","status":403,"hint":"safe hint"}))
        );
        let unrecognized_code = log_safe_request_error(ClientRequestFailure {
            code: 7,
            message: "secret".into(),
            data: None,
        });
        assert_eq!(unrecognized_code.code, 7);
        assert_eq!(unrecognized_code.message, "ACP client request failed");
        assert_eq!(unrecognized_code.data, None);
    }

    #[test]
    fn callback_size_estimate_caps_large_parameters() {
        let params = json!({"body":"x".repeat(100)});
        assert!(estimate_handler_bytes(&params, 12) > 12);
        assert_eq!(estimate_handler_bytes(&json!({}), 12), 2);
    }
}
