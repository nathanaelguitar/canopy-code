//! High-level route client for the daemon REST/SSE and ACP transports.
//!
//! The TypeScript SDK has typed wrappers for each daemon endpoint. Rust keeps
//! response payloads as `serde_json::Value` so new endpoint fields remain
//! forward-compatible, while this client owns the shared URL, auth, error,
//! timeout, and cancellation behavior for all URL-shaped routes.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue,
};
use reqwest::{Method, Response, StatusCode};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::{Instant, sleep, timeout};
use url::Url;

use crate::daemon_auto_reconnect::{
    AutoReconnectError, AutoReconnectOptions, AutoReconnectRouteRequest,
    AutoReconnectSubscribeOptions, AutoReconnectTransport, DaemonTransportType,
    ReconnectEventStream, ReconnectFetchRequest,
};
use crate::daemon_rest::RestSseCancellation;
use crate::daemon_sse::DaemonEvent;
use crate::daemon_upload_progress::{UploadProgress, body_with_progress};

const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PROMPT_LIMIT: usize = 5;
const MAX_TIMER_DELAY_MS: u64 = 2_147_483_647;
const MAX_ROUTE_BODY_BYTES: usize = 1024 * 1024;
const MAX_ROUTE_UPLOAD_BYTES: usize = 32 * 1024 * 1024;
const EXTENSION_ARCHIVE_UPLOAD_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;
const AUTH_EXPIRY_GRACE: Duration = Duration::from_secs(30);

/// Client construction settings. `None` for `fetch_timeout` disables request
/// timeouts; `None` for `max_pending_prompts_per_session` disables the local
/// prompt cap. The defaults match the TypeScript client (30 seconds, 5).
#[derive(Clone)]
pub struct DaemonClientOptions {
    pub token: Option<String>,
    pub fetch_timeout: Option<Duration>,
    pub max_pending_prompts_per_session: Option<usize>,
    pub rest_client: Option<reqwest::Client>,
}

impl Default for DaemonClientOptions {
    fn default() -> Self {
        Self {
            token: None,
            fetch_timeout: Some(DEFAULT_FETCH_TIMEOUT),
            max_pending_prompts_per_session: Some(DEFAULT_PROMPT_LIMIT),
            rest_client: None,
        }
    }
}

/// Select the timeout behavior for one route call. A per-call zero duration
/// disables the timeout, matching `fetchWithTimeout(..., timeoutMs: 0)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RequestTimeout {
    #[default]
    ClientDefault,
    Disabled,
    After(Duration),
}

/// Choose the configured daemon transport or force the native REST client.
/// REST-only operations include downloads and endpoints whose TypeScript
/// wrappers explicitly use `mode: 'rest'`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DaemonRequestMode {
    #[default]
    Transport,
    Rest,
}

/// Per-request metadata shared by the JSON, no-content, and raw route helpers.
#[derive(Clone, Debug, Default)]
pub struct DaemonRequestOptions {
    pub client_id: Option<String>,
    pub cancellation: Option<RestSseCancellation>,
    pub timeout: RequestTimeout,
    pub mode: DaemonRequestMode,
    /// Optional content type for the REST transport. JSON routes default to
    /// `application/json` when a body is supplied.
    pub content_type: Option<String>,
    /// Additional REST headers. ACP routes currently support only auth and
    /// the client ID metadata field; custom headers are rejected there.
    pub extra_headers: HeaderMap,
    /// For the daemon's session prompt queue-full response, map its 503 body
    /// to `DaemonClientError::PendingPromptLimit` as the TypeScript client does.
    pub session_id_for_queue_error: Option<String>,
}

/// Deadline selection for `wait_for_extension_operation`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExtensionOperationWaitTimeout {
    /// Match TypeScript's default ten-minute deadline.
    #[default]
    Default,
    /// Stop waiting after this duration. Zero times out immediately.
    After(Duration),
    /// Continue polling until the operation is terminal or cancelled.
    Infinite,
}

/// Polling controls for an extension operation. Timing out or cancelling the
/// wait only stops the local poll; it does not cancel the server operation.
#[derive(Clone, Default)]
pub struct ExtensionOperationWaitOptions {
    /// Defaults to one second. Zero requests immediate repolling.
    pub poll_interval: Option<Duration>,
    pub timeout: ExtensionOperationWaitTimeout,
    pub cancellation: Option<RestSseCancellation>,
}

/// Validated event from workspace/session content generation. `raw` retains
/// additional wire properties accepted by the TypeScript parser.
#[derive(Clone, Debug, PartialEq)]
pub enum DaemonGenerationEvent {
    Started {
        request_id: String,
        model: String,
        model_source: String,
        raw: Value,
    },
    Thinking {
        request_id: String,
        raw: Value,
    },
    Delta {
        request_id: String,
        seq: u64,
        text: String,
        raw: Value,
    },
    Done {
        request_id: String,
        model: String,
        model_source: String,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        raw: Value,
    },
    Error {
        code: String,
        message: String,
        raw: Value,
    },
}

/// Incremental generation events; errors are yielded once before the stream
/// ends. Dropping the stream closes the response body.
pub type DaemonGenerationEventStream =
    Pin<Box<dyn Stream<Item = Result<DaemonGenerationEvent, DaemonClientError>> + Send>>;

/// Request metadata and cancellation for a generated-content stream.
#[derive(Clone, Default)]
pub struct DaemonGenerationOptions {
    pub client_id: Option<String>,
    pub cancellation: Option<RestSseCancellation>,
}

struct GenerationStreamState {
    source: Pin<Box<dyn Stream<Item = Result<Value, crate::daemon_sse::SseError>> + Send>>,
    cancellation: Option<watch::Receiver<bool>>,
    // Keep the sender alive for the stream lifetime when the caller moved its
    // cancellation handle into `DaemonGenerationOptions`.
    _cancellation_handle: Option<RestSseCancellation>,
    require_terminal: bool,
    saw_terminal: bool,
    finished: bool,
}

/// HTTP error contract exposed by the TypeScript `DaemonHttpError` class.
#[derive(Clone, Debug)]
pub struct DaemonHttpError {
    pub status: u16,
    pub body: Option<Value>,
    pub message: String,
    /// True for a `turn_error` event converted into the TypeScript daemon
    /// turn-error variant.
    pub is_daemon_turn_error: bool,
}

impl std::fmt::Display for DaemonHttpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for DaemonHttpError {}

/// A structured variant of the daemon's `prompt_queue_full` error.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("Pending prompts full: \"{session_id}\" ({pending_count}/{limit})")]
pub struct DaemonPendingPromptLimitError {
    pub session_id: String,
    pub limit: f64,
    pub pending_count: f64,
}

/// Missing daemon feature error returned by `require_capability`.
#[derive(Clone, Debug, Error, PartialEq)]
#[error(
    "DaemonCapabilities.{capability} is missing — {hint}. The daemon you are connected to likely predates the feature that added this field; upgrade the daemon or fall back to a different code path that doesn't require it."
)]
pub struct DaemonCapabilityMissingError {
    pub capability: String,
    pub hint: String,
}

/// Protocol error for a daemon that creates a different session than requested.
#[derive(Clone, Debug, Error, PartialEq)]
#[error(
    "Daemon returned session \"{actual_session_id}\" instead of requested session \"{requested_session_id}\"."
)]
pub struct DaemonSessionIdProtocolError {
    pub requested_session_id: String,
    pub actual_session_id: String,
}

/// Failures returned by the high-level daemon client.
#[derive(Debug, Error)]
pub enum DaemonClientError {
    #[error("{0}")]
    Http(#[from] DaemonHttpError),
    #[error("{0}")]
    PendingPromptLimit(#[from] DaemonPendingPromptLimitError),
    #[error("{0}")]
    CapabilityMissing(#[from] DaemonCapabilityMissingError),
    #[error("{0}")]
    SessionIdProtocol(#[from] DaemonSessionIdProtocolError),
    #[error("daemon request timed out")]
    Timeout,
    #[error("daemon request was cancelled")]
    Cancelled,
    #[error("daemon event stream ended before the operation completed")]
    EventStreamEnded,
    #[error(
        "Timed out waiting for extension operation {operation_id}. The server operation was not cancelled."
    )]
    ExtensionOperationTimeout { operation_id: String },
    #[error("generation SSE stream ended without a terminal event")]
    GenerationStreamEnded,
    #[error("invalid daemon URL: {0}")]
    InvalidUrl(String),
    #[error("invalid daemon route: {0}")]
    InvalidRoute(String),
    #[error("invalid daemon response: {0}")]
    InvalidResponse(String),
    #[error("daemon response body exceeds {limit} bytes")]
    ResponseTooLarge { limit: usize },
    #[error("unsupported request shape for the selected ACP transport: {0}")]
    UnsupportedRequest(String),
    #[error("daemon transport operation failed: {0}")]
    Transport(#[from] AutoReconnectError),
}

/// Rust route-level counterpart to TypeScript's `DaemonClient`.
///
/// `request_json` and `request_no_content` expose every daemon URL route,
/// including endpoints added after this client was compiled. Named helpers
/// below cover session prompts, event subscriptions, health/capability
/// discovery, and workspace scoping.
#[derive(Clone)]
pub struct DaemonClient {
    base_url: String,
    token: Option<String>,
    rest_client: reqwest::Client,
    fetch_timeout: Option<Duration>,
    prompt_limit: Option<usize>,
    prompt_counts: Arc<Mutex<HashMap<String, usize>>>,
    cached_restore_timeout: Arc<Mutex<Option<Duration>>>,
    transport: AutoReconnectTransport,
}

impl DaemonClient {
    /// Create a REST/SSE client with the default 30-second request timeout and
    /// five admitted prompt calls per session.
    pub fn new(base_url: impl Into<String>) -> Result<Self, DaemonClientError> {
        Self::with_options(base_url, DaemonClientOptions::default())
    }

    /// Create a REST/SSE client with explicit settings.
    pub fn with_options(
        base_url: impl Into<String>,
        mut options: DaemonClientOptions,
    ) -> Result<Self, DaemonClientError> {
        let base_url = strip_trailing_slashes(base_url.into());
        // Explicit tokens are passed through as supplied. Only the
        // environment fallback is trimmed, matching the TypeScript client.
        let token = options
            .token
            .take()
            .or_else(read_token_from_env)
            .filter(|value| !value.is_empty());
        let rest_client = options.rest_client.clone().unwrap_or_default();
        let mut transport_options = AutoReconnectOptions::new(base_url.clone());
        transport_options.token = token.clone();
        transport_options.rest_client = Some(rest_client.clone());
        let transport = AutoReconnectTransport::with_options(transport_options)?;
        Self::assemble(base_url, token, rest_client, options, transport)
    }

    /// Wrap a caller-configured REST, ACP-over-HTTP, ACP-over-WebSocket, or
    /// auto-reconnecting transport. `token` must match the transport's auth
    /// token when ACP is selected; it is also used for forced REST requests.
    pub fn with_transport(
        base_url: impl Into<String>,
        transport: AutoReconnectTransport,
        mut options: DaemonClientOptions,
    ) -> Result<Self, DaemonClientError> {
        let base_url = strip_trailing_slashes(base_url.into());
        let token = options
            .token
            .take()
            .or_else(read_token_from_env)
            .filter(|value| !value.is_empty());
        let rest_client = options.rest_client.clone().unwrap_or_default();
        Self::assemble(base_url, token, rest_client, options, transport)
    }

    fn assemble(
        base_url: String,
        token: Option<String>,
        rest_client: reqwest::Client,
        options: DaemonClientOptions,
        transport: AutoReconnectTransport,
    ) -> Result<Self, DaemonClientError> {
        let parsed = Url::parse(&base_url)
            .map_err(|error| DaemonClientError::InvalidUrl(error.to_string()))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(DaemonClientError::InvalidUrl(
                "daemon URL must use http or https".into(),
            ));
        }
        Ok(Self {
            base_url,
            token,
            rest_client,
            fetch_timeout: options.fetch_timeout,
            prompt_limit: options
                .max_pending_prompts_per_session
                .filter(|limit| *limit > 0),
            prompt_counts: Arc::new(Mutex::new(HashMap::new())),
            cached_restore_timeout: Arc::new(Mutex::new(None)),
            transport,
        })
    }

    pub fn transport(&self) -> &AutoReconnectTransport {
        &self.transport
    }

    pub fn auth(&self) -> DaemonAuthFlow {
        DaemonAuthFlow::new(self.clone())
    }

    pub fn max_pending_prompts_per_session(&self) -> Option<usize> {
        self.prompt_limit
    }

    pub fn cached_session_restore_timeout(&self) -> Option<Duration> {
        *lock_recover(&self.cached_restore_timeout)
    }

    pub fn dispose(&self) {
        self.transport.dispose();
    }

    /// Dispatch any URL-shaped daemon route and parse its successful body as
    /// JSON. `path` is rooted at `/`, with query parameters passed separately.
    pub async fn request_json(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        options: DaemonRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let response = self
            .request_raw(method, path, query, body, options.clone())
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(self.http_error(
                response.status,
                response.body,
                label,
                options.session_id_for_queue_error.as_deref(),
            ));
        }
        response.body.ok_or_else(|| {
            DaemonClientError::InvalidResponse(format!("{label}: expected a JSON response body"))
        })
    }

    /// Dispatch a route expected to return no content. A caller can optionally
    /// treat one daemon 404 code as an idempotent success.
    pub async fn request_no_content(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        ok_not_found_code: Option<&str>,
        options: DaemonRequestOptions,
    ) -> Result<(), DaemonClientError> {
        let response = self
            .request_raw(method, path, query, body, options.clone())
            .await?;
        if response.status == StatusCode::NO_CONTENT.as_u16() {
            return Ok(());
        }
        if response.status == StatusCode::NOT_FOUND.as_u16()
            && ok_not_found_code.is_some_and(|code| {
                response
                    .body
                    .as_ref()
                    .and_then(|body| body.get("code"))
                    .and_then(Value::as_str)
                    == Some(code)
            })
        {
            return Ok(());
        }
        Err(self.http_error(
            response.status,
            response.body,
            label,
            options.session_id_for_queue_error.as_deref(),
        ))
    }

    /// Dispatch a route and retain the status/body pair. Non-2xx statuses are
    /// returned for route-specific cases such as permission-vote 404s.
    pub async fn request_raw(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Value>,
        options: DaemonRequestOptions,
    ) -> Result<DaemonRawResponse, DaemonClientError> {
        validate_route(path)?;
        let request = AutoReconnectRouteRequest {
            http_method: method.to_owned(),
            path: path.to_owned(),
            query: query.trim_start_matches('?').to_owned(),
            body: body.clone().unwrap_or(Value::Null),
            client_id: options.client_id.clone(),
            cancellation: options.cancellation.clone(),
        };
        let timeout = self.resolve_timeout(options.timeout);
        let operation = async {
            if options.mode == DaemonRequestMode::Rest {
                self.direct_rest_request(method, path, query, body, &options)
                    .await
            } else if self.transport.transport_type() == DaemonTransportType::Rest {
                match self
                    .rest_fetch_request(method, path, query, body, &options)
                    .await
                {
                    Err(error) if self.transport.transport_type() != DaemonTransportType::Rest => {
                        if !options.extra_headers.is_empty()
                            || options
                                .content_type
                                .as_deref()
                                .is_some_and(|value| value != "application/json")
                        {
                            Err(DaemonClientError::UnsupportedRequest(
                                "custom headers and non-JSON content types require REST mode"
                                    .into(),
                            ))
                        } else {
                            self.dispatch_acp_route(request).await.or(Err(error))
                        }
                    }
                    result => result,
                }
            } else {
                if !options.extra_headers.is_empty()
                    || options
                        .content_type
                        .as_deref()
                        .is_some_and(|value| value != "application/json")
                {
                    return Err(DaemonClientError::UnsupportedRequest(
                        "custom headers and non-JSON content types require REST mode".into(),
                    ));
                }
                self.dispatch_acp_route(request).await
            }
        };
        let operation = timeout_future(operation, timeout);
        if let Some(cancellation) = options.cancellation.as_ref() {
            if cancellation.is_cancelled() {
                return Err(DaemonClientError::Cancelled);
            }
            let mut receiver = cancellation.subscribe_cancelled();
            tokio::select! {
                biased;
                _ = wait_for_cancel(&mut receiver) => Err(DaemonClientError::Cancelled),
                result = operation => result,
            }
        } else {
            operation.await
        }
    }

    /// Send and receive opaque bytes over REST. This covers audio, archive,
    /// file, and multipart requests that cannot use the JSON-shaped ACP route
    /// table. Uploads and successful responses are capped at 32 MiB; error
    /// responses are capped at 1 MiB.
    pub async fn request_bytes(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Vec<u8>>,
        label: &str,
        options: DaemonRequestOptions,
    ) -> Result<DaemonBytesResponse, DaemonClientError> {
        validate_route(path)?;
        if body
            .as_ref()
            .is_some_and(|body| body.len() > MAX_ROUTE_UPLOAD_BYTES)
        {
            return Err(DaemonClientError::InvalidRoute(format!(
                "request body exceeds {MAX_ROUTE_UPLOAD_BYTES} bytes"
            )));
        }
        let timeout = self.resolve_timeout(options.timeout);
        let operation = async {
            let response = if options.mode == DaemonRequestMode::Rest {
                self.direct_rest_bytes(method, path, query, body, &options)
                    .await?
            } else if self.transport.transport_type() == DaemonTransportType::Rest {
                self.rest_fetch_bytes(method, path, query, body, &options)
                    .await?
            } else {
                return Err(DaemonClientError::UnsupportedRequest(
                    "binary bodies require REST mode; ACP routes accept JSON values".into(),
                ));
            };
            if !(200..300).contains(&response.status) {
                let parsed_body = serde_json::from_slice(&response.body).ok().or_else(|| {
                    (!response.body.is_empty()).then(|| {
                        Value::String(String::from_utf8_lossy(&response.body).into_owned())
                    })
                });
                return Err(self.http_error(response.status, parsed_body, label, None));
            }
            Ok(response)
        };
        let operation = timeout_future(operation, timeout);
        if let Some(cancellation) = options.cancellation.as_ref() {
            if cancellation.is_cancelled() {
                return Err(DaemonClientError::Cancelled);
            }
            let mut receiver = cancellation.subscribe_cancelled();
            tokio::select! {
                biased;
                _ = wait_for_cancel(&mut receiver) => Err(DaemonClientError::Cancelled),
                result = operation => result,
            }
        } else {
            operation.await
        }
    }

    pub(crate) async fn request_bytes_with_progress<F>(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Vec<u8>,
        label: &str,
        options: DaemonRequestOptions,
        on_progress: F,
    ) -> Result<DaemonBytesResponse, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        validate_route(path)?;
        if body.len() > MAX_ROUTE_UPLOAD_BYTES {
            return Err(DaemonClientError::InvalidRoute(format!(
                "request body exceeds {MAX_ROUTE_UPLOAD_BYTES} bytes"
            )));
        }
        if options.mode != DaemonRequestMode::Rest {
            return Err(DaemonClientError::UnsupportedRequest(
                "upload progress requires direct REST mode".into(),
            ));
        }
        let timeout = self.resolve_timeout(options.timeout);
        let operation = async {
            let response = self
                .direct_rest_bytes_with_progress(method, path, query, body, &options, on_progress)
                .await?;
            if !(200..300).contains(&response.status) {
                let parsed_body = serde_json::from_slice(&response.body).ok().or_else(|| {
                    (!response.body.is_empty()).then(|| {
                        Value::String(String::from_utf8_lossy(&response.body).into_owned())
                    })
                });
                return Err(self.http_error(response.status, parsed_body, label, None));
            }
            Ok(response)
        };
        let operation = timeout_future(operation, timeout);
        if let Some(cancellation) = options.cancellation.as_ref() {
            if cancellation.is_cancelled() {
                return Err(DaemonClientError::Cancelled);
            }
            let mut receiver = cancellation.subscribe_cancelled();
            tokio::select! {
                biased;
                _ = wait_for_cancel(&mut receiver) => Err(DaemonClientError::Cancelled),
                result = operation => result,
            }
        } else {
            operation.await
        }
    }

    /// Workspace-scoped form of [`Self::request_json`]. `workspace_selector`
    /// is percent-encoded as one path component, matching `workspaceById` and
    /// `workspaceByCwd` in the TypeScript client.
    pub async fn workspace_request_json(
        &self,
        workspace_selector: &str,
        method: &str,
        suffix: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        options: DaemonRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspaces/{}{}",
            encode_uri_component(workspace_selector),
            suffix
        );
        self.request_json(method, &path, query, body, label, options)
            .await
    }

    pub fn workspace_by_id(&self, workspace_id: &str) -> WorkspaceDaemonClient {
        WorkspaceDaemonClient {
            client: self.clone(),
            selector: encode_uri_component(workspace_id),
        }
    }

    pub fn workspace_by_cwd(&self, workspace_cwd: &str) -> WorkspaceDaemonClient {
        WorkspaceDaemonClient {
            client: self.clone(),
            selector: encode_uri_component(workspace_cwd),
        }
    }

    pub async fn health(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/health",
            "",
            None,
            "GET /health",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn capabilities(&self) -> Result<Value, DaemonClientError> {
        let value = self
            .request_json(
                "GET",
                "/capabilities",
                "",
                None,
                "GET /capabilities",
                DaemonRequestOptions::default(),
            )
            .await?;
        let restore_timeout = value
            .get("limits")
            .and_then(|limits| limits.get("sessionRestoreTimeoutMs"))
            .and_then(Value::as_f64)
            .filter(|value| {
                value.is_finite()
                    && value.fract() == 0.0
                    && *value > 0.0
                    && *value <= MAX_TIMER_DELAY_MS as f64
            })
            .map(|value| Duration::from_millis(value as u64));
        *lock_recover(&self.cached_restore_timeout) = restore_timeout;
        Ok(value)
    }

    pub async fn require_capability(&self, capability: &str) -> Result<(), DaemonClientError> {
        let capabilities = self.capabilities().await?;
        let has_capability = capabilities
            .get("features")
            .and_then(Value::as_array)
            .is_some_and(|features| {
                features
                    .iter()
                    .any(|feature| feature.as_str() == Some(capability))
            });
        if has_capability {
            Ok(())
        } else {
            Err(DaemonCapabilityMissingError {
                capability: capability.to_owned(),
                hint: format!("daemon does not advertise the {capability} feature"),
            }
            .into())
        }
    }

    /// Create or attach a session from the TypeScript request payload shape.
    /// The payload remains open JSON so newer daemon fields pass through.
    pub async fn create_or_attach_session(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let requested_session_id = request
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_lowercase);
        if request
            .get("sessionId")
            .is_some_and(|session_id| !session_id.is_null())
        {
            self.require_capability("session_id_override").await?;
        }
        if request.get("sourceType").is_some() || request.get("sourceId").is_some() {
            self.require_capability("session_source_metadata").await?;
        }
        let model_service_id = request
            .get("modelServiceId")
            .filter(|value| js_truthy(value))
            .cloned();
        let request_body = json_object([
            ("cwd", request.get("workspaceCwd").cloned()),
            ("sessionId", request.get("sessionId").cloned()),
            ("modelServiceId", model_service_id),
            ("sessionScope", request.get("sessionScope").cloned()),
            ("approvalMode", request.get("approvalMode").cloned()),
            ("sourceType", request.get("sourceType").cloned()),
            ("sourceId", request.get("sourceId").cloned()),
            ("worktree", request.get("worktree").cloned()),
            ("branch", request.get("branch").cloned()),
        ]);
        let response = self
            .request_json(
                "POST",
                "/session",
                "",
                Some(request_body),
                "POST /session",
                DaemonRequestOptions {
                    client_id,
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        if let Some(requested_session_id) = requested_session_id {
            let actual_session_id = response
                .get("sessionId")
                .map(js_string)
                .unwrap_or_else(|| "undefined".into());
            if actual_session_id != requested_session_id {
                return Err(DaemonSessionIdProtocolError {
                    requested_session_id,
                    actual_session_id,
                }
                .into());
            }
        }
        Ok(response)
    }

    /// Open the transport's session event stream. An unset connect timeout is
    /// filled from the client's short-request timeout; an explicit zero
    /// disables it.
    pub async fn subscribe_events(
        &self,
        session_id: &str,
        mut options: AutoReconnectSubscribeOptions,
    ) -> Result<ReconnectEventStream, DaemonClientError> {
        if options.connect_timeout.is_none() {
            options.connect_timeout = self.fetch_timeout;
        }
        self.transport
            .subscribe_events(session_id.to_owned(), options)
            .await
            .map_err(Into::into)
    }

    /// Send a prompt without waiting for the resulting turn. Returns the
    /// daemon's 202 acceptance envelope or legacy 200 prompt result unchanged.
    pub async fn prompt_non_blocking(
        &self,
        session_id: &str,
        request: Value,
        cancellation: Option<RestSseCancellation>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let options = DaemonRequestOptions {
            mode: DaemonRequestMode::Transport,
            timeout: RequestTimeout::Disabled,
            client_id,
            cancellation,
            session_id_for_queue_error: Some(session_id.to_owned()),
            ..DaemonRequestOptions::default()
        };
        let response = self
            .request_raw(
                "POST",
                &format!("/session/{}/prompt", encode_uri_component(session_id)),
                "",
                Some(request),
                options.clone(),
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(self.http_error(
                response.status,
                response.body,
                "POST /session/:id/prompt",
                Some(session_id),
            ));
        }
        response.body.ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "POST /session/:id/prompt: expected a JSON response body".into(),
            )
        })
    }

    /// Send a prompt and await its matching terminal event for 202 daemons.
    /// The per-session pending slot remains occupied until that event arrives.
    pub async fn prompt(
        &self,
        session_id: &str,
        request: Value,
        cancellation: Option<RestSseCancellation>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let _permit = self.reserve_prompt_slot(session_id)?;
        let options = DaemonRequestOptions {
            mode: DaemonRequestMode::Transport,
            timeout: RequestTimeout::Disabled,
            client_id: client_id.clone(),
            cancellation: cancellation.clone(),
            session_id_for_queue_error: Some(session_id.to_owned()),
            ..DaemonRequestOptions::default()
        };
        let response = self
            .request_raw(
                "POST",
                &format!("/session/{}/prompt", encode_uri_component(session_id)),
                "",
                Some(request),
                options.clone(),
            )
            .await?;
        if !(200..300).contains(&response.status) {
            return Err(self.http_error(
                response.status,
                response.body,
                "POST /session/:id/prompt",
                Some(session_id),
            ));
        }
        let body = response.body.ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "POST /session/:id/prompt: expected a JSON response body".into(),
            )
        })?;
        if response.status != StatusCode::ACCEPTED.as_u16() {
            return Ok(body);
        }

        let prompt_id = body
            .get("promptId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                DaemonClientError::InvalidResponse("202 prompt response has no promptId".into())
            })?
            .to_owned();
        let last_event_id = body
            .get("lastEventId")
            .and_then(Value::as_f64)
            .filter(|value| {
                value.is_finite()
                    && value.fract() == 0.0
                    && (0.0..=MAX_SAFE_INTEGER).contains(value)
            })
            .map(|value| value as u64)
            .ok_or_else(|| {
                DaemonClientError::InvalidResponse(
                    "202 prompt response has an invalid lastEventId".into(),
                )
            })?;
        let mut stream = self
            .subscribe_events(
                session_id,
                AutoReconnectSubscribeOptions {
                    last_event_id: Some(last_event_id),
                    epoch: body
                        .get("eventEpoch")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    client_id,
                    cancellation: cancellation.clone(),
                    ..AutoReconnectSubscribeOptions::default()
                },
            )
            .await?;
        while let Some(event) = stream.next().await {
            let event = match event {
                Ok(event) => event,
                Err(_)
                    if cancellation
                        .as_ref()
                        .is_some_and(RestSseCancellation::is_cancelled) =>
                {
                    let client = self.clone();
                    let session_id = session_id.to_owned();
                    let client_id = options.client_id.clone();
                    tokio::spawn(async move {
                        let _ = client.cancel(&session_id, client_id).await;
                    });
                    return Err(DaemonClientError::Cancelled);
                }
                Err(error) => return Err(error.into()),
            };
            if let Some(result) = match_turn_event(&event, &prompt_id)? {
                return Ok(result);
            }
        }
        if cancellation
            .as_ref()
            .is_some_and(RestSseCancellation::is_cancelled)
        {
            let client = self.clone();
            let session_id = session_id.to_owned();
            let client_id = options.client_id.clone();
            tokio::spawn(async move {
                let _ = client.cancel(&session_id, client_id).await;
            });
            return Err(DaemonClientError::Cancelled);
        }
        Err(DaemonClientError::EventStreamEnded)
    }

    pub async fn cancel(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let response = self
            .request_raw(
                "POST",
                &format!("/session/{}/cancel", encode_uri_component(session_id)),
                "",
                Some(json!({})),
                DaemonRequestOptions {
                    client_id,
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else {
            Err(self.http_error(
                response.status,
                response.body,
                "POST /session/:id/cancel",
                None,
            ))
        }
    }

    pub async fn close_session(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let response = self
            .request_raw(
                "DELETE",
                &format!("/session/{}", encode_uri_component(session_id)),
                "",
                None,
                DaemonRequestOptions {
                    client_id,
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        if response.status == StatusCode::NO_CONTENT.as_u16()
            || response.status == StatusCode::NOT_FOUND.as_u16()
        {
            Ok(())
        } else {
            Err(self.http_error(response.status, response.body, "DELETE /session/:id", None))
        }
    }

    fn resolve_timeout(&self, requested: RequestTimeout) -> Option<Duration> {
        match requested {
            RequestTimeout::ClientDefault => self.fetch_timeout,
            RequestTimeout::Disabled => None,
            RequestTimeout::After(duration) if !duration.is_zero() => Some(duration),
            RequestTimeout::After(_) => None,
        }
    }

    async fn dispatch_acp_route(
        &self,
        request: AutoReconnectRouteRequest,
    ) -> Result<DaemonRawResponse, DaemonClientError> {
        let response = self.transport.dispatch_route(request).await?;
        Ok(DaemonRawResponse {
            status: response.status,
            body: response.body,
        })
    }

    async fn rest_fetch_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Value>,
        options: &DaemonRequestOptions,
    ) -> Result<DaemonRawResponse, DaemonClientError> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        let url = self.route_url(path, query)?;
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, options)?;
        let bytes = body
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|error| DaemonClientError::InvalidResponse(error.to_string()))?;
        if bytes.is_some() && !headers.contains_key(CONTENT_TYPE) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        let request = ReconnectFetchRequest {
            url,
            method,
            headers,
            body: bytes,
            timeout: None,
        };
        let response = self.transport.fetch(request).await;
        match response {
            Ok(response) => response_to_json(response, MAX_ROUTE_BODY_BYTES).await,
            Err(error) if self.transport.transport_type() != DaemonTransportType::Rest => {
                Err(DaemonClientError::Transport(error))
            }
            Err(error) => Err(DaemonClientError::Transport(error)),
        }
    }

    async fn direct_rest_request(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Value>,
        options: &DaemonRequestOptions,
    ) -> Result<DaemonRawResponse, DaemonClientError> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        let url = self.route_url(path, query)?;
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, options)?;
        if body.is_some() && !headers.contains_key(CONTENT_TYPE) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        let mut request = self.rest_client.request(method, url).headers(headers);
        if let Some(body) = body {
            let bytes = serde_json::to_vec(&body)
                .map_err(|error| DaemonClientError::InvalidResponse(error.to_string()))?;
            request = request.body(bytes);
        }
        let response = request
            .send()
            .await
            .map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
        response_to_json(response, MAX_ROUTE_BODY_BYTES).await
    }

    async fn rest_fetch_bytes(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Vec<u8>>,
        options: &DaemonRequestOptions,
    ) -> Result<DaemonBytesResponse, DaemonClientError> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        let url = self.route_url(path, query)?;
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, options)?;
        if body.is_some() && !headers.contains_key(CONTENT_TYPE) {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
        }
        let request = ReconnectFetchRequest {
            url,
            method,
            headers,
            body,
            timeout: None,
        };
        let response = self.transport.fetch(request).await?;
        response_to_bytes(response, MAX_ROUTE_UPLOAD_BYTES).await
    }

    async fn direct_rest_bytes(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Option<Vec<u8>>,
        options: &DaemonRequestOptions,
    ) -> Result<DaemonBytesResponse, DaemonClientError> {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        let url = self.route_url(path, query)?;
        let has_body = body.is_some();
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, options)?;
        if has_body && !headers.contains_key(CONTENT_TYPE) {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
        }
        let mut request = self.rest_client.request(method, url).headers(headers);
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
        response_to_bytes(response, MAX_ROUTE_UPLOAD_BYTES).await
    }

    async fn direct_rest_bytes_with_progress<F>(
        &self,
        method: &str,
        path: &str,
        query: &str,
        body: Vec<u8>,
        options: &DaemonRequestOptions,
        on_progress: F,
    ) -> Result<DaemonBytesResponse, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        let method = Method::from_bytes(method.as_bytes())
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        let url = self.route_url(path, query)?;
        let content_length = HeaderValue::from_str(&body.len().to_string()).map_err(|error| {
            DaemonClientError::InvalidRoute(format!("invalid upload length: {error}"))
        })?;
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, options)?;
        if !headers.contains_key(CONTENT_TYPE) {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
        }
        headers.insert(CONTENT_LENGTH, content_length);
        let response = self
            .rest_client
            .request(method, url)
            .headers(headers)
            .body(body_with_progress(body, on_progress))
            .send()
            .await
            .map_err(|error| AutoReconnectError::Operation(error.to_string()))?;
        response_to_bytes(response, MAX_ROUTE_UPLOAD_BYTES).await
    }

    fn route_url(&self, path: &str, query: &str) -> Result<String, DaemonClientError> {
        validate_route(path)?;
        let mut url = format!("{}{}", self.base_url, path);
        let query = query.trim_start_matches('?');
        if !query.is_empty() {
            url.push('?');
            url.push_str(query);
        }
        Url::parse(&url).map_err(|error| DaemonClientError::InvalidUrl(error.to_string()))?;
        Ok(url)
    }

    fn http_error(
        &self,
        status: u16,
        body: Option<Value>,
        label: &str,
        session_id: Option<&str>,
    ) -> DaemonClientError {
        if status == StatusCode::SERVICE_UNAVAILABLE.as_u16()
            && let (Some(session_id), Some(body)) = (session_id, body.as_ref())
            && body.get("code").and_then(Value::as_str) == Some("prompt_queue_full")
        {
            return DaemonPendingPromptLimitError {
                session_id: body
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or(session_id)
                    .to_owned(),
                limit: body.get("limit").and_then(Value::as_f64).unwrap_or(0.0),
                pending_count: body
                    .get("pendingCount")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
            }
            .into();
        }
        let detail = body
            .as_ref()
            .and_then(|body| body.get("error"))
            .map(js_string)
            .unwrap_or_else(|| format!("HTTP {status}"));
        DaemonHttpError {
            status,
            body,
            message: format!("{label}: {detail}"),
            is_daemon_turn_error: false,
        }
        .into()
    }

    fn reserve_prompt_slot(&self, session_id: &str) -> Result<PromptSlot, DaemonClientError> {
        let Some(limit) = self.prompt_limit else {
            return Ok(PromptSlot::unlimited());
        };
        let mut counts = lock_recover(&self.prompt_counts);
        let pending_count = counts.get(session_id).copied().unwrap_or_default();
        if pending_count >= limit {
            return Err(DaemonPendingPromptLimitError {
                session_id: session_id.to_owned(),
                limit: limit as f64,
                pending_count: pending_count as f64,
            }
            .into());
        }
        counts.insert(session_id.to_owned(), pending_count + 1);
        Ok(PromptSlot {
            counts: Some(Arc::clone(&self.prompt_counts)),
            session_id: session_id.to_owned(),
        })
    }
}

/// A status/body pair from a route. The body is `None` for empty responses.
#[derive(Clone, Debug, PartialEq)]
pub struct DaemonRawResponse {
    pub status: u16,
    pub body: Option<Value>,
}

/// Bounded raw REST response for file/audio/archive routes.
#[derive(Clone, Debug, PartialEq)]
pub struct DaemonBytesResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_disposition: Option<String>,
    pub body: Vec<u8>,
}

macro_rules! daemon_get_method {
    ($name:ident, $path:literal, $label:literal, $mode:expr) => {
        pub async fn $name(&self) -> Result<Value, DaemonClientError> {
            self.request_json(
                "GET",
                $path,
                "",
                None,
                $label,
                request_options(None, $mode, RequestTimeout::ClientDefault),
            )
            .await
        }
    };
}

macro_rules! daemon_post_empty_method {
    ($name:ident, $path:literal, $label:literal, $mode:expr) => {
        pub async fn $name(&self) -> Result<Value, DaemonClientError> {
            self.request_json(
                "POST",
                $path,
                "",
                Some(json!({})),
                $label,
                request_options(None, $mode, RequestTimeout::ClientDefault),
            )
            .await
        }
    };
}

macro_rules! session_get_method {
    ($name:ident, $suffix:literal, $label:literal, $mode:expr) => {
        pub async fn $name(
            &self,
            session_id: &str,
            client_id: Option<String>,
        ) -> Result<Value, DaemonClientError> {
            let path = format!("/session/{}{}", encode_uri_component(session_id), $suffix);
            self.request_json(
                "GET",
                &path,
                "",
                None,
                $label,
                request_options(client_id, $mode, RequestTimeout::ClientDefault),
            )
            .await
        }
    };
}

impl DaemonClient {
    pub async fn notify(
        &self,
        request: Value,
        timeout_duration: Option<Duration>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/notify",
            "",
            Some(request),
            "POST /workspace/notify",
            request_options(
                None,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::After(Duration::from_secs(35))),
            ),
        )
        .await
    }

    pub async fn daemon_status(&self, detail: Option<&str>) -> Result<Value, DaemonClientError> {
        let query = if detail.is_some_and(|detail| detail != "summary") {
            encode_query_pairs(&[("detail", detail.map(str::to_owned))])
        } else {
            String::new()
        };
        self.request_json(
            "GET",
            "/daemon/status",
            &query,
            None,
            "GET /daemon/status",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn usage_dashboard(
        &self,
        range: Option<&str>,
        heatmap_days: Option<u32>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("range", range.map(str::to_owned)),
            ("heatmapDays", heatmap_days.map(|days| days.to_string())),
        ]);
        self.request_json(
            "GET",
            "/usage/dashboard",
            &query,
            None,
            "GET /usage/dashboard",
            DaemonRequestOptions::default(),
        )
        .await
    }

    daemon_get_method!(
        workspace_mcp,
        "/workspace/mcp",
        "GET /workspace/mcp",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_skills,
        "/workspace/skills",
        "GET /workspace/skills",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_providers,
        "/workspace/providers",
        "GET /workspace/providers",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_hooks,
        "/workspace/hooks",
        "GET /workspace/hooks",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_extensions,
        "/workspace/extensions",
        "GET /workspace/extensions",
        DaemonRequestMode::Rest
    );
    daemon_get_method!(
        workspace_env,
        "/workspace/env",
        "GET /workspace/env",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_preflight,
        "/workspace/preflight",
        "GET /workspace/preflight",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_tools,
        "/workspace/tools",
        "GET /workspace/tools",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        workspace_memory,
        "/workspace/memory",
        "GET /workspace/memory",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        list_workspace_agents,
        "/workspace/agents",
        "GET /workspace/agents",
        DaemonRequestMode::Transport
    );
    daemon_get_method!(
        extension_catalog,
        "/extensions",
        "GET /extensions",
        DaemonRequestMode::Rest
    );
    daemon_get_method!(
        active_extension_operations,
        "/workspace/extensions/operations",
        "GET /workspace/extensions/operations",
        DaemonRequestMode::Rest
    );

    daemon_post_empty_method!(
        initialize_workspace_mcp,
        "/workspace/mcp/initialize",
        "POST /workspace/mcp/initialize",
        DaemonRequestMode::Transport
    );

    pub async fn reload_workspace_mcp(&self, options: Value) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/mcp/reload",
            "",
            Some(options),
            "POST /workspace/mcp/reload",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_git(&self, wait: bool) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/git",
            if wait { "wait=1" } else { "" },
            None,
            "GET /workspace/git",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_diff(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/git/diff",
            "",
            None,
            "GET /workspace/git/diff",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_diff_file(
        &self,
        path: &str,
        old_path: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("path", Some(path.to_owned())),
            ("oldPath", old_path.map(str::to_owned)),
        ]);
        self.request_json(
            "GET",
            "/workspace/git/diff/file",
            &query,
            None,
            "GET /workspace/git/diff/file",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_log(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        range: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("limit", limit.map(|value| value.to_string())),
            ("skip", skip.map(|value| value.to_string())),
            ("range", range.map(str::to_owned)),
        ]);
        self.request_json(
            "GET",
            "/workspace/git/log",
            &query,
            None,
            "GET /workspace/git/log",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_commit_detail(&self, sha: &str) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("sha", Some(sha.to_owned()))]);
        self.request_json(
            "GET",
            "/workspace/git/log/commit",
            &query,
            None,
            "GET /workspace/git/log/commit",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_branches(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/git/branches",
            "",
            None,
            "GET /workspace/git/branches",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_checkout(
        &self,
        reference: &str,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/git/checkout",
            "",
            Some(json!({"ref": reference})),
            "POST /workspace/git/checkout",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_create_branch(
        &self,
        name: &str,
        start_point: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let body = json_object([
            ("name", Some(json!(name))),
            ("startPoint", start_point.map(|value| json!(value))),
        ]);
        self.request_json(
            "POST",
            "/workspace/git/branch",
            "",
            Some(body),
            "POST /workspace/git/branch",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_push(
        &self,
        options: Option<Value>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/git/push",
            "",
            Some(
                options
                    .filter(|options| !options.is_null())
                    .unwrap_or_else(|| json!({})),
            ),
            "POST /workspace/git/push",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_pull(
        &self,
        options: Option<Value>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/git/pull",
            "",
            Some(
                options
                    .filter(|options| !options.is_null())
                    .unwrap_or_else(|| json!({})),
            ),
            "POST /workspace/git/pull",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_git_commit(
        &self,
        message: &str,
        all: Option<bool>,
    ) -> Result<Value, DaemonClientError> {
        let body = json_object([
            ("message", Some(json!(message))),
            ("all", all.map(|value| json!(value))),
        ]);
        self.request_json(
            "POST",
            "/workspace/git/commit",
            "",
            Some(body),
            "POST /workspace/git/commit",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn workspace_mcp_tools(&self, server_name: &str) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/mcp/{}/tools", encode_uri_component(server_name));
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/mcp/:server/tools",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_mcp_resources(
        &self,
        server_name: &str,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/mcp/{}/resources",
            encode_uri_component(server_name)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/mcp/:server/resources",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_acp_preheat(
        &self,
        timeout_ms: Option<u64>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("timeoutMs", timeout_ms.map(|value| value.to_string()))]);
        let server_budget = Duration::from_millis(timeout_ms.unwrap_or(5_000));
        self.request_json(
            "POST",
            "/workspace/acp/preheat",
            &query,
            None,
            "POST /workspace/acp/preheat",
            request_options(
                None,
                DaemonRequestMode::Rest,
                RequestTimeout::After(server_budget.saturating_add(Duration::from_secs(2))),
            ),
        )
        .await
    }

    pub async fn workspace_acp_status(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/acp/status",
            "",
            None,
            "GET /workspace/acp/status",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn session_hooks(&self, session_id: &str) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/hooks", encode_uri_component(session_id));
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /session/:id/hooks",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn extension_operation_status(
        &self,
        operation_id: &str,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/extensions/operations/{}",
            encode_uri_component(operation_id)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/extensions/operations/:operationId",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn respond_to_extension_interaction(
        &self,
        operation_id: &str,
        interaction_id: &str,
        response: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/extensions/operations/{}/interactions/{}",
            encode_uri_component(operation_id),
            encode_uri_component(interaction_id)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(response),
            "POST /workspace/extensions/operations/:operationId/interactions/:interactionId",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn install_extension(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/extensions/install",
            "",
            Some(request),
            "POST /workspace/extensions/install",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn install_extension_archive(
        &self,
        filename: &str,
        consent: bool,
        archive: Vec<u8>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("filename", Some(filename.to_owned())),
            ("consent", Some(consent.to_string())),
        ]);
        let response = self
            .request_bytes(
                "POST",
                "/workspace/extensions/install-archive",
                &query,
                Some(archive),
                "POST /workspace/extensions/install-archive",
                DaemonRequestOptions {
                    client_id,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some("application/octet-stream".into()),
                    timeout: RequestTimeout::After(EXTENSION_ARCHIVE_UPLOAD_TIMEOUT),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "extension archive install response was not JSON: {error}"
            ))
        })
    }

    /// Install an extension from archive bytes while reporting upload progress.
    /// The TypeScript SDK currently has no archive progress callback or caller
    /// cancellation option; this Rust extension retains its 120-second upload
    /// timeout and adds the optional cancellation token and progress callback.
    pub async fn install_extension_archive_with_progress<F>(
        &self,
        filename: &str,
        consent: bool,
        archive: Vec<u8>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
        on_progress: F,
    ) -> Result<Value, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        let query = encode_query_pairs(&[
            ("filename", Some(filename.to_owned())),
            ("consent", Some(consent.to_string())),
        ]);
        let response = self
            .request_bytes_with_progress(
                "POST",
                "/workspace/extensions/install-archive",
                &query,
                archive,
                "POST /workspace/extensions/install-archive",
                DaemonRequestOptions {
                    client_id,
                    cancellation,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some("application/octet-stream".into()),
                    timeout: RequestTimeout::After(EXTENSION_ARCHIVE_UPLOAD_TIMEOUT),
                    ..DaemonRequestOptions::default()
                },
                on_progress,
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "extension archive install response was not JSON: {error}"
            ))
        })
    }

    pub async fn enable_extension(
        &self,
        name: &str,
        scope: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/extensions/{}/enable",
            encode_uri_component(name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(scope),
            "POST /workspace/extensions/:name/enable",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn disable_extension(
        &self,
        name: &str,
        scope: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/extensions/{}/disable",
            encode_uri_component(name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(scope),
            "POST /workspace/extensions/:name/disable",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn update_extension(
        &self,
        name: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/extensions/{}/update",
            encode_uri_component(name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /workspace/extensions/:name/update",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn uninstall_extension(
        &self,
        name: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let path = format!("/workspace/extensions/{}", encode_uri_component(name));
        self.request_no_content(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /workspace/extensions/:name",
            None,
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn install_user_extension(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/extensions/install",
            "",
            Some(request),
            "POST /extensions/install",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn update_user_extension(
        &self,
        extension_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/extensions/{}/update", encode_uri_component(extension_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /extensions/:extensionId/update",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn uninstall_user_extension(
        &self,
        extension_id: &str,
        client_id: Option<String>,
    ) -> Result<Option<Value>, DaemonClientError> {
        let path = format!("/extensions/{}", encode_uri_component(extension_id));
        let response = self
            .request_raw(
                "DELETE",
                &path,
                "",
                None,
                request_options(
                    client_id,
                    DaemonRequestMode::Rest,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        if response.status == StatusCode::NO_CONTENT.as_u16() {
            return Ok(None);
        }
        if !(200..300).contains(&response.status) {
            return Err(self.http_error(
                response.status,
                response.body,
                "DELETE /extensions/:extensionId",
                None,
            ));
        }
        Ok(response.body)
    }

    pub async fn set_extension_default_activation(
        &self,
        extension_id: &str,
        state: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/extensions/{}/activation",
            encode_uri_component(extension_id)
        );
        self.request_json(
            "PUT",
            &path,
            "",
            Some(json!({"state": state})),
            "PUT /extensions/:extensionId/activation",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn extension_operation(
        &self,
        operation_id: &str,
        cancellation: Option<RestSseCancellation>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/extensions/operations/{}",
            encode_uri_component(operation_id)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /extensions/operations/:operationId",
            DaemonRequestOptions {
                cancellation,
                mode: DaemonRequestMode::Rest,
                ..DaemonRequestOptions::default()
            },
        )
        .await
    }

    /// Poll an extension operation until it reaches a state other than
    /// `queued` or `running`. A timeout or caller cancellation only aborts the
    /// local HTTP poll; it never sends a cancellation request to the daemon.
    pub async fn wait_for_extension_operation(
        &self,
        handle: &Value,
        options: ExtensionOperationWaitOptions,
    ) -> Result<Value, DaemonClientError> {
        let operation_id = handle
            .get("operationId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DaemonClientError::InvalidResponse(
                    "extension operation handle has no operationId".into(),
                )
            })?
            .to_owned();
        let poll_interval = options
            .poll_interval
            .unwrap_or_else(|| Duration::from_secs(1));
        let timeout_duration = match options.timeout {
            ExtensionOperationWaitTimeout::Default => Some(Duration::from_secs(10 * 60)),
            ExtensionOperationWaitTimeout::After(duration) => Some(duration),
            ExtensionOperationWaitTimeout::Infinite => None,
        };
        let deadline = timeout_duration.and_then(|duration| Instant::now().checked_add(duration));
        let mut caller_cancellation = options
            .cancellation
            .as_ref()
            .map(RestSseCancellation::subscribe_cancelled);

        loop {
            if caller_cancellation
                .as_ref()
                .is_some_and(|receiver| *receiver.borrow())
            {
                return Err(DaemonClientError::Cancelled);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(DaemonClientError::ExtensionOperationTimeout { operation_id });
            }

            // Each poll gets its own cancellation handle. Timing out this
            // request drops the local fetch without cancelling server work.
            let poll_cancellation = RestSseCancellation::new();
            let poll = self.extension_operation(&operation_id, Some(poll_cancellation.clone()));
            let operation = if let Some(deadline) = deadline {
                tokio::select! {
                    biased;
                    _ = wait_for_optional_cancel(&mut caller_cancellation) => {
                        poll_cancellation.cancel();
                        return Err(DaemonClientError::Cancelled);
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        poll_cancellation.cancel();
                        return Err(DaemonClientError::ExtensionOperationTimeout { operation_id });
                    }
                    result = poll => result?,
                }
            } else {
                tokio::select! {
                    biased;
                    _ = wait_for_optional_cancel(&mut caller_cancellation) => {
                        poll_cancellation.cancel();
                        return Err(DaemonClientError::Cancelled);
                    }
                    result = poll => result?,
                }
            };

            if !matches!(
                operation.get("status").and_then(Value::as_str),
                Some("queued" | "running")
            ) {
                return Ok(operation);
            }

            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(DaemonClientError::ExtensionOperationTimeout { operation_id });
            }
            let delay = deadline
                .map(|deadline| {
                    poll_interval.min(deadline.saturating_duration_since(Instant::now()))
                })
                .unwrap_or(poll_interval);
            if delay.is_zero() {
                tokio::select! {
                    biased;
                    _ = wait_for_optional_cancel(&mut caller_cancellation) => {
                        return Err(DaemonClientError::Cancelled);
                    }
                    _ = tokio::task::yield_now() => {}
                }
            } else {
                tokio::select! {
                    biased;
                    _ = wait_for_optional_cancel(&mut caller_cancellation) => {
                        return Err(DaemonClientError::Cancelled);
                    }
                    _ = sleep(delay) => {}
                }
            }
        }
    }

    /// Stream validated version-1 generation events for workspace content.
    /// This endpoint requires a terminal `done` or `error` event before EOF.
    pub async fn generate_workspace_content(
        &self,
        prompt: &str,
        options: DaemonGenerationOptions,
    ) -> Result<DaemonGenerationEventStream, DaemonClientError> {
        self.generate_content_events(
            "/workspace/generate",
            "POST /workspace/generate",
            prompt,
            options,
            true,
        )
        .await
    }

    /// Stream validated version-1 generation events for a session. Session
    /// generation permits EOF without a terminal event, matching TypeScript.
    pub async fn generate_session_content(
        &self,
        session_id: &str,
        prompt: &str,
        options: DaemonGenerationOptions,
    ) -> Result<DaemonGenerationEventStream, DaemonClientError> {
        let path = format!("/session/{}/generate", encode_uri_component(session_id));
        self.generate_content_events(&path, "POST /session/:id/generate", prompt, options, false)
            .await
    }

    async fn generate_content_events(
        &self,
        path: &str,
        label: &str,
        prompt: &str,
        options: DaemonGenerationOptions,
        require_terminal: bool,
    ) -> Result<DaemonGenerationEventStream, DaemonClientError> {
        let url = self.route_url(path, "")?;
        let request_options = DaemonRequestOptions {
            client_id: options.client_id,
            ..DaemonRequestOptions::default()
        };
        let mut headers = HeaderMap::new();
        add_request_headers(&mut headers, &self.token, &request_options)?;
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        let body = serde_json::to_vec(&json!({"prompt": prompt}))
            .map_err(|error| DaemonClientError::InvalidResponse(error.to_string()))?;
        let request = ReconnectFetchRequest {
            url,
            method: Method::POST,
            headers,
            body: Some(body),
            timeout: None,
        };
        let cancellation_handle = options.cancellation;
        let mut cancellation = cancellation_handle
            .as_ref()
            .map(RestSseCancellation::subscribe_cancelled);
        let response = if cancellation.is_some() {
            tokio::select! {
                biased;
                _ = wait_for_optional_cancel(&mut cancellation) => {
                    return Err(DaemonClientError::Cancelled);
                }
                response = self.transport.fetch(request) => response?,
            }
        } else {
            self.transport.fetch(request).await?
        };

        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let response = response_to_json(response, MAX_ROUTE_BODY_BYTES).await?;
            return Err(self.http_error(status, response.body, label, None));
        }
        if status == StatusCode::NO_CONTENT.as_u16() {
            return Err(DaemonClientError::InvalidResponse(
                "Generation response body is missing".into(),
            ));
        }

        let source = crate::daemon_sse::parse_sse_json_stream(response.bytes_stream(), None);
        let state = GenerationStreamState {
            source: Box::pin(source),
            cancellation,
            _cancellation_handle: cancellation_handle,
            require_terminal,
            saw_terminal: false,
            finished: false,
        };
        let events = stream::unfold(state, |mut state| async move {
            if state.finished {
                return None;
            }
            loop {
                let next = if let Some(receiver) = state.cancellation.as_mut() {
                    tokio::select! {
                        biased;
                        _ = wait_for_cancel(receiver) => None,
                        item = state.source.next() => Some(item),
                    }
                } else {
                    Some(state.source.next().await)
                };
                match next {
                    None => {
                        state.finished = true;
                        state.source = Box::pin(stream::empty());
                        return Some((Err(DaemonClientError::Cancelled), state));
                    }
                    Some(None) => {
                        state.finished = true;
                        return if state.require_terminal && !state.saw_terminal {
                            Some((Err(DaemonClientError::GenerationStreamEnded), state))
                        } else {
                            None
                        };
                    }
                    Some(Some(Err(error))) => {
                        state.finished = true;
                        return Some((
                            Err(DaemonClientError::InvalidResponse(error.to_string())),
                            state,
                        ));
                    }
                    Some(Some(Ok(value))) => {
                        let Some(event) = parse_generation_event(value) else {
                            continue;
                        };
                        state.saw_terminal = matches!(
                            event,
                            DaemonGenerationEvent::Done { .. }
                                | DaemonGenerationEvent::Error { .. }
                        );
                        if state.require_terminal && state.saw_terminal {
                            state.finished = true;
                            state.source = Box::pin(stream::empty());
                        }
                        return Some((Ok(event), state));
                    }
                }
            }
        });
        Ok(Box::pin(events))
    }

    pub async fn read_workspace_file(
        &self,
        file_path: &str,
        options: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("path", Some(file_path.to_owned())),
            ("maxBytes", options.get("maxBytes").map(js_string)),
            ("line", options.get("line").map(js_string)),
            ("limit", options.get("limit").map(js_string)),
            (
                "cursor",
                options
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
        ]);
        self.request_json(
            "GET",
            "/file",
            &query,
            None,
            "GET /file",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn read_workspace_file_bytes(
        &self,
        file_path: &str,
        offset: Option<u64>,
        max_bytes: Option<u64>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[
            ("path", Some(file_path.to_owned())),
            ("offset", offset.map(|value| value.to_string())),
            ("maxBytes", max_bytes.map(|value| value.to_string())),
        ]);
        self.request_json(
            "GET",
            "/file/bytes",
            &query,
            None,
            "GET /file/bytes",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn file_stat(&self, file_path: &str) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("path", Some(file_path.to_owned()))]);
        self.request_json(
            "GET",
            "/stat",
            &query,
            None,
            "GET /stat",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn dir_list(&self, directory_path: &str) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("path", Some(directory_path.to_owned()))]);
        self.request_json(
            "GET",
            "/list",
            &query,
            None,
            "GET /list",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_path_suggestions(
        &self,
        prefix: &str,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("prefix", Some(prefix.to_owned()))]);
        self.request_json(
            "GET",
            "/workspace-path-suggestions",
            &query,
            None,
            "GET /workspace-path-suggestions",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn workspace_directory_picker(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace-directory-picker",
            "",
            Some(json!({})),
            "POST /workspace-directory-picker",
            request_options(
                None,
                DaemonRequestMode::Rest,
                RequestTimeout::After(Duration::from_secs(310)),
            ),
        )
        .await
    }

    pub async fn glob(&self, pattern: &str) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("pattern", Some(pattern.to_owned()))]);
        self.request_json(
            "GET",
            "/glob",
            &query,
            None,
            "GET /glob",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn write_workspace_file(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/file/write",
            "",
            Some(request),
            "POST /file/write",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn edit_workspace_file(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/file/edit",
            "",
            Some(request),
            "POST /file/edit",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn upload_workspace_file(
        &self,
        file_path: &str,
        data: Vec<u8>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.upload_file_to_path("/file/upload", file_path, data, timeout_duration, client_id)
            .await
    }

    /// Upload a workspace file and report request-body progress as chunks are
    /// consumed by the HTTP transport. `loaded` counts bytes yielded, not
    /// bytes confirmed by the daemon. The callback runs on the async task that
    /// drives the request and should return promptly.
    pub async fn upload_workspace_file_with_progress<F>(
        &self,
        file_path: &str,
        data: Vec<u8>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
        on_progress: F,
    ) -> Result<Value, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        self.upload_file_to_path_with_progress(
            "/file/upload",
            file_path,
            data,
            "POST /file/upload",
            timeout_duration,
            client_id,
            cancellation,
            on_progress,
        )
        .await
    }

    pub(crate) async fn upload_file_to_path_with_progress<F>(
        &self,
        upload_path: &str,
        file_path: &str,
        data: Vec<u8>,
        label: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
        on_progress: F,
    ) -> Result<Value, DaemonClientError>
    where
        F: FnMut(UploadProgress) + Send + 'static,
    {
        let query = encode_query_pairs(&[("path", Some(file_path.to_owned()))]);
        let response = self
            .request_bytes_with_progress(
                "POST",
                upload_path,
                &query,
                data,
                label,
                DaemonRequestOptions {
                    client_id,
                    cancellation,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some("application/octet-stream".into()),
                    timeout: timeout_duration
                        .map(RequestTimeout::After)
                        .unwrap_or(RequestTimeout::ClientDefault),
                    ..DaemonRequestOptions::default()
                },
                on_progress,
            )
            .await?;
        let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "{label}: invalid upload response JSON: {error}"
            ))
        })?;
        if value.get("path").is_none() {
            return Err(DaemonClientError::InvalidResponse(format!(
                "{label}: invalid upload response body"
            )));
        }
        Ok(value)
    }

    pub async fn upload_file_to_path(
        &self,
        upload_path: &str,
        file_path: &str,
        data: Vec<u8>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[("path", Some(file_path.to_owned()))]);
        let response = self
            .request_bytes(
                "POST",
                upload_path,
                &query,
                Some(data),
                "POST /file/upload",
                DaemonRequestOptions {
                    client_id,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some("application/octet-stream".into()),
                    timeout: timeout_duration
                        .map(RequestTimeout::After)
                        .unwrap_or(RequestTimeout::ClientDefault),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /file/upload: invalid upload response JSON: {error}"
            ))
        })?;
        if value.get("path").is_none() {
            return Err(DaemonClientError::InvalidResponse(
                "POST /file/upload: invalid upload response body".into(),
            ));
        }
        Ok(value)
    }

    pub async fn write_workspace_memory(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/memory",
            "",
            Some(request),
            "POST /workspace/memory",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn remember_workspace_memory(
        &self,
        content: &str,
        context_mode: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/memory/remember",
            "",
            Some(json!({"content": content, "contextMode": context_mode.unwrap_or("workspace")})),
            "POST /workspace/memory/remember",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_workspace_memory_remember_task(
        &self,
        task_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_memory_task("remember", task_id, client_id)
            .await
    }

    pub async fn forget_workspace_memory(
        &self,
        query: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/memory/forget",
            "",
            Some(json!({"query": query})),
            "POST /workspace/memory/forget",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_workspace_memory_forget_task(
        &self,
        task_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_memory_task("forget", task_id, client_id)
            .await
    }

    pub async fn dream_workspace_memory(
        &self,
        _options: Option<Value>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/memory/dream",
            "",
            Some(json!({})),
            "POST /workspace/memory/dream",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_workspace_memory_dream_task(
        &self,
        task_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_memory_task("dream", task_id, client_id)
            .await
    }

    async fn workspace_memory_task(
        &self,
        task: &str,
        task_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/memory/{}/{}",
            task,
            encode_uri_component(task_id)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/memory/:task/:taskId",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn create_workspace_agent(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/agents",
            "",
            Some(request),
            "POST /workspace/agents",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn generate_workspace_agent(
        &self,
        description: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/agents/generate",
            "",
            Some(json!({"description": description})),
            "POST /workspace/agents/generate",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(Duration::from_secs(330)),
            ),
        )
        .await
    }

    pub async fn get_workspace_agent(
        &self,
        agent_type: &str,
        scope: Option<&str>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/agents/{}", encode_uri_component(agent_type));
        let query = encode_query_pairs(&[("scope", scope.map(str::to_owned))]);
        self.request_json(
            "GET",
            &path,
            &query,
            None,
            "GET /workspace/agents/:agentType",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn update_workspace_agent(
        &self,
        agent_type: &str,
        request: Value,
        scope: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/agents/{}", encode_uri_component(agent_type));
        let query = encode_query_pairs(&[("scope", scope.map(str::to_owned))]);
        self.request_json(
            "POST",
            &path,
            &query,
            Some(request),
            "POST /workspace/agents/:agentType",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn delete_workspace_agent(
        &self,
        agent_type: &str,
        scope: Option<&str>,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let path = format!("/workspace/agents/{}", encode_uri_component(agent_type));
        let query = encode_query_pairs(&[("scope", scope.map(str::to_owned))]);
        self.request_no_content(
            "DELETE",
            &path,
            &query,
            None,
            "DELETE /workspace/agents/:agentType",
            Some("agent_not_found"),
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }
}

impl DaemonClient {
    pub async fn list_workspace_sessions_page(
        &self,
        workspace_cwd: &str,
        options: Value,
    ) -> Result<Value, DaemonClientError> {
        if options.get("sourceType").is_some() || options.get("sourceId").is_some() {
            self.require_capability("session_source_metadata").await?;
        }
        let requested = options
            .get("pageSize")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .unwrap_or(20.0);
        let page_size = requested.round().clamp(1.0, 1000.0) as u32;
        if options.get("sourceType").is_some() || options.get("sourceId").is_some() {
            self.require_capability("session_source_metadata").await?;
        }
        let query = encode_query_pairs(&[
            ("size", Some(page_size.to_string())),
            (
                "cursor",
                options
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "archiveState",
                options
                    .get("archiveState")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "view",
                options
                    .get("view")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "group",
                options
                    .get("group")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "parentSessionId",
                options
                    .get("parentSessionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "sourceType",
                options
                    .get("sourceType")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "sourceId",
                options
                    .get("sourceId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
        ]);
        let path = format!(
            "/workspace/{}/sessions",
            encode_uri_component(workspace_cwd)
        );
        self.request_json(
            "GET",
            &path,
            &query,
            None,
            "GET /workspace/sessions",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn list_workspace_sessions(
        &self,
        workspace_cwd: &str,
        options: Value,
    ) -> Result<Value, DaemonClientError> {
        let page = self
            .list_workspace_sessions_page(workspace_cwd, options)
            .await?;
        Ok(page.get("sessions").cloned().unwrap_or(Value::Null))
    }

    pub async fn get_workspace_session_live_state(
        &self,
        workspace_cwd: &str,
        client_id: Option<String>,
        timeout_duration: Option<Duration>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspaces/{}/sessions/live-state",
            encode_uri_component(workspace_cwd)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspaces/:workspace/sessions/live-state",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn list_session_groups(
        &self,
        workspace_cwd: &str,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/{}/session-groups",
            encode_uri_component(workspace_cwd)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/session-groups",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn create_session_group(
        &self,
        workspace_cwd: &str,
        input: Value,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/{}/session-groups",
            encode_uri_component(workspace_cwd)
        );
        let response = self
            .request_json(
                "POST",
                &path,
                "",
                Some(input),
                "POST /workspace/session-groups",
                DaemonRequestOptions::default(),
            )
            .await?;
        Ok(response.get("group").cloned().unwrap_or(Value::Null))
    }

    pub async fn update_session_group(
        &self,
        workspace_cwd: &str,
        group_id: &str,
        update: Value,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/{}/session-groups/{}",
            encode_uri_component(workspace_cwd),
            encode_uri_component(group_id)
        );
        let response = self
            .request_json(
                "PATCH",
                &path,
                "",
                Some(update),
                "PATCH /workspace/session-groups/:groupId",
                DaemonRequestOptions::default(),
            )
            .await?;
        Ok(response.get("group").cloned().unwrap_or(Value::Null))
    }

    pub async fn delete_session_group(
        &self,
        workspace_cwd: &str,
        group_id: &str,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/{}/session-groups/{}",
            encode_uri_component(workspace_cwd),
            encode_uri_component(group_id)
        );
        self.request_json(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /workspace/session-groups/:groupId",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn update_session_organization(
        &self,
        session_id: &str,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/organization", encode_uri_component(session_id));
        self.request_json(
            "PATCH",
            &path,
            "",
            Some(update),
            "PATCH /session/:id/organization",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn load_session(
        &self,
        session_id: &str,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.restore_session("load", session_id, request, client_id)
            .await
    }

    pub async fn resume_session(
        &self,
        session_id: &str,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.restore_session("resume", session_id, request, client_id)
            .await
    }

    async fn restore_session(
        &self,
        action: &str,
        session_id: &str,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let timeout = match request.get("timeoutMs") {
            Some(value) => {
                let ms = value
                    .as_f64()
                    .filter(|ms| ms.is_finite() && ms.fract() == 0.0 && *ms >= 0.0)
                    .ok_or_else(|| {
                        DaemonClientError::InvalidResponse(
                            "RestoreSessionRequest.timeoutMs must be a non-negative integer".into(),
                        )
                    })?;
                if ms == 0.0 || ms > MAX_TIMER_DELAY_MS as f64 {
                    RequestTimeout::Disabled
                } else {
                    RequestTimeout::After(Duration::from_millis(ms as u64))
                }
            }
            None => RequestTimeout::After(
                self.cached_session_restore_timeout()
                    .unwrap_or(Duration::from_secs(60))
                    .saturating_add(Duration::from_secs(10)),
            ),
        };
        let body = json_object([
            ("cwd", request.get("workspaceCwd").cloned()),
            ("approvalMode", request.get("approvalMode").cloned()),
            (
                "historyPageSize",
                (action == "load")
                    .then(|| request.get("historyPageSize").cloned())
                    .flatten(),
            ),
            (
                "liveReplayMode",
                (action == "load")
                    .then(|| request.get("liveReplayMode").cloned())
                    .flatten(),
            ),
        ]);
        let path = format!("/session/{}/{}", encode_uri_component(session_id), action);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            &format!("POST /session/:id/{action}"),
            request_options(client_id, DaemonRequestMode::Transport, timeout),
        )
        .await
    }

    pub async fn export_session(
        &self,
        session_id: &str,
        format: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/export", encode_uri_component(session_id));
        let query = encode_query_pairs(&[("format", format.map(str::to_owned))]);
        let response = self
            .request_bytes(
                "GET",
                &path,
                &query,
                None,
                "GET /session/:id/export",
                request_options(
                    client_id,
                    DaemonRequestMode::Rest,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        let effective_format = format.unwrap_or("html");
        let filename = response
            .content_disposition
            .as_deref()
            .and_then(content_disposition_filename)
            .unwrap_or_else(|| format!("export.{effective_format}"));
        Ok(json!({
            "content": String::from_utf8_lossy(&response.body),
            "filename": filename,
            "mimeType": response.content_type.unwrap_or_default(),
            "format": effective_format,
        }))
    }

    pub async fn get_session_transcript_page(
        &self,
        session_id: &str,
        options: Value,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/transcript", encode_uri_component(session_id));
        let query = encode_query_pairs(&[
            (
                "cursor",
                options
                    .get("cursor")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            (
                "beforeRecordId",
                options
                    .get("beforeRecordId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            ),
            ("limit", options.get("limit").map(js_string)),
        ]);
        self.request_json(
            "GET",
            &path,
            &query,
            None,
            "GET /session/:id/transcript",
            request_options(
                options
                    .get("clientId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn resolve_subagent_session(
        &self,
        session_id: &str,
        subagent_ref: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/subagents/{}",
            encode_uri_component(session_id),
            encode_uri_component(subagent_ref)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /session/:id/subagents/:subagentRef",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn cancel_subagent_session(
        &self,
        session_id: &str,
        subagent_ref: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/subagents/{}/cancel",
            encode_uri_component(session_id),
            encode_uri_component(subagent_ref)
        );
        self.request_json(
            "POST",
            &path,
            "",
            None,
            "POST /session/:id/subagents/:subagentRef/cancel",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    session_get_method!(
        session_context,
        "/context",
        "GET /session/:id/context",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        session_status,
        "/status",
        "GET /session/:id/status",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        session_supported_commands,
        "/supported-commands",
        "GET /session/:id/supported-commands",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        session_tasks,
        "/tasks",
        "GET /session/:id/tasks",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        session_lsp_status,
        "/lsp",
        "GET /session/:id/lsp",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        session_stats,
        "/stats",
        "GET /session/:id/stats",
        DaemonRequestMode::Transport
    );
    session_get_method!(
        get_rewind_snapshots,
        "/rewind/snapshots",
        "GET /session/:id/rewind/snapshots",
        DaemonRequestMode::Rest
    );

    pub async fn session_context_usage(
        &self,
        session_id: &str,
        detail: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/context-usage",
            encode_uri_component(session_id)
        );
        let query = if detail { "detail=true" } else { "" };
        self.request_json(
            "GET",
            &path,
            query,
            None,
            "GET /session/:id/context-usage",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn session_task_cancel(
        &self,
        session_id: &str,
        task_id: &str,
        kind: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/tasks/{}/cancel",
            encode_uri_component(session_id),
            encode_uri_component(task_id)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"kind": kind})),
            "POST /session/:id/tasks/:taskId/cancel",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn session_goal_clear(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/goal/clear", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /session/:id/goal/clear",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_session_approval_mode(
        &self,
        session_id: &str,
        mode: &str,
        persist: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/approval-mode",
            encode_uri_component(session_id)
        );
        let body = json_object([
            ("mode", Some(json!(mode))),
            ("persist", persist.then_some(json!(true))),
        ]);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            "POST /session/:id/approval-mode",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn rewind_session(
        &self,
        session_id: &str,
        prompt_id: &str,
        rewind_files: Option<bool>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/rewind", encode_uri_component(session_id));
        let body = json_object([
            ("promptId", Some(json!(prompt_id))),
            ("rewindFiles", rewind_files.map(|value| json!(value))),
        ]);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            "POST /session/:id/rewind",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn recap_session(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/recap", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /session/:id/recap",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::Disabled,
            ),
        )
        .await
    }

    pub async fn btw_session(
        &self,
        session_id: &str,
        question: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/btw", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"question": question})),
            "POST /session/:id/btw",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::Disabled,
            ),
        )
        .await
    }

    pub async fn upload_session_media(
        &self,
        session_id: &str,
        data: Vec<u8>,
        mime_type: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/media", encode_uri_component(session_id));
        let response = self
            .request_bytes(
                "POST",
                &path,
                "",
                Some(data),
                "POST /session/:id/media",
                DaemonRequestOptions {
                    client_id,
                    content_type: Some(mime_type.to_owned()),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /session/:id/media: invalid JSON response: {error}"
            ))
        })
    }

    pub async fn read_session_media(
        &self,
        session_id: &str,
        media_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/media/{}",
            encode_uri_component(session_id),
            encode_uri_component(media_id)
        );
        let response = self
            .request_bytes(
                "GET",
                &path,
                "",
                None,
                "GET /session/:id/media/:mediaId",
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        Ok(
            json!({"data": base64_encode(&response.body), "mimeType": response.content_type.unwrap_or_else(|| "application/octet-stream".into())}),
        )
    }

    pub async fn remove_session_media(
        &self,
        session_id: &str,
        media_id: &str,
        client_id: Option<String>,
    ) -> Result<bool, DaemonClientError> {
        let path = format!(
            "/session/{}/media/{}",
            encode_uri_component(session_id),
            encode_uri_component(media_id)
        );
        let response = self
            .request_json(
                "DELETE",
                &path,
                "",
                None,
                "DELETE /session/:id/media/:mediaId",
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        Ok(response.get("removed").and_then(Value::as_bool) == Some(true))
    }

    pub async fn enqueue_mid_turn_message(
        &self,
        session_id: &str,
        message: &str,
        message_id: Option<&str>,
        content: Option<Value>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/mid-turn-message",
            encode_uri_component(session_id)
        );
        let content =
            content.filter(|value| value.as_array().is_none_or(|values| !values.is_empty()));
        let body = json_object([
            ("message", Some(json!(message))),
            ("messageId", message_id.map(|value| json!(value))),
            ("content", content),
        ]);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            "POST /session/:id/mid-turn-message",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn remove_mid_turn_message(
        &self,
        session_id: &str,
        message_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/mid-turn-messages/{}",
            encode_uri_component(session_id),
            encode_uri_component(message_id)
        );
        self.request_json(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /session/:id/mid-turn-messages/:messageId",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_mid_turn_messages(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/mid-turn-messages",
            encode_uri_component(session_id)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /session/:id/mid-turn-messages",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_pending_prompts(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/pending-prompts",
            encode_uri_component(session_id)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /session/:id/pending-prompts",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn remove_pending_prompt(
        &self,
        session_id: &str,
        prompt_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/pending-prompts/{}",
            encode_uri_component(session_id),
            encode_uri_component(prompt_id)
        );
        self.request_json(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /session/:id/pending-prompts/:promptId",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn shell_command(
        &self,
        session_id: &str,
        command: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/shell", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"command": command})),
            "POST /session/:id/shell",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::Disabled,
            ),
        )
        .await
    }
}

impl DaemonClient {
    pub async fn branch_session(
        &self,
        session_id: &str,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/branch", encode_uri_component(session_id));
        let body = json_object([
            ("name", request.get("name").cloned()),
            ("atRecordId", request.get("atRecordId").cloned()),
        ]);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            "POST /session/:id/branch",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(Duration::from_secs(120)),
            ),
        )
        .await
    }

    pub async fn create_side_task_session(
        &self,
        session_id: &str,
        name: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/side-task", encode_uri_component(session_id));
        let body = json_object([("name", name.map(|name| json!(name)))]);
        self.request_json(
            "POST",
            &path,
            "",
            Some(body),
            "POST /session/:id/side-task",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn fork_session(
        &self,
        session_id: &str,
        directive: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/fork", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"directive": directive})),
            "POST /session/:id/fork",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_session_model(
        &self,
        session_id: &str,
        model_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/model", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"modelId": model_id})),
            "POST /session/:id/model",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_session_config_option(
        &self,
        session_id: &str,
        config_id: &str,
        value: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/config-option",
            encode_uri_component(session_id)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"configId": config_id, "value": value})),
            "POST /session/:id/config-option",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_session_language(
        &self,
        session_id: &str,
        language: &str,
        sync_output_language: Option<bool>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/language", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"language": language, "syncOutputLanguage": sync_output_language.unwrap_or(false)})),
            "POST /session/:id/language",
            request_options(client_id, DaemonRequestMode::Transport, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn heartbeat(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/heartbeat", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /session/:id/heartbeat",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn respond_to_permission(
        &self,
        request_id: &str,
        response: Value,
        client_id: Option<String>,
    ) -> Result<bool, DaemonClientError> {
        let path = format!("/permission/{}", encode_uri_component(request_id));
        let response = self
            .request_raw(
                "POST",
                &path,
                "",
                Some(response),
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        match response.status {
            200 => Ok(true),
            404 => Ok(false),
            status => {
                Err(self.http_error(status, response.body, "POST /permission/:requestId", None))
            }
        }
    }

    pub async fn respond_to_session_permission(
        &self,
        session_id: &str,
        request_id: &str,
        response: Value,
        client_id: Option<String>,
    ) -> Result<bool, DaemonClientError> {
        let path = format!(
            "/session/{}/permission/{}",
            encode_uri_component(session_id),
            encode_uri_component(request_id)
        );
        let response = self
            .request_raw(
                "POST",
                &path,
                "",
                Some(response),
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        match response.status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(self.http_error(
                status,
                response.body,
                "POST /session/:id/permission/:requestId",
                None,
            )),
        }
    }

    pub async fn detach_session(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        let Some(client_id) = client_id else {
            return Ok(());
        };
        let path = format!("/session/{}/detach", encode_uri_component(session_id));
        let response = self
            .request_raw(
                "POST",
                &path,
                "",
                None,
                request_options(
                    Some(client_id),
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        if response.status == StatusCode::NO_CONTENT.as_u16()
            || response.status == StatusCode::NOT_FOUND.as_u16()
        {
            Ok(())
        } else {
            Err(self.http_error(
                response.status,
                response.body,
                "POST /session/:id/detach",
                None,
            ))
        }
    }

    pub async fn delete_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/sessions/delete",
            "",
            Some(json!({"sessionIds": session_ids})),
            "POST /sessions/delete",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn archive_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/sessions/archive",
            "",
            Some(json!({"sessionIds": session_ids})),
            "POST /sessions/archive",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn unarchive_sessions_data(
        &self,
        session_ids: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/sessions/unarchive",
            "",
            Some(json!({"sessionIds": session_ids})),
            "POST /sessions/unarchive",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn start_device_flow(
        &self,
        provider_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let response = self
            .request_raw(
                "POST",
                "/workspace/auth/device-flow",
                "",
                Some(json!({"providerId": provider_id})),
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        if response.status != 200 && response.status != 201 {
            return Err(self.http_error(
                response.status,
                response.body,
                "POST /workspace/auth/device-flow",
                None,
            ));
        }
        response.body.ok_or_else(|| {
            DaemonClientError::InvalidResponse(
                "POST /workspace/auth/device-flow: expected JSON response".into(),
            )
        })
    }

    pub async fn get_device_flow(
        &self,
        device_flow_id: &str,
        client_id: Option<String>,
        cancellation: Option<RestSseCancellation>,
    ) -> Result<Value, DaemonClientError> {
        get_device_flow(self, device_flow_id, client_id, cancellation).await
    }

    pub async fn cancel_device_flow(
        &self,
        device_flow_id: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        cancel_device_flow(self, device_flow_id, client_id).await
    }

    pub async fn get_auth_status(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/auth/status",
            "",
            None,
            "GET /workspace/auth/status",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn get_auth_providers(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/auth/providers",
            "",
            None,
            "GET /workspace/auth/providers",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn install_auth_provider(&self, request: Value) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/auth/provider",
            "",
            Some(request),
            "POST /workspace/auth/provider",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn add_workspace(
        &self,
        cwd: &str,
        options: Value,
    ) -> Result<Value, DaemonClientError> {
        let persist = options.get("persist").and_then(Value::as_bool) == Some(true);
        let display_name = options.get("displayName").cloned();
        let body = json_object([
            ("cwd", Some(json!(cwd))),
            ("persist", persist.then_some(json!(true))),
            ("displayName", display_name),
        ]);
        self.request_json(
            "POST",
            "/workspaces",
            "",
            Some(body),
            "POST /workspaces",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn update_workspace(
        &self,
        workspace_selector: &str,
        update: Value,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspaces/{}", encode_uri_component(workspace_selector));
        self.request_json(
            "PATCH",
            &path,
            "",
            Some(update),
            "PATCH /workspaces/:workspace",
            request_options(None, DaemonRequestMode::Rest, RequestTimeout::ClientDefault),
        )
        .await
    }

    pub async fn add_scratch_workspace(&self) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspaces",
            "",
            Some(json!({"kind": "scratch"})),
            "POST /workspaces",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn list_session_artifacts(
        &self,
        session_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/artifacts", encode_uri_component(session_id));
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /session/:id/artifacts",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn add_session_artifact(
        &self,
        session_id: &str,
        artifact: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/artifacts", encode_uri_component(session_id));
        self.request_json(
            "POST",
            &path,
            "",
            Some(artifact),
            "POST /session/:id/artifacts",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn remove_session_artifact(
        &self,
        session_id: &str,
        artifact_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/session/{}/artifacts/{}",
            encode_uri_component(session_id),
            encode_uri_component(artifact_id)
        );
        self.request_json(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /session/:id/artifacts/:artifactId",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn update_session_metadata(
        &self,
        session_id: &str,
        metadata: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/session/{}/metadata", encode_uri_component(session_id));
        let response = self
            .request_json(
                "PATCH",
                &path,
                "",
                Some(metadata),
                "PATCH /session/:id/metadata",
                request_options(
                    client_id,
                    DaemonRequestMode::Transport,
                    RequestTimeout::ClientDefault,
                ),
            )
            .await?;
        Ok(match response.get("displayName").and_then(Value::as_str) {
            Some(display_name) => json!({"displayName": display_name}),
            None => json!({}),
        })
    }
}

impl DaemonClient {
    pub async fn check_extension_updates(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/extensions/check-updates",
            "",
            Some(json!({})),
            "POST /workspace/extensions/check-updates",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn refresh_extensions(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/extensions/refresh",
            "",
            Some(json!({})),
            "POST /workspace/extensions/refresh",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn check_user_extension_updates(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/extensions/check-updates",
            "",
            Some(json!({})),
            "POST /extensions/check-updates",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_workspace_tool_enabled(
        &self,
        tool_name: &str,
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/tools/{}/enable",
            encode_uri_component(tool_name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"enabled": enabled})),
            "POST /workspace/tools/:name/enable",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_workspace_skill_enabled(
        &self,
        skill_name: &str,
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/skills/{}/enable",
            encode_uri_component(skill_name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({"enabled": enabled})),
            "POST /workspace/skills/:name/enable",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_workspace_skills_enabled(
        &self,
        skill_names: Vec<String>,
        enabled: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/skills/enable",
            "",
            Some(json!({"skillNames": skill_names, "enabled": enabled})),
            "POST /workspace/skills/enable",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn install_workspace_skill(
        &self,
        request: Value,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/skills/install",
            "",
            Some(request),
            "Skill",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn delete_workspace_skill(
        &self,
        skill_name: &str,
        scope: &str,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/skills/{}", encode_uri_component(skill_name));
        let query = encode_query_pairs(&[("scope", Some(scope.to_owned()))]);
        self.request_json(
            "DELETE",
            &path,
            &query,
            None,
            "Skill",
            DaemonRequestOptions::default(),
        )
        .await
    }

    pub async fn set_workspace_setting(
        &self,
        scope: &str,
        key: &str,
        value: Value,
        mcp_server_mutation: Option<Value>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let body = json_object([
            ("scope", Some(json!(scope))),
            ("key", Some(json!(key))),
            ("value", Some(value)),
            ("mcpServerMutation", mcp_server_mutation),
        ]);
        self.request_json(
            "POST",
            "/workspace/settings",
            "",
            Some(body),
            "POST /workspace/settings",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn delete_model(
        &self,
        target: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "DELETE",
            "/workspace/models",
            "",
            Some(target),
            "DELETE /workspace/models",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn workspace_settings(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/settings",
            "",
            None,
            "GET /workspace/settings",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn workspace_voice(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/voice",
            "",
            None,
            "GET /workspace/voice",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_workspace_voice(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/voice",
            "",
            Some(update),
            "POST /workspace/voice",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn transcribe_workspace_voice(
        &self,
        audio: Vec<u8>,
        mime_type: &str,
        voice_model: Option<&str>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = encode_query_pairs(&[(
            "voiceModel",
            voice_model
                .filter(|model| !model.is_empty())
                .map(str::to_owned),
        )]);
        let response = self
            .request_bytes(
                "POST",
                "/workspace/voice/transcribe",
                &query,
                Some(audio),
                "POST /workspace/voice/transcribe",
                DaemonRequestOptions {
                    client_id,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some(mime_type.to_owned()),
                    timeout: RequestTimeout::After(
                        timeout_duration.unwrap_or(Duration::from_secs(65)),
                    ),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /workspace/voice/transcribe: invalid JSON response: {error}"
            ))
        })
    }

    /// Workspace-qualified counterpart used by `WorkspaceDaemonClient`.
    /// `workspace_selector` is already percent-encoded, as returned by the
    /// `workspace_by_id` and `workspace_by_cwd` constructors.
    pub async fn workspace_voice_transcription_request(
        &self,
        workspace_selector: &str,
        audio: Vec<u8>,
        mime_type: &str,
        voice_model: Option<&str>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspaces/{workspace_selector}/voice/transcribe");
        let query = encode_query_pairs(&[(
            "voiceModel",
            voice_model
                .filter(|model| !model.is_empty())
                .map(str::to_owned),
        )]);
        let response = self
            .request_bytes(
                "POST",
                &path,
                &query,
                Some(audio),
                "POST /workspaces/:workspace/voice/transcribe",
                DaemonRequestOptions {
                    client_id,
                    mode: DaemonRequestMode::Rest,
                    content_type: Some(mime_type.to_owned()),
                    timeout: RequestTimeout::After(
                        timeout_duration.unwrap_or(Duration::from_secs(65)),
                    ),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        serde_json::from_slice(&response.body).map_err(|error| {
            DaemonClientError::InvalidResponse(format!(
                "POST /workspaces/:workspace/voice/transcribe: invalid JSON response: {error}"
            ))
        })
    }

    pub async fn live_status(&self, client_id: Option<String>) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/live/status",
            "",
            None,
            "GET /live/status",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn live_setup_status(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/live/setup",
            "",
            None,
            "GET /live/setup",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn update_live_setup(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/setup",
            "",
            Some(update),
            "POST /live/setup",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn retry_live_host_install(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/setup/install",
            "",
            Some(json!({})),
            "POST /live/setup/install",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn launch_live_host(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/setup/launch",
            "",
            Some(json!({})),
            "POST /live/setup/launch",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn start_live(
        &self,
        mode: Option<&str>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let is_new = mode == Some("new");
        let path = if is_new { "/live/new" } else { "/live/start" };
        let label = if is_new {
            "POST /live/new"
        } else {
            "POST /live/start"
        };
        self.request_json(
            "POST",
            path,
            "",
            Some(json!({})),
            label,
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn stop_live(&self, client_id: Option<String>) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/stop",
            "",
            Some(json!({})),
            "POST /live/stop",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_live_mute(
        &self,
        update: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/mute",
            "",
            Some(update),
            "POST /live/mute",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_live_shortcut(
        &self,
        shortcut: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/live/shortcut",
            "",
            Some(json!({"shortcut": shortcut})),
            "POST /live/shortcut",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn workspace_trust(
        &self,
        status_version: Option<u8>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let query = if status_version == Some(2) {
            "statusVersion=2"
        } else {
            ""
        };
        self.request_json(
            "GET",
            "/workspace/trust",
            query,
            None,
            "GET /workspace/trust",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn request_workspace_trust_change(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/trust/request",
            "",
            Some(request),
            "POST /workspace/trust/request",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn workspace_permissions(
        &self,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/permissions",
            "",
            None,
            "GET /workspace/permissions",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn set_workspace_permission_rules(
        &self,
        scope: &str,
        rule_type: &str,
        rules: Vec<String>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/permissions",
            "",
            Some(json!({"scope": scope, "ruleType": rule_type, "rules": rules})),
            "POST /workspace/permissions",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn add_workspace_permission_rule(
        &self,
        scope: &str,
        rule_type: &str,
        rule: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let rule = rule.trim();
        if rule.is_empty() {
            return Err(DaemonClientError::InvalidResponse(
                "rule must be a non-empty string".into(),
            ));
        }
        let current = self.workspace_permissions(client_id.clone()).await?;
        let Some(rules) = current
            .get(scope)
            .and_then(|value| value.get("rules"))
            .and_then(|value| value.get(rule_type))
            .and_then(Value::as_array)
        else {
            return Err(DaemonClientError::InvalidResponse(
                "workspace permissions response is missing the requested rule list".into(),
            ));
        };
        if rules.iter().any(|existing| existing.as_str() == Some(rule)) {
            return Ok(current);
        }
        let mut next = rules
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        next.push(rule.to_owned());
        self.set_workspace_permission_rules(scope, rule_type, next, client_id)
            .await
    }

    pub async fn remove_workspace_permission_rule(
        &self,
        scope: &str,
        rule_type: &str,
        rule: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let rule = rule.trim();
        if rule.is_empty() {
            return Err(DaemonClientError::InvalidResponse(
                "rule must be a non-empty string".into(),
            ));
        }
        let current = self.workspace_permissions(client_id.clone()).await?;
        let Some(rules) = current
            .get(scope)
            .and_then(|value| value.get("rules"))
            .and_then(|value| value.get(rule_type))
            .and_then(Value::as_array)
        else {
            return Err(DaemonClientError::InvalidResponse(
                "workspace permissions response is missing the requested rule list".into(),
            ));
        };
        if !rules.iter().any(|existing| existing.as_str() == Some(rule)) {
            return Ok(current);
        }
        let next = rules
            .iter()
            .filter_map(Value::as_str)
            .filter(|existing| *existing != rule)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        self.set_workspace_permission_rules(scope, rule_type, next, client_id)
            .await
    }

    pub async fn restart_mcp_server(
        &self,
        server_name: &str,
        entry_index: Option<&str>,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/mcp/{}/restart",
            encode_uri_component(server_name)
        );
        let query = encode_query_pairs(&[("entryIndex", entry_index.map(str::to_owned))]);
        self.request_json(
            "POST",
            &path,
            &query,
            Some(json!({})),
            "POST /workspace/mcp/:server/restart",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(330))),
            ),
        )
        .await
    }

    pub async fn reload(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/reload",
            "",
            Some(json!({})),
            "POST /workspace/reload",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn reload_channel_worker(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/channel/reload",
            "",
            Some(json!({})),
            "POST /workspace/channel/reload",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn get_channel_worker_control(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/channel",
            "",
            None,
            "GET /workspace/channel",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn set_channel_worker_selection(
        &self,
        selection: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "PUT",
            "/workspace/channel",
            "",
            Some(json!({"selection": selection})),
            "PUT /workspace/channel",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn stop_channel_worker(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "DELETE",
            "/workspace/channel",
            "",
            None,
            "DELETE /workspace/channel",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn workspace_channel_types(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/channel-types",
            "",
            None,
            "GET /workspace/channel-types",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn workspace_channels(
        &self,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            "/workspace/channels",
            "",
            None,
            "GET /workspace/channels",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn upsert_workspace_channel(
        &self,
        name: &str,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/channels/{}", encode_uri_component(name));
        self.request_json(
            "PUT",
            &path,
            "",
            Some(request),
            "PUT /workspace/channels/:name",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn delete_workspace_channel(
        &self,
        name: &str,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/channels/{}", encode_uri_component(name));
        self.request_json(
            "DELETE",
            &path,
            "",
            Some(request),
            "DELETE /workspace/channels/:name",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn set_workspace_channel_startup(
        &self,
        name: &str,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/channels/{}/startup", encode_uri_component(name));
        self.request_json(
            "PUT",
            &path,
            "",
            Some(request),
            "PUT /workspace/channels/:name/startup",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn workspace_channel_action(
        &self,
        name: &str,
        action: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        if !matches!(action, "start" | "stop" | "restart") {
            return Err(DaemonClientError::InvalidRoute(
                "channel action must be start, stop, or restart".into(),
            ));
        }
        let path = format!(
            "/workspace/channels/{}/{}",
            encode_uri_component(name),
            action
        );
        let label = format!("POST /workspace/channels/:name/{action}");
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            &label,
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn start_workspace_channel(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_channel_action(name, "start", timeout_duration, client_id)
            .await
    }

    pub async fn stop_workspace_channel(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_channel_action(name, "stop", timeout_duration, client_id)
            .await
    }

    pub async fn restart_workspace_channel(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.workspace_channel_action(name, "restart", timeout_duration, client_id)
            .await
    }

    pub async fn workspace_channel_pairing_requests(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/channels/{}/pairing-requests",
            encode_uri_component(name)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/channels/:name/pairing-requests",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn approve_workspace_channel_pairing(
        &self,
        name: &str,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/channels/{}/pairing-requests/approve",
            encode_uri_component(name)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(request),
            "POST /workspace/channels/:name/pairing-requests/approve",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn workspace_channel_pairing_approvals(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/channels/{}/pairing-approvals",
            encode_uri_component(name)
        );
        self.request_json(
            "GET",
            &path,
            "",
            None,
            "GET /workspace/channels/:name/pairing-approvals",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                timeout_duration
                    .map(RequestTimeout::After)
                    .unwrap_or(RequestTimeout::ClientDefault),
            ),
        )
        .await
    }

    pub async fn revoke_workspace_channel_pairing_approval(
        &self,
        name: &str,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/channels/{}/pairing-approvals",
            encode_uri_component(name)
        );
        self.request_json(
            "DELETE",
            &path,
            "",
            Some(request),
            "DELETE /workspace/channels/:name/pairing-approvals",
            request_options(
                client_id,
                DaemonRequestMode::Rest,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(2_130))),
            ),
        )
        .await
    }

    pub async fn manage_mcp_server(
        &self,
        server_name: &str,
        action: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!(
            "/workspace/mcp/{}/{}",
            encode_uri_component(server_name),
            encode_uri_component(action)
        );
        self.request_json(
            "POST",
            &path,
            "",
            Some(json!({})),
            "POST /workspace/mcp/:server/:action",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(330))),
            ),
        )
        .await
    }

    pub async fn add_runtime_mcp_server(
        &self,
        request: Value,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/mcp/servers",
            "",
            Some(request),
            "POST /workspace/mcp/servers",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(330))),
            ),
        )
        .await
    }

    pub async fn remove_runtime_mcp_server(
        &self,
        name: &str,
        timeout_duration: Option<Duration>,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspace/mcp/servers/{}", encode_uri_component(name));
        self.request_json(
            "DELETE",
            &path,
            "",
            None,
            "DELETE /workspace/mcp/servers/:name",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(timeout_duration.unwrap_or(Duration::from_secs(330))),
            ),
        )
        .await
    }

    pub async fn init_workspace(
        &self,
        force: bool,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        let body = if force {
            json!({"force": true})
        } else {
            json!({})
        };
        self.request_json(
            "POST",
            "/workspace/init",
            "",
            Some(body),
            "POST /workspace/init",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::ClientDefault,
            ),
        )
        .await
    }

    pub async fn setup_github(
        &self,
        request: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            "/workspace/setup-github",
            "",
            Some(request),
            "POST /workspace/setup-github",
            request_options(
                client_id,
                DaemonRequestMode::Transport,
                RequestTimeout::After(Duration::from_secs(90)),
            ),
        )
        .await
    }
}

/// Workspace-scoped route facade; dynamic endpoints remain available through
/// `request_json` so new TypeScript wrappers do not require a Rust release.
#[derive(Clone)]
pub struct WorkspaceDaemonClient {
    client: DaemonClient,
    selector: String,
}

impl WorkspaceDaemonClient {
    pub fn selector(&self) -> &str {
        &self.selector
    }

    pub async fn request_json(
        &self,
        method: &str,
        suffix: &str,
        query: &str,
        body: Option<Value>,
        label: &str,
        options: DaemonRequestOptions,
    ) -> Result<Value, DaemonClientError> {
        let path = format!("/workspaces/{}{}", self.selector, suffix);
        self.client
            .request_json(method, &path, query, body, label, options)
            .await
    }

    pub async fn get(
        &self,
        suffix: &str,
        label: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "GET",
            suffix,
            "",
            None,
            label,
            DaemonRequestOptions {
                client_id,
                ..DaemonRequestOptions::default()
            },
        )
        .await
    }

    pub async fn post(
        &self,
        suffix: &str,
        label: &str,
        body: Value,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        self.request_json(
            "POST",
            suffix,
            "",
            Some(body),
            label,
            DaemonRequestOptions {
                client_id,
                ..DaemonRequestOptions::default()
            },
        )
        .await
    }
}

/// Basic URL encoding equivalent to JavaScript's `encodeURIComponent` for a
/// single dynamic path component.
pub fn encode_uri_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&byte) {
            encoded.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn encode_query_pairs(pairs: &[(&str, Option<String>)]) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in pairs {
        if let Some(value) = value {
            query.append_pair(key, value);
        }
    }
    query.finish()
}

fn json_object(fields: impl IntoIterator<Item = (&'static str, Option<Value>)>) -> Value {
    let mut object = serde_json::Map::new();
    for (key, value) in fields {
        if let Some(value) = value {
            object.insert(key.to_owned(), value);
        }
    }
    Value::Object(object)
}

fn content_disposition_filename(header: &str) -> Option<String> {
    header.split(';').find_map(|parameter| {
        let (name, value) = parameter.trim().split_once('=')?;
        if !name.eq_ignore_ascii_case("filename") {
            return None;
        }
        let value = value.trim();
        let value = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or(value);
        Some(value.to_owned())
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

fn request_options(
    client_id: Option<String>,
    mode: DaemonRequestMode,
    timeout: RequestTimeout,
) -> DaemonRequestOptions {
    DaemonRequestOptions {
        client_id,
        mode,
        timeout,
        ..DaemonRequestOptions::default()
    }
}

fn validate_route(path: &str) -> Result<(), DaemonClientError> {
    if !path.starts_with('/') || path.starts_with("//") || path.contains('#') {
        return Err(DaemonClientError::InvalidRoute(
            "route path must begin with one slash and must not contain a fragment".into(),
        ));
    }
    Ok(())
}

fn strip_trailing_slashes(mut value: String) -> String {
    while value.ends_with('/') {
        value.pop();
    }
    value
}

fn read_token_from_env() -> Option<String> {
    std::env::var("QWEN_SERVER_TOKEN")
        .ok()
        .map(|value| trim_js_whitespace(&value).to_owned())
        .filter(|value| !value.is_empty())
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(is_js_whitespace)
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

fn add_request_headers(
    headers: &mut HeaderMap,
    token: &Option<String>,
    options: &DaemonRequestOptions,
) -> Result<(), DaemonClientError> {
    headers.extend(options.extra_headers.clone());
    if let Some(token) = token.as_ref().filter(|token| !token.is_empty()) {
        let value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        headers.insert(AUTHORIZATION, value);
    }
    if let Some(client_id) = options.client_id.as_deref() {
        let value = HeaderValue::from_str(client_id)
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        headers.insert(HeaderName::from_static("x-qwen-client-id"), value);
    }
    if let Some(content_type) = options.content_type.as_deref() {
        let value = HeaderValue::from_str(content_type)
            .map_err(|error| DaemonClientError::InvalidRoute(error.to_string()))?;
        headers.insert(CONTENT_TYPE, value);
    }
    Ok(())
}

async fn response_to_json(
    response: Response,
    max_bytes: usize,
) -> Result<DaemonRawResponse, DaemonClientError> {
    let status = response.status().as_u16();
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| AutoReconnectError::Operation(error.to_string()))?
    {
        let remaining = max_bytes.saturating_sub(bytes.len());
        if chunk.len() > remaining {
            bytes.extend_from_slice(&chunk[..remaining]);
            bytes.extend_from_slice(b"\n...[truncated]");
            break;
        }
        bytes.extend_from_slice(&chunk);
        if bytes.len() == max_bytes {
            bytes.extend_from_slice(b"\n...[truncated]");
            break;
        }
    }
    let body = if bytes.is_empty() {
        None
    } else {
        Some(
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())),
        )
    };
    Ok(DaemonRawResponse { status, body })
}

async fn response_to_bytes(
    response: Response,
    max_bytes: usize,
) -> Result<DaemonBytesResponse, DaemonClientError> {
    let status = response.status().as_u16();
    let max_bytes = if (200..300).contains(&status) {
        max_bytes
    } else {
        MAX_ROUTE_BODY_BYTES
    };
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_disposition = response
        .headers()
        .get("content-disposition")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream
        .try_next()
        .await
        .map_err(|error| AutoReconnectError::Operation(error.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(DaemonClientError::ResponseTooLarge { limit: max_bytes });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(DaemonBytesResponse {
        status,
        content_type,
        content_disposition,
        body: bytes,
    })
}

async fn timeout_future<F, T>(
    future: F,
    timeout_duration: Option<Duration>,
) -> Result<T, DaemonClientError>
where
    F: Future<Output = Result<T, DaemonClientError>>,
{
    match timeout_duration.filter(|duration| !duration.is_zero()) {
        Some(duration) => timeout(duration, future)
            .await
            .map_err(|_| DaemonClientError::Timeout)?,
        None => future.await,
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

async fn wait_for_optional_cancel(receiver: &mut Option<watch::Receiver<bool>>) {
    match receiver {
        Some(receiver) => wait_for_cancel(receiver).await,
        None => std::future::pending().await,
    }
}

fn parse_generation_event(value: Value) -> Option<DaemonGenerationEvent> {
    let event = value.as_object()?;
    if event.get("v")?.as_f64()? != 1.0 {
        return None;
    }
    let event_type = event.get("type")?.as_str()?;
    let request_id = || {
        event
            .get("requestId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let model_source = || match event.get("modelSource").and_then(Value::as_str) {
        Some(value @ ("fast" | "main")) => Some(value.to_owned()),
        _ => None,
    };
    let safe_count = |key: &str| -> Option<Option<u64>> {
        match event.get(key) {
            None => Some(None),
            Some(value) => {
                let number = value.as_f64()?;
                (number.is_finite()
                    && number.fract() == 0.0
                    && (0.0..=MAX_SAFE_INTEGER).contains(&number))
                .then_some(Some(number as u64))
            }
        }
    };

    match event_type {
        "started" => Some(DaemonGenerationEvent::Started {
            request_id: request_id()?,
            model: event.get("model")?.as_str()?.to_owned(),
            model_source: model_source()?,
            raw: value,
        }),
        "thinking" => Some(DaemonGenerationEvent::Thinking {
            request_id: request_id()?,
            raw: value,
        }),
        "delta" => {
            let seq = event.get("seq")?.as_f64()?;
            if !seq.is_finite() || seq.fract() != 0.0 || !(0.0..=MAX_SAFE_INTEGER).contains(&seq) {
                return None;
            }
            let text = event.get("text")?.as_str()?.to_owned();
            if text.is_empty() {
                return None;
            }
            Some(DaemonGenerationEvent::Delta {
                request_id: request_id()?,
                seq: seq as u64,
                text,
                raw: value,
            })
        }
        "done" => Some(DaemonGenerationEvent::Done {
            request_id: request_id()?,
            model: event.get("model")?.as_str()?.to_owned(),
            model_source: model_source()?,
            input_tokens: safe_count("inputTokens")?,
            output_tokens: safe_count("outputTokens")?,
            raw: value,
        }),
        "error" => Some(DaemonGenerationEvent::Error {
            code: event.get("code")?.as_str()?.to_owned(),
            message: event.get("message")?.as_str()?.to_owned(),
            raw: value,
        }),
        _ => None,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => "null".into(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                _ => js_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

pub fn match_turn_event(
    event: &DaemonEvent,
    prompt_id: &str,
) -> Result<Option<Value>, DaemonClientError> {
    let data = event.data.as_ref();
    if event.event_type == "turn_complete"
        && data
            .and_then(|data| data.get("promptId"))
            .and_then(Value::as_str)
            == Some(prompt_id)
    {
        let stop_reason = data
            .and_then(|data| data.get("stopReason"))
            .and_then(Value::as_str)
            .unwrap_or("end_turn");
        let mut result = json!({ "stopReason": stop_reason });
        if stop_reason == "end_turn"
            && let Some(branch) = data.and_then(|data| data.get("branchPoint"))
            && let (Some(assistant), Some(checkpoint)) = (
                branch.get("assistantRecordUuid").and_then(Value::as_str),
                branch.get("checkpointUuid").and_then(Value::as_str),
            )
            && is_record_uuid(assistant)
            && is_record_uuid(checkpoint)
        {
            result["branchPoint"] = json!({
                "assistantRecordUuid": assistant,
                "checkpointUuid": checkpoint
            });
        }
        return Ok(Some(result));
    }
    if event.event_type == "turn_error"
        && data
            .and_then(|data| data.get("promptId"))
            .and_then(Value::as_str)
            == Some(prompt_id)
    {
        let code = data
            .and_then(|data| data.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("turn_error");
        let message = data
            .and_then(|data| data.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Prompt failed");
        return Err(DaemonHttpError {
            status: 500,
            body: Some(Value::String(code.to_owned())),
            message: message.to_owned(),
            is_daemon_turn_error: true,
        }
        .into());
    }
    Ok(None)
}

fn is_record_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
    {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        if !byte.is_ascii_hexdigit() {
            return false;
        }
    }
    matches!(bytes[14], b'1'..=b'5') && matches!(bytes[19].to_ascii_lowercase(), b'8'..=b'b')
}

/// TypeScript `isDaemonTurnError` equivalent.
pub fn is_daemon_turn_error(error: &DaemonClientError) -> bool {
    matches!(error, DaemonClientError::Http(error) if error.is_daemon_turn_error)
}

/// TypeScript `isStaleBranchPointError` equivalent.
pub fn is_stale_branch_point_error(error: &DaemonClientError) -> bool {
    matches!(error, DaemonClientError::Http(error)
        if error.status == StatusCode::CONFLICT.as_u16()
            && error.body.as_ref().and_then(|body| body.get("code"))
                .and_then(Value::as_str) == Some("branch_point_invalid"))
}

/// TypeScript `isSubagentSessionNotFound` equivalent. With no tool-call ID,
/// either the session-level or subagent-level 404 is accepted.
pub fn is_subagent_session_not_found(
    error: &DaemonClientError,
    tool_call_id: Option<&str>,
) -> bool {
    matches!(error, DaemonClientError::Http(error)
        if error.status == StatusCode::NOT_FOUND.as_u16()
            && error.body.as_ref().and_then(|body| body.get("code"))
                .and_then(Value::as_str) == Some("session_not_found")
            && tool_call_id.is_none_or(|tool_call_id|
                error.body.as_ref()
                    .and_then(|body| body.get("toolCallId"))
                    .and_then(Value::as_str) == Some(tool_call_id)))
}

/// TypeScript `isSessionLevelNotFound` equivalent. Missing and null
/// `toolCallId` both describe a missing parent session.
pub fn is_session_level_not_found(error: &DaemonClientError) -> bool {
    matches!(error, DaemonClientError::Http(error)
        if error.status == StatusCode::NOT_FOUND.as_u16()
            && error.body.as_ref().and_then(|body| body.get("code"))
                .and_then(Value::as_str) == Some("session_not_found")
            && error.body.as_ref()
                .and_then(|body| body.get("toolCallId"))
                .is_none_or(Value::is_null))
}

/// TypeScript `isNonBlockingAccepted` equivalent.
pub fn is_non_blocking_accepted(result: &Value) -> bool {
    result.get("promptId").is_some() && result.get("lastEventId").is_some()
}

struct PromptSlot {
    counts: Option<Arc<Mutex<HashMap<String, usize>>>>,
    session_id: String,
}

impl PromptSlot {
    fn unlimited() -> Self {
        Self {
            counts: None,
            session_id: String::new(),
        }
    }
}

impl Drop for PromptSlot {
    fn drop(&mut self) {
        let Some(counts) = self.counts.as_ref() else {
            return;
        };
        let mut counts = lock_recover(counts);
        if let Some(count) = counts.get_mut(&self.session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.session_id);
            }
        }
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// High-level OAuth device-flow facade matching `DaemonClient.auth`.
#[derive(Clone)]
pub struct DaemonAuthFlow {
    client: DaemonClient,
}

impl DaemonAuthFlow {
    pub fn new(client: DaemonClient) -> Self {
        Self { client }
    }

    pub async fn start(
        &self,
        provider_id: &str,
        client_id: Option<String>,
    ) -> Result<DaemonAuthFlowHandle, DaemonClientError> {
        let response = self
            .client
            .request_raw(
                "POST",
                "/workspace/auth/device-flow",
                "",
                Some(json!({ "providerId": provider_id })),
                DaemonRequestOptions {
                    client_id: client_id.clone(),
                    ..DaemonRequestOptions::default()
                },
            )
            .await?;
        if response.status != 200 && response.status != 201 {
            return Err(self.client.http_error(
                response.status,
                response.body,
                "POST /workspace/auth/device-flow",
                None,
            ));
        }
        let initial = response.body.ok_or_else(|| {
            DaemonClientError::InvalidResponse("device-flow response has no JSON body".into())
        })?;
        Ok(DaemonAuthFlowHandle {
            initial,
            client: self.client.clone(),
            client_id,
        })
    }

    pub async fn status(
        &self,
        device_flow_id: &str,
        client_id: Option<String>,
    ) -> Result<Value, DaemonClientError> {
        get_device_flow(&self.client, device_flow_id, client_id, None).await
    }

    pub async fn cancel(
        &self,
        device_flow_id: &str,
        client_id: Option<String>,
    ) -> Result<(), DaemonClientError> {
        cancel_device_flow(&self.client, device_flow_id, client_id).await
    }
}

/// A started device-flow and the client identity that owns it.
#[derive(Clone)]
pub struct DaemonAuthFlowHandle {
    pub initial: Value,
    client: DaemonClient,
    client_id: Option<String>,
}

impl DaemonAuthFlowHandle {
    pub async fn cancel(&self) -> Result<(), DaemonClientError> {
        let id = self
            .initial
            .get("deviceFlowId")
            .and_then(Value::as_str)
            .ok_or_else(|| DaemonClientError::InvalidResponse("deviceFlowId is missing".into()))?;
        cancel_device_flow(&self.client, id, self.client_id.clone()).await
    }

    pub async fn await_completion(
        &self,
        options: AuthAwaitOptions,
    ) -> Result<Value, DaemonClientError> {
        let id = self
            .initial
            .get("deviceFlowId")
            .and_then(Value::as_str)
            .ok_or_else(|| DaemonClientError::InvalidResponse("deviceFlowId is missing".into()))?
            .to_owned();
        let provider_id = self
            .initial
            .get("providerId")
            .cloned()
            .unwrap_or(Value::Null);
        let initial_interval = self
            .initial
            .get("intervalMs")
            .and_then(Value::as_u64)
            .unwrap_or(5_000);
        let expires_at = self
            .initial
            .get("expiresAt")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| Duration::from_millis(value.min(u64::MAX as f64) as u64))
            .unwrap_or_else(|| unix_now());
        let now = unix_now();
        let ceiling = options.timeout.map_or_else(
            || expires_at.saturating_add(AUTH_EXPIRY_GRACE),
            |duration| now.saturating_add(duration),
        );
        let mut interval = options
            .poll_override
            .filter(|duration| !duration.is_zero())
            .unwrap_or_else(|| Duration::from_millis(initial_interval.max(1_000)));
        let mut last_interval = interval;

        loop {
            if options
                .cancellation
                .as_ref()
                .is_some_and(RestSseCancellation::is_cancelled)
            {
                return Err(DaemonClientError::Cancelled);
            }
            let now = unix_now();
            let snapshot = match get_device_flow(
                &self.client,
                &id,
                self.client_id.clone(),
                options.cancellation.clone(),
            )
            .await
            {
                Ok(snapshot) => snapshot,
                Err(DaemonClientError::Http(error)) if error.status == 404 => {
                    json!({
                        "deviceFlowId": id,
                        "providerId": provider_id,
                        "status": "error",
                        "errorKind": "not_found_or_evicted",
                        "hint": "device-flow not found on daemon (evicted past terminal grace, daemon restart, or unknown deviceFlowId)",
                        "createdAt": epoch_millis(),
                    })
                }
                Err(error) => return Err(error),
            };
            if now >= ceiling || is_terminal_auth_state(&snapshot) {
                return Ok(snapshot);
            }
            if let Some(next_interval) = snapshot
                .get("intervalMs")
                .and_then(Value::as_u64)
                .filter(|millis| *millis > 0)
                .map(Duration::from_millis)
                .filter(|duration| *duration != last_interval)
            {
                last_interval = next_interval;
                interval = next_interval;
                if let Some(callback) = options.on_throttled.as_ref() {
                    callback(next_interval);
                }
            }
            let remaining = ceiling.saturating_sub(unix_now());
            let delay = interval.min(remaining);
            if delay.is_zero() {
                continue;
            }
            if let Some(cancellation) = options.cancellation.as_ref() {
                let mut receiver = cancellation.subscribe_cancelled();
                tokio::select! {
                    biased;
                    _ = wait_for_cancel(&mut receiver) => return Err(DaemonClientError::Cancelled),
                    _ = sleep(delay) => {}
                }
            } else {
                sleep(delay).await;
            }
        }
    }
}

/// Controls for device-flow polling.
#[derive(Clone, Default)]
pub struct AuthAwaitOptions {
    pub cancellation: Option<RestSseCancellation>,
    /// `Some(Duration::ZERO)` returns the current daemon snapshot immediately.
    pub timeout: Option<Duration>,
    /// A zero poll override is ignored; intervals have a one-second floor.
    pub poll_override: Option<Duration>,
    pub on_throttled: Option<Arc<dyn Fn(Duration) + Send + Sync>>,
}

async fn get_device_flow(
    client: &DaemonClient,
    device_flow_id: &str,
    client_id: Option<String>,
    cancellation: Option<RestSseCancellation>,
) -> Result<Value, DaemonClientError> {
    client
        .request_json(
            "GET",
            &format!(
                "/workspace/auth/device-flow/{}",
                encode_uri_component(device_flow_id)
            ),
            "",
            None,
            "GET /workspace/auth/device-flow/:id",
            DaemonRequestOptions {
                client_id,
                cancellation,
                ..DaemonRequestOptions::default()
            },
        )
        .await
}

async fn cancel_device_flow(
    client: &DaemonClient,
    device_flow_id: &str,
    client_id: Option<String>,
) -> Result<(), DaemonClientError> {
    let response = client
        .request_raw(
            "DELETE",
            &format!(
                "/workspace/auth/device-flow/{}",
                encode_uri_component(device_flow_id)
            ),
            "",
            None,
            DaemonRequestOptions {
                client_id,
                ..DaemonRequestOptions::default()
            },
        )
        .await?;
    if response.status == StatusCode::NO_CONTENT.as_u16()
        || response.status == StatusCode::NOT_FOUND.as_u16()
    {
        Ok(())
    } else {
        Err(client.http_error(
            response.status,
            response.body,
            "DELETE /workspace/auth/device-flow/:id",
            None,
        ))
    }
}

fn is_terminal_auth_state(state: &Value) -> bool {
    matches!(
        state.get("status").and_then(Value::as_str),
        Some("authorized" | "expired" | "error" | "cancelled")
    )
}

fn unix_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
