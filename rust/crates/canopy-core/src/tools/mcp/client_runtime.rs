//! MCP client lifecycle and JSON-RPC operations.
//!
//! The source client uses `@modelcontextprotocol/sdk` to own low-level
//! transport framing. This module keeps that boundary injectable: native
//! stdio, SSE, Streamable HTTP, WebSocket, and SDK-control adapters implement
//! [`McpTransport`] and [`McpTransportFactory`], while this module owns the
//! MCP initialize handshake, request IDs and envelopes, discovery semantics,
//! cancellation, timeouts, retries, status, and cleanup.

pub use super::status::{
    McpClientStatus, McpServerStatus, McpServerStatusRegistry, McpStatusListener,
    McpStatusListenerId, add_mcp_status_change_listener, get_all_mcp_server_statuses,
    get_mcp_server_status, mcp_server_status_registry, remove_mcp_server_status,
    remove_mcp_status_change_listener, update_mcp_server_status,
};
use crate::mcp::oauth_utils::OAuthUtils;
use crate::mcp::token_storage::{BaseTokenStorage, OAuthCredentials, OAuthToken, TokenStorage};
use crate::tools::mcp::resource_content::{
    FormatMcpResourceOptions, FormattedMcpResource, format_mcp_resource_contents,
};
use crate::tools::mcp::retry::{McpRetryError, McpRetryOptions, retry_with_backoff};
use crate::utils::cancellation::CancellationToken;
use crate::utils::sanitize_child_env::sanitize_child_env;
use futures_util::future::BoxFuture;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

/// The default MCP request timeout used by `mcp-client.ts`.
pub const MCP_DEFAULT_TIMEOUT_MSEC: u64 = 10 * 60 * 1000;
/// The protocol version sent by the pinned MCP SDK line in this repository.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const MCP_SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
const MCP_OAUTH_PROBE_TIMEOUT_MS: u64 = 5_000;
const MCP_AUTOMATIC_OAUTH_TIMEOUT_MS: u64 = 60_000;
const STREAMABLE_HTTP_GET_SSE_ERROR_BODY_LIMIT: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum StatusByte {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum McpTransportError {
    #[error("HTTP {status} {message}{challenge}", challenge = www_authenticate.as_ref().map(|v| format!("; WWW-Authenticate: {v}")).unwrap_or_default())]
    HttpStatus {
        status: u16,
        message: String,
        www_authenticate: Option<String>,
    },
    #[error("JSON-RPC error {code}: {message}")]
    JsonRpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("MCP transport error: {0}")]
    Transport(String),
    #[error("MCP transport request cancelled")]
    Cancelled,
}

#[derive(Clone, Debug, Error, PartialEq)]
pub enum McpClientError {
    #[error("MCP client is not connected")]
    NotConnected,
    #[error("MCP operation '{method}' timed out after {timeout_ms}ms")]
    Timeout { method: String, timeout_ms: u64 },
    #[error("MCP operation cancelled")]
    Cancelled,
    #[error("MCP transport setup failed: {0}")]
    TransportSetup(String),
    #[error("MCP transport error: {0}")]
    Transport(String),
    #[error("HTTP {status}: {message}")]
    HttpStatus {
        status: u16,
        message: String,
        www_authenticate: Option<String>,
    },
    #[error("MCP JSON-RPC error {code}: {message}")]
    JsonRpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("invalid MCP response: {0}")]
    InvalidResponse(String),
    #[error("No prompts, tools, or resources found on the server.")]
    NoDiscoverableContent,
    #[error("MCP server '{server_name}' requires OAuth authentication. {instruction}")]
    OAuthRequired {
        server_name: String,
        instruction: String,
    },
}

impl From<McpTransportError> for McpClientError {
    fn from(value: McpTransportError) -> Self {
        match value {
            McpTransportError::HttpStatus {
                status,
                message,
                www_authenticate,
            } => Self::HttpStatus {
                status,
                message,
                www_authenticate,
            },
            McpTransportError::JsonRpc {
                code,
                message,
                data,
            } => Self::JsonRpc {
                code,
                message,
                data,
            },
            McpTransportError::Transport(message) => Self::Transport(message),
            McpTransportError::Cancelled => Self::Cancelled,
        }
    }
}

impl McpClientError {
    pub fn json_rpc_code(&self) -> Option<i64> {
        match self {
            Self::JsonRpc { code, .. } => Some(*code),
            _ => None,
        }
    }

    pub fn http_status(&self) -> Option<u16> {
        match self {
            Self::HttpStatus { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Matches the source's exact, case-sensitive fallback when a transport
    /// drops JSON-RPC's `-32601` code.
    pub fn is_method_not_found(&self) -> bool {
        self.json_rpc_code() == Some(-32601) || self.to_string().contains("Method not found")
    }

    fn is_transient(&self) -> bool {
        if matches!(self.json_rpc_code(), Some(-32602..=-32600))
            || matches!(self, Self::Cancelled | Self::Timeout { .. })
            || matches!(self.http_status(), Some(401 | 403))
        {
            return false;
        }

        if matches!(self.http_status(), Some(502..=504)) {
            return true;
        }

        super::retry::is_transient_network_error(
            &self.to_string(),
            self.json_rpc_code().map(|code| code as f64),
        )
    }
}

/// Request shape passed to a transport adapter. `request` must return the
/// complete JSON-RPC response envelope so this module can validate the ID and
/// map protocol errors consistently.
pub trait McpTransport: Send + Sync {
    fn request<'a>(
        &'a self,
        request: Value,
        cancellation: Option<CancellationToken>,
    ) -> BoxFuture<'a, Result<Value, McpTransportError>>;

    fn notify<'a>(
        &'a self,
        notification: Value,
        cancellation: Option<CancellationToken>,
    ) -> BoxFuture<'a, Result<(), McpTransportError>>;

    fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), McpTransportError>>;

    fn set_server_request_handler(&self, _handler: McpServerRequestHandler) {}

    fn set_error_handler(&self, _handler: McpTransportErrorHandler) {}

    fn set_protocol_version(&self, _version: &str) {}

    fn pid(&self) -> Option<u32> {
        None
    }
}

pub type McpServerRequestHandler = Arc<dyn Fn(&Value) -> Value + Send + Sync>;
pub type McpTransportErrorHandler = Arc<dyn Fn(McpTransportError) + Send + Sync>;

/// Transport adapters receive normalized settings and own platform-specific
/// process, socket, authentication, and HTTP behavior.
pub trait McpTransportFactory: Send + Sync {
    fn create<'a>(
        &'a self,
        spec: McpTransportSpec,
        cancellation: Option<CancellationToken>,
    ) -> BoxFuture<'a, Result<Arc<dyn McpTransport>, McpTransportError>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpTransportKind {
    Sdk,
    StreamableHttp,
    Sse,
    WebSocket,
    Stdio,
}

#[derive(Clone, Eq, PartialEq)]
pub enum McpTransportAuth {
    None,
    BearerToken(String),
    GoogleCredentials {
        target_audience: Option<String>,
    },
    ServiceAccountImpersonation {
        target_service_account: Option<String>,
        target_audience: Option<String>,
    },
}

impl std::fmt::Debug for McpTransportAuth {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::BearerToken(_) => formatter.write_str("BearerToken([redacted])"),
            Self::GoogleCredentials { target_audience } => formatter
                .debug_struct("GoogleCredentials")
                .field("target_audience", target_audience)
                .finish(),
            Self::ServiceAccountImpersonation {
                target_service_account,
                target_audience,
            } => formatter
                .debug_struct("ServiceAccountImpersonation")
                .field("target_service_account", target_service_account)
                .field("target_audience", target_audience)
                .finish(),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct McpTransportSpec {
    pub server_name: String,
    pub kind: McpTransportKind,
    pub endpoint: Option<String>,
    pub command: Option<String>,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub auth: McpTransportAuth,
    pub timeout_ms: u64,
    /// Streamable HTTP uses the Canopy compatibility fetch for OAuth
    /// challenge capture and the optional GET-SSE fallback normalization.
    pub compatibility_fetch: bool,
    /// Credential-bearing Agent Plugin requests must not follow redirects.
    pub stop_agent_plugin_redirects: bool,
    pub debug_mode: bool,
    pub workspace_directories: Vec<PathBuf>,
    pub oauth_config: Option<Value>,
}

impl std::fmt::Debug for McpTransportSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let env_keys = self.env.keys().collect::<Vec<_>>();
        let safe_headers = self
            .headers
            .iter()
            .map(|(name, value)| {
                (
                    name,
                    if name.eq_ignore_ascii_case("authorization")
                        || name.eq_ignore_ascii_case("cookie")
                    {
                        "[redacted]"
                    } else {
                        value.as_str()
                    },
                )
            })
            .collect::<Vec<_>>();
        formatter
            .debug_struct("McpTransportSpec")
            .field("server_name", &self.server_name)
            .field("kind", &self.kind)
            .field("endpoint", &self.endpoint)
            .field("command", &self.command)
            .field("args", &self.args)
            .field("cwd", &self.cwd)
            .field("env_keys", &env_keys)
            .field("headers", &safe_headers)
            .field("auth", &self.auth)
            .field("timeout_ms", &self.timeout_ms)
            .field("compatibility_fetch", &self.compatibility_fetch)
            .field(
                "stop_agent_plugin_redirects",
                &self.stop_agent_plugin_redirects,
            )
            .field("debug_mode", &self.debug_mode)
            .field("workspace_directories", &self.workspace_directories)
            .field(
                "oauth_config",
                &self.oauth_config.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

#[derive(Clone, Default)]
pub struct McpTransportBuildOptions {
    /// Already-sanitized parent environment, supplied by the process host.
    /// Canopy daemon secrets are removed here as a second boundary check.
    pub parent_env: BTreeMap<String, String>,
    /// A token resolved by the OAuth storage/provider layer. When OAuth is
    /// explicitly enabled, absence of a valid token is a connection error;
    /// otherwise saved credentials may still authorize a network transport.
    pub oauth_access_token: Option<String>,
    /// Resolve a token for the current server. This callback is invoked by
    /// `McpClientRuntime::connect` for OAuth-enabled servers and network
    /// transports before normalization, allowing the host to load and refresh
    /// credentials independently for each server.
    pub oauth_token_resolver: Option<Arc<dyn McpOAuthTokenResolver>>,
    /// `true` simulates Windows PATH normalization in unit tests; production
    /// callers pass `cfg!(windows)`.
    pub windows: bool,
    pub debug_mode: bool,
    /// Workspace roots advertised through the registered `roots/list`
    /// server-request handler.
    pub workspace_directories: Vec<PathBuf>,
}

/// Host-owned resolver for cached MCP OAuth credentials.
pub trait McpOAuthTokenResolver: Send + Sync {
    fn resolve_token<'a>(
        &'a self,
        server_name: &'a str,
        oauth_config: &'a Value,
    ) -> BoxFuture<'a, Result<Option<String>, String>>;
}

/// Normalize a server config to the transport choices made by
/// `createTransport` plus the WebSocket adapter recognized by the MCP pool.
/// OAuth tokens can be supplied by the per-server build-option resolver;
/// Google credential acquisition and SA token exchange remain injected at
/// the transport factory boundary.
pub fn build_mcp_transport_spec(
    server_name: &str,
    config: &Value,
    options: &McpTransportBuildOptions,
) -> Result<McpTransportSpec, McpClientError> {
    let object = config.as_object();
    let field = |name: &str| object.and_then(|value| value.get(name));
    let string = |name: &str| {
        field(name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    let timeout_ms = field("timeout")
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n > 0.0)
        .map(|n| n.min(u64::MAX as f64) as u64)
        .unwrap_or(MCP_DEFAULT_TIMEOUT_MSEC);
    let mut headers = string_map(field("headers"));
    let explicit_authorization_header = headers
        .keys()
        .any(|name| name.eq_ignore_ascii_case("authorization"));
    let auth_provider_type = string("authProviderType").unwrap_or_default();
    let oauth_enabled = field("oauth")
        .and_then(|oauth| oauth.get("enabled"))
        .is_some_and(js_truthy);

    let (kind, endpoint, command, args, cwd, env, auth) = if string("type") == Some("sdk") {
        (
            McpTransportKind::Sdk,
            None,
            None,
            Vec::new(),
            None,
            BTreeMap::new(),
            McpTransportAuth::None,
        )
    } else if auth_provider_type == "service_account_impersonation" {
        let kind = if string("httpUrl").is_some() {
            McpTransportKind::StreamableHttp
        } else if string("url").is_some() {
            McpTransportKind::Sse
        } else {
            return Err(McpClientError::TransportSetup(
                "No URL configured for ServiceAccountImpersonation MCP Server".to_owned(),
            ));
        };
        (
            kind,
            string(if kind == McpTransportKind::StreamableHttp {
                "httpUrl"
            } else {
                "url"
            })
            .map(str::to_owned),
            None,
            Vec::new(),
            None,
            BTreeMap::new(),
            McpTransportAuth::ServiceAccountImpersonation {
                target_service_account: string("targetServiceAccount").map(str::to_owned),
                target_audience: string("targetAudience").map(str::to_owned),
            },
        )
    } else if auth_provider_type == "google_credentials" {
        let kind = if string("httpUrl").is_some() {
            McpTransportKind::StreamableHttp
        } else if string("url").is_some() {
            McpTransportKind::Sse
        } else {
            return Err(McpClientError::TransportSetup(
                "No URL configured for Google Credentials MCP server".to_owned(),
            ));
        };
        (
            kind,
            string(if kind == McpTransportKind::StreamableHttp {
                "httpUrl"
            } else {
                "url"
            })
            .map(str::to_owned),
            None,
            Vec::new(),
            None,
            BTreeMap::new(),
            McpTransportAuth::GoogleCredentials {
                target_audience: string("targetAudience").map(str::to_owned),
            },
        )
    } else {
        if oauth_enabled && options.oauth_access_token.is_none() && !explicit_authorization_header {
            return Err(McpClientError::OAuthRequired {
                server_name: server_name.to_owned(),
                instruction: get_mcp_oauth_dialog_instruction("authenticate", server_name),
            });
        }
        let (kind, endpoint, command) = if let Some(url) = string("httpUrl") {
            (McpTransportKind::StreamableHttp, Some(url.to_owned()), None)
        } else if let Some(url) = string("url") {
            (McpTransportKind::Sse, Some(url.to_owned()), None)
        } else if let Some(command) = string("command") {
            (McpTransportKind::Stdio, None, Some(command.to_owned()))
        } else if let Some(url) = string("tcp") {
            (McpTransportKind::WebSocket, Some(url.to_owned()), None)
        } else {
            return Err(McpClientError::TransportSetup(
                "Invalid configuration: missing httpUrl (for Streamable HTTP), url (for SSE), and command (for stdio).".to_owned(),
            ));
        };
        let args = field("args")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let cwd = string("cwd").map(PathBuf::from);
        if kind == McpTransportKind::Stdio {
            if let Some(cwd_path) = cwd.as_ref() {
                if !cwd_path.exists() {
                    return Err(McpClientError::TransportSetup(format!(
                        "MCP server '{server_name}': configured cwd does not exist: {}",
                        cwd_path.display()
                    )));
                }
            }
        }
        let mut env = sanitize_child_env(&options.parent_env);
        if options.windows {
            normalize_windows_path_env(&mut env);
        }
        if let Some(config_env) = field("env").and_then(Value::as_object) {
            for (name, value) in config_env {
                if let Some(value) = value.as_str() {
                    env.insert(name.clone(), value.to_owned());
                }
            }
        }
        // Configured env values are user-controlled too, so apply the denylist
        // after merging them as well as to the inherited parent environment.
        env = sanitize_child_env(&env);
        let auth = options
            .oauth_access_token
            .as_ref()
            .map(|token| McpTransportAuth::BearerToken(token.clone()))
            .unwrap_or(McpTransportAuth::None);
        if let McpTransportAuth::BearerToken(token) = &auth {
            // Match source order: OAuth Authorization replaces a configured
            // Authorization header while preserving all other headers.
            insert_header_case_insensitive(
                &mut headers,
                "Authorization",
                format!("Bearer {token}"),
            );
        }
        (kind, endpoint, command, args, cwd, env, auth)
    };

    let (headers, auth) = if matches!(kind, McpTransportKind::Stdio | McpTransportKind::Sdk) {
        (BTreeMap::new(), McpTransportAuth::None)
    } else {
        (headers, auth)
    };
    Ok(McpTransportSpec {
        server_name: server_name.to_owned(),
        kind,
        compatibility_fetch: kind == McpTransportKind::StreamableHttp,
        stop_agent_plugin_redirects: field("agentPluginV1").and_then(Value::as_bool) == Some(true)
            && (!headers.is_empty()
                || headers
                    .keys()
                    .any(|key| key.eq_ignore_ascii_case("authorization"))),
        endpoint,
        command,
        args,
        cwd,
        env: if kind == McpTransportKind::Stdio {
            env
        } else {
            BTreeMap::new()
        },
        headers,
        auth,
        timeout_ms,
        debug_mode: options.debug_mode,
        workspace_directories: options.workspace_directories.clone(),
        oauth_config: field("oauth").cloned(),
    })
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn config_has_explicit_authorization_header(config: &Value) -> bool {
    config
        .get("headers")
        .and_then(Value::as_object)
        .is_some_and(|headers| {
            headers
                .keys()
                .any(|name| name.eq_ignore_ascii_case("authorization"))
        })
}

fn string_map(value: Option<&Value>) -> BTreeMap<String, String> {
    value
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|object| object.iter())
        .filter_map(|(key, value)| value.as_str().map(|value| (key.clone(), value.to_owned())))
        .collect()
}

fn insert_header_case_insensitive(
    headers: &mut BTreeMap<String, String>,
    name: &str,
    value: String,
) {
    headers.retain(|key, _| !key.eq_ignore_ascii_case(name));
    headers.insert(name.to_owned(), value);
}

fn normalize_windows_path_env(env: &mut BTreeMap<String, String>) {
    let mut path_keys = env
        .keys()
        .filter(|key| key.eq_ignore_ascii_case("path"))
        .cloned()
        .collect::<Vec<_>>();
    path_keys.sort_by(|left, right| match (left.as_str(), right.as_str()) {
        ("PATH", "PATH") => std::cmp::Ordering::Equal,
        ("PATH", _) => std::cmp::Ordering::Less,
        (_, "PATH") => std::cmp::Ordering::Greater,
        _ => left.encode_utf16().cmp(right.encode_utf16()),
    });
    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for key in &path_keys {
        if let Some(path) = env.get(key) {
            for item in path.split(';') {
                if seen.insert(item.to_owned()) {
                    values.push(item.to_owned());
                }
            }
        }
    }
    for key in path_keys {
        if key != "PATH" {
            env.remove(&key);
        }
    }
    if !values.is_empty() {
        env.insert("PATH".to_owned(), values.join(";"));
    }
}

/// Human-readable remediation text kept byte-for-byte aligned with the source
/// helper's wording.
pub fn get_mcp_oauth_dialog_instruction(action: &str, mcp_server_name: &str) -> String {
    format!(
        "In interactive Canopy Code sessions, open the /mcp dialog to {action} with MCP server '{mcp_server_name}'. For headless or SDK usage, configure MCP OAuth with canopy mcp add --oauth-* or settings.json, then {action} once in an interactive session before connecting."
    )
}

/// OAuth settings consumed by the stored-token provider. Interactive browser
/// authorization is supplied by the application layer; this function ports
/// the source provider's valid-token and refresh-token path.
#[derive(Clone, Debug, Default)]
pub struct McpOAuthTokenConfig {
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub audiences: Vec<String>,
}

/// Return an unexpired MCP OAuth token, refreshing and persisting it when the
/// source provider's five-minute expiry buffer says it is stale. Refresh
/// failures delete the stale credentials, matching `MCPOAuthProvider`.
pub async fn get_valid_mcp_oauth_token(
    storage: &impl TokenStorage,
    http: &reqwest::Client,
    server_name: &str,
    config: &McpOAuthTokenConfig,
    now_ms: i64,
) -> Result<Option<String>, McpClientError> {
    let credentials = storage
        .get_credentials(server_name)
        .await
        .map_err(|error| McpClientError::TransportSetup(error.to_string()))?;
    let Some(credentials) = credentials else {
        return Ok(None);
    };
    if !BaseTokenStorage::is_token_expired_at(&credentials, now_ms) {
        return Ok(Some(credentials.token.access_token));
    }

    let client_id = config
        .client_id
        .as_deref()
        .or(credentials.client_id.as_deref());
    let (Some(refresh_token), Some(client_id), Some(token_url)) = (
        credentials.token.refresh_token.as_deref(),
        client_id,
        credentials.token_url.as_deref(),
    ) else {
        return Ok(None);
    };

    let refreshed = refresh_mcp_oauth_token(
        http,
        config,
        refresh_token,
        client_id,
        token_url,
        credentials.mcp_server_url.as_deref(),
    )
    .await;
    let updated = match refreshed {
        Ok(response) => {
            let expires_at = response
                .expires_in
                .map(|seconds| now_ms.saturating_add(seconds.saturating_mul(1000)));
            OAuthCredentials {
                server_name: server_name.to_owned(),
                token: OAuthToken {
                    access_token: response.access_token,
                    refresh_token: response.refresh_token.or(credentials.token.refresh_token),
                    expires_at,
                    token_type: response.token_type,
                    scope: response.scope.or(credentials.token.scope),
                },
                client_id: Some(client_id.to_owned()),
                token_url: credentials.token_url,
                mcp_server_url: credentials.mcp_server_url,
                updated_at: now_ms,
            }
        }
        Err(_) => {
            storage
                .delete_credentials(server_name)
                .await
                .map_err(|error| McpClientError::TransportSetup(error.to_string()))?;
            return Ok(None);
        }
    };
    BaseTokenStorage::validate_credentials(&updated)
        .map_err(|error| McpClientError::TransportSetup(error.to_string()))?;
    let access_token = updated.token.access_token.clone();
    storage
        .set_credentials(updated)
        .await
        .map_err(|error| McpClientError::TransportSetup(error.to_string()))?;
    Ok(Some(access_token))
}

#[derive(Clone, Debug)]
struct RefreshedOAuthToken {
    access_token: String,
    token_type: String,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    scope: Option<String>,
}

async fn refresh_mcp_oauth_token(
    http: &reqwest::Client,
    config: &McpOAuthTokenConfig,
    refresh_token: &str,
    client_id: &str,
    token_url: &str,
    mcp_server_url: Option<&str>,
) -> Result<RefreshedOAuthToken, McpClientError> {
    let mut form = vec![
        ("grant_type".to_owned(), "refresh_token".to_owned()),
        ("refresh_token".to_owned(), refresh_token.to_owned()),
        ("client_id".to_owned(), client_id.to_owned()),
    ];
    if let Some(secret) = config.client_secret.as_ref() {
        form.push(("client_secret".to_owned(), secret.clone()));
    }
    if !config.scopes.is_empty() {
        form.push(("scope".to_owned(), config.scopes.join(" ")));
    }
    if !config.audiences.is_empty() {
        form.push(("audience".to_owned(), config.audiences.join(" ")));
    }
    if let Some(server_url) = mcp_server_url {
        if let Ok(resource) = OAuthUtils::build_resource_parameter(server_url) {
            form.push(("resource".to_owned(), resource));
        }
    }

    let response = http
        .post(token_url)
        .header(
            reqwest::header::ACCEPT,
            "application/json, application/x-www-form-urlencoded",
        )
        .form(&form)
        .send()
        .await
        .map_err(|error| McpClientError::Transport(error.to_string()))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| McpClientError::Transport(error.to_string()))?;
    if !status.is_success() {
        let message = String::from_utf8_lossy(&bytes[..bytes.len().min(1024)]).into_owned();
        return Err(McpClientError::HttpStatus {
            status: status.as_u16(),
            message: format!("Token refresh failed: {message}"),
            www_authenticate: None,
        });
    }
    let value = if content_type.contains("application/x-www-form-urlencoded") {
        form_response_to_json(&String::from_utf8_lossy(&bytes))
    } else {
        serde_json::from_slice::<Value>(&bytes).map_err(|error| {
            McpClientError::InvalidResponse(format!(
                "OAuth token response is invalid JSON: {error}"
            ))
        })?
    };
    let access_token = value
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            McpClientError::InvalidResponse(
                "OAuth refresh response is missing access_token".to_owned(),
            )
        })?;
    let token_type = value
        .get("token_type")
        .and_then(Value::as_str)
        .filter(|token_type| !token_type.is_empty())
        .ok_or_else(|| {
            McpClientError::InvalidResponse(
                "OAuth refresh response is missing token_type".to_owned(),
            )
        })?;
    let expires_in = match value.get("expires_in") {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => number.as_i64().filter(|seconds| *seconds >= 0),
        Some(Value::String(string)) if string.trim().bytes().all(|byte| byte.is_ascii_digit()) => {
            string.trim().parse::<i64>().ok()
        }
        Some(_) => None,
    };
    if value
        .get("expires_in")
        .is_some_and(|value| !value.is_null())
        && expires_in.is_none()
    {
        return Err(McpClientError::InvalidResponse(
            "OAuth refresh response has invalid expires_in".to_owned(),
        ));
    }
    Ok(RefreshedOAuthToken {
        access_token: access_token.to_owned(),
        token_type: token_type.to_owned(),
        refresh_token: value
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_owned),
        expires_in,
        scope: value
            .get("scope")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn form_response_to_json(value: &str) -> Value {
    let mut object = Map::new();
    for pair in value.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        object.insert(
            percent_decode_form_component(key),
            Value::String(percent_decode_form_component(value)),
        );
    }
    Value::Object(object)
}

fn percent_decode_form_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = (bytes[index + 1] as char).to_digit(16);
                let low = (bytes[index + 2] as char).to_digit(16);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push(((high << 4) | low) as u8);
                    index += 3;
                } else {
                    decoded.push(bytes[index]);
                    index += 1;
                }
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct McpTool {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
    pub annotations: Option<Value>,
    pub raw: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DiscoveredMcpTool {
    pub server_name: String,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub annotations: Option<Value>,
    pub trust: Option<bool>,
    pub always_load: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct McpPrompt {
    pub server_name: String,
    pub name: String,
    pub description: Option<String>,
    pub arguments: Vec<Value>,
    pub raw: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct McpResource {
    pub server_name: String,
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct McpDiscoverySnapshot {
    pub tools: Vec<DiscoveredMcpTool>,
    pub prompts: Vec<McpPrompt>,
    pub resources: Vec<McpResource>,
}

#[derive(Clone, Default)]
pub struct McpRequestOptions {
    pub timeout_ms: Option<u64>,
    pub cancellation: Option<CancellationToken>,
    /// Optional synchronous hook called immediately before the transport
    /// request future is first polled. Hosts can use this to distinguish a
    /// cancelled operation that never reached transport dispatch from one
    /// whose remote side effect may already have started.
    pub on_request_started: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Optional hook invoked after a valid JSON-RPC response is received.
    /// This lets a host distinguish an interrupted request from one whose
    /// response already completed before cancellation.
    pub on_request_completed: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// A single connected MCP server client. Concurrent request multiplexing is
/// delegated to the transport adapter (as it is to the SDK `Client` in the
/// TypeScript source); request IDs, envelopes, validation, and lifecycle are
/// owned here.
pub struct McpClientRuntime {
    server_name: String,
    config: Value,
    factory: Arc<dyn McpTransportFactory>,
    status: Arc<AtomicU8>,
    is_disconnecting: Arc<AtomicBool>,
    request_id: AtomicU64,
    transport: RwLock<Option<Arc<dyn McpTransport>>>,
    lifecycle: AsyncMutex<()>,
    last_transport_error: Arc<RwLock<Option<McpClientError>>>,
    instructions: RwLock<Option<String>>,
    server_info: RwLock<Option<Value>>,
    server_capabilities: RwLock<Option<Value>>,
}

impl McpClientRuntime {
    pub fn new(
        server_name: impl Into<String>,
        config: Value,
        factory: Arc<dyn McpTransportFactory>,
    ) -> Self {
        Self {
            server_name: server_name.into(),
            config,
            factory,
            status: Arc::new(AtomicU8::new(StatusByte::Disconnected as u8)),
            is_disconnecting: Arc::new(AtomicBool::new(false)),
            request_id: AtomicU64::new(1),
            transport: RwLock::new(None),
            lifecycle: AsyncMutex::new(()),
            last_transport_error: Arc::new(RwLock::new(None)),
            instructions: RwLock::new(None),
            server_info: RwLock::new(None),
            server_capabilities: RwLock::new(None),
        }
    }

    pub fn status(&self) -> McpClientStatus {
        match self.status.load(Ordering::Acquire) {
            1 => McpClientStatus::Connecting,
            2 => McpClientStatus::Connected,
            _ => McpClientStatus::Disconnected,
        }
    }

    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    pub fn get_instructions(&self) -> Option<String> {
        self.instructions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn get_server_info(&self) -> Option<Value> {
        self.server_info
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn get_server_capabilities(&self) -> Option<Value> {
        self.server_capabilities
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn get_transport_pid(&self) -> Option<u32> {
        self.transport
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(|transport| transport.pid())
    }

    pub fn get_last_transport_error(&self) -> Option<McpClientError> {
        self.last_transport_error
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Set `DISCONNECTED` before publishing the last error, so a pool's
    /// synchronous status observer can read the upstream cause immediately.
    pub fn report_transport_error(&self, error: McpClientError) {
        if self.is_disconnecting.load(Ordering::Acquire) {
            return;
        }
        *self
            .last_transport_error
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(error);
        self.update_status(McpClientStatus::Disconnected);
    }

    pub async fn connect(
        &self,
        options: &McpTransportBuildOptions,
        cancellation: Option<CancellationToken>,
    ) -> Result<(), McpClientError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.is_disconnecting.store(false, Ordering::Release);
        *self
            .last_transport_error
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
        *self
            .instructions
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
        *self
            .server_info
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
        *self
            .server_capabilities
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.update_status(McpClientStatus::Connecting);

        let mut resolved_options = options.clone();
        let oauth_config = self.config.get("oauth");
        let oauth_enabled = oauth_config
            .and_then(|oauth| oauth.get("enabled"))
            .is_some_and(js_truthy);
        let network_transport = has_network_transport(&self.config);
        if resolved_options.oauth_access_token.is_none()
            && (oauth_enabled || network_transport)
            && supports_mcp_oauth(&self.config)
            && !config_has_explicit_authorization_header(&self.config)
            && let Some(resolver) = resolved_options.oauth_token_resolver.clone()
        {
            match resolver
                .resolve_token(&self.server_name, oauth_config.unwrap_or(&Value::Null))
                .await
            {
                Ok(token) => resolved_options.oauth_access_token = token,
                Err(error) => {
                    self.update_status(McpClientStatus::Disconnected);
                    return Err(McpClientError::TransportSetup(format!(
                        "could not resolve OAuth credentials for MCP server '{}': {error}",
                        self.server_name
                    )));
                }
            }
        }

        let spec =
            match build_mcp_transport_spec(&self.server_name, &self.config, &resolved_options) {
                Ok(spec) => spec,
                Err(error) => {
                    self.update_status(McpClientStatus::Disconnected);
                    return Err(error);
                }
            };
        let timeout_ms = spec.timeout_ms;
        let transport = match self
            .run_bounded(
                "transport connect",
                timeout_ms,
                cancellation.clone(),
                self.factory.create(spec, cancellation.clone()),
            )
            .await
        {
            Ok(transport) => transport,
            Err(error) => {
                self.update_status(McpClientStatus::Disconnected);
                return Err(error);
            }
        };
        let directories = resolved_options.workspace_directories.clone();
        transport.set_server_request_handler(Arc::new(move |request| {
            handle_server_request(request, &directories)
        }));
        let status = Arc::clone(&self.status);
        let is_disconnecting = Arc::clone(&self.is_disconnecting);
        let last_transport_error = Arc::clone(&self.last_transport_error);
        let server_name = self.server_name.clone();
        transport.set_error_handler(Arc::new(move |error| {
            if is_disconnecting.load(Ordering::Acquire) {
                return;
            }
            *last_transport_error
                .write()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(error.into());
            status.store(StatusByte::Disconnected as u8, Ordering::Release);
            mcp_server_status_registry().update(&server_name, McpClientStatus::Disconnected);
        }));
        *self
            .transport
            .write()
            .unwrap_or_else(|error| error.into_inner()) = Some(transport);

        let result = async {
            let initialize = self
                .request_raw(
                    "initialize",
                    json!({
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": { "roots": {} },
                        "clientInfo": {
                            "name": format!("canopy-cli-mcp-client-{}", self.server_name),
                            "version": "0.0.1"
                        }
                    }),
                    McpRequestOptions {
                        timeout_ms: Some(timeout_ms),
                        cancellation: cancellation.clone(),
                        ..McpRequestOptions::default()
                    },
                )
                .await?;
            let negotiated_version = initialize
                .get("protocolVersion")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    McpClientError::InvalidResponse(
                        "initialize result is missing protocolVersion".to_owned(),
                    )
                })?;
            if !MCP_SUPPORTED_PROTOCOL_VERSIONS.contains(&negotiated_version) {
                return Err(McpClientError::InvalidResponse(format!(
                    "server selected unsupported protocol version '{negotiated_version}'"
                )));
            }
            self.transport()?.set_protocol_version(negotiated_version);
            let capabilities = initialize.get("capabilities").ok_or_else(|| {
                McpClientError::InvalidResponse(
                    "initialize result is missing capabilities".to_owned(),
                )
            })?;
            if !capabilities.is_object() {
                return Err(McpClientError::InvalidResponse(
                    "initialize capabilities must be an object".to_owned(),
                ));
            }
            *self
                .server_capabilities
                .write()
                .unwrap_or_else(|error| error.into_inner()) = Some(capabilities.clone());
            *self
                .server_info
                .write()
                .unwrap_or_else(|error| error.into_inner()) = initialize.get("serverInfo").cloned();
            *self
                .instructions
                .write()
                .unwrap_or_else(|error| error.into_inner()) = initialize
                .get("instructions")
                .and_then(Value::as_str)
                .map(str::to_owned);
            self.notify(
                "notifications/initialized",
                Value::Null,
                cancellation.clone(),
                timeout_ms,
            )
            .await?;
            Ok::<(), McpClientError>(())
        }
        .await;

        match result {
            Ok(()) => {
                self.update_status(McpClientStatus::Connected);
                Ok(())
            }
            Err(error) => {
                self.update_status(McpClientStatus::Disconnected);
                let transport = self
                    .transport
                    .write()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .take();
                if let Some(transport) = transport {
                    let _ = transport.close().await;
                }
                Err(error)
            }
        }
    }

    /// Intentionally publishes DISCONNECTED before teardown, matching the
    /// source client's observable ordering for in-flight discovery callers.
    pub async fn disconnect(&self) -> Result<(), McpClientError> {
        let _lifecycle = self.lifecycle.lock().await;
        self.update_status(McpClientStatus::Disconnected);
        self.is_disconnecting.store(true, Ordering::Release);
        let transport = self
            .transport
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        *self
            .instructions
            .write()
            .unwrap_or_else(|error| error.into_inner()) = None;
        if let Some(transport) = transport {
            transport.close().await.map_err(McpClientError::from)?;
        }
        Ok(())
    }

    pub async fn request_raw(
        &self,
        method: &str,
        params: Value,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        let transport = self.transport()?;
        let id = self.request_id.fetch_add(1, Ordering::Relaxed);
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let timeout_ms = options
            .timeout_ms
            .unwrap_or_else(|| timeout_from_config(&self.config));
        let on_request_started = options.on_request_started;
        let on_request_completed = options.on_request_completed;
        let mut operation = transport.request(request, options.cancellation.clone());
        let operation = async move {
            let mut started = false;
            std::future::poll_fn(move |context| {
                if !started {
                    started = true;
                    if let Some(on_request_started) = on_request_started.as_ref() {
                        on_request_started();
                    }
                }
                operation.as_mut().poll(context)
            })
            .await
        };
        let response = self
            .run_bounded(method, timeout_ms, options.cancellation.clone(), operation)
            .await
            .map_err(|error| {
                if matches!(
                    error,
                    McpClientError::Transport(_) | McpClientError::HttpStatus { .. }
                ) {
                    self.report_transport_error(error.clone());
                }
                error
            })?;
        let response = parse_json_rpc_response(response, id);
        if response.is_ok()
            && let Some(on_request_completed) = on_request_completed.as_ref()
        {
            on_request_completed();
        }
        response
    }

    pub async fn call_tool(
        &self,
        name: &str,
        arguments: &Map<String, Value>,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        self.request_raw(
            "tools/call",
            json!({"name": name, "arguments": arguments}),
            options,
        )
        .await
    }

    /// Requests tools directly and preserves MCP annotations. An undeclared
    /// `tools` capability is not checked; the source always attempts the
    /// request and lets JSON-RPC `-32601` describe a missing method.
    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpClientError> {
        let response = self
            .request_with_list_retry("tools/list", json!({}), None)
            .await?;
        parse_tool_list(response)
    }

    pub async fn list_mcp_prompts(&self) -> Vec<McpPrompt> {
        let response = self
            .request_with_list_retry("prompts/list", json!({}), None)
            .await;
        response
            .and_then(|value| parse_prompt_list(&self.server_name, value))
            .unwrap_or_default()
    }

    pub async fn get_prompt(
        &self,
        prompt_name: &str,
        arguments: &Map<String, Value>,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        self.request_raw(
            "prompts/get",
            json!({"name": prompt_name, "arguments": arguments}),
            options,
        )
        .await
    }

    pub async fn list_mcp_resources(&self) -> Vec<McpResource> {
        let response = self
            .request_with_list_retry("resources/list", json!({}), None)
            .await;
        response
            .and_then(|value| parse_resource_list(&self.server_name, value))
            .unwrap_or_default()
    }

    /// As in the TS client, this makes a raw request without prechecking the
    /// server's declared resources capability.
    pub async fn read_resource(
        &self,
        uri: &str,
        options: McpRequestOptions,
    ) -> Result<Value, McpClientError> {
        self.request_raw("resources/read", json!({"uri": uri}), options)
            .await
    }

    /// Read and format a resource using the Rust port of the source's bounded
    /// resource-content formatter. The raw [`read_resource`] method remains
    /// available to callers that need the full protocol response.
    pub async fn read_resource_for_context(
        &self,
        uri: &str,
        label: &str,
        options: McpRequestOptions,
        format_options: Option<FormatMcpResourceOptions>,
    ) -> Result<FormattedMcpResource, McpClientError> {
        let response = self.read_resource(uri, options).await?;
        format_mcp_resource_contents(&response, label, format_options)
            .map_err(|error| McpClientError::InvalidResponse(error.to_string()))
    }

    /// Lenient tool discovery matching `discoverTools`: malformed/missing
    /// names are skipped, config filtering can be disabled for shared pool
    /// snapshots, and per-tool schema gaps get an empty-object schema.
    pub async fn discover_tools(&self, apply_config_filters: bool) -> Vec<DiscoveredMcpTool> {
        let Ok(tools) = self.list_tools().await else {
            return Vec::new();
        };
        tools
            .into_iter()
            .filter(|tool| !apply_config_filters || is_enabled(&tool.name, &self.config))
            .map(|tool| DiscoveredMcpTool {
                server_name: self.server_name.clone(),
                name: tool.name,
                description: tool.description.unwrap_or_default(),
                input_schema: tool.input_schema,
                annotations: tool.annotations,
                trust: if apply_config_filters {
                    self.config.get("trust").and_then(Value::as_bool)
                } else {
                    None
                },
                always_load: self.config.get("alwaysLoadTools").and_then(Value::as_bool)
                    == Some(true),
            })
            .collect()
    }

    /// Independent discovery requests run concurrently. Tool, prompt, and
    /// resource listing errors are intentionally swallowed into empty lists;
    /// only a server that exposes no content at all fails the snapshot.
    pub async fn discover_and_return(
        &self,
        apply_config_filters: bool,
    ) -> Result<McpDiscoverySnapshot, McpClientError> {
        if self.status() != McpClientStatus::Connected {
            return Err(McpClientError::NotConnected);
        }
        let (prompts, resources, tools) = tokio::join!(
            self.list_mcp_prompts(),
            self.list_mcp_resources(),
            self.discover_tools(apply_config_filters),
        );
        let snapshot = McpDiscoverySnapshot {
            tools,
            prompts,
            resources,
        };
        if snapshot.tools.is_empty() && snapshot.prompts.is_empty() && snapshot.resources.is_empty()
        {
            return Err(McpClientError::NoDiscoverableContent);
        }
        Ok(snapshot)
    }

    async fn request_with_list_retry(
        &self,
        method: &str,
        params: Value,
        cancellation: Option<CancellationToken>,
    ) -> Result<Value, McpClientError> {
        match retry_with_backoff(
            || {
                self.request_raw(
                    method,
                    params.clone(),
                    McpRequestOptions {
                        timeout_ms: None,
                        cancellation: cancellation.clone(),
                        ..McpRequestOptions::default()
                    },
                )
            },
            McpRetryOptions::default(),
            cancellation.as_ref(),
            McpClientError::is_transient,
        )
        .await
        {
            Ok(value) => Ok(value),
            Err(McpRetryError::Operation(error)) => Err(error),
            Err(McpRetryError::Aborted) => Err(McpClientError::Cancelled),
        }
    }

    async fn notify(
        &self,
        method: &str,
        params: Value,
        cancellation: Option<CancellationToken>,
        timeout_ms: u64,
    ) -> Result<(), McpClientError> {
        let transport = self.transport()?;
        let mut notification = json!({
            "jsonrpc": "2.0",
            "method": method,
        });
        if !params.is_null() {
            notification["params"] = params;
        }
        self.run_bounded(
            method,
            timeout_ms,
            cancellation.clone(),
            transport.notify(notification, cancellation),
        )
        .await
    }

    fn transport(&self) -> Result<Arc<dyn McpTransport>, McpClientError> {
        self.transport
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or(McpClientError::NotConnected)
    }

    fn update_status(&self, status: McpClientStatus) {
        let byte = match status {
            McpClientStatus::Disconnected => StatusByte::Disconnected as u8,
            McpClientStatus::Connecting => StatusByte::Connecting as u8,
            McpClientStatus::Connected => StatusByte::Connected as u8,
        };
        self.status.store(byte, Ordering::Release);
        if !self.is_disconnecting.load(Ordering::Acquire) {
            mcp_server_status_registry().update(&self.server_name, status);
        }
    }

    async fn run_bounded<T, F>(
        &self,
        method: &str,
        timeout_ms: u64,
        cancellation: Option<CancellationToken>,
        operation: F,
    ) -> Result<T, McpClientError>
    where
        F: Future<Output = Result<T, McpTransportError>> + Send,
        T: Send,
    {
        let timeout = tokio::time::sleep(Duration::from_millis(timeout_ms));
        tokio::pin!(timeout);
        tokio::select! {
            biased;
            _ = wait_for_cancellation(cancellation.clone()) => Err(McpClientError::Cancelled),
            result = operation => result.map_err(McpClientError::from),
            _ = &mut timeout => Err(McpClientError::Timeout {
                method: method.to_owned(),
                timeout_ms,
            }),
        }
    }
}

async fn wait_for_cancellation(cancellation: Option<CancellationToken>) {
    if let Some(cancellation) = cancellation {
        let _ = cancellation.cancelled().await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn timeout_from_config(config: &Value) -> u64 {
    config
        .get("timeout")
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n > 0.0)
        .map(|n| n.min(u64::MAX as f64) as u64)
        .unwrap_or(MCP_DEFAULT_TIMEOUT_MSEC)
}

fn parse_json_rpc_response(mut response: Value, expected_id: u64) -> Result<Value, McpClientError> {
    if response.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(McpClientError::InvalidResponse(
            "JSON-RPC response must have jsonrpc='2.0'".to_owned(),
        ));
    }
    if response.get("id") != Some(&json!(expected_id)) {
        return Err(McpClientError::InvalidResponse(format!(
            "JSON-RPC response id did not match request id {expected_id}"
        )));
    }
    if let Some(error) = response.get("error") {
        let code = error.get("code").and_then(Value::as_i64).ok_or_else(|| {
            McpClientError::InvalidResponse("JSON-RPC error is missing an integer code".to_owned())
        })?;
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                McpClientError::InvalidResponse("JSON-RPC error is missing a message".to_owned())
            })?;
        return Err(McpClientError::JsonRpc {
            code,
            message: message.to_owned(),
            data: error.get("data").cloned(),
        });
    }
    response
        .as_object_mut()
        .and_then(|object| object.remove("result"))
        .ok_or_else(|| {
            McpClientError::InvalidResponse("JSON-RPC response is missing result".to_owned())
        })
}

fn parse_tool_list(response: Value) -> Result<Vec<McpTool>, McpClientError> {
    let tools = response
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpClientError::InvalidResponse(
                "tools/list result must contain a tools array".to_owned(),
            )
        })?;
    Ok(tools
        .iter()
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?;
            Some(McpTool {
                name: name.to_owned(),
                description: tool
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                input_schema: tool
                    .get("inputSchema")
                    .cloned()
                    .unwrap_or_else(|| json!({"type":"object","properties":{}})),
                annotations: tool.get("annotations").cloned(),
                raw: tool.clone(),
            })
        })
        .collect::<Vec<_>>())
}

fn parse_prompt_list(server_name: &str, response: Value) -> Result<Vec<McpPrompt>, McpClientError> {
    let prompts = response
        .get("prompts")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpClientError::InvalidResponse(
                "prompts/list result must contain a prompts array".to_owned(),
            )
        })?;
    prompts
        .iter()
        .map(|prompt| {
            let name = prompt.get("name").and_then(Value::as_str).ok_or_else(|| {
                McpClientError::InvalidResponse("prompt is missing a string name".to_owned())
            })?;
            Ok(McpPrompt {
                server_name: server_name.to_owned(),
                name: name.to_owned(),
                description: prompt
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                arguments: prompt
                    .get("arguments")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default(),
                raw: prompt.clone(),
            })
        })
        .collect()
}

fn parse_resource_list(
    server_name: &str,
    response: Value,
) -> Result<Vec<McpResource>, McpClientError> {
    let resources = response
        .get("resources")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpClientError::InvalidResponse(
                "resources/list result must contain a resources array".to_owned(),
            )
        })?;
    resources
        .iter()
        .map(|resource| {
            let uri = resource.get("uri").and_then(Value::as_str).ok_or_else(|| {
                McpClientError::InvalidResponse("resource is missing a string uri".to_owned())
            })?;
            let name = resource
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    McpClientError::InvalidResponse("resource is missing a string name".to_owned())
                })?;
            Ok(McpResource {
                server_name: server_name.to_owned(),
                uri: uri.to_owned(),
                name: name.to_owned(),
                description: resource
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                mime_type: resource
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                raw: resource.clone(),
            })
        })
        .collect()
}

/// `isEnabled` parity: excludes win; an absent include list allows all tools;
/// include suffixes such as `search(query)` match the base tool name.
pub fn is_enabled(tool_name: &str, config: &Value) -> bool {
    let excludes = config
        .get("excludeTools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    if excludes.into_iter().any(|name| name == tool_name) {
        return false;
    }
    let Some(include) = config.get("includeTools") else {
        return true;
    };
    if include.is_null() {
        return true;
    }
    include
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .any(|name| name == tool_name || name.starts_with(&format!("{tool_name}(")))
}

/// Build the Roots result for a server-originated `roots/list` request. The
/// transport adapter calls this for incoming client-side requests.
pub fn handle_roots_list_request(request: &Value, directories: &[PathBuf]) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let roots = directories
        .iter()
        .filter_map(|directory| {
            let path = if directory.is_absolute() {
                directory.clone()
            } else {
                std::env::current_dir().ok()?.join(directory)
            };
            let uri = Url::from_file_path(&path).ok()?.to_string();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            Some(json!({"uri":uri,"name":name}))
        })
        .collect::<Vec<_>>();
    json!({"jsonrpc":"2.0","id":id,"result":{"roots":roots}})
}

/// Make the compatibility-fetch decision made for an optional Streamable
/// HTTP GET SSE request. The SDK-native 405 sentinel and resumable requests
/// carrying Last-Event-ID are left alone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamableHttpFallback {
    pub status: u16,
    pub status_text: &'static str,
    pub diagnostic_excerpt: Option<String>,
}

pub fn streamable_http_get_sse_fallback(
    method: Option<&str>,
    headers: &[(String, String)],
    status: u16,
    body: &[u8],
) -> Option<StreamableHttpFallback> {
    if status != 400 {
        return None;
    }
    let method = method.unwrap_or("GET");
    if !method.eq_ignore_ascii_case("GET") {
        return None;
    }
    let mut accept = None;
    let mut last_event_id = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("last-event-id") {
            last_event_id = true;
        } else if name.eq_ignore_ascii_case("accept") {
            accept = Some(value.as_str());
        }
    }
    if last_event_id {
        return None;
    }
    let wants_sse = accept.unwrap_or_default().split(',').any(|value| {
        value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("text/event-stream")
    });
    if !wants_sse {
        return None;
    }
    let bytes = &body[..body.len().min(STREAMABLE_HTTP_GET_SSE_ERROR_BODY_LIMIT)];
    let excerpt = String::from_utf8_lossy(bytes).trim().to_owned();
    let diagnostic_excerpt = if excerpt.is_empty() {
        None
    } else if body.len() > STREAMABLE_HTTP_GET_SSE_ERROR_BODY_LIMIT {
        Some(format!(
            "{}...",
            excerpt
                .chars()
                .take(STREAMABLE_HTTP_GET_SSE_ERROR_BODY_LIMIT)
                .collect::<String>()
        ))
    } else {
        Some(excerpt)
    };
    Some(StreamableHttpFallback {
        status: 405,
        status_text: "Method Not Allowed",
        diagnostic_excerpt,
    })
}

/// Extract a case-insensitive WWW-Authenticate field from a header map.
pub fn www_authenticate_header(headers: &BTreeMap<String, String>) -> Option<String> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("www-authenticate"))
        .map(|(_, value)| value.clone())
}

/// Map the lower-level connection failure to the concise source diagnostics.
pub fn map_mcp_connect_error(
    server_name: &str,
    error: &McpClientError,
    sandbox_enabled: bool,
) -> String {
    let error_message = error.to_string();
    let is_network_error =
        error_message.contains("ENOTFOUND") || error_message.contains("ECONNREFUSED");
    let mut concise = if is_network_error {
        format!("Cannot connect to '{server_name}' - server may be down or URL incorrect")
    } else {
        format!("Connection failed for '{server_name}': {error_message}")
    };
    if sandbox_enabled {
        concise.push_str(" (check sandbox availability)");
    }
    concise
}

/// Settings and challenge state for the source's OAuth recovery probe.
/// Actual token storage/browser auth stays in the OAuth provider layer; the
/// injected probe and recovery callback make network and UI behavior testable.
#[derive(Default)]
pub struct McpOAuthRecoveryState {
    state: Mutex<OAuthStateInner>,
    probe_gates: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    automatic_oauth_gate: AsyncMutex<()>,
}

#[derive(Default)]
struct OAuthStateInner {
    requirements: HashMap<String, String>,
    probe_candidates: HashSet<String>,
    probe_versions: HashMap<String, u64>,
    server_requires_oauth: HashSet<String>,
}

impl McpOAuthRecoveryState {
    pub fn server_requires_oauth(&self, server_name: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .server_requires_oauth
            .contains(server_name)
    }

    pub fn challenge(&self, server_name: &str, config: &Value) -> Option<String> {
        let key = oauth_recovery_key(server_name, config);
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .requirements
            .get(&key)
            .cloned()
    }

    pub fn record_connect_error(
        &self,
        server_name: &str,
        config: &Value,
        error: &McpClientError,
    ) -> bool {
        if !supports_mcp_oauth(config) || !has_network_transport(config) {
            return false;
        }
        let is_401 = error.http_status() == Some(401) || error.to_string().contains("401");
        if is_401 {
            let challenge = match error {
                McpClientError::HttpStatus {
                    www_authenticate, ..
                } => www_authenticate.clone().unwrap_or_default(),
                _ => extract_www_authenticate_from_message(&error.to_string()).unwrap_or_default(),
            };
            set_oauth_requirement(
                &mut self.state.lock().unwrap_or_else(|e| e.into_inner()),
                server_name,
                config,
                challenge,
            );
            true
        } else {
            self.state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .probe_candidates
                .insert(oauth_recovery_key(server_name, config));
            false
        }
    }

    pub fn clear(&self, server_name: &str, config: &Value) {
        let key = oauth_recovery_key(server_name, config);
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let version = state.probe_versions.entry(key.clone()).or_default();
        *version = version.saturating_add(1);
        state.probe_candidates.remove(&key);
        state.requirements.remove(&key);
        let still_required = state
            .requirements
            .keys()
            .any(|requirement| requirement.starts_with(&format!("{server_name}\0")));
        if !still_required {
            state.server_requires_oauth.remove(server_name);
        }
    }

    /// HEAD probe with a 5s bound. Per-key locking coalesces concurrent probes
    /// while URL-specific versioning prevents stale results from restoring a
    /// requirement after `clear()`.
    pub async fn probe(
        &self,
        server_name: &str,
        config: &Value,
        probe: &dyn McpOAuthProbe,
    ) -> bool {
        if !supports_mcp_oauth(config) || !has_network_transport(config) {
            return false;
        }
        let key = oauth_recovery_key(server_name, config);
        let gate = {
            let mut gates = self.probe_gates.lock().unwrap_or_else(|e| e.into_inner());
            gates
                .entry(key.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        let _guard = gate.lock().await;
        let probe_version = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.requirements.contains_key(&key) {
                return true;
            }
            if !state.probe_candidates.contains(&key) {
                return false;
            }
            *state.probe_versions.get(&key).unwrap_or(&0)
        };
        let Some(url) = config
            .get("httpUrl")
            .and_then(Value::as_str)
            .filter(|url| !url.is_empty())
            .or_else(|| {
                config
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|url| !url.is_empty())
            })
        else {
            return false;
        };
        let mut headers = string_map(config.get("headers"));
        insert_header_case_insensitive(
            &mut headers,
            "Accept",
            if config.get("httpUrl").and_then(Value::as_str).is_some() {
                "application/json".to_owned()
            } else {
                "text/event-stream".to_owned()
            },
        );
        let response = tokio::time::timeout(
            Duration::from_millis(MCP_OAUTH_PROBE_TIMEOUT_MS),
            probe.head(url, headers),
        )
        .await;
        if let Ok(Ok(response)) = response {
            if response.status == 401 {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.probe_versions.get(&key).copied().unwrap_or(0) != probe_version {
                    return false;
                }
                set_oauth_requirement(
                    &mut state,
                    server_name,
                    config,
                    response.www_authenticate.unwrap_or_default(),
                );
                return true;
            }
        }
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .probe_candidates
            .remove(&key);
        false
    }

    /// Serialize interactive browser auth across servers and leave recovery
    /// running after the source-compatible 60s wait timeout.
    pub async fn attempt_automatic<F, Fut>(
        self: &Arc<Self>,
        server_name: &str,
        config: &Value,
        allow_browser_launch: bool,
        recovery: F,
    ) -> bool
    where
        F: FnOnce(String) -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send + 'static,
    {
        if !allow_browser_launch
            || !supports_mcp_oauth(config)
            || (!has_http_url(config) && !oauth_enabled(config))
        {
            return false;
        }
        let has_challenge = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .requirements
                .contains_key(&oauth_recovery_key(server_name, config))
        };
        if !has_challenge {
            return false;
        }
        let _guard = self.automatic_oauth_gate.lock().await;
        let Some(challenge) = self.challenge(server_name, config) else {
            return false;
        };
        let task = tokio::spawn(async move { recovery(challenge).await });
        match tokio::time::timeout(Duration::from_millis(MCP_AUTOMATIC_OAUTH_TIMEOUT_MS), task)
            .await
        {
            Ok(Ok(succeeded)) => succeeded,
            Ok(Err(_)) | Err(_) => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthProbeResponse {
    pub status: u16,
    pub www_authenticate: Option<String>,
}

pub trait McpOAuthProbe: Send + Sync {
    fn head<'a>(
        &'a self,
        url: &'a str,
        headers: BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<McpOAuthProbeResponse, String>>;
}

fn oauth_recovery_key(server_name: &str, config: &Value) -> String {
    let url = config
        .get("httpUrl")
        .and_then(Value::as_str)
        .or_else(|| config.get("url").and_then(Value::as_str))
        .unwrap_or_default();
    format!("{server_name}\0{url}")
}

fn supports_mcp_oauth(config: &Value) -> bool {
    config
        .get("authProviderType")
        .and_then(Value::as_str)
        .is_none_or(|provider| provider == "dynamic_discovery")
}

fn has_network_transport(config: &Value) -> bool {
    ["httpUrl", "url"].iter().any(|field| {
        config
            .get(*field)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty())
    })
}

fn has_http_url(config: &Value) -> bool {
    config
        .get("httpUrl")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty())
}

fn oauth_enabled(config: &Value) -> bool {
    config
        .get("oauth")
        .and_then(|oauth| oauth.get("enabled"))
        .is_some_and(js_truthy)
}

fn set_oauth_requirement(
    state: &mut OAuthStateInner,
    server_name: &str,
    config: &Value,
    challenge: String,
) {
    let key = oauth_recovery_key(server_name, config);
    state.probe_candidates.remove(&key);
    let old_challenge = state.requirements.get(&key).cloned().unwrap_or_default();
    state.requirements.insert(
        key,
        if challenge.is_empty() {
            old_challenge
        } else {
            challenge
        },
    );
    state.server_requires_oauth.insert(server_name.to_owned());
}

fn extract_www_authenticate_from_message(message: &str) -> Option<String> {
    for line in message.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("www-authenticate") {
                return Some(value.trim().to_owned());
            }
        }
    }
    None
}

/// JSON-RPC client-side `roots/list` handler. This is separate from client
/// requests because the MCP server initiates this request during operation.
pub fn handle_server_request(request: &Value, workspace_directories: &[PathBuf]) -> Value {
    if request.get("method").and_then(Value::as_str) == Some("roots/list") {
        return handle_roots_list_request(request, workspace_directories);
    }
    json!({
        "jsonrpc":"2.0",
        "id":request.get("id").cloned().unwrap_or(Value::Null),
        "error":{"code":-32601,"message":"Method not found"}
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FakeTransport {
        responses: Mutex<VecDeque<Result<Value, McpTransportError>>>,
        requests: Mutex<Vec<Value>>,
        notifications: Mutex<Vec<Value>>,
        close_count: AtomicUsize,
    }

    impl FakeTransport {
        fn new(responses: impl IntoIterator<Item = Result<Value, McpTransportError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
                notifications: Mutex::new(Vec::new()),
                close_count: AtomicUsize::new(0),
            }
        }
    }

    impl McpTransport for FakeTransport {
        fn request<'a>(
            &'a self,
            request: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Value, McpTransportError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| {
                        Err(McpTransportError::Transport(
                            "missing fake response".to_owned(),
                        ))
                    })
            })
        }

        fn notify<'a>(
            &'a self,
            notification: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async move {
                self.notifications.lock().unwrap().push(notification);
                Ok(())
            })
        }

        fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async move {
                self.close_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        }

        fn pid(&self) -> Option<u32> {
            Some(321)
        }
    }

    struct FakeFactory(Arc<FakeTransport>);

    impl McpTransportFactory for FakeFactory {
        fn create<'a>(
            &'a self,
            _spec: McpTransportSpec,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Arc<dyn McpTransport>, McpTransportError>> {
            Box::pin(async move { Ok(self.0.clone() as Arc<dyn McpTransport>) })
        }
    }

    fn response(id: u64, result: Value) -> Result<Value, McpTransportError> {
        Ok(json!({"jsonrpc":"2.0","id":id,"result":result}))
    }

    fn initialize_response(instructions: Option<&str>) -> Value {
        let mut result = json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "serverInfo": {"name":"test-server","version":"1"}
        });
        if let Some(instructions) = instructions {
            result["instructions"] = json!(instructions);
        }
        result
    }

    fn build_options() -> McpTransportBuildOptions {
        McpTransportBuildOptions::default()
    }

    #[tokio::test]
    async fn connect_initializes_roots_and_stores_instructions_then_disconnects_once() {
        let transport = Arc::new(FakeTransport::new([response(
            1,
            initialize_response(Some("Use concise replies.")),
        )]));
        let factory = Arc::new(FakeFactory(transport.clone()));
        let client = McpClientRuntime::new("docs", json!({"command":"mcp"}), factory);
        client.connect(&build_options(), None).await.unwrap();
        assert_eq!(client.status(), McpClientStatus::Connected);
        assert_eq!(
            client.get_instructions().as_deref(),
            Some("Use concise replies.")
        );
        assert_eq!(client.get_transport_pid(), Some(321));
        let requests = transport.requests.lock().unwrap().clone();
        let notifications = transport.notifications.lock().unwrap().clone();
        assert_eq!(requests[0]["method"], "initialize");
        assert_eq!(requests[0]["params"]["capabilities"]["roots"], json!({}));
        assert_eq!(notifications[0]["method"], "notifications/initialized");
        client.disconnect().await.unwrap();
        assert_eq!(client.status(), McpClientStatus::Disconnected);
        assert_eq!(client.get_instructions(), None);
        assert_eq!(transport.close_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn discovery_is_lenient_about_undeclared_capabilities_and_allows_resource_only_servers() {
        let transport = Arc::new(FakeTransport::new([
            response(1, initialize_response(None)),
            response(2, json!({"prompts":[]})),
            response(3, json!({"resources":[{"uri":"file:///x","name":"only"}]})),
            response(4, json!({"tools":[]})),
        ]));
        let client = McpClientRuntime::new(
            "resources",
            json!({"httpUrl":"https://mcp"}),
            Arc::new(FakeFactory(transport.clone())),
        );
        client.connect(&build_options(), None).await.unwrap();
        let snapshot = client.discover_and_return(true).await.unwrap();
        assert_eq!(snapshot.resources.len(), 1);
        let methods = transport
            .requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request["method"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert!(
            methods
                .iter()
                .any(|method| method.as_str() == "prompts/list")
        );
        assert!(
            methods
                .iter()
                .any(|method| method.as_str() == "resources/list")
        );
        assert!(methods.iter().any(|method| method.as_str() == "tools/list"));
    }

    #[tokio::test]
    async fn list_retries_transient_transport_failures_and_stops_on_method_not_found() {
        let transport = Arc::new(FakeTransport::new([
            response(1, initialize_response(None)),
            Err(McpTransportError::Transport("ECONNRESET".to_owned())),
            response(
                3,
                json!({"tools":[{"name":"search","annotations":{"readOnlyHint":true}}]}),
            ),
        ]));
        let client = McpClientRuntime::new(
            "tools",
            json!({"command":"mcp","timeout":1000}),
            Arc::new(FakeFactory(transport.clone())),
        );
        client.connect(&build_options(), None).await.unwrap();
        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools[0].name, "search");
        assert_eq!(tools[0].annotations, Some(json!({"readOnlyHint":true})));
        assert_eq!(transport.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn malformed_and_mismatched_json_rpc_responses_are_rejected() {
        assert!(matches!(
            parse_json_rpc_response(json!({"jsonrpc":"2.0","id":2,"result":{}}), 1),
            Err(McpClientError::InvalidResponse(_))
        ));
        assert!(matches!(
            parse_json_rpc_response(json!({"id":1,"result":{}}), 1),
            Err(McpClientError::InvalidResponse(_))
        ));
        assert_eq!(
            parse_json_rpc_response(
                json!({"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}),
                1
            ),
            Err(McpClientError::JsonRpc {
                code: -32601,
                message: "Method not found".to_owned(),
                data: None
            })
        );
    }

    #[test]
    fn tool_filters_and_missing_schemas_match_source_rules() {
        let config =
            json!({"includeTools":["search(query)","other"],"excludeTools":["other"],"trust":true});
        assert!(is_enabled("search", &config));
        assert!(!is_enabled("other", &config));
        assert!(!is_enabled("missing", &config));
        let tools =
            parse_tool_list(json!({"tools":[{"name":"x"},{"description":"unnamed"}]})).unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(
            tools[0].input_schema,
            json!({"type":"object","properties":{}})
        );
    }

    #[test]
    fn transport_config_prioritizes_http_and_overrides_authorization_with_oauth() {
        let spec = build_mcp_transport_spec(
            "server",
            &json!({"httpUrl":"https://example/mcp","url":"https://example/sse","headers":{"authorization":"old","X-Tenant":"t"},"oauth":{"enabled":true}}),
            &McpTransportBuildOptions { oauth_access_token: Some("token".to_owned()), ..Default::default() },
        ).unwrap();
        assert_eq!(spec.kind, McpTransportKind::StreamableHttp);
        assert_eq!(
            spec.headers.get("Authorization").map(String::as_str),
            Some("Bearer token")
        );
        assert_eq!(spec.headers.get("X-Tenant").map(String::as_str), Some("t"));
        assert!(spec.compatibility_fetch);
    }

    #[test]
    fn websocket_transport_is_selected_for_tcp_without_changing_stdio_precedence() {
        let websocket = build_mcp_transport_spec(
            "websocket",
            &json!({"tcp":"tcp://localhost:9000"}),
            &McpTransportBuildOptions::default(),
        )
        .unwrap();
        assert_eq!(websocket.kind, McpTransportKind::WebSocket);

        let stdio = build_mcp_transport_spec(
            "stdio",
            &json!({"tcp":"tcp://localhost:9000","command":"node"}),
            &McpTransportBuildOptions::default(),
        )
        .unwrap();
        assert_eq!(stdio.kind, McpTransportKind::Stdio);
    }

    #[test]
    fn stdio_filters_internal_secrets_after_config_merge_and_preserves_other_credentials() {
        let spec = build_mcp_transport_spec(
            "local",
            &json!({
                "command":"node",
                "env":{
                    "PATH":"C:\\local",
                    "QWEN_SERVER_TOKEN":"config-serve-secret",
                    "QWEN_DAEMON_TOKEN":"config-daemon-secret",
                    "QWEN_CODE_PRIVATE_ACP_CAPABILITY":"config-acp-secret",
                    "CANOPY_PRIVATE_ACP_CAPABILITY":"legacy-capability",
                    "GH_TOKEN":"config-gh-token",
                    "NPM_TOKEN":"config-npm-token"
                }
            }),
            &McpTransportBuildOptions {
                parent_env: BTreeMap::from([
                    ("QWEN_SERVER_TOKEN".to_owned(), "secret".to_owned()),
                    ("QWEN_DAEMON_TOKEN".to_owned(), "daemon-secret".to_owned()),
                    (
                        "QWEN_CODE_PRIVATE_ACP_CAPABILITY".to_owned(),
                        "acp-secret".to_owned(),
                    ),
                    ("PATH".to_owned(), "C:\\bin;C:\\shared".to_owned()),
                    ("Path".to_owned(), "C:\\shared;C:\\tools".to_owned()),
                    ("GH_TOKEN".to_owned(), "parent-gh-token".to_owned()),
                    ("AWS_ACCESS_KEY_ID".to_owned(), "aws-key".to_owned()),
                ]),
                windows: true,
                ..Default::default()
            },
        )
        .unwrap();
        for key in [
            "QWEN_SERVER_TOKEN",
            "QWEN_DAEMON_TOKEN",
            "QWEN_CODE_PRIVATE_ACP_CAPABILITY",
        ] {
            assert!(!spec.env.contains_key(key));
        }
        assert_eq!(spec.env.get("PATH").map(String::as_str), Some("C:\\local"));
        assert!(!spec.env.contains_key("Path"));
        assert_eq!(
            spec.env
                .get("CANOPY_PRIVATE_ACP_CAPABILITY")
                .map(String::as_str),
            Some("legacy-capability")
        );
        assert_eq!(
            spec.env.get("GH_TOKEN").map(String::as_str),
            Some("config-gh-token")
        );
        assert_eq!(
            spec.env.get("NPM_TOKEN").map(String::as_str),
            Some("config-npm-token")
        );
        assert_eq!(
            spec.env.get("AWS_ACCESS_KEY_ID").map(String::as_str),
            Some("aws-key")
        );
    }

    #[test]
    fn get_sse_fallback_only_normalizes_the_optional_non_resumable_sse_get() {
        let headers = vec![(
            "Accept".to_owned(),
            "application/json, text/event-stream; q=0.9".to_owned(),
        )];
        let fallback =
            streamable_http_get_sse_fallback(Some("GET"), &headers, 400, b"bad request").unwrap();
        assert_eq!(fallback.status, 405);
        assert_eq!(fallback.status_text, "Method Not Allowed");
        assert_eq!(fallback.diagnostic_excerpt.as_deref(), Some("bad request"));
        assert!(
            streamable_http_get_sse_fallback(Some("GET"), &headers, 400, b"")
                .unwrap()
                .diagnostic_excerpt
                .is_none()
        );
        assert_eq!(
            streamable_http_get_sse_fallback(Some("GET"), &headers, 400, b"x")
                .unwrap()
                .diagnostic_excerpt
                .as_deref(),
            Some("x")
        );
        let resumable = [
            headers[0].clone(),
            ("Last-Event-ID".to_owned(), "cursor".to_owned()),
        ];
        assert!(
            streamable_http_get_sse_fallback(Some("GET"), &resumable, 400, b"invalid cursor")
                .is_none()
        );
        assert!(streamable_http_get_sse_fallback(Some("POST"), &headers, 400, b"error").is_none());
        assert!(
            streamable_http_get_sse_fallback(Some("GET"), &headers, 502, b"upstream").is_none()
        );
    }

    #[test]
    fn oauth_state_is_scoped_by_url_and_clearing_one_keeps_other_requirements() {
        let state = McpOAuthRecoveryState::default();
        let first = json!({"httpUrl":"https://one/mcp"});
        let second = json!({"httpUrl":"https://two/mcp"});
        let unauthorized = McpClientError::HttpStatus {
            status: 401,
            message: "Unauthorized".to_owned(),
            www_authenticate: Some("Bearer resource_metadata=\"https://meta\"".to_owned()),
        };
        assert!(state.record_connect_error("same", &first, &unauthorized));
        assert!(state.record_connect_error("same", &second, &unauthorized));
        assert!(state.server_requires_oauth("same"));
        assert_eq!(
            state.challenge("same", &first).as_deref(),
            Some("Bearer resource_metadata=\"https://meta\"")
        );
        state.clear("same", &first);
        assert!(state.server_requires_oauth("same"));
        assert!(state.challenge("same", &first).is_none());
        state.clear("same", &second);
        assert!(!state.server_requires_oauth("same"));
    }

    #[test]
    fn oauth_guidance_and_connect_error_mapping_are_actionable() {
        assert!(
            get_mcp_oauth_dialog_instruction("authenticate", "git")
                .contains("open the /mcp dialog to authenticate with MCP server 'git'")
        );
        assert_eq!(
            map_mcp_connect_error(
                "local",
                &McpClientError::Transport("ECONNREFUSED".to_owned()),
                true
            ),
            "Cannot connect to 'local' - server may be down or URL incorrect (check sandbox availability)"
        );
        assert_eq!(
            map_mcp_connect_error(
                "local",
                &McpClientError::Transport("broken".to_owned()),
                false
            ),
            "Connection failed for 'local': MCP transport error: broken"
        );
    }

    #[test]
    fn roots_handler_returns_file_urls_and_method_not_found_for_other_requests() {
        let request = json!({"jsonrpc":"2.0","id":7,"method":"roots/list"});
        let roots = handle_server_request(&request, &[PathBuf::from("/tmp/project")]);
        assert_eq!(roots["id"], 7);
        assert_eq!(roots["result"]["roots"][0]["uri"], "file:///tmp/project");
        let other = handle_server_request(&json!({"id":8,"method":"unknown"}), &[]);
        assert_eq!(other["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn cancellation_is_observed_during_list_backoff() {
        let transport = Arc::new(FakeTransport::new([
            response(1, initialize_response(None)),
            Err(McpTransportError::Transport("ECONNRESET".to_owned())),
        ]));
        let client = Arc::new(McpClientRuntime::new(
            "cancel",
            json!({"httpUrl":"https://mcp","timeout":1000}),
            Arc::new(FakeFactory(transport)),
        ));
        client.connect(&build_options(), None).await.unwrap();
        let token = CancellationToken::new();
        let child = token.clone();
        let task = tokio::spawn(async move {
            client
                .request_with_list_retry("tools/list", json!({}), Some(child))
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
        assert!(matches!(
            task.await.unwrap(),
            Err(McpClientError::Cancelled)
        ));
    }
}
