//! Native MCP OAuth authorization-code provider.
//!
//! The source provider owns discovery, dynamic client registration, PKCE,
//! localhost callback validation, token exchange, refresh, and persistence.
//! This module keeps those operations independent from a particular CLI or UI:
//! callers can either drive the authorization session explicitly or use the
//! interactive convenience method.

use crate::browser_launch::open_browser_securely;
use crate::mcp::oauth_utils::{McpOAuthConfig, OAuthUrlError, OAuthUtils};
use crate::mcp::token_storage::{
    BaseTokenStorage, OAuthCredentials, OAuthToken, TokenStorage, TokenStorageError,
};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub const MCP_OAUTH_CLIENT_NAME: &str = "Canopy Code MCP Client";
pub const MCP_SA_IMPERSONATION_CLIENT_NAME: &str = "Canopy Code (Service Account Impersonation)";
pub const OAUTH_REDIRECT_PORT: u16 = 7777;
pub const OAUTH_REDIRECT_PATH: &str = "/oauth/callback";
pub const OAUTH_DISPLAY_MESSAGE_EVENT: &str = "oauth-display-message";
pub const OAUTH_AUTH_URL_EVENT: &str = "oauth-auth-url";

const DEFAULT_CALLBACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_ERROR_EXCERPT_BYTES: usize = 1024;
const MAX_CALLBACK_REQUEST_BYTES: usize = 8 * 1024;

/// OAuth settings accepted by the MCP provider. Names and serialization match
/// the source `MCPOAuthConfig` camel-case settings shape.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpOAuthProviderConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub audiences: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_param_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_url: Option<String>,
}

impl From<McpOAuthConfig> for McpOAuthProviderConfig {
    fn from(config: McpOAuthConfig) -> Self {
        Self {
            authorization_url: Some(config.authorization_url),
            token_url: Some(config.token_url),
            scopes: config.scopes,
            registration_url: config.registration_url,
            ..Self::default()
        }
    }
}

/// OAuth authorization response returned by the local callback endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct McpOAuthAuthorizationResponse {
    pub code: String,
    pub state: String,
}

/// A prepared authorization session. The verifier and expected state stay
/// private so callers cannot accidentally log or overwrite PKCE state.
pub struct McpOAuthAuthorizationSession {
    pub authorization_url: String,
    pub redirect_uri: String,
    pub client_id: String,
    state: String,
    code_verifier: String,
    server_name: String,
    mcp_server_url: Option<String>,
    config: McpOAuthProviderConfig,
}

impl McpOAuthAuthorizationSession {
    pub fn state(&self) -> &str {
        &self.state
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct DynamicRegistrationRequest {
    client_name: String,
    redirect_uris: Vec<String>,
    grant_types: Vec<String>,
    response_types: Vec<String>,
    token_endpoint_auth_method: String,
    code_challenge_method: Vec<String>,
    scope: String,
}

#[derive(Clone, Debug, Deserialize)]
struct DynamicRegistrationResponse {
    client_id: String,
    #[serde(default)]
    client_secret: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct OAuthTokenResponseWire {
    access_token: Option<String>,
    token_type: Option<String>,
    expires_in: Option<Value>,
    refresh_token: Option<String>,
    scope: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

/// Parsed access-token response. `expires_in` is seconds from receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpOAuthTokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<i64>,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Error)]
pub enum McpOAuthProviderError {
    #[error("OAuth URL error: {0}")]
    Url(#[from] OAuthUrlError),
    #[error("OAuth token storage failed: {0}")]
    Storage(#[from] TokenStorageError),
    #[error("OAuth HTTP request timed out during {operation}")]
    Timeout { operation: &'static str },
    #[error("OAuth request failed during {operation}: {message}")]
    Request {
        operation: &'static str,
        message: String,
    },
    #[error("OAuth {operation} failed with HTTP {status}: {message}")]
    Http {
        operation: &'static str,
        status: u16,
        message: String,
    },
    #[error("OAuth configuration is incomplete: {0}")]
    Configuration(String),
    #[error("OAuth response is invalid: {0}")]
    InvalidResponse(String),
    #[error("OAuth callback failed: {0}")]
    Callback(String),
    #[error("OAuth callback timed out after {timeout_seconds} seconds")]
    CallbackTimeout { timeout_seconds: u64 },
    #[error("OAuth browser launch failed: {0}")]
    Browser(String),
}

/// MCP OAuth provider core for dynamic registration, PKCE authorization,
/// callback validation, code/token exchange, refresh, and storage.
#[derive(Clone)]
pub struct McpOAuthProvider {
    http: Client,
}

impl Default for McpOAuthProvider {
    fn default() -> Self {
        Self::new().expect("building bounded OAuth HTTP client should succeed")
    }
}

impl McpOAuthProvider {
    pub fn new() -> Result<Self, McpOAuthProviderError> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| request_error("HTTP client setup", error))?;
        Ok(Self { http })
    }

    /// Use a caller-provided HTTP client. Every provider request still applies
    /// the provider's bounded per-request timeout.
    pub fn with_http_client(http: Client) -> Self {
        Self { http }
    }

    /// Resolve discovery and dynamic registration, then prepare an S256 PKCE
    /// authorization URL. The returned session must be completed with a
    /// callback carrying the same state before the code can be exchanged.
    pub async fn prepare_authorization(
        &self,
        server_name: impl Into<String>,
        mut config: McpOAuthProviderConfig,
        mcp_server_url: Option<String>,
    ) -> Result<McpOAuthAuthorizationSession, McpOAuthProviderError> {
        if config
            .authorization_url
            .as_deref()
            .is_none_or(str::is_empty)
            && let Some(server_url) = mcp_server_url.as_deref()
        {
            if let Some(discovered) = self.discover_from_auth_challenge(server_url).await {
                merge_discovered_config(&mut config, discovered);
            }
            if config
                .authorization_url
                .as_deref()
                .is_none_or(str::is_empty)
            {
                let discovered = OAuthUtils::discover_oauth_config(&self.http, server_url)
                    .await
                    .ok_or_else(|| {
                        McpOAuthProviderError::Configuration(
                            "failed to discover OAuth configuration from MCP server".to_owned(),
                        )
                    })?;
                merge_discovered_config(&mut config, discovered);
            }
        }

        let authorization_url = required_config(&config.authorization_url, "authorization URL")?;
        let token_url = required_config(&config.token_url, "token URL")?;
        validate_http_url(&authorization_url, "authorization URL")?;
        validate_http_url(&token_url, "token URL")?;

        let redirect_uri = config
            .redirect_uri
            .clone()
            .unwrap_or_else(default_redirect_uri);
        validate_http_url(&redirect_uri, "redirect URI")?;

        if config.client_id.as_deref().is_none_or(str::is_empty) {
            let registration_url = match config.registration_url.clone() {
                Some(url) if !url.is_empty() => Some(url),
                _ => self.discover_registration_url(&authorization_url).await?,
            }
            .ok_or_else(|| {
                McpOAuthProviderError::Configuration(
                    "no client ID provided and dynamic registration is unavailable".to_owned(),
                )
            })?;
            let registered = self
                .register_client(&registration_url, &redirect_uri, &config)
                .await?;
            config.client_id = Some(registered.client_id);
            if registered.client_secret.is_some() {
                config.client_secret = registered.client_secret;
            }
        }

        let client_id = required_config(&config.client_id, "client ID")?;
        let code_verifier = random_url_token(32);
        let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
        let state = random_url_token(16);
        let authorization_url = build_authorization_url(
            &authorization_url,
            &redirect_uri,
            &client_id,
            &state,
            &code_challenge,
            &config,
            mcp_server_url.as_deref(),
        )?;

        Ok(McpOAuthAuthorizationSession {
            authorization_url,
            redirect_uri,
            client_id,
            state,
            code_verifier,
            server_name: server_name.into(),
            mcp_server_url,
            config,
        })
    }

    /// Wait for the configured loopback callback and validate its OAuth state.
    /// Custom non-loopback redirects are handled by calling
    /// [`parse_callback_url`](Self::parse_callback_url) in the host application.
    pub async fn wait_for_callback(
        &self,
        session: &McpOAuthAuthorizationSession,
    ) -> Result<McpOAuthAuthorizationResponse, McpOAuthProviderError> {
        let (listener, expected_path) = bind_callback_listener(&session.redirect_uri).await?;
        await_oauth_callback(listener, expected_path, session.state.clone()).await
    }

    /// Parse a callback URL supplied by a host-controlled redirect handler.
    /// URL path and state must match the prepared session.
    pub fn parse_callback_url(
        session: &McpOAuthAuthorizationSession,
        callback_url: &str,
    ) -> Result<McpOAuthAuthorizationResponse, McpOAuthProviderError> {
        let callback = Url::parse(callback_url)
            .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))?;
        let redirect = Url::parse(&session.redirect_uri)
            .map_err(|error| McpOAuthProviderError::Configuration(error.to_string()))?;
        if callback.path() != redirect.path() {
            return Err(McpOAuthProviderError::Callback(
                "callback path does not match redirect URI".to_owned(),
            ));
        }
        callback_response_from_query(callback.query_pairs(), &session.state)
    }

    /// Exchange a validated callback authorization code and persist its token
    /// in the source-compatible `OAuthCredentials[]` storage shape.
    pub async fn complete_authorization(
        &self,
        storage: &impl TokenStorage,
        session: McpOAuthAuthorizationSession,
        callback: McpOAuthAuthorizationResponse,
    ) -> Result<OAuthToken, McpOAuthProviderError> {
        if callback.state != session.state {
            return Err(McpOAuthProviderError::Callback(
                "state parameter did not match the authorization request".to_owned(),
            ));
        }
        if callback.code.is_empty() {
            return Err(McpOAuthProviderError::Callback(
                "authorization code is empty".to_owned(),
            ));
        }
        let token_url = required_config(&session.config.token_url, "token URL")?;
        let response = self
            .exchange_code_for_token(
                &session.config,
                &token_url,
                &callback.code,
                &session.code_verifier,
                &session.redirect_uri,
                session.mcp_server_url.as_deref(),
            )
            .await?;
        let now_ms = now_unix_millis();
        let token = to_oauth_token(response, now_ms);
        let credentials = OAuthCredentials {
            server_name: session.server_name,
            token: token.clone(),
            client_id: Some(session.client_id),
            token_url: Some(token_url),
            mcp_server_url: session.mcp_server_url,
            updated_at: now_ms,
        };
        BaseTokenStorage::validate_credentials(&credentials)?;
        storage.set_credentials(credentials).await?;
        Ok(token)
    }

    /// Drive the full localhost PKCE flow: prepare, launch the browser, await
    /// the callback, exchange the code, and persist the resulting token.
    pub async fn authenticate(
        &self,
        storage: &impl TokenStorage,
        server_name: impl Into<String>,
        config: McpOAuthProviderConfig,
        mcp_server_url: Option<String>,
    ) -> Result<OAuthToken, McpOAuthProviderError> {
        let session = self
            .prepare_authorization(server_name, config, mcp_server_url)
            .await?;
        // Bind before opening the browser so fast redirects cannot beat the
        // callback listener. Move owned callback state into the task.
        let (listener, expected_path) = bind_callback_listener(&session.redirect_uri).await?;
        let expected_state = session.state.clone();
        let callback_task = tokio::spawn(async move {
            await_oauth_callback(listener, expected_path, expected_state).await
        });
        if let Err(error) = open_browser_securely(&session.authorization_url) {
            eprintln!(
                "Could not open the OAuth browser automatically: {error}. Open this URL manually:\n{}",
                session.authorization_url
            );
        }
        let callback = callback_task
            .await
            .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))??;
        self.complete_authorization(storage, session, callback)
            .await
    }

    /// Perform the source provider's refresh-token grant.
    pub async fn refresh_access_token(
        &self,
        config: &McpOAuthProviderConfig,
        refresh_token: &str,
        token_url: &str,
        mcp_server_url: Option<&str>,
    ) -> Result<McpOAuthTokenResponse, McpOAuthProviderError> {
        let client_id = required_config(&config.client_id, "client ID")?;
        let mut params = vec![
            ("grant_type".to_owned(), "refresh_token".to_owned()),
            ("refresh_token".to_owned(), refresh_token.to_owned()),
            ("client_id".to_owned(), client_id),
        ];
        if let Some(secret) = config.client_secret.as_deref() {
            params.push(("client_secret".to_owned(), secret.to_owned()));
        }
        append_scope_audience_resource(&mut params, config, mcp_server_url, true);
        self.post_token_form(token_url, params, "token refresh")
            .await
    }

    /// Return the cached access token, refreshing it when the source storage
    /// policy says it is expiring. A failed refresh deletes stale credentials,
    /// matching `MCPOAuthProvider` and `get_valid_mcp_oauth_token`.
    pub async fn get_valid_access_token(
        &self,
        storage: &impl TokenStorage,
        server_name: &str,
        config: &McpOAuthProviderConfig,
        now_ms: i64,
    ) -> Result<Option<String>, McpOAuthProviderError> {
        let Some(credentials) = storage.get_credentials(server_name).await? else {
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
        let mut refresh_config = config.clone();
        refresh_config.client_id = Some(client_id.to_owned());
        let updated = match self
            .refresh_access_token(
                &refresh_config,
                refresh_token,
                token_url,
                credentials.mcp_server_url.as_deref(),
            )
            .await
        {
            Ok(response) => OAuthCredentials {
                server_name: server_name.to_owned(),
                token: OAuthToken {
                    access_token: response.access_token,
                    refresh_token: response.refresh_token.or(credentials.token.refresh_token),
                    expires_at: response
                        .expires_in
                        .map(|seconds| now_ms.saturating_add(seconds.saturating_mul(1000))),
                    token_type: response.token_type,
                    scope: response.scope.or(credentials.token.scope),
                },
                client_id: Some(client_id.to_owned()),
                token_url: credentials.token_url,
                mcp_server_url: credentials.mcp_server_url,
                updated_at: now_ms,
            },
            Err(_) => {
                storage.delete_credentials(server_name).await?;
                return Ok(None);
            }
        };
        BaseTokenStorage::validate_credentials(&updated)?;
        let access_token = updated.token.access_token.clone();
        storage.set_credentials(updated).await?;
        Ok(Some(access_token))
    }

    async fn discover_from_auth_challenge(&self, server_url: &str) -> Option<McpOAuthConfig> {
        let accept = if OAuthUtils::is_sse_endpoint(server_url) {
            "text/event-stream"
        } else {
            "application/json"
        };
        let response = self
            .http
            .head(server_url)
            .header(reqwest::header::ACCEPT, accept)
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .ok()?;
        if !matches!(
            response.status(),
            StatusCode::UNAUTHORIZED | StatusCode::TEMPORARY_REDIRECT
        ) {
            return None;
        }
        let challenge = response
            .headers()
            .get(reqwest::header::WWW_AUTHENTICATE)?
            .to_str()
            .ok()?;
        OAuthUtils::discover_oauth_from_www_authenticate(&self.http, challenge)
            .await
            .ok()
            .flatten()
    }

    async fn discover_registration_url(
        &self,
        authorization_url: &str,
    ) -> Result<Option<String>, McpOAuthProviderError> {
        let authorization = Url::parse(authorization_url)
            .map_err(|error| McpOAuthProviderError::Configuration(error.to_string()))?;
        let origin = format!(
            "{}://{}",
            authorization.scheme(),
            authorization.host_str().unwrap_or_default()
        );
        let metadata =
            OAuthUtils::discover_authorization_server_metadata(&self.http, &origin).await?;
        Ok(metadata.and_then(|metadata| metadata.registration_endpoint))
    }

    async fn register_client(
        &self,
        registration_url: &str,
        redirect_uri: &str,
        config: &McpOAuthProviderConfig,
    ) -> Result<DynamicRegistrationResponse, McpOAuthProviderError> {
        validate_http_url(registration_url, "registration URL")?;
        let request = DynamicRegistrationRequest {
            client_name: MCP_OAUTH_CLIENT_NAME.to_owned(),
            redirect_uris: vec![redirect_uri.to_owned()],
            grant_types: vec!["authorization_code".to_owned(), "refresh_token".to_owned()],
            response_types: vec!["code".to_owned()],
            token_endpoint_auth_method: "none".to_owned(),
            code_challenge_method: vec!["S256".to_owned()],
            scope: config.scopes.join(" "),
        };
        let response = self
            .http
            .post(registration_url)
            .timeout(REQUEST_TIMEOUT)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|error| request_error("client registration", error))?;
        let status = response.status();
        let body = read_bounded_body(response, MAX_RESPONSE_BYTES, "client registration").await?;
        if !status.is_success() {
            return Err(http_error("client registration", status, &body));
        }
        let registration: DynamicRegistrationResponse =
            serde_json::from_slice(&body).map_err(|error| {
                McpOAuthProviderError::InvalidResponse(format!(
                    "client registration response is invalid JSON: {error}"
                ))
            })?;
        if registration.client_id.trim().is_empty() {
            return Err(McpOAuthProviderError::InvalidResponse(
                "client registration response is missing client_id".to_owned(),
            ));
        }
        Ok(registration)
    }

    async fn exchange_code_for_token(
        &self,
        config: &McpOAuthProviderConfig,
        token_url: &str,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
        mcp_server_url: Option<&str>,
    ) -> Result<McpOAuthTokenResponse, McpOAuthProviderError> {
        let client_id = required_config(&config.client_id, "client ID")?;
        let mut params = vec![
            ("grant_type".to_owned(), "authorization_code".to_owned()),
            ("code".to_owned(), code.to_owned()),
            ("redirect_uri".to_owned(), redirect_uri.to_owned()),
            ("code_verifier".to_owned(), code_verifier.to_owned()),
            ("client_id".to_owned(), client_id),
        ];
        if let Some(secret) = config.client_secret.as_deref() {
            params.push(("client_secret".to_owned(), secret.to_owned()));
        }
        append_scope_audience_resource(&mut params, config, mcp_server_url, false);
        self.post_token_form(token_url, params, "token exchange")
            .await
    }

    async fn post_token_form(
        &self,
        token_url: &str,
        params: Vec<(String, String)>,
        operation: &'static str,
    ) -> Result<McpOAuthTokenResponse, McpOAuthProviderError> {
        validate_http_url(token_url, "token URL")?;
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params)
            .finish();
        let response = self
            .http
            .post(token_url)
            .timeout(REQUEST_TIMEOUT)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(
                reqwest::header::ACCEPT,
                "application/json, application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .map_err(|error| request_error(operation, error))?;
        let status = response.status();
        let bytes = read_bounded_body(response, MAX_RESPONSE_BYTES, operation).await?;
        if !status.is_success() {
            return Err(http_error(operation, status, &bytes));
        }
        parse_token_response(&bytes, operation)
    }
}

async fn read_bounded_body(
    response: reqwest::Response,
    max_bytes: usize,
    operation: &'static str,
) -> Result<Vec<u8>, McpOAuthProviderError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(McpOAuthProviderError::InvalidResponse(format!(
            "{operation} response exceeds the {max_bytes}-byte limit"
        )));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::with_capacity(4096);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| request_error(operation, error))?;
        if bytes.len().saturating_add(chunk.len()) > max_bytes {
            return Err(McpOAuthProviderError::InvalidResponse(format!(
                "{operation} response exceeds the {max_bytes}-byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn parse_token_response(
    bytes: &[u8],
    operation: &'static str,
) -> Result<McpOAuthTokenResponse, McpOAuthProviderError> {
    let parsed_json = serde_json::from_slice::<OAuthTokenResponseWire>(bytes);
    let wire = match parsed_json {
        Ok(wire) => wire,
        Err(_) => {
            let values = url::form_urlencoded::parse(bytes)
                .into_owned()
                .collect::<std::collections::HashMap<_, _>>();
            OAuthTokenResponseWire {
                access_token: values.get("access_token").cloned(),
                token_type: values.get("token_type").cloned(),
                expires_in: values.get("expires_in").cloned().map(Value::String),
                refresh_token: values.get("refresh_token").cloned(),
                scope: values.get("scope").cloned(),
                error: values.get("error").cloned(),
                error_description: values.get("error_description").cloned(),
            }
        }
    };
    let access_token = wire
        .access_token
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            let error = wire.error.as_deref().unwrap_or("no_access_token");
            let description = wire
                .error_description
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| bounded_excerpt(bytes));
            McpOAuthProviderError::InvalidResponse(format!(
                "{operation} failed: {error} - {description}"
            ))
        })?;
    let expires_in = match wire.expires_in.as_ref() {
        None | Some(Value::Null) => None,
        Some(Value::Number(number)) => number.as_i64().filter(|seconds| *seconds >= 0),
        Some(Value::String(seconds)) if !seconds.trim().is_empty() => {
            let seconds = seconds.trim();
            if seconds.bytes().all(|byte| byte.is_ascii_digit()) {
                seconds.parse::<i64>().ok()
            } else {
                None
            }
        }
        Some(_) => None,
    };
    if wire
        .expires_in
        .as_ref()
        .is_some_and(|value| !value.is_null())
        && expires_in.is_none()
    {
        return Err(McpOAuthProviderError::InvalidResponse(
            "token response has invalid expires_in".to_owned(),
        ));
    }
    Ok(McpOAuthTokenResponse {
        access_token,
        token_type: wire
            .token_type
            .filter(|token_type| !token_type.is_empty())
            .unwrap_or_else(|| "Bearer".to_owned()),
        expires_in,
        refresh_token: wire.refresh_token.filter(|token| !token.is_empty()),
        scope: wire.scope.filter(|scope| !scope.is_empty()),
    })
}

fn append_scope_audience_resource(
    params: &mut Vec<(String, String)>,
    config: &McpOAuthProviderConfig,
    mcp_server_url: Option<&str>,
    include_scope: bool,
) {
    if include_scope && !config.scopes.is_empty() {
        params.push(("scope".to_owned(), config.scopes.join(" ")));
    }
    if !config.audiences.is_empty() {
        params.push(("audience".to_owned(), config.audiences.join(" ")));
    }
    if let Some(server_url) = mcp_server_url
        && let Ok(resource) = OAuthUtils::build_resource_parameter(server_url)
    {
        params.push(("resource".to_owned(), resource));
    }
}

fn build_authorization_url(
    authorization_url: &str,
    redirect_uri: &str,
    client_id: &str,
    state: &str,
    code_challenge: &str,
    config: &McpOAuthProviderConfig,
    mcp_server_url: Option<&str>,
) -> Result<String, McpOAuthProviderError> {
    let mut url = Url::parse(authorization_url)
        .map_err(|error| McpOAuthProviderError::Configuration(error.to_string()))?;
    {
        let mut params = url.query_pairs_mut();
        params
            .append_pair("client_id", client_id)
            .append_pair("response_type", "code")
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("state", state)
            .append_pair("code_challenge", code_challenge)
            .append_pair("code_challenge_method", "S256");
        if !config.scopes.is_empty() {
            params.append_pair("scope", &config.scopes.join(" "));
        }
        if !config.audiences.is_empty() {
            params.append_pair("audience", &config.audiences.join(" "));
        }
        if let Some(server_url) = mcp_server_url
            && let Ok(resource) = OAuthUtils::build_resource_parameter(server_url)
        {
            params.append_pair("resource", &resource);
        }
    }
    Ok(url.to_string())
}

fn merge_discovered_config(config: &mut McpOAuthProviderConfig, discovered: McpOAuthConfig) {
    if config
        .authorization_url
        .as_deref()
        .is_none_or(str::is_empty)
    {
        config.authorization_url = Some(discovered.authorization_url);
    }
    if config.token_url.as_deref().is_none_or(str::is_empty) {
        config.token_url = Some(discovered.token_url);
    }
    if config.scopes.is_empty() && !discovered.scopes.is_empty() {
        config.scopes = discovered.scopes;
    }
    if config.registration_url.as_deref().is_none_or(str::is_empty) {
        config.registration_url = discovered.registration_url;
    }
}

fn required_config(value: &Option<String>, label: &str) -> Result<String, McpOAuthProviderError> {
    value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| McpOAuthProviderError::Configuration(format!("missing {label}")))
}

fn validate_http_url(value: &str, label: &str) -> Result<(), McpOAuthProviderError> {
    let url = Url::parse(value).map_err(|error| {
        McpOAuthProviderError::Configuration(format!("invalid {label}: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(McpOAuthProviderError::Configuration(format!(
            "{label} must be an absolute HTTP(S) URL"
        )));
    }
    Ok(())
}

fn default_redirect_uri() -> String {
    format!("http://localhost:{OAUTH_REDIRECT_PORT}{OAUTH_REDIRECT_PATH}")
}

fn random_url_token(bytes: usize) -> String {
    // UUID v4 uses the platform CSPRNG. Two identifiers provide 32 random
    // bytes for a verifier; one provides a high-entropy callback state.
    let mut random = Vec::with_capacity(bytes);
    while random.len() < bytes {
        random.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    random.truncate(bytes);
    URL_SAFE_NO_PAD.encode(random)
}

fn callback_response_from_query<'a>(
    pairs: impl Iterator<Item = (std::borrow::Cow<'a, str>, std::borrow::Cow<'a, str>)>,
    expected_state: &str,
) -> Result<McpOAuthAuthorizationResponse, McpOAuthProviderError> {
    let mut code = None;
    let mut state = None;
    let mut error = None;
    let mut error_description = None;
    for (key, value) in pairs {
        match key.as_ref() {
            "code" if code.is_none() => code = Some(value.into_owned()),
            "state" if state.is_none() => state = Some(value.into_owned()),
            "error" if error.is_none() => error = Some(value.into_owned()),
            "error_description" if error_description.is_none() => {
                error_description = Some(value.into_owned());
            }
            _ => {}
        }
    }
    let state = state
        .ok_or_else(|| McpOAuthProviderError::Callback("missing state parameter".to_owned()))?;
    if state != expected_state {
        return Err(McpOAuthProviderError::Callback(
            "state parameter did not match the authorization request".to_owned(),
        ));
    }
    if let Some(error) = error {
        let description = error_description.unwrap_or_default();
        return Err(McpOAuthProviderError::Callback(if description.is_empty() {
            error
        } else {
            format!("{error}: {description}")
        }));
    }
    let code = code
        .filter(|code| !code.is_empty())
        .ok_or_else(|| McpOAuthProviderError::Callback("missing code parameter".to_owned()))?;
    Ok(McpOAuthAuthorizationResponse { code, state })
}

async fn accept_oauth_callback(
    listener: TcpListener,
    expected_path: &str,
    expected_state: &str,
) -> Result<McpOAuthAuthorizationResponse, McpOAuthProviderError> {
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))?;
        let request = match read_callback_request(&mut stream).await {
            Ok(request) => request,
            Err(error) => {
                let _ = write_callback_response(&mut stream, 400, "Invalid callback request").await;
                return Err(error);
            }
        };
        let Some(target) = request.split_whitespace().nth(1) else {
            let _ = write_callback_response(&mut stream, 400, "Invalid callback request").await;
            continue;
        };
        let callback = match Url::parse(&format!("http://localhost{target}")) {
            Ok(callback) => callback,
            Err(_) => {
                let _ = write_callback_response(&mut stream, 400, "Invalid callback URL").await;
                continue;
            }
        };
        if callback.path() != expected_path {
            let _ = write_callback_response(&mut stream, 404, "Not found").await;
            continue;
        }
        match callback_response_from_query(callback.query_pairs(), expected_state) {
            Ok(response) => {
                write_callback_response(
                    &mut stream,
                    200,
                    "<html><body><h1>Authentication successful</h1><p>You can close this window and return to Canopy Code.</p><script>window.close();</script></body></html>",
                )
                .await?;
                return Ok(response);
            }
            Err(McpOAuthProviderError::Callback(message))
                if message.contains("state parameter did not match")
                    || message == "missing state parameter"
                    || message == "missing code parameter" =>
            {
                let safe = html_escape(&message);
                write_callback_response(
                    &mut stream,
                    400,
                    &format!("<html><body><h1>Authentication callback rejected</h1><p>{safe}</p></body></html>"),
                )
                .await?;
            }
            Err(McpOAuthProviderError::Callback(message)) => {
                let safe = html_escape(&message);
                write_callback_response(
                    &mut stream,
                    200,
                    &format!("<html><body><h1>Authentication failed</h1><p>{safe}</p><p>You can close this window.</p></body></html>"),
                )
                .await?;
                return Err(McpOAuthProviderError::Callback(message));
            }
            Err(error) => return Err(error),
        }
    }
}

async fn bind_callback_listener(
    redirect_uri: &str,
) -> Result<(TcpListener, String), McpOAuthProviderError> {
    let redirect = Url::parse(redirect_uri)
        .map_err(|error| McpOAuthProviderError::Configuration(error.to_string()))?;
    let host = redirect.host_str().unwrap_or_default();
    if redirect.scheme() != "http" || !matches!(host, "localhost" | "127.0.0.1" | "[::1]" | "::1") {
        return Err(McpOAuthProviderError::Configuration(
            "automatic OAuth callback listening only supports HTTP loopback redirect URIs"
                .to_owned(),
        ));
    }
    let port = redirect.port().unwrap_or(80);
    let bind_host = if host == "localhost" {
        "127.0.0.1"
    } else {
        host
    };
    let listener = TcpListener::bind((bind_host, port))
        .await
        .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))?;
    Ok((listener, redirect.path().to_owned()))
}

async fn await_oauth_callback(
    listener: TcpListener,
    expected_path: String,
    expected_state: String,
) -> Result<McpOAuthAuthorizationResponse, McpOAuthProviderError> {
    match tokio::time::timeout(
        DEFAULT_CALLBACK_TIMEOUT,
        accept_oauth_callback(listener, &expected_path, &expected_state),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(McpOAuthProviderError::CallbackTimeout {
            timeout_seconds: DEFAULT_CALLBACK_TIMEOUT.as_secs(),
        }),
    }
}

async fn read_callback_request(
    stream: &mut tokio::net::TcpStream,
) -> Result<String, McpOAuthProviderError> {
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let read = tokio::time::timeout(REQUEST_TIMEOUT, stream.read(&mut chunk))
            .await
            .map_err(|_| McpOAuthProviderError::Callback("callback request timed out".to_owned()))?
            .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))?;
        if read == 0 {
            return Err(McpOAuthProviderError::Callback(
                "callback request ended before headers".to_owned(),
            ));
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() > MAX_CALLBACK_REQUEST_BYTES {
            return Err(McpOAuthProviderError::Callback(
                "callback request exceeds the size limit".to_owned(),
            ));
        }
    }
    let request = String::from_utf8(request)
        .map_err(|_| McpOAuthProviderError::Callback("callback request is not UTF-8".to_owned()))?;
    if !request.starts_with("GET ") {
        return Err(McpOAuthProviderError::Callback(
            "callback request must use GET".to_owned(),
        ));
    }
    Ok(request)
}

async fn write_callback_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> Result<(), McpOAuthProviderError> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|error| McpOAuthProviderError::Callback(error.to_string()))?;
    let _ = stream.shutdown().await;
    Ok(())
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn to_oauth_token(response: McpOAuthTokenResponse, now_ms: i64) -> OAuthToken {
    OAuthToken {
        access_token: response.access_token,
        refresh_token: response.refresh_token,
        expires_at: response
            .expires_in
            .map(|seconds| now_ms.saturating_add(seconds.saturating_mul(1000))),
        token_type: response.token_type,
        scope: response.scope,
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

fn request_error(operation: &'static str, error: reqwest::Error) -> McpOAuthProviderError {
    if error.is_timeout() {
        McpOAuthProviderError::Timeout { operation }
    } else {
        McpOAuthProviderError::Request {
            operation,
            message: error.to_string(),
        }
    }
}

fn http_error(operation: &'static str, status: StatusCode, body: &[u8]) -> McpOAuthProviderError {
    let message = parse_oauth_error(body).unwrap_or_else(|| bounded_excerpt(body));
    McpOAuthProviderError::Http {
        operation,
        status: status.as_u16(),
        message,
    }
}

fn parse_oauth_error(body: &[u8]) -> Option<String> {
    let json = serde_json::from_slice::<Value>(body).ok();
    if let Some(json) = json {
        let error = json.get("error").and_then(Value::as_str)?;
        let description = json
            .get("error_description")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        return Some(match description {
            Some(description) => format!(
                "{error} - {}",
                truncate(description, MAX_ERROR_EXCERPT_BYTES)
            ),
            None => error.to_owned(),
        });
    }
    let form = url::form_urlencoded::parse(body)
        .into_owned()
        .collect::<std::collections::HashMap<_, _>>();
    let error = form.get("error")?;
    Some(
        match form
            .get("error_description")
            .filter(|value| !value.is_empty())
        {
            Some(description) => format!(
                "{error} - {}",
                truncate(description, MAX_ERROR_EXCERPT_BYTES)
            ),
            None => error.to_owned(),
        },
    )
}

fn bounded_excerpt(bytes: &[u8]) -> String {
    truncate(&String::from_utf8_lossy(bytes), MAX_ERROR_EXCERPT_BYTES)
}

fn truncate(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &value[..boundary])
}
