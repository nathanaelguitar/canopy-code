//! Bounded HTTP fetches for remote extension marketplace configuration.
//!
//! Redirects are followed manually. Each destination passes through the
//! extension network policy before a request is made, and public-policy DNS
//! results are pinned into the default reqwest transport. The resolver and
//! one-hop transport are injected so callers can choose their host policy and
//! tests or embedding applications can supply another HTTP executor.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::header::{
    ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, LOCATION, PROXY_AUTHORIZATION, USER_AGENT,
};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode, Url};

use crate::extension_install_source::{
    InstallSourceType, parse_github_repo_for_releases, parse_install_source,
};
use crate::extension_network_policy::{
    ExtensionAddressResolver, ExtensionNetworkPolicy, NetworkPin, resolve_network_target,
};
use crate::extensions::redact_url_credentials;
use crate::utils::cancellation::{CancellationReason, CancellationToken};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_REDIRECTS: usize = 10;

/// Bounds for one marketplace document fetch, including every redirect hop.
#[derive(Clone, Debug)]
pub struct MarketplaceFetchOptions {
    pub timeout: Duration,
    pub max_body_bytes: usize,
    pub max_redirects: usize,
    /// `Public` requires HTTPS without URL credentials and validates plus pins
    /// every DNS result. `None` preserves the caller's broader HTTP behavior.
    pub network_policy: Option<ExtensionNetworkPolicy>,
}

impl Default for MarketplaceFetchOptions {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            network_policy: None,
        }
    }
}

/// Request data for one hop. A public-policy request carries a DNS pin that
/// the transport must apply and must not bypass through a proxy or redirect.
pub struct MarketplaceHopRequest<'a> {
    pub url: &'a Url,
    pub pin: Option<&'a NetworkPin>,
    pub headers: &'a HeaderMap,
    pub timeout: Duration,
    pub max_body_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketplaceHopResponse {
    pub status: u16,
    pub location: Option<String>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketplaceTransportError {
    Request(String),
    ResponseTooLarge { limit: usize },
    BodyAllocationFailed,
}

pub type MarketplaceHopFuture<'a> = Pin<
    Box<dyn Future<Output = Result<MarketplaceHopResponse, MarketplaceTransportError>> + Send + 'a>,
>;

/// One-hop HTTP seam. Implementations must disable automatic redirects, apply
/// `pin` when present, and stop streaming once `max_body_bytes` is exceeded.
pub trait MarketplaceTransport: Send + Sync {
    fn get<'a>(&'a self, request: MarketplaceHopRequest<'a>) -> MarketplaceHopFuture<'a>;
}

/// Reqwest transport that pins public hosts and opts out of proxy variables
/// whenever a DNS pin is supplied. Every request disables automatic redirects.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReqwestMarketplaceTransport;

impl MarketplaceTransport for ReqwestMarketplaceTransport {
    fn get<'a>(&'a self, request: MarketplaceHopRequest<'a>) -> MarketplaceHopFuture<'a> {
        Box::pin(async move {
            let mut builder = Client::builder()
                .redirect(Policy::none())
                .timeout(request.timeout);
            if let Some(pin) = request.pin {
                builder = pin.apply_to_client_builder(builder).no_proxy();
            }
            let client = builder.build().map_err(|error| {
                MarketplaceTransportError::Request(redact_url_credentials(&error.to_string()))
            })?;
            let response = client
                .get(request.url.clone())
                .headers(request.headers.clone())
                .send()
                .await
                .map_err(|error| {
                    MarketplaceTransportError::Request(redact_url_credentials(&error.to_string()))
                })?;

            let status = response.status();
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            if is_redirect(status.as_u16()) || status != StatusCode::OK {
                return Ok(MarketplaceHopResponse {
                    status: status.as_u16(),
                    location,
                    body: Vec::new(),
                });
            }

            if response
                .content_length()
                .is_some_and(|length| length > request.max_body_bytes as u64)
            {
                return Err(MarketplaceTransportError::ResponseTooLarge {
                    limit: request.max_body_bytes,
                });
            }

            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| {
                    MarketplaceTransportError::Request(redact_url_credentials(&error.to_string()))
                })?;
                if body.len().saturating_add(chunk.len()) > request.max_body_bytes {
                    return Err(MarketplaceTransportError::ResponseTooLarge {
                        limit: request.max_body_bytes,
                    });
                }
                body.try_reserve(chunk.len())
                    .map_err(|_| MarketplaceTransportError::BodyAllocationFailed)?;
                body.extend_from_slice(&chunk);
            }

            Ok(MarketplaceHopResponse {
                status: status.as_u16(),
                location,
                body,
            })
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MarketplaceFetchError {
    #[error("Invalid marketplace URL: {0}")]
    InvalidUrl(String),
    #[error("Marketplace network policy rejected the request: {0}")]
    NetworkPolicy(String),
    #[error("Marketplace request timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    #[error("Marketplace request failed: {0}")]
    Request(String),
    #[error("Marketplace redirect location is missing")]
    MissingRedirectLocation,
    #[error("Marketplace redirect location is invalid: {0}")]
    InvalidRedirectLocation(String),
    #[error("Too many marketplace redirects (limit {0})")]
    TooManyRedirects(usize),
    #[error("Marketplace response exceeded the {0}-byte limit")]
    ResponseTooLarge(usize),
    #[error("Could not allocate the marketplace response body")]
    BodyAllocationFailed,
    #[error("Marketplace fetch was cancelled: {0:?}")]
    Cancelled(Option<CancellationReason>),
    #[error("Invalid marketplace request header: {0}")]
    InvalidHeader(String),
}

/// Fetch one remote marketplace document. Non-200 responses return `None`,
/// matching the TypeScript helper's null result. Redirect destinations are
/// parsed and policy-checked before each subsequent request.
pub async fn fetch_marketplace_text<R, T>(
    source: &str,
    headers: &HeaderMap,
    options: &MarketplaceFetchOptions,
    resolver: &R,
    transport: &T,
    cancellation: Option<&CancellationToken>,
) -> Result<Option<String>, MarketplaceFetchError>
where
    R: ExtensionAddressResolver + ?Sized,
    T: MarketplaceTransport + ?Sized,
{
    let initial = parse_http_url(source)?;
    let deadline = tokio::time::Instant::now() + options.timeout;
    let timeout_ms = options.timeout.as_millis().min(u128::from(u64::MAX)) as u64;
    let mut current_url = initial;
    let mut request_headers = headers.clone();

    for redirect_count in 0..=options.max_redirects {
        let resolved = cancellable(
            cancellation,
            tokio::time::timeout_at(
                deadline,
                resolve_network_target(
                    current_url.as_str(),
                    options.network_policy,
                    resolver,
                    cancellation,
                ),
            ),
        )
        .await?
        .map_err(|_| MarketplaceFetchError::Timeout { timeout_ms })?
        .map_err(|error| {
            MarketplaceFetchError::NetworkPolicy(redact_url_credentials(&error.to_string()))
        })?;

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(MarketplaceFetchError::Timeout { timeout_ms });
        }
        let response = cancellable(
            cancellation,
            tokio::time::timeout_at(
                deadline,
                transport.get(MarketplaceHopRequest {
                    url: &resolved.url,
                    pin: resolved.pin.as_ref(),
                    headers: &request_headers,
                    timeout: remaining,
                    max_body_bytes: options.max_body_bytes,
                }),
            ),
        )
        .await?
        .map_err(|_| MarketplaceFetchError::Timeout { timeout_ms })?
        .map_err(map_transport_error)?;

        if response.body.len() > options.max_body_bytes {
            return Err(MarketplaceFetchError::ResponseTooLarge(
                options.max_body_bytes,
            ));
        }

        if is_redirect(response.status) {
            if redirect_count == options.max_redirects {
                return Err(MarketplaceFetchError::TooManyRedirects(
                    options.max_redirects,
                ));
            }
            let location = response
                .location
                .ok_or(MarketplaceFetchError::MissingRedirectLocation)?;
            let next_url = resolved.url.join(&location).map_err(|_| {
                MarketplaceFetchError::InvalidRedirectLocation(redact_url_credentials(&location))
            })?;
            validate_http_url(&next_url)?;
            if !same_origin(&resolved.url, &next_url) {
                remove_sensitive_headers(&mut request_headers);
            }
            current_url = next_url;
            continue;
        }

        if response.status != StatusCode::OK.as_u16() {
            return Ok(None);
        }
        return Ok(Some(String::from_utf8_lossy(&response.body).into_owned()));
    }

    Err(MarketplaceFetchError::TooManyRedirects(
        options.max_redirects,
    ))
}

/// Load marketplace JSON text from a GitHub source or direct HTTP URL.
/// GitHub repositories use the API first and raw.githubusercontent.com as a
/// fallback; `github_token` is supplied by the host to avoid reading ambient
/// credentials inside this module.
pub async fn fetch_marketplace_text_from_source<R, T>(
    source: &str,
    github_token: Option<&str>,
    options: &MarketplaceFetchOptions,
    resolver: &R,
    transport: &T,
    cancellation: Option<&CancellationToken>,
) -> Result<Option<String>, MarketplaceFetchError>
where
    R: ExtensionAddressResolver + ?Sized,
    T: MarketplaceTransport + ?Sized,
{
    let trimmed = source.trim();
    let parsed = match parse_install_source(trimmed, false) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(None),
    };

    if parsed.install_type == InstallSourceType::Git {
        if let Some(repository) = github_repository_for_source(&parsed.source) {
            let api_url = format!(
                "https://api.github.com/repos/{}/{}/contents/.claude-plugin/marketplace.json",
                repository.owner, repository.repo
            );
            let mut api_headers = marketplace_headers();
            api_headers.insert(
                ACCEPT,
                HeaderValue::from_static("application/vnd.github.v3.raw"),
            );
            if let Some(token) = github_token {
                let value = HeaderValue::from_str(&format!("token {token}"))
                    .map_err(|_| MarketplaceFetchError::InvalidHeader("authorization".into()))?;
                api_headers.insert(AUTHORIZATION, value);
            }
            if let Some(content) = fetch_marketplace_text(
                &api_url,
                &api_headers,
                options,
                resolver,
                transport,
                cancellation,
            )
            .await?
            {
                return Ok(Some(content));
            }

            let raw_url = format!(
                "https://raw.githubusercontent.com/{}/{}/HEAD/.claude-plugin/marketplace.json",
                repository.owner, repository.repo
            );
            let raw_headers = marketplace_headers();
            if let Some(content) = fetch_marketplace_text(
                &raw_url,
                &raw_headers,
                options,
                resolver,
                transport,
                cancellation,
            )
            .await?
            {
                return Ok(Some(content));
            }
        }
    }

    if is_http_source(trimmed) {
        let headers = marketplace_headers();
        return fetch_marketplace_text(
            &parsed.source,
            &headers,
            options,
            resolver,
            transport,
            cancellation,
        )
        .await;
    }
    Ok(None)
}

fn github_repository_for_source(
    source: &str,
) -> Option<crate::extension_install_source::GitHubRepository> {
    if source.to_ascii_lowercase().starts_with("git@github.com:") {
        let (host, path) = source.split_once(':')?;
        if !host.eq_ignore_ascii_case("git@github.com") {
            return None;
        }
        let mut parts = path.split('/');
        let owner = parts.next()?;
        let repo = parts.next()?;
        let repo = repo.strip_suffix(".git").unwrap_or(repo);
        let valid_component = |value: &str| {
            !value.is_empty()
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.-".contains(&byte))
        };
        if !valid_component(owner) || !valid_component(repo) || parts.next().is_some() {
            return None;
        }
        return Some(crate::extension_install_source::GitHubRepository {
            owner: owner.to_owned(),
            repo: repo.to_owned(),
        });
    }
    parse_github_repo_for_releases(source).ok()
}

fn marketplace_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("canopy-code"));
    headers
}

fn parse_http_url(source: &str) -> Result<Url, MarketplaceFetchError> {
    let url = Url::parse(source).map_err(|error| {
        MarketplaceFetchError::InvalidUrl(redact_url_credentials(&error.to_string()))
    })?;
    validate_http_url(&url)?;
    Ok(url)
}

fn validate_http_url(url: &Url) -> Result<(), MarketplaceFetchError> {
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MarketplaceFetchError::InvalidUrl(redact_url_credentials(
            url.as_str(),
        )));
    }
    Ok(())
}

fn is_http_source(source: &str) -> bool {
    let lower = source.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn remove_sensitive_headers(headers: &mut HeaderMap) {
    headers.remove(AUTHORIZATION);
    headers.remove(PROXY_AUTHORIZATION);
    headers.remove("cookie");
    headers.remove("host");
}

fn is_redirect(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn map_transport_error(error: MarketplaceTransportError) -> MarketplaceFetchError {
    match error {
        MarketplaceTransportError::Request(message) => MarketplaceFetchError::Request(message),
        MarketplaceTransportError::ResponseTooLarge { limit } => {
            MarketplaceFetchError::ResponseTooLarge(limit)
        }
        MarketplaceTransportError::BodyAllocationFailed => {
            MarketplaceFetchError::BodyAllocationFailed
        }
    }
}

async fn cancellable<F, T>(
    cancellation: Option<&CancellationToken>,
    future: F,
) -> Result<T, MarketplaceFetchError>
where
    F: Future<Output = T>,
{
    if let Some(cancellation) = cancellation {
        if cancellation.is_cancelled() {
            return Err(MarketplaceFetchError::Cancelled(cancellation.reason()));
        }
        tokio::select! {
            biased;
            reason = cancellation.cancelled() => {
                Err(MarketplaceFetchError::Cancelled(reason))
            }
            result = future => Ok(result),
        }
    } else {
        Ok(future.await)
    }
}
