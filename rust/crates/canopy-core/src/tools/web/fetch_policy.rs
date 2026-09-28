//! Bounded HTTP transport policy for the WebFetch tool.
//!
//! Redirects are handled manually so a request only follows redirects that
//! preserve the approved origin. The transfer has one timeout budget across
//! redirects, retries, response headers, and streamed body bytes. Dropping the
//! returned future cancels the in-flight request and body stream.

use std::error::Error as StdError;
use std::io;
use std::time::Duration;

use futures_util::StreamExt;
use reqwest::header::{
    CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, LOCATION,
};
use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode, Url};

use super::validate_web_fetch_url;

const RETRYABLE_STATUSES: &[u16] = &[403, 429];
const RETRY_DELAY: Duration = Duration::from_millis(500);

/// Limits and headers for one WebFetch transfer.
#[derive(Clone, Debug)]
pub struct FetchPolicyOptions {
    /// Budget for request, redirects, retries, and body transfer as one unit.
    pub timeout: Duration,
    /// Maximum response-body size in bytes.
    pub max_bytes: usize,
    /// Maximum number of same-origin redirects to follow.
    pub max_redirects: usize,
    /// Request headers. Header names are case-insensitive.
    pub headers: Vec<(String, String)>,
}

impl Default for FetchPolicyOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            max_bytes: 10 * 1024 * 1024,
            max_redirects: 10,
            headers: Vec::new(),
        }
    }
}

/// A completed HTTP response. Non-2xx responses have an empty body because
/// WebFetch only uses their status and headers for the error result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPolicyResponse {
    pub status: u16,
    pub status_text: String,
    pub content_type: String,
    pub content_disposition: String,
    pub body: Vec<u8>,
    /// URL after any permitted same-origin redirects.
    pub final_url: String,
}

/// Result of an HTTP request that either completed or reached a redirect
/// outside the origin approved by the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchPolicyResult {
    Response(FetchPolicyResponse),
    CrossOriginRedirect {
        original_url: String,
        redirect_url: String,
        status: u16,
    },
}

/// Failures produced by WebFetch's bounded HTTP transport.
#[derive(Debug, thiserror::Error)]
pub enum FetchPolicyError {
    #[error("The 'url' is invalid: {0}")]
    InvalidUrl(String),
    #[error("Request timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
    #[error("HTTP request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("Invalid HTTP header name: {0}")]
    InvalidHeaderName(String),
    #[error("Invalid HTTP header value")]
    InvalidHeaderValue,
    #[error("Redirect response missing Location header")]
    MissingLocation,
    #[error("Redirect response has a malformed Location header: {0}")]
    MalformedLocation(String),
    #[error("Too many redirects (exceeded {0})")]
    TooManyRedirects(usize),
    #[error("Response too large: {size} bytes exceeds the {limit}-byte limit")]
    ContentLengthTooLarge { size: u64, limit: usize },
    #[error("Response too large: exceeded the {0}-byte limit while streaming")]
    StreamingBodyTooLarge(usize),
    #[error("Could not allocate memory for the bounded response body")]
    BodyAllocationFailed,
}

/// A reusable client with automatic redirects disabled.
///
/// Keep one instance per process or runtime to reuse connection pools. The
/// default reqwest proxy settings are retained, matching the process-wide
/// proxy behavior expected by WebFetch callers.
#[derive(Clone, Debug)]
pub struct FetchPolicyClient {
    client: Client,
}

impl FetchPolicyClient {
    /// Create a pooled client that leaves redirect handling to this module.
    pub fn new() -> Result<Self, reqwest::Error> {
        Self::new_with_proxy(None)
    }

    /// Create a pooled client with the process proxy defaults or a configured
    /// proxy override. `NO_PROXY` remains active when an explicit proxy is
    /// supplied, so local MCP and development services continue to connect
    /// directly.
    pub fn new_with_proxy(proxy_url: Option<&str>) -> Result<Self, reqwest::Error> {
        let mut builder = Client::builder().redirect(Policy::none());
        if let Some(proxy_url) = proxy_url.filter(|value| !value.trim().is_empty()) {
            let proxy = reqwest::Proxy::all(proxy_url)?.no_proxy(reqwest::NoProxy::from_env());
            builder = builder.proxy(proxy);
        }
        if tls_verification_disabled() {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder.build()?;
        Ok(Self { client })
    }

    /// Fetch a URL with bounded transfer size, timeout, redirects, and retry.
    ///
    /// Caller cancellation is supported by dropping this future. Tokio drops
    /// the request/body stream and reqwest cancels the network operation.
    pub async fn fetch(
        &self,
        url: &str,
        options: &FetchPolicyOptions,
    ) -> Result<FetchPolicyResult, FetchPolicyError> {
        validate_web_fetch_url(url)
            .map_err(|error| FetchPolicyError::InvalidUrl(error.to_owned()))?;
        let initial_url =
            Url::parse(url).map_err(|error| FetchPolicyError::InvalidUrl(error.to_string()))?;
        let headers = build_headers(&options.headers)?;
        let timeout_ms = options.timeout.as_millis().min(u128::from(u64::MAX)) as u64;

        match tokio::time::timeout(
            options.timeout,
            self.fetch_with_retries(initial_url, headers, options),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(FetchPolicyError::Timeout { timeout_ms }),
        }
    }

    async fn fetch_with_retries(
        &self,
        initial_url: Url,
        headers: HeaderMap,
        options: &FetchPolicyOptions,
    ) -> Result<FetchPolicyResult, FetchPolicyError> {
        let first = match self
            .fetch_attempt(initial_url.clone(), headers.clone(), options)
            .await
        {
            Ok(result) => result,
            Err(error) if is_retryable_request_error(&error) => {
                tokio::time::sleep(RETRY_DELAY).await;
                return self.fetch_attempt(initial_url, headers, options).await;
            }
            Err(error) => return Err(error),
        };

        let should_retry = matches!(
            &first,
            FetchPolicyResult::Response(response)
                if RETRYABLE_STATUSES.contains(&response.status)
        );
        if !should_retry {
            return Ok(first);
        }

        tokio::time::sleep(RETRY_DELAY).await;
        // The original 403/429 remains the most useful result if a status
        // retry fails. The outer timeout still takes precedence because it
        // wraps this whole operation.
        Ok(self
            .fetch_attempt(initial_url, headers, options)
            .await
            .unwrap_or(first))
    }

    async fn fetch_attempt(
        &self,
        initial_url: Url,
        headers: HeaderMap,
        options: &FetchPolicyOptions,
    ) -> Result<FetchPolicyResult, FetchPolicyError> {
        let mut current_url = initial_url;
        for hop in 0..=options.max_redirects {
            let response = self
                .client
                .get(current_url.clone())
                .headers(headers.clone())
                .send()
                .await?;
            let status = response.status();

            if is_redirect_status(status) {
                let location = response
                    .headers()
                    .get(LOCATION)
                    .ok_or(FetchPolicyError::MissingLocation)?
                    .to_str()
                    .map_err(|_| FetchPolicyError::MalformedLocation(String::new()))?;
                let redirect_url = current_url
                    .join(location)
                    .map_err(|_| FetchPolicyError::MalformedLocation(location.to_owned()))?;

                if !is_permitted_redirect(&current_url, &redirect_url) {
                    let result = FetchPolicyResult::CrossOriginRedirect {
                        original_url: current_url.to_string(),
                        redirect_url: redirect_url.to_string(),
                        status: status.as_u16(),
                    };
                    drop(response);
                    return Ok(result);
                }

                drop(response);
                if hop == options.max_redirects {
                    break;
                }
                current_url = redirect_url;
                continue;
            }

            let status_text = status.canonical_reason().unwrap_or_default().to_owned();
            let content_type = header_value(response.headers(), CONTENT_TYPE);
            let content_disposition = header_value(response.headers(), CONTENT_DISPOSITION);
            if !status.is_success() {
                let result = FetchPolicyResponse {
                    status: status.as_u16(),
                    status_text,
                    content_type,
                    content_disposition,
                    body: Vec::new(),
                    final_url: current_url.to_string(),
                };
                drop(response);
                return Ok(FetchPolicyResult::Response(result));
            }

            let declared_length = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            if let Some(size) = declared_length {
                if size > options.max_bytes as u64 {
                    drop(response);
                    return Err(FetchPolicyError::ContentLengthTooLarge {
                        size,
                        limit: options.max_bytes,
                    });
                }
            }

            let reserve = declared_length
                .map(|size| size.min(options.max_bytes as u64) as usize)
                .unwrap_or(0);
            let mut body = Vec::new();
            body.try_reserve(reserve)
                .map_err(|_| FetchPolicyError::BodyAllocationFailed)?;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                let next_size = body.len().checked_add(chunk.len()).unwrap_or(usize::MAX);
                if next_size > options.max_bytes {
                    return Err(FetchPolicyError::StreamingBodyTooLarge(options.max_bytes));
                }
                body.try_reserve(chunk.len())
                    .map_err(|_| FetchPolicyError::BodyAllocationFailed)?;
                body.extend_from_slice(&chunk);
            }

            return Ok(FetchPolicyResult::Response(FetchPolicyResponse {
                status: status.as_u16(),
                status_text,
                content_type,
                content_disposition,
                body,
                final_url: current_url.to_string(),
            }));
        }
        Err(FetchPolicyError::TooManyRedirects(options.max_redirects))
    }
}

fn tls_verification_disabled() -> bool {
    if std::env::var("CANOPY_TLS_INSECURE").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        return true;
    }
    std::env::var("NODE_TLS_REJECT_UNAUTHORIZED").is_ok_and(|value| value == "0")
}

fn build_headers(values: &[(String, String)]) -> Result<HeaderMap, FetchPolicyError> {
    let mut headers = HeaderMap::new();
    for (name, value) in values {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| FetchPolicyError::InvalidHeaderName(name.clone()))?;
        let value =
            HeaderValue::from_str(value).map_err(|_| FetchPolicyError::InvalidHeaderValue)?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn header_value(headers: &HeaderMap, name: reqwest::header::HeaderName) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

fn is_redirect_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

/// Check the same-host, same-scheme, same-port redirect rule used by WebFetch.
/// A leading www. may be added or removed; credentials are never permitted.
pub fn is_permitted_redirect(original: &Url, redirect: &Url) -> bool {
    if original.scheme() != redirect.scheme() || original.port() != redirect.port() {
        return false;
    }
    if !redirect.username().is_empty()
        || redirect
            .password()
            .is_some_and(|password| !password.is_empty())
    {
        return false;
    }
    let original_host = original.host_str().unwrap_or_default();
    let redirect_host = redirect.host_str().unwrap_or_default();
    strip_www(original_host).eq_ignore_ascii_case(strip_www(redirect_host))
}

fn strip_www(host: &str) -> &str {
    host.strip_prefix("www.")
        .or_else(|| host.strip_prefix("WWW."))
        .unwrap_or(host)
}

fn is_retryable_request_error(error: &FetchPolicyError) -> bool {
    let FetchPolicyError::Request(error) = error else {
        return false;
    };
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(io_error) = cause.downcast_ref::<io::Error>() {
            if matches!(
                io_error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
                    | io::ErrorKind::BrokenPipe
            ) {
                return true;
            }
        }
        source = cause.source();
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("connection reset")
        || message.contains("temporary failure in name resolution")
        || message.contains("temporarily unavailable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn server_with_responses(responses: Vec<&'static str>) -> String {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind local HTTP server");
        let address = listener.local_addr().expect("local server address");
        let _server = tokio::spawn(async move {
            for response in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut request = [0_u8; 4096];
                let _ = stream.read(&mut request).await;
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        format!("http://{address}/")
    }

    #[tokio::test]
    async fn returns_success_body_and_response_metadata() {
        let base = server_with_responses(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=note.txt\r\nConnection: close\r\n\r\nhello",
        ])
        .await;
        let client = FetchPolicyClient::new().expect("HTTP client");
        let result = client
            .fetch(&base, &FetchPolicyOptions::default())
            .await
            .expect("fetch");

        assert_eq!(
            result,
            FetchPolicyResult::Response(FetchPolicyResponse {
                status: 200,
                status_text: "OK".to_owned(),
                content_type: "text/plain".to_owned(),
                content_disposition: "attachment; filename=note.txt".to_owned(),
                body: b"hello".to_vec(),
                final_url: base,
            })
        );
    }

    #[tokio::test]
    async fn follows_same_origin_redirects() {
        let base = server_with_responses(vec![
            "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        ])
        .await;
        let client = FetchPolicyClient::new().expect("HTTP client");
        let result = client
            .fetch(&base, &FetchPolicyOptions::default())
            .await
            .expect("fetch");

        let FetchPolicyResult::Response(response) = result else {
            panic!("expected final response");
        };
        assert_eq!(response.body, b"ok".to_vec());
        assert_eq!(response.final_url, format!("{}final", base));
    }

    #[tokio::test]
    async fn reports_cross_origin_redirect_without_following_it() {
        let base = server_with_responses(vec![
            "HTTP/1.1 301 Moved Permanently\r\nLocation: https://elsewhere.example/path\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ])
        .await;
        let client = FetchPolicyClient::new().expect("HTTP client");
        let result = client
            .fetch(&base, &FetchPolicyOptions::default())
            .await
            .expect("fetch");

        assert_eq!(
            result,
            FetchPolicyResult::CrossOriginRedirect {
                original_url: base,
                redirect_url: "https://elsewhere.example/path".to_owned(),
                status: 301,
            }
        );
    }

    #[tokio::test]
    async fn rejects_declared_and_streamed_oversized_bodies() {
        let declared = server_with_responses(vec![
            "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n",
        ])
        .await;
        let client = FetchPolicyClient::new().expect("HTTP client");
        let options = FetchPolicyOptions {
            max_bytes: 5,
            ..FetchPolicyOptions::default()
        };
        assert!(matches!(
            client.fetch(&declared, &options).await,
            Err(FetchPolicyError::ContentLengthTooLarge { size: 6, limit: 5 })
        ));

        let streamed = server_with_responses(vec![
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n6\r\n123456\r\n0\r\n\r\n",
        ])
        .await;
        assert!(matches!(
            client.fetch(&streamed, &options).await,
            Err(FetchPolicyError::StreamingBodyTooLarge(5))
        ));
    }

    #[tokio::test]
    async fn retries_a_transient_rate_limit_once() {
        let base = server_with_responses(vec![
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        ])
        .await;
        let client = FetchPolicyClient::new().expect("HTTP client");
        let result = client
            .fetch(&base, &FetchPolicyOptions::default())
            .await
            .expect("fetch");

        let FetchPolicyResult::Response(response) = result else {
            panic!("expected final response");
        };
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"ok".to_vec());
    }

    #[tokio::test]
    async fn applies_one_timeout_to_the_entire_transfer() {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind local HTTP server");
        let address = listener.local_addr().expect("local server address");
        let _server = tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
                .await;
            tokio::time::sleep(Duration::from_millis(150)).await;
            let _ = stream.write_all(b"ok").await;
        });

        let client = FetchPolicyClient::new().expect("HTTP client");
        let options = FetchPolicyOptions {
            timeout: Duration::from_millis(25),
            ..FetchPolicyOptions::default()
        };
        assert!(matches!(
            client.fetch(&format!("http://{address}/"), &options).await,
            Err(FetchPolicyError::Timeout { timeout_ms: 25 })
        ));
    }
}
