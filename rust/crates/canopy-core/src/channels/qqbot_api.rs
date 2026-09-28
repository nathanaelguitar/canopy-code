//! QQ Bot HTTP API client.
//!
//! Port of `packages/channels/qqbot/src/api.ts`. Requests use fixed QQ Bot
//! endpoints and a per-request timeout; the transport seam keeps requests,
//! response-body draining, and timeout behavior independently testable.

use reqwest::Client;
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Method, Url};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const TOKEN_URL: &str = "https://bots.qq.com/app/getAppAccessToken";
const API_HOST: &str = "https://api.sgroup.qq.com";
const SANDBOX_HOST: &str = "https://sandbox.api.sgroup.qq.com";
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(15);

pub type QqbotFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq)]
pub struct TokenResponse {
    pub access_token: String,
    pub expires_in: f64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QqbotApiError {
    pub message: String,
}

impl QqbotApiError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for QqbotApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for QqbotApiError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QqbotHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
}

/// Streaming response-body seam. Error paths cancel without parsing or
/// exposing the response body; success paths consume it as bytes.
pub trait QqbotResponseBody: Send {
    fn read_all<'a>(&'a mut self) -> QqbotFuture<'a, Result<Vec<u8>, QqbotApiError>>;
    fn cancel<'a>(&'a mut self) -> QqbotFuture<'a, ()>;
}

/// HTTP response returned by the injected transport. `send_qq_message` returns
/// this without reading the body, matching the source's raw `Response` result.
pub struct QqbotHttpResponse {
    pub status: u16,
    body: Option<Box<dyn QqbotResponseBody>>,
}

impl QqbotHttpResponse {
    pub fn new(status: u16, body: impl QqbotResponseBody + 'static) -> Self {
        Self {
            status,
            body: Some(Box::new(body)),
        }
    }

    pub fn from_bytes(status: u16, body: Vec<u8>) -> Self {
        Self::new(status, BufferedResponseBody { bytes: Some(body) })
    }

    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub async fn read_body(&mut self) -> Result<Vec<u8>, QqbotApiError> {
        let Some(mut body) = self.body.take() else {
            return Ok(Vec::new());
        };
        body.read_all().await
    }

    pub async fn json<T: DeserializeOwned>(&mut self) -> Result<T, QqbotApiError> {
        let body = self.read_body().await?;
        serde_json::from_slice(&body)
            .map_err(|error| QqbotApiError::new(format!("Invalid QQ Bot JSON response: {error}")))
    }

    pub async fn cancel_body(&mut self) {
        if let Some(mut body) = self.body.take() {
            body.cancel().await;
        }
    }
}

pub trait QqbotHttpTransport: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: QqbotHttpRequest,
    ) -> QqbotFuture<'a, Result<QqbotHttpResponse, QqbotApiError>>;
}

/// Reqwest-backed production implementation of [`QqbotHttpTransport`].
pub struct ReqwestQqbotHttpTransport<'a> {
    client: &'a Client,
}

impl<'a> ReqwestQqbotHttpTransport<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }
}

impl QqbotHttpTransport for ReqwestQqbotHttpTransport<'_> {
    fn execute<'a>(
        &'a self,
        request: QqbotHttpRequest,
    ) -> QqbotFuture<'a, Result<QqbotHttpResponse, QqbotApiError>> {
        Box::pin(async move {
            let method = Method::from_bytes(request.method.as_bytes())
                .map_err(|error| QqbotApiError::new(error.to_string()))?;
            let mut builder = self
                .client
                .request(method, &request.url)
                .timeout(request.timeout);
            for (name, value) in request.headers {
                let name = HeaderName::from_bytes(name.as_bytes())
                    .map_err(|error| QqbotApiError::new(error.to_string()))?;
                let value = HeaderValue::from_str(&value)
                    .map_err(|error| QqbotApiError::new(error.to_string()))?;
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }

            let response = builder
                .send()
                .await
                .map_err(|error| QqbotApiError::new(error.to_string()))?;
            let status = response.status().as_u16();
            Ok(QqbotHttpResponse::new(
                status,
                ReqwestResponseBody {
                    response: Some(response),
                },
            ))
        })
    }
}

struct ReqwestResponseBody {
    response: Option<reqwest::Response>,
}

impl QqbotResponseBody for ReqwestResponseBody {
    fn read_all<'a>(&'a mut self) -> QqbotFuture<'a, Result<Vec<u8>, QqbotApiError>> {
        Box::pin(async move {
            let response = self
                .response
                .take()
                .ok_or_else(|| QqbotApiError::new("QQ Bot response body was already consumed"))?;
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| QqbotApiError::new(error.to_string()))
        })
    }

    fn cancel<'a>(&'a mut self) -> QqbotFuture<'a, ()> {
        Box::pin(async move {
            // Dropping reqwest's streaming response aborts the body stream and
            // releases the connection just as ReadableStream.cancel() does.
            drop(self.response.take());
        })
    }
}

struct BufferedResponseBody {
    bytes: Option<Vec<u8>>,
}

impl QqbotResponseBody for BufferedResponseBody {
    fn read_all<'a>(&'a mut self) -> QqbotFuture<'a, Result<Vec<u8>, QqbotApiError>> {
        Box::pin(async move {
            self.bytes
                .take()
                .ok_or_else(|| QqbotApiError::new("QQ Bot response body was already consumed"))
        })
    }

    fn cancel<'a>(&'a mut self) -> QqbotFuture<'a, ()> {
        Box::pin(async move {
            drop(self.bytes.take());
        })
    }
}

/// Obtain an access token using the standard 15-second request timeout.
pub async fn fetch_access_token(
    client: &Client,
    app_id: &str,
    app_secret: &str,
) -> Result<TokenResponse, QqbotApiError> {
    fetch_access_token_with_transport(
        &ReqwestQqbotHttpTransport::new(client),
        FETCH_TIMEOUT,
        app_id,
        app_secret,
    )
    .await
}

/// Injectable variant of [`fetch_access_token`]. The timeout is part of the
/// request passed to the transport, allowing tests to inspect it without
/// waiting on a real deadline.
pub async fn fetch_access_token_with_transport(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    app_id: &str,
    app_secret: &str,
) -> Result<TokenResponse, QqbotApiError> {
    let body = serde_json::to_vec(&json!({
        "appId": app_id,
        "clientSecret": app_secret,
    }))
    .map_err(|error| QqbotApiError::new(error.to_string()))?;
    let request = QqbotHttpRequest {
        method: "POST".to_owned(),
        url: TOKEN_URL.to_owned(),
        headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
        body: Some(body),
        timeout,
    };
    let mut response = transport.execute(request).await?;

    if !response.ok() {
        let status = response.status;
        response.cancel_body().await;
        eprintln!("[QQ] Token request failed (HTTP {status})");
        return Err(QqbotApiError::new(format!(
            "QQ Bot token request failed (HTTP {status})"
        )));
    }

    let data: Value = response.json().await?;
    let access_token = data
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| QqbotApiError::new("QQ Bot token response missing access_token"))?;
    let expires_in = data
        .get("expires_in")
        .filter(|value| !value.is_null())
        .and_then(Value::as_f64)
        .unwrap_or(7200.0);
    Ok(TokenResponse {
        access_token: access_token.to_owned(),
        expires_in,
    })
}

/// Validate a gateway URL, requiring `wss:` and a hostname ending in
/// `.qq.com`. User information is stripped from the returned canonical URL.
pub fn validate_gateway_url(url: &str) -> Result<String, QqbotApiError> {
    let mut parsed =
        Url::parse(url).map_err(|_| QqbotApiError::new("QQ Bot gateway URL is not a valid URL"))?;
    if parsed.scheme() != "wss" {
        return Err(QqbotApiError::new(format!(
            "QQ Bot gateway URL must use wss:// protocol, got: {}:",
            parsed.scheme()
        )));
    }
    let hostname = parsed.host_str().unwrap_or_default();
    if !hostname.to_ascii_lowercase().ends_with(".qq.com") {
        return Err(QqbotApiError::new(format!(
            "QQ Bot gateway URL has unexpected hostname: {hostname} (expected *.qq.com)"
        )));
    }
    parsed
        .set_username("")
        .map_err(|_| QqbotApiError::new("QQ Bot gateway URL is not a valid URL"))?;
    parsed
        .set_password(None)
        .map_err(|_| QqbotApiError::new("QQ Bot gateway URL is not a valid URL"))?;
    Ok(parsed.to_string())
}

/// Resolve the gateway with the standard 15-second request timeout.
pub async fn fetch_gateway_url(
    client: &Client,
    access_token: &str,
    sandbox: bool,
) -> Result<String, QqbotApiError> {
    fetch_gateway_url_with_transport(
        &ReqwestQqbotHttpTransport::new(client),
        FETCH_TIMEOUT,
        access_token,
        sandbox,
    )
    .await
}

/// Injectable variant of [`fetch_gateway_url`].
pub async fn fetch_gateway_url_with_transport(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    access_token: &str,
    sandbox: bool,
) -> Result<String, QqbotApiError> {
    let host = if sandbox { SANDBOX_HOST } else { API_HOST };
    let request = QqbotHttpRequest {
        method: "GET".to_owned(),
        url: format!("{host}/gateway"),
        headers: vec![("Authorization".to_owned(), format!("QQBot {access_token}"))],
        body: None,
        timeout,
    };
    let mut response = transport.execute(request).await?;

    if !response.ok() {
        let status = response.status;
        response.cancel_body().await;
        return Err(QqbotApiError::new(format!(
            "QQ Bot gateway request failed (HTTP {status})"
        )));
    }

    let data: Value = response.json().await?;
    let url = data
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| QqbotApiError::new("QQ Bot gateway response missing WebSocket URL"))?;
    validate_gateway_url(url)
}

/// Determine the API base URL from the sandbox flag.
pub fn get_api_base(sandbox: bool) -> &'static str {
    if sandbox { SANDBOX_HOST } else { API_HOST }
}

/// Send a message chunk using the standard 15-second request timeout. The
/// response is returned without consuming its body.
pub async fn send_qq_message(
    client: &Client,
    base: &str,
    path: &str,
    access_token: &str,
    body: &Value,
) -> Result<QqbotHttpResponse, QqbotApiError> {
    send_qq_message_with_transport(
        &ReqwestQqbotHttpTransport::new(client),
        FETCH_TIMEOUT,
        base,
        path,
        access_token,
        body,
    )
    .await
}

/// Injectable variant of [`send_qq_message`].
pub async fn send_qq_message_with_transport(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    base: &str,
    path: &str,
    access_token: &str,
    body: &Value,
) -> Result<QqbotHttpResponse, QqbotApiError> {
    let request = QqbotHttpRequest {
        method: "POST".to_owned(),
        url: format!("{base}{path}"),
        headers: vec![
            ("Content-Type".to_owned(), "application/json".to_owned()),
            ("Authorization".to_owned(), format!("QQBot {access_token}")),
        ],
        body: Some(
            serde_json::to_vec(body).map_err(|error| QqbotApiError::new(error.to_string()))?,
        ),
        timeout,
    };
    transport.execute(request).await
}

#[cfg(test)]
mod tests {
    use super::{
        FETCH_TIMEOUT, QqbotApiError, QqbotFuture, QqbotHttpRequest, QqbotHttpResponse,
        QqbotHttpTransport, QqbotResponseBody, TokenResponse, fetch_access_token_with_transport,
        fetch_gateway_url_with_transport, get_api_base, send_qq_message_with_transport,
        validate_gateway_url,
    };
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct MemoryBody {
        bytes: Option<Vec<u8>>,
        was_cancelled: Option<Arc<AtomicBool>>,
    }

    impl MemoryBody {
        fn new(bytes: impl Into<Vec<u8>>) -> Self {
            Self {
                bytes: Some(bytes.into()),
                was_cancelled: None,
            }
        }

        fn with_cancel_flag(bytes: impl Into<Vec<u8>>, flag: Arc<AtomicBool>) -> Self {
            Self {
                bytes: Some(bytes.into()),
                was_cancelled: Some(flag),
            }
        }
    }

    impl QqbotResponseBody for MemoryBody {
        fn read_all<'a>(&'a mut self) -> QqbotFuture<'a, Result<Vec<u8>, QqbotApiError>> {
            Box::pin(async move {
                self.bytes
                    .take()
                    .ok_or_else(|| QqbotApiError::new("body already consumed"))
            })
        }

        fn cancel<'a>(&'a mut self) -> QqbotFuture<'a, ()> {
            Box::pin(async move {
                self.bytes.take();
                if let Some(flag) = &self.was_cancelled {
                    flag.store(true, Ordering::SeqCst);
                }
            })
        }
    }

    struct FakeTransport {
        response: Mutex<Option<QqbotHttpResponse>>,
        requests: Mutex<Vec<QqbotHttpRequest>>,
    }

    impl FakeTransport {
        fn new(response: QqbotHttpResponse) -> Self {
            Self {
                response: Mutex::new(Some(response)),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn json(status: u16, body: Value) -> Self {
            Self::new(QqbotHttpResponse::new(
                status,
                MemoryBody::new(serde_json::to_vec(&body).unwrap()),
            ))
        }
    }

    impl QqbotHttpTransport for FakeTransport {
        fn execute<'a>(
            &'a self,
            request: QqbotHttpRequest,
        ) -> QqbotFuture<'a, Result<QqbotHttpResponse, QqbotApiError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                self.response
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| QqbotApiError::new("fake response missing"))
            })
        }
    }

    fn request(transport: &FakeTransport) -> QqbotHttpRequest {
        transport.requests.lock().unwrap()[0].clone()
    }

    #[test]
    fn api_base_uses_fixed_production_and_sandbox_hosts() {
        assert_eq!(get_api_base(false), "https://api.sgroup.qq.com");
        assert_eq!(get_api_base(true), "https://sandbox.api.sgroup.qq.com");
    }

    #[tokio::test]
    async fn access_token_request_has_exact_endpoint_headers_body_and_timeout() {
        let transport = FakeTransport::json(
            200,
            json!({ "access_token": "tok-abc", "expires_in": 3600 }),
        );
        let result = fetch_access_token_with_transport(
            &transport,
            Duration::from_millis(321),
            "app-id",
            "secret",
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            TokenResponse {
                access_token: "tok-abc".to_owned(),
                expires_in: 3600.0,
            }
        );
        let request = request(&transport);
        assert_eq!(request.method, "POST");
        assert_eq!(request.url, "https://bots.qq.com/app/getAppAccessToken");
        assert_eq!(
            request.headers,
            [("Content-Type".to_owned(), "application/json".to_owned())]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(request.body.as_ref().unwrap()).unwrap(),
            json!({ "appId": "app-id", "clientSecret": "secret" })
        );
        assert_eq!(request.timeout, Duration::from_millis(321));
        assert_eq!(FETCH_TIMEOUT, Duration::from_secs(15));
    }

    #[tokio::test]
    async fn access_token_defaults_expiration_and_rejects_missing_token() {
        let transport = FakeTransport::json(200, json!({ "access_token": "tok" }));
        let result =
            fetch_access_token_with_transport(&transport, Duration::from_secs(15), "app", "secret")
                .await
                .unwrap();
        assert_eq!(result.expires_in, 7200.0);

        let missing = FakeTransport::json(200, json!({ "expires_in": 9 }));
        let error =
            fetch_access_token_with_transport(&missing, Duration::from_secs(15), "app", "secret")
                .await
                .unwrap_err();
        assert_eq!(error.message, "QQ Bot token response missing access_token");
    }

    #[tokio::test]
    async fn access_token_http_error_is_status_only_and_cancels_body() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let transport = FakeTransport::new(QqbotHttpResponse::new(
            401,
            MemoryBody::with_cancel_flag(b"unauthorized secret body".to_vec(), cancelled.clone()),
        ));
        let error = fetch_access_token_with_transport(
            &transport,
            Duration::from_secs(15),
            "bad-app",
            "bad-secret",
        )
        .await
        .unwrap_err();
        assert_eq!(error.message, "QQ Bot token request failed (HTTP 401)");
        assert!(!error.message.contains("unauthorized"));
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn gateway_request_uses_environment_endpoint_header_and_timeout() {
        let transport = FakeTransport::json(200, json!({ "url": "wss://gateway.qq.com/ws" }));
        let result = fetch_gateway_url_with_transport(
            &transport,
            Duration::from_millis(512),
            "access-token",
            true,
        )
        .await
        .unwrap();
        assert_eq!(result, "wss://gateway.qq.com/ws");

        let request = request(&transport);
        assert_eq!(request.method, "GET");
        assert_eq!(request.url, "https://sandbox.api.sgroup.qq.com/gateway");
        assert_eq!(
            request.headers,
            [("Authorization".to_owned(), "QQBot access-token".to_owned())]
        );
        assert!(request.body.is_none());
        assert_eq!(request.timeout, Duration::from_millis(512));
    }

    #[tokio::test]
    async fn gateway_http_error_is_status_only_and_cancels_body() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let transport = FakeTransport::new(QqbotHttpResponse::new(
            502,
            MemoryBody::with_cancel_flag(b"backend secret body".to_vec(), cancelled.clone()),
        ));
        let error =
            fetch_gateway_url_with_transport(&transport, Duration::from_secs(15), "tok", false)
                .await
                .unwrap_err();
        assert_eq!(error.message, "QQ Bot gateway request failed (HTTP 502)");
        assert!(!error.message.contains("backend"));
        assert!(cancelled.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn gateway_rejects_missing_url_and_validates_returned_url() {
        let missing = FakeTransport::json(200, json!({}));
        let error =
            fetch_gateway_url_with_transport(&missing, Duration::from_secs(15), "tok", false)
                .await
                .unwrap_err();
        assert_eq!(
            error.message,
            "QQ Bot gateway response missing WebSocket URL"
        );

        for url in ["http://gateway.qq.com/ws", "ws://gateway.qq.com/ws"] {
            let transport = FakeTransport::json(200, json!({ "url": url }));
            let error =
                fetch_gateway_url_with_transport(&transport, Duration::from_secs(15), "tok", false)
                    .await
                    .unwrap_err();
            assert!(error.message.contains("must use wss:// protocol"));
        }
    }

    #[tokio::test]
    async fn send_message_posts_exact_json_headers_timeout_and_returns_raw_response() {
        let transport =
            FakeTransport::new(QqbotHttpResponse::from_bytes(201, b"raw response".to_vec()));
        let mut response = send_qq_message_with_transport(
            &transport,
            Duration::from_millis(765),
            "https://api.example.com",
            "/v2/users/abc/messages",
            "token-123",
            &json!({ "content": "hello", "msg_type": 0 }),
        )
        .await
        .unwrap();
        assert_eq!(response.status, 201);
        assert!(response.ok());
        assert_eq!(response.read_body().await.unwrap(), b"raw response");

        let request = request(&transport);
        assert_eq!(request.method, "POST");
        assert_eq!(request.url, "https://api.example.com/v2/users/abc/messages");
        assert_eq!(
            request.headers,
            [
                ("Content-Type".to_owned(), "application/json".to_owned()),
                ("Authorization".to_owned(), "QQBot token-123".to_owned()),
            ]
        );
        assert_eq!(
            serde_json::from_slice::<Value>(request.body.as_ref().unwrap()).unwrap(),
            json!({ "content": "hello", "msg_type": 0 })
        );
        assert_eq!(
            std::str::from_utf8(request.body.as_ref().unwrap()).unwrap(),
            r#"{"content":"hello","msg_type":0}"#
        );
        assert_eq!(request.timeout, Duration::from_millis(765));
    }

    #[test]
    fn gateway_validation_requires_wss_and_strict_qq_com_suffix() {
        assert_eq!(
            validate_gateway_url("wss://api.sgroup.qq.com/ws").unwrap(),
            "wss://api.sgroup.qq.com/ws"
        );
        assert_eq!(
            validate_gateway_url("wss://sandbox.gateway.qq.com/ws").unwrap(),
            "wss://sandbox.gateway.qq.com/ws"
        );

        for url in [
            "https://gateway.qq.com/ws",
            "http://gateway.qq.com/ws",
            "ws://gateway.qq.com/ws",
        ] {
            assert!(
                validate_gateway_url(url)
                    .unwrap_err()
                    .message
                    .contains("wss://")
            );
        }
        for url in [
            "wss://service-apigw.tencentcs.com/ws",
            "wss://malicious.tencent.com/ws",
            "wss://evil.example.com/ws",
            "wss://qq.com/ws",
            "wss://gateway.qq.com.evil.com/ws",
        ] {
            assert!(
                validate_gateway_url(url)
                    .unwrap_err()
                    .message
                    .contains("unexpected hostname"),
                "accepted {url}"
            );
        }
        assert_eq!(
            validate_gateway_url("wss://user:password@gateway.qq.com/ws").unwrap(),
            "wss://gateway.qq.com/ws"
        );
        assert_eq!(
            validate_gateway_url("not a valid url").unwrap_err().message,
            "QQ Bot gateway URL is not a valid URL"
        );
    }
}
