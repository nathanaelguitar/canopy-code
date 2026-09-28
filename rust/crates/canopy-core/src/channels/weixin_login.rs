//! QR-code login flow for the Weixin iLink Bot API.
//!
//! Port of `packages/channels/weixin/src/login.ts`. HTTP, time, and status
//! output are injected so the polling and expiration behavior can be tested
//! without network access or wall-clock waits.

use reqwest::Client;
use serde_json::Value;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_LOGIN_TIMEOUT_MS: i64 = 8 * 60 * 1000;
const STATUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_DELAY: Duration = Duration::from_secs(1);

pub type LoginFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Result of the QR login flow.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LoginResult {
    pub connected: bool,
    pub token: Option<String>,
    pub base_url: Option<String>,
    pub user_id: Option<String>,
    pub message: String,
}

/// Login failure. Abort errors are handled by the poll loop; other failures
/// are returned to the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoginError {
    pub message: String,
    kind: LoginErrorKind,
}

impl LoginError {
    pub fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LoginErrorKind::Other,
        }
    }

    pub fn abort(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            kind: LoginErrorKind::Abort,
        }
    }

    fn is_abort(&self) -> bool {
        self.kind == LoginErrorKind::Abort
    }
}

impl fmt::Display for LoginError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for LoginError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoginErrorKind {
    Abort,
    Other,
}

/// GET request passed to the login transport. The optional timeout applies to
/// waiting for response headers only, matching `fetch` plus the source timer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoginHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub timeout: Option<Duration>,
}

/// HTTP response with raw JSON bytes. Non-success bodies may remain unread, as
/// the TypeScript flow checks status before calling `response.json()`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoginHttpResponse {
    pub status: u16,
    pub body: Result<Vec<u8>, LoginError>,
}

impl LoginHttpResponse {
    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            body: Ok(serde_json::to_vec(&value).expect("JSON values always serialize")),
        }
    }

    pub fn raw(status: u16, bytes: Vec<u8>) -> Self {
        Self {
            status,
            body: Ok(bytes),
        }
    }

    pub fn with_body_error(mut self, error: LoginError) -> Self {
        self.body = Err(error);
        self
    }
}

/// GET seam shared by production reqwest and offline tests.
pub trait LoginHttpClient: Send + Sync {
    fn get<'a>(
        &'a self,
        request: LoginHttpRequest,
    ) -> LoginFuture<'a, Result<LoginHttpResponse, LoginError>>;
}

/// Wall-clock and sleeper seam for the total login deadline and poll delay.
pub trait LoginClock: Send + Sync {
    fn now_ms(&self) -> i64;
    fn sleep<'a>(&'a self, duration: Duration) -> LoginFuture<'a, ()>;
}

/// Status output seam. The production implementation writes to stderr.
pub trait LoginStatusOutput: Send + Sync {
    fn write_status(&self, message: &str);
}

struct ReqwestLoginHttpClient<'a> {
    client: &'a Client,
}

impl LoginHttpClient for ReqwestLoginHttpClient<'_> {
    fn get<'a>(
        &'a self,
        request: LoginHttpRequest,
    ) -> LoginFuture<'a, Result<LoginHttpResponse, LoginError>> {
        Box::pin(async move {
            let mut builder = self.client.get(&request.url);
            for (name, value) in request.headers {
                builder = builder.header(name, value);
            }

            let response = match request.timeout {
                Some(timeout) => match tokio::time::timeout(timeout, builder.send()).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) if error.is_timeout() => {
                        return Err(LoginError::abort(error.to_string()));
                    }
                    Ok(Err(error)) => return Err(LoginError::other(error.to_string())),
                    Err(_) => return Err(LoginError::abort("request timed out")),
                },
                None => builder
                    .send()
                    .await
                    .map_err(|error| LoginError::other(error.to_string()))?,
            };
            let status = response.status().as_u16();
            if !(200..300).contains(&status) {
                // The source returns the HTTP status error before attempting to
                // parse or consume a non-success response body.
                return Ok(LoginHttpResponse {
                    status,
                    body: Ok(Vec::new()),
                });
            }

            // The source clears its 60-second timer once fetch returns, then
            // parses JSON outside that timer. Read the body after the timed
            // send for the same cancellation boundary.
            let body = response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| LoginError::other(error.to_string()));
            Ok(LoginHttpResponse { status, body })
        })
    }
}

struct SystemLoginClock;

impl LoginClock for SystemLoginClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or_default()
    }

    fn sleep<'a>(&'a self, duration: Duration) -> LoginFuture<'a, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

struct StderrLoginStatusOutput;

impl LoginStatusOutput for StderrLoginStatusOutput {
    fn write_status(&self, message: &str) {
        eprint!("{message}");
    }
}

/// Fetch and display a QR-code login URL.
pub async fn start_login(client: &Client, api_base_url: &str) -> Result<String, LoginError> {
    let http = ReqwestLoginHttpClient { client };
    let output = StderrLoginStatusOutput;
    start_login_with_http(&http, &output, api_base_url).await
}

/// Mockable variant of [`start_login`].
pub async fn start_login_with_http(
    http: &dyn LoginHttpClient,
    output: &dyn LoginStatusOutput,
    api_base_url: &str,
) -> Result<String, LoginError> {
    start_login_inner(http, output, api_base_url).await
}

async fn start_login_inner(
    http: &dyn LoginHttpClient,
    output: &dyn LoginStatusOutput,
    api_base_url: &str,
) -> Result<String, LoginError> {
    let request = LoginHttpRequest {
        method: "GET".to_owned(),
        url: format!("{api_base_url}/ilink/bot/get_bot_qrcode?bot_type=3"),
        headers: Vec::new(),
        timeout: None,
    };
    let response = http.get(request).await?;
    if !(200..300).contains(&response.status) {
        return Err(LoginError::other(format!(
            "Failed to get QR code: HTTP {}",
            response.status
        )));
    }
    let body = response.body.map_err(|error| match error.kind {
        LoginErrorKind::Abort => LoginError::abort(error.message),
        LoginErrorKind::Other => LoginError::other(error.message),
    })?;
    let value: Value =
        serde_json::from_slice(&body).map_err(|error| LoginError::other(error.to_string()))?;
    let qrcode = value
        .get("qrcode")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LoginError::other("No qrcode in response"))?;

    if let Some(qrcode_image_url) = value
        .get("qrcode_img_content")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        output.write_status(&format!(
            "QR code URL: {qrcode_image_url}\nScan this URL with WeChat.\n"
        ));
    }
    output.write_status("Scan the QR code with WeChat to connect.\n");
    Ok(qrcode.to_owned())
}

/// Poll a QR login session until it is confirmed, expires repeatedly, or the
/// overall deadline passes.
pub async fn wait_for_login(
    client: &Client,
    qrcode_id: &str,
    api_base_url: &str,
    timeout_ms: Option<i64>,
) -> Result<LoginResult, LoginError> {
    let http = ReqwestLoginHttpClient { client };
    let clock = SystemLoginClock;
    let output = StderrLoginStatusOutput;
    wait_for_login_with_http(&http, &clock, &output, qrcode_id, api_base_url, timeout_ms).await
}

/// Mockable variant of [`wait_for_login`]. `None` uses the source's default
/// eight-minute deadline.
pub async fn wait_for_login_with_http(
    http: &dyn LoginHttpClient,
    clock: &dyn LoginClock,
    output: &dyn LoginStatusOutput,
    qrcode_id: &str,
    api_base_url: &str,
    timeout_ms: Option<i64>,
) -> Result<LoginResult, LoginError> {
    let deadline = clock
        .now_ms()
        .saturating_add(timeout_ms.unwrap_or(DEFAULT_LOGIN_TIMEOUT_MS));
    let mut current_qrcode_id = qrcode_id.to_owned();
    let mut retry_count = 0;

    while clock.now_ms() < deadline {
        let step: Result<Option<LoginResult>, LoginError> = async {
            let request = LoginHttpRequest {
                method: "GET".to_owned(),
                url: format!(
                    "{api_base_url}/ilink/bot/get_qrcode_status?qrcode={}",
                    encode_uri_component(&current_qrcode_id)
                ),
                headers: crate::channels::weixin_api::build_headers(None),
                timeout: Some(STATUS_REQUEST_TIMEOUT),
            };
            let response = http.get(request).await?;
            if !(200..300).contains(&response.status) {
                return Err(LoginError::other(format!("HTTP {}", response.status)));
            }
            let body = response.body.map_err(|error| match error.kind {
                LoginErrorKind::Abort => LoginError::abort(error.message),
                LoginErrorKind::Other => LoginError::other(error.message),
            })?;
            let value: Value = serde_json::from_slice(&body)
                .map_err(|error| LoginError::other(error.to_string()))?;

            match value.get("status").and_then(Value::as_str) {
                Some("confirmed") => Ok(Some(LoginResult {
                    connected: true,
                    token: value
                        .get("bot_token")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    base_url: value
                        .get("baseurl")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    user_id: value
                        .get("ilink_user_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    message: "Connected to WeChat successfully!".to_owned(),
                })),
                Some("scaned") => {
                    output.write_status("QR code scanned, waiting for confirmation...\n");
                    Ok(None)
                }
                Some("expired") => {
                    retry_count += 1;
                    if retry_count >= 3 {
                        return Ok(Some(LoginResult {
                            connected: false,
                            message: "QR code expired after maximum retries.".to_owned(),
                            ..LoginResult::default()
                        }));
                    }
                    output.write_status("QR code expired, refreshing...\n");
                    current_qrcode_id = start_login_inner(http, output, api_base_url).await?;
                    Ok(None)
                }
                _ => Ok(None),
            }
        }
        .await;

        match step {
            Ok(Some(result)) => return Ok(result),
            Ok(None) => {}
            Err(error) if error.is_abort() => continue,
            Err(error) => return Err(error),
        }
        clock.sleep(POLL_DELAY).await;
    }

    Ok(LoginResult {
        connected: false,
        message: "Login timed out.".to_owned(),
        ..LoginResult::default()
    })
}

fn encode_uri_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(
                *byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            )
        {
            encoded.push(char::from(*byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};

    struct FakeHttp {
        requests: Mutex<Vec<LoginHttpRequest>>,
        responses: Mutex<VecDeque<Result<LoginHttpResponse, LoginError>>>,
    }

    impl FakeHttp {
        fn new(responses: impl IntoIterator<Item = Result<LoginHttpResponse, LoginError>>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<LoginHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl LoginHttpClient for FakeHttp {
        fn get<'a>(
            &'a self,
            request: LoginHttpRequest,
        ) -> LoginFuture<'a, Result<LoginHttpResponse, LoginError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| Err(LoginError::other("no mock response")))
            })
        }
    }

    struct FakeClock {
        now: AtomicI64,
        sleeps: Mutex<Vec<Duration>>,
    }

    impl FakeClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now: AtomicI64::new(now_ms),
                sleeps: Mutex::new(Vec::new()),
            }
        }

        fn sleeps(&self) -> Vec<Duration> {
            self.sleeps.lock().unwrap().clone()
        }
    }

    impl LoginClock for FakeClock {
        fn now_ms(&self) -> i64 {
            self.now.load(Ordering::SeqCst)
        }

        fn sleep<'a>(&'a self, duration: Duration) -> LoginFuture<'a, ()> {
            Box::pin(async move {
                self.sleeps.lock().unwrap().push(duration);
                self.now
                    .fetch_add(duration.as_millis() as i64, Ordering::SeqCst);
            })
        }
    }

    #[derive(Default)]
    struct FakeOutput(Mutex<Vec<String>>);

    impl FakeOutput {
        fn messages(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl LoginStatusOutput for FakeOutput {
        fn write_status(&self, message: &str) {
            self.0.lock().unwrap().push(message.to_owned());
        }
    }

    fn response(value: Value) -> Result<LoginHttpResponse, LoginError> {
        Ok(LoginHttpResponse::json(200, value))
    }

    fn abort() -> Result<LoginHttpResponse, LoginError> {
        Err(LoginError::abort("request aborted"))
    }

    fn status_request_url(request: &LoginHttpRequest) -> &str {
        &request.url
    }

    #[tokio::test]
    async fn start_login_requests_qr_and_prints_optional_image_url() {
        let http = FakeHttp::new([response(json!({
            "qrcode": "qr-token",
            "qrcode_img_content": "https://qr.example/image"
        }))]);
        let output = FakeOutput::default();
        let qr = start_login_with_http(&http, &output, "https://api.test")
            .await
            .unwrap();
        assert_eq!(qr, "qr-token");
        assert_eq!(
            http.requests(),
            [LoginHttpRequest {
                method: "GET".to_owned(),
                url: "https://api.test/ilink/bot/get_bot_qrcode?bot_type=3".to_owned(),
                headers: Vec::new(),
                timeout: None,
            }]
        );
        assert_eq!(
            output.messages(),
            [
                "QR code URL: https://qr.example/image\nScan this URL with WeChat.\n",
                "Scan the QR code with WeChat to connect.\n"
            ]
        );
    }

    #[tokio::test]
    async fn start_login_rejects_http_errors_and_missing_qr_codes() {
        let http = FakeHttp::new([Ok(LoginHttpResponse::json(
            503,
            json!({"qrcode": "ignored"}),
        ))]);
        let output = FakeOutput::default();
        let error = start_login_with_http(&http, &output, "https://api.test")
            .await
            .unwrap_err();
        assert_eq!(error.message, "Failed to get QR code: HTTP 503");

        let http = FakeHttp::new([response(json!({"qrcode": ""}))]);
        let error = start_login_with_http(&http, &output, "https://api.test")
            .await
            .unwrap_err();
        assert_eq!(error.message, "No qrcode in response");
    }

    #[tokio::test]
    async fn status_http_errors_propagate_without_poll_delay() {
        let http = FakeHttp::new([Ok(LoginHttpResponse::json(403, json!({"error": "denied"})))]);
        let clock = FakeClock::new(0);
        let output = FakeOutput::default();
        let error =
            wait_for_login_with_http(&http, &clock, &output, "qr-id", "https://api.test", None)
                .await
                .unwrap_err();
        assert_eq!(error.message, "HTTP 403");
        assert!(clock.sleeps().is_empty());
    }

    #[tokio::test]
    async fn confirmed_login_returns_credentials_and_uses_status_headers() {
        let http = FakeHttp::new([response(json!({
            "status": "confirmed",
            "bot_token": "secret",
            "baseurl": "https://api.weixin.test",
            "ilink_user_id": "wx-user"
        }))]);
        let clock = FakeClock::new(1000);
        let output = FakeOutput::default();
        let result =
            wait_for_login_with_http(&http, &clock, &output, "qr-id", "https://api.test", None)
                .await
                .unwrap();
        assert_eq!(
            result,
            LoginResult {
                connected: true,
                token: Some("secret".to_owned()),
                base_url: Some("https://api.weixin.test".to_owned()),
                user_id: Some("wx-user".to_owned()),
                message: "Connected to WeChat successfully!".to_owned(),
            }
        );
        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            status_request_url(&requests[0]),
            "https://api.test/ilink/bot/get_qrcode_status?qrcode=qr-id"
        );
        assert_eq!(requests[0].timeout, Some(Duration::from_secs(60)));
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(key, _)| key == "X-WECHAT-UIN")
        );
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(key, value)| { key == "iLink-App-ClientVersion" && value == "131331" })
        );
        assert!(
            !requests[0]
                .headers
                .iter()
                .any(|(key, _)| key == "Authorization")
        );
        assert!(clock.sleeps().is_empty());
    }

    #[tokio::test]
    async fn scaned_and_unknown_states_wait_one_second_before_next_poll() {
        let http = FakeHttp::new([
            response(json!({"status": "scaned"})),
            response(json!({"status": "unknown"})),
            response(json!({"status": "confirmed"})),
        ]);
        let clock = FakeClock::new(0);
        let output = FakeOutput::default();
        let result = wait_for_login_with_http(
            &http,
            &clock,
            &output,
            "qr-id",
            "https://api.test",
            Some(5000),
        )
        .await
        .unwrap();
        assert!(result.connected);
        assert_eq!(http.requests().len(), 3);
        assert_eq!(
            clock.sleeps(),
            [Duration::from_secs(1), Duration::from_secs(1)]
        );
        assert_eq!(
            output.messages(),
            ["QR code scanned, waiting for confirmation...\n"]
        );
    }

    #[tokio::test]
    async fn expired_qr_refreshes_twice_and_stops_on_third_expiration() {
        let http = FakeHttp::new([
            response(json!({"status": "expired"})),
            response(json!({"qrcode": "qr-2"})),
            response(json!({"status": "expired"})),
            response(json!({"qrcode": "qr-3"})),
            response(json!({"status": "expired"})),
        ]);
        let clock = FakeClock::new(0);
        let output = FakeOutput::default();
        let result = wait_for_login_with_http(
            &http,
            &clock,
            &output,
            "qr-1",
            "https://api.test",
            Some(30_000),
        )
        .await
        .unwrap();
        assert_eq!(result.message, "QR code expired after maximum retries.");
        assert!(!result.connected);
        let requests = http.requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            status_request_url(&requests[0]),
            "https://api.test/ilink/bot/get_qrcode_status?qrcode=qr-1"
        );
        assert_eq!(
            requests[2].url,
            "https://api.test/ilink/bot/get_qrcode_status?qrcode=qr-2"
        );
        assert_eq!(
            requests[4].url,
            "https://api.test/ilink/bot/get_qrcode_status?qrcode=qr-3"
        );
        assert_eq!(
            clock.sleeps(),
            [Duration::from_secs(1), Duration::from_secs(1)]
        );
        assert_eq!(
            output
                .messages()
                .iter()
                .filter(|message| message.as_str() == "QR code expired, refreshing...\n")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn deadline_uses_eight_minute_default_and_returns_timed_out_result() {
        assert_eq!(DEFAULT_LOGIN_TIMEOUT_MS, 8 * 60 * 1000);
        let default_http = FakeHttp::new(
            std::iter::repeat_with(|| response(json!({"status": "waiting"}))).take(8 * 60),
        );
        let default_clock = FakeClock::new(0);
        let output = FakeOutput::default();
        let default_result = wait_for_login_with_http(
            &default_http,
            &default_clock,
            &output,
            "qr-id",
            "https://api.test",
            None,
        )
        .await
        .unwrap();
        assert_eq!(default_result.message, "Login timed out.");
        assert_eq!(default_http.requests().len(), 8 * 60);
        assert_eq!(default_clock.sleeps().len(), 8 * 60);

        let http = FakeHttp::new([
            response(json!({"status": "waiting"})),
            response(json!({"status": "waiting"})),
        ]);
        let clock = FakeClock::new(50);
        let result = wait_for_login_with_http(
            &http,
            &clock,
            &output,
            "qr-id",
            "https://api.test",
            Some(2000),
        )
        .await
        .unwrap();
        assert_eq!(result.message, "Login timed out.");
        assert_eq!(http.requests().len(), 2);
        assert_eq!(
            clock.sleeps(),
            [Duration::from_secs(1), Duration::from_secs(1)]
        );

        let no_requests = FakeHttp::new([]);
        let clock = FakeClock::new(10);
        let result = wait_for_login_with_http(
            &no_requests,
            &clock,
            &output,
            "qr-id",
            "https://api.test",
            Some(0),
        )
        .await
        .unwrap();
        assert_eq!(result.message, "Login timed out.");
        assert!(no_requests.requests().is_empty());
    }

    #[tokio::test]
    async fn abort_errors_immediately_retry_but_other_errors_propagate() {
        let http = FakeHttp::new([abort(), response(json!({"status": "confirmed"}))]);
        let clock = FakeClock::new(0);
        let output = FakeOutput::default();
        let result = wait_for_login_with_http(
            &http,
            &clock,
            &output,
            "qr-id",
            "https://api.test",
            Some(1000),
        )
        .await
        .unwrap();
        assert!(result.connected);
        assert_eq!(http.requests().len(), 2);
        assert!(clock.sleeps().is_empty());

        let http = FakeHttp::new([Err(LoginError::other("connection reset"))]);
        let clock = FakeClock::new(0);
        let error = wait_for_login_with_http(
            &http,
            &clock,
            &output,
            "qr-id",
            "https://api.test",
            Some(1000),
        )
        .await
        .unwrap_err();
        assert_eq!(error.message, "connection reset");
        assert!(clock.sleeps().is_empty());
    }

    #[tokio::test]
    async fn qrcode_parameter_uses_encode_uri_component_rules() {
        let http = FakeHttp::new([response(json!({"status": "confirmed"}))]);
        let clock = FakeClock::new(0);
        let output = FakeOutput::default();
        wait_for_login_with_http(&http, &clock, &output, "a b!?/()", "https://api.test", None)
            .await
            .unwrap();
        assert_eq!(
            http.requests()[0].url,
            "https://api.test/ilink/bot/get_qrcode_status?qrcode=a%20b!%3F%2F()"
        );
    }
}
