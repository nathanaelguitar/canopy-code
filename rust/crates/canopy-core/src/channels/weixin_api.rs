//! HTTP API wrapper for the Weixin iLink Bot API.
//!
//! Port of `packages/channels/weixin/src/api.ts`. The HTTP and runtime traits
//! keep request construction, retries, random UIN values, and waits testable
//! without a live service or real backoff delays.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use reqwest::Client;
use reqwest::Url;
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::watch;

const ILINK_PROTOCOL_VERSION: &str = "2.1.3";
const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(40);
const CDN_HOST: &str = "novac2c.cdn.weixin.qq.com";
const MAX_RETRIES: u32 = 3;
const BASE_RETRY_DELAY: Duration = Duration::from_secs(1);

pub type ApiFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Structured HTTP or API failure. `status` is zero for transport failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WeixinApiError {
    pub message: String,
    pub status: u16,
    pub ret: Option<i64>,
    pub errcode: Option<i64>,
    kind: ErrorKind,
}

impl WeixinApiError {
    fn structured(
        message: impl Into<String>,
        status: u16,
        ret: Option<i64>,
        errcode: Option<i64>,
    ) -> Self {
        Self {
            message: message.into(),
            status,
            ret,
            errcode,
            kind: ErrorKind::Structured,
        }
    }

    fn parse(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: 0,
            ret: None,
            errcode: None,
            kind: ErrorKind::Parse,
        }
    }

    fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: 0,
            ret: None,
            errcode: None,
            kind: ErrorKind::Other,
        }
    }

    fn from_transport(error: ApiTransportError) -> Self {
        let (message, kind) = match error {
            ApiTransportError::Network(message) => (message, ErrorKind::Network),
            ApiTransportError::Timeout(message) => (message, ErrorKind::Abort),
            ApiTransportError::Cancelled => ("request aborted".to_owned(), ErrorKind::Abort),
            ApiTransportError::Other(message) => (message, ErrorKind::Other),
        };
        Self {
            message,
            status: 0,
            ret: None,
            errcode: None,
            kind,
        }
    }

    fn is_retryable(&self) -> bool {
        match self.kind {
            ErrorKind::Network => return true,
            // Fetch's AbortError is not a TypeError and has no `.code`, so it
            // is not retried. `get_updates` handles it as a poll cancellation.
            ErrorKind::Abort | ErrorKind::Parse | ErrorKind::Other => return false,
            ErrorKind::Structured => {}
        }

        if self.errcode == Some(-14) {
            return false;
        }
        if matches!(self.errcode, Some(-1 | 45011)) {
            return true;
        }
        if self.ret.is_some_and(|ret| ret != 0) {
            return false;
        }
        if (400..500).contains(&self.status) {
            return self.status == 429;
        }
        self.status == 0 || self.status >= 500
    }

    fn is_abort(&self) -> bool {
        self.kind == ErrorKind::Abort
    }
}

impl fmt::Display for WeixinApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for WeixinApiError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ErrorKind {
    Structured,
    Network,
    Abort,
    Parse,
    Other,
}

/// Transport failure categories. Network failures retry; timeouts/cancellation
/// behave like fetch `AbortError` and do not retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiTransportError {
    Network(String),
    Timeout(String),
    Cancelled,
    Other(String),
}

/// One API POST request. The request body is already JSON encoded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiHttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

/// Response from the API transport. Body read errors are held separately so
/// HTTP error responses can ignore them just like the source's best-effort
/// `response.json()` diagnostic parsing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiHttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Result<Vec<u8>, ApiTransportError>,
}

impl ApiHttpResponse {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: Ok(serde_json::to_vec(&body).expect("JSON values always serialize")),
        }
    }

    pub fn raw(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: Ok(body),
        }
    }

    pub fn with_body_error(mut self, error: ApiTransportError) -> Self {
        self.body = Err(error);
        self
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers
            .insert(name.into().to_ascii_lowercase(), value.into());
        self
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Async POST seam used by the production reqwest transport and mock tests.
pub trait ApiHttpClient: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: ApiHttpRequest,
    ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>>;
}

/// Randomness and backoff clock seam. Production values use UUID-backed
/// operating-system randomness and Tokio timers; tests can inject fixed UIN
/// bytes and record delays without sleeping.
pub trait ApiRuntime: Send + Sync {
    fn random_uin_bytes(&self) -> [u8; 4];
    fn sleep<'a>(&'a self, duration: Duration) -> ApiFuture<'a, ()>;
}

/// Cancellation handle for long-poll requests.
#[derive(Clone, Debug)]
pub struct CancellationToken {
    sender: watch::Sender<bool>,
    receiver: watch::Receiver<bool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        let (sender, receiver) = watch::channel(false);
        Self { sender, receiver }
    }

    pub fn cancel(&self) {
        let _ = self.sender.send(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

struct SystemApiRuntime;

impl ApiRuntime for SystemApiRuntime {
    fn random_uin_bytes(&self) -> [u8; 4] {
        // UUID v4 is backed by the platform CSPRNG. Taking two bytes from each
        // UUID avoids its version and variant bits while retaining 32 random
        // bits without another workspace dependency.
        let first = *uuid::Uuid::new_v4().as_bytes();
        let second = *uuid::Uuid::new_v4().as_bytes();
        [first[0], first[1], second[0], second[1]]
    }

    fn sleep<'a>(&'a self, duration: Duration) -> ApiFuture<'a, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

struct ReqwestApiHttpClient<'a> {
    client: &'a Client,
}

impl ApiHttpClient for ReqwestApiHttpClient<'_> {
    fn execute<'a>(
        &'a self,
        request: ApiHttpRequest,
    ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>> {
        Box::pin(async move {
            let mut builder = self.client.post(&request.url);
            for (name, value) in request.headers {
                builder = builder.header(name, value);
            }
            let response = builder
                .body(request.body)
                .timeout(request.timeout)
                .send()
                .await
                .map_err(classify_reqwest_error)?;
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
                })
                .collect();
            let body = response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(classify_reqwest_error);
            Ok(ApiHttpResponse {
                status,
                headers,
                body,
            })
        })
    }
}

fn classify_reqwest_error(error: reqwest::Error) -> ApiTransportError {
    if error.is_timeout() {
        ApiTransportError::Timeout(error.to_string())
    } else {
        // `fetch` reports request construction and network failures as
        // TypeError, and the TypeScript retry classifier retries TypeError.
        ApiTransportError::Network(error.to_string())
    }
}

/// Build request headers with a fresh random `X-WECHAT-UIN` value.
pub fn build_headers(token: Option<&str>) -> Vec<(String, String)> {
    let runtime = SystemApiRuntime;
    build_headers_with_runtime(token, &runtime)
}

fn build_headers_with_runtime(
    token: Option<&str>,
    runtime: &dyn ApiRuntime,
) -> Vec<(String, String)> {
    let mut headers = vec![
        ("Content-Type".to_owned(), "application/json".to_owned()),
        (
            "X-WECHAT-UIN".to_owned(),
            BASE64_STANDARD.encode(runtime.random_uin_bytes()),
        ),
        ("iLink-App-Id".to_owned(), "bot".to_owned()),
        (
            "iLink-App-ClientVersion".to_owned(),
            build_client_version(ILINK_PROTOCOL_VERSION).to_string(),
        ),
    ];
    if let Some(token) = token.filter(|token| !token.is_empty()) {
        headers.push(("AuthorizationType".to_owned(), "ilink_bot_token".to_owned()));
        headers.push(("Authorization".to_owned(), format!("Bearer {token}")));
    }
    headers
}

fn build_client_version(version: &str) -> u32 {
    let mut parts = version.split('.').map(parse_js_integer_component);
    let major = parts.next().flatten().unwrap_or(0) as u32;
    let minor = parts.next().flatten().unwrap_or(0) as u32;
    let patch = parts.next().flatten().unwrap_or(0) as u32;
    ((major & 0xff) << 16) | ((minor & 0xff) << 8) | (patch & 0xff)
}

fn parse_js_integer_component(component: &str) -> Option<i64> {
    let trimmed = component.trim_start_matches(char::is_whitespace);
    let end = trimmed
        .char_indices()
        .find_map(|(index, ch)| {
            (!ch.is_ascii_digit() && !(index == 0 && matches!(ch, '+' | '-'))).then_some(index)
        })
        .unwrap_or(trimmed.len());
    trimmed[..end].parse().ok()
}

fn base_info() -> Value {
    json!({ "channel_version": ILINK_PROTOCOL_VERSION })
}

/// Fetch message updates. Timeout or cancellation returns an empty response
/// carrying the same cursor, matching the poller's graceful-abort behavior.
pub async fn get_updates(
    client: &Client,
    base_url: &str,
    token: &str,
    get_updates_buf: &str,
    timeout: Option<Duration>,
    cancellation: Option<&CancellationToken>,
) -> Result<Value, WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    get_updates_with_http(
        &http,
        &runtime,
        base_url,
        token,
        get_updates_buf,
        timeout,
        cancellation,
    )
    .await
}

/// Mockable variant of [`get_updates`]. `None` uses the source's 40-second
/// default request timeout.
pub async fn get_updates_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    base_url: &str,
    token: &str,
    get_updates_buf: &str,
    timeout: Option<Duration>,
    cancellation: Option<&CancellationToken>,
) -> Result<Value, WeixinApiError> {
    let body = json!({
        "get_updates_buf": get_updates_buf,
        "base_info": base_info(),
    });
    let result = post_json(
        client,
        runtime,
        PostJsonRequest {
            base_url,
            path: "/ilink/bot/getupdates",
            body,
            token: Some(token),
            timeout: timeout.unwrap_or(DEFAULT_API_TIMEOUT),
            cancellation,
        },
    )
    .await;
    match result {
        Err(error) if error.is_abort() => Ok(json!({
            "ret": 0,
            "msgs": [],
            "get_updates_buf": get_updates_buf,
        })),
        other => other,
    }
}

/// Send a bot message, retrying retryable transport/API failures.
pub async fn send_message(
    client: &Client,
    base_url: &str,
    token: &str,
    msg: Option<Value>,
) -> Result<(), WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    send_message_with_http(&http, &runtime, base_url, token, msg).await
}

/// Mockable variant of [`send_message`].
pub async fn send_message_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    base_url: &str,
    token: &str,
    msg: Option<Value>,
) -> Result<(), WeixinApiError> {
    let mut body = Map::new();
    if let Some(msg) = msg {
        body.insert("msg".to_owned(), msg);
    }
    body.insert("base_info".to_owned(), base_info());

    retry_with_backoff(runtime, || async {
        let response = post_json(
            client,
            runtime,
            PostJsonRequest {
                base_url,
                path: "/ilink/bot/sendmessage",
                body: Value::Object(body.clone()),
                token: Some(token),
                timeout: DEFAULT_API_TIMEOUT,
                cancellation: None,
            },
        )
        .await?;
        if has_nonzero_field(&response, "ret") || has_nonzero_field(&response, "errcode") {
            return Err(WeixinApiError::structured(
                format!(
                    "sendMessage failed: ret={} errcode={} {}",
                    js_field_text(&response, "ret", "undefined"),
                    js_field_text(&response, "errcode", "undefined"),
                    truthy_field_text(&response, "errmsg").unwrap_or_default()
                ),
                200,
                numeric_field(&response, "ret"),
                numeric_field(&response, "errcode"),
            ));
        }
        Ok(())
    })
    .await
}

/// Fetch per-user configuration.
pub async fn get_config(
    client: &Client,
    base_url: &str,
    token: &str,
    user_id: &str,
    context_token: Option<&str>,
) -> Result<Value, WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    get_config_with_http(&http, &runtime, base_url, token, user_id, context_token).await
}

/// Mockable variant of [`get_config`].
pub async fn get_config_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    base_url: &str,
    token: &str,
    user_id: &str,
    context_token: Option<&str>,
) -> Result<Value, WeixinApiError> {
    let mut body = Map::new();
    body.insert("ilink_user_id".to_owned(), json!(user_id));
    if let Some(context_token) = context_token {
        body.insert("context_token".to_owned(), json!(context_token));
    }
    body.insert("base_info".to_owned(), base_info());
    post_json(
        client,
        runtime,
        PostJsonRequest {
            base_url,
            path: "/ilink/bot/getconfig",
            body: Value::Object(body),
            token: Some(token),
            timeout: DEFAULT_API_TIMEOUT,
            cancellation: None,
        },
    )
    .await
}

/// Send typing state for a user.
pub async fn send_typing(
    client: &Client,
    base_url: &str,
    token: &str,
    req: Value,
) -> Result<Value, WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    send_typing_with_http(&http, &runtime, base_url, token, req).await
}

/// Mockable variant of [`send_typing`].
pub async fn send_typing_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    base_url: &str,
    token: &str,
    req: Value,
) -> Result<Value, WeixinApiError> {
    let mut body = match req {
        Value::Object(body) => body,
        _ => Map::new(),
    };
    body.insert("base_info".to_owned(), base_info());
    post_json(
        client,
        runtime,
        PostJsonRequest {
            base_url,
            path: "/ilink/bot/sendtyping",
            body: Value::Object(body),
            token: Some(token),
            timeout: DEFAULT_API_TIMEOUT,
            cancellation: None,
        },
    )
    .await
}

/// Request an upload URL and CDN credentials for encrypted media.
// Retain this compatibility signature so existing callers can continue to
// pass the individual media fields directly.
#[allow(clippy::too_many_arguments)]
pub async fn get_upload_url(
    client: &Client,
    base_url: &str,
    token: &str,
    to_user_id: &str,
    filekey: &str,
    rawsize: u64,
    rawfilemd5: &str,
    encrypted_size: u64,
    aeskey_hex: &str,
) -> Result<String, WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    get_upload_url_with_http(
        &http,
        &runtime,
        base_url,
        token,
        to_user_id,
        filekey,
        rawsize,
        rawfilemd5,
        encrypted_size,
        aeskey_hex,
    )
    .await
}

/// Mockable variant of [`get_upload_url`]. `upload_full_url` takes precedence
/// over `upload_param` when both are present.
// The public test seam mirrors get_upload_url's existing field-by-field API.
#[allow(clippy::too_many_arguments)]
pub async fn get_upload_url_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    base_url: &str,
    token: &str,
    to_user_id: &str,
    filekey: &str,
    rawsize: u64,
    rawfilemd5: &str,
    encrypted_size: u64,
    aeskey_hex: &str,
) -> Result<String, WeixinApiError> {
    let body = json!({
        "filekey": filekey,
        "media_type": 1,
        "to_user_id": to_user_id,
        "rawsize": rawsize,
        "rawfilemd5": rawfilemd5,
        "filesize": encrypted_size,
        "no_need_thumb": true,
        "aeskey": aeskey_hex,
        "base_info": base_info(),
    });
    retry_with_backoff(runtime, || async {
        let response = post_json(
            client,
            runtime,
            PostJsonRequest {
                base_url,
                path: "/ilink/bot/getuploadurl",
                body: body.clone(),
                token: Some(token),
                timeout: DEFAULT_API_TIMEOUT,
                cancellation: None,
            },
        )
        .await?;

        if has_nonzero_field(&response, "ret") || has_nonzero_field(&response, "errcode") {
            return Err(upload_url_error("getuploadurl failed", &response));
        }
        if let Some(url) = truthy_field_text(&response, "upload_full_url") {
            return Ok(url);
        }
        if let Some(param) = truthy_field_text(&response, "upload_param") {
            return Ok(param);
        }
        Err(upload_url_error("getuploadurl returned no URL", &response))
    })
    .await
}

fn upload_url_error(prefix: &str, response: &Value) -> WeixinApiError {
    WeixinApiError::structured(
        format!(
            "{prefix}: ret={} errcode={} errmsg={}",
            js_field_text(response, "ret", "undefined"),
            js_nullish_field_text(response, "errcode", "(none)"),
            truthy_field_text(response, "errmsg").unwrap_or_else(|| "(none)".to_owned()),
        ),
        200,
        numeric_field(response, "ret"),
        numeric_field(response, "errcode"),
    )
}

/// Upload encrypted bytes to Weixin's CDN.
pub async fn upload_to_cdn(
    client: &Client,
    url_or_param: &str,
    filekey: &str,
    encrypted_data: &[u8],
) -> Result<String, WeixinApiError> {
    let http = ReqwestApiHttpClient { client };
    let runtime = SystemApiRuntime;
    upload_to_cdn_with_http(
        &http,
        &runtime,
        url_or_param,
        filekey,
        encrypted_data,
        DEFAULT_API_TIMEOUT,
    )
    .await
}

/// Mockable variant of [`upload_to_cdn`]. The timeout argument is exposed for
/// deterministic timeout tests; production uses the source's 40-second limit.
pub async fn upload_to_cdn_with_http(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    url_or_param: &str,
    filekey: &str,
    encrypted_data: &[u8],
    timeout: Duration,
) -> Result<String, WeixinApiError> {
    let url = cdn_upload_url(url_or_param, filekey)?;
    retry_with_backoff(runtime, || async {
        let request = ApiHttpRequest {
            method: "POST".to_owned(),
            url: url.clone(),
            headers: vec![(
                "Content-Type".to_owned(),
                "application/octet-stream".to_owned(),
            )],
            body: encrypted_data.to_vec(),
            timeout,
        };
        let response = execute_request(client, request, None).await?;
        if !(200..300).contains(&response.status) {
            let error_value = response
                .body
                .as_ref()
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
            let errmsg = error_value
                .as_ref()
                .and_then(|value| truthy_field_text(value, "errmsg"));
            let ret = error_value
                .as_ref()
                .and_then(|value| numeric_field(value, "ret"));
            let errcode = error_value
                .as_ref()
                .and_then(|value| numeric_field(value, "errcode"));
            return Err(WeixinApiError::structured(
                errmsg.map_or_else(
                    || format!("CDN upload failed: HTTP {}", response.status),
                    |message| format!("CDN upload failed: HTTP {} — {message}", response.status),
                ),
                response.status,
                ret,
                errcode,
            ));
        }

        let Some(encrypt_param) = response.header("x-encrypted-param") else {
            return Err(WeixinApiError::structured(
                "CDN upload succeeded but missing x-encrypted-param header",
                response.status,
                None,
                None,
            ));
        };
        Ok(encrypt_param.to_owned())
    })
    .await
}

fn cdn_upload_url(url_or_param: &str, filekey: &str) -> Result<String, WeixinApiError> {
    let lower = url_or_param.to_ascii_lowercase();
    if lower.starts_with("https://") {
        let parsed =
            Url::parse(url_or_param).map_err(|error| WeixinApiError::other(error.to_string()))?;
        let hostname = parsed.host_str().unwrap_or_default();
        if hostname != CDN_HOST {
            return Err(WeixinApiError::other(format!(
                "CDN upload URL has unexpected host: {hostname}"
            )));
        }
        return Ok(url_or_param.to_owned());
    }
    if lower.starts_with("http://") {
        return Err(WeixinApiError::other(
            "CDN upload URL must use HTTPS".to_owned(),
        ));
    }

    Ok(format!(
        "https://{CDN_HOST}/c2c/upload?encrypted_query_param={}&filekey={}",
        encode_uri_component(url_or_param),
        encode_uri_component(filekey),
    ))
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

struct PostJsonRequest<'a> {
    base_url: &'a str,
    path: &'a str,
    body: Value,
    token: Option<&'a str>,
    timeout: Duration,
    cancellation: Option<&'a CancellationToken>,
}

async fn post_json(
    client: &dyn ApiHttpClient,
    runtime: &dyn ApiRuntime,
    request: PostJsonRequest<'_>,
) -> Result<Value, WeixinApiError> {
    let PostJsonRequest {
        base_url,
        path,
        body,
        token,
        timeout,
        cancellation,
    } = request;
    let body =
        serde_json::to_vec(&body).map_err(|error| WeixinApiError::other(error.to_string()))?;
    let request = ApiHttpRequest {
        method: "POST".to_owned(),
        url: format!("{base_url}{path}"),
        headers: build_headers_with_runtime(token, runtime),
        body,
        timeout,
    };
    let response = execute_request(client, request, cancellation).await?;
    if !(200..300).contains(&response.status) {
        let error_value = response
            .body
            .as_ref()
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
        let ret = error_value
            .as_ref()
            .and_then(|value| numeric_field(value, "ret"));
        let errcode = error_value
            .as_ref()
            .and_then(|value| numeric_field(value, "errcode"));
        let errmsg = error_value
            .as_ref()
            .and_then(|value| truthy_field_text(value, "errmsg"));
        let message = errmsg.map_or_else(
            || format!("WeChat API error (HTTP {})", response.status),
            |errmsg| {
                format!(
                    "WeChat API error (HTTP {}, ret={}, errcode={}): {errmsg}",
                    response.status,
                    js_optional_field_text(error_value.as_ref(), "ret"),
                    js_optional_field_text(error_value.as_ref(), "errcode"),
                )
            },
        );
        return Err(WeixinApiError::structured(
            message,
            response.status,
            ret,
            errcode,
        ));
    }

    let bytes = response.body.map_err(WeixinApiError::from_transport)?;
    serde_json::from_slice(&bytes).map_err(|error| WeixinApiError::parse(error.to_string()))
}

async fn execute_request(
    client: &dyn ApiHttpClient,
    request: ApiHttpRequest,
    cancellation: Option<&CancellationToken>,
) -> Result<ApiHttpResponse, WeixinApiError> {
    let timeout = request.timeout;
    let future = client.execute(request);
    tokio::pin!(future);
    let result = if let Some(cancellation) = cancellation {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(ApiTransportError::Cancelled),
            result = tokio::time::timeout(timeout, &mut future) => match result {
                Ok(result) => result,
                Err(_) => Err(ApiTransportError::Timeout("request timed out".to_owned())),
            },
        }
    } else {
        match tokio::time::timeout(timeout, &mut future).await {
            Ok(result) => result,
            Err(_) => Err(ApiTransportError::Timeout("request timed out".to_owned())),
        }
    };
    result.map_err(WeixinApiError::from_transport)
}

async fn retry_with_backoff<T, F, Fut>(
    runtime: &dyn ApiRuntime,
    mut operation: F,
) -> Result<T, WeixinApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, WeixinApiError>>,
{
    for attempt in 1..=(MAX_RETRIES + 1) {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt <= MAX_RETRIES && error.is_retryable() => {
                let multiplier = 1_u32 << (attempt - 1);
                runtime
                    .sleep(BASE_RETRY_DELAY.saturating_mul(multiplier))
                    .await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("retry loop always returns")
}

fn has_nonzero_field(value: &Value, key: &str) -> bool {
    value
        .as_object()
        .and_then(|object| object.get(key))
        .is_some_and(|field| field.as_f64().is_none_or(|number| number != 0.0))
}

fn numeric_field(value: &Value, key: &str) -> Option<i64> {
    value.as_object()?.get(key)?.as_i64()
}

fn js_optional_field_text(value: Option<&Value>, key: &str) -> String {
    value
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .map(js_value_text)
        .unwrap_or_else(|| "undefined".to_owned())
}

fn js_field_text(value: &Value, key: &str, missing: &str) -> String {
    value
        .as_object()
        .and_then(|object| object.get(key))
        .map(js_value_text)
        .unwrap_or_else(|| missing.to_owned())
}

fn js_nullish_field_text(value: &Value, key: &str, missing: &str) -> String {
    match value.as_object().and_then(|object| object.get(key)) {
        None | Some(Value::Null) => missing.to_owned(),
        Some(value) => js_value_text(value),
    }
}

fn truthy_field_text(value: &Value, key: &str) -> Option<String> {
    value
        .as_object()
        .and_then(|object| object.get(key))
        .filter(|value| is_js_truthy(value))
        .map(js_value_text)
}

fn is_js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_none_or(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_value_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::String(value) => value.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_value_text(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::Notify;

    struct FakeRuntime {
        values: Mutex<VecDeque<[u8; 4]>>,
        next: AtomicUsize,
        delays: Mutex<Vec<Duration>>,
    }

    impl FakeRuntime {
        fn new(values: impl IntoIterator<Item = [u8; 4]>) -> Self {
            Self {
                values: Mutex::new(values.into_iter().collect()),
                next: AtomicUsize::new(0),
                delays: Mutex::new(Vec::new()),
            }
        }

        fn delays(&self) -> Vec<Duration> {
            self.delays.lock().unwrap().clone()
        }
    }

    impl ApiRuntime for FakeRuntime {
        fn random_uin_bytes(&self) -> [u8; 4] {
            self.next.fetch_add(1, Ordering::Relaxed);
            self.values.lock().unwrap().pop_front().unwrap_or([0; 4])
        }

        fn sleep<'a>(&'a self, duration: Duration) -> ApiFuture<'a, ()> {
            Box::pin(async move {
                self.delays.lock().unwrap().push(duration);
            })
        }
    }

    struct FakeHttp {
        requests: Mutex<Vec<ApiHttpRequest>>,
        responses: Mutex<VecDeque<Result<ApiHttpResponse, ApiTransportError>>>,
    }

    impl FakeHttp {
        fn new(
            responses: impl IntoIterator<Item = Result<ApiHttpResponse, ApiTransportError>>,
        ) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<ApiHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl ApiHttpClient for FakeHttp {
        fn execute<'a>(
            &'a self,
            request: ApiHttpRequest,
        ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_else(|| Err(ApiTransportError::Other("no mock response".to_owned())))
            })
        }
    }

    struct BlockingHttp {
        started: Arc<Notify>,
    }

    impl ApiHttpClient for BlockingHttp {
        fn execute<'a>(
            &'a self,
            request: ApiHttpRequest,
        ) -> ApiFuture<'a, Result<ApiHttpResponse, ApiTransportError>> {
            Box::pin(async move {
                let _ = request;
                self.started.notify_one();
                std::future::pending().await
            })
        }
    }

    fn success(value: Value) -> Result<ApiHttpResponse, ApiTransportError> {
        Ok(ApiHttpResponse::json(200, value))
    }

    fn json_body(request: &ApiHttpRequest) -> Value {
        serde_json::from_slice(&request.body).unwrap()
    }

    fn request_header<'a>(request: &'a ApiHttpRequest, name: &str) -> Option<&'a str> {
        request
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn new_runtime() -> FakeRuntime {
        FakeRuntime::new([
            [1, 2, 3, 4],
            [5, 6, 7, 8],
            [9, 10, 11, 12],
            [13, 14, 15, 16],
        ])
    }

    #[tokio::test]
    async fn builds_protocol_headers_and_random_uin() {
        let runtime = new_runtime();
        let headers = build_headers_with_runtime(Some("secret"), &runtime);
        let headers: HashMap<_, _> = headers.into_iter().collect();
        assert_eq!(headers["Content-Type"], "application/json");
        assert_eq!(headers["X-WECHAT-UIN"], "AQIDBA==");
        assert_eq!(headers["iLink-App-Id"], "bot");
        assert_eq!(headers["iLink-App-ClientVersion"], "131331");
        assert_eq!(headers["AuthorizationType"], "ilink_bot_token");
        assert_eq!(headers["Authorization"], "Bearer secret");

        let no_token: HashMap<_, _> = build_headers_with_runtime(Some(""), &runtime)
            .into_iter()
            .collect();
        assert!(!no_token.contains_key("Authorization"));
    }

    #[tokio::test]
    async fn get_updates_posts_exact_body_and_honors_timeout() {
        let http = FakeHttp::new([success(json!({"ret": 0, "msgs": []}))]);
        let runtime = new_runtime();
        let response = get_updates_with_http(
            &http,
            &runtime,
            "https://example.test",
            "secret",
            "cursor-1",
            Some(Duration::from_secs(17)),
            None,
        )
        .await
        .unwrap();
        assert_eq!(response, json!({"ret": 0, "msgs": []}));

        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(requests[0].url, "https://example.test/ilink/bot/getupdates");
        assert_eq!(requests[0].timeout, Duration::from_secs(17));
        assert_eq!(
            json_body(&requests[0]),
            json!({
                "get_updates_buf": "cursor-1",
                "base_info": {"channel_version": "2.1.3"}
            })
        );
        assert_eq!(
            request_header(&requests[0], "authorization"),
            Some("Bearer secret")
        );
    }

    #[tokio::test]
    async fn parses_http_error_fields_and_preserves_retry_precedence() {
        let http = FakeHttp::new([Ok(ApiHttpResponse::json(
            429,
            json!({"ret": 3, "errcode": 45011, "errmsg": "rate limited"}),
        ))]);
        let runtime = new_runtime();
        let error = post_json(
            &http,
            &runtime,
            PostJsonRequest {
                base_url: "https://api.test",
                path: "/ilink/bot/getconfig",
                body: json!({"base_info": base_info()}),
                token: Some("token"),
                timeout: Duration::from_secs(3),
                cancellation: None,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, 429);
        assert_eq!(error.ret, Some(3));
        assert_eq!(error.errcode, Some(45011));
        assert_eq!(
            error.message,
            "WeChat API error (HTTP 429, ret=3, errcode=45011): rate limited"
        );
        assert!(error.is_retryable());
    }

    #[tokio::test]
    async fn get_updates_converts_timeout_and_cancellation_to_empty_cursor_response() {
        let runtime = new_runtime();
        let timeout_http = BlockingHttp {
            started: Arc::new(Notify::new()),
        };
        let fallback = get_updates_with_http(
            &timeout_http,
            &runtime,
            "https://example.test",
            "token",
            "same-cursor",
            Some(Duration::from_millis(2)),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            fallback,
            json!({"ret": 0, "msgs": [], "get_updates_buf": "same-cursor"})
        );

        let cancel_http = BlockingHttp {
            started: Arc::new(Notify::new()),
        };
        let started = cancel_http.started.clone();
        let cancellation = CancellationToken::new();
        let task_runtime = new_runtime();
        let token = cancellation.clone();
        let task = tokio::spawn(async move {
            get_updates_with_http(
                &cancel_http,
                &task_runtime,
                "https://example.test",
                "token",
                "cursor-cancel",
                None,
                Some(&token),
            )
            .await
        });
        started.notified().await;
        cancellation.cancel();
        let response = task.await.unwrap().unwrap();
        assert_eq!(response["get_updates_buf"], "cursor-cancel");
        assert_eq!(response["msgs"], json!([]));
    }

    #[tokio::test]
    async fn send_message_sends_msg_and_base_info() {
        let http = FakeHttp::new([success(json!({"ret": 0}))]);
        let runtime = new_runtime();
        send_message_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            Some(json!({"to_user_id": "user", "item_list": []})),
        )
        .await
        .unwrap();
        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://api.test/ilink/bot/sendmessage");
        assert_eq!(
            json_body(&requests[0]),
            json!({
                "msg": {"to_user_id": "user", "item_list": []},
                "base_info": {"channel_version": "2.1.3"}
            })
        );
    }

    #[tokio::test]
    async fn send_message_retries_four_times_with_exponential_delays() {
        let http = FakeHttp::new(
            (0..4).map(|_| Ok(ApiHttpResponse::json(503, json!({"errmsg": "busy"})))),
        );
        let runtime = new_runtime();
        let error = send_message_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            Some(json!({"to_user_id": "user"})),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, 503);
        assert_eq!(http.requests().len(), 4);
        assert_eq!(
            runtime.delays(),
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4)
            ]
        );
    }

    #[tokio::test]
    async fn send_message_retries_transient_api_codes_but_not_expired_sessions() {
        let http = FakeHttp::new([
            success(json!({"errcode": -1, "errmsg": "busy"})),
            success(json!({"errcode": 45011, "errmsg": "rate limited"})),
            Err(ApiTransportError::Network("ECONNRESET".to_owned())),
            success(json!({"ret": 0})),
        ]);
        let runtime = new_runtime();
        send_message_with_http(&http, &runtime, "https://api.test", "token", None)
            .await
            .unwrap();
        assert_eq!(http.requests().len(), 4);
        assert_eq!(
            runtime.delays(),
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4)
            ]
        );

        let expired = FakeHttp::new([success(json!({"errcode": -14, "errmsg": "expired"}))]);
        let runtime = new_runtime();
        let error = send_message_with_http(&expired, &runtime, "https://api.test", "token", None)
            .await
            .unwrap_err();
        assert_eq!(error.errcode, Some(-14));
        assert_eq!(expired.requests().len(), 1);
        assert!(runtime.delays().is_empty());
    }

    #[test]
    fn retry_classification_matches_status_ret_and_errcode_precedence() {
        let error = |status, ret, errcode| WeixinApiError::structured("x", status, ret, errcode);
        assert!(error(503, None, None).is_retryable());
        assert!(error(429, None, None).is_retryable());
        assert!(!error(400, None, None).is_retryable());
        assert!(!error(503, Some(1), Some(0)).is_retryable());
        assert!(error(400, Some(1), Some(-1)).is_retryable());
        assert!(error(200, None, Some(45011)).is_retryable());
        assert!(!error(200, None, Some(-14)).is_retryable());
        assert!(
            WeixinApiError::from_transport(ApiTransportError::Network("reset".into()))
                .is_retryable()
        );
        assert!(
            !WeixinApiError::from_transport(ApiTransportError::Timeout("timeout".into()))
                .is_retryable()
        );
    }

    #[tokio::test]
    async fn get_config_omits_absent_context_token_and_adds_base_info() {
        let http = FakeHttp::new([success(json!({"typing_ticket": "ticket"}))]);
        let runtime = new_runtime();
        let result =
            get_config_with_http(&http, &runtime, "https://api.test", "token", "user-1", None)
                .await
                .unwrap();
        assert_eq!(result["typing_ticket"], "ticket");
        assert_eq!(
            json_body(&http.requests()[0]),
            json!({
                "ilink_user_id": "user-1",
                "base_info": {"channel_version": "2.1.3"}
            })
        );

        let http = FakeHttp::new([success(json!({"ret": 0}))]);
        get_config_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            "user-1",
            Some("ctx"),
        )
        .await
        .unwrap();
        assert_eq!(json_body(&http.requests()[0])["context_token"], "ctx");
    }

    #[tokio::test]
    async fn send_typing_spreads_request_and_appends_base_info() {
        let http = FakeHttp::new([success(json!({"ret": 0}))]);
        let runtime = new_runtime();
        send_typing_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            json!({"ilink_user_id": "user-1", "typing_ticket": "ticket", "status": 1}),
        )
        .await
        .unwrap();
        assert_eq!(
            http.requests()[0].url,
            "https://api.test/ilink/bot/sendtyping"
        );
        assert_eq!(
            json_body(&http.requests()[0]),
            json!({
                "ilink_user_id": "user-1",
                "typing_ticket": "ticket",
                "status": 1,
                "base_info": {"channel_version": "2.1.3"}
            })
        );
    }

    #[tokio::test]
    async fn upload_url_sends_contract_and_prefers_full_url() {
        let http = FakeHttp::new([success(json!({
            "ret": 0,
            "upload_full_url": "https://cdn.test/full?x=1",
            "upload_param": "param-only"
        }))]);
        let runtime = new_runtime();
        let result = get_upload_url_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            "user-1",
            "file-key",
            12,
            "md5-value",
            32,
            "00112233445566778899aabbccddeeff",
        )
        .await
        .unwrap();
        assert_eq!(result, "https://cdn.test/full?x=1");
        assert_eq!(
            http.requests()[0].url,
            "https://api.test/ilink/bot/getuploadurl"
        );
        assert_eq!(
            json_body(&http.requests()[0]),
            json!({
                "filekey": "file-key",
                "media_type": 1,
                "to_user_id": "user-1",
                "rawsize": 12,
                "rawfilemd5": "md5-value",
                "filesize": 32,
                "no_need_thumb": true,
                "aeskey": "00112233445566778899aabbccddeeff",
                "base_info": {"channel_version": "2.1.3"}
            })
        );
    }

    #[tokio::test]
    async fn upload_url_falls_back_to_param_and_retries_errcode_minus_one() {
        let http = FakeHttp::new([
            success(json!({"ret": 0, "errcode": -1, "errmsg": "busy"})),
            success(json!({"upload_param": "param-only"})),
        ]);
        let runtime = new_runtime();
        let result = get_upload_url_with_http(
            &http,
            &runtime,
            "https://api.test",
            "token",
            "user-1",
            "1",
            1,
            "md5",
            16,
            "key",
        )
        .await
        .unwrap();
        assert_eq!(result, "param-only");
        assert_eq!(http.requests().len(), 2);
        assert_eq!(runtime.delays(), [Duration::from_secs(1)]);
    }

    #[tokio::test]
    async fn upload_to_cdn_builds_encoded_url_and_sends_octets() {
        let http = FakeHttp::new([Ok(ApiHttpResponse::json(200, json!({"ret": 0}))
            .with_header("X-Encrypted-Param", "cdn-param"))]);
        let runtime = new_runtime();
        let param = "a b!~*'()&x";
        let result = upload_to_cdn_with_http(
            &http,
            &runtime,
            param,
            "file/key",
            b"encrypted",
            Duration::from_secs(9),
        )
        .await
        .unwrap();
        assert_eq!(result, "cdn-param");
        let request = &http.requests()[0];
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.url,
            "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=a%20b!~*'()%26x&filekey=file%2Fkey"
        );
        assert_eq!(request.body, b"encrypted");
        assert_eq!(request.timeout, Duration::from_secs(9));
        assert_eq!(
            request_header(request, "content-type"),
            Some("application/octet-stream")
        );
    }

    #[tokio::test]
    async fn upload_to_cdn_accepts_case_insensitive_https_and_preserves_full_url() {
        let url = "HTTPS://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=abc";
        let http = FakeHttp::new([Ok(
            ApiHttpResponse::json(200, json!({})).with_header("x-encrypted-param", "ok")
        )]);
        let runtime = new_runtime();
        assert_eq!(
            upload_to_cdn_with_http(&http, &runtime, url, "key", b"bytes", DEFAULT_API_TIMEOUT)
                .await
                .unwrap(),
            "ok"
        );
        assert_eq!(http.requests()[0].url, url);
    }

    #[tokio::test]
    async fn upload_to_cdn_rejects_http_and_unexpected_hosts_before_network() {
        for (url, expected) in [
            (
                "HTTP://novac2c.cdn.weixin.qq.com/path",
                "CDN upload URL must use HTTPS",
            ),
            (
                "https://evil.example/path",
                "CDN upload URL has unexpected host: evil.example",
            ),
            (
                "https://novac2c.cdn.weixin.qq.com.evil.example/path",
                "CDN upload URL has unexpected host: novac2c.cdn.weixin.qq.com.evil.example",
            ),
            (
                "https://novac2c.cdn.weixin.qq.com@evil.example/path",
                "CDN upload URL has unexpected host: evil.example",
            ),
        ] {
            let http = FakeHttp::new([]);
            let runtime = new_runtime();
            let error =
                upload_to_cdn_with_http(&http, &runtime, url, "key", b"bytes", DEFAULT_API_TIMEOUT)
                    .await
                    .unwrap_err();
            assert!(error.message.contains(expected), "{}", error.message);
            assert!(http.requests().is_empty());
        }
    }

    #[tokio::test]
    async fn upload_to_cdn_retries_server_errors_but_missing_header_is_not_retryable() {
        let http = FakeHttp::new(
            (0..4).map(|_| Ok(ApiHttpResponse::json(503, json!({"errmsg": "busy"})))),
        );
        let runtime = new_runtime();
        let error = upload_to_cdn_with_http(
            &http,
            &runtime,
            "https://novac2c.cdn.weixin.qq.com/path",
            "key",
            b"bytes",
            DEFAULT_API_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("HTTP 503 — busy"));
        assert_eq!(http.requests().len(), 4);
        assert_eq!(
            runtime.delays(),
            [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4)
            ]
        );

        let http = FakeHttp::new([Ok(ApiHttpResponse::json(200, json!({})))]);
        let runtime = new_runtime();
        let error = upload_to_cdn_with_http(
            &http,
            &runtime,
            "https://novac2c.cdn.weixin.qq.com/path",
            "key",
            b"bytes",
            DEFAULT_API_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert!(error.message.contains("missing x-encrypted-param"));
        assert_eq!(http.requests().len(), 1);
        assert!(runtime.delays().is_empty());
    }

    #[tokio::test]
    async fn upload_to_cdn_timeout_is_not_retried() {
        let http = BlockingHttp {
            started: Arc::new(Notify::new()),
        };
        let runtime = new_runtime();
        let error = upload_to_cdn_with_http(
            &http,
            &runtime,
            "https://novac2c.cdn.weixin.qq.com/path",
            "key",
            b"bytes",
            Duration::from_millis(2),
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, 0);
        assert!(runtime.delays().is_empty());
    }
}
