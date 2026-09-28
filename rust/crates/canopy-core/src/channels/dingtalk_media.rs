//! DingTalk media downloads through the robot message-files API.
//!
//! Port of `packages/channels/dingtalk/src/media.ts`. The download is a
//! two-request operation: a token-authenticated POST exchanges `downloadCode`
//! for a temporary URL, then an unauthenticated timed GET streams the file.
//! The transport and body traits keep request shape, streaming limits, and
//! awaited cancellation testable without network access.

use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const DOWNLOAD_API: &str = "https://api.dingtalk.com/v1.0/robot/messageFiles/download";
const MAX_DOWNLOAD_BYTES: usize = 50 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30);

pub type DingtalkMediaFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaFile {
    pub buffer: Vec<u8>,
    pub mime_type: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaHttpMethod {
    Post,
    Get,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaHttpRequest {
    pub method: MediaHttpMethod,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    /// The TypeScript API POST has no explicit timeout; the CDN GET is limited
    /// to 30 seconds so a stalled file connection cannot hang the caller.
    pub timeout: Option<Duration>,
}

pub struct MediaHttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Option<Box<dyn MediaBody>>,
}

impl MediaHttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: None,
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers
            .insert(name.into().to_ascii_lowercase(), value.into());
        self
    }

    pub fn with_body(mut self, body: impl MediaBody + 'static) -> Self {
        self.body = Some(Box::new(body));
        self
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

/// Streaming response body. Cancellation is asynchronous on purpose: the
/// downloader awaits teardown so cancellation failures are caught by the same
/// outer error path as fetch and stream failures.
pub trait MediaBody: Send {
    fn read_chunk<'a>(&'a mut self) -> DingtalkMediaFuture<'a, Result<Option<Vec<u8>>, String>>;
    fn cancel<'a>(&'a mut self) -> DingtalkMediaFuture<'a, Result<(), String>>;
}

/// One request seam covers both the download-code exchange and file request.
pub trait DingtalkMediaHttpClient: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: MediaHttpRequest,
    ) -> DingtalkMediaFuture<'a, Result<MediaHttpResponse, String>>;
}

/// Downloads through the pooled reqwest client using the production transport.
pub async fn download_media(
    client: &Client,
    download_code: &str,
    robot_code: &str,
    access_token: &str,
) -> Option<MediaFile> {
    let http = ReqwestDingtalkMediaHttpClient { client };
    download_media_with_http(&http, download_code, robot_code, access_token).await
}

/// Downloads through an injected transport. This is the same request and
/// streaming path as [`download_media`], without requiring live HTTP in tests.
pub async fn download_media_with_http(
    client: &dyn DingtalkMediaHttpClient,
    download_code: &str,
    robot_code: &str,
    access_token: &str,
) -> Option<MediaFile> {
    download_media_with_limit(
        client,
        download_code,
        robot_code,
        access_token,
        MAX_DOWNLOAD_BYTES,
    )
    .await
}

async fn download_media_with_limit(
    client: &dyn DingtalkMediaHttpClient,
    download_code: &str,
    robot_code: &str,
    access_token: &str,
    max_download_bytes: usize,
) -> Option<MediaFile> {
    if download_code.is_empty() || robot_code.is_empty() || access_token.is_empty() {
        return None;
    }

    match download_media_inner(
        client,
        download_code,
        robot_code,
        access_token,
        max_download_bytes,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            eprintln!("[DingTalk] downloadMedia error: {error}");
            None
        }
    }
}

async fn download_media_inner(
    client: &dyn DingtalkMediaHttpClient,
    download_code: &str,
    robot_code: &str,
    access_token: &str,
    max_download_bytes: usize,
) -> Result<Option<MediaFile>, String> {
    let api_body = serde_json::to_vec(&json!({
        "downloadCode": download_code,
        "robotCode": robot_code,
    }))
    .map_err(|error| error.to_string())?;
    let mut api_response = client
        .execute(MediaHttpRequest {
            method: MediaHttpMethod::Post,
            url: DOWNLOAD_API.to_owned(),
            headers: vec![
                (
                    "x-acs-dingtalk-access-token".to_owned(),
                    access_token.to_owned(),
                ),
                ("Content-Type".to_owned(), "application/json".to_owned()),
            ],
            body: Some(api_body),
            timeout: None,
        })
        .await?;

    if !(200..300).contains(&api_response.status) {
        let detail = match api_response.body.as_mut() {
            Some(body) => read_body_text(body.as_mut()).await.unwrap_or_default(),
            None => String::new(),
        };
        eprintln!(
            "[DingTalk] downloadMedia API failed: HTTP {} {}",
            api_response.status, detail
        );
        return Ok(None);
    }

    let api_bytes = match api_response.body.take() {
        Some(body) => read_body_bytes(body).await?,
        None => return Ok(None),
    };
    let payload: Value = match serde_json::from_slice(&api_bytes) {
        Ok(payload) => payload,
        Err(error) => return Err(error.to_string()),
    };
    let download_url_value = match payload.get("downloadUrl") {
        Some(value) if !value.is_null() => Some(value),
        _ => payload
            .get("data")
            .and_then(Value::as_object)
            .and_then(|data| data.get("downloadUrl")),
    };
    let Some(download_url) = download_url_value.and_then(Value::as_str) else {
        eprintln!("[DingTalk] downloadMedia: no downloadUrl in response");
        return Ok(None);
    };
    if download_url.is_empty() {
        eprintln!("[DingTalk] downloadMedia: no downloadUrl in response");
        return Ok(None);
    }

    let mut file_response = client
        .execute(MediaHttpRequest {
            method: MediaHttpMethod::Get,
            url: download_url.to_owned(),
            headers: Vec::new(),
            body: None,
            timeout: Some(DOWNLOAD_TIMEOUT),
        })
        .await?;

    if !(200..300).contains(&file_response.status) {
        eprintln!(
            "[DingTalk] downloadMedia file fetch failed: HTTP {}",
            file_response.status
        );
        return Ok(None);
    }

    let content_length = file_response.header("content-length").map(str::to_owned);
    if let Some(content_length) = content_length
        .as_deref()
        .filter(|content_length| !content_length.is_empty())
    {
        if parse_js_integer(content_length).is_some_and(|size| size > max_download_bytes as f64) {
            if let Some(body) = file_response.body.as_mut() {
                body.cancel().await?;
            }
            eprintln!(
                "[DingTalk] downloadMedia rejected: size {content_length} exceeds {max_download_bytes} byte limit"
            );
            return Ok(None);
        }
    }

    let mime_type = file_response
        .header("content-type")
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let Some(mut body) = file_response.body.take() else {
        return Ok(None);
    };

    let mut buffer = Vec::new();
    let mut total_size = 0_usize;
    while let Some(chunk) = body.read_chunk().await? {
        total_size = total_size.saturating_add(chunk.len());
        if total_size > max_download_bytes {
            body.cancel().await?;
            eprintln!(
                "[DingTalk] downloadMedia rejected: actual size exceeds {max_download_bytes} byte limit"
            );
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk);
    }

    Ok(Some(MediaFile { buffer, mime_type }))
}

async fn read_body_bytes(mut body: Box<dyn MediaBody>) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.read_chunk().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn read_body_text(body: &mut dyn MediaBody) -> Result<String, String> {
    let mut bytes = Vec::new();
    while let Some(chunk) = body.read_chunk().await? {
        bytes.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

/// Mirrors JavaScript `parseInt(text, 10)`, which accepts a sign and leading
/// digit run and ignores any following non-digit suffix.
fn parse_js_integer(value: &str) -> Option<f64> {
    let trimmed = value.trim_matches(is_ecmascript_whitespace);
    let (sign, digits) = match trimmed.as_bytes().first() {
        Some(b'+') => (1.0, &trimmed[1..]),
        Some(b'-') => (-1.0, &trimmed[1..]),
        _ => (1.0, trimmed),
    };
    let digit_count = digits.bytes().take_while(u8::is_ascii_digit).count();
    if digit_count == 0 {
        return None;
    }
    digits[..digit_count]
        .parse::<f64>()
        .ok()
        .map(|number| sign * number)
}

fn is_ecmascript_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'..='\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

struct ReqwestDingtalkMediaHttpClient<'a> {
    client: &'a Client,
}

impl DingtalkMediaHttpClient for ReqwestDingtalkMediaHttpClient<'_> {
    fn execute<'a>(
        &'a self,
        request: MediaHttpRequest,
    ) -> DingtalkMediaFuture<'a, Result<MediaHttpResponse, String>> {
        Box::pin(async move {
            let mut builder = match request.method {
                MediaHttpMethod::Post => self.client.post(&request.url),
                MediaHttpMethod::Get => self.client.get(&request.url),
            };
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if let Some(body) = request.body {
                builder = builder.body(body);
            }
            if let Some(timeout) = request.timeout {
                builder = builder.timeout(timeout);
            }

            let response = builder.send().await.map_err(|error| error.to_string())?;
            let status = response.status().as_u16();
            let mut result = MediaHttpResponse::new(status);
            if let Some(value) = response
                .headers()
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
            {
                result
                    .headers
                    .insert("content-length".to_owned(), value.to_owned());
            }
            if let Some(value) = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
            {
                result
                    .headers
                    .insert("content-type".to_owned(), value.to_owned());
            }
            if !matches!(status, 204 | 304) {
                result.body = Some(Box::new(ReqwestMediaBody {
                    stream: Some(Box::pin(response.bytes_stream())),
                }));
            }
            Ok(result)
        })
    }
}

type ReqwestByteStream =
    Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;

struct ReqwestMediaBody {
    stream: Option<ReqwestByteStream>,
}

impl MediaBody for ReqwestMediaBody {
    fn read_chunk<'a>(&'a mut self) -> DingtalkMediaFuture<'a, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            match self.stream.as_mut() {
                Some(stream) => stream
                    .next()
                    .await
                    .map(|result| {
                        result
                            .map(|bytes| bytes.to_vec())
                            .map_err(|error| error.to_string())
                    })
                    .transpose(),
                None => Ok(None),
            }
        })
    }

    fn cancel<'a>(&'a mut self) -> DingtalkMediaFuture<'a, Result<(), String>> {
        Box::pin(async move {
            // Dropping reqwest's body stream releases the response connection.
            self.stream.take();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DOWNLOAD_API, DOWNLOAD_TIMEOUT, DingtalkMediaFuture, DingtalkMediaHttpClient,
        MAX_DOWNLOAD_BYTES, MediaBody, MediaFile, MediaHttpMethod, MediaHttpRequest,
        MediaHttpResponse, download_media_with_http, download_media_with_limit,
    };
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    struct MockBody {
        chunks: VecDeque<Result<Option<Vec<u8>>, String>>,
        read_count: Arc<AtomicUsize>,
        cancel_count: Arc<AtomicUsize>,
        cancel_error: Option<String>,
        cancel_started: Option<Arc<Notify>>,
        cancel_release: Option<Arc<Notify>>,
    }

    impl MockBody {
        fn chunks(chunks: impl IntoIterator<Item = Vec<u8>>) -> (Self, Arc<AtomicUsize>) {
            let counter = Arc::new(AtomicUsize::new(0));
            let read_count = Arc::new(AtomicUsize::new(0));
            let chunks = chunks
                .into_iter()
                .map(|chunk| Ok(Some(chunk)))
                .chain(std::iter::once(Ok(None)))
                .collect();
            (
                Self {
                    chunks,
                    read_count,
                    cancel_count: Arc::clone(&counter),
                    cancel_error: None,
                    cancel_started: None,
                    cancel_release: None,
                },
                counter,
            )
        }

        fn with_cancel_error(mut self, error: &str) -> Self {
            self.cancel_error = Some(error.to_owned());
            self
        }

        fn with_read_error(error: &str) -> (Self, Arc<AtomicUsize>) {
            let counter = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    chunks: VecDeque::from([Err(error.to_owned())]),
                    read_count: Arc::clone(&counter),
                    cancel_count: Arc::new(AtomicUsize::new(0)),
                    cancel_error: None,
                    cancel_started: None,
                    cancel_release: None,
                },
                counter,
            )
        }
    }

    impl MediaBody for MockBody {
        fn read_chunk<'a>(
            &'a mut self,
        ) -> DingtalkMediaFuture<'a, Result<Option<Vec<u8>>, String>> {
            self.read_count.fetch_add(1, Ordering::SeqCst);
            let next = self.chunks.pop_front().unwrap_or(Ok(None));
            Box::pin(async move { next })
        }

        fn cancel<'a>(&'a mut self) -> DingtalkMediaFuture<'a, Result<(), String>> {
            self.cancel_count.fetch_add(1, Ordering::SeqCst);
            let started = self.cancel_started.clone();
            let release = self.cancel_release.clone();
            let error = self.cancel_error.clone();
            Box::pin(async move {
                if let Some(started) = started {
                    started.notify_one();
                }
                if let Some(release) = release {
                    release.notified().await;
                }
                match error {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RequestRecord {
        method: MediaHttpMethod,
        url: String,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
        timeout: Option<Duration>,
    }

    struct MockHttpClient {
        responses: Mutex<VecDeque<Result<MediaHttpResponse, String>>>,
        requests: Mutex<Vec<RequestRecord>>,
    }

    impl MockHttpClient {
        fn new(responses: impl IntoIterator<Item = Result<MediaHttpResponse, String>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<RequestRecord> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl DingtalkMediaHttpClient for MockHttpClient {
        fn execute<'a>(
            &'a self,
            request: MediaHttpRequest,
        ) -> DingtalkMediaFuture<'a, Result<MediaHttpResponse, String>> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RequestRecord {
                    method: request.method,
                    url: request.url,
                    headers: request.headers,
                    body: request.body,
                    timeout: request.timeout,
                });
            let response = self
                .responses
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_else(|| Err("no mock response".to_owned()));
            Box::pin(async move { response })
        }
    }

    fn response(status: u16, body: MockBody) -> MediaHttpResponse {
        MediaHttpResponse::new(status).with_body(body)
    }

    fn api_response_json(status: u16, body: String) -> MediaHttpResponse {
        let (body, _) = MockBody::chunks([body.into_bytes()]);
        response(status, body)
    }

    #[tokio::test]
    async fn downloads_file_with_two_step_request_shape_and_expected_headers() {
        assert_eq!(MAX_DOWNLOAD_BYTES, 50 * 1024 * 1024);
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.dingtalk.example/file"}"#.to_vec()]);
        let (file_body, _) = MockBody::chunks([vec![1, 2], vec![3, 4]]);
        let client = MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, file_body)
                .with_header("Content-Length", "4")
                .with_header("Content-Type", "image/png")),
        ]);

        let result = download_media_with_http(&client, "code", "robot", "token").await;

        assert_eq!(
            result,
            Some(MediaFile {
                buffer: vec![1, 2, 3, 4],
                mime_type: "image/png".to_owned(),
            })
        );
        let requests = client.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, MediaHttpMethod::Post);
        assert_eq!(requests[0].url, DOWNLOAD_API);
        assert_eq!(
            requests[0].headers,
            [
                ("x-acs-dingtalk-access-token".to_owned(), "token".to_owned()),
                ("Content-Type".to_owned(), "application/json".to_owned()),
            ]
        );
        assert_eq!(
            requests[0].body.as_deref(),
            Some(br#"{"downloadCode":"code","robotCode":"robot"}"#.as_slice())
        );
        assert_eq!(requests[0].timeout, None);
        assert_eq!(requests[1].method, MediaHttpMethod::Get);
        assert_eq!(requests[1].url, "https://dl.dingtalk.example/file");
        assert!(requests[1].headers.is_empty());
        assert_eq!(requests[1].timeout, Some(DOWNLOAD_TIMEOUT));
    }

    #[tokio::test]
    async fn falls_back_to_nested_data_download_url() {
        let (api_body, _) = MockBody::chunks([
            br#"{"data":{"downloadUrl":"https://cdn.example/fallback"}}"#.to_vec(),
        ]);
        let (file_body, _) = MockBody::chunks([vec![9]]);
        let client =
            MockHttpClient::new([Ok(response(200, api_body)), Ok(response(200, file_body))]);
        let result = download_media_with_http(&client, "code", "robot", "token")
            .await
            .unwrap();
        assert_eq!(result.buffer, [9]);
        assert_eq!(client.requests()[1].url, "https://cdn.example/fallback");
    }

    #[tokio::test]
    async fn null_primary_url_falls_back_but_empty_primary_does_not() {
        let (api_body, _) = MockBody::chunks([
            br#"{"downloadUrl":null,"data":{"downloadUrl":"https://cdn.example/fallback"}}"#
                .to_vec(),
        ]);
        let (file_body, _) = MockBody::chunks([vec![2]]);
        let client =
            MockHttpClient::new([Ok(response(200, api_body)), Ok(response(200, file_body))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_some()
        );

        let (api_body, _) = MockBody::chunks([
            br#"{"downloadUrl":"","data":{"downloadUrl":"https://cdn.example/ignored"}}"#.to_vec(),
        ]);
        let client = MockHttpClient::new([Ok(response(200, api_body))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        assert_eq!(client.requests().len(), 1);
    }

    #[tokio::test]
    async fn rejects_empty_credentials_without_making_requests() {
        let client = MockHttpClient::new([]);
        for (code, robot, token) in [
            ("", "robot", "token"),
            ("code", "", "token"),
            ("code", "robot", ""),
        ] {
            assert!(
                download_media_with_http(&client, code, robot, token)
                    .await
                    .is_none()
            );
        }
        assert!(client.requests().is_empty());
    }

    #[tokio::test]
    async fn returns_none_on_api_http_error_and_swallows_error_body_failure() {
        let (api_error_body, _) = MockBody::chunks([b"denied".to_vec()]);
        let (failed_body, _) = MockBody::with_read_error("failed reading error body");
        let client = MockHttpClient::new([
            Ok(response(401, api_error_body)),
            Ok(response(502, failed_body)),
        ]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        assert_eq!(client.requests().len(), 2);
    }

    #[tokio::test]
    async fn returns_none_for_bad_api_json_or_missing_url_without_get() {
        let (body, _) = MockBody::chunks([b"not json".to_vec()]);
        let client = MockHttpClient::new([Ok(response(200, body))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        assert_eq!(client.requests().len(), 1);

        let client = MockHttpClient::new([Ok(api_response_json(200, r#"{"data":{}}"#.to_owned()))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        assert_eq!(client.requests().len(), 1);
    }

    #[tokio::test]
    async fn rejects_oversized_content_length_and_awaits_body_cancel() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut body, cancel_count) = MockBody::chunks([vec![1]]);
        body.cancel_started = Some(Arc::clone(&started));
        body.cancel_release = Some(Arc::clone(&release));
        let client = Arc::new(MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, body).with_header("content-length", "60000000")),
        ]));

        let download = tokio::spawn({
            let client = Arc::clone(&client);
            async move { download_media_with_http(client.as_ref(), "code", "robot", "token").await }
        });
        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
        assert!(!download.is_finished());
        release.notify_one();
        assert!(download.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn advertised_size_uses_javascript_parse_int_behavior() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (body, cancel_count) = MockBody::chunks([vec![1]]);
        // A numeric prefix followed by units still exceeds the local test cap.
        let client = MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, body).with_header("content-length", " 11bytes")),
        ]);
        assert!(
            download_media_with_limit(&client, "code", "robot", "token", 10)
                .await
                .is_none()
        );
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);

        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (body, _) = MockBody::chunks([vec![1]]);
        let client = MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, body).with_header("content-length", "not-a-size")),
        ]);
        assert!(
            download_media_with_limit(&client, "code", "robot", "token", 10)
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn accepts_advertised_and_streamed_bodies_exactly_at_the_limit() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (file_body, _) = MockBody::chunks([vec![1; 10]]);
        let client = MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, file_body).with_header("content-length", "10")),
        ]);
        assert_eq!(
            download_media_with_limit(&client, "code", "robot", "token", 10)
                .await
                .unwrap()
                .buffer
                .len(),
            10
        );

        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (file_body, _) = MockBody::chunks([vec![1; 7], vec![2; 3]]);
        let client =
            MockHttpClient::new([Ok(response(200, api_body)), Ok(response(200, file_body))]);
        assert_eq!(
            download_media_with_limit(&client, "code", "robot", "token", 10)
                .await
                .unwrap()
                .buffer
                .len(),
            10
        );
    }

    #[tokio::test]
    async fn streamed_size_limit_awaits_cancel_without_content_length() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut body, cancel_count) = MockBody::chunks([vec![1; 7], vec![2; 4], vec![3]]);
        body.cancel_started = Some(Arc::clone(&started));
        body.cancel_release = Some(Arc::clone(&release));
        let client = Arc::new(MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(response(200, body)),
        ]));
        let download = tokio::spawn({
            let client = Arc::clone(&client);
            async move { download_media_with_limit(client.as_ref(), "code", "robot", "token", 10).await }
        });

        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
        assert!(!download.is_finished());
        release.notify_one();
        assert!(download.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn awaited_cancel_errors_and_stream_read_errors_return_none() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (body, cancel_count) = MockBody::chunks([vec![1]]);
        let client = MockHttpClient::new([
            Ok(response(200, api_body)),
            Ok(
                response(200, body.with_cancel_error("body teardown failed"))
                    .with_header("content-length", "11"),
            ),
        ]);
        assert!(
            download_media_with_limit(&client, "code", "robot", "token", 10)
                .await
                .is_none()
        );
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);

        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (body, _) = MockBody::with_read_error("stream read failed");
        let client = MockHttpClient::new([Ok(response(200, api_body)), Ok(response(200, body))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn rejects_non_success_file_response_without_reading_its_body() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let (file_body, read_count) = MockBody::with_read_error("must not be read");
        let client =
            MockHttpClient::new([Ok(response(200, api_body)), Ok(response(403, file_body))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );
        // This mock body errors on read; the returned failure is due to status.
        assert_eq!(read_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn absent_body_returns_none_and_missing_or_empty_mime_uses_default() {
        let (api_body, _) =
            MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
        let client =
            MockHttpClient::new([Ok(response(200, api_body)), Ok(MediaHttpResponse::new(200))]);
        assert!(
            download_media_with_http(&client, "code", "robot", "token")
                .await
                .is_none()
        );

        for header in [None, Some("")] {
            let (api_body, _) =
                MockBody::chunks([br#"{"downloadUrl":"https://dl.example/file"}"#.to_vec()]);
            let (body, _) = MockBody::chunks([vec![1, 2, 3]]);
            let mut file_response = response(200, body);
            if let Some(header) = header {
                file_response = file_response.with_header("content-type", header);
            }
            let client = MockHttpClient::new([Ok(response(200, api_body)), Ok(file_response)]);
            let result = download_media_with_http(&client, "code", "robot", "token")
                .await
                .unwrap();
            assert_eq!(result.buffer, [1, 2, 3]);
            assert_eq!(result.mime_type, "application/octet-stream");
        }
    }
}
