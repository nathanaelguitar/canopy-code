//! Feishu Open API resource downloads.
//!
//! Port of `packages/channels/feishu/src/media.ts`. The HTTP seam keeps
//! streaming, timeout, body-release, and failure behavior testable without a
//! network connection.

use futures_util::StreamExt;
use reqwest::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

const BASE_URL: &str = "https://open.feishu.cn/open-apis";
const MAX_DOWNLOAD_BYTES: usize = 50 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(30);

pub type MediaFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeishuResourceType {
    Image,
    File,
}

impl FeishuResourceType {
    fn as_query_value(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MediaFile {
    pub buffer: Vec<u8>,
    pub mime_type: String,
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

/// HTTP response-body seam. `cancel` is async so adapters and tests can
/// surface teardown errors; download rejection awaits it before returning.
pub trait MediaBody: Send {
    fn read_chunk<'a>(&'a mut self) -> MediaFuture<'a, Result<Option<Vec<u8>>, String>>;
    fn cancel<'a>(&'a mut self) -> MediaFuture<'a, Result<(), String>>;
}

/// Minimal GET interface used by the downloader and local/mock test clients.
pub trait MediaHttpClient: Send + Sync {
    fn get<'a>(
        &'a self,
        url: &'a str,
        access_token: &'a str,
        timeout: Duration,
    ) -> MediaFuture<'a, Result<MediaHttpResponse, String>>;
}

/// Download a Feishu image or file using the supplied pooled reqwest client.
pub async fn download_media(
    client: &Client,
    message_id: &str,
    file_key: &str,
    resource_type: FeishuResourceType,
    access_token: &str,
) -> Option<MediaFile> {
    let http = ReqwestMediaHttpClient { client };
    download_media_with_http(&http, message_id, file_key, resource_type, access_token).await
}

/// Download through a caller-provided transport. This is the same path used by
/// [`download_media`] and permits deterministic tests without external HTTP.
pub async fn download_media_with_http(
    client: &dyn MediaHttpClient,
    message_id: &str,
    file_key: &str,
    resource_type: FeishuResourceType,
    access_token: &str,
) -> Option<MediaFile> {
    download_media_with_limit(
        client,
        message_id,
        file_key,
        resource_type,
        access_token,
        MAX_DOWNLOAD_BYTES,
    )
    .await
}

async fn download_media_with_limit(
    client: &dyn MediaHttpClient,
    message_id: &str,
    file_key: &str,
    resource_type: FeishuResourceType,
    access_token: &str,
    max_download_bytes: usize,
) -> Option<MediaFile> {
    if message_id.is_empty()
        || file_key.is_empty()
        || access_token.is_empty()
        || !is_valid_feishu_id(message_id)
        || !is_valid_feishu_id(file_key)
    {
        return None;
    }

    match download_media_inner(
        client,
        message_id,
        file_key,
        resource_type,
        access_token,
        max_download_bytes,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            eprintln!("[Feishu] downloadMedia error: {error}");
            None
        }
    }
}

async fn download_media_inner(
    client: &dyn MediaHttpClient,
    message_id: &str,
    file_key: &str,
    resource_type: FeishuResourceType,
    access_token: &str,
    max_download_bytes: usize,
) -> Result<Option<MediaFile>, String> {
    let url = format!(
        "{BASE_URL}/im/v1/messages/{message_id}/resources/{file_key}?type={}",
        resource_type.as_query_value()
    );
    let mut response = client.get(&url, access_token, DOWNLOAD_TIMEOUT).await?;

    if !(200..300).contains(&response.status) {
        let detail = match response.body.as_mut() {
            Some(body) => read_http_error_text(body.as_mut())
                .await
                .unwrap_or_default(),
            None => String::new(),
        };
        eprintln!(
            "[Feishu] downloadMedia failed: HTTP {} {}",
            response.status, detail
        );
        return Ok(None);
    }

    let content_length = response.header("content-length").map(str::to_owned);
    if let Some(content_length) = content_length.as_deref().filter(|value| !value.is_empty()) {
        if parse_js_integer(content_length).is_some_and(|size| size > max_download_bytes as f64) {
            if let Some(body) = response.body.as_mut() {
                body.cancel().await?;
            }
            eprintln!(
                "[Feishu] downloadMedia rejected: size {content_length} exceeds {max_download_bytes} byte limit"
            );
            return Ok(None);
        }
    }

    let mime_type = response
        .header("content-type")
        .filter(|value| !value.is_empty())
        .unwrap_or("application/octet-stream")
        .to_owned();
    let Some(mut body) = response.body.take() else {
        return Ok(None);
    };

    let mut bytes = Vec::new();
    let mut total_size = 0_usize;
    while let Some(chunk) = body.read_chunk().await? {
        total_size = total_size.saturating_add(chunk.len());
        if total_size > max_download_bytes {
            body.cancel().await?;
            eprintln!(
                "[Feishu] downloadMedia rejected: actual size exceeds {max_download_bytes} byte limit"
            );
            return Ok(None);
        }
        bytes.extend_from_slice(&chunk);
    }

    Ok(Some(MediaFile {
        buffer: bytes,
        mime_type,
    }))
}

async fn read_http_error_text(body: &mut dyn MediaBody) -> Result<String, String> {
    let mut bytes = Vec::new();
    loop {
        match body.read_chunk().await? {
            Some(chunk) => bytes.extend_from_slice(&chunk),
            None => return Ok(String::from_utf8_lossy(&bytes).into_owned()),
        }
    }
}

fn is_valid_feishu_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
}

/// JavaScript's `parseInt(text, 10)` accepts a sign and a leading decimal digit
/// run, ignoring any suffix. Non-numeric and empty strings produce `NaN` there
/// and therefore never trip the `>` size check.
fn parse_js_integer(value: &str) -> Option<f64> {
    let trimmed = value.trim_matches(is_js_whitespace);
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

fn is_js_whitespace(character: char) -> bool {
    character.is_whitespace() || character == '\u{feff}'
}

struct ReqwestMediaHttpClient<'a> {
    client: &'a Client,
}

impl MediaHttpClient for ReqwestMediaHttpClient<'_> {
    fn get<'a>(
        &'a self,
        url: &'a str,
        access_token: &'a str,
        timeout: Duration,
    ) -> MediaFuture<'a, Result<MediaHttpResponse, String>> {
        let url = url.to_owned();
        let access_token = access_token.to_owned();
        Box::pin(async move {
            let response = self
                .client
                .get(&url)
                .header(AUTHORIZATION, format!("Bearer {access_token}"))
                .timeout(timeout)
                .send()
                .await
                .map_err(|error| error.to_string())?;
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
    Pin<Box<dyn futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>;

struct ReqwestMediaBody {
    stream: Option<ReqwestByteStream>,
}

impl MediaBody for ReqwestMediaBody {
    fn read_chunk<'a>(&'a mut self) -> MediaFuture<'a, Result<Option<Vec<u8>>, String>> {
        Box::pin(async move {
            match self.stream.as_mut() {
                Some(stream) => stream
                    .next()
                    .await
                    .map(|result| {
                        result
                            .map(|bytes| bytes.to_vec())
                            .map_err(|e| e.to_string())
                    })
                    .transpose(),
                None => Ok(None),
            }
        })
    }

    fn cancel<'a>(&'a mut self) -> MediaFuture<'a, Result<(), String>> {
        Box::pin(async move {
            // reqwest exposes cancellation by dropping the response stream;
            // the interface remains async so transports with fallible teardown
            // can report that failure to the downloader's outer catch.
            self.stream.take();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DOWNLOAD_TIMEOUT, FeishuResourceType, MediaBody, MediaFile, MediaFuture, MediaHttpClient,
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
        cancel_count: Arc<AtomicUsize>,
        cancel_error: Option<String>,
        cancel_started: Option<Arc<Notify>>,
        cancel_release: Option<Arc<Notify>>,
    }

    impl MockBody {
        fn chunks(chunks: impl IntoIterator<Item = Vec<u8>>) -> (Self, Arc<AtomicUsize>) {
            let counter = Arc::new(AtomicUsize::new(0));
            let chunks = chunks
                .into_iter()
                .map(|chunk| Ok(Some(chunk)))
                .chain(std::iter::once(Ok(None)))
                .collect();
            (
                Self {
                    chunks,
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

        fn with_read_error(error: &str, counter: Arc<AtomicUsize>) -> Self {
            Self {
                chunks: VecDeque::from([Err(error.to_owned())]),
                cancel_count: counter,
                cancel_error: None,
                cancel_started: None,
                cancel_release: None,
            }
        }
    }

    impl MediaBody for MockBody {
        fn read_chunk<'a>(&'a mut self) -> MediaFuture<'a, Result<Option<Vec<u8>>, String>> {
            let next = self.chunks.pop_front().unwrap_or(Ok(None));
            Box::pin(async move { next })
        }

        fn cancel<'a>(&'a mut self) -> MediaFuture<'a, Result<(), String>> {
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
        url: String,
        access_token: String,
        timeout: Duration,
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

    impl MediaHttpClient for MockHttpClient {
        fn get<'a>(
            &'a self,
            url: &'a str,
            access_token: &'a str,
            timeout: Duration,
        ) -> MediaFuture<'a, Result<MediaHttpResponse, String>> {
            self.requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RequestRecord {
                    url: url.to_owned(),
                    access_token: access_token.to_owned(),
                    timeout,
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

    #[tokio::test]
    async fn downloads_media_with_expected_url_bearer_and_timeout() {
        let (body, _) = MockBody::chunks([vec![1, 2], vec![3, 4]]);
        let client = MockHttpClient::new([Ok(response(200, body)
            .with_header("Content-Length", "4")
            .with_header("Content-Type", "image/png"))]);

        let result = download_media_with_http(
            &client,
            "om_valid_msg",
            "file_valid_key",
            FeishuResourceType::Image,
            "valid_token",
        )
        .await;

        assert_eq!(
            result,
            Some(MediaFile {
                buffer: vec![1, 2, 3, 4],
                mime_type: "image/png".to_owned(),
            })
        );
        assert_eq!(
            client.requests(),
            [RequestRecord {
                url: "https://open.feishu.cn/open-apis/im/v1/messages/om_valid_msg/resources/file_valid_key?type=image".to_owned(),
                access_token: "valid_token".to_owned(),
                timeout: DOWNLOAD_TIMEOUT,
            }]
        );
    }

    #[tokio::test]
    async fn rejects_empty_or_path_unsafe_ids_before_requesting() {
        let client = MockHttpClient::new([]);
        assert!(
            download_media_with_http(
                &client,
                "../../../etc/passwd",
                "file_key",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "../file",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
        for (message_id, file_key, token) in [
            ("", "file_key", "token"),
            ("om_msg", "", "token"),
            ("om_msg", "file_key", ""),
        ] {
            assert!(
                download_media_with_http(
                    &client,
                    message_id,
                    file_key,
                    FeishuResourceType::File,
                    token
                )
                .await
                .is_none()
            );
        }
        assert!(client.requests().is_empty());
    }

    #[tokio::test]
    async fn returns_null_for_http_failure_and_swallows_error_body_read_failure() {
        let (body, _) = MockBody::chunks([b"Not found".to_vec()]);
        let counter = Arc::new(AtomicUsize::new(0));
        let failed_body = MockBody::with_read_error("failed reading error response", counter);
        let client = MockHttpClient::new([Ok(response(404, body)), Ok(response(502, failed_body))]);

        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
        assert_eq!(client.requests().len(), 2);
    }

    #[tokio::test]
    async fn content_length_limit_awaits_body_cancellation() {
        assert_eq!(super::MAX_DOWNLOAD_BYTES, 50 * 1024 * 1024);
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut body, cancel_count) = MockBody::chunks([vec![1]]);
        body.cancel_started = Some(Arc::clone(&started));
        body.cancel_release = Some(Arc::clone(&release));
        let client = Arc::new(MockHttpClient::new([Ok(
            response(200, body).with_header("content-length", " 11bytes")
        )]));
        let download = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                download_media_with_limit(
                    client.as_ref(),
                    "om_msg",
                    "file_key",
                    FeishuResourceType::File,
                    "token",
                    10,
                )
                .await
            }
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
    async fn content_length_cancel_errors_are_caught_as_download_failures() {
        let (body, cancel_count) = MockBody::chunks(Vec::<Vec<u8>>::new());
        let body = body.with_cancel_error("body teardown failed");
        let client =
            MockHttpClient::new([Ok(response(200, body).with_header("content-length", "11MB"))]);

        assert!(
            download_media_with_limit(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token",
                10
            )
            .await
            .is_none()
        );
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn streamed_limit_awaits_cancel_and_enforces_limit_without_content_length() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (mut body, cancel_count) = MockBody::chunks([vec![1; 7], vec![2; 4], vec![3]]);
        body.cancel_started = Some(Arc::clone(&started));
        body.cancel_release = Some(Arc::clone(&release));
        // The body should stop at the second chunk that crosses the byte limit.
        let client = Arc::new(MockHttpClient::new([Ok(response(200, body))]));
        let download = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                download_media_with_limit(
                    client.as_ref(),
                    "om_msg",
                    "file_key",
                    FeishuResourceType::Image,
                    "token",
                    10,
                )
                .await
            }
        });

        timeout(Duration::from_secs(2), started.notified())
            .await
            .unwrap();
        assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
        release.notify_one();
        assert!(download.await.unwrap().is_none());
    }

    #[tokio::test]
    async fn stream_cancel_errors_and_read_errors_return_null() {
        let (body, _) = MockBody::chunks([vec![0; 11]]);
        let body = body.with_cancel_error("reader teardown failed");
        let client = MockHttpClient::new([Ok(response(200, body))]);
        assert!(
            download_media_with_limit(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token",
                10
            )
            .await
            .is_none()
        );

        let counter = Arc::new(AtomicUsize::new(0));
        let body = MockBody::with_read_error("stream read failed", counter);
        let client = MockHttpClient::new([Ok(response(200, body))]);
        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn absent_body_returns_null_and_missing_or_empty_mime_type_uses_fallback() {
        let client = MockHttpClient::new([Ok(MediaHttpResponse::new(200))]);
        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::Image,
                "token"
            )
            .await
            .is_none()
        );

        let (body, _) = MockBody::chunks([vec![1, 2, 3]]);
        let client = MockHttpClient::new([Ok(response(200, body).with_header("content-type", ""))]);
        let result = download_media_with_http(
            &client,
            "om_msg",
            "file_key",
            FeishuResourceType::File,
            "token",
        )
        .await
        .unwrap();
        assert_eq!(result.mime_type, "application/octet-stream");
        assert_eq!(result.buffer, [1, 2, 3]);
    }

    #[tokio::test]
    async fn request_transport_failure_returns_null() {
        let client = MockHttpClient::new([Err("Network error".to_owned())]);
        assert!(
            download_media_with_http(
                &client,
                "om_msg",
                "file_key",
                FeishuResourceType::File,
                "token"
            )
            .await
            .is_none()
        );
    }
}
