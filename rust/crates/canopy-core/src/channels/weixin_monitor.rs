//! Long-polling monitor for the Weixin iLink Bot API.
//!
//! Port of `packages/channels/weixin/src/monitor.ts`. The API, cursor store,
//! clock, sleeper, and log output are injectable so the polling state machine
//! can be exercised without network access or real delays.

use crate::channels::weixin_accounts;
use crate::channels::weixin_api::{self, ApiFuture, CancellationToken};
use crate::channels::weixin_types::{GetUpdatesResp, MessageItemType, MessageType, WeixinMessage};
use reqwest::Client;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static CONTEXT_TOKENS: LazyLock<Mutex<HashMap<String, String>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Look up the most recently observed context token for a Weixin user.
pub fn get_context_token(user_id: &str) -> Option<String> {
    CONTEXT_TOKENS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(user_id)
        .cloned()
}

/// CDN fields used to fetch a deferred image.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CdnRef {
    pub encrypt_query_param: String,
    pub aes_key: String,
}

/// CDN fields and filename used to fetch a deferred file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileCdnRef {
    pub encrypt_query_param: String,
    pub aes_key: String,
    pub file_name: String,
}

/// Text and deferred media extracted from one user message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedMessage {
    pub from_user_id: String,
    pub message_id: String,
    pub text: String,
    pub image: Option<CdnRef>,
    pub file: Option<FileCdnRef>,
    pub ref_text: Option<String>,
}

/// Extract a USER message, updating the process-wide context-token cache.
/// Returns `None` for non-user messages, missing users, or messages without
/// text or valid deferred media.
pub fn extract_user_message(msg: &WeixinMessage, now_ms: u128) -> Option<ParsedMessage> {
    if msg.message_type != Some(MessageType::USER) {
        return None;
    }

    let from_user_id = msg
        .from_user_id
        .as_deref()
        .filter(|user| !user.is_empty())?;
    if let Some(context_token) = msg
        .context_token
        .as_deref()
        .filter(|context_token| !context_token.is_empty())
    {
        CONTEXT_TOKENS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(from_user_id.to_owned(), context_token.to_owned());
    }

    let mut text_content = String::new();
    let mut image = None;
    let mut file = None;
    let mut ref_text = None;

    if let Some(items) = msg.item_list.as_deref() {
        for item in items {
            if item.r#type == Some(MessageItemType::TEXT) {
                if let Some(text) = item
                    .text_item
                    .as_ref()
                    .and_then(|text| text.text.as_deref())
                {
                    if !text.is_empty() {
                        if !text_content.is_empty() {
                            text_content.push('\n');
                        }
                        text_content.push_str(text);
                    }
                }
            }

            if let Some(reference) = item.ref_msg.as_ref() {
                let referenced_text = reference
                    .message_item
                    .as_deref()
                    .and_then(|referenced| {
                        referenced
                            .text_item
                            .as_ref()
                            .and_then(|text| text.text.as_deref())
                    })
                    .filter(|text| !text.is_empty())
                    .or_else(|| reference.title.as_deref().filter(|title| !title.is_empty()));
                if let Some(text) = referenced_text {
                    ref_text = Some(text.to_owned());
                }
            }

            if item.r#type == Some(MessageItemType::IMAGE) {
                if let Some(media) = item
                    .image_item
                    .as_ref()
                    .and_then(|image| image.media.as_ref())
                {
                    if let (Some(encrypt_query_param), Some(aes_key)) = (
                        media
                            .encrypt_query_param
                            .as_deref()
                            .filter(|value| !value.is_empty()),
                        media.aes_key.as_deref().filter(|value| !value.is_empty()),
                    ) {
                        image = Some(CdnRef {
                            encrypt_query_param: encrypt_query_param.to_owned(),
                            aes_key: aes_key.to_owned(),
                        });
                    }
                }
            } else if item.r#type == Some(MessageItemType::FILE) {
                if let Some(file_item) = item.file_item.as_ref() {
                    if let Some(media) = file_item.media.as_ref() {
                        if let (Some(encrypt_query_param), Some(aes_key)) = (
                            media
                                .encrypt_query_param
                                .as_deref()
                                .filter(|value| !value.is_empty()),
                            media.aes_key.as_deref().filter(|value| !value.is_empty()),
                        ) {
                            let file_name = file_item
                                .file_name
                                .as_deref()
                                .filter(|name| !name.is_empty())
                                .map(str::to_owned)
                                .unwrap_or_else(|| format!("file_{now_ms}"));
                            file = Some(FileCdnRef {
                                encrypt_query_param: encrypt_query_param.to_owned(),
                                aes_key: aes_key.to_owned(),
                                file_name,
                            });
                        }
                    }
                }
            }
        }
    }

    if text_content.is_empty() && image.is_none() && file.is_none() {
        return None;
    }

    let message_id = msg
        .message_id
        .filter(|message_id| *message_id != 0)
        .map(|message_id| message_id.to_string())
        .unwrap_or_default();
    let text = if !text_content.is_empty() {
        text_content
    } else if let Some(file) = file.as_ref() {
        format!("(file: {})", file.file_name)
    } else {
        "(image)".to_owned()
    };

    Some(ParsedMessage {
        from_user_id: from_user_id.to_owned(),
        message_id,
        text,
        image,
        file,
        ref_text,
    })
}

/// Future type used by asynchronous monitor seams.
pub type MonitorFuture<'a, T> = ApiFuture<'a, T>;

/// API boundary used by the polling loop.
pub trait MonitorApi: Send + Sync {
    fn get_updates<'a>(
        &'a self,
        base_url: &'a str,
        token: &'a str,
        cursor: &'a str,
        timeout: Duration,
        cancellation: &'a CancellationToken,
    ) -> MonitorFuture<'a, Result<GetUpdatesResp, String>>;
}

/// Production API adapter backed by the shared Weixin API client.
pub struct ReqwestMonitorApi<'a> {
    client: &'a Client,
}

impl<'a> ReqwestMonitorApi<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }
}

impl MonitorApi for ReqwestMonitorApi<'_> {
    fn get_updates<'a>(
        &'a self,
        base_url: &'a str,
        token: &'a str,
        cursor: &'a str,
        timeout: Duration,
        cancellation: &'a CancellationToken,
    ) -> MonitorFuture<'a, Result<GetUpdatesResp, String>> {
        Box::pin(async move {
            let value = weixin_api::get_updates(
                self.client,
                base_url,
                token,
                cursor,
                Some(timeout),
                Some(cancellation),
            )
            .await
            .map_err(|error| error.to_string())?;
            serde_json::from_value(value)
                .map_err(|error| format!("getUpdates response parse failed: {error}"))
        })
    }
}

/// Persistent cursor boundary. Implementations may be memory-backed in tests.
pub trait CursorStore: Send + Sync {
    fn load_cursor(&self) -> io::Result<String>;
    fn save_cursor(&self, cursor: &str) -> io::Result<()>;
}

/// The source cursor file is `<Weixin state directory>/cursor.txt`.
#[derive(Clone, Debug)]
pub struct FileCursorStore {
    path: PathBuf,
}

impl FileCursorStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn for_state_dir() -> io::Result<Self> {
        Ok(Self::new(
            weixin_accounts::get_state_dir()?.join("cursor.txt"),
        ))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CursorStore for FileCursorStore {
    fn load_cursor(&self) -> io::Result<String> {
        if !self.path.exists() {
            return Ok(String::new());
        }
        Ok(std::fs::read_to_string(&self.path)?.trim().to_owned())
    }

    fn save_cursor(&self, cursor: &str) -> io::Result<()> {
        std::fs::write(&self.path, cursor)
    }
}

/// Time source for `file_<Date.now()>` fallback names.
pub trait MonitorClock: Send + Sync {
    fn now_millis(&self) -> u128;
}

/// Asynchronous delay boundary for pauses and error backoff.
pub trait MonitorSleeper: Send + Sync {
    fn sleep<'a>(&'a self, duration: Duration) -> MonitorFuture<'a, ()>;
}

/// Log-output boundary used by the poller.
pub trait MonitorOutput: Send + Sync {
    fn write_line(&self, line: &str);
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemMonitorClock;

impl MonitorClock for SystemMonitorClock {
    fn now_millis(&self) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioMonitorSleeper;

impl MonitorSleeper for TokioMonitorSleeper {
    fn sleep<'a>(&'a self, duration: Duration) -> MonitorFuture<'a, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StderrMonitorOutput;

impl MonitorOutput for StderrMonitorOutput {
    fn write_line(&self, line: &str) {
        eprintln!("{line}");
    }
}

/// Start polling using the shared API module, state-directory cursor file, and
/// production clock, sleeper, and stderr output.
pub async fn start_poll_loop<F, Fut>(
    client: &Client,
    base_url: &str,
    token: &str,
    cancellation: &CancellationToken,
    on_message: F,
) -> io::Result<()>
where
    F: FnMut(ParsedMessage) -> Fut + Send,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let api = ReqwestMonitorApi::new(client);
    let cursor_store = FileCursorStore::for_state_dir()?;
    let clock = SystemMonitorClock;
    let sleeper = TokioMonitorSleeper;
    let output = StderrMonitorOutput;
    start_poll_loop_with(
        &api,
        &cursor_store,
        &clock,
        &sleeper,
        &output,
        base_url,
        token,
        cancellation,
        on_message,
    )
    .await
}

/// Start polling with injected API, cursor, clock, sleep, and output
/// implementations. The callback's `Err` follows the same retry path as a
/// rejected TypeScript callback promise.
#[allow(clippy::too_many_arguments)]
pub async fn start_poll_loop_with<F, Fut>(
    api: &dyn MonitorApi,
    cursor_store: &dyn CursorStore,
    clock: &dyn MonitorClock,
    sleeper: &dyn MonitorSleeper,
    output: &dyn MonitorOutput,
    base_url: &str,
    token: &str,
    cancellation: &CancellationToken,
    mut on_message: F,
) -> io::Result<()>
where
    F: FnMut(ParsedMessage) -> Fut + Send,
    Fut: Future<Output = Result<(), String>> + Send,
{
    let mut cursor = cursor_store.load_cursor()?;
    let mut consecutive_errors = 0_u32;
    let mut poll_timeout = Duration::from_millis(40_000);

    output.write_line("[weixin] Starting message poll loop...");

    while !cancellation.is_cancelled() {
        let response = api
            .get_updates(base_url, token, &cursor, poll_timeout, cancellation)
            .await;

        let poll_result: Result<(), String> = match response {
            Err(error) => Err(error),
            Ok(response) if response.errcode == Some(-14) => {
                output.write_line("[weixin] Session expired (errcode -14). Pausing 30s...");
                sleeper.sleep(Duration::from_secs(30)).await;
                continue;
            }
            Ok(response) => {
                if response.ret.is_some_and(|ret| ret != 0) {
                    Err(format!(
                        "getUpdates error: ret={} errcode={} {}",
                        js_number(response.ret),
                        js_number(response.errcode),
                        response.errmsg.as_deref().unwrap_or("undefined")
                    ))
                } else {
                    consecutive_errors = 0;

                    if let Some(timeout_ms) = response
                        .longpolling_timeout_ms
                        .filter(|timeout_ms| *timeout_ms > 0)
                    {
                        poll_timeout = Duration::from_millis(timeout_ms as u64)
                            .saturating_add(Duration::from_millis(5_000));
                    }

                    let mut callback_error = None;
                    if let Some(messages) = response.msgs.as_deref() {
                        for message in messages {
                            if let Some(parsed) = extract_user_message(message, clock.now_millis())
                            {
                                if let Err(error) = on_message(parsed).await {
                                    callback_error = Some(error);
                                    break;
                                }
                            }
                        }
                    }

                    if let Some(error) = callback_error {
                        Err(error)
                    } else if let Some(next_cursor) = response
                        .get_updates_buf
                        .as_deref()
                        .filter(|cursor| !cursor.is_empty())
                    {
                        // The TypeScript implementation advances its in-memory
                        // cursor before writing the file; keep the same ordering.
                        cursor = next_cursor.to_owned();
                        cursor_store
                            .save_cursor(&cursor)
                            .map_err(|error| error.to_string())
                    } else {
                        Ok(())
                    }
                }
            }
        };

        if let Err(error) = poll_result {
            if cancellation.is_cancelled() {
                break;
            }

            consecutive_errors = consecutive_errors.saturating_add(1);
            output.write_line(&format!(
                "[weixin] Poll error ({consecutive_errors}): {error}"
            ));

            if consecutive_errors >= 3 {
                output.write_line("[weixin] Too many consecutive errors, backing off 30s...");
                sleeper.sleep(Duration::from_secs(30)).await;
                consecutive_errors = 0;
            } else {
                sleeper.sleep(Duration::from_secs(2)).await;
            }
        }
    }

    output.write_line("[weixin] Poll loop stopped.");
    Ok(())
}

fn js_number(value: Option<i64>) -> String {
    value.map_or_else(|| "undefined".to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        CdnRef, CursorStore, FileCdnRef, MonitorApi, MonitorClock, MonitorFuture, MonitorOutput,
        MonitorSleeper, ParsedMessage, extract_user_message, get_context_token,
        start_poll_loop_with,
    };
    use crate::channels::weixin_api::CancellationToken;
    use crate::channels::weixin_types::{
        CDNMedia, FileItem, GetUpdatesResp, ImageItem, MessageItem, MessageItemType, MessageType,
        RefMessage, TextItem, WeixinMessage,
    };
    use std::collections::VecDeque;
    use std::io;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Default)]
    struct MemoryCursor {
        initial: Mutex<String>,
        saved: Arc<Mutex<Vec<String>>>,
    }

    impl MemoryCursor {
        fn with_initial(initial: &str) -> Self {
            Self {
                initial: Mutex::new(initial.to_owned()),
                saved: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn saved(&self) -> Vec<String> {
            self.saved.lock().unwrap().clone()
        }

        fn saved_handle(&self) -> Arc<Mutex<Vec<String>>> {
            self.saved.clone()
        }
    }

    impl CursorStore for MemoryCursor {
        fn load_cursor(&self) -> io::Result<String> {
            Ok(self.initial.lock().unwrap().clone())
        }

        fn save_cursor(&self, cursor: &str) -> io::Result<()> {
            self.saved.lock().unwrap().push(cursor.to_owned());
            Ok(())
        }
    }

    struct FixedClock(u128);

    impl MonitorClock for FixedClock {
        fn now_millis(&self) -> u128 {
            self.0
        }
    }

    #[derive(Default)]
    struct RecordingOutput(Mutex<Vec<String>>);

    impl MonitorOutput for RecordingOutput {
        fn write_line(&self, line: &str) {
            self.0.lock().unwrap().push(line.to_owned());
        }
    }

    struct RecordingSleeper {
        delays: Arc<Mutex<Vec<Duration>>>,
        cancel_at: Option<usize>,
        cancellation: CancellationToken,
    }

    impl RecordingSleeper {
        fn new(cancellation: &CancellationToken, cancel_at: Option<usize>) -> Self {
            Self {
                delays: Arc::new(Mutex::new(Vec::new())),
                cancel_at,
                cancellation: cancellation.clone(),
            }
        }

        fn delays(&self) -> Vec<Duration> {
            self.delays.lock().unwrap().clone()
        }
    }

    impl MonitorSleeper for RecordingSleeper {
        fn sleep<'a>(&'a self, duration: Duration) -> MonitorFuture<'a, ()> {
            let mut delays = self.delays.lock().unwrap();
            delays.push(duration);
            if self.cancel_at == Some(delays.len()) {
                self.cancellation.cancel();
            }
            drop(delays);
            Box::pin(async {})
        }
    }

    struct ApiStep {
        result: Result<GetUpdatesResp, String>,
    }

    struct FakeApi {
        steps: Mutex<VecDeque<ApiStep>>,
        calls: Mutex<Vec<(String, Duration)>>,
        cancel_at: Option<usize>,
    }

    impl FakeApi {
        fn new(steps: Vec<Result<GetUpdatesResp, String>>, cancel_at: Option<usize>) -> Self {
            Self {
                steps: Mutex::new(steps.into_iter().map(|result| ApiStep { result }).collect()),
                calls: Mutex::new(Vec::new()),
                cancel_at,
            }
        }

        fn calls(&self) -> Vec<(String, Duration)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl MonitorApi for FakeApi {
        fn get_updates<'a>(
            &'a self,
            _base_url: &'a str,
            _token: &'a str,
            cursor: &'a str,
            timeout: Duration,
            cancellation: &'a CancellationToken,
        ) -> MonitorFuture<'a, Result<GetUpdatesResp, String>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .unwrap()
                    .push((cursor.to_owned(), timeout));
                let call_number = self.calls.lock().unwrap().len();
                let result = self
                    .steps
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected API call")
                    .result;
                if self.cancel_at == Some(call_number) {
                    cancellation.cancel();
                }
                result
            })
        }
    }

    fn text_item(text: &str) -> MessageItem {
        MessageItem {
            r#type: Some(MessageItemType::TEXT),
            text_item: Some(TextItem {
                text: Some(text.to_owned()),
            }),
            ..MessageItem::default()
        }
    }

    fn media(encrypt_query_param: &str, aes_key: &str) -> CDNMedia {
        CDNMedia {
            encrypt_query_param: Some(encrypt_query_param.to_owned()),
            aes_key: Some(aes_key.to_owned()),
            ..CDNMedia::default()
        }
    }

    fn user_message(items: Vec<MessageItem>) -> WeixinMessage {
        WeixinMessage {
            message_id: Some(42),
            from_user_id: Some("user-monitor-test".to_owned()),
            message_type: Some(MessageType::USER),
            item_list: Some(items),
            ..WeixinMessage::default()
        }
    }

    async fn run<F, Fut>(
        api: &FakeApi,
        cursor: &MemoryCursor,
        clock: &FixedClock,
        sleeper: &RecordingSleeper,
        output: &RecordingOutput,
        cancellation: &CancellationToken,
        callback: F,
    ) -> io::Result<()>
    where
        F: FnMut(ParsedMessage) -> Fut + Send,
        Fut: std::future::Future<Output = Result<(), String>> + Send,
    {
        start_poll_loop_with(
            api,
            cursor,
            clock,
            sleeper,
            output,
            "https://example.invalid",
            "test-token",
            cancellation,
            callback,
        )
        .await
    }

    #[test]
    fn extracts_text_media_references_and_context_token() {
        let message = WeixinMessage {
            message_id: Some(42),
            from_user_id: Some("weixin-user-extract-test".to_owned()),
            message_type: Some(MessageType::USER),
            context_token: Some("context-42".to_owned()),
            item_list: Some(vec![
                text_item("hello"),
                text_item("world"),
                MessageItem {
                    r#type: Some(MessageItemType::IMAGE),
                    image_item: Some(ImageItem {
                        media: Some(media("image-query", "image-key")),
                        ..ImageItem::default()
                    }),
                    ..MessageItem::default()
                },
                MessageItem {
                    r#type: Some(MessageItemType::FILE),
                    file_item: Some(FileItem {
                        media: Some(media("file-query", "file-key")),
                        file_name: Some(String::new()),
                        ..FileItem::default()
                    }),
                    ..MessageItem::default()
                },
                MessageItem {
                    ref_msg: Some(RefMessage {
                        message_item: Some(Box::new(text_item("quoted text"))),
                        title: Some("quoted title".to_owned()),
                    }),
                    ..MessageItem::default()
                },
                MessageItem {
                    ref_msg: Some(RefMessage {
                        message_item: None,
                        title: Some("last reference".to_owned()),
                    }),
                    ..MessageItem::default()
                },
            ]),
            ..WeixinMessage::default()
        };

        let parsed = extract_user_message(&message, 1_234).unwrap();
        assert_eq!(parsed.from_user_id, "weixin-user-extract-test");
        assert_eq!(parsed.message_id, "42");
        assert_eq!(parsed.text, "hello\nworld");
        assert_eq!(
            parsed.image,
            Some(CdnRef {
                encrypt_query_param: "image-query".to_owned(),
                aes_key: "image-key".to_owned(),
            })
        );
        assert_eq!(
            parsed.file,
            Some(FileCdnRef {
                encrypt_query_param: "file-query".to_owned(),
                aes_key: "file-key".to_owned(),
                file_name: "file_1234".to_owned(),
            })
        );
        assert_eq!(parsed.ref_text.as_deref(), Some("last reference"));
        assert_eq!(
            get_context_token("weixin-user-extract-test").as_deref(),
            Some("context-42")
        );
    }

    #[test]
    fn media_only_text_fallbacks_and_user_message_filter_match_source() {
        let image_only = user_message(vec![MessageItem {
            r#type: Some(MessageItemType::IMAGE),
            image_item: Some(ImageItem {
                media: Some(media("query", "key")),
                ..ImageItem::default()
            }),
            ..MessageItem::default()
        }]);
        assert_eq!(
            extract_user_message(&image_only, 7).unwrap().text,
            "(image)"
        );

        let file_and_image = user_message(vec![
            MessageItem {
                r#type: Some(MessageItemType::IMAGE),
                image_item: Some(ImageItem {
                    media: Some(media("image", "key")),
                    ..ImageItem::default()
                }),
                ..MessageItem::default()
            },
            MessageItem {
                r#type: Some(MessageItemType::FILE),
                file_item: Some(FileItem {
                    media: Some(media("file", "key")),
                    file_name: Some("readme.pdf".to_owned()),
                    ..FileItem::default()
                }),
                ..MessageItem::default()
            },
        ]);
        let parsed = extract_user_message(&file_and_image, 7).unwrap();
        assert_eq!(parsed.text, "(file: readme.pdf)");
        assert!(parsed.image.is_some());
        assert!(parsed.file.is_some());

        let bot_message = WeixinMessage {
            message_type: Some(MessageType::BOT),
            item_list: Some(vec![text_item("ignore me")]),
            ..WeixinMessage::default()
        };
        assert!(extract_user_message(&bot_message, 7).is_none());
        assert!(extract_user_message(&user_message(Vec::new()), 7).is_none());
    }

    #[test]
    fn file_cursor_store_trims_on_load_and_writes_raw_cursor() {
        let root = std::env::temp_dir().join(format!(
            "weixin-monitor-cursor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("cursor.txt");
        std::fs::write(&path, "  restored-cursor\n").unwrap();
        let store = super::FileCursorStore::new(&path);

        assert_eq!(store.load_cursor().unwrap(), "restored-cursor");
        store.save_cursor("next-cursor\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "next-cursor\n");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn restores_cursor_and_persists_only_after_callback_finishes() {
        let message = user_message(vec![text_item("hello")]);
        let api = FakeApi::new(
            vec![Ok(GetUpdatesResp {
                msgs: Some(vec![message]),
                get_updates_buf: Some("next-cursor".to_owned()),
                ..GetUpdatesResp::default()
            })],
            Some(1),
        );
        let cursor = MemoryCursor::with_initial("restored-cursor");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, None);
        let output = RecordingOutput::default();
        let cursor_during_callback = Arc::new(Mutex::new(None));
        let cursor_during_callback_copy = cursor_during_callback.clone();
        let saved_handle = cursor.saved_handle();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            move |_message| {
                let observed = cursor_during_callback_copy.clone();
                let saved_handle = saved_handle.clone();
                async move {
                    *observed.lock().unwrap() = Some(saved_handle.lock().unwrap().clone());
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(api.calls()[0].0, "restored-cursor");
        assert_eq!(*cursor_during_callback.lock().unwrap(), Some(Vec::new()));
        assert_eq!(cursor.saved(), vec!["next-cursor"]);
        assert_eq!(api.calls().len(), 1);
    }

    #[tokio::test]
    async fn callback_failure_keeps_old_cursor_and_uses_short_backoff() {
        let api = FakeApi::new(
            vec![Ok(GetUpdatesResp {
                msgs: Some(vec![user_message(vec![text_item("retry me")])]),
                get_updates_buf: Some("must-not-save".to_owned()),
                ..GetUpdatesResp::default()
            })],
            None,
        );
        let cursor = MemoryCursor::with_initial("old");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, Some(1));
        let output = RecordingOutput::default();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            |_message| async { Err("callback rejected".to_owned()) },
        )
        .await
        .unwrap();

        assert!(cursor.saved().is_empty());
        assert_eq!(api.calls()[0].0, "old");
        assert_eq!(sleeper.delays(), vec![Duration::from_secs(2)]);
    }

    #[tokio::test]
    async fn adopts_server_timeout_plus_five_seconds_and_stops_on_abort() {
        let api = FakeApi::new(
            vec![
                Ok(GetUpdatesResp {
                    longpolling_timeout_ms: Some(1_234),
                    get_updates_buf: Some("cursor-one".to_owned()),
                    ..GetUpdatesResp::default()
                }),
                Ok(GetUpdatesResp {
                    get_updates_buf: Some("cursor-two".to_owned()),
                    ..GetUpdatesResp::default()
                }),
            ],
            Some(2),
        );
        let cursor = MemoryCursor::with_initial("start");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, None);
        let output = RecordingOutput::default();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            |_message| async { Ok(()) },
        )
        .await
        .unwrap();

        let calls = api.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "start");
        assert_eq!(calls[0].1, Duration::from_secs(40));
        assert_eq!(calls[1].0, "cursor-one");
        assert_eq!(calls[1].1, Duration::from_millis(6_234));
        assert_eq!(cursor.saved(), vec!["cursor-one", "cursor-two"]);
        assert!(sleeper.delays().is_empty());
    }

    #[tokio::test]
    async fn session_expiry_pauses_thirty_seconds_without_saving_cursor() {
        let api = FakeApi::new(
            vec![Ok(GetUpdatesResp {
                errcode: Some(-14),
                get_updates_buf: Some("ignored-cursor".to_owned()),
                ..GetUpdatesResp::default()
            })],
            None,
        );
        let cursor = MemoryCursor::with_initial("before-expiry");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, Some(1));
        let output = RecordingOutput::default();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            |_message| async { Ok(()) },
        )
        .await
        .unwrap();

        assert_eq!(api.calls().len(), 1);
        assert!(cursor.saved().is_empty());
        assert_eq!(sleeper.delays(), vec![Duration::from_secs(30)]);
    }

    #[tokio::test]
    async fn consecutive_errors_back_off_two_two_then_thirty_seconds() {
        let api = FakeApi::new(
            vec![
                Err("transport 1".to_owned()),
                Err("transport 2".to_owned()),
                Err("transport 3".to_owned()),
                Ok(GetUpdatesResp {
                    get_updates_buf: Some("recovered".to_owned()),
                    ..GetUpdatesResp::default()
                }),
            ],
            Some(4),
        );
        let cursor = MemoryCursor::with_initial("retry-cursor");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, None);
        let output = RecordingOutput::default();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            |_message| async { Ok(()) },
        )
        .await
        .unwrap();

        assert_eq!(
            sleeper.delays(),
            vec![
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(30),
            ]
        );
        assert_eq!(
            api.calls()
                .iter()
                .map(|call| call.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "retry-cursor",
                "retry-cursor",
                "retry-cursor",
                "retry-cursor"
            ]
        );
        assert_eq!(cursor.saved(), vec!["recovered"]);
    }

    #[tokio::test]
    async fn cancellation_during_api_call_stops_without_retrying() {
        let api = FakeApi::new(vec![Err("cancelled request".to_owned())], Some(1));
        let cursor = MemoryCursor::with_initial("cursor");
        let cancellation = CancellationToken::new();
        let clock = FixedClock(10);
        let sleeper = RecordingSleeper::new(&cancellation, None);
        let output = RecordingOutput::default();

        run(
            &api,
            &cursor,
            &clock,
            &sleeper,
            &output,
            &cancellation,
            |_message| async { Ok(()) },
        )
        .await
        .unwrap();

        assert_eq!(api.calls().len(), 1);
        assert!(sleeper.delays().is_empty());
        assert!(cursor.saved().is_empty());
    }
}
