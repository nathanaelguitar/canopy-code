//! Weixin adapter outbound orchestration.
//!
//! Port of `WeixinChannel.sendMessage` from
//! `packages/channels/weixin/src/WeixinAdapter.ts`. It parses image markers,
//! sends cleaned text before images, and sends a fallback text after an image
//! failure. Actual API and logging behavior remain injectable.

use crate::channels::weixin_api::{ApiFuture, WeixinApiError};
use crate::channels::weixin_send_image::{SendImageError, SendImageParams, send_image};
use crate::channels::weixin_send_utils::{SendTextParams, send_text};
use regex::Regex;
use reqwest::Client;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::LazyLock;

const IMAGE_FAILURE_FALLBACK: &str = "图片发送失败，请稍后重试";
const JS_WHITESPACE: &str =
    r"\t-\r \x{00a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}";

static CODE_BLOCK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"```[\s\S]*?```").expect("static fenced-code regex is valid"));
static INLINE_CODE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"`[^`]*`").expect("static inline-code regex is valid"));
static IMAGE_MARKER_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"(?i)\[IMAGE:[{JS_WHITESPACE}]*([^\]]+)\]"))
        .expect("static image-marker regex is valid")
});
static BLANK_LINES_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\n{3,}").expect("static blank-lines regex is valid"));

/// Parsed image paths and text with only those image markers removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedOutboundContent {
    pub cleaned_text: String,
    pub image_paths: Vec<String>,
}

/// Parse image markers outside fenced and inline code, then remove each parsed
/// path's marker from the original text. Unparsed markers inside code remain
/// visible in `cleaned_text`.
pub fn parse_outbound_content(text: &str) -> ParsedOutboundContent {
    let code_free = CODE_BLOCK_RE.replace_all(text, "");
    let code_free = INLINE_CODE_RE.replace_all(&code_free, "");
    let image_paths = IMAGE_MARKER_RE
        .captures_iter(&code_free)
        .filter_map(|captures| captures.get(1))
        .map(|path| trim_ecmascript_whitespace(path.as_str()).to_owned())
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();

    let mut cleaned_text = text.to_owned();
    for path in &image_paths {
        // Match the source's escaped, case-insensitive dynamic replacement.
        // In particular, trailing whitespace before `]` does not match because
        // the extracted path is trimmed before this regex is built.
        let marker = Regex::new(&format!(
            r"(?i)\[IMAGE:[{JS_WHITESPACE}]*{}\]",
            regex::escape(path)
        ))
        .expect("escaped image path produces a valid regex");
        cleaned_text = marker.replace_all(&cleaned_text, "").into_owned();
    }
    cleaned_text = BLANK_LINES_RE
        .replace_all(&cleaned_text, "\n\n")
        .into_owned();
    cleaned_text = trim_ecmascript_whitespace(&cleaned_text).to_owned();

    ParsedOutboundContent {
        cleaned_text,
        image_paths,
    }
}

fn trim_ecmascript_whitespace(text: &str) -> &str {
    const WHITESPACE: &[char] = &[
        '\u{0009}', '\u{000a}', '\u{000b}', '\u{000c}', '\u{000d}', '\u{0020}', '\u{00a0}',
        '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}',
        '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}', '\u{2029}',
        '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
    ];
    text.trim_matches(WHITESPACE)
}

/// Fields needed to send one adapter message.
#[derive(Clone, Copy, Debug)]
pub struct OutboundMessageParams<'a> {
    pub channel_name: &'a str,
    pub chat_id: &'a str,
    pub text: &'a str,
    pub base_url: &'a str,
    pub token: &'a str,
    pub context_token: &'a str,
    pub workspace_dirs: &'a [PathBuf],
}

/// Unified failure details needed for the adapter's source-compatible log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundSendError {
    pub message: String,
    pub status: u16,
    pub ret: Option<i64>,
    pub errcode: Option<i64>,
}

impl OutboundSendError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: 0,
            ret: None,
            errcode: None,
        }
    }
}

impl fmt::Display for OutboundSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for OutboundSendError {}

impl From<WeixinApiError> for OutboundSendError {
    fn from(error: WeixinApiError) -> Self {
        Self {
            message: error.message,
            status: error.status,
            ret: error.ret,
            errcode: error.errcode,
        }
    }
}

impl From<SendImageError> for OutboundSendError {
    fn from(error: SendImageError) -> Self {
        match error {
            SendImageError::Api(error) => error.into(),
            error => Self::new(error.to_string()),
        }
    }
}

/// Future returned by injected text and image senders.
pub type OutboundFuture<'a> = ApiFuture<'a, Result<(), OutboundSendError>>;

/// Network boundary for outbound messages.
pub trait OutboundSender: Send + Sync {
    fn send_text<'a>(&'a self, params: SendTextParams<'a>) -> OutboundFuture<'a>;
    fn send_image<'a>(&'a self, params: SendImageParams<'a>) -> OutboundFuture<'a>;
}

/// Log boundary for image and fallback failures.
pub trait OutboundOutput: Send + Sync {
    fn write_line(&self, line: &str);
}

/// Production network adapter backed by the shared Weixin API helpers.
pub struct ReqwestOutboundSender<'a> {
    client: &'a Client,
}

impl<'a> ReqwestOutboundSender<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }
}

impl OutboundSender for ReqwestOutboundSender<'_> {
    fn send_text<'a>(&'a self, params: SendTextParams<'a>) -> OutboundFuture<'a> {
        Box::pin(async move { send_text(self.client, params).await.map_err(Into::into) })
    }

    fn send_image<'a>(&'a self, params: SendImageParams<'a>) -> OutboundFuture<'a> {
        Box::pin(async move { send_image(self.client, params).await.map_err(Into::into) })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct StderrOutboundOutput;

impl OutboundOutput for StderrOutboundOutput {
    fn write_line(&self, line: &str) {
        eprintln!("{line}");
    }
}

/// Coordinate one text-first outbound message and sequential image sends.
/// Text-send errors propagate. Image-send errors are logged and trigger one
/// Chinese fallback text attempt; a fallback error is logged and processing
/// continues with the next parsed image.
pub async fn send_outbound_message(
    sender: &dyn OutboundSender,
    output: &dyn OutboundOutput,
    params: OutboundMessageParams<'_>,
) -> Result<(), OutboundSendError> {
    let parsed = parse_outbound_content(params.text);

    if !parsed.cleaned_text.is_empty() {
        sender
            .send_text(SendTextParams {
                to: params.chat_id,
                text: &parsed.cleaned_text,
                base_url: params.base_url,
                token: params.token,
                context_token: params.context_token,
            })
            .await?;
    }

    for image in &parsed.image_paths {
        let image_path = PathBuf::from(image);
        if let Err(error) = sender
            .send_image(SendImageParams {
                to: params.chat_id,
                image_path: &image_path,
                base_url: params.base_url,
                token: params.token,
                context_token: params.context_token,
                workspace_dirs: params.workspace_dirs,
            })
            .await
        {
            output.write_line(&format!(
                "[Weixin:{}] Failed to send image (status={} ret={} errcode={}): {}",
                params.channel_name,
                error.status,
                optional_number(error.ret),
                optional_number(error.errcode),
                error.message
            ));
            if let Err(fallback_error) = sender
                .send_text(SendTextParams {
                    to: params.chat_id,
                    text: IMAGE_FAILURE_FALLBACK,
                    base_url: params.base_url,
                    token: params.token,
                    context_token: params.context_token,
                })
                .await
            {
                output.write_line(&format!(
                    "[Weixin:{}] Fallback text also failed: {}",
                    params.channel_name, fallback_error.message
                ));
            }
        }
    }
    Ok(())
}

fn optional_number(value: Option<i64>) -> String {
    value.map_or_else(|| "undefined".to_owned(), |value| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        OutboundFuture, OutboundMessageParams, OutboundOutput, OutboundSendError, OutboundSender,
        ParsedOutboundContent, parse_outbound_content, send_outbound_message,
    };
    use crate::channels::weixin_send_image::SendImageParams;
    use crate::channels::weixin_send_utils::SendTextParams;
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::Mutex;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Text(String),
        Image(String),
    }

    #[derive(Default)]
    struct FakeSender {
        events: Mutex<Vec<Event>>,
        text_results: Mutex<VecDeque<Result<(), OutboundSendError>>>,
        image_results: Mutex<VecDeque<Result<(), OutboundSendError>>>,
    }

    impl FakeSender {
        fn with_results(
            text_results: impl IntoIterator<Item = Result<(), OutboundSendError>>,
            image_results: impl IntoIterator<Item = Result<(), OutboundSendError>>,
        ) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                text_results: Mutex::new(text_results.into_iter().collect()),
                image_results: Mutex::new(image_results.into_iter().collect()),
            }
        }
    }

    impl OutboundSender for FakeSender {
        fn send_text<'a>(&'a self, params: SendTextParams<'a>) -> OutboundFuture<'a> {
            self.events
                .lock()
                .unwrap()
                .push(Event::Text(params.text.to_owned()));
            let result = self
                .text_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()));
            Box::pin(async move { result })
        }

        fn send_image<'a>(&'a self, params: SendImageParams<'a>) -> OutboundFuture<'a> {
            self.events.lock().unwrap().push(Event::Image(
                params.image_path.to_string_lossy().into_owned(),
            ));
            let result = self
                .image_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()));
            Box::pin(async move { result })
        }
    }

    #[derive(Default)]
    struct RecordingOutput(Mutex<Vec<String>>);

    impl OutboundOutput for RecordingOutput {
        fn write_line(&self, line: &str) {
            self.0.lock().unwrap().push(line.to_owned());
        }
    }

    fn params<'a>(text: &'a str, workspace_dirs: &'a [PathBuf]) -> OutboundMessageParams<'a> {
        OutboundMessageParams {
            channel_name: "weixin-test",
            chat_id: "user-1",
            text,
            base_url: "https://example.test",
            token: "token",
            context_token: "context",
            workspace_dirs,
        }
    }

    #[test]
    fn parses_code_free_image_markers_and_leaves_code_markers_visible() {
        let parsed = parse_outbound_content(
            "caption [IMAGE: /tmp/one.png]\n```md\n[IMAGE: /tmp/code.png]\n```\n`[IMAGE: /tmp/inline.png]`",
        );
        assert_eq!(
            parsed,
            ParsedOutboundContent {
                cleaned_text:
                    "caption \n```md\n[IMAGE: /tmp/code.png]\n```\n`[IMAGE: /tmp/inline.png]`"
                        .to_owned(),
                image_paths: vec!["/tmp/one.png".to_owned()],
            }
        );
    }

    #[test]
    fn extracts_case_insensitively_and_only_removes_matching_parsed_markers() {
        let parsed = parse_outbound_content(
            "[image: /tmp/a.png] and [IMAGE: /tmp/a.png] plus [IMAGE: /tmp/a.png ]",
        );
        assert_eq!(
            parsed.image_paths,
            vec![
                "/tmp/a.png".to_owned(),
                "/tmp/a.png".to_owned(),
                "/tmp/a.png".to_owned()
            ]
        );
        assert_eq!(parsed.cleaned_text, "and  plus [IMAGE: /tmp/a.png ]");
    }

    #[test]
    fn ignores_empty_marker_paths_and_cleans_only_excess_blank_lines() {
        let parsed = parse_outbound_content("\n  hello\n\n\n[IMAGE:   ]\n\nworld  \n");
        assert!(parsed.image_paths.is_empty());
        assert_eq!(parsed.cleaned_text, "hello\n\n[IMAGE:   ]\n\nworld");
    }

    #[tokio::test]
    async fn sends_cleaned_text_first_then_images_in_appearance_order() {
        let sender = FakeSender::default();
        let output = RecordingOutput::default();
        let workspace_dirs = vec![PathBuf::from("/workspace")];

        send_outbound_message(
            &sender,
            &output,
            params(
                "summary [IMAGE: /tmp/first.png]\n\n[IMAGE: /tmp/second.png]",
                &workspace_dirs,
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            *sender.events.lock().unwrap(),
            vec![
                Event::Text("summary".to_owned()),
                Event::Image("/tmp/first.png".to_owned()),
                Event::Image("/tmp/second.png".to_owned()),
            ]
        );
        assert!(output.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn skips_empty_text_but_sends_an_image_marker() {
        let sender = FakeSender::default();
        let output = RecordingOutput::default();

        send_outbound_message(&sender, &output, params("\n[IMAGE: /tmp/only.png]\n", &[]))
            .await
            .unwrap();

        assert_eq!(
            *sender.events.lock().unwrap(),
            vec![Event::Image("/tmp/only.png".to_owned())]
        );
    }

    #[tokio::test]
    async fn text_failure_propagates_before_any_image_is_sent() {
        let sender =
            FakeSender::with_results([Err(OutboundSendError::new("text rejected"))], [Ok(())]);
        let output = RecordingOutput::default();

        let error = send_outbound_message(
            &sender,
            &output,
            params("caption [IMAGE: /tmp/image.png]", &[]),
        )
        .await
        .unwrap_err();

        assert_eq!(error.message, "text rejected");
        assert_eq!(
            *sender.events.lock().unwrap(),
            vec![Event::Text("caption".to_owned())]
        );
        assert!(output.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn image_failure_logs_falls_back_and_continues_sequentially() {
        let sender = FakeSender::with_results(
            [
                Ok(()),
                Err(OutboundSendError::new("fallback rejected")),
                Ok(()),
            ],
            [
                Err(OutboundSendError {
                    message: "expired".to_owned(),
                    status: 401,
                    ret: Some(9),
                    errcode: Some(-14),
                }),
                Ok(()),
            ],
        );
        let output = RecordingOutput::default();

        send_outbound_message(
            &sender,
            &output,
            params(
                "caption [IMAGE: /tmp/broken.png] [IMAGE: /tmp/good.png]",
                &[],
            ),
        )
        .await
        .unwrap();

        assert_eq!(
            *sender.events.lock().unwrap(),
            vec![
                Event::Text("caption".to_owned()),
                Event::Image("/tmp/broken.png".to_owned()),
                Event::Text("图片发送失败，请稍后重试".to_owned()),
                Event::Image("/tmp/good.png".to_owned()),
            ]
        );
        assert_eq!(
            *output.0.lock().unwrap(),
            vec![
                "[Weixin:weixin-test] Failed to send image (status=401 ret=9 errcode=-14): expired",
                "[Weixin:weixin-test] Fallback text also failed: fallback rejected",
            ]
        );
    }
}
