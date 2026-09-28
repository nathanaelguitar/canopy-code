//! QQ Bot outbound markdown and text fallback delivery.
//!
//! Port of the delivery sequence in `QQChannel.sendMessage`. Routing and
//! persisted state stay with the caller; this module accepts a resolved API
//! route and a local sequence state, then returns its updated state in place.

use crate::channels::qqbot_api::{
    FETCH_TIMEOUT, QqbotApiError, QqbotHttpResponse, QqbotHttpTransport, ReqwestQqbotHttpTransport,
    send_qq_message_with_transport,
};
use crate::channels::sanitize::sanitize_log_text;
use reqwest::Client;
use serde_json::{Value, json};
use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Delivery error codes exposed by the TypeScript channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryErrorCode {
    RateLimited,
    RetryExhausted,
    FallbackFailed,
    ActiveMsgDisabled,
}

impl DeliveryErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RateLimited => "RATE_LIMITED",
            Self::RetryExhausted => "RETRY_EXHAUSTED",
            Self::FallbackFailed => "FALLBACK_FAILED",
            Self::ActiveMsgDisabled => "ACTIVE_MSG_DISABLED",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryError {
    pub code: DeliveryErrorCode,
    pub message: String,
}

impl DeliveryError {
    fn new(code: DeliveryErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DeliveryError {}

#[derive(Debug)]
pub enum QqbotSendError {
    Delivery(DeliveryError),
    Api(QqbotApiError),
}

impl fmt::Display for QqbotSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delivery(error) => error.fmt(formatter),
            Self::Api(error) => error.fmt(formatter),
        }
    }
}

impl Error for QqbotSendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Delivery(error) => Some(error),
            Self::Api(error) => Some(error),
        }
    }
}

impl From<DeliveryError> for QqbotSendError {
    fn from(error: DeliveryError) -> Self {
        Self::Delivery(error)
    }
}

impl From<QqbotApiError> for QqbotSendError {
    fn from(error: QqbotApiError) -> Self {
        Self::Api(error)
    }
}

/// The message sequence for one reply ID. Callers load this from their
/// routing map and persist the value after this function returns.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QqbotSendState {
    pub msg_seq: u64,
}

/// Per-send inputs after the channel has resolved its route and applied reply
/// ID freshness checks. `reply_msg_id` should be `None` for expired context.
#[derive(Clone, Copy, Debug)]
pub struct QqbotSendParams<'a> {
    pub api_base: &'a str,
    pub api_path: &'a str,
    pub access_token: &'a str,
    pub chat_id: &'a str,
    pub text: &'a str,
    pub reply_msg_id: Option<&'a str>,
    /// The map is unset or true by default. Existing reply context is still
    /// attempted when active messages are disabled; only fallback sends stop.
    pub active_messages_enabled: bool,
}

/// Send a QQ Bot message through reqwest-backed API transport.
pub async fn send_message(
    client: &Client,
    params: QqbotSendParams<'_>,
    state: &mut QqbotSendState,
) -> Result<(), QqbotSendError> {
    let transport = ReqwestQqbotHttpTransport::new(client);
    send_message_with_transport(&transport, FETCH_TIMEOUT, params, state).await
}

/// Injectable variant used by the channel adapter and offline tests.
pub async fn send_message_with_transport(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    params: QqbotSendParams<'_>,
    state: &mut QqbotSendState,
) -> Result<(), QqbotSendError> {
    if is_no_reply_text(params.text) {
        return Ok(());
    }

    let reply_msg_id = params.reply_msg_id.filter(|id| !id.is_empty());
    let previous_seq = state.msg_seq;
    let result = send_message_inner(transport, timeout, params, reply_msg_id, state).await;
    if result.is_err() && reply_msg_id.is_some() {
        state.msg_seq = previous_seq;
    }
    result
}

async fn send_message_inner(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    params: QqbotSendParams<'_>,
    reply_msg_id: Option<&str>,
    state: &mut QqbotSendState,
) -> Result<(), QqbotSendError> {
    if reply_msg_id.is_none() && !params.active_messages_enabled {
        return Err(active_messages_disabled(params.chat_id).into());
    }

    let previous_seq = state.msg_seq;
    let next_seq = if reply_msg_id.is_some() {
        previous_seq.saturating_add(1)
    } else {
        0
    };
    if reply_msg_id.is_some() {
        state.msg_seq = next_seq;
    }

    let mut passive_body = markdown_body(params.text);
    if let Some(reply_msg_id) = reply_msg_id {
        let object = passive_body
            .as_object_mut()
            .expect("markdown request is a JSON object");
        object.insert("msg_id".to_owned(), Value::String(reply_msg_id.to_owned()));
        object.insert("msg_seq".to_owned(), json!(next_seq));
    }
    let mut response = send_attempt(transport, timeout, params, &passive_body).await?;
    if response.ok() {
        consume_response_text(&mut response).await;
        return Ok(());
    }

    let passive_status = response.status;
    consume_response_text(&mut response).await;
    if passive_status == 429 {
        if reply_msg_id.is_some() {
            state.msg_seq = previous_seq;
        }
        return Err(rate_limited(params.chat_id).into());
    }

    if reply_msg_id.is_some() {
        state.msg_seq = previous_seq;
        if !params.active_messages_enabled {
            return Err(active_messages_disabled(params.chat_id).into());
        }

        let active_markdown_body = markdown_body(params.text);
        let mut active_markdown_response =
            send_attempt(transport, timeout, params, &active_markdown_body).await?;
        if active_markdown_response.ok() {
            consume_response_text(&mut active_markdown_response).await;
            return Ok(());
        }
        let active_markdown_status = active_markdown_response.status;
        consume_response_text(&mut active_markdown_response).await;
        if active_markdown_status == 429 {
            return Err(rate_limited(params.chat_id).into());
        }

        let active_text_body = text_body(params.text);
        let mut active_text_response =
            send_attempt(transport, timeout, params, &active_text_body).await?;
        if active_text_response.ok() {
            consume_response_text(&mut active_text_response).await;
            return Ok(());
        }
        let active_text_status = active_text_response.status;
        consume_response_text(&mut active_text_response).await;
        if active_text_status == 429 {
            return Err(rate_limited(params.chat_id).into());
        }
        return Err(DeliveryError::new(
            DeliveryErrorCode::FallbackFailed,
            format!(
                "All delivery attempts exhausted for {}",
                sanitize_log_text(params.chat_id, 64)
            ),
        )
        .into());
    }

    let plain_text_body = text_body(params.text);
    let mut fallback_response = send_attempt(transport, timeout, params, &plain_text_body).await?;
    if fallback_response.ok() {
        consume_response_text(&mut fallback_response).await;
        return Ok(());
    }

    let fallback_status = fallback_response.status;
    consume_response_text(&mut fallback_response).await;
    if fallback_status == 429 {
        return Err(rate_limited(params.chat_id).into());
    }
    Err(DeliveryError::new(
        DeliveryErrorCode::FallbackFailed,
        format!(
            "Plain-text fallback delivery failed for {}",
            sanitize_log_text(params.chat_id, 64)
        ),
    )
    .into())
}

async fn send_attempt(
    transport: &dyn QqbotHttpTransport,
    timeout: Duration,
    params: QqbotSendParams<'_>,
    body: &Value,
) -> Result<QqbotHttpResponse, QqbotSendError> {
    Ok(send_qq_message_with_transport(
        transport,
        timeout,
        params.api_base,
        params.api_path,
        params.access_token,
        body,
    )
    .await?)
}

async fn consume_response_text(response: &mut QqbotHttpResponse) {
    let _ = response.read_body().await;
}

fn markdown_body(text: &str) -> Value {
    json!({
        "msg_type": 2,
        "markdown": { "content": text },
    })
}

fn text_body(text: &str) -> Value {
    json!({
        "content": text,
        "msg_type": 0,
    })
}

fn is_no_reply_text(text: &str) -> bool {
    text.trim_matches(is_ecmascript_trim_char) == "<noreply>"
}

fn is_ecmascript_trim_char(character: char) -> bool {
    matches!(
        character,
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

fn rate_limited(chat_id: &str) -> DeliveryError {
    DeliveryError::new(
        DeliveryErrorCode::RateLimited,
        format!(
            "Message blocked by rate limit for {}",
            sanitize_log_text(chat_id, 64)
        ),
    )
}

fn active_messages_disabled(chat_id: &str) -> DeliveryError {
    DeliveryError::new(
        DeliveryErrorCode::ActiveMsgDisabled,
        format!(
            "Active messages disabled for {}",
            sanitize_log_text(chat_id, 64)
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DeliveryErrorCode, QqbotSendError, QqbotSendParams, QqbotSendState,
        send_message_with_transport,
    };
    use crate::channels::qqbot_api::{
        QqbotApiError, QqbotFuture, QqbotHttpRequest, QqbotHttpResponse, QqbotHttpTransport,
        QqbotResponseBody,
    };
    use serde_json::{Value, json};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct MockBody {
        body: Option<Vec<u8>>,
        reads: Arc<AtomicUsize>,
        fail_read: bool,
    }

    impl QqbotResponseBody for MockBody {
        fn read_all<'a>(&'a mut self) -> QqbotFuture<'a, Result<Vec<u8>, QqbotApiError>> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let body = self.body.take();
            let fail = self.fail_read;
            Box::pin(async move {
                if fail {
                    Err(QqbotApiError {
                        message: "body read failed".to_owned(),
                    })
                } else {
                    body.ok_or_else(|| QqbotApiError {
                        message: "body consumed twice".to_owned(),
                    })
                }
            })
        }

        fn cancel<'a>(&'a mut self) -> QqbotFuture<'a, ()> {
            self.body.take();
            Box::pin(async {})
        }
    }

    struct ResponseStep {
        status: u16,
        body: Vec<u8>,
        reads: Arc<AtomicUsize>,
        fail_read: bool,
    }

    impl ResponseStep {
        fn new(status: u16, body: &str) -> (Self, Arc<AtomicUsize>) {
            Self::with_read_failure(status, body, false)
        }

        fn with_read_failure(status: u16, body: &str, fail_read: bool) -> (Self, Arc<AtomicUsize>) {
            let reads = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    status,
                    body: body.as_bytes().to_vec(),
                    reads: Arc::clone(&reads),
                    fail_read,
                },
                reads,
            )
        }

        fn into_response(self) -> QqbotHttpResponse {
            QqbotHttpResponse::new(
                self.status,
                MockBody {
                    body: Some(self.body),
                    reads: self.reads,
                    fail_read: self.fail_read,
                },
            )
        }
    }

    enum Step {
        Response(ResponseStep),
        TransportError(String),
    }

    struct MockTransport {
        requests: Mutex<Vec<QqbotHttpRequest>>,
        responses: Mutex<VecDeque<Step>>,
    }

    impl MockTransport {
        fn new(steps: impl IntoIterator<Item = Step>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(steps.into_iter().collect()),
            }
        }

        fn requests(&self) -> Vec<QqbotHttpRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl QqbotHttpTransport for MockTransport {
        fn execute<'a>(
            &'a self,
            request: QqbotHttpRequest,
        ) -> QqbotFuture<'a, Result<QqbotHttpResponse, QqbotApiError>> {
            self.requests.lock().unwrap().push(request);
            let step = self.responses.lock().unwrap().pop_front();
            Box::pin(async move {
                match step {
                    Some(Step::Response(response)) => Ok(response.into_response()),
                    Some(Step::TransportError(message)) => Err(QqbotApiError { message }),
                    None => Err(QqbotApiError {
                        message: "mock response queue empty".to_owned(),
                    }),
                }
            })
        }
    }

    fn params<'a>(
        reply_msg_id: Option<&'a str>,
        active_messages_enabled: bool,
    ) -> QqbotSendParams<'a> {
        QqbotSendParams {
            api_base: "https://api.sgroup.qq.com",
            api_path: "/v2/users/test-chat-id/messages",
            access_token: "test-token",
            chat_id: "test-chat-id",
            text: "**bold**",
            reply_msg_id,
            active_messages_enabled,
        }
    }

    fn request_json(request: &QqbotHttpRequest) -> Value {
        serde_json::from_slice(request.body.as_ref().unwrap()).unwrap()
    }

    fn delivery_code(error: QqbotSendError) -> DeliveryErrorCode {
        match error {
            QqbotSendError::Delivery(error) => error.code,
            other => panic!("expected delivery error, got {other}"),
        }
    }

    #[tokio::test]
    async fn sends_markdown_reply_with_incremented_sequence_and_drains_success_body() {
        let (response, reads) = ResponseStep::new(200, "accepted");
        let http = MockTransport::new([Step::Response(response)]);
        let mut state = QqbotSendState { msg_seq: 4 };

        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-1"), true),
            &mut state,
        )
        .await
        .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].url,
            "https://api.sgroup.qq.com/v2/users/test-chat-id/messages"
        );
        assert_eq!(requests[0].timeout, Duration::from_secs(15));
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(name, value)| name == "Authorization" && value == "QQBot test-token")
        );
        assert!(
            requests[0]
                .headers
                .iter()
                .any(|(name, value)| name == "Content-Type" && value == "application/json")
        );
        assert_eq!(
            request_json(&requests[0]),
            json!({
                "msg_type": 2,
                "markdown": {"content": "**bold**"},
                "msg_id": "msg-1",
                "msg_seq": 5,
            })
        );
        assert_eq!(state.msg_seq, 5);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn passive_failure_rolls_back_then_retries_active_markdown_without_reply_fields() {
        let (passive, passive_reads) = ResponseStep::new(400, "unsupported");
        let (active, active_reads) = ResponseStep::new(200, "accepted");
        let http = MockTransport::new([Step::Response(passive), Step::Response(active)]);
        let mut state = QqbotSendState { msg_seq: 8 };

        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-1"), true),
            &mut state,
        )
        .await
        .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(request_json(&requests[0])["msg_seq"], 9);
        assert_eq!(
            request_json(&requests[1]),
            json!({
                "msg_type": 2,
                "markdown": {"content": "**bold**"},
            })
        );
        assert!(request_json(&requests[1]).get("msg_id").is_none());
        assert_eq!(state.msg_seq, 8);
        assert_eq!(passive_reads.load(Ordering::SeqCst), 1);
        assert_eq!(active_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn active_markdown_failure_falls_back_to_plain_text_for_reply() {
        let (passive, passive_reads) = ResponseStep::new(400, "markdown unsupported");
        let (active_markdown, markdown_reads) = ResponseStep::new(500, "active markdown failed");
        let (active_text, text_reads) = ResponseStep::new(200, "text accepted");
        let http = MockTransport::new([
            Step::Response(passive),
            Step::Response(active_markdown),
            Step::Response(active_text),
        ]);
        let mut state = QqbotSendState { msg_seq: 2 };

        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-2"), true),
            &mut state,
        )
        .await
        .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            request_json(&requests[2]),
            json!({
                "content": "**bold**",
                "msg_type": 0,
            })
        );
        assert_eq!(state.msg_seq, 2);
        assert_eq!(passive_reads.load(Ordering::SeqCst), 1);
        assert_eq!(markdown_reads.load(Ordering::SeqCst), 1);
        assert_eq!(text_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pure_active_send_tries_markdown_then_plain_text() {
        let (markdown, markdown_reads) = ResponseStep::new(400, "not supported");
        let (plain_text, text_reads) = ResponseStep::new(200, "accepted");
        let http = MockTransport::new([Step::Response(markdown), Step::Response(plain_text)]);
        let mut state = QqbotSendState::default();

        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(None, true),
            &mut state,
        )
        .await
        .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            request_json(&requests[0]),
            json!({
                "msg_type": 2,
                "markdown": {"content": "**bold**"},
            })
        );
        assert_eq!(
            request_json(&requests[1]),
            json!({
                "content": "**bold**",
                "msg_type": 0,
            })
        );
        assert_eq!(state.msg_seq, 0);
        assert_eq!(markdown_reads.load(Ordering::SeqCst), 1);
        assert_eq!(text_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn initial_429_is_not_retried_and_rolls_back_reply_sequence() {
        let (response, reads) = ResponseStep::new(429, "rate limited");
        let http = MockTransport::new([Step::Response(response)]);
        let mut state = QqbotSendState { msg_seq: 6 };

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-3"), true),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::RateLimited);
        assert_eq!(http.requests().len(), 1);
        assert_eq!(state.msg_seq, 6);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn active_retry_429_stops_before_text_and_keeps_sequence_rolled_back() {
        let (passive, passive_reads) = ResponseStep::new(400, "bad request");
        let (active, active_reads) = ResponseStep::new(429, "rate limited");
        let http = MockTransport::new([Step::Response(passive), Step::Response(active)]);
        let mut state = QqbotSendState { msg_seq: 3 };

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-4"), true),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::RateLimited);
        assert_eq!(http.requests().len(), 2);
        assert_eq!(state.msg_seq, 3);
        assert_eq!(passive_reads.load(Ordering::SeqCst), 1);
        assert_eq!(active_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn active_text_fallback_429_stops_and_preserves_rolled_back_reply_sequence() {
        let (passive, passive_reads) = ResponseStep::new(400, "bad request");
        let (active_markdown, markdown_reads) = ResponseStep::new(500, "markdown failed");
        let (active_text, text_reads) = ResponseStep::new(429, "rate limited");
        let http = MockTransport::new([
            Step::Response(passive),
            Step::Response(active_markdown),
            Step::Response(active_text),
        ]);
        let mut state = QqbotSendState { msg_seq: 7 };

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-4"), true),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::RateLimited);
        assert_eq!(http.requests().len(), 3);
        assert_eq!(request_json(&http.requests()[2])["msg_type"], 0);
        assert_eq!(state.msg_seq, 7);
        for reads in [passive_reads, markdown_reads, text_reads] {
            assert_eq!(reads.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn pure_active_fallback_failure_has_its_own_delivery_error() {
        let (markdown, markdown_reads) = ResponseStep::new(500, "markdown failed");
        let (plain_text, text_reads) = ResponseStep::new(500, "plain text failed");
        let http = MockTransport::new([Step::Response(markdown), Step::Response(plain_text)]);
        let mut state = QqbotSendState::default();

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(None, true),
            &mut state,
        )
        .await
        .unwrap_err();

        match error {
            QqbotSendError::Delivery(error) => {
                assert_eq!(error.code, DeliveryErrorCode::FallbackFailed);
                assert_eq!(
                    error.message,
                    "Plain-text fallback delivery failed for test-chat-id"
                );
            }
            other => panic!("expected delivery failure, got {other}"),
        }
        assert_eq!(http.requests().len(), 2);
        assert_eq!(state.msg_seq, 0);
        assert_eq!(markdown_reads.load(Ordering::SeqCst), 1);
        assert_eq!(text_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn exhausted_reply_fallbacks_return_fallback_failed_and_drain_all_bodies() {
        let (passive, passive_reads) = ResponseStep::new(400, "passive failed");
        let (active_markdown, markdown_reads) = ResponseStep::new(500, "markdown failed");
        let (active_text, text_reads) = ResponseStep::new(500, "text failed");
        let http = MockTransport::new([
            Step::Response(passive),
            Step::Response(active_markdown),
            Step::Response(active_text),
        ]);
        let mut state = QqbotSendState { msg_seq: 1 };

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("msg-5"), true),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::FallbackFailed);
        assert_eq!(http.requests().len(), 3);
        assert_eq!(state.msg_seq, 1);
        for reads in [passive_reads, markdown_reads, text_reads] {
            assert_eq!(reads.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn plain_text_fallback_429_is_delivery_rate_limited() {
        let (markdown, markdown_reads) = ResponseStep::new(500, "markdown failed");
        let (plain_text, text_reads) = ResponseStep::new(429, "rate limited");
        let http = MockTransport::new([Step::Response(markdown), Step::Response(plain_text)]);
        let mut state = QqbotSendState::default();

        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(None, true),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::RateLimited);
        assert_eq!(http.requests().len(), 2);
        assert_eq!(markdown_reads.load(Ordering::SeqCst), 1);
        assert_eq!(text_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn active_disabled_rejects_pure_send_but_allows_passive_reply() {
        let http = MockTransport::new([]);
        let mut state = QqbotSendState::default();
        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(None, false),
            &mut state,
        )
        .await
        .unwrap_err();
        assert_eq!(delivery_code(error), DeliveryErrorCode::ActiveMsgDisabled);
        assert!(http.requests().is_empty());

        let (passive, reads) = ResponseStep::new(200, "reply accepted");
        let http = MockTransport::new([Step::Response(passive)]);
        let mut state = QqbotSendState { msg_seq: 0 };
        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("reply-msg"), false),
            &mut state,
        )
        .await
        .unwrap();
        assert_eq!(http.requests().len(), 1);
        assert_eq!(state.msg_seq, 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn passive_failure_with_active_disabled_rolls_back_and_stops() {
        let (passive, reads) = ResponseStep::new(400, "bad request");
        let http = MockTransport::new([Step::Response(passive)]);
        let mut state = QqbotSendState { msg_seq: 5 };
        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("reply-msg"), false),
            &mut state,
        )
        .await
        .unwrap_err();

        assert_eq!(delivery_code(error), DeliveryErrorCode::ActiveMsgDisabled);
        assert_eq!(http.requests().len(), 1);
        assert_eq!(state.msg_seq, 5);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn network_error_rolls_back_reply_sequence_and_propagates_api_error() {
        let http = MockTransport::new([Step::TransportError("connection reset".to_owned())]);
        let mut state = QqbotSendState { msg_seq: 9 };
        let error = send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(Some("reply-msg"), true),
            &mut state,
        )
        .await
        .unwrap_err();

        match error {
            QqbotSendError::Api(error) => assert_eq!(error.message, "connection reset"),
            other => panic!("expected API transport failure, got {other}"),
        }
        assert_eq!(http.requests().len(), 1);
        assert_eq!(state.msg_seq, 9);
    }

    #[tokio::test]
    async fn body_read_errors_are_ignored_after_consumption_like_response_text_catch() {
        let (failed_body, reads) = ResponseStep::with_read_failure(400, "ignored", true);
        let (fallback, fallback_reads) = ResponseStep::new(200, "accepted");
        let http = MockTransport::new([Step::Response(failed_body), Step::Response(fallback)]);
        let mut state = QqbotSendState::default();

        send_message_with_transport(
            &http,
            Duration::from_secs(15),
            params(None, true),
            &mut state,
        )
        .await
        .unwrap();

        assert_eq!(http.requests().len(), 2);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn noreply_suppression_does_not_call_transport_or_change_state() {
        let http = MockTransport::new([]);
        let mut state = QqbotSendState { msg_seq: 4 };
        let mut send = params(Some("reply-msg"), true);
        send.text = " \u{feff}<noreply>\u{feff} ";

        send_message_with_transport(&http, Duration::from_secs(15), send, &mut state)
            .await
            .unwrap();

        assert!(http.requests().is_empty());
        assert_eq!(state.msg_seq, 4);
    }

    #[tokio::test]
    async fn noreply_suppression_uses_ecmascript_whitespace_rules() {
        let (response, _) = ResponseStep::new(200, "accepted");
        let http = MockTransport::new([Step::Response(response)]);
        let mut state = QqbotSendState::default();
        let mut send = params(None, true);
        // ECMAScript String.trim() does not trim NEXT LINE (U+0085).
        send.text = "\u{0085}<noreply>\u{0085}";

        send_message_with_transport(&http, Duration::from_secs(15), send, &mut state)
            .await
            .unwrap();

        let requests = http.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(request_json(&requests[0])["markdown"]["content"], send.text);
    }

    #[test]
    fn delivery_codes_match_the_channel_contract() {
        assert_eq!(DeliveryErrorCode::RateLimited.as_str(), "RATE_LIMITED");
        assert_eq!(
            DeliveryErrorCode::RetryExhausted.as_str(),
            "RETRY_EXHAUSTED"
        );
        assert_eq!(
            DeliveryErrorCode::FallbackFailed.as_str(),
            "FALLBACK_FAILED"
        );
        assert_eq!(
            DeliveryErrorCode::ActiveMsgDisabled.as_str(),
            "ACTIVE_MSG_DISABLED"
        );
    }
}
