//! Best-effort “where did I leave off” recaps, ported from
//! `packages/core/src/services/sessionRecap.ts`.
//!
//! History access (including the startup-context offset), the side-query
//! provider, cancellation, and logging are injected so this service stays
//! independent of the TypeScript `Config` and Gemini client. History
//! projection and `<recap>` extraction retain the source behavior.

use std::future::Future;
use std::pin::Pin;

use crate::utils::cancellation::CancellationToken;

pub const RECENT_MESSAGE_WINDOW: usize = 30;

pub const RECAP_SYSTEM_PROMPT: &str = r#"You generate session recaps for a programming assistant CLI.

The user stepped away and is coming back. Recap in under 40 words, 1-2 plain sentences, no markdown. Lead with the overall goal and current task, then the one next action. Skip root-cause narrative, fix internals, secondary to-dos, and em-dash tangents.

Output format — strict:
- Wrap your recap in <recap>...</recap> tags.
- Put NOTHING outside the tags. No preamble, no reasoning, no closing remarks.

Example:
<recap>Debugging the auth retry race condition. Next: add deterministic timing to the integration test.</recap>"#;

pub const RECAP_USER_PROMPT: &str =
    "Generate the recap now. Wrap it in <recap>...</recap>. Nothing outside the tags.";

const RECAP_OPEN_TAG: &str = "<recap>";
const RECAP_CLOSE_TAG: &str = "</recap>";
const SYSTEM_REMINDER_OPEN: &str = "<system-reminder>";
const SYSTEM_REMINDER_CLOSE: &str = "</system-reminder>";

/// A provider-neutral representation of one API-history part. Non-text parts
/// have `text: None`; thought flags preserve the source's truthy filtering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionRecapPart {
    pub text: Option<String>,
    pub thought: bool,
    pub thought_signature: bool,
}

impl SessionRecapPart {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            thought: false,
            thought_signature: false,
        }
    }
}

/// Gemini-shaped history item. Tool, system, and other roles are accepted so
/// the recap projection can remove them exactly as the source does.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionRecapMessage {
    pub role: String,
    pub parts: Vec<SessionRecapPart>,
}

impl SessionRecapMessage {
    pub fn new(role: impl Into<String>, parts: Vec<SessionRecapPart>) -> Self {
        Self {
            role: role.into(),
            parts,
        }
    }
}

/// Snapshot from the history provider. `startup_context_length` is the number
/// of leading API entries returned by Canopy's `getStartupContextLength` and
/// omitted before dialog filtering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionRecapHistory {
    pub messages: Vec<SessionRecapMessage>,
    pub startup_context_length: usize,
}

pub trait SessionRecapHistoryProvider: Send + Sync {
    /// `Ok(None)` means no initialized history client is available. Errors
    /// are logged and treated as best-effort failures.
    fn history_shallow(&self) -> Result<Option<SessionRecapHistory>, String>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionRecapQueryRequest {
    pub purpose: &'static str,
    pub contents: Vec<SessionRecapMessage>,
    pub system_instruction: &'static str,
    pub max_output_tokens: u32,
    pub temperature: f32,
    pub max_attempts: u8,
    /// `runSideQuery` defaults hidden thoughts off and excludes tools.
    pub include_thoughts: bool,
    pub tools_enabled: bool,
}

pub type SessionRecapQueryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;

pub trait SessionRecapSideQuery: Send + Sync {
    /// The adapter applies Canopy's configured fast-model, main-model, and
    /// default-model fallback chain, then performs the one-shot text query.
    fn query<'a>(
        &'a self,
        request: SessionRecapQueryRequest,
        cancellation: &'a CancellationToken,
    ) -> SessionRecapQueryFuture<'a>;
}

pub trait SessionRecapLogger: Send + Sync {
    fn debug(&self, message: &str);
    fn warn(&self, message: &str);
}

/// Filter startup context, tool/non-dialog entries, blank text, hidden
/// reasoning, and reminder blocks from the API history.
pub fn filter_to_dialog(
    history: &[SessionRecapMessage],
    startup_context_length: usize,
) -> Vec<SessionRecapMessage> {
    let mut output = Vec::new();
    for message in &history[startup_context_length.min(history.len())..] {
        if message.role != "user" && message.role != "model" {
            continue;
        }
        let parts = message
            .parts
            .iter()
            .filter_map(|part| {
                let text = part.text.as_deref()?;
                if trim_js(text).is_empty() || part.thought || part.thought_signature {
                    return None;
                }
                let text = strip_system_reminder_blocks(text);
                if trim_js(&text).is_empty() {
                    return None;
                }
                Some(SessionRecapPart {
                    text: Some(text),
                    thought: false,
                    thought_signature: false,
                })
            })
            .collect::<Vec<_>>();
        if !parts.is_empty() {
            output.push(SessionRecapMessage::new(message.role.clone(), parts));
        }
    }
    output
}

/// Return the latest `window_size` messages while keeping a user-turn
/// boundary: if the tentative first entry is an assistant message, advance
/// to the next user message instead of retaining a dangling reply.
pub fn take_recent_dialog(
    history: &[SessionRecapMessage],
    window_size: usize,
) -> Vec<SessionRecapMessage> {
    if history.len() <= window_size {
        return history.to_vec();
    }
    let mut start = history.len() - window_size;
    while start < history.len() && history[start].role != "user" {
        start += 1;
    }
    history[start..].to_vec()
}

/// Extract a recap using the TypeScript implementation's case-insensitive
/// first-pair behavior. If the closing tag is absent, use everything after
/// the opening tag; without an opening tag return `None`.
pub fn extract_recap(raw: &str) -> Option<String> {
    let lowered = raw.to_ascii_lowercase();
    let open_index = lowered.find(RECAP_OPEN_TAG)?;
    let body_start = open_index + RECAP_OPEN_TAG.len();
    if let Some(close_offset) = lowered[body_start..].find(RECAP_CLOSE_TAG) {
        let tagged = &raw[body_start..body_start + close_offset];
        // Source JS checks capture truthiness before trimming. An empty pair
        // falls through to the open-tag fallback (and thus returns the close
        // tag), while whitespace-only capture trims to an empty result.
        if !tagged.is_empty() {
            return Some(trim_js(tagged).to_owned());
        }
    }
    Some(trim_js(&raw[body_start..]).to_owned())
}

/// Generate a recap from the newest dialog messages. Every operational
/// failure is intentionally converted to `None` so cosmetic recap generation
/// cannot interrupt the main session flow.
pub async fn generate_session_recap(
    history_provider: &impl SessionRecapHistoryProvider,
    side_query: &impl SessionRecapSideQuery,
    logger: &impl SessionRecapLogger,
    cancellation: &CancellationToken,
) -> Option<String> {
    let history = match history_provider.history_shallow() {
        Ok(Some(history)) => history,
        Ok(None) => {
            logger.debug("recap skipped: no geminiClient available");
            return None;
        }
        Err(error) => {
            logger.warn(&format!("Recap generation failed: {error}"));
            return None;
        }
    };
    if history.messages.len() < 2 {
        logger.debug(&format!(
            "recap skipped: history too short ({} messages)",
            history.messages.len()
        ));
        return None;
    }

    let dialog = filter_to_dialog(&history.messages, history.startup_context_length);
    let recent_history = take_recent_dialog(&dialog, RECENT_MESSAGE_WINDOW);
    if recent_history.is_empty() {
        logger.debug("recap skipped: no dialog messages after filtering");
        return None;
    }

    logger.debug(&format!(
        "recap: sending side-query with {} messages",
        recent_history.len()
    ));
    let mut contents = recent_history;
    contents.push(SessionRecapMessage::new(
        "user",
        vec![SessionRecapPart::text(RECAP_USER_PROMPT)],
    ));
    let request = SessionRecapQueryRequest {
        purpose: "session-recap",
        contents,
        system_instruction: RECAP_SYSTEM_PROMPT,
        max_output_tokens: 300,
        temperature: 0.3,
        max_attempts: 1,
        include_thoughts: false,
        tools_enabled: false,
    };

    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            logger.debug("recap aborted by signal");
            return None;
        }
        result = side_query.query(request, cancellation) => result,
    };
    if cancellation.is_cancelled() {
        logger.debug("recap aborted by signal");
        return None;
    }
    let raw = match result {
        Ok(raw) => raw,
        Err(error) => {
            logger.warn(&format!("Recap generation failed: {error}"));
            return None;
        }
    };
    if raw.is_empty() {
        logger.debug("recap: model returned empty text");
        return None;
    }
    let Some(recap) = extract_recap(&raw).filter(|recap| !recap.is_empty()) else {
        logger.debug("recap: failed to extract <recap> tags from response");
        return None;
    };
    logger.debug(&format!(
        "recap generated: len={}",
        recap.encode_utf16().count()
    ));
    Some(recap)
}

fn strip_system_reminder_blocks(text: &str) -> String {
    let mut output = String::new();
    let mut cursor = 0;
    while cursor < text.len() {
        let Some(open_offset) = text[cursor..].find(SYSTEM_REMINDER_OPEN) else {
            output.push_str(&text[cursor..]);
            break;
        };
        let open = cursor + open_offset;
        let close_search = open + SYSTEM_REMINDER_OPEN.len();
        let Some(close_offset) = text[close_search..].find(SYSTEM_REMINDER_CLOSE) else {
            output.push_str(&text[cursor..open]);
            break;
        };
        output.push_str(&text[cursor..open]);
        cursor = close_search + close_offset + SYSTEM_REMINDER_CLOSE.len();
    }
    output
}

fn is_js_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn trim_js(text: &str) -> &str {
    text.trim_matches(is_js_whitespace)
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::{
        RECAP_SYSTEM_PROMPT, RECENT_MESSAGE_WINDOW, SessionRecapHistory,
        SessionRecapHistoryProvider, SessionRecapLogger, SessionRecapMessage, SessionRecapPart,
        SessionRecapQueryFuture, SessionRecapQueryRequest, SessionRecapSideQuery, extract_recap,
        filter_to_dialog, generate_session_recap, take_recent_dialog,
    };
    use crate::utils::cancellation::CancellationToken;

    #[derive(Clone)]
    struct MockHistory(Result<Option<SessionRecapHistory>, String>);

    impl SessionRecapHistoryProvider for MockHistory {
        fn history_shallow(&self) -> Result<Option<SessionRecapHistory>, String> {
            self.0.clone()
        }
    }

    struct MockQuery {
        result: Result<String, String>,
        request: Mutex<Option<SessionRecapQueryRequest>>,
        never_completes: bool,
    }

    impl MockQuery {
        fn result(result: Result<String, String>) -> Self {
            Self {
                result,
                request: Mutex::new(None),
                never_completes: false,
            }
        }

        fn pending() -> Self {
            Self {
                result: Ok(String::new()),
                request: Mutex::new(None),
                never_completes: true,
            }
        }
    }

    impl SessionRecapSideQuery for MockQuery {
        fn query<'a>(
            &'a self,
            request: SessionRecapQueryRequest,
            _cancellation: &'a CancellationToken,
        ) -> SessionRecapQueryFuture<'a> {
            *self.request.lock().unwrap() = Some(request);
            if self.never_completes {
                Box::pin(pending())
            } else {
                let result = self.result.clone();
                Box::pin(async move { result })
            }
        }
    }

    #[derive(Default)]
    struct MockLogger {
        debug: Mutex<Vec<String>>,
        warnings: Mutex<Vec<String>>,
    }

    impl SessionRecapLogger for MockLogger {
        fn debug(&self, message: &str) {
            self.debug.lock().unwrap().push(message.to_owned());
        }

        fn warn(&self, message: &str) {
            self.warnings.lock().unwrap().push(message.to_owned());
        }
    }

    fn msg(role: &str, text: &str) -> SessionRecapMessage {
        SessionRecapMessage::new(role, vec![SessionRecapPart::text(text)])
    }

    fn history() -> SessionRecapHistory {
        SessionRecapHistory {
            messages: vec![
                msg("user", "fix the session recap behavior"),
                msg("model", "I will inspect the recap service."),
            ],
            startup_context_length: 0,
        }
    }

    #[test]
    fn filters_startup_tool_thought_blank_and_reminder_content() {
        let reminder = |body: &str| format!("<system-reminder>\n{body}\n</system-reminder>");
        let history = vec![
            msg("user", "startup context"),
            SessionRecapMessage::new(
                "user",
                vec![
                    SessionRecapPart {
                        text: Some("hidden reasoning".to_owned()),
                        thought: true,
                        thought_signature: false,
                    },
                    SessionRecapPart {
                        text: Some(reminder("PLAN_MODE_REMINDER")).to_owned(),
                        thought: false,
                        thought_signature: true,
                    },
                    SessionRecapPart::text("continue work\n"),
                    SessionRecapPart::text("  \t "),
                    SessionRecapPart::text("ship fix ".to_owned() + &reminder("IDE_CONTEXT")),
                ],
            ),
            msg("tool", "large result should not be sent"),
            msg("model", "Visible answer."),
        ];
        let filtered = filter_to_dialog(&history, 1);
        assert_eq!(filtered.len(), 2);
        let serialized = format!("{filtered:?}");
        assert!(!serialized.contains("startup context"));
        assert!(!serialized.contains("PLAN_MODE_REMINDER"));
        assert!(!serialized.contains("IDE_CONTEXT"));
        assert!(!serialized.contains("hidden reasoning"));
        assert!(!serialized.contains("large result"));
        assert!(serialized.contains("continue work"));
        assert!(serialized.contains("ship fix"));
    }

    #[test]
    fn recent_window_never_starts_on_an_unpaired_model_reply() {
        let mut history = Vec::new();
        for index in 0..16 {
            history.push(msg("user", &format!("u{index}")));
            if index < 15 {
                history.push(msg("model", &format!("m{index}")));
            }
        }
        let recent = take_recent_dialog(&history, RECENT_MESSAGE_WINDOW);
        assert_eq!(recent.len(), 29);
        assert_eq!(recent.first().unwrap().role, "user");
        assert_eq!(recent.first().unwrap().parts[0].text.as_deref(), Some("u1"));

        let short = take_recent_dialog(&history[..4], RECENT_MESSAGE_WINDOW);
        assert_eq!(short, history[..4]);
        assert!(take_recent_dialog(&history, 0).is_empty());
    }

    #[test]
    fn recap_extraction_is_case_insensitive_and_supports_truncated_output() {
        assert_eq!(
            extract_recap("reasoning\n<ReCaP>  Fix auth. Next: test. </rEcAp>tail"),
            Some("Fix auth. Next: test.".to_owned())
        );
        assert_eq!(
            extract_recap("preamble <recap>Keep this ending"),
            Some("Keep this ending".to_owned())
        );
        assert_eq!(extract_recap("unwrapped answer"), None);
        assert_eq!(extract_recap("<recap>   </recap>"), Some(String::new()));
        assert_eq!(
            extract_recap("<recap></recap>"),
            Some("</recap>".to_owned())
        );
    }

    #[tokio::test]
    async fn sends_clean_recent_history_with_one_shot_query_settings() {
        let history = MockHistory(Ok(Some(SessionRecapHistory {
            messages: vec![
                msg("user", "startup prelude"),
                msg("user", "fix session title pollution"),
                msg("model", "I found the title service."),
                SessionRecapMessage::new(
                    "user",
                    vec![
                        SessionRecapPart::text("continue with recap coverage\n"),
                        SessionRecapPart::text("<system-reminder>IDE_CONTEXT</system-reminder>"),
                    ],
                ),
            ],
            startup_context_length: 1,
        })));
        let query = MockQuery::result(Ok(
            "analysis <recap>Fixing session recap coverage. Next: verify tests.</recap>".into(),
        ));
        let logger = MockLogger::default();
        let outcome =
            generate_session_recap(&history, &query, &logger, &CancellationToken::new()).await;
        assert_eq!(
            outcome.as_deref(),
            Some("Fixing session recap coverage. Next: verify tests.")
        );

        let request = query.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.purpose, "session-recap");
        assert_eq!(request.system_instruction, RECAP_SYSTEM_PROMPT);
        assert_eq!(request.max_output_tokens, 300);
        assert_eq!(request.temperature, 0.3);
        assert_eq!(request.max_attempts, 1);
        assert!(!request.include_thoughts);
        assert!(!request.tools_enabled);
        let serialized = format!("{:?}", request.contents);
        assert!(!serialized.contains("startup prelude"));
        assert!(!serialized.contains("IDE_CONTEXT"));
        assert!(serialized.contains("fix session title pollution"));
        assert!(serialized.contains("continue with recap coverage"));
        assert!(serialized.contains(super::RECAP_USER_PROMPT));
        assert!(
            logger
                .debug
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry == "recap generated: len=50")
        );
    }

    #[tokio::test]
    async fn unavailable_history_errors_short_history_and_model_errors_are_best_effort() {
        let logger = MockLogger::default();
        let token = CancellationToken::new();
        let query = MockQuery::result(Ok("<recap>unused</recap>".into()));

        assert_eq!(
            generate_session_recap(&MockHistory(Ok(None)), &query, &logger, &token).await,
            None
        );
        assert_eq!(
            generate_session_recap(
                &MockHistory(Err("history unavailable".into())),
                &query,
                &logger,
                &token
            )
            .await,
            None
        );
        assert_eq!(
            generate_session_recap(
                &MockHistory(Ok(Some(SessionRecapHistory {
                    messages: vec![msg("user", "only one")],
                    startup_context_length: 0,
                }))),
                &query,
                &logger,
                &token
            )
            .await,
            None
        );
        assert_eq!(
            generate_session_recap(
                &MockHistory(Ok(Some(history()))),
                &MockQuery::result(Err("provider failed".into())),
                &logger,
                &token
            )
            .await,
            None
        );
        assert!(
            logger
                .warnings
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.contains("history unavailable"))
        );
        assert!(
            logger
                .warnings
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.contains("provider failed"))
        );
    }

    #[tokio::test]
    async fn cancellation_interrupts_the_side_query() {
        let token = CancellationToken::new();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel.cancel();
        });
        let logger = MockLogger::default();
        let result = generate_session_recap(
            &MockHistory(Ok(Some(history()))),
            &MockQuery::pending(),
            &logger,
            &token,
        )
        .await;
        assert_eq!(result, None);
        assert!(
            logger
                .debug
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry == "recap aborted by signal")
        );
    }

    #[tokio::test]
    async fn absent_or_invalid_tagged_output_is_skipped() {
        let logger = MockLogger::default();
        for response in ["", "reasoning without tags", "<recap>   </recap>"] {
            let result = generate_session_recap(
                &MockHistory(Ok(Some(history()))),
                &MockQuery::result(Ok(response.to_owned())),
                &logger,
                &CancellationToken::new(),
            )
            .await;
            assert_eq!(result, None);
        }
    }
}
