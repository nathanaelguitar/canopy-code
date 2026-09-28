//! Best-effort session title generation, ported from
//! `packages/core/src/services/sessionTitle.ts`.
//!
//! The history projection and title cleanup are deterministic and provider
//! independent. [`SessionTitleSideQuery`] is the small async seam for the
//! configured side-query client; it receives the exact model, prompt, schema,
//! and one-shot generation limits without depending on Canopy `Config`.

use std::future::Future;
use std::pin::Pin;

use serde_json::{Value, json};

use crate::transcript::is_user_prompt_submit_context_part_text;
use crate::utils::cancellation::CancellationToken;

const MAX_CONVERSATION_CHARS: usize = 1000;
const RECENT_MESSAGE_WINDOW: usize = 20;
pub const SESSION_TITLE_MAX_LENGTH: usize = 200;

const SYSTEM_REMINDER_OPEN: &str = "<system-reminder>";
const SYSTEM_REMINDER_CLOSE: &str = "</system-reminder>";

pub const TITLE_SYSTEM_PROMPT: &str = r#"Generate a concise, sentence-case title (3-7 words) that captures what this programming-assistant session is about. Think of it as a git commit subject for the session.

Rules:
- 3-7 words.
- Sentence case: capitalize only the first word and proper nouns. NOT Title Case.
- No trailing punctuation.
- No quotes, backticks, or markdown.
- Be specific about the user's actual goal — name the feature, bug, or subject area. Avoid vague "Code changes", "Help request", "Conversation".

Good examples:
{"title": "Fix login button on mobile"}
{"title": "Add OAuth authentication flow"}
{"title": "Debug failing CI pipeline tests"}
{"title": "重构用户鉴权中间件"}

Bad (too vague): {"title": "Code changes"}
Bad (too long): {"title": "Investigate and fix the session title generation issue in the chat recording service"}
Bad (wrong case): {"title": "Fix Login Button On Mobile"}
Bad (trailing punctuation): {"title": "Fix login button."}

Return ONLY a JSON object with a single "title" key. No preamble, no reasoning, no closing remarks."#;

pub const TITLE_USER_PROMPT: &str =
    "Generate the session title now. Populate the schema with a single short title string.";

/// A provider-neutral representation of one API history item. Function calls,
/// function responses, and other non-text parts use `text: None`. The two
/// hidden-reasoning markers are kept separately because the source drops a
/// text part when either value is truthy.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionTitlePart {
    pub text: Option<String>,
    pub thought: bool,
    pub thought_signature: bool,
}

impl SessionTitlePart {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: Some(text.into()),
            thought: false,
            thought_signature: false,
        }
    }
}

/// Gemini-shaped message history. Roles outside `user` and `model` are
/// intentionally accepted so tool and system records can be filtered out.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionTitleMessage {
    pub role: String,
    pub parts: Vec<SessionTitlePart>,
}

impl SessionTitleMessage {
    pub fn new(role: impl Into<String>, parts: Vec<SessionTitlePart>) -> Self {
        Self {
            role: role.into(),
            parts,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTitleFailureReason {
    NoFastModel,
    NoClient,
    EmptyHistory,
    EmptyResult,
    Aborted,
    ModelError,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionTitleOutcome {
    Success { title: String, model_used: String },
    Failure(SessionTitleFailureReason),
}

impl SessionTitleOutcome {
    fn failure(reason: SessionTitleFailureReason) -> Self {
        Self::Failure(reason)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTitlePromptPart {
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTitlePromptContent {
    pub role: String,
    pub parts: Vec<SessionTitlePromptPart>,
}

/// Request contract passed to the injected JSON side-query implementation.
/// `contents` contains one user message and `max_attempts` is always one.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionTitleQueryRequest {
    pub purpose: String,
    pub model: String,
    pub system_instruction: String,
    pub schema: Value,
    pub contents: Vec<SessionTitlePromptContent>,
    pub temperature: f64,
    pub max_output_tokens: usize,
    pub max_attempts: usize,
}

/// Async injection point for a side-query client. Implementations should
/// honor `abort_signal` while the request is in flight; the caller also races
/// this future against cancellation and discards a result if cancellation
/// arrives as the query completes.
pub trait SessionTitleSideQuery: Send + Sync {
    fn generate_json<'a>(
        &'a self,
        request: &'a SessionTitleQueryRequest,
        abort_signal: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;
}

/// Generate a short title using an injected one-shot side query. Best-effort
/// failures are represented as [`SessionTitleOutcome::Failure`], never as a
/// returned error. `None` for `side_query` represents an uninitialized model
/// client; `None` for `fast_model` represents missing fast-model config.
///
/// `user_display_texts` preserves source semantics: if at least one entry is
/// present, title context comes only from those recorded display projections;
/// `None` entries from old/resumed history are omitted. `Some("")` still opts
/// into projection mode but contributes no message.
pub async fn try_generate_session_title(
    side_query: Option<&dyn SessionTitleSideQuery>,
    fast_model: Option<&str>,
    full_history: Option<&[SessionTitleMessage]>,
    abort_signal: &CancellationToken,
    user_display_texts: &[Option<String>],
) -> SessionTitleOutcome {
    let Some(model) = fast_model.filter(|model| !model.is_empty()) else {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::NoFastModel);
    };
    let Some(side_query) = side_query else {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::NoClient);
    };
    let Some(full_history) = full_history else {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::NoClient);
    };
    if full_history.len() < 2 {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::EmptyHistory);
    }

    let has_display_projection = user_display_texts.iter().any(Option::is_some);
    let dialog = if has_display_projection {
        user_display_texts
            .iter()
            .filter_map(|display_text| display_text.as_deref())
            .filter(|display_text| !display_text.is_empty())
            .map(|display_text| {
                SessionTitleMessage::new("user", vec![SessionTitlePart::text(display_text)])
            })
            .collect()
    } else {
        filter_to_dialog(full_history)
    };
    let recent_history = take_recent_dialog(&dialog, RECENT_MESSAGE_WINDOW);
    if recent_history.is_empty() {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::EmptyHistory);
    }

    let conversation_text = flatten_to_tail(&recent_history, MAX_CONVERSATION_CHARS);
    if trim_js(&conversation_text).is_empty() {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::EmptyHistory);
    }

    let request = SessionTitleQueryRequest {
        purpose: "session-title".to_owned(),
        model: model.to_owned(),
        system_instruction: TITLE_SYSTEM_PROMPT.to_owned(),
        schema: json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "A concise sentence-case session title, 3-7 words, no trailing punctuation."
                }
            },
            "required": ["title"]
        }),
        contents: vec![SessionTitlePromptContent {
            role: "user".to_owned(),
            parts: vec![SessionTitlePromptPart {
                text: format!("Conversation so far:\n{conversation_text}\n\n{TITLE_USER_PROMPT}"),
            }],
        }],
        temperature: 0.2,
        max_output_tokens: 100,
        max_attempts: 1,
    };

    let result = tokio::select! {
        biased;
        _ = abort_signal.cancelled() => {
            return SessionTitleOutcome::failure(SessionTitleFailureReason::Aborted);
        }
        result = side_query.generate_json(&request, abort_signal) => result,
    };
    if abort_signal.is_cancelled() {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::Aborted);
    }
    let result = match result {
        Ok(result) => result,
        Err(_) => return SessionTitleOutcome::failure(SessionTitleFailureReason::ModelError),
    };
    let raw_title = result
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let title = sanitize_title(raw_title);
    if title.is_empty() {
        return SessionTitleOutcome::failure(SessionTitleFailureReason::EmptyResult);
    }
    SessionTitleOutcome::Success {
        title,
        model_used: model.to_owned(),
    }
}

/// Remove terminal control sequences and residual markdown/punctuation from
/// a model-returned title. The output is capped at 200 UTF-16 code units, as
/// in the TypeScript session-list limit, without splitting an astral scalar.
pub fn sanitize_title(input: &str) -> String {
    let mut title = strip_terminal_control_sequences(input);
    title = strip_leading_paired_bracket(&title);
    title = strip_trailing_paired_bracket(&title);
    title = title
        .trim_start_matches(|ch: char| {
            is_js_whitespace(ch) || matches!(ch, '>' | '*' | '-' | '#' | '`' | '"' | '\'' | '_')
        })
        .to_owned();
    title = title
        .trim_end_matches(|ch: char| {
            is_js_whitespace(ch) || matches!(ch, '*' | '`' | '"' | '\'' | '_')
        })
        .to_owned();
    title = title
        .trim_end_matches(|ch: char| {
            matches!(
                ch,
                '.' | '!' | '?' | '。' | '！' | '？' | ',' | '，' | ';' | '；' | ':' | '：'
            )
        })
        .to_owned();
    title = collapse_whitespace(&title);
    if title.is_empty() {
        return title;
    }
    truncate_utf16(&title, SESSION_TITLE_MAX_LENGTH)
        .trim_matches(is_js_whitespace)
        .to_owned()
}

/// Drop hidden/non-dialog history entries and remove startup, reminder, and
/// UserPromptSubmit context using the same structural rules as TypeScript.
pub fn filter_to_dialog(history: &[SessionTitleMessage]) -> Vec<SessionTitleMessage> {
    let startup_context_length = get_startup_context_length(history);
    let mut output = Vec::new();
    for message in &history[startup_context_length..] {
        if message.role != "user" && message.role != "model" {
            continue;
        }
        let mut parts = message.parts.as_slice();
        if parts.len() > 1
            && parts
                .last()
                .and_then(|part| part.text.as_deref())
                .is_some_and(is_user_prompt_submit_context_part_text)
        {
            parts = &parts[..parts.len() - 1];
        }

        let text_parts = parts
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
                Some(SessionTitlePart {
                    text: Some(text),
                    thought: false,
                    thought_signature: false,
                })
            })
            .collect::<Vec<_>>();
        if !text_parts.is_empty() {
            output.push(SessionTitleMessage::new(message.role.clone(), text_parts));
        }
    }
    output
}

/// Take the latest `window_size` dialog messages without beginning on an
/// assistant response whose preceding user turn fell outside the window.
pub fn take_recent_dialog(
    history: &[SessionTitleMessage],
    window_size: usize,
) -> Vec<SessionTitleMessage> {
    if history.len() <= window_size {
        return history.to_vec();
    }
    let mut start = history.len() - window_size;
    while start < history.len() && history[start].role != "user" {
        start += 1;
    }
    history[start..].to_vec()
}

/// Flatten labeled user/assistant dialog and retain only its UTF-16 tail.
pub fn flatten_to_tail(history: &[SessionTitleMessage], max_chars: usize) -> String {
    let lines = history
        .iter()
        .filter_map(|message| {
            let role = if message.role == "user" {
                "User"
            } else {
                "Assistant"
            };
            let text = message
                .parts
                .iter()
                .filter_map(|part| part.text.as_deref())
                .collect::<String>();
            let text = trim_js(&text);
            (!text.is_empty()).then(|| format!("{role}: {text}"))
        })
        .collect::<Vec<_>>();
    let joined = lines.join("\n");
    truncate_utf16_tail(&joined, max_chars)
}

fn get_startup_context_length(history: &[SessionTitleMessage]) -> usize {
    let Some(first) = history.first() else {
        return 0;
    };
    if first.role != "user" {
        return 0;
    }
    if !first.parts.is_empty()
        && first.parts.iter().all(|part| {
            part.text.as_deref().is_some_and(|text| {
                text.starts_with(SYSTEM_REMINDER_OPEN)
                    && trim_end_js(text).ends_with(SYSTEM_REMINDER_CLOSE)
            })
        })
    {
        return 1;
    }
    if history.get(1).is_some_and(|second| {
        second.role == "model"
            && second.parts.first().and_then(|part| part.text.as_deref())
                == Some("Got it. Thanks for the context!")
    }) {
        return 2;
    }
    0
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

fn strip_terminal_control_sequences(text: &str) -> String {
    let chars = text.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(text.len());
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\u{1b}' && chars.get(index + 1) == Some(&']') {
            if let Some(end) = find_osc_end(&chars, index + 2) {
                output.push(' ');
                index = end;
                continue;
            }
        }
        if chars[index] == '\u{1b}' && chars.get(index + 1) == Some(&'[') {
            if let Some(end) = find_csi_end(&chars, index + 2) {
                output.push(' ');
                index = end;
                continue;
            }
        }
        if chars[index] == '\u{1b}'
            && chars
                .get(index + 1)
                .is_some_and(|ch| matches!(*ch, 'N' | 'O' | 'P'))
        {
            index += 2;
            continue;
        }
        let ch = chars[index];
        if ch <= '\u{1f}' || ('\u{7f}'..='\u{9f}').contains(&ch) {
            output.push(' ');
        } else {
            output.push(ch);
        }
        index += 1;
    }
    output
}

/// Return an exclusive character index after a complete OSC sequence.
fn find_osc_end(chars: &[char], start: usize) -> Option<usize> {
    let mut index = start;
    while index < chars.len() {
        if chars[index] == '\u{7}' {
            return Some(index + 1);
        }
        if chars[index] == '\u{1b}' && chars.get(index + 1) == Some(&'\\') {
            return Some(index + 2);
        }
        if chars[index] == '\u{1b}' {
            return None;
        }
        index += 1;
    }
    None
}

/// Return an exclusive character index after a CSI with source-compatible
/// parameter and final-byte classes.
fn find_csi_end(chars: &[char], start: usize) -> Option<usize> {
    let mut index = start;
    while index < chars.len()
        && (chars[index].is_ascii_digit() || matches!(chars[index], ';' | '?'))
    {
        index += 1;
    }
    chars
        .get(index)
        .is_some_and(char::is_ascii_alphabetic)
        .then_some(index + 1)
}

fn strip_leading_paired_bracket(text: &str) -> String {
    let leading_whitespace = text.len() - trim_start_js(text).len();
    let rest = &text[leading_whitespace..];
    if !rest.chars().next().is_some_and(is_open_bracket) {
        return text.to_owned();
    }
    let after_open = rest.chars().next().map_or(0, char::len_utf8);
    let Some(close_offset) = rest[after_open..]
        .char_indices()
        .find_map(|(offset, ch)| is_close_bracket(ch).then_some(offset + ch.len_utf8()))
    else {
        return text.to_owned();
    };
    let after_close = leading_whitespace + after_open + close_offset;
    trim_start_js(&text[after_close..]).to_owned()
}

fn strip_trailing_paired_bracket(text: &str) -> String {
    let trimmed_end = trim_end_js(text);
    let Some((close_index, close_char)) = trimmed_end.char_indices().next_back() else {
        return text.to_owned();
    };
    if !is_close_bracket(close_char) {
        return text.to_owned();
    }
    let Some((open_index, _)) = trimmed_end[..close_index]
        .char_indices()
        .rev()
        .find(|(_, ch)| is_open_bracket(*ch))
    else {
        return text.to_owned();
    };
    format!("{}{}", &text[..open_index], &text[trimmed_end.len()..])
}

fn is_open_bracket(ch: char) -> bool {
    matches!(ch, '「' | '『' | '【' | '〈' | '《')
}

fn is_close_bracket(ch: char) -> bool {
    matches!(ch, '」' | '』' | '】' | '〉' | '》')
}

fn collapse_whitespace(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut previous_was_whitespace = false;
    for ch in trim_js(text).chars() {
        if is_js_whitespace(ch) {
            if !previous_was_whitespace {
                output.push(' ');
            }
            previous_was_whitespace = true;
        } else {
            output.push(ch);
            previous_was_whitespace = false;
        }
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

fn trim_start_js(text: &str) -> &str {
    text.trim_start_matches(is_js_whitespace)
}

fn trim_end_js(text: &str) -> &str {
    text.trim_end_matches(is_js_whitespace)
}

fn truncate_utf16(text: &str, max_units: usize) -> String {
    let mut units = 0;
    let mut end = 0;
    for (index, ch) in text.char_indices() {
        let width = ch.len_utf16();
        if units + width > max_units {
            break;
        }
        units += width;
        end = index + ch.len_utf8();
    }
    text[..end].to_owned()
}

fn truncate_utf16_tail(text: &str, max_units: usize) -> String {
    let total_units = text.encode_utf16().count();
    if total_units <= max_units {
        return text.to_owned();
    }
    let excess = total_units - max_units;
    let mut units = 0;
    let mut start = text.len();
    for (index, ch) in text.char_indices() {
        let next_units = units + ch.len_utf16();
        if next_units > excess {
            // JS slice can start in the middle of a surrogate pair. Its
            // cleanup drops that orphan low surrogate, so skip this scalar.
            start = index + ch.len_utf8();
            break;
        }
        if next_units == excess {
            start = index + ch.len_utf8();
            break;
        }
        units = next_units;
    }
    text[start..].to_owned()
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending};
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::time::Duration;

    use serde_json::{Value, json};

    use super::{
        SYSTEM_REMINDER_CLOSE, SYSTEM_REMINDER_OPEN, SessionTitleFailureReason as Failure,
        SessionTitleMessage as Message, SessionTitleOutcome as Outcome, SessionTitlePart as Part,
        SessionTitleQueryRequest, SessionTitleSideQuery, flatten_to_tail, sanitize_title,
        take_recent_dialog, try_generate_session_title,
    };
    use crate::utils::cancellation::CancellationToken;

    struct MockQuery {
        result: Result<Value, String>,
        request: Mutex<Option<SessionTitleQueryRequest>>,
        never_completes: bool,
    }

    impl MockQuery {
        fn result(result: Result<Value, String>) -> Self {
            Self {
                result,
                request: Mutex::new(None),
                never_completes: false,
            }
        }

        fn pending() -> Self {
            Self {
                result: Ok(Value::Null),
                request: Mutex::new(None),
                never_completes: true,
            }
        }
    }

    impl SessionTitleSideQuery for MockQuery {
        fn generate_json<'a>(
            &'a self,
            request: &'a SessionTitleQueryRequest,
            _abort_signal: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            *self.request.lock().unwrap() = Some(request.clone());
            if self.never_completes {
                Box::pin(pending())
            } else {
                let result = self.result.clone();
                Box::pin(async move { result })
            }
        }
    }

    fn history() -> Vec<Message> {
        vec![
            Message::new(
                "user",
                vec![Part::text("my login button is broken on mobile")],
            ),
            Message::new(
                "model",
                vec![Part::text(
                    "Let's look at the button handler and the viewport CSS.",
                )],
            ),
        ]
    }

    #[tokio::test]
    async fn returns_best_effort_failure_reasons_for_missing_inputs_and_errors() {
        let token = CancellationToken::new();
        let query = MockQuery::result(Err("API down".to_owned()));
        assert_eq!(
            try_generate_session_title(None, None, None, &token, &[]).await,
            Outcome::Failure(Failure::NoFastModel)
        );
        assert_eq!(
            try_generate_session_title(None, Some("fast"), Some(&history()), &token, &[]).await,
            Outcome::Failure(Failure::NoClient)
        );
        assert_eq!(
            try_generate_session_title(Some(&query), Some("fast"), Some(&[]), &token, &[]).await,
            Outcome::Failure(Failure::EmptyHistory)
        );
        assert_eq!(
            try_generate_session_title(Some(&query), Some("fast"), Some(&history()), &token, &[])
                .await,
            Outcome::Failure(Failure::ModelError)
        );
    }

    #[tokio::test]
    async fn uses_the_fast_model_schema_and_single_attempt_contract() {
        let token = CancellationToken::new();
        let query = MockQuery::result(Ok(json!({"title":"Fix login button on mobile"})));
        let outcome = try_generate_session_title(
            Some(&query),
            Some("qwen-turbo"),
            Some(&history()),
            &token,
            &[],
        )
        .await;
        assert_eq!(
            outcome,
            Outcome::Success {
                title: "Fix login button on mobile".to_owned(),
                model_used: "qwen-turbo".to_owned()
            }
        );
        let request = query.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.purpose, "session-title");
        assert_eq!(request.model, "qwen-turbo");
        assert_eq!(request.schema["type"], "object");
        assert_eq!(request.schema["required"], json!(["title"]));
        assert_eq!(request.schema["properties"]["title"]["type"], "string");
        assert_eq!(request.temperature, 0.2);
        assert_eq!(request.max_output_tokens, 100);
        assert_eq!(request.max_attempts, 1);
        assert!(
            request
                .system_instruction
                .contains("sentence-case title (3-7 words)")
        );
    }

    #[tokio::test]
    async fn returns_empty_result_for_unusable_model_output() {
        let token = CancellationToken::new();
        let query = MockQuery::result(Ok(json!({"title":"   ...  "})));
        assert_eq!(
            try_generate_session_title(Some(&query), Some("fast"), Some(&history()), &token, &[])
                .await,
            Outcome::Failure(Failure::EmptyResult)
        );
    }

    #[tokio::test]
    async fn display_projection_overrides_hidden_history_and_empty_projection_stays_empty() {
        let token = CancellationToken::new();
        let query = MockQuery::result(Ok(json!({"title":"Visible topic"})));
        let raw = vec![
            Message::new("user", vec![Part::text("hidden channel instructions")]),
            Message::new("model", vec![Part::text("Hello!")]),
        ];
        let projection = vec![Some("你好".to_owned())];
        let outcome =
            try_generate_session_title(Some(&query), Some("fast"), Some(&raw), &token, &projection)
                .await;
        assert!(matches!(outcome, Outcome::Success { .. }));
        let request = query.request.lock().unwrap().clone().unwrap();
        let prompt = &request.contents[0].parts[0].text;
        assert!(prompt.contains("你好"));
        assert!(!prompt.contains("hidden channel instructions"));

        let empty_query = MockQuery::result(Ok(json!({"title":"Unused"})));
        assert_eq!(
            try_generate_session_title(
                Some(&empty_query),
                Some("fast"),
                Some(&raw),
                &token,
                &[Some(String::new()), Some(String::new())]
            )
            .await,
            Outcome::Failure(Failure::EmptyHistory)
        );
        assert!(empty_query.request.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn old_unprojected_user_turns_are_omitted_when_projection_mode_is_active() {
        let token = CancellationToken::new();
        let query = MockQuery::result(Ok(json!({"title":"Answer greeting"})));
        let raw = vec![
            Message::new("user", vec![Part::text("older hidden instructions")]),
            Message::new("model", vec![Part::text("Old reply")]),
            Message::new("user", vec![Part::text("current hidden instructions")]),
            Message::new("model", vec![Part::text("Current reply")]),
        ];
        try_generate_session_title(
            Some(&query),
            Some("fast"),
            Some(&raw),
            &token,
            &[None, None, Some("当前消息".to_owned())],
        )
        .await;
        let request = query.request.lock().unwrap().clone().unwrap();
        let prompt = &request.contents[0].parts[0].text;
        assert!(prompt.contains("当前消息"));
        assert!(!prompt.contains("undefined"));
        assert!(!prompt.contains("older hidden instructions"));
        assert!(!prompt.contains("current hidden instructions"));
    }

    #[tokio::test]
    async fn cancellation_interrupts_the_query_and_returns_aborted() {
        let token = CancellationToken::new();
        let query = MockQuery::pending();
        let cancel = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            cancel.cancel();
        });
        assert_eq!(
            try_generate_session_title(Some(&query), Some("fast"), Some(&history()), &token, &[])
                .await,
            Outcome::Failure(Failure::Aborted)
        );
    }

    #[test]
    fn filters_tools_hidden_reasoning_startup_and_reminder_context() {
        let reminder =
            |text: &str| format!("{SYSTEM_REMINDER_OPEN}\n{text}\n{SYSTEM_REMINDER_CLOSE}");
        let history = vec![
            Message::new("user", vec![Part::text(reminder("STARTUP_SKILL_LIST"))]),
            Message::new("user", vec![Part::text("fix the session title")]),
            Message::new(
                "model",
                vec![
                    Part {
                        text: Some("hidden reasoning".to_owned()),
                        thought: true,
                        thought_signature: false,
                    },
                    Part::text("I will inspect the title code."),
                ],
            ),
            Message::new(
                "user",
                vec![
                    Part::text(reminder("MID_SESSION_TOOL_METADATA")),
                    Part::text("please add a regression test"),
                ],
            ),
            Message::new("tool", vec![Part::text("tool output should not appear")]),
        ];
        let dialog = super::filter_to_dialog(&history);
        let flattened = flatten_to_tail(&dialog, 1000);
        assert!(!flattened.contains("STARTUP_SKILL_LIST"));
        assert!(!flattened.contains("MID_SESSION_TOOL_METADATA"));
        assert!(!flattened.contains("hidden reasoning"));
        assert!(!flattened.contains("tool output should not appear"));
        assert!(flattened.contains("fix the session title"));
        assert!(flattened.contains("I will inspect the title code."));
        assert!(flattened.contains("please add a regression test"));
    }

    #[test]
    fn strips_trailing_user_prompt_submit_context_part() {
        let context = crate::transcript::wrap_user_prompt_submit_context("UNRELATED_MEMORY_TOPIC");
        let history = vec![
            Message::new(
                "user",
                vec![
                    Part::text("Diagnose the parser CI failure."),
                    Part::text(context),
                ],
            ),
            Message::new(
                "model",
                vec![Part::text("I will inspect the parser workflow.")],
            ),
        ];
        let flattened = flatten_to_tail(&super::filter_to_dialog(&history), 1000);
        assert!(flattened.contains("Diagnose the parser CI failure."));
        assert!(!flattened.contains("UNRELATED_MEMORY_TOPIC"));
    }

    #[test]
    fn take_recent_dialog_never_starts_with_a_dangling_model_message() {
        let mut dialog = Vec::new();
        for index in 0..11 {
            dialog.push(Message::new("user", vec![Part::text(format!("u{index}"))]));
            dialog.push(Message::new("model", vec![Part::text(format!("m{index}"))]));
        }
        let recent = take_recent_dialog(&dialog, 20);
        assert_eq!(recent.len(), 20);
        let recent = take_recent_dialog(&dialog, 19);
        assert_eq!(
            recent.first().map(|message| message.role.as_str()),
            Some("user")
        );
        assert_eq!(recent.len(), 18);
    }

    #[test]
    fn tail_prompt_is_bounded_and_keeps_the_latest_topic_and_valid_unicode() {
        let history = vec![
            Message::new(
                "user",
                vec![Part::text(format!(
                    "BEGIN_HEAD {} END_TAIL_MARKER",
                    "x".repeat(3000)
                ))],
            ),
            Message::new("model", vec![Part::text("END_MODEL_MARKER")]),
        ];
        let tail = flatten_to_tail(&history, 1000);
        assert!(!tail.contains("BEGIN_HEAD"));
        assert!(tail.contains("END_MODEL_MARKER"));
        assert!(tail.encode_utf16().count() <= 1000);
        let astral_tail = flatten_to_tail(
            &[Message::new(
                "user",
                vec![Part::text(format!("{}😀Z", "x".repeat(10)))],
            )],
            2,
        );
        assert_eq!(astral_tail, "Z");
    }

    #[test]
    fn sanitizes_markdown_cjk_punctuation_whitespace_and_control_sequences() {
        assert_eq!(sanitize_title("> **Fix login button**"), "Fix login button");
        assert_eq!(sanitize_title("- Fix login button"), "Fix login button");
        assert_eq!(sanitize_title("`Fix login button`"), "Fix login button");
        assert_eq!(sanitize_title("Fix login button."), "Fix login button");
        assert_eq!(sanitize_title("修复登录按钮。"), "修复登录按钮");
        assert_eq!(sanitize_title("修复登录按钮，"), "修复登录按钮");
        assert_eq!(sanitize_title("Fix   login   button"), "Fix login button");
        assert_eq!(sanitize_title("【Draft】Fix login"), "Fix login");
        assert_eq!(sanitize_title("Fix login【draft】"), "Fix login");
        assert_eq!(sanitize_title(""), "");
        assert_eq!(sanitize_title("   \n  "), "");
        assert_eq!(sanitize_title("..."), "");
        assert_eq!(sanitize_title("**"), "");
        assert_eq!(
            sanitize_title("\u{1b}[2J\u{1b}[HHello world"),
            "Hello world"
        );
        assert_eq!(sanitize_title("before\u{7}after"), "before after");
        assert_eq!(
            sanitize_title("\u{1b}]8;;http://evil\u{1b}\\click\u{1b}]8;;\u{1b}\\"),
            "click"
        );
        assert_eq!(sanitize_title("a\0b"), "a b");
    }

    #[test]
    fn truncates_titles_without_splitting_an_astral_scalar() {
        let sanitized = sanitize_title(&format!("{}😀!", "x".repeat(199)));
        assert_eq!(sanitized.encode_utf16().count(), 199);
        assert!(!sanitized.contains('😀'));
    }
}
