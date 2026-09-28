//! Tool-use summary generation port of
//! `packages/core/src/services/toolUseSummary.ts`.
//!
//! Model access is injected through [`ToolSummaryModelQuery`], keeping this
//! service independent of provider clients and runtime configuration. JSON
//! values are serialized through a bounded-depth adapter so large strings in
//! the commonly shallow tool argument/result structures are sliced before
//! serialization.

use std::future::Future;
use std::pin::Pin;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq};
use serde_json::Value;
use uuid::Uuid;

use crate::utils::cancellation::CancellationToken;

/// System instruction sent to the fast model. Keep aligned with the source
/// TypeScript prompt because the label is shown as a single-line UI row.
pub const TOOL_USE_SUMMARY_SYSTEM_PROMPT: &str = "Write a short summary label describing what these tool calls accomplished. It appears as a single-line row in a mobile app and truncates around 30 characters, so think git-commit-subject, not sentence.\n\nKeep the verb in past tense and the most distinctive noun. Drop articles, connectors, and long location context first.\n\nExamples:\n- Searched in auth/\n- Fixed NPE in UserService\n- Created signup endpoint\n- Read config.json\n- Ran failing tests";

const INPUT_TRUNCATE_LENGTH: usize = 300;
const LAST_ASSISTANT_TEXT_LENGTH: usize = 200;
const MAX_SUMMARY_LENGTH: usize = 100;

/// Stream-compatible summary message. Serialization uses the SDK field names,
/// including `precedingToolUseIds`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ToolUseSummaryMessage {
    #[serde(rename = "type")]
    pub message_type: &'static str,
    pub summary: String,
    #[serde(rename = "precedingToolUseIds")]
    pub preceding_tool_use_ids: Vec<String>,
    pub uuid: String,
    pub timestamp: String,
}

/// Construct a serializable stream message with a fresh UUID and UTC timestamp.
pub fn create_tool_use_summary_message(
    summary: impl Into<String>,
    preceding_tool_use_ids: impl IntoIterator<Item = impl Into<String>>,
) -> ToolUseSummaryMessage {
    ToolUseSummaryMessage {
        message_type: "tool_use_summary",
        summary: summary.into(),
        preceding_tool_use_ids: preceding_tool_use_ids.into_iter().map(Into::into).collect(),
        uuid: Uuid::new_v4().to_string(),
        timestamp: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
    }
}

/// One completed tool call included in the model prompt.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolInfo {
    pub name: String,
    /// `None` models JavaScript `undefined`; JSON `null` is `Some(Value::Null)`.
    pub input: Option<Value>,
    /// `None` models JavaScript `undefined`; JSON `null` is `Some(Value::Null)`.
    pub output: Option<Value>,
}

impl ToolInfo {
    pub fn new(name: impl Into<String>, input: Option<Value>, output: Option<Value>) -> Self {
        Self {
            name: name.into(),
            input,
            output,
        }
    }
}

/// Provider-neutral request for the tool summary query.
#[derive(Clone, Debug, PartialEq)]
pub struct ToolSummaryQueryRequest {
    pub purpose: &'static str,
    pub model: String,
    pub user_text: String,
    pub system_instruction: &'static str,
    pub max_output_tokens: u32,
    pub temperature: f32,
    pub max_attempts: u8,
}

/// Boxed future returned by an injected model-query implementation.
pub type ToolSummaryQueryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>>;

/// Supplies the configured fast model and performs one best-effort query.
///
/// Implementations should honor the cancellation token while doing provider
/// work. The summary service also drops the in-flight future when cancelled.
pub trait ToolSummaryModelQuery: Send + Sync {
    fn fast_model(&self) -> Option<&str>;

    /// Whether a usable model client is available. Defaults to true for
    /// adapters where client presence is implicit in the implementation.
    fn is_available(&self) -> bool {
        true
    }

    fn query<'a>(
        &'a self,
        request: ToolSummaryQueryRequest,
        cancellation: &'a CancellationToken,
    ) -> ToolSummaryQueryFuture<'a>;
}

/// Build the prompt, query the injected fast-model adapter once, and clean the
/// result. Errors, empty replies, and cancellation are intentionally non-fatal.
pub async fn generate_tool_use_summary(
    query: &impl ToolSummaryModelQuery,
    tools: &[ToolInfo],
    cancellation: &CancellationToken,
    last_assistant_text: Option<&str>,
) -> Option<String> {
    if tools.is_empty() {
        return None;
    }

    let model = query.fast_model()?.to_owned();
    if cancellation.is_cancelled() {
        return None;
    }

    let tool_summaries = tools
        .iter()
        .map(|tool| {
            let input = truncate_optional_json(tool.input.as_ref(), INPUT_TRUNCATE_LENGTH);
            let output = truncate_optional_json(tool.output.as_ref(), INPUT_TRUNCATE_LENGTH);
            format!("Tool: {}\nInput: {input}\nOutput: {output}", tool.name)
        })
        .collect::<Vec<_>>()
        .join("\n\n");

    let context_prefix = last_assistant_text
        .filter(|text| !text.is_empty())
        .map(|text| {
            format!(
                "User's intent (from assistant's last message): {}\n\n",
                truncate_utf16(text, LAST_ASSISTANT_TEXT_LENGTH)
            )
        })
        .unwrap_or_default();
    let user_text = format!("{context_prefix}Tools completed:\n\n{tool_summaries}\n\nLabel:");

    if !query.is_available() || cancellation.is_cancelled() {
        return None;
    }

    let request = ToolSummaryQueryRequest {
        purpose: "tool-use-summary",
        model,
        user_text,
        system_instruction: TOOL_USE_SUMMARY_SYSTEM_PROMPT,
        max_output_tokens: 60,
        temperature: 0.3,
        max_attempts: 1,
    };

    let result = tokio::select! {
        _ = cancellation.cancelled() => return None,
        response = query.query(request, cancellation) => response,
    };
    if cancellation.is_cancelled() {
        return None;
    }

    let raw = result.ok().flatten()?;
    if raw.is_empty() {
        return None;
    }
    let cleaned = clean_summary(&raw);
    (!cleaned.is_empty()).then_some(cleaned)
}

/// Serialize a JSON value after slicing string leaves above depth four.
/// `None` is the source's JavaScript `[undefined]` marker.
pub fn truncate_optional_json(value: Option<&Value>, max_length: usize) -> String {
    let Some(value) = value else {
        return "[undefined]".to_owned();
    };
    truncate_json(value, max_length)
}

/// Truncate JSON for model input without first serializing large shallow
/// string fields in full. Strings are pre-truncated before serde sees them;
/// recursion stops at depth four to match the TypeScript source behavior.
pub fn truncate_json(value: &Value, max_length: usize) -> String {
    let serialized = match serde_json::to_string(&PreTruncated {
        value,
        max_length,
        depth: 0,
    }) {
        Ok(serialized) => serialized,
        Err(_) => return "[unable to serialize]".to_owned(),
    };

    if utf16_len(&serialized) <= max_length {
        serialized
    } else {
        let prefix = truncate_utf16(&serialized, max_length.saturating_sub(3));
        format!("{prefix}...")
    }
}

struct PreTruncated<'a> {
    value: &'a Value,
    max_length: usize,
    depth: usize,
}

impl Serialize for PreTruncated<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.depth >= 4 {
            return self.value.serialize(serializer);
        }

        match self.value {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(value) => serializer.serialize_bool(*value),
            Value::Number(value) => value.serialize(serializer),
            Value::String(value) => serializer.serialize_str(&truncate_utf16(
                value,
                self.max_length.min(utf16_len(value)),
            )),
            Value::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(&PreTruncated {
                        value,
                        max_length: self.max_length,
                        depth: self.depth + 1,
                    })?;
                }
                sequence.end()
            }
            Value::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(
                        key,
                        &PreTruncated {
                            value,
                            max_length: self.max_length,
                            depth: self.depth + 1,
                        },
                    )?;
                }
                map.end()
            }
        }
    }
}

/// Strip markdown and common prefix noise, reject refusal/error responses, and
/// cap the result at 100 UTF-16 code units to follow JavaScript string limits.
pub fn clean_summary(raw: &str) -> String {
    let mut text = raw.split('\n').next().unwrap_or_default().trim().to_owned();

    if let Some(first) = text.chars().next()
        && matches!(first, '-' | '*' | '•')
    {
        let after_first = &text[first.len_utf8()..];
        if after_first.chars().next().is_some_and(char::is_whitespace) {
            text = after_first.trim_start().to_owned();
        }
    }

    let leading_markers = text
        .chars()
        .take_while(|ch| matches!(ch, '*' | '_'))
        .take(3)
        .map(char::len_utf8)
        .sum::<usize>();
    text.replace_range(..leading_markers, "");
    let trailing_markers = text
        .chars()
        .rev()
        .take_while(|ch| matches!(ch, '*' | '_'))
        .take(3)
        .map(char::len_utf8)
        .sum::<usize>();
    let keep_to = text.len().saturating_sub(trailing_markers);
    text.truncate(keep_to);
    text = text.trim().to_owned();

    let leading_quotes = quote_run_prefix_bytes(&text);
    text.replace_range(..leading_quotes, "");
    let trailing_quotes = quote_run_suffix_bytes(&text);
    text.truncate(text.len().saturating_sub(trailing_quotes));
    text = text.trim().to_owned();

    text = strip_label_prefix(text);
    if text.is_empty() || is_refusal_or_error(&text) {
        return String::new();
    }

    truncate_utf16(&text, MAX_SUMMARY_LENGTH).trim().to_owned()
}

fn strip_label_prefix(text: String) -> String {
    for prefix in ["label", "summary", "result", "output"] {
        if !text
            .get(..prefix.len())
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
        {
            continue;
        }
        let Some(remainder) = text.get(prefix.len()..) else {
            continue;
        };
        let remainder = remainder.trim_start();
        if let Some(after_colon) = remainder.strip_prefix(':') {
            return after_colon.trim_start().to_owned();
        }
        if let Some(after_colon) = remainder.strip_prefix('：') {
            return after_colon.trim_start().to_owned();
        }
    }
    text.trim().to_owned()
}

fn is_refusal_or_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    if starts_with_word(&lower, "api error")
        || lower.starts_with("error:")
        || lower.starts_with("error：")
        || starts_with_word(&lower, "unable to")
        || starts_with_word(&lower, "failed to")
        || starts_with_word(&lower, "request failed")
        || lower.starts_with("sorry,")
        || text.starts_with("抱歉,")
        || text.starts_with("抱歉，")
        || text.starts_with("抱歉")
        || text.starts_with("我无法")
        || text.starts_with("我不能")
        || text.starts_with("无法")
    {
        return true;
    }

    if let Some(rest) = lower.strip_prefix("i can") {
        for ending in ["'t", "’t", "not"] {
            if rest.strip_prefix(ending).is_some_and(word_boundary_after) {
                return true;
            }
        }
    }

    false
}

fn starts_with_word(text: &str, prefix: &str) -> bool {
    text.strip_prefix(prefix).is_some_and(word_boundary_after)
}

fn word_boundary_after(rest: &str) -> bool {
    rest.chars()
        .next()
        .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
}

fn quote_run_prefix_bytes(text: &str) -> usize {
    text.char_indices()
        .take_while(|(_, ch)| is_quote(*ch))
        .take(10)
        .map(|(index, ch)| index + ch.len_utf8())
        .last()
        .unwrap_or(0)
}

fn quote_run_suffix_bytes(text: &str) -> usize {
    text.char_indices()
        .rev()
        .take_while(|(_, ch)| is_quote(*ch))
        .take(10)
        .map(|(index, _)| text.len() - index)
        .last()
        .unwrap_or(0)
}

fn is_quote(ch: char) -> bool {
    matches!(
        ch,
        '"' | '\'' | '`' | '‘' | '’' | '“' | '”' | '「' | '」' | '『' | '』'
    )
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

/// Return a prefix no longer than `max_units` UTF-16 units. Rust strings
/// cannot contain JavaScript's lone surrogate values, so a cut between a
/// surrogate pair omits that scalar instead of creating invalid text.
fn truncate_utf16(value: &str, max_units: usize) -> String {
    let mut units = 0;
    let mut end = 0;
    for (index, ch) in value.char_indices() {
        let width = ch.len_utf16();
        if units + width > max_units {
            break;
        }
        units += width;
        end = index + ch.len_utf8();
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct FakeQuery {
        model: Option<String>,
        available: bool,
        response: Result<Option<String>, String>,
        seen: Mutex<Vec<ToolSummaryQueryRequest>>,
    }

    impl FakeQuery {
        fn new(model: Option<&str>, response: Result<Option<&str>, &str>) -> Self {
            Self {
                model: model.map(str::to_owned),
                available: true,
                response: response
                    .map(|value| value.map(str::to_owned))
                    .map_err(str::to_owned),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl ToolSummaryModelQuery for FakeQuery {
        fn fast_model(&self) -> Option<&str> {
            self.model.as_deref()
        }

        fn is_available(&self) -> bool {
            self.available
        }

        fn query<'a>(
            &'a self,
            request: ToolSummaryQueryRequest,
            _cancellation: &'a CancellationToken,
        ) -> ToolSummaryQueryFuture<'a> {
            self.seen.lock().unwrap().push(request);
            let response = self.response.clone();
            Box::pin(async move { response })
        }
    }

    fn tool(name: &str, input: Value, output: Value) -> ToolInfo {
        ToolInfo::new(name, Some(input), Some(output))
    }

    #[test]
    fn truncate_json_serializes_short_values() {
        assert_eq!(
            truncate_json(&serde_json::json!({"foo":"bar"}), 100),
            r#"{"foo":"bar"}"#
        );
        assert_eq!(
            truncate_json(&serde_json::json!("hello"), 100),
            r#""hello""#
        );
        assert_eq!(truncate_json(&serde_json::json!(42), 100), "42");
        assert_eq!(truncate_optional_json(None, 100), "[undefined]");
    }

    #[test]
    fn truncate_json_caps_long_strings_with_ellipsis() {
        let result = truncate_json(&Value::String("x".repeat(500)), 50);
        assert_eq!(result.len(), 50);
        assert!(result.ends_with("..."));
    }

    #[test]
    fn truncate_json_pretruncates_large_top_level_and_shallow_fields() {
        let result = truncate_json(&Value::String("x".repeat(10_000_000)), 300);
        assert!(result.len() <= 300);
        assert!(result.ends_with("..."));

        let result = truncate_json(&serde_json::json!({"content": "y".repeat(10_000_000)}), 300);
        assert!(result.len() <= 300);
        assert!(result.matches('y').count() < 300);
    }

    #[test]
    fn truncate_json_stops_pretruncating_after_depth_four() {
        let nested = serde_json::json!({"a":{"b":{"c":{"d":{"text":"z".repeat(20)}}}}});
        let result = truncate_json(&nested, 1000);
        assert!(result.contains(&"z".repeat(20)));
    }

    #[test]
    fn clean_summary_preserves_labels_and_takes_only_first_line() {
        assert_eq!(clean_summary("Searched in auth/"), "Searched in auth/");
        assert_eq!(
            clean_summary("Fixed NPE in UserService"),
            "Fixed NPE in UserService"
        );
        assert_eq!(
            clean_summary("Created signup endpoint\nSome reasoning"),
            "Created signup endpoint"
        );
    }

    #[test]
    fn clean_summary_strips_quotes_bullets_and_prefixes() {
        for (input, expected) in [
            (r#""Read config.json""#, "Read config.json"),
            ("'Ran failing tests'", "Ran failing tests"),
            ("‘‘‘Read a file’’’", "Read a file"),
            ("`Fixed bug`", "Fixed bug"),
            ("- Searched auth", "Searched auth"),
            ("* Read files", "Read files"),
            ("• Fixed NPE", "Fixed NPE"),
            ("Label: Fixed bug", "Fixed bug"),
            ("Summary: Ran tests", "Ran tests"),
            ("Label:Searched files", "Searched files"),
            ("**Read 4 files**", "Read 4 files"),
            ("_Searched auth_", "Searched auth"),
            ("__Fixed NPE__", "Fixed NPE"),
        ] {
            assert_eq!(clean_summary(input), expected, "input: {input}");
        }
    }

    #[test]
    fn clean_summary_strips_unicode_quotes_and_preserves_cjk_labels() {
        assert_eq!(clean_summary("“Read config.json”"), "Read config.json");
        assert_eq!(clean_summary("‘Ran tests’"), "Ran tests");
        assert_eq!(clean_summary("「搜索了 auth 模块」"), "搜索了 auth 模块");
        assert_eq!(clean_summary("『Fixed bug』"), "Fixed bug");
        assert_eq!(clean_summary("搜索了 auth 模块"), "搜索了 auth 模块");
        assert_eq!(clean_summary("抱歉，我不能帮助"), "");
    }

    #[test]
    fn clean_summary_rejects_refusals_and_errors() {
        for response in [
            "API error: 500",
            "Error: something went wrong",
            "I cannot generate a summary",
            "I can't help with that",
            "I can’t generate that",
            "Unable to determine",
            "Failed to read files",
            "Sorry, I cannot",
            "Request failed",
            "我无法生成摘要",
            "我不能回答这个",
            "抱歉，我不能帮助",
            "无法确定",
            "无法完成",
        ] {
            assert_eq!(clean_summary(response), "", "response: {response}");
        }
    }

    #[test]
    fn clean_summary_caps_length_at_100_and_handles_empty_input() {
        assert_eq!(clean_summary(&"x".repeat(200)).len(), 100);
        assert_eq!(clean_summary(""), "");
        assert_eq!(clean_summary("   "), "");
        assert_eq!(clean_summary("\n\n"), "");
    }

    #[test]
    fn message_has_expected_sdk_json_fields() {
        let message = create_tool_use_summary_message("Fixed bug", ["call-1", "call-2"]);
        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(value["type"], "tool_use_summary");
        assert_eq!(value["summary"], "Fixed bug");
        assert_eq!(
            value["precedingToolUseIds"],
            serde_json::json!(["call-1", "call-2"])
        );
        assert_eq!(message.uuid.len(), 36);
        assert!(message.timestamp.starts_with("20"));

        let other = create_tool_use_summary_message("a", std::iter::empty::<&str>());
        assert_ne!(message.uuid, other.uuid);
    }

    #[tokio::test]
    async fn generate_skips_empty_tools_missing_model_and_cancelled_calls() {
        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("label")));
        let token = CancellationToken::new();
        assert_eq!(
            generate_tool_use_summary(&query, &[], &token, None).await,
            None
        );

        let no_model = FakeQuery::new(None, Ok(Some("label")));
        assert_eq!(
            generate_tool_use_summary(
                &no_model,
                &[tool("Read", Value::Null, Value::Null)],
                &token,
                None
            )
            .await,
            None
        );

        token.cancel();
        assert_eq!(
            generate_tool_use_summary(
                &query,
                &[tool("Read", Value::Null, Value::Null)],
                &token,
                None
            )
            .await,
            None
        );
        assert!(query.seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn generate_builds_prompt_and_uses_model_options() {
        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("Searched in auth/")));
        let token = CancellationToken::new();
        let result = generate_tool_use_summary(
            &query,
            &[tool(
                "Grep",
                serde_json::json!({"pattern":"login"}),
                serde_json::json!("3 matches"),
            )],
            &token,
            None,
        )
        .await;
        assert_eq!(result.as_deref(), Some("Searched in auth/"));

        let seen = query.seen.lock().unwrap();
        let request = &seen[0];
        assert_eq!(request.purpose, "tool-use-summary");
        assert_eq!(request.model, "canopy-fast");
        assert_eq!(request.system_instruction, TOOL_USE_SUMMARY_SYSTEM_PROMPT);
        assert_eq!(request.max_output_tokens, 60);
        assert_eq!(request.temperature, 0.3);
        assert_eq!(request.max_attempts, 1);
        assert!(request.user_text.contains("Tool: Grep"));
        assert!(request.user_text.contains(r#""pattern":"login""#));
        assert!(request.user_text.contains("3 matches"));
        assert!(request.user_text.ends_with("Label:"));
    }

    #[tokio::test]
    async fn generate_includes_and_truncates_last_assistant_text() {
        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("Fixed auth bug")));
        let token = CancellationToken::new();
        generate_tool_use_summary(
            &query,
            &[tool("Edit", Value::Null, Value::Null)],
            &token,
            Some("I will now fix the authentication bug in the login flow."),
        )
        .await;
        {
            let seen = query.seen.lock().unwrap();
            assert!(
                seen[0]
                    .user_text
                    .contains("User's intent (from assistant's last message):")
            );
            assert!(seen[0].user_text.contains("fix the authentication bug"));
        }
        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("Done")));
        generate_tool_use_summary(
            &query,
            &[tool("Edit", Value::Null, Value::Null)],
            &token,
            Some(&"A".repeat(500)),
        )
        .await;
        let seen = query.seen.lock().unwrap();
        assert!(seen[0].user_text.contains(&"A".repeat(200)));
        assert!(!seen[0].user_text.contains(&"A".repeat(201)));
    }

    #[tokio::test]
    async fn generate_handles_empty_failure_unavailable_and_long_tool_data() {
        let token = CancellationToken::new();
        for response in [
            Ok(None),
            Ok(Some(String::new())),
            Err("API error".to_owned()),
        ] {
            let query = FakeQuery {
                model: Some("canopy-fast".into()),
                available: true,
                response,
                seen: Mutex::new(Vec::new()),
            };
            assert_eq!(
                generate_tool_use_summary(
                    &query,
                    &[tool("Read", Value::Null, Value::Null)],
                    &token,
                    None
                )
                .await,
                None
            );
        }

        let query = FakeQuery {
            model: Some("canopy-fast".into()),
            available: false,
            response: Ok(Some("unused".into())),
            seen: Mutex::new(Vec::new()),
        };
        assert_eq!(
            generate_tool_use_summary(
                &query,
                &[tool("Read", Value::Null, Value::Null)],
                &token,
                None
            )
            .await,
            None
        );
        assert!(query.seen.lock().unwrap().is_empty());

        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("Read file")));
        generate_tool_use_summary(
            &query,
            &[tool(
                "Read",
                serde_json::json!({"content":"x".repeat(10_000)}),
                Value::String("y".repeat(10_000)),
            )],
            &token,
            None,
        )
        .await;
        let seen = query.seen.lock().unwrap();
        assert!(!seen[0].user_text.contains(&"x".repeat(500)));
        assert!(!seen[0].user_text.contains(&"y".repeat(500)));
        assert!(seen[0].user_text.contains("..."));
    }

    #[tokio::test]
    async fn generate_cleans_markdown_output() {
        let query = FakeQuery::new(Some("canopy-fast"), Ok(Some("- \"Searched auth/\"")));
        let result = generate_tool_use_summary(
            &query,
            &[tool("Grep", Value::Null, Value::Null)],
            &CancellationToken::new(),
            None,
        )
        .await;
        assert_eq!(result.as_deref(), Some("Searched auth/"));
    }
}
