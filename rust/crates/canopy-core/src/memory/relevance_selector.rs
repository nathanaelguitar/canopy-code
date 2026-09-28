//! Model-driven auto-memory selection.
//!
//! Port of `packages/core/src/memory/relevanceSelector.ts`. The application
//! supplies the side-query implementation and decides which configured model
//! to use; this module defines the request, the fixed 30-second safety ceiling,
//! response validation, and result ordering. The query resolver in `recall`
//! owns fallback selection, exclusions, telemetry, and prompt assembly.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{Value, json};

use super::recall::MAX_RELEVANT_DOCS;
use super::scan::ScannedAutoMemoryDocument;
use crate::utils::cancellation::{
    CancellationReason, CancellationToken, combine_cancellation_tokens,
};

pub const AUTO_MEMORY_RECALL_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Exact system instruction used by the TypeScript selector.
pub const AUTO_MEMORY_RECALL_SYSTEM_INSTRUCTION: &str = "You are selecting memories that will be useful to an AI coding assistant as it processes a user's query. You will be given the user's query and a list of available memory files with their filenames and descriptions.\n\nReturn a list of filenames for the memories that will clearly be useful to the assistant as it processes the user's query (up to 5). Only include memories that you are certain will be helpful based on their name and description.\n- If you are unsure if a memory will be useful in processing the user's query, then do not include it in your list. Be selective and discerning.\n- If there are no memories in the list that would clearly be useful, feel free to return an empty list.\n- If a list of recently-used tools is provided, do not select memories that are usage reference, API documentation, parameter schemas, field mappings, guessed call formats, or failed-call transcripts for those tools. Live tool definitions are the source of truth. Do still select durable operational context that cannot be obtained from the live schema, such as credentials location, ownership, external escalation paths, known gotchas, warnings, or confirmed workarounds.";

/// JSON content supplied to the injected side-query client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoMemoryRecallContent {
    pub role: String,
    pub text: String,
}

/// Request contract for application-provided side-query clients.
///
/// No model name is included: the source delegates fast-model/main-model
/// selection to `runSideQuery`.
#[derive(Clone, Debug, PartialEq)]
pub struct AutoMemoryRecallRequest {
    pub purpose: String,
    pub contents: Vec<AutoMemoryRecallContent>,
    pub schema: Value,
    pub skip_output_language_preference: bool,
    pub system_instruction: String,
    pub temperature: f64,
    pub deadline: Duration,
}

/// Async seam for the configured application's JSON side-query client.
///
/// The caller races this future against the combined caller/deadline token,
/// while implementations can also observe that token to stop their own work.
pub trait AutoMemoryRecallSelector: Send + Sync {
    fn select_json<'a>(
        &'a self,
        request: &'a AutoMemoryRecallRequest,
        cancellation: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AutoMemoryRecallSelectionError {
    InvalidTimestamp,
    CallerCancelled(Option<CancellationReason>),
    DeadlineExceeded,
    SideQuery(String),
    InvalidResponse(String),
}

impl std::fmt::Display for AutoMemoryRecallSelectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTimestamp => {
                formatter.write_str("Recall selector encountered an invalid memory timestamp")
            }
            Self::CallerCancelled(Some(CancellationReason::Explicit(reason))) => {
                formatter.write_str(reason)
            }
            Self::CallerCancelled(Some(CancellationReason::Timeout)) => {
                formatter.write_str("operation timed out")
            }
            Self::CallerCancelled(None) => formatter.write_str("operation cancelled"),
            Self::DeadlineExceeded => {
                formatter.write_str("Recall selector timed out after 30 seconds")
            }
            Self::SideQuery(error) => formatter.write_str(error),
            Self::InvalidResponse(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for AutoMemoryRecallSelectionError {}

/// Build the selector request. The manifest uses each absolute `file_path` as
/// its identifier, so same-named relative paths in separate memory scopes do
/// not collapse. Document bodies are intentionally omitted.
pub fn build_auto_memory_recall_request(
    query: &str,
    docs: &[ScannedAutoMemoryDocument],
    recent_tools: &[String],
) -> Result<AutoMemoryRecallRequest, AutoMemoryRecallSelectionError> {
    let manifest = format_memory_manifest(docs)?;
    let tools_section = if recent_tools.is_empty() {
        String::new()
    } else {
        format!("\n\nRecently used tools: {}", recent_tools.join(", "))
    };
    Ok(AutoMemoryRecallRequest {
        purpose: "auto-memory-recall".to_owned(),
        contents: vec![AutoMemoryRecallContent {
            role: "user".to_owned(),
            text: format!(
                "Query: {}\n\nAvailable memories:\n{manifest}{tools_section}",
                trim_js_whitespace(query)
            ),
        }],
        schema: recall_selection_response_schema(),
        skip_output_language_preference: true,
        system_instruction: AUTO_MEMORY_RECALL_SYSTEM_INSTRUCTION.to_owned(),
        temperature: 0.0,
        deadline: AUTO_MEMORY_RECALL_SELECTION_TIMEOUT,
    })
}

/// Ask the injected model selector to choose useful memories.
///
/// Empty inputs, whitespace-only queries, and nonpositive limits short-circuit
/// without invoking the selector. The model may return at most five items even
/// when the caller's requested limit is larger, matching the system policy.
pub async fn select_relevant_auto_memory_documents_by_model<'a>(
    query: &str,
    docs: &'a [ScannedAutoMemoryDocument],
    limit: i64,
    recent_tools: &[String],
    selector: &dyn AutoMemoryRecallSelector,
    caller_cancellation: Option<&CancellationToken>,
) -> Result<Vec<&'a ScannedAutoMemoryDocument>, AutoMemoryRecallSelectionError> {
    if docs.is_empty() || limit <= 0 || trim_js_whitespace(query).is_empty() {
        return Ok(Vec::new());
    }

    select_with_deadline(
        query,
        docs,
        limit,
        recent_tools,
        selector,
        caller_cancellation,
        AUTO_MEMORY_RECALL_SELECTION_TIMEOUT,
    )
    .await
}

async fn select_with_deadline<'a>(
    query: &str,
    docs: &'a [ScannedAutoMemoryDocument],
    limit: i64,
    recent_tools: &[String],
    selector: &dyn AutoMemoryRecallSelector,
    caller_cancellation: Option<&CancellationToken>,
    deadline: Duration,
) -> Result<Vec<&'a ScannedAutoMemoryDocument>, AutoMemoryRecallSelectionError> {
    let mut request = build_auto_memory_recall_request(query, docs, recent_tools)?;
    request.deadline = deadline;

    let effective_limit = limit.min(MAX_RELEVANT_DOCS as i64);
    let by_file_path = docs
        .iter()
        .map(|doc| (doc.file_path.to_string_lossy().into_owned(), doc))
        .collect::<HashMap<_, _>>();

    let timed = combine_cancellation_tokens([caller_cancellation], Some(deadline));
    let query_result = tokio::select! {
        biased;
        _ = timed.token.cancelled() => {
            if let Some(caller) = caller_cancellation.filter(|token| token.is_cancelled()) {
                return Err(AutoMemoryRecallSelectionError::CallerCancelled(caller.reason()));
            }
            return Err(AutoMemoryRecallSelectionError::DeadlineExceeded);
        }
        result = selector.select_json(&request, &timed.token) => result,
    };
    drop(timed);

    if let Some(caller) = caller_cancellation.filter(|token| token.is_cancelled()) {
        return Err(AutoMemoryRecallSelectionError::CallerCancelled(
            caller.reason(),
        ));
    }
    let response = query_result.map_err(AutoMemoryRecallSelectionError::SideQuery)?;
    let selected_paths = response
        .get("selected_memories")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AutoMemoryRecallSelectionError::InvalidResponse(
                "Recall selector must return selected_memories array".to_owned(),
            )
        })?;
    if selected_paths.len() > effective_limit as usize {
        return Err(AutoMemoryRecallSelectionError::InvalidResponse(format!(
            "Recall selector returned too many documents: {}",
            selected_paths.len()
        )));
    }

    let mut selected_docs = Vec::with_capacity(selected_paths.len());
    for selected_path in selected_paths {
        let Some(selected_path) = selected_path.as_str() else {
            return Err(AutoMemoryRecallSelectionError::InvalidResponse(
                "Recall selector returned unknown file path".to_owned(),
            ));
        };
        let Some(doc) = by_file_path.get(selected_path) else {
            return Err(AutoMemoryRecallSelectionError::InvalidResponse(
                "Recall selector returned unknown file path".to_owned(),
            ));
        };
        selected_docs.push(*doc);
    }
    selected_docs.truncate(effective_limit as usize);
    Ok(selected_docs)
}

fn format_memory_manifest(
    docs: &[ScannedAutoMemoryDocument],
) -> Result<String, AutoMemoryRecallSelectionError> {
    docs.iter()
        .map(|doc| {
            let timestamp = iso_timestamp(doc.mtime_ms)?;
            let description = if doc.description.is_empty() {
                String::new()
            } else {
                format!(": {}", doc.description)
            };
            Ok(format!(
                "- [{}] {} ({timestamp}){description}",
                doc.memory_type.as_str(),
                doc.file_path.to_string_lossy()
            ))
        })
        .collect::<Result<Vec<_>, AutoMemoryRecallSelectionError>>()
        .map(|lines| lines.join("\n"))
}

fn iso_timestamp(milliseconds: f64) -> Result<String, AutoMemoryRecallSelectionError> {
    // JavaScript Date applies TimeClip: truncate fractional milliseconds toward
    // zero, then reject values outside the ±8.64e15 ms range.
    if !milliseconds.is_finite() || milliseconds.abs() > 8_640_000_000_000_000.0 {
        return Err(AutoMemoryRecallSelectionError::InvalidTimestamp);
    }
    let clipped = milliseconds.trunc() as i64;
    let days = clipped.div_euclid(86_400_000);
    let milliseconds_in_day = clipped.rem_euclid(86_400_000);
    let hours = milliseconds_in_day / 3_600_000;
    let minutes = milliseconds_in_day % 3_600_000 / 60_000;
    let seconds = milliseconds_in_day % 60_000 / 1_000;
    let millis = milliseconds_in_day % 1_000;
    let (year, month, day) = civil_from_days(days);
    let year = if (0..=9_999).contains(&year) {
        format!("{year:04}")
    } else if year > 9_999 {
        format!("+{year:06}")
    } else {
        format!("-{magnitude:06}", magnitude = -year)
    };
    Ok(format!(
        "{year}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z"
    ))
}

/// Proleptic Gregorian civil date for a day offset from 1970-01-01. This
/// supports JavaScript Date's full ±8.64e15 ms range, beyond chrono's year
/// range, and its ISO year formatting can then match `Date#toISOString`.
fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let days = days_since_epoch + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn recall_selection_response_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "selected_memories": {
                "type": "array",
                "items": { "type": "string" }
            }
        },
        "required": ["selected_memories"],
        "additionalProperties": false
    })
}

fn trim_js_whitespace(value: &str) -> &str {
    value.trim_matches(is_js_whitespace)
}

fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::store::AutoMemoryType;
    use std::sync::{Arc, Mutex};

    fn doc(
        path: &str,
        relative_path: &str,
        description: &str,
        body: &str,
    ) -> ScannedAutoMemoryDocument {
        ScannedAutoMemoryDocument {
            memory_type: AutoMemoryType::User,
            file_path: path.into(),
            relative_path: relative_path.to_owned(),
            filename: path.rsplit('/').next().unwrap_or(path).to_owned(),
            title: "Memory title".to_owned(),
            description: description.to_owned(),
            body: body.to_owned(),
            mtime_ms: 1_700_000_000_123.0,
        }
    }

    #[derive(Default)]
    struct FixedSelector {
        response: Mutex<Option<Value>>,
        request: Mutex<Option<AutoMemoryRecallRequest>>,
    }

    impl FixedSelector {
        fn new(response: Value) -> Self {
            Self {
                response: Mutex::new(Some(response)),
                request: Mutex::new(None),
            }
        }
    }

    impl AutoMemoryRecallSelector for FixedSelector {
        fn select_json<'a>(
            &'a self,
            request: &'a AutoMemoryRecallRequest,
            _cancellation: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            *self.request.lock().unwrap() = Some(request.clone());
            let response = self.response.lock().unwrap().take().unwrap();
            Box::pin(async move { Ok(response) })
        }
    }

    struct PendingSelector;

    impl AutoMemoryRecallSelector for PendingSelector {
        fn select_json<'a>(
            &'a self,
            _request: &'a AutoMemoryRecallRequest,
            cancellation: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move {
                cancellation.cancelled().await;
                Err("cancelled".to_owned())
            })
        }
    }

    struct CancelCallerSelector {
        caller: CancellationToken,
    }

    impl AutoMemoryRecallSelector for CancelCallerSelector {
        fn select_json<'a>(
            &'a self,
            _request: &'a AutoMemoryRecallRequest,
            cancellation: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
            Box::pin(async move {
                self.caller.cancel_with_reason("caller stopped");
                cancellation.cancelled().await;
                Ok(json!({ "selected_memories": [] }))
            })
        }
    }

    #[tokio::test]
    async fn request_shape_uses_header_only_manifest_and_recent_tool_suffix() {
        let docs = [doc(
            "/canopy/projects/p/memory/user/role.md",
            "user/role.md",
            "Project-scoped role",
            "PRIVATE BODY MUST NOT APPEAR",
        )];
        let selector = FixedSelector::new(json!({ "selected_memories": [] }));

        let result = select_relevant_auto_memory_documents_by_model(
            "  who is the user? \u{feff}",
            &docs,
            20,
            &["mcp__docs__search".to_owned()],
            &selector,
            None,
        )
        .await
        .unwrap();
        assert!(result.is_empty());

        let request = selector.request.lock().unwrap().clone().unwrap();
        assert_eq!(request.purpose, "auto-memory-recall");
        assert_eq!(request.deadline, Duration::from_secs(30));
        assert_eq!(request.temperature, 0.0);
        assert!(request.skip_output_language_preference);
        assert_eq!(
            request.system_instruction,
            AUTO_MEMORY_RECALL_SYSTEM_INSTRUCTION
        );
        assert_eq!(request.contents[0].role, "user");
        assert_eq!(
            request.contents[0].text,
            "Query: who is the user?\n\nAvailable memories:\n- [user] /canopy/projects/p/memory/user/role.md (2023-11-14T22:13:20.123Z): Project-scoped role\n\nRecently used tools: mcp__docs__search"
        );
        assert_eq!(
            request.schema,
            json!({
                "type": "object",
                "properties": {
                    "selected_memories": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["selected_memories"],
                "additionalProperties": false
            })
        );
        assert!(
            !request.contents[0]
                .text
                .contains("PRIVATE BODY MUST NOT APPEAR")
        );
    }

    #[tokio::test]
    async fn duplicate_relative_paths_in_different_scopes_remain_selectable() {
        let docs = [
            doc(
                "/canopy/projects/p/memory/user/role.md",
                "user/role.md",
                "Project note",
                "project body",
            ),
            doc(
                "/canopy/memories/user/role.md",
                "user/role.md",
                "User note",
                "user body",
            ),
        ];
        let selector = FixedSelector::new(json!({ "selected_memories": [
            "/canopy/memories/user/role.md",
            "/canopy/projects/p/memory/user/role.md"
        ] }));

        let selected = select_relevant_auto_memory_documents_by_model(
            "user role",
            &docs,
            5,
            &[],
            &selector,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            selected
                .iter()
                .map(|doc| doc.file_path.to_string_lossy().to_string())
                .collect::<Vec<_>>(),
            [
                "/canopy/memories/user/role.md",
                "/canopy/projects/p/memory/user/role.md"
            ]
        );
    }

    #[tokio::test]
    async fn preserves_model_order_and_applies_five_item_ceiling() {
        let docs = (0..6)
            .map(|index| {
                doc(
                    &format!("/memory/{index}.md"),
                    &format!("{index}.md"),
                    "",
                    "",
                )
            })
            .collect::<Vec<_>>();
        let selector = FixedSelector::new(json!({ "selected_memories": [
            "/memory/4.md", "/memory/1.md"
        ] }));
        let selected = select_relevant_auto_memory_documents_by_model(
            "select some",
            &docs,
            99,
            &[],
            &selector,
            None,
        )
        .await
        .unwrap();
        assert_eq!(selected[0].file_path.to_string_lossy(), "/memory/4.md");
        assert_eq!(selected[1].file_path.to_string_lossy(), "/memory/1.md");

        let too_many = FixedSelector::new(json!({ "selected_memories": [
            "/memory/0.md", "/memory/1.md", "/memory/2.md", "/memory/3.md", "/memory/4.md", "/memory/5.md"
        ] }));
        let error = select_relevant_auto_memory_documents_by_model(
            "select some",
            &docs,
            99,
            &[],
            &too_many,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            AutoMemoryRecallSelectionError::InvalidResponse(
                "Recall selector returned too many documents: 6".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn validates_response_array_count_and_exact_absolute_path_allowlist() {
        let docs = [doc("/memory/known.md", "known.md", "", "")];
        let missing_array = FixedSelector::new(json!({}));
        assert!(matches!(
            select_relevant_auto_memory_documents_by_model(
                "query", &docs, 1, &[], &missing_array, None
            )
            .await,
            Err(AutoMemoryRecallSelectionError::InvalidResponse(message))
                if message == "Recall selector must return selected_memories array"
        ));

        let relative_path = FixedSelector::new(json!({ "selected_memories": ["known.md"] }));
        assert!(matches!(
            select_relevant_auto_memory_documents_by_model(
                "query", &docs, 1, &[], &relative_path, None
            )
            .await,
            Err(AutoMemoryRecallSelectionError::InvalidResponse(message))
                if message == "Recall selector returned unknown file path"
        ));

        let wrong_type = FixedSelector::new(json!({ "selected_memories": [7] }));
        assert!(matches!(
            select_relevant_auto_memory_documents_by_model(
                "query", &docs, 1, &[], &wrong_type, None
            )
            .await,
            Err(AutoMemoryRecallSelectionError::InvalidResponse(message))
                if message == "Recall selector returned unknown file path"
        ));
    }

    #[tokio::test]
    async fn skips_selector_for_empty_inputs_and_nonpositive_limit() {
        let selector = FixedSelector::new(json!({ "selected_memories": [] }));
        let docs = [doc("/memory/known.md", "known.md", "", "")];
        assert!(
            select_relevant_auto_memory_documents_by_model(
                "  \u{feff} ",
                &docs,
                1,
                &[],
                &selector,
                None
            )
            .await
            .unwrap()
            .is_empty()
        );
        assert!(
            select_relevant_auto_memory_documents_by_model("query", &[], 1, &[], &selector, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            select_relevant_auto_memory_documents_by_model("query", &docs, 0, &[], &selector, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(selector.request.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn caller_cancellation_is_distinguished_from_the_deadline() {
        let docs = [doc("/memory/known.md", "known.md", "", "")];
        let caller = CancellationToken::new();
        let selector = CancelCallerSelector {
            caller: caller.clone(),
        };
        let error = select_relevant_auto_memory_documents_by_model(
            "query",
            &docs,
            1,
            &[],
            &selector,
            Some(&caller),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            AutoMemoryRecallSelectionError::CallerCancelled(Some(CancellationReason::Explicit(
                Arc::from("caller stopped")
            )))
        );
    }

    #[tokio::test]
    async fn deadline_cancels_a_stalled_side_query() {
        let docs = [doc("/memory/known.md", "known.md", "", "")];
        let error = select_with_deadline(
            "query",
            &docs,
            1,
            &[],
            &PendingSelector,
            None,
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();
        assert_eq!(error, AutoMemoryRecallSelectionError::DeadlineExceeded);
    }

    #[test]
    fn manifest_uses_javascript_date_time_clip() {
        assert_eq!(iso_timestamp(1.9).unwrap(), "1970-01-01T00:00:00.001Z");
        assert_eq!(iso_timestamp(-1.9).unwrap(), "1969-12-31T23:59:59.999Z");
        assert_eq!(iso_timestamp(-0.9).unwrap(), "1970-01-01T00:00:00.000Z");
        assert!(iso_timestamp(f64::NAN).is_err());
        assert!(iso_timestamp(8_640_000_000_000_001.0).is_err());
        assert_eq!(
            iso_timestamp(8_640_000_000_000_000.0).unwrap(),
            "+275760-09-13T00:00:00.000Z"
        );
        assert_eq!(
            iso_timestamp(-8_640_000_000_000_000.0).unwrap(),
            "-271821-04-20T00:00:00.000Z"
        );
    }
}
