// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//!
//! Post-fetch WebFetch result projection and best-effort side-query
//! summarization. The host supplies model preferences, cancellation, and the
//! provider-neutral side-query executor; this module does not construct a
//! provider or fetch a URL.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::json;
use thiserror::Error;
use tokio::time::Instant as TokioInstant;

use crate::token_limits::parse_positive_integer_env_value;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::tools::web::fetch_processing::FetchProcessedResponse;
use crate::tools::web::fetch_service::FetchContentFormat;
use crate::tools::web::{format_web_fetch_display, is_preapproved_url};
use crate::utils::cancellation::{CancellationReason, CancellationToken};
use crate::utils::side_query::{
    SideQueryError, SideQueryExecutor, SideQueryJsonValidator, SideQueryOptions, SideQueryResult,
    run_side_query,
};

const MAX_FETCH_CONTENT_UTF16_UNITS: usize = 100_000;
const DEFAULT_PROCESSING_TIMEOUT_MS: u64 = 300_000;
const MAX_TIMER_DELAY_MS: u64 = 2_147_483_647;
const PROCESSING_TIMEOUT_ENV: &str = "CANOPY_WEB_FETCH_PROCESSING_TIMEOUT_MS";
const SIDE_QUERY_SYSTEM_INSTRUCTION: &str = "Extract and summarize the requested information from the provided web content. Be concise and accurate. Respond only with the requested information.";
const EMPTY_SIDE_QUERY_RESULT: &str = "[The processing model returned no content. The fetch itself succeeded — see the metadata above.]";

const WEB_FETCH_DESCRIPTION: &str = r#"Fetches content from a specified URL and processes it using an AI model
- Takes a URL and a prompt as input
- Supports content negotiation for markdown (reduces tokens by ~80%)
- Fetches the URL content and converts HTML to markdown (links preserved)
- Processes the content with the prompt using an AI model
- Returns the model's response about the content, prefixed with fetch metadata (HTTP status, content type, size)
- Use this tool when you need to retrieve and analyze web content

Usage notes:
  - IMPORTANT: This tool cannot access authenticated or private URLs (e.g. Google Docs, Confluence, Jira, private GitHub). If an MCP-provided web fetch tool is available, prefer using that tool instead of this one, as it may have fewer restrictions. All MCP-provided tools start with "mcp__".
  - The URL must be a fully-formed valid URL
  - Plain-http URLs to public hosts are upgraded to https automatically; localhost/private hosts are fetched as-is
  - When a URL redirects to a different host, the redirect is NOT followed; the tool returns the redirect URL so you can re-issue web_fetch with it
  - Binary content (PDFs, images, archives) is saved to a local file; the result includes the file path — use read_file on it (it reads PDFs and images natively)
  - Repeated fetches of the same URL within 15 minutes are served from a local cache
  - The prompt should describe what information you want to extract from the page
  - format parameter (optional): controls only the Accept header sent to the server. All content is normalized to plain text for LLM processing, regardless of format.
  - "auto" (default): Prefers markdown via content negotiation, accepts HTML, text, or other content as fallback. Use when user does NOT specify a format.
  - "markdown": Prefers text/markdown. Use when user explicitly asks for markdown content.
  - "html": Prefers text/html. Content is still converted to markdown for LLM processing.
  - "text": Prefers text/plain. Use when user explicitly asks for plain text.
  - This tool does not modify any files (other than saving fetched binary content)
  - Results may be summarized if the content is very large
  - Supports both public and private/localhost URLs using direct fetch"#;

/// Input parameters exposed by the WebFetch function declaration. An omitted
/// format selects `FetchContentFormat::Auto` at the fetch-service boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebFetchParams {
    pub url: String,
    pub prompt: String,
    pub format: Option<FetchContentFormat>,
}

/// Provider-neutral function schema corresponding to the TypeScript
/// `WebFetchTool` declaration.
pub fn function_declaration() -> serde_json::Value {
    json!({
        "name": "web_fetch",
        "description": WEB_FETCH_DESCRIPTION,
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "url": {
                    "description": "The URL to fetch content from",
                    "type": "STRING"
                },
                "prompt": {
                    "description": "The prompt to run on the fetched content",
                    "type": "STRING"
                },
                "format": {
                    "description": "Preferred content format (Accept header only): auto (default, prefers markdown), markdown, html, or text. All content is normalized to plain text.",
                    "type": "STRING",
                    "enum": ["auto", "markdown", "html", "text"]
                }
            },
            "required": ["url", "prompt"]
        }
    })
}

/// Apply the TypeScript tool's custom parameter checks in source order. The
/// function schema handles field types and the `format` enum before this runs.
pub fn validate_web_fetch_params(params: &WebFetchParams) -> Result<(), &'static str> {
    crate::tools::web::validate_web_fetch_url(&params.url)?;
    if params.prompt.trim().is_empty() {
        return Err("The 'prompt' parameter cannot be empty.");
    }
    Ok(())
}

/// Resolve WebFetch's best-effort processing deadline from its deployment
/// override. Invalid, non-positive, or timer-overflow values use the 300s
/// default, matching `sideQueryTimeoutMs()` in the TypeScript tool.
pub fn side_query_timeout_ms_from_env(raw: Option<&str>) -> u64 {
    parse_positive_integer_env_value(raw)
        .filter(|timeout_ms| *timeout_ms <= MAX_TIMER_DELAY_MS)
        .unwrap_or(DEFAULT_PROCESSING_TIMEOUT_MS)
}

/// Current process-configured processing backstop.
pub fn configured_side_query_timeout() -> Duration {
    let raw = std::env::var(PROCESSING_TIMEOUT_ENV).ok();
    Duration::from_millis(side_query_timeout_ms_from_env(raw.as_deref()))
}

/// Host-supplied context needed to project one processed WebFetch response.
pub struct FetchInvocationOptions {
    pub user_prompt: String,
    pub cancellation: CancellationToken,
    pub fast_model: Option<String>,
    pub configured_model: Option<String>,
    pub output_language_file_path: Option<PathBuf>,
    /// Optional override for deterministic hosts/tests. Invalid values fall
    /// back to the bounded process-configured timeout.
    pub processing_timeout: Duration,
    /// Start of the whole WebFetch invocation, used in the visible summary.
    pub invocation_started_at: Instant,
}

impl FetchInvocationOptions {
    pub fn new(user_prompt: impl Into<String>, cancellation: CancellationToken) -> Self {
        Self {
            user_prompt: user_prompt.into(),
            cancellation,
            fast_model: None,
            configured_model: None,
            output_language_file_path: None,
            processing_timeout: configured_side_query_timeout(),
            invocation_started_at: Instant::now(),
        }
    }
}

/// Provider-neutral invocation output. The text shown to the user is also
/// placed in `tool_output.display`; file paths stay alongside it so the host
/// can propagate them through its tool-result metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchInvocationResult {
    pub tool_output: ToolExecutionOutput,
    pub visible_display: String,
    pub result_file_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum FetchInvocationError {
    #[error("WebFetch side query was cancelled")]
    Cancelled(Option<CancellationReason>),
}

/// Project one fetched response into model-facing content and a visible
/// summary. Text extraction, binary persistence, and metadata are already
/// supplied by [`FetchProcessedResponse`].
pub async fn invoke_fetch_response<E, V>(
    response: &FetchProcessedResponse,
    options: FetchInvocationOptions,
    executor: &E,
    json_validator: &V,
) -> Result<FetchInvocationResult, FetchInvocationError>
where
    E: SideQueryExecutor + ?Sized,
    V: SideQueryJsonValidator + ?Sized,
{
    let header = response.metadata_header();
    let binary_note = response.binary_note().unwrap_or_default();
    let persisted_path = response
        .persisted
        .as_ref()
        .map(|persisted| persisted.filepath.clone());
    let result_file_paths = persisted_path.into_iter().collect::<Vec<_>>();
    let persisted_path_text = result_file_paths
        .first()
        .map(|path| path.to_string_lossy().into_owned());
    let display_summary = format_web_fetch_display(
        response.byte_length,
        response.status,
        &response.status_text,
        &response.requested_url,
        persisted_path_text.as_deref(),
    );

    if !result_file_paths.is_empty() && response.content.is_empty() {
        return Ok(build_result(
            format!(
                "{header}\n\n[No text could be extracted from this binary content.]{binary_note}"
            ),
            format!(
                "{display_summary}{}",
                elapsed_suffix(options.invocation_started_at)
            ),
            result_file_paths,
        ));
    }

    if is_preapproved_url(&response.final_url)
        && response.content_type.contains("text/markdown")
        && utf16_units(&response.content) <= MAX_FETCH_CONTENT_UTF16_UNITS
    {
        return Ok(build_result(
            format!("{header}\n\n{}{binary_note}", response.content),
            format!(
                "{display_summary}{}",
                elapsed_suffix(options.invocation_started_at)
            ),
            result_file_paths,
        ));
    }

    let fallback_prompt = format!(
        "The user requested the following: \"{}\".\n\nI have fetched the content from {}. Fetch metadata:\n{}\n\nPlease use the following content to answer the user's request.\n\n---\n{}\n---",
        options.user_prompt, response.requested_url, header, response.content
    );
    let timeout = bounded_timeout(options.processing_timeout);
    let deadline = TokioInstant::now() + timeout;
    let cancellation = options.cancellation.clone();
    let side_query_options = SideQueryOptions {
        contents: vec![json!({
            "role": "user",
            "parts": [{"text": fallback_prompt}],
        })],
        schema: None,
        cancellation: cancellation.clone(),
        model_override: None,
        fast_model: options.fast_model,
        configured_model: options.configured_model,
        system_instruction: Some(json!(SIDE_QUERY_SYSTEM_INSTRUCTION)),
        prompt_id: None,
        purpose: Some("web-fetch".to_owned()),
        generation_config: None,
        max_attempts: Some(1),
        skip_output_language_preference: false,
        output_language_file_path: options.output_language_file_path,
        stream: Some(true),
        fail_closed: None,
        deadline: Some(deadline),
        validate_text: None,
        validate_json: None,
    };

    let query_result = run_side_query(executor, json_validator, side_query_options).await;
    // The source checks the caller signal again after awaiting the model so a
    // final streamed chunk cannot race cancellation and become visible.
    if cancellation.is_cancelled() {
        return Err(FetchInvocationError::Cancelled(cancellation.reason()));
    }

    let result_text = match query_result {
        Ok(SideQueryResult::Text(result)) => result.text.trim().to_owned(),
        Ok(_) => {
            return Ok(processing_failed_result(
                response,
                &header,
                &binary_note,
                &display_summary,
                &result_file_paths,
                &SideQueryError::UnexpectedResponseMode.to_string(),
                options.invocation_started_at,
            ));
        }
        Err(SideQueryError::Cancelled(reason)) => {
            return Err(FetchInvocationError::Cancelled(reason));
        }
        Err(error) => {
            return Ok(processing_failed_result(
                response,
                &header,
                &binary_note,
                &display_summary,
                &result_file_paths,
                &error.to_string(),
                options.invocation_started_at,
            ));
        }
    };
    let result_text = if result_text.is_empty() {
        EMPTY_SIDE_QUERY_RESULT
    } else {
        &result_text
    };

    Ok(build_result(
        format!("{header}\n\n{result_text}{binary_note}"),
        format!(
            "{display_summary}{}",
            elapsed_suffix(options.invocation_started_at)
        ),
        result_file_paths,
    ))
}

fn processing_failed_result(
    response: &FetchProcessedResponse,
    header: &str,
    binary_note: &str,
    display_summary: &str,
    result_file_paths: &[PathBuf],
    error: &str,
    invocation_started_at: Instant,
) -> FetchInvocationResult {
    build_result(
        format!(
            "{header}\n\n[Content processing failed ({error}). The raw fetched content follows.]\n\n{}{binary_note}",
            response.content
        ),
        format!(
            "{display_summary}{} — processing failed, raw content returned",
            elapsed_suffix(invocation_started_at)
        ),
        result_file_paths.to_vec(),
    )
}

fn build_result(
    llm_content: String,
    visible_display: String,
    result_file_paths: Vec<PathBuf>,
) -> FetchInvocationResult {
    let tool_output =
        ToolExecutionOutput::with_display(llm_content, json!({"displayText": visible_display}));
    FetchInvocationResult {
        tool_output,
        visible_display,
        result_file_paths,
    }
}

fn elapsed_suffix(started_at: Instant) -> String {
    format!(" in {:.1}s", started_at.elapsed().as_secs_f64())
}

fn utf16_units(value: &str) -> usize {
    value.encode_utf16().count()
}

fn bounded_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() || timeout.as_millis() > u128::from(MAX_TIMER_DELAY_MS) {
        configured_side_query_timeout()
    } else {
        timeout
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_PROCESSING_TIMEOUT_MS, FetchInvocationError, FetchInvocationOptions,
        FetchInvocationResult, MAX_TIMER_DELAY_MS, SIDE_QUERY_SYSTEM_INSTRUCTION,
        invoke_fetch_response, side_query_timeout_ms_from_env,
    };
    use crate::tool_response_finalizer::ToolExecutionOutput;
    use crate::tools::web::fetch_processing::{FetchProcessedResponse, PersistedFetchBinary};
    use crate::utils::binary_content::{ExtensionSource, SniffedFileKind};
    use crate::utils::cancellation::{CancellationReason, CancellationToken};
    use crate::utils::side_query::{
        SideQueryExecutor, SideQueryFuture, SideQueryMode, SideQueryRequest, SideQueryResponse,
        SideQueryTextResult,
    };
    use serde_json::{Value, json};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};
    use tokio::time::sleep;

    enum ExecutorBehavior {
        Return(SideQueryResponse),
        Fail(String),
        Pending,
        CancelCallerThenReturn {
            caller: CancellationToken,
            response: SideQueryResponse,
        },
    }

    struct TestExecutor {
        behavior: ExecutorBehavior,
        request: Mutex<Option<SideQueryRequest>>,
    }

    impl TestExecutor {
        fn returning(text: &str) -> Self {
            Self {
                behavior: ExecutorBehavior::Return(SideQueryResponse::Text(SideQueryTextResult {
                    text: text.to_owned(),
                    usage: None,
                })),
                request: Mutex::new(None),
            }
        }

        fn request(&self) -> SideQueryRequest {
            self.request
                .lock()
                .unwrap()
                .clone()
                .expect("side-query request captured")
        }
    }

    impl SideQueryExecutor for TestExecutor {
        fn execute<'a>(
            &'a self,
            request: SideQueryRequest,
            _cancellation: CancellationToken,
        ) -> SideQueryFuture<'a> {
            *self.request.lock().unwrap() = Some(request);
            match &self.behavior {
                ExecutorBehavior::Return(response) => {
                    let response = response.clone();
                    Box::pin(async move { Ok(response) })
                }
                ExecutorBehavior::Fail(error) => {
                    let error = error.clone();
                    Box::pin(async move { Err(error) })
                }
                ExecutorBehavior::Pending => Box::pin(std::future::pending()),
                ExecutorBehavior::CancelCallerThenReturn { caller, response } => {
                    let caller = caller.clone();
                    let response = response.clone();
                    Box::pin(async move {
                        caller.cancel_with_reason("caller stopped");
                        Ok(response)
                    })
                }
            }
        }
    }

    fn accept_schema(_: &Value, _: &mut Value) -> Option<String> {
        None
    }

    fn response(content: &str) -> FetchProcessedResponse {
        FetchProcessedResponse {
            requested_url: "https://example.test/page".to_owned(),
            final_url: "https://example.test/page".to_owned(),
            status: 200,
            status_text: "OK".to_owned(),
            content_type: "text/html".to_owned(),
            byte_length: content.len(),
            content: content.to_owned(),
            is_binary: false,
            persisted: None,
            pdf_extraction_error: None,
            html_conversion_error: None,
            sniffed_kind: SniffedFileKind {
                extension: "html".to_owned(),
                mime_type: "text/html".to_owned(),
                magic_matched: false,
                extension_source: ExtensionSource::Mime,
            },
        }
    }

    fn options(prompt: &str) -> FetchInvocationOptions {
        FetchInvocationOptions {
            user_prompt: prompt.to_owned(),
            cancellation: CancellationToken::new(),
            fast_model: Some("fast-model".to_owned()),
            configured_model: Some("main-model".to_owned()),
            output_language_file_path: None,
            processing_timeout: Duration::from_secs(5),
            invocation_started_at: Instant::now(),
        }
    }

    fn text_from_output(result: &FetchInvocationResult) -> &str {
        &result.tool_output.output
    }

    #[tokio::test]
    async fn returns_binary_without_text_before_calling_side_query() {
        let mut fetched = response("");
        fetched.is_binary = true;
        fetched.content_type = "application/zip".to_owned();
        fetched.persisted = Some(PersistedFetchBinary {
            filepath: PathBuf::from("/tmp/archive.zip"),
            size: 17,
            mime_type: "application/zip".to_owned(),
        });
        fetched.sniffed_kind.extension = "zip".to_owned();
        let executor = TestExecutor::returning("must not run");

        let result = invoke_fetch_response(&fetched, options("inspect"), &executor, &accept_schema)
            .await
            .unwrap();

        assert!(executor.request.lock().unwrap().is_none());
        assert!(text_from_output(&result).contains("No text could be extracted"));
        assert!(text_from_output(&result).contains("/tmp/archive.zip"));
        assert_eq!(
            result.result_file_paths,
            vec![PathBuf::from("/tmp/archive.zip")]
        );
        assert!(
            result
                .visible_display
                .contains("binary saved to /tmp/archive.zip")
        );
        assert_eq!(
            result.tool_output.display,
            Some(json!({"displayText":result.visible_display}))
        );
    }

    #[tokio::test]
    async fn trusted_final_https_markdown_is_returned_without_a_model_call() {
        let mut fetched = response("# Raw documentation\n\nVerbatim details.");
        fetched.requested_url = "https://untrusted.example/readme".to_owned();
        fetched.final_url = "https://docs.python.org/3/library/json.md".to_owned();
        fetched.content_type = "text/markdown; charset=utf-8".to_owned();
        let executor = TestExecutor::returning("must not run");

        let result =
            invoke_fetch_response(&fetched, options("summarize"), &executor, &accept_schema)
                .await
                .unwrap();

        assert!(executor.request.lock().unwrap().is_none());
        assert!(text_from_output(&result).contains("# Raw documentation"));
        assert!(text_from_output(&result).contains("Verbatim details."));
    }

    #[tokio::test]
    async fn fallback_prompt_and_side_query_policy_match_the_source() {
        let fetched = response("RAW ARTICLE TEXT");
        let executor = TestExecutor::returning("  concise answer  ");
        let result = invoke_fetch_response(
            &fetched,
            options("find the answer"),
            &executor,
            &accept_schema,
        )
        .await
        .unwrap();

        let request = executor.request();
        assert_eq!(request.model, "fast-model");
        assert_eq!(request.prompt_id, "side-query:web-fetch");
        assert_eq!(request.max_attempts, Some(1));
        assert_eq!(
            request.system_instruction,
            Some(json!(SIDE_QUERY_SYSTEM_INSTRUCTION))
        );
        assert!(matches!(
            request.mode,
            SideQueryMode::Text {
                stream: Some(true),
                ..
            }
        ));
        let prompt = request.contents[0]["parts"][0]["text"].as_str().unwrap();
        assert!(prompt.contains("The user requested the following: \"find the answer\"."));
        assert!(prompt.contains("I have fetched the content from https://example.test/page."));
        assert!(prompt.contains("Status: 200 OK"));
        assert!(prompt.contains("RAW ARTICLE TEXT"));
        assert!(text_from_output(&result).contains("concise answer"));
        assert!(!text_from_output(&result).contains("RAW ARTICLE TEXT"));
    }

    #[tokio::test]
    async fn side_query_failure_returns_metadata_raw_text_error_note_and_paths() {
        let mut fetched = response("EXTRACTED PDF TEXT");
        fetched.is_binary = true;
        fetched.content_type = "application/pdf".to_owned();
        fetched.persisted = Some(PersistedFetchBinary {
            filepath: PathBuf::from("/tmp/report.pdf"),
            size: 33,
            mime_type: "application/pdf".to_owned(),
        });
        let executor = TestExecutor {
            behavior: ExecutorBehavior::Fail("provider unavailable".to_owned()),
            request: Mutex::new(None),
        };

        let result = invoke_fetch_response(
            &fetched,
            options("extract all values"),
            &executor,
            &accept_schema,
        )
        .await
        .unwrap();

        assert!(text_from_output(&result).contains("Status: 200 OK"));
        assert!(
            text_from_output(&result).contains("Content processing failed (provider unavailable)")
        );
        assert!(text_from_output(&result).contains("EXTRACTED PDF TEXT"));
        assert!(text_from_output(&result).contains("saved to /tmp/report.pdf"));
        assert_eq!(
            result.result_file_paths,
            vec![PathBuf::from("/tmp/report.pdf")]
        );
        assert!(
            result
                .visible_display
                .contains("processing failed, raw content returned")
        );
    }

    #[tokio::test]
    async fn processing_deadline_falls_back_to_raw_content_without_cancelling_caller() {
        let fetched = response("Raw fallback text");
        let mut invocation = options("summarize");
        invocation.processing_timeout = Duration::from_millis(5);
        let caller_token = invocation.cancellation.clone();
        let executor = TestExecutor {
            behavior: ExecutorBehavior::Pending,
            request: Mutex::new(None),
        };

        let result = invoke_fetch_response(&fetched, invocation, &executor, &accept_schema)
            .await
            .unwrap();

        assert!(!caller_token.is_cancelled());
        assert!(
            text_from_output(&result)
                .contains("Content processing failed (side query deadline exceeded)")
        );
        assert!(text_from_output(&result).contains("Raw fallback text"));
    }

    #[tokio::test]
    async fn user_cancellation_propagates_and_late_success_is_discarded() {
        let fetched = response("Raw fallback text");
        let mut invocation = options("summarize");
        let caller_token = invocation.cancellation.clone();
        let executor = TestExecutor {
            behavior: ExecutorBehavior::CancelCallerThenReturn {
                caller: caller_token,
                response: SideQueryResponse::Text(SideQueryTextResult {
                    text: "late success".to_owned(),
                    usage: None,
                }),
            },
            request: Mutex::new(None),
        };

        let error = invoke_fetch_response(&fetched, invocation, &executor, &accept_schema)
            .await
            .unwrap_err();

        assert_eq!(
            error,
            FetchInvocationError::Cancelled(Some(CancellationReason::Explicit(Arc::from(
                "caller stopped"
            ))))
        );
    }

    #[test]
    fn processing_timeout_override_is_positive_decimal_and_timer_bounded() {
        for raw in [
            None,
            Some(""),
            Some("0"),
            Some("-1"),
            Some("2.5"),
            Some("1e3"),
            Some("0x32"),
            Some("2147483648"),
        ] {
            assert_eq!(
                side_query_timeout_ms_from_env(raw),
                DEFAULT_PROCESSING_TIMEOUT_MS
            );
        }
        assert_eq!(side_query_timeout_ms_from_env(Some("50")), 50);
        assert_eq!(
            side_query_timeout_ms_from_env(Some("2147483647")),
            MAX_TIMER_DELAY_MS
        );
    }

    #[tokio::test]
    async fn uses_empty_response_note_and_keeps_elapsed_visible_text() {
        let fetched = response("Raw text");
        let executor = TestExecutor::returning(" \n ");
        let mut invocation = options("summarize");
        invocation.invocation_started_at = Instant::now() - Duration::from_secs(2);

        let result = invoke_fetch_response(&fetched, invocation, &executor, &accept_schema)
            .await
            .unwrap();

        assert!(text_from_output(&result).contains("processing model returned no content"));
        assert!(result.visible_display.contains("in 2."));
    }
}
