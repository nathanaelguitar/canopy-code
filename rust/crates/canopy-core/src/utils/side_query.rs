//! Provider-neutral orchestration policy for side queries.
//!
//! Port of `packages/core/src/utils/sideQuery.ts`. The host resolves its model
//! configuration and constructs the provider-backed [`SideQueryExecutor`];
//! this module applies shared defaults, output-language instructions,
//! cancellation/deadline behavior, and response validation hooks.
//!
//! Content, system instructions, and generation configuration use JSON values
//! so hosts can preserve their provider SDK's shapes without this utility
//! depending on a specific SDK.

use serde_json::{Map, Value, json};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Instant;

use super::cancellation::{CancellationReason, CancellationToken};

/// Fallback model used when the host has no fast or session model configured.
pub const DEFAULT_SIDE_QUERY_MODEL: &str = "coder-model";

const OUTPUT_LANGUAGE_INSTRUCTION_PREFIX: &str = "Follow the user-visible output language preference below for this side query.\n\nThis preference overrides any earlier language-selection rule in this system instruction.";

/// A response returned by the host-owned generation client.
#[derive(Clone, Debug, PartialEq)]
pub enum SideQueryResponse {
    Text(SideQueryTextResult),
    Json(Value),
}

/// Text response and optional provider usage metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct SideQueryTextResult {
    pub text: String,
    pub usage: Option<Value>,
}

/// Distinguishes text and schema-constrained JSON generation for the host.
#[derive(Clone, Debug, PartialEq)]
pub enum SideQueryMode {
    Text {
        /// `None` means omit the provider option, preserving source behavior.
        stream: Option<bool>,
        fail_closed: Option<bool>,
    },
    Json {
        schema: Value,
    },
}

/// Fully resolved generation request. Provider creation and auth selection
/// remain in the host that implements [`SideQueryExecutor`].
#[derive(Clone, Debug, PartialEq)]
pub struct SideQueryRequest {
    pub contents: Vec<Value>,
    pub model: String,
    pub system_instruction: Option<Value>,
    pub prompt_id: String,
    pub generation_config: Value,
    pub max_attempts: Option<u32>,
    pub mode: SideQueryMode,
}

/// Provider adapter seam. Implementations should route text and JSON modes to
/// their corresponding host client methods and honor cancellation while the
/// request is in flight.
pub trait SideQueryExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        request: SideQueryRequest,
        cancellation: CancellationToken,
    ) -> SideQueryFuture<'a>;
}

pub type SideQueryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SideQueryResponse, String>> + Send + 'a>>;

/// JSON-schema validation hook supplied by the host.
///
/// The host can use its existing Ajv-equivalent validator here. Implementors
/// may coerce the response in place before returning `None`, matching the
/// source validator's coercion behavior.
pub trait SideQueryJsonValidator: Send + Sync {
    fn validate(&self, schema: &Value, response: &mut Value) -> Option<String>;
}

impl<F> SideQueryJsonValidator for F
where
    F: Fn(&Value, &mut Value) -> Option<String> + Send + Sync,
{
    fn validate(&self, schema: &Value, response: &mut Value) -> Option<String> {
        self(schema, response)
    }
}

pub type TextValidationHook = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;
pub type JsonValidationHook = Arc<dyn Fn(&Value) -> Option<String> + Send + Sync>;

/// Input policy. The host fills in its model configuration and output-language
/// path from its Config object, then passes this to [`run_side_query`].
#[derive(Default)]
pub struct SideQueryOptions {
    pub contents: Vec<Value>,
    /// `None` means text mode. A JSON `null` schema also selects text mode,
    /// matching the source's overload discriminator.
    pub schema: Option<Value>,
    pub cancellation: CancellationToken,
    /// Optional host-selected model override.
    pub model_override: Option<String>,
    /// Host configuration's fast model, if present.
    pub fast_model: Option<String>,
    /// Host configuration's primary model, if present.
    pub configured_model: Option<String>,
    pub system_instruction: Option<Value>,
    pub prompt_id: Option<String>,
    pub purpose: Option<String>,
    pub generation_config: Option<Value>,
    pub max_attempts: Option<u32>,
    pub skip_output_language_preference: bool,
    pub output_language_file_path: Option<PathBuf>,
    pub stream: Option<bool>,
    pub fail_closed: Option<bool>,
    /// Absolute host deadline. No deadline is imposed by default.
    pub deadline: Option<Instant>,
    pub validate_text: Option<TextValidationHook>,
    pub validate_json: Option<JsonValidationHook>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SideQueryResult {
    Text(SideQueryTextResult),
    Json(Value),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SideQueryError {
    #[error("side query was cancelled")]
    Cancelled(Option<CancellationReason>),
    #[error("side query deadline exceeded")]
    DeadlineExceeded,
    #[error("{0}")]
    Execution(String),
    #[error("Invalid side query response: {0}")]
    InvalidJsonResponse(String),
    #[error("{0}")]
    Validation(String),
    #[error("side query executor returned a response for the wrong mode")]
    UnexpectedResponseMode,
}

/// Run a text or JSON side query using host-owned provider and schema
/// validation implementations.
pub async fn run_side_query<E, V>(
    executor: &E,
    json_validator: &V,
    options: SideQueryOptions,
) -> Result<SideQueryResult, SideQueryError>
where
    E: SideQueryExecutor + ?Sized,
    V: SideQueryJsonValidator + ?Sized,
{
    let cancellation = options.cancellation.clone();
    let executor_cancellation = cancellation.child();
    let deadline = options.deadline;
    let mut options = options;
    options.cancellation = executor_cancellation.clone();
    cancellable(cancellation, executor_cancellation, deadline, async move {
        run_side_query_inner(executor, json_validator, options).await
    })
    .await?
}

async fn run_side_query_inner<E, V>(
    executor: &E,
    json_validator: &V,
    options: SideQueryOptions,
) -> Result<SideQueryResult, SideQueryError>
where
    E: SideQueryExecutor + ?Sized,
    V: SideQueryJsonValidator + ?Sized,
{
    let model = resolve_model(
        options.model_override.as_deref(),
        options.fast_model.as_deref(),
        options.configured_model.as_deref(),
    );
    let prompt_id = options
        .prompt_id
        .clone()
        .unwrap_or_else(|| build_default_prompt_id(options.purpose.as_deref()));
    let generation_config = apply_thinking_default(options.generation_config);
    let output_language_instruction = if options.skip_output_language_preference {
        None
    } else if let Some(path) = options.output_language_file_path.as_deref() {
        read_output_language_instruction(path).await
    } else {
        None
    };
    let system_instruction = append_system_instruction(
        options.system_instruction,
        output_language_instruction.as_deref(),
    );
    let schema = options.schema.filter(|schema| !schema.is_null());
    let mode = match schema.as_ref() {
        Some(schema) => SideQueryMode::Json {
            schema: schema.clone(),
        },
        None => SideQueryMode::Text {
            stream: options.stream,
            fail_closed: options.fail_closed,
        },
    };
    let request = SideQueryRequest {
        contents: options.contents,
        model,
        system_instruction,
        prompt_id,
        generation_config,
        max_attempts: options.max_attempts,
        mode,
    };
    let response = executor
        .execute(request, options.cancellation)
        .await
        .map_err(SideQueryError::Execution)?;

    match (schema.as_ref(), response) {
        (Some(schema), SideQueryResponse::Json(mut response)) => {
            if !response.is_object() && !response.is_array() {
                return Err(SideQueryError::InvalidJsonResponse(
                    "Value of params must be an object".to_owned(),
                ));
            }
            if let Some(error) = json_validator.validate(schema, &mut response) {
                return Err(SideQueryError::InvalidJsonResponse(error));
            }
            if let Some(validate) = options.validate_json.as_ref() {
                if let Some(error) = validate(&response) {
                    return Err(SideQueryError::Validation(error));
                }
            }
            Ok(SideQueryResult::Json(response))
        }
        (None, SideQueryResponse::Text(response)) => {
            if let Some(validate) = options.validate_text.as_ref() {
                if let Some(error) = validate(&response.text) {
                    return Err(SideQueryError::Validation(error));
                }
            }
            Ok(SideQueryResult::Text(response))
        }
        _ => Err(SideQueryError::UnexpectedResponseMode),
    }
}

async fn cancellable<T>(
    cancellation: CancellationToken,
    executor_cancellation: CancellationToken,
    deadline: Option<Instant>,
    future: impl Future<Output = T>,
) -> Result<T, SideQueryError> {
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            Err(SideQueryError::Cancelled(cancellation.reason()))
        }
        result = async {
            match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, future)
                    .await
                    .map_err(|_| SideQueryError::DeadlineExceeded),
                None => Ok(future.await),
            }
        } => result,
    };
    if matches!(&result, Err(SideQueryError::DeadlineExceeded)) {
        executor_cancellation.cancel_with_reason(CancellationReason::Timeout);
    }
    result
}

fn resolve_model(
    override_model: Option<&str>,
    fast_model: Option<&str>,
    configured_model: Option<&str>,
) -> String {
    override_model
        .or(fast_model)
        .or(configured_model)
        .unwrap_or(DEFAULT_SIDE_QUERY_MODEL)
        .to_owned()
}

fn build_default_prompt_id(purpose: Option<&str>) -> String {
    match purpose.filter(|purpose| !purpose.is_empty()) {
        Some(purpose) => format!("side-query:{purpose}"),
        None => "side-query".to_owned(),
    }
}

fn apply_thinking_default(config: Option<Value>) -> Value {
    let mut config = match config {
        Some(Value::Object(config)) => config,
        _ => Map::new(),
    };
    let mut thinking = match config.remove("thinkingConfig") {
        Some(Value::Object(thinking)) => thinking,
        _ => Map::new(),
    };
    thinking
        .entry("includeThoughts".to_owned())
        .or_insert(Value::Bool(false));
    config.insert("thinkingConfig".to_owned(), Value::Object(thinking));
    Value::Object(config)
}

async fn read_output_language_instruction(path: &std::path::Path) -> Option<String> {
    let preference = tokio::fs::read_to_string(path).await.ok()?;
    let preference = preference.trim();
    if preference.is_empty() {
        return None;
    }
    Some(format!(
        "{OUTPUT_LANGUAGE_INSTRUCTION_PREFIX}\n\n{preference}"
    ))
}

fn append_system_instruction(
    system_instruction: Option<Value>,
    output_language_instruction: Option<&str>,
) -> Option<Value> {
    let Some(language_instruction) = output_language_instruction else {
        return system_instruction;
    };
    let language_part = json!({"text": language_instruction});
    match system_instruction {
        None => Some(Value::String(language_instruction.to_owned())),
        Some(Value::String(instruction)) => Some(Value::String(format!(
            "{instruction}\n\n{language_instruction}"
        ))),
        Some(Value::Array(mut parts)) => {
            parts.push(language_part);
            Some(Value::Array(parts))
        }
        Some(Value::Object(mut content)) if content.get("parts").is_some_and(Value::is_array) => {
            content
                .get_mut("parts")
                .and_then(Value::as_array_mut)
                .expect("parts was checked as an array")
                .push(language_part);
            Some(Value::Object(content))
        }
        Some(instruction) => Some(Value::Array(vec![instruction, language_part])),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_SIDE_QUERY_MODEL, SideQueryError, SideQueryExecutor, SideQueryMode,
        SideQueryOptions, SideQueryRequest, SideQueryResponse, SideQueryResult,
        SideQueryTextResult, append_system_instruction, apply_thinking_default,
        read_output_language_instruction, run_side_query,
    };
    use crate::utils::cancellation::{CancellationReason, CancellationToken};
    use serde_json::{Value, json};
    use std::future::Future;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::Instant;

    #[derive(Default)]
    struct RecordingExecutor {
        request: Mutex<Option<SideQueryRequest>>,
        response: Mutex<Option<SideQueryResponse>>,
        cancellation: Mutex<Option<CancellationToken>>,
        pending: bool,
    }

    impl RecordingExecutor {
        fn with_response(response: SideQueryResponse) -> Self {
            Self {
                request: Mutex::new(None),
                response: Mutex::new(Some(response)),
                cancellation: Mutex::new(None),
                pending: false,
            }
        }

        fn pending() -> Self {
            Self {
                request: Mutex::new(None),
                response: Mutex::new(None),
                cancellation: Mutex::new(None),
                pending: true,
            }
        }

        fn request(&self) -> SideQueryRequest {
            self.request
                .lock()
                .unwrap()
                .clone()
                .expect("request captured")
        }

        fn cancellation(&self) -> CancellationToken {
            self.cancellation
                .lock()
                .unwrap()
                .clone()
                .expect("executor cancellation captured")
        }
    }

    impl SideQueryExecutor for RecordingExecutor {
        fn execute<'a>(
            &'a self,
            request: SideQueryRequest,
            cancellation: CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<SideQueryResponse, String>> + Send + 'a>> {
            *self.request.lock().unwrap() = Some(request);
            *self.cancellation.lock().unwrap() = Some(cancellation);
            let pending = self.pending;
            let response = self.response.lock().unwrap().clone();
            Box::pin(async move {
                if pending {
                    std::future::pending::<()>().await;
                }
                response.ok_or_else(|| "missing test response".to_owned())
            })
        }
    }

    fn accept_schema(_: &Value, _: &mut Value) -> Option<String> {
        None
    }

    fn text_options() -> SideQueryOptions {
        SideQueryOptions {
            contents: vec![json!({"role":"user","parts":[{"text":"query"}]})],
            ..SideQueryOptions::default()
        }
    }

    #[tokio::test]
    async fn resolves_model_fallback_prompt_id_and_thinking_default() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "answer".to_owned(),
                usage: Some(json!({"totalTokenCount": 4})),
            }));
        let options = SideQueryOptions {
            fast_model: Some("fast-model".to_owned()),
            configured_model: Some("main-model".to_owned()),
            purpose: Some("session-recap".to_owned()),
            ..text_options()
        };

        let result = run_side_query(&executor, &accept_schema, options)
            .await
            .unwrap();

        assert!(matches!(result, SideQueryResult::Text(_)));
        let request = executor.request();
        assert_eq!(request.model, "fast-model");
        assert_eq!(request.prompt_id, "side-query:session-recap");
        assert_eq!(
            request.generation_config["thinkingConfig"]["includeThoughts"],
            false
        );
        assert_eq!(request.max_attempts, None);
        assert_eq!(request.contents[0]["parts"][0]["text"], "query");
    }

    #[tokio::test]
    async fn explicit_model_and_prompt_id_override_host_defaults() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "answer".to_owned(),
                usage: None,
            }));
        let options = SideQueryOptions {
            model_override: Some("pinned-model".to_owned()),
            fast_model: Some("fast-model".to_owned()),
            configured_model: Some("main-model".to_owned()),
            prompt_id: Some("legacy-id".to_owned()),
            purpose: Some("ignored".to_owned()),
            ..text_options()
        };

        run_side_query(&executor, &accept_schema, options)
            .await
            .unwrap();

        let request = executor.request();
        assert_eq!(request.model, "pinned-model");
        assert_eq!(request.prompt_id, "legacy-id");
    }

    #[tokio::test]
    async fn uses_configured_model_then_default_and_empty_purpose_has_base_id() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "answer".to_owned(),
                usage: None,
            }));
        run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                configured_model: Some("main-model".to_owned()),
                purpose: Some(String::new()),
                ..text_options()
            },
        )
        .await
        .unwrap();
        assert_eq!(executor.request().model, "main-model");
        assert_eq!(executor.request().prompt_id, "side-query");

        let default_executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "answer".to_owned(),
                usage: None,
            }));
        run_side_query(&default_executor, &accept_schema, text_options())
            .await
            .unwrap();
        assert_eq!(default_executor.request().model, DEFAULT_SIDE_QUERY_MODEL);
    }

    #[test]
    fn adds_thinking_default_without_overwriting_other_generation_settings() {
        assert_eq!(
            apply_thinking_default(Some(json!({
                "maxOutputTokens": 128,
                "thinkingConfig": {"thinkingBudget": 7}
            }))),
            json!({
                "maxOutputTokens": 128,
                "thinkingConfig": {"includeThoughts": false, "thinkingBudget": 7}
            })
        );
        assert_eq!(
            apply_thinking_default(Some(json!({
                "thinkingConfig": {"includeThoughts": true}
            }))),
            json!({"thinkingConfig":{"includeThoughts":true}})
        );
    }

    #[test]
    fn appends_output_language_instruction_using_source_shapes() {
        let instruction = "Follow this language rule.";
        let part = json!({"text": instruction});
        assert_eq!(
            append_system_instruction(None, Some(instruction)),
            Some(json!(instruction))
        );
        assert_eq!(
            append_system_instruction(Some(json!("Existing instruction")), Some(instruction)),
            Some(json!("Existing instruction\n\nFollow this language rule."))
        );
        assert_eq!(
            append_system_instruction(Some(json!([{"text":"first"}])), Some(instruction)),
            Some(json!([{"text":"first"}, part]))
        );
        assert_eq!(
            append_system_instruction(
                Some(json!({"parts":[{"text":"first"}],"role":"system"})),
                Some(instruction)
            ),
            Some(json!({"parts":[{"text":"first"},part],"role":"system"}))
        );
        assert_eq!(
            append_system_instruction(Some(json!({"inlineData":{"data":"x"}})), Some(instruction)),
            Some(json!([{"inlineData":{"data":"x"}},part]))
        );
        assert_eq!(
            append_system_instruction(Some(json!("kept")), None),
            Some(json!("kept"))
        );
    }

    #[tokio::test]
    async fn reads_and_trims_output_language_rule_and_ignores_missing_or_blank_files() {
        let path = std::env::temp_dir().join(format!(
            "canopy-side-query-language-{}.md",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&path, "  Respond in Spanish.\n\n")
            .await
            .unwrap();
        let instruction = read_output_language_instruction(&path)
            .await
            .expect("non-empty preference is loaded");
        assert!(instruction.starts_with("Follow the user-visible output language preference"));
        assert!(instruction.ends_with("Respond in Spanish."));
        tokio::fs::write(&path, " \n\t").await.unwrap();
        assert_eq!(read_output_language_instruction(&path).await, None);
        assert_eq!(
            read_output_language_instruction(&PathBuf::from(format!("{}.missing", path.display())))
                .await,
            None
        );
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn forwards_json_schema_and_max_attempts_then_runs_validation_hooks() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Json(json!({"ok":true})));
        let validated_schema = Arc::new(Mutex::new(None));
        let validator = {
            let validated_schema = Arc::clone(&validated_schema);
            move |schema: &Value, response: &mut Value| {
                *validated_schema.lock().unwrap() = Some(schema.clone());
                if response["ok"] == true {
                    response["normalized"] = json!(true);
                    None
                } else {
                    Some("missing ok".to_owned())
                }
            }
        };
        let options = SideQueryOptions {
            schema: Some(json!({"type":"object","required":["ok"]})),
            max_attempts: Some(1),
            validate_json: Some(Arc::new(|response| {
                (!response["normalized"].as_bool().unwrap_or(false))
                    .then(|| "not normalized".to_owned())
            })),
            ..text_options()
        };

        let result = run_side_query(&executor, &validator, options)
            .await
            .unwrap();

        assert_eq!(
            *validated_schema.lock().unwrap(),
            Some(json!({"type":"object","required":["ok"]}))
        );
        assert_eq!(
            result,
            SideQueryResult::Json(json!({"ok":true,"normalized":true}))
        );
        let request = executor.request();
        assert_eq!(request.max_attempts, Some(1));
        assert!(matches!(request.mode, SideQueryMode::Json { .. }));
    }

    #[tokio::test]
    async fn reports_schema_and_custom_validation_failures() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Json(json!({"ok":false})));
        let error = run_side_query(
            &executor,
            &|_: &Value, _: &mut Value| Some("schema mismatch".to_owned()),
            SideQueryOptions {
                schema: Some(json!({"type":"object"})),
                ..text_options()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            SideQueryError::InvalidJsonResponse("schema mismatch".to_owned())
        );

        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Json(json!({"ok":true})));
        let error = run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                schema: Some(json!({"type":"object"})),
                validate_json: Some(Arc::new(|_| Some("custom JSON check".to_owned()))),
                ..text_options()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            SideQueryError::Validation("custom JSON check".to_owned())
        );
    }

    #[tokio::test]
    async fn forwards_text_stream_and_fail_closed_options_and_runs_text_validation() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "short".to_owned(),
                usage: None,
            }));
        let error = run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                stream: Some(true),
                fail_closed: Some(true),
                validate_text: Some(Arc::new(|text| {
                    (text.len() < 10).then(|| "too short".to_owned())
                })),
                ..text_options()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error, SideQueryError::Validation("too short".to_owned()));
        assert_eq!(
            executor.request().mode,
            SideQueryMode::Text {
                stream: Some(true),
                fail_closed: Some(true)
            }
        );
    }

    #[tokio::test]
    async fn output_language_can_be_skipped_and_explicit_thoughts_are_preserved() {
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "ok".to_owned(),
                usage: None,
            }));
        run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                system_instruction: Some(json!("original")),
                generation_config: Some(json!({
                    "thinkingConfig":{"includeThoughts":true}
                })),
                output_language_file_path: Some(PathBuf::from("/nonexistent/output-language.md")),
                skip_output_language_preference: true,
                ..text_options()
            },
        )
        .await
        .unwrap();
        let request = executor.request();
        assert_eq!(request.system_instruction, Some(json!("original")));
        assert_eq!(
            request.generation_config["thinkingConfig"]["includeThoughts"],
            true
        );
    }

    #[tokio::test]
    async fn run_side_query_loads_output_language_and_appends_it_to_system_text() {
        let path = std::env::temp_dir().join(format!(
            "canopy-side-query-language-{}.md",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::write(&path, "Respond in Spanish.")
            .await
            .unwrap();
        let executor =
            RecordingExecutor::with_response(SideQueryResponse::Text(SideQueryTextResult {
                text: "vale".to_owned(),
                usage: None,
            }));

        run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                system_instruction: Some(json!("Summarize this.")),
                output_language_file_path: Some(path.clone()),
                ..text_options()
            },
        )
        .await
        .unwrap();

        let request = executor.request();
        assert_eq!(
            request.system_instruction,
            Some(json!(
                "Summarize this.\n\nFollow the user-visible output language preference below for this side query.\n\nThis preference overrides any earlier language-selection rule in this system instruction.\n\nRespond in Spanish."
            ))
        );
        let _ = tokio::fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn cancellation_and_deadline_interrupt_an_in_flight_executor() {
        let cancellation = CancellationToken::new();
        let executor = RecordingExecutor::pending();
        let cancel_task = {
            let cancellation = cancellation.clone();
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                cancellation.cancel_with_reason("caller stopped");
            })
        };
        let error = run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                cancellation,
                ..text_options()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            SideQueryError::Cancelled(Some(CancellationReason::Explicit(Arc::from(
                "caller stopped"
            ))))
        );
        assert!(executor.cancellation().is_cancelled());
        cancel_task.await.unwrap();

        let executor = RecordingExecutor::pending();
        let error = run_side_query(
            &executor,
            &accept_schema,
            SideQueryOptions {
                deadline: Some(Instant::now() + Duration::from_millis(5)),
                ..text_options()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error, SideQueryError::DeadlineExceeded);
        assert_eq!(
            executor.cancellation().reason(),
            Some(CancellationReason::Timeout)
        );
    }

    #[tokio::test]
    async fn propagates_executor_failures_without_rewriting_them() {
        struct FailedExecutor;
        impl SideQueryExecutor for FailedExecutor {
            fn execute<'a>(
                &'a self,
                _request: SideQueryRequest,
                _cancellation: CancellationToken,
            ) -> super::SideQueryFuture<'a> {
                Box::pin(async { Err("upstream 503".to_owned()) })
            }
        }

        let error = run_side_query(&FailedExecutor, &accept_schema, text_options())
            .await
            .unwrap_err();
        assert_eq!(error, SideQueryError::Execution("upstream 503".to_owned()));
    }
}
