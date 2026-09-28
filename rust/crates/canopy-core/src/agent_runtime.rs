//! Model/tool coordination for the Rust Canopy runtime.
//!
//! `Turn` owns provider-response conversion. This module owns the next layer:
//! durable user and assistant records, tool execution intents, bounded tool
//! results, and continuation requests. Product integrations supply the
//! permission-aware tool executor.

use std::collections::HashSet;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures_util::future::join_all;
use indexmap::IndexMap;
use serde_json::{Map, Value, json};
use thiserror::Error;
use uuid::Uuid;

use crate::file_read_cache::FileReadCache;
use crate::goals::{GoalTurnPermit, run_with_goal_turn_context, run_without_goal_turn_context};
use crate::providers::anthropic::{
    AnthropicGeminiEventStream, AnthropicGeminiStreamError, AnthropicMessagesClient,
    AnthropicProviderConfig,
};
use crate::providers::gemini::{GeminiEventStream, GeminiNativeClient, GeminiProviderConfig};
use crate::providers::openai_compatible::{
    OpenAiCompatibleClient, OpenAiCompatibleConfig, ProviderError,
};
use crate::providers::openai_pipeline::{OpenAiPipelineConfig, build_openai_request};
use crate::providers::openai_stream::{OpenAiGeminiEventStream, OpenAiGeminiStreamError};
use crate::providers::retry_adapter::ProviderRetryAdapter;
use crate::providers::retry_error_classification::RetryErrorClassificationContext;
use crate::providers::streaming_converter::{ConvertedStreamChunk, OpenAiStreamContext};
use crate::recording::SessionRecorder;
use crate::services::commit_attribution::AttributionSnapshot;
use crate::services::compaction_input_slimming::resolve_compaction_tuning;
use crate::services::file_history::FileHistorySnapshot;
use crate::services::image_payload_references::{
    ImagePayloadStore, InMemoryImagePayloadStore, StoredImagePayload, build_reattach_parts,
    count_all_inline_images, inline_image_data_bytes, recent_unique_images,
    replace_image_payloads_in_place,
};
use crate::services::loop_detection::{LoopDetectionConfig, LoopDetectionService};
use crate::services::microcompaction::{
    ClearContextOnIdleSettings, MicrocompactOptions, microcompact_history_with_env,
};
use crate::services::runtime_session_metrics::RuntimeSessionMetricsCollector;
use crate::services::usage_history::SessionMetrics;
use crate::session_writer::SessionWriterError;
use crate::tool_effect_journal::record_tool_execution_intent;
use crate::tool_response_finalizer::{
    FileToolOutputStore, ToolExecutionOutput, ToolResponseBudgetEntry, finalize_tool_responses,
};
use crate::tools::todo_write::active_todo_reminder_update;
use crate::turn::{ToolCallRequestInfo, Turn, TurnEvent, TurnResponseError};
use crate::utils::cancellation::CancellationToken;
use crate::utils::retry::{
    RetryHooks, RetryOptions, RetryWithBackoffError, is_unattended_mode, retry_with_backoff,
    system_time_ms, tokio_retry_sleep,
};
use crate::utils::xml::escape_system_reminder_tags;

pub const MAX_MODEL_TURNS: usize = 100;
pub const MAX_TOOL_CALLS_PER_MODEL_TURN: usize = 100;
const DEFAULT_MAX_TURN_OUTPUT_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_TOOL_RESPONSE_BUDGET_CHARS: usize = 32_000;
const DEFAULT_MAX_CONCURRENT_TOOL_CALLS: usize = 10;
const MAX_HISTORY_BYTES: usize = 12 * 1024 * 1024;
const IMAGE_HISTORY_BYTE_PRESSURE_THRESHOLD: usize = 6 * 1024 * 1024;
const IMAGE_HISTORY_PRESSURE_HEADROOM_BYTES: usize = 2 * 1024 * 1024;
const MAX_IMAGE_PAYLOAD_CACHE_SESSIONS: usize = 4;
const MAX_IMAGE_PAYLOAD_CACHE_BYTES_PER_SESSION: usize = 8 * 1024 * 1024;
const MAX_IMAGE_PAYLOAD_CACHE_ENTRIES_PER_SESSION: usize = 64;
const IMAGE_PAYLOAD_CACHE_ENTRY_OVERHEAD_BYTES: usize = 256;
const MAX_SINGLE_TOOL_OUTPUT_BYTES: usize = 50 * 1024 * 1024;
const MAX_TOOL_BATCH_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
const CONSECUTIVE_IDENTICAL_TOOL_LIMIT: usize = 5;
const ACTIVE_TODO_REMINDER_REFRESH_TURNS: usize = 3;
const IMAGE_REATTACH_SUMMARY_PREFIX: &str = "Recent images reattached for visual context:";

pub type ReadFileRetentionPredicate = Arc<dyn Fn(&str) -> bool + Send + Sync>;

fn default_max_concurrent_tool_calls() -> usize {
    std::env::var("CANOPY_CODE_MAX_TOOL_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_CONCURRENT_TOOL_CALLS)
        .min(MAX_TOOL_CALLS_PER_MODEL_TURN)
}

async fn execute_tool_call<E: AgentToolExecutor>(
    executor: &E,
    call: &ToolCallRequestInfo,
    request_memory_check: Option<&(dyn Fn() + Send + Sync)>,
) -> (Result<ToolExecutionOutput, String>, u64) {
    let started = Instant::now();
    let result = if call.was_output_truncated == Some(true) {
        (
            Err(
                "Tool arguments may be incomplete because the model output was truncated."
                    .to_owned(),
            ),
            elapsed_milliseconds(started),
        )
    } else {
        let execution = async { executor.execute(call).await };
        let permit = call
            .goal_context
            .as_ref()
            .and_then(|context| serde_json::from_value::<GoalTurnPermit>(context.clone()).ok());
        let result = if let Some(permit) = permit {
            run_with_goal_turn_context(permit, execution).await
        } else {
            run_without_goal_turn_context(execution).await
        };
        (result, elapsed_milliseconds(started))
    };
    if let Some(request_memory_check) = request_memory_check {
        request_memory_check();
    }
    result
}

fn elapsed_milliseconds(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[derive(Default)]
struct ActiveTodoReminder {
    reminder: Option<String>,
    tool_result_turns: usize,
}

impl ActiveTodoReminder {
    fn update(&mut self, reminder: Option<String>) {
        self.reminder = reminder;
        self.tool_result_turns = 0;
    }

    fn take_for_tool_result(&mut self) -> Option<String> {
        let reminder = self.reminder.as_ref()?;
        self.tool_result_turns = self.tool_result_turns.saturating_add(1);
        if self.tool_result_turns < ACTIVE_TODO_REMINDER_REFRESH_TURNS {
            return None;
        }
        self.tool_result_turns = 0;
        Some(reminder.clone())
    }
}

#[derive(Clone, Debug)]
pub struct AgentRuntimeConfig {
    pub model: String,
    pub system_instruction: Option<Value>,
    /// Google GenAI function declarations, converted to the provider's tool
    /// format by the shared OpenAI request pipeline.
    pub tool_declarations: Vec<Value>,
    pub pipeline: OpenAiPipelineConfig,
    pub max_model_turns: usize,
    pub max_concurrent_tool_calls: usize,
    pub max_turn_output_bytes: usize,
    pub tool_response_budget_chars: usize,
    pub tool_output_dir: PathBuf,
    pub context_window_size: Option<u64>,
    pub goal_context: Option<Value>,
    /// Persist provider token usage under the host's privacy setting.
    pub usage_statistics_enabled: bool,
    /// Provider authentication path label used in usage summaries.
    pub usage_auth_type: String,
    /// Host surface label used in usage summaries.
    pub usage_source: String,
}

impl AgentRuntimeConfig {
    pub fn new(model: impl Into<String>, tool_output_dir: impl Into<PathBuf>) -> Self {
        let model = model.into();
        let mut pipeline = OpenAiPipelineConfig::default();
        pipeline.request_context.model = model.clone();
        Self {
            model,
            system_instruction: None,
            tool_declarations: Vec::new(),
            pipeline,
            max_model_turns: MAX_MODEL_TURNS,
            max_concurrent_tool_calls: default_max_concurrent_tool_calls(),
            max_turn_output_bytes: DEFAULT_MAX_TURN_OUTPUT_BYTES,
            tool_response_budget_chars: DEFAULT_TOOL_RESPONSE_BUDGET_CHARS,
            tool_output_dir: tool_output_dir.into(),
            context_window_size: None,
            goal_context: None,
            usage_statistics_enabled: true,
            usage_auth_type: "unknown".to_owned(),
            usage_source: "main".to_owned(),
        }
    }
}

async fn record_token_usage_best_effort(
    runtime_base_dir: PathBuf,
    session_id: String,
    model: String,
    auth_type: String,
    source: String,
    usage_metadata: Value,
    api_duration_ms: u64,
) {
    let _ = tokio::task::spawn_blocking(move || {
        let event = crate::services::token_usage::ApiResponseUsageInput::from_usage_metadata(
            &usage_metadata,
            None,
            Some(&model),
            Some(&auth_type),
            Some(&source),
            Some(api_duration_ms as f64),
        );
        let _ = crate::services::token_usage::record_api_response(
            &runtime_base_dir,
            session_id,
            event,
            chrono::Utc::now(),
        );
    })
    .await;
}

/// A provider-independent interface for the permission-aware tool layer.
///
/// The executor returns model-visible text and optional content parts.
/// Side-effecting tools must report that fact so the runtime persists a
/// write-ahead intent before calling them.
pub trait AgentToolExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
    ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>>;

    /// Return the active prompt's cancellation token when the host exposes
    /// one. Composed tool adapters use this to stop their own in-flight work
    /// when the host cancels a prompt. The runtime itself does not assume all
    /// executors can safely cancel side effects.
    fn cancellation_token_for_current_prompt(&self) -> Option<CancellationToken> {
        None
    }

    fn is_side_effecting(&self, _tool_name: &str) -> bool {
        true
    }

    /// Return true only for operations that can run alongside other
    /// concurrency-safe calls without sharing mutable state or side effects.
    fn is_concurrency_safe(&self, _call: &ToolCallRequestInfo) -> bool {
        false
    }

    /// Return host-provided context to append after the tool response has
    /// passed through the model-facing output finalizer. This stays outside
    /// the output budget so hook context is never cut by the tool truncator.
    fn additional_context_after_tool_use<'a>(
        &'a self,
        _call: &'a ToolCallRequestInfo,
        _result_file_paths: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(async { None })
    }

    /// Prepare file-history state for a newly recorded user prompt.
    ///
    /// Hosts can create a turn snapshot and enqueue it through their
    /// file-history service. The runtime drains and records those updates
    /// after this hook returns. Existing executors need not implement it.
    fn begin_user_prompt<'a>(
        &'a self,
        _prompt_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Drain file-history snapshots queued by prompt setup or executed tools.
    /// Existing executors have no queued snapshots by default.
    fn take_file_history_snapshot_updates(&self) -> Vec<FileHistorySnapshot> {
        Vec::new()
    }

    /// Expose the current host-owned attribution state for durable session
    /// persistence. The runtime records snapshots after prompt setup and tool
    /// batches; existing executors need not implement attribution.
    fn commit_attribution_snapshot(&self) -> Option<AttributionSnapshot> {
        None
    }
}

fn persist_file_history_snapshot_updates<E: AgentToolExecutor>(
    executor: &E,
    recorder: &mut SessionRecorder,
) {
    for snapshot in executor.take_file_history_snapshot_updates() {
        // File-history persistence is best-effort; it must not suppress the
        // prompt or tool response that caused the snapshot update.
        let _ = recorder.record_file_history_snapshot(&snapshot);
    }
}

fn persist_commit_attribution_snapshot<E: AgentToolExecutor>(
    executor: &E,
    recorder: &mut SessionRecorder,
) {
    if let Some(snapshot) = executor.commit_attribution_snapshot() {
        // Attribution is diagnostic metadata; persistence failure must not
        // suppress the prompt or tool response that caused the update.
        let _ = recorder.record_attribution_snapshot(&snapshot);
    }
}

pub enum AgentRunEvent {
    Turn(TurnEvent),
    ToolExecutionStarted {
        call_id: String,
        name: String,
    },
    ToolExecutionFinished {
        call_id: String,
        name: String,
        response: Value,
        display: Option<Value>,
        was_truncated: bool,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentRunSummary {
    pub model_turns: usize,
    pub tool_calls: usize,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Error)]
pub enum AgentRuntimeError {
    #[error("model name must not be empty")]
    EmptyModel,
    #[error("agent runtime limits must be positive; model turns cannot exceed {MAX_MODEL_TURNS}")]
    InvalidLimits,
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error(transparent)]
    Retry(#[from] RetryWithBackoffError<ProviderError>),
    #[error(transparent)]
    Stream(#[from] OpenAiGeminiStreamError),
    #[error(transparent)]
    AnthropicStream(#[from] AnthropicGeminiStreamError),
    #[error(transparent)]
    Turn(#[from] TurnResponseError),
    #[error(transparent)]
    Session(#[from] SessionWriterError),
    #[error("model response exceeded the configured {limit}-byte turn limit")]
    TurnOutputTooLarge { limit: usize },
    #[error("conversation history exceeded the {limit}-byte in-memory limit")]
    HistoryTooLarge { limit: usize },
    #[error("provider stream ended before a finish reason was received")]
    IncompleteProviderTurn,
    #[error("model exceeded the {0}-turn continuation limit")]
    ModelTurnLimit(usize),
    #[error("model requested {requested} tools in one turn; limit is {limit}")]
    ToolCallLimit { requested: usize, limit: usize },
    #[error("loop detected: the same tool call was requested {count} times consecutively")]
    RepeatedToolCall { count: usize },
    #[error("loop detected: {loop_type}")]
    LoopDetected { loop_type: String },
    #[error("event consumer failed: {0}")]
    EventConsumer(String),
}

pub struct AgentRuntime {
    client: RuntimeProviderClient,
    config: AgentRuntimeConfig,
    prevent_system_sleep: bool,
    retry_cancellation: Option<CancellationToken>,
    memory_pressure_compaction: Option<MemoryPressureCompaction>,
    memory_pressure_check: Option<Arc<dyn Fn() + Send + Sync>>,
    session_metrics: RuntimeSessionMetricsCollector,
    // The session id scopes reference lookup; IndexMap order also tracks the
    // bounded least-recently-used session cache.
    image_payload_stores: Mutex<IndexMap<String, RuntimeImagePayloadCache>>,
}

enum RuntimeProviderClient {
    OpenAiCompatible(OpenAiCompatibleClient),
    Anthropic(AnthropicMessagesClient),
    Gemini(GeminiNativeClient),
}

enum RuntimeProviderStream {
    OpenAiCompatible(OpenAiGeminiEventStream),
    Anthropic(AnthropicGeminiEventStream),
    Gemini(GeminiEventStream),
}

enum RuntimeProviderStreamError {
    OpenAiCompatible(OpenAiGeminiStreamError),
    Anthropic(AnthropicGeminiStreamError),
    Gemini(ProviderError),
}

impl RuntimeProviderStream {
    async fn next_chunk(
        &mut self,
    ) -> Result<Option<ConvertedStreamChunk>, RuntimeProviderStreamError> {
        match self {
            Self::OpenAiCompatible(stream) => stream
                .next_chunk()
                .await
                .map_err(RuntimeProviderStreamError::OpenAiCompatible),
            Self::Anthropic(stream) => stream
                .next_chunk()
                .await
                .map_err(RuntimeProviderStreamError::Anthropic),
            Self::Gemini(stream) => stream
                .next_chunk()
                .await
                .map(|response| {
                    response.map(|response| ConvertedStreamChunk {
                        response,
                        ..ConvertedStreamChunk::default()
                    })
                })
                .map_err(RuntimeProviderStreamError::Gemini),
        }
    }
}

#[derive(Default)]
struct RuntimeImagePayloadCache {
    store: InMemoryImagePayloadStore,
    /// Most-recent-first retention order used to make byte-cap eviction stable.
    ids: Vec<String>,
}

struct MemoryPressureCompaction {
    requested: Arc<AtomicBool>,
    file_cache: FileReadCache,
    settings: ClearContextOnIdleSettings,
    preserve_read_file_result: ReadFileRetentionPredicate,
    keep_recent_env: Option<String>,
}

impl AgentRuntime {
    pub fn new(
        provider: OpenAiCompatibleConfig,
        config: AgentRuntimeConfig,
    ) -> Result<Self, AgentRuntimeError> {
        let config = Self::validate_config(config)?;
        let client = OpenAiCompatibleClient::new(provider)?;
        Ok(Self::with_client(
            RuntimeProviderClient::OpenAiCompatible(client),
            config,
        ))
    }

    /// Construct a runtime that sends its primary model turns to Anthropic's
    /// Messages API. Existing callers of [`AgentRuntime::new`] remain on the
    /// OpenAI-compatible path.
    pub fn new_anthropic(
        provider: AnthropicProviderConfig,
        mut config: AgentRuntimeConfig,
    ) -> Result<Self, AgentRuntimeError> {
        if !provider.model.trim().is_empty() {
            config.model = provider.model.clone();
        }
        let config = Self::validate_config(config)?;
        let client = AnthropicMessagesClient::new(provider)?;
        Ok(Self::with_client(
            RuntimeProviderClient::Anthropic(client),
            config,
        ))
    }

    /// Construct a runtime that sends primary turns directly to Google's
    /// Gemini GenerateContent API. `new` remains OpenAI-compatible by default.
    pub fn new_gemini(
        mut provider: GeminiProviderConfig,
        mut config: AgentRuntimeConfig,
    ) -> Result<Self, AgentRuntimeError> {
        if !provider.model.trim().is_empty() {
            config.model = provider.model.clone();
        } else {
            provider.model = config.model.clone();
        }
        let config = Self::validate_config(config)?;
        let client = GeminiNativeClient::new(provider)?;
        Ok(Self::with_client(
            RuntimeProviderClient::Gemini(client),
            config,
        ))
    }

    fn validate_config(
        mut config: AgentRuntimeConfig,
    ) -> Result<AgentRuntimeConfig, AgentRuntimeError> {
        if config.model.trim().is_empty() {
            return Err(AgentRuntimeError::EmptyModel);
        }
        if config.max_model_turns == 0
            || config.max_model_turns > MAX_MODEL_TURNS
            || config.max_concurrent_tool_calls == 0
            || config.max_concurrent_tool_calls > MAX_TOOL_CALLS_PER_MODEL_TURN
            || config.max_turn_output_bytes == 0
            || config.tool_response_budget_chars == 0
        {
            return Err(AgentRuntimeError::InvalidLimits);
        }
        config.pipeline.request_context.model = config.model.clone();
        Ok(config)
    }

    fn with_client(client: RuntimeProviderClient, config: AgentRuntimeConfig) -> Self {
        Self {
            client,
            config,
            prevent_system_sleep: true,
            retry_cancellation: None,
            memory_pressure_compaction: None,
            memory_pressure_check: None,
            session_metrics: RuntimeSessionMetricsCollector::default(),
            image_payload_stores: Mutex::new(IndexMap::new()),
        }
    }

    /// Return a point-in-time copy of collected metrics for a session.
    ///
    /// The snapshot is absent until an eligible model request or tool call has
    /// been recorded. Prompt contents and tool payloads are never retained.
    pub fn session_metrics_snapshot(&self, session_id: &str) -> Option<SessionMetrics> {
        self.session_metrics.snapshot(session_id)
    }

    fn record_model_metrics(
        &self,
        session_id: &str,
        succeeded: bool,
        duration_ms: u64,
        usage: Option<&Value>,
    ) {
        self.session_metrics.record_model_request(
            session_id,
            &self.config.model,
            &self.config.usage_source,
            succeeded,
            duration_ms,
            usage,
        );
    }

    fn record_tool_metrics(
        &self,
        session_id: &str,
        tool_name: &str,
        succeeded: bool,
        duration_ms: u64,
    ) {
        self.session_metrics
            .record_tool_call(session_id, tool_name, succeeded, duration_ms);
    }

    /// Enable or disable the platform sleep inhibitor for agent prompts.
    /// Hosts should map this to the persisted `preventSystemSleep` setting.
    pub fn with_prevent_system_sleep(mut self, enabled: bool) -> Self {
        self.prevent_system_sleep = enabled;
        self
    }

    /// Apply a queued memory-pressure compaction at a safe point between
    /// provider turns. The monitor only sets the request flag; this runtime
    /// owns and rewrites the live provider history.
    pub fn with_memory_pressure_compaction(
        mut self,
        requested: Arc<AtomicBool>,
        file_cache: FileReadCache,
        settings: ClearContextOnIdleSettings,
        preserve_read_file_result: ReadFileRetentionPredicate,
        keep_recent_env: Option<String>,
    ) -> Self {
        self.memory_pressure_compaction = Some(MemoryPressureCompaction {
            requested,
            file_cache,
            settings,
            preserve_read_file_result,
            keep_recent_env,
        });
        self
    }

    /// Request a coalesced native memory check after each tool execution.
    /// The host callback should be non-blocking and bounded; it only signals
    /// the host's serialized memory-pressure task.
    pub fn with_memory_pressure_check(
        mut self,
        request_check: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.memory_pressure_check = Some(request_check);
        self
    }

    /// Attach a cancellation source for provider request setup and retry waits.
    /// Once the SSE stream starts, stream consumption remains governed by the
    /// caller's existing turn lifecycle.
    pub fn with_retry_cancellation(mut self, token: CancellationToken) -> Self {
        self.retry_cancellation = Some(token);
        self
    }

    async fn start_stream_with_retries(
        &self,
        request: &Value,
        context: OpenAiStreamContext,
    ) -> Result<RuntimeProviderStream, RetryWithBackoffError<ProviderError>> {
        let now_ms = system_time_ms;
        let adapter = ProviderRetryAdapter::new(
            RetryErrorClassificationContext {
                auth_type: Some(&self.config.usage_auth_type),
                extra_retry_error_codes: &self.config.pipeline.retry_error_codes,
            },
            &now_ms,
        );
        let classify_error = |error: &ProviderError| adapter.classify(error);
        let default_should_retry = |error: &ProviderError| adapter.should_retry_by_default(error);
        let sleep = |delay_ms| tokio_retry_sleep(delay_ms);
        let mut random = || {
            const MASK_53_BITS: u128 = (1_u128 << 53) - 1;
            (Uuid::new_v4().as_u128() & MASK_53_BITS) as f64 / (1_u64 << 53) as f64
        };
        let mut options = RetryOptions::<ProviderError, RuntimeProviderStream>::new();
        if let Some(max_attempts) = self.config.pipeline.retry_max_attempts {
            options.max_attempts = max_attempts;
        }
        if let Some(initial_delay_ms) = self.config.pipeline.retry_initial_delay_ms {
            options.initial_delay_ms = initial_delay_ms;
        }
        if let Some(max_delay_ms) = self.config.pipeline.retry_max_delay_ms {
            options.max_delay_ms = max_delay_ms;
        }
        options.persistent_mode = is_unattended_mode();
        options.signal = self.retry_cancellation.as_ref();
        let mut hooks = RetryHooks {
            classify_error: &classify_error,
            default_should_retry: &default_should_retry,
            now_ms: &now_ms,
            random: &mut random,
            sleep: &sleep,
            logger: None,
        };
        match &self.client {
            RuntimeProviderClient::OpenAiCompatible(client) => {
                let operation = |_attempt| async {
                    OpenAiGeminiEventStream::start(client, request, context.clone())
                        .await
                        .map(RuntimeProviderStream::OpenAiCompatible)
                };
                let retry = retry_with_backoff(operation, &options, &mut hooks);
                if let Some(token) = self.retry_cancellation.as_ref() {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => Err(RetryWithBackoffError::Cancelled),
                        result = retry => result,
                    }
                } else {
                    retry.await
                }
            }
            RuntimeProviderClient::Anthropic(client) => {
                let cancellation = self.retry_cancellation.clone().unwrap_or_default();
                let operation = |_attempt| async {
                    client
                        .stream_gemini(request, Some(cancellation.clone()))
                        .await
                        .map(RuntimeProviderStream::Anthropic)
                };
                let retry = retry_with_backoff(operation, &options, &mut hooks);
                if let Some(token) = self.retry_cancellation.as_ref() {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => Err(RetryWithBackoffError::Cancelled),
                        result = retry => result,
                    }
                } else {
                    retry.await
                }
            }
            RuntimeProviderClient::Gemini(client) => {
                let cancellation = self.retry_cancellation.clone().unwrap_or_default();
                let operation = |_attempt| async {
                    client
                        .stream_gemini(
                            request,
                            self.config.pipeline.sampling_params.as_ref(),
                            self.config.pipeline.reasoning.as_ref(),
                            Some(cancellation.clone()),
                        )
                        .await
                        .map(RuntimeProviderStream::Gemini)
                };
                let retry = retry_with_backoff(operation, &options, &mut hooks);
                if let Some(token) = self.retry_cancellation.as_ref() {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => Err(RetryWithBackoffError::Cancelled),
                        result = retry => result,
                    }
                } else {
                    retry.await
                }
            }
        }
    }

    pub async fn run_prompt<E, F>(
        &self,
        prompt: &str,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        self.run_prompt_with_history(prompt, Vec::new(), recorder, executor, emit)
            .await
    }

    /// Start a new prompt on top of an already recovered, provider-facing
    /// history. The recorder is expected to own the same session represented
    /// by that history.
    pub async fn run_prompt_with_history<E, F>(
        &self,
        prompt: &str,
        history: Vec<Value>,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        self.run_history(
            Some(prompt),
            None,
            history,
            self.config.system_instruction.clone(),
            recorder,
            executor,
            emit,
        )
        .await
    }

    /// Start a prompt with a session-specific system instruction. This is for
    /// hosts whose instruction depends on the incoming prompt, such as ACP
    /// memory recall; the runtime-wide instruction is left unchanged.
    pub async fn run_prompt_with_history_and_system_instruction<E, F>(
        &self,
        prompt: &str,
        history: Vec<Value>,
        system_instruction: Option<Value>,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        self.run_history(
            Some(prompt),
            None,
            history,
            system_instruction,
            recorder,
            executor,
            emit,
        )
        .await
    }

    /// Start a user turn whose model-facing message has multiple parts while
    /// keeping `display_text` as the visible prompt in transcript projections.
    /// This lets native hosts add read-only reference material to the same user
    /// turn without recording that injected context as text the user typed.
    pub async fn run_prompt_with_parts_and_history_and_system_instruction<E, F>(
        &self,
        display_text: &str,
        prompt_parts: Vec<Value>,
        history: Vec<Value>,
        system_instruction: Option<Value>,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        self.run_history(
            Some(display_text),
            Some(prompt_parts),
            history,
            system_instruction,
            recorder,
            executor,
            emit,
        )
        .await
    }

    /// Continue an interrupted session from its recovered API history without
    /// appending a second user prompt.
    pub async fn continue_from_history<E, F>(
        &self,
        history: Vec<Value>,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        self.run_history(
            None,
            None,
            history,
            self.config.system_instruction.clone(),
            recorder,
            executor,
            emit,
        )
        .await
    }

    async fn run_history<E, F>(
        &self,
        prompt: Option<&str>,
        prompt_parts: Option<Vec<Value>>,
        mut history: Vec<Value>,
        system_instruction: Option<Value>,
        recorder: &mut SessionRecorder,
        executor: &mut E,
        mut emit: F,
    ) -> Result<AgentRunSummary, AgentRuntimeError>
    where
        E: AgentToolExecutor,
        F: FnMut(AgentRunEvent) -> Result<(), String>,
    {
        let mut history_bytes = history.iter().fold(0usize, |total, content| {
            total.saturating_add(serde_json::to_vec(content).map_or(0, |bytes| bytes.len()))
        });
        self.compact_images_under_pressure(
            &mut history,
            &mut history_bytes,
            recorder.session_id(),
        )?;
        let _sleep_inhibitor = self.prevent_system_sleep.then(|| {
            crate::services::sleep_inhibitor::acquire("Canopy Code is processing a request")
        });
        let prompt_id = Uuid::new_v4().to_string();
        let mut loop_detector = LoopDetectionService::new(LoopDetectionConfig::default());
        loop_detector.reset(prompt_id.clone());
        if let Some(prompt) = prompt {
            let custom_parts = prompt_parts.is_some();
            let parts = prompt_parts
                .as_ref()
                .cloned()
                .unwrap_or_else(|| vec![json!({"text":prompt})]);
            let user_message = json!({"role":"user","parts":parts});
            let user_message_bytes =
                serde_json::to_vec(&user_message).map_or(0, |bytes| bytes.len());
            let candidate_image_bytes = inline_image_data_bytes(&history)
                .saturating_add(inline_image_data_bytes(std::slice::from_ref(&user_message)));
            if history_bytes.saturating_add(user_message_bytes) > MAX_HISTORY_BYTES
                && candidate_image_bytes == 0
            {
                return Err(AgentRuntimeError::HistoryTooLarge {
                    limit: MAX_HISTORY_BYTES,
                });
            }
            history.push(user_message);
            history_bytes = history.iter().fold(0usize, |total, content| {
                total.saturating_add(serde_json::to_vec(content).map_or(0, |bytes| bytes.len()))
            });
            self.compact_images_under_pressure(
                &mut history,
                &mut history_bytes,
                recorder.session_id(),
            )?;
            recorder.record_user_message(
                prompt_parts
                    .map(Value::Array)
                    .unwrap_or_else(|| Value::String(prompt.to_owned())),
                self.config.goal_context.clone(),
                Some(if custom_parts {
                    json!({"displayText":prompt,"hookContext":"injected prompt parts"})
                } else {
                    json!({"displayText":prompt})
                }),
            )?;
            let _ = executor.begin_user_prompt(&prompt_id).await;
        }
        // Also drains validation or restore updates when continuing from
        // history, which deliberately does not create a new prompt snapshot.
        persist_file_history_snapshot_updates(executor, recorder);
        persist_commit_attribution_snapshot(executor, recorder);
        let mut summary = AgentRunSummary::default();
        let mut previous_tool_signature: Option<String> = None;
        let mut repeated_tool_count = 0usize;
        let mut active_todo_reminder = ActiveTodoReminder::default();

        for model_turn in 0..self.config.max_model_turns {
            self.compact_history_if_requested(&mut history, &mut history_bytes);
            self.compact_images_under_pressure(
                &mut history,
                &mut history_bytes,
                recorder.session_id(),
            )?;
            let request = self.build_request(
                &mut history,
                recorder.session_id(),
                system_instruction.as_ref(),
            )?;
            history_bytes = history.iter().fold(0usize, |total, content| {
                total.saturating_add(serde_json::to_vec(content).map_or(0, |bytes| bytes.len()))
            });
            if history_bytes > MAX_HISTORY_BYTES {
                return Err(AgentRuntimeError::HistoryTooLarge {
                    limit: MAX_HISTORY_BYTES,
                });
            }
            let api_request_started = Instant::now();
            let mut stream = match self
                .start_stream_with_retries(
                    &request,
                    OpenAiStreamContext::new(self.config.model.clone()),
                )
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    self.record_model_metrics(
                        recorder.session_id(),
                        false,
                        elapsed_milliseconds(api_request_started),
                        None,
                    );
                    return Err(error.into());
                }
            };
            let mut turn = Turn::new(prompt_id.clone(), self.config.goal_context.clone());
            let mut response_parts = Vec::new();
            let mut response_bytes = 0usize;
            let mut usage_metadata = None;
            let mut model_metrics_recorded = false;

            loop {
                let chunk = match stream.next_chunk().await {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break,
                    Err(RuntimeProviderStreamError::OpenAiCompatible(error)) => {
                        if !model_metrics_recorded {
                            self.record_model_metrics(
                                recorder.session_id(),
                                false,
                                elapsed_milliseconds(api_request_started),
                                None,
                            );
                        }
                        return Err(AgentRuntimeError::Stream(error));
                    }
                    Err(RuntimeProviderStreamError::Anthropic(error)) => {
                        if !model_metrics_recorded {
                            self.record_model_metrics(
                                recorder.session_id(),
                                false,
                                elapsed_milliseconds(api_request_started),
                                None,
                            );
                        }
                        return Err(AgentRuntimeError::AnthropicStream(error));
                    }
                    Err(RuntimeProviderStreamError::Gemini(error)) => {
                        if !model_metrics_recorded {
                            self.record_model_metrics(
                                recorder.session_id(),
                                false,
                                elapsed_milliseconds(api_request_started),
                                None,
                            );
                        }
                        return Err(AgentRuntimeError::Provider(error));
                    }
                };
                if let Some(parts) = chunk
                    .response
                    .pointer("/candidates/0/content/parts")
                    .and_then(Value::as_array)
                {
                    for part in parts {
                        let part_bytes = serde_json::to_vec(part)
                            .map_or(0, |bytes| bytes.len())
                            .saturating_add(16);
                        response_bytes = response_bytes.saturating_add(part_bytes);
                        if response_bytes > self.config.max_turn_output_bytes {
                            if !model_metrics_recorded {
                                self.record_model_metrics(
                                    recorder.session_id(),
                                    false,
                                    elapsed_milliseconds(api_request_started),
                                    None,
                                );
                            }
                            return Err(AgentRuntimeError::TurnOutputTooLarge {
                                limit: self.config.max_turn_output_bytes,
                            });
                        }
                        response_parts.push(part.clone());
                    }
                }
                if let Some(usage) = chunk.response.get("usageMetadata") {
                    usage_metadata = Some(usage.clone());
                }
                let events =
                    match turn.accept_response(&chunk.response, chunk.openai_reasoning_thought) {
                        Ok(events) => events,
                        Err(error) => {
                            if !model_metrics_recorded {
                                self.record_model_metrics(
                                    recorder.session_id(),
                                    false,
                                    elapsed_milliseconds(api_request_started),
                                    None,
                                );
                            }
                            return Err(error.into());
                        }
                    };
                if turn.finish_reason.is_some() && !model_metrics_recorded {
                    self.record_model_metrics(
                        recorder.session_id(),
                        true,
                        elapsed_milliseconds(api_request_started),
                        usage_metadata.as_ref(),
                    );
                    model_metrics_recorded = true;
                }
                for event in events {
                    if loop_detector.add_and_check(&event) {
                        if !model_metrics_recorded {
                            self.record_model_metrics(
                                recorder.session_id(),
                                true,
                                elapsed_milliseconds(api_request_started),
                                usage_metadata.as_ref(),
                            );
                        }
                        let loop_type = loop_detector
                            .last_loop_type()
                            .map(|loop_type| loop_type.as_str())
                            .unwrap_or("unknown");
                        let mut payload = json!({
                            "loopType": loop_type,
                            "promptId": prompt_id,
                        });
                        if let TurnEvent::ToolCallRequest { value } = &event {
                            payload["callId"] = Value::String(value.call_id.clone());
                            payload["name"] = Value::String(value.name.clone());
                        }
                        recorder.record_system_event("loop_detected", payload)?;
                        return Err(AgentRuntimeError::LoopDetected {
                            loop_type: loop_type.to_owned(),
                        });
                    }
                    if let Err(error) = emit(AgentRunEvent::Turn(event)) {
                        if !model_metrics_recorded {
                            self.record_model_metrics(
                                recorder.session_id(),
                                turn.finish_reason.is_some(),
                                elapsed_milliseconds(api_request_started),
                                usage_metadata.as_ref(),
                            );
                        }
                        return Err(AgentRuntimeError::EventConsumer(error));
                    }
                }
            }

            let api_duration_ms = api_request_started
                .elapsed()
                .as_millis()
                .min(u64::MAX as u128) as u64;
            let usage_for_statistics = if self.config.usage_statistics_enabled
                && !crate::utils::internal_prompt_ids::is_internal_prompt_id(Some(&prompt_id))
            {
                usage_metadata.clone()
            } else {
                None
            };
            summary.model_turns = model_turn + 1;
            summary.finish_reason = turn.finish_reason.clone();
            let assistant_message = (!response_parts.is_empty())
                .then(|| json!({"role":"model","parts":response_parts}));
            recorder.record_assistant_turn(
                self.config.model.clone(),
                assistant_message
                    .as_ref()
                    .and_then(|message| message.get("parts"))
                    .cloned(),
                usage_metadata,
                self.config.context_window_size,
                self.config.goal_context.clone(),
            )?;
            if let Some(assistant_message) = assistant_message {
                history_bytes = history_bytes
                    .saturating_add(response_bytes)
                    .saturating_add(32);
                history.push(assistant_message);
            }
            self.compact_images_under_pressure(
                &mut history,
                &mut history_bytes,
                recorder.session_id(),
            )?;

            if turn.finish_reason.is_none() {
                if !model_metrics_recorded {
                    self.record_model_metrics(recorder.session_id(), false, api_duration_ms, None);
                }
                return Err(AgentRuntimeError::IncompleteProviderTurn);
            }
            if let Some(usage_metadata) = usage_for_statistics {
                record_token_usage_best_effort(
                    recorder.runtime_base_dir().to_path_buf(),
                    recorder.session_id().to_owned(),
                    self.config.model.clone(),
                    self.config.usage_auth_type.clone(),
                    self.config.usage_source.clone(),
                    usage_metadata,
                    api_duration_ms,
                )
                .await;
            }
            if turn.pending_tool_calls.is_empty() {
                self.compact_history_if_requested(&mut history, &mut history_bytes);
                return Ok(summary);
            }
            if history_bytes > MAX_HISTORY_BYTES {
                return Err(AgentRuntimeError::HistoryTooLarge {
                    limit: MAX_HISTORY_BYTES,
                });
            }
            let requested_tool_calls = summary
                .tool_calls
                .saturating_add(turn.pending_tool_calls.len());
            if requested_tool_calls > MAX_TOOL_CALLS_PER_MODEL_TURN {
                return Err(AgentRuntimeError::ToolCallLimit {
                    requested: requested_tool_calls,
                    limit: MAX_TOOL_CALLS_PER_MODEL_TURN,
                });
            }
            if model_turn + 1 >= self.config.max_model_turns {
                return Err(AgentRuntimeError::ModelTurnLimit(
                    self.config.max_model_turns,
                ));
            }

            let mut budget_entries = Vec::with_capacity(turn.pending_tool_calls.len());
            let mut call_metadata = Vec::with_capacity(turn.pending_tool_calls.len());
            let mut batch_output_bytes = 0usize;
            let mut call_results = Vec::with_capacity(turn.pending_tool_calls.len());
            let mut call_index = 0;
            while call_index < turn.pending_tool_calls.len() {
                let call = &turn.pending_tool_calls[call_index];
                let concurrency_safe =
                    executor.is_concurrency_safe(call) && !executor.is_side_effecting(&call.name);
                let mut batch_end = call_index + 1;
                if concurrency_safe {
                    while batch_end < turn.pending_tool_calls.len()
                        && batch_end - call_index < self.config.max_concurrent_tool_calls
                    {
                        let next = &turn.pending_tool_calls[batch_end];
                        if !executor.is_concurrency_safe(next)
                            || executor.is_side_effecting(&next.name)
                        {
                            break;
                        }
                        batch_end += 1;
                    }
                }
                let calls = &turn.pending_tool_calls[call_index..batch_end];

                for call in calls {
                    let signature =
                        serde_json::to_string(&json!({"name":call.name,"args":call.args}))
                            .unwrap_or_else(|_| call.name.clone());
                    if previous_tool_signature.as_deref() == Some(signature.as_str()) {
                        repeated_tool_count = repeated_tool_count.saturating_add(1);
                    } else {
                        previous_tool_signature = Some(signature);
                        repeated_tool_count = 1;
                    }
                    if repeated_tool_count >= CONSECUTIVE_IDENTICAL_TOOL_LIMIT {
                        let _ = recorder.record_system_event(
                            "loop_detected",
                            json!({
                                "loopType":"consecutive_identical_tool_calls",
                                "callId":call.call_id,
                                "name":call.name,
                            }),
                        );
                        return Err(AgentRuntimeError::RepeatedToolCall {
                            count: repeated_tool_count,
                        });
                    }

                    emit(AgentRunEvent::ToolExecutionStarted {
                        call_id: call.call_id.clone(),
                        name: call.name.clone(),
                    })
                    .map_err(AgentRuntimeError::EventConsumer)?;

                    if executor.is_side_effecting(&call.name) {
                        record_tool_execution_intent(
                            recorder,
                            call.call_id.clone(),
                            call.name.clone(),
                        )?;
                    }
                }

                let request_memory_check = self.memory_pressure_check.as_deref();
                let outputs = if concurrency_safe && calls.len() > 1 {
                    join_all(
                        calls
                            .iter()
                            .map(|call| execute_tool_call(&*executor, call, request_memory_check)),
                    )
                    .await
                } else {
                    let mut outputs = Vec::with_capacity(calls.len());
                    for call in calls {
                        outputs
                            .push(execute_tool_call(&*executor, call, request_memory_check).await);
                    }
                    outputs
                };
                call_results.extend(calls.iter().cloned().zip(outputs));
                call_index = batch_end;
                persist_file_history_snapshot_updates(executor, recorder);
                persist_commit_attribution_snapshot(executor, recorder);
            }

            for (call, (execution_result, duration_ms)) in call_results {
                let (
                    response,
                    response_parts,
                    display,
                    original_output_bytes,
                    result_file_paths,
                    artifacts,
                ) = match execution_result {
                    Ok(output) => {
                        let estimated_bytes = output.estimated_bytes();
                        if estimated_bytes > MAX_SINGLE_TOOL_OUTPUT_BYTES
                            || batch_output_bytes.saturating_add(estimated_bytes)
                                > MAX_TOOL_BATCH_OUTPUT_BYTES
                        {
                            let message = "Tool output exceeded the Rust runtime output limit.";
                            let message_length = message.len();
                            (
                                json!({"error":message}),
                                Vec::new(),
                                None,
                                message_length,
                                Vec::new(),
                                Vec::new(),
                            )
                        } else {
                            batch_output_bytes = batch_output_bytes.saturating_add(estimated_bytes);
                            let output_length = output.output.len();
                            let display = output.display;
                            let result_file_paths = output.result_file_paths;
                            let artifacts = output.artifacts;
                            if call.name == "todo_write" {
                                if let Some(reminder_update) =
                                    display.as_ref().and_then(active_todo_reminder_update)
                                {
                                    active_todo_reminder.update(reminder_update);
                                }
                            }
                            (
                                json!({"output":output.output}),
                                output.parts,
                                display,
                                output_length,
                                result_file_paths,
                                artifacts,
                            )
                        }
                    }
                    Err(error) => {
                        let error = if error.len() > 4096 {
                            error.chars().take(4096).collect::<String>()
                        } else {
                            error
                        };
                        let error_length = error.len();
                        (
                            json!({"error":error}),
                            Vec::new(),
                            None,
                            error_length,
                            Vec::new(),
                            Vec::new(),
                        )
                    }
                };
                self.record_tool_metrics(
                    recorder.session_id(),
                    &call.name,
                    response.get("output").is_some(),
                    duration_ms,
                );

                let mut function_response = json!({
                    "id":call.call_id,
                    "name":call.name,
                    "response":response,
                });
                if !response_parts.is_empty() {
                    function_response["parts"] = Value::Array(response_parts);
                }
                let response_part = json!({"functionResponse":function_response});
                budget_entries.push(ToolResponseBudgetEntry {
                    call_id: call.call_id.clone(),
                    tool_name: call.name.clone(),
                    response_parts: vec![response_part],
                    persisted_output_files: None,
                });
                call_metadata.push((
                    call,
                    original_output_bytes,
                    display,
                    result_file_paths,
                    artifacts,
                ));
            }

            let reminder_for_batch = active_todo_reminder.take_for_tool_result();

            let output_dir = self.config.tool_output_dir.join(recorder.session_id());
            let mut output_store = FileToolOutputStore::new(output_dir);
            finalize_tool_responses(
                &mut budget_entries,
                Some(self.config.tool_response_budget_chars as f64),
                &[],
                &mut output_store,
            )
            .await;

            for (entry, (call, original_output_bytes, display, result_file_paths, artifacts)) in
                budget_entries.into_iter().zip(call_metadata)
            {
                let mut response_part = entry
                    .response_parts
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Null);
                let model_visible_response = response_part
                    .pointer("/functionResponse/response")
                    .cloned()
                    .unwrap_or(Value::Null);
                let model_visible_text = model_visible_response
                    .get("output")
                    .or_else(|| model_visible_response.get("error"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let was_truncated = model_visible_text.len() != original_output_bytes;

                // PostToolUse context is added only to successful tool
                // responses, after finalization has applied its truncation and
                // persistence policy. Escape reminder tags in host-provided
                // text so context cannot inject a forged reminder envelope.
                if response_part
                    .pointer("/functionResponse/response/output")
                    .is_some()
                    && let Some(additional_context) = executor
                        .additional_context_after_tool_use(&call, &result_file_paths)
                        .await
                    && !additional_context.is_empty()
                {
                    let escaped_context = escape_system_reminder_tags(&additional_context);
                    let reminder =
                        format!("<system-reminder>\n{escaped_context}\n</system-reminder>");
                    if let Some(Value::String(output)) =
                        response_part.pointer_mut("/functionResponse/response/output")
                    {
                        output.push_str("\n\n");
                        output.push_str(&reminder);
                    }
                }

                let model_visible_response = response_part
                    .pointer("/functionResponse/response")
                    .cloned()
                    .unwrap_or(Value::Null);
                let tool_result_message = json!({
                    "role":"user",
                    "parts":[response_part.clone()],
                });
                history_bytes = history_bytes
                    .saturating_add(
                        serde_json::to_vec(&tool_result_message).map_or(0, |bytes| bytes.len()),
                    )
                    .saturating_add(32);
                let tool_result_part = response_part.clone();
                let tool_call_result = json!({
                    "callId":call.call_id,
                    "name":call.name,
                    "args":call.args,
                    "response":model_visible_response,
                    "persistedOutputFiles":entry.persisted_output_files.clone(),
                });
                let mut tool_call_result = tool_call_result;
                if !artifacts.is_empty() {
                    tool_call_result["artifacts"] = Value::Array(artifacts);
                }
                if let Some(display_value) = display.as_ref() {
                    tool_call_result["resultDisplay"] = display_value.clone();
                }
                recorder.record_tool_result(
                    tool_result_part,
                    Some(tool_call_result),
                    call.goal_context.clone(),
                    Some("tool_result".to_owned()),
                )?;
                history.push(tool_result_message);
                self.compact_images_under_pressure(
                    &mut history,
                    &mut history_bytes,
                    recorder.session_id(),
                )?;
                summary.tool_calls = summary.tool_calls.saturating_add(1);
                emit(AgentRunEvent::ToolExecutionFinished {
                    call_id: call.call_id,
                    name: call.name,
                    response: model_visible_response,
                    display,
                    was_truncated,
                })
                .map_err(AgentRuntimeError::EventConsumer)?;
            }
            if let Some(reminder) = reminder_for_batch {
                let reminder_message = json!({
                    "role":"user",
                    "parts":[{"text":reminder}],
                });
                history_bytes = history_bytes.saturating_add(
                    serde_json::to_vec(&reminder_message).map_or(0, |bytes| bytes.len()),
                );
                history.push(reminder_message);
                self.compact_images_under_pressure(
                    &mut history,
                    &mut history_bytes,
                    recorder.session_id(),
                )?;
            }
        }

        self.compact_history_if_requested(&mut history, &mut history_bytes);
        Err(AgentRuntimeError::ModelTurnLimit(
            self.config.max_model_turns,
        ))
    }

    fn compact_history_if_requested(&self, history: &mut Vec<Value>, history_bytes: &mut usize) {
        let Some(compaction) = &self.memory_pressure_compaction else {
            return;
        };
        if !compaction.requested.swap(false, Ordering::AcqRel) {
            return;
        }

        let replacement = {
            let preserve_read_file_result: Option<&dyn Fn(&str) -> bool> =
                Some(compaction.preserve_read_file_result.as_ref());
            let configured_threshold = compaction
                .settings
                .tool_results_threshold_minutes
                .unwrap_or(0.0);
            let pressure_settings = ClearContextOnIdleSettings {
                tool_results_threshold_minutes: Some(if configured_threshold < 0.0 {
                    configured_threshold
                } else {
                    0.0
                }),
                ..compaction.settings
            };
            let now_ms = system_time_ms();
            let result = microcompact_history_with_env(
                history,
                Some(now_ms - 1.0),
                &pressure_settings,
                MicrocompactOptions {
                    preserve_read_file_result,
                    ..MicrocompactOptions::default()
                },
                compaction.keep_recent_env.as_deref(),
                now_ms,
            );
            result.meta.map(|meta| (result.history.into_owned(), meta))
        };
        let Some((compacted_history, meta)) = replacement else {
            return;
        };

        *history = compacted_history;
        *history_bytes = history.iter().fold(0usize, |total, content| {
            total.saturating_add(serde_json::to_vec(content).map_or(0, |bytes| bytes.len()))
        });
        compaction.file_cache.clear();
        eprintln!(
            "[CANOPY] Memory-pressure compaction cleared {} tool result(s) and {} media item(s), saving about {} tokens",
            meta.tools_cleared, meta.media_cleared, meta.tokens_saved
        );
    }

    /// Move image bytes out of retained history before the serialized-history
    /// limit becomes a hard failure. The 6 MiB trigger leaves room below both
    /// the 12 MiB history cap and the 8 MiB per-session image cache cap.
    fn compact_images_under_pressure(
        &self,
        history: &mut Vec<Value>,
        history_bytes: &mut usize,
        session_id: &str,
    ) -> Result<(), AgentRuntimeError> {
        *history_bytes = serialized_history_bytes(history);
        let inline_image_bytes = inline_image_data_bytes(history);
        let history_over_cap = *history_bytes > MAX_HISTORY_BYTES;
        if history_over_cap && inline_image_bytes == 0 {
            return Err(AgentRuntimeError::HistoryTooLarge {
                limit: MAX_HISTORY_BYTES,
            });
        }

        let near_cap = *history_bytes
            >= MAX_HISTORY_BYTES.saturating_sub(IMAGE_HISTORY_PRESSURE_HEADROOM_BYTES);
        let byte_pressure = inline_image_bytes >= IMAGE_HISTORY_BYTE_PRESSURE_THRESHOLD;
        if !history_over_cap && !near_cap && !byte_pressure {
            return Ok(());
        }

        let tuning = resolve_compaction_tuning(None);
        let preserved = self.externalize_history_images(
            history,
            session_id,
            tuning.max_recent_images,
            history_over_cap,
        );
        *history_bytes = serialized_history_bytes(history);
        if !preserved || *history_bytes > MAX_HISTORY_BYTES {
            return Err(AgentRuntimeError::HistoryTooLarge {
                limit: MAX_HISTORY_BYTES,
            });
        }
        Ok(())
    }

    /// Replace image payloads in retained history, keep the selected payloads
    /// in the existing bounded session cache, and add references for the next
    /// request. The references make a pre-request compaction equivalent to the
    /// request-time path: the next request restores those image parts.
    fn externalize_history_images(
        &self,
        history: &mut Vec<Value>,
        session_id: &str,
        max_recent_images: usize,
        include_latest_user: bool,
    ) -> bool {
        if inline_image_data_bytes(history) == 0 {
            return true;
        }

        let explicit_ids = collect_referenced_image_ids(history.last());
        let latest_user_index = history
            .last()
            .filter(|content| {
                !include_latest_user && content.get("role").and_then(Value::as_str) == Some("user")
            })
            .map(|_| history.len().saturating_sub(1));
        let mut stores = self
            .image_payload_stores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous_cache = stores.shift_remove(session_id).unwrap_or_default();
        let mut store = previous_cache.store;
        let previous_ids = previous_cache.ids;
        let (replaced, put_ids) = {
            let mut tracked = TrackedImagePayloadStore::new(&mut store);
            let replaced =
                replace_image_payloads_in_place(history, &mut tracked, latest_user_index);
            (replaced, std::mem::take(&mut tracked.put_ids))
        };
        if replaced.is_empty() {
            stores.insert(
                session_id.to_owned(),
                RuntimeImagePayloadCache {
                    store,
                    ids: previous_ids,
                },
            );
            return true;
        }

        let latest_reference_ids = collect_referenced_image_ids(history.last());
        let new_latest_ids = latest_reference_ids
            .iter()
            .filter(|id| !explicit_ids.contains(id))
            .cloned()
            .collect::<Vec<_>>();
        let mut reattach_by_id = IndexMap::<String, StoredImagePayload>::new();
        for image in recent_unique_images(&replaced, Some(max_recent_images as f64)) {
            reattach_by_id.insert(image.id.clone(), image.clone());
        }
        for id in explicit_ids.iter().chain(new_latest_ids.iter()) {
            if !reattach_by_id.contains_key(id)
                && let Some(image) = store.get(id)
            {
                reattach_by_id.insert(image.id.clone(), image);
            }
        }

        let mut retain_ids = explicit_ids.clone();
        let mut seen = retain_ids.iter().cloned().collect::<HashSet<_>>();
        for id in new_latest_ids.iter().chain(reattach_by_id.keys()) {
            if seen.insert(id.clone()) {
                retain_ids.push(id.clone());
            }
        }
        for id in put_ids.into_iter().rev().chain(previous_ids) {
            if seen.insert(id.clone()) {
                retain_ids.push(id);
            }
        }
        let retained_cache = bounded_image_payload_cache(&store, &retain_ids);
        let retained_ids = retained_cache.ids.iter().cloned().collect::<HashSet<_>>();
        let required_explicit_was_retained = explicit_ids
            .iter()
            .all(|id| !reattach_by_id.contains_key(id) || retained_ids.contains(id));
        let required_latest_was_retained =
            !include_latest_user || new_latest_ids.iter().all(|id| retained_ids.contains(id));
        let required_request_context_was_retained =
            reattach_by_id.keys().all(|id| retained_ids.contains(id));
        let retained_references = reattach_by_id
            .values()
            .filter(|image| retained_ids.contains(&image.id))
            .cloned()
            .collect::<Vec<_>>();
        drop(store);
        stores.insert(session_id.to_owned(), retained_cache);
        while stores.len() > MAX_IMAGE_PAYLOAD_CACHE_SESSIONS {
            stores.shift_remove_index(0);
        }

        if !retained_references.is_empty() {
            append_image_reference_summary(history, &retained_references);
        }
        required_explicit_was_retained
            && required_latest_was_retained
            && required_request_context_was_retained
    }

    fn build_request(
        &self,
        history: &mut Vec<Value>,
        session_id: &str,
        system_instruction: Option<&Value>,
    ) -> Result<Value, AgentRuntimeError> {
        let image_payload_tuning = resolve_compaction_tuning(None);
        let transformed_history = if count_all_inline_images(history)
            >= image_payload_tuning.image_payload_threshold
            || inline_image_data_bytes(history) >= IMAGE_HISTORY_BYTE_PRESSURE_THRESHOLD
            || !collect_referenced_image_ids(history.last()).is_empty()
            || !collect_recent_history_image_references(
                history,
                image_payload_tuning.max_recent_images,
            )
            .is_empty()
        {
            let skip_content_index = history
                .last()
                .filter(|content| content.get("role").and_then(Value::as_str) == Some("user"))
                .map(|_| history.len().saturating_sub(1));
            let mut referenced_ids = collect_referenced_image_ids(history.last());
            let mut referenced_seen = referenced_ids.iter().cloned().collect::<HashSet<_>>();
            for id in collect_recent_history_image_references(
                history,
                image_payload_tuning.max_recent_images,
            ) {
                if referenced_seen.insert(id.clone()) {
                    referenced_ids.push(id);
                }
            }
            let mut stores = self
                .image_payload_stores
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Retain the previous bounded cache so older images can still be
            // restored by a later explicit reference. This replacement pass
            // moves older history images into the same cache.
            let previous_cache = stores.shift_remove(session_id).unwrap_or_default();
            let mut store = previous_cache.store;
            let previous_ids = previous_cache.ids;
            let (replaced, put_ids) = {
                let mut tracked = TrackedImagePayloadStore::new(&mut store);
                let replaced =
                    replace_image_payloads_in_place(history, &mut tracked, skip_content_index);
                (replaced, std::mem::take(&mut tracked.put_ids))
            };

            let mut reattach_by_id = IndexMap::<String, StoredImagePayload>::new();
            for image in recent_unique_images(
                &replaced,
                Some(image_payload_tuning.max_recent_images as f64),
            ) {
                reattach_by_id.insert(image.id.clone(), image.clone());
            }
            for id in &referenced_ids {
                if !reattach_by_id.contains_key(id)
                    && let Some(image) = store.get(id)
                {
                    reattach_by_id.insert(image.id.clone(), image);
                }
            }
            let reattach_payloads = reattach_by_id.into_values().collect::<Vec<_>>();
            let reattach_parts = build_reattach_parts(&reattach_payloads, None);

            // Replace the retained history in place. Only the request copy
            // gets the recent image bytes back, so old screenshots do not
            // accumulate in the session's live history.
            let mut transformed = history.clone();
            if !reattach_parts.is_empty() {
                if transformed.last().is_some_and(|content| {
                    content.get("role").and_then(Value::as_str) == Some("user")
                }) {
                    let last = transformed.last_mut().expect("last user content exists");
                    if let Some(parts) = last.get_mut("parts").and_then(Value::as_array_mut) {
                        parts.extend(reattach_parts);
                    } else {
                        last["parts"] = Value::Array(reattach_parts);
                    }
                } else {
                    transformed.push(json!({"role":"user","parts":reattach_parts}));
                }
            }

            // Keep explicit references first, then the newest images attached
            // to this request. The IDs come from the generated reattachment
            // summary, so this reflects exactly what the request can restore.
            let mut retain_ids = referenced_ids;
            let mut seen = retain_ids.iter().cloned().collect::<HashSet<_>>();
            for id in reattached_image_ids(&transformed).into_iter().rev() {
                if seen.insert(id.clone()) {
                    retain_ids.push(id);
                }
            }
            // Consider newly encountered history images newest-first, then
            // older cache entries in their prior MRU order. Explicit and
            // reattached IDs above take priority when the cap is exceeded.
            for id in put_ids.into_iter().rev().chain(previous_ids) {
                if seen.insert(id.clone()) {
                    retain_ids.push(id);
                }
            }
            let retained_cache = bounded_image_payload_cache(&store, &retain_ids);
            drop(store);
            stores.insert(session_id.to_owned(), retained_cache);
            while stores.len() > MAX_IMAGE_PAYLOAD_CACHE_SESSIONS {
                stores.shift_remove_index(0);
            }
            Some(transformed)
        } else {
            let mut stores = self
                .image_payload_stores
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cache) = stores.shift_remove(session_id) {
                stores.insert(session_id.to_owned(), cache);
            }
            None
        };
        let request_history = transformed_history.as_deref().unwrap_or(history.as_slice());
        let mut request_config = serde_json::Map::new();
        if let Some(system_instruction) = system_instruction {
            request_config.insert("systemInstruction".to_owned(), system_instruction.clone());
        }
        if !self.config.tool_declarations.is_empty() {
            request_config.insert(
                "tools".to_owned(),
                Value::Array(self.config.tool_declarations.clone()),
            );
        }
        let request = json!({
            "model":self.config.model,
            "contents":request_history,
            "config":request_config,
        });
        match &self.client {
            RuntimeProviderClient::Gemini(_) => Ok(request),
            _ => Ok(build_openai_request(
                &request,
                &self.config.pipeline,
                "rust-agent-turn",
                true,
                |request, _prompt_id| request,
            )),
        }
    }
}

fn serialized_history_bytes(history: &[Value]) -> usize {
    history.iter().fold(0usize, |total, content| {
        total.saturating_add(serde_json::to_vec(content).map_or(0, |bytes| bytes.len()))
    })
}

fn append_image_reference_summary(history: &mut Vec<Value>, images: &[StoredImagePayload]) {
    let references = images
        .iter()
        .map(|image| format!("Image #{}", image.id))
        .collect::<Vec<_>>()
        .join(", ");
    if references.is_empty() {
        return;
    }
    let summary = format!("{IMAGE_REATTACH_SUMMARY_PREFIX} {references}");
    if let Some(last) = history.last_mut() {
        if let Some(parts) = last.get_mut("parts").and_then(Value::as_array_mut) {
            parts.push(json!({"text":summary}));
        } else {
            last["parts"] = json!([{"text":summary}]);
        }
    } else {
        history.push(json!({"role":"user","parts":[{"text":summary}]}));
    }
}

fn collect_referenced_image_ids(content: Option<&Value>) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    let Some(parts) = content
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
    else {
        return ids;
    };
    for part in parts {
        collect_image_references_in_part(part, &mut ids, &mut seen);
    }
    ids
}

fn collect_recent_history_image_references(
    contents: &[Value],
    max_recent_images: usize,
) -> Vec<String> {
    if max_recent_images == 0 {
        return Vec::new();
    }
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for content in contents {
        if let Some(parts) = content.get("parts").and_then(Value::as_array) {
            for part in parts {
                collect_image_references_in_part(part, &mut ids, &mut seen);
            }
        }
    }
    if ids.len() > max_recent_images {
        ids.drain(..ids.len() - max_recent_images);
    }
    ids
}

fn collect_image_references_in_part(
    part: &Value,
    ids: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    if let Some(text) = part.get("text").and_then(Value::as_str) {
        for id in image_reference_ids_in_text(text) {
            if seen.insert(id.clone()) {
                ids.push(id);
            }
        }
    }
    if let Some(nested_parts) = part
        .get("functionResponse")
        .and_then(|response| response.get("parts"))
        .and_then(Value::as_array)
    {
        for nested in nested_parts {
            collect_image_references_in_part(nested, ids, seen);
        }
    }
}

fn reattached_image_ids(contents: &[Value]) -> Vec<String> {
    let Some(parts) = contents
        .last()
        .and_then(|content| content.get("parts"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    for part in parts.iter().rev() {
        let Some(text) = part.get("text").and_then(Value::as_str) else {
            continue;
        };
        if text.starts_with(IMAGE_REATTACH_SUMMARY_PREFIX) {
            return image_reference_ids_in_text(text);
        }
    }
    Vec::new()
}

/// Match the image reference expression used by the payload service:
/// `/Image #([a-f0-9]{12})/gi`, with no surrounding token boundary.
fn image_reference_ids_in_text(text: &str) -> Vec<String> {
    const PREFIX: &[u8] = b"Image #";
    const ID_LENGTH: usize = 12;

    let bytes = text.as_bytes();
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    let mut index = 0;
    while index + PREFIX.len() + ID_LENGTH <= bytes.len() {
        let prefix_end = index + PREFIX.len();
        if !bytes[index..prefix_end].eq_ignore_ascii_case(PREFIX) {
            index += 1;
            continue;
        }
        let id_end = prefix_end + ID_LENGTH;
        let id_bytes = &bytes[prefix_end..id_end];
        if id_bytes.iter().all(u8::is_ascii_hexdigit) {
            // Matching bytes are ASCII hex, so this conversion cannot fail.
            let id = std::str::from_utf8(id_bytes)
                .expect("hexadecimal ASCII")
                .to_ascii_lowercase();
            if seen.insert(id.clone()) {
                ids.push(id);
            }
            index = id_end;
        } else {
            index += 1;
        }
    }
    ids
}

struct TrackedImagePayloadStore<'a> {
    inner: &'a mut InMemoryImagePayloadStore,
    put_ids: Vec<String>,
    seen_ids: HashSet<String>,
}

impl<'a> TrackedImagePayloadStore<'a> {
    fn new(inner: &'a mut InMemoryImagePayloadStore) -> Self {
        Self {
            inner,
            put_ids: Vec::new(),
            seen_ids: HashSet::new(),
        }
    }
}

impl ImagePayloadStore for TrackedImagePayloadStore<'_> {
    fn put(&mut self, part: &Value) -> StoredImagePayload {
        let payload = self.inner.put(part);
        if self.seen_ids.insert(payload.id.clone()) {
            self.put_ids.push(payload.id.clone());
        }
        payload
    }

    fn get(&self, id: &str) -> Option<StoredImagePayload> {
        self.inner.get(id)
    }
}

fn bounded_image_payload_cache(
    source: &InMemoryImagePayloadStore,
    ids: &[String],
) -> RuntimeImagePayloadCache {
    let mut bounded = InMemoryImagePayloadStore::default();
    let mut seen = HashSet::new();
    let mut retained_ids = Vec::new();
    let mut retained_bytes = 0usize;
    let mut retained_entries = 0usize;
    for id in ids {
        if !seen.insert(id.as_str()) {
            continue;
        }
        let Some(payload) = source.get(id) else {
            continue;
        };
        let payload_bytes = image_payload_cache_cost(&payload);
        if payload_bytes > MAX_IMAGE_PAYLOAD_CACHE_BYTES_PER_SESSION
            || retained_entries >= MAX_IMAGE_PAYLOAD_CACHE_ENTRIES_PER_SESSION
            || retained_bytes.saturating_add(payload_bytes)
                > MAX_IMAGE_PAYLOAD_CACHE_BYTES_PER_SESSION
        {
            continue;
        }
        bounded.put(&stored_image_payload_part(&payload));
        retained_ids.push(id.clone());
        retained_bytes = retained_bytes.saturating_add(payload_bytes);
        retained_entries += 1;
    }
    RuntimeImagePayloadCache {
        store: bounded,
        ids: retained_ids,
    }
}

fn image_payload_cache_cost(payload: &StoredImagePayload) -> usize {
    let display_name_bytes = payload
        .display_name
        .as_ref()
        .and_then(|display_name| serde_json::to_vec(display_name).ok())
        .map_or(0, |bytes| bytes.len());
    payload
        .id
        .len()
        .saturating_add(payload.mime_type.len())
        .saturating_add(payload.data.len())
        .saturating_add(display_name_bytes)
        .saturating_add(IMAGE_PAYLOAD_CACHE_ENTRY_OVERHEAD_BYTES)
}

fn stored_image_payload_part(payload: &StoredImagePayload) -> Value {
    let mut inline_data = Map::new();
    inline_data.insert(
        "mimeType".to_owned(),
        Value::String(payload.mime_type.clone()),
    );
    inline_data.insert("data".to_owned(), Value::String(payload.data.clone()));
    if let Some(display_name) = &payload.display_name {
        inline_data.insert("displayName".to_owned(), display_name.clone());
    }
    json!({"inlineData": inline_data})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct EchoTool;

    impl AgentToolExecutor for EchoTool {
        fn execute<'a>(
            &'a self,
            call: &'a ToolCallRequestInfo,
        ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>>
        {
            Box::pin(async move {
                let output = format!(
                    "echoed: {}",
                    call.args.get("value").and_then(Value::as_str).unwrap_or("")
                );
                let parts = if call
                    .args
                    .get("include_media")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    vec![json!({
                        "inlineData": {
                            "mimeType":"image/png",
                            "data":"AQ==",
                            "displayName":"picture.png"
                        }
                    })]
                } else {
                    Vec::new()
                };
                let mut output = ToolExecutionOutput::with_parts(output, parts);
                output.display = Some(json!({
                    "type":"todo_list",
                    "todos":[{"id":"one","content":"outside the model response","status":"pending"}]
                }));
                Ok(output)
            })
        }

        fn is_side_effecting(&self, _tool_name: &str) -> bool {
            false
        }
    }

    struct CountingEchoTool(Arc<AtomicUsize>);

    impl AgentToolExecutor for CountingEchoTool {
        fn execute<'a>(
            &'a self,
            call: &'a ToolCallRequestInfo,
        ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>>
        {
            Box::pin(async move {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(ToolExecutionOutput::text(
                    call.args
                        .get("value")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                ))
            })
        }

        fn is_side_effecting(&self, _tool_name: &str) -> bool {
            false
        }
    }

    async fn read_http_body(stream: &mut TcpStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before sending HTTP headers");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break header_end + 4;
            }
        };
        let header_text = std::str::from_utf8(&bytes[..header_end]).unwrap();
        let content_length = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while bytes.len() - header_end < content_length {
            let read = stream.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "client closed before sending HTTP body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        bytes[header_end..header_end + content_length].to_vec()
    }

    async fn write_sse(stream: &mut TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn write_http_error(
        stream: &mut TcpStream,
        status: &str,
        body: &str,
        retry_after: Option<&str>,
    ) {
        let retry_after =
            retry_after.map_or_else(String::new, |value| format!("Retry-After: {value}\r\n"));
        let response = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{retry_after}content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

    fn create_test_recorder(
        root: &std::path::Path,
    ) -> (crate::session_store::SessionStore, String, SessionRecorder) {
        let runtime_dir = root.join("runtime");
        let cwd = root.join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let store = crate::session_store::SessionStore::new(&runtime_dir, &cwd);
        let (session_id, recorder) = store
            .create_session(
                crate::session_writer::SessionWriterProcessKind::Unknown,
                "test",
                None,
            )
            .unwrap();
        (store, session_id, recorder)
    }

    struct FileHistoryHookTool {
        pending_updates: std::sync::Mutex<Vec<FileHistorySnapshot>>,
        started_prompt_id: std::sync::Mutex<Option<String>>,
        prompt_starts: AtomicUsize,
    }

    impl FileHistoryHookTool {
        fn new() -> Self {
            Self {
                pending_updates: std::sync::Mutex::new(Vec::new()),
                started_prompt_id: std::sync::Mutex::new(None),
                prompt_starts: AtomicUsize::new(0),
            }
        }

        fn snapshot(prompt_id: &str) -> FileHistorySnapshot {
            FileHistorySnapshot {
                prompt_id: prompt_id.to_owned(),
                tracked_file_backups: IndexMap::new(),
                timestamp: chrono::Utc::now(),
            }
        }
    }

    impl AgentToolExecutor for FileHistoryHookTool {
        fn execute<'a>(
            &'a self,
            _call: &'a ToolCallRequestInfo,
        ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>>
        {
            Box::pin(async move {
                self.pending_updates
                    .lock()
                    .expect("snapshot queue lock")
                    .push(Self::snapshot("post-tool"));
                Ok(ToolExecutionOutput::text("tool finished"))
            })
        }

        fn is_side_effecting(&self, _tool_name: &str) -> bool {
            false
        }

        fn begin_user_prompt<'a>(
            &'a self,
            prompt_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            self.prompt_starts.fetch_add(1, Ordering::SeqCst);
            *self.started_prompt_id.lock().expect("prompt ID lock") = Some(prompt_id.to_owned());
            let snapshot = Self::snapshot(prompt_id);
            Box::pin(async move {
                self.pending_updates
                    .lock()
                    .expect("snapshot queue lock")
                    .push(snapshot);
                Ok(())
            })
        }

        fn take_file_history_snapshot_updates(&self) -> Vec<FileHistorySnapshot> {
            std::mem::take(&mut *self.pending_updates.lock().expect("snapshot queue lock"))
        }
    }

    #[tokio::test]
    async fn file_history_hooks_default_to_noop() {
        let executor = EchoTool;
        executor
            .begin_user_prompt("prompt-1")
            .await
            .expect("default prompt hook succeeds");
        assert!(executor.take_file_history_snapshot_updates().is_empty());
    }

    #[tokio::test]
    async fn persists_prompt_and_tool_file_history_before_tool_result() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for response in [
                concat!(
                    "data: {\"id\":\"snapshot-call\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"snapshot-call-id\",\"type\":\"function\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"snapshot-call\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
                concat!(
                    "data: {\"id\":\"snapshot-final\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"snapshot-final\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
                concat!(
                    "data: {\"id\":\"snapshot-continued\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"continued\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"snapshot-continued\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                bodies.push(read_http_body(&mut stream).await);
                write_sse(&mut stream, response).await;
            }
            bodies
        });

        let root =
            std::env::temp_dir().join(format!("canopy-agent-file-history-{}", Uuid::new_v4()));
        let (store, session_id, mut recorder) = create_test_recorder(&root);
        let mut config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        config.tool_declarations = vec![json!({
            "functionDeclarations":[{"name":"echo","description":"Echo a value","parameters":{"type":"OBJECT","properties":{}}}]
        })];
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap();
        let mut executor = FileHistoryHookTool::new();
        runtime
            .run_prompt("save this turn", &mut recorder, &mut executor, |_| Ok(()))
            .await
            .expect("prompt with tool call completes");
        recorder.close().unwrap();

        let mut resumed = store
            .resume_session(
                &session_id,
                crate::session_store::SessionResumeOptions {
                    process_kind: crate::session_writer::SessionWriterProcessKind::Unknown,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: false,
                },
            )
            .unwrap();
        executor
            .pending_updates
            .lock()
            .expect("snapshot queue lock")
            .push(FileHistoryHookTool::snapshot("restored-validation"));
        runtime
            .continue_from_history(
                resumed.recovery_plan.api_history,
                &mut resumed.recorder,
                &mut executor,
                |_| Ok(()),
            )
            .await
            .expect("continuation from history completes");
        resumed.recorder.close().unwrap();
        provider.await.unwrap();

        let transcript = store
            .read_transcript(
                &session_id,
                crate::session_paths::SessionArchiveState::Active,
            )
            .unwrap();
        std::fs::remove_dir_all(&root).unwrap();

        let user_index = transcript
            .iter()
            .position(|record| record["type"] == "user")
            .expect("user record");
        let snapshot_indices = transcript
            .iter()
            .enumerate()
            .filter_map(|(index, record)| {
                (record["subtype"] == "file_history_snapshot").then_some(index)
            })
            .collect::<Vec<_>>();
        let assistant_indices = transcript
            .iter()
            .enumerate()
            .filter_map(|(index, record)| (record["type"] == "assistant").then_some(index))
            .collect::<Vec<_>>();
        let tool_result_index = transcript
            .iter()
            .position(|record| record["type"] == "tool_result")
            .expect("tool result record");

        assert_eq!(executor.prompt_starts.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot_indices.len(), 3);
        assert_eq!(assistant_indices.len(), 3);
        assert!(user_index < snapshot_indices[0]);
        assert!(snapshot_indices[0] < assistant_indices[0]);
        assert!(assistant_indices[0] < snapshot_indices[1]);
        assert!(snapshot_indices[1] < tool_result_index);
        assert!(tool_result_index < assistant_indices[1]);
        assert!(assistant_indices[1] < snapshot_indices[2]);
        assert!(snapshot_indices[2] < assistant_indices[2]);
        assert_eq!(
            transcript[snapshot_indices[0]]["systemPayload"]["snapshots"][0]["promptId"],
            executor
                .started_prompt_id
                .lock()
                .expect("prompt ID lock")
                .as_deref()
                .expect("prompt-start hook captured ID")
        );
        assert_eq!(
            transcript[snapshot_indices[2]]["systemPayload"]["snapshots"][0]["promptId"],
            "restored-validation"
        );
    }

    #[tokio::test]
    async fn retries_provider_start_after_rate_limit_before_consuming_sse() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let mut bodies = Vec::new();
            let (mut first, _) = listener.accept().await.unwrap();
            bodies.push(read_http_body(&mut first).await);
            write_http_error(
                &mut first,
                "429 Too Many Requests",
                r#"{"error":{"code":429,"message":"Rate limit exceeded"}}"#,
                Some("0.001"),
            )
            .await;

            let (mut second, _) = listener.accept().await.unwrap();
            bodies.push(read_http_body(&mut second).await);
            write_sse(
                &mut second,
                concat!(
                    "data: {\"id\":\"retry-response\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"retry-response\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
            )
            .await;
            bodies
        });

        let root = std::env::temp_dir().join(format!("canopy-agent-retry-{}", Uuid::new_v4()));
        let (_store, _session_id, mut recorder) = create_test_recorder(&root);
        let config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap();
        let mut executor = EchoTool;
        let summary = runtime
            .run_prompt("hello", &mut recorder, &mut executor, |_| Ok(()))
            .await
            .unwrap();
        recorder.close().unwrap();

        let bodies = provider.await.unwrap();
        assert_eq!(bodies.len(), 2);
        assert_eq!(summary.finish_reason.as_deref(), Some("STOP"));
    }

    #[tokio::test]
    async fn does_not_retry_permanent_client_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let body = read_http_body(&mut stream).await;
            write_http_error(
                &mut stream,
                "400 Bad Request",
                r#"{"error":{"message":"invalid request"}}"#,
                None,
            )
            .await;
            body
        });

        let root = std::env::temp_dir().join(format!("canopy-agent-no-retry-{}", Uuid::new_v4()));
        let (_store, _session_id, mut recorder) = create_test_recorder(&root);
        let config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap();
        let mut executor = EchoTool;
        let error = runtime
            .run_prompt("hello", &mut recorder, &mut executor, |_| Ok(()))
            .await
            .unwrap_err();
        recorder.close().unwrap();

        let body = provider.await.unwrap();
        assert!(!body.is_empty());
        assert!(matches!(
            error,
            AgentRuntimeError::Retry(RetryWithBackoffError::Operation(
                ProviderError::HttpStatus { status, .. }
            )) if status == reqwest::StatusCode::BAD_REQUEST
        ));
    }

    #[tokio::test]
    async fn cancelled_retry_source_aborts_before_provider_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let no_connection = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept()).await
        });

        let root = std::env::temp_dir().join(format!("canopy-agent-cancel-{}", Uuid::new_v4()));
        let (_store, _session_id, mut recorder) = create_test_recorder(&root);
        let config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap()
        .with_retry_cancellation(cancellation);
        let mut executor = EchoTool;
        let error = runtime
            .run_prompt("hello", &mut recorder, &mut executor, |_| Ok(()))
            .await
            .unwrap_err();
        recorder.close().unwrap();

        assert!(matches!(
            error,
            AgentRuntimeError::Retry(RetryWithBackoffError::Cancelled)
        ));
        assert!(no_connection.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn records_tool_result_before_requesting_the_continuation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for response in [
                concat!(
                    "data: {\"id\":\"response-1\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"echo\",\"arguments\":\"{\\\"value\\\":\\\"hi\\\",\\\"include_media\\\":true}\"}}]},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"response-1\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
                concat!(
                    "data: {\"id\":\"response-2\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"response-2\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
                concat!(
                    "data: {\"id\":\"response-3\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"resumed\"},\"finish_reason\":null}]}\n\n",
                    "data: {\"id\":\"response-3\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    "data: [DONE]\n\n"
                ),
            ] {
                let (mut stream, _) = listener.accept().await.unwrap();
                bodies.push(read_http_body(&mut stream).await);
                write_sse(&mut stream, response).await;
            }
            bodies
        });

        let root = std::env::temp_dir().join(format!("canopy-agent-runtime-{}", Uuid::new_v4()));
        let runtime_dir = root.join("runtime");
        let cwd = root.join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let store = crate::session_store::SessionStore::new(&runtime_dir, &cwd);
        let (session_id, mut recorder) = store
            .create_session(
                crate::session_writer::SessionWriterProcessKind::Unknown,
                "test",
                None,
            )
            .unwrap();
        let mut config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        config.pipeline.request_context.modalities.image = true;
        config.tool_declarations = vec![json!({
            "functionDeclarations":[{"name":"echo","description":"Echo a value","parameters":{"type":"OBJECT","properties":{"value":{"type":"STRING"},"include_media":{"type":"BOOLEAN"}},"required":["value"]}}]
        })];
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap();
        let mut executor = EchoTool;
        let mut visible_text = String::new();
        let mut result_display = None;
        let summary = runtime
            .run_prompt("call echo", &mut recorder, &mut executor, |event| {
                match event {
                    AgentRunEvent::Turn(TurnEvent::Content { value, .. }) => {
                        visible_text.push_str(&value);
                    }
                    AgentRunEvent::ToolExecutionFinished { display, .. } => {
                        result_display = display;
                    }
                    _ => {}
                }
                Ok(())
            })
            .await
            .unwrap();
        recorder.close().unwrap();

        let mut resumed = store
            .resume_session(
                &session_id,
                crate::session_store::SessionResumeOptions {
                    process_kind: crate::session_writer::SessionWriterProcessKind::Unknown,
                    version: "test".to_owned(),
                    git_branch: None,
                    allow_auto_continue: false,
                },
            )
            .unwrap();
        assert_eq!(
            resumed.recovery_plan.kind,
            crate::session_recovery::SessionRecoveryKind::Clean
        );
        let resumed_summary = runtime
            .run_prompt_with_history(
                "follow up",
                resumed.recovery_plan.api_history,
                &mut resumed.recorder,
                &mut executor,
                |event| {
                    if let AgentRunEvent::Turn(TurnEvent::Content { value, .. }) = event {
                        visible_text.push_str(&value);
                    }
                    Ok(())
                },
            )
            .await
            .unwrap();
        resumed.recorder.close().unwrap();

        let requests = provider.await.unwrap();
        let continuation: Value = serde_json::from_slice(&requests[1]).unwrap();
        let resumed_request: Value = serde_json::from_slice(&requests[2]).unwrap();
        let transcript = store
            .read_transcript(
                &session_id,
                crate::session_paths::SessionArchiveState::Active,
            )
            .unwrap();
        std::fs::remove_dir_all(&root).unwrap();

        assert_eq!(summary.model_turns, 2);
        assert_eq!(summary.tool_calls, 1);
        assert_eq!(summary.finish_reason.as_deref(), Some("STOP"));
        assert_eq!(resumed_summary.model_turns, 1);
        assert_eq!(resumed_summary.tool_calls, 0);
        assert_eq!(visible_text, "doneresumed");
        assert_eq!(
            result_display,
            Some(json!({
                "type":"todo_list",
                "todos":[{"id":"one","content":"outside the model response","status":"pending"}]
            }))
        );
        assert!(
            continuation["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| { message["role"] == "tool" && message["content"] == "echoed: hi" })
        );
        assert!(
            continuation["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| {
                    message["role"] == "user"
                        && message.pointer("/content/0/text")
                            == Some(&Value::String(
                                "(attached media from previous tool call)".to_owned(),
                            ))
                        && message.pointer("/content/1/image_url/url")
                            == Some(&Value::String("data:image/png;base64,AQ==".to_owned()))
                })
        );
        let resumed_messages = resumed_request["messages"].as_array().unwrap();
        assert_eq!(
            resumed_messages
                .last()
                .and_then(|message| message.pointer("/content/0/text"))
                .and_then(Value::as_str),
            Some("follow up")
        );
        assert!(
            resumed_messages
                .iter()
                .any(|message| { message["role"] == "tool" && message["content"] == "echoed: hi" })
        );
        assert!(resumed_messages.iter().any(|message| {
            message["role"] == "user"
                && message.pointer("/content/0/text")
                    == Some(&Value::String(
                        "(attached media from previous tool call)".to_owned(),
                    ))
                && message.pointer("/content/1/image_url/url")
                    == Some(&Value::String("data:image/png;base64,AQ==".to_owned()))
        }));
        assert!(transcript.iter().any(|record| {
            record["type"] == "tool_result"
                && record.pointer("/message/parts/0/functionResponse/response/output")
                    == Some(&Value::String("echoed: hi".to_owned()))
                && record.pointer("/message/parts/0/functionResponse/parts/0/inlineData/data")
                    == Some(&Value::String("AQ==".to_owned()))
                && record.pointer("/toolCallResult/resultDisplay/type")
                    == Some(&Value::String("todo_list".to_owned()))
        }));
        assert!(
            continuation["messages"]
                .as_array()
                .unwrap()
                .iter()
                .all(|message| {
                    message
                        .pointer("/content/0/text")
                        .and_then(Value::as_str)
                        .is_none_or(|text| !text.contains("outside the model response"))
                })
        );
    }

    #[tokio::test]
    async fn loop_detector_halts_a_repeated_tool_call_before_execution() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for index in 0..5 {
                let (mut stream, _) = listener.accept().await.unwrap();
                bodies.push(read_http_body(&mut stream).await);
                let response_id = format!("repeat-{index}");
                let call_id = format!("call-{index}");
                let arguments = serde_json::to_string(&json!({"value":"same"})).unwrap();
                let call = json!({
                    "id":response_id,
                    "model":"test-model",
                    "choices":[{
                        "index":0,
                        "delta":{"tool_calls":[{
                            "index":0,
                            "id":call_id,
                            "type":"function",
                            "function":{"name":"echo","arguments":arguments}
                        }]},
                        "finish_reason":null
                    }]
                });
                let finish = json!({
                    "id":response_id,
                    "model":"test-model",
                    "choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]
                });
                let response = format!(
                    "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                    serde_json::to_string(&call).unwrap(),
                    serde_json::to_string(&finish).unwrap(),
                );
                write_sse(&mut stream, &response).await;
            }
            bodies
        });

        let root = std::env::temp_dir().join(format!("canopy-loop-runtime-{}", Uuid::new_v4()));
        let runtime_dir = root.join("runtime");
        let cwd = root.join("project");
        std::fs::create_dir_all(&cwd).unwrap();
        let store = crate::session_store::SessionStore::new(&runtime_dir, &cwd);
        let (session_id, mut recorder) = store
            .create_session(
                crate::session_writer::SessionWriterProcessKind::Unknown,
                "test",
                None,
            )
            .unwrap();
        let mut config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        config.tool_declarations = vec![json!({
            "functionDeclarations":[{"name":"echo","description":"Echo a value","parameters":{"type":"OBJECT","properties":{"value":{"type":"STRING"}},"required":["value"]}}]
        })];
        let runtime = AgentRuntime::new(
            OpenAiCompatibleConfig {
                base_url: format!("http://{address}/v1"),
                ..OpenAiCompatibleConfig::default()
            },
            config,
        )
        .unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let mut executor = CountingEchoTool(Arc::clone(&executions));
        let error = runtime
            .run_prompt("repeat forever", &mut recorder, &mut executor, |_| Ok(()))
            .await
            .unwrap_err();
        recorder.close().unwrap();

        let requests = provider.await.unwrap();
        let transcript = store
            .read_transcript(
                &session_id,
                crate::session_paths::SessionArchiveState::Active,
            )
            .unwrap();
        std::fs::remove_dir_all(&root).unwrap();

        assert!(matches!(
            error,
            AgentRuntimeError::LoopDetected {
                ref loop_type
            } if loop_type == "consecutive_identical_tool_calls"
        ));
        assert_eq!(requests.len(), 5);
        assert_eq!(executions.load(Ordering::SeqCst), 4);
        assert!(transcript.iter().any(|record| {
            record["type"] == "system"
                && record["subtype"] == "loop_detected"
                && record.pointer("/systemPayload/loopType")
                    == Some(&Value::String(
                        "consecutive_identical_tool_calls".to_owned(),
                    ))
        }));
    }

    #[test]
    fn request_builder_removes_old_inline_images_from_live_history() {
        let tuning = resolve_compaction_tuning(None);
        let root = std::env::temp_dir().join(format!("canopy-image-history-{}", Uuid::new_v4()));
        let config = AgentRuntimeConfig::new("test-model", root.join("tool-results"));
        let runtime = AgentRuntime::new(OpenAiCompatibleConfig::default(), config).unwrap();
        let images = (0..tuning.image_payload_threshold)
            .map(|index| {
                json!({
                    "inlineData": {
                        "mimeType":"image/png",
                        "data":format!("payload-{index}-unique")
                    }
                })
            })
            .collect::<Vec<_>>();
        let mut history = vec![
            json!({"role":"model","parts":images}),
            json!({"role":"user","parts":[{"text":"What changed?"}]}),
        ];

        let request = runtime.build_request(&mut history, "image-session", None);

        assert_eq!(count_all_inline_images(&history), 0);
        assert!(
            history[0]["parts"][0]["text"]
                .as_str()
                .is_some_and(|text| text.starts_with("[Image #"))
        );
        let retained_history = serde_json::to_string(&history).unwrap();
        assert!(!retained_history.contains("payload-0-unique"));
        let request_image_count = request
            .pointer("/contents")
            .and_then(Value::as_array)
            .map_or(0, |contents| count_all_inline_images(contents));
        assert_eq!(
            request_image_count,
            tuning.max_recent_images.min(tuning.image_payload_threshold)
        );

        let stores = runtime
            .image_payload_stores
            .lock()
            .expect("image store lock");
        let cache = stores.get("image-session").expect("session image cache");
        let retained_bytes = cache
            .ids
            .iter()
            .filter_map(|id| cache.store.get(id))
            .map(|payload| image_payload_cache_cost(&payload))
            .sum::<usize>();
        assert!(retained_bytes <= MAX_IMAGE_PAYLOAD_CACHE_BYTES_PER_SESSION);
        assert!(cache.ids.len() <= MAX_IMAGE_PAYLOAD_CACHE_ENTRIES_PER_SESSION);
    }
}
