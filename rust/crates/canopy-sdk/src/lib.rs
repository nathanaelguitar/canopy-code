use futures_util::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, Command};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

pub mod daemon_acp_http;
pub mod daemon_acp_ws;
pub mod daemon_auto_reconnect;
pub mod daemon_client;
pub mod daemon_event_denormalizer;
pub mod daemon_rest;
pub mod daemon_sse;
pub mod daemon_upload_progress;
mod mcp;
pub mod serve_bridge;
pub mod workspace_daemon_client;
pub use daemon_upload_progress::UploadProgress;
use mcp::McpServerRegistry;
pub use mcp::{
    McpPromptDefinition, McpPromptHandler, McpPromptHandlerWithContext, McpRequestCancellation,
    McpRequestContext, McpResourceDefinition, McpResourceHandler, McpResourceHandlerWithContext,
    McpResourceTemplateDefinition, McpResourceTemplateHandler,
    McpResourceTemplateHandlerWithContext, McpResourceTemplateListHandler, McpServerConfig,
    McpToolDefinition, McpToolDefinitionWithContext, McpToolHandler, McpToolHandlerWithContext,
    SdkMcpServerConfig,
};

const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONFIGURED_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
const MESSAGE_QUEUE_CAPACITY: usize = 2;
const DEFAULT_CONTROL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CAN_USE_TOOL_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STREAM_CLOSE_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);
const STDERR_CHUNK_BYTES: usize = 16 * 1024;
const STDERR_QUEUE_CAPACITY: usize = 8;
const STDERR_DRAIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Errors surfaced by the process-backed query stream.
#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Invalid QueryOptions: {0}")]
    InvalidOptions(String),
    #[error("Failed to start CLI process: {0}")]
    Spawn(#[source] io::Error),
    #[error("Process transport I/O failed: {0}")]
    Io(#[source] io::Error),
    #[error("Failed to serialize message to JSON: {0}")]
    JsonEncode(#[source] serde_json::Error),
    #[error("CLI process exited with code {code}")]
    ProcessExit { code: i32 },
    #[error("CLI process terminated by signal {signal}")]
    ProcessSignal { signal: i32 },
    #[error("CLI process terminated without an exit code")]
    ProcessTerminated,
    #[error("Process message exceeds the configured {limit}-byte limit")]
    MessageTooLarge { limit: usize },
    #[error("Query is closed")]
    Closed,
    #[error("Query aborted by user")]
    Aborted,
    #[error("Input stream closed")]
    InputClosed,
    #[error("Control request timed out: {0}")]
    ControlTimeout(String),
    #[error("Control request failed: {0}")]
    Control(String),
}

/// Query permission mode passed through to the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionMode {
    Default,
    Plan,
    AutoEdit,
    Auto,
    Yolo,
}

impl PermissionMode {
    fn as_cli_value(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
            Self::AutoEdit => "auto-edit",
            Self::Auto => "auto",
            Self::Yolo => "yolo",
        }
    }
}

/// Authentication backend selected by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthType {
    OpenAi,
    Anthropic,
    QwenOauth,
    Gemini,
    VertexAi,
}

/// Reasoning effort requested when the query initializes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffortTier {
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl EffortTier {
    fn as_cli_value(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

/// A wire override that takes precedence over an effort request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffortOverride {
    pub source: String,
    pub field: String,
}

/// Effective reasoning-effort state reported by the CLI.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffortStatus {
    pub applied: bool,
    pub effort_override: Option<EffortOverride>,
    pub reason: Option<String>,
}

/// Source level accepted for an SDK-provided subagent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubagentLevel {
    Session,
}

impl SubagentLevel {
    fn as_wire_value(self) -> &'static str {
        match self {
            Self::Session => "session",
        }
    }
}

/// Partial runtime limits accepted by the TypeScript SDK for a subagent.
#[derive(Clone, Debug, Default)]
pub struct SubagentRunConfig {
    pub max_time_minutes: Option<serde_json::Number>,
    pub max_turns: Option<serde_json::Number>,
}

/// SDK-provided subagent configuration forwarded in the initialize request.
///
/// `level` is optional here because the TypeScript `queryOptionsSchema.ts`
/// validator only requires `name`, `description`, and `systemPrompt`, even
/// though its protocol interface declares `level` required. Unknown extension
/// fields can be retained in `extra_fields` and are forwarded unchanged.
#[derive(Clone, Debug)]
pub struct SubagentConfig {
    pub name: String,
    pub description: String,
    pub tools: Option<Vec<String>>,
    pub system_prompt: String,
    pub level: Option<SubagentLevel>,
    pub file_path: Option<String>,
    pub model: Option<String>,
    pub run_config: Option<SubagentRunConfig>,
    pub color: Option<String>,
    pub is_builtin: Option<bool>,
    pub extra_fields: Map<String, Value>,
}

impl SubagentConfig {
    /// Create a subagent with the three fields required by the SDK option
    /// validator. Optional TypeScript fields are omitted from the wire unless
    /// set on the returned value.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        system_prompt: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            tools: None,
            system_prompt: system_prompt.into(),
            level: None,
            file_path: None,
            model: None,
            run_config: None,
            color: None,
            is_builtin: None,
            extra_fields: Map::new(),
        }
    }

    fn to_wire_value(&self) -> Value {
        let mut value = self.extra_fields.clone();
        value.insert("name".into(), Value::String(self.name.clone()));
        value.insert(
            "description".into(),
            Value::String(self.description.clone()),
        );
        if let Some(tools) = &self.tools {
            value.insert(
                "tools".into(),
                Value::Array(tools.iter().cloned().map(Value::String).collect()),
            );
        }
        value.insert(
            "systemPrompt".into(),
            Value::String(self.system_prompt.clone()),
        );
        if let Some(level) = self.level {
            value.insert(
                "level".into(),
                Value::String(level.as_wire_value().to_owned()),
            );
        }
        if let Some(file_path) = &self.file_path {
            value.insert("filePath".into(), Value::String(file_path.clone()));
        }
        if let Some(model) = &self.model {
            value.insert("model".into(), Value::String(model.clone()));
        }
        if let Some(run_config) = &self.run_config {
            let mut run_config_value = Map::new();
            if let Some(max_time_minutes) = &run_config.max_time_minutes {
                run_config_value.insert(
                    "max_time_minutes".into(),
                    Value::Number(max_time_minutes.clone()),
                );
            }
            if let Some(max_turns) = &run_config.max_turns {
                run_config_value.insert("max_turns".into(), Value::Number(max_turns.clone()));
            }
            value.insert("runConfig".into(), Value::Object(run_config_value));
        }
        if let Some(color) = &self.color {
            value.insert("color".into(), Value::String(color.clone()));
        }
        if let Some(is_builtin) = self.is_builtin {
            value.insert("isBuiltin".into(), Value::Bool(is_builtin));
        }
        Value::Object(value)
    }
}

impl AuthType {
    fn as_cli_value(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Anthropic => "anthropic",
            Self::QwenOauth => "qwen-oauth",
            Self::Gemini => "gemini",
            Self::VertexAi => "vertex-ai",
        }
    }
}

/// System prompt behavior supported by the TypeScript SDK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemPrompt {
    /// Replace the CLI's system prompt.
    Override(String),
    /// Use the built-in Qwen Code prompt and append extra instructions.
    Append(String),
}

/// Timeouts accepted by the TypeScript SDK query options.
#[derive(Clone, Debug, Default)]
pub struct QueryTimeoutOptions {
    pub can_use_tool_ms: Option<u64>,
    /// Timeout for an SDK MCP request callback; defaults to 60 seconds.
    pub mcp_request_ms: Option<u64>,
    pub control_request_ms: Option<u64>,
    pub stream_close_ms: Option<u64>,
}

/// Caller-owned cancellation handle for a process-backed SDK query.
///
/// Set `Some(handle.clone())` in [`QueryOptions::cancellation`] and call
/// [`cancel`](Self::cancel) to abort the query. Cancellation is idempotent and
/// wakes pending stream, control, permission, and MCP callback work.
#[derive(Clone, Debug)]
pub struct QueryCancellation {
    sender: watch::Sender<bool>,
}

impl Default for QueryCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryCancellation {
    /// Create a cancellation handle that has not been cancelled.
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    /// Cancel the associated query. Repeated calls have no additional effect.
    pub fn cancel(&self) {
        self.sender.send_replace(true);
    }

    /// Return whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        *self.sender.borrow()
    }

    /// Wait until cancellation is requested.
    pub async fn cancelled(&self) {
        let mut receiver = self.sender.subscribe();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                // This handle owns a sender for the lifetime of this future.
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Permission suggestion supplied by the CLI with a `can_use_tool` request.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionSuggestion {
    #[serde(rename = "type")]
    pub suggestion_type: PermissionSuggestionType,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_input: Option<Value>,
}

/// Kind of permission suggestion offered by the CLI.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionSuggestionType {
    Allow,
    Deny,
    Modify,
}

/// Cancellation state for one custom tool permission callback.
///
/// The token is cancelled when the CLI cancels the request, the query closes,
/// the process exits, or the callback times out. Callback implementations can
/// check [`is_cancelled`](Self::is_cancelled) or await
/// [`cancelled`](Self::cancelled) to stop their own work promptly.
#[derive(Clone, Debug)]
pub struct CanUseToolCancellation {
    receiver: watch::Receiver<bool>,
}

impl CanUseToolCancellation {
    /// Return whether this permission request has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Wait until this permission request is cancelled.
    pub async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                // The sender remains alive for the duration of the callback.
                // If a caller retains a token after completion, channel closure
                // alone must not be mistaken for an abort signal.
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Typed arguments passed to a custom tool permission callback.
#[derive(Clone, Debug)]
pub struct CanUseToolRequest {
    pub tool_name: String,
    /// Tool invocation ID when supplied by the CLI. Older CLI builds may omit it.
    pub tool_use_id: Option<String>,
    pub input: Map<String, Value>,
    /// `None` represents absent or null permission suggestions.
    pub suggestions: Option<Vec<PermissionSuggestion>>,
    /// Path blocked by the CLI's policy check, when one applies.
    pub blocked_path: Option<String>,
    /// Abort signal for this individual permission callback.
    pub cancellation: CanUseToolCancellation,
}

/// Decision made by a custom tool permission callback.
#[derive(Clone, Debug)]
pub struct CanUseToolDecision {
    pub behavior: PermissionBehavior,
    /// For an allow decision this must be an object when present. If omitted,
    /// the original tool input is passed through, matching the TypeScript SDK.
    pub updated_input: Option<Value>,
    pub message: Option<String>,
    pub interrupt: Option<bool>,
}

impl CanUseToolDecision {
    /// Allow the tool with the supplied updated input object.
    pub fn allow(updated_input: Map<String, Value>) -> Self {
        Self {
            behavior: PermissionBehavior::Allow,
            updated_input: Some(Value::Object(updated_input)),
            message: None,
            interrupt: None,
        }
    }

    /// Allow the tool with the original input unchanged.
    pub fn allow_unchanged() -> Self {
        Self {
            behavior: PermissionBehavior::Allow,
            updated_input: None,
            message: None,
            interrupt: None,
        }
    }

    /// Deny the tool with an explanatory message.
    pub fn deny(message: impl Into<String>) -> Self {
        Self {
            behavior: PermissionBehavior::Deny,
            updated_input: None,
            message: Some(message.into()),
            interrupt: None,
        }
    }

    /// Deny the tool and optionally interrupt the active turn.
    pub fn deny_with_interrupt(message: impl Into<String>, interrupt: bool) -> Self {
        Self {
            interrupt: Some(interrupt),
            ..Self::deny(message)
        }
    }
}

/// Whether a custom tool permission callback approves or denies a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionBehavior {
    Allow,
    Deny,
}

/// Asynchronous custom permission callback. Return `Err` to fail closed.
pub type CanUseToolHandler = Arc<
    dyn Fn(CanUseToolRequest) -> BoxFuture<'static, Result<CanUseToolDecision, String>>
        + Send
        + Sync
        + 'static,
>;

/// An MCP JSON-RPC message forwarded by the CLI to an SDK-owned server.
#[derive(Clone, Debug)]
pub struct McpMessageRequest {
    pub server_name: String,
    pub message: Value,
}

/// Asynchronous adapter for messages sent to SDK-owned MCP servers.
///
/// For requests, return the server's JSON-RPC response. For notifications,
/// the callback is dispatched and its return value is ignored.
pub type McpMessageHandler = Arc<
    dyn Fn(McpMessageRequest) -> BoxFuture<'static, Result<Value, String>> + Send + Sync + 'static,
>;

/// Synchronous callback for bounded chunks read from the CLI's stderr pipe.
/// The callback must return promptly; it is invoked on a blocking worker.
pub type StderrHandler = Arc<dyn Fn(String) + Send + Sync + 'static>;

/// Options for starting a local stream-json CLI process.
///
/// With no executable override, the SDK looks for the TypeScript SDK's bundled
/// CLI beside the Rust crate, in the checked-out SDK package, or in an installed
/// `@qwen-code/sdk` package before falling back to `qwen` from `PATH`.
#[derive(Clone)]
pub struct QueryOptions {
    pub executable: Option<String>,
    pub cwd: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
    pub model: Option<String>,
    pub permission_mode: Option<PermissionMode>,
    pub system_prompt: Option<SystemPrompt>,
    pub max_session_turns: Option<i64>,
    pub core_tools: Vec<String>,
    pub exclude_tools: Vec<String>,
    pub allowed_tools: Vec<String>,
    /// Session subagents supplied in the SDK initialize control request.
    pub agents: Option<Vec<SubagentConfig>>,
    /// External CLI-managed and SDK-hosted MCP servers, keyed by config name.
    /// SDK servers are routed through this process; external JSON configs are
    /// forwarded to the CLI in the initialize control request.
    pub mcp_servers: BTreeMap<String, McpServerConfig>,
    pub auth_type: Option<AuthType>,
    pub effort: Option<EffortTier>,
    pub include_partial_messages: bool,
    pub continue_session: bool,
    pub resume: Option<String>,
    pub session_id: Option<String>,
    pub fork_session: bool,
    pub max_tool_calls: Option<i64>,
    pub max_subagent_depth: Option<u8>,
    pub include_directories: Vec<String>,
    pub extensions: Vec<String>,
    pub allowed_mcp_server_names: Vec<String>,
    pub fallback_model: Vec<String>,
    pub proxy: Option<String>,
    pub sandbox: bool,
    pub safe_mode: bool,
    pub insecure: bool,
    pub worktree: bool,
    pub disabled_slash_commands: Vec<String>,
    pub extra_args: Vec<String>,
    pub timeout: QueryTimeoutOptions,
    /// Optional callback for CLI `can_use_tool` control requests. Missing,
    /// failed, panicking, and timed out callbacks deny the tool request.
    pub can_use_tool: Option<CanUseToolHandler>,
    /// Optional handler for CLI `mcp_message` control requests. It receives
    /// both requests and notifications; requests wait for a JSON-RPC response
    /// up to `timeout.mcp_request_ms` (60 seconds by default).
    pub mcp_message: Option<McpMessageHandler>,
    /// Optional callback receiving stderr in UTF-8 lossy chunks of at most
    /// 16 KiB of source bytes. Set this or `debug` to capture stderr.
    pub stderr: Option<StderrHandler>,
    /// Optional caller-owned handle that aborts this query when cancelled.
    pub cancellation: Option<QueryCancellation>,
    /// Maximum serialized input and output JSON line size. Defaults to 16 MiB.
    pub max_message_bytes: usize,
    /// Pipe stderr through to the parent process, similar to SDK debug mode.
    pub debug: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            executable: None,
            cwd: None,
            env: BTreeMap::new(),
            model: None,
            permission_mode: None,
            system_prompt: None,
            max_session_turns: None,
            core_tools: Vec::new(),
            exclude_tools: Vec::new(),
            allowed_tools: Vec::new(),
            agents: None,
            mcp_servers: BTreeMap::new(),
            auth_type: None,
            effort: None,
            include_partial_messages: false,
            continue_session: false,
            resume: None,
            session_id: None,
            fork_session: false,
            max_tool_calls: None,
            max_subagent_depth: None,
            include_directories: Vec::new(),
            extensions: Vec::new(),
            allowed_mcp_server_names: Vec::new(),
            fallback_model: Vec::new(),
            proxy: None,
            sandbox: false,
            safe_mode: false,
            insecure: false,
            worktree: false,
            disabled_slash_commands: Vec::new(),
            extra_args: Vec::new(),
            timeout: QueryTimeoutOptions::default(),
            can_use_tool: None,
            mcp_message: None,
            stderr: None,
            cancellation: None,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            debug: false,
        }
    }
}

impl QueryOptions {
    /// Validate SDK-level option constraints before starting a subprocess.
    pub fn validate(&self) -> Result<(), QueryError> {
        McpServerRegistry::from_configs(&self.mcp_servers)?;
        if self
            .executable
            .as_deref()
            .is_some_and(|path| path.trim().is_empty())
        {
            return Err(QueryError::InvalidOptions(
                "pathToQwenExecutable cannot be empty".into(),
            ));
        }
        if let Some(SystemPrompt::Override(prompt) | SystemPrompt::Append(prompt)) =
            &self.system_prompt
        {
            if prompt.is_empty() {
                return Err(QueryError::InvalidOptions(
                    "systemPrompt must be a non-empty string".into(),
                ));
            }
        }
        if self.fork_session
            && self.resume.as_deref().is_none_or(str::is_empty)
            && !self.continue_session
        {
            return Err(QueryError::InvalidOptions(
                "forkSession requires resume or continue to be set".into(),
            ));
        }
        if self.agents.as_ref().is_some_and(|agents| {
            agents.iter().any(|agent| {
                agent.name.is_empty()
                    || agent.description.is_empty()
                    || agent.system_prompt.is_empty()
            })
        }) {
            return Err(QueryError::InvalidOptions(
                "agents must be an array of SubagentConfig objects with non-empty name, description, and systemPrompt".into(),
            ));
        }
        if self.max_tool_calls.is_some_and(|count| count < -1) {
            return Err(QueryError::InvalidOptions(
                "maxToolCalls must be at least -1".into(),
            ));
        }
        if self
            .max_subagent_depth
            .is_some_and(|depth| !(1..=100).contains(&depth))
        {
            return Err(QueryError::InvalidOptions(
                "maxSubagentDepth must be between 1 and 100".into(),
            ));
        }
        for (name, values) in [
            ("includeDirectories", &self.include_directories),
            ("extensions", &self.extensions),
            ("allowedMcpServerNames", &self.allowed_mcp_server_names),
            ("fallbackModel", &self.fallback_model),
            ("disabledSlashCommands", &self.disabled_slash_commands),
        ] {
            if values.iter().any(|value| value.contains(',')) {
                return Err(QueryError::InvalidOptions(format!(
                    "{name} items cannot contain commas"
                )));
            }
        }
        if self.fallback_model.len() > 3 {
            return Err(QueryError::InvalidOptions(
                "fallbackModel supports a maximum of 3 models".into(),
            ));
        }
        if self.extra_args.iter().any(String::is_empty) {
            return Err(QueryError::InvalidOptions(
                "extraArgs items cannot be empty".into(),
            ));
        }
        if let Some(arg) = self
            .extra_args
            .iter()
            .find(|arg| RESERVED_CLI_FLAGS.contains(&arg.split('=').next().unwrap_or_default()))
        {
            return Err(QueryError::InvalidOptions(format!(
                "extraArgs cannot contain reserved flag: {arg}"
            )));
        }
        if self
            .proxy
            .as_deref()
            .is_some_and(|proxy| proxy.trim().is_empty())
        {
            return Err(QueryError::InvalidOptions("proxy cannot be empty".into()));
        }
        for (name, value) in [
            ("canUseTool", self.timeout.can_use_tool_ms),
            ("mcpRequest", self.timeout.mcp_request_ms),
            ("controlRequest", self.timeout.control_request_ms),
            ("streamClose", self.timeout.stream_close_ms),
        ] {
            if value == Some(0) {
                return Err(QueryError::InvalidOptions(format!(
                    "timeout.{name} must be positive"
                )));
            }
        }
        if self.max_message_bytes == 0 || self.max_message_bytes > MAX_CONFIGURED_MESSAGE_BYTES {
            return Err(QueryError::InvalidOptions(format!(
                "maxMessageBytes must be between 1 and {MAX_CONFIGURED_MESSAGE_BYTES}"
            )));
        }
        for (name, value) in [("sessionId", &self.session_id), ("resume", &self.resume)] {
            if let Some(value) = value {
                if !value.is_empty() && !is_valid_session_id(value) {
                    return Err(QueryError::InvalidOptions(format!(
                        "Invalid {name}: \"{value}\". Must be a valid UUID."
                    )));
                }
            }
        }
        resolve_executable(self.executable.as_deref())?;
        Ok(())
    }
}

const RESERVED_CLI_FLAGS: &[&str] = &[
    "--input-format",
    "--output-format",
    "-o",
    "--channel",
    "--model",
    "-m",
    "--auth-type",
    "--fallback-model",
    "--approval-mode",
    "--yolo",
    "-y",
    "--insecure",
    "--no-insecure",
    "--core-tools",
    "--exclude-tools",
    "--allowed-tools",
    "--max-tool-calls",
    "--max-subagent-depth",
    "--resume",
    "-r",
    "--continue",
    "-c",
    "--session-id",
    "--fork-session",
    "--max-session-turns",
    "--system-prompt",
    "--append-system-prompt",
    "--include-directories",
    "--add-dir",
    "--allowed-mcp-server-names",
    "--extensions",
    "-e",
    "--proxy",
    "--sandbox",
    "--no-sandbox",
    "-s",
    "--sandbox-image",
    "--sandbox-session-id",
    "--safe-mode",
    "--no-safe-mode",
    "--worktree",
    "--disabled-slash-commands",
    "--include-partial-messages",
    "--chat-recording",
    "--openai-logging",
    "--openai-logging-dir",
    "--openai-base-url",
    "--openai-api-key",
    "--mcp-config",
    "--prompt",
    "-p",
    "--prompt-interactive",
    "-i",
    "--json-schema",
    "--json-fd",
    "--json-file",
    "--input-file",
];

/// Process-backed stream-json query.
///
/// Messages are yielded in arrival order through [`Query::next_message`]. The
/// receiver and both process pipes use bounded buffers; callers that stop
/// consuming apply backpressure to the subprocess stdout pipe.
pub struct Query {
    writer: Arc<ProcessWriter>,
    pending_control: PendingControls,
    messages: mpsc::Receiver<Result<Value, QueryError>>,
    session_id: String,
    control_request_timeout: Duration,
    stream_close_timeout: Duration,
    initial_effort_status: Option<EffortStatus>,
    closed: Arc<AtomicBool>,
    active_permissions: Arc<ActivePermissionRequests>,
    first_result: watch::Receiver<bool>,
    cancellation: QueryCancellation,
    abort_reported: bool,
}

/// Typed view of one SDK output message.
///
/// `message_type` remains an open string so newer CLI message types can pass
/// through unchanged. [`kind`](Self::kind) exposes typed variants for known
/// TypeScript SDK messages. The complete message is retained in `raw` for
/// fields and nested body schemas that are not modeled directly.
#[derive(Clone, Debug)]
pub struct SdkMessage {
    pub message_type: String,
    pub session_id: Option<String>,
    pub uuid: Option<String>,
    pub parent_tool_use_id: Option<String>,
    pub raw: Value,
}

impl SdkMessage {
    fn from_raw(raw: Value) -> Self {
        let string_field = |name: &str| raw.get(name).and_then(Value::as_str).map(str::to_owned);
        Self {
            // The stream-json transport filters messages without a string type.
            // Keep a defensive fallback in case another source is added later.
            message_type: string_field("type").unwrap_or_else(|| "unknown".into()),
            session_id: string_field("session_id"),
            uuid: string_field("uuid"),
            parent_tool_use_id: string_field("parent_tool_use_id"),
            raw,
        }
    }

    /// View this message as one of the TypeScript SDK's known message variants.
    ///
    /// The returned variant borrows from this message. Unknown message types,
    /// malformed known variants, and unknown nested stream events remain
    /// available through [`as_raw`](Self::as_raw).
    pub fn kind(&self) -> SdkMessageKind<'_> {
        SdkMessageKind::from_raw(&self.raw)
    }

    /// Borrow the complete message payload, including fields outside this
    /// common envelope.
    pub fn as_raw(&self) -> &Value {
        &self.raw
    }

    /// Return the complete message payload.
    pub fn into_raw(self) -> Value {
        self.raw
    }
}

/// Typed variants of the TypeScript SDK's public `SDKMessage` union.
///
/// Nested message bodies and content blocks remain `serde_json::Value`, since
/// their schema is large and evolving. Every field is borrowed from the
/// enclosing [`SdkMessage`], whose `raw` value preserves unknown fields and
/// exact JSON structure.
#[derive(Clone, Debug)]
pub enum SdkMessageKind<'a> {
    User(SdkUserMessage<'a>),
    Assistant(SdkAssistantMessage<'a>),
    System(SdkSystemMessage<'a>),
    ResultSuccess(SdkResultMessageSuccess<'a>),
    ResultError(SdkResultMessageError<'a>),
    /// A `result` with a future or inconsistent subtype/marker.
    ResultUnknown {
        subtype: Option<&'a str>,
    },
    StreamEvent(SdkPartialAssistantMessage<'a>),
    /// A future message type or a known type that lacks required SDK fields.
    Unknown {
        message_type: &'a str,
    },
}

impl<'a> SdkMessageKind<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        let message_type = raw.get("type").and_then(Value::as_str).unwrap_or("unknown");
        match message_type {
            "user" if raw.get("message").is_some() => Self::User(SdkUserMessage {
                uuid: json_str(raw, "uuid"),
                session_id: json_str(raw, "session_id"),
                message: &raw["message"],
                body: SdkApiUserMessage::from_raw(&raw["message"]),
                parent_tool_use_id: optional_nullable_string(raw, "parent_tool_use_id"),
                options: raw.get("options").and_then(Value::as_object),
            }),
            "assistant"
                if has_fields(
                    raw,
                    &["uuid", "session_id", "message", "parent_tool_use_id"],
                ) =>
            {
                Self::Assistant(SdkAssistantMessage {
                    uuid: json_str(raw, "uuid"),
                    session_id: json_str(raw, "session_id"),
                    message: &raw["message"],
                    body: SdkApiAssistantMessage::from_raw(&raw["message"]),
                    parent_tool_use_id: optional_nullable_string(raw, "parent_tool_use_id"),
                })
            }
            "system" if has_fields(raw, &["subtype", "uuid", "session_id"]) => {
                Self::System(SdkSystemMessage::from_raw(raw))
            }
            "result"
                if has_fields(
                    raw,
                    &["subtype", "duration_ms", "is_error", "uuid", "session_id"],
                ) =>
            {
                let subtype = json_str(raw, "subtype");
                match (subtype, raw.get("is_error").and_then(Value::as_bool)) {
                    (Some("success"), Some(false)) => {
                        Self::ResultSuccess(SdkResultMessageSuccess::from_raw(raw))
                    }
                    (Some("error_max_turns" | "error_during_execution"), Some(true)) => {
                        Self::ResultError(SdkResultMessageError::from_raw(raw))
                    }
                    _ => Self::ResultUnknown { subtype },
                }
            }
            "stream_event"
                if has_fields(raw, &["uuid", "session_id", "event", "parent_tool_use_id"]) =>
            {
                Self::StreamEvent(SdkPartialAssistantMessage {
                    uuid: json_str(raw, "uuid"),
                    session_id: json_str(raw, "session_id"),
                    event: SdkStreamEvent::from_raw(&raw["event"]),
                    parent_tool_use_id: optional_nullable_string(raw, "parent_tool_use_id"),
                })
            }
            _ => Self::Unknown { message_type },
        }
    }
}

/// Typed view of an SDK user message.
#[derive(Clone, Debug)]
pub struct SdkUserMessage<'a> {
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub message: &'a Value,
    /// Typed view of the nested API message when it matches the SDK schema.
    /// The original value remains available through `message`.
    pub body: Option<SdkApiUserMessage<'a>>,
    /// `None` means absent or malformed; `Some(None)` is explicit JSON null.
    pub parent_tool_use_id: Option<Option<&'a str>>,
    pub options: Option<&'a Map<String, Value>>,
}

/// Typed view of an SDK assistant message.
#[derive(Clone, Debug)]
pub struct SdkAssistantMessage<'a> {
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub message: &'a Value,
    /// Typed view of the nested API message when it matches the SDK schema.
    /// The original value remains available through `message`.
    pub body: Option<SdkApiAssistantMessage<'a>>,
    /// `None` means absent or malformed; `Some(None)` is explicit JSON null.
    pub parent_tool_use_id: Option<Option<&'a str>>,
}

/// Nested user message body from the TypeScript SDK protocol.
#[derive(Clone, Debug)]
pub struct SdkApiUserMessage<'a> {
    pub role: Option<&'a str>,
    pub content: Option<SdkContent<'a>>,
    pub raw: &'a Value,
}

impl<'a> SdkApiUserMessage<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            role: json_str(raw, "role"),
            content: raw.get("content").and_then(SdkContent::from_raw),
            raw,
        })
    }
}

/// Nested assistant message body from the TypeScript SDK protocol.
#[derive(Clone, Debug)]
pub struct SdkApiAssistantMessage<'a> {
    pub id: Option<&'a str>,
    pub message_type: Option<&'a str>,
    pub role: Option<&'a str>,
    pub model: Option<&'a str>,
    pub content: Option<Vec<SdkContentBlock<'a>>>,
    /// `None` means absent or malformed; `Some(None)` is explicit JSON null.
    pub stop_reason: Option<Option<&'a str>>,
    pub usage: Option<SdkUsage<'a>>,
    pub raw: &'a Value,
}

impl<'a> SdkApiAssistantMessage<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            id: json_str(raw, "id"),
            message_type: json_str(raw, "type"),
            role: json_str(raw, "role"),
            model: json_str(raw, "model"),
            content: raw.get("content").and_then(parse_content_blocks),
            stop_reason: raw.get("stop_reason").map(|value| value.as_str()),
            usage: raw.get("usage").and_then(SdkUsage::from_raw),
            raw,
        })
    }
}

/// User or tool-result content, which may be plain text or typed blocks.
#[derive(Clone, Debug)]
pub enum SdkContent<'a> {
    Text(&'a str),
    Blocks(Vec<SdkContentBlock<'a>>),
    /// Malformed or future content shape retained without interpretation.
    Unknown(&'a Value),
}

impl<'a> SdkContent<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        Some(if let Some(text) = raw.as_str() {
            Self::Text(text)
        } else if let Some(blocks) = parse_content_blocks(raw) {
            Self::Blocks(blocks)
        } else {
            Self::Unknown(raw)
        })
    }
}

/// Known SDK API content blocks, with unknown variants preserved in `Unknown`.
#[derive(Clone, Debug)]
pub enum SdkContentBlock<'a> {
    Text {
        text: Option<&'a str>,
        annotations: Option<Vec<SdkAnnotation<'a>>>,
        raw: &'a Value,
    },
    Thinking {
        thinking: Option<&'a str>,
        signature: Option<&'a str>,
        annotations: Option<Vec<SdkAnnotation<'a>>>,
        raw: &'a Value,
    },
    ToolUse {
        id: Option<&'a str>,
        name: Option<&'a str>,
        input: Option<&'a Value>,
        annotations: Option<Vec<SdkAnnotation<'a>>>,
        raw: &'a Value,
    },
    ToolResult {
        tool_use_id: Option<&'a str>,
        content: Option<SdkContent<'a>>,
        is_error: Option<bool>,
        annotations: Option<Vec<SdkAnnotation<'a>>>,
        raw: &'a Value,
    },
    Unknown {
        block_type: Option<&'a str>,
        raw: &'a Value,
    },
}

impl<'a> SdkContentBlock<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        let annotations = || raw.get("annotations").and_then(parse_annotations);
        match json_str(raw, "type") {
            Some("text") => Self::Text {
                text: json_str(raw, "text"),
                annotations: annotations(),
                raw,
            },
            Some("thinking") => Self::Thinking {
                thinking: json_str(raw, "thinking"),
                signature: json_str(raw, "signature"),
                annotations: annotations(),
                raw,
            },
            Some("tool_use") => Self::ToolUse {
                id: json_str(raw, "id"),
                name: json_str(raw, "name"),
                input: raw.get("input"),
                annotations: annotations(),
                raw,
            },
            Some("tool_result") => Self::ToolResult {
                tool_use_id: json_str(raw, "tool_use_id"),
                content: raw.get("content").and_then(SdkContent::from_raw),
                is_error: raw.get("is_error").and_then(Value::as_bool),
                annotations: annotations(),
                raw,
            },
            block_type => Self::Unknown { block_type, raw },
        }
    }
}

/// Annotation attached to an API content block.
#[derive(Clone, Debug)]
pub struct SdkAnnotation<'a> {
    pub annotation_type: Option<&'a str>,
    pub value: Option<&'a str>,
    pub raw: &'a Value,
}

fn parse_content_blocks(raw: &Value) -> Option<Vec<SdkContentBlock<'_>>> {
    raw.as_array()
        .map(|blocks| blocks.iter().map(SdkContentBlock::from_raw).collect())
}

fn parse_annotations(raw: &Value) -> Option<Vec<SdkAnnotation<'_>>> {
    raw.as_array().map(|annotations| {
        annotations
            .iter()
            .map(|raw| SdkAnnotation {
                annotation_type: json_str(raw, "type"),
                value: json_str(raw, "value"),
                raw,
            })
            .collect()
    })
}

/// Typed view of an SDK system message.
#[derive(Clone, Debug)]
pub struct SdkSystemMessage<'a> {
    pub subtype: Option<&'a str>,
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub data: Option<&'a Value>,
    pub cwd: Option<&'a str>,
    pub tools: Option<Vec<&'a str>>,
    pub mcp_servers: Option<Vec<SdkMcpServerStatus<'a>>>,
    pub model: Option<&'a str>,
    pub permission_mode: Option<&'a str>,
    pub slash_commands: Option<Vec<&'a str>>,
    pub qwen_code_version: Option<&'a str>,
    pub output_style: Option<&'a str>,
    pub agents: Option<Vec<&'a str>>,
    pub skills: Option<Vec<&'a str>>,
    pub capabilities: Option<&'a Map<String, Value>>,
    pub compact_metadata: Option<SdkCompactMetadata<'a>>,
}

impl<'a> SdkSystemMessage<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        Self {
            subtype: json_str(raw, "subtype"),
            uuid: json_str(raw, "uuid"),
            session_id: json_str(raw, "session_id"),
            data: raw.get("data"),
            cwd: json_str(raw, "cwd"),
            tools: json_str_array(raw, "tools"),
            mcp_servers: raw.get("mcp_servers").and_then(parse_mcp_server_statuses),
            model: json_str(raw, "model"),
            permission_mode: json_str(raw, "permission_mode"),
            slash_commands: json_str_array(raw, "slash_commands"),
            qwen_code_version: json_str(raw, "qwen_code_version"),
            output_style: json_str(raw, "output_style"),
            agents: json_str_array(raw, "agents"),
            skills: json_str_array(raw, "skills"),
            capabilities: raw.get("capabilities").and_then(Value::as_object),
            compact_metadata: raw
                .get("compact_metadata")
                .and_then(SdkCompactMetadata::from_raw),
        }
    }
}

/// Name and status of a server in a system initialization message.
#[derive(Clone, Debug)]
pub struct SdkMcpServerStatus<'a> {
    pub name: Option<&'a str>,
    pub status: Option<&'a str>,
}

/// Compaction metadata from a system message. `trigger` remains open to future
/// values while preserving the TypeScript SDK's current `manual`/`auto` values.
#[derive(Clone, Debug)]
pub struct SdkCompactMetadata<'a> {
    pub trigger: Option<&'a str>,
    pub pre_tokens: Option<&'a serde_json::Number>,
}

impl<'a> SdkCompactMetadata<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            trigger: json_str(raw, "trigger"),
            pre_tokens: raw.get("pre_tokens").and_then(Value::as_number),
        })
    }
}

/// Successful SDK result message.
#[derive(Clone, Debug)]
pub struct SdkResultMessageSuccess<'a> {
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub duration_ms: Option<&'a serde_json::Number>,
    pub duration_api_ms: Option<&'a serde_json::Number>,
    pub num_turns: Option<&'a serde_json::Number>,
    pub result: Option<&'a str>,
    pub usage: Option<&'a Value>,
    /// Typed result usage while `usage` preserves the original JSON value.
    pub usage_details: Option<SdkExtendedUsage<'a>>,
    pub model_usage: Option<&'a Map<String, Value>>,
    /// Typed per-model usage entries; malformed entries remain in `model_usage`.
    pub model_usage_details: Option<BTreeMap<&'a str, SdkModelUsage<'a>>>,
    pub permission_denials: Option<&'a Vec<Value>>,
}

impl<'a> SdkResultMessageSuccess<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        Self {
            uuid: json_str(raw, "uuid"),
            session_id: json_str(raw, "session_id"),
            duration_ms: raw.get("duration_ms").and_then(Value::as_number),
            duration_api_ms: raw.get("duration_api_ms").and_then(Value::as_number),
            num_turns: raw.get("num_turns").and_then(Value::as_number),
            result: json_str(raw, "result"),
            usage: raw.get("usage"),
            usage_details: raw.get("usage").and_then(SdkExtendedUsage::from_raw),
            model_usage: raw.get("modelUsage").and_then(Value::as_object),
            model_usage_details: raw
                .get("modelUsage")
                .and_then(Value::as_object)
                .map(parse_model_usage),
            permission_denials: raw.get("permission_denials").and_then(Value::as_array),
        }
    }
}

/// Error SDK result message.
#[derive(Clone, Debug)]
pub struct SdkResultMessageError<'a> {
    pub subtype: Option<&'a str>,
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub duration_ms: Option<&'a serde_json::Number>,
    pub duration_api_ms: Option<&'a serde_json::Number>,
    pub num_turns: Option<&'a serde_json::Number>,
    pub usage: Option<&'a Value>,
    /// Typed result usage while `usage` preserves the original JSON value.
    pub usage_details: Option<SdkExtendedUsage<'a>>,
    pub model_usage: Option<&'a Map<String, Value>>,
    /// Typed per-model usage entries; malformed entries remain in `model_usage`.
    pub model_usage_details: Option<BTreeMap<&'a str, SdkModelUsage<'a>>>,
    pub permission_denials: Option<&'a Vec<Value>>,
    pub error: Option<SdkResultError<'a>>,
}

impl<'a> SdkResultMessageError<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        Self {
            subtype: json_str(raw, "subtype"),
            uuid: json_str(raw, "uuid"),
            session_id: json_str(raw, "session_id"),
            duration_ms: raw.get("duration_ms").and_then(Value::as_number),
            duration_api_ms: raw.get("duration_api_ms").and_then(Value::as_number),
            num_turns: raw.get("num_turns").and_then(Value::as_number),
            usage: raw.get("usage"),
            usage_details: raw.get("usage").and_then(SdkExtendedUsage::from_raw),
            model_usage: raw.get("modelUsage").and_then(Value::as_object),
            model_usage_details: raw
                .get("modelUsage")
                .and_then(Value::as_object)
                .map(parse_model_usage),
            permission_denials: raw.get("permission_denials").and_then(Value::as_array),
            error: raw.get("error").and_then(SdkResultError::from_raw),
        }
    }
}

/// Base token usage from an assistant message.
#[derive(Clone, Debug)]
pub struct SdkUsage<'a> {
    pub input_tokens: Option<&'a serde_json::Number>,
    pub output_tokens: Option<&'a serde_json::Number>,
    pub cache_creation_input_tokens: Option<&'a serde_json::Number>,
    pub cache_read_input_tokens: Option<&'a serde_json::Number>,
    pub total_tokens: Option<&'a serde_json::Number>,
    pub raw: &'a Value,
}

impl<'a> SdkUsage<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            input_tokens: raw.get("input_tokens").and_then(Value::as_number),
            output_tokens: raw.get("output_tokens").and_then(Value::as_number),
            cache_creation_input_tokens: raw
                .get("cache_creation_input_tokens")
                .and_then(Value::as_number),
            cache_read_input_tokens: raw
                .get("cache_read_input_tokens")
                .and_then(Value::as_number),
            total_tokens: raw.get("total_tokens").and_then(Value::as_number),
            raw,
        })
    }
}

/// Additional token usage fields present on CLI result messages.
#[derive(Clone, Debug)]
pub struct SdkExtendedUsage<'a> {
    pub base: SdkUsage<'a>,
    pub server_tool_use: Option<SdkServerToolUse<'a>>,
    pub service_tier: Option<&'a str>,
    pub cache_creation: Option<SdkCacheCreationUsage<'a>>,
    pub raw: &'a Value,
}

impl<'a> SdkExtendedUsage<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        let base = SdkUsage::from_raw(raw)?;
        Some(Self {
            base,
            server_tool_use: raw.get("server_tool_use").and_then(|value| {
                value.as_object().map(|_| SdkServerToolUse {
                    web_search_requests: value
                        .get("web_search_requests")
                        .and_then(Value::as_number),
                    raw: value,
                })
            }),
            service_tier: json_str(raw, "service_tier"),
            cache_creation: raw.get("cache_creation").and_then(|value| {
                value.as_object().map(|_| SdkCacheCreationUsage {
                    ephemeral_1h_input_tokens: value
                        .get("ephemeral_1h_input_tokens")
                        .and_then(Value::as_number),
                    ephemeral_5m_input_tokens: value
                        .get("ephemeral_5m_input_tokens")
                        .and_then(Value::as_number),
                    raw: value,
                })
            }),
            raw,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SdkServerToolUse<'a> {
    pub web_search_requests: Option<&'a serde_json::Number>,
    pub raw: &'a Value,
}

#[derive(Clone, Debug)]
pub struct SdkCacheCreationUsage<'a> {
    pub ephemeral_1h_input_tokens: Option<&'a serde_json::Number>,
    pub ephemeral_5m_input_tokens: Option<&'a serde_json::Number>,
    pub raw: &'a Value,
}

/// Per-model token and context-window counts from a result message.
#[derive(Clone, Debug)]
pub struct SdkModelUsage<'a> {
    pub input_tokens: Option<&'a serde_json::Number>,
    pub output_tokens: Option<&'a serde_json::Number>,
    pub cache_read_input_tokens: Option<&'a serde_json::Number>,
    pub cache_creation_input_tokens: Option<&'a serde_json::Number>,
    pub web_search_requests: Option<&'a serde_json::Number>,
    pub context_window: Option<&'a serde_json::Number>,
    pub raw: &'a Value,
}

impl<'a> SdkModelUsage<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            input_tokens: raw.get("inputTokens").and_then(Value::as_number),
            output_tokens: raw.get("outputTokens").and_then(Value::as_number),
            cache_read_input_tokens: raw.get("cacheReadInputTokens").and_then(Value::as_number),
            cache_creation_input_tokens: raw
                .get("cacheCreationInputTokens")
                .and_then(Value::as_number),
            web_search_requests: raw.get("webSearchRequests").and_then(Value::as_number),
            context_window: raw.get("contextWindow").and_then(Value::as_number),
            raw,
        })
    }
}

fn parse_model_usage<'a>(raw: &'a Map<String, Value>) -> BTreeMap<&'a str, SdkModelUsage<'a>> {
    raw.iter()
        .filter_map(|(model, usage)| {
            SdkModelUsage::from_raw(usage).map(|usage| (model.as_str(), usage))
        })
        .collect()
}

/// Typed portion of an SDK result error; extra error fields remain in `raw`.
#[derive(Clone, Debug)]
pub struct SdkResultError<'a> {
    pub error_type: Option<&'a str>,
    pub message: Option<&'a str>,
    pub raw: &'a Value,
}

impl<'a> SdkResultError<'a> {
    fn from_raw(raw: &'a Value) -> Option<Self> {
        raw.as_object().map(|_| Self {
            error_type: json_str(raw, "type"),
            message: json_str(raw, "message"),
            raw,
        })
    }
}

/// Typed view of a partial assistant message and its streaming event.
#[derive(Clone, Debug)]
pub struct SdkPartialAssistantMessage<'a> {
    pub uuid: Option<&'a str>,
    pub session_id: Option<&'a str>,
    pub event: SdkStreamEvent<'a>,
    /// `None` means absent or malformed; `Some(None)` is explicit JSON null.
    pub parent_tool_use_id: Option<Option<&'a str>>,
}

/// Known stream events emitted by the TypeScript SDK. Raw event fields remain
/// available alongside typed views for nested content blocks and deltas.
#[derive(Clone, Debug)]
pub enum SdkStreamEvent<'a> {
    MessageStart {
        message: Option<&'a Value>,
    },
    ContentBlockStart {
        index: Option<&'a serde_json::Number>,
        content_block: Option<&'a Value>,
        typed_content_block: Option<SdkContentBlock<'a>>,
    },
    ContentBlockDelta {
        index: Option<&'a serde_json::Number>,
        delta: SdkContentBlockDelta<'a>,
    },
    ContentBlockStop {
        index: Option<&'a serde_json::Number>,
    },
    MessageStop,
    Unknown {
        event_type: Option<&'a str>,
        raw: &'a Value,
    },
}

impl<'a> SdkStreamEvent<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        match json_str(raw, "type") {
            Some("message_start") => Self::MessageStart {
                message: raw.get("message"),
            },
            Some("content_block_start") => Self::ContentBlockStart {
                index: raw.get("index").and_then(Value::as_number),
                content_block: raw.get("content_block"),
                typed_content_block: raw.get("content_block").map(SdkContentBlock::from_raw),
            },
            Some("content_block_delta") => Self::ContentBlockDelta {
                index: raw.get("index").and_then(Value::as_number),
                delta: SdkContentBlockDelta::from_raw(&raw["delta"]),
            },
            Some("content_block_stop") => Self::ContentBlockStop {
                index: raw.get("index").and_then(Value::as_number),
            },
            Some("message_stop") => Self::MessageStop,
            event_type => Self::Unknown { event_type, raw },
        }
    }
}

/// Known delta variants nested in a content-block stream event.
#[derive(Clone, Debug)]
pub enum SdkContentBlockDelta<'a> {
    Text {
        text: Option<&'a str>,
    },
    Thinking {
        thinking: Option<&'a str>,
    },
    InputJson {
        partial_json: Option<&'a str>,
    },
    Unknown {
        delta_type: Option<&'a str>,
        raw: &'a Value,
    },
}

impl<'a> SdkContentBlockDelta<'a> {
    fn from_raw(raw: &'a Value) -> Self {
        match json_str(raw, "type") {
            Some("text_delta") => Self::Text {
                text: json_str(raw, "text"),
            },
            Some("thinking_delta") => Self::Thinking {
                thinking: json_str(raw, "thinking"),
            },
            Some("input_json_delta") => Self::InputJson {
                partial_json: json_str(raw, "partial_json"),
            },
            delta_type => Self::Unknown { delta_type, raw },
        }
    }
}

fn json_str<'a>(raw: &'a Value, name: &str) -> Option<&'a str> {
    raw.get(name).and_then(Value::as_str)
}

fn optional_nullable_string<'a>(raw: &'a Value, name: &str) -> Option<Option<&'a str>> {
    raw.get(name).map(|value| value.as_str())
}

fn has_fields(raw: &Value, fields: &[&str]) -> bool {
    fields.iter().all(|field| raw.get(*field).is_some())
}

fn json_str_array<'a>(raw: &'a Value, name: &str) -> Option<Vec<&'a str>> {
    raw.get(name)?
        .as_array()?
        .iter()
        .map(Value::as_str)
        .collect()
}

fn parse_mcp_server_statuses(raw: &Value) -> Option<Vec<SdkMcpServerStatus<'_>>> {
    raw.as_array().map(|servers| {
        servers
            .iter()
            .map(|server| SdkMcpServerStatus {
                name: json_str(server, "name"),
                status: json_str(server, "status"),
            })
            .collect()
    })
}

type PendingControls = Arc<Mutex<BTreeMap<String, oneshot::Sender<Result<Value, String>>>>>;

#[derive(Default)]
struct ActivePermissionRequests {
    state: std::sync::Mutex<ActivePermissionState>,
}

#[derive(Default)]
struct ActivePermissionState {
    closed: bool,
    requests: BTreeMap<String, watch::Sender<bool>>,
}

impl ActivePermissionRequests {
    fn register(&self, request_id: &str, cancellation: watch::Sender<bool>) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            cancellation.send_replace(true);
            return false;
        }
        if let Some(previous) = state
            .requests
            .insert(request_id.to_owned(), cancellation.clone())
        {
            previous.send_replace(true);
        }
        true
    }

    fn cancel(&self, request_id: &str) {
        let cancellation = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requests
            .remove(request_id);
        if let Some(cancellation) = cancellation {
            cancellation.send_replace(true);
        }
    }

    fn remove_if_same(&self, request_id: &str, cancellation: &watch::Sender<bool>) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .requests
            .get(request_id)
            .is_some_and(|active| active.same_channel(cancellation))
        {
            state.requests.remove(request_id);
        }
    }

    fn cancel_all(&self) {
        let requests = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            std::mem::take(&mut state.requests)
        };
        for (_, cancellation) in requests {
            cancellation.send_replace(true);
        }
    }
}

type PendingPermissionResponse = BoxFuture<'static, (String, watch::Sender<bool>, Option<Value>)>;
type PendingMcpResponse = BoxFuture<'static, (String, Result<Value, String>)>;
type PendingMcpNotification = BoxFuture<'static, ()>;

impl Query {
    /// Start an interactive query and complete the SDK initialize handshake.
    pub async fn start(options: QueryOptions) -> Result<Self, QueryError> {
        Self::start_inner(options, false).await
    }

    /// Start a single-turn query, send its text prompt, and return its message stream.
    pub async fn query(
        prompt: impl Into<String>,
        options: QueryOptions,
    ) -> Result<Self, QueryError> {
        let query = Self::start_inner(options, true).await?;
        query.send_text(prompt).await?;
        Ok(query)
    }

    async fn start_inner(options: QueryOptions, single_turn: bool) -> Result<Self, QueryError> {
        options.validate()?;
        let cancellation = options.cancellation.clone().unwrap_or_default();
        if cancellation.is_cancelled() {
            return Err(QueryError::Aborted);
        }
        let session_id = if options.fork_session {
            options.session_id.clone().unwrap_or_else(new_session_id)
        } else {
            options
                .resume
                .clone()
                .or_else(|| options.session_id.clone())
                .unwrap_or_else(new_session_id)
        };
        let control_request_timeout = options
            .timeout
            .control_request_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_CONTROL_TIMEOUT);
        let stream_close_timeout = options
            .timeout
            .stream_close_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_STREAM_CLOSE_TIMEOUT);
        let can_use_tool_timeout = options
            .timeout
            .can_use_tool_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_CAN_USE_TOOL_TIMEOUT);
        let mcp_request_timeout = options
            .timeout
            .mcp_request_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_MCP_REQUEST_TIMEOUT);
        let can_use_tool = options.can_use_tool.clone();
        let mcp_message = options.mcp_message.clone();
        let mcp_servers = McpServerRegistry::from_configs(&options.mcp_servers)?;
        let active_permissions = Arc::new(ActivePermissionRequests::default());
        let (writer, output) =
            ProcessTransport::spawn(&options, &session_id, active_permissions.clone()).await?;
        let (message_tx, messages) = mpsc::channel(MESSAGE_QUEUE_CAPACITY);
        let (first_result_tx, first_result) = watch::channel(false);
        let pending_control = Arc::new(Mutex::new(BTreeMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        tokio::spawn(route_messages(
            output,
            message_tx,
            writer.clone(),
            pending_control.clone(),
            active_permissions.clone(),
            single_turn,
            closed.clone(),
            first_result_tx,
            can_use_tool,
            can_use_tool_timeout,
            mcp_message,
            mcp_servers.clone(),
            mcp_request_timeout,
            cancellation.clone(),
        ));

        let mut initialize = Map::new();
        initialize.insert("hooks".into(), Value::Null);
        if let Some(agents) = &options.agents {
            initialize.insert(
                "agents".into(),
                Value::Array(agents.iter().map(SubagentConfig::to_wire_value).collect()),
            );
        }
        mcp_servers.insert_initialize_fields(&mut initialize);
        if let Some(effort) = options.effort {
            initialize.insert("effort".into(), Value::String(effort.as_cli_value().into()));
        }

        let query = Self {
            writer,
            pending_control,
            messages,
            session_id,
            control_request_timeout,
            stream_close_timeout,
            initial_effort_status: None,
            closed,
            active_permissions,
            first_result,
            cancellation,
            abort_reported: false,
        };
        let initial_response = query
            .send_control_request_internal("initialize", Value::Object(initialize))
            .await?;
        let mut query = query;
        query.initial_effort_status = parse_effort_status(&initial_response["effort_status"]);
        Ok(query)
    }

    /// The SDK session ID sent on generated user messages.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Request cancellation of this query and wake its pending work.
    pub fn cancel(&self) {
        self.cancellation.cancel();
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.active_permissions.cancel_all();
            self.writer.abort();
        }
    }

    /// Receive the next SDK message. Transport failures and cancellation are
    /// yielded once, then the stream ends.
    pub async fn next_message(&mut self) -> Option<Result<Value, QueryError>> {
        if self.abort_reported {
            return None;
        }
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => {
                self.abort_reported = true;
                Some(Err(QueryError::Aborted))
            }
            message = self.messages.recv() => message,
        }
    }

    /// Receive the next message with its common fields and known SDK variant
    /// parsed while retaining the full raw JSON payload. Unknown message types
    /// remain open strings. This shares the same queue as `next_message` and
    /// the `Stream` implementation, so each received message is consumed once.
    pub async fn next_typed_message(&mut self) -> Option<Result<SdkMessage, QueryError>> {
        self.next_message()
            .await
            .map(|message| message.map(SdkMessage::from_raw))
    }

    /// Borrow this query as a stream of typed message views. Each view's
    /// [`SdkMessage::kind`] method exposes the known discriminated variant.
    /// Messages remain available through the raw receive APIs, but pulling
    /// from either view consumes the next item from the shared stream.
    pub fn typed_stream(&mut self) -> impl Stream<Item = Result<SdkMessage, QueryError>> + '_ {
        self.map(|message| message.map(SdkMessage::from_raw))
    }

    /// Send an SDK user-message object without changing its wire representation.
    pub async fn send_message(&self, message: &Value) -> Result<(), QueryError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueryError::Closed);
        }
        if self.cancellation.is_cancelled() {
            return Err(QueryError::Aborted);
        }
        if message.get("type").and_then(Value::as_str) != Some("user")
            || message.get("message").is_none()
        {
            return Err(QueryError::InvalidOptions(
                "send_message expects an SDK user message".into(),
            ));
        }
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(QueryError::Aborted),
            result = self.writer.write_json(message) => result,
        }
    }

    /// Send a text prompt using the SDK user-message envelope.
    pub async fn send_text(&self, prompt: impl Into<String>) -> Result<(), QueryError> {
        let message = UserMessage {
            message_type: "user",
            session_id: &self.session_id,
            message: UserMessageContent {
                role: "user",
                content: prompt.into(),
            },
            parent_tool_use_id: None,
        };
        self.send_message(&serde_json::to_value(message).map_err(QueryError::JsonEncode)?)
            .await
    }

    /// Write an asynchronous sequence of SDK user messages. When the input
    /// stream ends, this waits for the first result or `timeout.streamClose`
    /// before closing stdin, matching the TypeScript SDK's input-stream finish.
    pub async fn stream_input<S>(&self, mut input: S) -> Result<(), QueryError>
    where
        S: Stream<Item = Value> + Unpin,
    {
        loop {
            let message = tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(QueryError::Aborted),
                message = input.next() => message,
            };
            let Some(message) = message else {
                break;
            };
            self.send_message(&message).await?;
        }

        let mut first_result = self.first_result.clone();
        if !*first_result.borrow_and_update() {
            let wait_for_result = async {
                loop {
                    if *first_result.borrow() || first_result.changed().await.is_err() {
                        break;
                    }
                }
            };
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(QueryError::Aborted),
                _ = timeout(self.stream_close_timeout, wait_for_result) => {}
            }
        }
        self.end_input().await
    }

    /// Finish the input side while allowing the CLI to finish the current turn.
    pub async fn end_input(&self) -> Result<(), QueryError> {
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(QueryError::Aborted),
            result = self.writer.end_input() => result,
        }
    }

    /// Interrupt the current CLI turn.
    pub async fn interrupt(&self) -> Result<(), QueryError> {
        self.send_control_request("interrupt", Map::new())
            .await
            .map(|_| ())
    }

    /// Continue the most recent unfinished turn.
    pub async fn continue_last_turn(&self) -> Result<Value, QueryError> {
        self.send_control_request("continue_last_turn", Map::new())
            .await
    }

    /// Change the active permission mode.
    pub async fn set_permission_mode(&self, mode: PermissionMode) -> Result<(), QueryError> {
        self.send_control_request(
            "set_permission_mode",
            json!({ "mode": mode.as_cli_value() }),
        )
        .await
        .map(|_| ())
    }

    /// Change the active model.
    pub async fn set_model(&self, model: impl Into<String>) -> Result<(), QueryError> {
        self.send_control_request("set_model", json!({ "model": model.into() }))
            .await
            .map(|_| ())
    }

    /// Request context usage from the CLI.
    pub async fn get_context_usage(&self, show_details: bool) -> Result<Value, QueryError> {
        self.send_control_request("get_context_usage", json!({ "show_details": show_details }))
            .await
    }

    /// Request models available to the current authentication backend.
    pub async fn get_available_models(&self) -> Result<Value, QueryError> {
        self.send_control_request("get_available_models", Map::new())
            .await
    }

    /// Request usage dashboard data from the CLI.
    pub async fn get_usage_info(&self, range: Option<&str>) -> Result<Value, QueryError> {
        let mut data = Map::new();
        if let Some(range) = range {
            if !matches!(range, "today" | "week" | "month" | "all") {
                return Err(QueryError::InvalidOptions(
                    "usage range must be today, week, month, or all".into(),
                ));
            }
            data.insert("range".into(), Value::String(range.into()));
        }
        self.send_control_request("get_usage_info", data).await
    }

    /// Request the control commands supported by the CLI.
    pub async fn supported_commands(&self) -> Result<Value, QueryError> {
        self.send_control_request("supported_commands", Map::new())
            .await
    }

    /// Request MCP server status, including SDK-hosted servers registered for
    /// this query and servers managed by the CLI.
    pub async fn mcp_server_status(&self) -> Result<Value, QueryError> {
        self.send_control_request("mcp_server_status", Map::new())
            .await
    }

    /// Set reasoning effort and return whether the CLI applied it.
    pub async fn set_effort(&self, effort: EffortTier) -> Result<bool, QueryError> {
        Ok(self.set_effort_status(effort).await?.applied)
    }

    /// Set reasoning effort and return the effective wire status.
    pub async fn set_effort_status(&self, effort: EffortTier) -> Result<EffortStatus, QueryError> {
        let response = self
            .send_control_request("set_effort", json!({ "effort": effort.as_cli_value() }))
            .await?;
        Ok(parse_effort_status(&response).unwrap_or(EffortStatus {
            applied: false,
            effort_override: None,
            reason: None,
        }))
    }

    /// Return the effective status reported by the initial effort request.
    pub fn initial_effort_status(&self) -> Option<&EffortStatus> {
        self.initial_effort_status.as_ref()
    }

    /// Close stdin and ask the child process to terminate, escalating after five seconds.
    pub async fn close(&self) -> Result<(), QueryError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.active_permissions.cancel_all();
        reject_pending(&self.pending_control, "Query is closed").await;
        self.writer.close().await
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn send_control_request(
        &self,
        subtype: &str,
        data: impl Into<Value>,
    ) -> Result<Value, QueryError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueryError::Closed);
        }
        let mut data = data.into();
        if !data.is_object() {
            return Err(QueryError::InvalidOptions(
                "control request data must be a JSON object".into(),
            ));
        }
        if let Some(data) = data.as_object_mut() {
            self.send_control_request_internal(subtype, Value::Object(std::mem::take(data)))
                .await
        } else {
            unreachable!()
        }
    }

    async fn send_control_request_internal(
        &self,
        subtype: &str,
        data: Value,
    ) -> Result<Value, QueryError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueryError::Closed);
        }
        if self.cancellation.is_cancelled() {
            return Err(QueryError::Aborted);
        }
        let request_id = Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_control
            .lock()
            .await
            .insert(request_id.clone(), response_tx);
        let mut request = Map::new();
        request.insert("subtype".into(), Value::String(subtype.into()));
        if let Some(data) = data.as_object() {
            request.extend(data.clone());
        }
        let wire_message = json!({
            "type": "control_request",
            "request_id": request_id,
            "request": request,
        });
        let write_result = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => Err(QueryError::Aborted),
            result = self.writer.write_json(&wire_message) => result,
        };
        if let Err(error) = write_result {
            self.pending_control.lock().await.remove(&request_id);
            return Err(error);
        }

        let response = tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => {
                self.pending_control.lock().await.remove(&request_id);
                return Err(QueryError::Aborted);
            }
            response = timeout(self.control_request_timeout, response_rx) => match response {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => Err("transport closed before response".into()),
                Err(_) => {
                    self.pending_control.lock().await.remove(&request_id);
                    return Err(QueryError::ControlTimeout(subtype.into()));
                }
            }
        };
        response.map_err(QueryError::Control)
    }
}

impl Drop for Query {
    fn drop(&mut self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.active_permissions.cancel_all();
            self.writer.request_shutdown();
        }
    }
}

impl Stream for Query {
    type Item = Result<Value, QueryError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.abort_reported {
            return Poll::Ready(None);
        }
        if this.cancellation.is_cancelled() {
            this.abort_reported = true;
            return Poll::Ready(Some(Err(QueryError::Aborted)));
        }
        match this.messages.poll_recv(context) {
            Poll::Ready(None) if this.cancellation.is_cancelled() => {
                this.abort_reported = true;
                Poll::Ready(Some(Err(QueryError::Aborted)))
            }
            result => result,
        }
    }
}

#[derive(Serialize)]
struct UserMessage<'a> {
    #[serde(rename = "type")]
    message_type: &'static str,
    session_id: &'a str,
    message: UserMessageContent,
    parent_tool_use_id: Option<&'static str>,
}

#[derive(Serialize)]
struct UserMessageContent {
    role: &'static str,
    content: String,
}

struct ProcessWriter {
    stdin: Mutex<Option<ChildStdin>>,
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    max_message_bytes: usize,
    input_closed: AtomicBool,
    closed: AtomicBool,
}

impl ProcessWriter {
    async fn write_json(&self, value: &Value) -> Result<(), QueryError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(QueryError::Closed);
        }
        if self.input_closed.load(Ordering::Acquire) {
            return Err(QueryError::InputClosed);
        }
        let mut line = CappedJsonBuffer::new(self.max_message_bytes);
        if let Err(error) = serde_json::to_writer(&mut line, value) {
            return if line.exceeded_limit {
                Err(QueryError::MessageTooLarge {
                    limit: self.max_message_bytes,
                })
            } else {
                Err(QueryError::JsonEncode(error))
            };
        }
        line.bytes.push(b'\n');
        let mut stdin = self.stdin.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(QueryError::Closed);
        }
        if self.input_closed.load(Ordering::Acquire) {
            return Err(QueryError::InputClosed);
        }
        let Some(stdin) = stdin.as_mut() else {
            return Err(QueryError::InputClosed);
        };
        stdin
            .write_all(&line.bytes)
            .await
            .map_err(map_write_error)?;
        stdin.flush().await.map_err(map_write_error)
    }

    async fn end_input(&self) -> Result<(), QueryError> {
        if self.input_closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(mut stdin) = self.stdin.lock().await.take() {
            stdin.shutdown().await.map_err(map_write_error)?;
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), QueryError> {
        self.closed.store(true, Ordering::Release);
        self.request_shutdown();
        match self.end_input().await {
            Err(QueryError::InputClosed) => Ok(()),
            result => result,
        }
    }

    fn request_shutdown(&self) {
        if let Ok(mut shutdown) = self.shutdown.lock() {
            if let Some(shutdown) = shutdown.take() {
                let _ = shutdown.send(());
            }
        }
    }

    fn abort(&self) {
        self.closed.store(true, Ordering::Release);
        self.input_closed.store(true, Ordering::Release);
        self.request_shutdown();
    }
}

struct CappedJsonBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded_limit: bool,
}

impl CappedJsonBuffer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(8192)),
            limit,
            exceeded_limit: false,
        }
    }
}

impl Write for CappedJsonBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded_limit = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "serialized JSON exceeds the configured message limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn map_write_error(error: io::Error) -> QueryError {
    if matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::NotConnected
    ) {
        QueryError::InputClosed
    } else {
        QueryError::Io(error)
    }
}

struct ProcessTransport;

impl ProcessTransport {
    async fn spawn(
        options: &QueryOptions,
        session_id: &str,
        active_permissions: Arc<ActivePermissionRequests>,
    ) -> Result<
        (
            Arc<ProcessWriter>,
            mpsc::Receiver<Result<Value, QueryError>>,
        ),
        QueryError,
    > {
        let (program, prefix_args) = resolve_executable(options.executable.as_deref())?;
        let mut command = Command::new(program);
        command.args(prefix_args);
        command.args(build_cli_arguments(options, session_id));
        if let Some(cwd) = &options.cwd {
            command.current_dir(cwd);
        }
        command.envs(options.env.iter());
        command.stdin(Stdio::piped()).stdout(Stdio::piped());
        let stderr_enabled = options.debug || options.stderr.is_some();
        command.stderr(if stderr_enabled {
            Stdio::piped()
        } else {
            Stdio::null()
        });
        command.kill_on_drop(true);
        let mut child = command.spawn().map_err(QueryError::Spawn)?;
        let stdin = child.stdin.take().ok_or_else(|| {
            QueryError::Spawn(io::Error::other("child stdin pipe was not created"))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            QueryError::Spawn(io::Error::other("child stdout pipe was not created"))
        })?;
        let stderr = if stderr_enabled {
            Some(child.stderr.take().ok_or_else(|| {
                QueryError::Spawn(io::Error::other("child stderr pipe was not created"))
            })?)
        } else {
            None
        };
        let stderr_tasks = spawn_stderr_tasks(stderr, options.stderr.clone(), options.debug);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (exit_tx, exit_rx) = oneshot::channel();
        tokio::spawn(supervise_child(
            child,
            shutdown_rx,
            exit_tx,
            active_permissions,
            stderr_tasks,
        ));
        let writer = Arc::new(ProcessWriter {
            stdin: Mutex::new(Some(stdin)),
            shutdown: std::sync::Mutex::new(Some(shutdown_tx)),
            max_message_bytes: options.max_message_bytes,
            input_closed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        });
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_QUEUE_CAPACITY);
        tokio::spawn(read_process_output(
            stdout,
            exit_rx,
            message_tx,
            writer.clone(),
            options.max_message_bytes,
        ));
        Ok((writer, message_rx))
    }
}

fn spawn_stderr_tasks(
    stderr: Option<ChildStderr>,
    handler: Option<StderrHandler>,
    debug: bool,
) -> Vec<JoinHandle<()>> {
    let Some(stderr) = stderr else {
        return Vec::new();
    };
    if let Some(handler) = handler {
        let (sender, receiver) = mpsc::channel(STDERR_QUEUE_CAPACITY);
        vec![
            tokio::spawn(read_process_stderr(stderr, Some(sender), false)),
            tokio::spawn(dispatch_stderr(receiver, handler)),
        ]
    } else if debug {
        vec![tokio::spawn(read_process_stderr(stderr, None, true))]
    } else {
        Vec::new()
    }
}

async fn read_process_stderr(
    mut stderr: ChildStderr,
    mut chunks: Option<mpsc::Sender<String>>,
    mut debug: bool,
) {
    let mut parent_stderr = tokio::io::stderr();
    let mut buffer = [0_u8; STDERR_CHUNK_BYTES];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => return,
            Ok(read) => {
                if let Some(sender) = chunks.as_ref() {
                    let message = String::from_utf8_lossy(&buffer[..read]).into_owned();
                    if sender.send(message).await.is_err() {
                        // Continue draining if the callback worker has already stopped.
                        chunks = None;
                    }
                } else if debug && parent_stderr.write_all(&buffer[..read]).await.is_err() {
                    debug = false;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => return,
        }
    }
}

async fn dispatch_stderr(mut chunks: mpsc::Receiver<String>, handler: StderrHandler) {
    while let Some(message) = chunks.recv().await {
        let handler = handler.clone();
        let callback = tokio::task::spawn_blocking(move || {
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| handler(message)));
        });
        let _ = callback.await;
    }
}

async fn supervise_child(
    mut child: Child,
    mut shutdown: oneshot::Receiver<()>,
    exit_tx: oneshot::Sender<io::Result<ExitStatus>>,
    active_permissions: Arc<ActivePermissionRequests>,
    stderr_tasks: Vec<JoinHandle<()>>,
) {
    let result = tokio::select! {
        result = child.wait() => result,
        _ = &mut shutdown => {
            terminate_child(&mut child).await
        }
    };
    join_stderr_tasks(stderr_tasks).await;
    active_permissions.cancel_all();
    let _ = exit_tx.send(result);
}

async fn join_stderr_tasks(mut tasks: Vec<JoinHandle<()>>) {
    let joined = timeout(STDERR_DRAIN_SHUTDOWN_TIMEOUT, async {
        for task in &mut tasks {
            let _ = task.await;
        }
    })
    .await;
    if joined.is_err() {
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }
}

async fn terminate_child(child: &mut Child) -> io::Result<ExitStatus> {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: pid is obtained from the live child process handle.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    child.start_kill()?;

    match timeout(SHUTDOWN_GRACE_PERIOD, child.wait()).await {
        Ok(result) => result,
        Err(_) => {
            child.start_kill()?;
            child.wait().await
        }
    }
}

async fn read_process_output(
    stdout: tokio::process::ChildStdout,
    exit_rx: oneshot::Receiver<io::Result<ExitStatus>>,
    messages: mpsc::Sender<Result<Value, QueryError>>,
    writer: Arc<ProcessWriter>,
    max_message_bytes: usize,
) {
    let mut reader = BufReader::new(stdout);
    loop {
        match read_bounded_line(&mut reader, max_message_bytes).await {
            Ok(Some(BoundedLine::Line(mut bytes))) => {
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
                let line = String::from_utf8_lossy(&bytes);
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if !value.is_object() || value.get("type").and_then(Value::as_str).is_none() {
                    continue;
                }
                if messages.send(Ok(value)).await.is_err() {
                    writer.request_shutdown();
                    return;
                }
            }
            Ok(Some(BoundedLine::TooLong)) => {
                let _ = writer.close().await;
                let _ = messages
                    .send(Err(QueryError::MessageTooLarge {
                        limit: max_message_bytes,
                    }))
                    .await;
                return;
            }
            Ok(None) => break,
            Err(error) => {
                let _ = writer.close().await;
                let _ = messages.send(Err(QueryError::Io(error))).await;
                return;
            }
        }
    }

    match exit_rx.await {
        Ok(Ok(status)) if status.success() => {}
        Ok(Ok(status)) => {
            let error = exit_error(status);
            let _ = messages.send(Err(error)).await;
        }
        Ok(Err(error)) => {
            let _ = messages.send(Err(QueryError::Io(error))).await;
        }
        Err(_) => {
            let _ = messages.send(Err(QueryError::ProcessTerminated)).await;
        }
    }
}

enum BoundedLine {
    Line(Vec<u8>),
    TooLong,
}

async fn read_bounded_line<R>(reader: &mut R, limit: usize) -> io::Result<Option<BoundedLine>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::with_capacity(limit.min(8192));
    let mut over_limit = false;
    let mut read_any = false;
    loop {
        let (consume, newline, chunk) = {
            let available = reader.fill_buf().await?;
            if available.is_empty() {
                return if !read_any {
                    Ok(None)
                } else if over_limit {
                    Ok(Some(BoundedLine::TooLong))
                } else {
                    Ok(Some(BoundedLine::Line(line)))
                };
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consume = newline.map_or(available.len(), |index| index + 1);
            let content_len = newline.unwrap_or(consume);
            let chunk = if over_limit || line.len().saturating_add(content_len) > limit {
                over_limit = true;
                Vec::new()
            } else {
                available[..content_len].to_vec()
            };
            (consume, newline.is_some(), chunk)
        };
        reader.consume(consume);
        read_any = true;
        if !over_limit {
            line.extend_from_slice(&chunk);
        }
        if newline {
            return if over_limit {
                Ok(Some(BoundedLine::TooLong))
            } else {
                Ok(Some(BoundedLine::Line(line)))
            };
        }
    }
}

fn exit_error(status: ExitStatus) -> QueryError {
    if let Some(code) = status.code() {
        QueryError::ProcessExit { code }
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return QueryError::ProcessSignal { signal };
            }
        }
        QueryError::ProcessTerminated
    }
}

async fn route_messages(
    mut input: mpsc::Receiver<Result<Value, QueryError>>,
    messages: mpsc::Sender<Result<Value, QueryError>>,
    writer: Arc<ProcessWriter>,
    pending: PendingControls,
    active_permissions: Arc<ActivePermissionRequests>,
    single_turn: bool,
    closed: Arc<AtomicBool>,
    first_result: watch::Sender<bool>,
    can_use_tool: Option<CanUseToolHandler>,
    can_use_tool_timeout: Duration,
    mcp_message: Option<McpMessageHandler>,
    mcp_servers: McpServerRegistry,
    mcp_request_timeout: Duration,
    query_cancellation: QueryCancellation,
) {
    let mut permission_responses = FuturesUnordered::<PendingPermissionResponse>::new();
    let mut mcp_responses = FuturesUnordered::<PendingMcpResponse>::new();
    let mut mcp_notifications = FuturesUnordered::<PendingMcpNotification>::new();
    loop {
        tokio::select! {
            biased;
            _ = query_cancellation.cancelled() => {
                abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                return;
            }
            Some((request_id, result)) = mcp_responses.next(), if !mcp_responses.is_empty() => {
                let wire_message = match result {
                    Ok(response) => control_success_response(
                        &request_id,
                        json!({ "mcp_response": response }),
                    ),
                    Err(error) => control_error_response(&request_id, error),
                };
                tokio::select! {
                    biased;
                    _ = query_cancellation.cancelled() => {
                        abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                        return;
                    }
                    _ = writer.write_json(&wire_message) => {}
                }
            }
            Some(()) = mcp_notifications.next(), if !mcp_notifications.is_empty() => {}
            Some((request_id, permission_cancellation, response)) = permission_responses.next(), if !permission_responses.is_empty() => {
                active_permissions.remove_if_same(&request_id, &permission_cancellation);
                if let Some(response) = response {
                    let wire_message = control_success_response(&request_id, response);
                    tokio::select! {
                        biased;
                        _ = query_cancellation.cancelled() => {
                            abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                            return;
                        }
                        _ = writer.write_json(&wire_message) => {}
                    }
                }
            }
            message = input.recv() => {
                if closed.load(Ordering::Acquire) {
                    active_permissions.cancel_all();
                    break;
                }
                let Some(message) = message else {
                    active_permissions.cancel_all();
                    break;
                };
                let value = match message {
                    Ok(value) => value,
                    Err(error) => {
                        active_permissions.cancel_all();
                        reject_pending(&pending, &error.to_string()).await;
                        if !closed.load(Ordering::Acquire) {
                            tokio::select! {
                                biased;
                                _ = query_cancellation.cancelled() => {
                                    abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                }
                                _ = messages.send(Err(error)) => {}
                            }
                        }
                        return;
                    }
                };
                match value.get("type").and_then(Value::as_str) {
                    Some("control_response") => handle_control_response(&value, &pending).await,
                    Some("control_cancel_request") => {
                        handle_control_cancel(&value, &pending, &active_permissions).await;
                    }
                    Some("control_request") => {
                        let request = &value["request"];
                        let subtype = request.get("subtype").and_then(Value::as_str);
                        if subtype == Some("can_use_tool") {
                            let Some(request_id) = value.get("request_id").and_then(Value::as_str) else {
                                continue;
                            };
                            let request_id = request_id.to_owned();
                            let (cancellation, receiver) = watch::channel(false);
                            if !active_permissions.register(&request_id, cancellation.clone()) {
                                continue;
                            }
                            let handler = can_use_tool.clone();
                            let payload = request.clone();
                            let timeout = can_use_tool_timeout;
                            permission_responses.push(async move {
                                let signal = CanUseToolCancellation { receiver };
                                let response = can_use_tool_response(
                                    &payload,
                                    handler.as_ref(),
                                    timeout,
                                    signal,
                                    cancellation.clone(),
                                ).await;
                                (request_id, cancellation, response)
                            }.boxed());
                        } else if subtype == Some("mcp_message") {
                            let Some(request_id) = value.get("request_id").and_then(Value::as_str) else {
                                continue;
                            };
                            let request_id = request_id.to_owned();
                            let server_name = request
                                .get("server_name")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned();
                            let message = request.get("message").cloned().unwrap_or(Value::Null);
                            let is_request = message.as_object().is_some_and(|message| {
                                message.contains_key("method")
                                    && message.get("id").is_some_and(|id| !id.is_null())
                            });
                            let is_sdk_server = mcp_servers.contains_sdk_server(&server_name);
                            let handler = mcp_message.clone();
                            if !is_sdk_server && handler.is_none() {
                                let error = format!(
                                    "MCP server '{server_name}' not found in SDK-embedded servers"
                                );
                                let wire_message = control_error_response(&request_id, error);
                                tokio::select! {
                                    biased;
                                    _ = query_cancellation.cancelled() => {
                                        abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                        return;
                                    }
                                    _ = writer.write_json(&wire_message) => {}
                                }
                                continue;
                            }
                            let payload = McpMessageRequest { server_name, message };
                            if is_request {
                                let timeout_duration = mcp_request_timeout;
                                let mcp_servers = mcp_servers.clone();
                                let is_sdk_server = is_sdk_server;
                                mcp_responses.push(async move {
                                    let callback = AssertUnwindSafe(async move {
                                        if is_sdk_server {
                                            mcp_servers
                                                .dispatch(payload.server_name, payload.message)
                                                .await
                                        } else if let Some(handler) = handler {
                                            handler(payload).await
                                        } else {
                                            Err("MCP message handler is missing".into())
                                        }
                                    })
                                    .catch_unwind();
                                    let response = match timeout(timeout_duration, callback).await {
                                        Ok(Ok(result)) => result,
                                        Ok(Err(_)) => Err("MCP message callback panicked".into()),
                                        Err(_) => Err("MCP request timeout".into()),
                                    };
                                    (request_id, response)
                                }.boxed());
                            } else {
                                let mcp_servers = mcp_servers.clone();
                                let is_sdk_server = is_sdk_server;
                                mcp_notifications.push(async move {
                                    let callback = AssertUnwindSafe(async move {
                                        if is_sdk_server {
                                            let _ = mcp_servers
                                                .dispatch(payload.server_name, payload.message)
                                                .await;
                                        } else if let Some(handler) = handler {
                                            let _ = handler(payload).await;
                                        }
                                    })
                                    .catch_unwind();
                                    let _ = callback.await;
                                }.boxed());
                                let response = json!({
                                    "mcp_response": { "jsonrpc": "2.0", "result": {}, "id": 0 }
                                });
                                let wire_message = control_success_response(&request_id, response);
                                tokio::select! {
                                    biased;
                                    _ = query_cancellation.cancelled() => {
                                        abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                        return;
                                    }
                                    _ = writer.write_json(&wire_message) => {}
                                }
                            }
                        } else {
                            tokio::select! {
                                biased;
                                _ = query_cancellation.cancelled() => {
                                    abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                    return;
                                }
                                _ = handle_control_request(&value, &writer) => {}
                            }
                        }
                    }
                    _ => {
                        if value.get("type").and_then(Value::as_str) == Some("result") {
                            first_result.send_replace(true);
                            if single_turn {
                                tokio::select! {
                                    biased;
                                    _ = query_cancellation.cancelled() => {
                                        abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                        return;
                                    }
                                    _ = writer.end_input() => {}
                                }
                            }
                        }
                        tokio::select! {
                            biased;
                            _ = query_cancellation.cancelled() => {
                                abort_query_work(&messages, &writer, &pending, &active_permissions, &closed).await;
                                return;
                            }
                            result = messages.send(Ok(value)) => {
                                if result.is_err() {
                                    active_permissions.cancel_all();
                                    let _ = writer.close().await;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    active_permissions.cancel_all();
    reject_pending(&pending, "Transport closed before control response").await;
}

async fn abort_query_work(
    messages: &mpsc::Sender<Result<Value, QueryError>>,
    writer: &ProcessWriter,
    pending: &PendingControls,
    active_permissions: &ActivePermissionRequests,
    closed: &AtomicBool,
) {
    closed.store(true, Ordering::Release);
    active_permissions.cancel_all();
    reject_pending(pending, "Query aborted by user").await;
    writer.abort();
    let _ = messages.try_send(Err(QueryError::Aborted));
}

async fn handle_control_response(value: &Value, pending: &PendingControls) {
    let response = &value["response"];
    let Some(request_id) = response.get("request_id").and_then(Value::as_str) else {
        return;
    };
    let sender = pending.lock().await.remove(request_id);
    let Some(sender) = sender else {
        return;
    };
    let result = match response.get("subtype").and_then(Value::as_str) {
        Some("success") => Ok(response.get("response").cloned().unwrap_or(Value::Null)),
        _ => {
            let error = response.get("error");
            Err(error
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    error
                        .and_then(|value| value.get("message"))
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| "Unknown control request error".into()))
        }
    };
    let _ = sender.send(result);
}

async fn handle_control_cancel(
    value: &Value,
    pending: &PendingControls,
    active_permissions: &ActivePermissionRequests,
) {
    let Some(request_id) = value.get("request_id").and_then(Value::as_str) else {
        return;
    };
    active_permissions.cancel(request_id);
    if let Some(sender) = pending.lock().await.remove(request_id) {
        let _ = sender.send(Err("Request cancelled".into()));
    }
}

async fn handle_control_request(value: &Value, writer: &ProcessWriter) {
    let Some(request_id) = value.get("request_id").and_then(Value::as_str) else {
        return;
    };
    let subtype = value
        .get("request")
        .and_then(|request| request.get("subtype"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let response = control_error_response(
        request_id,
        format!("Unknown control request subtype: {subtype}"),
    );
    let _ = writer.write_json(&response).await;
}

fn control_success_response(request_id: &str, response: Value) -> Value {
    json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": request_id,
            "response": response,
        }
    })
}

fn control_error_response(request_id: &str, error: impl Into<String>) -> Value {
    json!({
        "type": "control_response",
        "response": {
            "subtype": "error",
            "request_id": request_id,
            "error": error.into(),
        }
    })
}

async fn can_use_tool_response(
    payload: &Value,
    handler: Option<&CanUseToolHandler>,
    timeout_duration: Duration,
    cancellation: CanUseToolCancellation,
    cancellation_sender: watch::Sender<bool>,
) -> Option<Value> {
    if cancellation.is_cancelled() {
        return None;
    }
    let Some(handler) = handler else {
        return Some(json!({ "behavior": "deny", "message": "Denied" }));
    };

    let Some(tool_name) = payload.get("tool_name").and_then(Value::as_str) else {
        return Some(permission_check_failure(
            "Invalid can_use_tool request: tool_name must be a string",
        ));
    };
    let Some(input) = payload.get("input").and_then(Value::as_object).cloned() else {
        return Some(permission_check_failure(
            "Invalid can_use_tool request: input must be an object",
        ));
    };
    let suggestions = match payload.get("permission_suggestions") {
        None | Some(Value::Null) => None,
        Some(Value::Array(suggestions)) => {
            match serde_json::from_value::<Vec<PermissionSuggestion>>(Value::Array(
                suggestions.clone(),
            )) {
                Ok(suggestions) => Some(suggestions),
                Err(error) => {
                    return Some(permission_check_failure(format!(
                        "Invalid can_use_tool permission_suggestions: {error}"
                    )));
                }
            }
        }
        Some(_) => {
            return Some(permission_check_failure(
                "Invalid can_use_tool request: permission_suggestions must be an array or null",
            ));
        }
    };

    let tool_use_id = match payload.get("tool_use_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(tool_use_id)) => Some(tool_use_id.clone()),
        Some(_) => {
            return Some(permission_check_failure(
                "Invalid can_use_tool request: tool_use_id must be a string or null",
            ));
        }
    };
    let blocked_path = match payload.get("blocked_path") {
        None | Some(Value::Null) => None,
        Some(Value::String(blocked_path)) => Some(blocked_path.clone()),
        Some(_) => {
            return Some(permission_check_failure(
                "Invalid can_use_tool request: blocked_path must be a string or null",
            ));
        }
    };

    let request = CanUseToolRequest {
        tool_name: tool_name.to_owned(),
        tool_use_id,
        input: input.clone(),
        suggestions,
        blocked_path,
        cancellation: cancellation.clone(),
    };
    let callback = AssertUnwindSafe(async move { handler(request).await }).catch_unwind();
    let result = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return None,
        result = timeout(timeout_duration, callback) => result,
    };
    let decision = match result {
        Ok(Ok(Ok(decision))) => decision,
        Ok(Ok(Err(error))) => {
            return Some(permission_check_failure(error));
        }
        Ok(Err(_)) => return Some(permission_check_failure("Permission callback panicked")),
        Err(_) => {
            cancellation_sender.send_replace(true);
            return Some(permission_check_failure("Permission callback timeout"));
        }
    };

    match decision.behavior {
        PermissionBehavior::Allow => {
            if decision.message.is_some() || decision.interrupt.is_some() {
                return Some(permission_check_failure(
                    "canUseTool allow result cannot include message or interrupt",
                ));
            }
            let updated_input = match decision.updated_input {
                Some(Value::Object(updated_input)) => updated_input,
                None => input,
                Some(_) => {
                    return Some(permission_check_failure(
                        "canUseTool allow result updatedInput must be an object",
                    ));
                }
            };
            Some(json!({ "behavior": "allow", "updatedInput": updated_input }))
        }
        PermissionBehavior::Deny => {
            if decision.updated_input.is_some() {
                return Some(permission_check_failure(
                    "canUseTool deny result cannot include updatedInput",
                ));
            }
            let mut response = Map::new();
            response.insert("behavior".into(), Value::String("deny".into()));
            response.insert(
                "message".into(),
                Value::String(decision.message.unwrap_or_else(|| "Denied".into())),
            );
            if let Some(interrupt) = decision.interrupt {
                response.insert("interrupt".into(), Value::Bool(interrupt));
            }
            Some(Value::Object(response))
        }
    }
}

fn permission_check_failure(message: impl Into<String>) -> Value {
    json!({
        "behavior": "deny",
        "message": format!("Permission check failed: {}", message.into()),
    })
}

async fn reject_pending(pending: &PendingControls, reason: &str) {
    let senders = std::mem::take(&mut *pending.lock().await);
    for (_, sender) in senders {
        let _ = sender.send(Err(reason.into()));
    }
}

fn build_cli_arguments(options: &QueryOptions, session_id: &str) -> Vec<String> {
    let mut args = vec![
        "--input-format".into(),
        "stream-json".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--channel=SDK".into(),
    ];
    if let Some(model) = &options.model {
        args.extend(["--model".into(), model.clone()]);
    }
    if let Some(prompt) = &options.system_prompt {
        match prompt {
            SystemPrompt::Override(prompt) => {
                args.extend(["--system-prompt".into(), prompt.clone()]);
            }
            SystemPrompt::Append(prompt) => {
                args.extend(["--append-system-prompt".into(), prompt.clone()]);
            }
        }
    }
    if let Some(mode) = options.permission_mode {
        args.extend(["--approval-mode".into(), mode.as_cli_value().into()]);
    }
    if let Some(turns) = options.max_session_turns {
        args.extend(["--max-session-turns".into(), turns.to_string()]);
    }
    push_list(&mut args, "--core-tools", &options.core_tools);
    push_list(&mut args, "--exclude-tools", &options.exclude_tools);
    push_list(&mut args, "--allowed-tools", &options.allowed_tools);
    if let Some(auth) = options.auth_type {
        args.extend(["--auth-type".into(), auth.as_cli_value().into()]);
    }
    if options.include_partial_messages {
        args.push("--include-partial-messages".into());
    }
    if let Some(resume) = options.resume.as_ref().filter(|resume| !resume.is_empty()) {
        args.extend(["--resume".into(), resume.clone()]);
    } else if options.continue_session {
        args.push("--continue".into());
    } else if !session_id.is_empty() {
        args.extend(["--session-id".into(), session_id.into()]);
    }
    if options.fork_session {
        args.push("--fork-session".into());
    }
    if let Some(calls) = options.max_tool_calls {
        args.extend(["--max-tool-calls".into(), calls.to_string()]);
    }
    if let Some(depth) = options.max_subagent_depth {
        args.extend(["--max-subagent-depth".into(), depth.to_string()]);
    }
    push_list(
        &mut args,
        "--include-directories",
        &options.include_directories,
    );
    push_list(&mut args, "--extensions", &options.extensions);
    push_list(
        &mut args,
        "--allowed-mcp-server-names",
        &options.allowed_mcp_server_names,
    );
    push_list(&mut args, "--fallback-model", &options.fallback_model);
    if let Some(proxy) = &options.proxy {
        args.extend(["--proxy".into(), proxy.clone()]);
    }
    for (enabled, flag) in [
        (options.sandbox, "--sandbox"),
        (options.safe_mode, "--safe-mode"),
        (options.insecure, "--insecure"),
        (options.worktree, "--worktree"),
    ] {
        if enabled {
            args.push(flag.into());
        }
    }
    push_list(
        &mut args,
        "--disabled-slash-commands",
        &options.disabled_slash_commands,
    );
    args.extend(options.extra_args.iter().cloned());
    args
}

fn push_list(args: &mut Vec<String>, flag: &str, values: &[String]) {
    if !values.is_empty() {
        args.extend([flag.into(), values.join(",")]);
    }
}

fn resolve_executable(spec: Option<&str>) -> Result<(String, Vec<String>), QueryError> {
    let Some(spec) = spec else {
        for candidate in bundled_cli_candidates() {
            if candidate.is_file() {
                let candidate = candidate.to_string_lossy();
                return resolve_executable(Some(&candidate));
            }
        }
        return Ok(("qwen".into(), Vec::new()));
    };
    if spec.trim().is_empty() {
        return Err(QueryError::InvalidOptions(
            "pathToQwenExecutable cannot be empty".into(),
        ));
    }
    if !spec.contains('/') && !spec.contains('\\') {
        if !spec
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
        {
            return Err(QueryError::InvalidOptions(format!(
                "Invalid command name '{spec}'; use letters, numbers, dots, hyphens, and underscores"
            )));
        }
        return Ok((spec.into(), Vec::new()));
    }

    let path = Path::new(spec);
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(QueryError::Spawn)?
            .join(path)
    };
    if !path.is_file() {
        return Err(QueryError::InvalidOptions(format!(
            "Executable file not found or is not a file: {}",
            path.display()
        )));
    }
    let path = path.canonicalize().unwrap_or(path);
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or_default();
    if matches!(
        extension.to_ascii_lowercase().as_str(),
        "js" | "mjs" | "cjs"
    ) {
        return Ok(("node".into(), vec![path.to_string_lossy().into_owned()]));
    }
    if matches!(extension.to_ascii_lowercase().as_str(), "ts" | "tsx") {
        return Ok(("tsx".into(), vec![path.to_string_lossy().into_owned()]));
    }
    Ok((path.to_string_lossy().into_owned(), Vec::new()))
}

fn bundled_cli_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    candidates.push(manifest_dir.join("dist/cli/cli.js"));
    add_sdk_package_candidates(&mut candidates, manifest_dir, true);

    if let Ok(executable) = std::env::current_exe() {
        if let Some(parent) = executable.parent() {
            add_sdk_package_candidates(&mut candidates, parent, false);
        }
    }
    if let Ok(current_dir) = std::env::current_dir() {
        add_sdk_package_candidates(&mut candidates, &current_dir, false);
    }

    let mut unique = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !unique.contains(&candidate) {
            unique.push(candidate);
        }
    }
    unique
}

fn add_sdk_package_candidates(
    candidates: &mut Vec<PathBuf>,
    start: &Path,
    include_monorepo_bundle: bool,
) {
    for ancestor in start.ancestors() {
        candidates.push(ancestor.join("node_modules/@qwen-code/sdk/dist/cli/cli.js"));
        candidates.push(ancestor.join("packages/sdk-typescript/dist/cli/cli.js"));
        if include_monorepo_bundle
            && ancestor
                .join("packages/sdk-typescript/package.json")
                .is_file()
        {
            candidates.push(ancestor.join("dist/cli.js"));
        }
    }
}

fn is_valid_session_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 || [8, 13, 18, 23].iter().any(|index| bytes[*index] != b'-') {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
    {
        return false;
    }
    matches!(bytes[14], b'1'..=b'5')
        && matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b')
}

fn new_session_id() -> String {
    Uuid::new_v4().to_string()
}

/// Start a single-turn query using the process SDK.
pub async fn query(prompt: impl Into<String>, options: QueryOptions) -> Result<Query, QueryError> {
    Query::query(prompt, options).await
}

fn parse_effort_status(value: &Value) -> Option<EffortStatus> {
    let applied = value.get("applied")?.as_bool()?;
    let effort_override = value
        .get("override")
        .and_then(Value::as_object)
        .and_then(|value| {
            Some(EffortOverride {
                source: value.get("source")?.as_str()?.to_owned(),
                field: value.get("field")?.as_str()?.to_owned(),
            })
        });
    let reason = value
        .get("reason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Some(EffortStatus {
        applied,
        effort_override,
        reason,
    })
}
