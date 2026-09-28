//! Inbound ACP JSON-RPC server for `canopy --acp`.
//!
//! The wire protocol is newline-delimited JSON-RPC on stdin/stdout. Diagnostics
//! stay on stderr so stdout remains safe for ACP clients.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use canopy_core::acp_bridge::{
    BridgeEvent, BridgeRestoreAction, BridgeRestoreRequest, BridgeSessionAgent,
    BridgeSessionFactory, BridgeSessionFactoryError, BridgeSessionRuntime,
    BridgeSessionRuntimeError, BridgeSessionRuntimeOptions, BridgeSessionScope, BridgeSpawnRequest,
    CreatedBridgeSession, EventBusOptions, JournalGrowthRegistry, LiveReplayMode,
    SESSION_SOURCE_META_KEY, SessionRuntimeFuture,
};
use canopy_core::agent_runtime::{
    AgentRunEvent, AgentRuntime, AgentRuntimeConfig, AgentToolExecutor,
};
use canopy_core::config::{
    LoadSettingsOptions, load_settings, merge_settings, update_setting_value,
};
use canopy_core::extension_inventory::{
    ExtensionInventoryOptions, load_active_local_extension_references,
};
use canopy_core::file_read_cache::FileReadCache;
use canopy_core::mcp::token_storage::ConfiguredTokenStorage;
use canopy_core::mcp::{McpOAuthProvider, McpOAuthProviderConfig};
use canopy_core::permissions::{
    PermissionCheckContext, PermissionDecision, PermissionRuleSet, RuleType, parse_rules,
    shell_command_uses_indirection,
};
use canopy_core::providers::anthropic::AnthropicProviderConfig;
use canopy_core::providers::gemini::GeminiProviderConfig;
use canopy_core::providers::openai_compatible::OpenAiCompatibleConfig;
use canopy_core::recording::{SessionRecorder, SessionRecorderOptions};
use canopy_core::services::at_resource_references::{
    AtResourceReferenceResolver, LocalExtensionReference,
};
use canopy_core::services::commit_attribution::AttributionSnapshot;
use canopy_core::services::daemon_memory_budget::{
    AvailableMemorySource, DaemonMemoryBudget, MemoryBudgetInput, resolve_daemon_memory_budget,
};
use canopy_core::services::file_history::FileHistorySnapshot;
use canopy_core::services::native_memory_probe::NativeMemoryProbe;
use canopy_core::services::session_attribution_state::restored_attribution_snapshot;
use canopy_core::services::usage_history::SessionMetrics;
use canopy_core::session_organization_store::{
    SESSION_ORGANIZATION_STORE_FILE, SessionOrganizationStore,
};
use canopy_core::session_paths::{SessionArchiveState, SessionPaths, get_project_hash};
use canopy_core::session_recovery::{
    HistoryGap, SessionRecoveryKind, SessionRecoveryOptions, build_session_recovery_plan,
};
use canopy_core::session_store::{SessionResumeOptions, SessionStore};
use canopy_core::session_writer::SessionWriterProcessKind;
use canopy_core::skills::{
    ListSkillsOptions, SkillConfig, SkillLevel, SkillManager, SkillManagerConfig,
};
use canopy_core::storage::Storage;
use canopy_core::tool_response_finalizer::ToolExecutionOutput;
use canopy_core::tools::mcp::client_manager::McpClientManager;
use canopy_core::tools::mcp::client_runtime::{McpPrompt, McpRequestOptions};
use canopy_core::transcript::{TranscriptRecord, TranscriptRecordType};
use canopy_core::turn::{ToolCallRequestInfo, TurnEvent};
use canopy_core::utils::cancellation::CancellationToken;
use futures_util::future::join_all;
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot, watch};

#[path = "acp_server/computer_use.rs"]
mod acp_computer_use;

use super::mcp_host::{
    ExtensionMcpSource, McpCliApprovalPrompt, McpCliSession, McpCliSettings, McpCliWorkspace,
    McpSessionSkillGrants, TerminalMcpApprovalPrompt,
};
use super::{
    RuntimeSettings, WorkspaceTools, declaration_is_enabled, load_runtime_settings,
    managed_memory_path_retention, record_synthesized_recovery_results,
    restored_file_history_snapshots, write_session_runtime_status,
};

const PROTOCOL_VERSION: u64 = 1;
const REQUESTED_SESSION_ID_META_KEY: &str = "qwen-code/sessionId";
const INITIAL_APPROVAL_MODE_META_KEY: &str = "qwen.session.approvalMode";
const MAX_STDIO_LINE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PENDING_ACP_INPUT_LINES: usize = 2;
const MAX_CONCURRENT_ACP_REQUESTS: usize = 16;
const DEFAULT_SESSION_PAGE_SIZE: usize = 20;
const MAX_SESSION_PAGE_SIZE: usize = 100;
const MAX_SESSION_FILES_TO_SCAN: usize = 10_000;
const MAX_PROMPT_SCAN_LINES: usize = 10;
const SESSION_TITLE_SCAN_BYTES: u64 = 64 * 1024;
const SESSION_SIDECAR_MAX_BYTES: u64 = 64 * 1024;
const ACP_MCP_OAUTH_PERMISSION_TIMEOUT: Duration = Duration::from_secs(30);
const ACP_MCP_OAUTH_FLOW_TIMEOUT: Duration = Duration::from_secs(8 * 60);
const ACP_USER_QUESTION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const ACP_TOOL_PERMISSION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const ACP_WORKSPACE_MEMORY_DREAM_TIMEOUT: Duration = Duration::from_millis(295_000);
const PRIVATE_ACP_CAPABILITY_ENV: &str = "QWEN_CODE_PRIVATE_ACP_CAPABILITY";
const PRIVATE_EXTERNAL_TOOL_GUARD_ENV: &str = "QWEN_CODE_PRIVATE_EXTERNAL_TOOL_GUARD";
const PRIVATE_EXTERNAL_TOOL_GUARD_PROVIDER_ENV: &str =
    "QWEN_CODE_PRIVATE_EXTERNAL_TOOL_GUARD_PROVIDER";
const EXTERNAL_TOOL_GUARD_REQUIRED_VALUE: &str = "required-v1";
const EXTERNAL_TOOL_GUARD_PROVIDER_ATTACHED_VALUE: &str = "attached-v1";
const MAX_ACP_PERMISSION_PREVIEW_BYTES: usize = 128 * 1024;
const MAX_ACP_PERMISSION_REQUEST_BYTES: usize = MAX_STDIO_LINE_BYTES - 1;
const MAX_ACP_USER_QUESTION_BYTES: usize = 64 * 1024;
const MAX_ACP_USER_QUESTION_ANSWER_BYTES: usize = 8 * 1024;
const MAX_ACP_USER_QUESTION_ANSWERS_BYTES: usize = 32 * 1024;
const MAX_ACP_USER_QUESTION_ERROR_CHARS: usize = 1_000;
const MAX_ACP_PROMPT_REFERENCES: usize = 32;
const MAX_ACP_REFERENCE_DIAGNOSTICS: usize = 32;
const MAX_ACP_AUDIO_BYTES: usize = 10 * 1024 * 1024;
const MAX_ACP_REFERENCE_DIAGNOSTIC_CHARS: usize = 1_000;
const MAX_ACP_STATS_COMMAND_BYTES: usize = 8 * 1024;
const MAX_ACP_STATS_OUTPUT_BYTES: usize = 32 * 1024;
const MAX_ACP_MEMORY_COMMAND_BYTES: usize = 8 * 1024;
const MAX_ACP_SKILL_REMINDER_BYTES: usize = 64 * 1024;
const MAX_ACP_MCP_PROMPT_REMINDER_BYTES: usize = 32 * 1024;
const MAX_ACP_MCP_PROMPT_ARGS_BYTES: usize = 16 * 1024;
const MAX_ACP_MCP_PROMPT_RESULT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
struct WorktreeSummary {
    slug: String,
    path: String,
    branch: String,
}

#[derive(Clone, Debug, Default)]
struct SessionOrganizationMetadata {
    group_id: Option<String>,
    color: Option<String>,
    pinned_at: Option<String>,
}

impl SessionOrganizationMetadata {
    fn is_pinned(&self) -> bool {
        self.pinned_at.is_some()
    }
}

#[derive(Clone, Debug, Default)]
struct SessionOrganizationSnapshot {
    group_ids: HashSet<String>,
    sessions: HashMap<String, SessionOrganizationMetadata>,
}

#[derive(Clone, Debug)]
struct SessionListItem {
    session_id: String,
    cwd: String,
    created_at: String,
    updated_at: String,
    activity_time: f64,
    display_name: Option<String>,
    parent_session_id: Option<String>,
    source_type: Option<String>,
    source_id: Option<String>,
    client_count: usize,
    has_active_prompt: bool,
    is_archived: bool,
    worktree: Option<WorktreeSummary>,
}

impl SessionListItem {
    fn to_response(&self) -> Value {
        let mut response = serde_json::Map::new();
        response.insert("sessionId".to_owned(), json!(self.session_id));
        response.insert("workspaceCwd".to_owned(), json!(self.cwd));
        response.insert("cwd".to_owned(), json!(self.cwd));
        response.insert("createdAt".to_owned(), json!(self.created_at));
        response.insert("updatedAt".to_owned(), json!(self.updated_at));
        if let Some(display_name) = &self.display_name {
            response.insert("displayName".to_owned(), json!(display_name));
            response.insert("title".to_owned(), json!(display_name));
        }
        if let Some(parent_session_id) = &self.parent_session_id {
            response.insert("parentSessionId".to_owned(), json!(parent_session_id));
        }
        if let Some(source_type) = &self.source_type {
            response.insert("sourceType".to_owned(), json!(source_type));
        }
        if let Some(source_id) = &self.source_id {
            response.insert("sourceId".to_owned(), json!(source_id));
        }
        response.insert("clientCount".to_owned(), json!(self.client_count));
        response.insert("hasActivePrompt".to_owned(), json!(self.has_active_prompt));
        response.insert("isArchived".to_owned(), json!(self.is_archived));
        if let Some(worktree) = &self.worktree {
            response.insert(
                "worktree".to_owned(),
                json!({
                    "slug":worktree.slug,
                    "path":worktree.path,
                    "branch":worktree.branch
                }),
            );
        }
        Value::Object(response)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct AcpOptions {
    model: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    provider: Option<AcpProviderKind>,
    system: Option<String>,
    max_turns: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcpProviderKind {
    OpenAiCompatible,
    Anthropic,
    Gemini,
}

impl AcpProviderKind {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "openai" => Some(Self::OpenAiCompatible),
            "anthropic" => Some(Self::Anthropic),
            "gemini" => Some(Self::Gemini),
            _ => None,
        }
    }

    fn from_auth_type(value: &str) -> Option<Self> {
        Self::parse(value)
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "OpenAI-compatible",
            Self::Anthropic => "Anthropic",
            Self::Gemini => "Gemini",
        }
    }

    fn model_env(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "OPENAI_MODEL",
            Self::Anthropic => "ANTHROPIC_MODEL",
            Self::Gemini => "GEMINI_MODEL",
        }
    }

    fn base_url_env(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "OPENAI_BASE_URL",
            Self::Anthropic => "ANTHROPIC_BASE_URL",
            Self::Gemini => "",
        }
    }

    fn default_base_url(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "https://api.openai.com/v1",
            Self::Anthropic => "https://api.anthropic.com",
            Self::Gemini => "https://generativelanguage.googleapis.com",
        }
    }

    fn model_requirement(self) -> &'static str {
        match self {
            Self::OpenAiCompatible => "specify --model <model> or set OPENAI_MODEL",
            Self::Anthropic => "specify --model <model> or set ANTHROPIC_MODEL",
            Self::Gemini => "specify --model <model> or set GEMINI_MODEL",
        }
    }

    fn auth_type(self) -> canopy_core::utils::error_parsing::AuthType {
        match self {
            Self::OpenAiCompatible => canopy_core::utils::error_parsing::AuthType::OpenAi,
            Self::Anthropic => canopy_core::utils::error_parsing::AuthType::Anthropic,
            Self::Gemini => canopy_core::utils::error_parsing::AuthType::Gemini,
        }
    }
}

/// Resolve a configured provider ID to the native ACP adapter it uses. An
/// explicit `providerProtocol` value takes precedence over the provider ID,
/// matching `ModelRegistry::resolveProviderProtocol`.
fn configured_provider_protocol(
    provider_id: &str,
    provider_protocol: Option<&Value>,
) -> Option<AcpProviderKind> {
    let protocol = match provider_protocol.and_then(|mapping| mapping.get(provider_id)) {
        Some(value) => value.as_str()?,
        None => provider_id,
    };
    AcpProviderKind::from_auth_type(protocol)
}

/// Find a saved model entry by model ID and, when present, the paired saved
/// endpoint. Duplicate IDs prefer an exact endpoint match and otherwise fall
/// back to the first provider entry in settings order, as the TypeScript CLI
/// resolver does.
fn find_configured_model_provider<'a>(
    settings: &'a Value,
    model_id: &str,
    required_protocol: Option<AcpProviderKind>,
    preferred_base_url: Option<&str>,
) -> Option<(AcpProviderKind, &'a Value)> {
    let model_providers = settings.get("modelProviders")?.as_object()?;
    let provider_protocol = settings.get("providerProtocol");
    let mut matches = Vec::new();

    for (provider_id, configured_models) in model_providers {
        let Some(protocol) = configured_provider_protocol(provider_id, provider_protocol) else {
            continue;
        };
        if required_protocol.is_some_and(|required| required != protocol) {
            continue;
        }
        let Some(configured_models) = configured_models.as_array() else {
            continue;
        };
        for model in configured_models {
            if model.get("id").and_then(Value::as_str) == Some(model_id) {
                matches.push((protocol, model));
            }
        }
    }

    preferred_base_url
        .and_then(|base_url| {
            matches
                .iter()
                .find(|(_, model)| model.get("baseUrl").and_then(Value::as_str) == Some(base_url))
        })
        .copied()
        .or_else(|| matches.first().copied())
}

fn nonempty_settings_string<'a>(value: Option<&'a Value>) -> Option<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

enum AcpRuntimeProviderConfig {
    OpenAiCompatible(OpenAiCompatibleConfig),
    Anthropic(AnthropicProviderConfig),
    Gemini(GeminiProviderConfig),
}

impl AcpOptions {
    fn max_turns(&self) -> usize {
        if self.max_turns == 0 {
            100
        } else {
            self.max_turns
        }
    }

    fn provider(&self) -> AcpProviderKind {
        self.provider.unwrap_or(AcpProviderKind::OpenAiCompatible)
    }
}

#[derive(Default)]
struct ClientRequestBroker {
    pending: Mutex<HashMap<String, PendingClientResponse>>,
    oauth_cancellations: Mutex<HashMap<String, (u64, CancellationToken)>>,
    next_id: AtomicU64,
    next_oauth_cancellation_id: AtomicU64,
}

struct PendingClientResponse {
    session_id: Option<String>,
    sender: oneshot::Sender<Result<Value, String>>,
}

#[derive(Clone)]
struct ProtocolOutput {
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    client_requests: Arc<ClientRequestBroker>,
}

impl ProtocolOutput {
    fn stdout() -> Self {
        Self {
            writer: Arc::new(Mutex::new(Box::new(io::stdout()))),
            client_requests: Arc::new(ClientRequestBroker::default()),
        }
    }

    fn write(&self, message: &Value) -> Result<(), String> {
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::to_writer(&mut *writer, message)
            .map_err(|error| format!("could not encode ACP message: {error}"))?;
        writer
            .write_all(b"\n")
            .and_then(|()| writer.flush())
            .map_err(|error| format!("could not write ACP message: {error}"))
    }

    fn response(&self, id: Value, result: Result<Value, RpcError>) -> Result<(), String> {
        let message = match result {
            Ok(result) => json!({"jsonrpc":"2.0", "id":id, "result":result}),
            Err(error) => error.response(id),
        };
        self.write(&message)
    }

    fn notification(&self, method: &str, params: Value) -> Result<(), String> {
        self.write(&json!({"jsonrpc":"2.0", "method":method, "params":params}))
    }

    async fn request_client(&self, method: &str, params: Value) -> Result<Value, String> {
        self.request_client_with_session(None, method, params).await
    }

    async fn request_client_for_session(
        &self,
        session_id: &str,
        method: &str,
        params: Value,
    ) -> Result<Value, String> {
        self.request_client_with_session(Some(session_id), method, params)
            .await
    }

    async fn request_client_with_session(
        &self,
        session_id: Option<&str>,
        method: &str,
        params: Value,
    ) -> Result<Value, String> {
        let id = json!(format!(
            "canopy-acp-{}",
            self.client_requests.next_id.fetch_add(1, Ordering::Relaxed) + 1
        ));
        let key = serde_json::to_string(&id)
            .map_err(|error| format!("could not encode ACP request id: {error}"))?;
        let (sender, receiver) = oneshot::channel();
        self.client_requests
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                key.clone(),
                PendingClientResponse {
                    session_id: session_id.map(str::to_owned),
                    sender,
                },
            );
        let _pending = PendingClientRequest {
            broker: self.client_requests.clone(),
            key,
        };
        if let Err(error) = self.write(&json!({
            "jsonrpc":"2.0",
            "id":id,
            "method":method,
            "params":params
        })) {
            return Err(error);
        }
        receiver
            .await
            .map_err(|_| "ACP client request was cancelled".to_owned())?
    }

    fn resolve_client_response(&self, message: &Value) -> bool {
        let Some(id) = message.get("id") else {
            return false;
        };
        let Ok(key) = serde_json::to_string(id) else {
            return false;
        };
        let sender = self
            .client_requests
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
        let Some(sender) = sender else {
            return false;
        };
        let response = if let Some(error) = message.get("error") {
            Err(format!("ACP client request failed: {error}"))
        } else if let Some(result) = message.get("result") {
            Ok(result.clone())
        } else {
            Err("ACP client returned a malformed response".to_owned())
        };
        let _ = sender.sender.send(response);
        true
    }

    fn register_oauth_cancellation(
        &self,
        session_id: &str,
    ) -> (CancellationToken, SessionOAuthCancellationGuard) {
        let id = self
            .client_requests
            .next_oauth_cancellation_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let cancellation = CancellationToken::new();
        self.client_requests
            .oauth_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_owned(), (id, cancellation.clone()));
        (
            cancellation,
            SessionOAuthCancellationGuard {
                broker: self.client_requests.clone(),
                session_id: session_id.to_owned(),
                id,
            },
        )
    }

    fn cancel_client_requests_for_session(&self, session_id: &str, reason: &str) {
        let pending = {
            let mut pending = self
                .client_requests
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ids = pending
                .iter()
                .filter(|(_, request)| request.session_id.as_deref() == Some(session_id))
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| pending.remove(&id))
                .collect::<Vec<_>>()
        };
        for request in pending {
            let _ = request.sender.send(Err(reason.to_owned()));
        }
        if let Some((_, cancellation)) = self
            .client_requests
            .oauth_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id)
        {
            cancellation.cancel();
        }
    }

    fn fail_pending_client_requests(&self, reason: &str) {
        let pending = std::mem::take(
            &mut *self
                .client_requests
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for (_, request) in pending {
            let _ = request.sender.send(Err(reason.to_owned()));
        }
        let cancellations = std::mem::take(
            &mut *self
                .client_requests
                .oauth_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for (_, cancellation) in cancellations.into_values() {
            cancellation.cancel();
        }
    }
}

struct PendingClientRequest {
    broker: Arc<ClientRequestBroker>,
    key: String,
}

impl Drop for PendingClientRequest {
    fn drop(&mut self) {
        self.broker
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

struct SessionOAuthCancellationGuard {
    broker: Arc<ClientRequestBroker>,
    session_id: String,
    id: u64,
}

impl Drop for SessionOAuthCancellationGuard {
    fn drop(&mut self) {
        let mut cancellations = self
            .broker
            .oauth_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cancellations
            .get(&self.session_id)
            .is_some_and(|(id, _)| *id == self.id)
        {
            cancellations.remove(&self.session_id);
        }
    }
}

async fn authorize_acp_mcp_servers(
    output: &ProtocolOutput,
    session_id: &str,
    settings: &mut McpCliSettings,
    discovery_errors: &HashMap<String, String>,
    cancellation: &CancellationToken,
) -> Result<bool, String> {
    if cancellation.is_cancelled() {
        return Err("ACP session was cancelled during MCP OAuth authorization".to_owned());
    }
    let mut candidates = discovery_errors
        .iter()
        .filter_map(|(server_name, error)| {
            let config = settings.servers.get(server_name)?;
            let auth_provider = config
                .get("authProviderType")
                .and_then(Value::as_str)
                .unwrap_or("dynamic_discovery");
            let explicit_authorization = config
                .get("headers")
                .and_then(Value::as_object)
                .is_some_and(|headers| {
                    headers
                        .keys()
                        .any(|header| header.eq_ignore_ascii_case("authorization"))
                });
            let has_http_url = config
                .get("httpUrl")
                .and_then(Value::as_str)
                .is_some_and(|url| !url.is_empty());
            let oauth_enabled =
                config.pointer("/oauth/enabled").and_then(Value::as_bool) == Some(true);
            let is_auth_challenge = {
                let error = error.to_ascii_lowercase();
                error.contains("requires oauth authentication")
                    || (error.contains("http 401") && error.contains("www-authenticate"))
            };
            let server_url = config
                .get("httpUrl")
                .and_then(Value::as_str)
                .filter(|url| !url.is_empty())
                .or_else(|| {
                    config
                        .get("url")
                        .and_then(Value::as_str)
                        .filter(|url| !url.is_empty())
                })?;
            (auth_provider == "dynamic_discovery"
                && !explicit_authorization
                && is_auth_challenge
                && (has_http_url || oauth_enabled))
                .then(|| (server_name.clone(), config.clone(), server_url.to_owned()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    if candidates.is_empty() {
        return Ok(false);
    }

    let mut authenticated_any = false;
    for (server_name, server_config, server_url) in candidates {
        if cancellation.is_cancelled() {
            return Err("ACP session was cancelled during MCP OAuth authorization".to_owned());
        }
        let request = json!({
            "sessionId":session_id,
            "options":[
                {"optionId":"proceed_once","name":"Allow","kind":"allow_once"},
                {"optionId":"cancel","name":"Reject","kind":"reject_once"}
            ],
            "toolCall":{
                "toolCallId":format!("mcp-oauth-{session_id}-{server_name}"),
                "status":"pending",
                "title":format!("Authorize MCP server {server_name}"),
                "kind":"other",
                "content":[{
                    "type":"content",
                    "content":{"type":"text","text":"Canopy will open a browser to authorize this MCP server and save its OAuth token. Continue?"}
                }],
                "rawInput":{"serverName":server_name},
                "_meta":{"toolName":"mcp_oauth_authorize","serverName":server_name}
            }
        });
        let permission = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err("ACP session was cancelled during MCP OAuth consent".to_owned());
            }
            result = tokio::time::timeout(
                ACP_MCP_OAUTH_PERMISSION_TIMEOUT,
                output.request_client_for_session(
                    session_id,
                    "session/request_permission",
                    request,
                ),
            ) => match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    eprintln!("[CANOPY] MCP OAuth consent for `{server_name}` was unavailable: {error}. Browser login was skipped.");
                    continue;
                }
                Err(_) => {
                    eprintln!("[CANOPY] MCP OAuth consent for `{server_name}` timed out after {} seconds. Browser login was skipped.", ACP_MCP_OAUTH_PERMISSION_TIMEOUT.as_secs());
                    continue;
                }
            }
        };
        match permission
            .pointer("/outcome/outcome")
            .and_then(Value::as_str)
        {
            Some("cancelled") => {
                return Err("ACP session was cancelled during MCP OAuth consent".to_owned());
            }
            Some("selected") => match permission
                .pointer("/outcome/optionId")
                .and_then(Value::as_str)
            {
                Some("proceed_once") => {}
                Some("cancel") => continue,
                _ => {
                    eprintln!(
                        "[CANOPY] MCP OAuth consent for `{server_name}` returned an unknown option. Browser login was skipped."
                    );
                    continue;
                }
            },
            _ => {
                eprintln!(
                    "[CANOPY] MCP OAuth consent for `{server_name}` returned an invalid response. Browser login was skipped."
                );
                continue;
            }
        }

        let mut oauth_config: McpOAuthProviderConfig = match server_config
            .get("oauth")
            .cloned()
            .filter(Value::is_object)
            .map(serde_json::from_value)
            .transpose()
        {
            Ok(config) => config.unwrap_or_default(),
            Err(error) => {
                eprintln!(
                    "[CANOPY] MCP OAuth configuration for `{server_name}` is invalid: {error}"
                );
                continue;
            }
        };
        oauth_config.enabled = Some(true);
        let provider = match McpOAuthProvider::new() {
            Ok(provider) => provider,
            Err(error) => {
                eprintln!("[CANOPY] Could not prepare MCP OAuth for `{server_name}`: {error}");
                continue;
            }
        };
        let storage = ConfiguredTokenStorage::new(
            Storage::get_mcp_oauth_tokens_path(),
            Storage::get_global_canopy_dir(),
        );
        let authorization = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err("ACP session was cancelled during MCP OAuth authorization".to_owned());
            }
            result = tokio::time::timeout(
                ACP_MCP_OAUTH_FLOW_TIMEOUT,
                provider.authenticate(
                    &storage,
                    server_name.clone(),
                    oauth_config,
                    Some(server_url),
                ),
            ) => result,
        };
        match authorization {
            Ok(Ok(_)) => {
                if let Some(server) = settings
                    .servers
                    .get_mut(&server_name)
                    .and_then(Value::as_object_mut)
                {
                    let mut oauth = server
                        .get("oauth")
                        .and_then(Value::as_object)
                        .cloned()
                        .unwrap_or_default();
                    oauth.insert("enabled".to_owned(), Value::Bool(true));
                    server.insert("oauth".to_owned(), Value::Object(oauth));
                    authenticated_any = true;
                }
            }
            Ok(Err(error)) => {
                eprintln!("[CANOPY] MCP OAuth authorization for `{server_name}` failed: {error}");
            }
            Err(_) => {
                eprintln!(
                    "[CANOPY] MCP OAuth authorization for `{server_name}` timed out after {} seconds.",
                    ACP_MCP_OAUTH_FLOW_TIMEOUT.as_secs()
                );
            }
        }
    }
    Ok(authenticated_any)
}

#[derive(Clone, Debug)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

type PromptTicketRegistry = Arc<Mutex<HashMap<String, HashMap<u64, watch::Sender<bool>>>>>;
type RequestCancellationRegistry = Arc<Mutex<HashMap<String, (u64, watch::Sender<bool>)>>>;

impl RpcError {
    fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::new(-32603, message)
    }

    fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    fn response(&self, id: Value) -> Value {
        let mut error = json!({"code":self.code, "message":self.message});
        if let Some(data) = &self.data {
            error["data"] = data.clone();
        }
        json!({"jsonrpc":"2.0", "id":id, "error":error})
    }
}

struct PromptTicket {
    session_id: String,
    sequence: u64,
    receiver: watch::Receiver<bool>,
    registry: PromptTicketRegistry,
}

impl PromptTicket {
    fn register(registry: &PromptTicketRegistry, sequence: &AtomicU64, session_id: String) -> Self {
        let id = sequence.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = watch::channel(false);
        registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(session_id.clone())
            .or_default()
            .insert(id, sender.clone());
        Self {
            session_id,
            sequence: id,
            receiver,
            registry: registry.clone(),
        }
    }
}

impl Drop for PromptTicket {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session_tickets) = registry.get_mut(&self.session_id) {
            session_tickets.remove(&self.sequence);
            if session_tickets.is_empty() {
                registry.remove(&self.session_id);
            }
        }
    }
}

struct RequestCancellationTicket {
    key: String,
    sequence: u64,
    receiver: watch::Receiver<bool>,
    registry: RequestCancellationRegistry,
}

impl RequestCancellationTicket {
    fn register(
        registry: &RequestCancellationRegistry,
        sequence: &AtomicU64,
        request_id: &Value,
    ) -> Self {
        let key =
            serde_json::to_string(request_id).expect("a JSON-RPC request id must be serializable");
        let sequence = sequence.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = watch::channel(false);
        registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone(), (sequence, sender));
        Self {
            key,
            sequence,
            receiver,
            registry: registry.clone(),
        }
    }

    fn cancel(registry: &RequestCancellationRegistry, request_id: &Value) {
        let Ok(key) = serde_json::to_string(request_id) else {
            return;
        };
        if let Some((_, sender)) = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
        {
            sender.send_replace(true);
        }
    }

    fn cancel_all(registry: &RequestCancellationRegistry) {
        let registry = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, sender) in registry.values() {
            sender.send_replace(true);
        }
    }
}

impl Drop for RequestCancellationTicket {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .get(&self.key)
            .is_some_and(|(sequence, _)| *sequence == self.sequence)
        {
            registry.remove(&self.key);
        }
    }
}

async fn wait_for_request_cancellation(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

fn managed_external_tool_guard_provider_attached() -> bool {
    std::env::var_os(PRIVATE_ACP_CAPABILITY_ENV).is_some()
        && std::env::var(PRIVATE_EXTERNAL_TOOL_GUARD_ENV).as_deref()
            == Ok(EXTERNAL_TOOL_GUARD_REQUIRED_VALUE)
        && std::env::var(PRIVATE_EXTERNAL_TOOL_GUARD_PROVIDER_ENV).as_deref()
            == Ok(EXTERNAL_TOOL_GUARD_PROVIDER_ATTACHED_VALUE)
}

fn native_acp_memory_budget() -> Option<DaemonMemoryBudget> {
    const BYTES_PER_MEGABYTE: u64 = 1024 * 1024;

    let probe = NativeMemoryProbe::system();
    let host_memory_bytes = probe.host_total_memory_bytes().ok()?;
    let effective_limit_bytes = probe
        .effective_memory_limit_bytes()
        .unwrap_or(host_memory_bytes);
    let (available_memory_bytes, available_memory_source) =
        if effective_limit_bytes > 0 && effective_limit_bytes < host_memory_bytes {
            (effective_limit_bytes, AvailableMemorySource::Constrained)
        } else {
            (host_memory_bytes, AvailableMemorySource::Host)
        };

    resolve_daemon_memory_budget(MemoryBudgetInput {
        budget_mb: None,
        available_memory_mb: available_memory_bytes / BYTES_PER_MEGABYTE,
        available_memory_source,
    })
    .ok()
}

struct WorkspaceRuntime {
    runtime: BridgeSessionRuntime,
    factory: Arc<CliAcpSessionFactory>,
}

struct WorkspaceRegistry {
    options: AcpOptions,
    output: ProtocolOutput,
    runtimes: AsyncMutex<HashMap<PathBuf, WorkspaceRuntime>>,
    sessions: Mutex<HashMap<String, PathBuf>>,
    memory_budget: Option<DaemonMemoryBudget>,
    journal_growth_registry: Arc<JournalGrowthRegistry>,
}

impl WorkspaceRegistry {
    fn new(options: AcpOptions, output: ProtocolOutput) -> Self {
        Self {
            options,
            output,
            runtimes: AsyncMutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            memory_budget: native_acp_memory_budget(),
            journal_growth_registry: Arc::new(JournalGrowthRegistry::new()),
        }
    }

    async fn runtime_for_cwd(&self, cwd: &str) -> Result<(PathBuf, WorkspaceRuntimeRef), RpcError> {
        let requested = Path::new(cwd);
        if !requested.is_absolute() {
            return Err(RpcError::invalid_params("cwd must be an absolute path"));
        }
        let workspace = std::fs::canonicalize(requested)
            .map_err(|error| RpcError::invalid_params(format!("invalid cwd: {error}")))?;
        if !workspace.is_dir() {
            return Err(RpcError::invalid_params("cwd must name a directory"));
        }
        let mut runtimes = self.runtimes.lock().await;
        if !runtimes.contains_key(&workspace) {
            let settings = load_runtime_settings(&workspace).map_err(RpcError::internal)?;
            let mcp_workspace = Arc::new(McpCliWorkspace::new_native(
                workspace.clone(),
                &settings.effective_env,
            ));
            let factory = Arc::new(CliAcpSessionFactory {
                options: self.options.clone(),
                output: self.output.clone(),
                workspace: workspace.clone(),
                mcp_workspace,
                memory_pressure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                approval_modes: Arc::new(Mutex::new(HashMap::new())),
                active_agents: Mutex::new(HashMap::new()),
                workspace_skill_snapshot: Mutex::new(None),
            });
            let factory_trait: Arc<dyn BridgeSessionFactory> = factory.clone();
            let mut runtime_options = BridgeSessionRuntimeOptions {
                bound_workspace: workspace.clone(),
                session_scope: BridgeSessionScope::Thread,
                max_sessions: None,
                max_pending_prompts_per_session: None,
                event_bus: EventBusOptions::default(),
                max_artifacts_per_session: None,
            };
            if let Some(memory_budget) = self.memory_budget {
                runtime_options = runtime_options.with_memory_budget(&memory_budget);
                if runtime_options
                    .event_bus
                    .journal_growth_pool_bytes
                    .is_some()
                {
                    runtime_options = runtime_options
                        .with_shared_journal_growth_registry(self.journal_growth_registry.clone());
                }
            }
            let runtime = BridgeSessionRuntime::new(runtime_options, factory_trait)
                .map_err(|error| RpcError::internal(error.to_string()))?;
            runtimes.insert(workspace.clone(), WorkspaceRuntime { runtime, factory });
        }
        let workspace_runtime = runtimes
            .get(&workspace)
            .expect("runtime was inserted above");
        Ok((
            workspace,
            WorkspaceRuntimeRef {
                runtime: workspace_runtime.runtime.clone(),
                factory: workspace_runtime.factory.clone(),
            },
        ))
    }

    fn session_workspace(&self, session_id: &str) -> Result<PathBuf, RpcError> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
            .ok_or_else(|| RpcError::new(-32002, format!("Session not found: {session_id}")))
    }

    async fn runtime_for_session(&self, session_id: &str) -> Result<WorkspaceRuntimeRef, RpcError> {
        let workspace = self.session_workspace(session_id)?;
        let runtimes = self.runtimes.lock().await;
        let entry = runtimes
            .get(&workspace)
            .ok_or_else(|| RpcError::new(-32002, format!("Session not found: {session_id}")))?;
        Ok(WorkspaceRuntimeRef {
            runtime: entry.runtime.clone(),
            factory: entry.factory.clone(),
        })
    }

    fn register_session(&self, session_id: &str, workspace: PathBuf) {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_owned(), workspace);
    }

    fn forget_session(&self, session_id: &str) {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }

    async fn shutdown(&self) {
        let runtimes = {
            let mut guard = self.runtimes.lock().await;
            std::mem::take(&mut *guard)
                .into_values()
                .map(|entry| (entry.runtime, entry.factory))
                .collect::<Vec<_>>()
        };
        for (runtime, factory) in runtimes {
            if let Err(error) = runtime.shutdown().await {
                eprintln!("canopy --acp: session shutdown failed: {error}");
            }
            factory.mcp_workspace.shutdown().await;
            factory
                .approval_modes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

#[derive(Clone)]
struct WorkspaceRuntimeRef {
    runtime: BridgeSessionRuntime,
    factory: Arc<CliAcpSessionFactory>,
}

struct AcpServer {
    registry: Arc<WorkspaceRegistry>,
    output: ProtocolOutput,
    initialized: std::sync::atomic::AtomicBool,
    prompt_tickets: PromptTicketRegistry,
    next_ticket: AtomicU64,
    request_cancellations: RequestCancellationRegistry,
    next_request_cancellation: AtomicU64,
    reject_guarded_hidden_memory_agents: bool,
}

impl AcpServer {
    fn new(options: AcpOptions, output: ProtocolOutput) -> Self {
        Self {
            registry: Arc::new(WorkspaceRegistry::new(options, output.clone())),
            output,
            initialized: std::sync::atomic::AtomicBool::new(false),
            prompt_tickets: Arc::new(Mutex::new(HashMap::new())),
            next_ticket: AtomicU64::new(1),
            request_cancellations: Arc::new(Mutex::new(HashMap::new())),
            next_request_cancellation: AtomicU64::new(1),
            reject_guarded_hidden_memory_agents: managed_external_tool_guard_provider_attached(),
        }
    }

    async fn serve(self: Arc<Self>, mut input: mpsc::Receiver<InputLine>) -> Result<(), String> {
        let mut requests = tokio::task::JoinSet::new();
        loop {
            let line = if requests.len() >= MAX_CONCURRENT_ACP_REQUESTS {
                tokio::select! {
                    line = input.recv() => line,
                    completed = requests.join_next() => {
                        if let Some(Err(error)) = completed {
                            eprintln!("canopy --acp request task failed: {error}");
                        }
                        continue;
                    }
                }
            } else {
                input.recv().await
            };
            let Some(line) = line else {
                break;
            };
            let line = match line {
                InputLine::Line(line) => line,
                InputLine::TooLarge => {
                    self.output.response(
                        Value::Null,
                        Err(RpcError::new(
                            -32600,
                            "ACP message exceeds the maximum line size",
                        )),
                    )?;
                    continue;
                }
                InputLine::ReadError(error) => return Err(error),
                InputLine::Eof => break,
            };
            if line.trim().is_empty() {
                continue;
            }
            let message: Value = match serde_json::from_str(&line) {
                Ok(message) => message,
                Err(error) => {
                    self.output.response(
                        Value::Null,
                        Err(RpcError::new(-32700, format!("Parse error: {error}"))),
                    )?;
                    continue;
                }
            };
            let Some(object) = message.as_object() else {
                self.output.response(
                    Value::Null,
                    Err(RpcError::new(-32600, "Invalid JSON-RPC request")),
                )?;
                continue;
            };
            if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
                let id = object.get("id").cloned().unwrap_or(Value::Null);
                self.output
                    .response(id, Err(RpcError::new(-32600, "jsonrpc must be \"2.0\"")))?;
                continue;
            }
            if object.get("method").is_none()
                && (object.contains_key("result") || object.contains_key("error"))
            {
                self.output.resolve_client_response(&message);
                continue;
            }
            let Some(method) = object.get("method").and_then(Value::as_str) else {
                let id = object.get("id").cloned().unwrap_or(Value::Null);
                self.output.response(
                    id,
                    Err(RpcError::new(-32600, "JSON-RPC method is required")),
                )?;
                continue;
            };
            let method = method.to_owned();
            let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
            let id = object.get("id").cloned();
            if method == "session/cancel" {
                let _ = self.cancel_session_notification(&params).await;
                if let Some(id) = id {
                    self.output.response(id, Ok(json!({})))?;
                }
                continue;
            }
            if method == "$/cancelRequest" {
                if let Some(request_id) = params.get("id") {
                    RequestCancellationTicket::cancel(&self.request_cancellations, request_id);
                }
                if let Some(id) = id {
                    self.output.response(id, Ok(json!({})))?;
                }
                continue;
            }
            let Some(id) = id else {
                // ACP notifications other than session/cancel need no reply.
                continue;
            };
            if !valid_request_id(&id) {
                self.output.response(
                    Value::Null,
                    Err(RpcError::new(-32600, "Invalid JSON-RPC request id")),
                )?;
                continue;
            }
            let ticket = if method == "session/prompt" {
                params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .map(|session_id| {
                        PromptTicket::register(
                            &self.prompt_tickets,
                            &self.next_ticket,
                            session_id.to_owned(),
                        )
                    })
            } else {
                None
            };
            let request_cancellation =
                (method == "qwen/control/workspace/memory/dream").then(|| {
                    RequestCancellationTicket::register(
                        &self.request_cancellations,
                        &self.next_request_cancellation,
                        &id,
                    )
                });
            if method == "initialize" {
                let result = self
                    .handle_request(&method, params, ticket, request_cancellation)
                    .await;
                self.output.response(id, result)?;
                continue;
            }
            let server = self.clone();
            requests.spawn(async move {
                let result = server
                    .handle_request(&method, params, ticket, request_cancellation)
                    .await;
                let publish_available_commands = matches!(
                    method.as_str(),
                    "session/new" | "session/load" | "unstable_resumeSession" | "session/resume"
                )
                .then(|| {
                    result
                        .as_ref()
                        .ok()?
                        .get("sessionId")?
                        .as_str()
                        .map(str::to_owned)
                })
                .flatten();
                if let Err(error) = server.output.response(id, result) {
                    eprintln!("canopy --acp: {error}");
                    return;
                }
                if let Some(session_id) = publish_available_commands {
                    server
                        .publish_available_commands_for_session(&session_id)
                        .await;
                }
            });
        }

        self.cancel_all_prompts();
        RequestCancellationTicket::cancel_all(&self.request_cancellations);
        self.output
            .fail_pending_client_requests("ACP server shut down");
        while requests.join_next().await.is_some() {}
        self.registry.shutdown().await;
        Ok(())
    }

    async fn publish_available_commands_for_session(&self, session_id: &str) {
        let Ok(runtime) = self.registry.runtime_for_session(session_id).await else {
            return;
        };
        let Some(agent) = runtime
            .factory
            .active_agents()
            .into_iter()
            .find(|agent| agent.session_id == session_id)
        else {
            return;
        };
        if let Err(error) = agent.publish_available_commands().await {
            eprintln!("canopy --acp: session {session_id} initial command update failed: {error}");
        }
    }

    async fn handle_request(
        &self,
        method: &str,
        params: Value,
        ticket: Option<PromptTicket>,
        request_cancellation: Option<RequestCancellationTicket>,
    ) -> Result<Value, RpcError> {
        if method == "initialize" {
            if self
                .initialized
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(RpcError::invalid_params(
                    "initialize may only be called once",
                ));
            }
            return Ok(initialize_response());
        }
        if !self.initialized.load(Ordering::Acquire) {
            return Err(RpcError::new(-32002, "initialize must be called first"));
        }

        match method {
            "session/new" => self.new_session(&params).await,
            "session/list" => self.list_sessions(&params).await,
            "session/load" => {
                self.restore_session(&params, BridgeRestoreAction::Load)
                    .await
            }
            "unstable_resumeSession" | "session/resume" => {
                self.restore_session(&params, BridgeRestoreAction::Resume)
                    .await
            }
            "session/prompt" => self.prompt(&params, ticket).await,
            "session/set_mode" => self.set_session_mode(&params).await,
            "session/close" | "qwen/control/session/close" => self.close_session(&params).await,
            "qwen/control/workspace/memory/dream" => {
                self.workspace_memory_dream(&params, request_cancellation)
                    .await
            }
            "qwen/control/workspace/skills/refresh" => self.workspace_skills_refresh(&params).await,
            _ => Err(RpcError::new(-32601, format!("Method not found: {method}"))),
        }
    }

    async fn workspace_skills_refresh(&self, params: &Value) -> Result<Value, RpcError> {
        let reason = match params.get("reason") {
            None => "all",
            Some(Value::String(reason))
                if matches!(reason.as_str(), "settings" | "content" | "all") =>
            {
                reason.as_str()
            }
            _ => {
                return Err(RpcError::invalid_params(
                    "reason must be settings, content, or all",
                ));
            }
        };
        let cwd = required_string(params, "cwd")?;
        let (_, runtime) = self.registry.runtime_for_cwd(cwd).await?;
        runtime
            .factory
            .refresh_workspace_skills(reason)
            .await
            .map_err(RpcError::internal)
    }

    async fn workspace_memory_dream(
        &self,
        params: &Value,
        request_cancellation: Option<RequestCancellationTicket>,
    ) -> Result<Value, RpcError> {
        if self.reject_guarded_hidden_memory_agents {
            return Err(RpcError::invalid_params(
                "Managed external tool guard v1 does not support agent-backed workspace memory dream.",
            ));
        }
        let cwd = required_string(params, "cwd")?;
        let (_, runtime) = self.registry.runtime_for_cwd(cwd).await?;
        let request_cancellation = request_cancellation.ok_or_else(|| {
            RpcError::internal("workspace dream request has no cancellation ticket")
        })?;
        runtime
            .factory
            .run_workspace_memory_dream(request_cancellation)
            .await
    }

    async fn new_session(&self, params: &Value) -> Result<Value, RpcError> {
        let cwd = required_string(params, "cwd")?;
        let session_mcp_servers = parse_session_mcp_servers(params, true)?;
        let requested_id = requested_session_id(params)?;
        let requested_approval_mode = initial_approval_mode_from_metadata(params)?;
        let safe_mode = canopy_core::utils::safe_mode::is_safe_mode_env();
        let bare_mode = canopy_core::utils::bare_mode::is_bare_mode(None);
        let (workspace, runtime) = self.registry.runtime_for_cwd(cwd).await?;
        let source = params
            .get("_meta")
            .and_then(|meta| meta.get(SESSION_SOURCE_META_KEY));
        let request = BridgeSpawnRequest {
            workspace_cwd: workspace.to_string_lossy().into_owned(),
            session_scope: Some("thread".to_owned()),
            session_id: requested_id,
            source_type: source
                .and_then(|value| value.get("sourceType"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            source_id: source
                .and_then(|value| value.get("sourceId"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            session_mcp_servers,
            ..BridgeSpawnRequest::default()
        };
        let session = runtime
            .runtime
            .spawn_or_attach(request)
            .await
            .map_err(runtime_error)?;
        if let Some(mode) = requested_approval_mode.filter(|_| !safe_mode && !bare_mode) {
            let Some(mode_state) = runtime.factory.approval_mode_state(&session.session_id) else {
                let _ = runtime
                    .runtime
                    .kill_session(&session.session_id, false)
                    .await;
                runtime.factory.forget_approval_mode(&session.session_id);
                return Err(RpcError::internal(
                    "new ACP session has no approval mode state",
                ));
            };
            if let Err(error) = mode_state.set(mode) {
                let _ = runtime
                    .runtime
                    .kill_session(&session.session_id, false)
                    .await;
                runtime.factory.forget_approval_mode(&session.session_id);
                return Err(error);
            }
        }
        self.registry
            .register_session(&session.session_id, workspace);
        let model = runtime
            .factory
            .resolve_model(&runtime.runtime.bound_workspace().clone())
            .map_err(RpcError::internal)?;
        let mode = runtime
            .factory
            .approval_mode_state(&session.session_id)
            .map(|state| state.current())
            .unwrap_or(AcpApprovalMode::Default);
        Ok(session_state_response(&session.session_id, &model, mode))
    }

    async fn list_sessions(&self, params: &Value) -> Result<Value, RpcError> {
        let workspace_request = params
            .get("workspaceCwd")
            .and_then(Value::as_str)
            .or_else(|| params.get("cwd").and_then(Value::as_str))
            .map(str::to_owned)
            .unwrap_or_else(|| {
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .to_string_lossy()
                    .into_owned()
            });
        let (workspace, runtime) = self.registry.runtime_for_cwd(&workspace_request).await?;

        let cursor = params
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let size = params
            .get("_meta")
            .and_then(|meta| meta.get("size"))
            .and_then(safe_integer)
            .map(|size| size.clamp(1, MAX_SESSION_PAGE_SIZE as i64) as usize)
            .unwrap_or(DEFAULT_SESSION_PAGE_SIZE);
        let archive_state = parse_archive_state(params)?;
        let raw_view = match params.get("view") {
            None => None,
            Some(Value::String(view)) => Some(view.as_str()),
            Some(_) => return Err(RpcError::invalid_params("`view` must be \"organized\"")),
        };
        if let Some(view) = raw_view
            && view != "organized"
        {
            return Err(RpcError::invalid_params("`view` must be \"organized\""));
        }
        let group = match params.get("group") {
            None => None,
            Some(Value::String(group)) => Some(group.as_str()),
            Some(_) => return Err(RpcError::invalid_params("`group` must be a string")),
        };
        if group.is_some() && raw_view != Some("organized") {
            return Err(RpcError::invalid_params(
                "`group` requires `view` to be \"organized\"",
            ));
        }
        let parent_session_id = params
            .get("parentSessionId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if parent_session_id.as_deref() == Some("") {
            return Err(RpcError::invalid_params(
                "`parentSessionId` must be a non-empty string",
            ));
        }
        let (source_type, source_id) = parse_list_source(params)?;
        if raw_view == Some("organized") && parent_session_id.is_some() {
            return Err(RpcError::invalid_params(
                "`parentSessionId` is not supported with `view` \"organized\"",
            ));
        }
        let store = session_store(&workspace);
        if raw_view == Some("organized") {
            let group = group.unwrap_or("all");
            let organization = read_session_organization_snapshot(store.paths());
            if group != "all"
                && group != "pinned"
                && group != "ungrouped"
                && !organization.group_ids.contains(group)
            {
                return Err(RpcError::invalid_params(format!(
                    "Group not found: {group}"
                )));
            }

            let cursor_key = cursor
                .as_deref()
                .filter(|cursor| !cursor.is_empty())
                .map(|cursor| {
                    parse_organized_session_cursor(
                        cursor,
                        group,
                        archive_state,
                        source_type.as_deref(),
                        source_id.as_deref(),
                    )
                })
                .transpose()?;
            let is_first_page = cursor_key.is_none();
            let (mut sessions, truncated) = scan_persisted_sessions(store.paths(), archive_state)?;

            if archive_state == SessionArchiveState::Active && is_first_page {
                for live in runtime.runtime.sessions() {
                    if let Some(existing) = sessions
                        .iter_mut()
                        .find(|item| item.session_id == live.session_id)
                    {
                        merge_live_session(existing, &live);
                    } else {
                        let transcript_path = store
                            .transcript_path(&live.session_id, SessionArchiveState::Active)
                            .map_err(|error| RpcError::internal(error.to_string()))?;
                        if !transcript_path.exists() {
                            sessions.push(live_session_summary(&live));
                        }
                    }
                }
            }

            let filtered = sessions
                .into_iter()
                .filter_map(|session| {
                    if !matches_source(&session, source_type.as_deref(), source_id.as_deref()) {
                        return None;
                    }
                    let metadata = organization
                        .sessions
                        .get(&session.session_id)
                        .cloned()
                        .unwrap_or_default();
                    if !matches_organized_group(&metadata, group) {
                        return None;
                    }
                    Some((session, metadata))
                })
                .collect::<Vec<_>>();
            let mut filtered = filtered;
            filtered.sort_by(|(session_a, metadata_a), (session_b, metadata_b)| {
                metadata_b
                    .is_pinned()
                    .cmp(&metadata_a.is_pinned())
                    .then_with(|| compare_activity(session_a, session_b))
            });

            let after_cursor = filtered
                .into_iter()
                .filter(|(session, metadata)| {
                    cursor_key.as_ref().is_none_or(|cursor| {
                        compare_organized_cursor_keys(
                            cursor,
                            &organized_cursor_key(session, metadata),
                        ) == std::cmp::Ordering::Less
                    })
                })
                .collect::<Vec<_>>();
            let next_cursor = if after_cursor.len() > size {
                after_cursor.get(size - 1).map(|(session, metadata)| {
                    encode_organized_session_cursor(
                        &organized_cursor_key(session, metadata),
                        group,
                        archive_state,
                        source_type.as_deref(),
                        source_id.as_deref(),
                    )
                })
            } else {
                None
            };
            let page = after_cursor.into_iter().take(size).collect::<Vec<_>>();
            let mut response = json!({
                "sessions": page.iter().map(|(session, metadata)| {
                    organized_session_response(session, metadata)
                }).collect::<Vec<_>>()
            });
            if let Some(cursor) = next_cursor {
                response["nextCursor"] = json!(cursor);
            }
            if truncated {
                response["truncated"] = json!(true);
            }
            return Ok(response);
        }
        let metadata_filter = parent_session_id.is_some() || source_type.is_some();
        let (mut persisted, truncated) = scan_persisted_sessions(store.paths(), archive_state)?;

        let (sessions, next_cursor) = if metadata_filter {
            for live in runtime.runtime.sessions() {
                if archive_state == SessionArchiveState::Active {
                    let path = store
                        .transcript_path(&live.session_id, SessionArchiveState::Active)
                        .map_err(|error| RpcError::internal(error.to_string()))?;
                    if let Some(existing) = persisted
                        .iter_mut()
                        .find(|item| item.session_id == live.session_id)
                    {
                        merge_live_session(existing, &live);
                    } else if !path.exists() {
                        persisted.push(live_session_summary(&live));
                    }
                }
            }
            persisted.retain(|session| {
                parent_session_id
                    .as_deref()
                    .is_none_or(|expected| session.parent_session_id.as_deref() == Some(expected))
                    && matches_source(session, source_type.as_deref(), source_id.as_deref())
            });
            persisted.sort_by(compare_activity);
            let cursor_key = if cursor.as_deref().is_some_and(|cursor| !cursor.is_empty()) {
                Some(parse_metadata_session_cursor(
                    cursor.as_deref().unwrap_or_default(),
                    parent_session_id.as_deref(),
                    source_type.as_deref(),
                    source_id.as_deref(),
                    archive_state,
                )?)
            } else {
                None
            };
            let after_cursor = persisted
                .into_iter()
                .filter(|session| {
                    cursor_key
                        .as_ref()
                        .is_none_or(|key| is_after_cursor(session, key))
                })
                .collect::<Vec<_>>();
            let next_cursor = if after_cursor.len() > size {
                after_cursor.get(size - 1).map(|item| {
                    encode_metadata_session_cursor(
                        item,
                        parent_session_id.as_deref(),
                        source_type.as_deref(),
                        source_id.as_deref(),
                        archive_state,
                    )
                })
            } else {
                None
            };
            (
                after_cursor.into_iter().take(size).collect::<Vec<_>>(),
                next_cursor,
            )
        } else {
            let numeric_cursor = match cursor.as_deref() {
                None | Some("") => None,
                Some(value) => Some(parse_numeric_session_cursor(value)?),
            };
            let page_candidates = persisted
                .into_iter()
                .filter(|item| numeric_cursor.is_none_or(|cursor| item.activity_time < cursor))
                .collect::<Vec<_>>();
            let next_cursor = if page_candidates.len() > size {
                page_candidates
                    .get(size - 1)
                    .map(|item| item.activity_time.to_string())
            } else {
                None
            };
            let has_next = next_cursor.is_some();
            let mut page = page_candidates.into_iter().take(size).collect::<Vec<_>>();
            if archive_state == SessionArchiveState::Active && numeric_cursor.is_none() {
                for live in runtime.runtime.sessions() {
                    if let Some(existing) = page
                        .iter_mut()
                        .find(|item| item.session_id == live.session_id)
                    {
                        merge_live_session(existing, &live);
                    } else {
                        let transcript_path = store
                            .transcript_path(&live.session_id, SessionArchiveState::Active)
                            .map_err(|error| RpcError::internal(error.to_string()))?;
                        if !transcript_path.exists() {
                            page.push(live_session_summary(&live));
                        }
                    }
                }
                page.sort_by(compare_activity);
            }
            let next_cursor = if has_next { next_cursor } else { None };
            (page, next_cursor)
        };

        let mut response = json!({
            "sessions":sessions.iter().map(SessionListItem::to_response).collect::<Vec<_>>()
        });
        if let Some(cursor) = next_cursor {
            response["nextCursor"] = json!(cursor);
        }
        if truncated {
            response["truncated"] = json!(true);
        }
        Ok(response)
    }

    async fn restore_session(
        &self,
        params: &Value,
        action: BridgeRestoreAction,
    ) -> Result<Value, RpcError> {
        let session_id = required_string(params, "sessionId")?.to_owned();
        let cwd = required_string(params, "cwd")?;
        let session_mcp_servers =
            parse_session_mcp_servers(params, action == BridgeRestoreAction::Load)?;
        let (workspace, runtime) = self.registry.runtime_for_cwd(cwd).await?;
        let meta = params.get("_meta");
        let bulk_replay = action == BridgeRestoreAction::Load
            && meta
                .and_then(|meta| meta.get("qwen.session.loadReplayMode"))
                .and_then(Value::as_str)
                == Some("bulk");
        let history_replay = meta
            .and_then(|meta| meta.get("qwen.session.loadReplayMode"))
            .and_then(Value::as_str)
            .map(|mode| {
                if mode == "bulk" {
                    "response".to_owned()
                } else {
                    "stream".to_owned()
                }
            });
        let live_replay_mode = params.get("liveReplayMode").map(|mode| {
            mode.as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| mode.to_string())
        });
        let request = BridgeRestoreRequest {
            session_id: session_id.clone(),
            workspace_cwd: workspace.to_string_lossy().into_owned(),
            history_replay,
            live_replay_mode: live_replay_mode.clone(),
            history_page_size: meta
                .and_then(|meta| meta.get("qwen.session.loadReplayPageSize"))
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok()),
            hide_inherited_history: meta
                .and_then(|meta| meta.get("qwen.session.loadReplayHideInherited"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            source_type: meta
                .and_then(|meta| meta.get(SESSION_SOURCE_META_KEY))
                .and_then(|value| value.get("sourceType"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            source_id: meta
                .and_then(|meta| meta.get(SESSION_SOURCE_META_KEY))
                .and_then(|value| value.get("sourceId"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            session_mcp_servers,
            ..BridgeRestoreRequest::default()
        };
        runtime
            .runtime
            .restore_session(action, request)
            .await
            .map_err(runtime_error)?;
        self.registry.register_session(&session_id, workspace);
        let mut response = {
            let model = runtime
                .factory
                .resolve_model(&runtime.runtime.bound_workspace().clone())
                .map_err(RpcError::internal)?;
            let mode = runtime
                .factory
                .approval_mode_state(&session_id)
                .map(|state| state.current())
                .unwrap_or(AcpApprovalMode::Default);
            session_state_response(&session_id, &model, mode)
        };
        if action == BridgeRestoreAction::Load {
            let transcript_events = runtime
                .factory
                .transcript_replay_events(&session_id)
                .map_err(RpcError::internal)?;
            let session = runtime
                .runtime
                .session(&session_id)
                .ok_or_else(|| RpcError::internal("restored session is unavailable"))?;
            let replay_mode = match live_replay_mode.as_deref() {
                Some("summary") => LiveReplayMode::Summary,
                _ => LiveReplayMode::Full,
            };
            let initial_snapshot = session.events.replay_snapshot(replay_mode);
            if initial_snapshot.last_event_id == 0
                && initial_snapshot.compacted_turns.is_empty()
                && initial_snapshot.live_journal.is_empty()
            {
                session.events.seed_replay_events(transcript_events);
            }
            let snapshot = session.events.replay_snapshot(replay_mode);
            let history_truncated = snapshot
                .compacted_turns
                .iter()
                .chain(snapshot.live_journal.iter())
                .any(|event| event.event_type == "history_truncated");
            let updates = snapshot
                .compacted_turns
                .iter()
                .chain(snapshot.live_journal.iter())
                .filter(|event| event.event_type == "session_update")
                .filter_map(|event| event.data.get("update").cloned())
                .collect::<Vec<_>>();
            if bulk_replay {
                let mut replay = json!({"v":1,"updates":updates});
                if history_truncated {
                    replay["partial"] = json!(true);
                }
                response["_meta"] = json!({"qwen.session.loadReplay":replay});
            } else {
                for update in updates {
                    self.output
                        .notification(
                            "session/update",
                            json!({"sessionId":session_id,"update":update}),
                        )
                        .map_err(RpcError::internal)?;
                }
            }
        }
        Ok(response)
    }

    async fn prompt(
        &self,
        params: &Value,
        ticket: Option<PromptTicket>,
    ) -> Result<Value, RpcError> {
        let session_id = required_string(params, "sessionId")?.to_owned();
        let blocks = params
            .get("prompt")
            .and_then(Value::as_array)
            .ok_or_else(|| RpcError::invalid_params("prompt must be an array"))?;
        let text = prompt_text(blocks)?;
        if text.trim().is_empty()
            && !blocks.iter().any(|block| {
                matches!(
                    block.get("type").and_then(Value::as_str),
                    Some("image" | "audio")
                )
            })
        {
            return Err(RpcError::invalid_params(
                "prompt must contain text, image, or audio content",
            ));
        }
        let runtime = self.registry.runtime_for_session(&session_id).await?;
        let ticket =
            ticket.ok_or_else(|| RpcError::invalid_params("missing prompt cancellation state"))?;
        let _ = self.output.notification(
            "session/update",
            json!({
                "sessionId":session_id,
                "update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":text}}
            }),
        );
        let response = runtime
            .runtime
            .send_prompt(
                &session_id,
                Value::Array(blocks.clone()),
                ticket.receiver.clone(),
            )
            .await;
        drop(ticket);
        match response {
            Ok(value) => Ok(value),
            Err(BridgeSessionRuntimeError::PromptAborted) => Ok(json!({"stopReason":"cancelled"})),
            Err(error) => Err(runtime_error(error)),
        }
    }

    async fn set_session_mode(&self, params: &Value) -> Result<Value, RpcError> {
        let session_id = required_string(params, "sessionId")?.to_owned();
        let runtime = self.registry.runtime_for_session(&session_id).await?;
        let mode_value = params
            .get("modeId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                let value = params
                    .get("modeId")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "undefined".to_owned());
                RpcError::invalid_params(format!("Unknown approval mode: {value}"))
            })?;
        let mode = AcpApprovalMode::parse(mode_value).ok_or_else(|| {
            RpcError::invalid_params(format!("Unknown approval mode: {mode_value}"))
        })?;
        let state = runtime
            .factory
            .approval_mode_state(&session_id)
            .ok_or_else(|| RpcError::new(-32002, format!("Session not found: {session_id}")))?;
        state.set(mode)?;

        // Like the TypeScript ACP session, approval mode is session-local and
        // is not written into settings or the transcript. Publish the same
        // current-mode update shape used by its bridge demultiplexer.
        let _ = self.output.notification(
            "session/update",
            json!({
                "sessionId":session_id,
                "update":{"sessionUpdate":"current_mode_update","currentModeId":mode.as_str()}
            }),
        );
        Ok(json!({}))
    }

    async fn cancel_session_notification(&self, params: &Value) -> Result<(), RpcError> {
        let session_id = required_string(params, "sessionId")?.to_owned();
        self.output
            .cancel_client_requests_for_session(&session_id, "ACP session was cancelled");
        self.cancel_prompts_for(&session_id);
        if let Ok(runtime) = self.registry.runtime_for_session(&session_id).await {
            runtime
                .runtime
                .cancel(&session_id)
                .await
                .map_err(runtime_error)?;
        }
        Ok(())
    }

    async fn close_session(&self, params: &Value) -> Result<Value, RpcError> {
        let session_id = required_string(params, "sessionId")?.to_owned();
        let runtime = self.registry.runtime_for_session(&session_id).await?;
        let closed = runtime
            .runtime
            .kill_session(&session_id, false)
            .await
            .map_err(runtime_error)?;
        if closed {
            self.registry.forget_session(&session_id);
            runtime.factory.forget_approval_mode(&session_id);
        }
        Ok(json!({"closed":closed}))
    }

    fn cancel_prompts_for(&self, session_id: &str) {
        if let Some(tickets) = self
            .prompt_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
        {
            for sender in tickets.values() {
                sender.send_replace(true);
            }
        }
    }

    fn cancel_all_prompts(&self) {
        let tickets = self
            .prompt_tickets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for session_tickets in tickets.values() {
            for sender in session_tickets.values() {
                sender.send_replace(true);
            }
        }
    }
}

enum InputLine {
    Line(String),
    TooLarge,
    ReadError(String),
    Eof,
}

pub(super) fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!(
            "Usage: canopy --acp [--provider <openai|anthropic|gemini>] [--model <model>] [--base-url <url>] [--openai-api-key <key>] [--system <text>] [--max-turns <n>]"
        );
        return Ok(());
    }
    let options = parse_options(args)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("could not start async runtime: {error}"))?;
    runtime.block_on(run_stdio(options))
}

fn parse_options(args: &[String]) -> Result<AcpOptions, String> {
    let mut options = AcpOptions {
        max_turns: 100,
        ..AcpOptions::default()
    };
    let mut index = 0;
    while index < args.len() {
        let option = args[index].as_str();
        match option {
            "--provider" | "--model" | "--base-url" | "--openai-api-key" | "--system"
            | "--max-turns" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or_else(|| format!("{option} requires a value"))?;
                match option {
                    "--provider" => {
                        options.provider =
                            Some(AcpProviderKind::parse(value).ok_or_else(|| {
                                "--provider must be `openai`, `anthropic`, or `gemini`".to_owned()
                            })?);
                    }
                    "--model" => options.model = Some(value.clone()),
                    "--base-url" => options.base_url = Some(value.clone()),
                    "--openai-api-key" => options.api_key = Some(value.clone()),
                    "--system" => options.system = Some(value.clone()),
                    "--max-turns" => {
                        options.max_turns = value
                            .parse::<usize>()
                            .map_err(|_| "--max-turns must be a positive integer".to_owned())?;
                        if options.max_turns == 0 || options.max_turns > 100 {
                            return Err("--max-turns must be between 1 and 100".to_owned());
                        }
                    }
                    _ => unreachable!(),
                }
            }
            other => return Err(format!("unknown ACP option: {other}")),
        }
        index += 1;
    }
    Ok(options)
}

async fn run_stdio(options: AcpOptions) -> Result<(), String> {
    let output = ProtocolOutput::stdout();
    let server = Arc::new(AcpServer::new(options, output));
    let (sender, receiver) = mpsc::channel(MAX_PENDING_ACP_INPUT_LINES);
    thread::Builder::new()
        .name("canopy-acp-stdin".to_owned())
        .spawn(move || {
            let stdin = io::stdin();
            let mut reader = stdin.lock();
            loop {
                match read_bounded_stdio_line(&mut reader, MAX_STDIO_LINE_BYTES) {
                    Ok(InputLine::Eof) => {
                        let _ = sender.blocking_send(InputLine::Eof);
                        break;
                    }
                    Ok(line) => {
                        if sender.blocking_send(line).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.blocking_send(InputLine::ReadError(error.to_string()));
                        break;
                    }
                }
            }
        })
        .map_err(|error| format!("could not start ACP stdin reader: {error}"))?;
    server.serve(receiver).await
}

/// Read one ACP input line without ever buffering more than its configured
/// limit. Overlong lines are drained through their newline so the next frame
/// remains aligned with the JSON-RPC stream.
fn read_bounded_stdio_line<R: io::BufRead>(
    reader: &mut R,
    max_bytes: usize,
) -> io::Result<InputLine> {
    let mut line = Vec::with_capacity(max_bytes.min(8 * 1024));
    let mut too_large = false;
    let mut read_any = false;

    loop {
        let (consume, newline, chunk) = {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if !read_any {
                    return Ok(InputLine::Eof);
                }
                break;
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consume = newline.map_or(available.len(), |index| index + 1);
            let chunk = if too_large || line.len().saturating_add(consume) > max_bytes {
                too_large = true;
                Vec::new()
            } else {
                available[..consume].to_vec()
            };
            (consume, newline.is_some(), chunk)
        };
        reader.consume(consume);
        read_any = true;
        if !too_large {
            line.extend_from_slice(&chunk);
        }
        if newline {
            break;
        }
    }

    if too_large {
        return Ok(InputLine::TooLarge);
    }
    String::from_utf8(line)
        .map(InputLine::Line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn initialize_response() -> Value {
    json!({
        "protocolVersion":PROTOCOL_VERSION,
        "agentInfo":{"name":"canopy-code","title":"Canopy Code","version":env!("CARGO_PKG_VERSION")},
        "authMethods":[],
        "agentCapabilities":{
            "loadSession":true,
            "promptCapabilities":{"image":true,"audio":false,"embeddedContext":true},
            "sessionCapabilities":{"list":{},"resume":{}},
            "mcpCapabilities":{"sse":true,"http":true}
        }
    })
}

fn initial_acp_approval_mode(
    settings: &RuntimeSettings,
    safe_mode: bool,
    bare_mode: bool,
) -> AcpApprovalMode {
    if safe_mode || bare_mode || !settings.workspace_trusted {
        return AcpApprovalMode::Default;
    }
    settings
        .merged_settings
        .pointer("/tools/approvalMode")
        .and_then(Value::as_str)
        .and_then(AcpApprovalMode::parse)
        .unwrap_or(AcpApprovalMode::Auto)
}

fn initial_approval_mode_from_metadata(
    params: &Value,
) -> Result<Option<AcpApprovalMode>, RpcError> {
    let Some(value) = params
        .get("_meta")
        .and_then(|meta| meta.get(INITIAL_APPROVAL_MODE_META_KEY))
    else {
        return Ok(None);
    };
    let mode_value = value
        .as_str()
        .ok_or_else(|| RpcError::invalid_params(format!("Unknown approval mode: {value}")))?;
    AcpApprovalMode::parse(mode_value)
        .map(Some)
        .ok_or_else(|| RpcError::invalid_params(format!("Unknown approval mode: {mode_value}")))
}

fn session_state_response(session_id: &str, model: &str, mode: AcpApprovalMode) -> Value {
    let available_modes = AcpApprovalMode::ALL
        .into_iter()
        .map(|mode| {
            json!({
                "id":mode.as_str(),
                "name":mode.name(),
                "description":mode.description()
            })
        })
        .collect::<Vec<_>>();
    let mode_options = AcpApprovalMode::ALL
        .into_iter()
        .map(|mode| {
            json!({
                "value":mode.as_str(),
                "name":mode.name(),
                "description":mode.description()
            })
        })
        .collect::<Vec<_>>();
    json!({
        "sessionId":session_id,
        "models":{
            "currentModelId":model,
            "availableModels":[{"modelId":model,"name":model}]
        },
        "modes":{
            "currentModeId":mode.as_str(),
            "availableModes":available_modes
        },
        "configOptions":[{
            "id":"mode",
            "name":"Mode",
            "description":"Session permission mode",
            "category":"mode",
            "type":"select",
            "currentValue":mode.as_str(),
            "options":mode_options
        }]
    })
}

fn valid_request_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

fn required_string<'a>(params: &'a Value, key: &str) -> Result<&'a str, RpcError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RpcError::invalid_params(format!("{key} must be a non-empty string")))
}

fn parse_session_mcp_servers(params: &Value, required: bool) -> Result<Vec<Value>, RpcError> {
    match params.get("mcpServers") {
        Some(Value::Array(servers)) => Ok(servers.clone()),
        Some(_) if required => Err(RpcError::invalid_params("mcpServers must be an array")),
        None if required => Err(RpcError::invalid_params("mcpServers must be an array")),
        _ => Ok(Vec::new()),
    }
}

fn session_mcp_server_map(servers: &[Value]) -> Map<String, Value> {
    let mut result = Map::new();
    for server in servers {
        let Some(name) = server.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(object) = server.as_object() else {
            continue;
        };
        let mut config = Map::new();
        if ["command", "args", "env"]
            .iter()
            .all(|field| object.contains_key(*field))
        {
            for field in ["command", "args"] {
                if let Some(value) = object.get(field) {
                    config.insert(field.to_owned(), value.clone());
                }
            }
            config.insert(
                "env".to_owned(),
                Value::Object(acp_name_value_map(object.get("env"))),
            );
        } else if object.get("type").and_then(Value::as_str) == Some("sse") {
            if let Some(url) = object.get("url") {
                config.insert("url".to_owned(), url.clone());
            }
            config.insert(
                "headers".to_owned(),
                Value::Object(acp_name_value_map(object.get("headers"))),
            );
        } else if object.get("type").and_then(Value::as_str) == Some("http") {
            if let Some(url) = object.get("url") {
                config.insert("httpUrl".to_owned(), url.clone());
            }
            config.insert(
                "headers".to_owned(),
                Value::Object(acp_name_value_map(object.get("headers"))),
            );
        } else {
            continue;
        }
        result.insert(name.to_owned(), Value::Object(config));
    }
    result
}

fn acp_name_value_map(entries: Option<&Value>) -> Map<String, Value> {
    let mut result = Map::new();
    let Some(entries) = entries.and_then(Value::as_array) else {
        return result;
    };
    for entry in entries {
        let (Some(name), Some(value)) = (
            entry.get("name").and_then(Value::as_str),
            entry.get("value").and_then(Value::as_str),
        ) else {
            continue;
        };
        result.insert(name.to_owned(), Value::String(value.to_owned()));
    }
    result
}

fn requested_session_id(params: &Value) -> Result<Option<String>, RpcError> {
    let Some(requested) = params
        .get("_meta")
        .and_then(|meta| meta.get(REQUESTED_SESSION_ID_META_KEY))
    else {
        return Ok(None);
    };
    let Some(value) = requested.as_str() else {
        return Err(RpcError::invalid_params(format!(
            "_meta[{REQUESTED_SESSION_ID_META_KEY:?}] must be an RFC UUID v1-v5"
        )));
    };
    if !is_rfc_uuid_v1_to_v5(value) {
        return Err(RpcError::invalid_params(format!(
            "_meta[{REQUESTED_SESSION_ID_META_KEY:?}] must be an RFC UUID v1-v5"
        )));
    }
    Ok(Some(value.to_ascii_lowercase()))
}

fn is_rfc_uuid_v1_to_v5(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 || [8, 13, 18, 23].iter().any(|index| bytes[*index] != b'-') {
        return false;
    }
    if bytes
        .iter()
        .enumerate()
        .any(|(index, byte)| ![8, 13, 18, 23].contains(&index) && !byte.is_ascii_hexdigit())
    {
        return false;
    }
    matches!(bytes[14].to_ascii_lowercase(), b'1'..=b'5')
        && matches!(bytes[19].to_ascii_lowercase(), b'8' | b'9' | b'a' | b'b')
}

fn fresh_session_id() -> String {
    static NEXT_UUID: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let counter = NEXT_UUID.fetch_add(1, Ordering::Relaxed) as u128;
    let mut value = time ^ counter.rotate_left(47);
    value ^= (std::process::id() as u128) << 64;
    let mut bytes = value.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn prompt_text(blocks: &[Value]) -> Result<String, RpcError> {
    let mut text = String::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                let value = block
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::invalid_params("text content block requires text"))?;
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(value);
            }
            Some("resource_link") => {
                let uri = block
                    .get("uri")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::invalid_params("resource_link requires uri"))?;
                let title = block
                    .get("title")
                    .or_else(|| block.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or(uri);
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(&format!("[Resource link: {title}] {uri}"));
                if let Some(description) = block.get("description").and_then(Value::as_str) {
                    text.push('\n');
                    text.push_str(description);
                }
            }
            Some("resource") => {
                let resource = block.get("resource").unwrap_or(block);
                let resource_text =
                    resource
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            RpcError::invalid_params("only text embedded resources are supported")
                        })?;
                if !text.is_empty() {
                    text.push('\n');
                }
                text.push_str(resource_text);
            }
            Some("image") => {
                let valid_mime_type = block
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .is_some_and(|mime_type| mime_type.starts_with("image/"));
                if !valid_mime_type {
                    return Err(RpcError::invalid_params(
                        "image content block requires an image mimeType",
                    ));
                }
                let valid_data = block
                    .get("data")
                    .and_then(Value::as_str)
                    .is_some_and(|data| !data.is_empty());
                if !valid_data {
                    return Err(RpcError::invalid_params(
                        "image content block requires base64 data",
                    ));
                }
            }
            Some("audio") => {
                let valid_mime_type = block
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .is_some_and(|mime_type| mime_type.starts_with("audio/"));
                if !valid_mime_type {
                    return Err(RpcError::invalid_params(
                        "audio content block requires an audio mimeType",
                    ));
                }
                let valid_data = block
                    .get("data")
                    .and_then(Value::as_str)
                    .is_some_and(|data| !data.is_empty());
                if !valid_data {
                    return Err(RpcError::invalid_params(
                        "audio content block requires base64 data",
                    ));
                }
            }
            Some(other) => {
                return Err(RpcError::invalid_params(format!(
                    "unsupported ACP prompt block type: {other}"
                )));
            }
            None => return Err(RpcError::invalid_params("prompt block requires type")),
        }
    }
    Ok(text)
}

fn acp_audio_base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Validate standard or unpadded standard base64 without allocating a second
/// copy of an audio attachment. The input JSON line already owns the payload.
fn validate_acp_audio_base64(
    data: &str,
    cancelled: &watch::Receiver<bool>,
) -> Result<Option<usize>, RpcError> {
    let bytes = data.as_bytes();
    if bytes.is_empty() {
        return Err(RpcError::invalid_params(
            "audio content block requires base64 data",
        ));
    }

    let padding = if bytes.ends_with(b"==") {
        2
    } else if bytes.ends_with(b"=") {
        1
    } else {
        0
    };
    let encoded_len = bytes.len() - padding;
    let remainder = encoded_len % 4;
    if remainder == 1 || (padding == 1 && remainder != 3) || (padding == 2 && remainder != 2) {
        return Err(RpcError::invalid_params(
            "audio content block data is not valid base64",
        ));
    }

    let decoded_bytes = (encoded_len / 4) * 3
        + match remainder {
            2 => 1,
            3 => 2,
            _ => 0,
        };
    if decoded_bytes > MAX_ACP_AUDIO_BYTES {
        return Err(RpcError::invalid_params(format!(
            "audio content block exceeds the {} MiB limit",
            MAX_ACP_AUDIO_BYTES / (1024 * 1024)
        )));
    }

    for (index, byte) in bytes.iter().copied().enumerate() {
        if index % (64 * 1024) == 0 && *cancelled.borrow() {
            return Ok(None);
        }
        if index < encoded_len {
            if acp_audio_base64_value(byte).is_none() {
                return Err(RpcError::invalid_params(
                    "audio content block data is not valid base64",
                ));
            }
        } else if byte != b'=' {
            return Err(RpcError::invalid_params(
                "audio content block data is not valid base64",
            ));
        }
    }

    if remainder == 3
        && bytes
            .get(encoded_len.saturating_sub(1))
            .and_then(|byte| acp_audio_base64_value(*byte))
            .is_some_and(|value| value & 0b11 != 0)
    {
        return Err(RpcError::invalid_params(
            "audio content block data is not valid base64",
        ));
    }
    if remainder == 2
        && bytes
            .get(encoded_len.saturating_sub(1))
            .and_then(|byte| acp_audio_base64_value(*byte))
            .is_some_and(|value| value & 0b1111 != 0)
    {
        return Err(RpcError::invalid_params(
            "audio content block data is not valid base64",
        ));
    }

    Ok(Some(decoded_bytes))
}

fn acp_audio_format_supported(provider: AcpProviderKind, mime_type: &str) -> bool {
    let mime_type = mime_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match provider {
        AcpProviderKind::Gemini => matches!(
            mime_type.as_str(),
            "audio/wav"
                | "audio/mp3"
                | "audio/aiff"
                | "audio/aac"
                | "audio/ogg"
                | "audio/flac"
                | "audio/mpeg"
                | "audio/m4a"
                | "audio/l16"
                | "audio/opus"
                | "audio/alaw"
                | "audio/mulaw"
                | "audio/webm"
        ),
        AcpProviderKind::OpenAiCompatible => matches!(
            mime_type.as_str(),
            "audio/wav" | "audio/x-wav" | "audio/mp3" | "audio/mpeg"
        ),
        AcpProviderKind::Anthropic => false,
    }
}

fn validate_acp_audio_blocks(
    blocks: &[Value],
    provider: AcpProviderKind,
    modalities: canopy_core::providers::openai_request::InputModalities,
    cancelled: &watch::Receiver<bool>,
) -> Result<bool, RpcError> {
    for block in blocks {
        if *cancelled.borrow() {
            return Ok(false);
        }
        if block.get("type").and_then(Value::as_str) != Some("audio") {
            continue;
        }
        let mime_type = block
            .get("mimeType")
            .and_then(Value::as_str)
            .filter(|mime_type| mime_type.starts_with("audio/"))
            .ok_or_else(|| {
                RpcError::invalid_params("audio content block requires an audio mimeType")
            })?;
        let data = block
            .get("data")
            .and_then(Value::as_str)
            .filter(|data| !data.is_empty())
            .ok_or_else(|| RpcError::invalid_params("audio content block requires base64 data"))?;

        if !modalities.audio {
            return Err(RpcError::invalid_params(format!(
                "the active {} provider/model does not support audio input",
                provider.display_name()
            )));
        }
        if !acp_audio_format_supported(provider, mime_type) {
            let formats = match provider {
                AcpProviderKind::Gemini => {
                    "WAV, MP3, AIFF, AAC, OGG, FLAC, MPEG, M4A, L16, Opus, ALAW, MULAW, and WebM"
                }
                AcpProviderKind::OpenAiCompatible => "WAV and MP3",
                AcpProviderKind::Anthropic => "none",
            };
            return Err(RpcError::invalid_params(format!(
                "audio MIME type `{mime_type}` is not supported by the active {} provider (supported formats: {formats})",
                provider.display_name()
            )));
        }
        if validate_acp_audio_base64(data, cancelled)?.is_none() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn acp_stats_slash_command<'a>(blocks: &[Value], text: &'a str) -> Option<&'a str> {
    if blocks
        .iter()
        .any(|block| block.get("type").and_then(Value::as_str) != Some("text"))
    {
        return None;
    }
    let value = text.trim().strip_prefix('/')?;
    let mut parts = value.splitn(2, char::is_whitespace);
    let command = parts.next()?;
    if !matches!(command, "stats" | "usage") {
        return None;
    }
    Some(parts.next().unwrap_or_default().trim())
}

fn acp_doctor_memory_slash_command<'a>(blocks: &[Value], text: &'a str) -> Option<&'a str> {
    if blocks
        .iter()
        .any(|block| block.get("type").and_then(Value::as_str) != Some("text"))
    {
        return None;
    }
    let value = text.trim().strip_prefix('/')?;
    let (command, arguments) = value.split_once(char::is_whitespace).unwrap_or((value, ""));
    if !command.eq_ignore_ascii_case("doctor") {
        return None;
    }
    let arguments = arguments.trim_start();
    let subcommand_end = arguments
        .find(char::is_whitespace)
        .unwrap_or(arguments.len());
    arguments[..subcommand_end]
        .eq_ignore_ascii_case("memory")
        .then(|| arguments[subcommand_end..].trim())
}

fn acp_doctor_rollback_slash_command(blocks: &[Value], text: &str) -> bool {
    if blocks
        .iter()
        .any(|block| block.get("type").and_then(Value::as_str) != Some("text"))
    {
        return false;
    }
    let Some(value) = text.trim().strip_prefix('/') else {
        return false;
    };
    let (command, arguments) = value.split_once(char::is_whitespace).unwrap_or((value, ""));
    if !command.eq_ignore_ascii_case("doctor") {
        return false;
    }
    arguments
        .split_whitespace()
        .next()
        .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case("rollback"))
}

fn acp_session_stats_summary(metrics: Option<&SessionMetrics>) -> String {
    let Some(metrics) = metrics else {
        return "Session metrics are not available for this session.".to_owned();
    };
    let (requests, prompt_tokens, output_tokens) = metrics.models.values().fold(
        (0.0, 0.0, 0.0),
        |(requests, prompt_tokens, output_tokens), model| {
            (
                requests + model.api.total_requests,
                prompt_tokens + model.tokens.prompt_tokens,
                output_tokens + model.tokens.candidates,
            )
        },
    );
    format!(
        "Session stats\nAPI requests: {}\nTokens — prompt: {}, output: {}\nTool calls: {} ({} ok, {} fail)",
        format_acp_stats_metric(requests),
        format_acp_stats_metric(prompt_tokens),
        format_acp_stats_metric(output_tokens),
        format_acp_stats_metric(metrics.tools.total_calls),
        format_acp_stats_metric(metrics.tools.total_success),
        format_acp_stats_metric(metrics.tools.total_fail),
    )
}

fn format_acp_stats_metric(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "0".to_owned();
    }
    let rounded = value.round();
    if (value - rounded).abs() < 0.000_001 {
        return format!("{rounded:.0}");
    }
    format!("{value:.1}")
}

fn bound_acp_stats_output(value: &str) -> String {
    bound_acp_output(
        value,
        MAX_ACP_STATS_OUTPUT_BYTES,
        "\n[Stats output truncated.]",
    )
}

fn bound_acp_output(value: &str, max_bytes: usize, suffix: &str) -> String {
    let mut output = String::with_capacity(value.len().min(max_bytes));
    let mut truncated = false;
    for character in value.chars() {
        let character = if character.is_control() && !matches!(character, '\n' | '\t') {
            '\u{fffd}'
        } else {
            character
        };
        if output.len().saturating_add(character.len_utf8()) > max_bytes {
            truncated = true;
            break;
        }
        output.push(character);
    }
    if truncated {
        let target_len = max_bytes.saturating_sub(suffix.len());
        while output.len() > target_len {
            output.pop();
        }
        output.push_str(suffix);
    }
    output
}

fn acp_prompt_reference_tokens(blocks: &[Value], server_names: &[String]) -> (Vec<String>, usize) {
    let mut references = Vec::new();
    let mut seen = HashSet::new();
    let mut omitted_count = 0;
    for block in blocks {
        let Some(text) = block
            .get("type")
            .filter(|kind| kind.as_str() == Some("text"))
            .and_then(|_| block.get("text"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let mut current_index = 0;
        while current_index < text.len() {
            let at_index = (current_index..text.len()).find(|index| {
                text.as_bytes()[*index] == b'@'
                    && (*index == 0 || text.as_bytes()[index - 1] != b'\\')
            });
            let Some(at_index) = at_index else {
                break;
            };
            let path_start = at_index + 1;
            let mut path_end = path_start;
            let mut in_escape = false;
            for (offset, character) in text[path_start..].char_indices() {
                let index = path_start + offset;
                let next_index = index + character.len_utf8();
                if in_escape {
                    in_escape = false;
                    path_end = next_index;
                    continue;
                }
                if character == '\\' {
                    in_escape = true;
                    path_end = next_index;
                    continue;
                }
                if matches!(
                    character,
                    ',' | ';' | '!' | '?' | '(' | ')' | '[' | ']' | '{' | '}'
                ) || is_acp_reference_whitespace(character)
                {
                    break;
                }
                if character == '.' {
                    let next_character = text[next_index..].chars().next();
                    if next_character.is_none_or(is_acp_reference_whitespace) {
                        break;
                    }
                }
                path_end = next_index;
            }

            let raw_path = &text[path_start..path_end];
            let path = if cfg!(windows) {
                raw_path.to_owned()
            } else {
                unescape_acp_reference_path(raw_path)
            };
            if is_acp_prompt_reference(&path, server_names) && seen.insert(path.clone()) {
                if references.len() < MAX_ACP_PROMPT_REFERENCES {
                    references.push(path);
                } else {
                    omitted_count += 1;
                }
            }
            current_index = path_end;
        }
    }
    (references, omitted_count)
}

fn is_acp_prompt_reference(path: &str, server_names: &[String]) -> bool {
    if path
        .strip_prefix("ext:")
        .is_some_and(|name| !name.is_empty())
    {
        return true;
    }
    if path
        .strip_prefix("mcp:")
        .is_some_and(|name| !name.is_empty())
    {
        return true;
    }
    server_names.iter().any(|server_name| {
        path.strip_prefix(&format!("{server_name}:"))
            .is_some_and(|uri| !uri.is_empty())
    })
}

fn is_acp_reference_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

fn unescape_acp_reference_path(value: &str) -> String {
    let mut chars = value.chars().peekable();
    let mut output = String::with_capacity(value.len());
    while let Some(character) = chars.next() {
        if character == '\\'
            && chars.peek().is_some_and(|next| {
                matches!(
                    next,
                    ' ' | '\t'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | ';'
                        | '|'
                        | '*'
                        | '?'
                        | '$'
                        | '`'
                        | '\''
                        | '"'
                        | '#'
                        | '&'
                        | '<'
                        | '>'
                        | '!'
                        | '~'
                        | ','
                )
            })
        {
            output.push(chars.next().expect("peeked shell-special character"));
        } else {
            output.push(character);
        }
    }
    output
}

async fn wait_for_acp_prompt_cancellation(cancelled: &mut watch::Receiver<bool>) {
    loop {
        if *cancelled.borrow() || cancelled.changed().await.is_err() {
            break;
        }
    }
}

fn bound_acp_reference_diagnostic(value: &str) -> String {
    let stripped = canopy_core::utils::terminal_safe::strip_terminal_control_sequences(value);
    let safe = stripped
        .chars()
        .filter(|character| {
            !matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
            )
        })
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .take(MAX_ACP_REFERENCE_DIAGNOSTIC_CHARS)
        .collect::<String>();
    safe.trim().to_owned()
}

fn runtime_error(error: BridgeSessionRuntimeError) -> RpcError {
    match error {
        BridgeSessionRuntimeError::InvalidScope(_)
        | BridgeSessionRuntimeError::InvalidMetadata(_)
        | BridgeSessionRuntimeError::InvalidApprovalMode(_)
        | BridgeSessionRuntimeError::InvalidHistoryReplay(_)
        | BridgeSessionRuntimeError::InvalidLiveReplayMode(_)
        | BridgeSessionRuntimeError::InvalidHistoryPageSize
        | BridgeSessionRuntimeError::WorkspaceMismatch { .. }
        | BridgeSessionRuntimeError::MissingWorkspace => {
            RpcError::invalid_params(error.to_string())
        }
        BridgeSessionRuntimeError::SessionNotFound(session_id) => {
            RpcError::new(-32002, format!("Session not found: {session_id}"))
        }
        BridgeSessionRuntimeError::PromptAborted => RpcError::new(-32800, "Request cancelled"),
        BridgeSessionRuntimeError::PromptQueueFull { .. } => {
            RpcError::new(-32000, error.to_string())
        }
        _ => RpcError::internal(error.to_string()),
    }
}

struct CliAcpSessionFactory {
    options: AcpOptions,
    output: ProtocolOutput,
    workspace: PathBuf,
    mcp_workspace: Arc<McpCliWorkspace>,
    memory_pressure: Arc<std::sync::atomic::AtomicBool>,
    approval_modes: Arc<Mutex<HashMap<String, Arc<AcpApprovalModeState>>>>,
    active_agents: Mutex<HashMap<String, Weak<CliAcpAgent>>>,
    workspace_skill_snapshot: Mutex<Option<Arc<RwLock<AcpSkillSnapshot>>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcpApprovalMode {
    Plan,
    Default,
    AutoEdit,
    Auto,
    Yolo,
}

impl AcpApprovalMode {
    const ALL: [Self; 5] = [
        Self::Plan,
        Self::Default,
        Self::AutoEdit,
        Self::Auto,
        Self::Yolo,
    ];

    fn parse(value: &str) -> Option<Self> {
        match value {
            "plan" => Some(Self::Plan),
            "default" => Some(Self::Default),
            "auto-edit" => Some(Self::AutoEdit),
            "auto" => Some(Self::Auto),
            "yolo" => Some(Self::Yolo),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Default => "default",
            Self::AutoEdit => "auto-edit",
            Self::Auto => "auto",
            Self::Yolo => "yolo",
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Plan => 0,
            Self::Default => 1,
            Self::AutoEdit => 2,
            Self::Auto => 3,
            Self::Yolo => 4,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Plan,
            1 => Self::Default,
            2 => Self::AutoEdit,
            3 => Self::Auto,
            4 => Self::Yolo,
            _ => Self::Default,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Plan => "Plan",
            Self::Default => "Default",
            Self::AutoEdit => "Auto Edit",
            Self::Auto => "Auto",
            Self::Yolo => "YOLO",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::Plan => "Analyze only, do not modify files or execute commands",
            Self::Default => "Require approval for file edits or shell commands",
            Self::AutoEdit => "Automatically approve file edits",
            Self::Auto => "LLM classifier auto-approves safe actions, blocks risky ones",
            Self::Yolo => "Automatically approve all tools",
        }
    }

    fn is_privileged(self) -> bool {
        matches!(self, Self::AutoEdit | Self::Auto | Self::Yolo)
    }
}

struct AcpApprovalModeState {
    current: AtomicU8,
    workspace_trusted: bool,
}

impl AcpApprovalModeState {
    fn new(current: AcpApprovalMode, workspace_trusted: bool) -> Self {
        Self {
            current: AtomicU8::new(current.as_u8()),
            workspace_trusted,
        }
    }

    fn current(&self) -> AcpApprovalMode {
        AcpApprovalMode::from_u8(self.current.load(Ordering::Acquire))
    }

    fn set(&self, mode: AcpApprovalMode) -> Result<AcpApprovalMode, RpcError> {
        if mode.is_privileged() && !self.workspace_trusted {
            return Err(RpcError::new(
                -32003,
                "Cannot enable privileged approval modes in an untrusted folder.",
            )
            .with_data(json!({"errorKind":"trust_gate"})));
        }
        let previous = AcpApprovalMode::from_u8(self.current.swap(mode.as_u8(), Ordering::AcqRel));
        Ok(previous)
    }
}

struct AcpModeAwareMcpApprovalPrompt {
    approval_mode: Arc<AcpApprovalModeState>,
}

impl McpCliApprovalPrompt for AcpModeAwareMcpApprovalPrompt {
    fn is_available(&self) -> bool {
        matches!(
            self.approval_mode.current(),
            AcpApprovalMode::Plan | AcpApprovalMode::Yolo
        ) || TerminalMcpApprovalPrompt.is_available()
    }

    fn confirm(&self, prompt: &str) -> Result<bool, String> {
        match self.approval_mode.current() {
            AcpApprovalMode::Plan => Ok(false),
            AcpApprovalMode::Yolo => Ok(true),
            AcpApprovalMode::Default | AcpApprovalMode::AutoEdit | AcpApprovalMode::Auto => {
                TerminalMcpApprovalPrompt.confirm(prompt)
            }
        }
    }
}

impl CliAcpSessionFactory {
    fn approval_mode_state(&self, session_id: &str) -> Option<Arc<AcpApprovalModeState>> {
        self.approval_modes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    fn register_approval_mode(&self, session_id: &str, state: Arc<AcpApprovalModeState>) {
        self.approval_modes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session_id.to_owned(), state);
    }

    fn forget_approval_mode(&self, session_id: &str) {
        self.approval_modes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(session_id);
    }
}

fn workspace_memory_dream_failure(detail: impl ToString) -> RpcError {
    workspace_memory_dream_error(
        "dream_failed",
        "Workspace memory dream failed",
        Some(detail.to_string()),
    )
}

fn workspace_memory_dream_error(
    error_kind: &str,
    message: &str,
    details: Option<String>,
) -> RpcError {
    let mut data = Map::new();
    data.insert("errorKind".to_owned(), Value::String(error_kind.to_owned()));
    if error_kind != "managed_memory_unavailable"
        && let Some(details) = details.and_then(sanitize_workspace_memory_error_details)
    {
        data.insert("details".to_owned(), Value::String(details));
    }
    RpcError::new(-32099, message).with_data(Value::Object(data))
}

fn sanitize_workspace_memory_error_details(details: String) -> Option<String> {
    let normalized = details
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    let redacted = canopy_core::acp_bridge::redact_log_credentials(&normalized);
    let details = canopy_core::utils::terminal_safe::strip_display_control_chars(&redacted);
    let details = details.trim();
    if details.is_empty() {
        return None;
    }
    let details = if details.chars().count() <= 1_000 {
        details.to_owned()
    } else {
        let suffix = "... [truncated]";
        format!(
            "{}{}",
            details
                .chars()
                .take(1_000 - suffix.len())
                .collect::<String>(),
            suffix
        )
    };
    Some(details)
}

impl CliAcpSessionFactory {
    fn register_active_agent(&self, agent: &Arc<CliAcpAgent>) {
        self.active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(agent.session_id.clone(), Arc::downgrade(agent));
    }

    fn active_agents(&self) -> Vec<Arc<CliAcpAgent>> {
        let mut registry = self
            .active_agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut active = Vec::new();
        registry.retain(|_, weak| {
            let Some(agent) = weak.upgrade() else {
                return false;
            };
            if agent.closed.load(Ordering::Acquire) {
                return false;
            }
            active.push(agent);
            true
        });
        active.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        active
    }

    fn register_workspace_skill_snapshot(&self, session_snapshot: &RwLock<AcpSkillSnapshot>) {
        let mut workspace_snapshot = self
            .workspace_skill_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if workspace_snapshot.is_some() {
            return;
        }
        let mut snapshot = clone_acp_skill_snapshot(session_snapshot);
        snapshot.manager = Arc::new(SkillManager::new(snapshot.manager_config.clone()));
        *workspace_snapshot = Some(Arc::new(RwLock::new(snapshot)));
    }

    async fn refresh_workspace_skills(&self, reason: &str) -> Result<Value, String> {
        let refresh_settings = reason != "content";
        let refresh_content = reason != "settings";
        let agents = self.active_agents();
        let workspace_snapshot = self
            .workspace_skill_snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        let mut failed_settings_sessions = HashSet::new();
        if refresh_settings {
            if let Some(snapshot) = &workspace_snapshot {
                snapshot
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .reload_workspace_settings()?;
            } else {
                // The TypeScript ACP agent owns a workspace-level Settings
                // instance even before sessions are active. Reload the scope
                // here when this factory has not created its base skill state.
                let _ = load_acp_workspace_settings_scope(&self.workspace)?;
            }
            for agent in &agents {
                if let Err(error) = agent.reload_skill_settings() {
                    eprintln!(
                        "canopy --acp: session {} skill settings reload failed: {error}",
                        agent.session_id
                    );
                    failed_settings_sessions.insert(agent.session_id.clone());
                }
            }
        }

        let mut configs_refreshed = 0usize;
        let configs_failed = 0usize;
        if refresh_content {
            let mut managers = HashMap::<usize, Arc<SkillManager>>::new();
            if let Some(snapshot) = workspace_snapshot {
                let snapshot = clone_acp_skill_snapshot(&snapshot);
                managers.insert(Arc::as_ptr(&snapshot.manager) as usize, snapshot.manager);
            }
            for agent in &agents {
                let snapshot = clone_acp_skill_snapshot(&agent.skill_snapshot);
                managers.insert(Arc::as_ptr(&snapshot.manager) as usize, snapshot.manager);
            }
            let managers = managers.into_values().collect::<Vec<_>>();
            let results = join_all(managers.iter().map(|manager| manager.refresh_cache())).await;
            configs_refreshed = results.len();
        }

        let mut sessions_refreshed = 0usize;
        let mut sessions_failed = 0usize;
        for agent in agents {
            if failed_settings_sessions.contains(&agent.session_id) {
                sessions_failed += 1;
                continue;
            }
            match agent.publish_skill_refresh(!refresh_content).await {
                Ok(()) => sessions_refreshed += 1,
                Err(error) => {
                    eprintln!(
                        "canopy --acp: session {} skill refresh failed: {error}",
                        agent.session_id
                    );
                    sessions_failed += 1;
                }
            }
        }

        Ok(json!({
            "sessionsRefreshed":sessions_refreshed,
            "sessionsFailed":sessions_failed,
            "configsRefreshed":configs_refreshed,
            "configsFailed":configs_failed,
            "reason":reason,
        }))
    }

    fn resolve_model(&self, workspace: &Path) -> Result<String, String> {
        if let Some(model) = self
            .options
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
        {
            return Ok(model.to_owned());
        }
        let provider = self.options.provider();
        load_runtime_settings(workspace)?
            .effective_env
            .get(provider.model_env())
            .filter(|model| !model.trim().is_empty())
            .cloned()
            .ok_or_else(|| provider.model_requirement().to_owned())
    }

    async fn run_workspace_memory_dream(
        &self,
        request_cancellation: RequestCancellationTicket,
    ) -> Result<Value, RpcError> {
        let runtime_settings = load_runtime_settings(&self.workspace)
            .map_err(|error| workspace_memory_dream_failure(error))?;
        let managed_memory_enabled = runtime_settings
            .merged_settings
            .pointer("/memory/enableManagedAutoMemory")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if !managed_memory_enabled || canopy_core::utils::bare_mode::is_bare_mode(None) {
            return Err(RpcError::new(
                -32009,
                "Managed memory is unavailable for this daemon workspace",
            )
            .with_data(json!({"errorKind":"managed_memory_unavailable"})));
        }
        let runtime_output_dir = runtime_settings.runtime_output_dir.clone();
        let workspace = self.workspace.clone();
        Storage::run_with_runtime_base_dir(
            runtime_output_dir.as_deref(),
            Some(&workspace),
            || async move {
                self.run_workspace_memory_dream_with_settings(
                    runtime_settings,
                    request_cancellation,
                )
                .await
            },
        )
        .await
    }

    async fn run_workspace_memory_dream_with_settings(
        &self,
        runtime_settings: super::RuntimeSettings,
        mut request_cancellation: RequestCancellationTicket,
    ) -> Result<Value, RpcError> {
        let settings_model =
            nonempty_settings_string(runtime_settings.merged_settings.pointer("/model/name"));
        let settings_model_base_url =
            nonempty_settings_string(runtime_settings.merged_settings.pointer("/model/baseUrl"));
        let settings_provider_model = settings_model.and_then(|model_id| {
            find_configured_model_provider(
                &runtime_settings.merged_settings,
                model_id,
                None,
                settings_model_base_url,
            )
        });
        let provider_kind = self
            .options
            .provider
            .or_else(|| settings_provider_model.map(|(protocol, _)| protocol))
            .unwrap_or(AcpProviderKind::OpenAiCompatible);
        let cli_model = self
            .options
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty());
        let env_model = runtime_settings
            .effective_env
            .get(provider_kind.model_env())
            .map(String::as_str)
            .filter(|model| !model.trim().is_empty());
        let selected_model = cli_model.or(settings_model).or(env_model);
        let configured_model = selected_model.and_then(|model_id| {
            find_configured_model_provider(
                &runtime_settings.merged_settings,
                model_id,
                Some(provider_kind),
                self.options.base_url.as_deref().or_else(|| {
                    if cli_model.is_none() {
                        settings_model_base_url
                    } else {
                        None
                    }
                }),
            )
        });
        let model = self
            .options
            .model
            .clone()
            .filter(|model| !model.trim().is_empty())
            .or_else(|| settings_model.map(str::to_owned))
            .or_else(|| {
                runtime_settings
                    .effective_env
                    .get(provider_kind.model_env())
                    .cloned()
            })
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| workspace_memory_dream_failure(provider_kind.model_requirement()))?;
        let model_generation_config =
            super::model_generation_config::NativeModelGenerationConfig::resolve(
                &runtime_settings.merged_settings,
                configured_model.map(|(_, model)| model),
                &model,
                &runtime_settings.effective_env,
            );
        let base_url = self
            .options
            .base_url
            .clone()
            .or_else(|| {
                configured_model
                    .and_then(|(_, model)| nonempty_settings_string(model.get("baseUrl")))
                    .map(str::to_owned)
            })
            .or_else(|| {
                runtime_settings
                    .effective_env
                    .get(provider_kind.base_url_env())
                    .cloned()
            })
            .or_else(|| {
                nonempty_settings_string(
                    runtime_settings
                        .merged_settings
                        .pointer("/security/auth/baseUrl"),
                )
                .map(str::to_owned)
            })
            .unwrap_or_else(|| provider_kind.default_base_url().to_owned());
        let configured_api_key = configured_model
            .and_then(|(_, model)| nonempty_settings_string(model.get("envKey")))
            .and_then(|env_key| runtime_settings.effective_env.get(env_key))
            .filter(|api_key| !api_key.trim().is_empty())
            .cloned();
        let fallback_api_key = match provider_kind {
            AcpProviderKind::OpenAiCompatible => runtime_settings
                .effective_env
                .get("OPENAI_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty())
                .or_else(|| {
                    runtime_settings
                        .effective_env
                        .get("CANOPY_API_KEY")
                        .filter(|api_key| !api_key.trim().is_empty())
                }),
            AcpProviderKind::Anthropic => runtime_settings
                .effective_env
                .get("ANTHROPIC_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty()),
            AcpProviderKind::Gemini => runtime_settings
                .effective_env
                .get("GEMINI_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty()),
        }
        .cloned()
        .or_else(|| {
            nonempty_settings_string(
                runtime_settings
                    .merged_settings
                    .pointer("/security/auth/apiKey"),
            )
            .map(str::to_owned)
        });
        let api_key = configured_api_key
            .or_else(|| {
                self.options
                    .api_key
                    .as_deref()
                    .filter(|key| !key.trim().is_empty())
                    .map(str::to_owned)
            })
            .or(fallback_api_key);
        let provider = match provider_kind {
            AcpProviderKind::OpenAiCompatible => {
                let mut config = OpenAiCompatibleConfig {
                    base_url,
                    api_key,
                    proxy: runtime_settings.proxy_url.clone(),
                    ..OpenAiCompatibleConfig::default()
                };
                model_generation_config.apply_to_openai_compatible(&mut config);
                super::RunRuntimeProvider::OpenAiCompatible(config)
            }
            AcpProviderKind::Anthropic => {
                let mut config = AnthropicProviderConfig {
                    model: model.clone(),
                    base_url,
                    api_key,
                    proxy: runtime_settings.proxy_url.clone(),
                    cli_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                    ..AnthropicProviderConfig::default()
                };
                model_generation_config.apply_to_anthropic(&mut config);
                super::RunRuntimeProvider::Anthropic(config)
            }
            AcpProviderKind::Gemini => {
                let mut config = GeminiProviderConfig {
                    model: model.clone(),
                    base_url,
                    api_key,
                    proxy: runtime_settings.proxy_url.clone(),
                    user_agent: Some(format!(
                        "CanopyCode/{} ({}; {})",
                        env!("CARGO_PKG_VERSION"),
                        std::env::consts::OS,
                        std::env::consts::ARCH
                    )),
                    ..GeminiProviderConfig::default()
                };
                model_generation_config.apply_to_gemini(&mut config);
                super::RunRuntimeProvider::Gemini(config)
            }
        };
        let runtime_base_dir = Storage::new(&self.workspace)
            .runtime_base_dir()
            .to_path_buf();
        let mut runtime_config =
            AgentRuntimeConfig::new(model.clone(), runtime_base_dir.join("tool-results"));
        runtime_config.usage_auth_type = provider_kind.auth_type().as_str().to_owned();
        runtime_config.usage_source = "acp".to_owned();
        model_generation_config.apply_to_runtime(&mut runtime_config);
        runtime_config.max_model_turns = self.options.max_turns();
        let paths = super::native_auto_memory_paths(
            &self.workspace,
            &runtime_base_dir,
            &runtime_settings.effective_env,
        );
        let memory_runtime = super::NativeAutoMemoryRuntime {
            provider,
            runtime_config,
            runtime_base_dir,
            memory_paths: paths.clone(),
            effective_env: runtime_settings.effective_env.clone(),
            permissions: runtime_settings.permissions.clone(),
            core_tools: runtime_settings.core_tools.clone(),
            excluded_tools: runtime_settings.excluded_tools.clone(),
            managed_auto_memory_enabled: true,
            managed_auto_dream_enabled: true,
            auto_skill_enabled: false,
            max_turns: runtime_settings
                .merged_settings
                .pointer("/memory/agentMaxTurns")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            timeout_minutes: runtime_settings
                .merged_settings
                .pointer("/memory/agentTimeoutMinutes")
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok()),
            memory_pressure: Arc::clone(&self.memory_pressure),
        };

        let (abort_sender, abort_receiver) = watch::channel(false);
        let dream = canopy_core::memory::run_managed_auto_memory_dream(
            &paths,
            chrono::Utc::now(),
            Some(&memory_runtime),
            Some(abort_receiver),
            canopy_core::memory::RunDreamOptions {
                trigger: canopy_core::memory::MemoryDreamTrigger::Manual,
                record_metadata: true,
                suppress_chat_recording: true,
            },
            canopy_core::memory::DreamPlannerOptions {
                suppress_chat_recording: true,
                ..canopy_core::memory::DreamPlannerOptions::default()
            },
        );
        tokio::pin!(dream);
        let result = tokio::select! {
            biased;
            _ = wait_for_request_cancellation(&mut request_cancellation.receiver) => {
                let _ = abort_sender.send(true);
                return Err(RpcError::new(-32800, "Request cancelled"));
            }
            _ = tokio::time::sleep(ACP_WORKSPACE_MEMORY_DREAM_TIMEOUT) => {
                let _ = abort_sender.send(true);
                return Err(workspace_memory_dream_error(
                    "dream_timeout",
                    "Workspace memory dream timed out",
                    None,
                ));
            }
            result = &mut dream => result,
        };
        match result {
            Ok(result) => {
                let mut response = json!({
                    "touchedTopics":result.touched_topics.into_iter().map(|topic| topic.as_str()).collect::<Vec<_>>(),
                    "dedupedEntries":result.deduped_entries,
                });
                if let Some(summary) = result.system_message {
                    response["summary"] = Value::String(summary);
                }
                Ok(response)
            }
            Err(error) => Err(workspace_memory_dream_error(
                "dream_failed",
                "Workspace memory dream failed",
                Some(error.to_string()),
            )),
        }
    }

    async fn build_agent(
        &self,
        workspace: &Path,
        session_id: String,
        store: SessionStore,
        recorder: SessionRecorder,
        history: Vec<Value>,
        restored_snapshots: Vec<
            canopy_core::services::session_file_history_state::FileHistorySnapshot,
        >,
        restored_attribution: Option<Value>,
        session_mcp_servers: Vec<Value>,
    ) -> Result<Arc<CliAcpAgent>, String> {
        let runtime_settings = load_runtime_settings(workspace)?;
        Storage::set_runtime_base_dir(
            runtime_settings.runtime_output_dir.as_deref(),
            Some(workspace),
        );
        let safe_mode = canopy_core::utils::safe_mode::is_safe_mode_env();
        let bare_mode = canopy_core::utils::bare_mode::is_bare_mode(None);
        let approval_mode = initial_acp_approval_mode(&runtime_settings, safe_mode, bare_mode);
        let approval_mode_state = Arc::new(AcpApprovalModeState::new(
            approval_mode,
            runtime_settings.workspace_trusted,
        ));
        let user_extensions_dir = Storage::get_user_extensions_dir();
        let extension_store_dir = Storage::get_global_canopy_dir().join("extension-store");
        let extension_inventory =
            load_active_local_extension_references(ExtensionInventoryOptions {
                workspace_root: workspace,
                user_extensions_dir: &user_extensions_dir,
                extension_store_dir: &extension_store_dir,
                enabled_extension_overrides: &[],
                workspace_trusted: runtime_settings.workspace_trusted,
                safe_mode,
                bare_mode,
            });
        for diagnostic in extension_inventory
            .diagnostics
            .iter()
            .take(MAX_ACP_REFERENCE_DIAGNOSTICS)
        {
            let diagnostic = bound_acp_reference_diagnostic(diagnostic);
            if !diagnostic.is_empty() {
                eprintln!("[CANOPY] {diagnostic}");
            }
        }
        if extension_inventory.diagnostics.len() > MAX_ACP_REFERENCE_DIAGNOSTICS {
            eprintln!(
                "[CANOPY] Skipped {} additional extension inventory diagnostic(s).",
                extension_inventory.diagnostics.len() - MAX_ACP_REFERENCE_DIAGNOSTICS
            );
        }
        let active_skill_extensions = extension_inventory.active_skill_extensions;
        let active_extensions = extension_inventory.active_extensions;
        let extension_mcp_sources = extension_inventory
            .active_mcp_servers
            .into_iter()
            .map(ExtensionMcpSource::from)
            .collect::<Vec<_>>();
        let user_skill_dirs = Storage::get_user_skills_dirs();
        let home_dir = user_skill_dirs[1]
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| workspace.to_path_buf());
        let global_canopy_dir = Storage::get_global_canopy_dir();
        let mut skill_manager_config = SkillManagerConfig::new(
            workspace,
            home_dir,
            &global_canopy_dir,
            workspace,
            super::resolve_bundled_skills_dir(),
        );
        skill_manager_config.safe_mode = safe_mode;
        skill_manager_config.bare_mode = bare_mode;
        skill_manager_config.active_extensions = active_skill_extensions;
        let skill_settings =
            AcpSkillSettingsLayers::load(workspace, runtime_settings.workspace_trusted)?;
        let skill_snapshot = Arc::new(RwLock::new(AcpSkillSnapshot::new(
            skill_manager_config,
            skill_settings,
        )));
        self.register_workspace_skill_snapshot(&skill_snapshot);
        let initial_skill_snapshot = clone_acp_skill_snapshot(&skill_snapshot);
        let all_skills = initial_skill_snapshot
            .manager
            .list_skills(ListSkillsOptions::default())
            .await;
        let has_model_invocable_skills = all_skills.iter().any(|skill| {
            skill.disable_model_invocation != Some(true)
                && !initial_skill_snapshot
                    .disabled_skill_names
                    .contains(&skill.name.to_lowercase())
        });
        let mut memory_request =
            canopy_core::memory::discovery::MemoryDiscoveryRequest::from_process(
                workspace.to_path_buf(),
            );
        memory_request.folder_trust = runtime_settings.mcp_settings.trusted_workspace;
        let startup_memory = canopy_core::memory::discovery::load_server_hierarchical_memory(
            &memory_request,
            &canopy_core::memory::discovery::MemoryDiscoveryCallbacks::default(),
        )
        .await
        .map_err(|error| format!("could not load hierarchical instructions: {error}"))?;
        let conditional_rules = if startup_memory.conditional_rules.is_empty() {
            None
        } else {
            Some(Arc::new(
                canopy_core::memory::rules_discovery::ConditionalRulesRegistry::new(
                    &startup_memory.conditional_rules,
                    startup_memory.project_root.clone(),
                )
                .map_err(|error| format!("could not initialize conditional rules: {error}"))?,
            ))
        };
        let base_system_instruction = super::combine_user_and_hierarchical_instructions(
            self.options.system.as_deref(),
            &startup_memory.memory_content,
        );
        let settings_model =
            nonempty_settings_string(runtime_settings.merged_settings.pointer("/model/name"));
        let settings_model_base_url =
            nonempty_settings_string(runtime_settings.merged_settings.pointer("/model/baseUrl"));
        let settings_provider_model = settings_model.and_then(|model_id| {
            find_configured_model_provider(
                &runtime_settings.merged_settings,
                model_id,
                None,
                settings_model_base_url,
            )
        });
        let provider_kind = self
            .options
            .provider
            .or_else(|| settings_provider_model.map(|(protocol, _)| protocol))
            .unwrap_or(AcpProviderKind::OpenAiCompatible);
        let cli_model = self
            .options
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty());
        let env_model = runtime_settings
            .effective_env
            .get(provider_kind.model_env())
            .map(String::as_str)
            .filter(|model| !model.trim().is_empty());
        let selected_model = cli_model.or(settings_model).or(env_model);
        let configured_model = selected_model.and_then(|model_id| {
            find_configured_model_provider(
                &runtime_settings.merged_settings,
                model_id,
                Some(provider_kind),
                self.options.base_url.as_deref().or_else(|| {
                    if cli_model.is_none() {
                        settings_model_base_url
                    } else {
                        None
                    }
                }),
            )
        });
        let model = self
            .options
            .model
            .clone()
            .filter(|model| !model.trim().is_empty())
            .or_else(|| settings_model.map(str::to_owned))
            .or_else(|| {
                runtime_settings
                    .effective_env
                    .get(provider_kind.model_env())
                    .cloned()
            })
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| provider_kind.model_requirement().to_owned())?;
        let model_generation_config =
            super::model_generation_config::NativeModelGenerationConfig::resolve(
                &runtime_settings.merged_settings,
                configured_model.map(|(_, model)| model),
                &model,
                &runtime_settings.effective_env,
            );
        let base_url = self
            .options
            .base_url
            .clone()
            .or_else(|| {
                configured_model
                    .and_then(|(_, model)| nonempty_settings_string(model.get("baseUrl")))
                    .map(str::to_owned)
            })
            .or_else(|| {
                runtime_settings
                    .effective_env
                    .get(provider_kind.base_url_env())
                    .cloned()
            })
            .or_else(|| {
                nonempty_settings_string(
                    runtime_settings
                        .merged_settings
                        .pointer("/security/auth/baseUrl"),
                )
                .map(str::to_owned)
            })
            .unwrap_or_else(|| provider_kind.default_base_url().to_owned());
        let auxiliary_base_url = if provider_kind == AcpProviderKind::OpenAiCompatible {
            base_url.clone()
        } else {
            runtime_settings
                .effective_env
                .get("OPENAI_BASE_URL")
                .cloned()
                .unwrap_or_else(|| OpenAiCompatibleConfig::default().base_url)
        };
        let input_modalities = model_generation_config.modalities;
        let configured_api_key = configured_model
            .and_then(|(_, model)| nonempty_settings_string(model.get("envKey")))
            .and_then(|env_key| runtime_settings.effective_env.get(env_key))
            .filter(|api_key| !api_key.trim().is_empty())
            .cloned();
        let fallback_api_key = match provider_kind {
            AcpProviderKind::OpenAiCompatible => runtime_settings
                .effective_env
                .get("OPENAI_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty())
                .or_else(|| {
                    runtime_settings
                        .effective_env
                        .get("CANOPY_API_KEY")
                        .filter(|api_key| !api_key.trim().is_empty())
                }),
            AcpProviderKind::Anthropic => runtime_settings
                .effective_env
                .get("ANTHROPIC_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty()),
            AcpProviderKind::Gemini => runtime_settings
                .effective_env
                .get("GEMINI_API_KEY")
                .filter(|api_key| !api_key.trim().is_empty()),
        }
        .cloned()
        .or_else(|| {
            nonempty_settings_string(
                runtime_settings
                    .merged_settings
                    .pointer("/security/auth/apiKey"),
            )
            .map(str::to_owned)
        });
        // Match the TypeScript model resolver's credential order: the active
        // model's envKey, an explicit CLI key, the provider's environment key,
        // then settings.security.auth.apiKey. `effective_env` already applies
        // process environment over dotenv and settings.env values.
        let api_key = configured_api_key
            .or_else(|| {
                self.options
                    .api_key
                    .as_deref()
                    .filter(|key| !key.trim().is_empty())
                    .map(str::to_owned)
            })
            .or(fallback_api_key);
        let runtime_provider = match provider_kind {
            AcpProviderKind::OpenAiCompatible => {
                let mut config = OpenAiCompatibleConfig {
                    base_url,
                    api_key: api_key.clone(),
                    proxy: runtime_settings.proxy_url.clone(),
                    ..OpenAiCompatibleConfig::default()
                };
                model_generation_config.apply_to_openai_compatible(&mut config);
                AcpRuntimeProviderConfig::OpenAiCompatible(config)
            }
            AcpProviderKind::Anthropic => {
                let mut config = AnthropicProviderConfig {
                    model: model.clone(),
                    base_url,
                    api_key: api_key.clone(),
                    proxy: runtime_settings.proxy_url.clone(),
                    cli_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                    ..AnthropicProviderConfig::default()
                };
                model_generation_config.apply_to_anthropic(&mut config);
                AcpRuntimeProviderConfig::Anthropic(config)
            }
            AcpProviderKind::Gemini => {
                let mut config = GeminiProviderConfig {
                    model: model.clone(),
                    base_url,
                    api_key,
                    proxy: runtime_settings.proxy_url.clone(),
                    user_agent: Some(format!(
                        "CanopyCode/{} ({}; {})",
                        env!("CARGO_PKG_VERSION"),
                        std::env::consts::OS,
                        std::env::consts::ARCH
                    )),
                    ..GeminiProviderConfig::default()
                };
                model_generation_config.apply_to_gemini(&mut config);
                AcpRuntimeProviderConfig::Gemini(config)
            }
        };
        // The ACP host's optional side-query integrations have not yet been
        // routed through the selected native provider. Keep their existing
        // OpenAI-compatible config isolated from the primary model route.
        let auxiliary_provider = OpenAiCompatibleConfig {
            base_url: auxiliary_base_url,
            api_key: runtime_settings
                .effective_env
                .get("OPENAI_API_KEY")
                .or_else(|| runtime_settings.effective_env.get("CANOPY_API_KEY"))
                .cloned(),
            proxy: runtime_settings.proxy_url.clone(),
            ..OpenAiCompatibleConfig::default()
        };
        let web_search_settings = super::web_search_config::resolve_web_search_settings(
            &runtime_settings.merged_settings,
            &runtime_settings.effective_env,
        );
        let search_model_entries =
            super::web_search_model_entries(&runtime_settings.merged_settings);
        let fast_model = runtime_settings
            .merged_settings
            .get("fastModel")
            .and_then(Value::as_str);
        let web_search_backend = if web_search_settings
            .as_ref()
            .is_some_and(super::web_search_config::WebSearchSettings::is_enabled)
        {
            let context = super::web_search_config::WebSearchModelContext {
                current_model: Some(&model),
                current_auth_type: Some(provider_kind.auth_type()),
                fast_model,
                model_entries: &search_model_entries,
            };
            match super::web_search_config::evaluate_web_search_gate(
                web_search_settings.as_ref(),
                &context,
                &runtime_settings.effective_env,
            ) {
                super::web_search_config::WebSearchGateResult::Ready(backend) => Some(backend),
                super::web_search_config::WebSearchGateResult::Notice(notice) => {
                    eprintln!("[CANOPY] {}", notice.message);
                    None
                }
            }
        } else {
            None
        };
        let file_read_cache = FileReadCache::default();
        let (computer_use_adapter, computer_use_declarations) = acp_computer_use::build_adapter(
            self.output.clone(),
            session_id.clone(),
            workspace,
            runtime_settings.permissions.clone(),
            runtime_settings.core_tools.clone(),
            runtime_settings.excluded_tools.clone(),
            runtime_settings.computer_use_enabled,
            runtime_settings.computer_use_max_image_dimension,
            runtime_settings.computer_use_idle_timeout_ms,
            runtime_settings
                .effective_env
                .get(canopy_core::tools::computer_use::MAX_IMAGE_DIMENSION_ENV)
                .map(String::as_str),
            Arc::clone(&approval_mode_state),
        )?;
        let mut declarations = vec![
            canopy_core::tools::read_file::function_declaration(),
            canopy_core::tools::image_view::ZoomImageTool::function_declaration(),
            canopy_core::tools::list_directory::function_declaration(),
            canopy_core::tools::glob::function_declaration(),
            canopy_core::tools::grep::function_declaration(),
            canopy_core::tools::notebook_edit::function_declaration(),
            canopy_core::tools::edit_file::function_declaration(),
            canopy_core::tools::write_file::function_declaration(),
            canopy_core::tools::record_artifact::function_declaration(),
            canopy_core::tools::shell::function_declaration(),
            canopy_core::tools::todo_write::function_declaration(),
            canopy_core::tools::web::fetch_invocation::function_declaration(),
        ];
        let skill_declaration = canopy_core::tools::skill::SkillTool::function_declaration();
        let skill_declaration_enabled =
            declaration_is_enabled(&skill_declaration, &runtime_settings, workspace)
                && canopy_core::tool_utils::is_tool_enabled(
                    "skill",
                    runtime_settings.core_tools.as_deref(),
                    Some(&runtime_settings.excluded_tools),
                );
        if has_model_invocable_skills {
            declarations.push(skill_declaration.clone());
        }
        if super::artifact_tool_enabled(&runtime_settings, true) {
            declarations.push(canopy_core::tools::artifact::ArtifactTool::function_declaration());
        }
        if web_search_backend.is_some() {
            declarations.push(canopy_core::tools::web::search_executor::function_declaration());
        }
        if super::image_generation_tool_config(&runtime_settings).is_some() {
            declarations.push(canopy_core::tools::image_gen::ImageGenTool::function_declaration());
        }
        declarations.retain(|declaration| {
            declaration_is_enabled(declaration, &runtime_settings, workspace)
                && (declaration.get("name").and_then(Value::as_str) != Some("skill")
                    || canopy_core::tool_utils::is_tool_enabled(
                        "skill",
                        runtime_settings.core_tools.as_deref(),
                        Some(&runtime_settings.excluded_tools),
                    ))
        });
        let session_server_map = session_mcp_server_map(&session_mcp_servers);
        let mut mcp_settings = runtime_settings.mcp_settings.clone().with_sources(
            workspace,
            None,
            Some(&session_server_map),
            &extension_mcp_sources,
        )?;
        let mcp_reference_server_names = mcp_settings.servers.keys().cloned().collect::<Vec<_>>();
        for warning in &mcp_settings.source_warnings {
            eprintln!("Warning: {warning}");
        }
        let mcp_permissions = runtime_settings.permissions.clone();
        let mcp_core_tools = runtime_settings.core_tools.clone();
        let mcp_excluded_tools = runtime_settings.excluded_tools.clone();
        let effective_env = runtime_settings.effective_env.clone();
        let prevent_system_sleep = runtime_settings.prevent_system_sleep;
        let runtime_base_dir = Storage::new(workspace).runtime_base_dir().to_path_buf();
        let pressure_compaction_settings = runtime_settings.clear_context_on_idle;
        let pressure_read_file_retention = managed_memory_path_retention(
            workspace,
            &runtime_base_dir,
            &runtime_settings.effective_env,
        );
        let pressure_keep_recent = runtime_settings
            .effective_env
            .get("CANOPY_MC_KEEP_RECENT")
            .cloned();
        let mut runtime_config =
            AgentRuntimeConfig::new(model, runtime_base_dir.join("tool-results"));
        runtime_config.usage_statistics_enabled = runtime_settings
            .merged_settings
            .pointer("/privacy/usageStatisticsEnabled")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        runtime_config.usage_auth_type = provider_kind.auth_type().as_str().to_owned();
        runtime_config.usage_source = "acp".to_owned();
        model_generation_config.apply_to_runtime(&mut runtime_config);
        runtime_config.max_model_turns = self.options.max_turns();
        runtime_config.system_instruction =
            super::build_system_instruction(base_system_instruction.as_deref(), "", "");
        runtime_config.tool_declarations = if declarations.is_empty() {
            Vec::new()
        } else {
            vec![json!({"functionDeclarations":declarations})]
        };
        runtime_config
            .tool_declarations
            .extend(computer_use_declarations);
        let memory_recall_selector = (provider_kind == AcpProviderKind::OpenAiCompatible)
            .then(|| {
                super::build_auto_memory_recall_selector(
                    auxiliary_provider.clone(),
                    &runtime_config.model,
                    fast_model,
                    &runtime_settings.merged_settings,
                    &runtime_settings.effective_env,
                )
            })
            .flatten();
        canopy_core::utils::tool_result_cleanup::schedule_cleanup_old_tool_results(
            Storage::get_global_temp_dir(),
            24.0 * 60.0 * 60.0 * 1000.0,
        );
        let mut executor = WorkspaceTools::new_for_acp(
            workspace,
            &runtime_base_dir,
            input_modalities,
            runtime_settings,
            file_read_cache.clone(),
            &runtime_config.model,
        )?;
        executor.conditional_rules = conditional_rules;
        executor.configure_side_query(auxiliary_provider, runtime_config.pipeline.clone())?;
        if let Some(backend) = web_search_backend {
            executor.configure_web_search(backend)?;
        }
        executor
            .select_session_with_attribution(&session_id, restored_snapshots, restored_attribution)
            .await?;
        let mcp_prompt: Arc<dyn McpCliApprovalPrompt> = Arc::new(AcpModeAwareMcpApprovalPrompt {
            approval_mode: Arc::clone(&approval_mode_state),
        });
        let (mcp_oauth_cancellation, mcp_oauth_cancellation_guard) =
            self.output.register_oauth_cancellation(&session_id);
        let mut mcp_session = self
            .mcp_workspace
            .open_session(
                session_id.clone(),
                &mcp_settings,
                mcp_permissions.clone(),
                mcp_core_tools.clone(),
                mcp_excluded_tools.clone(),
                Arc::clone(&mcp_prompt),
            )
            .await
            .map_err(|error| format!("could not prepare MCP tools: {error}"))?;
        let oauth_authorization = authorize_acp_mcp_servers(
            &self.output,
            &session_id,
            &mut mcp_settings,
            mcp_session.discovery_errors(),
            &mcp_oauth_cancellation,
        )
        .await;
        drop(mcp_oauth_cancellation_guard);
        let oauth_authenticated = match oauth_authorization {
            Ok(authenticated) => authenticated,
            Err(error) => {
                mcp_session.stop();
                return Err(error);
            }
        };
        if oauth_authenticated {
            mcp_session.stop();
            mcp_session = self
                .mcp_workspace
                .open_session(
                    session_id.clone(),
                    &mcp_settings,
                    mcp_permissions,
                    mcp_core_tools.clone(),
                    mcp_excluded_tools.clone(),
                    mcp_prompt,
                )
                .await
                .map_err(|error| format!("could not reconnect authenticated MCP tools: {error}"))?;
        }
        for (server, reason) in mcp_session.skipped_servers() {
            eprintln!("[CANOPY] Skipping MCP server `{server}`: {reason}.");
        }
        for (server, error) in mcp_session.discovery_errors() {
            eprintln!("[CANOPY] MCP server `{server}` could not be started: {error}");
        }
        mcp_session.append_function_declarations(
            &mut runtime_config.tool_declarations,
            mcp_core_tools.as_deref(),
            &mcp_excluded_tools,
        );
        let (_, prompt_registry) = mcp_session.reference_registries();
        let discovered_mcp_prompts = prompt_registry.get_all_prompts();
        let mcp_prompts = if skill_declaration_enabled {
            discovered_mcp_prompts
        } else {
            Vec::new()
        };
        if !mcp_prompts.is_empty() {
            let already_declared = runtime_config.tool_declarations.iter().any(|group| {
                group
                    .get("functionDeclarations")
                    .and_then(Value::as_array)
                    .is_some_and(|functions| {
                        functions.iter().any(|function| {
                            function.get("name").and_then(Value::as_str) == Some("skill")
                        })
                    })
            });
            if !already_declared {
                if let Some(functions) =
                    runtime_config
                        .tool_declarations
                        .iter_mut()
                        .find_map(|group| {
                            group
                                .get_mut("functionDeclarations")
                                .and_then(Value::as_array_mut)
                        })
                {
                    functions.push(skill_declaration);
                } else {
                    runtime_config.tool_declarations.push(json!({
                        "functionDeclarations":[skill_declaration]
                    }));
                }
            }
        }
        let runtime = match runtime_provider {
            AcpRuntimeProviderConfig::OpenAiCompatible(provider) => {
                AgentRuntime::new(provider, runtime_config)
            }
            AcpRuntimeProviderConfig::Anthropic(provider) => {
                AgentRuntime::new_anthropic(provider, runtime_config)
            }
            AcpRuntimeProviderConfig::Gemini(provider) => {
                AgentRuntime::new_gemini(provider, runtime_config)
            }
        };
        let runtime = match runtime {
            Ok(runtime) => runtime.with_prevent_system_sleep(prevent_system_sleep),
            Err(error) => {
                mcp_session.stop();
                return Err(format!("could not start model runtime: {error}"));
            }
        };
        let tool_cancellation = Arc::new(Mutex::new(CancellationToken::new()));
        let executor = computer_use_adapter.compose(mcp_session.compose(AcpImageGenToolExecutor {
            inner: executor,
            persistent_allow_rules: RwLock::new(PermissionRuleSet::default()),
            approval_mode: Arc::clone(&approval_mode_state),
            cancellation: tool_cancellation.clone(),
            output: self.output.clone(),
            session_id: session_id.clone(),
            skill_snapshot: Arc::clone(&skill_snapshot),
            mcp_prompts: mcp_prompts.clone(),
            mcp_skill_grants: mcp_session.skill_grants(),
            mcp_manager: Arc::clone(mcp_session.manager()),
        }));
        let memory_pressure_task =
            super::memory_pressure::MemoryPressureTask::start_with_pressure_state(
                file_read_cache.clone(),
                session_id.clone(),
                Arc::clone(&self.memory_pressure),
            );
        let runtime = if let Some(task) = memory_pressure_task.as_ref() {
            runtime
                .with_memory_pressure_compaction(
                    task.compaction_requested(),
                    file_read_cache,
                    pressure_compaction_settings,
                    pressure_read_file_retention,
                    pressure_keep_recent,
                )
                .with_memory_pressure_check(task.check_requester())
        } else {
            runtime
        };
        self.register_approval_mode(&session_id, approval_mode_state);
        // ACP can host several sessions in one process. Keep one atomic
        // sidecar per session, rooted beside this session's transcript, and
        // let a status-write failure leave session startup unaffected.
        let _ =
            write_session_runtime_status(store.paths().runtime_base_dir(), workspace, &session_id)
                .await;
        let agent = Arc::new(CliAcpAgent {
            session_id,
            provider_kind,
            input_modalities,
            workspace_root: workspace.to_path_buf(),
            runtime_base_dir,
            effective_env,
            active_extensions,
            mcp_reference_server_names,
            mcp_prompts,
            skill_snapshot,
            memory_recall_selector,
            base_system_instruction,
            runtime: Arc::new(runtime),
            state: AsyncMutex::new(CliAcpPromptState {
                recorder,
                executor,
                store,
                history,
            }),
            mcp_session,
            output: self.output.clone(),
            cancel_generation: watch::channel(0).0,
            tool_cancellation,
            _memory_pressure_task: memory_pressure_task,
            closed: std::sync::atomic::AtomicBool::new(false),
        });
        self.register_active_agent(&agent);
        Ok(agent)
    }

    fn create_recorder(
        &self,
        store: &SessionStore,
        session_id: &str,
        cwd: &Path,
    ) -> Result<SessionRecorder, String> {
        let lease = store
            .acquire_writer_lease(
                session_id,
                SessionWriterProcessKind::Acp,
                Some(env!("CARGO_PKG_VERSION").to_owned()),
            )
            .map_err(|error| format!("could not acquire session writer: {error}"))?;
        let options = SessionRecorderOptions::new(cwd.to_string_lossy(), env!("CARGO_PKG_VERSION"));
        Ok(SessionRecorder::new(lease, options))
    }

    fn transcript_replay_events(&self, session_id: &str) -> Result<Vec<BridgeEvent>, String> {
        let store = SessionStore::new(
            Storage::new(&self.workspace)
                .runtime_base_dir()
                .to_path_buf(),
            self.workspace.clone(),
        );
        let prepared = store
            .prepare_active_transcript(session_id)
            .map_err(|error| error.to_string())?;
        let mut events = Vec::new();
        for record in prepared.records {
            let update_kind = match record.record_type {
                TranscriptRecordType::User => "user_message_chunk",
                TranscriptRecordType::Assistant => "agent_message_chunk",
                TranscriptRecordType::ToolResult | TranscriptRecordType::System => continue,
            };
            let Some(text) = transcript_text(&record) else {
                continue;
            };
            let record_id = record.uuid.clone();
            let mut update_meta = Map::new();
            update_meta.insert("qwen.session.recordId".to_owned(), json!(record_id));
            update_meta.insert(
                "qwenTranscript".to_owned(),
                json!({"sourceRecordIds":[record.uuid]}),
            );
            if let Some(timestamp) = record.timestamp.as_deref().and_then(|timestamp| {
                chrono::DateTime::parse_from_rfc3339(timestamp)
                    .ok()
                    .map(|timestamp| timestamp.timestamp_millis())
            }) {
                update_meta.insert("timestamp".to_owned(), json!(timestamp));
            }
            let mut update = json!({
                "sessionUpdate":update_kind,
                "content":{"type":"text","text":text}
            });
            update["_meta"] = Value::Object(update_meta);
            events.push(BridgeEvent::new("session_update", json!({"update":update})));
        }
        Ok(events)
    }
}

impl BridgeSessionFactory for CliAcpSessionFactory {
    fn spawn<'a>(
        &'a self,
        _request: BridgeSpawnRequest,
        workspace_cwd: PathBuf,
        _effective_scope: BridgeSessionScope,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, String>> {
        Box::pin(async move {
            let session_id = _request.session_id.clone().unwrap_or_else(fresh_session_id);
            let session_mcp_servers = _request.session_mcp_servers;
            let store = session_store(&workspace_cwd);
            let recorder = self.create_recorder(&store, &session_id, &workspace_cwd)?;
            let agent = self
                .build_agent(
                    &workspace_cwd,
                    session_id.clone(),
                    store.clone(),
                    recorder,
                    Vec::new(),
                    Vec::new(),
                    None,
                    session_mcp_servers,
                )
                .await?;
            let transcript_path = store
                .transcript_path(
                    &session_id,
                    canopy_core::session_paths::SessionArchiveState::Active,
                )
                .ok();
            Ok(CreatedBridgeSession {
                session_id,
                effective_cwd: workspace_cwd.clone(),
                created_at: timestamp_now(),
                transcript_path,
                restore_state: None,
                agent,
            })
        })
    }

    fn restore<'a>(
        &'a self,
        request: BridgeRestoreRequest,
        _action: BridgeRestoreAction,
        workspace_cwd: PathBuf,
    ) -> SessionRuntimeFuture<'a, Result<CreatedBridgeSession, BridgeSessionFactoryError>> {
        Box::pin(async move {
            let session_mcp_servers = request.session_mcp_servers;
            let store = session_store(&workspace_cwd);
            let resumed = store
                .resume_session(
                    &request.session_id,
                    SessionResumeOptions {
                        process_kind: SessionWriterProcessKind::Acp,
                        version: env!("CARGO_PKG_VERSION").to_owned(),
                        git_branch: None,
                        allow_auto_continue: false,
                    },
                )
                .map_err(|error| BridgeSessionFactoryError::Operation(error.to_string()))?;
            let restored_snapshots =
                restored_file_history_snapshots(&resumed.prepared_transcript.records);
            let restored_attribution =
                restored_attribution_snapshot(&resumed.prepared_transcript.records);
            let plan = resumed.recovery_plan;
            if plan.kind == SessionRecoveryKind::DegradedHistory {
                return Err(BridgeSessionFactoryError::Operation(
                    plan.visible_notice.unwrap_or_else(|| {
                        "session history is incomplete and cannot be restored safely".to_owned()
                    }),
                ));
            }
            let mut recorder = resumed.recorder;
            if plan.kind == SessionRecoveryKind::InterruptedTurn {
                record_synthesized_recovery_results(&plan, &mut recorder)
                    .map_err(BridgeSessionFactoryError::Operation)?;
            }
            let history = plan.api_history.clone();
            let restore_state = json!({
                "recoveryKind":recovery_kind_name(plan.kind),
                "canContinue":plan.can_continue,
                "requiresUserConfirmation":plan.requires_user_confirmation,
                "visibleNotice":plan.visible_notice,
            });
            let agent = self
                .build_agent(
                    &workspace_cwd,
                    request.session_id.clone(),
                    store.clone(),
                    recorder,
                    history,
                    restored_snapshots,
                    restored_attribution,
                    session_mcp_servers,
                )
                .await
                .map_err(BridgeSessionFactoryError::Operation)?;
            let transcript_path = store
                .transcript_path(
                    &request.session_id,
                    canopy_core::session_paths::SessionArchiveState::Active,
                )
                .ok();
            Ok(CreatedBridgeSession {
                session_id: request.session_id,
                effective_cwd: workspace_cwd,
                created_at: timestamp_now(),
                transcript_path,
                restore_state: Some(restore_state),
                agent,
            })
        })
    }
}

/// Adds ACP client interactions and per-prompt cancellation to host tools,
/// while delegating ordinary calls to the native workspace executor.
#[derive(Clone)]
struct AcpSkillSettingsLayers {
    system: Map<String, Value>,
    system_defaults: Map<String, Value>,
    user: Map<String, Value>,
    workspace: Map<String, Value>,
    workspace_trusted: bool,
}

fn load_acp_workspace_settings_scope(workspace: &Path) -> Result<Map<String, Value>, String> {
    let mut options = LoadSettingsOptions::default();
    let loaded =
        load_settings(workspace.to_path_buf(), &mut options).map_err(|error| error.to_string())?;
    Ok(loaded.workspace.settings)
}

impl AcpSkillSettingsLayers {
    fn load(workspace: &Path, workspace_trusted: bool) -> Result<Self, String> {
        let mut options = LoadSettingsOptions::default();
        let loaded = load_settings(workspace.to_path_buf(), &mut options)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            system: loaded.system.settings,
            system_defaults: loaded.system_defaults.settings,
            user: loaded.user.settings,
            workspace: loaded.workspace.settings,
            workspace_trusted,
        })
    }

    fn reload_workspace_scope(&mut self, workspace: &Path) -> Result<(), String> {
        self.workspace = load_acp_workspace_settings_scope(workspace)?;
        Ok(())
    }

    fn merged(&self) -> Map<String, Value> {
        merge_settings(
            &self.system,
            &self.system_defaults,
            &self.user,
            &self.workspace,
            self.workspace_trusted,
        )
    }
}

#[derive(Clone)]
struct AcpSkillSnapshot {
    manager: Arc<SkillManager>,
    manager_config: SkillManagerConfig,
    disabled_skill_names: HashSet<String>,
    settings: AcpSkillSettingsLayers,
}

impl AcpSkillSnapshot {
    fn new(mut manager_config: SkillManagerConfig, settings: AcpSkillSettingsLayers) -> Self {
        let merged_settings = Value::Object(settings.merged());
        let disabled_skill_names = acp_disabled_skill_names(
            &merged_settings,
            manager_config.safe_mode,
            manager_config.bare_mode,
        );
        manager_config.disabled_skill_levels = acp_disabled_skill_levels(
            &merged_settings,
            manager_config.safe_mode,
            manager_config.bare_mode,
        );
        manager_config.custom_skill_dirs = acp_custom_skill_dirs(
            &merged_settings,
            manager_config.safe_mode,
            manager_config.bare_mode,
        );
        let manager = Arc::new(SkillManager::new(manager_config.clone()));
        Self {
            manager,
            manager_config,
            disabled_skill_names,
            settings,
        }
    }

    fn reload_workspace_settings(&mut self) -> Result<Arc<SkillManager>, String> {
        self.settings
            .reload_workspace_scope(&self.manager_config.project_root)?;
        let merged_settings = Value::Object(self.settings.merged());
        self.disabled_skill_names = acp_disabled_skill_names(
            &merged_settings,
            self.manager_config.safe_mode,
            self.manager_config.bare_mode,
        );
        let disabled_skill_levels = acp_disabled_skill_levels(
            &merged_settings,
            self.manager_config.safe_mode,
            self.manager_config.bare_mode,
        );
        let custom_skill_dirs = acp_custom_skill_dirs(
            &merged_settings,
            self.manager_config.safe_mode,
            self.manager_config.bare_mode,
        );
        let manager_config_changed = self.manager_config.disabled_skill_levels
            != disabled_skill_levels
            || self.manager_config.custom_skill_dirs != custom_skill_dirs;
        self.manager_config.disabled_skill_levels = disabled_skill_levels;
        self.manager_config.custom_skill_dirs = custom_skill_dirs;
        if manager_config_changed {
            self.manager = Arc::new(SkillManager::new(self.manager_config.clone()));
        }
        Ok(Arc::clone(&self.manager))
    }
}

fn clone_acp_skill_snapshot(snapshot: &RwLock<AcpSkillSnapshot>) -> AcpSkillSnapshot {
    snapshot
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

struct AcpImageGenToolExecutor {
    inner: WorkspaceTools,
    persistent_allow_rules: RwLock<PermissionRuleSet>,
    approval_mode: Arc<AcpApprovalModeState>,
    cancellation: Arc<Mutex<CancellationToken>>,
    output: ProtocolOutput,
    session_id: String,
    skill_snapshot: Arc<RwLock<AcpSkillSnapshot>>,
    mcp_prompts: Vec<Arc<McpPrompt>>,
    mcp_skill_grants: McpSessionSkillGrants,
    mcp_manager: Arc<McpClientManager>,
}

fn acp_skill_setting_names(settings: &Value, key: &str) -> HashSet<String> {
    settings
        .pointer(&format!("/skills/{key}"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|name| name.trim().to_lowercase())
        .filter(|name| !name.is_empty())
        .collect()
}

fn acp_disabled_skill_names(settings: &Value, safe_mode: bool, bare_mode: bool) -> HashSet<String> {
    if safe_mode || bare_mode {
        return HashSet::new();
    }
    let mut disabled = acp_skill_setting_names(settings, "disabled");
    let enabled = acp_skill_setting_names(settings, "enabled");
    disabled.extend(
        acp_skill_setting_names(settings, "defaultDisabled")
            .difference(&enabled)
            .cloned(),
    );
    disabled
}

fn acp_disabled_skill_levels(
    settings: &Value,
    safe_mode: bool,
    bare_mode: bool,
) -> Vec<SkillLevel> {
    if safe_mode || bare_mode {
        return Vec::new();
    }
    settings
        .pointer("/skills/disabledLevels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter_map(|level| match level {
            "project" => Some(SkillLevel::Project),
            "user" => Some(SkillLevel::User),
            "extension" => Some(SkillLevel::Extension),
            "bundled" => Some(SkillLevel::Bundled),
            _ => None,
        })
        .collect()
}

fn acp_custom_skill_dirs(settings: &Value, safe_mode: bool, bare_mode: bool) -> Vec<String> {
    if safe_mode || bare_mode {
        return Vec::new();
    }
    settings
        .pointer("/skills/directories")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn acp_available_skill_configs(
    manager: &SkillManager,
    all_skills: &[SkillConfig],
    disabled_names: &HashSet<String>,
) -> Vec<SkillConfig> {
    all_skills
        .iter()
        .filter(|skill| {
            skill.disable_model_invocation != Some(true)
                && !disabled_names.contains(&skill.name.to_lowercase())
                && manager.is_skill_active(skill)
        })
        .cloned()
        .collect()
}

fn acp_skill_level_name(level: SkillLevel) -> &'static str {
    match level {
        SkillLevel::Project => "project",
        SkillLevel::User => "user",
        SkillLevel::Extension => "extension",
        SkillLevel::Bundled => "bundled",
    }
}

fn render_acp_available_skills_block(skills: &[SkillConfig]) -> String {
    let mut sorted = skills.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.name.cmp(&right.name));
    let mut rendered = String::new();
    let mut omitted = 0usize;
    for skill in sorted {
        let mut description = canopy_core::utils::xml::escape_xml(&skill.description);
        if let Some(when_to_use) = skill
            .when_to_use
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            description.push_str(" — ");
            description.push_str(&canopy_core::utils::xml::escape_xml(when_to_use));
        }
        let entry = format!(
            "<skill>\n<name>\n{}\n</name>\n<description>\n{} ({})\n</description>\n<location>\n{}\n</location>\n</skill>",
            canopy_core::utils::xml::escape_xml(&skill.name),
            description,
            acp_skill_level_name(skill.level),
            acp_skill_level_name(skill.level),
        );
        let separator = usize::from(!rendered.is_empty());
        if rendered
            .len()
            .saturating_add(separator)
            .saturating_add(entry.len())
            > MAX_ACP_SKILL_REMINDER_BYTES
        {
            omitted += 1;
            continue;
        }
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&entry);
    }
    if omitted > 0 {
        let note =
            format!("\nAdditional skills omitted ({omitted}) due to the ACP prompt size limit.");
        if rendered.len().saturating_add(note.len()) <= MAX_ACP_SKILL_REMINDER_BYTES {
            rendered.push_str(&note);
        }
    }
    rendered
}

fn render_acp_available_mcp_prompts_block(prompts: &[Arc<McpPrompt>]) -> String {
    let mut sorted = prompts.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.name.cmp(&right.name));
    let mut rendered = String::new();
    let mut omitted = 0usize;
    for prompt in sorted {
        let arguments = prompt
            .arguments
            .iter()
            .filter_map(|argument| {
                let name = argument.get("name")?.as_str()?;
                let required = argument
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let description = argument
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                Some(format!(
                    "<argument>\n<name>{}</name>\n<required>{}</required>\n<description>{}</description>\n</argument>",
                    canopy_core::utils::xml::escape_xml(name),
                    required,
                    canopy_core::utils::xml::escape_xml(description),
                ))
            })
            .collect::<Vec<_>>()
            .join("\n");
        let entry = format!(
            "<skill>\n<name>\n{}\n</name>\n<description>\n{} (MCP prompt on server {})\n</description>\n<arguments>\n{}\n</arguments>\n</skill>",
            canopy_core::utils::xml::escape_xml(&prompt.name),
            canopy_core::utils::xml::escape_xml(prompt.description.as_deref().unwrap_or_default()),
            canopy_core::utils::xml::escape_xml(&prompt.server_name),
            arguments,
        );
        let separator = usize::from(!rendered.is_empty());
        if rendered
            .len()
            .saturating_add(separator)
            .saturating_add(entry.len())
            > MAX_ACP_MCP_PROMPT_REMINDER_BYTES
        {
            omitted += 1;
            continue;
        }
        if !rendered.is_empty() {
            rendered.push('\n');
        }
        rendered.push_str(&entry);
    }
    if omitted > 0 {
        let note = format!(
            "\nAdditional MCP prompts omitted ({omitted}) due to the ACP prompt size limit."
        );
        if rendered.len().saturating_add(note.len()) <= MAX_ACP_MCP_PROMPT_REMINDER_BYTES {
            rendered.push_str(&note);
        }
    }
    rendered
}

async fn acp_skill_system_reminder(
    manager: &SkillManager,
    disabled_names: &HashSet<String>,
    mcp_prompts: &[Arc<McpPrompt>],
) -> String {
    let all_skills = manager.list_skills(ListSkillsOptions::default()).await;
    let available = acp_available_skill_configs(manager, &all_skills, disabled_names);
    let available_prompts = mcp_prompts
        .iter()
        .filter(|prompt| {
            !all_skills.iter().any(|skill| {
                skill.name == prompt.name
                    && skill.disable_model_invocation != Some(true)
                    && !disabled_names.contains(&skill.name.to_lowercase())
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if available.is_empty() && available_prompts.is_empty() {
        return String::new();
    }
    let mut sections = Vec::new();
    let skill_entries = render_acp_available_skills_block(&available);
    let prompt_entries = render_acp_available_mcp_prompts_block(&available_prompts);
    let entries = [skill_entries, prompt_entries]
        .into_iter()
        .filter(|entries| !entries.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    sections.push(format!(
        "Available skills and model-invocable commands — invoke one by calling the Skill tool with its name and, for MCP prompts, optional arguments:\n<available_skills>\n{entries}\n</available_skills>"
    ));
    format!(
        "<system-reminder>\n{}\n</system-reminder>",
        sections.join("\n")
    )
}

async fn acp_available_commands_update(
    snapshot: &AcpSkillSnapshot,
    mcp_prompts: &[Arc<McpPrompt>],
) -> Value {
    let all_skills = snapshot
        .manager
        .list_skills(ListSkillsOptions::default())
        .await;
    let available_skills = all_skills
        .iter()
        .filter(|skill| {
            !snapshot
                .disabled_skill_names
                .contains(&skill.name.to_lowercase())
                && snapshot.manager.is_skill_active(skill)
        })
        .collect::<Vec<_>>();
    let skill_commands = available_skills
        .iter()
        .filter(|skill| skill.user_invocable != Some(false))
        .map(|skill| {
            let mut command = json!({
                "name":skill.name,
                "description":skill.description,
                "input":skill.argument_hint.as_ref().map(|hint| json!({"hint":hint})),
                "_meta":{
                    "source":"skill",
                    "modelInvocable":skill.disable_model_invocation != Some(true),
                },
            });
            if let Some(argument_hint) = &skill.argument_hint {
                command["_meta"]["argumentHint"] = Value::String(argument_hint.clone());
            }
            command
        });
    let visible_skill_names = available_skills
        .iter()
        .map(|skill| skill.name.as_str())
        .collect::<HashSet<_>>();
    let prompt_commands = mcp_prompts
        .iter()
        .filter(|prompt| !visible_skill_names.contains(prompt.name.as_str()))
        .map(|prompt| {
            let argument_hint = prompt
                .arguments
                .iter()
                .filter_map(|argument| argument.get("name").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(", ");
            json!({
                "name":prompt.name,
                "description":prompt.description.as_deref().unwrap_or_default(),
                "input":(!argument_hint.is_empty()).then(|| json!({"hint":argument_hint})),
                "_meta":{
                    "source":"mcp",
                    "modelInvocable":true,
                },
            })
        });
    let mut available_commands = vec![
        json!({
            "name":"stats",
            "description":"Show usage statistics dashboard.",
            "input":{"hint":"[model|tools|skills|daily|monthly|export]"},
            "_meta":{
                "argumentHint":"[model|tools|skills|daily|monthly|export]",
                "source":"built-in",
                "sourceLabel":"Built-in",
                "supportedModes":["acp"],
                "subcommands":["model","tools","skills","daily","day","monthly","month","export"],
                "modelInvocable":false,
                "altNames":["usage"],
            },
        }),
        json!({
            "name":"doctor",
            "description":"Run native process diagnostics or inspect rollback availability.",
            "input":{"hint":"[memory [--json] [--sample] | rollback]"},
            "_meta":{
                "argumentHint":"[memory [--json] [--sample] | rollback]",
                "source":"built-in",
                "sourceLabel":"Built-in",
                "supportedModes":["acp"],
                "subcommands":["memory","rollback"],
                "modelInvocable":false,
            },
        }),
    ];
    available_commands.extend(skill_commands.chain(prompt_commands));
    let available_skills_names = available_skills
        .iter()
        .map(|skill| skill.name.clone())
        .collect::<Vec<_>>();
    let available_skill_details = available_skills
        .iter()
        .map(|skill| {
            json!({
                "name":skill.name,
                "description":skill.description,
                "body":skill.body,
                "filePath":skill.file_path,
                "level":acp_skill_level_name(skill.level),
                "modelInvocable":skill.disable_model_invocation != Some(true),
            })
        })
        .collect::<Vec<_>>();
    let mut metadata = json!({"availableSkills":available_skills_names});
    if !available_skill_details.is_empty() {
        metadata["availableSkillDetails"] = Value::Array(available_skill_details);
    }
    json!({
        "sessionUpdate":"available_commands_update",
        "availableCommands":available_commands,
        "_meta":metadata,
    })
}

async fn execute_acp_user_question(
    output: &ProtocolOutput,
    session_id: &str,
    call: &ToolCallRequestInfo,
    cancellation: &CancellationToken,
) -> Result<ToolExecutionOutput, String> {
    use canopy_core::tools::ask_user_question::{answer_result, parse_questions};

    let questions = parse_questions(&call.args)?;
    let question_values = questions
        .iter()
        .map(|question| {
            json!({
                "question":question.question,
                "header":question.header,
                "options":question.options.iter().map(|option| json!({
                    "label":option.label,
                    "description":option.description
                })).collect::<Vec<_>>(),
                "multiSelect":question.multi_select
            })
        })
        .collect::<Vec<_>>();
    let raw_input = json!({"questions":question_values});
    let request = json!({
        "sessionId":session_id,
        "options":[
            {"optionId":"proceed_once","name":"Submit","kind":"allow_once"},
            {"optionId":"cancel","name":"Cancel","kind":"reject_once"}
        ],
        "toolCall":{
            "toolCallId":call.call_id,
            "status":"pending",
            "title":format!("AskUserQuestion: Ask user {} question{}", questions.len(), if questions.len() == 1 { "" } else { "s" }),
            "kind":"other",
            "rawInput":raw_input,
            "_meta":{
                "qwenInteractionKind":"user_question",
                "qwenQuestions":question_values
            }
        }
    });
    let request_size = serde_json::to_vec(&request)
        .map_err(|error| format!("could not encode ACP user question: {error}"))?
        .len();
    if request_size > MAX_ACP_USER_QUESTION_BYTES {
        return Err(format!(
            "ACP user question is too large to send (maximum {MAX_ACP_USER_QUESTION_BYTES} bytes)."
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err("ACP prompt was cancelled while waiting for the user to answer questions.".to_owned());
        }
        result = tokio::time::timeout(
            ACP_USER_QUESTION_TIMEOUT,
            output.request_client_for_session(session_id, "session/request_permission", request),
        ) => match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let bounded = bound_acp_user_question_error(&error);
                return Err(format!("ACP client could not accept the user question: {bounded}"));
            }
            Err(_) => {
                return Err(format!(
                    "ACP client did not answer the user question within {} seconds.",
                    ACP_USER_QUESTION_TIMEOUT.as_secs()
                ));
            }
        }
    };

    match response.pointer("/outcome/outcome").and_then(Value::as_str) {
        Some("selected") => match response
            .pointer("/outcome/optionId")
            .and_then(Value::as_str)
        {
            Some("proceed_once") => {
                let answers = parse_acp_user_question_answers(&response, questions.len())?;
                Ok(answer_result(&questions, &answers, true))
            }
            Some("cancel") => Ok(answer_result(&questions, &HashMap::new(), false)),
            _ => Err("ACP client returned an unknown user question option.".to_owned()),
        },
        Some("cancelled") => Err("User cancelled answering the questions.".to_owned()),
        _ => Err("ACP client returned an invalid user question response.".to_owned()),
    }
}

fn parse_acp_user_question_answers(
    response: &Value,
    question_count: usize,
) -> Result<HashMap<String, String>, String> {
    let Some(value) = response.get("answers") else {
        return Ok(HashMap::new());
    };
    let answers = value
        .as_object()
        .ok_or_else(|| "ACP client returned malformed user question answers.".to_owned())?;
    if answers.len() > question_count {
        return Err("ACP client returned answers for unknown questions.".to_owned());
    }

    let mut parsed = HashMap::with_capacity(answers.len());
    let mut total_bytes = 0usize;
    for (key, value) in answers {
        let valid_key = key
            .parse::<usize>()
            .ok()
            .filter(|index| *index < question_count)
            .is_some_and(|index| index.to_string() == *key);
        if !valid_key {
            return Err("ACP client returned an answer for an unknown question.".to_owned());
        }
        let answer = value
            .as_str()
            .ok_or_else(|| "ACP client returned a non-text user question answer.".to_owned())?;
        if answer.len() > MAX_ACP_USER_QUESTION_ANSWER_BYTES {
            return Err(format!(
                "ACP user question answers must be at most {MAX_ACP_USER_QUESTION_ANSWER_BYTES} bytes each."
            ));
        }
        total_bytes = total_bytes.saturating_add(answer.len());
        if total_bytes > MAX_ACP_USER_QUESTION_ANSWERS_BYTES {
            return Err(format!(
                "ACP user question answers exceed the {MAX_ACP_USER_QUESTION_ANSWERS_BYTES}-byte total limit."
            ));
        }
        parsed.insert(key.clone(), answer.to_owned());
    }
    Ok(parsed)
}

fn bound_acp_user_question_error(value: &str) -> String {
    let prefix = value
        .chars()
        .take(MAX_ACP_USER_QUESTION_ERROR_CHARS * 4)
        .collect::<String>();
    canopy_core::utils::terminal_safe::strip_terminal_control_sequences(&prefix)
        .chars()
        .filter(|character| {
            !matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
            )
        })
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .take(MAX_ACP_USER_QUESTION_ERROR_CHARS)
        .collect::<String>()
        .trim()
        .to_owned()
}

fn bound_acp_permission_preview(value: &str) -> String {
    const TRUNCATION_MARKER: &str = "\n[Permission preview truncated.]";
    let prefix = value
        .chars()
        .take(MAX_ACP_PERMISSION_PREVIEW_BYTES.saturating_mul(4))
        .collect::<String>();
    let safe = canopy_core::utils::terminal_safe::strip_terminal_control_sequences(&prefix)
        .chars()
        .filter(|character| {
            !matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
            )
        })
        .map(|character| {
            if character.is_control() && !matches!(character, '\n' | '\t') {
                '\u{fffd}'
            } else {
                character
            }
        })
        .collect::<String>();
    if safe.len() <= MAX_ACP_PERMISSION_PREVIEW_BYTES {
        return safe;
    }

    let target_len = MAX_ACP_PERMISSION_PREVIEW_BYTES.saturating_sub(TRUNCATION_MARKER.len());
    let mut end = target_len.min(safe.len());
    while !safe.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}{}", safe[..end].trim_end(), TRUNCATION_MARKER)
}

fn acp_permission_text_content(text: &str) -> Value {
    json!({
        "type":"content",
        "content":{"type":"text","text":bound_acp_permission_preview(text)}
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcpPermissionOutcome {
    AllowOnce,
    AllowAlwaysProject,
    AllowAlwaysUser,
}

static ACP_PERMISSION_SETTINGS_LOCK: Mutex<()> = Mutex::new(());

fn acp_path_permission_rule(
    tool_name: &str,
    path: &Path,
    workspace_root: &Path,
) -> Option<(String, bool)> {
    let path_text = path.to_str()?.replace('\\', "/");
    if path_text.is_empty()
        || path_text.len() > 4096
        || path_text
            .chars()
            .any(|character| character.is_control() || matches!(character, '*' | '?' | '['))
    {
        return None;
    }
    let (specifier, project_scoped) = if let Ok(relative) = path.strip_prefix(workspace_root) {
        let relative = relative.to_str()?.replace('\\', "/");
        let relative = relative.trim_start_matches('/');
        if relative.is_empty() {
            return None;
        }
        (format!("/{relative}"), true)
    } else if path_text.starts_with('/') {
        (format!("//{}", path_text.trim_start_matches('/')), false)
    } else {
        return None;
    };
    Some((format!("{tool_name}({specifier})"), project_scoped))
}

fn acp_shell_permission_rule(command: &str) -> Option<String> {
    let command = command.trim();
    if command.is_empty()
        || command.len() > 1024
        || shell_command_uses_indirection(command)
        || command.chars().any(|character| {
            character.is_control() || matches!(character, ';' | '|' | '&' | '*' | '(' | ')')
        })
    {
        return None;
    }
    Some(format!("Bash({command})"))
}

fn persist_acp_permission_rule(
    inner: &WorkspaceTools,
    scope: AcpPermissionOutcome,
    rule: &str,
    project_scope_allowed: bool,
    workspace_is_trusted: bool,
) -> Result<(), String> {
    let path = match scope {
        AcpPermissionOutcome::AllowAlwaysProject if project_scope_allowed => {
            if !workspace_is_trusted {
                return Err(
                    "Cannot save a project permission in an untrusted workspace.".to_owned(),
                );
            }
            inner.storage.get_workspace_settings_path()
        }
        AcpPermissionOutcome::AllowAlwaysProject => {
            return Err("This permission can only be saved for the user because its scope is outside the workspace.".to_owned());
        }
        AcpPermissionOutcome::AllowAlwaysUser => Storage::get_global_settings_path(),
        AcpPermissionOutcome::AllowOnce => return Ok(()),
    };
    let _guard = ACP_PERMISSION_SETTINGS_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let settings = match std::fs::read_to_string(&path) {
        Ok(contents) => canopy_core::jsonc::parse_jsonc_object(&contents)
            .map_err(|error| format!("could not read permission settings: {error}"))?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => Map::new(),
        Err(error) => return Err(format!("could not read permission settings: {error}")),
    };
    let permissions = match settings.get("permissions") {
        Some(Value::Object(permissions)) => permissions,
        Some(_) => {
            return Err("permissions settings must be an object to save this rule.".to_owned());
        }
        None => {
            return update_setting_value(&path, "permissions.allow", json!([rule]))
                .map_err(|error| format!("could not save permission rule: {error}"));
        }
    };
    let mut rules = match permissions.get("allow") {
        Some(Value::Array(rules)) => rules.clone(),
        Some(_) => return Err("permissions.allow must be an array to save this rule.".to_owned()),
        None => Vec::new(),
    };
    if rules.iter().any(|existing| existing.as_str() == Some(rule)) {
        return Ok(());
    }
    rules.push(Value::String(rule.to_owned()));
    update_setting_value(&path, "permissions.allow", Value::Array(rules))
        .map_err(|error| format!("could not save permission rule: {error}"))
}

async fn request_acp_tool_permission(
    output: &ProtocolOutput,
    session_id: &str,
    call: &ToolCallRequestInfo,
    title: &str,
    kind: &str,
    content: Vec<Value>,
    file_path: Option<&Path>,
    persistent_rules: &[String],
    allow_project_scope: bool,
    cancellation: &CancellationToken,
) -> Result<AcpPermissionOutcome, String> {
    let mut tool_call = json!({
        "toolCallId":call.call_id,
        "status":"pending",
        "title":bound_acp_permission_preview(title),
        "kind":kind,
        "content":content,
        "rawInput":call.args
    });
    if let Some(path) = file_path {
        tool_call["locations"] = json!([{
            "path":bound_acp_permission_preview(&path.to_string_lossy())
        }]);
    }
    let mut options = Vec::with_capacity(4);
    if !persistent_rules.is_empty() {
        if allow_project_scope {
            options.push(json!({
                "optionId":"proceed_always_project",
                "name":format!("Always allow in project: {}", bound_acp_permission_preview(&persistent_rules.join(", "))),
                "kind":"allow_always"
            }));
        }
        options.push(json!({
            "optionId":"proceed_always_user",
            "name":format!("Always allow for user: {}", bound_acp_permission_preview(&persistent_rules.join(", "))),
            "kind":"allow_always"
        }));
    }
    options.push(json!({"optionId":"proceed_once","name":"Allow once","kind":"allow_once"}));
    options.push(json!({"optionId":"cancel","name":"Reject","kind":"reject_once"}));
    let request = json!({
        "sessionId":session_id,
        "options":options,
        "toolCall":tool_call
    });
    let request_size = serde_json::to_vec(&request)
        .map_err(|error| format!("could not encode ACP permission request: {error}"))?
        .len();
    if request_size > MAX_ACP_PERMISSION_REQUEST_BYTES {
        return Err(format!(
            "ACP permission preview for `{}` is too large to send safely; no changes were made.",
            call.name
        ));
    }

    let response = tokio::select! {
        biased;
        _ = cancellation.cancelled() => {
            return Err(format!(
                "ACP prompt was cancelled while waiting to approve `{}`; no changes were made.",
                call.name
            ));
        }
        result = tokio::time::timeout(
            ACP_TOOL_PERMISSION_TIMEOUT,
            output.request_client_for_session(session_id, "session/request_permission", request),
        ) => match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                let bounded = bound_acp_user_question_error(&error);
                return Err(format!(
                    "ACP client permission request for `{}` failed: {bounded}. No changes were made.",
                    call.name
                ));
            }
            Err(_) => {
                return Err(format!(
                    "ACP client did not answer the permission request for `{}` within {} seconds; no changes were made.",
                    call.name,
                    ACP_TOOL_PERMISSION_TIMEOUT.as_secs()
                ));
            }
        }
    };

    if cancellation.is_cancelled() {
        return Err(format!(
            "ACP prompt was cancelled before `{}` could be approved; no changes were made.",
            call.name
        ));
    }

    match response.pointer("/outcome/outcome").and_then(Value::as_str) {
        Some("selected") => match response
            .pointer("/outcome/optionId")
            .and_then(Value::as_str)
        {
            Some("proceed_once") => Ok(AcpPermissionOutcome::AllowOnce),
            Some("proceed_always_project")
                if allow_project_scope && !persistent_rules.is_empty() =>
            {
                Ok(AcpPermissionOutcome::AllowAlwaysProject)
            }
            Some("proceed_always_user") if !persistent_rules.is_empty() => {
                Ok(AcpPermissionOutcome::AllowAlwaysUser)
            }
            Some("cancel") => Err(format!(
                "ACP client rejected `{}`; no changes were made.",
                call.name
            )),
            _ => Err(format!(
                "ACP client selected an unknown permission option for `{}`; no changes were made.",
                call.name
            )),
        },
        Some("cancelled") => Err(format!(
            "ACP client cancelled the permission request for `{}`; no changes were made.",
            call.name
        )),
        _ => Err(format!(
            "ACP client returned an invalid permission response for `{}`; no changes were made.",
            call.name
        )),
    }
}

fn validate_acp_mutation_dispatch(
    inner: &WorkspaceTools,
    call: &ToolCallRequestInfo,
) -> Result<(), String> {
    let configured_name = super::source_tool_name(&call.name);
    if inner.permission_decision(configured_name, None, None, &inner.workspace_root, None)
        == canopy_core::permissions::PermissionDecision::Deny
    {
        return Err(format!("{} blocked by a permissions.deny rule.", call.name));
    }
    if super::is_core_source_tool(configured_name)
        && !canopy_core::tool_utils::is_tool_enabled(
            configured_name,
            inner.core_tools.as_deref(),
            Some(&inner.excluded_tools),
        )
    {
        return Err(format!(
            "Tool `{}` is disabled by the configured tools.core/tools.exclude settings.",
            call.name
        ));
    }
    Ok(())
}

fn resolved_shell_cwd(inner: &WorkspaceTools, args: &Value) -> PathBuf {
    args.get("directory")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .map(|directory| {
            if directory.is_absolute() {
                directory
            } else {
                inner.workspace_root.join(directory)
            }
        })
        .unwrap_or_else(|| inner.workspace_root.clone())
}

fn unescape_acp_prompt_argument(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character == '\\' {
            if let Some(escaped) = characters.next() {
                output.push(escaped);
            } else {
                output.push(character);
            }
        } else {
            output.push(character);
        }
    }
    output
}

fn parse_acp_mcp_prompt_arguments(
    user_args: &str,
    prompt: &McpPrompt,
) -> Result<Map<String, Value>, String> {
    if user_args.len() > MAX_ACP_MCP_PROMPT_ARGS_BYTES {
        return Err(format!(
            "MCP prompt arguments exceed the {MAX_ACP_MCP_PROMPT_ARGS_BYTES}-byte ACP limit."
        ));
    }

    static NAMED_ARGUMENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static POSITIONAL_ARGUMENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let named_argument = NAMED_ARGUMENT.get_or_init(|| {
        regex::Regex::new(r#"--([^=]+)=(?:"((?:\\.|[^"\\])*)"|([^ ]+))"#)
            .expect("static MCP prompt named-argument regex is valid")
    });
    let positional_argument = POSITIONAL_ARGUMENT.get_or_init(|| {
        regex::Regex::new(r#"(?:"((?:\\.|[^"\\])*)"|([^ ]+))"#)
            .expect("static MCP prompt positional-argument regex is valid")
    });

    let mut named_values = HashMap::<String, String>::new();
    let mut positional_parts = Vec::new();
    let mut last_index = 0;
    for captures in named_argument.captures_iter(user_args) {
        let whole = captures
            .get(0)
            .expect("a regex capture set includes the full match");
        let key = captures
            .get(1)
            .expect("the named-argument regex captures its key")
            .as_str();
        let value = captures
            .get(2)
            .or_else(|| captures.get(3))
            .expect("the named-argument regex captures its value")
            .as_str();
        named_values.insert(key.to_owned(), unescape_acp_prompt_argument(value));
        if whole.start() > last_index {
            positional_parts.push(&user_args[last_index..whole.start()]);
        }
        last_index = whole.end();
    }
    if last_index < user_args.len() {
        positional_parts.push(&user_args[last_index..]);
    }

    let positional_text = positional_parts.join("").trim().to_owned();
    let positional_values = positional_argument
        .captures_iter(&positional_text)
        .filter_map(|captures| captures.get(1).or_else(|| captures.get(2)))
        .map(|value| unescape_acp_prompt_argument(value.as_str()))
        .collect::<Vec<_>>();

    let mut inputs = Map::new();
    let arguments = prompt
        .arguments
        .iter()
        .filter_map(|argument| {
            Some((
                argument.get("name")?.as_str()?.to_owned(),
                argument
                    .get("required")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            ))
        })
        .collect::<Vec<_>>();

    if arguments.is_empty() {
        for (name, value) in &named_values {
            inputs.insert(name.clone(), Value::String(value.clone()));
        }
        let positional_input = positional_values.join(" ");
        if !positional_input.is_empty() && !named_values.contains_key("input") {
            inputs.insert("input".to_owned(), Value::String(positional_input));
        }
        return Ok(inputs);
    }

    for (name, _) in &arguments {
        if let Some(value) = named_values.get(name) {
            inputs.insert(name.clone(), Value::String(value.clone()));
        }
    }
    let mut unfilled = arguments
        .iter()
        .filter(|argument| !inputs.contains_key(argument.0.as_str()))
        .collect::<Vec<_>>();
    unfilled.sort_by_key(|argument| !argument.1);
    if unfilled.len() == 1 && !positional_values.is_empty() {
        inputs.insert(
            unfilled[0].0.as_str().to_owned(),
            Value::String(positional_values.join(" ")),
        );
    } else {
        for (argument, value) in unfilled.iter().zip(&positional_values) {
            inputs.insert(argument.0.as_str().to_owned(), Value::String(value.clone()));
        }
    }

    let missing_required = arguments
        .iter()
        .filter(|argument| argument.1 && !inputs.contains_key(argument.0.as_str()))
        .map(|argument| format!("--{}", argument.0))
        .collect::<Vec<_>>();
    if !missing_required.is_empty() {
        return Err(format!(
            "Missing required argument(s): {}",
            missing_required.join(", ")
        ));
    }
    Ok(inputs)
}

impl AcpImageGenToolExecutor {
    fn can_offer_persistent_allow(
        &self,
        tool_name: &str,
        command: Option<&str>,
        file_path: Option<&Path>,
        cwd: &Path,
        tool_params: Option<&Value>,
    ) -> bool {
        if self
            .inner
            .permission_decision(tool_name, command, file_path, cwd, tool_params)
            != PermissionDecision::Default
        {
            return false;
        }
        !(tool_name == "run_shell_command"
            && self
                .inner
                .permissions
                .has_path_or_domain_rule(RuleType::Ask))
    }

    fn persisted_allow_matches(
        &self,
        tool_name: &str,
        command: Option<&str>,
        file_path: Option<&Path>,
        cwd: &Path,
        tool_params: Option<&Value>,
    ) -> bool {
        if !self.can_offer_persistent_allow(tool_name, command, file_path, cwd, tool_params) {
            return false;
        }
        self.persistent_allow_rules
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .evaluate(&PermissionCheckContext {
                tool_name,
                command,
                file_path,
                domain: None,
                specifier: None,
                tool_params,
                project_root: &self.inner.workspace_root,
                cwd,
            })
            == PermissionDecision::Allow
    }

    fn persist_selected_permission(
        &self,
        outcome: AcpPermissionOutcome,
        rules: &[String],
        project_scope_allowed: bool,
    ) -> Result<(), String> {
        if outcome == AcpPermissionOutcome::AllowOnce {
            return Ok(());
        }
        if rules.is_empty() {
            return Err(
                "ACP client selected an unoffered persistent permission option.".to_owned(),
            );
        }
        for rule in rules {
            persist_acp_permission_rule(
                &self.inner,
                outcome,
                rule,
                project_scope_allowed,
                self.approval_mode.workspace_trusted,
            )?;
        }
        let parsed = parse_rules(rules.iter().cloned());
        self.persistent_allow_rules
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .allow
            .extend(parsed);
        Ok(())
    }

    fn mode_auto_approves(&self, file_edit: bool) -> bool {
        match self.approval_mode.current() {
            AcpApprovalMode::Yolo => true,
            AcpApprovalMode::AutoEdit => file_edit,
            AcpApprovalMode::Plan | AcpApprovalMode::Default | AcpApprovalMode::Auto => false,
        }
    }

    fn plan_mode_allows(&self, call: &ToolCallRequestInfo) -> bool {
        if self.approval_mode.current() != AcpApprovalMode::Plan {
            return true;
        }
        matches!(
            call.name.as_str(),
            "read_file"
                | "zoom_image"
                | "list_directory"
                | "glob"
                | "grep"
                | "skill"
                | "ask_user_question"
                | "todo_write"
                | "task_list"
                | "web_fetch"
                | "web_search"
                | "run_shell_command"
        )
    }

    fn reject_plan_mode_call(&self, call: &ToolCallRequestInfo) -> Result<(), String> {
        if self.plan_mode_allows(call) {
            Ok(())
        } else {
            Err(format!(
                "Plan mode is active. The tool \"{}\" cannot be executed because it may modify the system or execute commands.",
                call.name
            ))
        }
    }

    async fn execute_skill(
        &self,
        call: &ToolCallRequestInfo,
    ) -> Result<ToolExecutionOutput, String> {
        let configured_name = super::source_tool_name(&call.name);
        if self.inner.permission_decision(
            configured_name,
            None,
            None,
            &self.inner.workspace_root,
            None,
        ) == canopy_core::permissions::PermissionDecision::Deny
        {
            return Err("skill blocked by a permissions.deny rule.".to_owned());
        }
        if !canopy_core::tool_utils::is_tool_enabled(
            "skill",
            self.inner.core_tools.as_deref(),
            Some(&self.inner.excluded_tools),
        ) {
            return Err(
                "Tool `skill` is disabled by the configured tools.core/tools.exclude settings."
                    .to_owned(),
            );
        }

        let cancellation = self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if cancellation.is_cancelled() {
            return Err("ACP prompt was cancelled before the skill could be loaded.".to_owned());
        }
        let skill_snapshot = clone_acp_skill_snapshot(&self.skill_snapshot);
        let params = canopy_core::tools::skill::SkillTool::parse_params(&call.args)?;
        let all_skills = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err("ACP prompt was cancelled while loading the skill list.".to_owned());
            }
            skills = skill_snapshot.manager.list_skills(ListSkillsOptions::default()) => skills,
        };
        let available = acp_available_skill_configs(
            skill_snapshot.manager.as_ref(),
            &all_skills,
            &skill_snapshot.disabled_skill_names,
        );
        if let Some(skill) = all_skills.iter().find(|skill| {
            skill.name == params.skill
                && skill.disable_model_invocation != Some(true)
                && !skill_snapshot
                    .disabled_skill_names
                    .contains(&skill.name.to_lowercase())
                && !skill_snapshot.manager.is_skill_active(skill)
        }) {
            return Err(format!(
                "Skill \"{}\" is gated by path-based activation (paths: frontmatter). Access a matching file first.",
                skill.name
            ));
        }
        if let Some(skill) = available.iter().find(|skill| skill.name == params.skill) {
            // Match the CLI skill loaders: declared tool grants become
            // session-scoped allow rules before the skill body reaches the
            // model. The settings permission rules remain authoritative and
            // are evaluated before these additional grants.
            self.inner
                .skills
                .apply_allowed_tools(skill.allowed_tools.as_deref());
            self.mcp_skill_grants
                .apply_allowed_tools(skill.allowed_tools.as_deref());
            let base_dir = skill
                .skill_root
                .as_deref()
                .or_else(|| skill.file_path.parent())
                .unwrap_or_else(|| Path::new("."))
                .to_string_lossy();
            let content =
                canopy_core::tools::skill::build_skill_llm_content(&base_dir, &skill.body);
            return Ok(ToolExecutionOutput::text(content));
        }
        if let Some(prompt) = self
            .mcp_prompts
            .iter()
            .find(|prompt| prompt.name == params.skill)
        {
            return self
                .execute_mcp_prompt(prompt, params.args.as_deref(), &cancellation)
                .await;
        }
        if skill_snapshot
            .disabled_skill_names
            .contains(&params.skill.trim().to_lowercase())
        {
            return Err(format!(
                "Skill \"{}\" is disabled by the configured skill settings.",
                params.skill
            ));
        }
        if let Some(error) =
            canopy_core::tools::skill::SkillTool::validate_tool_params(&params, &available)
        {
            return Err(error);
        }
        Err(format!("Skill \"{}\" not found.", params.skill))
    }

    async fn execute_mcp_prompt(
        &self,
        prompt: &McpPrompt,
        user_args: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Result<ToolExecutionOutput, String> {
        let arguments = parse_acp_mcp_prompt_arguments(user_args.unwrap_or_default(), prompt)?;
        let connection = self
            .mcp_manager
            .connection(&prompt.server_name)
            .ok_or_else(|| {
                format!(
                    "MCP server \"{}\" is no longer connected.",
                    prompt.server_name
                )
            })?;
        let client = connection.client();
        let wire_name = prompt
            .raw
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(&prompt.name);
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err("ACP prompt was cancelled while invoking the MCP prompt.".to_owned());
            }
            result = client.get_prompt(
                wire_name,
                &arguments,
                McpRequestOptions {
                    cancellation: Some(cancellation.clone()),
                    ..McpRequestOptions::default()
                },
            ) => result.map_err(|error| format!("Error invoking MCP prompt: {error}"))?,
        };
        if cancellation.is_cancelled() {
            return Err("ACP prompt was cancelled after invoking the MCP prompt.".to_owned());
        }
        let content = result
            .get("messages")
            .and_then(Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("content"));
        if content
            .and_then(|content| content.get("type"))
            .and_then(Value::as_str)
            != Some("text")
        {
            return Err("Received an empty or invalid prompt response from the server.".to_owned());
        }
        let text = content
            .and_then(|content| content.get("text"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                "Received an empty or invalid prompt response from the server.".to_owned()
            })?;
        if text.len() > MAX_ACP_MCP_PROMPT_RESULT_BYTES {
            return Err(format!(
                "MCP prompt response exceeds the {}-byte ACP limit.",
                MAX_ACP_MCP_PROMPT_RESULT_BYTES
            ));
        }
        let serialized = serde_json::to_string(text)
            .map_err(|error| format!("could not encode MCP prompt response: {error}"))?;
        Ok(ToolExecutionOutput::text(serialized))
    }

    async fn execute_approved_mutation(
        &self,
        call: &ToolCallRequestInfo,
    ) -> Result<ToolExecutionOutput, String> {
        validate_acp_mutation_dispatch(&self.inner, call)?;
        let cancellation = self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let configured_name = super::source_tool_name(&call.name);

        match call.name.as_str() {
            "edit_file" | "write_file" | "notebook_edit" => {
                let preview = match call.name.as_str() {
                    "edit_file" => self.inner.edit_file.preview(&call.args)?,
                    "write_file" => self.inner.write_file.preview(&call.args)?,
                    "notebook_edit" => self.inner.notebook_edit.preview(&call.args)?,
                    _ => unreachable!(),
                };
                let requires_confirmation = self.inner.permission_requires_confirmation(
                    configured_name,
                    None,
                    Some(&preview.path),
                    &self.inner.workspace_root,
                    Some(&call.args),
                )? && !self.persisted_allow_matches(
                    configured_name,
                    None,
                    Some(&preview.path),
                    &self.inner.workspace_root,
                    Some(&call.args),
                );
                if requires_confirmation {
                    if self.mode_auto_approves(true) {
                        let _approval = self.inner.grant_approval_once(call)?;
                        return self.inner.execute(call).await;
                    }
                    let configured_decision = self.inner.permission_decision(
                        configured_name,
                        None,
                        Some(&preview.path),
                        &self.inner.workspace_root,
                        Some(&call.args),
                    );
                    let persistent_rule = acp_path_permission_rule(
                        configured_name,
                        &preview.path,
                        &self.inner.workspace_root,
                    );
                    let persistent_rules = persistent_rule
                        .as_ref()
                        .filter(|_| configured_decision == PermissionDecision::Default)
                        .map(|(rule, _)| vec![rule.clone()])
                        .unwrap_or_default();
                    let project_scope_allowed = persistent_rule
                        .as_ref()
                        .is_some_and(|(_, project_scoped)| *project_scoped)
                        && self.approval_mode.workspace_trusted;
                    let title = match call.name.as_str() {
                        "edit_file" => format!("Confirm Edit: {}", preview.path.display()),
                        "write_file" => format!("Confirm Write: {}", preview.path.display()),
                        _ => format!("Confirm Notebook Edit: {}", preview.path.display()),
                    };
                    let preview_text = format!(
                        "Proposed change to {}:\n\n{}",
                        preview.path.display(),
                        preview.diff
                    );
                    let outcome = request_acp_tool_permission(
                        &self.output,
                        &self.session_id,
                        call,
                        &title,
                        "edit",
                        vec![acp_permission_text_content(&preview_text)],
                        Some(&preview.path),
                        &persistent_rules,
                        project_scope_allowed,
                        &cancellation,
                    )
                    .await?;
                    self.persist_selected_permission(
                        outcome,
                        &persistent_rules,
                        project_scope_allowed,
                    )?;
                    let _approval = self.inner.grant_approval_once(call)?;
                    return self.inner.execute(call).await;
                }
                self.inner.execute(call).await
            }
            "run_shell_command" => {
                let command = call
                    .args
                    .get("command")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Shell command must be a string.".to_owned())?;
                let cwd = resolved_shell_cwd(&self.inner, &call.args);
                if self.approval_mode.current() == AcpApprovalMode::Plan {
                    if cancellation.is_cancelled() {
                        return Err(
                            "ACP prompt was cancelled before Plan mode classified the shell command."
                                .to_owned(),
                        );
                    }
                    let safety = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => {
                            return Err(
                                "ACP prompt was cancelled while Plan mode classified the shell command."
                                    .to_owned(),
                            );
                        }
                        safety = canopy_core::shell_read_only::classify_shell_command_safety_in_directory(
                            command,
                            &cwd,
                        ) => safety,
                    };
                    if cancellation.is_cancelled() {
                        return Err(
                            "ACP prompt was cancelled after Plan mode classified the shell command."
                                .to_owned(),
                        );
                    }
                    if self.approval_mode.current() == AcpApprovalMode::Plan
                        && safety != canopy_core::shell_read_only::ShellCommandSafety::ReadOnly
                    {
                        return Err(match safety {
                            canopy_core::shell_read_only::ShellCommandSafety::Write => {
                                "Plan mode blocked this shell command because it was classified as state-modifying. Do not retry it through wrappers or obfuscation; continue read-only investigation and include the action in the plan.".to_owned()
                            }
                            canopy_core::shell_read_only::ShellCommandSafety::Unknown => {
                                "Plan mode blocked this shell command because its safety could not be determined. Only commands classified as read-only can run while Plan mode is active.".to_owned()
                            }
                            canopy_core::shell_read_only::ShellCommandSafety::ReadOnly => {
                                unreachable!("read-only shell commands pass the Plan mode gate")
                            }
                        });
                    }
                }
                let requires_confirmation = self.inner.permission_requires_confirmation(
                    configured_name,
                    Some(command),
                    None,
                    &cwd,
                    Some(&call.args),
                )? && !self.persisted_allow_matches(
                    configured_name,
                    Some(command),
                    None,
                    &cwd,
                    Some(&call.args),
                );
                if requires_confirmation {
                    if self.mode_auto_approves(false) {
                        let _approval = self.inner.grant_approval_once(call)?;
                        return self.inner.execute(call).await;
                    }
                    let persistent_rule = acp_shell_permission_rule(command);
                    let persistent_rules = persistent_rule
                        .filter(|_| {
                            self.can_offer_persistent_allow(
                                configured_name,
                                Some(command),
                                None,
                                &cwd,
                                Some(&call.args),
                            )
                        })
                        .into_iter()
                        .collect::<Vec<_>>();
                    let project_scope_allowed = self.approval_mode.workspace_trusted;
                    let mut preview = format!(
                        "Command to run:\n{command}\n\nWorking directory: {}",
                        cwd.display()
                    );
                    if let Some(description) = call
                        .args
                        .get("description")
                        .and_then(Value::as_str)
                        .filter(|description| !description.trim().is_empty())
                    {
                        preview.push_str("\n\nPurpose: ");
                        preview.push_str(&description.replace('\n', " "));
                    }
                    preview.push_str("\n\nAllow once applies only to this command invocation.");
                    let outcome = request_acp_tool_permission(
                        &self.output,
                        &self.session_id,
                        call,
                        "Confirm shell command",
                        "execute",
                        vec![acp_permission_text_content(&preview)],
                        None,
                        &persistent_rules,
                        project_scope_allowed,
                        &cancellation,
                    )
                    .await?;
                    self.persist_selected_permission(
                        outcome,
                        &persistent_rules,
                        project_scope_allowed,
                    )?;
                    let _approval = self.inner.grant_approval_once(call)?;
                    return self.inner.execute(call).await;
                }
                self.inner.execute(call).await
            }
            "artifact" => {
                let params = canopy_core::tools::artifact::ArtifactTool::parse_params(&call.args)?;
                let artifact = self.inner.artifact.as_ref().ok_or_else(|| {
                    "Artifact publishing is not enabled for this ACP session.".to_owned()
                })?;
                let requires_confirmation = self.inner.permission_requires_confirmation(
                    configured_name,
                    None,
                    Some(&params.file_path),
                    &self.inner.workspace_root,
                    Some(&call.args),
                )?;
                if requires_confirmation
                    && !self.mode_auto_approves(false)
                    && !self.persisted_allow_matches(
                        configured_name,
                        None,
                        Some(&params.file_path),
                        &self.inner.workspace_root,
                        Some(&call.args),
                    )
                {
                    let persistent_rules = if self.can_offer_persistent_allow(
                        configured_name,
                        None,
                        Some(&params.file_path),
                        &self.inner.workspace_root,
                        Some(&call.args),
                    ) {
                        vec![configured_name.to_owned()]
                    } else {
                        Vec::new()
                    };
                    let project_scope_allowed = self.approval_mode.workspace_trusted;
                    let outcome = request_acp_tool_permission(
                        &self.output,
                        &self.session_id,
                        call,
                        "Publish artifact",
                        "other",
                        vec![acp_permission_text_content(
                            &artifact.confirmation_prompt(&params.file_path),
                        )],
                        Some(&params.file_path),
                        &persistent_rules,
                        project_scope_allowed,
                        &cancellation,
                    )
                    .await?;
                    self.persist_selected_permission(
                        outcome,
                        &persistent_rules,
                        project_scope_allowed,
                    )?;
                }
                artifact.execute(&params, &cancellation).await
            }
            _ => Err(format!(
                "No ACP approval handler is registered for `{}`.",
                call.name
            )),
        }
    }

    async fn execute_zoom_image(
        &self,
        call: &ToolCallRequestInfo,
    ) -> Result<ToolExecutionOutput, String> {
        let (requested_path, _) = canopy_core::tools::image_view::parse_params(&call.args)?;
        let requested_path = PathBuf::from(requested_path);
        let configured_name = super::source_tool_name(&call.name);
        let decision = self.inner.permission_decision(
            configured_name,
            None,
            Some(&requested_path),
            &self.inner.workspace_root,
            Some(&call.args),
        );
        match decision {
            canopy_core::permissions::PermissionDecision::Deny => {
                return Err("zoom_image blocked by a permissions.deny rule.".to_owned());
            }
            canopy_core::permissions::PermissionDecision::Ask => {
                if self.mode_auto_approves(false) {
                    let _approval = self.inner.grant_approval_once(call)?;
                    return self.inner.execute(call).await;
                }
                let cancellation = self
                    .cancellation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let preview = format!(
                    "Canopy will read this image from the workspace:\n{}",
                    requested_path.display()
                );
                let _outcome = request_acp_tool_permission(
                    &self.output,
                    &self.session_id,
                    call,
                    &format!("Confirm Read: {}", requested_path.display()),
                    "read",
                    vec![acp_permission_text_content(&preview)],
                    Some(&requested_path),
                    &[],
                    false,
                    &cancellation,
                )
                .await?;
                let _approval = self.inner.grant_approval_once(call)?;
                return self.inner.execute(call).await;
            }
            canopy_core::permissions::PermissionDecision::Allow
            | canopy_core::permissions::PermissionDecision::Default => {}
        }
        self.inner.execute(call).await
    }
}

impl AgentToolExecutor for AcpImageGenToolExecutor {
    fn cancellation_token_for_current_prompt(&self) -> Option<CancellationToken> {
        Some(
            self.cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    fn execute<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
    ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>> {
        if let Err(error) = self.reject_plan_mode_call(call) {
            return Box::pin(async move { Err(error) });
        }

        if call.name == "skill" {
            return Box::pin(self.execute_skill(call));
        }

        if call.name == "record_artifact" {
            return Box::pin(async move {
                let configured_name = super::source_tool_name(&call.name);
                if self.inner.permission_decision(
                    configured_name,
                    None,
                    None,
                    &self.inner.workspace_root,
                    Some(&call.args),
                ) == canopy_core::permissions::PermissionDecision::Deny
                {
                    return Err("record_artifact blocked by a permissions.deny rule.".to_owned());
                }
                if super::is_core_source_tool(configured_name)
                    && !canopy_core::tool_utils::is_tool_enabled(
                        configured_name,
                        self.inner.core_tools.as_deref(),
                        Some(&self.inner.excluded_tools),
                    )
                {
                    return Err(
                        "Tool `record_artifact` is disabled by the configured tools.core/tools.exclude settings."
                            .to_owned(),
                    );
                }
                canopy_core::tools::record_artifact::RecordArtifactTool::new(
                    self.inner.workspace_root.clone(),
                )
                .execute(&call.args)
            });
        }

        if call.name == "ask_user_question" {
            return Box::pin(async move {
                let configured_name = super::source_tool_name(&call.name);
                if self.inner.permission_decision(
                    configured_name,
                    None,
                    None,
                    &self.inner.workspace_root,
                    None,
                ) == canopy_core::permissions::PermissionDecision::Deny
                {
                    return Err("ask_user_question blocked by a permissions.deny rule.".to_owned());
                }
                if super::is_core_source_tool(configured_name)
                    && !canopy_core::tool_utils::is_tool_enabled(
                        configured_name,
                        self.inner.core_tools.as_deref(),
                        Some(&self.inner.excluded_tools),
                    )
                {
                    return Err(
                        "Tool `ask_user_question` is disabled by the configured tools.core/tools.exclude settings."
                            .to_owned(),
                    );
                }
                let cancellation = self
                    .cancellation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                execute_acp_user_question(&self.output, &self.session_id, call, &cancellation).await
            });
        }

        if call.name == "zoom_image" {
            return Box::pin(self.execute_zoom_image(call));
        }

        if matches!(
            call.name.as_str(),
            "edit_file" | "write_file" | "notebook_edit" | "run_shell_command" | "artifact"
        ) {
            return Box::pin(self.execute_approved_mutation(call));
        }

        if call.name != "image_gen" {
            return self.inner.execute(call);
        }

        Box::pin(async move {
            let params = canopy_core::tools::image_gen::ImageGenTool::parse_params(&call.args)?;
            let configured_name = super::source_tool_name(&call.name);
            if self.inner.permission_decision(
                configured_name,
                None,
                None,
                &self.inner.workspace_root,
                Some(&call.args),
            ) == canopy_core::permissions::PermissionDecision::Deny
            {
                return Err("image_gen blocked by a permissions.deny rule.".to_owned());
            }
            if super::is_core_source_tool(configured_name)
                && !canopy_core::tool_utils::is_tool_enabled(
                    configured_name,
                    self.inner.core_tools.as_deref(),
                    Some(&self.inner.excluded_tools),
                )
            {
                return Err(
                    "Tool `image_gen` is disabled by the configured tools.core/tools.exclude settings."
                        .to_owned(),
                );
            }

            let requires_approval = self.inner.permission_requires_confirmation(
                configured_name,
                None,
                None,
                &self.inner.workspace_root,
                Some(&call.args),
            )? && !self.persisted_allow_matches(
                configured_name,
                None,
                None,
                &self.inner.workspace_root,
                Some(&call.args),
            );
            let cancellation = self
                .cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if requires_approval && !self.mode_auto_approves(false) {
                let image_gen = self
                    .inner
                    .image_gen
                    .as_ref()
                    .ok_or_else(|| "Image generation is not configured.".to_owned())?;
                let mut preview = format!("Image model: {}", image_gen.model_name());
                if let Some(size) = params.size.as_deref() {
                    preview.push_str(&format!("\nOutput size: {size}"));
                }
                preview.push_str("\n\nPrompt:\n");
                preview.push_str(&params.prompt);
                preview.push_str("\n\nAllow once applies only to this image generation request.");
                let persistent_rules = if self.can_offer_persistent_allow(
                    configured_name,
                    None,
                    None,
                    &self.inner.workspace_root,
                    Some(&call.args),
                ) {
                    vec![configured_name.to_owned()]
                } else {
                    Vec::new()
                };
                let project_scope_allowed = self.approval_mode.workspace_trusted;
                let outcome = request_acp_tool_permission(
                    &self.output,
                    &self.session_id,
                    call,
                    "Generate image",
                    "other",
                    vec![acp_permission_text_content(&preview)],
                    None,
                    &persistent_rules,
                    project_scope_allowed,
                    &cancellation,
                )
                .await?;
                self.persist_selected_permission(
                    outcome,
                    &persistent_rules,
                    project_scope_allowed,
                )?;
            }

            let image_gen = self
                .inner
                .image_gen
                .as_ref()
                .ok_or_else(|| "Image generation is not configured.".to_owned())?;
            image_gen.execute(&params, &cancellation).await
        })
    }

    fn is_side_effecting(&self, tool_name: &str) -> bool {
        self.inner.is_side_effecting(tool_name)
    }

    fn is_concurrency_safe(&self, call: &ToolCallRequestInfo) -> bool {
        self.inner.is_concurrency_safe(call)
    }

    fn additional_context_after_tool_use<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        result_file_paths: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        Box::pin(async move {
            let mut context = self
                .inner
                .additional_context_after_tool_use(call, result_file_paths)
                .await
                .unwrap_or_default();
            let path_tool_name = super::source_tool_name(&call.name);
            if !canopy_core::utils::tool_file_paths::is_filesystem_path_tool(path_tool_name) {
                return (!context.is_empty()).then_some(context);
            }

            let mut seen_paths = HashSet::new();
            let mut paths = Vec::new();
            for path in canopy_core::utils::tool_file_paths::extract_tool_file_paths(
                path_tool_name,
                &call.args,
            ) {
                if seen_paths.insert(path.clone()) {
                    paths.push(PathBuf::from(path));
                }
            }
            for path in result_file_paths {
                if seen_paths.insert(path.clone()) {
                    paths.push(PathBuf::from(path));
                }
            }
            if paths.is_empty() {
                return (!context.is_empty()).then_some(context);
            }

            let cancellation = self
                .cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if cancellation.is_cancelled() {
                return (!context.is_empty()).then_some(context);
            }
            let skill_snapshot = clone_acp_skill_snapshot(&self.skill_snapshot);
            let newly_activated = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return (!context.is_empty()).then_some(context);
                }
                names = skill_snapshot.manager.match_and_activate_by_paths(&paths) => names,
            };
            if newly_activated.is_empty() {
                return (!context.is_empty()).then_some(context);
            }
            let all_skills = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return (!context.is_empty()).then_some(context);
                }
                skills = skill_snapshot.manager.list_skills(ListSkillsOptions::default()) => skills,
            };
            let activated = all_skills
                .iter()
                .filter(|skill| {
                    newly_activated.iter().any(|name| name == &skill.name)
                        && skill.disable_model_invocation != Some(true)
                        && !skill_snapshot
                            .disabled_skill_names
                            .contains(&skill.name.to_lowercase())
                        && skill_snapshot.manager.is_skill_active(skill)
                })
                .cloned()
                .collect::<Vec<_>>();
            if !activated.is_empty() {
                let reminder = format!(
                    "The following skill(s) became available based on the file you just accessed; invoke a skill by passing its name to the Skill tool:\n<available_skills>\n{}\n</available_skills>",
                    render_acp_available_skills_block(&activated)
                );
                if !context.is_empty() {
                    context.push_str("\n\n");
                }
                context.push_str(&reminder);
            }
            (!context.is_empty()).then_some(context)
        })
    }

    fn begin_user_prompt<'a>(
        &'a self,
        prompt_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        self.inner.begin_user_prompt(prompt_id)
    }

    fn take_file_history_snapshot_updates(&self) -> Vec<FileHistorySnapshot> {
        self.inner.take_file_history_snapshot_updates()
    }

    fn commit_attribution_snapshot(&self) -> Option<AttributionSnapshot> {
        self.inner.commit_attribution_snapshot()
    }
}

struct CliAcpPromptState {
    recorder: SessionRecorder,
    executor: canopy_core::tools::computer_use::ComputerUseComposedToolExecutor<
        canopy_core::tools::mcp::agent_tool_adapter::McpComposedToolExecutor<
            AcpImageGenToolExecutor,
        >,
    >,
    store: SessionStore,
    history: Vec<Value>,
}

struct CliAcpAgent {
    session_id: String,
    provider_kind: AcpProviderKind,
    input_modalities: canopy_core::providers::openai_request::InputModalities,
    workspace_root: PathBuf,
    runtime_base_dir: PathBuf,
    effective_env: HashMap<String, String>,
    active_extensions: Vec<LocalExtensionReference>,
    mcp_reference_server_names: Vec<String>,
    mcp_prompts: Vec<Arc<McpPrompt>>,
    skill_snapshot: Arc<RwLock<AcpSkillSnapshot>>,
    memory_recall_selector:
        Option<super::auto_memory_recall_selector::OpenAiAutoMemoryRecallSelector>,
    base_system_instruction: Option<String>,
    runtime: Arc<AgentRuntime>,
    state: AsyncMutex<CliAcpPromptState>,
    mcp_session: McpCliSession,
    output: ProtocolOutput,
    cancel_generation: watch::Sender<u64>,
    tool_cancellation: Arc<Mutex<CancellationToken>>,
    _memory_pressure_task: Option<super::memory_pressure::MemoryPressureTask>,
    closed: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct ResolvedAcpPromptReferences {
    parts: Vec<Value>,
    diagnostics: Vec<String>,
}

impl CliAcpAgent {
    fn reload_skill_settings(&self) -> Result<(), String> {
        self.skill_snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reload_workspace_settings()?;
        Ok(())
    }

    async fn publish_available_commands(&self) -> Result<(), String> {
        let snapshot = clone_acp_skill_snapshot(&self.skill_snapshot);
        let update = acp_available_commands_update(&snapshot, &self.mcp_prompts).await;
        self.output.notification(
            "session/update",
            json!({"sessionId":self.session_id,"update":update}),
        )
    }

    async fn publish_skill_refresh(&self, notify_config_changed: bool) -> Result<(), String> {
        let snapshot = clone_acp_skill_snapshot(&self.skill_snapshot);
        let update_result = self
            .publish_available_commands()
            .await
            .map_err(|error| format!("could not send ACP skill update: {error}"));
        if notify_config_changed {
            snapshot.manager.suppress_next_slash_reload();
            snapshot.manager.notify_config_changed().await;
        }
        update_result
    }

    async fn resolve_prompt_references(
        &self,
        content: &[Value],
        cancelled: &mut watch::Receiver<bool>,
    ) -> Option<ResolvedAcpPromptReferences> {
        let (references, omitted_count) =
            acp_prompt_reference_tokens(content, &self.mcp_reference_server_names);
        if references.is_empty() && omitted_count == 0 {
            return Some(ResolvedAcpPromptReferences::default());
        }

        let (resources, prompts) = self.mcp_session.reference_registries();
        let cancellation = CancellationToken::new();
        let mut resolver = AtResourceReferenceResolver::new(
            &self.active_extensions,
            &self.mcp_reference_server_names,
            &resources,
            &prompts,
            self.mcp_session.manager().as_ref(),
            canopy_core::tools::mcp::client_runtime::McpRequestOptions {
                timeout_ms: None,
                cancellation: Some(cancellation.clone()),
                ..canopy_core::tools::mcp::client_runtime::McpRequestOptions::default()
            },
        );
        let mut resolved = ResolvedAcpPromptReferences::default();

        for reference in references {
            if *cancelled.borrow() {
                cancellation.cancel();
                return None;
            }
            let result = tokio::select! {
                biased;
                _ = wait_for_acp_prompt_cancellation(cancelled) => {
                    cancellation.cancel();
                    return None;
                }
                result = resolver.resolve(&reference) => result,
            };
            if *cancelled.borrow() {
                cancellation.cancel();
                return None;
            }
            let Some(result) = result else {
                continue;
            };
            resolved.diagnostics.extend(
                result
                    .diagnostics
                    .into_iter()
                    .map(|diagnostic| format!("@{reference}: {diagnostic}")),
            );
            resolved.parts.extend(result.parts);
        }

        if omitted_count > 0 {
            resolved.diagnostics.push(format!(
                "Skipped {omitted_count} additional prompt reference(s); this prompt is limited to {MAX_ACP_PROMPT_REFERENCES}."
            ));
        }
        Some(resolved)
    }

    fn emit_reference_diagnostics(&self, diagnostics: &[String]) {
        for diagnostic in diagnostics.iter().take(MAX_ACP_REFERENCE_DIAGNOSTICS) {
            let diagnostic = bound_acp_reference_diagnostic(diagnostic);
            if diagnostic.is_empty() {
                continue;
            }
            let _ = self.output.notification(
                "session/update",
                json!({
                    "sessionId":self.session_id,
                    "update":{
                        "sessionUpdate":"agent_thought_chunk",
                        "content":{"type":"text","text":format!("Prompt reference: {diagnostic}")}
                    }
                }),
            );
        }
        if diagnostics.len() > MAX_ACP_REFERENCE_DIAGNOSTICS {
            let omitted = diagnostics.len() - MAX_ACP_REFERENCE_DIAGNOSTICS;
            let _ = self.output.notification(
                "session/update",
                json!({
                    "sessionId":self.session_id,
                    "update":{
                        "sessionUpdate":"agent_thought_chunk",
                        "content":{"type":"text","text":format!("Prompt reference diagnostics: {omitted} additional message(s) omitted.")}
                    }
                }),
            );
        }
    }
}

impl BridgeSessionAgent for CliAcpAgent {
    fn prompt<'a>(
        &'a self,
        prompt: Value,
        mut cancelled: watch::Receiver<bool>,
    ) -> SessionRuntimeFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return Err("session is closed".to_owned());
            }
            let content = match prompt {
                Value::Array(content) => content,
                _ => return Err("prompt must be an array".to_owned()),
            };
            let text = prompt_text(&content).map_err(|error| error.message)?;
            if !validate_acp_audio_blocks(
                &content,
                self.provider_kind,
                self.input_modalities,
                &cancelled,
            )
            .map_err(|error| error.message)?
                || *cancelled.borrow()
            {
                return Ok(json!({"stopReason":"cancelled"}));
            }
            if let Some(args) = acp_doctor_memory_slash_command(&content, &text) {
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                let result = if args.len() > MAX_ACP_MEMORY_COMMAND_BYTES {
                    Err(format!(
                        "Memory command exceeds the {MAX_ACP_MEMORY_COMMAND_BYTES}-byte limit."
                    ))
                } else {
                    tokio::select! {
                        result = super::memory_diagnostics_command::run(args) => result,
                        _ = wait_for_acp_prompt_cancellation(&mut cancelled) => {
                            return Ok(json!({"stopReason":"cancelled"}));
                        }
                    }
                };
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                let is_error = result.is_err();
                let result =
                    result.unwrap_or_else(|error| format!("Memory diagnostics failed: {error}"));
                let result = bound_acp_output(
                    &result,
                    MAX_ACP_STATS_OUTPUT_BYTES,
                    "\n[Memory diagnostics output truncated.]",
                );
                let display = if is_error {
                    format!("Memory diagnostics error: {result}")
                } else {
                    result
                };
                self.output
                    .notification(
                        "session/update",
                        json!({
                            "sessionId":self.session_id,
                            "update":{
                                "sessionUpdate":"agent_message_chunk",
                                "content":{"type":"text","text":display}
                            }
                        }),
                    )
                    .map_err(|error| {
                        format!("could not send ACP memory diagnostics response: {error}")
                    })?;
                return Ok(json!({"stopReason":"end_turn"}));
            }
            if acp_doctor_rollback_slash_command(&content, &text) {
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                self.output
                    .notification(
                        "session/update",
                        json!({
                            "sessionId":self.session_id,
                            "update":{
                                "sessionUpdate":"agent_message_chunk",
                                "content":{"type":"text","text":"Rollback is not available in ACP mode."}
                            }
                        }),
                    )
                    .map_err(|error| {
                        format!("could not send ACP doctor rollback response: {error}")
                    })?;
                return Ok(json!({"stopReason":"end_turn"}));
            }
            if let Some(args) = acp_stats_slash_command(&content, &text) {
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                let (result, is_error) = if args.len() > MAX_ACP_STATS_COMMAND_BYTES {
                    (
                        format!(
                            "Stats command exceeds the {MAX_ACP_STATS_COMMAND_BYTES}-byte limit."
                        ),
                        true,
                    )
                } else {
                    let session_metrics = self.runtime.session_metrics_snapshot(&self.session_id);
                    if args.is_empty() {
                        (acp_session_stats_summary(session_metrics.as_ref()), false)
                    } else {
                        let is_export = args.split_whitespace().next() == Some("export");
                        match super::stats_command::execute_with_session_metrics(
                            &self.runtime_base_dir,
                            &self.workspace_root,
                            args,
                            session_metrics.as_ref(),
                        ) {
                            Ok(content) => (content, false),
                            Err(error) => (
                                format!(
                                    "Failed to {} token usage stats: {error}",
                                    if is_export { "export" } else { "load" }
                                ),
                                true,
                            ),
                        }
                    }
                };
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                let result = bound_acp_stats_output(&result);
                let display = if is_error {
                    format!("Stats command error: {result}")
                } else {
                    result
                };
                self.output
                    .notification(
                        "session/update",
                        json!({
                            "sessionId":self.session_id,
                            "update":{
                                "sessionUpdate":"agent_message_chunk",
                                "content":{"type":"text","text":display}
                            }
                        }),
                    )
                    .map_err(|error| format!("could not send ACP stats response: {error}"))?;
                return Ok(json!({"stopReason":"end_turn"}));
            }
            let Some(references) = self
                .resolve_prompt_references(&content, &mut cancelled)
                .await
            else {
                return Ok(json!({"stopReason":"cancelled"}));
            };
            self.emit_reference_diagnostics(&references.diagnostics);
            let mut prompt_parts = vec![json!({"text":text})];
            for block in content {
                if *cancelled.borrow() {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                let Value::Object(mut block) = block else {
                    continue;
                };
                let content_type = block.get("type").and_then(Value::as_str);
                if !matches!(content_type, Some("image" | "audio")) {
                    continue;
                }
                let mime_type = match block.remove("mimeType") {
                    Some(Value::String(mime_type)) => mime_type,
                    _ => continue,
                };
                let data = match block.remove("data") {
                    Some(Value::String(data)) => data,
                    _ => continue,
                };
                let mut inline_data = Map::with_capacity(2);
                inline_data.insert("mimeType".to_owned(), Value::String(mime_type));
                inline_data.insert("data".to_owned(), Value::String(data));
                let mut media_part = Map::with_capacity(1);
                media_part.insert("inlineData".to_owned(), Value::Object(inline_data));
                prompt_parts.push(Value::Object(media_part));
            }
            prompt_parts.extend(references.parts);
            let has_reference_context = prompt_parts.len() > 1;
            let recent_tools = {
                let state = self.state.lock().await;
                super::recent_memory_tool_names(&state.history)
            };
            let memory_cancellation = CancellationToken::new();
            let memory_prompt = tokio::select! {
                biased;
                _ = async {
                    loop {
                        if *cancelled.borrow() {
                            break;
                        }
                        if cancelled.changed().await.is_err() {
                            break;
                        }
                    }
                } => {
                    memory_cancellation.cancel();
                    String::new()
                }
                prompt = super::recalled_memory_prompt(
                    &text,
                    &self.workspace_root,
                    &self.runtime_base_dir,
                    &self.effective_env,
                    &recent_tools,
                    self.memory_recall_selector.as_ref().map(|selector| selector as &dyn canopy_core::memory::AutoMemoryRecallSelector),
                    Some(&memory_cancellation),
                ) => prompt,
            };
            if *cancelled.borrow() {
                return Ok(json!({"stopReason":"cancelled"}));
            }
            let skill_snapshot = clone_acp_skill_snapshot(&self.skill_snapshot);
            let skill_prompt = tokio::select! {
                biased;
                _ = async {
                    loop {
                        if *cancelled.borrow() {
                            break;
                        }
                        if cancelled.changed().await.is_err() {
                            break;
                        }
                    }
                } => {
                    return Ok(json!({"stopReason":"cancelled"}));
                }
                prompt = acp_skill_system_reminder(
                    skill_snapshot.manager.as_ref(),
                    &skill_snapshot.disabled_skill_names,
                    &self.mcp_prompts,
                ) => prompt,
            };
            if *cancelled.borrow() {
                return Ok(json!({"stopReason":"cancelled"}));
            }
            let system_instruction = super::build_system_instruction(
                self.base_system_instruction.as_deref(),
                &memory_prompt,
                &skill_prompt,
            );
            let mut state = self.state.lock().await;
            if *cancelled.borrow() {
                return Ok(json!({"stopReason":"cancelled"}));
            }
            let mut cancel_rx = self.cancel_generation.subscribe();
            let initial_generation = { *cancel_rx.borrow() };
            let prompt_cancellation = CancellationToken::new();
            *self
                .tool_cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = prompt_cancellation.clone();
            let history = state.history.clone();
            let runtime = self.runtime.clone();
            let output = self.output.clone();
            let session_id = self.session_id.clone();
            let result = {
                let CliAcpPromptState {
                    recorder, executor, ..
                } = &mut *state;
                let emit = |event| emit_agent_event(&output, &session_id, event);
                let run = async {
                    if has_reference_context {
                        runtime
                            .run_prompt_with_parts_and_history_and_system_instruction(
                                &text,
                                prompt_parts,
                                history,
                                system_instruction,
                                recorder,
                                executor,
                                emit,
                            )
                            .await
                    } else {
                        runtime
                            .run_prompt_with_history_and_system_instruction(
                                &text,
                                history,
                                system_instruction,
                                recorder,
                                executor,
                                emit,
                            )
                            .await
                    }
                };
                tokio::pin!(run);
                tokio::select! {
                    biased;
                    changed = cancelled.changed() => {
                        let _ = changed;
                        prompt_cancellation.cancel();
                        None
                    }
                    changed = cancel_rx.changed() => {
                        let _ = changed;
                        let cancelled_by_agent = *cancel_rx.borrow() != initial_generation;
                        if cancelled_by_agent {
                            prompt_cancellation.cancel();
                            None
                        } else {
                            Some(run.await)
                        }
                    }
                    result = &mut run => Some(result),
                }
            };
            if result.is_none() {
                let store = state.store.clone();
                match rebuild_session_history(&store, &self.session_id, &mut state.recorder) {
                    Ok(history) => state.history = history,
                    Err(error) => eprintln!(
                        "canopy --acp: could not refresh cancelled session history: {error}"
                    ),
                }
                return Ok(json!({"stopReason":"cancelled"}));
            }
            let store = state.store.clone();
            match rebuild_session_history(&store, &self.session_id, &mut state.recorder) {
                Ok(history) => state.history = history,
                Err(error) => return Err(format!("could not refresh session history: {error}")),
            }
            let summary = result
                .expect("result checked above")
                .map_err(|error| error.to_string())?;
            if *cancelled.borrow()
                || *cancel_rx.borrow() != initial_generation
                || self.closed.load(Ordering::Acquire)
            {
                return Ok(json!({"stopReason":"cancelled"}));
            }
            let stop_reason = match summary.finish_reason.as_deref() {
                Some("LENGTH") | Some("length") | Some("max_tokens") => "max_tokens",
                _ => "end_turn",
            };
            Ok(json!({"stopReason":stop_reason}))
        })
    }

    fn cancel<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.cancel_generation
                .send_modify(|generation| *generation = generation.wrapping_add(1));
            Ok(())
        })
    }

    fn shutdown<'a>(&'a self) -> SessionRuntimeFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.closed.swap(true, Ordering::AcqRel) {
                return Ok(());
            }
            self.cancel_generation
                .send_modify(|generation| *generation = generation.wrapping_add(1));
            let mut state = self.state.lock().await;
            let result = state
                .recorder
                .close()
                .map_err(|error| format!("could not close session transcript: {error}"));
            self.mcp_session.stop();
            result
        })
    }
}

#[derive(Clone, Debug)]
struct MetadataCursorKey {
    activity_time: f64,
    session_id: String,
}

#[derive(Clone, Debug)]
struct OrganizedSessionCursorKey {
    is_pinned: bool,
    activity_time: f64,
    session_id: String,
}

fn safe_integer(value: &Value) -> Option<i64> {
    let number = value.as_f64()?;
    (number.is_finite() && number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0)
        .then_some(number as i64)
}

fn parse_archive_state(params: &Value) -> Result<SessionArchiveState, RpcError> {
    let meta = params.get("_meta");
    let raw = params
        .get("archiveState")
        .and_then(Value::as_str)
        .or_else(|| {
            meta.and_then(|meta| meta.get("archiveState"))
                .and_then(Value::as_str)
        });
    match raw {
        None | Some("active") => Ok(SessionArchiveState::Active),
        Some("archived") => Ok(SessionArchiveState::Archived),
        Some(_) => Err(RpcError::invalid_params(
            "`archiveState` must be \"active\" or \"archived\"",
        )),
    }
}

fn parse_list_source(params: &Value) -> Result<(Option<String>, Option<String>), RpcError> {
    let source_type_value = params.get("sourceType");
    let source_id_value = params.get("sourceId");
    if source_type_value.is_none() && source_id_value.is_none() {
        return Ok((None, None));
    }
    let source_type = source_type_value.and_then(Value::as_str).ok_or_else(|| {
        RpcError::invalid_params("`sourceType` must match [a-z][a-z0-9_-]{0,63} when provided")
    })?;
    let valid_type = !source_type.is_empty()
        && source_type.len() <= 64
        && source_type.as_bytes()[0].is_ascii_lowercase()
        && source_type.as_bytes()[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        });
    if !valid_type {
        return Err(RpcError::invalid_params(
            "`sourceType` must match [a-z][a-z0-9_-]{0,63} when provided",
        ));
    }
    let source_id = match source_id_value {
        None => None,
        Some(value) => {
            let source_id = value.as_str().ok_or_else(|| {
                RpcError::invalid_params(
                    "`sourceId` must be a non-empty string of at most 256 characters without control characters",
                )
            })?;
            if source_id.is_empty()
                || source_id.chars().count() > 256
                || source_id.chars().any(char::is_control)
            {
                return Err(RpcError::invalid_params(
                    "`sourceId` must be a non-empty string of at most 256 characters without control characters",
                ));
            }
            Some(source_id.to_owned())
        }
    };
    Ok((Some(source_type.to_owned()), source_id))
}

fn parse_numeric_session_cursor(cursor: &str) -> Result<f64, RpcError> {
    let trimmed = cursor.trim();
    if trimmed.is_empty() {
        return Err(RpcError::invalid_params(format!(
            "Invalid cursor: {cursor:?} is not a valid numeric cursor"
        )));
    }
    let value = trimmed.parse::<f64>().map_err(|_| {
        RpcError::invalid_params(format!(
            "Invalid cursor: {cursor:?} is not a valid numeric cursor"
        ))
    })?;
    if !value.is_finite() || value < 0.0 || value > 9_007_199_254_740_991.0 {
        return Err(RpcError::invalid_params(format!(
            "Invalid cursor: {cursor:?} is not a valid numeric cursor"
        )));
    }
    Ok(value)
}

fn scan_persisted_sessions(
    paths: &SessionPaths,
    archive_state: SessionArchiveState,
) -> Result<(Vec<SessionListItem>, bool), RpcError> {
    let chats_directory = paths.chats_directory(archive_state, cfg!(windows));
    let directory = match std::fs::read_dir(&chats_directory) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), false)),
        Err(error) => {
            return Err(RpcError::internal(format!(
                "could not list sessions: {error}"
            )));
        }
    };
    let mut files = Vec::new();
    for entry in directory {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(session_id) = file_name.strip_suffix(".jsonl") else {
            continue;
        };
        if !is_session_filename_id(session_id) {
            continue;
        }
        if !entry.file_type().is_ok_and(|file_type| file_type.is_file()) {
            continue;
        }
        let metadata = match std::fs::symlink_metadata(entry.path()) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => metadata,
            _ => continue,
        };
        let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        let mtime_ms = modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as f64)
            .unwrap_or_default();
        files.push((session_id.to_owned(), entry.path(), mtime_ms));
    }
    files.sort_by(|a, b| b.2.total_cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    let truncated = files.len() > MAX_SESSION_FILES_TO_SCAN;
    files.truncate(MAX_SESSION_FILES_TO_SCAN);

    let mut sessions = Vec::new();
    for (session_id, path, mtime_ms) in files {
        if let Some(session) =
            read_persisted_session(paths, &session_id, &path, mtime_ms, archive_state)?
        {
            sessions.push(session);
        }
    }
    sessions.sort_by(compare_activity);
    Ok((sessions, truncated))
}

fn is_session_filename_id(session_id: &str) -> bool {
    (32..=36).contains(&session_id.len())
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
}

fn read_persisted_session(
    paths: &SessionPaths,
    session_id: &str,
    path: &Path,
    mtime_ms: f64,
    archive_state: SessionArchiveState,
) -> Result<Option<SessionListItem>, RpcError> {
    let records = canopy_core::jsonl::read_lines(path, MAX_PROMPT_SCAN_LINES)
        .map_err(|error| RpcError::internal(format!("could not read session summary: {error}")))?;
    let Some(first_record) = records.first() else {
        return Ok(None);
    };
    if first_record.get("sessionId").and_then(Value::as_str) != Some(session_id) {
        return Ok(None);
    }
    let Some(cwd) = first_record.get("cwd").and_then(Value::as_str) else {
        return Ok(None);
    };
    if !session_belongs_to_workspace(paths, session_id, cwd) {
        return Ok(None);
    }
    let created_at = first_record
        .get("timestamp")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let (parent_session_id, source_type, source_id) = extract_session_metadata(&records);
    let prompt = first_prompt(&records);
    let title = read_last_custom_title(path).filter(|title| !title.is_empty());
    let worktree = read_worktree_sidecar(paths, session_id, archive_state, cwd);
    Ok(Some(SessionListItem {
        session_id: session_id.to_owned(),
        cwd: cwd.to_owned(),
        created_at,
        updated_at: format_epoch_millis(mtime_ms as u128),
        activity_time: mtime_ms,
        display_name: title.or_else(|| (!prompt.is_empty()).then_some(prompt)),
        parent_session_id,
        source_type,
        source_id,
        client_count: 0,
        has_active_prompt: false,
        is_archived: archive_state == SessionArchiveState::Archived,
        worktree,
    }))
}

fn session_belongs_to_workspace(paths: &SessionPaths, session_id: &str, record_cwd: &str) -> bool {
    if get_project_hash(Path::new(record_cwd)) == paths.project_hash() {
        return true;
    }

    // Worktree transcripts use the worktree root as cwd. Resolve the owning
    // repository from the last marker so nested worktrees remain listable.
    if let Some(repo_root) = worktree_repo_root(record_cwd)
        && get_project_hash(Path::new(repo_root)) == paths.project_hash()
    {
        return true;
    }

    // Older/migrated sessions may have a cwd that no longer hashes to this
    // workspace. The runtime status sidecar provides the same fallback as the
    // TypeScript SessionService, but is only trusted when its session id and
    // complete v1 shape validate.
    let runtime_status_path = paths
        .chats_directory(SessionArchiveState::Active, cfg!(windows))
        .join(format!("{session_id}.runtime.json"));
    let Some(bytes) = read_bounded_regular_file(&runtime_status_path, SESSION_SIDECAR_MAX_BYTES)
    else {
        return false;
    };
    let Ok(status) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    let Some(status) = status.as_object() else {
        return false;
    };
    let valid_shape = status.get("schema_version").and_then(Value::as_i64) == Some(1)
        && status
            .get("pid")
            .is_some_and(|value| value.as_i64().is_some() || value.as_u64().is_some())
        && status.get("session_id").and_then(Value::as_str).is_some()
        && status.get("work_dir").and_then(Value::as_str).is_some()
        && status.get("hostname").and_then(Value::as_str).is_some()
        && status
            .get("started_at")
            .and_then(Value::as_f64)
            .is_some_and(f64::is_finite)
        && status
            .get("canopy_version")
            .is_some_and(|value| value.is_null() || value.is_string());
    if !valid_shape || status.get("session_id").and_then(Value::as_str) != Some(session_id) {
        return false;
    }
    status
        .get("work_dir")
        .and_then(Value::as_str)
        .is_some_and(|work_dir| get_project_hash(Path::new(work_dir)) == paths.project_hash())
}

fn worktrees_marker() -> String {
    format!(
        "{}.canopy{}worktrees{}",
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR,
        std::path::MAIN_SEPARATOR
    )
}

fn worktree_repo_root(path: &str) -> Option<&str> {
    let marker_index = path.rfind(&worktrees_marker())?;
    (marker_index > 0).then_some(&path[..marker_index])
}

fn read_worktree_sidecar(
    paths: &SessionPaths,
    session_id: &str,
    archive_state: SessionArchiveState,
    record_cwd: &str,
) -> Option<WorktreeSummary> {
    let sidecar_path = paths
        .chats_directory(archive_state, cfg!(windows))
        .join(format!("{session_id}.worktree.json"));
    let bytes = read_bounded_regular_file(&sidecar_path, SESSION_SIDECAR_MAX_BYTES)?;
    let sidecar = serde_json::from_slice::<Value>(&bytes).ok()?;
    let sidecar = sidecar.as_object()?;
    let slug = sidecar.get("slug")?.as_str()?;
    let worktree_path = sidecar.get("worktreePath")?.as_str()?;
    let worktree_branch = sidecar.get("worktreeBranch")?.as_str()?;
    let original_cwd = sidecar.get("originalCwd")?.as_str()?;
    // Validate the full source schema even though only three fields are
    // returned. This mirrors readWorktreeSession and rejects partial writes.
    sidecar.get("originalBranch")?.as_str()?;
    sidecar.get("originalHeadCommit")?.as_str()?;

    let worktree_path_buf = Path::new(worktree_path);
    let record_cwd_path = Path::new(record_cwd);
    let repo_root = worktree_repo_root(worktree_path)?;
    let marker_index = worktree_path.rfind(&worktrees_marker())?;
    let path_tail = &worktree_path[marker_index + worktrees_marker().len()..];
    let path_slug = Path::new(path_tail);
    if !worktree_path_buf.is_absolute()
        || !record_cwd_path.is_absolute()
        || path_tail.is_empty()
        || path_slug.components().count() != 1
        || path_slug.file_name().and_then(|part| part.to_str()) != Some(slug)
        || Path::new(repo_root) != Path::new(original_cwd)
        || !record_cwd_path.starts_with(worktree_path_buf)
        || get_project_hash(Path::new(repo_root)) != paths.project_hash()
    {
        return None;
    }

    Some(WorktreeSummary {
        slug: slug.to_owned(),
        path: worktree_path.to_owned(),
        branch: worktree_branch.to_owned(),
    })
}

fn read_bounded_regular_file(path: &Path, max_bytes: u64) -> Option<Vec<u8>> {
    let before_open = std::fs::symlink_metadata(path).ok()?;
    if !before_open.is_file()
        || before_open.file_type().is_symlink()
        || before_open.len() > max_bytes
    {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let opened = file.metadata().ok()?;
    if !opened.is_file() || opened.len() > max_bytes {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before_open.dev() != opened.dev() || before_open.ino() != opened.ino() {
            return None;
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    (&mut file)
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .ok()?;
    (bytes.len() as u64 <= max_bytes).then_some(bytes)
}

fn read_session_organization_snapshot(paths: &SessionPaths) -> SessionOrganizationSnapshot {
    let path = paths
        .project_directory(cfg!(windows))
        .join(SESSION_ORGANIZATION_STORE_FILE);
    let snapshot = SessionOrganizationStore::at_store_path(path).read_snapshot();
    let group_ids = snapshot
        .groups
        .iter()
        .map(|group| group.id.clone())
        .collect();
    let sessions = snapshot
        .sessions
        .into_iter()
        .map(|(session_id, organization)| {
            (
                session_id,
                SessionOrganizationMetadata {
                    group_id: organization.group_id,
                    color: organization.color.map(|color| color.as_str().to_owned()),
                    pinned_at: organization.pinned_at,
                },
            )
        })
        .collect();
    SessionOrganizationSnapshot {
        group_ids,
        sessions,
    }
}

fn extract_session_metadata(records: &[Value]) -> (Option<String>, Option<String>, Option<String>) {
    let mut parent_session_id = None;
    let mut source_type = None;
    let mut source_id = None;
    for record in records {
        if record.get("type").and_then(Value::as_str) != Some("system") {
            continue;
        }
        let payload = record.get("systemPayload");
        match record.get("subtype").and_then(Value::as_str) {
            Some("parent_session") if parent_session_id.is_none() => {
                parent_session_id = payload
                    .and_then(|payload| payload.get("parentSessionId"))
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned);
            }
            Some("session_source") if source_type.is_none() => {
                source_type = payload
                    .and_then(|payload| payload.get("sourceType"))
                    .and_then(Value::as_str)
                    .filter(|source_type| !source_type.is_empty())
                    .map(str::to_owned);
                source_id = payload
                    .and_then(|payload| payload.get("sourceId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            _ => {}
        }
    }
    (parent_session_id, source_type, source_id)
}

fn first_prompt(records: &[Value]) -> String {
    for record in records {
        if record.get("type").and_then(Value::as_str) != Some("user")
            || record.get("subtype").is_some()
        {
            continue;
        }
        let payload = record.get("systemPayload");
        if let Some(display_text) = payload
            .and_then(|payload| payload.get("displayText"))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            return truncate_prompt(display_text);
        }
        let parts = record
            .get("message")
            .and_then(|message| message.get("parts"))
            .and_then(Value::as_array);
        if let Some(text) = parts
            .and_then(|parts| {
                parts
                    .iter()
                    .find_map(|part| part.get("text").and_then(Value::as_str))
            })
            .filter(|text| !text.is_empty())
        {
            return truncate_prompt(text);
        }
    }
    String::new()
}

fn truncate_prompt(prompt: &str) -> String {
    let mut characters = prompt.chars();
    let prefix = characters.by_ref().take(200).collect::<String>();
    if characters.next().is_some() {
        format!("{prefix}...")
    } else {
        prefix
    }
}

fn read_last_custom_title(path: &Path) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let file_size = file.metadata().ok()?.len();
    if file_size == 0 {
        return None;
    }
    let tail_size = file_size.min(SESSION_TITLE_SCAN_BYTES);
    let tail_start = file_size - tail_size;
    if file.seek(SeekFrom::Start(tail_start)).is_err() {
        return None;
    }
    let mut tail = Vec::with_capacity(tail_size as usize);
    if file.read_to_end(&mut tail).is_err() {
        return None;
    }
    let tail_text = String::from_utf8_lossy(&tail);
    let tail_text = if tail_start > 0 {
        tail_text
            .find('\n')
            .map(|newline| &tail_text[newline + 1..])
            .unwrap_or("")
    } else {
        tail_text.as_ref()
    };
    if let Some(title) = title_in_lines(tail_text) {
        return (!title.is_empty()).then_some(title);
    }
    if tail_start == 0 || file.seek(SeekFrom::Start(0)).is_err() {
        return None;
    }
    let head_size = file_size.min(SESSION_TITLE_SCAN_BYTES) as usize;
    let mut head = vec![0; head_size];
    let bytes_read = file.read(&mut head).ok()?;
    let head_text = String::from_utf8_lossy(&head[..bytes_read]);
    let head_text = if (bytes_read as u64) < file_size {
        head_text
            .rfind('\n')
            .map(|newline| &head_text[..newline + 1])
            .unwrap_or("")
    } else {
        head_text.as_ref()
    };
    title_in_lines(head_text).filter(|title| !title.is_empty())
}

fn title_in_lines(text: &str) -> Option<String> {
    let mut found = None;
    for line in text.lines() {
        for record in canopy_core::jsonl::parse_line_tolerant(line) {
            if record.get("type").and_then(Value::as_str) == Some("system")
                && record.get("subtype").and_then(Value::as_str) == Some("custom_title")
                && let Some(title) = record
                    .get("systemPayload")
                    .and_then(|payload| payload.get("customTitle"))
                    .and_then(Value::as_str)
            {
                found = Some(title.to_owned());
            }
        }
    }
    found
}

fn live_session_summary(
    live: &canopy_core::acp_bridge::BridgeSessionRuntimeEntry,
) -> SessionListItem {
    SessionListItem {
        session_id: live.session_id.clone(),
        cwd: live.workspace_cwd.to_string_lossy().into_owned(),
        created_at: live.created_at.clone(),
        updated_at: live.created_at.clone(),
        activity_time: parse_canonical_session_timestamp_millis(&live.created_at)
            .unwrap_or_default(),
        display_name: None,
        parent_session_id: live.parent_session_id.clone(),
        source_type: live.source.source_type.clone(),
        source_id: live.source.source_id.clone(),
        client_count: live.attach_count(),
        has_active_prompt: live.pending_prompt_count() > 0,
        is_archived: false,
        worktree: None,
    }
}

fn parse_canonical_session_timestamp_millis(value: &str) -> Option<f64> {
    if !value.is_ascii() || !value.ends_with('Z') || value.len() < 20 {
        return None;
    }
    let year = value.get(0..4)?.parse::<i64>().ok()?;
    let month = value.get(5..7)?.parse::<i64>().ok()?;
    let day = value.get(8..10)?.parse::<i64>().ok()?;
    let hour = value.get(11..13)?.parse::<i64>().ok()?;
    let minute = value.get(14..16)?.parse::<i64>().ok()?;
    let second = value.get(17..19)?.parse::<i64>().ok()?;
    if value.as_bytes().get(4) != Some(&b'-')
        || value.as_bytes().get(7) != Some(&b'-')
        || value.as_bytes().get(10) != Some(&b'T')
        || value.as_bytes().get(13) != Some(&b':')
        || value.as_bytes().get(16) != Some(&b':')
        || year < 1
        || !(1..=12).contains(&month)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=59).contains(&second)
    {
        return None;
    }
    let leap_year = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 if leap_year => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if !(1..=days_in_month).contains(&day) {
        return None;
    }
    let fraction = match value.get(19..value.len().checked_sub(1)?)? {
        "" => 0,
        raw if raw.starts_with('.') => {
            let digits = &raw[1..];
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let millis_digits = &digits[..digits.len().min(3)];
            let parsed = millis_digits.parse::<i64>().ok()?;
            parsed * 10_i64.pow((3 - millis_digits.len()) as u32)
        }
        _ => return None,
    };

    // Gregorian civil date to days from the Unix epoch.
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days_since_epoch = era * 146_097 + day_of_era - 719_468;
    let millis = days_since_epoch * 86_400_000
        + hour * 3_600_000
        + minute * 60_000
        + second * 1_000
        + fraction;
    Some(millis as f64)
}

fn merge_live_session(
    existing: &mut SessionListItem,
    live: &canopy_core::acp_bridge::BridgeSessionRuntimeEntry,
) {
    existing.client_count = live.attach_count();
    existing.has_active_prompt = live.pending_prompt_count() > 0;
    if existing.parent_session_id.is_none() {
        existing.parent_session_id = live.parent_session_id.clone();
    }
    if existing.source_type.is_none() {
        existing.source_type = live.source.source_type.clone();
        existing.source_id = live.source.source_id.clone();
    }
}

fn matches_source(
    session: &SessionListItem,
    source_type: Option<&str>,
    source_id: Option<&str>,
) -> bool {
    let type_matches = source_type.is_none_or(|source_type| {
        session.source_type.as_deref() == Some(source_type)
            || (source_type == "default" && session.source_type.is_none())
    });
    type_matches
        && source_id.is_none_or(|source_id| session.source_id.as_deref() == Some(source_id))
}

fn compare_activity(a: &SessionListItem, b: &SessionListItem) -> std::cmp::Ordering {
    b.activity_time
        .total_cmp(&a.activity_time)
        .then_with(|| a.session_id.cmp(&b.session_id))
}

fn organized_cursor_key(
    session: &SessionListItem,
    metadata: &SessionOrganizationMetadata,
) -> OrganizedSessionCursorKey {
    OrganizedSessionCursorKey {
        is_pinned: metadata.is_pinned(),
        activity_time: session.activity_time,
        session_id: session.session_id.clone(),
    }
}

fn compare_organized_cursor_keys(
    a: &OrganizedSessionCursorKey,
    b: &OrganizedSessionCursorKey,
) -> std::cmp::Ordering {
    b.is_pinned
        .cmp(&a.is_pinned)
        .then_with(|| b.activity_time.total_cmp(&a.activity_time))
        .then_with(|| a.session_id.cmp(&b.session_id))
}

fn matches_organized_group(metadata: &SessionOrganizationMetadata, group: &str) -> bool {
    match group {
        "all" => true,
        "pinned" => metadata.is_pinned(),
        "ungrouped" => metadata.group_id.is_none() && metadata.color.is_none(),
        group => metadata.color.is_none() && metadata.group_id.as_deref() == Some(group),
    }
}

fn organized_session_response(
    session: &SessionListItem,
    metadata: &SessionOrganizationMetadata,
) -> Value {
    let mut response = session.to_response();
    if let Some(object) = response.as_object_mut() {
        object.insert("groupId".to_owned(), json!(metadata.group_id));
        object.insert("color".to_owned(), json!(metadata.color));
        object.insert("isPinned".to_owned(), json!(metadata.is_pinned()));
        if let Some(pinned_at) = metadata.pinned_at.as_deref() {
            object.insert("pinnedAt".to_owned(), json!(pinned_at));
        }
    }
    response
}

fn parse_metadata_session_cursor(
    cursor: &str,
    parent_session_id: Option<&str>,
    source_type: Option<&str>,
    source_id: Option<&str>,
    archive_state: SessionArchiveState,
) -> Result<MetadataCursorKey, RpcError> {
    let invalid = || {
        RpcError::invalid_params(format!(
            "Invalid cursor: {cursor:?} is not a valid metadata cursor"
        ))
    };
    let decoded = base64url_decode(cursor).ok_or_else(invalid)?;
    let value: Value = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    let Some(object) = value.as_object() else {
        return Err(invalid());
    };
    let matches_optional = |key: &str, expected: Option<&str>| match expected {
        Some(expected) => object.get(key).and_then(Value::as_str) == Some(expected),
        None => !object.contains_key(key),
    };
    if !matches_optional("parentSessionId", parent_session_id)
        || !matches_optional("sourceType", source_type)
        || !matches_optional("sourceId", source_id)
        || object.get("archiveState").and_then(Value::as_str)
            != Some(match archive_state {
                SessionArchiveState::Active => "active",
                SessionArchiveState::Archived => "archived",
            })
    {
        return Err(invalid());
    }
    let last = object
        .get("last")
        .and_then(Value::as_object)
        .ok_or_else(invalid)?;
    let activity_time = last
        .get("activityTime")
        .and_then(Value::as_f64)
        .filter(|time| time.is_finite())
        .ok_or_else(invalid)?;
    let session_id = last
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
        .ok_or_else(invalid)?
        .to_owned();
    Ok(MetadataCursorKey {
        activity_time,
        session_id,
    })
}

fn is_after_cursor(session: &SessionListItem, cursor: &MetadataCursorKey) -> bool {
    session.activity_time < cursor.activity_time
        || (session.activity_time == cursor.activity_time && session.session_id > cursor.session_id)
}

fn encode_metadata_session_cursor(
    last: &SessionListItem,
    parent_session_id: Option<&str>,
    source_type: Option<&str>,
    source_id: Option<&str>,
    archive_state: SessionArchiveState,
) -> String {
    let mut cursor = serde_json::Map::new();
    if let Some(parent_session_id) = parent_session_id {
        cursor.insert("parentSessionId".to_owned(), json!(parent_session_id));
    }
    if let Some(source_type) = source_type {
        cursor.insert("sourceType".to_owned(), json!(source_type));
    }
    if let Some(source_id) = source_id {
        cursor.insert("sourceId".to_owned(), json!(source_id));
    }
    cursor.insert(
        "archiveState".to_owned(),
        json!(match archive_state {
            SessionArchiveState::Active => "active",
            SessionArchiveState::Archived => "archived",
        }),
    );
    cursor.insert(
        "last".to_owned(),
        json!({"activityTime":last.activity_time,"sessionId":last.session_id}),
    );
    base64url_encode(
        serde_json::to_string(&cursor)
            .unwrap_or_default()
            .as_bytes(),
    )
}

fn parse_organized_session_cursor(
    cursor: &str,
    group: &str,
    archive_state: SessionArchiveState,
    source_type: Option<&str>,
    source_id: Option<&str>,
) -> Result<OrganizedSessionCursorKey, RpcError> {
    let invalid = || {
        RpcError::invalid_params(format!(
            "Invalid cursor: {cursor:?} is not a valid organized cursor"
        ))
    };
    let decoded = base64url_decode(cursor).ok_or_else(invalid)?;
    let value: Value = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    let Some(object) = value.as_object() else {
        return Err(invalid());
    };
    let matches_optional = |key: &str, expected: Option<&str>| match expected {
        Some(expected) => object.get(key).and_then(Value::as_str) == Some(expected),
        None => !object.contains_key(key),
    };
    if object.get("group").and_then(Value::as_str) != Some(group)
        || object.get("archiveState").and_then(Value::as_str)
            != Some(match archive_state {
                SessionArchiveState::Active => "active",
                SessionArchiveState::Archived => "archived",
            })
        || !matches_optional("sourceType", source_type)
        || !matches_optional("sourceId", source_id)
    {
        return Err(invalid());
    }
    let last = object
        .get("last")
        .and_then(Value::as_object)
        .ok_or_else(invalid)?;
    let is_pinned = last
        .get("isPinned")
        .and_then(Value::as_bool)
        .ok_or_else(invalid)?;
    let activity_time = last
        .get("activityTime")
        .and_then(Value::as_f64)
        .filter(|time| time.is_finite())
        .ok_or_else(invalid)?;
    let session_id = last
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
        .ok_or_else(invalid)?
        .to_owned();
    Ok(OrganizedSessionCursorKey {
        is_pinned,
        activity_time,
        session_id,
    })
}

fn encode_organized_session_cursor(
    last: &OrganizedSessionCursorKey,
    group: &str,
    archive_state: SessionArchiveState,
    source_type: Option<&str>,
    source_id: Option<&str>,
) -> String {
    let mut cursor = serde_json::Map::new();
    cursor.insert("group".to_owned(), json!(group));
    cursor.insert(
        "archiveState".to_owned(),
        json!(match archive_state {
            SessionArchiveState::Active => "active",
            SessionArchiveState::Archived => "archived",
        }),
    );
    if let Some(source_type) = source_type {
        cursor.insert("sourceType".to_owned(), json!(source_type));
    }
    if let Some(source_id) = source_id {
        cursor.insert("sourceId".to_owned(), json!(source_id));
    }
    cursor.insert(
        "last".to_owned(),
        json!({
            "isPinned":last.is_pinned,
            "activityTime":last.activity_time,
            "sessionId":last.session_id
        }),
    );
    base64url_encode(
        serde_json::to_string(&cursor)
            .unwrap_or_default()
            .as_bytes(),
    )
}

fn base64url_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        let a = chunk[0] as usize;
        let b = chunk.get(1).copied().unwrap_or_default() as usize;
        let c = chunk.get(2).copied().unwrap_or_default() as usize;
        encoded.push(ALPHABET[a >> 2] as char);
        encoded.push(ALPHABET[((a & 0x03) << 4) | (b >> 4)] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[((b & 0x0f) << 2) | (c >> 6)] as char);
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[c & 0x3f] as char);
        }
    }
    encoded
}

fn base64url_decode(value: &str) -> Option<Vec<u8>> {
    fn decode_byte(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    if value.len() % 4 == 1 {
        return None;
    }
    let mut output = Vec::with_capacity(value.len() * 3 / 4);
    let mut accumulator = 0u32;
    let mut bits = 0u8;
    for byte in value.bytes() {
        accumulator = (accumulator << 6) | u32::from(decode_byte(byte)?);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits) as u8);
        }
    }
    Some(output)
}

fn format_epoch_millis(millis: u128) -> String {
    let seconds = millis / 1_000;
    let days = (seconds / 86_400) as i64;
    let seconds_of_day = (seconds % 86_400) as u32;
    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days / 146_097
    } else {
        (shifted_days - 146_096) / 146_097
    };
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{:03}Z",
        millis % 1_000
    )
}

fn session_store(workspace: &Path) -> SessionStore {
    SessionStore::new(
        Storage::new(workspace).runtime_base_dir().to_path_buf(),
        workspace,
    )
}

fn timestamp_now() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format_epoch_millis(millis)
}

fn recovery_kind_name(kind: SessionRecoveryKind) -> &'static str {
    match kind {
        SessionRecoveryKind::Clean => "clean",
        SessionRecoveryKind::InterruptedPrompt => "interrupted_prompt",
        SessionRecoveryKind::InterruptedTurn => "interrupted_turn",
        SessionRecoveryKind::DegradedHistory => "degraded_history",
    }
}

fn rebuild_session_history(
    store: &SessionStore,
    session_id: &str,
    recorder: &mut SessionRecorder,
) -> Result<Vec<Value>, String> {
    let prepared = store
        .prepare_active_transcript(session_id)
        .map_err(|error| error.to_string())?;
    let records = prepared
        .records
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let gaps = prepared
        .gaps
        .iter()
        .map(|gap| HistoryGap {
            child_uuid: gap.child_uuid.clone(),
            missing_parent_uuid: gap.missing_parent_uuid.clone(),
        })
        .collect::<Vec<_>>();
    let plan = build_session_recovery_plan(
        session_id.to_owned(),
        records,
        &gaps,
        SessionRecoveryOptions {
            allow_auto_continue: false,
        },
    )
    .map_err(|error| error.to_string())?;
    if plan.kind == SessionRecoveryKind::DegradedHistory {
        return Err(plan.visible_notice.unwrap_or_else(|| {
            "session history is incomplete and cannot be continued safely".to_owned()
        }));
    }
    if plan.kind == SessionRecoveryKind::InterruptedTurn {
        record_synthesized_recovery_results(&plan, recorder)?;
    }
    Ok(plan.api_history)
}

fn transcript_text(record: &TranscriptRecord) -> Option<String> {
    let parts = record.message.as_ref()?.parts.as_ref()?;
    let mut text = String::new();
    for part in parts {
        if let Some(value) = part.get("text").and_then(Value::as_str) {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(value);
        }
    }
    (!text.is_empty()).then_some(text)
}

fn emit_agent_event(
    output: &ProtocolOutput,
    session_id: &str,
    event: AgentRunEvent,
) -> Result<(), String> {
    let update = match event {
        AgentRunEvent::Turn(TurnEvent::Content { value, .. }) => Some(json!({
            "sessionUpdate":"agent_message_chunk",
            "content":{"type":"text","text":value}
        })),
        AgentRunEvent::Turn(TurnEvent::Thought { value }) => Some(json!({
            "sessionUpdate":"agent_thought_chunk",
            "content":{"type":"text","text":value.description}
        })),
        AgentRunEvent::Turn(TurnEvent::ToolCallRequest { value }) => Some(tool_call_update(&value)),
        AgentRunEvent::ToolExecutionStarted { call_id, name } => Some(json!({
            "sessionUpdate":"tool_call_update",
            "toolCallId":call_id,
            "status":"in_progress",
            "title":name
        })),
        AgentRunEvent::ToolExecutionFinished {
            call_id,
            name,
            response,
            display,
            ..
        } => {
            let rendered = display
                .as_ref()
                .and_then(|value| value.get("text"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| response.to_string());
            Some(json!({
                "sessionUpdate":"tool_call_update",
                "toolCallId":call_id,
                "status":"completed",
                "title":name,
                "content":[{"type":"content","content":{"type":"text","text":rendered}}],
                "rawOutput":response
            }))
        }
        AgentRunEvent::Turn(_) => None,
    };
    if let Some(update) = update {
        output.notification(
            "session/update",
            json!({"sessionId":session_id,"update":update}),
        )?;
    }
    Ok(())
}

fn tool_call_update(call: &ToolCallRequestInfo) -> Value {
    let kind = match call.name.as_str() {
        "read_file" | "zoom_image" | "list_directory" | "glob" | "grep" => "read",
        "edit_file" | "write_file" | "notebook_edit" => "edit",
        "run_shell_command" => "execute",
        _ => "other",
    };
    json!({
        "sessionUpdate":"tool_call",
        "toolCallId":call.call_id,
        "title":call.name,
        "kind":kind,
        "status":"pending",
        "rawInput":call.args
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn test_output() -> (ProtocolOutput, SharedWriter) {
        let writer = SharedWriter::default();
        (
            ProtocolOutput {
                writer: Arc::new(Mutex::new(Box::new(writer.clone()))),
                client_requests: Arc::new(ClientRequestBroker::default()),
            },
            writer,
        )
    }

    fn output_lines(writer: &SharedWriter) -> Vec<Value> {
        let bytes = writer
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn test_session_paths() -> (PathBuf, SessionPaths) {
        let root = std::env::temp_dir().join(format!("canopy-session-list-{}", fresh_session_id()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let workspace = std::fs::canonicalize(workspace).unwrap();
        let paths = SessionPaths::new(root.join("runtime"), workspace);
        (root, paths)
    }

    fn write_list_fixture(
        paths: &SessionPaths,
        session_id: &str,
        state: SessionArchiveState,
        cwd: &str,
        prompt: &str,
        title: &str,
    ) {
        let directory = paths.chats_directory(state, cfg!(windows));
        std::fs::create_dir_all(&directory).unwrap();
        let file = directory.join(format!("{session_id}.jsonl"));
        let records = [
            json!({
                "uuid":"first",
                "parentUuid":null,
                "sessionId":session_id,
                "type":"user",
                "timestamp":"2026-09-25T10:00:00.000Z",
                "cwd":cwd,
                "message":{"role":"user","parts":[{"text":prompt}]}
            }),
            json!({
                "uuid":"parent",
                "parentUuid":"first",
                "sessionId":session_id,
                "type":"system",
                "subtype":"parent_session",
                "systemPayload":{"parentSessionId":"parent-session"}
            }),
            json!({
                "uuid":"source",
                "parentUuid":"parent",
                "sessionId":session_id,
                "type":"system",
                "subtype":"session_source",
                "systemPayload":{"sourceType":"scheduled_task","sourceId":"task-4"}
            }),
            json!({
                "uuid":"title",
                "parentUuid":"source",
                "sessionId":session_id,
                "type":"system",
                "subtype":"custom_title",
                "systemPayload":{"customTitle":title,"titleSource":"manual"}
            }),
        ];
        let contents = records
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n");
        std::fs::write(file, format!("{contents}\n")).unwrap();
    }

    #[test]
    fn initialize_advertises_supported_protocol_capabilities() {
        let response = initialize_response();
        assert_eq!(response["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(response["agentCapabilities"]["loadSession"], true);
        assert_eq!(
            response["agentCapabilities"]["promptCapabilities"]["embeddedContext"],
            true
        );
        assert_eq!(
            response["agentCapabilities"]["promptCapabilities"]["image"],
            true
        );
        assert_eq!(
            response["agentCapabilities"]["sessionCapabilities"]["resume"],
            json!({})
        );
        assert_eq!(
            response["agentCapabilities"]["sessionCapabilities"]["list"],
            json!({})
        );
    }

    #[test]
    fn prompt_parser_preserves_supported_text_and_rejects_binary_blocks() {
        let blocks = vec![
            json!({"type":"text","text":"Inspect this"}),
            json!({"type":"resource_link","uri":"file:///tmp/a.rs","title":"a.rs"}),
            json!({"type":"resource","resource":{"text":"source text"}}),
        ];
        assert_eq!(
            prompt_text(&blocks).unwrap(),
            "Inspect this\n[Resource link: a.rs] file:///tmp/a.rs\nsource text"
        );
        assert_eq!(
            prompt_text(&[json!({"type":"image","data":"AA=="})])
                .unwrap_err()
                .code,
            -32602
        );
    }

    #[test]
    fn caller_session_id_requires_rfc_variant_and_version_and_normalizes_case() {
        let value = "A0B1C2D3-E4F5-4A67-89AB-CDEF01234567";
        assert_eq!(
            requested_session_id(&json!({"_meta":{"qwen-code/sessionId":value}})).unwrap(),
            Some(value.to_ascii_lowercase())
        );
        for value in [
            "A0B1C2D3-E4F5-6A67-89AB-CDEF01234567",
            "A0B1C2D3-E4F5-4A67-79AB-CDEF01234567",
            "not-a-uuid",
        ] {
            assert_eq!(
                requested_session_id(&json!({"_meta":{"qwen-code/sessionId":value}}))
                    .unwrap_err()
                    .code,
                -32602
            );
        }
        let generated = fresh_session_id();
        assert!(is_rfc_uuid_v1_to_v5(&generated));
        assert_ne!(generated, fresh_session_id());
    }

    #[test]
    fn session_timestamps_use_rfc3339_utc_format() {
        let timestamp = timestamp_now();
        assert_eq!(timestamp.len(), 24);
        assert_eq!(&timestamp[10..11], "T");
        assert_eq!(&timestamp[23..24], "Z");
    }

    #[test]
    fn session_list_reads_active_and_archived_session_summaries() {
        let (root, paths) = test_session_paths();
        let workspace = paths.project_root().to_string_lossy().into_owned();
        let active_id = "11111111-1111-4111-8111-111111111111";
        let archived_id = "22222222-2222-4222-8222-222222222222";
        write_list_fixture(
            &paths,
            active_id,
            SessionArchiveState::Active,
            &workspace,
            "Find the active session",
            "Active title",
        );
        write_list_fixture(
            &paths,
            archived_id,
            SessionArchiveState::Archived,
            &workspace,
            "Find the archived session",
            "Archived title",
        );

        let (active, active_truncated) =
            scan_persisted_sessions(&paths, SessionArchiveState::Active).unwrap();
        let (archived, archived_truncated) =
            scan_persisted_sessions(&paths, SessionArchiveState::Archived).unwrap();
        assert!(!active_truncated && !archived_truncated);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].session_id, active_id);
        assert_eq!(active[0].display_name.as_deref(), Some("Active title"));
        assert_eq!(
            active[0].parent_session_id.as_deref(),
            Some("parent-session")
        );
        assert_eq!(active[0].source_type.as_deref(), Some("scheduled_task"));
        assert_eq!(active[0].source_id.as_deref(), Some("task-4"));
        assert!(!active[0].is_archived);
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].session_id, archived_id);
        assert_eq!(archived[0].display_name.as_deref(), Some("Archived title"));
        assert!(archived[0].is_archived);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_list_skips_malformed_ids_and_other_workspace_paths() {
        let (root, paths) = test_session_paths();
        let active_directory = paths.chats_directory(SessionArchiveState::Active, cfg!(windows));
        std::fs::create_dir_all(&active_directory).unwrap();
        std::fs::write(active_directory.join("not-a-session.jsonl"), "{}\n").unwrap();
        let invalid_project_id = "33333333-3333-4333-8333-333333333333";
        write_list_fixture(
            &paths,
            invalid_project_id,
            SessionArchiveState::Active,
            "/different/workspace",
            "wrong workspace",
            "Wrong workspace",
        );
        assert!(!is_session_filename_id("../outside"));
        let (sessions, _) = scan_persisted_sessions(&paths, SessionArchiveState::Active).unwrap();
        assert!(sessions.is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn session_list_skips_symlinked_transcripts() {
        let (root, paths) = test_session_paths();
        let active_directory = paths.chats_directory(SessionArchiveState::Active, cfg!(windows));
        std::fs::create_dir_all(&active_directory).unwrap();
        let target = root.join("outside.jsonl");
        let session_id = "66666666-6666-4666-8666-666666666666";
        std::fs::write(
            &target,
            format!(
                "{}\n",
                json!({
                    "uuid":"first",
                    "sessionId":session_id,
                    "type":"user",
                    "timestamp":"2026-09-25T10:00:00.000Z",
                    "cwd":paths.project_root().to_string_lossy()
                })
            ),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            &target,
            active_directory.join(format!("{session_id}.jsonl")),
        )
        .unwrap();
        let (sessions, _) = scan_persisted_sessions(&paths, SessionArchiveState::Active).unwrap();
        assert!(sessions.is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn session_list_returns_empty_and_paginated_response_shapes() {
        let (root, paths) = test_session_paths();
        let workspace = paths.project_root().to_path_buf();
        let workspace_arg = workspace.to_string_lossy().into_owned();
        let runtime_base_dir = paths.runtime_base_dir().to_path_buf();
        let result = Storage::run_with_resolved_runtime_base_dir(&runtime_base_dir, || async {
            let (output, _) = test_output();
            let server = AcpServer::new(AcpOptions::default(), output);
            let empty = server
                .list_sessions(&json!({"workspaceCwd":workspace_arg.clone()}))
                .await
                .unwrap();
            assert_eq!(empty["sessions"], json!([]));
            assert!(empty.get("nextCursor").is_none());

            write_list_fixture(
                &paths,
                "77777777-7777-4777-8777-777777777777",
                SessionArchiveState::Active,
                &workspace_arg,
                "first prompt",
                "first title",
            );
            std::thread::sleep(std::time::Duration::from_millis(15));
            write_list_fixture(
                &paths,
                "88888888-8888-4888-8888-888888888888",
                SessionArchiveState::Active,
                &workspace_arg,
                "second prompt",
                "second title",
            );
            assert_eq!(
                scan_persisted_sessions(
                    session_store(&workspace).paths(),
                    SessionArchiveState::Active
                )
                .unwrap()
                .0
                .len(),
                2
            );
            let first = server
                .list_sessions(&json!({"workspaceCwd":workspace_arg.clone(),"_meta":{"size":1}}))
                .await
                .unwrap();
            assert_eq!(first["sessions"].as_array().unwrap().len(), 1, "{first}");
            let cursor = first["nextCursor"].as_str().unwrap();
            let second = server
                .list_sessions(&json!({
                    "workspaceCwd":workspace_arg.clone(),
                    "cursor":cursor,
                    "_meta":{"size":1}
                }))
                .await
                .unwrap();
            assert_eq!(second["sessions"].as_array().unwrap().len(), 1);
            assert!(second.get("nextCursor").is_none());
            assert_ne!(
                first["sessions"][0]["sessionId"],
                second["sessions"][0]["sessionId"]
            );
            server.registry.shutdown().await;
        })
        .await;
        assert_eq!(result, ());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_list_validates_numeric_and_metadata_cursors_and_filters() {
        assert!(parse_numeric_session_cursor("NaN").is_err());
        assert!(parse_numeric_session_cursor("-1").is_err());
        assert_eq!(parse_numeric_session_cursor("12.5").unwrap(), 12.5);
        assert!(parse_list_source(&json!({"sourceId":"task-1"})).is_err());
        assert!(parse_list_source(&json!({"sourceType":"Invalid source"})).is_err());
        assert!(parse_list_source(&json!({"sourceType":"scheduled_task","sourceId":""})).is_err());

        let item = SessionListItem {
            session_id: "44444444-4444-4444-8444-444444444444".to_owned(),
            cwd: "/workspace".to_owned(),
            created_at: "2026-09-25T10:00:00.000Z".to_owned(),
            updated_at: "2026-09-25T10:01:00.000Z".to_owned(),
            activity_time: 100.0,
            display_name: None,
            parent_session_id: Some("parent-session".to_owned()),
            source_type: Some("scheduled_task".to_owned()),
            source_id: Some("task-4".to_owned()),
            client_count: 0,
            has_active_prompt: false,
            is_archived: false,
            worktree: None,
        };
        let encoded = encode_metadata_session_cursor(
            &item,
            Some("parent-session"),
            Some("scheduled_task"),
            Some("task-4"),
            SessionArchiveState::Active,
        );
        let decoded = parse_metadata_session_cursor(
            &encoded,
            Some("parent-session"),
            Some("scheduled_task"),
            Some("task-4"),
            SessionArchiveState::Active,
        )
        .unwrap();
        assert_eq!(decoded.session_id, item.session_id);
        assert!(is_after_cursor(
            &SessionListItem {
                session_id: "55555555-5555-4555-8555-555555555555".to_owned(),
                activity_time: 99.0,
                ..item.clone()
            },
            &decoded
        ));
        assert!(
            parse_metadata_session_cursor(
                &encoded,
                Some("different-parent"),
                Some("scheduled_task"),
                Some("task-4"),
                SessionArchiveState::Active,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn session_list_rejects_nonexistent_workspace_paths() {
        let (output, _) = test_output();
        let registry = WorkspaceRegistry::new(AcpOptions::default(), output);
        let missing_workspace =
            std::env::temp_dir().join(format!("canopy-missing-workspace-{}", fresh_session_id()));
        let error = registry
            .runtime_for_cwd(&missing_workspace.to_string_lossy())
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, -32602);
    }

    #[test]
    fn protocol_output_writes_single_line_jsonrpc_envelopes() {
        let (output, writer) = test_output();
        output
            .response(Value::from(9), Ok(json!({"ok":true})))
            .unwrap();
        output
            .notification("session/update", json!({"update":{}}))
            .unwrap();
        let values = output_lines(&writer);
        assert_eq!(values.len(), 2);
        assert_eq!(
            values[0],
            json!({"jsonrpc":"2.0","id":9,"result":{"ok":true}})
        );
        assert_eq!(
            values[1],
            json!({"jsonrpc":"2.0","method":"session/update","params":{"update":{}}})
        );
    }

    #[tokio::test]
    async fn serve_emits_jsonrpc_parse_error_without_polluting_stdout() {
        let (output, writer) = test_output();
        let server = Arc::new(AcpServer::new(AcpOptions::default(), output));
        let (sender, receiver) = mpsc::unbounded_channel();
        sender
            .send(InputLine::Line("{broken json}".to_owned()))
            .unwrap();
        sender.send(InputLine::Eof).unwrap();
        server.serve(receiver).await.unwrap();
        let values = output_lines(&writer);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["jsonrpc"], "2.0");
        assert!(values[0]["id"].is_null());
        assert_eq!(values[0]["error"]["code"], -32700);
        assert!(
            values[0]["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("Parse error:")
        );
    }

    #[tokio::test]
    async fn initialize_is_processed_before_following_pipelined_requests() {
        let (output, writer) = test_output();
        let server = Arc::new(AcpServer::new(AcpOptions::default(), output));
        let (sender, receiver) = mpsc::unbounded_channel();
        sender
            .send(InputLine::Line(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#.to_owned(),
            ))
            .unwrap();
        sender
            .send(InputLine::Line(
                r#"{"jsonrpc":"2.0","id":2,"method":"not-supported"}"#.to_owned(),
            ))
            .unwrap();
        sender.send(InputLine::Eof).unwrap();
        server.serve(receiver).await.unwrap();
        let values = output_lines(&writer);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["id"], 1);
        assert_eq!(values[0]["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(values[1]["id"], 2);
        assert_eq!(values[1]["error"]["code"], -32601);
    }
}
