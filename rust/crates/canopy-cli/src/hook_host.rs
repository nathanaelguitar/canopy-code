//! Host integration for configured prompt hooks in the native CLI.
//!
//! Settings ownership, workspace trust, extension activation, model selection,
//! and credentials stay in the CLI. The core hook crate owns event planning,
//! aggregation, prompt policy, and provider request conversion.

use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::{BufRead, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use canopy_core::agent_runtime::AgentToolExecutor;
use canopy_core::hooks::aggregator::SpecificHookOutput;
use canopy_core::hooks::async_registry::AsyncHookRegistry;
use canopy_core::hooks::command_runner::CommandHookRunner;
use canopy_core::hooks::config_loader::{
    ExtensionHookSource, HookConfigSources, load_hook_entries,
};
use canopy_core::hooks::event_inputs::HookBaseInput;
use canopy_core::hooks::function_runner::FunctionHookRunner;
use canopy_core::hooks::http_runner::HttpHookRunner;
use canopy_core::hooks::native_dispatch_executor::NativeDispatchExecutor;
use canopy_core::hooks::planner::HookEventName;
use canopy_core::hooks::prompt_provider::{
    PromptProviderBackend, PromptProviderModelResolver, ProviderPromptModelExecutor,
};
use canopy_core::hooks::prompt_runner::{PromptHookRunner, ResolvedPromptModel};
use canopy_core::hooks::registry::HookRegistry;
use canopy_core::hooks::session_manager::SessionHooksManager;
use canopy_core::hooks::system::HookSystem;
use canopy_core::hooks::system_events::HookEventExecutionOptions;
use canopy_core::providers::anthropic::{AnthropicMessagesClient, AnthropicProviderConfig};
use canopy_core::providers::gemini::{GeminiNativeClient, GeminiProviderConfig};
use canopy_core::providers::openai_compatible::{OpenAiCompatibleClient, OpenAiCompatibleConfig};
use canopy_core::providers::openai_pipeline::OpenAiPipelineConfig;
use canopy_core::providers::openai_profiles::{
    OpenAiProviderProfile, detect_openai_provider_profile,
};
use canopy_core::providers::prefix_caching::{OpenAiAuthMode, OpenAiPrefixCacheConfig};
use canopy_core::services::commit_attribution::AttributionSnapshot;
use canopy_core::services::file_history::FileHistorySnapshot;
use canopy_core::tool_response_finalizer::ToolExecutionOutput;
use canopy_core::turn::ToolCallRequestInfo;
use canopy_core::utils::cancellation::CancellationToken;
use chrono::Utc;
use serde_json::Value;

use crate::model_generation_config::NativeModelGenerationConfig;
use crate::{
    RunProviderKind, RunRuntimeProvider, configured_run_provider_protocol,
    find_configured_run_model_provider, nonempty_runtime_setting_string, parse_auth_type,
};

const MAX_EXTENSION_MANIFEST_BYTES: u64 = 1024 * 1024;
static PRE_TOOL_HOOK_CONFIRMATION_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

type CliPromptProviderExecutor = ProviderPromptModelExecutor<CliPromptModelResolver>;
type CliToolCallKey = (String, String, String);

#[derive(Clone, Default)]
pub struct CliToolCallHookState {
    calls: Arc<Mutex<HashMap<CliToolCallKey, CliToolCallHookOutcome>>>,
    cancellation: Arc<Mutex<Option<CancellationToken>>>,
}

#[derive(Clone, Debug, Default)]
pub struct CliToolCallHookOutcome {
    pub effective_tool_input: Option<serde_json::Map<String, Value>>,
    pub suppress_post_tool_use_failure: bool,
    pub entered_native_executor: bool,
    pub tool_execution_started: bool,
}

impl CliToolCallHookState {
    fn call_key(call: &ToolCallRequestInfo) -> CliToolCallKey {
        (
            call.prompt_id.clone(),
            call.call_id.clone(),
            call.name.clone(),
        )
    }

    pub fn set_cancellation(&self, cancellation: Option<CancellationToken>) {
        *self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = cancellation;
    }

    pub fn cancellation(&self) -> Option<CancellationToken> {
        self.cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn record_effective_input(
        &self,
        call: &ToolCallRequestInfo,
        input: serde_json::Map<String, Value>,
    ) {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(Self::call_key(call))
            .or_default()
            .effective_tool_input = Some(input);
    }

    pub fn suppress_post_tool_use_failure(&self, call: &ToolCallRequestInfo) {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(Self::call_key(call))
            .or_default()
            .suppress_post_tool_use_failure = true;
    }

    pub fn mark_entered_native_executor(&self, call: &ToolCallRequestInfo) {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(Self::call_key(call))
            .or_default()
            .entered_native_executor = true;
    }

    pub fn mark_tool_execution_started(&self, call: &ToolCallRequestInfo) {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(Self::call_key(call))
            .or_default()
            .tool_execution_started = true;
    }

    fn take_outcome(&self, call: &ToolCallRequestInfo) -> CliToolCallHookOutcome {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&Self::call_key(call))
            .unwrap_or_default()
    }
}

/// Configured Rust CLI hook runtime for prompt and tool lifecycle dispatch.
pub struct CliPromptHookHost {
    hook_system: HookSystem<CliPromptProviderExecutor>,
    runtime_base_dir: PathBuf,
    workspace_root: PathBuf,
}

impl CliPromptHookHost {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        settings: Value,
        effective_env: HashMap<String, String>,
        runtime_provider: &RunRuntimeProvider,
        main_model: &str,
        current_provider_kind: RunProviderKind,
        current_pipeline: &OpenAiPipelineConfig,
        proxy_url: Option<&str>,
        runtime_base_dir: &Path,
        workspace_root: &Path,
        user_hooks: Option<Value>,
        project_hooks: Option<Value>,
        active_extensions: &[canopy_core::services::at_resource_references::LocalExtensionReference],
    ) -> Result<Option<Self>, String> {
        if settings.get("disableAllHooks").and_then(Value::as_bool) == Some(true) {
            return Ok(None);
        }

        let extension_sources = active_extensions
            .iter()
            .filter_map(|extension| match load_extension_hooks(extension) {
                Ok(Some(hooks)) => Some(ExtensionHookSource {
                    is_active: true,
                    hooks: Some(hooks),
                }),
                Ok(None) => None,
                Err(error) => {
                    eprintln!(
                        "[CANOPY] Skipping hooks for extension {}: {error}",
                        extension.name
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        let sources = HookConfigSources {
            user_hooks,
            project_hooks,
            extensions: extension_sources,
        };
        let loaded = load_hook_entries(&sources, None);
        for issue in &loaded.issues {
            eprintln!("[CANOPY] {}", issue.message);
        }
        let supported_hooks_exist = loaded.entries.iter().any(|entry| {
            matches!(
                entry.event_name,
                HookEventName::UserPromptSubmit
                    | HookEventName::PreToolUse
                    | HookEventName::PostToolUse
                    | HookEventName::PostToolUseFailure
                    | HookEventName::PermissionRequest
            )
        });
        if !supported_hooks_exist {
            return Ok(None);
        }

        let resolver = CliPromptModelResolver::new(
            settings.clone(),
            effective_env.clone(),
            runtime_provider,
            main_model,
            current_provider_kind,
            current_pipeline,
            proxy_url,
        )?;
        let prompt_executor = ProviderPromptModelExecutor::new(resolver);
        let command_runner = Arc::new(CommandHookRunner {
            process_environment: Some(effective_env.into_iter().collect()),
            ..CommandHookRunner::default()
        });
        let async_registry = Arc::new(tokio::sync::Mutex::new(AsyncHookRegistry::default()));
        let allowed_urls = settings
            .pointer("/security/allowedHttpHookUrls")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            });
        // The settings loader strips this bypass from Workspace scope. Safe
        // and bare mode have already removed configured hooks and extensions.
        let allow_private_network_hosts = settings
            .pointer("/security/allowPrivateNetworkHooks")
            .and_then(Value::as_bool)
            == Some(true);
        let http_runner = HttpHookRunner::new(allowed_urls, allow_private_network_hosts)
            .map_err(|error| format!("could not initialize hook URL policy: {error}"))?;
        let dispatch = Arc::new(NativeDispatchExecutor::new(
            command_runner,
            async_registry,
            http_runner,
            FunctionHookRunner,
            PromptHookRunner::new(prompt_executor),
            None,
        ));
        let mut registry = HookRegistry::new();
        registry.initialize(loaded.entries);
        let hook_system = HookSystem::new(registry, SessionHooksManager::new(), dispatch);

        Ok(Some(Self {
            hook_system,
            runtime_base_dir: runtime_base_dir.to_path_buf(),
            workspace_root: workspace_root.to_path_buf(),
        }))
    }

    /// Return the ephemeral session-hook state associated with this host.
    pub fn session_manager_snapshot(&self) -> SessionHooksManager {
        self.hook_system.session_manager_snapshot()
    }

    pub async fn fire_user_prompt_submit(
        &self,
        session_id: &str,
        prompt: &str,
        submitted_prompt: Option<&str>,
        cancellation: &CancellationToken,
    ) -> Option<SpecificHookOutput> {
        let base = self.base_input(session_id);
        self.hook_system
            .fire_user_prompt_submit_event(
                &base,
                prompt,
                submitted_prompt,
                HookEventExecutionOptions {
                    messages: None,
                    cancellation: Some(cancellation),
                },
            )
            .await
    }

    pub async fn fire_pre_tool_use(
        &self,
        session_id: &str,
        tool_name: &str,
        tool_input: serde_json::Map<String, Value>,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
    ) -> Option<SpecificHookOutput> {
        self.hook_system
            .fire_pre_tool_use_event(
                &self.base_input(session_id),
                "default",
                tool_name,
                tool_input,
                tool_use_id,
                tool_call_id,
                HookEventExecutionOptions {
                    messages: None,
                    cancellation: None,
                },
            )
            .await
    }

    pub async fn fire_post_tool_use(
        &self,
        session_id: &str,
        tool_name: &str,
        tool_input: serde_json::Map<String, Value>,
        tool_response: serde_json::Map<String, Value>,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
    ) -> Option<SpecificHookOutput> {
        self.hook_system
            .fire_post_tool_use_event(
                &self.base_input(session_id),
                "default",
                tool_name,
                tool_input,
                tool_response,
                tool_use_id,
                tool_call_id,
                HookEventExecutionOptions {
                    messages: None,
                    cancellation: None,
                },
            )
            .await
    }

    pub async fn fire_post_tool_use_failure(
        &self,
        session_id: &str,
        tool_name: &str,
        tool_input: serde_json::Map<String, Value>,
        error_message: &str,
        is_interrupt: bool,
        tool_use_id: &str,
        tool_call_id: Option<&str>,
    ) -> Option<SpecificHookOutput> {
        self.hook_system
            .fire_post_tool_use_failure_event(
                &self.base_input(session_id),
                tool_use_id,
                tool_name,
                tool_input,
                error_message,
                Some(is_interrupt),
                Some("default"),
                tool_call_id,
                HookEventExecutionOptions {
                    messages: None,
                    cancellation: None,
                },
            )
            .await
    }

    pub async fn fire_permission_request(
        &self,
        session_id: &str,
        tool_name: &str,
        tool_input: serde_json::Map<String, Value>,
        permission_suggestions: Option<Vec<Value>>,
        cancellation: Option<&CancellationToken>,
    ) -> Option<SpecificHookOutput> {
        self.hook_system
            .fire_permission_request_event(
                &self.base_input(session_id),
                "default",
                tool_name,
                tool_input,
                permission_suggestions,
                HookEventExecutionOptions {
                    messages: None,
                    cancellation,
                },
            )
            .await
    }

    fn base_input(&self, session_id: &str) -> HookBaseInput {
        let store = canopy_core::session_store::SessionStore::new(
            &self.runtime_base_dir,
            &self.workspace_root,
        );
        let transcript_path = store
            .transcript_path(
                session_id,
                canopy_core::session_paths::SessionArchiveState::Active,
            )
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        HookBaseInput {
            session_id: session_id.to_owned(),
            source_type: None,
            source_id: None,
            transcript_path,
            cwd: self.workspace_root.to_string_lossy().into_owned(),
            timestamp: Utc::now().to_rfc3339(),
        }
    }
}

/// Applies configured CLI hook decisions around the real tool executor while
/// forwarding its host-owned lifecycle state unchanged.
pub struct CliHookedToolExecutor<E> {
    inner: E,
    hook_host: Option<Arc<CliPromptHookHost>>,
    call_hook_state: CliToolCallHookState,
    session_id: String,
    post_context: Mutex<HashMap<String, String>>,
}

impl<E> CliHookedToolExecutor<E> {
    pub fn new_with_call_hook_state(
        inner: E,
        hook_host: Option<Arc<CliPromptHookHost>>,
        session_id: String,
        call_hook_state: CliToolCallHookState,
    ) -> Self {
        Self {
            inner,
            hook_host,
            call_hook_state,
            session_id,
            post_context: Mutex::new(HashMap::new()),
        }
    }

    fn input_map(call: &ToolCallRequestInfo) -> serde_json::Map<String, Value> {
        call.args.as_object().cloned().unwrap_or_default()
    }

    fn tool_use_id() -> String {
        format!("toolu_{}", uuid::Uuid::new_v4())
    }

    fn append_hook_context(error: String, hook_output: Option<&SpecificHookOutput>) -> String {
        let Some(context) = hook_output
            .and_then(|output| {
                output
                    .value
                    .pointer("/hookSpecificOutput/additionalContext")
            })
            .and_then(Value::as_str)
            .filter(|context| !context.is_empty())
        else {
            return error;
        };
        let safe_context = context.replace('<', "&lt;").replace('>', "&gt;");
        format!("{error}\n\n{safe_context}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PreToolHookDecision {
    Proceed,
    Block(String),
    Ask(String),
}

pub fn pre_tool_hook_decision(output: Option<&SpecificHookOutput>) -> PreToolHookDecision {
    let Some(value) = output.map(|output| &output.value) else {
        return PreToolHookDecision::Proceed;
    };
    let specific = value.get("hookSpecificOutput");
    let specific_permission_decision = specific
        .and_then(|specific| specific.get("permissionDecision"))
        .and_then(Value::as_str)
        .filter(|decision| matches!(*decision, "allow" | "deny" | "ask"));
    let permission_decision =
        specific_permission_decision.or_else(|| value.get("decision").and_then(Value::as_str));
    if matches!(permission_decision, Some("deny" | "block")) {
        return PreToolHookDecision::Block(
            specific
                .and_then(|specific| specific.get("permissionDecisionReason"))
                .and_then(truthy_string)
                .or_else(|| value.get("reason").and_then(truthy_string))
                .unwrap_or("No reason provided")
                .to_owned(),
        );
    }
    if permission_decision == Some("ask") {
        return PreToolHookDecision::Ask(
            specific
                .and_then(|specific| specific.get("permissionDecisionReason"))
                .and_then(truthy_string)
                .or_else(|| value.get("reason").and_then(truthy_string))
                .unwrap_or("User confirmation required")
                .to_owned(),
        );
    }
    if value.get("continue").and_then(Value::as_bool) == Some(false) {
        return PreToolHookDecision::Block(
            value
                .get("stopReason")
                .and_then(truthy_string)
                .or_else(|| value.get("reason").and_then(truthy_string))
                .unwrap_or("No reason provided")
                .to_owned(),
        );
    }
    PreToolHookDecision::Proceed
}

pub fn confirm_pre_tool_hook_ask(tool_name: &str, reason: &str) -> Result<bool, String> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Ok(false);
    }
    let _prompt_guard = PRE_TOOL_HOOK_CONFIRMATION_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    eprintln!(
        "A PreToolUse hook requests confirmation to run `{}`.",
        crate::safe_terminal_text(tool_name, 160)
    );
    eprintln!("{}", crate::safe_terminal_text(reason, 1_000));
    eprint!("Run this tool once? [y/N] ");
    std::io::stderr()
        .flush()
        .map_err(|error| format!("could not display hook permission request: {error}"))?;
    let mut answer = String::new();
    stdin
        .lock()
        .read_line(&mut answer)
        .map_err(|error| format!("could not read hook permission response: {error}"))?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

impl<E: AgentToolExecutor> AgentToolExecutor for CliHookedToolExecutor<E> {
    fn execute<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
    ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>> {
        Box::pin(async move {
            let Some(hook_host) = self.hook_host.as_ref() else {
                let result = self.inner.execute(call).await;
                self.call_hook_state.take_outcome(call);
                return result;
            };
            let cancellation = self.inner.cancellation_token_for_current_prompt();
            self.call_hook_state.set_cancellation(cancellation.clone());
            let tool_name = canopy_core::tool_utils::canonical_tool_name(&call.name);
            let input = Self::input_map(call);
            let tool_use_id = Self::tool_use_id();
            let tool_call_id = call
                .provider_call_id
                .as_deref()
                .or(Some(call.call_id.as_str()));

            let pre_output = hook_host
                .fire_pre_tool_use(
                    &self.session_id,
                    &tool_name,
                    input.clone(),
                    &tool_use_id,
                    tool_call_id,
                )
                .await;
            if cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err("Tool hook execution aborted".to_owned());
            }
            match pre_tool_hook_decision(pre_output.as_ref()) {
                PreToolHookDecision::Proceed => {}
                PreToolHookDecision::Block(reason) => return Err(reason),
                PreToolHookDecision::Ask(reason) => {
                    if !confirm_pre_tool_hook_ask(&tool_name, &reason)? {
                        return Err(format!(
                            "Permission denied by PreToolUse hook for `{tool_name}`: {reason}"
                        ));
                    }
                }
            }

            match self.inner.execute(call).await {
                Ok(mut output) => {
                    let hook_state = self.call_hook_state.take_outcome(call);
                    let hook_input = hook_state
                        .effective_tool_input
                        .unwrap_or_else(|| input.clone());
                    if cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        let error = "Tool execution aborted".to_owned();
                        let failure_output = hook_host
                            .fire_post_tool_use_failure(
                                &self.session_id,
                                &tool_name,
                                hook_input,
                                &error,
                                true,
                                &tool_use_id,
                                tool_call_id,
                            )
                            .await;
                        return Err(Self::append_hook_context(error, failure_output.as_ref()));
                    }
                    let mut response = serde_json::Map::new();
                    let llm_content = if output.parts.is_empty() {
                        Value::String(output.output.clone())
                    } else {
                        let mut parts = Vec::with_capacity(output.parts.len() + 1);
                        if !output.output.is_empty() {
                            parts.push(serde_json::json!({"text": output.output}));
                        }
                        parts.extend(output.parts.clone());
                        Value::Array(parts)
                    };
                    response.insert("llmContent".to_owned(), llm_content);
                    if let Some(display) = output.display.as_ref() {
                        response.insert("returnDisplay".to_owned(), display.clone());
                    }
                    let post_output = hook_host
                        .fire_post_tool_use(
                            &self.session_id,
                            &tool_name,
                            hook_input,
                            response,
                            &tool_use_id,
                            tool_call_id,
                        )
                        .await;
                    if cancellation
                        .as_ref()
                        .is_some_and(CancellationToken::is_cancelled)
                    {
                        return Err("Tool hook execution aborted".to_owned());
                    }
                    if let Some(reason) = post_tool_stop_reason(post_output.as_ref()) {
                        return Err(reason);
                    }
                    let context = post_output
                        .as_ref()
                        .and_then(|hook_output| {
                            hook_output
                                .value
                                .pointer("/hookSpecificOutput/additionalContext")
                        })
                        .and_then(Value::as_str)
                        .filter(|context| !context.is_empty())
                        .map(|context| context.replace('<', "&lt;").replace('>', "&gt;"));
                    if let Some(context) = context {
                        self.post_context
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(call.call_id.clone(), context);
                    }
                    if let Some(Value::Array(artifacts)) = post_output.as_ref().and_then(|output| {
                        output
                            .value
                            .pointer("/hookSpecificOutput/artifacts")
                            .cloned()
                    }) {
                        output
                            .artifacts
                            .extend(artifacts.into_iter().filter(tool_artifact_like));
                    }
                    Ok(output)
                }
                Err(error) => {
                    let hook_state = self.call_hook_state.take_outcome(call);
                    if hook_state.suppress_post_tool_use_failure
                        || (hook_state.entered_native_executor
                            && !hook_state.tool_execution_started)
                    {
                        return Err(error);
                    }
                    let hook_input = hook_state
                        .effective_tool_input
                        .unwrap_or_else(|| input.clone());
                    let failure_output = hook_host
                        .fire_post_tool_use_failure(
                            &self.session_id,
                            &tool_name,
                            hook_input,
                            &error,
                            cancellation
                                .as_ref()
                                .is_some_and(CancellationToken::is_cancelled),
                            &tool_use_id,
                            tool_call_id,
                        )
                        .await;
                    Err(Self::append_hook_context(error, failure_output.as_ref()))
                }
            }
        })
    }

    fn cancellation_token_for_current_prompt(&self) -> Option<CancellationToken> {
        self.inner.cancellation_token_for_current_prompt()
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
            let delegated = self
                .inner
                .additional_context_after_tool_use(call, result_file_paths)
                .await;
            let hook_context = self
                .post_context
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&call.call_id);
            match (delegated, hook_context) {
                (Some(delegated), Some(hook_context)) => {
                    Some(format!("{delegated}\n\n{hook_context}"))
                }
                (Some(context), None) | (None, Some(context)) => Some(context),
                (None, None) => None,
            }
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

fn post_tool_stop_reason(output: Option<&SpecificHookOutput>) -> Option<String> {
    let value = &output?.value;
    (value.get("continue").and_then(Value::as_bool) == Some(false)
        || matches!(
            value.get("decision").and_then(Value::as_str),
            Some("deny" | "block")
        ))
    .then(|| {
        value
            .get("stopReason")
            .and_then(truthy_string)
            .or_else(|| value.get("reason").and_then(truthy_string))
            .unwrap_or("No reason provided")
            .to_owned()
    })
}

fn truthy_string(value: &Value) -> Option<&str> {
    value.as_str().filter(|value| !value.is_empty())
}

fn tool_artifact_like(value: &Value) -> bool {
    let Some(artifact) = value.as_object() else {
        return false;
    };
    let optional_string = |key: &str| artifact.get(key).is_none_or(Value::is_string);
    artifact.get("title").is_some_and(Value::is_string)
        && [
            "kind",
            "storage",
            "description",
            "workspacePath",
            "managedId",
            "url",
            "mimeType",
        ]
        .iter()
        .all(|key| optional_string(key))
        && artifact.get("sizeBytes").is_none_or(|value| {
            value
                .as_u64()
                .is_some_and(|number| number <= 9_007_199_254_740_991)
                || value.as_f64().is_some_and(|number| {
                    number.is_finite()
                        && number >= 0.0
                        && number.fract() == 0.0
                        && number <= 9_007_199_254_740_991.0
                })
        })
        && artifact.get("metadata").is_none_or(|value| {
            value.as_object().is_some_and(|metadata| {
                metadata.values().all(|item| {
                    item.is_null() || item.is_string() || item.is_boolean() || item.is_number()
                })
            })
        })
}

fn load_extension_hooks(
    extension: &canopy_core::services::at_resource_references::LocalExtensionReference,
) -> Result<Option<Value>, String> {
    if extension.path.as_os_str().is_empty() {
        return Ok(None);
    }
    let canonical_root = extension
        .path
        .canonicalize()
        .map_err(|error| format!("could not canonicalize active extension root: {error}"))?;
    let manifest_path = canonical_root.join("canopy-extension.json");
    let canonical_manifest = match manifest_path.canonicalize() {
        Ok(path) if path.starts_with(&canonical_root) && path.is_file() => path,
        Ok(_) => return Err("extension manifest resolves outside its active root".to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("could not resolve extension manifest: {error}")),
    };
    let file = File::open(canonical_manifest)
        .map_err(|error| format!("could not open extension manifest: {error}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_EXTENSION_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("could not read extension manifest: {error}"))?;
    if bytes.len() as u64 > MAX_EXTENSION_MANIFEST_BYTES {
        return Err("extension manifest exceeds the 1 MiB read limit".to_owned());
    }
    let manifest: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("extension manifest is not valid JSON: {error}"))?;
    Ok(manifest.get("hooks").cloned())
}

/// Load a read-only registry snapshot for the interactive hooks browser.
///
/// Unlike `CliPromptHookHost::new`, this path does not require a supported
/// runtime event or an available prompt model. It also intentionally ignores
/// `disableAllHooks`, since the UI needs to show the configured count while
/// explaining that execution is disabled. Extension display metadata is
/// attached only to this snapshot; runtime hook configs are not changed.
pub fn load_registry_for_hooks_ui(
    user_hooks: Option<Value>,
    project_hooks: Option<Value>,
    active_extensions: &[canopy_core::services::at_resource_references::LocalExtensionReference],
) -> HookRegistry {
    let base = load_hook_entries(
        &HookConfigSources {
            user_hooks,
            project_hooks,
            extensions: Vec::new(),
        },
        None,
    );
    for issue in &base.issues {
        eprintln!("[CANOPY] {}", issue.message);
    }
    let mut entries = base.entries;

    for extension in active_extensions {
        let hooks = match load_extension_hooks(extension) {
            Ok(Some(hooks)) => hooks,
            Ok(None) => continue,
            Err(error) => {
                eprintln!(
                    "[CANOPY] Skipping hooks for extension {}: {error}",
                    extension.name
                );
                continue;
            }
        };
        let loaded = load_hook_entries(
            &HookConfigSources {
                extensions: vec![ExtensionHookSource {
                    is_active: true,
                    hooks: Some(hooks),
                }],
                ..HookConfigSources::default()
            },
            None,
        );
        for issue in &loaded.issues {
            eprintln!("[CANOPY] {}", issue.message);
        }
        for mut entry in loaded.entries {
            if let Some(config) = entry.config.as_object_mut() {
                config.insert(
                    "sourceDisplay".to_owned(),
                    Value::String(
                        extension
                            .display_name
                            .as_deref()
                            .unwrap_or(&extension.name)
                            .to_owned(),
                    ),
                );
                config.insert(
                    "sourcePath".to_owned(),
                    Value::String(extension.path.to_string_lossy().into_owned()),
                );
            }
            entries.push(entry);
        }
    }

    let mut registry = HookRegistry::new();
    registry.initialize(entries);
    registry
}

#[derive(Clone)]
struct CliPromptModelResolver {
    settings: Value,
    effective_env: HashMap<String, String>,
    main_model: String,
    current_provider_kind: RunProviderKind,
    current_base_url: String,
    proxy_url: Option<String>,
    current_model: Option<ResolvedPromptModel<PromptProviderBackend>>,
    current_pipeline: OpenAiPipelineConfig,
}

impl CliPromptModelResolver {
    fn new(
        settings: Value,
        effective_env: HashMap<String, String>,
        runtime_provider: &RunRuntimeProvider,
        main_model: &str,
        current_provider_kind: RunProviderKind,
        current_pipeline: &OpenAiPipelineConfig,
        proxy_url: Option<&str>,
    ) -> Result<Self, String> {
        let current_base_url = match runtime_provider {
            RunRuntimeProvider::OpenAiCompatible(provider) => provider.base_url.clone(),
            RunRuntimeProvider::Anthropic(provider) => provider.base_url.clone(),
            RunRuntimeProvider::Gemini(provider) => provider.base_url.clone(),
        };
        let current_model =
            backend_for_current_provider(runtime_provider, main_model, current_pipeline)?;
        Ok(Self {
            settings,
            effective_env,
            main_model: main_model.to_owned(),
            current_provider_kind,
            current_base_url,
            proxy_url: proxy_url.map(str::to_owned),
            current_model,
            current_pipeline: current_pipeline.clone(),
        })
    }

    fn resolve_model(
        &self,
        selector: &str,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedPromptModel<PromptProviderBackend>, String> {
        if cancellation.is_cancelled() {
            return Err("Prompt hook execution aborted".to_owned());
        }
        let selector = selector.trim();
        let selector = if selector.is_empty() || selector == "inherit" {
            self.main_model.as_str()
        } else if selector == "fast" {
            self.settings
                .get("fastModel")
                .and_then(Value::as_str)
                .filter(|model| !model.trim().is_empty() && model.trim() != "fast")
                .unwrap_or(&self.main_model)
                .trim()
        } else {
            selector
        };
        let selector = if selector.is_empty() || selector == "inherit" || selector == "fast" {
            self.main_model.as_str()
        } else {
            selector
        };
        let (selector_model, requested_base_url) = selector
            .split_once('\0')
            .map(|(model, base_url)| {
                (
                    model,
                    (!base_url.trim().is_empty()).then(|| base_url.to_owned()),
                )
            })
            .unwrap_or((selector, None));
        let (required_protocol, model_id) = match selector_model.split_once(':') {
            Some((prefix, model_id)) => match parse_auth_type(prefix.trim())
                .and_then(RunProviderKind::from_auth_type)
            {
                Some(protocol) if !model_id.trim().is_empty() => (Some(protocol), model_id.trim()),
                Some(_) => {
                    return Err("Prompt hook model selector has an empty model ID".to_owned());
                }
                None => (None, selector_model),
            },
            None => (None, selector_model),
        };
        if model_id == self.main_model
            && required_protocol.is_none_or(|protocol| protocol == self.current_provider_kind)
            && requested_base_url
                .as_deref()
                .is_none_or(|base_url| base_url == self.current_base_url)
        {
            return self.current_model.clone().ok_or_else(|| {
                "Current model provider is unavailable for prompt hooks".to_owned()
            });
        }
        let preferred_base_url = requested_base_url
            .as_deref()
            .or_else(|| Some(self.current_base_url.as_str()));
        let configured = if let Some(protocol) = required_protocol {
            find_configured_run_model_provider(
                &self.settings,
                model_id,
                Some(protocol),
                requested_base_url.as_deref(),
            )
        } else {
            find_configured_run_model_provider(
                &self.settings,
                model_id,
                Some(self.current_provider_kind),
                preferred_base_url,
            )
            .or_else(|| {
                find_configured_run_model_provider(
                    &self.settings,
                    model_id,
                    None,
                    requested_base_url.as_deref(),
                )
            })
        }
        .filter(|(_, model)| {
            requested_base_url
                .as_deref()
                .is_none_or(|requested_base_url| {
                    model.get("baseUrl").and_then(Value::as_str) == Some(requested_base_url)
                })
        })
        .ok_or_else(|| {
            format!(
                "Prompt hook model {selector:?} is not present in a supported modelProviders entry"
            )
        })?;
        let (protocol, model_config) = configured;
        let provider_id = self
            .settings
            .get("modelProviders")
            .and_then(Value::as_object)
            .and_then(|providers| {
                providers.iter().find_map(|(provider_id, models)| {
                    let resolved_protocol = configured_run_provider_protocol(
                        provider_id,
                        self.settings.get("providerProtocol"),
                    )?;
                    if resolved_protocol != protocol {
                        return None;
                    }
                    models
                        .as_array()?
                        .iter()
                        .any(|model| std::ptr::eq(model, model_config))
                        .then_some(provider_id.as_str())
                })
            })
            .unwrap_or(protocol.auth_type().as_str());
        self.build_provider_model(provider_id, protocol, model_id, model_config)
    }

    fn build_provider_model(
        &self,
        provider_id: &str,
        protocol: RunProviderKind,
        model_id: &str,
        model_config: &Value,
    ) -> Result<ResolvedPromptModel<PromptProviderBackend>, String> {
        let provider_base_url = nonempty_runtime_setting_string(model_config.get("baseUrl"));
        let env_base_url = self
            .effective_env
            .get(protocol.base_url_env())
            .filter(|value| !value.trim().is_empty())
            .map(String::as_str);
        let current_provider_base_url =
            (protocol == self.current_provider_kind).then_some(self.current_base_url.as_str());
        let base_url = provider_base_url
            .or(current_provider_base_url)
            .or(env_base_url)
            .unwrap_or_else(|| protocol.default_base_url());
        let api_key = match nonempty_runtime_setting_string(model_config.get("envKey")) {
            Some(env_key) => self
                .effective_env
                .get(env_key)
                .filter(|value| !value.trim().is_empty())
                .cloned(),
            None => self.provider_api_key(protocol).map(str::to_owned),
        };
        let generation = NativeModelGenerationConfig::resolve(
            &self.settings,
            Some(model_config),
            model_id,
            &self.effective_env,
        );

        let backend = match protocol {
            RunProviderKind::OpenAiCompatible => {
                let mut config = OpenAiCompatibleConfig {
                    base_url: base_url.to_owned(),
                    api_key,
                    proxy: self.proxy_url.clone(),
                    ..OpenAiCompatibleConfig::default()
                };
                generation.apply_to_openai_compatible(&mut config);
                let client = Arc::new(OpenAiCompatibleClient::new(config).map_err(|error| {
                    format!("could not create prompt hook model client: {error}")
                })?);
                let profile = detect_openai_provider_profile(
                    Some(provider_id),
                    Some(base_url),
                    Some(model_id),
                    None,
                );
                let pipeline =
                    self.pipeline_for(model_id, provider_id, base_url, profile, &generation);
                PromptProviderBackend::OpenAiCompatible { client, pipeline }
            }
            RunProviderKind::Anthropic => {
                let mut config = AnthropicProviderConfig {
                    model: model_id.to_owned(),
                    base_url: base_url.to_owned(),
                    api_key,
                    proxy: self.proxy_url.clone(),
                    cli_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                    ..AnthropicProviderConfig::default()
                };
                generation.apply_to_anthropic(&mut config);
                let client = Arc::new(AnthropicMessagesClient::new(config).map_err(|error| {
                    format!("could not create prompt hook model client: {error}")
                })?);
                PromptProviderBackend::Anthropic(client)
            }
            RunProviderKind::Gemini => {
                let mut config = GeminiProviderConfig {
                    model: model_id.to_owned(),
                    base_url: base_url.to_owned(),
                    api_key,
                    proxy: self.proxy_url.clone(),
                    user_agent: Some(format!(
                        "CanopyCode/{} ({}; {})",
                        env!("CARGO_PKG_VERSION"),
                        std::env::consts::OS,
                        std::env::consts::ARCH
                    )),
                    ..GeminiProviderConfig::default()
                };
                generation.apply_to_gemini(&mut config);
                let client = Arc::new(GeminiNativeClient::new(config).map_err(|error| {
                    format!("could not create prompt hook model client: {error}")
                })?);
                PromptProviderBackend::Gemini {
                    client,
                    sampling_params: generation.sampling_params.clone(),
                }
            }
        };
        Ok(ResolvedPromptModel {
            model: model_id.to_owned(),
            reasoning_configured: generation
                .reasoning
                .as_ref()
                .is_some_and(|value| value != &Value::Bool(false)),
            handle: backend,
        })
    }

    fn provider_api_key(&self, protocol: RunProviderKind) -> Option<&str> {
        let env_key = match protocol {
            RunProviderKind::OpenAiCompatible => "OPENAI_API_KEY",
            RunProviderKind::Anthropic => "ANTHROPIC_API_KEY",
            RunProviderKind::Gemini => "GEMINI_API_KEY",
        };
        self.effective_env
            .get(env_key)
            .or_else(|| {
                (protocol == RunProviderKind::OpenAiCompatible)
                    .then(|| self.effective_env.get("CANOPY_API_KEY"))
                    .flatten()
            })
            .map(String::as_str)
            .or_else(|| {
                (protocol == self.current_provider_kind)
                    .then(|| {
                        self.settings
                            .pointer("/security/auth/apiKey")
                            .and_then(Value::as_str)
                    })
                    .flatten()
            })
    }

    fn pipeline_for(
        &self,
        model_id: &str,
        provider_id: &str,
        base_url: &str,
        profile: OpenAiProviderProfile,
        generation: &NativeModelGenerationConfig,
    ) -> OpenAiPipelineConfig {
        let mut pipeline = self.current_pipeline.clone();
        pipeline.request_context.model = model_id.to_owned();
        pipeline.request_context.modalities = generation.modalities;
        pipeline.request_context.split_tool_media = generation.split_tool_media;
        pipeline.request_context.tool_result_content_format = generation.tool_result_content_format;
        pipeline.sampling_params = generation.sampling_params.clone();
        pipeline.reasoning = generation.reasoning.clone();
        pipeline.retry_max_attempts = generation.retry_max_attempts;
        pipeline.retry_initial_delay_ms = generation.retry_initial_delay_ms;
        pipeline.retry_max_delay_ms = generation.retry_max_delay_ms;
        pipeline.retry_error_codes = generation.retry_error_codes.clone();
        pipeline.schema_compliance = generation.schema_compliance;
        pipeline.enable_cache_control = generation.enable_cache_control;
        pipeline.thinking_mandatory = generation.thinking_mandatory;
        pipeline.extra_body = generation.extra_body.clone();
        pipeline.provider_profile = profile;
        pipeline.prefix_cache_config = OpenAiPrefixCacheConfig {
            auth_mode: if provider_id.eq_ignore_ascii_case("openai") {
                OpenAiAuthMode::OpenAi
            } else {
                OpenAiAuthMode::Other
            },
            base_url: Some(base_url.to_owned()),
        };
        pipeline
    }
}

impl PromptProviderModelResolver for CliPromptModelResolver {
    fn main_model(&self) -> String {
        self.main_model.clone()
    }

    fn current_model(&self) -> Option<ResolvedPromptModel<PromptProviderBackend>> {
        self.current_model.clone()
    }

    fn resolve_for_model<'a>(
        &'a self,
        model: &'a str,
        cancellation: &'a CancellationToken,
    ) -> canopy_core::hooks::prompt_provider::PromptProviderResolveFuture<'a> {
        Box::pin(async move { self.resolve_model(model, cancellation) })
    }
}

fn backend_for_current_provider(
    provider: &RunRuntimeProvider,
    model: &str,
    pipeline: &OpenAiPipelineConfig,
) -> Result<Option<ResolvedPromptModel<PromptProviderBackend>>, String> {
    let (handle, reasoning_configured) = match provider {
        RunRuntimeProvider::OpenAiCompatible(config) => {
            let client = Arc::new(OpenAiCompatibleClient::new(config.clone()).map_err(
                |error| format!("could not create current prompt hook client: {error}"),
            )?);
            (
                PromptProviderBackend::OpenAiCompatible {
                    client,
                    pipeline: pipeline.clone(),
                },
                pipeline
                    .reasoning
                    .as_ref()
                    .is_some_and(|value| value != &Value::Bool(false)),
            )
        }
        RunRuntimeProvider::Anthropic(config) => {
            let client = Arc::new(AnthropicMessagesClient::new(config.clone()).map_err(
                |error| format!("could not create current prompt hook client: {error}"),
            )?);
            (
                PromptProviderBackend::Anthropic(client),
                config
                    .reasoning
                    .as_ref()
                    .is_some_and(|value| value != &Value::Bool(false)),
            )
        }
        RunRuntimeProvider::Gemini(config) => {
            let client = Arc::new(GeminiNativeClient::new(config.clone()).map_err(|error| {
                format!("could not create current prompt hook client: {error}")
            })?);
            (
                PromptProviderBackend::Gemini {
                    client,
                    sampling_params: pipeline.sampling_params.clone(),
                },
                pipeline
                    .reasoning
                    .as_ref()
                    .is_some_and(|value| value != &Value::Bool(false)),
            )
        }
    };
    Ok(Some(ResolvedPromptModel {
        model: model.to_owned(),
        reasoning_configured,
        handle,
    }))
}

#[allow(dead_code)]
fn _provider_is_supported(protocol: RunProviderKind) -> bool {
    matches!(
        protocol,
        RunProviderKind::OpenAiCompatible | RunProviderKind::Anthropic | RunProviderKind::Gemini
    )
}
