//! Agent-runtime declarations and forwarding for Computer Use tools.
//!
//! This ports the registration and call path from
//! `packages/core/src/tools/computer-use/index.ts` and `tool.ts`. The runtime
//! has no lazy tool registry yet, so callers register a filtered function
//! declaration group with `AgentRuntimeConfig` and compose this executor around
//! their normal tool executor. Host authorization and bootstrap operations are
//! injected; missing adapters fail closed before the driver is called.

use super::bootstrap::{
    BootstrapFuture, BootstrapHost, BootstrapOptions, ComputerUseClient as BootstrapClient,
    PermissionKind, PermissionProbeResult, StatusDaemon, run_bootstrap,
};
use super::client::{ComputerUseClient, ComputerUseClientOptions, ComputerUseProgress};
use super::result::{build_display_text, build_llm_content};
use super::schemas::{canopy_tool_name, computer_use_tool_names, computer_use_tool_schema};
use super::tool_policy::{coerce_types, is_high_risk_call};
use crate::agent_runtime::AgentToolExecutor;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::turn::ToolCallRequestInfo;
use crate::utils::cancellation::CancellationToken;
use serde_json::{Map, Value, json};
use std::collections::BTreeSet;
use std::future::Future;
use std::io::{self, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use thiserror::Error;

const COMPUTER_USE_PREFIX: &str = "computer_use__";
/// Maximum serialized argument size accepted from a model tool call.
pub const MAX_COMPUTER_USE_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_ARGUMENT_NODES: usize = 10_000;
const MAX_ARGUMENT_DEPTH: usize = 64;

pub type ComputerUseAuthorizationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
pub type ComputerUseClientFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Per-action host permission check. Implementations should use `high_risk`
/// to keep the sensitive calls from receiving silent auto-approval in modes
/// that normally approve informational tools.
pub trait ComputerUseCallAuthorizer: Send + Sync {
    fn authorize<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        upstream_name: &'a str,
        params: &'a Map<String, Value>,
        high_risk: bool,
    ) -> ComputerUseAuthorizationFuture<'a>;

    /// Best-effort host notification when an in-flight desktop action is
    /// cancelled after dispatch. The driver may already have applied it.
    fn report_uncertain_cancellation(&self, _upstream_name: &str) {}
}

/// Small driver boundary used by the adapter and its tests. A production host
/// can wrap [`ComputerUseClient`] with [`NativeComputerUseDriverClient`].
pub trait ComputerUseDriverClient: Send + Sync {
    fn is_started(&self) -> bool;

    fn start<'a>(
        &'a self,
        on_progress: Option<ComputerUseProgress>,
    ) -> ComputerUseClientFuture<'a, ()>;

    fn set_max_image_dimension(&self, value: Option<u32>);

    fn set_idle_timeout_ms(&self, value: Option<f64>);

    fn call_tool<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
    ) -> ComputerUseClientFuture<'a, Value>;

    fn call_tool_with_cancellation<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
        cancellation: Option<CancellationToken>,
        on_request_started: Option<Arc<dyn Fn() + Send + Sync>>,
        on_request_completed: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> ComputerUseClientFuture<'a, Value> {
        Box::pin(async move {
            let result = if let Some(token) = cancellation {
                if token.is_cancelled() {
                    Err("MCP operation cancelled".to_owned())
                } else {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => Err("MCP operation cancelled".to_owned()),
                        result = async {
                            if let Some(on_request_started) = on_request_started.as_ref() {
                                on_request_started();
                            }
                            self.call_tool(name, arguments).await
                        } => result,
                    }
                }
            } else {
                if let Some(on_request_started) = on_request_started.as_ref() {
                    on_request_started();
                }
                self.call_tool(name, arguments).await
            };
            if result.is_ok()
                && let Some(on_request_completed) = on_request_completed.as_ref()
            {
                on_request_completed();
            }
            result
        })
    }
}

/// Adapter for the repository's shared Rust CUA MCP client.
#[derive(Clone)]
pub struct NativeComputerUseDriverClient {
    client: Arc<ComputerUseClient>,
}

impl NativeComputerUseDriverClient {
    pub fn new(client: Arc<ComputerUseClient>) -> Self {
        Self { client }
    }

    pub fn from_options(options: ComputerUseClientOptions) -> Self {
        Self::new(Arc::new(ComputerUseClient::new(options)))
    }

    pub fn client(&self) -> &Arc<ComputerUseClient> {
        &self.client
    }
}

impl ComputerUseDriverClient for NativeComputerUseDriverClient {
    fn is_started(&self) -> bool {
        self.client.is_started()
    }

    fn start<'a>(
        &'a self,
        on_progress: Option<ComputerUseProgress>,
    ) -> ComputerUseClientFuture<'a, ()> {
        Box::pin(async move {
            self.client
                .start(on_progress)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn set_max_image_dimension(&self, value: Option<u32>) {
        self.client.set_max_image_dimension(value);
    }

    fn set_idle_timeout_ms(&self, value: Option<f64>) {
        self.client.set_idle_timeout_ms(value);
    }

    fn call_tool<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
    ) -> ComputerUseClientFuture<'a, Value> {
        Box::pin(async move {
            self.client
                .call_tool(name, arguments)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn call_tool_with_cancellation<'a>(
        &'a self,
        name: &'a str,
        arguments: Map<String, Value>,
        cancellation: Option<CancellationToken>,
        on_request_started: Option<Arc<dyn Fn() + Send + Sync>>,
        on_request_completed: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> ComputerUseClientFuture<'a, Value> {
        Box::pin(async move {
            self.client
                .call_tool_with_cancellation_and_dispatch(
                    name,
                    arguments,
                    cancellation,
                    on_request_started,
                    on_request_completed,
                )
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// Runtime values that would normally come from Canopy's `Config` object.
/// `auto_approve_install` must only be set by a host after its normal tool
/// scheduler has approved the current action.
#[derive(Clone, Debug)]
pub struct ComputerUseAdapterOptions {
    pub bootstrap: BootstrapOptions,
    pub max_image_dimension: Option<u32>,
    pub idle_timeout_ms: Option<f64>,
}

impl ComputerUseAdapterOptions {
    pub fn new(bootstrap: BootstrapOptions) -> Self {
        Self {
            bootstrap,
            max_image_dimension: None,
            idle_timeout_ms: None,
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ComputerUseAdapterError {
    #[error("computer-use declaration name collision for `{0}`")]
    ToolNameCollision(String),
}

/// Computer Use executor component. It begins with no enabled tools; the host
/// must call [`Self::register_enabled_tools`] so its feature flag and tool
/// allowlist are applied before declarations or calls are accepted.
pub struct ComputerUseAgentToolAdapter {
    client: Arc<dyn ComputerUseDriverClient>,
    options: ComputerUseAdapterOptions,
    authorizer: Option<Arc<dyn ComputerUseCallAuthorizer>>,
    bootstrap_host: Option<Arc<dyn BootstrapHost>>,
    registered_tools: RwLock<BTreeSet<String>>,
}

impl ComputerUseAgentToolAdapter {
    pub fn new(
        client: Arc<dyn ComputerUseDriverClient>,
        options: ComputerUseAdapterOptions,
    ) -> Self {
        Self {
            client,
            options,
            authorizer: None,
            bootstrap_host: None,
            registered_tools: RwLock::new(BTreeSet::new()),
        }
    }

    pub fn with_authorizer(mut self, authorizer: Arc<dyn ComputerUseCallAuthorizer>) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    pub fn with_bootstrap_host(mut self, host: Arc<dyn BootstrapHost>) -> Self {
        self.bootstrap_host = Some(host);
        self
    }

    /// Port of the TypeScript registration loop. The feature flag gates the
    /// whole group; `is_tool_enabled` represents the caller's
    /// `PermissionManager.isToolEnabled()` check for each `computer_use__*`
    /// name. The registered route set is replaced atomically on each call.
    pub fn register_enabled_tools(
        &self,
        computer_use_enabled: bool,
        mut is_tool_enabled: impl FnMut(&str) -> bool,
        declarations: &mut Vec<Value>,
    ) -> Result<usize, ComputerUseAdapterError> {
        let mut registered = BTreeSet::new();
        let mut function_declarations = Vec::new();

        if computer_use_enabled {
            for upstream_name in computer_use_tool_names() {
                let canopy_name = canopy_tool_name(upstream_name)
                    .expect("catalog tool name must have a Canopy name");
                if !is_tool_enabled(&canopy_name) {
                    continue;
                }
                let schema = computer_use_tool_schema(upstream_name)
                    .expect("catalog tool name must have a schema");
                if !registered.insert(upstream_name.clone()) {
                    return Err(ComputerUseAdapterError::ToolNameCollision(canopy_name));
                }
                function_declarations.push(json!({
                    "name": canopy_name,
                    "description": schema.description,
                    "parametersJsonSchema": schema.parameter_schema,
                }));
            }
        }

        *write_lock(&self.registered_tools) = registered;
        let count = function_declarations.len();
        if count > 0 {
            declarations.push(json!({"functionDeclarations": function_declarations}));
        }
        Ok(count)
    }

    pub fn compose<E>(self: &Arc<Self>, fallback: E) -> ComputerUseComposedToolExecutor<E> {
        ComputerUseComposedToolExecutor {
            adapter: Arc::clone(self),
            fallback,
        }
    }

    pub fn is_registered(&self, canopy_name: &str) -> bool {
        canopy_name
            .strip_prefix(COMPUTER_USE_PREFIX)
            .is_some_and(|upstream_name| read_lock(&self.registered_tools).contains(upstream_name))
    }

    fn handles_name(&self, name: &str) -> bool {
        name.starts_with(COMPUTER_USE_PREFIX)
    }

    async fn execute_call(
        &self,
        call: &ToolCallRequestInfo,
        cancellation: Option<CancellationToken>,
    ) -> Result<ToolExecutionOutput, String> {
        let upstream_name = call
            .name
            .strip_prefix(COMPUTER_USE_PREFIX)
            .ok_or_else(|| format!("Not a Computer Use tool: `{}`.", call.name))?;
        let schema = computer_use_tool_schema(upstream_name)
            .ok_or_else(|| format!("Unknown Computer Use tool `{upstream_name}`."))?;
        if !self.is_registered(&call.name) {
            return Err(format!(
                "Computer Use tool `{}` is not enabled for this session.",
                call.name
            ));
        }

        let raw_params = call
            .args
            .as_object()
            .ok_or_else(|| "Computer Use tool arguments must be a JSON object.".to_owned())?;
        validate_bounded_json(raw_params)?;
        let params = coerce_types(raw_params, &schema.parameter_schema);
        validate_params(&params, &schema.parameter_schema)?;

        // Do not bootstrap or call the desktop until both host policies exist.
        let authorizer = self.authorizer.as_ref().ok_or_else(|| {
            "Computer Use is unavailable: host authorization is not configured.".to_owned()
        })?;
        let bootstrap_host = self.bootstrap_host.as_ref().ok_or_else(|| {
            "Computer Use is unavailable: host bootstrap is not configured.".to_owned()
        })?;
        let high_risk = is_high_risk_call(upstream_name, &params);
        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(format!(
                "ACP prompt was cancelled before `{upstream_name}` was approved; no desktop action was sent."
            ));
        }
        let authorization = authorizer.authorize(call, upstream_name, &params, high_risk);
        if let Some(token) = cancellation.as_ref() {
            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    return Err(format!(
                        "ACP prompt was cancelled before `{upstream_name}` was approved; no desktop action was sent."
                    ));
                }
                result = authorization => result?,
            }
        } else {
            authorization.await?;
        }

        self.client
            .set_max_image_dimension(self.options.max_image_dimension);
        self.client
            .set_idle_timeout_ms(self.options.idle_timeout_ms);
        let client = BootstrapClientRef(self.client.as_ref());
        let host = BootstrapHostRef(bootstrap_host.as_ref());
        if let Some(token) = cancellation.as_ref() {
            tokio::select! {
                biased;
                _ = token.cancelled() => {
                    return Err(format!(
                        "ACP prompt was cancelled before `{upstream_name}` reached the desktop; no desktop action was sent."
                    ));
                }
                result = run_bootstrap(&client, &host, &self.options.bootstrap) => {
                    result.map_err(|error| format!("Computer Use bootstrap failed: {error}"))?;
                }
            }
        } else {
            run_bootstrap(&client, &host, &self.options.bootstrap)
                .await
                .map_err(|error| format!("Computer Use bootstrap failed: {error}"))?;
        }

        if cancellation
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(format!(
                "ACP prompt was cancelled before `{upstream_name}` was sent to the desktop driver; no desktop action was sent."
            ));
        }

        let can_change_desktop = !is_read_only_computer_use_tool(upstream_name);
        const CALL_NOT_DISPATCHED: u8 = 0;
        const CALL_DISPATCHED: u8 = 1;
        const CALL_COMPLETED: u8 = 2;
        const CANCELLATION_REPORTED: u8 = 3;
        let call_state = Arc::new(AtomicU8::new(CALL_NOT_DISPATCHED));
        let dispatch_state = Arc::clone(&call_state);
        let on_request_started: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = dispatch_state.compare_exchange(
                CALL_NOT_DISPATCHED,
                CALL_DISPATCHED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
        let response_state = Arc::clone(&call_state);
        let on_request_completed: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = response_state.compare_exchange(
                CALL_DISPATCHED,
                CALL_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
        let cancellation_reporter = if can_change_desktop {
            cancellation.as_ref().map(|token| {
                let token = token.clone();
                let state = Arc::clone(&call_state);
                let authorizer = Arc::clone(authorizer);
                let name = upstream_name.to_owned();
                tokio::spawn(async move {
                    let _ = token.cancelled().await;
                    if state
                        .compare_exchange(
                            CALL_DISPATCHED,
                            CANCELLATION_REPORTED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        authorizer.report_uncertain_cancellation(&name);
                    }
                })
            })
        } else {
            None
        };

        let cancellation_for_result = cancellation.clone();
        let result = self
            .client
            .call_tool_with_cancellation(
                upstream_name,
                params,
                cancellation,
                Some(on_request_started),
                Some(on_request_completed),
            )
            .await;
        if result.is_ok() {
            let _ = call_state.compare_exchange(
                CALL_DISPATCHED,
                CALL_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        if let Some(task) = cancellation_reporter {
            task.abort();
        }

        let cancelled = result.is_err()
            && cancellation_for_result
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled);
        let report_now = can_change_desktop
            && cancelled
            && call_state
                .compare_exchange(
                    CALL_DISPATCHED,
                    CANCELLATION_REPORTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok();
        if report_now {
            authorizer.report_uncertain_cancellation(upstream_name);
        }
        let cancellation_reported = call_state.load(Ordering::Acquire) == CANCELLATION_REPORTED;
        if !cancellation_reported {
            let _ = call_state.compare_exchange(
                CALL_NOT_DISPATCHED,
                CALL_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            let _ = call_state.compare_exchange(
                CALL_DISPATCHED,
                CALL_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }

        match result {
            Ok(result) => Ok(project_result(upstream_name, result)),
            Err(_error) if cancellation_reported && can_change_desktop => {
                let message = format!(
                    "Computer Use action `{upstream_name}` was interrupted after its MCP request began dispatching. The driver may have received it and may have completed all or part of the action. Inspect the desktop and application state before retrying."
                );
                Ok(ToolExecutionOutput::with_display(
                    message.clone(),
                    Value::String(message),
                ))
            }
            Err(_error) if cancelled => Ok(ToolExecutionOutput::with_display(
                format!(
                    "Computer Use `{upstream_name}` was cancelled before its request began dispatching; no desktop action was sent."
                ),
                Value::String(format!(
                    "Computer Use `{upstream_name}` was cancelled before its request began dispatching; no desktop action was sent."
                )),
            )),
            Err(error) => Ok(ToolExecutionOutput::with_display(
                format!("Computer Use tool `{upstream_name}` failed: {error}"),
                Value::String(format!("Error: {error}")),
            )),
        }
    }
}

/// Routes enabled `computer_use__*` calls through the adapter and delegates
/// all unrelated tools to the host's normal executor.
pub struct ComputerUseComposedToolExecutor<E> {
    adapter: Arc<ComputerUseAgentToolAdapter>,
    fallback: E,
}

impl<E: AgentToolExecutor> AgentToolExecutor for ComputerUseComposedToolExecutor<E> {
    fn begin_user_prompt<'a>(
        &'a self,
        prompt_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        self.fallback.begin_user_prompt(prompt_id)
    }

    fn take_file_history_snapshot_updates(
        &self,
    ) -> Vec<crate::services::file_history::FileHistorySnapshot> {
        self.fallback.take_file_history_snapshot_updates()
    }

    fn commit_attribution_snapshot(
        &self,
    ) -> Option<crate::services::commit_attribution::AttributionSnapshot> {
        self.fallback.commit_attribution_snapshot()
    }

    fn execute<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
    ) -> Pin<Box<dyn Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>> {
        Box::pin(async move {
            if self.adapter.handles_name(&call.name) {
                self.adapter
                    .execute_call(call, self.fallback.cancellation_token_for_current_prompt())
                    .await
            } else {
                self.fallback.execute(call).await
            }
        })
    }

    fn cancellation_token_for_current_prompt(&self) -> Option<CancellationToken> {
        self.fallback.cancellation_token_for_current_prompt()
    }

    fn is_side_effecting(&self, tool_name: &str) -> bool {
        if self.adapter.handles_name(tool_name) {
            true
        } else {
            self.fallback.is_side_effecting(tool_name)
        }
    }

    fn is_concurrency_safe(&self, call: &ToolCallRequestInfo) -> bool {
        if self.adapter.handles_name(&call.name) {
            false
        } else {
            self.fallback.is_concurrency_safe(call)
        }
    }

    fn additional_context_after_tool_use<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        result_file_paths: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Option<String>> + Send + 'a>> {
        self.fallback
            .additional_context_after_tool_use(call, result_file_paths)
    }
}

fn is_read_only_computer_use_tool(name: &str) -> bool {
    matches!(
        name,
        "check_for_update"
            | "check_permissions"
            | "get_accessibility_tree"
            | "get_agent_cursor_state"
            | "get_config"
            | "get_cursor_position"
            | "get_recording_state"
            | "get_screen_size"
            | "get_window_state"
            | "list_apps"
            | "list_windows"
    )
}

struct BootstrapClientRef<'a>(&'a dyn ComputerUseDriverClient);

impl BootstrapClient for BootstrapClientRef<'_> {
    fn is_started(&self) -> bool {
        self.0.is_started()
    }

    fn start<'a>(
        &'a self,
        _on_progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BootstrapFuture<'a, Result<(), String>> {
        // The concrete CUA client currently accepts only an owned `'static`
        // progress closure. BootstrapHost already receives installer progress;
        // client startup progress is therefore left at the client's own hook.
        self.0.start(None)
    }
}

struct BootstrapHostRef<'a>(&'a dyn BootstrapHost);

impl BootstrapHost for BootstrapHostRef<'_> {
    fn approval_is_granted(
        &self,
        home_dir: PathBuf,
        approval_key: String,
    ) -> BootstrapFuture<'_, bool> {
        self.0.approval_is_granted(home_dir, approval_key)
    }

    fn prompt_install_approval(&self, approval_key: String) -> BootstrapFuture<'_, bool> {
        self.0.prompt_install_approval(approval_key)
    }

    fn save_install_approval(
        &self,
        state: super::InstallState,
    ) -> BootstrapFuture<'_, Result<(), String>> {
        self.0.save_install_approval(state)
    }

    fn install<'a>(
        &'a self,
        on_progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BootstrapFuture<'a, Result<(), String>> {
        self.0.install(on_progress)
    }

    fn start_status_daemon(&self) -> Box<dyn StatusDaemon> {
        self.0.start_status_daemon()
    }

    fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult> {
        self.0.probe_permissions()
    }

    fn open_permission_pane(&self, kind: PermissionKind) {
        self.0.open_permission_pane(kind)
    }

    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    fn update_output(&self, output: &str) {
        self.0.update_output(output)
    }

    fn now_millis(&self) -> u64 {
        self.0.now_millis()
    }

    fn now_iso(&self) -> String {
        self.0.now_iso()
    }

    fn sleep(&self, duration: std::time::Duration) -> BootstrapFuture<'_, ()> {
        self.0.sleep(duration)
    }
}

fn project_result(tool_name: &str, mut result: Value) -> ToolExecutionOutput {
    let Some(object) = result.as_object_mut() else {
        return ToolExecutionOutput::text(format!(
            "Computer Use tool `{tool_name}` returned a malformed result."
        ));
    };
    let content = match object.remove("content") {
        Some(Value::Array(content)) => content,
        _ => Vec::new(),
    };
    let structured_content = object.remove("structuredContent");
    let is_error = object
        .remove("isError")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);

    // Clone only the vector of block values here; media strings are moved
    // again by build_llm_content, avoiding a second base64 string allocation.
    let display_text = build_display_text(&content);
    let llm_content = build_llm_content(content, tool_name, structured_content.as_ref());

    if is_error {
        let error_text = if display_text.is_empty() {
            format!("Tool '{tool_name}' returned isError=true")
        } else {
            display_text
        };
        let display = Value::String(error_text.clone());
        match llm_content {
            Value::String(text) if text.is_empty() => {
                ToolExecutionOutput::with_display(error_text, display)
            }
            Value::String(text) => ToolExecutionOutput::with_display(text, display),
            Value::Array(parts) => {
                let mut output = ToolExecutionOutput::with_parts("", parts);
                output.display = Some(display);
                output
            }
            _ => ToolExecutionOutput::with_display(error_text, display),
        }
    } else {
        let display = Value::String(display_text);
        match llm_content {
            Value::String(text) => ToolExecutionOutput::with_display(text, display),
            Value::Array(parts) => {
                let mut output = ToolExecutionOutput::with_parts("", parts);
                output.display = Some(display);
                output
            }
            _ => ToolExecutionOutput::with_display(String::new(), display),
        }
    }
}

fn validate_bounded_json(params: &Map<String, Value>) -> Result<(), String> {
    let mut pending = vec![(1usize, ValueRef::Object(params))];
    let mut nodes = 0usize;
    while let Some((depth, value)) = pending.pop() {
        nodes += 1;
        if nodes > MAX_ARGUMENT_NODES {
            return Err("Computer Use arguments contain too many values.".to_owned());
        }
        if depth > MAX_ARGUMENT_DEPTH {
            return Err("Computer Use arguments are nested too deeply.".to_owned());
        }
        match value {
            ValueRef::Object(object) => {
                enqueue_children(
                    &mut pending,
                    object.values().map(ValueRef::Value),
                    object.len(),
                    depth + 1,
                    nodes,
                )?;
            }
            ValueRef::Value(Value::Array(array)) => {
                enqueue_children(
                    &mut pending,
                    array.iter().map(ValueRef::Value),
                    array.len(),
                    depth + 1,
                    nodes,
                )?;
            }
            ValueRef::Value(_) => {}
        }
    }

    // Serialize only after depth and node count are bounded. `serde_json`
    // recursively serializes values, so doing this first could walk an
    // adversarially deep programmatically-created argument tree.
    let mut counter = CountingWriter::default();
    let serialization = serde_json::to_writer(&mut counter, params);
    if counter.exceeded {
        return Err(format!(
            "Computer Use arguments exceed the {} byte limit.",
            MAX_COMPUTER_USE_ARGUMENT_BYTES
        ));
    }
    serialization.map_err(|error| format!("Could not inspect Computer Use arguments: {error}"))?;
    Ok(())
}

fn enqueue_children<'a>(
    pending: &mut Vec<(usize, ValueRef<'a>)>,
    children: impl Iterator<Item = ValueRef<'a>>,
    child_count: usize,
    depth: usize,
    processed_nodes: usize,
) -> Result<(), String> {
    if child_count > 0 && depth > MAX_ARGUMENT_DEPTH {
        return Err("Computer Use arguments are nested too deeply.".to_owned());
    }
    let total_known_nodes = processed_nodes
        .saturating_add(pending.len())
        .saturating_add(child_count);
    if total_known_nodes > MAX_ARGUMENT_NODES {
        return Err("Computer Use arguments contain too many values.".to_owned());
    }
    pending.extend(children.map(|value| (depth, value)));
    Ok(())
}

enum ValueRef<'a> {
    Object(&'a Map<String, Value>),
    Value(&'a Value),
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
    exceeded: bool,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self.bytes.saturating_add(bytes.len());
        if next > MAX_COMPUTER_USE_ARGUMENT_BYTES {
            self.bytes = next;
            self.exceeded = true;
            return Err(io::Error::other(
                "Computer Use argument byte limit exceeded",
            ));
        }
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_params(params: &Map<String, Value>, schema: &Value) -> Result<(), String> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err("Computer Use catalog has an unsupported parameter schema.".to_owned());
    }
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| "Computer Use catalog has an invalid properties schema.".to_owned())?;

    if schema.get("additionalProperties").and_then(Value::as_bool) == Some(false) {
        if let Some(key) = params.keys().find(|key| !properties.contains_key(*key)) {
            return Err(format!("Unknown Computer Use parameter `{key}`."));
        }
    }

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for key in required.iter().filter_map(Value::as_str) {
            if !params.contains_key(key) {
                return Err(format!("Missing required Computer Use parameter `{key}`."));
            }
        }
    }

    for (key, value) in params {
        if let Some(property_schema) = properties.get(key) {
            validate_value(value, property_schema, &format!("params.{key}"))?;
        }
    }
    Ok(())
}

fn validate_value(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    let expected_type = schema.get("type").and_then(Value::as_str);
    let matches_type = match expected_type {
        Some("string") => value.is_string(),
        Some("integer") => value
            .as_f64()
            .is_some_and(|number| number.is_finite() && number.fract() == 0.0),
        Some("number") => value.as_f64().is_some_and(f64::is_finite),
        Some("boolean") => value.is_boolean(),
        Some("array") => value.is_array(),
        Some("object") => value.is_object(),
        Some(_) => false,
        None => true,
    };
    if !matches_type {
        return Err(format!(
            "Computer Use parameter `{path}` must have type `{}`.",
            expected_type.unwrap_or("any")
        ));
    }

    if let Some(choices) = schema.get("enum").and_then(Value::as_array)
        && !choices.contains(value)
    {
        return Err(format!(
            "Computer Use parameter `{path}` has an invalid value."
        ));
    }

    if let Some(number) = value.as_f64() {
        if schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
        {
            return Err(format!(
                "Computer Use parameter `{path}` is below its minimum."
            ));
        }
        if schema
            .get("maximum")
            .and_then(Value::as_f64)
            .is_some_and(|maximum| number > maximum)
        {
            return Err(format!(
                "Computer Use parameter `{path}` exceeds its maximum."
            ));
        }
    }

    if let Some(array) = value.as_array() {
        if schema
            .get("minItems")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| array.len() < minimum as usize)
        {
            return Err(format!(
                "Computer Use parameter `{path}` has too few items."
            ));
        }
        if let Some(item_schema) = schema.get("items") {
            for (index, item) in array.iter().enumerate() {
                validate_value(item, item_schema, &format!("{path}[{index}]"))?;
            }
        }
    }

    Ok(())
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|error| error.into_inner())
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn::ToolCallRequestInfo;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeClient {
        started: bool,
        settings: Mutex<Vec<(Option<u32>, Option<f64>)>>,
        calls: Mutex<Vec<(String, Map<String, Value>)>>,
        result: Mutex<Value>,
    }

    impl ComputerUseDriverClient for FakeClient {
        fn is_started(&self) -> bool {
            self.started
        }

        fn start<'a>(
            &'a self,
            _on_progress: Option<ComputerUseProgress>,
        ) -> ComputerUseClientFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn set_max_image_dimension(&self, value: Option<u32>) {
            let mut settings = self.settings.lock().unwrap();
            settings.push((value, None));
        }

        fn set_idle_timeout_ms(&self, value: Option<f64>) {
            let mut settings = self.settings.lock().unwrap();
            settings.push((None, value));
        }

        fn call_tool<'a>(
            &'a self,
            name: &'a str,
            arguments: Map<String, Value>,
        ) -> ComputerUseClientFuture<'a, Value> {
            self.calls
                .lock()
                .unwrap()
                .push((name.to_owned(), arguments));
            let result = self.result.lock().unwrap().clone();
            Box::pin(async move { Ok(result) })
        }
    }

    struct FakeAuthorizer {
        high_risk: Mutex<Vec<bool>>,
    }

    impl ComputerUseCallAuthorizer for FakeAuthorizer {
        fn authorize<'a>(
            &'a self,
            _call: &'a ToolCallRequestInfo,
            _upstream_name: &'a str,
            _params: &'a Map<String, Value>,
            high_risk: bool,
        ) -> ComputerUseAuthorizationFuture<'a> {
            self.high_risk.lock().unwrap().push(high_risk);
            Box::pin(async { Ok(()) })
        }
    }

    struct NoopDaemon;
    impl StatusDaemon for NoopDaemon {
        fn kill(&self) {}
    }

    struct FakeBootstrap;
    impl BootstrapHost for FakeBootstrap {
        fn approval_is_granted(
            &self,
            _home_dir: PathBuf,
            _approval_key: String,
        ) -> BootstrapFuture<'_, bool> {
            Box::pin(async { true })
        }
        fn prompt_install_approval(&self, _approval_key: String) -> BootstrapFuture<'_, bool> {
            Box::pin(async { true })
        }
        fn save_install_approval(
            &self,
            _state: super::super::InstallState,
        ) -> BootstrapFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn install<'a>(
            &'a self,
            _on_progress: &'a (dyn Fn(&str) + Send + Sync),
        ) -> BootstrapFuture<'a, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn start_status_daemon(&self) -> Box<dyn StatusDaemon> {
            Box::new(NoopDaemon)
        }
        fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult> {
            Box::pin(async { PermissionProbeResult::Ok })
        }
        fn open_permission_pane(&self, _kind: PermissionKind) {}
        fn is_cancelled(&self) -> bool {
            false
        }
        fn update_output(&self, _output: &str) {}
        fn now_millis(&self) -> u64 {
            0
        }
        fn now_iso(&self) -> String {
            "2026-09-25T00:00:00.000Z".to_owned()
        }
        fn sleep(&self, _duration: std::time::Duration) -> BootstrapFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    fn options() -> ComputerUseAdapterOptions {
        ComputerUseAdapterOptions::new(BootstrapOptions::new("/tmp", "cua-v0.5.2", "linux"))
    }

    fn call(name: &str, args: Value) -> ToolCallRequestInfo {
        ToolCallRequestInfo {
            call_id: "call-1".to_owned(),
            provider_call_id: None,
            name: name.to_owned(),
            args,
            is_client_initiated: false,
            prompt_id: "prompt-1".to_owned(),
            response_id: None,
            was_output_truncated: None,
            goal_context: None,
        }
    }

    fn registered_adapter(
        client: Arc<FakeClient>,
        authorizer: Arc<FakeAuthorizer>,
    ) -> ComputerUseAgentToolAdapter {
        let adapter = ComputerUseAgentToolAdapter::new(client, options())
            .with_authorizer(authorizer)
            .with_bootstrap_host(Arc::new(FakeBootstrap));
        let count = adapter
            .register_enabled_tools(true, |_| true, &mut Vec::new())
            .unwrap();
        assert_eq!(count, 35);
        adapter
    }

    #[test]
    fn registration_obeys_feature_and_permission_filters_and_replaces_routes() {
        let adapter = ComputerUseAgentToolAdapter::new(Arc::new(FakeClient::default()), options());
        let mut declarations = Vec::new();
        assert_eq!(
            adapter
                .register_enabled_tools(true, |name| name.ends_with("__click"), &mut declarations)
                .unwrap(),
            1
        );
        assert_eq!(
            declarations[0]["functionDeclarations"][0]["name"],
            "computer_use__click"
        );
        assert!(adapter.is_registered("computer_use__click"));
        assert!(!adapter.is_registered("computer_use__drag"));

        let mut disabled_declarations = Vec::new();
        assert_eq!(
            adapter
                .register_enabled_tools(false, |_| true, &mut disabled_declarations)
                .unwrap(),
            0
        );
        assert!(disabled_declarations.is_empty());
        assert!(!adapter.is_registered("computer_use__click"));
    }

    #[test]
    fn schema_validation_checks_required_types_enums_and_numeric_bounds() {
        let schema = computer_use_tool_schema("drag").unwrap();
        let missing = Map::new();
        assert!(
            validate_params(&missing, &schema.parameter_schema)
                .unwrap_err()
                .contains("Missing required")
        );

        let mut invalid = json!({
            "pid": 1,
            "from_x": 1,
            "from_y": 2,
            "to_x": 3,
            "to_y": 4,
            "steps": 201,
            "button": "extra"
        })
        .as_object()
        .unwrap()
        .clone();
        invalid.insert("steps".to_owned(), json!(20));
        assert!(
            validate_params(&invalid, &schema.parameter_schema)
                .unwrap_err()
                .contains("invalid value")
        );
        invalid.insert("button".to_owned(), json!("left"));
        invalid.insert("steps".to_owned(), json!(201));
        assert!(
            validate_params(&invalid, &schema.parameter_schema)
                .unwrap_err()
                .contains("maximum")
        );
    }

    #[test]
    fn argument_limits_bound_size_depth_and_node_count() {
        let huge = Map::from_iter([(
            "text".to_owned(),
            Value::String("x".repeat(MAX_COMPUTER_USE_ARGUMENT_BYTES + 1)),
        )]);
        assert!(
            validate_bounded_json(&huge)
                .unwrap_err()
                .contains("byte limit")
        );

        let mut deep = Value::Null;
        for _ in 0..MAX_ARGUMENT_DEPTH + 2 {
            deep = Value::Array(vec![deep]);
        }
        let deep = Map::from_iter([("value".to_owned(), deep)]);
        assert!(
            validate_bounded_json(&deep)
                .unwrap_err()
                .contains("nested too deeply")
        );

        let many = Map::from_iter([(
            "values".to_owned(),
            Value::Array(vec![Value::Null; MAX_ARGUMENT_NODES]),
        )]);
        assert!(
            validate_bounded_json(&many)
                .unwrap_err()
                .contains("too many values")
        );
    }

    #[tokio::test]
    async fn dispatch_coerces_arguments_applies_settings_and_projects_structured_media() {
        let client = Arc::new(FakeClient {
            started: true,
            result: Mutex::new(json!({
                "content": [
                    {"type":"text", "text":"screenshot"},
                    {"type":"image", "mimeType":"image/png", "data":"png=="}
                ],
                "structuredContent": {"window_id": 41, "tree_markdown":"large duplicate"},
                "isError": false
            })),
            ..FakeClient::default()
        });
        let authorizer = Arc::new(FakeAuthorizer {
            high_risk: Mutex::new(Vec::new()),
        });
        let adapter = registered_adapter(Arc::clone(&client), Arc::clone(&authorizer));
        let mut options = adapter.options.clone();
        options.max_image_dimension = Some(1200);
        options.idle_timeout_ms = Some(10_000.0);
        let adapter = ComputerUseAgentToolAdapter { options, ..adapter };
        let output = adapter
            .execute_call(&call(
                "computer_use__click",
                json!({"pid": 1, "x": "500", "y": "920"}),
            ))
            .await
            .unwrap();

        assert_eq!(client.calls.lock().unwrap()[0].1["x"], 500.0);
        assert!(
            client
                .settings
                .lock()
                .unwrap()
                .contains(&(Some(1200), None))
        );
        assert!(
            client
                .settings
                .lock()
                .unwrap()
                .contains(&(None, Some(10_000.0)))
        );
        assert_eq!(authorizer.high_risk.lock().unwrap().as_slice(), &[false]);
        assert_eq!(output.output, "");
        assert_eq!(output.display, Some(Value::String("screenshot".to_owned())));
        assert!(
            output.parts[0]["text"]
                .as_str()
                .unwrap()
                .contains("screenshot")
        );
        assert_eq!(output.parts[2]["inlineData"]["data"], "png==");
        assert!(output.parts.iter().all(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .is_none_or(|text| !text.contains("large duplicate"))
        }));
    }

    #[tokio::test]
    async fn high_risk_authorization_is_flagged_and_missing_adapters_fail_closed() {
        let client = Arc::new(FakeClient {
            started: true,
            result: Mutex::new(json!({"content":[], "isError":false})),
            ..FakeClient::default()
        });
        let authorizer = Arc::new(FakeAuthorizer {
            high_risk: Mutex::new(Vec::new()),
        });
        let adapter = registered_adapter(Arc::clone(&client), Arc::clone(&authorizer));
        adapter
            .execute_call(&call(
                "computer_use__page",
                json!({"action":"execute_javascript", "javascript":"1+1"}),
            ))
            .await
            .unwrap();
        assert_eq!(authorizer.high_risk.lock().unwrap().as_slice(), &[true]);

        let missing_host = ComputerUseAgentToolAdapter::new(client.clone(), options())
            .with_authorizer(Arc::new(FakeAuthorizer {
                high_risk: Mutex::new(Vec::new()),
            }));
        missing_host
            .register_enabled_tools(true, |_| true, &mut Vec::new())
            .unwrap();
        let error = missing_host
            .execute_call(&call("computer_use__click", json!({"pid":1})))
            .await
            .unwrap_err();
        assert!(error.contains("bootstrap is not configured"));
        assert_eq!(client.calls.lock().unwrap().len(), 1);

        let missing_authorizer = ComputerUseAgentToolAdapter::new(client.clone(), options())
            .with_bootstrap_host(Arc::new(FakeBootstrap));
        missing_authorizer
            .register_enabled_tools(true, |_| true, &mut Vec::new())
            .unwrap();
        let error = missing_authorizer
            .execute_call(&call("computer_use__click", json!({"pid":1})))
            .await
            .unwrap_err();
        assert!(error.contains("authorization is not configured"));
        assert_eq!(client.calls.lock().unwrap().len(), 1);
    }
}
