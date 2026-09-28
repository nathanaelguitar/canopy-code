//! Agent-tool declarations and dispatch for a session's MCP connections.
//!
//! The adapter stays outside [`crate::agent_runtime::AgentRuntime`]: hosts can
//! compose it around their existing executor, provide their own authorization
//! policy, and append its function-declaration group to the runtime config.

use super::client_manager::{McpClientManager, McpManagerDiscoveryReport};
use super::client_runtime::{DiscoveredMcpTool, McpDiscoverySnapshot, McpRequestOptions};
use super::resource_content::{
    FormatMcpResourceOptions, FormattedMcpResource, empty_mcp_resource_text,
    format_mcp_resource_contents, summarize_mcp_resource,
};
use crate::agent_runtime::AgentToolExecutor;
use crate::tool_response_finalizer::ToolExecutionOutput;
use crate::turn::ToolCallRequestInfo;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use thiserror::Error;

pub const MCP_READ_RESOURCE_TOOL_NAME: &str = "read_mcp_resource";
pub const MCP_AGENT_TOOL_DEFAULT_BLOB_LIMIT: usize = 8_000_000;
const MCP_AGENT_TOOL_TURN_BLOB_LIMIT: usize = MCP_AGENT_TOOL_DEFAULT_BLOB_LIMIT * 3;
const MCP_AGENT_TOOL_BUDGET_MAX_TURNS: usize = 256;
const MCP_AGENT_TOOL_BUDGET_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Clone, Debug, PartialEq)]
pub enum McpAuthorizationOperation {
    ToolCall {
        server_name: String,
        tool_name: String,
        trust: Option<bool>,
        annotations: Option<Value>,
    },
    ResourceRead {
        server_name: String,
        uri: String,
    },
}

pub type McpAuthorizationFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Host-owned permission check for model-initiated MCP operations.
///
/// The Rust core deliberately does not assume that a model-facing MCP tool is
/// authorized just because its server is connected. CLI and server hosts must
/// provide a check that applies their approval, trust, and deny rules.
pub trait McpAgentToolAuthorizer: Send + Sync {
    fn authorize<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        operation: McpAuthorizationOperation,
    ) -> McpAuthorizationFuture<'a>;
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum McpAgentToolAdapterError {
    #[error("MCP declaration name collision for `{0}`")]
    ToolNameCollision(String),
}

#[derive(Clone, Debug)]
struct McpToolRoute {
    server_name: String,
    tool_name: String,
    trust: Option<bool>,
    annotations: Option<Value>,
    declaration: Value,
}

#[derive(Default)]
struct ResourceBlobBudgetState {
    turns: HashMap<String, ResourceBlobBudgetEntry>,
}

struct ResourceBlobBudgetEntry {
    used_blob_chars: usize,
    last_seen: Instant,
}

/// Maps discovered MCP declarations to the owning per-session manager.
pub struct McpAgentToolAdapter {
    manager: Arc<McpClientManager>,
    authorizer: Arc<dyn McpAgentToolAuthorizer>,
    request_options: McpRequestOptions,
    routes: RwLock<BTreeMap<String, McpToolRoute>>,
    resource_blob_budget: Mutex<ResourceBlobBudgetState>,
}

impl McpAgentToolAdapter {
    pub fn new(
        manager: Arc<McpClientManager>,
        authorizer: Arc<dyn McpAgentToolAuthorizer>,
    ) -> Self {
        Self::with_request_options(manager, authorizer, McpRequestOptions::default())
    }

    pub fn with_request_options(
        manager: Arc<McpClientManager>,
        authorizer: Arc<dyn McpAgentToolAuthorizer>,
        request_options: McpRequestOptions,
    ) -> Self {
        Self {
            manager,
            authorizer,
            request_options,
            routes: RwLock::new(BTreeMap::new()),
            resource_blob_budget: Mutex::new(ResourceBlobBudgetState::default()),
        }
    }

    /// Connect configured servers and atomically refresh this adapter's tool
    /// routes from the manager's filtered per-session snapshots.
    pub async fn discover_all(
        &self,
        servers: &Map<String, Value>,
    ) -> Result<McpManagerDiscoveryReport, McpAgentToolAdapterError> {
        let report = self.manager.discover_all(servers).await;
        self.update_from_discovery_report(&report)?;
        Ok(report)
    }

    pub fn update_from_discovery_report(
        &self,
        report: &McpManagerDiscoveryReport,
    ) -> Result<(), McpAgentToolAdapterError> {
        self.update_from_snapshots(&report.snapshots)
    }

    pub fn update_from_snapshots(
        &self,
        snapshots: &std::collections::HashMap<String, McpDiscoverySnapshot>,
    ) -> Result<(), McpAgentToolAdapterError> {
        let mut routes = BTreeMap::new();
        let mut tools = snapshots
            .values()
            .flat_map(|snapshot| snapshot.tools.iter())
            .collect::<Vec<_>>();
        tools.sort_by(|left, right| {
            left.server_name
                .cmp(&right.server_name)
                .then_with(|| left.name.cmp(&right.name))
        });

        for tool in tools {
            let function_name = mcp_function_name(&tool.server_name, &tool.name);
            let route = route_for_discovered_tool(tool, function_name.clone());
            if routes.insert(function_name.clone(), route).is_some() {
                return Err(McpAgentToolAdapterError::ToolNameCollision(function_name));
            }
        }
        *write_lock(&self.routes) = routes;
        Ok(())
    }

    /// Gemini-shaped tool group accepted by `AgentRuntimeConfig` and the
    /// provider-neutral request pipeline. The raw MCP JSON schema stays under
    /// `parametersJsonSchema` so provider conversion does not mistake it for a
    /// Gemini-only schema dialect.
    pub fn function_declaration_group(&self) -> Value {
        let routes = read_lock(&self.routes);
        let mut function_declarations = routes
            .values()
            .map(|route| route.declaration.clone())
            .collect::<Vec<_>>();
        function_declarations.push(read_resource_declaration());
        json!({"functionDeclarations": function_declarations})
    }

    pub fn append_function_declarations(&self, declarations: &mut Vec<Value>) {
        declarations.push(self.function_declaration_group());
    }

    pub fn compose<E>(self: &Arc<Self>, fallback: E) -> McpComposedToolExecutor<E> {
        McpComposedToolExecutor {
            adapter: Arc::clone(self),
            fallback,
        }
    }

    pub fn manager(&self) -> &Arc<McpClientManager> {
        &self.manager
    }

    fn route(&self, function_name: &str) -> Option<McpToolRoute> {
        read_lock(&self.routes).get(function_name).cloned()
    }

    async fn execute_mcp_call(
        &self,
        call: &ToolCallRequestInfo,
    ) -> Result<ToolExecutionOutput, String> {
        if call.name == MCP_READ_RESOURCE_TOOL_NAME {
            return self.execute_resource_read(call).await;
        }
        let route = self
            .route(&call.name)
            .ok_or_else(|| format!("No discovered MCP tool matches `{}`.", call.name))?;
        let arguments = call
            .args
            .as_object()
            .ok_or_else(|| "MCP tool arguments must be a JSON object.".to_owned())?;
        self.authorizer
            .authorize(
                call,
                McpAuthorizationOperation::ToolCall {
                    server_name: route.server_name.clone(),
                    tool_name: route.tool_name.clone(),
                    trust: route.trust,
                    annotations: route.annotations.clone(),
                },
            )
            .await?;
        // MCP tool-call transport failures can arrive after the server has
        // already performed the operation. Do not replay automatically; any
        // future retry must be enabled by a host policy that knows the tool's
        // idempotency and the transport failure's outcome semantics.
        let result = self
            .manager
            .call_tool(
                &route.server_name,
                &route.tool_name,
                arguments,
                self.request_options.clone(),
            )
            .await
            .map_err(|error| error.to_string())?;
        Ok(tool_result_output(&route.tool_name, &call.args, &result))
    }

    async fn execute_resource_read(
        &self,
        call: &ToolCallRequestInfo,
    ) -> Result<ToolExecutionOutput, String> {
        let (server_name, uri) = parse_resource_read_args(&call.args)?;
        self.authorizer
            .authorize(
                call,
                McpAuthorizationOperation::ResourceRead {
                    server_name: server_name.to_owned(),
                    uri: uri.to_owned(),
                },
            )
            .await?;
        let result = self
            .manager
            .read_resource(server_name, uri, self.request_options.clone())
            .await
            .map_err(|error| error.to_string())?;
        let label = format!("{server_name}:{uri}");
        let formatted = self.format_resource_for_turn(call, &result, &label)?;
        let display = format!(
            "Read resource {label} — {}",
            summarize_mcp_resource(&formatted)
        );
        if formatted.parts.is_empty() {
            Ok(ToolExecutionOutput::with_display(
                empty_mcp_resource_text(&formatted, &label),
                Value::String(display),
            ))
        } else {
            let mut output = ToolExecutionOutput::with_parts("", formatted.parts);
            output.display = Some(Value::String(display));
            Ok(output)
        }
    }

    fn is_registered_mcp_name(&self, name: &str) -> bool {
        name == MCP_READ_RESOURCE_TOOL_NAME || name.starts_with("mcp__")
    }

    fn format_resource_for_turn(
        &self,
        call: &ToolCallRequestInfo,
        result: &Value,
        label: &str,
    ) -> Result<FormattedMcpResource, String> {
        let key = resource_budget_turn_key(call);
        let now = Instant::now();
        let mut budget = mutex_lock(&self.resource_blob_budget);
        budget.turns.retain(|_, entry| {
            now.saturating_duration_since(entry.last_seen) <= MCP_AGENT_TOOL_BUDGET_TTL
        });

        let used = budget
            .turns
            .get(&key)
            .map(|entry| entry.used_blob_chars)
            .unwrap_or_default();
        let remaining = MCP_AGENT_TOOL_TURN_BLOB_LIMIT.saturating_sub(used);
        let max_blob_chars = MCP_AGENT_TOOL_DEFAULT_BLOB_LIMIT.min(remaining);
        let formatted = format_mcp_resource_contents(
            result,
            label,
            Some(FormatMcpResourceOptions {
                max_blob_chars: Some(max_blob_chars),
            }),
        )
        .map_err(|error| error.to_string())?;

        if !budget.turns.contains_key(&key) && budget.turns.len() >= MCP_AGENT_TOOL_BUDGET_MAX_TURNS
        {
            if let Some(oldest_key) = budget
                .turns
                .iter()
                .min_by_key(|(_, entry)| entry.last_seen)
                .map(|(key, _)| key.clone())
            {
                budget.turns.remove(&oldest_key);
            }
        }
        let entry = budget.turns.entry(key).or_insert(ResourceBlobBudgetEntry {
            used_blob_chars: 0,
            last_seen: now,
        });
        entry.used_blob_chars = entry.used_blob_chars.saturating_add(formatted.blob_chars);
        entry.last_seen = now;
        Ok(formatted)
    }

    fn is_read_only(&self, call: &ToolCallRequestInfo) -> bool {
        if call.name == MCP_READ_RESOURCE_TOOL_NAME {
            return true;
        }
        self.route(&call.name)
            .and_then(|route| route.annotations)
            .and_then(|annotations| annotations.get("readOnlyHint").and_then(Value::as_bool))
            == Some(true)
    }
}

/// Wrapper that routes MCP names through [`McpAgentToolAdapter`] and delegates
/// every other tool to the host's existing executor.
pub struct McpComposedToolExecutor<E> {
    adapter: Arc<McpAgentToolAdapter>,
    fallback: E,
}

impl<E: AgentToolExecutor> AgentToolExecutor for McpComposedToolExecutor<E> {
    fn cancellation_token_for_current_prompt(
        &self,
    ) -> Option<crate::utils::cancellation::CancellationToken> {
        self.fallback.cancellation_token_for_current_prompt()
    }

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
            if self.adapter.is_registered_mcp_name(&call.name) {
                self.adapter.execute_mcp_call(call).await
            } else {
                self.fallback.execute(call).await
            }
        })
    }

    fn is_side_effecting(&self, tool_name: &str) -> bool {
        if tool_name == MCP_READ_RESOURCE_TOOL_NAME {
            return false;
        }
        if tool_name.starts_with("mcp__") {
            return self
                .adapter
                .route(tool_name)
                .and_then(|route| route.annotations)
                .and_then(|annotations| annotations.get("readOnlyHint").and_then(Value::as_bool))
                != Some(true);
        }
        self.fallback.is_side_effecting(tool_name)
    }

    fn is_concurrency_safe(&self, call: &ToolCallRequestInfo) -> bool {
        if self.adapter.is_registered_mcp_name(&call.name) {
            self.adapter.is_read_only(call)
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

fn route_for_discovered_tool(tool: &DiscoveredMcpTool, function_name: String) -> McpToolRoute {
    let mut declaration = json!({
        "name": function_name,
        "description": tool.description.clone(),
    });
    if tool.input_schema.is_object() {
        declaration["parametersJsonSchema"] = tool.input_schema.clone();
    } else {
        declaration["parametersJsonSchema"] = json!({
            "type":"object",
            "properties":{},
            "additionalProperties":false
        });
    }
    McpToolRoute {
        server_name: tool.server_name.clone(),
        tool_name: tool.name.clone(),
        trust: tool.trust,
        annotations: tool.annotations.clone(),
        declaration,
    }
}

/// Return the provider-safe function name used by the TypeScript MCP tool
/// registry for `mcp__<server>__<tool>` declarations.
pub fn mcp_function_name(server_name: &str, tool_name: &str) -> String {
    normalize_mcp_function_name(&format!("mcp__{server_name}__{tool_name}"))
}

fn normalize_mcp_function_name(name: &str) -> String {
    let units = name.encode_utf16().collect::<Vec<_>>();
    let provider_safe = units.len() <= 63
        && units.iter().all(|unit| is_safe_name_unit(*unit))
        && units.first().is_some_and(|unit| is_ascii_alpha_unit(*unit));
    if provider_safe {
        return name.to_owned();
    }

    let sanitized = units
        .iter()
        .map(|unit| {
            if is_safe_name_unit(*unit) {
                char::from_u32(*unit as u32).unwrap_or('_')
            } else {
                '_'
            }
        })
        .collect::<String>();
    let sanitized = if sanitized
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_alphabetic())
    {
        sanitized
    } else {
        format!("tool_{sanitized}")
    };
    let suffix = format!("_{}", stable_name_hash(&units));
    let keep = 63usize.saturating_sub(suffix.len());
    format!(
        "{}{suffix}",
        sanitized.chars().take(keep).collect::<String>()
    )
}

fn is_safe_name_unit(unit: u16) -> bool {
    matches!(unit, 45 | 48..=57 | 65..=90 | 95 | 97..=122)
}

fn is_ascii_alpha_unit(unit: u16) -> bool {
    matches!(unit, 65..=90 | 97..=122)
}

fn stable_name_hash(units: &[u16]) -> String {
    let mut hash = 2_166_136_261_u32;
    for unit in units {
        hash = (hash ^ u32::from(*unit)).wrapping_mul(16_777_619);
    }
    let mut value = hash;
    let mut encoded = Vec::new();
    while value > 0 {
        let digit = (value % 36) as u8;
        encoded.push(if digit < 10 {
            char::from(b'0' + digit)
        } else {
            char::from(b'a' + digit - 10)
        });
        value /= 36;
    }
    while encoded.len() < 7 {
        encoded.push('0');
    }
    encoded.iter().rev().collect()
}

fn read_resource_declaration() -> Value {
    json!({
        "name": MCP_READ_RESOURCE_TOOL_NAME,
        "description": "Reads a resource from a configured MCP server by server_name and URI. The server_name must match a configured MCP server. The uri must be an exact resource URI previously advertised by that server.",
        "parametersJsonSchema": {
            "type":"object",
            "properties":{
                "server_name":{
                    "type":"string",
                    "minLength":1,
                    "maxLength":1024,
                    "description":"The configured MCP server name."
                },
                "uri":{
                    "type":"string",
                    "minLength":1,
                    "maxLength":4096,
                    "description":"The exact resource URI to read from that server."
                }
            },
            "required":["server_name","uri"],
            "additionalProperties":false
        }
    })
}

fn parse_resource_read_args(args: &Value) -> Result<(&str, &str), String> {
    let object = args
        .as_object()
        .ok_or_else(|| "read_mcp_resource arguments must be a JSON object.".to_owned())?;
    if object
        .keys()
        .any(|key| key != "server_name" && key != "uri")
    {
        return Err("read_mcp_resource accepts only server_name and uri.".to_owned());
    }
    let server_name = object
        .get("server_name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && utf16_len(value) <= 1024)
        .ok_or_else(|| "read_mcp_resource requires a valid server_name.".to_owned())?;
    let uri = object
        .get("uri")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && utf16_len(value) <= 4096)
        .ok_or_else(|| "read_mcp_resource requires a valid uri.".to_owned())?;
    Ok((server_name, uri))
}

fn tool_result_output(tool_name: &str, arguments: &Value, result: &Value) -> ToolExecutionOutput {
    let Some(content) = result.get("content").and_then(Value::as_array) else {
        return ToolExecutionOutput::text("[Error: Could not parse tool response]");
    };
    let mut parts = content
        .iter()
        .flat_map(|block| transform_mcp_content_block(block, tool_name))
        .collect::<Vec<_>>();
    let is_error = result
        .get("isError")
        .is_some_and(|value| value.as_bool() == Some(true) || value.as_str() == Some("true"));
    if is_error {
        let text = format!(
            "MCP tool `{tool_name}` reported an error for arguments {}.",
            serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_owned())
        );
        parts.insert(0, json!({"text": text}));
    }
    let display = display_from_parts(&parts);
    let mut output = ToolExecutionOutput::with_parts("", parts);
    if is_error || !display.is_empty() {
        output.display = Some(Value::String(display));
    }
    output
}

fn transform_mcp_content_block(block: &Value, tool_name: &str) -> Vec<Value> {
    let Some(block) = block.as_object() else {
        return Vec::new();
    };
    match block.get("type").and_then(Value::as_str) {
        Some("text") => block
            .get("text")
            .and_then(Value::as_str)
            .map(|text| vec![json!({"text":text})])
            .unwrap_or_default(),
        Some("image" | "audio") => {
            let Some(data) = block.get("data").and_then(Value::as_str) else {
                return Vec::new();
            };
            let mime_type = block
                .get("mimeType")
                .and_then(Value::as_str)
                .unwrap_or("application/octet-stream");
            let modality = block.get("type").and_then(Value::as_str).unwrap_or("media");
            vec![
                json!({"text":format!("[Tool '{tool_name}' provided the following {modality} data with mime-type: {mime_type}]" )}),
                json!({"inlineData":{"mimeType":mime_type,"data":data}}),
            ]
        }
        Some("resource") => {
            let Some(resource) = block.get("resource").and_then(Value::as_object) else {
                return Vec::new();
            };
            if let Some(text) = resource
                .get("text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                vec![json!({"text":text})]
            } else if let Some(blob) = resource
                .get("blob")
                .and_then(Value::as_str)
                .filter(|blob| !blob.is_empty())
            {
                let mime_type = resource
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .unwrap_or("application/octet-stream");
                vec![
                    json!({"text":format!("[Tool '{tool_name}' provided the following embedded resource with mime-type: {mime_type}]" )}),
                    json!({"inlineData":{"mimeType":mime_type,"data":blob}}),
                ]
            } else {
                Vec::new()
            }
        }
        Some("resource_link") => {
            let title = block
                .get("title")
                .and_then(Value::as_str)
                .filter(|title| !title.is_empty())
                .or_else(|| block.get("name").and_then(Value::as_str))
                .unwrap_or("resource");
            let uri = block.get("uri").and_then(Value::as_str).unwrap_or("");
            vec![json!({"text":format!("Resource Link: {title} at {uri}")})]
        }
        _ => Vec::new(),
    }
}

fn display_from_parts(parts: &[Value]) -> String {
    parts
        .iter()
        .filter_map(|part| {
            part.get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    part.pointer("/inlineData/mimeType")
                        .and_then(Value::as_str)
                        .map(|mime_type| format!("[{mime_type}]"))
                })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn utf16_len(value: &str) -> usize {
    value.encode_utf16().count()
}

fn resource_budget_turn_key(call: &ToolCallRequestInfo) -> String {
    match call.response_id.as_deref().filter(|id| !id.is_empty()) {
        Some(response_id) => format!("{}:response:{response_id}", call.prompt_id),
        None => format!("{}:prompt", call.prompt_id),
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_lock<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn mutex_lock<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::{mcp_function_name, route_for_discovered_tool};
    use crate::tools::mcp::client_runtime::DiscoveredMcpTool;
    use serde_json::json;

    #[test]
    fn declaration_preserves_the_server_provided_tool_description() {
        let tool = DiscoveredMcpTool {
            server_name: "fixture".to_owned(),
            name: "read".to_owned(),
            description: "Read a fixture item by its identifier.".to_owned(),
            input_schema: json!({"type":"object","properties":{}}),
            annotations: None,
            trust: None,
            always_load: false,
        };

        let route =
            route_for_discovered_tool(&tool, mcp_function_name(&tool.server_name, &tool.name));

        assert_eq!(
            route.declaration["description"],
            "Read a fixture item by its identifier."
        );
    }
}
