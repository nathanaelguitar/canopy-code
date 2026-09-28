//! SDK-hosted MCP server configuration and per-query dispatch.
//!
//! The TypeScript SDK accepts MCP server instances created with
//! `createSdkMcpServer`. Rust models tools, static resources, and prompts with
//! JSON values and async handlers, then routes JSON-RPC messages from the CLI
//! control plane to the matching server.

use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

use crate::QueryError;

/// Async implementation for one tool exposed by an SDK-hosted MCP server.
///
/// The handler receives the tool's arguments as JSON and returns the MCP
/// `CallToolResult` payload. Schema validation is the handler's responsibility;
/// the schema is sent to the CLI for model-side tool discovery.
pub type McpToolHandler =
    Arc<dyn Fn(Value) -> BoxFuture<'static, Result<Value, String>> + Send + Sync + 'static>;

/// Async implementation for an MCP tool that receives request context.
pub type McpToolHandlerWithContext = Arc<
    dyn Fn(Value, McpRequestContext) -> BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync
        + 'static,
>;

/// Async implementation for an SDK-hosted MCP resource read.
pub type McpResourceHandler =
    Arc<dyn Fn(String) -> BoxFuture<'static, Result<Value, String>> + Send + Sync + 'static>;

/// Async implementation for an MCP resource read that receives request
/// context.
pub type McpResourceHandlerWithContext = Arc<
    dyn Fn(String, McpRequestContext) -> BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync
        + 'static,
>;

/// Async implementation for an SDK-hosted resource template read. The URI
/// and captured template variables are passed as raw values, matching the MCP
/// server callback contract.
pub type McpResourceTemplateHandler = Arc<
    dyn Fn(String, Map<String, Value>) -> BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync
        + 'static,
>;

/// Context-aware resource-template read callback.
pub type McpResourceTemplateHandlerWithContext = Arc<
    dyn Fn(
            String,
            Map<String, Value>,
            McpRequestContext,
        ) -> BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync
        + 'static,
>;

/// Optional callback that lists concrete resources represented by a template.
/// Its result is an MCP `ListResourcesResult` object, normally with a
/// `resources` array.
pub type McpResourceTemplateListHandler = Arc<
    dyn Fn(McpRequestContext) -> BoxFuture<'static, Result<Value, String>> + Send + Sync + 'static,
>;

/// Async implementation for an SDK-hosted MCP prompt request.
pub type McpPromptHandler =
    Arc<dyn Fn(Value) -> BoxFuture<'static, Result<Value, String>> + Send + Sync + 'static>;

/// Async implementation for an MCP prompt that receives request context.
pub type McpPromptHandlerWithContext = Arc<
    dyn Fn(Value, McpRequestContext) -> BoxFuture<'static, Result<Value, String>>
        + Send
        + Sync
        + 'static,
>;

/// Request-local cancellation token passed to context-aware MCP handlers.
#[derive(Clone, Debug)]
pub struct McpRequestCancellation {
    receiver: watch::Receiver<bool>,
}

impl McpRequestCancellation {
    /// Return whether the peer cancelled the request or its transport ended.
    pub fn is_cancelled(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Wait for peer cancellation or for request work to be dropped early.
    pub async fn cancelled(&self) {
        let mut receiver = self.receiver.clone();
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Subset of MCP `RequestHandlerExtra` available through the in-memory SDK
/// transport. The transport supplies a request ID and request metadata; it
/// does not expose an MCP session ID, HTTP request, or server-to-client
/// request/notification methods.
#[derive(Clone, Debug)]
pub struct McpRequestContext {
    pub request_id: Value,
    pub meta: Option<Value>,
    pub cancellation: McpRequestCancellation,
}

#[derive(Clone, Default)]
struct ActiveMcpRequests {
    state: Arc<Mutex<ActiveMcpRequestState>>,
}

#[derive(Default)]
struct ActiveMcpRequestState {
    next_generation: u64,
    requests: BTreeMap<(String, String), (u64, watch::Sender<bool>)>,
}

struct ActiveMcpRequestGuard {
    state: Arc<Mutex<ActiveMcpRequestState>>,
    key: (String, String),
    generation: u64,
    sender: watch::Sender<bool>,
    completed: bool,
}

fn request_id_key(request_id: &Value) -> String {
    match request_id {
        Value::String(value) => format!("string:{value}"),
        Value::Number(number) => {
            let numeric = number
                .as_i64()
                .map(|value| value.to_string())
                .or_else(|| number.as_u64().map(|value| value.to_string()))
                .or_else(|| number.as_f64().map(|value| value.to_string()))
                .unwrap_or_else(|| number.to_string());
            format!("number:{numeric}")
        }
        other => format!("other:{other}"),
    }
}

impl ActiveMcpRequests {
    fn register(
        &self,
        server_name: &str,
        request_id: Value,
        meta: Option<Value>,
    ) -> (McpRequestContext, ActiveMcpRequestGuard) {
        let key = (server_name.to_owned(), request_id_key(&request_id));
        let (sender, receiver) = watch::channel(false);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        if let Some((_, previous)) = state
            .requests
            .insert(key.clone(), (generation, sender.clone()))
        {
            previous.send_replace(true);
        }
        drop(state);

        let context = McpRequestContext {
            request_id,
            meta,
            cancellation: McpRequestCancellation { receiver },
        };
        let guard = ActiveMcpRequestGuard {
            state: self.state.clone(),
            key,
            generation,
            sender,
            completed: false,
        };
        (context, guard)
    }

    fn cancel(&self, server_name: &str, request_id: &Value) {
        let key = (server_name.to_owned(), request_id_key(request_id));
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, sender)) = state.requests.get(&key) {
            sender.send_replace(true);
        }
    }
}

impl ActiveMcpRequestGuard {
    fn complete(mut self) {
        self.remove();
        self.completed = true;
    }

    fn remove(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .requests
            .get(&self.key)
            .is_some_and(|(generation, _)| *generation == self.generation)
        {
            state.requests.remove(&self.key);
        }
    }
}

impl Drop for ActiveMcpRequestGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.sender.send_replace(true);
            self.remove();
        }
    }
}

/// Tool definition for an SDK-hosted MCP server.
#[derive(Clone)]
pub struct McpToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub handler: McpToolHandler,
}

impl McpToolDefinition {
    /// Create a tool definition, applying the TypeScript SDK's name and
    /// required-field checks.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        handler: McpToolHandler,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        let description = description.into();
        validate_tool_name(&name)?;
        if !input_schema.is_object() {
            return Err(QueryError::InvalidOptions(format!(
                "Tool '{name}' must have an inputSchema (object)"
            )));
        }
        Ok(Self {
            name,
            description,
            input_schema,
            handler,
        })
    }
}

/// Tool definition whose handler receives [`McpRequestContext`].
#[derive(Clone)]
pub struct McpToolDefinitionWithContext {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub handler: McpToolHandlerWithContext,
}

impl McpToolDefinitionWithContext {
    /// Create a tool whose callback receives request metadata and cancellation.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        input_schema: Value,
        handler: McpToolHandlerWithContext,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        let description = description.into();
        validate_tool_name(&name)?;
        if !input_schema.is_object() {
            return Err(QueryError::InvalidOptions(format!(
                "Tool '{name}' must have an inputSchema (object)"
            )));
        }
        Ok(Self {
            name,
            description,
            input_schema,
            handler,
        })
    }
}

/// One static resource exposed by an SDK-hosted MCP server.
#[derive(Clone)]
pub struct McpResourceDefinition {
    pub name: String,
    pub uri: String,
    /// Resource metadata fields such as `description`, `mimeType`, `title`,
    /// and `annotations`. Unrecognized fields are forwarded unchanged.
    pub metadata: Map<String, Value>,
    pub handler: McpResourceHandler,
    context_handler: Option<McpResourceHandlerWithContext>,
}

impl McpResourceDefinition {
    /// Create a static resource definition. The callback receives the URI from
    /// `resources/read` and returns the MCP result object, normally containing
    /// a `contents` array.
    pub fn new(
        name: impl Into<String>,
        uri: impl Into<String>,
        metadata: Map<String, Value>,
        handler: McpResourceHandler,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        let uri = uri.into();
        if name.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP resource name must be a non-empty string".into(),
            ));
        }
        if uri.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP resource URI must be a non-empty string".into(),
            ));
        }
        Ok(Self {
            name,
            uri,
            metadata,
            handler,
            context_handler: None,
        })
    }

    /// Create a static resource whose callback receives
    /// [`McpRequestContext`].
    pub fn new_with_context(
        name: impl Into<String>,
        uri: impl Into<String>,
        metadata: Map<String, Value>,
        handler: McpResourceHandlerWithContext,
    ) -> Result<Self, QueryError> {
        let mut resource = Self::new(
            name,
            uri,
            metadata,
            Arc::new(|_| Box::pin(async { Err("context-aware handler required".into()) })),
        )?;
        resource.context_handler = Some(handler);
        Ok(resource)
    }
}

/// One dynamic resource exposed through an MCP URI template.
#[derive(Clone)]
pub struct McpResourceTemplateDefinition {
    pub name: String,
    pub uri_template: String,
    /// Resource metadata such as `description`, `mimeType`, `title`, and
    /// `annotations`; unrecognized fields are forwarded unchanged.
    pub metadata: Map<String, Value>,
    pub handler: McpResourceTemplateHandler,
    pub list_handler: Option<McpResourceTemplateListHandler>,
    context_handler: Option<McpResourceTemplateHandlerWithContext>,
}

impl McpResourceTemplateDefinition {
    /// Create a resource-template definition. The callback receives the
    /// requested URI and the variables captured from `uri_template`.
    pub fn new(
        name: impl Into<String>,
        uri_template: impl Into<String>,
        metadata: Map<String, Value>,
        handler: McpResourceTemplateHandler,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        let uri_template = uri_template.into();
        if name.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP resource template name must be a non-empty string".into(),
            ));
        }
        parse_resource_uri_template(&uri_template)?;
        Ok(Self {
            name,
            uri_template,
            metadata,
            handler,
            list_handler: None,
            context_handler: None,
        })
    }

    /// Create a resource template whose read callback receives
    /// [`McpRequestContext`].
    pub fn new_with_context(
        name: impl Into<String>,
        uri_template: impl Into<String>,
        metadata: Map<String, Value>,
        handler: McpResourceTemplateHandlerWithContext,
    ) -> Result<Self, QueryError> {
        let mut template = Self::new(
            name,
            uri_template,
            metadata,
            Arc::new(|_, _| Box::pin(async { Err("context-aware handler required".into()) })),
        )?;
        template.context_handler = Some(handler);
        Ok(template)
    }

    /// Attach the optional concrete-resource listing callback supported by
    /// TypeScript `ResourceTemplate`.
    pub fn with_list_handler(mut self, handler: McpResourceTemplateListHandler) -> Self {
        self.list_handler = Some(handler);
        self
    }
}

/// One prompt exposed by an SDK-hosted MCP server.
#[derive(Clone)]
pub struct McpPromptDefinition {
    pub name: String,
    pub description: Option<String>,
    /// Additional prompt metadata such as `title`; unrecognized fields are
    /// forwarded unchanged by `prompts/list`.
    pub metadata: Map<String, Value>,
    /// MCP prompt argument descriptions. Values are retained as raw JSON so
    /// future protocol fields pass through unchanged.
    pub arguments: Option<Vec<Value>>,
    pub handler: McpPromptHandler,
    context_handler: Option<McpPromptHandlerWithContext>,
}

impl McpPromptDefinition {
    /// Create a prompt definition. The callback receives the raw `arguments`
    /// object (or an empty object when none was supplied) and returns the MCP
    /// `GetPromptResult` object.
    pub fn new(
        name: impl Into<String>,
        description: Option<String>,
        arguments: Option<Vec<Value>>,
        handler: McpPromptHandler,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        if name.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP prompt name must be a non-empty string".into(),
            ));
        }
        Ok(Self {
            name,
            description,
            metadata: Map::new(),
            arguments,
            handler,
            context_handler: None,
        })
    }

    /// Attach additional prompt metadata fields such as `title`.
    pub fn with_metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Create a prompt whose callback receives [`McpRequestContext`].
    pub fn new_with_context(
        name: impl Into<String>,
        description: Option<String>,
        arguments: Option<Vec<Value>>,
        handler: McpPromptHandlerWithContext,
    ) -> Result<Self, QueryError> {
        let mut prompt = Self::new(
            name,
            description,
            arguments,
            Arc::new(|_| Box::pin(async { Err("context-aware handler required".into()) })),
        )?;
        prompt.context_handler = Some(handler);
        Ok(prompt)
    }
}

/// Definition for an SDK-hosted MCP server.
#[derive(Clone)]
pub struct SdkMcpServerConfig {
    pub name: String,
    pub version: String,
    /// `None` means the server does not advertise tool capability, matching
    /// TypeScript `createSdkMcpServer({ tools: undefined })`.
    pub tools: Option<Vec<McpToolDefinition>>,
    /// Optional context-aware tools, which are listed and dispatched alongside
    /// `tools`.
    pub context_tools: Vec<McpToolDefinitionWithContext>,
    /// Static MCP resources. `None` means the server does not advertise the
    /// resources capability.
    pub resources: Option<Vec<McpResourceDefinition>>,
    /// Dynamic resources registered through MCP `ResourceTemplate`.
    pub resource_templates: Option<Vec<McpResourceTemplateDefinition>>,
    /// MCP prompts. `None` means the server does not advertise the prompts
    /// capability.
    pub prompts: Option<Vec<McpPromptDefinition>>,
}

impl SdkMcpServerConfig {
    /// Build a server definition. The TypeScript factory defaults the version
    /// to `1.0.0`; callers using Rust can use [`Self::default_version`].
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        tools: Option<Vec<McpToolDefinition>>,
    ) -> Result<Self, QueryError> {
        let name = name.into();
        let version = version.into();
        if name.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP server name must be a non-empty string".into(),
            ));
        }
        if version.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP server version must be a non-empty string".into(),
            ));
        }
        let mut names = BTreeSet::new();
        if let Some(tools) = &tools {
            for tool in tools {
                validate_tool_name(&tool.name)?;
                if !names.insert(tool.name.as_str()) {
                    return Err(QueryError::InvalidOptions(format!(
                        "Duplicate tool name '{}' in MCP server '{name}'",
                        tool.name
                    )));
                }
                if !tool.input_schema.is_object() {
                    return Err(QueryError::InvalidOptions(format!(
                        "Tool '{}' must have an inputSchema (object)",
                        tool.name
                    )));
                }
            }
        }
        Ok(Self {
            name,
            version,
            tools,
            context_tools: Vec::new(),
            resources: None,
            resource_templates: None,
            prompts: None,
        })
    }

    /// Add static resources and advertise the MCP resources capability.
    pub fn with_resources(mut self, resources: Vec<McpResourceDefinition>) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Add URI-template resources and advertise the MCP resources capability.
    pub fn with_resource_templates(
        mut self,
        templates: Vec<McpResourceTemplateDefinition>,
    ) -> Result<Self, QueryError> {
        let mut names = BTreeSet::new();
        for template in &templates {
            if !names.insert(template.name.as_str()) {
                return Err(QueryError::InvalidOptions(format!(
                    "Duplicate resource template name '{}' in MCP server '{}'",
                    template.name, self.name
                )));
            }
        }
        self.resource_templates = Some(templates);
        Ok(self)
    }

    /// Add context-aware tools while keeping existing plain tool handlers
    /// unchanged.
    pub fn with_context_tools(
        mut self,
        tools: Vec<McpToolDefinitionWithContext>,
    ) -> Result<Self, QueryError> {
        let mut names = self
            .tools
            .iter()
            .flatten()
            .map(|tool| tool.name.as_str())
            .collect::<BTreeSet<_>>();
        for tool in &tools {
            validate_tool_name(&tool.name)?;
            if !tool.input_schema.is_object() {
                return Err(QueryError::InvalidOptions(format!(
                    "Tool '{}' must have an inputSchema (object)",
                    tool.name
                )));
            }
            if !names.insert(tool.name.as_str()) {
                return Err(QueryError::InvalidOptions(format!(
                    "Duplicate tool name '{}' in MCP server '{}'",
                    tool.name, self.name
                )));
            }
        }
        self.context_tools = tools;
        Ok(self)
    }

    /// Add prompts and advertise the MCP prompts capability.
    pub fn with_prompts(mut self, prompts: Vec<McpPromptDefinition>) -> Self {
        self.prompts = Some(prompts);
        self
    }

    /// Build a server using the TypeScript factory's default version.
    pub fn default_version(
        name: impl Into<String>,
        tools: Option<Vec<McpToolDefinition>>,
    ) -> Result<Self, QueryError> {
        Self::new(name, "1.0.0", tools)
    }
}

/// One entry in [`crate::QueryOptions::mcp_servers`].
#[derive(Clone)]
pub enum McpServerConfig {
    /// External server configuration passed through to the CLI. The value
    /// should be an object using CLI MCP config fields such as `command`,
    /// `httpUrl`, `url`, or `tcp`.
    External(Value),
    /// An MCP server whose tools run in the Rust SDK process.
    Sdk(SdkMcpServerConfig),
}

impl McpServerConfig {
    /// Create an external CLI-managed MCP server configuration.
    pub fn external(config: Value) -> Self {
        Self::External(config)
    }

    /// Create an SDK-hosted MCP server configuration.
    pub fn sdk(config: SdkMcpServerConfig) -> Self {
        Self::Sdk(config)
    }
}

/// Per-query view of SDK-hosted MCP servers and external CLI configurations.
#[derive(Clone, Default)]
pub(crate) struct McpServerRegistry {
    sdk_servers: BTreeMap<String, SdkMcpServerConfig>,
    external_servers: Map<String, Value>,
    active_requests: ActiveMcpRequests,
}

impl McpServerRegistry {
    pub(crate) fn from_configs(
        configs: &BTreeMap<String, McpServerConfig>,
    ) -> Result<Self, QueryError> {
        let mut registry = Self::default();
        for (key, config) in configs {
            match config {
                McpServerConfig::External(value) => {
                    if !value.is_object() {
                        return Err(QueryError::InvalidOptions(format!(
                            "MCP server '{key}' config must be an object"
                        )));
                    }
                    registry.external_servers.insert(key.clone(), value.clone());
                }
                McpServerConfig::Sdk(server) => {
                    let server_name = if server.name.is_empty() {
                        key.clone()
                    } else {
                        server.name.clone()
                    };
                    let normalized = SdkMcpServerConfig::new(
                        server_name.clone(),
                        server.version.clone(),
                        server.tools.clone(),
                    )?
                    .with_context_tools(server.context_tools.clone())?;
                    registry.sdk_servers.insert(
                        server_name,
                        SdkMcpServerConfig {
                            context_tools: server.context_tools.clone(),
                            resources: server.resources.clone(),
                            resource_templates: server.resource_templates.clone(),
                            prompts: server.prompts.clone(),
                            ..normalized
                        },
                    );
                }
            }
        }
        Ok(registry)
    }

    pub(crate) fn insert_initialize_fields(&self, initialize: &mut Map<String, Value>) {
        if !self.sdk_servers.is_empty() {
            let sdk_servers = self
                .sdk_servers
                .keys()
                .map(|name| (name.clone(), json!({ "type": "sdk", "name": name })))
                .collect::<Map<_, _>>();
            initialize.insert("sdkMcpServers".into(), Value::Object(sdk_servers));
        }
        if !self.external_servers.is_empty() {
            initialize.insert(
                "mcpServers".into(),
                Value::Object(self.external_servers.clone()),
            );
        }
    }

    pub(crate) fn contains_sdk_server(&self, name: &str) -> bool {
        self.sdk_servers.contains_key(name)
    }

    pub(crate) fn dispatch(
        &self,
        server_name: String,
        message: Value,
    ) -> BoxFuture<'static, Result<Value, String>> {
        let Some(server) = self.sdk_servers.get(&server_name).cloned() else {
            return Box::pin(async move {
                Err(format!(
                    "MCP server '{server_name}' not found in SDK-embedded servers"
                ))
            });
        };
        let active_requests = self.active_requests.clone();
        Box::pin(
            async move { dispatch_message(server_name, server, message, active_requests).await },
        )
    }
}

async fn dispatch_message(
    server_name: String,
    server: SdkMcpServerConfig,
    message: Value,
    active_requests: ActiveMcpRequests,
) -> Result<Value, String> {
    let object = message
        .as_object()
        .ok_or_else(|| "Invalid MCP JSON-RPC message: expected an object".to_owned())?;
    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "Invalid MCP JSON-RPC message: method must be a string".to_owned())?;
    let id = object.get("id").filter(|id| !id.is_null()).cloned();
    let Some(id) = id else {
        if method == "notifications/cancelled"
            && let Some(request_id) = object
                .get("params")
                .and_then(|params| params.get("requestId"))
        {
            active_requests.cancel(&server_name, request_id);
        }
        // MCP notifications have no response. The outer SDK control response
        // still acknowledges delivery, as the TypeScript transport does.
        return Ok(json!({ "jsonrpc": "2.0", "result": {}, "id": 0 }));
    };

    let meta = object
        .get("params")
        .and_then(|params| params.get("_meta"))
        .cloned();
    let (request_context, request_guard) = active_requests.register(&server_name, id.clone(), meta);

    let response = async {
        let result = match method {
            "initialize" => {
                let protocol_version = object
                    .get("params")
                    .and_then(|params| params.get("protocolVersion"))
                    .and_then(Value::as_str)
                    .unwrap_or("2025-06-18");
                let mut capabilities = Map::new();
                if server.tools.is_some() || !server.context_tools.is_empty() {
                    capabilities.insert("tools".into(), json!({}));
                }
                if server.resources.is_some() || server.resource_templates.is_some() {
                    capabilities.insert("resources".into(), json!({ "listChanged": true }));
                }
                if server.prompts.is_some() {
                    capabilities.insert("prompts".into(), json!({}));
                }
                Ok(json!({
                    "protocolVersion": protocol_version,
                    "capabilities": capabilities,
                    "serverInfo": { "name": server.name, "version": server.version }
                }))
            }
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({
                "tools": server.tools.as_deref().unwrap_or_default().iter()
                    .map(|tool| json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema,
                    }))
                    .chain(server.context_tools.iter().map(|tool| json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.input_schema,
                    })))
                    .collect::<Vec<_>>()
            })),
            "tools/call" => {
                let params = object.get("params").and_then(Value::as_object);
                let tool_name = params
                    .and_then(|params| params.get("name"))
                    .and_then(Value::as_str);
                let Some(tool_name) = tool_name else {
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        "Invalid params: tools/call requires a tool name",
                    ));
                };
                let contextual_tool = server
                    .context_tools
                    .iter()
                    .find(|tool| tool.name == tool_name);
                let plain_tool = server
                    .tools
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .find(|tool| tool.name == tool_name);
                if contextual_tool.is_none() && plain_tool.is_none() {
                    return Ok(json_rpc_error(id, -32602, "Unknown tool"));
                }
                let arguments = params
                    .and_then(|params| params.get("arguments"))
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Map::new()));
                if !arguments.is_object() {
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        "Invalid params: tool arguments must be an object",
                    ));
                }
                let handler_result = if let Some(tool) = contextual_tool {
                    (tool.handler)(arguments, request_context.clone()).await
                } else if let Some(tool) = plain_tool {
                    (tool.handler)(arguments).await
                } else {
                    unreachable!("tool existence checked above")
                };
                match handler_result {
                    Ok(result) => Ok(result),
                    Err(error) => return Ok(json_rpc_error(id, -32603, &error)),
                }
            }
            "resources/list" => {
                let mut resources = server
                    .resources
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|resource| {
                        let mut item = resource.metadata.clone();
                        item.insert("name".into(), Value::String(resource.name.clone()));
                        item.insert("uri".into(), Value::String(resource.uri.clone()));
                        Value::Object(item)
                    })
                    .collect::<Vec<_>>();
                for template in server.resource_templates.as_deref().unwrap_or_default() {
                    let Some(list_handler) = &template.list_handler else {
                        continue;
                    };
                    let listed = match list_handler(request_context.clone()).await {
                        Ok(listed) => listed,
                        Err(error) => return Ok(json_rpc_error(id, -32603, &error)),
                    };
                    let Some(template_resources) =
                        listed.get("resources").and_then(Value::as_array)
                    else {
                        return Ok(json_rpc_error(
                            id,
                            -32603,
                            "Resource template list callback must return a resources array",
                        ));
                    };
                    for resource in template_resources {
                        let Some(resource) = resource.as_object() else {
                            return Ok(json_rpc_error(
                                id,
                                -32603,
                                "Resource template list callback returned a non-object resource",
                            ));
                        };
                        let mut item = template.metadata.clone();
                        item.extend(resource.clone());
                        resources.push(Value::Object(item));
                    }
                }
                Ok(json!({
                    "resources": resources
                }))
            }
            "resources/templates/list" => {
                let templates = server
                    .resource_templates
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(|template| {
                        let mut item = template.metadata.clone();
                        item.insert("name".into(), Value::String(template.name.clone()));
                        item.insert(
                            "uriTemplate".into(),
                            Value::String(template.uri_template.clone()),
                        );
                        Value::Object(item)
                    })
                    .collect::<Vec<_>>();
                Ok(json!({ "resourceTemplates": templates }))
            }
            "resources/read" => {
                let uri = object
                    .get("params")
                    .and_then(|params| params.get("uri"))
                    .and_then(Value::as_str);
                let Some(uri) = uri else {
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        "Invalid params: resources/read requires a URI",
                    ));
                };
                if let Some(resource) = server
                    .resources
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .find(|resource| resource.uri == uri)
                {
                    let handler_result = if let Some(handler) = &resource.context_handler {
                        (handler)(uri.to_owned(), request_context.clone()).await
                    } else {
                        (resource.handler)(uri.to_owned()).await
                    };
                    match handler_result {
                        Ok(result) => Ok(result),
                        Err(error) => return Ok(json_rpc_error(id, -32603, &error)),
                    }
                } else {
                    for template in server.resource_templates.as_deref().unwrap_or_default() {
                        let Some(variables) =
                            match_resource_uri_template(&template.uri_template, uri)
                        else {
                            continue;
                        };
                        let handler_result = if let Some(handler) = &template.context_handler {
                            (handler)(uri.to_owned(), variables, request_context.clone()).await
                        } else {
                            (template.handler)(uri.to_owned(), variables).await
                        };
                        return match handler_result {
                            Ok(result) => Ok(result),
                            Err(error) => Ok(json_rpc_error(id, -32603, &error)),
                        };
                    }
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        &format!("Resource {uri} not found"),
                    ));
                }
            }
            "prompts/list" => {
                let prompts = server.prompts.as_deref().unwrap_or_default();
                Ok(json!({
                    "prompts": prompts.iter().map(|prompt| {
                        let mut item = prompt.metadata.clone();
                        item.insert("name".into(), Value::String(prompt.name.clone()));
                        if let Some(description) = &prompt.description {
                            item.insert("description".into(), Value::String(description.clone()));
                        }
                        if let Some(arguments) = &prompt.arguments {
                            item.insert("arguments".into(), Value::Array(arguments.clone()));
                        }
                        Value::Object(item)
                    }).collect::<Vec<_>>()
                }))
            }
            "prompts/get" => {
                let params = object.get("params").and_then(Value::as_object);
                let prompt_name = params
                    .and_then(|params| params.get("name"))
                    .and_then(Value::as_str);
                let Some(prompt_name) = prompt_name else {
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        "Invalid params: prompts/get requires a prompt name",
                    ));
                };
                let Some(prompt) = server
                    .prompts
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .find(|prompt| prompt.name == prompt_name)
                else {
                    return Ok(json_rpc_error(
                        id,
                        -32602,
                        &format!("Prompt {prompt_name} not found"),
                    ));
                };
                let arguments = match params.and_then(|params| params.get("arguments")) {
                    None | Some(Value::Null) => Value::Object(Map::new()),
                    Some(Value::Object(arguments)) => Value::Object(arguments.clone()),
                    Some(_) => {
                        return Ok(json_rpc_error(
                            id,
                            -32602,
                            "Invalid params: prompt arguments must be an object",
                        ));
                    }
                };
                let handler_result = if let Some(handler) = &prompt.context_handler {
                    (handler)(arguments, request_context.clone()).await
                } else {
                    (prompt.handler)(arguments).await
                };
                match handler_result {
                    Ok(result) => Ok(result),
                    Err(error) => return Ok(json_rpc_error(id, -32603, &error)),
                }
            }
            "notifications/initialized" | "notifications/progress" => Ok(json!({})),
            _ => return Ok(json_rpc_error(id, -32601, "Method not found")),
        };

        result.map(|result| json!({ "jsonrpc": "2.0", "result": result, "id": id }))
    }
    .await;
    request_guard.complete();
    response
}

#[derive(Clone, Debug)]
enum ResourceUriTemplatePart {
    Literal(String),
    Expression {
        operator: char,
        names: Vec<String>,
        exploded: bool,
    },
}

fn parse_resource_uri_template(template: &str) -> Result<Vec<ResourceUriTemplatePart>, QueryError> {
    const MAX_TEMPLATE_BYTES: usize = 1_000_000;
    const MAX_TEMPLATE_EXPRESSIONS: usize = 256;
    if template.is_empty() {
        return Err(QueryError::InvalidOptions(
            "MCP resource URI template must be non-empty".into(),
        ));
    }
    if template.len() > MAX_TEMPLATE_BYTES {
        return Err(QueryError::InvalidOptions(
            "MCP resource URI template exceeds 1 MB".into(),
        ));
    }

    let mut parts = Vec::new();
    let mut cursor = 0;
    let mut expression_count = 0;
    while let Some(open_relative) = template[cursor..].find('{') {
        let open = cursor + open_relative;
        if open > cursor {
            parts.push(ResourceUriTemplatePart::Literal(
                template[cursor..open].to_owned(),
            ));
        }
        let expression_start = open + 1;
        let Some(close_relative) = template[expression_start..].find('}') else {
            return Err(QueryError::InvalidOptions(
                "MCP resource URI template has an unclosed expression".into(),
            ));
        };
        let close = expression_start + close_relative;
        let expression = &template[expression_start..close];
        let operator = expression
            .chars()
            .next()
            .filter(|operator| matches!(operator, '+' | '#' | '.' | '/' | '?' | '&'))
            .unwrap_or('\0');
        let variable_text = if operator == '\0' {
            expression
        } else {
            &expression[operator.len_utf8()..]
        };
        let exploded = expression.contains('*');
        let names = variable_text
            .split(',')
            .map(|name| name.replace('*', "").trim().to_owned())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        if names.is_empty() {
            return Err(QueryError::InvalidOptions(
                "MCP resource URI template expression has no variable name".into(),
            ));
        }
        if names.iter().any(|name| name.len() > MAX_TEMPLATE_BYTES) {
            return Err(QueryError::InvalidOptions(
                "MCP resource URI template variable name exceeds 1 MB".into(),
            ));
        }
        expression_count += 1;
        if expression_count > MAX_TEMPLATE_EXPRESSIONS {
            return Err(QueryError::InvalidOptions(format!(
                "MCP resource URI template has more than {MAX_TEMPLATE_EXPRESSIONS} expressions"
            )));
        }
        parts.push(ResourceUriTemplatePart::Expression {
            operator,
            names,
            exploded,
        });
        cursor = close + 1;
    }
    if cursor < template.len() {
        parts.push(ResourceUriTemplatePart::Literal(
            template[cursor..].to_owned(),
        ));
    }
    Ok(parts)
}

fn match_resource_uri_template(template: &str, uri: &str) -> Option<Map<String, Value>> {
    if uri.len() > 1_000_000 {
        return None;
    }
    let parts = parse_resource_uri_template(template).ok()?;
    match_resource_uri_template_parts(&parts, 0, uri, 0, Map::new())
}

fn match_resource_uri_template_parts(
    parts: &[ResourceUriTemplatePart],
    part_index: usize,
    uri: &str,
    uri_offset: usize,
    variables: Map<String, Value>,
) -> Option<Map<String, Value>> {
    if part_index == parts.len() {
        return (uri_offset == uri.len()).then_some(variables);
    }
    match &parts[part_index] {
        ResourceUriTemplatePart::Literal(literal) => {
            uri.get(uri_offset..)?.strip_prefix(literal).and_then(|_| {
                match_resource_uri_template_parts(
                    parts,
                    part_index + 1,
                    uri,
                    uri_offset + literal.len(),
                    variables,
                )
            })
        }
        ResourceUriTemplatePart::Expression {
            operator,
            names,
            exploded,
        } if matches!(*operator, '?' | '&') => match_resource_query_variables(
            parts,
            part_index + 1,
            uri,
            uri_offset,
            names,
            0,
            *operator,
            *exploded,
            variables,
        ),
        ResourceUriTemplatePart::Expression {
            operator,
            names,
            exploded,
        } => {
            // The current TypeScript UriTemplate matcher builds one capture
            // for non-query expressions, using the first name in the part.
            // Keep this behavior aligned; query expressions are handled above
            // and capture each named query parameter.
            let variable_name = names.first()?;
            let mut value_offset = uri_offset;
            if matches!(operator, '.' | '/') {
                let expected_prefix = if *operator == '.' { "." } else { "/" };
                if !uri.get(value_offset..)?.starts_with(expected_prefix) {
                    return None;
                }
                value_offset += expected_prefix.len();
            }
            let allows_path_separators = matches!(operator, '+' | '#');
            let mut ends = uri[value_offset..]
                .char_indices()
                .map(|(offset, _)| value_offset + offset)
                .chain(std::iter::once(uri.len()))
                .collect::<Vec<_>>();
            ends.reverse();
            for end in ends {
                let Some(value) = uri.get(value_offset..end) else {
                    continue;
                };
                if !valid_resource_template_value(value, *exploded, allows_path_separators) {
                    continue;
                }
                let mut next_variables = variables.clone();
                next_variables.insert(
                    variable_name.clone(),
                    resource_template_variable_value(value, *exploded),
                );
                if let Some(matched) = match_resource_uri_template_parts(
                    parts,
                    part_index + 1,
                    uri,
                    end,
                    next_variables,
                ) {
                    return Some(matched);
                }
            }
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn match_resource_query_variables(
    parts: &[ResourceUriTemplatePart],
    next_part: usize,
    uri: &str,
    uri_offset: usize,
    names: &[String],
    name_index: usize,
    operator: char,
    exploded: bool,
    variables: Map<String, Value>,
) -> Option<Map<String, Value>> {
    let name = names.get(name_index)?;
    let separator = if name_index == 0 { operator } else { '&' };
    let prefix = format!("{separator}{name}=");
    let value_offset = uri_offset + prefix.len();
    if !uri.get(uri_offset..)?.starts_with(&prefix) {
        return None;
    }
    let value_limit = uri[value_offset..]
        .find('&')
        .map_or(uri.len(), |relative| value_offset + relative);
    let mut ends = uri[value_offset..value_limit]
        .char_indices()
        .map(|(offset, _)| value_offset + offset)
        .chain(std::iter::once(value_limit))
        .collect::<Vec<_>>();
    ends.reverse();
    for end in ends {
        let Some(value) = uri.get(value_offset..end).filter(|value| !value.is_empty()) else {
            continue;
        };
        let mut next_variables = variables.clone();
        next_variables.insert(
            name.clone(),
            resource_template_variable_value(value, exploded),
        );
        let matched = if name_index + 1 < names.len() {
            match_resource_query_variables(
                parts,
                next_part,
                uri,
                end,
                names,
                name_index + 1,
                operator,
                exploded,
                next_variables,
            )
        } else {
            match_resource_uri_template_parts(parts, next_part, uri, end, next_variables)
        };
        if matched.is_some() {
            return matched;
        }
    }
    None
}

fn valid_resource_template_value(value: &str, exploded: bool, allow_path: bool) -> bool {
    if value.is_empty() {
        return false;
    }
    if allow_path {
        return true;
    }
    if !exploded {
        return !value.contains('/') && !value.contains(',');
    }
    let mut saw_value = false;
    for value in value.split(',') {
        if value.is_empty() || value.contains('/') {
            return false;
        }
        saw_value = true;
    }
    saw_value
}

fn resource_template_variable_value(value: &str, exploded: bool) -> Value {
    if exploded && value.contains(',') {
        Value::Array(
            value
                .split(',')
                .map(|item| Value::String(item.to_owned()))
                .collect(),
        )
    } else {
        Value::String(value.to_owned())
    }
}

fn json_rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": message },
        "id": id,
    })
}

fn validate_tool_name(name: &str) -> Result<(), QueryError> {
    if name.is_empty() {
        return Err(QueryError::InvalidOptions(
            "Tool name cannot be empty".into(),
        ));
    }
    if name.chars().count() > 64 {
        return Err(QueryError::InvalidOptions(format!(
            "Tool name '{name}' is too long (max 64 characters): {}",
            name.chars().count()
        )));
    }
    let mut chars = name.chars();
    let starts_with_letter = chars.next().is_some_and(|ch| ch.is_ascii_alphabetic());
    if !starts_with_letter || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(QueryError::InvalidOptions(format!(
            "Tool name '{name}' is invalid. Must start with a letter and contain only letters, numbers, and underscores."
        )));
    }
    Ok(())
}
