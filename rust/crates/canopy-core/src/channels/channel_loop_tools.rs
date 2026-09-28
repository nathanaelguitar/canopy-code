//! MCP tool surface for channel recurring loops.
//!
//! Port of `packages/channels/base/src/ChannelLoopTools.ts`. The handler trait
//! is a small async seam for the daemon to connect to its channel-loop service;
//! this module owns only the JSON-RPC and tool argument contract.

use serde_json::{Map, Number, Value, json};
use std::future::Future;
use std::pin::Pin;
use std::sync::LazyLock;

pub const CHANNEL_LOOP_MCP_SERVER_NAME: &str = "channel_loop";
pub const CLIENT_MCP_MESSAGE_METHOD: &str = "qwen/control/client_mcp/message";
pub const WORKSPACE_MCP_RUNTIME_ADD_METHOD: &str = "qwen/control/workspace/mcp/runtime-add";
pub const CLIENT_MCP_OVER_WS_CONFIG_FLAG: &str = "__clientMcpOverWs";

pub type JsonRpcMessage = Map<String, Value>;
pub type ChannelLoopHandlerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ChannelLoopToolResult, String>> + Send + 'a>>;

/// Schemas advertised by `tools/list`, matching the TypeScript constants.
pub static CHANNEL_LOOP_MCP_TOOLS: LazyLock<Value> = LazyLock::new(|| {
    json!([
        {
            "name": "channel_loop_create",
            "description": "Create a recurring proactive reminder or scheduled prompt for the current channel chat. Use this in channel sessions instead of cron_create.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cron": {
                        "type": "string",
                        "description": "Standard 5-field cron expression in local time, for example \"*/5 * * * *\"."
                    },
                    "prompt": {
                        "type": "string",
                        "description": "The message or instruction to run and proactively push to this channel chat."
                    },
                    "recurring": {
                        "type": "boolean",
                        "description": "Whether the loop recurs. Defaults to true."
                    }
                },
                "required": ["cron", "prompt"]
            }
        },
        {
            "name": "channel_loop_list",
            "description": "List proactive loops for the current channel chat.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "channel_loop_cancel",
            "description": "Cancel a proactive loop for the current channel chat.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Loop id to cancel." }
                },
                "required": ["id"]
            }
        }
    ])
});

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChannelLoopMcpContext {
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelLoopToolCreateInput {
    pub cron: String,
    pub prompt: String,
    pub recurring: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelLoopToolResult {
    pub text: String,
    pub is_error: bool,
}

impl ChannelLoopToolResult {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: false,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            is_error: true,
        }
    }
}

/// Async integration seam implemented by the channel-loop service.
///
/// The returned futures make the trait object safe without imposing an async
/// runtime or coupling the MCP protocol layer to storage.
pub trait ChannelLoopToolHandler: Send + Sync {
    fn create<'a>(
        &'a self,
        session_id: String,
        input: ChannelLoopToolCreateInput,
    ) -> ChannelLoopHandlerFuture<'a>;

    fn list<'a>(&'a self, session_id: String) -> ChannelLoopHandlerFuture<'a>;

    fn cancel<'a>(&'a self, session_id: String, id: String) -> ChannelLoopHandlerFuture<'a>;
}

pub struct ChannelLoopMcpServer<H> {
    handler: H,
}

impl<H: ChannelLoopToolHandler> ChannelLoopMcpServer<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }

    /// Process a JSON-RPC request. Notifications (missing or null IDs) do not
    /// produce responses, matching the source bridge's request handling.
    pub async fn handle_message(
        &self,
        message: &JsonRpcMessage,
        context: &ChannelLoopMcpContext,
    ) -> Option<Value> {
        let id = message.get("id")?;
        if id.is_null() {
            return None;
        }

        let result = match self.dispatch(message, context).await {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(error) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32603, "message": error }
            }),
        };
        Some(result)
    }

    async fn dispatch(
        &self,
        message: &JsonRpcMessage,
        context: &ChannelLoopMcpContext,
    ) -> Result<Value, String> {
        match message.get("method").and_then(Value::as_str) {
            Some("initialize") => Ok(json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": {} },
                "serverInfo": { "name": CHANNEL_LOOP_MCP_SERVER_NAME, "version": "0.0.1" }
            })),
            Some("tools/list") => Ok(json!({ "tools": CHANNEL_LOOP_MCP_TOOLS.clone() })),
            Some("tools/call") => self.call_tool(message.get("params"), context).await,
            Some("ping") => Ok(json!({})),
            _ => Err(format!(
                "Method not found: {}",
                js_string(message.get("method"))
            )),
        }
    }

    async fn call_tool(
        &self,
        raw_params: Option<&Value>,
        context: &ChannelLoopMcpContext,
    ) -> Result<Value, String> {
        // Keep validation order: the TypeScript handler checks the session
        // before it validates the call parameters.
        let session_id = context
            .session_id
            .as_deref()
            .filter(|session_id| !session_id.is_empty())
            .ok_or_else(|| "Missing channel session id.".to_owned())?
            .to_owned();

        let params = raw_params
            .filter(|value| value.is_object() || value.is_array())
            .ok_or_else(|| "Invalid tools/call params.".to_owned())?;
        let arguments = params
            .get("arguments")
            .filter(|value| value.is_object() || value.is_array());
        let name = params.get("name");

        let tool_result = match name.and_then(Value::as_str) {
            Some("channel_loop_create") => {
                self.handler
                    .create(session_id, read_create_input(arguments)?)
                    .await?
            }
            Some("channel_loop_list") => self.handler.list(session_id).await?,
            Some("channel_loop_cancel") => {
                self.handler.cancel(session_id, read_id(arguments)?).await?
            }
            _ => {
                return Err(format!("Unknown channel loop tool: {}", js_string(name)));
            }
        };

        let mut result = json!({
            "content": [{ "type": "text", "text": tool_result.text }]
        });
        if tool_result.is_error {
            result["isError"] = Value::Bool(true);
        }
        Ok(result)
    }
}

fn read_create_input(args: Option<&Value>) -> Result<ChannelLoopToolCreateInput, String> {
    let cron = args.and_then(|args| args.get("cron"));
    let prompt = args.and_then(|args| args.get("prompt"));
    let cron = read_non_empty_string(cron, "cron must be a non-empty string.")?;
    let prompt = read_non_empty_string(prompt, "prompt must be a non-empty string.")?;
    let recurring = args
        .and_then(|args| args.get("recurring"))
        .and_then(read_recurring);
    Ok(ChannelLoopToolCreateInput {
        cron,
        prompt,
        recurring,
    })
}

fn read_id(args: Option<&Value>) -> Result<String, String> {
    read_non_empty_string(
        args.and_then(|args| args.get("id")),
        "id must be a non-empty string.",
    )
}

fn read_non_empty_string(value: Option<&Value>, error: &str) -> Result<String, String> {
    match value.and_then(Value::as_str) {
        Some(value) => {
            let trimmed = js_trim(value);
            if trimmed.is_empty() {
                Err(error.to_owned())
            } else {
                Ok(trimmed.to_owned())
            }
        }
        None => Err(error.to_owned()),
    }
}

fn read_recurring(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) if value == "false" => Some(false),
        Value::String(value) if value == "true" => Some(true),
        Value::Number(value) if js_number_equals(value, 0.0) => Some(false),
        Value::Number(value) if js_number_equals(value, 1.0) => Some(true),
        _ => None,
    }
}

fn js_number_equals(value: &Number, expected: f64) -> bool {
    value.as_f64() == Some(expected)
}

/// JavaScript's String conversion for values that can appear in JSON-RPC
/// method and tool names. Missing properties are represented by `None`.
fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_owned(),
        Some(Value::Null) => "null".to_owned(),
        Some(Value::Bool(value)) => value.to_string(),
        Some(Value::Number(value)) => js_number_string(value),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                _ => js_string(Some(value)),
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".to_owned(),
    }
}

fn js_number_string(value: &Number) -> String {
    if let Some(value) = value.as_i64() {
        return value.to_string();
    }
    if let Some(value) = value.as_u64() {
        return value.to_string();
    }
    value
        .as_f64()
        .map_or_else(|| value.to_string(), |value| value.to_string())
}

/// `str::trim` omits FEFF although JavaScript `String.prototype.trim`
/// includes it. Keep the source coercion/validation behavior for such input.
fn js_trim(value: &str) -> &str {
    value.trim_matches(|character: char| {
        matches!(
            character,
            '\u{0009}'..='\u{000d}'
                | '\u{0020}'
                | '\u{00a0}'
                | '\u{1680}'
                | '\u{2000}'..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    #[derive(Default)]
    struct RecordingHandler {
        calls: Mutex<Vec<String>>,
        fail_create_as_tool_error: bool,
    }

    impl ChannelLoopToolHandler for RecordingHandler {
        fn create<'a>(
            &'a self,
            session_id: String,
            input: ChannelLoopToolCreateInput,
        ) -> ChannelLoopHandlerFuture<'a> {
            self.calls.lock().unwrap().push(format!(
                "create:{session_id}:{}:{}:{:?}",
                input.cron, input.prompt, input.recurring
            ));
            Box::pin(async move {
                if self.fail_create_as_tool_error {
                    Ok(ChannelLoopToolResult::error("loops are unavailable"))
                } else {
                    Ok(ChannelLoopToolResult::text("created"))
                }
            })
        }

        fn list<'a>(&'a self, session_id: String) -> ChannelLoopHandlerFuture<'a> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("list:{session_id}"));
            Box::pin(async { Ok(ChannelLoopToolResult::text("No loops.")) })
        }

        fn cancel<'a>(&'a self, session_id: String, id: String) -> ChannelLoopHandlerFuture<'a> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("cancel:{session_id}:{id}"));
            Box::pin(async { Ok(ChannelLoopToolResult::text("cancelled")) })
        }
    }

    fn request(value: Value) -> JsonRpcMessage {
        value.as_object().unwrap().clone()
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test handler unexpectedly returned Pending"),
        }
    }

    fn server(handler: RecordingHandler) -> ChannelLoopMcpServer<RecordingHandler> {
        ChannelLoopMcpServer::new(handler)
    }

    fn context() -> ChannelLoopMcpContext {
        ChannelLoopMcpContext {
            session_id: Some("s-1".into()),
        }
    }

    #[test]
    fn advertises_exact_tool_names_and_initialize_contract() {
        let server = server(RecordingHandler::default());
        let listed = block_on(server.handle_message(
            &request(json!({ "id": 1, "method": "tools/list" })),
            &context(),
        ))
        .unwrap();
        let names = listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "channel_loop_create",
                "channel_loop_list",
                "channel_loop_cancel"
            ]
        );
        assert_eq!(listed["result"]["tools"], *CHANNEL_LOOP_MCP_TOOLS);

        let initialized = block_on(server.handle_message(
            &request(json!({ "id": 2, "method": "initialize" })),
            &context(),
        ))
        .unwrap();
        assert_eq!(initialized["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(
            initialized["result"]["serverInfo"],
            json!({ "name": "channel_loop", "version": "0.0.1" })
        );
    }

    #[test]
    fn ignores_notifications_and_answers_ping() {
        let server = server(RecordingHandler::default());
        assert!(
            block_on(server.handle_message(
                &request(json!({ "method": "notifications/initialized" })),
                &context(),
            ))
            .is_none()
        );
        assert!(
            block_on(server.handle_message(
                &request(json!({ "id": null, "method": "ping" })),
                &context(),
            ))
            .is_none()
        );
        assert_eq!(
            block_on(
                server.handle_message(&request(json!({ "id": 7, "method": "ping" })), &context(),)
            ),
            Some(json!({ "jsonrpc": "2.0", "id": 7, "result": {} }))
        );
    }

    #[test]
    fn routes_create_with_trimmed_arguments_and_source_recurring_coercion() {
        let handler = RecordingHandler::default();
        let server = server(handler);
        for (recurring, expected) in [
            (json!("false"), Some(false)),
            (json!(0), Some(false)),
            (json!("true"), Some(true)),
            (json!(1), Some(true)),
            (json!("False"), None),
        ] {
            block_on(server.handle_message(
                &request(json!({
                    "id": 1,
                    "method": "tools/call",
                    "params": { "name": "channel_loop_create", "arguments": {
                        "cron": " \t*/5 * * * *\n", "prompt": " drink water ", "recurring": recurring
                    } }
                })),
                &context(),
            ))
            .unwrap();
            let expected_text = format!("create:s-1:*/5 * * * *:drink water:{expected:?}");
            assert_eq!(
                server.handler.calls.lock().unwrap().last().unwrap(),
                &expected_text
            );
        }
    }

    #[test]
    fn routes_list_cancel_and_converts_handler_errors_to_tool_results() {
        let handler = RecordingHandler {
            fail_create_as_tool_error: true,
            ..RecordingHandler::default()
        };
        let server = server(handler);
        let listed = block_on(server.handle_message(
            &request(json!({ "id": 1, "method": "tools/call", "params": { "name": "channel_loop_list" } })),
            &context(),
        ))
        .unwrap();
        assert_eq!(listed["result"]["content"][0]["text"], "No loops.");
        assert_eq!(
            server.handler.calls.lock().unwrap().last().unwrap(),
            "list:s-1"
        );

        let cancelled = block_on(server.handle_message(
            &request(json!({ "id": 2, "method": "tools/call", "params": {
                "name": "channel_loop_cancel", "arguments": { "id": " \u{feff}job-1\u{feff} " }
            } })),
            &context(),
        ))
        .unwrap();
        assert_eq!(cancelled["result"]["content"][0]["text"], "cancelled");
        assert_eq!(
            server.handler.calls.lock().unwrap().last().unwrap(),
            "cancel:s-1:job-1"
        );

        let failed = block_on(server.handle_message(
            &request(json!({ "id": 3, "method": "tools/call", "params": {
                "name": "channel_loop_create", "arguments": { "cron": "* * * * *", "prompt": "hi" }
            } })),
            &context(),
        ))
        .unwrap();
        assert_eq!(failed["result"]["isError"], true);
        assert_eq!(
            failed["result"]["content"][0]["text"],
            "loops are unavailable"
        );
    }

    #[test]
    fn returns_json_rpc_errors_in_source_validation_order() {
        let server = server(RecordingHandler::default());
        let missing_session = block_on(server.handle_message(
            &request(json!({ "id": 4, "method": "tools/call" })),
            &ChannelLoopMcpContext::default(),
        ))
        .unwrap();
        assert_eq!(
            missing_session["error"]["message"],
            "Missing channel session id."
        );

        let invalid_params = block_on(server.handle_message(
            &request(json!({ "id": 5, "method": "tools/call", "params": null })),
            &context(),
        ))
        .unwrap();
        assert_eq!(
            invalid_params["error"]["message"],
            "Invalid tools/call params."
        );

        let invalid_cron = block_on(server.handle_message(
            &request(json!({ "id": 6, "method": "tools/call", "params": {
                "name": "channel_loop_create", "arguments": { "cron": "  \u{feff}", "prompt": "x" }
            } })),
            &context(),
        ))
        .unwrap();
        assert_eq!(
            invalid_cron["error"]["message"],
            "cron must be a non-empty string."
        );

        let unknown = block_on(server.handle_message(
            &request(json!({ "id": 7, "method": "missing" })),
            &context(),
        ))
        .unwrap();
        assert_eq!(unknown["error"]["code"], -32603);
        assert_eq!(unknown["error"]["message"], "Method not found: missing");

        let unknown_tool = block_on(server.handle_message(
            &request(json!({ "id": 8, "method": "tools/call", "params": { "name": "unknown" } })),
            &context(),
        ))
        .unwrap();
        assert_eq!(
            unknown_tool["error"]["message"],
            "Unknown channel loop tool: unknown"
        );
    }
}
