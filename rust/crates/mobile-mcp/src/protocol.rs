//! Minimal MCP JSON-RPC dispatcher for stdio and HTTP transports.

use serde_json::{Value, json};

use crate::filter::{self, FilterDirection};
use crate::tools::{MobileServer, ToolError};

const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[
    LATEST_PROTOCOL_VERSION,
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
    "2024-10-07",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidJson(String),
    InvalidRequest(String),
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJson(error) => write!(f, "Invalid JSON: {error}"),
            Self::InvalidRequest(error) => f.write_str(error),
        }
    }
}

impl std::error::Error for ProtocolError {}

pub fn payload_filter_enabled() -> bool {
    std::env::var("MCP_MODEL_PAYLOAD_FILTER").is_ok_and(|value| value == "1")
}

pub fn handle_json_line(
    server: &mut MobileServer,
    line: &[u8],
) -> Result<Option<Value>, ProtocolError> {
    let mut request: Value = serde_json::from_slice(line)
        .map_err(|error| ProtocolError::InvalidJson(error.to_string()))?;
    if payload_filter_enabled() {
        request = match filter::transform_message(&request, FilterDirection::Decode) {
            Ok(value) => value,
            Err(error) => {
                return Ok(Some(error_response(
                    request.get("id").cloned(),
                    -32602,
                    error.to_string(),
                )));
            }
        };
    }

    let response = handle_request(server, &request)?;
    if payload_filter_enabled() {
        return response
            .map(|response| {
                filter::transform_message(&response, FilterDirection::Encode)
                    .map_err(|error| ProtocolError::InvalidRequest(error.to_string()))
            })
            .transpose();
    }
    Ok(response)
}

fn handle_request(
    server: &mut MobileServer,
    request: &Value,
) -> Result<Option<Value>, ProtocolError> {
    let Some(object) = request.as_object() else {
        return Ok(Some(error_response(None, -32600, "Invalid Request".into())));
    };

    let id = match object.get("id") {
        None => None,
        Some(value @ Value::String(_)) => Some(value.clone()),
        Some(Value::Number(value))
            if value.is_i64()
                || value.is_u64()
                || value.as_f64().is_some_and(|number| number.fract() == 0.0) =>
        {
            Some(Value::Number(value.clone()))
        }
        Some(_) => {
            return Ok(Some(error_response(
                None,
                -32600,
                "Invalid Request: id must be a string or integer".into(),
            )));
        }
    };
    let is_notification = id.is_none();

    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "jsonrpc" | "id" | "method" | "params"))
    {
        return Ok(Some(error_response(
            None,
            -32600,
            "Invalid Request: unexpected top-level field".into(),
        )));
    }

    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Ok(Some(error_response(
            None,
            -32600,
            "Invalid Request: jsonrpc must be \"2.0\"".into(),
        )));
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return Ok(Some(error_response(
            None,
            -32600,
            "Invalid Request: method must be a string".into(),
        )));
    };

    if object
        .get("params")
        .is_some_and(|params| !params.is_object())
    {
        return Ok(Some(error_response(
            id,
            -32602,
            "Invalid params: params must be an object".into(),
        )));
    }

    // MCP request methods sent without an id are notifications. The TypeScript
    // SDK ignores them because it has no matching notification handler; in
    // particular, do not run a mobile tool as a side effect of such a frame.
    if is_notification && matches!(method, "initialize" | "ping" | "tools/list" | "tools/call") {
        return Ok(None);
    }

    let response = match method {
        "notifications/initialized" | "notifications/cancelled" | "notifications/progress"
            if is_notification =>
        {
            None
        }
        "initialize" => match initialize_result(object) {
            Ok(result) => {
                let client_name = object
                    .get("params")
                    .and_then(Value::as_object)
                    .and_then(|params| params.get("clientInfo"))
                    .and_then(Value::as_object)
                    .and_then(|client_info| client_info.get("name"))
                    .and_then(Value::as_str);
                server.set_client_name(client_name);
                Some(result)
            }
            Err(message) => {
                return Ok(request_error(id, -32602, message));
            }
        },
        "ping" => Some(json!({})),
        "tools/list" => Some(json!({"tools": server.tools()})),
        "tools/call" => {
            let Some(params) = object.get("params").and_then(Value::as_object) else {
                return Ok(request_error(
                    id,
                    -32602,
                    "Invalid tools/call request: params must be an object".into(),
                ));
            };
            let Some(name) = params.get("name").and_then(Value::as_str) else {
                return Ok(request_error(
                    id,
                    -32602,
                    "Invalid tools/call request: params.name must be a string".into(),
                ));
            };
            if params
                .get("arguments")
                .is_some_and(|arguments| !arguments.is_object())
            {
                return Ok(request_error(
                    id,
                    -32602,
                    "Invalid tools/call request: params.arguments must be an object".into(),
                ));
            }
            let args = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let result = match server.call_tool(name, args) {
                Ok(result) => result,
                Err(ToolError::Actionable(message)) => {
                    json!({"content":[{"type":"text","text":format!("{message}. Please fix the issue and try again.")}]})
                }
                Err(ToolError::Failure(message)) => {
                    json!({"content":[{"type":"text","text":format!("Error: {message}")}],"isError":true})
                }
            };
            Some(result)
        }
        _ => {
            if is_notification {
                None
            } else {
                return Ok(Some(error_response(id, -32601, "Method not found".into())));
            }
        }
    };

    Ok(if is_notification {
        None
    } else {
        response.map(|result| json!({"jsonrpc":"2.0","id":id,"result":result}))
    })
}

fn initialize_result(object: &serde_json::Map<String, Value>) -> Result<Value, String> {
    let params = object
        .get("params")
        .and_then(Value::as_object)
        .ok_or_else(|| "Invalid initialize request: params must be an object".to_owned())?;
    let requested_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            "Invalid initialize request: params.protocolVersion must be a string".to_owned()
        })?;
    if !params.get("capabilities").is_some_and(Value::is_object) {
        return Err("Invalid initialize request: params.capabilities must be an object".into());
    }
    let client_info = params
        .get("clientInfo")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            "Invalid initialize request: params.clientInfo must be an object".to_owned()
        })?;
    if !client_info.get("name").is_some_and(Value::is_string)
        || !client_info.get("version").is_some_and(Value::is_string)
    {
        return Err(
            "Invalid initialize request: params.clientInfo.name and version must be strings".into(),
        );
    }

    let protocol_version = if SUPPORTED_PROTOCOL_VERSIONS.contains(&requested_version) {
        requested_version
    } else {
        LATEST_PROTOCOL_VERSION
    };
    let mut result = json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": crate::SERVER_NAME, "version": crate::SERVER_VERSION },
    });
    if crate::coord::normalized_enabled() {
        result["instructions"] = Value::String(format!(
            "All x/y coordinate inputs use 0-{} normalized coordinates (top-left origin), not pixels. Call mobile_get_screen_size to understand the device dimensions.",
            crate::coord::coordinate_scale()
        ));
    }
    Ok(result)
}

fn error_response(id: Option<Value>, code: i64, message: String) -> Value {
    json!({"jsonrpc":"2.0","id":id.unwrap_or(Value::Null),"error":{"code":code,"message":message}})
}

fn request_error(id: Option<Value>, code: i64, message: String) -> Option<Value> {
    id.map(|id| error_response(Some(id), code, message))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{ProtocolError, handle_json_line};
    use crate::tools::MobileServer;

    fn dispatch_request(request: Value) -> Value {
        handle_json_line(&mut MobileServer::default(), request.to_string().as_bytes())
            .expect("valid JSON should be dispatched")
            .expect("requests with ids produce responses")
    }

    fn error_code(response: &Value) -> i64 {
        response["error"]["code"].as_i64().expect("error code")
    }

    fn valid_initialize(version: &str) -> Value {
        json!({
            "jsonrpc":"2.0",
            "id":7,
            "method":"initialize",
            "params": {
                "protocolVersion":version,
                "capabilities":{},
                "clientInfo":{"name":"test-client","version":"1.0"}
            }
        })
    }

    #[test]
    fn initialization_negotiates_supported_versions_and_falls_back_to_latest() {
        assert_eq!(
            dispatch_request(valid_initialize("2025-03-26"))["result"]["protocolVersion"],
            "2025-03-26"
        );
        assert_eq!(
            dispatch_request(valid_initialize("future-version"))["result"]["protocolVersion"],
            "2025-11-25"
        );
    }

    #[test]
    fn invalid_json_rpc_shape_returns_invalid_request_with_null_id() {
        let response = dispatch_request(json!({"jsonrpc":"1.0","method":"ping","id":8}));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);

        let response = dispatch_request(json!([{"jsonrpc":"2.0","method":"ping","id":8}]));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);

        let response = dispatch_request(json!({
            "jsonrpc":"2.0", "method":"ping", "id":8, "extra":true
        }));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);
    }

    #[test]
    fn invalid_request_ids_are_not_mistaken_for_notifications() {
        let response = dispatch_request(json!({"jsonrpc":"2.0","method":"ping","id":null}));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);

        let response = dispatch_request(json!({"jsonrpc":"2.0","method":"ping","id":true}));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);

        let response = dispatch_request(json!({"jsonrpc":"2.0","method":"ping","id":1.5}));
        assert_eq!(error_code(&response), -32600);
        assert_eq!(response["id"], Value::Null);
    }

    #[test]
    fn malformed_method_params_return_invalid_params_and_keep_request_id() {
        let response = dispatch_request(json!({
            "jsonrpc":"2.0", "id":"call-1", "method":"tools/call",
            "params":{"name":"mobile_get_screen_size", "arguments":[]}
        }));
        assert_eq!(error_code(&response), -32602);
        assert_eq!(response["id"], "call-1");

        let response = dispatch_request(json!({"jsonrpc":"2.0", "id":9, "method":"initialize"}));
        assert_eq!(error_code(&response), -32602);
        assert_eq!(response["id"], 9);
    }

    #[test]
    fn unknown_methods_use_method_not_found_and_notifications_are_silent() {
        let response = dispatch_request(json!({"jsonrpc":"2.0","id":3,"method":"unknown/method"}));
        assert_eq!(error_code(&response), -32601);
        assert_eq!(response["error"]["message"], "Method not found");

        let response = handle_json_line(
            &mut MobileServer::default(),
            br#"{"jsonrpc":"2.0","method":"unknown/notification"}"#,
        )
        .expect("unknown notifications are ignored");
        assert!(response.is_none());

        let response = handle_json_line(
            &mut MobileServer::default(),
            br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":false}}"#,
        )
        .expect("request methods without ids are notifications");
        assert!(response.is_none());

        let response = dispatch_request(json!({
            "jsonrpc":"2.0", "id":4, "method":"notifications/initialized"
        }));
        assert_eq!(error_code(&response), -32601);
    }

    #[test]
    fn invalid_json_remains_a_parse_error() {
        assert!(matches!(
            handle_json_line(&mut MobileServer::default(), b"{not json"),
            Err(ProtocolError::InvalidJson(_))
        ));
    }
}
