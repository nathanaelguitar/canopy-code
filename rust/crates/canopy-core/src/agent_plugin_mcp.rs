// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Agent Plugins v1 MCP configuration loading and runtime path validation.

use std::fs;
use std::io;
use std::path::Path;

use indexmap::IndexMap;
use reqwest::Url;
use serde_json::{Map, Value, json};

use crate::agent_plugins::{
    is_path_within, resolve_contained_existing_path, resolve_contained_potential_path,
    resolve_existing_path_prefix,
};

pub const AGENT_PLUGIN_MCP_SCHEMA: &str = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json";

const CLIENT_OWNED_HEADERS: &[&str] = &[
    "accept",
    "authorization",
    "connection",
    "content-encoding",
    "content-length",
    "content-type",
    "host",
    "last-event-id",
    "mcp-protocol-version",
    "mcp-session-id",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "user-agent",
];

/// Load and validate `mcp.json`, skipping invalid server entries individually.
/// Errors that invalidate the whole file and per-server skips are reported to
/// `warn`; missing `mcp.json` is silent, matching the source loader.
pub fn load_agent_plugin_mcp_servers(
    plugin_root: &str,
    plugin_data_root: &str,
    create_data_dir: bool,
    mut warn: impl FnMut(&str),
) -> IndexMap<String, Value> {
    let mcp_path = Path::new(plugin_root).join("mcp.json");
    let resolved_mcp_path =
        match resolve_contained_existing_path(plugin_root, &mcp_path.to_string_lossy()) {
            Ok(path) => path,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return IndexMap::new(),
            Err(error) => {
                warn(&format!("Disabling Agent Plugins MCP: {}", error));
                return IndexMap::new();
            }
        };
    match fs::metadata(&resolved_mcp_path) {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            warn("Agent Plugins mcp.json is not a regular file; disabling MCP.");
            return IndexMap::new();
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return IndexMap::new(),
        Err(error) => {
            warn(&format!("Disabling Agent Plugins MCP: {}", error));
            return IndexMap::new();
        }
    }
    let contents = match fs::read(&resolved_mcp_path) {
        Ok(contents) => String::from_utf8_lossy(&contents).into_owned(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return IndexMap::new(),
        Err(error) => {
            warn(&format!("Disabling Agent Plugins MCP: {}", error));
            return IndexMap::new();
        }
    };
    let value = match serde_json::from_str::<Value>(&contents) {
        Ok(value) => value,
        Err(error) => {
            warn(&format!("Disabling Agent Plugins MCP: {error}"));
            return IndexMap::new();
        }
    };
    let mut servers = match normalize_agent_plugin_mcp_servers(
        &value,
        plugin_root,
        plugin_data_root,
        &mut warn,
    ) {
        Ok(servers) => servers,
        Err(error) => {
            warn(&format!("Disabling Agent Plugins MCP: {error}"));
            return IndexMap::new();
        }
    };

    if create_data_dir && servers.values().any(has_command) {
        let creation = create_server_data_directories(plugin_data_root, &servers);
        if let Err(error) = creation {
            warn(&format!(
                "Failed to create Agent Plugins data directory; disabling stdio MCP servers: {error}"
            ));
            servers.retain(|_, server| !has_command(server));
        }
    }
    servers
}

/// Validate and normalize an already-read Agent Plugins `mcp.json` value.
/// This variant has no filesystem side effects; callers are responsible for
/// bounding the source read and may choose not to create plugin data dirs.
pub fn normalize_agent_plugin_mcp_servers(
    value: &Value,
    plugin_root: &str,
    plugin_data_root: &str,
    mut warn: impl FnMut(&str),
) -> Result<IndexMap<String, Value>, String> {
    validate_mcp_file(value)?;
    let entries = value
        .as_object()
        .and_then(|object| object.get("mcpServers"))
        .and_then(Value::as_object)
        .expect("validate_mcp_file checked mcpServers");

    let mut servers = IndexMap::new();
    for (name, entry) in entries {
        match normalize_server(entry, plugin_root, plugin_data_root) {
            Ok(server) => {
                servers.insert(name.clone(), server);
            }
            Err(error) => warn(&format!(
                "Skipping Agent Plugins MCP server \"{name}\": {error}"
            )),
        }
    }
    Ok(servers)
}

/// Revalidate Agent Plugins stdio executable and cwd paths immediately before
/// launch, after symlinks may have changed since configuration was loaded.
pub fn validate_agent_plugin_stdio_runtime_paths(server: &Value) -> Result<(), String> {
    if !js_truthy(server.get("agentPluginV1")) || !js_truthy(server.get("command")) {
        return Ok(());
    }
    let environment = server.get("env").and_then(Value::as_object);
    let plugin_root = environment
        .and_then(|environment| environment.get("PLUGIN_ROOT"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let plugin_data_root = environment
        .and_then(|environment| environment.get("PLUGIN_DATA"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    let (Some(plugin_root), Some(plugin_data_root)) = (plugin_root, plugin_data_root) else {
        return Err("Agent Plugins stdio server is missing runtime roots.".to_owned());
    };

    if let Some(command) = server.get("command").and_then(Value::as_str) {
        if Path::new(command).is_absolute() {
            let resolved = resolve_contained_existing_path(plugin_root, command)
                .map_err(|error| error.to_string())?;
            if !fs::metadata(&resolved)
                .map_err(|error| error.to_string())?
                .is_file()
            {
                return Err("Agent Plugins stdio command must be a regular file.".to_owned());
            }
        }
    }

    if let Some(cwd) = server
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|cwd| !cwd.is_empty())
    {
        let resolved = resolve_contained_existing_path(plugin_root, cwd)
            .or_else(|_| resolve_contained_existing_path(plugin_data_root, cwd))
            .map_err(|error| error.to_string())?;
        if !fs::metadata(&resolved)
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            return Err("Agent Plugins stdio cwd must be a directory.".to_owned());
        }
    }
    Ok(())
}

fn validate_mcp_file(value: &Value) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err("Agent Plugins mcp.json must contain an object.".to_owned());
    };
    for field in object.keys() {
        if field != "$schema" && field != "mcpServers" {
            return Err(format!("Unknown Agent Plugins MCP field \"{field}\"."));
        }
    }
    if object.get("$schema").and_then(Value::as_str) != Some(AGENT_PLUGIN_MCP_SCHEMA) {
        let schema = object
            .get("$schema")
            .map(js_string)
            .unwrap_or_else(|| "undefined".to_owned());
        return Err(format!(
            "Unsupported Agent Plugins MCP schema \"{schema}\"."
        ));
    }
    if !object.get("mcpServers").is_some_and(is_record) {
        return Err("Agent Plugins mcpServers must be an object.".to_owned());
    }
    Ok(())
}

fn normalize_server(
    value: &Value,
    plugin_root: &str,
    plugin_data_root: &str,
) -> Result<Value, String> {
    let Some(object) = value.as_object() else {
        return Err("MCP server must be an object with a string type.".to_owned());
    };
    let Some(transport) = object.get("type").and_then(Value::as_str) else {
        return Err("MCP server must be an object with a string type.".to_owned());
    };
    match transport {
        "stdio" => normalize_stdio_server(object, plugin_root, plugin_data_root),
        "streamable-http" => normalize_http_server(object),
        "sse" => Err("Legacy SSE transport is not supported.".to_owned()),
        other => Err(format!("Unsupported MCP transport \"{other}\".")),
    }
}

fn normalize_stdio_server(
    value: &Map<String, Value>,
    plugin_root: &str,
    plugin_data_root: &str,
) -> Result<Value, String> {
    assert_only_fields(value, &["type", "command", "args", "env", "cwd"])?;
    let command_value = value.get("command");
    let Some(command_value) = command_value
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Err("Stdio command must be a non-empty string.".to_owned());
    };
    let args = value
        .get("args")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!([]));
    let Some(args) = args.as_array() else {
        return Err("Stdio args must be an array of strings.".to_owned());
    };
    if args.iter().any(|argument| !argument.is_string()) {
        return Err("Stdio args must be an array of strings.".to_owned());
    }

    let configured_env = value
        .get("env")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!({}));
    let Some(configured_env) = configured_env.as_object() else {
        return Err("Stdio env values must be strings.".to_owned());
    };
    for entry in configured_env.values() {
        if !entry.is_string() {
            return Err("Stdio env values must be strings.".to_owned());
        }
    }
    for key in configured_env.keys() {
        if reserved_environment_name(key) {
            return Err(format!(
                "Stdio env cannot override reserved variable \"{key}\"."
            ));
        }
    }
    let cwd = value
        .get("cwd")
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::String("${PLUGIN_ROOT}".to_owned()));
    let Some(cwd) = cwd.as_str() else {
        return Err("Stdio cwd must be a string.".to_owned());
    };

    let resolved_plugin_root = fs::canonicalize(plugin_root).map_err(|error| error.to_string())?;
    let resolved_data_root =
        resolve_existing_path_prefix(plugin_data_root).map_err(|error| error.to_string())?;
    let root_string = path_string(&resolved_plugin_root);
    let data_string = path_string(&resolved_data_root);
    let command = normalize_command(command_value, &root_string)?;
    let expanded_args = args
        .iter()
        .map(|argument| {
            Value::String(expand_plugin_variables(
                argument.as_str().expect("args validated as strings"),
                &root_string,
                &data_string,
            ))
        })
        .collect();
    let mut environment = Map::new();
    for (key, entry) in configured_env {
        environment.insert(
            key.clone(),
            Value::String(expand_plugin_variables(
                entry.as_str().expect("env values validated as strings"),
                &root_string,
                &data_string,
            )),
        );
    }
    environment.insert("PLUGIN_ROOT".to_owned(), Value::String(root_string.clone()));
    environment.insert("PLUGIN_DATA".to_owned(), Value::String(data_string.clone()));
    let normalized_cwd = normalize_cwd(cwd, &root_string, &data_string)?;

    let mut normalized = Map::new();
    normalized.insert("command".to_owned(), Value::String(command));
    normalized.insert("args".to_owned(), Value::Array(expanded_args));
    normalized.insert("env".to_owned(), Value::Object(environment));
    normalized.insert("cwd".to_owned(), Value::String(normalized_cwd));
    normalized.insert("agentPluginV1".to_owned(), Value::Bool(true));
    Ok(Value::Object(normalized))
}

fn normalize_command(command: &str, plugin_root: &str) -> Result<String, String> {
    let bare =
        !command.contains('/') && !command.contains('\\') && !has_windows_drive_prefix(command);
    if bare {
        return Ok(command.to_owned());
    }
    if !command.starts_with("./") || command.contains('\\') {
        return Err(
            "Stdio command must be a bare executable name or a contained ./ path.".to_owned(),
        );
    }
    let candidate = Path::new(plugin_root).join(&command[2..]);
    resolve_contained_potential_path(plugin_root, &path_string(&candidate))
        .map(|path| path_string(&path))
        .map_err(|error| error.to_string())
}

fn normalize_cwd(cwd: &str, root_string: &str, data_string: &str) -> Result<String, String> {
    if cwd.contains('\\') {
        return Err("Stdio cwd must use portable forward-slash paths.".to_owned());
    }
    let allowed_root_string = if cwd == "./" || cwd.starts_with("./") {
        root_string
    } else if cwd == "${PLUGIN_ROOT}" || cwd.starts_with("${PLUGIN_ROOT}/") {
        root_string
    } else if cwd == "${PLUGIN_DATA}" || cwd.starts_with("${PLUGIN_DATA}/") {
        data_string
    } else {
        return Err(
            "Stdio cwd must be a contained ./, ${PLUGIN_ROOT}, or ${PLUGIN_DATA} path.".to_owned(),
        );
    };

    let expanded = expand_plugin_variables(cwd, root_string, data_string);
    let resolved_allowed_root =
        resolve_existing_path_prefix(allowed_root_string).map_err(|error| error.to_string())?;
    let expanded_path = Path::new(&expanded);
    let candidate = if expanded_path.is_absolute() {
        expanded_path.to_path_buf()
    } else {
        resolved_allowed_root.join(expanded_path)
    };
    let resolved = resolve_existing_path_prefix(&path_string(&candidate))
        .map_err(|error| error.to_string())?;
    if !is_path_within(
        &path_string(&resolved_allowed_root),
        &path_string(&resolved),
    ) {
        return Err("Expanded stdio cwd escapes its allowed root.".to_owned());
    }
    Ok(path_string(&resolved))
}

fn normalize_http_server(value: &Map<String, Value>) -> Result<Value, String> {
    assert_only_fields(value, &["type", "url", "headers"])?;
    let Some(raw_url) = value
        .get("url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Err("Streamable HTTP url must be a non-empty string.".to_owned());
    };
    validate_http_url(raw_url)?;

    let headers = match value.get("headers") {
        None => None,
        Some(raw_headers) if is_record(raw_headers) => {
            let raw_headers = raw_headers.as_object().expect("record is an object");
            let headers = normalize_headers(raw_headers)?;
            (!headers.is_empty()).then_some(headers)
        }
        Some(_) => return Err("Streamable HTTP headers must be an object.".to_owned()),
    };
    let mut normalized = Map::new();
    normalized.insert("httpUrl".to_owned(), Value::String(raw_url.to_owned()));
    if let Some(headers) = headers {
        normalized.insert("headers".to_owned(), Value::Object(headers));
    }
    normalized.insert("agentPluginV1".to_owned(), Value::Bool(true));
    Ok(Value::Object(normalized))
}

fn validate_http_url(raw_url: &str) -> Result<(), String> {
    let parsed =
        Url::parse(raw_url).map_err(|error| format!("Invalid Streamable HTTP url: {error}"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err("Streamable HTTP url must be absolute HTTP or HTTPS.".to_owned());
    }
    if !parsed.username().is_empty()
        || parsed
            .password()
            .is_some_and(|password| !password.is_empty())
        || parsed
            .fragment()
            .is_some_and(|fragment| !fragment.is_empty())
    {
        return Err(
            "Streamable HTTP url must not contain user information or a fragment.".to_owned(),
        );
    }
    if parsed.scheme() == "http" && !is_loopback_host(parsed.host_str().expect("host was checked"))
    {
        return Err("Non-loopback Streamable HTTP endpoints must use HTTPS.".to_owned());
    }
    Ok(())
}

fn is_loopback_host(hostname: &str) -> bool {
    let normalized = hostname
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(hostname)
        .to_ascii_lowercase();
    normalized == "localhost"
        || normalized == "::1"
        || normalized
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|address| address.octets()[0] == 127)
}

fn normalize_headers(value: &Map<String, Value>) -> Result<Map<String, Value>, String> {
    let mut normalized = Map::new();
    let mut seen = Vec::new();
    for (name, raw_value) in value {
        let Some(header_value) = raw_value.as_str() else {
            return Err(format!("HTTP header \"{name}\" must have a string value."));
        };
        let lowercase_name = name.to_lowercase();
        if seen.iter().any(|seen_name| seen_name == &lowercase_name) {
            return Err(format!(
                "Duplicate case-insensitive HTTP header \"{name}\"."
            ));
        }
        seen.push(lowercase_name.clone());
        if !is_valid_header_name(name) {
            return Err(format!("Invalid HTTP header name \"{name}\"."));
        }
        if header_value.chars().any(is_invalid_header_value_character) {
            return Err(format!("Invalid HTTP header value for \"{name}\"."));
        }
        if !CLIENT_OWNED_HEADERS.contains(&lowercase_name.as_str()) {
            normalized.insert(name.clone(), Value::String(header_value.to_owned()));
        }
    }
    Ok(normalized)
}

fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn is_invalid_header_value_character(character: char) -> bool {
    let code = character as u32;
    code <= 8 || (10..=31).contains(&code) || code == 127
}

fn assert_only_fields(value: &Map<String, Value>, fields: &[&str]) -> Result<(), String> {
    if let Some(unknown) = value.keys().find(|field| !fields.contains(&field.as_str())) {
        return Err(format!("Unknown MCP server field \"{unknown}\"."));
    }
    Ok(())
}

fn reserved_environment_name(name: &str) -> bool {
    #[cfg(windows)]
    {
        name.eq_ignore_ascii_case("PLUGIN_ROOT") || name.eq_ignore_ascii_case("PLUGIN_DATA")
    }
    #[cfg(not(windows))]
    {
        name == "PLUGIN_ROOT" || name == "PLUGIN_DATA"
    }
}

fn expand_plugin_variables(value: &str, plugin_root: &str, plugin_data: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut copied_through = 0;
    let mut search_from = 0;
    while search_from < value.len() {
        let root_match = value[search_from..].find("${PLUGIN_ROOT}");
        let data_match = value[search_from..].find("${PLUGIN_DATA}");
        let next = match (root_match, data_match) {
            (Some(root), Some(data)) if root <= data => Some((root, true)),
            (Some(_), Some(data)) => Some((data, false)),
            (Some(root), None) => Some((root, true)),
            (None, Some(data)) => Some((data, false)),
            (None, None) => None,
        };
        let Some((relative_start, is_root)) = next else {
            break;
        };
        let start = search_from + relative_start;
        let token = if is_root {
            "${PLUGIN_ROOT}"
        } else {
            "${PLUGIN_DATA}"
        };
        let end = start + token.len();
        output.push_str(&value[copied_through..start]);
        output.push_str(if is_root { plugin_root } else { plugin_data });
        copied_through = end;
        search_from = end;
    }
    output.push_str(&value[copied_through..]);
    output
}

fn create_server_data_directories(
    plugin_data_root: &str,
    servers: &IndexMap<String, Value>,
) -> io::Result<()> {
    fs::create_dir_all(plugin_data_root)?;
    let resolved_data_root = fs::canonicalize(plugin_data_root)?;
    let resolved_data_root = path_string(&resolved_data_root);
    for server in servers.values() {
        if !has_command(server) {
            continue;
        }
        let Some(cwd) = server.get("cwd").and_then(Value::as_str) else {
            continue;
        };
        if is_path_within(&resolved_data_root, cwd) {
            fs::create_dir_all(cwd)?;
        }
    }
    Ok(())
}

fn has_command(server: &Value) -> bool {
    server
        .get("command")
        .and_then(Value::as_str)
        .is_some_and(|command| !command.is_empty())
}

fn is_record(value: &Value) -> bool {
    value.as_object().is_some()
}

fn js_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(_) | Value::Object(_)) => true,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value
            .as_i64()
            .map(|number| number.to_string())
            .or_else(|| value.as_u64().map(|number| number.to_string()))
            .or_else(|| value.as_f64().map(|number| number.to_string()))
            .unwrap_or_else(|| value.to_string()),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                value => js_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn has_windows_drive_prefix(path: &str) -> bool {
    let bytes = path.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
