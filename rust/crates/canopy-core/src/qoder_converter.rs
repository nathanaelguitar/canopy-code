//! Input and configuration resolution for Qoder plugin packages.
//!
//! This ports the pre-conversion portion of
//! `packages/core/src/extension/qoder-converter.ts`. It intentionally does
//! not copy or convert plugin files.

use std::fs;
use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value};

pub const QODER_PLUGIN_MANIFEST: &str = ".qoder-plugin/plugin.json";

/// Resolved Qoder input. `config` retains unknown manifest keys, and contains
/// normalized defaults and MCP server configuration. Context paths are
/// plugin-relative paths ready for the later package-copy conversion step.
#[derive(Clone, Debug, PartialEq)]
pub struct QoderPluginInput {
    pub config: Value,
    pub context_file_names: Option<Vec<String>>,
}

#[derive(Debug, thiserror::Error)]
pub enum QoderConverterError {
    #[error("Qoder plugin configuration not found at {path}")]
    ManifestNotFound { path: String },
    #[error("Qoder plugin configuration at {path} resolves through a symlink outside the plugin")]
    ManifestOutsidePlugin { path: String },
    #[error("Invalid Qoder plugin configuration at {path}: {message}")]
    InvalidManifest { path: String, message: String },
    #[error("Qoder plugin config must have name field")]
    MissingName,
    #[error("Qoder plugin mcpServers must be an object or file path")]
    InvalidMcpServers,
    #[error("Invalid Qoder MCP configuration at {path}: {message}")]
    InvalidMcpConfiguration { path: String, message: String },
}

/// Read and normalize `.qoder-plugin/plugin.json`, resolve MCP server input,
/// and select confined context files.
pub fn load_qoder_plugin_input(
    extension_dir: impl AsRef<Path>,
) -> Result<QoderPluginInput, QoderConverterError> {
    let extension_dir = extension_dir.as_ref();
    let manifest_path = extension_dir.join(QODER_PLUGIN_MANIFEST);
    let display_manifest_path = safe_path_display(&manifest_path);
    let Some(plugin_root) = canonical_plugin_root(extension_dir) else {
        return Err(QoderConverterError::ManifestNotFound {
            path: display_manifest_path,
        });
    };

    if !manifest_path.exists() {
        return Err(QoderConverterError::ManifestNotFound {
            path: display_manifest_path,
        });
    }
    let Some(safe_manifest_path) = confined_existing_path(&plugin_root, &manifest_path) else {
        return Err(QoderConverterError::ManifestOutsidePlugin {
            path: display_manifest_path,
        });
    };

    let contents = fs::read_to_string(&safe_manifest_path)
        .map_err(|error| invalid_manifest(&display_manifest_path, error.to_string()))?;
    let mut config: Value = serde_json::from_str(&contents)
        .map_err(|error| invalid_manifest(&display_manifest_path, error.to_string()))?;
    let Some(config_object) = config.as_object_mut() else {
        return Err(invalid_manifest(
            &display_manifest_path,
            "expected a JSON object".to_owned(),
        ));
    };
    if !config_object
        .get("name")
        .and_then(Value::as_str)
        .is_some_and(|name| !name.is_empty())
    {
        return Err(QoderConverterError::MissingName);
    }

    let version = config_object
        .get("version")
        .and_then(Value::as_str)
        .filter(|version| !version.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| "1.0.0".to_owned());
    config_object.insert("version".to_owned(), Value::String(version));
    for field in ["displayName", "description"] {
        if !config_object.get(field).is_some_and(Value::is_string) {
            config_object.remove(field);
        }
    }

    let configured_mcp = config_object.get("mcpServers").cloned();
    let mcp_servers = resolve_mcp_servers(
        &plugin_root,
        configured_mcp.as_ref(),
        &safe_path_display(&safe_manifest_path),
    )?;
    match mcp_servers {
        Some(servers) => {
            config_object.insert("mcpServers".to_owned(), Value::Object(servers));
        }
        None => {
            config_object.remove("mcpServers");
        }
    }

    let context_file_names =
        resolve_context_files(&plugin_root, config_object.get("contextFileName"));

    Ok(QoderPluginInput {
        config,
        context_file_names,
    })
}

fn invalid_manifest(path: &str, message: String) -> QoderConverterError {
    QoderConverterError::InvalidManifest {
        path: path.to_owned(),
        message: crate::utils::terminal_safe::strip_terminal_control_sequences(&message),
    }
}

fn invalid_mcp(path: &str, message: impl Into<String>) -> QoderConverterError {
    QoderConverterError::InvalidMcpConfiguration {
        path: safe_string(path),
        message: crate::utils::terminal_safe::strip_terminal_control_sequences(&message.into()),
    }
}

fn safe_path_display(path: &Path) -> String {
    safe_string(&path.to_string_lossy())
}

fn safe_string(value: &str) -> String {
    crate::utils::terminal_safe::strip_terminal_control_sequences(value)
}

fn canonical_plugin_root(extension_dir: &Path) -> Option<PathBuf> {
    fs::canonicalize(extension_dir).ok()
}

fn is_within(path: &Path, root: &Path) -> bool {
    path.starts_with(root)
}

fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Some(normalized)
}

/// Resolve a plugin-relative path using lexical and real-path containment.
/// Returns the lexical path so callers can retain the same relative spelling
/// used by the source converter.
fn resolve_plugin_relative_file(plugin_root: &Path, relative_path: &str) -> Option<PathBuf> {
    let relative_path = Path::new(relative_path);
    if relative_path.is_absolute() {
        return None;
    }
    let resolved = normalize_absolute_path(&plugin_root.join(relative_path))?;
    if !is_within(&resolved, plugin_root) {
        return None;
    }
    if resolved.exists() && confined_existing_path(plugin_root, &resolved).is_none() {
        return None;
    }
    Some(resolved)
}

fn confined_existing_path(plugin_root: &Path, path: &Path) -> Option<PathBuf> {
    let real_path = fs::canonicalize(path).ok()?;
    is_within(&real_path, plugin_root).then_some(real_path)
}

fn resolve_mcp_servers(
    plugin_root: &Path,
    configured: Option<&Value>,
    manifest_path: &str,
) -> Result<Option<Map<String, Value>>, QoderConverterError> {
    match configured {
        Some(Value::String(relative_path)) => {
            load_mcp_servers_file(plugin_root, relative_path, false)
        }
        Some(Value::Null) | None => load_mcp_servers_file(plugin_root, ".mcp.json", true),
        Some(Value::Object(servers)) => normalize_mcp_servers(servers, manifest_path).map(Some),
        Some(_) => Err(QoderConverterError::InvalidMcpServers),
    }
}

fn load_mcp_servers_file(
    plugin_root: &Path,
    relative_path: &str,
    require_wrapper: bool,
) -> Result<Option<Map<String, Value>>, QoderConverterError> {
    let Some(mcp_path) = resolve_plugin_relative_file(plugin_root, relative_path) else {
        return Ok(None);
    };
    if !mcp_path.exists() {
        return Ok(None);
    }
    let Some(safe_mcp_path) = confined_existing_path(plugin_root, &mcp_path) else {
        return Ok(None);
    };
    let display_path = safe_path_display(&safe_mcp_path);
    let contents = fs::read_to_string(&safe_mcp_path)
        .map_err(|error| invalid_mcp(&display_path, error.to_string()))?;
    let parsed: Value = serde_json::from_str(&contents)
        .map_err(|error| invalid_mcp(&display_path, error.to_string()))?;
    let Some(object) = parsed.as_object() else {
        return Err(invalid_mcp(&display_path, "expected a JSON object"));
    };
    let servers = match object.get("mcpServers") {
        Some(servers) => servers,
        None if require_wrapper => {
            return Err(invalid_mcp(
                &display_path,
                "expected an \"mcpServers\" object",
            ));
        }
        None => &parsed,
    };
    let Some(servers) = servers.as_object() else {
        return Err(invalid_mcp(
            &display_path,
            "expected an \"mcpServers\" object",
        ));
    };
    normalize_mcp_servers(servers, &display_path).map(Some)
}

fn normalize_mcp_servers(
    servers: &Map<String, Value>,
    config_path: &str,
) -> Result<Map<String, Value>, QoderConverterError> {
    let mut normalized = Map::new();
    for (name, server) in servers {
        let Some(server) = server.as_object() else {
            return Err(invalid_mcp(
                config_path,
                "server entries must be JSON objects",
            ));
        };
        normalized.insert(name.clone(), Value::Object(normalize_mcp_server(server)));
    }
    Ok(normalized)
}

fn normalize_mcp_server(raw: &Map<String, Value>) -> Map<String, Value> {
    let mut server = raw.clone();
    let direct_config = ["command", "httpUrl", "tcp"]
        .iter()
        .any(|key| server.get(*key).is_some_and(is_js_truthy));
    if direct_config {
        if server.get("type") != Some(&Value::String("sdk".to_owned())) {
            server.remove("type");
        }
        return server;
    }

    let Some(url) = server.get("url").and_then(Value::as_str).map(str::to_owned) else {
        return server;
    };
    let server_type = server
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_owned);
    server.remove("url");
    if server_type.as_deref() != Some("sdk") {
        server.remove("type");
    }
    let target = if server_type.as_deref() == Some("http") {
        "httpUrl"
    } else {
        "url"
    };
    server.insert(target.to_owned(), Value::String(url));
    server
}

fn is_js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn resolve_context_files(plugin_root: &Path, configured: Option<&Value>) -> Option<Vec<String>> {
    let configured_files: Vec<&str> = match configured {
        Some(Value::String(file)) => vec![file],
        Some(Value::Array(files)) => files.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    let mut context_files = Vec::new();
    for file in configured_files {
        add_context_file(plugin_root, file, false, &mut context_files);
    }
    add_context_file(plugin_root, "system-prompt.md", false, &mut context_files);
    if !context_files.is_empty() {
        add_context_file(plugin_root, "CANOPY.md", true, &mut context_files);
    }
    (!context_files.is_empty()).then_some(context_files)
}

fn add_context_file(
    plugin_root: &Path,
    relative_path: &str,
    prepend: bool,
    context_files: &mut Vec<String>,
) {
    let Some(resolved) = resolve_plugin_relative_file(plugin_root, relative_path) else {
        return;
    };
    let Some(real_path) = confined_existing_path(plugin_root, &resolved) else {
        return;
    };
    if !fs::metadata(real_path).is_ok_and(|metadata| metadata.is_file()) {
        return;
    }
    // Keep the configured in-plugin spelling for the stored relative path;
    // `real_path` above is only for symlink confinement and file-type checks.
    let Ok(relative) = resolved.strip_prefix(plugin_root) else {
        return;
    };
    let relative = relative.to_string_lossy().into_owned();
    if relative.is_empty() || context_files.iter().any(|existing| existing == &relative) {
        return;
    }
    if prepend {
        context_files.insert(0, relative);
    } else {
        context_files.push(relative);
    }
}
