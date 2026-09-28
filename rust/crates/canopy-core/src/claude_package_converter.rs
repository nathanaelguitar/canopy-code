//! Conversion of local Claude Code plugin packages into Canopy extension packages.
//!
//! This ports the filesystem conversion path in
//! `packages/core/src/extension/claude-converter.ts`. Marketplace entries with
//! local relative sources and standalone plugins are supported. Remote source
//! acquisition remains a host responsibility; callers can acquire a repository
//! and use [`build_canopy_extension_from_plugin`] on its confined plugin root.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value};

use crate::claude_converter::{
    ClaudePluginConfigError, convert_claude_agent_config, convert_claude_to_canopy_config,
};
use crate::extensions::substitute_hook_variables;
use crate::gemini_converter::is_path_within;
use crate::utils::terminal_safe::strip_terminal_control_sequences;
use crate::utils::yaml::{self, StringifyOptions};

const MARKETPLACE_FILE: &str = ".claude-plugin/marketplace.json";
const PLUGIN_FILE: &str = ".claude-plugin/plugin.json";

/// A completed Claude package conversion. The host owns cleanup of
/// `converted_dir` after it has installed or otherwise consumed the package.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeExtensionPackage {
    pub config: Value,
    pub converted_dir: PathBuf,
    pub external_content: bool,
    /// Non-fatal source-converter warnings (for example a missing resource or
    /// malformed optional hooks/MCP file) that the host may send to its logger.
    pub warnings: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ClaudePackageConversionError {
    #[error("marketplace configuration not found at {path}")]
    MarketplaceNotFound { path: PathBuf },
    #[error("marketplace configuration at {path} resolves through a symlink outside the plugin")]
    MarketplaceOutsidePlugin { path: PathBuf },
    #[error("failed to read Claude marketplace at {path}: {source}")]
    ReadMarketplace {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse Claude marketplace at {path}: {source}")]
    ParseMarketplace {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("plugin {plugin_name} not found in marketplace.json")]
    PluginNotFound { plugin_name: String },
    #[error("plugin source \"{source_text}\" escapes the marketplace directory")]
    PluginSourceEscapesMarketplace { source_text: String },
    #[error("plugin source not found at {path}")]
    PluginSourceNotFound { path: PathBuf },
    #[error(
        "plugin source \"{source_text}\" resolves through a symlink outside the marketplace directory"
    )]
    PluginSourceOutsideMarketplace { source_text: String },
    #[error("remote Claude plugin source requires host acquisition: {source_text}")]
    RemoteSourceRequiresHost { source_text: String },
    #[error("unsupported Claude plugin source type: {source_text}")]
    UnsupportedPluginSource { source_text: String },
    #[error("plugin configuration not found at {path}")]
    PluginConfigNotFound { path: PathBuf },
    #[error("plugin configuration at {path} resolves through a symlink outside the plugin")]
    PluginConfigOutsidePlugin { path: PathBuf },
    #[error("strict mode requires plugin.json at {path}")]
    StrictPluginConfigMissing { path: PathBuf },
    #[error("strict mode requires a trusted plugin.json at {path}")]
    StrictPluginConfigUntrusted { path: PathBuf },
    #[error("invalid plugin configuration at {path}: expected a JSON object")]
    InvalidPluginConfig { path: PathBuf },
    #[error("failed to read Claude plugin config at {path}: {source}")]
    ReadPluginConfig {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse Claude plugin config at {path}: {source}")]
    ParsePluginConfig {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error(transparent)]
    Config(#[from] ClaudePluginConfigError),
    #[error("failed to create temporary Claude extension directory at {path}: {source}")]
    CreateTemporaryDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to resolve Claude plugin directory at {path}: {source}")]
    ResolvePluginDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to copy Claude plugin package from {source_path} to {destination_path}: {source}"
    )]
    CopyPackage {
        source_path: PathBuf,
        destination_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to enumerate Claude agent files at {path}: {source}")]
    EnumerateAgents {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed to write converted Claude extension config at {path}: {source}")]
    WriteConvertedConfig {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Convert a standalone Claude plugin at `extension_dir`.
///
/// A trusted `.claude-plugin/plugin.json` is required. If the plugin manifest
/// omits `mcpServers`, a root `.mcp.json` with an object-valued `mcpServers`
/// property is loaded when it is a confined file. Malformed optional MCP data
/// is ignored and reported in `warnings`, as in the source converter.
pub fn convert_claude_plugin_standalone(
    extension_dir: impl AsRef<Path>,
) -> Result<ClaudeExtensionPackage, ClaudePackageConversionError> {
    let extension_dir = extension_dir.as_ref();
    let plugin_json_path = extension_dir.join(PLUGIN_FILE);
    if !plugin_json_path.exists() {
        return Err(ClaudePackageConversionError::PluginConfigNotFound {
            path: plugin_json_path,
        });
    }
    if !real_path_within(&plugin_json_path, extension_dir) {
        return Err(ClaudePackageConversionError::PluginConfigOutsidePlugin {
            path: plugin_json_path,
        });
    }

    let mut warnings = Vec::new();
    let mut config = read_plugin_config(&plugin_json_path)?;
    if !js_truthy(config.get("mcpServers")) {
        let mcp_path = extension_dir.join(".mcp.json");
        if mcp_path.exists() && real_path_within(&mcp_path, extension_dir) {
            match read_json_file(&mcp_path) {
                Ok(parsed) => {
                    if parsed
                        .get("mcpServers")
                        .is_some_and(|servers| servers.is_object())
                    {
                        config["mcpServers"] = parsed["mcpServers"].clone();
                    } else {
                        warnings.push(format!(
                            ".mcp.json at {} has no valid \"mcpServers\" object; skipping.",
                            mcp_path.display()
                        ));
                    }
                }
                Err(error) => warnings.push(format!(
                    "Failed to parse .mcp.json at {}: {error}",
                    mcp_path.display()
                )),
            }
        } else if mcp_path.exists() {
            warnings.push(format!(
                "Ignoring .mcp.json at {}; it resolves through a symlink outside the plugin.",
                mcp_path.display()
            ));
        }
    }

    build_canopy_extension_from_plugin(extension_dir, config, warnings)
}

/// Convert one plugin selected from a local Claude marketplace.
///
/// Local relative source strings (including `.`) are fully converted.
/// GitHub, URL, and git-subdirectory sources return
/// [`ClaudePackageConversionError::RemoteSourceRequiresHost`], allowing the
/// installer to acquire/pin/network-check the source using its existing
/// policy before it calls [`build_canopy_extension_from_plugin`].
pub fn convert_claude_plugin_package(
    marketplace_dir: impl AsRef<Path>,
    plugin_name: &str,
) -> Result<ClaudeExtensionPackage, ClaudePackageConversionError> {
    let marketplace_dir = marketplace_dir.as_ref();
    let marketplace_path = marketplace_dir.join(MARKETPLACE_FILE);
    if !marketplace_path.exists() {
        return Err(ClaudePackageConversionError::MarketplaceNotFound {
            path: marketplace_path,
        });
    }
    if !real_path_within(&marketplace_path, marketplace_dir) {
        return Err(ClaudePackageConversionError::MarketplaceOutsidePlugin {
            path: marketplace_path,
        });
    }
    let marketplace = read_json_file(&marketplace_path).map_err(|error| match error {
        JsonFileError::Read(source) => ClaudePackageConversionError::ReadMarketplace {
            path: marketplace_path.clone(),
            source,
        },
        JsonFileError::Parse(source) => ClaudePackageConversionError::ParseMarketplace {
            path: marketplace_path.clone(),
            source,
        },
    })?;
    let plugin = marketplace
        .get("plugins")
        .and_then(Value::as_array)
        .and_then(|plugins| {
            plugins
                .iter()
                .find(|plugin| plugin.get("name").and_then(Value::as_str) == Some(plugin_name))
        })
        .cloned()
        .ok_or_else(|| ClaudePackageConversionError::PluginNotFound {
            plugin_name: plugin_name.to_owned(),
        })?;

    let source = plugin.get("source").unwrap_or(&Value::Null);
    let plugin_source = if let Some(source) = source.as_str() {
        if source.to_ascii_lowercase().starts_with("http://")
            || source.to_ascii_lowercase().starts_with("https://")
        {
            return Err(ClaudePackageConversionError::RemoteSourceRequiresHost {
                source_text: safe_text(source),
            });
        }
        resolve_marketplace_local_source(marketplace_dir, source)?
    } else if let Some(source_object) = source.as_object() {
        let source_text = serde_json::to_string(source_object)
            .map(|source| safe_text(&source))
            .unwrap_or_else(|_| "<invalid source object>".to_owned());
        match source_object.get("source").and_then(Value::as_str) {
            Some("github" | "url" | "git-subdir") => {
                return Err(ClaudePackageConversionError::RemoteSourceRequiresHost { source_text });
            }
            _ => {
                return Err(ClaudePackageConversionError::UnsupportedPluginSource { source_text });
            }
        }
    } else {
        return Err(ClaudePackageConversionError::RemoteSourceRequiresHost {
            source_text: safe_text(&source.to_string()),
        });
    };

    let strict = js_truthy(plugin.get("strict"));
    let plugin_json_path = plugin_source.join(PLUGIN_FILE);
    let mut warnings = Vec::new();
    let merged_config = if strict && !plugin_json_path.exists() {
        return Err(ClaudePackageConversionError::StrictPluginConfigMissing {
            path: plugin_json_path,
        });
    } else if plugin_json_path.exists() && real_path_within(&plugin_json_path, &plugin_source) {
        let actual = read_marketplace_plugin_config(&plugin_json_path)?;
        merge_claude_configs(&plugin, Some(&actual))
    } else {
        if strict {
            return Err(ClaudePackageConversionError::StrictPluginConfigUntrusted {
                path: plugin_json_path,
            });
        }
        if plugin_json_path.exists() {
            warnings.push(format!(
                "Ignoring plugin.json at {}; it resolves through a symlink outside the plugin.",
                plugin_json_path.display()
            ));
        }
        merge_claude_configs(&plugin, None)
    };
    build_canopy_extension_from_plugin(&plugin_source, merged_config, warnings)
}

/// Build a converted package from an already-acquired plugin directory and its
/// JSON config. This is the reusable boundary for hosts that perform remote
/// GitHub-release or clone acquisition themselves.
pub fn build_canopy_extension_from_plugin(
    plugin_source: impl AsRef<Path>,
    merged_config: Value,
    warnings: Vec<String>,
) -> Result<ClaudeExtensionPackage, ClaudePackageConversionError> {
    build_canopy_extension_from_plugin_with_external_content(
        plugin_source,
        merged_config,
        warnings,
        false,
    )
}

/// Build a package after a host has acquired its source. Set
/// `external_content` when the marketplace entry fetched plugin contents from
/// a separate URL/repository instead of the marketplace checkout.
pub fn build_canopy_extension_from_plugin_with_external_content(
    plugin_source: impl AsRef<Path>,
    mut merged_config: Value,
    mut warnings: Vec<String>,
    external_content: bool,
) -> Result<ClaudeExtensionPackage, ClaudePackageConversionError> {
    let plugin_source = plugin_source.as_ref();
    let plugin_root = fs::canonicalize(plugin_source).map_err(|source| {
        ClaudePackageConversionError::ResolvePluginDirectory {
            path: plugin_source.to_path_buf(),
            source,
        }
    })?;

    load_optional_config_file(
        &plugin_root,
        &mut merged_config,
        "mcpServers",
        &mut warnings,
    );
    let converted_dir = create_temporary_directory()?;
    let conversion = (|| {
        copy_directory_confined(&plugin_root, &converted_dir, &plugin_root).map_err(|source| {
            ClaudePackageConversionError::CopyPackage {
                source_path: plugin_root.clone(),
                destination_path: converted_dir.clone(),
                source,
            }
        })?;
        let git_dir = converted_dir.join(".git");
        if git_dir.exists() || fs::symlink_metadata(&git_dir).is_ok() {
            remove_path(&git_dir).map_err(|source| ClaudePackageConversionError::CopyPackage {
                source_path: git_dir.clone(),
                destination_path: converted_dir.clone(),
                source,
            })?;
        }

        for name in ["commands", "skills", "agents"] {
            let config_value = merged_config.get(name).cloned().unwrap_or(Value::Null);
            let copied_folder = converted_dir.join(name);
            let source_folder = plugin_root.join(name);
            if js_truthy(Some(&config_value)) {
                if copied_folder.exists() {
                    remove_path(&copied_folder).map_err(|source| {
                        ClaudePackageConversionError::CopyPackage {
                            source_path: copied_folder.clone(),
                            destination_path: converted_dir.clone(),
                            source,
                        }
                    })?;
                }
                collect_resources(&config_value, &plugin_root, &copied_folder, &mut warnings)
                    .map_err(|source| ClaudePackageConversionError::CopyPackage {
                        source_path: plugin_root.clone(),
                        destination_path: copied_folder,
                        source,
                    })?;
            } else if !source_folder.exists() && copied_folder.exists() {
                remove_path(&copied_folder).map_err(|source| {
                    ClaudePackageConversionError::CopyPackage {
                        source_path: copied_folder.clone(),
                        destination_path: converted_dir.clone(),
                        source,
                    }
                })?;
            }
        }

        load_optional_config_file(&plugin_root, &mut merged_config, "hooks", &mut warnings);

        if merged_config
            .get("mcpServers")
            .is_some_and(|value| value.is_string() && js_truthy(Some(value)))
        {
            warnings.push(format!(
                "[Claude Converter] MCP servers path not yet supported: {}",
                safe_text(merged_config["mcpServers"].as_str().unwrap_or_default())
            ));
        }
        if js_truthy(merged_config.get("outputStyles")) {
            warnings.push(format!(
                "[Claude Converter] Output styles are not yet supported in {}",
                merged_config
                    .get("name")
                    .map(js_string)
                    .map(|name| safe_text(&name))
                    .unwrap_or_default()
            ));
        }

        let agents_dir = converted_dir.join("agents");
        convert_agent_files(&agents_dir, &mut warnings).map_err(|source| {
            ClaudePackageConversionError::EnumerateAgents {
                path: agents_dir,
                source,
            }
        })?;
        let config = convert_claude_to_canopy_config(&merged_config)?;
        let output_path = converted_dir.join("canopy-extension.json");
        let output = serde_json::to_vec_pretty(&config).map_err(|error| {
            ClaudePackageConversionError::WriteConvertedConfig {
                path: output_path.clone(),
                source: io::Error::new(io::ErrorKind::InvalidData, error),
            }
        })?;
        fs::write(&output_path, output).map_err(|source| {
            ClaudePackageConversionError::WriteConvertedConfig {
                path: output_path,
                source,
            }
        })?;
        Ok(ClaudeExtensionPackage {
            config,
            converted_dir: converted_dir.clone(),
            external_content,
            warnings,
        })
    })();
    if conversion.is_err() {
        let _ = fs::remove_dir_all(&converted_dir);
    }
    conversion
}

/// Marketplace fields override plugin.json fields when truthy, matching the
/// source `mergeClaudeConfigs`. Missing plugin config uses name/version defaults.
pub fn merge_claude_configs(marketplace_plugin: &Value, plugin_config: Option<&Value>) -> Value {
    let mut merged = match plugin_config.filter(|value| js_truthy(Some(value))) {
        Some(Value::Object(object)) => object.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Some(Value::String(value)) => value
            .encode_utf16()
            .enumerate()
            .map(|(index, unit)| {
                (
                    index.to_string(),
                    Value::String(
                        char::from_u32(u32::from(unit))
                            .unwrap_or('\u{fffd}')
                            .to_string(),
                    ),
                )
            })
            .collect(),
        Some(Value::Null | Value::Bool(_) | Value::Number(_)) | None => Map::new(),
    };
    if !plugin_config.is_some_and(|value| js_truthy(Some(value))) {
        merged = {
            let mut defaults = Map::new();
            if let Some(name) = marketplace_plugin.get("name") {
                defaults.insert("name".to_owned(), name.clone());
            }
            defaults.insert("version".to_owned(), Value::String("1.0.0".to_owned()));
            defaults
        };
    }

    for field in [
        "name",
        "version",
        "description",
        "author",
        "homepage",
        "repository",
        "license",
        "keywords",
        "commands",
        "agents",
        "skills",
        "hooks",
        "mcpServers",
        "outputStyles",
        "lspServers",
    ] {
        if let Some(value) = marketplace_plugin
            .get(field)
            .filter(|value| js_truthy(Some(value)))
        {
            merged.insert(field.to_owned(), value.clone());
        }
    }
    Value::Object(merged)
}

/// Heuristic detector for a Claude marketplace containing the selected plugin.
pub fn is_claude_plugin_config(
    extension_dir: impl AsRef<Path>,
    plugin_name: &str,
) -> Result<bool, ClaudePackageConversionError> {
    let path = extension_dir.as_ref().join(MARKETPLACE_FILE);
    if !path.exists() {
        return Ok(false);
    }
    let marketplace = read_json_file(&path).map_err(|error| match error {
        JsonFileError::Read(source) => ClaudePackageConversionError::ReadMarketplace {
            path: path.clone(),
            source,
        },
        JsonFileError::Parse(source) => ClaudePackageConversionError::ParseMarketplace {
            path: path.clone(),
            source,
        },
    })?;
    if !marketplace.is_object()
        || !marketplace.get("name").is_some_and(Value::is_string)
        || !marketplace
            .get("owner")
            .is_some_and(|owner| owner.is_object() || owner.is_null())
    {
        return Ok(false);
    }
    Ok(marketplace
        .get("plugins")
        .and_then(Value::as_array)
        .is_some_and(|plugins| {
            plugins
                .iter()
                .any(|plugin| plugin.get("name").and_then(Value::as_str) == Some(plugin_name))
        }))
}

/// Resolve a plugin-relative config path. Absolute paths and lexical traversal
/// outside the plugin are rejected; existing symlink targets are then checked
/// against the canonical plugin root before they are read.
pub fn resolve_plugin_relative_file(
    plugin_root: impl AsRef<Path>,
    relative: &str,
) -> Option<PathBuf> {
    let plugin_root = plugin_root.as_ref();
    let relative_path = Path::new(relative);
    if relative_path.is_absolute() {
        return None;
    }
    let root = absolute_lexical(plugin_root).ok()?;
    let resolved = normalize_lexical(&root.join(relative_path));
    if !is_path_within(&resolved, &root) {
        return None;
    }
    if resolved.exists() && !real_path_within(&resolved, plugin_root) {
        return None;
    }
    Some(resolved)
}

fn resolve_marketplace_local_source(
    marketplace_dir: &Path,
    source: &str,
) -> Result<PathBuf, ClaudePackageConversionError> {
    let root = absolute_lexical(marketplace_dir).map_err(|source_error| {
        ClaudePackageConversionError::ResolvePluginDirectory {
            path: marketplace_dir.to_path_buf(),
            source: source_error,
        }
    })?;
    let resolved = normalize_lexical(&root.join(source));
    if !is_path_within(&resolved, &root) {
        return Err(
            ClaudePackageConversionError::PluginSourceEscapesMarketplace {
                source_text: safe_text(source),
            },
        );
    }
    if !resolved.exists() {
        return Err(ClaudePackageConversionError::PluginSourceNotFound { path: resolved });
    }
    if !real_path_within(&resolved, marketplace_dir) {
        return Err(
            ClaudePackageConversionError::PluginSourceOutsideMarketplace {
                source_text: safe_text(source),
            },
        );
    }
    Ok(resolved)
}

fn load_optional_config_file(
    plugin_root: &Path,
    config: &mut Value,
    field: &str,
    warnings: &mut Vec<String>,
) {
    let Some(relative) = config.get(field).and_then(Value::as_str) else {
        return;
    };
    if !js_truthy(config.get(field)) {
        return;
    }
    let Some(path) = resolve_plugin_relative_file(plugin_root, relative) else {
        warnings.push(format!("Ignoring unsafe {field} path in plugin config."));
        return;
    };
    if !path.exists() {
        return;
    }
    match read_json_file(&path) {
        Ok(mut parsed) => {
            if field == "hooks" {
                if parsed
                    .get("hooks")
                    .is_some_and(|hooks| hooks.is_object() || hooks.is_array())
                {
                    parsed = parsed["hooks"].clone();
                }
                if let Some(hooks) =
                    substitute_hook_variables(Some(&parsed), &path_to_string(plugin_root))
                {
                    config[field] = hooks;
                }
            } else {
                config[field] = parsed;
            }
        }
        Err(error) => warnings.push(format!(
            "Failed to parse {field} file {}: {error}",
            path.display()
        )),
    }
}

fn collect_resources(
    resource_config: &Value,
    plugin_root: &Path,
    destination: &Path,
    warnings: &mut Vec<String>,
) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    let resources: Vec<&str> = match resource_config {
        Value::String(path) => vec![path],
        Value::Array(paths) => paths.iter().filter_map(Value::as_str).collect(),
        // The TS schema accepts only string/string[], but the source's runtime
        // array coercion would otherwise turn malformed values into paths.
        _ => Vec::new(),
    };
    let destination_name = destination.file_name().unwrap_or_default();

    for resource in resources {
        let Some(resolved) = resolve_plugin_relative_file(plugin_root, resource) else {
            warnings.push(format!(
                "Ignoring unsafe resource path in plugin config: {}",
                safe_text(resource)
            ));
            continue;
        };
        if !resolved.exists() {
            warnings.push(format!("Resource path not found: {}", resolved.display()));
            continue;
        }
        let metadata = match fs::metadata(&resolved) {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if metadata.is_dir() {
            let dir_name = resolved.file_name().unwrap_or_default();
            let final_destination = if dir_name == destination_name {
                destination.to_path_buf()
            } else {
                destination.join(dir_name)
            };
            let canonical_resource = match fs::canonicalize(&resolved) {
                Ok(path) => path,
                Err(_) => continue,
            };
            collect_resource_files(
                &resolved,
                &canonical_resource,
                &final_destination,
                &mut HashSet::new(),
            )?;
        } else {
            let file_name = resolved.file_name().unwrap_or_default();
            fs::copy(&resolved, destination.join(file_name))?;
        }
    }
    Ok(())
}

fn collect_resource_files(
    directory: &Path,
    canonical_root: &Path,
    destination: &Path,
    active: &mut HashSet<PathBuf>,
) -> io::Result<()> {
    let canonical_dir = match fs::canonicalize(directory) {
        Ok(path) if is_path_within(&path, canonical_root) => path,
        _ => return Ok(()),
    };
    if !active.insert(canonical_dir.clone()) {
        return Ok(());
    }
    let result = (|| {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            let path = entry.path();
            let output = destination.join(&name);
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                collect_resource_files(&path, canonical_root, &output, active)?;
            } else if file_type.is_symlink() {
                let Ok(real) = fs::canonicalize(&path) else {
                    continue;
                };
                if !is_path_within(&real, canonical_root) {
                    continue;
                }
                let Ok(metadata) = fs::metadata(&real) else {
                    continue;
                };
                if metadata.is_file() {
                    if let Some(parent) = output.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::copy(real, output)?;
                }
                // glob's default behavior does not recurse into symlink dirs.
            } else if file_type.is_file() {
                if let Some(parent) = output.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::copy(path, output)?;
            }
        }
        Ok(())
    })();
    active.remove(&canonical_dir);
    result
}

fn convert_agent_files(agents_dir: &Path, warnings: &mut Vec<String>) -> io::Result<()> {
    let entries = match fs::read_dir(agents_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "md") {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                warnings.push(format!(
                    "Failed to convert agent file {}: {error}",
                    path.display()
                ));
                continue;
            }
        };
        let content = normalize_content(&String::from_utf8_lossy(&bytes));
        let Some((frontmatter, body)) = split_frontmatter(&content) else {
            continue;
        };
        let frontmatter = yaml::parse(frontmatter);
        let mut agent = Map::new();
        agent.insert(
            "name".to_owned(),
            Value::String(js_string_or_empty(frontmatter.get("name"))),
        );
        agent.insert(
            "description".to_owned(),
            Value::String(js_string_or_empty(frontmatter.get("description"))),
        );
        if let Some(value) = frontmatter.get("tools").and_then(parse_string_or_array) {
            agent.insert(
                "tools".to_owned(),
                Value::Array(value.into_iter().map(Value::String).collect()),
            );
        }
        if let Some(value) = frontmatter
            .get("disallowedTools")
            .and_then(parse_string_or_array)
        {
            agent.insert(
                "disallowedTools".to_owned(),
                Value::Array(value.into_iter().map(Value::String).collect()),
            );
        }
        if let Some(value) = frontmatter.get("skills").and_then(parse_string_or_array) {
            agent.insert(
                "skills".to_owned(),
                Value::Array(value.into_iter().map(Value::String).collect()),
            );
        }
        for key in ["model", "permissionMode", "hooks", "mcpServers", "color"] {
            if let Some(value) = frontmatter.get(key) {
                agent.insert(key.to_owned(), value.clone());
            }
        }
        agent.insert(
            "systemPrompt".to_owned(),
            Value::String(body.trim().to_owned()),
        );
        let converted = match convert_claude_agent_config(&Value::Object(agent)) {
            Ok(Value::Object(converted)) => converted,
            Ok(_) => continue,
            Err(error) => {
                warnings.push(format!(
                    "Failed to convert agent file {}: {error}",
                    path.display()
                ));
                continue;
            }
        };
        let system_prompt = converted
            .get("systemPrompt")
            .and_then(Value::as_str)
            .unwrap_or(body.trim())
            .to_owned();
        let mut new_frontmatter = Map::new();
        for (key, value) in converted {
            if key != "systemPrompt" && !value.is_null() {
                new_frontmatter.insert(key, value);
            }
        }
        let yaml = yaml::stringify(
            &new_frontmatter,
            Some(StringifyOptions {
                line_width: Some(0),
                min_content_width: None,
            }),
        );
        let output = format!("---\n{}\n---\n\n{}\n", yaml.trim(), system_prompt);
        if let Err(error) = fs::write(&path, output) {
            warnings.push(format!(
                "Failed to convert agent file {}: {error}",
                path.display()
            ));
        }
    }
    Ok(())
}

fn split_frontmatter(content: &str) -> Option<(&str, &str)> {
    let rest = content.strip_prefix("---\n")?;
    let split = rest.find("\n---\n")?;
    Some((&rest[..split], &rest[split + 5..]))
}

fn normalize_content(content: &str) -> String {
    let without_bom = content.strip_prefix('\u{feff}').unwrap_or(content);
    without_bom.replace("\r\n", "\n").replace('\r', "\n")
}

fn parse_string_or_array(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Null => None,
        Value::Array(values) => Some(values.iter().map(js_string).collect()),
        Value::String(value) => Some(
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect(),
        ),
        _ => None,
    }
}

fn js_string_or_empty(value: Option<&Value>) -> String {
    value
        .filter(|value| js_truthy(Some(value)))
        .map(js_string)
        .unwrap_or_default()
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| {
                if value.is_null() {
                    String::new()
                } else {
                    js_string(value)
                }
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn read_plugin_config(path: &Path) -> Result<Value, ClaudePackageConversionError> {
    let value = read_plugin_config_json(path)?;
    if !value.is_object() {
        return Err(ClaudePackageConversionError::InvalidPluginConfig {
            path: path.to_path_buf(),
        });
    }
    Ok(value)
}

fn read_marketplace_plugin_config(path: &Path) -> Result<Value, ClaudePackageConversionError> {
    read_plugin_config_json(path)
}

fn read_plugin_config_json(path: &Path) -> Result<Value, ClaudePackageConversionError> {
    read_json_file(path).map_err(|error| match error {
        JsonFileError::Read(source) => ClaudePackageConversionError::ReadPluginConfig {
            path: path.to_path_buf(),
            source,
        },
        JsonFileError::Parse(source) => ClaudePackageConversionError::ParsePluginConfig {
            path: path.to_path_buf(),
            source,
        },
    })
}

#[derive(Debug)]
enum JsonFileError {
    Read(io::Error),
    Parse(serde_json::Error),
}

impl std::fmt::Display for JsonFileError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => error.fmt(formatter),
            Self::Parse(error) => error.fmt(formatter),
        }
    }
}

fn read_json_file(path: &Path) -> Result<Value, JsonFileError> {
    let bytes = fs::read(path).map_err(JsonFileError::Read)?;
    serde_json::from_str(&String::from_utf8_lossy(&bytes)).map_err(JsonFileError::Parse)
}

fn create_temporary_directory() -> Result<PathBuf, ClaudePackageConversionError> {
    let base = std::env::temp_dir();
    for _ in 0..8 {
        let path = base.join(format!("canopy-extension{}", uuid::Uuid::new_v4().simple()));
        match fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    if let Err(source) =
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    {
                        let _ = fs::remove_dir_all(&path);
                        return Err(ClaudePackageConversionError::CreateTemporaryDirectory {
                            path,
                            source,
                        });
                    }
                }
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(ClaudePackageConversionError::CreateTemporaryDirectory {
                    path,
                    source,
                });
            }
        }
    }
    Err(ClaudePackageConversionError::CreateTemporaryDirectory {
        path: base.join("canopy-extension<unique-suffix>"),
        source: io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a unique temporary directory",
        ),
    })
}

fn copy_directory_confined(source: &Path, destination: &Path, root: &Path) -> io::Result<()> {
    copy_directory_confined_inner(source, destination, root, &mut HashSet::new())
}

fn copy_directory_confined_inner(
    source: &Path,
    destination: &Path,
    root: &Path,
    active_directories: &mut HashSet<PathBuf>,
) -> io::Result<()> {
    fs::create_dir_all(destination)?;
    let real_source = match fs::canonicalize(source) {
        Ok(path) if is_path_within(&path, root) => path,
        _ => return Ok(()),
    };
    if !active_directories.insert(real_source.clone()) {
        return Ok(());
    }
    let result = (|| {
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                copy_directory_confined_inner(
                    &source_path,
                    &destination_path,
                    root,
                    active_directories,
                )?;
            } else if file_type.is_symlink() {
                let Ok(real_path) = fs::canonicalize(&source_path) else {
                    continue;
                };
                if !is_path_within(&real_path, root) {
                    continue;
                }
                let Ok(metadata) = fs::metadata(&real_path) else {
                    continue;
                };
                if metadata.is_dir() {
                    copy_directory_confined_inner(
                        &real_path,
                        &destination_path,
                        root,
                        active_directories,
                    )?;
                } else if metadata.is_file() {
                    fs::copy(real_path, destination_path)?;
                }
            } else if file_type.is_file() {
                let Ok(real_path) = fs::canonicalize(&source_path) else {
                    continue;
                };
                if is_path_within(&real_path, root) {
                    fs::copy(real_path, destination_path)?;
                }
            }
        }
        Ok(())
    })();
    active_directories.remove(&real_source);
    result
}

fn remove_path(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
            fs::remove_file(path)
        }
        Ok(_) => fs::remove_dir_all(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn real_path_within(target: &Path, root: &Path) -> bool {
    let (Ok(real_target), Ok(real_root)) = (fs::canonicalize(target), fs::canonicalize(root))
    else {
        return false;
    };
    is_path_within(&real_target, &real_root)
}

fn absolute_lexical(path: &Path) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(normalize_lexical(path))
    } else {
        Ok(normalize_lexical(&std::env::current_dir()?.join(path)))
    }
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
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

fn safe_text(value: &str) -> String {
    strip_terminal_control_sequences(value)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
