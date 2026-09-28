//! Read-only discovery of installed local extensions and MCP sources.
//!
//! Marketplace state is deliberately not consulted: a descriptor is produced
//! only from an on-disk extension directory with a supported local manifest.
//! All reads are bounded, and this module never repairs, quarantines, installs,
//! or fetches extension content.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::agent_plugins::{
    AGENT_PLUGIN_SCHEMA, AGENT_PLUGIN_SCHEMA_PREFIX, AgentPluginSchemaStatus,
    load_agent_plugin_manifest_value,
};
use crate::extension_activation::{
    ExtensionActivation, ExtensionIdentity, ExtensionPolicy, ExtensionStoreSnapshot,
    WorkspaceActivation, get_activation,
};
use crate::extension_preferences::ExtensionScope;
use crate::extension_setting_helpers::{ExtensionSetting, validate_extension_setting_env_vars};
use crate::extension_variables::{VariableContext, recursively_hydrate_strings};
use crate::services::at_resource_references::{
    LocalExtensionReference, LocalExtensionReferencePolicy,
    project_active_local_extension_reference,
};
use crate::skills::{SkillConfig, SkillExtension};

const INSTALL_METADATA_FILENAME: &str = ".canopy-extension-install.json";
const ENABLEMENT_FILENAME: &str = "extension-enablement.json";
const PREFERENCES_FILENAME: &str = "extension-preferences.json";
const ACTIVATION_STATE_FILENAME: &str = "state.json";

const MAX_INSTALLED_EXTENSIONS: usize = 128;
const MAX_ROOT_ENTRIES: usize = 256;
const MAX_COMPONENT_ENTRIES: usize = 128;
const MAX_COMPONENT_LABELS: usize = 64;
const MAX_COMMAND_ENTRIES: usize = 4096;
const MAX_COMMAND_LABELS: usize = 128;
const MAX_EXTENSION_MCP_SERVERS: usize = 64;
const MAX_EXTENSION_MCP_SERVER_NAME_BYTES: usize = 256;
const MAX_EXTENSION_MCP_SERVER_CONFIG_BYTES: usize = 64 * 1024;
const MAX_EXTENSION_MCP_CONFIG_BYTES: usize = 256 * 1024;
const MAX_ACTIVE_EXTENSION_MCP_SERVERS: usize = 128;
const MAX_ACTIVE_EXTENSION_MCP_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_CONTEXT_FILES: usize = 64;
const MAX_SNAPSHOT_POLICIES: usize = 512;
const MAX_PREFERENCE_SCOPES: usize = 512;
const MAX_LEGACY_POLICIES: usize = 512;
const MAX_POLICY_PATH_RULES: usize = 512;
const MAX_SKILL_MANIFEST_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
const MAX_INSTALL_METADATA_BYTES: usize = 64 * 1024;
const MAX_AUXILIARY_JSON_BYTES: usize = 1024 * 1024;
const MAX_PREFERENCES_BYTES: usize = 256 * 1024;
const MAX_TOTAL_READ_BYTES: usize = 8 * 1024 * 1024;
const MAX_EXTENSION_NAME_BYTES: usize = 128;
const MAX_CONTEXT_PATH_BYTES: usize = 4096;
const MAX_DIAGNOSTICS: usize = 64;

#[derive(Clone, Copy, Debug)]
pub struct ExtensionInventoryOptions<'a> {
    pub workspace_root: &'a Path,
    pub user_extensions_dir: &'a Path,
    pub extension_store_dir: &'a Path,
    pub enabled_extension_overrides: &'a [String],
    pub workspace_trusted: bool,
    pub safe_mode: bool,
    pub bare_mode: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ExtensionInventory {
    pub active_extensions: Vec<LocalExtensionReference>,
    /// Parsed skill configs from extensions that pass the active-extension
    /// policy gates. Hosts can forward this directly to `SkillManagerConfig`.
    pub active_skill_extensions: Vec<SkillExtension>,
    pub active_mcp_servers: Vec<ExtensionMcpServerSource>,
    pub diagnostics: Vec<String>,
}

/// Inputs for a read-only CLI listing of every installed local extension.
/// Unlike [`ExtensionInventoryOptions`], this includes disabled extensions and
/// does not apply safe/bare mode or explicit-name runtime gates.
#[derive(Clone, Copy, Debug)]
pub struct InstalledExtensionListOptions<'a> {
    pub workspace_root: &'a Path,
    pub user_home: &'a Path,
    pub user_extensions_dir: &'a Path,
    pub extension_store_dir: &'a Path,
}

/// All supported local extensions discovered by the CLI list command.
#[derive(Clone, Debug, Default)]
pub struct InstalledExtensionList {
    pub extensions: Vec<InstalledLocalExtension>,
    pub diagnostics: Vec<String>,
}

/// Display-ready metadata for one installed local extension.
#[derive(Clone, Debug)]
pub struct InstalledLocalExtension {
    /// Stable install identity derived from the install source metadata.
    pub id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub version: String,
    /// On-disk install slot. Linked installs may have a different effective
    /// manifest path in `path`.
    pub install_slot: PathBuf,
    pub path: PathBuf,
    pub source: Option<String>,
    pub source_type: Option<String>,
    pub origin_source: Option<String>,
    pub source_ref: Option<String>,
    pub release_tag: Option<String>,
    pub user_activation: Option<crate::extension_activation::ExtensionActivationResult>,
    pub workspace_activation: Option<crate::extension_activation::ExtensionActivationResult>,
    pub context_files: Vec<PathBuf>,
    pub commands: Vec<String>,
    pub skills: Vec<String>,
    pub agents: Vec<String>,
    pub mcp_servers: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ExtensionMcpServerSource {
    pub extension_id: String,
    pub extension_name: String,
    pub install_slot: PathBuf,
    pub settings: Vec<ExtensionSetting>,
    pub servers: serde_json::Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstallMetadata {
    source: String,
    #[serde(rename = "type")]
    install_type: String,
    #[serde(default)]
    plugin_name: Option<String>,
    #[serde(default)]
    origin_source: Option<String>,
    #[serde(default, rename = "ref")]
    source_ref: Option<String>,
    #[serde(default)]
    release_tag: Option<String>,
    #[serde(default)]
    marketplace_config: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyEnablement {
    #[serde(default)]
    overrides: Vec<String>,
}

#[derive(Default)]
struct LegacyProjection {
    overrides: HashMap<String, Vec<String>>,
    serialized_hash: Option<String>,
}

#[derive(Clone, Debug)]
struct Candidate {
    identity: ExtensionIdentity,
    scope: Option<ExtensionScope>,
    install_slot: PathBuf,
    reference: LocalExtensionReference,
    version: String,
    install_metadata: Option<InstallMetadata>,
    commands: Vec<String>,
    skill_configs: Vec<SkillConfig>,
    settings: Vec<ExtensionSetting>,
    mcp_servers: serde_json::Map<String, Value>,
}

#[derive(Default)]
struct ReadBudget {
    bytes: usize,
}

impl ReadBudget {
    fn consume(&mut self, amount: usize) -> Result<(), String> {
        if amount > self.remaining() {
            return Err(format!(
                "extension inventory exceeded its {}-byte total read limit",
                MAX_TOTAL_READ_BYTES
            ));
        }
        self.bytes += amount;
        Ok(())
    }

    fn remaining(&self) -> usize {
        MAX_TOTAL_READ_BYTES.saturating_sub(self.bytes)
    }
}

/// Read the installed user-extension inventory and return only extensions and
/// MCP sources allowed by activation, trust, safe/bare mode, and CLI overrides.
pub fn load_active_local_extension_references(
    options: ExtensionInventoryOptions<'_>,
) -> ExtensionInventory {
    let mut inventory = ExtensionInventory::default();
    if options.safe_mode {
        return inventory;
    }
    if options.bare_mode && options.enabled_extension_overrides.is_empty() {
        return inventory;
    }
    if options.enabled_extension_overrides.len() == 1
        && options.enabled_extension_overrides[0].eq_ignore_ascii_case("none")
    {
        return inventory;
    }

    let extensions_root = match fs::canonicalize(options.user_extensions_dir) {
        Ok(root) if root.is_dir() => root,
        Ok(_) => return inventory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return inventory,
        Err(error) => {
            push_diagnostic(
                &mut inventory,
                format!("Could not resolve user extensions directory: {error}"),
            );
            return inventory;
        }
    };

    let mut budget = ReadBudget::default();
    let preferences = match read_preferences(
        &extensions_root,
        &mut budget,
        &mut inventory.diagnostics,
    ) {
        Some(preferences) => preferences,
        None if !options.workspace_trusted => {
            push_diagnostic(
                &mut inventory,
                "Extension preferences could not be read in an untrusted workspace; local extensions were withheld.".to_owned(),
            );
            return inventory;
        }
        None => HashMap::new(),
    };
    let legacy_projection =
        match read_legacy_projection(&extensions_root, &mut budget, &mut inventory.diagnostics) {
            Ok(projection) => projection,
            Err(error) => {
                push_diagnostic(
                    &mut inventory,
                    format!("Could not load extension enablement projection: {error}"),
                );
                return inventory;
            }
        };

    let mut candidates = Vec::new();
    let entries = match fs::read_dir(&extensions_root) {
        Ok(entries) => entries,
        Err(error) => {
            push_diagnostic(
                &mut inventory,
                format!("Could not enumerate installed extensions: {error}"),
            );
            return inventory;
        }
    };
    for (index, entry) in entries.enumerate() {
        if index >= MAX_ROOT_ENTRIES {
            push_diagnostic(
                &mut inventory,
                format!(
                    "Extension directory contains more than {MAX_ROOT_ENTRIES} entries; remaining entries were skipped."
                ),
            );
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                push_diagnostic(
                    &mut inventory,
                    format!("Could not read an extension directory entry: {error}"),
                );
                continue;
            }
        };
        let entry_path = entry.path();
        match fs::symlink_metadata(&entry_path) {
            Ok(metadata) if metadata.is_dir() => {}
            // Direct symlink installs are intentionally not inferred. Canopy's
            // supported link install is a normal metadata slot naming a source.
            Ok(metadata) if metadata.file_type().is_symlink() => continue,
            Ok(_) => continue,
            Err(_) => continue,
        };
        let slot_root = match fs::canonicalize(&entry_path) {
            Ok(path) if path.starts_with(&extensions_root) && path.is_dir() => path,
            _ => continue,
        };

        if candidates.len() >= MAX_INSTALLED_EXTENSIONS {
            push_diagnostic(
                &mut inventory,
                format!(
                    "More than {MAX_INSTALLED_EXTENSIONS} installed extensions were found; remaining entries were skipped."
                ),
            );
            break;
        }
        match load_candidate(
            &slot_root,
            options.workspace_root,
            options.extension_store_dir,
            &preferences,
            &mut budget,
            &mut inventory.diagnostics,
        ) {
            Ok(Some(candidate)) => candidates.push(candidate),
            Ok(None) => {}
            Err(error) => push_diagnostic(
                &mut inventory,
                format!("Skipped extension at {}: {error}", entry_path.display()),
            ),
        }
    }

    let snapshot = match load_activation_snapshot(
        options.extension_store_dir,
        &extensions_root,
        &candidates,
        &legacy_projection,
        &mut budget,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            push_diagnostic(
                &mut inventory,
                format!("Could not load extension activation state: {error}"),
            );
            return inventory;
        }
    };

    let mut active_extension_mcp_server_count = 0usize;
    let mut active_extension_mcp_config_bytes = 0usize;
    for candidate in candidates {
        let activation =
            match get_activation(&snapshot, &candidate.identity, options.workspace_root) {
                Ok(activation) => activation,
                Err(error) => {
                    push_diagnostic(
                        &mut inventory,
                        format!(
                            "Could not resolve activation for {}: {error}",
                            candidate.identity.name
                        ),
                    );
                    continue;
                }
            };
        if let Some(mut reference) = project_active_local_extension_reference(
            &candidate.reference,
            LocalExtensionReferencePolicy {
                activation,
                scope: candidate.scope,
                enabled_extension_overrides: options.enabled_extension_overrides,
                workspace_trusted: options.workspace_trusted,
                safe_mode: options.safe_mode,
                bare_mode: options.bare_mode,
            },
        ) {
            let mut servers = serde_json::Map::new();
            for (name, config) in candidate.mcp_servers {
                if active_extension_mcp_server_count >= MAX_ACTIVE_EXTENSION_MCP_SERVERS {
                    push_diagnostic(
                        &mut inventory,
                        format!(
                            "Active extension MCP servers exceed the {MAX_ACTIVE_EXTENSION_MCP_SERVERS}-server limit; remaining entries were skipped."
                        ),
                    );
                    break;
                }
                if !config.is_object() {
                    push_diagnostic(
                        &mut inventory,
                        format!(
                            "Active extension MCP server `{name}` from `{}` must be an object; skipped.",
                            reference.name
                        ),
                    );
                    continue;
                }
                let config_bytes = match serde_json::to_vec(&config) {
                    Ok(bytes) => bytes.len(),
                    Err(_) => continue,
                };
                if active_extension_mcp_config_bytes.saturating_add(config_bytes)
                    > MAX_ACTIVE_EXTENSION_MCP_CONFIG_BYTES
                {
                    push_diagnostic(
                        &mut inventory,
                        format!(
                            "Active extension MCP configs exceed the {}-byte limit; remaining entries were skipped.",
                            MAX_ACTIVE_EXTENSION_MCP_CONFIG_BYTES
                        ),
                    );
                    break;
                }
                active_extension_mcp_server_count += 1;
                active_extension_mcp_config_bytes += config_bytes;
                servers.insert(name, config);
            }
            reference.mcp_servers = servers.keys().cloned().collect();
            if !candidate.skill_configs.is_empty() {
                inventory.active_skill_extensions.push(SkillExtension {
                    name: reference.name.clone(),
                    display_name: reference.display_name.clone(),
                    skills: candidate.skill_configs,
                });
            }
            inventory.active_extensions.push(reference);
            if !servers.is_empty() {
                inventory.active_mcp_servers.push(ExtensionMcpServerSource {
                    extension_id: candidate.identity.id.clone(),
                    extension_name: candidate.reference.name,
                    install_slot: candidate.install_slot,
                    settings: candidate.settings,
                    servers,
                });
            }
        }
    }
    inventory
}

/// Read every supported installed user extension, including disabled entries.
/// This list path is deliberately independent of runtime mode gates and never
/// repairs extension state or creates plugin data directories.
pub fn load_installed_local_extensions(
    options: InstalledExtensionListOptions<'_>,
) -> InstalledExtensionList {
    let mut result = InstalledExtensionList::default();
    let extensions_root = match fs::canonicalize(options.user_extensions_dir) {
        Ok(root) if root.is_dir() => root,
        Ok(_) => return result,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return result,
        Err(error) => {
            push_vec_diagnostic(
                &mut result.diagnostics,
                format!("Could not resolve user extensions directory: {error}"),
            );
            return result;
        }
    };

    let mut budget = ReadBudget::default();
    // Scope preferences do not hide entries in a list. They are read only so
    // the shared candidate parser can retain one scan path for both commands.
    let preferences = read_preferences(&extensions_root, &mut budget, &mut result.diagnostics)
        .unwrap_or_default();
    let legacy_projection =
        match read_legacy_projection(&extensions_root, &mut budget, &mut result.diagnostics) {
            Ok(projection) => Some(projection),
            Err(error) => {
                push_vec_diagnostic(
                    &mut result.diagnostics,
                    format!("Could not load extension activation state: {error}"),
                );
                None
            }
        };

    let mut candidates = Vec::new();
    let entries = match fs::read_dir(&extensions_root) {
        Ok(entries) => entries,
        Err(error) => {
            push_vec_diagnostic(
                &mut result.diagnostics,
                format!("Could not enumerate installed extensions: {error}"),
            );
            return result;
        }
    };
    for (index, entry) in entries.enumerate() {
        if index >= MAX_ROOT_ENTRIES {
            push_vec_diagnostic(
                &mut result.diagnostics,
                format!(
                    "Extension directory contains more than {MAX_ROOT_ENTRIES} entries; remaining entries were skipped."
                ),
            );
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                push_vec_diagnostic(
                    &mut result.diagnostics,
                    format!("Could not read an extension directory entry: {error}"),
                );
                continue;
            }
        };
        let entry_path = entry.path();
        match fs::symlink_metadata(&entry_path) {
            Ok(metadata) if metadata.is_dir() => {}
            // The TypeScript manager also scans directory slots, not direct
            // symlink entries. Link installs use install metadata in a slot.
            Ok(metadata) if metadata.file_type().is_symlink() => continue,
            Ok(_) => continue,
            Err(_) => continue,
        }
        let slot_root = match fs::canonicalize(&entry_path) {
            Ok(path) if path.starts_with(&extensions_root) && path.is_dir() => path,
            _ => continue,
        };
        if candidates.len() >= MAX_INSTALLED_EXTENSIONS {
            push_vec_diagnostic(
                &mut result.diagnostics,
                format!(
                    "More than {MAX_INSTALLED_EXTENSIONS} installed extensions were found; remaining entries were skipped."
                ),
            );
            break;
        }
        match load_candidate(
            &slot_root,
            options.workspace_root,
            options.extension_store_dir,
            &preferences,
            &mut budget,
            &mut result.diagnostics,
        ) {
            Ok(Some(candidate)) => candidates.push(candidate),
            Ok(None) => {}
            Err(error) => push_vec_diagnostic(
                &mut result.diagnostics,
                format!("Skipped extension at {}: {error}", entry_path.display()),
            ),
        }
    }

    let snapshot =
        legacy_projection.as_ref().and_then(|projection| {
            match load_activation_snapshot(
                options.extension_store_dir,
                &extensions_root,
                &candidates,
                projection,
                &mut budget,
            ) {
                Ok(snapshot) => Some(snapshot),
                Err(error) => {
                    push_vec_diagnostic(
                        &mut result.diagnostics,
                        format!("Could not load extension activation state: {error}"),
                    );
                    None
                }
            }
        });

    let listed_extensions = candidates
        .into_iter()
        .map(|candidate| {
            let user_activation = snapshot.as_ref().and_then(|snapshot| {
                match get_activation(snapshot, &candidate.identity, options.user_home) {
                    Ok(activation) => Some(activation),
                    Err(error) => {
                        push_vec_diagnostic(
                            &mut result.diagnostics,
                            format!(
                                "Could not resolve user activation for {}: {error}",
                                candidate.identity.name
                            ),
                        );
                        None
                    }
                }
            });
            let workspace_activation =
                snapshot.as_ref().and_then(|snapshot| {
                    match get_activation(snapshot, &candidate.identity, options.workspace_root) {
                        Ok(activation) => Some(activation),
                        Err(error) => {
                            push_vec_diagnostic(
                                &mut result.diagnostics,
                                format!(
                                    "Could not resolve workspace activation for {}: {error}",
                                    candidate.identity.name
                                ),
                            );
                            None
                        }
                    }
                });
            let install_metadata = candidate.install_metadata;
            InstalledLocalExtension {
                id: candidate.identity.id,
                name: candidate.reference.name,
                display_name: candidate.reference.display_name,
                description: candidate.reference.description,
                version: candidate.version,
                install_slot: candidate.install_slot,
                path: candidate.reference.path,
                source: install_metadata
                    .as_ref()
                    .map(|metadata| metadata.source.clone()),
                source_type: install_metadata
                    .as_ref()
                    .map(|metadata| metadata.install_type.clone()),
                origin_source: install_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.origin_source.clone()),
                source_ref: install_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.source_ref.clone()),
                release_tag: install_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.release_tag.clone()),
                user_activation,
                workspace_activation,
                context_files: candidate.reference.context_files,
                commands: candidate.commands,
                skills: candidate.reference.skills,
                agents: candidate.reference.agents,
                mcp_servers: candidate.reference.mcp_servers,
            }
        })
        .collect();
    result.extensions = listed_extensions;

    result
}

fn load_candidate(
    slot_root: &Path,
    workspace_root: &Path,
    extension_store_dir: &Path,
    preferences: &HashMap<String, ExtensionScope>,
    budget: &mut ReadBudget,
    diagnostics: &mut Vec<String>,
) -> Result<Option<Candidate>, String> {
    let install_metadata = read_install_metadata(slot_root, budget)?;
    let effective_root = if let Some(metadata) = install_metadata
        .as_ref()
        .filter(|metadata| metadata.install_type == "link" && !metadata.source.is_empty())
    {
        let source = PathBuf::from(&metadata.source);
        let source = if source.is_absolute() {
            source
        } else {
            std::env::current_dir()
                .map_err(|error| error.to_string())?
                .join(source)
        };
        let root = fs::canonicalize(source)
            .map_err(|error| format!("linked extension source could not be resolved: {error}"))?;
        if !root.is_dir() {
            return Err("linked extension source is not a directory".to_owned());
        }
        root
    } else {
        slot_root.to_path_buf()
    };

    let plugin_manifest_path = effective_root.join("plugin.json");
    let plugin_value = match read_optional_json(
        &effective_root,
        &plugin_manifest_path,
        MAX_MANIFEST_BYTES,
        budget,
    ) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(format!("could not read plugin.json: {error}")),
    };
    let agent_plugin_status =
        plugin_value
            .as_ref()
            .map_or(AgentPluginSchemaStatus::Unrelated, |value| {
                let schema = value
                    .as_object()
                    .and_then(|object| object.get("$schema"))
                    .and_then(Value::as_str);
                match schema {
                    Some(AGENT_PLUGIN_SCHEMA) => AgentPluginSchemaStatus::Supported,
                    Some(schema) if schema.starts_with(AGENT_PLUGIN_SCHEMA_PREFIX) => {
                        AgentPluginSchemaStatus::Unsupported
                    }
                    _ => AgentPluginSchemaStatus::Unrelated,
                }
            });

    let (
        name,
        display_name,
        description,
        context_files,
        mcp_servers,
        mcp_server_configs,
        skill_configs,
        settings,
        agents,
        version,
        commands,
    ) = if agent_plugin_status != AgentPluginSchemaStatus::Unrelated {
        if agent_plugin_status == AgentPluginSchemaStatus::Unsupported {
            return Err("Agent Plugins manifest schema is unsupported".to_owned());
        }
        let value = plugin_value
            .as_ref()
            .expect("Agent Plugin status requires a manifest value");
        let config = load_agent_plugin_manifest_value(value)
            .map_err(|error| format!("invalid Agent Plugins manifest: {error}"))?;
        let plugin_id = extension_id(&config.name, install_metadata.as_ref());
        let plugin_data_root = extension_store_dir
            .join("plugin-data")
            .join("agent-plugins")
            .join(plugin_id);
        let mcp_server_configs =
            read_agent_plugin_mcp_servers(&effective_root, &plugin_data_root, budget, diagnostics);
        let mcp_servers = mcp_server_configs.keys().cloned().collect();
        let skill_configs = scan_skill_configs(&effective_root, true, budget, diagnostics);
        (
            config.name.clone(),
            Some(config.display_name),
            config.description,
            Vec::new(),
            mcp_servers,
            mcp_server_configs,
            skill_configs,
            Vec::new(),
            Vec::new(),
            config.version,
            Vec::new(),
        )
    } else {
        let manifest_path = effective_root.join("canopy-extension.json");
        let value =
            match read_optional_json(&effective_root, &manifest_path, MAX_MANIFEST_BYTES, budget) {
                Ok(Some(value)) => value,
                Ok(None) => return Ok(None),
                Err(error) => return Err(format!("could not read canopy-extension.json: {error}")),
            };
        let value = hydrate_canopy_manifest(value, &effective_root, workspace_root);
        let Some(object) = value.as_object() else {
            return Err("canopy-extension.json must contain an object".to_owned());
        };
        let Some(name) = object.get("name").and_then(Value::as_str) else {
            return Err("canopy-extension.json is missing a string name".to_owned());
        };
        if !is_valid_canopy_extension_name(name) {
            return Err("canopy-extension.json has an invalid extension name".to_owned());
        }
        let context_files = get_context_file_names(object)
            .into_iter()
            .filter(|path| path.len() <= MAX_CONTEXT_PATH_BYTES)
            .map(PathBuf::from)
            .filter_map(|path| {
                canonical_contained_file(&effective_root, &effective_root.join(path)).ok()
            })
            .take(MAX_CONTEXT_FILES)
            .collect();
        let mcp_server_configs = collect_canopy_mcp_servers(object.get("mcpServers"), diagnostics);
        let mcp_servers = mcp_server_configs.keys().cloned().collect();
        let settings = object
            .get("settings")
            .cloned()
            .map(serde_json::from_value::<Vec<ExtensionSetting>>)
            .transpose()
            .map_err(|error| format!("Invalid extension settings: {error}"))?
            .unwrap_or_default();
        validate_extension_setting_env_vars(Some(&settings))?;
        let skill_configs = scan_skill_configs(&effective_root, false, budget, diagnostics);
        let agents = scan_agent_names(&effective_root, diagnostics);
        let version = object
            .get("version")
            .and_then(Value::as_str)
            .filter(|version| !version.is_empty())
            .or_else(|| {
                install_metadata
                    .as_ref()
                    .and_then(|metadata| metadata.marketplace_config.as_ref())
                    .and_then(|marketplace| marketplace.get("metadata"))
                    .and_then(|metadata| metadata.get("version"))
                    .and_then(Value::as_str)
                    .filter(|version| !version.is_empty())
            })
            .unwrap_or("1.0.0")
            .to_owned();
        let commands = scan_command_names(&effective_root, diagnostics);
        (
            name.to_owned(),
            object
                .get("displayName")
                .and_then(Value::as_str)
                .map(str::to_owned),
            object
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            context_files,
            mcp_servers,
            mcp_server_configs,
            skill_configs,
            settings,
            agents,
            version,
            commands,
        )
    };

    if name.len() > MAX_EXTENSION_NAME_BYTES || name.trim().is_empty() {
        return Err("extension name is empty or exceeds the inventory limit".to_owned());
    }
    let Some(effective_root) = fs::canonicalize(effective_root).ok() else {
        return Err("extension root could not be canonicalized".to_owned());
    };
    let id = extension_id(&name, install_metadata.as_ref());
    let scope = preferences.get(&name).copied();
    let skill_names = skill_configs
        .iter()
        .map(|skill| skill.name.clone())
        .collect();
    Ok(Some(Candidate {
        identity: ExtensionIdentity {
            id,
            name: name.clone(),
        },
        scope,
        install_slot: slot_root.to_path_buf(),
        reference: LocalExtensionReference {
            name: name.clone(),
            config_name: Some(name),
            display_name,
            description,
            path: effective_root,
            context_files,
            skills: skill_names,
            mcp_servers,
            agents,
        },
        version,
        install_metadata,
        commands,
        skill_configs,
        settings,
        mcp_servers: mcp_server_configs,
    }))
}

fn read_install_metadata(
    slot_root: &Path,
    budget: &mut ReadBudget,
) -> Result<Option<InstallMetadata>, String> {
    let path = slot_root.join(INSTALL_METADATA_FILENAME);
    let value = match read_optional_json(slot_root, &path, MAX_INSTALL_METADATA_BYTES, budget) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Ok(None),
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let Ok(metadata) = serde_json::from_value::<InstallMetadata>(value) else {
        return Ok(None);
    };
    if !matches!(
        metadata.install_type.as_str(),
        "git" | "local" | "link" | "github-release" | "npm" | "archive-url"
    ) {
        return Ok(None);
    }
    Ok(Some(metadata))
}

fn hydrate_canopy_manifest(value: Value, extension_root: &Path, workspace_root: &Path) -> Value {
    let extension_path = extension_root.to_string_lossy().into_owned();
    let workspace_path = workspace_root.to_string_lossy().into_owned();
    let mut variables = VariableContext::new();
    variables.insert("extensionPath".to_owned(), extension_path.clone());
    variables.insert("CLAUDE_PLUGIN_ROOT".to_owned(), extension_path);
    variables.insert("workspacePath".to_owned(), workspace_path);
    variables.insert("/".to_owned(), std::path::MAIN_SEPARATOR.to_string());
    variables.insert(
        "pathSeparator".to_owned(),
        std::path::MAIN_SEPARATOR.to_string(),
    );
    let value = recursively_hydrate_strings(&value, &variables);
    crate::env_var_resolver::resolve_env_vars_in_object(&value, None)
}

fn get_context_file_names(object: &serde_json::Map<String, Value>) -> Vec<String> {
    let paths = match object.get("contextFileName") {
        Some(Value::String(path)) if !path.is_empty() => vec![path.clone()],
        Some(Value::Array(paths)) => paths
            .iter()
            .filter_map(Value::as_str)
            .filter(|path| !path.is_empty())
            .take(MAX_CONTEXT_FILES)
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    if paths.is_empty() {
        vec!["CANOPY.md".to_owned()]
    } else {
        paths
    }
}

fn read_agent_plugin_mcp_servers(
    extension_root: &Path,
    plugin_data_root: &Path,
    budget: &mut ReadBudget,
    diagnostics: &mut Vec<String>,
) -> serde_json::Map<String, Value> {
    let path = extension_root.join("mcp.json");
    let value = match read_optional_json(extension_root, &path, MAX_MANIFEST_BYTES, budget) {
        Ok(Some(value)) => value,
        Ok(None) => return serde_json::Map::new(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return serde_json::Map::new(),
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not read Agent Plugins mcp.json: {error}"),
            );
            return serde_json::Map::new();
        }
    };
    let root = extension_root.to_string_lossy();
    let data_root = plugin_data_root.to_string_lossy();
    let servers = match crate::agent_plugin_mcp::normalize_agent_plugin_mcp_servers(
        &value,
        &root,
        &data_root,
        |warning| push_vec_diagnostic(diagnostics, warning.to_owned()),
    ) {
        Ok(servers) => servers.into_iter().collect(),
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not load Agent Plugins MCP config: {error}"),
            );
            serde_json::Map::new()
        }
    };
    limit_extension_mcp_servers(servers, "Agent Plugins", diagnostics)
}

fn collect_canopy_mcp_servers(
    value: Option<&Value>,
    diagnostics: &mut Vec<String>,
) -> serde_json::Map<String, Value> {
    let Some(servers) = value.and_then(Value::as_object) else {
        return serde_json::Map::new();
    };
    limit_extension_mcp_servers(servers.clone(), "Canopy extension", diagnostics)
}

fn limit_extension_mcp_servers(
    servers: serde_json::Map<String, Value>,
    source: &str,
    diagnostics: &mut Vec<String>,
) -> serde_json::Map<String, Value> {
    let mut bounded = serde_json::Map::new();
    let mut config_bytes = 0usize;
    for (index, (name, config)) in servers.into_iter().enumerate() {
        if index >= MAX_EXTENSION_MCP_SERVERS {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "{source} MCP config exceeds the {MAX_EXTENSION_MCP_SERVERS}-server limit; remaining entries were skipped."
                ),
            );
            break;
        }
        if name.is_empty() || name.len() > MAX_EXTENSION_MCP_SERVER_NAME_BYTES {
            push_vec_diagnostic(
                diagnostics,
                format!("{source} MCP server name exceeds the inventory limit; skipped."),
            );
            continue;
        }
        let entry_bytes = match serde_json::to_vec(&config) {
            Ok(bytes) => bytes.len(),
            Err(error) => {
                push_vec_diagnostic(
                    diagnostics,
                    format!("Could not size {source} MCP server `{name}`: {error}"),
                );
                continue;
            }
        };
        if entry_bytes > MAX_EXTENSION_MCP_SERVER_CONFIG_BYTES {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "{source} MCP server `{name}` exceeds the per-server config limit; skipped."
                ),
            );
            continue;
        }
        if config_bytes.saturating_add(entry_bytes) > MAX_EXTENSION_MCP_CONFIG_BYTES {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "{source} MCP configs exceed the {}-byte per-extension limit; remaining entries were skipped.",
                    MAX_EXTENSION_MCP_CONFIG_BYTES
                ),
            );
            break;
        }
        config_bytes += entry_bytes;
        bounded.insert(name, config);
    }
    bounded
}

fn scan_skill_configs(
    extension_root: &Path,
    agent_plugin: bool,
    budget: &mut ReadBudget,
    diagnostics: &mut Vec<String>,
) -> Vec<SkillConfig> {
    let skills_root = match fs::canonicalize(extension_root.join("skills")) {
        Ok(path) if path.starts_with(extension_root) && path.is_dir() => path,
        _ => return Vec::new(),
    };
    let entries = match fs::read_dir(skills_root) {
        Ok(entries) => entries,
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not enumerate extension skills: {error}"),
            );
            return Vec::new();
        }
    };
    let mut skills = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_COMPONENT_ENTRIES {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "Extension skills contains more than {MAX_COMPONENT_ENTRIES} entries; remaining entries were skipped."
                ),
            );
            break;
        }
        let Ok(entry) = entry else { continue };
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        // Native mention descriptors do not traverse skill-directory links.
        if !file_type.is_dir() {
            continue;
        }
        let directory_name = entry.file_name().to_string_lossy().into_owned();
        let manifest = entry.path().join("SKILL.md");
        let canonical_manifest = match canonical_contained_file(extension_root, &manifest) {
            Ok(path) => path,
            Err(_) => continue,
        };
        let content = match read_bounded_file(
            &canonical_manifest,
            MAX_SKILL_MANIFEST_BYTES.min(budget.remaining()),
        ) {
            Ok(content) => content,
            Err(error) => {
                push_vec_diagnostic(
                    diagnostics,
                    format!(
                        "Could not read skill manifest {}: {error}",
                        manifest.display()
                    ),
                );
                continue;
            }
        };
        if budget.consume(content.len()).is_err() {
            break;
        }
        let content = String::from_utf8_lossy(&content);
        let skill = if agent_plugin {
            crate::agent_plugin_skills::parse_agent_plugin_skill_in_directory(
                &content,
                &canonical_manifest,
                &directory_name,
            )
            .ok()
        } else {
            crate::skills::parse_skill_content(&content, &canonical_manifest).ok()
        };
        if let Some(skill) = skill {
            skills.push(skill);
            if skills.len() >= MAX_COMPONENT_LABELS {
                break;
            }
        }
    }
    skills
}

fn scan_command_names(extension_root: &Path, diagnostics: &mut Vec<String>) -> Vec<String> {
    let commands_root = extension_root.join("commands");
    if !commands_root.is_dir() {
        return Vec::new();
    }
    let mut commands = Vec::new();
    for (index, entry) in WalkDir::new(&commands_root)
        .follow_links(true)
        .max_depth(32)
        .max_open(8)
        .into_iter()
        .enumerate()
    {
        if index >= MAX_COMMAND_ENTRIES {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "Extension commands contain more than {MAX_COMMAND_ENTRIES} entries; remaining entries were skipped."
                ),
            );
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                push_vec_diagnostic(
                    diagnostics,
                    format!("Could not enumerate extension commands: {error}"),
                );
                continue;
            }
        };
        if !entry.file_type().is_file()
            || !matches!(
                entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str()),
                Some("md" | "toml")
            )
        {
            continue;
        }
        let Ok(relative) = entry.path().strip_prefix(&commands_root) else {
            continue;
        };
        let relative = relative.with_extension("");
        let name = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy().replace(':', "_"))
            .collect::<Vec<_>>()
            .join(":");
        if name.is_empty() {
            continue;
        }
        if commands.len() >= MAX_COMMAND_LABELS {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "Extension commands exceed the {MAX_COMMAND_LABELS}-command display limit; remaining commands were skipped."
                ),
            );
            break;
        }
        commands.push(name);
    }
    commands.sort();
    commands
}

fn scan_agent_names(extension_root: &Path, diagnostics: &mut Vec<String>) -> Vec<String> {
    scan_child_names(extension_root, "agents", diagnostics, |_, entry| {
        let Ok(file_type) = entry.file_type() else {
            return None;
        };
        if !file_type.is_file() {
            return None;
        }
        let path = entry.path();
        matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("md" | "toml")
        )
        .then(|| {
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        })
    })
}

fn scan_child_names(
    extension_root: &Path,
    child_name: &str,
    diagnostics: &mut Vec<String>,
    mut project: impl FnMut(&Path, &fs::DirEntry) -> Option<String>,
) -> Vec<String> {
    let child_path = extension_root.join(child_name);
    let child_root = match fs::canonicalize(&child_path) {
        Ok(path) if path.starts_with(extension_root) && path.is_dir() => path,
        _ => return Vec::new(),
    };
    let entries = match fs::read_dir(&child_root) {
        Ok(entries) => entries,
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not enumerate extension {child_name}: {error}"),
            );
            return Vec::new();
        }
    };
    let mut names = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_COMPONENT_ENTRIES {
            push_vec_diagnostic(
                diagnostics,
                format!(
                    "Extension {child_name} contains more than {MAX_COMPONENT_ENTRIES} entries; remaining entries were skipped."
                ),
            );
            break;
        }
        let Ok(entry) = entry else { continue };
        let path = entry.path();
        if let Some(name) = project(&path, &entry) {
            names.push(name);
            if names.len() >= MAX_COMPONENT_LABELS {
                break;
            }
        }
    }
    names
}

fn extension_id(name: &str, metadata: Option<&InstallMetadata>) -> String {
    let mut source = metadata
        .map(|metadata| metadata.source.clone())
        .unwrap_or_else(|| name.to_owned());
    if metadata.is_some_and(|metadata| {
        metadata.install_type == "git" || metadata.install_type == "github-release"
    }) {
        if let Some((owner, repository)) = github_repository(&source) {
            source = format!("https://github.com/{owner}/{repository}");
        }
    }
    if let Some(plugin_name) = metadata.and_then(|metadata| metadata.plugin_name.as_deref()) {
        source.push(':');
        source.push_str(plugin_name);
    }
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn github_repository(source: &str) -> Option<(String, String)> {
    let url = if source.contains("://") {
        reqwest::Url::parse(source).ok()?
    } else {
        reqwest::Url::parse(&format!("https://github.com/{source}")).ok()?
    };
    if url.host_str()? != "github.com" {
        return None;
    }
    let components = url
        .path()
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    if components.len() != 2 || components[0].is_empty() || components[1].is_empty() {
        return None;
    }
    let repository = components[1].strip_suffix(".git").unwrap_or(components[1]);
    Some((components[0].to_owned(), repository.to_owned()))
}

fn is_valid_canopy_extension_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|character| character.is_ascii_alphanumeric() || b"-_.".contains(&character))
}

fn read_preferences(
    extensions_root: &Path,
    budget: &mut ReadBudget,
    diagnostics: &mut Vec<String>,
) -> Option<HashMap<String, ExtensionScope>> {
    let path = extensions_root.join(PREFERENCES_FILENAME);
    let value = match read_optional_json(extensions_root, &path, MAX_PREFERENCES_BYTES, budget) {
        Ok(Some(value)) => value,
        Ok(None) => return Some(HashMap::new()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Some(HashMap::new()),
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not read extension preferences: {error}"),
            );
            return None;
        }
    };
    let Some(scopes) = value.get("scopes").and_then(Value::as_object) else {
        return Some(HashMap::new());
    };
    if scopes.len() > MAX_PREFERENCE_SCOPES {
        push_vec_diagnostic(
            diagnostics,
            format!(
                "Extension preferences contain more than {MAX_PREFERENCE_SCOPES} scopes; local extensions were withheld until the preferences are within limits."
            ),
        );
        return None;
    }
    Some(
        scopes
            .iter()
            .take(MAX_PREFERENCE_SCOPES)
            .filter_map(|(name, scope)| {
                let scope = match scope.as_str() {
                    Some("user") => ExtensionScope::User,
                    Some("project") => ExtensionScope::Project,
                    _ => return None,
                };
                Some((name.clone(), scope))
            })
            .collect(),
    )
}

fn read_legacy_projection(
    extensions_root: &Path,
    budget: &mut ReadBudget,
    diagnostics: &mut Vec<String>,
) -> Result<LegacyProjection, String> {
    let path = extensions_root.join(ENABLEMENT_FILENAME);
    let value = match read_optional_json(extensions_root, &path, MAX_AUXILIARY_JSON_BYTES, budget) {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(LegacyProjection::default()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(LegacyProjection::default());
        }
        Err(error) => {
            push_vec_diagnostic(
                diagnostics,
                format!("Could not read legacy extension enablement: {error}"),
            );
            return Err(error.to_string());
        }
    };
    let serialized = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
    let serialized_hash = format!("{:x}", Sha256::digest(serialized));
    let Some(object) = value.as_object() else {
        return Ok(LegacyProjection {
            overrides: HashMap::new(),
            serialized_hash: Some(serialized_hash),
        });
    };
    if object.len() > MAX_LEGACY_POLICIES {
        push_vec_diagnostic(
            diagnostics,
            format!(
                "Legacy extension enablement contains more than {MAX_LEGACY_POLICIES} extensions."
            ),
        );
        return Err(format!(
            "legacy extension enablement exceeds the {MAX_LEGACY_POLICIES}-extension limit"
        ));
    }
    for entry in object.values() {
        if let Some(rules) = entry.get("overrides").and_then(Value::as_array)
            && rules.len() > MAX_POLICY_PATH_RULES
        {
            return Err(format!(
                "a legacy extension policy exceeds the {MAX_POLICY_PATH_RULES}-rule limit"
            ));
        }
    }
    Ok(LegacyProjection {
        overrides: object
            .iter()
            .filter_map(|(name, entry)| {
                let parsed = serde_json::from_value::<LegacyEnablement>(entry.clone()).ok()?;
                Some((name.clone(), parsed.overrides))
            })
            .collect(),
        serialized_hash: Some(serialized_hash),
    })
}

fn load_activation_snapshot(
    store_dir: &Path,
    extensions_root: &Path,
    candidates: &[Candidate],
    legacy_projection: &LegacyProjection,
    budget: &mut ReadBudget,
) -> Result<ExtensionStoreSnapshot, String> {
    let state_path = store_dir.join(ACTIVATION_STATE_FILENAME);
    let store_root = match fs::canonicalize(store_dir) {
        Ok(path) if path.is_dir() => Some(path),
        Ok(_) => return Err("extension store path is not a directory".to_owned()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.to_string()),
    };
    let parsed_state = if let Some(store_root) = store_root.as_deref() {
        match read_optional_json(store_root, &state_path, MAX_AUXILIARY_JSON_BYTES, budget) {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(format!("could not read activation snapshot: {error}")),
        }
    } else {
        None
    };
    let mut snapshot = if let Some(value) = parsed_state.as_ref() {
        parse_activation_snapshot(value).map_err(|error| {
            format!("activation snapshot is invalid; no extensions were exposed: {error}")
        })?
    } else {
        ExtensionStoreSnapshot {
            version: 2,
            generation: 0,
            legacy_projection_hash: String::new(),
            extensions: IndexMap::new(),
        }
    };

    let legacy_path = extensions_root.join(ENABLEMENT_FILENAME);
    let legacy_newer = if parsed_state.is_none() {
        true
    } else if legacy_path.exists() {
        let legacy_modified = fs::metadata(&legacy_path).and_then(|metadata| metadata.modified());
        let state_modified = fs::metadata(&state_path).and_then(|metadata| metadata.modified());
        match (legacy_modified, state_modified) {
            (Ok(legacy), Ok(state)) if legacy == state => {
                if legacy_projection.serialized_hash.as_deref()
                    != Some(snapshot.legacy_projection_hash.as_str())
                {
                    return Err(
                        "extension store state and projection disagree at the same timestamp"
                            .to_owned(),
                    );
                }
                false
            }
            (Ok(legacy), Ok(state)) => legacy > state,
            (Err(error), _) | (_, Err(error)) => return Err(error.to_string()),
        }
    } else {
        false
    };

    for candidate in candidates {
        if !snapshot.extensions.contains_key(&candidate.identity.id) {
            let stale_key = snapshot
                .extensions
                .iter()
                .find(|(id, policy)| {
                    policy.name.eq_ignore_ascii_case(&candidate.identity.name)
                        && !candidates.iter().any(|loaded| &loaded.identity.id == *id)
                })
                .map(|(id, _)| id.clone());
            if let Some(stale_key) = stale_key {
                if let Some(policy) = snapshot.extensions.shift_remove(&stale_key) {
                    let mut policy = policy;
                    policy.name = candidate.identity.name.clone();
                    snapshot
                        .extensions
                        .insert(candidate.identity.id.clone(), policy);
                }
            }
        }
        let policy = snapshot
            .extensions
            .entry(candidate.identity.id.clone())
            .or_insert_with(|| ExtensionPolicy {
                name: candidate.identity.name.clone(),
                artifact_generation: None,
                default_activation: ExtensionActivation::Enabled,
                workspace_overrides: IndexMap::new(),
                legacy_path_rules: None,
            });
        if policy.name != candidate.identity.name {
            return Err(format!(
                "activation identity {} belongs to a different extension name",
                candidate.identity.id
            ));
        }
        if legacy_newer {
            import_legacy_rules(
                policy,
                legacy_projection
                    .overrides
                    .get(&candidate.identity.name)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            );
        } else if parsed_state.is_none() {
            policy.legacy_path_rules = legacy_projection
                .overrides
                .get(&candidate.identity.name)
                .cloned();
        }
    }
    Ok(snapshot)
}

pub(crate) fn parse_activation_snapshot(value: &Value) -> Result<ExtensionStoreSnapshot, String> {
    let Some(extensions) = value.get("extensions").and_then(Value::as_object) else {
        return Err("snapshot extensions must be an object".to_owned());
    };
    if extensions.len() > MAX_SNAPSHOT_POLICIES {
        return Err(format!(
            "snapshot exceeds the {MAX_SNAPSHOT_POLICIES}-extension limit"
        ));
    }
    for policy in extensions.values() {
        let Some(policy) = policy.as_object() else {
            return Err("snapshot contains a malformed extension policy".to_owned());
        };
        if policy
            .get("workspaceOverrides")
            .and_then(Value::as_object)
            .is_none_or(|overrides| overrides.len() > MAX_POLICY_PATH_RULES)
        {
            return Err(format!(
                "snapshot policy exceeds the {MAX_POLICY_PATH_RULES}-workspace-override limit"
            ));
        }
        if let Some(rules) = policy.get("legacyPathRules") {
            if rules
                .as_array()
                .is_none_or(|rules| rules.len() > MAX_POLICY_PATH_RULES)
            {
                return Err(format!(
                    "snapshot policy exceeds the {MAX_POLICY_PATH_RULES}-legacy-rule limit"
                ));
            }
        }
        if let Some(generation) = policy.get("artifactGeneration") {
            if generation
                .as_u64()
                .is_none_or(|generation| generation > 9_007_199_254_740_991)
            {
                return Err("snapshot policy has an invalid artifact generation".to_owned());
            }
        }
    }
    let snapshot = serde_json::from_value::<ExtensionStoreSnapshot>(value.clone())
        .map_err(|error| error.to_string())?;
    if snapshot.version != 2
        || snapshot.generation > 9_007_199_254_740_991
        || snapshot.extensions.len() > MAX_SNAPSHOT_POLICIES
        || snapshot.legacy_projection_hash.len() != 64
        || !snapshot
            .legacy_projection_hash
            .bytes()
            .all(|character| character.is_ascii_digit() || (b'a'..=b'f').contains(&character))
    {
        return Err("unsupported snapshot header".to_owned());
    }
    for (id, policy) in &snapshot.extensions {
        if id.len() != 64
            || !id
                .bytes()
                .all(|character| character.is_ascii_digit() || (b'a'..=b'f').contains(&character))
            || !is_valid_canopy_extension_name(&policy.name)
            || policy
                .artifact_generation
                .is_some_and(|generation| generation > 9_007_199_254_740_991)
        {
            return Err("invalid extension identity".to_owned());
        }
    }
    Ok(snapshot)
}

fn import_legacy_rules(policy: &mut ExtensionPolicy, incoming_rules: &[String]) {
    let mut generated_rules = Vec::<(crate::extension_activation::Override, Option<String>)>::new();
    if policy.default_activation == ExtensionActivation::Disabled {
        generated_rules.push((
            crate::extension_activation::Override::from_file_rule("!/*"),
            None,
        ));
    }
    for (workspace_path, activation) in &policy.workspace_overrides {
        let effective = match activation {
            WorkspaceActivation::Enabled => ExtensionActivation::Enabled,
            WorkspaceActivation::Disabled => ExtensionActivation::Disabled,
            WorkspaceActivation::Inherit => policy.default_activation,
        };
        let input = if effective == ExtensionActivation::Disabled {
            format!("!{workspace_path}")
        } else {
            workspace_path.clone()
        };
        generated_rules.push((
            crate::extension_activation::Override::from_input(&input, false),
            Some(workspace_path.clone()),
        ));
    }

    let mut consumed = vec![false; generated_rules.len()];
    let mut imported = Vec::new();
    for rule in incoming_rules {
        let incoming = crate::extension_activation::Override::from_file_rule(rule);
        if let Some(index) = generated_rules
            .iter()
            .enumerate()
            .find(|(index, (generated, _))| !consumed[*index] && generated == &incoming)
            .map(|(index, _)| index)
        {
            consumed[index] = true;
            continue;
        }
        if let Some(index) = generated_rules
            .iter()
            .enumerate()
            .find(|(index, (generated, _))| {
                !consumed[*index]
                    && generated.base_rule == incoming.base_rule
                    && generated.include_subdirs == incoming.include_subdirs
                    && generated.is_disable != incoming.is_disable
            })
            .map(|(index, _)| index)
        {
            consumed[index] = true;
            if let Some(workspace_path) = generated_rules[index].1.as_ref() {
                policy.workspace_overrides.insert(
                    workspace_path.clone(),
                    if incoming.is_disable {
                        WorkspaceActivation::Disabled
                    } else {
                        WorkspaceActivation::Enabled
                    },
                );
            } else {
                policy.default_activation = ExtensionActivation::Enabled;
            }
            continue;
        }
        imported.push(rule.clone());
    }
    policy.legacy_path_rules = (!imported.is_empty()).then_some(imported);
}

fn read_optional_json(
    root: &Path,
    path: &Path,
    limit: usize,
    budget: &mut ReadBudget,
) -> io::Result<Option<Value>> {
    let canonical = match canonical_contained_file(root, path) {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let effective_limit = limit.min(budget.remaining());
    let bytes = read_bounded_file(&canonical, effective_limit)?;
    budget
        .consume(bytes.len())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let value = serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(Some(value))
}

fn canonical_contained_file(root: &Path, path: &Path) -> io::Result<PathBuf> {
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(path)?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "extension inventory file resolves outside its owning directory",
        ));
    }
    if !fs::metadata(&canonical_path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "extension inventory path is not a regular file",
        ));
    }
    Ok(canonical_path)
}

fn read_bounded_file(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let metadata = fs::metadata(path)?;
    if metadata.len() > limit as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds the {limit}-byte inventory limit"),
        ));
    }
    let read_limit = limit.saturating_add(1);
    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(read_limit));
    File::open(path)?
        .take(read_limit as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("file exceeds the {limit}-byte inventory limit"),
        ));
    }
    Ok(bytes)
}

fn push_diagnostic(inventory: &mut ExtensionInventory, diagnostic: String) {
    if inventory.diagnostics.len() < MAX_DIAGNOSTICS {
        inventory.diagnostics.push(diagnostic);
    }
}

fn push_vec_diagnostic(diagnostics: &mut Vec<String>, diagnostic: String) {
    if diagnostics.len() < MAX_DIAGNOSTICS {
        diagnostics.push(diagnostic);
    }
}
