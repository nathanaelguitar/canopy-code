// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Link a local Canopy extension into the user extension store.

use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use canopy_core::agent_plugins::{
    AgentPluginSchemaStatus, get_agent_plugin_schema_status, load_agent_plugin_manifest,
};
use canopy_core::config::{LoadSettingsOptions, load_settings};
use canopy_core::extension_activation::ExtensionIdentity;
use canopy_core::extension_activation_store::{
    ExtensionArtifactOperation, InitialExtensionActivation, commit_extension_artifact,
    create_extension_staging_directory,
};
use canopy_core::extension_inventory::{
    InstalledExtensionListOptions, InstalledLocalExtension, load_installed_local_extensions,
};
use canopy_core::extension_setting_helpers::{
    ExtensionSetting, format_env_content, validate_extension_setting_env_vars,
};
#[cfg(target_os = "macos")]
use canopy_core::mcp::token_storage::KeychainTokenStorage;
use canopy_core::mcp::token_storage::{EncryptedFileTokenStorage, SecretStorage};
use canopy_core::storage::Storage;
use canopy_core::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::runtime::Builder;
use uuid::Uuid;

const USAGE: &str = "Usage: canopy extensions link <path>";
const INSTALL_METADATA_FILE: &str = ".canopy-extension-install.json";
const EXTENSION_MANIFEST_FILE: &str = "canopy-extension.json";
const SETTINGS_SELECTOR_FILE: &str = ".canopy-extension-settings.json";
const SETTINGS_BUNDLE_PREFIX: &str = "$canopy:extension-settings:v2:";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LinkInstallMetadata {
    source: String,
    #[serde(rename = "type")]
    install_type: &'static str,
    origin_source: &'static str,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LinkExtensionManifest {
    #[serde(default)]
    settings: Vec<ExtensionSetting>,
}

enum LinkSecretBackend {
    Encrypted(EncryptedFileTokenStorage),
    #[cfg(target_os = "macos")]
    Keychain(KeychainTokenStorage),
}

struct LinkStoredSecretState {
    backend: LinkSecretBackend,
    keys: Vec<String>,
}

impl LinkStoredSecretState {
    async fn cleanup(self) {
        for key in self.keys {
            self.backend.delete(&key).await;
        }
    }
}

impl LinkSecretBackend {
    async fn select(config_dir: &Path, service_name: &str) -> (Self, &'static str) {
        let force_file =
            std::env::var("CANOPY_CODE_FORCE_FILE_STORAGE").is_ok_and(|value| value == "true");
        #[cfg(target_os = "macos")]
        if !force_file {
            let keychain = KeychainTokenStorage::new(service_name);
            if keychain.is_available().await.unwrap_or(false) {
                return (Self::Keychain(keychain), "keychain");
            }
        }
        let _ = (force_file, service_name);
        (
            Self::Encrypted(EncryptedFileTokenStorage::new(config_dir, service_name)),
            "encrypted_file",
        )
    }

    async fn set(&self, key: &str, value: &str) -> Result<(), String> {
        match self {
            Self::Encrypted(storage) => storage
                .set_secret(key, value)
                .await
                .map_err(|_| "Could not save sensitive extension setting.".to_owned()),
            #[cfg(target_os = "macos")]
            Self::Keychain(storage) => storage
                .set_secret(key, value)
                .await
                .map_err(|_| "Could not save sensitive extension setting.".to_owned()),
        }
    }

    async fn delete(&self, key: &str) {
        match self {
            Self::Encrypted(storage) => {
                let _ = storage.delete_secret(key).await;
            }
            #[cfg(target_os = "macos")]
            Self::Keychain(storage) => {
                let _ = storage.delete_secret(key).await;
            }
        }
    }
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        println!("Links a native extension from a local directory.");
        return Ok(());
    }
    let source = match args {
        [source] if !source.starts_with('-') => source,
        _ => return Err(format!("{USAGE}\nExpected one local extension path.")),
    };
    link(source)
}

fn link(source: &str) -> Result<(), String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let loaded_settings = load_settings(workspace.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    if !loaded_settings.is_trusted {
        return Err(format!(
            "Could not link extension from untrusted folder at {}",
            strip_terminal_control_sequences(source)
        ));
    }

    let home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let source_path = PathBuf::from(source);
    let absolute_source = if source_path.is_absolute() {
        source_path
    } else {
        workspace.join(source_path)
    };
    let canonical_source = fs::canonicalize(&absolute_source)
        .map_err(|error| format!("Could not resolve extension path: {error}"))?;
    if !canonical_source.is_dir() {
        return Err("An extension link source must be a directory.".to_owned());
    }

    let agent_plugin_status = get_agent_plugin_schema_status(&canonical_source.to_string_lossy());
    if agent_plugin_status == AgentPluginSchemaStatus::Unsupported {
        return Err("The Agent Plugins manifest schema is unsupported.".to_owned());
    }
    let origin_source = if agent_plugin_status == AgentPluginSchemaStatus::Supported {
        load_agent_plugin_manifest(&canonical_source.to_string_lossy())
            .map_err(|error| format!("Invalid Agent Plugins manifest: {error}"))?;
        "AgentPlugins"
    } else {
        let manifest = canonical_source.join(EXTENSION_MANIFEST_FILE);
        let metadata = fs::symlink_metadata(&manifest)
            .map_err(|_| "The directory must contain canopy-extension.json.".to_owned())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(
                "The directory must contain a regular canopy-extension.json file.".to_owned(),
            );
        }
        "QwenCode"
    };
    let extension_settings =
        load_link_extension_settings(&canonical_source, &workspace, origin_source)?;
    validate_extension_setting_env_vars(Some(&extension_settings))?;

    let extensions_dir = Storage::get_user_extensions_dir();
    let store_dir = Storage::get_global_canopy_dir().join("extension-store");
    let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &workspace,
        user_home: &home,
        user_extensions_dir: &extensions_dir,
        extension_store_dir: &store_dir,
    });
    refuse_incomplete_inventory(&inventory.diagnostics)?;
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {diagnostic}");
    }

    let settings_runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start extension settings runtime: {error}"))?;
    let staging_path = create_extension_staging_directory(&store_dir)?;
    let mut stored_secrets = None;
    let result = (|| {
        write_link_metadata(&staging_path, &canonical_source, origin_source)?;
        let staging_root = staging_path
            .parent()
            .ok_or_else(|| "Extension staging directory has no parent.".to_owned())?;
        let staged_inventory = load_installed_local_extensions(InstalledExtensionListOptions {
            workspace_root: &workspace,
            user_home: &home,
            user_extensions_dir: staging_root,
            extension_store_dir: &store_dir,
        });
        refuse_incomplete_inventory(&staged_inventory.diagnostics)?;
        let canonical_staging = fs::canonicalize(&staging_path)
            .map_err(|error| format!("Could not resolve staged extension: {error}"))?;
        let staged = staged_inventory
            .extensions
            .iter()
            .find(|extension| extension.install_slot == canonical_staging)
            .ok_or_else(|| {
                staged_inventory
                    .diagnostics
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "The directory is not a valid Canopy extension.".to_owned())
            })?;
        if inventory
            .extensions
            .iter()
            .any(|extension| extension.name.eq_ignore_ascii_case(&staged.name))
        {
            return Err(format!(
                "Extension \"{}\" is already installed. Please uninstall it first.",
                staged.name
            ));
        }

        show_link_consent(staged, &canonical_source)?;

        let values = crate::extensions_install_command::prompt_extension_settings(
            &settings_in_prompt_order(&extension_settings),
        )?;
        stored_secrets = settings_runtime.block_on(save_link_extension_settings(
            &extension_settings,
            &values,
            &staged.name,
            &staged.id,
            &Storage::get_global_canopy_dir(),
            &staging_path,
        ))?;

        let identity = ExtensionIdentity {
            id: staged.id.clone(),
            name: staged.name.clone(),
        };
        let known_identities = inventory
            .extensions
            .iter()
            .map(|extension| ExtensionIdentity {
                id: extension.id.clone(),
                name: extension.name.clone(),
            })
            .collect::<Vec<_>>();
        commit_extension_artifact(
            &extensions_dir,
            &store_dir,
            ExtensionArtifactOperation::Install,
            &identity,
            &extensions_dir.join(&identity.name),
            Some(&staging_path),
            Some(&InitialExtensionActivation::User),
            None,
            &known_identities,
        )?;
        println!(
            "Extension \"{}\" linked successfully and enabled.",
            strip_terminal_control_sequences(&identity.name)
        );
        Ok(())
    })();

    if result.is_err() {
        if let Some(stored_secrets) = stored_secrets {
            settings_runtime.block_on(stored_secrets.cleanup());
        }
        let _ = fs::remove_dir_all(&staging_path);
    }
    result
}

fn load_link_extension_settings(
    root: &Path,
    workspace: &Path,
    origin_source: &str,
) -> Result<Vec<ExtensionSetting>, String> {
    if origin_source == "AgentPlugins" {
        return Ok(Vec::new());
    }
    let manifest_path = root.join(EXTENSION_MANIFEST_FILE);
    let metadata = fs::metadata(&manifest_path)
        .map_err(|error| format!("Could not read extension manifest: {error}"))?;
    if !metadata.is_file() || metadata.len() > MAX_MANIFEST_BYTES {
        return Err(
            "Extension manifest is not a regular file or exceeds the 1 MiB limit.".to_owned(),
        );
    }
    let file = fs::File::open(&manifest_path)
        .map_err(|error| format!("Could not read extension manifest: {error}"))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("Could not read extension manifest: {error}"))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err("Extension manifest exceeds the 1 MiB limit.".to_owned());
    }
    let raw: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not parse extension manifest: {error}"))?;

    let mut variables = IndexMap::new();
    variables.insert(
        "extensionPath".to_owned(),
        root.to_string_lossy().into_owned(),
    );
    variables.insert(
        "CLAUDE_PLUGIN_ROOT".to_owned(),
        root.to_string_lossy().into_owned(),
    );
    variables.insert(
        "workspacePath".to_owned(),
        workspace.to_string_lossy().into_owned(),
    );
    variables.insert("/".to_owned(), std::path::MAIN_SEPARATOR.to_string());
    variables.insert(
        "pathSeparator".to_owned(),
        std::path::MAIN_SEPARATOR.to_string(),
    );
    let hydrated = canopy_core::extension_variables::recursively_hydrate_strings(&raw, &variables);
    let hydrated = canopy_core::env_var_resolver::resolve_env_vars_in_object(&hydrated, None);
    let manifest: LinkExtensionManifest = serde_json::from_value(hydrated)
        .map_err(|error| format!("Invalid extension settings: {error}"))?;
    Ok(manifest.settings)
}

fn settings_in_prompt_order(settings: &[ExtensionSetting]) -> Vec<ExtensionSetting> {
    settings
        .iter()
        .filter(|setting| setting.is_sensitive())
        .chain(settings.iter().filter(|setting| !setting.is_sensitive()))
        .cloned()
        .collect()
}

async fn save_link_extension_settings(
    settings: &[ExtensionSetting],
    values: &[(String, String)],
    name: &str,
    identity: &str,
    global_config_dir: &Path,
    staging: &Path,
) -> Result<Option<LinkStoredSecretState>, String> {
    if settings.is_empty() {
        return Ok(None);
    }

    let mut env_values = Vec::new();
    let mut secret_values = Map::new();
    for setting in settings {
        let Some((_, value)) = values.iter().find(|(key, _)| key == &setting.env_var) else {
            continue;
        };
        if setting.is_sensitive() {
            secret_values.insert(setting.env_var.clone(), Value::String(value.clone()));
        } else {
            env_values.push((setting.env_var.clone(), value.clone()));
        }
    }

    let env = format_env_content(&env_values);
    atomic_write_file(
        &staging.join(".env"),
        env.as_bytes(),
        &AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        },
    )
    .map_err(|error| format!("Could not write extension settings: {error}"))?;

    if secret_values.is_empty() {
        return Ok(None);
    }
    let service_name = format!("Canopy Code Extensions {name} {identity}");
    let (backend, backend_name) = LinkSecretBackend::select(global_config_dir, &service_name).await;
    let bundle_key = format!("{SETTINGS_BUNDLE_PREFIX}{}", Uuid::new_v4());
    let bundle = serde_json::to_string(&Value::Object(secret_values.clone()))
        .map_err(|error| error.to_string())?;
    let mut stored_keys = Vec::new();
    let storage_result = async {
        backend.set(&bundle_key, &bundle).await?;
        stored_keys.push(bundle_key.clone());
        for (key, value) in &secret_values {
            let value = value
                .as_str()
                .ok_or_else(|| "Invalid extension secret value.".to_owned())?;
            backend.set(key, value).await?;
            stored_keys.push(key.clone());
        }
        let selector = serde_json::json!({
            "version": 1,
            "backend": backend_name,
            "bundleKey": bundle_key,
        });
        let bytes = serde_json::to_vec_pretty(&selector).map_err(|error| error.to_string())?;
        atomic_write_file(
            &staging.join(SETTINGS_SELECTOR_FILE),
            &bytes,
            &AtomicWriteOptions {
                mode: Some(0o600),
                force_mode: true,
                symlink_policy: SymlinkPolicy::NoFollow,
                ..AtomicWriteOptions::default()
            },
        )
        .map_err(|error| format!("Could not write extension settings selector: {error}"))?;
        Ok::<(), String>(())
    }
    .await;
    if let Err(error) = storage_result {
        for key in stored_keys {
            backend.delete(&key).await;
        }
        return Err(error);
    }
    Ok(Some(LinkStoredSecretState {
        backend,
        keys: stored_keys,
    }))
}

fn write_link_metadata(
    staging: &Path,
    source: &Path,
    origin_source: &'static str,
) -> Result<(), String> {
    let metadata = LinkInstallMetadata {
        source: source.to_string_lossy().into_owned(),
        install_type: "link",
        origin_source,
    };
    let bytes = serde_json::to_vec_pretty(&metadata).map_err(|error| error.to_string())?;
    atomic_write_file(
        &staging.join(INSTALL_METADATA_FILE),
        &bytes,
        &AtomicWriteOptions::default(),
    )
    .map_err(|error| format!("Could not write extension link metadata: {error}"))
}

fn show_link_consent(extension: &InstalledLocalExtension, source: &Path) -> Result<(), String> {
    println!(
        "Linking extension \"{}\" from {}.",
        strip_terminal_control_sequences(
            extension.display_name.as_deref().unwrap_or(&extension.name)
        ),
        strip_terminal_control_sequences(&source.to_string_lossy())
    );
    if let Some(description) = extension
        .description
        .as_deref()
        .filter(|description| !description.is_empty())
    {
        println!("{}", strip_terminal_control_sequences(description));
    }
    println!(
        "Extensions may introduce unexpected behavior. Ensure you have investigated the extension source and trust the author."
    );
    print_list("MCP servers", &extension.mcp_servers);
    print_list("Commands", &extension.commands);
    print_list("Skills", &extension.skills);
    print_list("Subagents", &extension.agents);
    if !extension.context_files.is_empty() {
        let paths = extension
            .context_files
            .iter()
            .map(|path| strip_terminal_control_sequences(&path.to_string_lossy()))
            .collect::<Vec<_>>();
        println!(
            "This extension will append context from: {}.",
            paths.join(", ")
        );
    }
    print!("Do you want to continue? [Y/n]: ");
    io::stdout().flush().map_err(|error| error.to_string())?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|error| error.to_string())?;
    let answer = answer.trim().to_ascii_lowercase();
    let accepted =
        matches!(answer.as_str(), "y" | "yes") || (answer.is_empty() && io::stdin().is_terminal());
    if accepted {
        Ok(())
    } else if answer.is_empty() {
        Err("Could not confirm extension link without an interactive response.".to_owned())
    } else {
        Err(format!(
            "Linking cancelled for \"{}\".",
            strip_terminal_control_sequences(&extension.name)
        ))
    }
}

fn print_list(label: &str, values: &[String]) {
    if !values.is_empty() {
        let values = values
            .iter()
            .map(|value| strip_terminal_control_sequences(value))
            .collect::<Vec<_>>();
        println!("This extension will add {label}: {}.", values.join(", "));
    }
}

fn refuse_incomplete_inventory(diagnostics: &[String]) -> Result<(), String> {
    if diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("More than 128 installed extensions")
            || diagnostic.starts_with("Extension directory contains more than 256 entries")
    }) {
        return Err(
            "The installed-extension scan was truncated; refusing to link from an incomplete inventory."
                .to_owned(),
        );
    }
    Ok(())
}

fn home_directory() -> Option<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            ["HOME", "USERPROFILE"]
                .into_iter()
                .filter_map(std::env::var_os)
                .find(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let drive = std::env::var_os("HOMEDRIVE")?;
            let path = std::env::var_os("HOMEPATH")?;
            Some(PathBuf::from(drive).join(path))
        })
}
