// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Native extension acquisition, conversion, consent, staging, and commit.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use canopy_core::agent_plugins::{
    AgentPluginSchemaStatus, get_agent_plugin_schema_status, load_agent_plugin_manifest,
};
use canopy_core::archive_extraction::extract_tar_gz_archive;
use canopy_core::claude_package_converter::{
    build_canopy_extension_from_plugin_with_external_content, convert_claude_plugin_package,
    convert_claude_plugin_standalone, merge_claude_configs,
};
use canopy_core::config::{LoadSettingsOptions, load_settings};
use canopy_core::extension_activation::ExtensionIdentity;
use canopy_core::extension_activation_store::{
    ExtensionArtifactOperation, InitialExtensionActivation, commit_extension_artifact,
    create_extension_staging_directory,
};
use canopy_core::extension_install_source::{
    InstallSourceType, is_supported_archive_url, parse_github_repo_for_releases,
    parse_install_source, parse_source_and_plugin_name,
};
use canopy_core::extension_inventory::{
    InstalledExtensionListOptions, InstalledLocalExtension, load_installed_local_extensions,
};
use canopy_core::extension_network_policy::{
    ExtensionNetworkPolicy, NetworkPin, ResolvedNetworkTarget, resolve_network_target,
};
use canopy_core::extension_preferences::{ExtensionPreferencesStore, ExtensionScope};
use canopy_core::extension_setting_helpers::{
    ExtensionSetting, format_env_content, get_settings_changes, validate_extension_setting_env_vars,
};
use canopy_core::extensions::redact_url_credentials;
use canopy_core::gemini_converter::is_gemini_extension_config;
use canopy_core::gemini_package_converter::convert_gemini_extension_package;
use canopy_core::mcp::token_storage::{EncryptedFileTokenStorage, SecretStorage};
use canopy_core::qoder_converter::{QODER_PLUGIN_MANIFEST, load_qoder_plugin_input};
use canopy_core::services::image_generation::SystemAddressResolver;
use canopy_core::storage::Storage;
use canopy_core::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use canopy_core::zip_extraction::extract_zip_archive;
use futures_util::StreamExt;
use indexmap::IndexMap;
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::runtime::Builder;
use uuid::Uuid;

const USAGE: &str = "Usage: canopy extensions install <source> [options]\n  --ref <ref>\n  --auto-update\n  --pre-release\n  --registry <url>\n  --consent\n  --scope user|project|workspace\n  --network-policy public";
const MAX_ARCHIVE_BYTES: usize = 100 * 1024 * 1024;
const MAX_NPM_METADATA_BYTES: usize = 10 * 1024 * 1024;
const MAX_REDIRECTS: usize = 10;
const NETWORK_POLICY_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(20);
const INSTALL_METADATA_FILE: &str = ".canopy-extension-install.json";
const INSTALL_MANIFEST_FILE: &str = "canopy-extension.json";
const SETTINGS_SELECTOR_FILE: &str = ".canopy-extension-settings.json";
const SETTINGS_BUNDLE_PREFIX: &str = "$canopy:extension-settings:v2:";

#[derive(Clone, Debug)]
struct InstallArgs {
    source: String,
    reference: Option<String>,
    auto_update: bool,
    allow_pre_release: bool,
    registry: Option<String>,
    consent: bool,
    scope: ExtensionScope,
    scope_explicit: bool,
    network_policy: Option<ExtensionNetworkPolicy>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstallMetadata {
    pub(crate) source: String,
    #[serde(rename = "type")]
    pub(crate) install_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) plugin_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) origin_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "ref")]
    pub(crate) source_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) release_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) git_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) marketplace_config: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) registry_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auto_update: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) allow_pre_release: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) external_content: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) network_policy: Option<ExtensionNetworkPolicy>,
}

#[derive(Clone, Debug)]
struct PreparedSource {
    path: PathBuf,
    cleanup_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
struct ConvertedExtension {
    path: PathBuf,
    origin: String,
    external_content: bool,
    is_agent_plugin: bool,
    cleanup_paths: Vec<PathBuf>,
    warnings: Vec<String>,
}

/// Files staged by the shared install/update acquisition lifecycle. The caller
/// owns every path until `commit_extension_artifact` succeeds.
pub(crate) struct PreparedUpdateArtifact {
    pub(crate) identity: ExtensionIdentity,
    pub(crate) staging_directory: PathBuf,
    pub(crate) cleanup_paths: Vec<PathBuf>,
    pub(crate) original_version: String,
    pub(crate) updated_version: String,
    pub(crate) previous_metadata: InstallMetadata,
    pub(crate) install_metadata: InstallMetadata,
    pub(crate) warnings: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
struct ExtensionConfig {
    name: String,
    display_name: Option<String>,
    description: Option<String>,
    mcp_servers: Map<String, Value>,
    settings: Vec<ExtensionSetting>,
}

#[derive(Debug, Deserialize)]
struct NpmPackument {
    #[serde(rename = "dist-tags", default)]
    dist_tags: HashMap<String, String>,
    #[serde(default)]
    versions: HashMap<String, NpmVersion>,
}

#[derive(Debug, Deserialize)]
struct NpmVersion {
    dist: NpmDist,
}

#[derive(Debug, Deserialize)]
struct NpmDist {
    tarball: String,
}

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    prerelease: bool,
    #[serde(default)]
    assets: Vec<GithubAsset>,
    tarball_url: Option<String>,
    zipball_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

enum SecretBackend {
    Encrypted(EncryptedFileTokenStorage),
    #[cfg(target_os = "macos")]
    Keychain(canopy_core::mcp::token_storage::KeychainTokenStorage),
}

struct StoredSecretState {
    backend: SecretBackend,
    keys: Vec<String>,
}

impl StoredSecretState {
    async fn cleanup(self) {
        for key in self.keys {
            self.backend.delete(&key).await;
        }
    }
}

impl SecretBackend {
    async fn select(config_dir: &Path, service_name: &str) -> (Self, &'static str) {
        let force_file =
            std::env::var("CANOPY_CODE_FORCE_FILE_STORAGE").is_ok_and(|value| value == "true");
        #[cfg(target_os = "macos")]
        if !force_file {
            let keychain = canopy_core::mcp::token_storage::KeychainTokenStorage::new(service_name);
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
                .map_err(|error| error.to_string()),
            #[cfg(target_os = "macos")]
            Self::Keychain(storage) => storage
                .set_secret(key, value)
                .await
                .map_err(|error| error.to_string()),
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
        return Ok(());
    }
    let parsed = parse_args(args)?;
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start extension installer runtime: {error}"))?
        .block_on(install(parsed))
}

fn parse_args(args: &[String]) -> Result<InstallArgs, String> {
    let source = args
        .first()
        .filter(|value| !value.starts_with('-'))
        .cloned()
        .ok_or_else(|| format!("{USAGE}\nAn install source is required."))?;
    let mut reference = None;
    let mut auto_update = false;
    let mut allow_pre_release = false;
    let mut registry = None;
    let mut consent = false;
    let mut scope = ExtensionScope::User;
    let mut scope_explicit = false;
    let mut network_policy = None;
    let mut index = 1;
    while index < args.len() {
        let argument = args[index].as_str();
        match argument {
            "--ref" => reference = Some(required_option_value(args, &mut index, "--ref")?),
            value if value.starts_with("--ref=") => {
                reference = Some(nonempty_option_value(value, "--ref")?);
            }
            "--registry" => {
                registry = Some(required_option_value(args, &mut index, "--registry")?);
            }
            value if value.starts_with("--registry=") => {
                registry = Some(nonempty_option_value(value, "--registry")?);
            }
            "--network-policy" => {
                let value = required_option_value(args, &mut index, "--network-policy")?;
                network_policy = Some(parse_network_policy_option(&value)?);
            }
            value if value.starts_with("--network-policy=") => {
                let value = nonempty_option_value(value, "--network-policy")?;
                network_policy = Some(parse_network_policy_option(&value)?);
            }
            "--scope" => {
                let value = required_option_value(args, &mut index, "--scope")?;
                scope = parse_scope_option(&value)?;
                scope_explicit = true;
            }
            value if value.starts_with("--scope=") => {
                let value = nonempty_option_value(value, "--scope")?;
                scope = parse_scope_option(&value)?;
                scope_explicit = true;
            }
            "--auto-update" => auto_update = true,
            "--pre-release" | "--allow-pre-release" => allow_pre_release = true,
            "--consent" => consent = true,
            value => return Err(format!("{USAGE}\nUnknown option: {value}")),
        }
        index += 1;
    }
    Ok(InstallArgs {
        source,
        reference,
        auto_update,
        allow_pre_release,
        registry,
        consent,
        scope,
        scope_explicit,
        network_policy,
    })
}

fn required_option_value(args: &[String], index: &mut usize, name: &str) -> Result<String, String> {
    *index += 1;
    args.get(*index)
        .filter(|value| !value.starts_with('-'))
        .cloned()
        .ok_or_else(|| format!("{name} requires a value."))
}

fn nonempty_option_value(argument: &str, name: &str) -> Result<String, String> {
    argument
        .split_once('=')
        .map(|(_, value)| value)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| format!("{name} requires a value."))
}

fn parse_scope_option(value: &str) -> Result<ExtensionScope, String> {
    match value {
        "user" => Ok(ExtensionScope::User),
        "project" | "workspace" => Ok(ExtensionScope::Project),
        _ => Err(format!("Invalid scope: {value}. Use user or project.")),
    }
}

fn parse_network_policy_option(value: &str) -> Result<ExtensionNetworkPolicy, String> {
    match value {
        "public" => Ok(ExtensionNetworkPolicy::Public),
        _ => Err(format!("Invalid network policy: {value}. Use public.")),
    }
}

async fn install(args: InstallArgs) -> Result<(), String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let loaded_settings = load_settings(workspace.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    if !loaded_settings.is_trusted {
        let source = redact_url_credentials(&args.source);
        return Err(format!(
            "Could not install extension from untrusted folder at {source}"
        ));
    }
    let extensions_dir = Storage::get_user_extensions_dir();
    let store_dir = Storage::get_global_canopy_dir().join("extension-store");

    let (repo_source, plugin_name) = parse_source_and_plugin_name(&args.source);
    let local_source = fs::metadata(repo_source).is_ok();
    let parsed =
        parse_install_source(repo_source, local_source).map_err(|error| error.to_string())?;
    validate_options(&parsed.install_type, &args)?;
    let mut metadata = InstallMetadata {
        source: parsed.source,
        install_type: install_type_name(parsed.install_type).to_owned(),
        plugin_name: plugin_name.map(str::to_owned),
        origin_source: None,
        source_ref: args.reference.clone(),
        release_tag: None,
        git_commit: None,
        marketplace_config: None,
        registry_url: args.registry.clone(),
        auto_update: args.auto_update.then_some(true),
        allow_pre_release: args.allow_pre_release.then_some(true),
        external_content: None,
        network_policy: args.network_policy,
    };
    if matches!(parsed.install_type, InstallSourceType::Local) {
        let source = PathBuf::from(&metadata.source);
        metadata.source = fs::canonicalize(&source)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default().join(source))
            .to_string_lossy()
            .into_owned();
    }

    let mut prepared = None;
    let mut converted_temp = None;
    let mut staging = None;
    let mut stored_secrets: Option<StoredSecretState> = None;
    let result = async {
        let mut source = acquire_source(&mut metadata, &args).await?;
        prepared = Some(source.clone());
        let marketplace_config = load_marketplace_config(&source.path)?;
        if let Some(config) = marketplace_config.as_ref() {
            metadata.marketplace_config = Some(config.clone());
            metadata.origin_source = Some("Claude".to_owned());
            if metadata.plugin_name.is_none() {
                metadata.plugin_name = Some(select_marketplace_plugin(config)?);
            }
        }
        let converted =
            convert_source(&source.path, metadata.plugin_name.as_deref(), &args).await?;
        for warning in &converted.warnings {
            eprintln!("[CANOPY] {warning}");
        }
        if converted.path != source.path {
            converted_temp = Some(converted.path.clone());
        }
        source.cleanup_paths.extend(converted.cleanup_paths.clone());
        prepared = Some(source.clone());
        metadata.origin_source = Some(converted.origin.clone());
        metadata.external_content = converted.external_content.then_some(true);
        if converted.external_content {
            metadata.git_commit = None;
        }

        let config = load_extension_config(&converted.path, &workspace)?;
        validate_name(&config.name)?;
        if config.name.len() > 128 {
            return Err("Extension name exceeds the 128-byte store limit.".to_owned());
        }
        validate_extension_setting_env_vars(Some(&config.settings))?;
        let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
            workspace_root: &workspace,
            user_home: &home,
            user_extensions_dir: &extensions_dir,
            extension_store_dir: &store_dir,
        });
        refuse_incomplete_inventory(&inventory.diagnostics, "install")?;
        for diagnostic in &inventory.diagnostics {
            eprintln!("[CANOPY] {diagnostic}");
        }
        if inventory
            .extensions
            .iter()
            .any(|extension| extension.name.eq_ignore_ascii_case(&config.name))
        {
            return Err(format!(
                "Extension \"{}\" is already installed. Please uninstall it first.",
                config.name
            ));
        }

        let identity = ExtensionIdentity {
            id: extension_id(&metadata, &config.name),
            name: config.name.clone(),
        };
        let staging_path = create_extension_staging_directory(&store_dir)?;
        staging = Some(staging_path.clone());
        if metadata.install_type != "link" {
            copy_tree(&converted.path, &staging_path, converted.is_agent_plugin)?;
        }

        metadata_file(&staging_path, &metadata)?;
        let staging_inventory_root = staging_path
            .parent()
            .ok_or_else(|| "Prepared extension staging path has no parent.".to_owned())?;
        let staged_inventory = load_installed_local_extensions(InstalledExtensionListOptions {
            workspace_root: &workspace,
            user_home: &home,
            user_extensions_dir: staging_inventory_root,
            extension_store_dir: &store_dir,
        });
        let canonical_staging_path = fs::canonicalize(&staging_path).map_err(|error| {
            format!("Could not resolve prepared extension staging path: {error}")
        })?;
        let staged = staged_inventory
            .extensions
            .iter()
            .find(|extension| {
                extension.install_slot == canonical_staging_path
                    && extension.name == identity.name
                    && extension.id == identity.id
            })
            .ok_or_else(|| {
                staged_inventory
                    .diagnostics
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Prepared extension could not be loaded.".to_owned())
            })?;
        if staged.version.is_empty() {
            return Err("Prepared extension has no valid version.".to_owned());
        }
        show_consent(
            &config,
            &converted,
            &staging_path,
            &workspace,
            &home,
            &store_dir,
            &args,
        )?;

        let env_values = prompt_extension_settings(&config.settings)?;
        let selector = save_extension_settings(
            &config.settings,
            &env_values,
            &config.name,
            &identity.id,
            &store_dir,
            &staging_path,
        )
        .await?;
        stored_secrets = selector;

        let known_identities = inventory
            .extensions
            .iter()
            .map(|extension| ExtensionIdentity {
                id: extension.id.clone(),
                name: extension.name.clone(),
            })
            .collect::<Vec<_>>();
        let initial_activation = match args.scope {
            ExtensionScope::User => InitialExtensionActivation::User,
            ExtensionScope::Project => InitialExtensionActivation::Workspace {
                workspace_path: workspace.clone(),
            },
        };
        commit_extension_artifact(
            &extensions_dir,
            &store_dir,
            ExtensionArtifactOperation::Install,
            &identity,
            &extensions_dir.join(&identity.name),
            Some(&staging_path),
            Some(&initial_activation),
            None,
            &known_identities,
        )?;

        if args.scope_explicit {
            let preferences =
                ExtensionPreferencesStore::new(extensions_dir.join("extension-preferences.json"));
            if let Err(error) = preferences.set_scope(&identity.name, args.scope) {
                eprintln!(
                    "Warning: Extension installed, but failed to save scope preference: {error}"
                );
            }
        }
        println!(
            "Extension \"{}\" installed successfully{}.",
            identity.name,
            if args.scope == ExtensionScope::Project {
                " and enabled for the current workspace"
            } else {
                " and enabled"
            }
        );
        Ok::<(), String>(())
    }
    .await;

    if result.is_err() {
        if let Some(secrets) = stored_secrets.take() {
            secrets.cleanup().await;
        }
        if let Some(staging) = staging.take() {
            let _ = fs::remove_dir_all(staging);
        }
    }
    if let Some(source) = prepared {
        for path in source.cleanup_paths {
            let _ = fs::remove_dir_all(path);
        }
    }
    if let Some(converted) = converted_temp {
        let _ = fs::remove_dir_all(converted);
    }
    result
}

/// Prepare an installed extension update with the exact acquisition and
/// conversion path used by install. The update command owns the returned
/// staging and cleanup paths until it commits or discards the artifact.
pub(crate) async fn prepare_extension_update_artifact(
    installed: &InstalledLocalExtension,
    workspace: &Path,
    home: &Path,
    extensions_dir: &Path,
    store_dir: &Path,
) -> Result<PreparedUpdateArtifact, String> {
    let metadata_path = installed.install_slot.join(INSTALL_METADATA_FILE);
    let metadata_stat = fs::symlink_metadata(&metadata_path)
        .map_err(|error| format!("Could not read extension install metadata: {error}"))?;
    if !metadata_stat.is_file() || metadata_stat.file_type().is_symlink() {
        return Err("Extension install metadata is not a regular file.".to_owned());
    }
    if metadata_stat.len() > 64 * 1024 {
        return Err("Extension install metadata exceeds the 64 KiB limit.".to_owned());
    }
    let metadata_bytes = fs::read(&metadata_path)
        .map_err(|error| format!("Could not read extension install metadata: {error}"))?;
    let mut metadata: InstallMetadata = serde_json::from_slice(&metadata_bytes)
        .map_err(|error| format!("Could not parse extension install metadata: {error}"))?;
    let previous_metadata = metadata.clone();
    if metadata.install_type == "link" {
        return Err("Extension is linked so does not need to be updated.".to_owned());
    }
    if metadata.external_content == Some(true) {
        return Err(format!(
            "Extension \"{}\" uses external content and cannot be updated.",
            installed.name
        ));
    }
    let canonical_extensions_dir = fs::canonicalize(extensions_dir)
        .map_err(|error| format!("Could not resolve extensions directory: {error}"))?;
    let canonical_install_slot = fs::canonicalize(&installed.install_slot)
        .map_err(|error| format!("Could not resolve installed extension slot: {error}"))?;
    if !canonical_install_slot.starts_with(&canonical_extensions_dir) {
        return Err("Installed extension slot is outside the extensions directory.".to_owned());
    }

    let previous_config = load_extension_config(&installed.path, workspace)?;
    if previous_config.name != installed.name {
        return Err("Installed extension name changed while preparing its update.".to_owned());
    }
    let identity = ExtensionIdentity {
        id: installed.id.clone(),
        name: installed.name.clone(),
    };
    if extension_id(&metadata, &identity.name) != identity.id {
        return Err("Installed extension source identity does not match its metadata.".to_owned());
    }

    let args = InstallArgs {
        source: metadata.source.clone(),
        reference: metadata.source_ref.clone(),
        auto_update: metadata.auto_update.unwrap_or(false),
        allow_pre_release: metadata.allow_pre_release.unwrap_or(false),
        registry: metadata.registry_url.clone(),
        consent: true,
        scope: ExtensionScope::User,
        scope_explicit: false,
        network_policy: metadata.network_policy,
    };
    let mut acquired: Option<PreparedSource> = None;
    let mut converted_temp: Option<PathBuf> = None;
    let mut staging: Option<PathBuf> = None;

    let result = async {
        let mut source = acquire_source(&mut metadata, &args).await?;
        acquired = Some(source.clone());
        if let Some(config) = load_marketplace_config(&source.path)? {
            metadata.marketplace_config = Some(config.clone());
            metadata.origin_source = Some("Claude".to_owned());
            if metadata.plugin_name.is_none() {
                metadata.plugin_name = Some(select_marketplace_plugin(&config)?);
            }
        }
        let converted =
            convert_source(&source.path, metadata.plugin_name.as_deref(), &args).await?;
        if converted.external_content {
            return Err(format!(
                "Extension \"{}\" now uses external content and cannot be updated safely.",
                installed.name
            ));
        }
        if converted.path != source.path {
            converted_temp = Some(converted.path.clone());
        }
        source.cleanup_paths.extend(converted.cleanup_paths.clone());
        acquired = Some(source.clone());
        metadata.origin_source = Some(converted.origin.clone());
        metadata.external_content = converted.external_content.then_some(true);
        if converted.external_content {
            metadata.git_commit = None;
        }

        let updated_config = load_extension_config(&converted.path, workspace)?;
        validate_name(&updated_config.name)?;
        if updated_config.name != identity.name {
            return Err(format!(
                "Extension update changed name from \"{}\" to \"{}\".",
                identity.name, updated_config.name
            ));
        }
        validate_extension_setting_env_vars(Some(&updated_config.settings))?;
        let settings_changes = get_settings_changes(
            &updated_config.settings,
            &previous_config.settings,
        );
        if !settings_changes.prompt_for_sensitive.is_empty()
            || !settings_changes.remove_sensitive.is_empty()
            || !settings_changes.prompt_for_env.is_empty()
            || !settings_changes.remove_env.is_empty()
        {
            return Err(format!(
                "Extension \"{}\" has settings changes that require interactive reconfiguration; update manually.",
                identity.name
            ));
        }

        let calculated_identity = extension_id(&metadata, &updated_config.name);
        if calculated_identity != identity.id {
            return Err(format!(
                "Extension \"{}\" changed its stable id during update.",
                identity.name
            ));
        }

        let staging_path = create_extension_staging_directory(store_dir)?;
        staging = Some(staging_path.clone());
        if metadata.install_type != "link" {
            copy_tree(&converted.path, &staging_path, converted.is_agent_plugin)?;
        }
        preserve_update_settings(&installed.install_slot, &staging_path)?;
        metadata_file(&staging_path, &metadata)?;

        let staging_inventory_root = staging_path
            .parent()
            .ok_or_else(|| "Prepared extension staging path has no parent.".to_owned())?;
        let staged_inventory = load_installed_local_extensions(InstalledExtensionListOptions {
            workspace_root: workspace,
            user_home: home,
            user_extensions_dir: staging_inventory_root,
            extension_store_dir: store_dir,
        });
        let canonical_staging_path = fs::canonicalize(&staging_path)
            .map_err(|error| format!("Could not resolve prepared extension staging path: {error}"))?;
        let staged = staged_inventory
            .extensions
            .iter()
            .find(|extension| {
                extension.install_slot == canonical_staging_path
                    && extension.name == identity.name
                    && extension.id == identity.id
            })
            .ok_or_else(|| {
                staged_inventory
                    .diagnostics
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Prepared extension could not be loaded.".to_owned())
            })?;
        if staged.version.is_empty() {
            return Err("Prepared extension has no valid version.".to_owned());
        }

        let mut cleanup_paths = source.cleanup_paths;
        if let Some(converted_path) = converted_temp.take() {
            if !cleanup_paths.contains(&converted_path) {
                cleanup_paths.push(converted_path);
            }
        }
        let warnings = converted
            .warnings
            .into_iter()
            .map(|warning| ("extension_conversion_warning".to_owned(), warning))
            .collect();
        staging = None;
        Ok::<_, String>(PreparedUpdateArtifact {
            identity,
            staging_directory: staging_path,
            cleanup_paths,
            original_version: installed.version.clone(),
            updated_version: staged.version.clone(),
            previous_metadata,
            install_metadata: metadata,
            warnings,
        })
    }
    .await;

    if result.is_err() {
        if let Some(staging_path) = staging {
            let _ = fs::remove_dir_all(staging_path);
        }
        if let Some(source) = acquired {
            for path in source.cleanup_paths {
                let _ = fs::remove_dir_all(path);
            }
        }
        if let Some(converted) = converted_temp {
            let _ = fs::remove_dir_all(converted);
        }
    }
    result
}

fn preserve_update_settings(previous: &Path, staging: &Path) -> Result<(), String> {
    for filename in [".env", SETTINGS_SELECTOR_FILE] {
        let source = previous.join(filename);
        let metadata = match fs::symlink_metadata(&source) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(format!(
                    "Could not inspect saved extension settings {}: {error}",
                    source.display()
                ));
            }
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(format!(
                "Saved extension settings {} are not a regular file.",
                source.display()
            ));
        }
        let destination = staging.join(filename);
        fs::copy(&source, &destination).map_err(|error| {
            format!(
                "Could not preserve saved extension settings {}: {error}",
                source.display()
            )
        })?;
    }
    Ok(())
}

fn validate_options(source_type: &InstallSourceType, args: &InstallArgs) -> Result<(), String> {
    match source_type {
        InstallSourceType::Git | InstallSourceType::GithubRelease => {}
        InstallSourceType::Npm | InstallSourceType::ArchiveUrl => {
            if args.reference.is_some() {
                return Err(if *source_type == InstallSourceType::Npm {
                    "--ref is not applicable for npm extensions. Use @version suffix instead."
                        .to_owned()
                } else {
                    "--ref is not applicable for archive URL extensions.".to_owned()
                });
            }
        }
        _ => {
            if args.reference.is_some() || args.auto_update {
                return Err(
                    "--ref and --auto-update are not applicable for local extensions.".to_owned(),
                );
            }
        }
    }
    if *source_type != InstallSourceType::Npm && args.registry.is_some() {
        return Err("--registry is only applicable for npm extensions.".to_owned());
    }
    Ok(())
}

fn install_type_name(source_type: InstallSourceType) -> &'static str {
    match source_type {
        InstallSourceType::Git => "git",
        InstallSourceType::Local => "local",
        InstallSourceType::Link => "link",
        InstallSourceType::GithubRelease => "github-release",
        InstallSourceType::Npm => "npm",
        InstallSourceType::ArchiveUrl => "archive-url",
    }
}

async fn acquire_source(
    metadata: &mut InstallMetadata,
    args: &InstallArgs,
) -> Result<PreparedSource, String> {
    match metadata.install_type.as_str() {
        "local" | "link" => {
            let path = PathBuf::from(&metadata.source);
            if is_archive_path(&path) {
                let root = create_temporary_directory("extension-source")?;
                let result = async {
                    extract_archive(&path, &root).await?;
                    flatten_archive_root(&root, None)?;
                    assert_has_supported_manifest(&root)?;
                    Ok::<_, String>(PreparedSource {
                        path: root.clone(),
                        cleanup_paths: vec![root.clone()],
                    })
                }
                .await;
                remove_temp_on_error(&root, result)
            } else {
                if !path.is_dir() {
                    return Err(format!(
                        "Extension source is not a directory or supported archive: {}",
                        redact_url_credentials(&metadata.source)
                    ));
                }
                Ok(PreparedSource {
                    path: path.clone(),
                    cleanup_paths: Vec::new(),
                })
            }
        }
        "archive-url" => {
            let root = create_temporary_directory("extension-source")?;
            let result = async {
                let archive = root.join(archive_filename(&metadata.source)?);
                download_to_file(
                    &metadata.source,
                    &archive,
                    MAX_ARCHIVE_BYTES,
                    false,
                    metadata.network_policy,
                )
                .await?;
                extract_archive(&archive, &root).await?;
                fs::remove_file(&archive).map_err(|error| error.to_string())?;
                flatten_archive_root(&root, None)?;
                assert_has_supported_manifest(&root)?;
                Ok::<_, String>(PreparedSource {
                    path: root.clone(),
                    cleanup_paths: vec![root.clone()],
                })
            }
            .await;
            remove_temp_on_error(&root, result)
        }
        "npm" => {
            let root = create_temporary_directory("extension-npm")?;
            let result = async {
                let (package_name, requested_version) = parse_npm_source(&metadata.source)?;
                let configured_registry = resolve_npm_registry(&package_name, None);
                let registry = resolve_npm_registry(&package_name, args.registry.as_deref());
                metadata.registry_url = Some(registry.clone());
                let auth_token = resolve_npm_auth_token(&registry, &configured_registry);
                let headers = auth_token.as_deref().map(bearer_headers).transpose()?;
                let escaped = package_name.replace('/', "%2f");
                let registry_url = format!("{registry}/{escaped}");
                let response = request_bounded(
                    &registry_url,
                    MAX_NPM_METADATA_BYTES,
                    headers.clone(),
                    metadata.network_policy,
                )
                .await?;
                if response.status != StatusCode::OK {
                    return Err(format!(
                        "npm registry request failed with status {}: {}",
                        response.status,
                        redact_url_credentials(&registry_url)
                    ));
                }
                let packument_bytes = response.body;
                let packument: NpmPackument = serde_json::from_slice(&packument_bytes)
                    .map_err(|error| format!("Invalid npm package metadata: {error}"))?;
                let version = requested_version
                    .as_deref()
                    .and_then(|requested| {
                        packument
                            .versions
                            .contains_key(requested)
                            .then_some(requested)
                            .or_else(|| packument.dist_tags.get(requested).map(String::as_str))
                    })
                    .or_else(|| packument.dist_tags.get("latest").map(String::as_str))
                    .ok_or_else(|| {
                        format!("No latest version found for npm package {package_name}.")
                    })?;
                let release = packument.versions.get(version).ok_or_else(|| {
                    format!("npm package {package_name} has no version {version}.")
                })?;
                let archive = root.join("package.tgz");
                let tarball_headers = if same_url_host(&release.dist.tarball, &registry) {
                    headers
                } else {
                    None
                };
                download_to_file_with_headers(
                    &release.dist.tarball,
                    &archive,
                    MAX_ARCHIVE_BYTES,
                    tarball_headers,
                    metadata.network_policy,
                )
                .await?;
                extract_archive(&archive, &root).await?;
                fs::remove_file(&archive).map_err(|error| error.to_string())?;
                flatten_archive_root(&root, None)?;
                assert_has_supported_manifest(&root)?;
                metadata.release_tag = Some(version.to_owned());
                Ok::<_, String>(PreparedSource {
                    path: root.clone(),
                    cleanup_paths: vec![root.clone()],
                })
            }
            .await;
            remove_temp_on_error(&root, result)
        }
        "git" | "github-release" => {
            let root = create_temporary_directory("extension-git")?;
            if let Ok(repository) = parse_github_repo_for_releases(&metadata.source) {
                if let Ok(Some((release_root, tag))) = acquire_github_release(
                    &repository.owner,
                    &repository.repo,
                    metadata.source_ref.as_deref(),
                    args,
                    metadata.network_policy,
                )
                .await
                {
                    let _ = fs::remove_dir_all(&root);
                    metadata.install_type = "github-release".to_owned();
                    metadata.release_tag = Some(tag);
                    return Ok(PreparedSource {
                        path: release_root.clone(),
                        cleanup_paths: vec![release_root.clone()],
                    });
                }
            }
            let commit = match clone_git(
                &metadata.source,
                args.reference.as_deref(),
                &root,
                metadata.network_policy,
            )
            .await
            {
                Ok(commit) => commit,
                Err(error) => {
                    let _ = fs::remove_dir_all(&root);
                    return Err(error);
                }
            };
            metadata.install_type = "git".to_owned();
            metadata.git_commit = Some(commit);
            Ok(PreparedSource {
                path: root.clone(),
                cleanup_paths: vec![root],
            })
        }
        kind => Err(format!("Unsupported install type: {kind}")),
    }
}

fn remove_temp_on_error<T>(path: &Path, result: Result<T, String>) -> Result<T, String> {
    if result.is_err() {
        let _ = fs::remove_dir_all(path);
    }
    result
}

async fn acquire_github_release(
    owner: &str,
    repo: &str,
    reference: Option<&str>,
    args: &InstallArgs,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Result<Option<(PathBuf, String)>, String> {
    let endpoint = if let Some(reference) = reference {
        format!("https://api.github.com/repos/{owner}/{repo}/releases/tags/{reference}")
    } else if args.allow_pre_release {
        format!("https://api.github.com/repos/{owner}/{repo}/releases")
    } else {
        format!("https://api.github.com/repos/{owner}/{repo}/releases/latest")
    };
    let headers = github_headers()?;
    let response = match request_bounded(
        &endpoint,
        MAX_NPM_METADATA_BYTES,
        Some(headers),
        network_policy,
    )
    .await
    {
        Ok(response) => response,
        Err(_) => return Ok(None),
    };
    if response.status != StatusCode::OK {
        return Ok(None);
    }
    let releases = if args.allow_pre_release && reference.is_none() {
        serde_json::from_slice::<Vec<GithubRelease>>(&response.body)
            .map_err(|error| format!("Invalid GitHub release response: {error}"))?
            .into_iter()
            .find(|release| args.allow_pre_release || !release.prerelease)
    } else {
        Some(
            serde_json::from_slice::<GithubRelease>(&response.body)
                .map_err(|error| format!("Invalid GitHub release response: {error}"))?,
        )
    };
    let Some(release) = releases else {
        return Ok(None);
    };
    let platform = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    };
    let arch = match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    };
    let asset = release
        .assets
        .iter()
        .find(|asset| {
            asset
                .name
                .to_ascii_lowercase()
                .starts_with(&format!("{platform}.{arch}."))
        })
        .or_else(|| {
            release.assets.iter().find(|asset| {
                asset
                    .name
                    .to_ascii_lowercase()
                    .starts_with(&format!("{platform}."))
            })
        })
        .or_else(|| {
            if release.assets.len() == 1
                && !["darwin", "linux", "win32"].iter().any(|platform| {
                    release.assets[0]
                        .name
                        .to_ascii_lowercase()
                        .contains(platform)
                })
            {
                release.assets.first()
            } else {
                None
            }
        });
    let archive_url = asset
        .map(|asset| asset.browser_download_url.clone())
        .or(release.tarball_url)
        .or(release.zipball_url);
    let Some(archive_url) = archive_url else {
        return Ok(None);
    };
    if !is_supported_archive_url(&archive_url) {
        return Ok(None);
    }
    let root = create_temporary_directory("extension-release")?;
    let result = async {
        let archive = root.join(archive_filename(&archive_url)?);
        download_to_file(
            &archive_url,
            &archive,
            MAX_ARCHIVE_BYTES,
            true,
            network_policy,
        )
        .await?;
        extract_archive(&archive, &root).await?;
        fs::remove_file(archive).map_err(|error| error.to_string())?;
        flatten_archive_root(&root, None)?;
        assert_has_supported_manifest(&root)?;
        Ok::<_, String>(())
    }
    .await;
    if let Err(error) = result {
        let _ = fs::remove_dir_all(&root);
        return Err(error);
    }
    Ok(Some((root, release.tag_name)))
}

struct BoundedResponse {
    status: StatusCode,
    body: Vec<u8>,
}

async fn request_bounded(
    initial_url: &str,
    max_bytes: usize,
    initial_headers: Option<HeaderMap>,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Result<BoundedResponse, String> {
    let mut current = Url::parse(initial_url).map_err(|error| error.to_string())?;
    if current.scheme() != "https" {
        return Err("Extension downloads require HTTPS.".to_owned());
    }
    let mut headers = initial_headers.unwrap_or_default();
    headers.insert(USER_AGENT, HeaderValue::from_static("canopy-code"));
    for redirect in 0..=MAX_REDIRECTS {
        let target = resolve_extension_network_target(current.as_str(), network_policy).await?;
        let response = extension_http_client(&target, Duration::from_secs(120))?
            .get(target.url.clone())
            .headers(headers.clone())
            .send()
            .await
            .map_err(|error| redact_url_credentials(&error.to_string()))?;
        if response.status().is_redirection() {
            if redirect == MAX_REDIRECTS {
                return Err("Too many redirects while downloading extension archive.".to_owned());
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| "Extension download redirect had no valid location.".to_owned())?;
            let next = current
                .join(location)
                .map_err(|error| format!("Invalid extension redirect URL: {error}"))?;
            if next.scheme() != "https" {
                return Err("Extension download redirects must use HTTPS.".to_owned());
            }
            if !same_url_host(next.as_str(), current.as_str()) {
                headers.remove(AUTHORIZATION);
            }
            current = next;
            continue;
        }
        let status = response.status();
        if status != StatusCode::OK {
            return Ok(BoundedResponse {
                status,
                body: Vec::new(),
            });
        }
        if response
            .content_length()
            .is_some_and(|length| length > max_bytes as u64)
        {
            return Err(format!("Extension download exceeds {max_bytes} bytes."));
        }
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| redact_url_credentials(&error.to_string()))?;
            if body.len().saturating_add(chunk.len()) > max_bytes {
                return Err(format!("Extension download exceeds {max_bytes} bytes."));
            }
            body.extend_from_slice(&chunk);
        }
        return Ok(BoundedResponse { status, body });
    }
    Err("Too many redirects while downloading extension archive.".to_owned())
}

async fn download_to_file(
    url: &str,
    destination: &Path,
    max_bytes: usize,
    github_token: bool,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Result<(), String> {
    let headers = if github_token {
        Some(github_headers()?)
    } else {
        None
    };
    download_to_file_with_headers(url, destination, max_bytes, headers, network_policy).await
}

async fn download_to_file_with_headers(
    url: &str,
    destination: &Path,
    max_bytes: usize,
    initial_headers: Option<HeaderMap>,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Result<(), String> {
    let mut current = Url::parse(url).map_err(|error| error.to_string())?;
    if current.scheme() != "https" {
        return Err("Extension downloads require HTTPS.".to_owned());
    }
    let mut headers = initial_headers.unwrap_or_default();
    headers.insert(USER_AGENT, HeaderValue::from_static("canopy-code"));
    let result = async {
        for redirect in 0..=MAX_REDIRECTS {
            let target = resolve_extension_network_target(current.as_str(), network_policy).await?;
            let response = extension_http_client(&target, Duration::from_secs(120))?
                .get(target.url.clone())
                .headers(headers.clone())
                .send()
                .await
                .map_err(|error| redact_url_credentials(&error.to_string()))?;
            if response.status().is_redirection() {
                if redirect == MAX_REDIRECTS {
                    return Err(
                        "Too many redirects while downloading extension archive.".to_owned()
                    );
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        "Extension download redirect had no valid location.".to_owned()
                    })?;
                let next = current
                    .join(location)
                    .map_err(|error| format!("Invalid extension redirect URL: {error}"))?;
                if next.scheme() != "https" {
                    return Err("Extension download redirects must use HTTPS.".to_owned());
                }
                if !same_url_host(next.as_str(), current.as_str()) {
                    headers.remove(AUTHORIZATION);
                }
                current = next;
                continue;
            }
            if response.status() != StatusCode::OK {
                return Err(format!(
                    "Extension download failed with HTTP status {}.",
                    response.status()
                ));
            }
            if response
                .content_length()
                .is_some_and(|length| length > max_bytes as u64)
            {
                return Err(format!("Extension download exceeds {max_bytes} bytes."));
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination)
                .map_err(|error| format!("Could not create extension archive file: {error}"))?;
            let mut stream = response.bytes_stream();
            let mut total = 0usize;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|error| redact_url_credentials(&error.to_string()))?;
                total = total.saturating_add(chunk.len());
                if total > max_bytes {
                    return Err(format!("Extension download exceeds {max_bytes} bytes."));
                }
                file.write_all(&chunk)
                    .map_err(|error| format!("Could not write extension archive: {error}"))?;
            }
            file.sync_all()
                .map_err(|error| format!("Could not sync extension archive: {error}"))?;
            return Ok(());
        }
        Err("Too many redirects while downloading extension archive.".to_owned())
    }
    .await;
    if result.is_err() {
        let _ = fs::remove_file(destination);
    }
    result
}

pub(crate) async fn resolve_extension_network_target(
    url: &str,
    policy: Option<ExtensionNetworkPolicy>,
) -> Result<ResolvedNetworkTarget, String> {
    tokio::time::timeout(
        NETWORK_POLICY_RESOLUTION_TIMEOUT,
        resolve_network_target(url, policy, &SystemAddressResolver, None),
    )
    .await
    .map_err(|_| "Timed out resolving extension network host.".to_owned())?
    .map_err(|error| redact_url_credentials(&error.to_string()))
}

fn extension_http_client(
    target: &ResolvedNetworkTarget,
    timeout: Duration,
) -> Result<Client, String> {
    let mut builder = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout);
    if let Some(pin) = &target.pin {
        // A proxy would resolve/connect to the origin independently and bypass
        // the checked address pin. Public-policy requests therefore go direct.
        builder = pin.apply_to_client_builder(builder.no_proxy());
    }
    builder.build().map_err(|error| error.to_string())
}

pub(crate) fn public_git_environment() -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    let mut environment = [
        "PATH",
        "Path",
        "SystemRoot",
        "SYSTEMROOT",
        "WINDIR",
        "TEMP",
        "TMP",
        "TMPDIR",
    ]
    .into_iter()
    .filter_map(|key| std::env::var_os(key).map(|value| (key.into(), value)))
    .collect::<Vec<_>>();
    environment.push(("GIT_CONFIG_NOSYSTEM".into(), "1".into()));
    environment.push((
        "GIT_CONFIG_GLOBAL".into(),
        if cfg!(windows) { "NUL" } else { "/dev/null" }.into(),
    ));
    environment.push(("GIT_TERMINAL_PROMPT".into(), "0".into()));
    environment
}

pub(crate) fn public_git_config_arguments(pin: &NetworkPin) -> Vec<String> {
    [
        format!("http.curloptResolve={}", pin.curl_resolve),
        "http.followRedirects=false".to_owned(),
        "http.proxy=".to_owned(),
        "protocol.allow=never".to_owned(),
        "protocol.https.allow=always".to_owned(),
    ]
    .into_iter()
    .flat_map(|setting| ["-c".to_owned(), setting])
    .collect()
}

pub(crate) fn ensure_public_git_supported() -> Result<(), String> {
    let output = Command::new("git")
        .arg("--version")
        .output()
        .map_err(|_| "Public extension Git installs require Git 2.37 or newer.".to_owned())?;
    if !output.status.success() {
        return Err("Public extension Git installs require Git 2.37 or newer.".to_owned());
    }
    let version = String::from_utf8_lossy(&output.stdout);
    let Some(version) = version.split_whitespace().nth(2) else {
        return Err("Public extension Git installs require Git 2.37 or newer.".to_owned());
    };
    let mut components = version.split('.');
    let major = components.next().and_then(|part| part.parse::<u32>().ok());
    let minor = components.next().and_then(|part| part.parse::<u32>().ok());
    if !matches!((major, minor), (Some(major), Some(minor)) if major > 2 || (major == 2 && minor >= 37))
    {
        return Err("Public extension Git installs require Git 2.37 or newer.".to_owned());
    }
    Ok(())
}

fn github_headers() -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/vnd.github+json"),
    );
    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        if !token.is_empty() {
            let value = HeaderValue::from_str(&format!("token {token}"))
                .map_err(|_| "GITHUB_TOKEN contains invalid header characters.".to_owned())?;
            headers.insert(AUTHORIZATION, value);
        }
    }
    Ok(headers)
}

async fn extract_archive(archive: &Path, destination: &Path) -> Result<(), String> {
    if is_zip_path(archive) {
        extract_zip_archive(archive, destination, None)
            .await
            .map_err(|error| format!("Extension archive could not be extracted: {error}"))
    } else if is_tar_gz_path(archive) {
        extract_tar_gz_archive(archive, destination, None)
            .await
            .map_err(|error| format!("Extension archive could not be extracted: {error}"))
    } else {
        Err(format!(
            "Unsupported archive file for extension install: {}",
            archive.display()
        ))
    }
}

fn is_archive_path(path: &Path) -> bool {
    is_zip_path(path) || is_tar_gz_path(path)
}

fn is_zip_path(path: &Path) -> bool {
    path.to_string_lossy()
        .to_ascii_lowercase()
        .ends_with(".zip")
}

fn is_tar_gz_path(path: &Path) -> bool {
    path.to_string_lossy()
        .to_ascii_lowercase()
        .ends_with(".tar.gz")
}

fn archive_filename(source: &str) -> Result<String, String> {
    let url = Url::parse(source).map_err(|error| format!("Invalid archive URL: {error}"))?;
    let name = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .unwrap_or("extension.tar.gz");
    let name = Path::new(name)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "Invalid extension archive filename.".to_owned())?;
    Ok(name.to_owned())
}

fn flatten_archive_root(destination: &Path, ignored_file: Option<&Path>) -> Result<(), String> {
    if has_supported_manifest(destination) {
        return Ok(());
    }
    let ignored = ignored_file.and_then(Path::file_name);
    let entries = fs::read_dir(destination)
        .map_err(|error| error.to_string())?
        .filter_map(Result::ok)
        .filter(|entry| Some(entry.file_name().as_os_str()) != ignored)
        .collect::<Vec<_>>();
    if entries.len() > 2 {
        return Ok(());
    }
    let Some(directory) = entries.iter().find(|entry| entry.path().is_dir()) else {
        return Ok(());
    };
    let root = directory.path();
    if !has_supported_manifest(&root) {
        return Ok(());
    }
    let children = fs::read_dir(&root)
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    for child in &children {
        let target = destination.join(child.file_name());
        if fs::symlink_metadata(&target).is_ok() {
            return Err(format!(
                "Extension archive cannot be flattened because \"{}\" exists both at the archive root and inside \"{}\".",
                child.file_name().to_string_lossy(),
                directory.file_name().to_string_lossy()
            ));
        }
    }
    for child in &children {
        fs::rename(child.path(), destination.join(child.file_name()))
            .map_err(|error| error.to_string())?;
    }
    fs::remove_dir(root).map_err(|error| error.to_string())
}

fn has_supported_manifest(root: &Path) -> bool {
    get_agent_plugin_schema_status(&root.to_string_lossy()) != AgentPluginSchemaStatus::Unrelated
        || [
            INSTALL_MANIFEST_FILE,
            "gemini-extension.json",
            ".claude-plugin/marketplace.json",
            ".claude-plugin/plugin.json",
            QODER_PLUGIN_MANIFEST,
        ]
        .iter()
        .any(|manifest| root.join(manifest).exists())
}

fn assert_has_supported_manifest(root: &Path) -> Result<(), String> {
    if has_supported_manifest(root) {
        return Ok(());
    }
    Err("Extension archive is missing a supported extension manifest. Expected canopy-extension.json, gemini-extension.json, .claude-plugin/marketplace.json, .claude-plugin/plugin.json, .qoder-plugin/plugin.json, or an Agent Plugins plugin.json at the archive root or inside a single top-level extension directory.".to_owned())
}

fn create_temporary_directory(prefix: &str) -> Result<PathBuf, String> {
    let base = std::env::temp_dir();
    for _ in 0..8 {
        let path = base.join(format!("canopy-{prefix}-{}", Uuid::new_v4()));
        match fs::create_dir(&path) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).map_err(
                        |error| format!("Could not secure extension temp directory: {error}"),
                    )?;
                }
                return Ok(path);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "Could not create extension temp directory: {error}"
                ));
            }
        }
    }
    Err("Could not allocate a unique extension temp directory.".to_owned())
}

async fn clone_git(
    source: &str,
    reference: Option<&str>,
    destination: &Path,
    network_policy: Option<ExtensionNetworkPolicy>,
) -> Result<String, String> {
    let pin = if network_policy == Some(ExtensionNetworkPolicy::Public) {
        ensure_public_git_supported()?;
        Some(
            resolve_extension_network_target(source, network_policy)
                .await?
                .pin
                .ok_or_else(|| "Could not pin the public Git source address.".to_owned())?,
        )
    } else {
        None
    };
    let run_git = |arguments: &[&str]| -> Result<std::process::Output, String> {
        let mut command = Command::new("git");
        if let Some(pin) = &pin {
            command.env_clear().envs(public_git_environment());
            command.args(public_git_config_arguments(pin));
        }
        let output = command
            .args(arguments)
            .current_dir(destination)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .map_err(|error| format!("Failed to run git: {error}"))?;
        if output.status.success() {
            Ok(output)
        } else {
            let detail = String::from_utf8_lossy(&output.stderr);
            Err(format!("{}", redact_url_credentials(detail.trim())))
        }
    };
    run_git(&["init", "--quiet"])?;
    run_git(&["remote", "add", "origin", source])?;
    let reference = reference.unwrap_or("HEAD");
    let mut fetch = Command::new("git");
    if let Some(pin) = &pin {
        fetch.env_clear().envs(public_git_environment());
        fetch.args(public_git_config_arguments(pin));
    }
    fetch
        .args(["fetch", "--depth", "1", "origin", reference])
        .current_dir(destination)
        .env("GIT_TERMINAL_PROMPT", "0");
    add_github_auth_env(&mut fetch);
    let output = fetch
        .output()
        .map_err(|error| format!("Failed to run git: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "Failed to clone Git repository from {}: {}",
            redact_url_credentials(source),
            redact_url_credentials(String::from_utf8_lossy(&output.stderr).trim())
        ));
    }
    run_git(&["checkout", "--quiet", "--detach", "FETCH_HEAD"])?;
    let output = run_git(&["rev-parse", "HEAD"])?;
    String::from_utf8(output.stdout)
        .map(|commit| commit.trim().to_owned())
        .map_err(|error| error.to_string())
}

fn add_github_auth_env(command: &mut Command) {
    let Ok(token) = std::env::var("GITHUB_TOKEN") else {
        return;
    };
    if token.is_empty() {
        return;
    }
    use base64::Engine;
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
    command
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
        .env(
            "GIT_CONFIG_VALUE_0",
            format!("AUTHORIZATION: basic {encoded}"),
        );
}

fn parse_npm_source(source: &str) -> Result<(String, Option<String>), String> {
    let Some(rest) = source.strip_prefix('@') else {
        return Err(format!("Invalid scoped npm package source: {source}"));
    };
    let Some((scope, package_and_version)) = rest.split_once('/') else {
        return Err(format!("Invalid scoped npm package source: {source}"));
    };
    let (package, version) = match package_and_version.split_once('@') {
        Some((package, version)) if !version.is_empty() => (package, Some(version.to_owned())),
        Some(_) => return Err(format!("Invalid scoped npm package source: {source}")),
        None => (package_and_version, None),
    };
    if scope.is_empty() || package.is_empty() {
        return Err(format!("Invalid scoped npm package source: {source}"));
    }
    Ok((format!("@{scope}/{package}"), version))
}

fn resolve_npm_registry(package: &str, override_url: Option<&str>) -> String {
    if let Some(registry) = override_url {
        return registry.trim_end_matches('/').to_owned();
    }
    let scope = package
        .strip_prefix('@')
        .and_then(|package| package.split('/').next())
        .unwrap_or_default();
    let mut scoped_registry = None;
    let mut default_registry = None;
    for path in [
        std::env::current_dir().unwrap_or_default().join(".npmrc"),
        home_directory().unwrap_or_default().join(".npmrc"),
    ] {
        let Ok(content) = fs::read_to_string(path) else {
            continue;
        };
        for line in content.lines().map(str::trim) {
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim().trim_end_matches('/');
                if key == format!("@{scope}:registry") && scoped_registry.is_none() {
                    scoped_registry = Some(value.to_owned());
                }
                if key == "registry" && default_registry.is_none() {
                    default_registry = Some(value.to_owned());
                }
            }
        }
    }
    scoped_registry
        .or(default_registry)
        .unwrap_or_else(|| "https://registry.npmjs.org".to_owned())
}

fn resolve_npm_auth_token(registry: &str, configured_registry: &str) -> Option<String> {
    let registry_url = Url::parse(registry).ok()?;
    if let Ok(token) = std::env::var("NPM_TOKEN") {
        if !token.is_empty()
            && Url::parse(configured_registry)
                .ok()
                .is_some_and(|configured| configured.origin() == registry_url.origin())
        {
            return Some(token);
        }
    }

    let authority = registry_url
        .port()
        .map(|port| format!("{}:{port}", registry_url.host_str().unwrap_or_default()))
        .unwrap_or_else(|| registry_url.host_str().unwrap_or_default().to_owned());
    let mut prefixes = Vec::new();
    let mut path_segments = registry_url
        .path()
        .trim_end_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    loop {
        let path = path_segments.join("/");
        prefixes.push(if path.is_empty() {
            authority.clone()
        } else {
            format!("{authority}/{path}")
        });
        if path_segments.pop().is_none() {
            break;
        }
    }
    for path in [
        std::env::current_dir().unwrap_or_default().join(".npmrc"),
        home_directory().unwrap_or_default().join(".npmrc"),
    ] {
        let Ok(contents) = fs::read_to_string(path) else {
            continue;
        };
        for line in contents.lines().map(str::trim) {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            let Some(prefix) = key
                .trim()
                .strip_prefix("//")
                .and_then(|key| key.strip_suffix(":_authToken"))
            else {
                continue;
            };
            if prefixes
                .iter()
                .any(|candidate| candidate == prefix.trim_end_matches('/'))
            {
                let token = value.trim();
                if !token.is_empty() {
                    return Some(token.to_owned());
                }
            }
        }
    }
    None
}

fn bearer_headers(token: &str) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| "npm auth token contains invalid header characters.".to_owned())?;
    headers.insert(AUTHORIZATION, value);
    Ok(headers)
}

fn same_url_host(first: &str, second: &str) -> bool {
    let (Ok(first), Ok(second)) = (Url::parse(first), Url::parse(second)) else {
        return false;
    };
    first.host_str() == second.host_str() && first.port() == second.port()
}

fn load_marketplace_config(root: &Path) -> Result<Option<Value>, String> {
    let path = root.join(".claude-plugin/marketplace.json");
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| format!("Could not parse Claude marketplace configuration: {error}")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!(
            "Could not read Claude marketplace configuration: {error}"
        )),
    }
}

fn select_marketplace_plugin(marketplace: &Value) -> Result<String, String> {
    let plugins = marketplace
        .get("plugins")
        .and_then(Value::as_array)
        .ok_or_else(|| "Claude marketplace has no plugin list.".to_owned())?;
    if plugins.is_empty() {
        return Err("No plugins available in this marketplace.".to_owned());
    }
    println!("Select a plugin to install from the marketplace:");
    for (index, plugin) in plugins.iter().enumerate() {
        let name = plugin
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>");
        println!("  {}. {}", index + 1, strip_terminal_controls(name));
    }
    print!("Plugin number: ");
    io::stdout().flush().map_err(|error| error.to_string())?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|error| error.to_string())?;
    let selected = line
        .trim()
        .parse::<usize>()
        .ok()
        .and_then(|index| plugins.get(index.saturating_sub(1)))
        .and_then(|plugin| plugin.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| "Plugin selection cancelled or invalid.".to_owned())?;
    Ok(selected.to_owned())
}

async fn convert_source(
    root: &Path,
    plugin_name: Option<&str>,
    args: &InstallArgs,
) -> Result<ConvertedExtension, String> {
    let agent_status = get_agent_plugin_schema_status(&root.to_string_lossy());
    if agent_status == AgentPluginSchemaStatus::Unsupported {
        return Err("Unsupported Agent Plugins schema.".to_owned());
    }
    if agent_status == AgentPluginSchemaStatus::Supported && plugin_name.is_none() {
        load_agent_plugin_manifest(&root.to_string_lossy())?;
        return Ok(ConvertedExtension {
            path: root.to_path_buf(),
            origin: "AgentPlugins".to_owned(),
            external_content: false,
            is_agent_plugin: true,
            cleanup_paths: Vec::new(),
            warnings: Vec::new(),
        });
    }
    if let Some(plugin_name) = plugin_name {
        let converted = match convert_claude_plugin_package(root, plugin_name) {
            Ok(converted) => converted,
            Err(error) if error.to_string().contains("requires host acquisition") => {
                return convert_remote_claude_plugin(root, plugin_name, args).await;
            }
            Err(error) => return Err(error.to_string()),
        };
        return Ok(ConvertedExtension {
            path: converted.converted_dir,
            origin: "Claude".to_owned(),
            external_content: converted.external_content,
            is_agent_plugin: false,
            cleanup_paths: Vec::new(),
            warnings: converted.warnings,
        });
    }
    if root.join(INSTALL_MANIFEST_FILE).is_file() {
        return Ok(ConvertedExtension {
            path: root.to_path_buf(),
            origin: "QwenCode".to_owned(),
            external_content: false,
            is_agent_plugin: false,
            cleanup_paths: Vec::new(),
            warnings: Vec::new(),
        });
    }
    if is_gemini_extension_config(root).map_err(|error| error.to_string())? {
        let converted =
            convert_gemini_extension_package(root).map_err(|error| error.to_string())?;
        return Ok(ConvertedExtension {
            path: converted.converted_dir,
            origin: "Gemini".to_owned(),
            external_content: false,
            is_agent_plugin: false,
            cleanup_paths: Vec::new(),
            warnings: converted.warnings,
        });
    }
    if root.join(QODER_PLUGIN_MANIFEST).is_file() {
        let converted_root = create_temporary_directory("extension-qoder")?;
        let result = (|| {
            copy_tree(root, &converted_root, false)?;
            let input = load_qoder_plugin_input(root).map_err(|error| error.to_string())?;
            let mut config = input.config;
            if let Some(object) = config.as_object_mut() {
                if let Some(context_files) = input.context_file_names {
                    object.insert(
                        "contextFileName".to_owned(),
                        Value::Array(context_files.into_iter().map(Value::String).collect()),
                    );
                } else {
                    object.remove("contextFileName");
                }
            }
            fs::write(
                converted_root.join(INSTALL_MANIFEST_FILE),
                serde_json::to_vec_pretty(&config).map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
            Ok::<_, String>(ConvertedExtension {
                path: converted_root.clone(),
                origin: "Qoder".to_owned(),
                external_content: false,
                is_agent_plugin: false,
                cleanup_paths: vec![converted_root.clone()],
                warnings: Vec::new(),
            })
        })();
        return remove_temp_on_error(&converted_root, result);
    }
    if root.join(".claude-plugin/plugin.json").is_file() {
        let converted =
            convert_claude_plugin_standalone(root).map_err(|error| error.to_string())?;
        return Ok(ConvertedExtension {
            path: converted.converted_dir,
            origin: "Claude".to_owned(),
            external_content: false,
            is_agent_plugin: false,
            cleanup_paths: Vec::new(),
            warnings: converted.warnings,
        });
    }
    Err("Configuration file not found: canopy-extension.json".to_owned())
}

async fn convert_remote_claude_plugin(
    marketplace_root: &Path,
    plugin_name: &str,
    args: &InstallArgs,
) -> Result<ConvertedExtension, String> {
    let marketplace_path = marketplace_root.join(".claude-plugin/marketplace.json");
    let marketplace: Value =
        serde_json::from_slice(&fs::read(&marketplace_path).map_err(|error| {
            format!("Could not read Claude marketplace configuration: {error}")
        })?)
        .map_err(|error| format!("Could not parse Claude marketplace configuration: {error}"))?;
    let plugin = marketplace
        .get("plugins")
        .and_then(Value::as_array)
        .and_then(|plugins| {
            plugins
                .iter()
                .find(|plugin| plugin.get("name").and_then(Value::as_str) == Some(plugin_name))
        })
        .cloned()
        .ok_or_else(|| format!("plugin {plugin_name} not found in marketplace.json"))?;
    let source = plugin
        .get("source")
        .ok_or_else(|| format!("Plugin {plugin_name} has no marketplace source."))?;

    let (url, reference, subdirectory) = match source {
        Value::String(source)
            if source.starts_with("http://") || source.starts_with("https://") =>
        {
            (source.clone(), None, None)
        }
        Value::Object(source) => match source.get("source").and_then(Value::as_str) {
            Some("github") => {
                let repo = source
                    .get("repo")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Claude GitHub plugin source has no repo.".to_owned())?;
                (format!("https://github.com/{repo}"), None, None)
            }
            Some("url") => {
                let url = source
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Claude URL plugin source has no URL.".to_owned())?;
                (url.to_owned(), None, None)
            }
            Some("git-subdir") => {
                let url = source
                    .get("url")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Claude git-subdir plugin source has no URL.".to_owned())?;
                let path = source
                    .get("path")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Claude git-subdir plugin source has no path.".to_owned())?;
                let reference = source
                    .get("sha")
                    .and_then(Value::as_str)
                    .or_else(|| source.get("ref").and_then(Value::as_str))
                    .map(str::to_owned);
                (url.to_owned(), reference, Some(path.to_owned()))
            }
            _ => return Err(format!("Unsupported Claude plugin source: {source:?}")),
        },
        _ => return Err(format!("Unsupported Claude plugin source: {source}")),
    };

    let mut acquisition_args = args.clone();
    acquisition_args.reference = reference;
    let remote_root = if let Ok(repository) = parse_github_repo_for_releases(&url) {
        match acquire_github_release(
            &repository.owner,
            &repository.repo,
            acquisition_args.reference.as_deref(),
            &acquisition_args,
            acquisition_args.network_policy,
        )
        .await
        {
            Ok(Some((path, _))) => path,
            _ => {
                let root = create_temporary_directory("claude-plugin-source")?;
                if let Err(error) = clone_git(
                    &url,
                    acquisition_args.reference.as_deref(),
                    &root,
                    acquisition_args.network_policy,
                )
                .await
                {
                    let _ = fs::remove_dir_all(&root);
                    return Err(error);
                }
                root
            }
        }
    } else {
        let root = create_temporary_directory("claude-plugin-source")?;
        if let Err(error) = clone_git(
            &url,
            acquisition_args.reference.as_deref(),
            &root,
            acquisition_args.network_policy,
        )
        .await
        {
            let _ = fs::remove_dir_all(&root);
            return Err(error);
        }
        root
    };
    let result = (|| {
        let plugin_root = if let Some(subdirectory) = subdirectory {
            let relative = Path::new(&subdirectory);
            if relative.is_absolute()
                || relative.components().any(|component| {
                    matches!(
                        component,
                        Component::ParentDir | Component::RootDir | Component::Prefix(_)
                    )
                })
            {
                let _ = fs::remove_dir_all(&remote_root);
                return Err(format!("Invalid plugin subdirectory \"{subdirectory}\"."));
            }
            let requested = remote_root.join(relative);
            let real_root = fs::canonicalize(&remote_root).map_err(|error| error.to_string())?;
            let real_plugin = fs::canonicalize(&requested).map_err(|_| {
                format!("Plugin subdirectory \"{subdirectory}\" not found in repository.")
            })?;
            if !real_plugin.starts_with(&real_root) || !real_plugin.is_dir() {
                let _ = fs::remove_dir_all(&remote_root);
                return Err(format!(
                    "Plugin subdirectory \"{subdirectory}\" escapes its repository."
                ));
            }
            real_plugin
        } else {
            remote_root.clone()
        };

        let plugin_manifest = plugin_root.join(".claude-plugin/plugin.json");
        let strict = plugin
            .get("strict")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let plugin_config = match fs::read(&plugin_manifest) {
            Ok(bytes) => {
                let real_root =
                    fs::canonicalize(&plugin_root).map_err(|error| error.to_string())?;
                let real_manifest =
                    fs::canonicalize(&plugin_manifest).map_err(|error| error.to_string())?;
                if !real_manifest.starts_with(&real_root) {
                    let _ = fs::remove_dir_all(&remote_root);
                    return Err(format!(
                        "Plugin config {} resolves outside the plugin root.",
                        plugin_manifest.display()
                    ));
                }
                Some(serde_json::from_slice::<Value>(&bytes).map_err(|error| error.to_string())?)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && !strict => None,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let _ = fs::remove_dir_all(&remote_root);
                return Err(format!(
                    "Strict mode requires plugin.json at {}.",
                    plugin_manifest.display()
                ));
            }
            Err(error) => return Err(format!("Could not read Claude plugin config: {error}")),
        };
        let merged = merge_claude_configs(&plugin, plugin_config.as_ref());
        let converted = match build_canopy_extension_from_plugin_with_external_content(
            &plugin_root,
            merged,
            Vec::new(),
            true,
        ) {
            Ok(converted) => converted,
            Err(error) => {
                let _ = fs::remove_dir_all(&remote_root);
                return Err(error.to_string());
            }
        };
        Ok(ConvertedExtension {
            path: converted.converted_dir,
            origin: "Claude".to_owned(),
            external_content: true,
            is_agent_plugin: false,
            cleanup_paths: vec![remote_root.clone()],
            warnings: converted.warnings,
        })
    })();
    remove_temp_on_error(&remote_root, result)
}

fn load_extension_config(root: &Path, workspace: &Path) -> Result<ExtensionConfig, String> {
    if get_agent_plugin_schema_status(&root.to_string_lossy()) == AgentPluginSchemaStatus::Supported
    {
        let manifest = load_agent_plugin_manifest(&root.to_string_lossy())?;
        return Ok(ExtensionConfig {
            name: manifest.name,
            display_name: Some(manifest.display_name),
            description: manifest.description,
            mcp_servers: Map::new(),
            settings: Vec::new(),
        });
    }
    let manifest_path = root.join(INSTALL_MANIFEST_FILE);
    let bytes = fs::read(&manifest_path)
        .map_err(|error| format!("Could not read extension manifest: {error}"))?;
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
    let object = hydrated
        .as_object()
        .ok_or_else(|| "Extension config must be a JSON object.".to_owned())?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            format!(
                "Invalid configuration in {}: missing \"name\"",
                manifest_path.display()
            )
        })?
        .to_owned();
    let mcp_servers = object
        .get("mcpServers")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let settings = object
        .get("settings")
        .cloned()
        .map(serde_json::from_value::<Vec<ExtensionSetting>>)
        .transpose()
        .map_err(|error| format!("Invalid extension settings: {error}"))?
        .unwrap_or_default();
    Ok(ExtensionConfig {
        name,
        display_name: object
            .get("displayName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        description: object
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        mcp_servers,
        settings,
    })
}

fn show_consent(
    config: &ExtensionConfig,
    converted: &ConvertedExtension,
    source_root: &Path,
    workspace: &Path,
    home: &Path,
    store_dir: &Path,
    args: &InstallArgs,
) -> Result<(), String> {
    if converted.origin != "QwenCode" && converted.origin != "AgentPlugins" {
        println!(
            "You are installing an extension from {}. Some features may not work perfectly with Canopy Code.",
            converted.origin
        );
    }
    println!(
        "Installing extension \"{}\".",
        config.display_name.as_deref().unwrap_or(&config.name)
    );
    if let Some(description) = config
        .description
        .as_deref()
        .filter(|description| !description.is_empty())
    {
        println!("{}", strip_terminal_controls(description));
    }
    println!(
        "Extensions may introduce unexpected behavior. Ensure you have investigated the extension source and trust the author."
    );
    if !config.mcp_servers.is_empty() {
        println!("This extension will run the following MCP servers:");
        for (name, server) in &config.mcp_servers {
            let is_local = server.get("command").is_some();
            let source = if let Some(command) = server.get("command").and_then(Value::as_str) {
                let args = server
                    .get("args")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>();
                if args.is_empty() {
                    command.to_owned()
                } else {
                    format!("{command} {}", args.join(" "))
                }
            } else {
                server
                    .get("httpUrl")
                    .or_else(|| server.get("url"))
                    .or_else(|| server.get("sseUrl"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            println!(
                "  * {name} ({}): {}",
                if is_local { "local" } else { "remote" },
                strip_terminal_controls(&source)
            );
        }
    }
    let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: workspace,
        user_home: home,
        user_extensions_dir: source_root.parent().unwrap_or(source_root),
        extension_store_dir: store_dir,
    });
    let canonical_source_root =
        fs::canonicalize(source_root).unwrap_or_else(|_| source_root.to_path_buf());
    if let Some(extension) = inventory.extensions.iter().find(|extension| {
        extension.install_slot == canonical_source_root && extension.name == config.name
    }) {
        if !extension.commands.is_empty() {
            println!(
                "This extension will add the following commands: {}.",
                strip_terminal_controls(&extension.commands.join(", "))
            );
        }
        if !extension.skills.is_empty() {
            println!(
                "This extension will install the following skills: {}.",
                strip_terminal_controls(&extension.skills.join(", "))
            );
        }
        if !extension.agents.is_empty() {
            println!(
                "This extension will install the following subagents: {}.",
                strip_terminal_controls(&extension.agents.join(", "))
            );
        }
        if !extension.context_files.is_empty() {
            println!(
                "This extension will append info to your CANOPY.md context using {}.",
                extension
                    .context_files
                    .iter()
                    .map(|path| strip_terminal_controls(&path.display().to_string()))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
    if args.consent {
        return Ok(());
    }
    print!("Do you want to continue? [Y/n]: ");
    io::stdout().flush().map_err(|error| error.to_string())?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|error| error.to_string())?;
    if matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "" | "y" | "yes"
    ) {
        Ok(())
    } else {
        Err(format!("Installation cancelled for \"{}\".", config.name))
    }
}

pub(crate) fn prompt_extension_settings(
    settings: &[ExtensionSetting],
) -> Result<Vec<(String, String)>, String> {
    let mut values = Vec::new();
    for setting in settings {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            return Err(format!(
                "Extension setting \"{}\" requires an interactive terminal.",
                strip_terminal_control_sequences(&setting.name)
            ));
        }
        print!(
            "{}\n{}\n{}: ",
            strip_terminal_control_sequences(&setting.name),
            strip_terminal_control_sequences(&setting.description),
            if setting.is_sensitive() {
                "Value (input hidden)"
            } else {
                "Value"
            }
        );
        io::stdout().flush().map_err(|error| error.to_string())?;
        let value = if setting.is_sensitive() {
            read_hidden_line()?
        } else {
            let mut value = String::new();
            io::stdin()
                .read_line(&mut value)
                .map_err(|error| error.to_string())?;
            value.trim_end_matches(['\r', '\n']).to_owned()
        };
        values.push((setting.env_var.clone(), value));
    }
    Ok(values)
}

fn read_hidden_line() -> Result<String, String> {
    use crossterm::event::{Event, KeyCode, KeyEventKind, read};
    use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
    enable_raw_mode().map_err(|error| format!("Could not enable hidden setting input: {error}"))?;
    let mut value = String::new();
    let result = (|| -> Result<(), String> {
        loop {
            match read().map_err(|error| error.to_string())? {
                Event::Key(event) if event.kind == KeyEventKind::Press => match event.code {
                    KeyCode::Enter => break,
                    KeyCode::Char(character)
                        if !event
                            .modifiers
                            .contains(crossterm::event::KeyModifiers::CONTROL) =>
                    {
                        value.push(character)
                    }
                    KeyCode::Backspace => {
                        value.pop();
                    }
                    KeyCode::Esc => return Err("Setting input cancelled.".to_owned()),
                    _ => {}
                },
                _ => {}
            }
        }
        Ok(())
    })();
    let restore =
        disable_raw_mode().map_err(|error| format!("Could not restore terminal: {error}"));
    println!();
    result?;
    restore?;
    Ok(value)
}

async fn save_extension_settings(
    settings: &[ExtensionSetting],
    values: &[(String, String)],
    name: &str,
    identity: &str,
    global_config: &Path,
    staging: &Path,
) -> Result<Option<StoredSecretState>, String> {
    if settings.is_empty() {
        let selector = staging.join(SETTINGS_SELECTOR_FILE);
        if fs::symlink_metadata(&selector).is_ok() {
            fs::remove_file(selector)
                .map_err(|error| format!("Could not clear extension settings selector: {error}"))?;
        }
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
    let env_path = staging.join(".env");
    atomic_write_file(
        &env_path,
        env.as_bytes(),
        &AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        },
    )
    .map_err(|error| format!("Could not write extension settings: {error}"))?;
    if secret_values.is_empty() {
        let selector = staging.join(SETTINGS_SELECTOR_FILE);
        if fs::symlink_metadata(&selector).is_ok() {
            fs::remove_file(selector)
                .map_err(|error| format!("Could not clear extension settings selector: {error}"))?;
        }
        return Ok(None);
    }

    let service_name = format!("Canopy Code Extensions {name} {identity}");
    let (backend, backend_name) = SecretBackend::select(global_config, &service_name).await;
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
        let selector_path = staging.join(SETTINGS_SELECTOR_FILE);
        let selector = serde_json::json!({
            "version": 1,
            "backend": backend_name,
            "bundleKey": bundle_key,
        });
        let bytes = serde_json::to_vec_pretty(&selector).map_err(|error| error.to_string())?;
        atomic_write_file(
            &selector_path,
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
    Ok(Some(StoredSecretState {
        backend,
        keys: stored_keys,
    }))
}

fn metadata_file(staging: &Path, metadata: &InstallMetadata) -> Result<(), String> {
    let path = staging.join(INSTALL_METADATA_FILE);
    let bytes = serde_json::to_vec_pretty(metadata).map_err(|error| error.to_string())?;
    atomic_write_file(&path, &bytes, &AtomicWriteOptions::default())
        .map_err(|error| format!("Could not write extension install metadata: {error}"))
}

fn extension_id(metadata: &InstallMetadata, name: &str) -> String {
    let mut source = metadata.source.clone();
    if matches!(metadata.install_type.as_str(), "git" | "github-release") {
        if let Ok(repository) = parse_github_repo_for_releases(&source) {
            source = format!(
                "https://github.com/{}/{}",
                repository.owner, repository.repo
            );
        }
    }
    if let Some(plugin_name) = &metadata.plugin_name {
        source.push(':');
        source.push_str(plugin_name);
    }
    if source.is_empty() {
        source = name.to_owned();
    }
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err(format!(
            "Invalid extension name: \"{name}\". Only letters, numbers, underscores, dots, and dashes are allowed."
        ));
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path, skip_symlinks: bool) -> Result<(), String> {
    let source_root = fs::canonicalize(source).map_err(|error| error.to_string())?;
    let mut active_directories = HashSet::new();
    copy_tree_inner(
        &source_root,
        destination,
        &source_root,
        skip_symlinks,
        &mut active_directories,
    )
}

fn copy_tree_inner(
    source: &Path,
    destination: &Path,
    source_root: &Path,
    skip_symlinks: bool,
    active_directories: &mut HashSet<PathBuf>,
) -> Result<(), String> {
    let real_source = fs::canonicalize(source).map_err(|error| error.to_string())?;
    if !real_source.starts_with(source_root) || !active_directories.insert(real_source.clone()) {
        return Ok(());
    }
    fs::create_dir_all(destination).map_err(|error| error.to_string())?;
    let result = (|| -> Result<(), String> {
        for entry in fs::read_dir(source).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let source_path = entry.path();
            let target_path = destination.join(entry.file_name());
            let file_type = entry.file_type().map_err(|error| error.to_string())?;
            if file_type.is_symlink() {
                if skip_symlinks {
                    continue;
                }
                let Ok(real_target) = fs::canonicalize(&source_path) else {
                    continue;
                };
                if !real_target.starts_with(source_root) {
                    continue;
                }
                let metadata = fs::metadata(&real_target).map_err(|error| error.to_string())?;
                if metadata.is_dir() {
                    copy_tree_inner(
                        &real_target,
                        &target_path,
                        source_root,
                        false,
                        active_directories,
                    )?;
                } else if metadata.is_file() {
                    fs::copy(real_target, target_path).map_err(|error| error.to_string())?;
                }
            } else if file_type.is_dir() {
                copy_tree_inner(
                    &source_path,
                    &target_path,
                    source_root,
                    skip_symlinks,
                    active_directories,
                )?;
            } else if file_type.is_file() {
                let real_file =
                    fs::canonicalize(&source_path).map_err(|error| error.to_string())?;
                if real_file.starts_with(source_root) {
                    fs::copy(real_file, target_path).map_err(|error| error.to_string())?;
                }
            }
        }
        Ok(())
    })();
    active_directories.remove(&real_source);
    result
}

fn strip_terminal_controls(value: &str) -> String {
    strip_terminal_control_sequences(value)
}

fn refuse_incomplete_inventory(diagnostics: &[String], operation: &str) -> Result<(), String> {
    if diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("More than 128 installed extensions")
            || diagnostic.starts_with("Extension directory contains more than 256 entries")
    }) {
        return Err(format!(
            "The installed-extension scan was truncated; refusing to {operation} from an incomplete inventory."
        ));
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
}
