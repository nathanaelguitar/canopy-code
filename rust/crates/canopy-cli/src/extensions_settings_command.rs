// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! List and update extension settings without revealing sensitive values.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use canopy_core::config::{LoadSettingsOptions, load_settings};
use canopy_core::extension_activation::ExtensionActivation;
use canopy_core::extension_inventory::{
    InstalledExtensionList, InstalledExtensionListOptions, InstalledLocalExtension,
    load_installed_local_extensions,
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
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use tokio::runtime::Builder;

const USAGE: &str = "Usage: canopy extensions settings list <name>\n       canopy extensions settings set [--scope user|workspace] <name> <setting>";
const SETTINGS_SELECTOR_FILE: &str = ".canopy-extension-settings.json";
const SETTINGS_BUNDLE_PREFIX: &str = "$canopy:extension-settings:v2:";
const WORKSPACE_ENV_FILE: &str = ".env";
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_SETTINGS_FILE_BYTES: u64 = 1024 * 1024;
const MAX_SELECTOR_BYTES: u64 = 64 * 1024;
const MAX_SETTINGS_BUNDLE_BYTES: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionManifest {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    settings: Vec<ExtensionSetting>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SettingsSelector {
    version: u32,
    backend: String,
    #[serde(rename = "bundleKey")]
    bundle_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettingsScope {
    User,
    Workspace,
}

enum SettingsAction {
    List {
        name: String,
    },
    Set {
        name: String,
        setting: String,
        scope: SettingsScope,
    },
}

struct SettingsContext {
    workspace: PathBuf,
    home: PathBuf,
    extensions_dir: PathBuf,
    store_dir: PathBuf,
    global_config_dir: PathBuf,
}

enum SettingsSecretBackend {
    Encrypted(EncryptedFileTokenStorage),
    #[cfg(target_os = "macos")]
    Keychain(KeychainTokenStorage),
}

impl SettingsSecretBackend {
    fn selected(
        backend: &str,
        global_config_dir: &Path,
        service_name: &str,
    ) -> Result<Self, String> {
        match backend {
            "encrypted_file" => Ok(Self::Encrypted(EncryptedFileTokenStorage::new(
                global_config_dir,
                service_name,
            ))),
            "keychain" => {
                #[cfg(target_os = "macos")]
                {
                    Ok(Self::Keychain(KeychainTokenStorage::new(service_name)))
                }
                #[cfg(not(target_os = "macos"))]
                {
                    Err("Extension settings are stored in the macOS keychain, which is unavailable on this platform.".to_owned())
                }
            }
            _ => Err("Stored extension settings selector has an unsupported backend.".to_owned()),
        }
    }

    async fn default_for(global_config_dir: &Path, service_name: &str) -> Self {
        let force_file =
            std::env::var("CANOPY_CODE_FORCE_FILE_STORAGE").is_ok_and(|value| value == "true");
        #[cfg(target_os = "macos")]
        if !force_file {
            let keychain = KeychainTokenStorage::new(service_name);
            if keychain.is_available().await.unwrap_or(false) {
                return Self::Keychain(keychain);
            }
        }
        let _ = force_file;
        Self::Encrypted(EncryptedFileTokenStorage::new(
            global_config_dir,
            service_name,
        ))
    }

    async fn get_secret(&self, key: &str) -> Result<Option<String>, String> {
        match self {
            Self::Encrypted(storage) => storage
                .get_secret(key)
                .await
                .map_err(|error| error.to_string()),
            #[cfg(target_os = "macos")]
            Self::Keychain(storage) => storage
                .get_secret(key)
                .await
                .map_err(|error| error.to_string()),
        }
    }

    async fn set_secret(&self, key: &str, value: &str) -> Result<(), String> {
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
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        print_help();
        return Ok(());
    }
    let action = parse_action(args)?;
    let name = match &action {
        SettingsAction::List { name } | SettingsAction::Set { name, .. } => name,
    };

    let context = load_context()?;
    let inventory = load_inventory(&context);
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {}", sanitize(diagnostic));
    }
    if inventory.extensions.is_empty() {
        return Ok(());
    }
    let Some(extension) = inventory.extensions.iter().find(|extension| {
        extension.name == *name
            && extension
                .workspace_activation
                .as_ref()
                .is_some_and(|activation| activation.effective == ExtensionActivation::Enabled)
    }) else {
        println!("Extension \"{}\" not found.", sanitize(name));
        return Ok(());
    };
    let extension = extension.clone();
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Could not start extension settings runtime: {error}"))?;
    match action {
        SettingsAction::List { .. } => runtime.block_on(list_settings(&extension, &context)),
        SettingsAction::Set { setting, scope, .. } => {
            runtime.block_on(set_setting(&extension, &context, &setting, scope))
        }
    }
}

fn parse_action(args: &[String]) -> Result<SettingsAction, String> {
    match args.first().map(String::as_str) {
        Some("list") if args.len() == 2 && !args[1].starts_with('-') => Ok(SettingsAction::List {
            name: args[1].clone(),
        }),
        Some("set") => parse_set_action(&args[1..]),
        None => Err(format!("{USAGE}\nSpecify `list` or `set`.")),
        _ => Err(format!(
            "{USAGE}\nSpecify `list <name>` or `set <name> <setting>`."
        )),
    }
}

fn parse_set_action(args: &[String]) -> Result<SettingsAction, String> {
    let mut positional = Vec::with_capacity(2);
    let mut scope = SettingsScope::User;
    let mut scope_seen = false;
    let mut index = 0;
    while index < args.len() {
        let argument = &args[index];
        if argument == "--scope" {
            if scope_seen {
                return Err("`--scope` may be specified only once.".to_owned());
            }
            index += 1;
            let value = args
                .get(index)
                .ok_or_else(|| "`--scope` requires `user` or `workspace`.".to_owned())?;
            scope = parse_scope(value)?;
            scope_seen = true;
        } else if let Some(value) = argument.strip_prefix("--scope=") {
            if scope_seen {
                return Err("`--scope` may be specified only once.".to_owned());
            }
            scope = parse_scope(value)?;
            scope_seen = true;
        } else if argument.starts_with('-') {
            return Err(format!(
                "Unknown extension settings option: {}",
                sanitize(argument)
            ));
        } else {
            positional.push(argument.clone());
        }
        index += 1;
    }
    let [name, setting] = positional.as_slice() else {
        return Err(format!(
            "{USAGE}\nThe `set` command needs an extension name and setting."
        ));
    };
    Ok(SettingsAction::Set {
        name: name.clone(),
        setting: setting.clone(),
        scope,
    })
}

fn parse_scope(scope: &str) -> Result<SettingsScope, String> {
    match scope {
        "user" => Ok(SettingsScope::User),
        "workspace" => Ok(SettingsScope::Workspace),
        _ => Err("`--scope` must be `user` or `workspace`.".to_owned()),
    }
}

fn load_context() -> Result<SettingsContext, String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let settings = load_settings(workspace.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    if !settings.is_trusted {
        return Err(format!(
            "Could not load extension settings from untrusted folder at {}",
            sanitize(&workspace.to_string_lossy())
        ));
    }
    let extensions_dir = Storage::get_user_extensions_dir();
    let global_config_dir = Storage::get_global_canopy_dir();
    Ok(SettingsContext {
        workspace,
        home,
        extensions_dir,
        store_dir: global_config_dir.join("extension-store"),
        global_config_dir,
    })
}

fn load_inventory(context: &SettingsContext) -> InstalledExtensionList {
    load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &context.workspace,
        user_home: &context.home,
        user_extensions_dir: &context.extensions_dir,
        extension_store_dir: &context.store_dir,
    })
}

async fn list_settings(
    extension: &InstalledLocalExtension,
    context: &SettingsContext,
) -> Result<(), String> {
    let settings = read_extension_settings(extension)?;
    if settings.is_empty() {
        println!(
            "Extension \"{}\" has no settings to configure.",
            sanitize(&extension.name)
        );
        return Ok(());
    }

    let user_service = format!("Canopy Code Extensions {} {}", extension.name, extension.id);
    let workspace_service = format!("{user_service} {}", context.workspace.display());
    let user_selector = read_settings_selector(&extension.install_slot)?;
    let user_settings = read_scoped_settings(
        &settings,
        &extension.install_slot.join(".env"),
        &user_service,
        user_selector.as_ref(),
        context,
    )
    .await?;
    let workspace_settings = read_scoped_settings(
        &settings,
        &context.workspace.join(WORKSPACE_ENV_FILE),
        &workspace_service,
        None,
        context,
    )
    .await?;

    println!("Settings for \"{}\":", sanitize(&extension.name));
    for setting in &settings {
        let workspace_value = workspace_settings.get(&setting.env_var);
        let user_value = user_settings.get(&setting.env_var);
        let (value, scope) = if let Some(value) = workspace_value {
            (Some(value), " (workspace)")
        } else if let Some(value) = user_value {
            (Some(value), " (user)")
        } else {
            (None, "")
        };
        let display_value = match value {
            None => "[not set]".to_owned(),
            Some(_) if setting.is_sensitive() => "[value stored in keychain]".to_owned(),
            Some(value) => sanitize(value),
        };
        println!(
            "\n- {} ({})",
            sanitize(&setting.name),
            sanitize(&setting.env_var)
        );
        println!("  Description: {}", sanitize(&setting.description));
        println!("  Value: {display_value}{scope}");
    }
    Ok(())
}

async fn set_setting(
    extension: &InstalledLocalExtension,
    context: &SettingsContext,
    setting_key: &str,
    scope: SettingsScope,
) -> Result<(), String> {
    let settings = read_extension_settings(extension)?;
    let Some(setting) = settings
        .iter()
        .find(|setting| setting.name == setting_key || setting.env_var == setting_key)
    else {
        return Ok(());
    };
    let value = prompt_setting(setting)?;

    if setting.is_sensitive() {
        set_sensitive_setting(extension, context, setting, &value, scope).await
    } else {
        set_environment_setting(extension, context, &settings, setting, &value, scope)
    }
}

fn prompt_setting(setting: &ExtensionSetting) -> Result<String, String> {
    let prompted = super::extensions_install_command::prompt_extension_settings(
        std::slice::from_ref(setting),
    )?;
    prompted
        .into_iter()
        .next()
        .map(|(_, value)| value)
        .ok_or_else(|| "Extension setting prompt did not return a value.".to_owned())
}

async fn set_sensitive_setting(
    extension: &InstalledLocalExtension,
    context: &SettingsContext,
    setting: &ExtensionSetting,
    value: &str,
    scope: SettingsScope,
) -> Result<(), String> {
    let service_name = format!("Canopy Code Extensions {} {}", extension.name, extension.id);
    let scoped_service = match scope {
        SettingsScope::User => service_name.clone(),
        SettingsScope::Workspace => format!("{service_name} {}", context.workspace.display()),
    };
    let selector = if scope == SettingsScope::User {
        read_settings_selector(&extension.install_slot)?
    } else {
        None
    };

    if let Some(selector) = selector {
        let selected = SettingsSecretBackend::selected(
            &selector.backend,
            &context.global_config_dir,
            &scoped_service,
        )?;
        selected
            .set_secret(
                &format!("{}:override:{}", selector.bundle_key, setting.env_var),
                value,
            )
            .await?;

        let legacy =
            SettingsSecretBackend::default_for(&context.global_config_dir, &service_name).await;
        if legacy.set_secret(&setting.env_var, value).await.is_err() {
            eprintln!(
                "[CANOPY] Could not synchronize legacy extension setting {}; saved setting remains active.",
                sanitize(&setting.env_var)
            );
        }
    } else {
        let storage =
            SettingsSecretBackend::default_for(&context.global_config_dir, &scoped_service).await;
        storage.set_secret(&setting.env_var, value).await?;
    }
    Ok(())
}

fn set_environment_setting(
    extension: &InstalledLocalExtension,
    context: &SettingsContext,
    settings: &[ExtensionSetting],
    setting: &ExtensionSetting,
    value: &str,
    scope: SettingsScope,
) -> Result<(), String> {
    let env_path = match scope {
        SettingsScope::User => extension.install_slot.join(".env"),
        SettingsScope::Workspace => context.workspace.join(WORKSPACE_ENV_FILE),
    };
    let mut values = read_env_settings(&env_path)?;
    values.insert(setting.env_var.clone(), value.to_owned());
    for sensitive_env_var in settings
        .iter()
        .filter(|setting| setting.is_sensitive())
        .map(|setting| &setting.env_var)
    {
        values.shift_remove(sensitive_env_var);
    }
    let entries = values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    let content = format_env_content(&entries);
    atomic_write_file(
        env_path,
        content.as_bytes(),
        &AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        },
    )
    .map_err(|error| {
        format!(
            "Could not write extension settings: {}",
            sanitize(&error.to_string())
        )
    })
}

fn read_extension_settings(
    extension: &InstalledLocalExtension,
) -> Result<Vec<ExtensionSetting>, String> {
    let manifest_path = extension.path.join("canopy-extension.json");
    let Some(bytes) = read_bounded_regular_file(&manifest_path, MAX_MANIFEST_BYTES)? else {
        return Ok(Vec::new());
    };
    let manifest: ExtensionManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("Could not parse extension settings manifest: {error}"))?;
    if manifest
        .name
        .as_deref()
        .is_some_and(|name| name != extension.name)
    {
        return Err(
            "Extension settings manifest name does not match the installed extension.".to_owned(),
        );
    }
    validate_extension_setting_env_vars(Some(&manifest.settings))?;
    Ok(manifest.settings)
}

async fn read_scoped_settings(
    settings: &[ExtensionSetting],
    env_path: &Path,
    service_name: &str,
    selector: Option<&SettingsSelector>,
    context: &SettingsContext,
) -> Result<IndexMap<String, String>, String> {
    let mut values = read_env_settings(env_path)?;
    if settings.iter().all(|setting| !setting.is_sensitive()) {
        return Ok(values);
    }

    let storage = match selector {
        Some(selector) => SettingsSecretBackend::selected(
            &selector.backend,
            &context.global_config_dir,
            service_name,
        )?,
        None => SettingsSecretBackend::default_for(&context.global_config_dir, service_name).await,
    };
    let sensitive_settings = settings
        .iter()
        .filter(|setting| setting.is_sensitive())
        .collect::<Vec<_>>();
    let bundle = if let Some(selector) = selector {
        let bundle_content = storage
            .get_secret(&selector.bundle_key)
            .await?
            .ok_or_else(|| "Stored extension settings bundle is missing.".to_owned())?;
        if bundle_content.len() > MAX_SETTINGS_BUNDLE_BYTES {
            return Err("Stored extension settings bundle exceeds the 1 MiB limit.".to_owned());
        }
        let parsed: Value = serde_json::from_str(&bundle_content)
            .map_err(|_| "Stored extension settings bundle is invalid.".to_owned())?;
        let object = parsed
            .as_object()
            .ok_or_else(|| "Stored extension settings bundle is invalid.".to_owned())?;
        if object.values().any(|value| !value.is_string()) {
            return Err("Stored extension settings bundle is invalid.".to_owned());
        }
        Some(object.clone())
    } else {
        None
    };

    for setting in sensitive_settings {
        let secret = if let Some(selector) = selector {
            let override_value = storage
                .get_secret(&format!(
                    "{}:override:{}",
                    selector.bundle_key, setting.env_var
                ))
                .await?;
            match override_value {
                Some(value) => Some(value),
                None => bundle
                    .as_ref()
                    .and_then(|bundle| bundle.get(&setting.env_var))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }
        } else {
            storage.get_secret(&setting.env_var).await?
        };
        if let Some(secret) = secret.filter(|secret| !secret.is_empty()) {
            values.insert(setting.env_var.clone(), secret);
        }
    }
    Ok(values)
}

fn read_settings_selector(extension_slot: &Path) -> Result<Option<SettingsSelector>, String> {
    let path = extension_slot.join(SETTINGS_SELECTOR_FILE);
    let Some(bytes) = read_bounded_regular_file(&path, MAX_SELECTOR_BYTES)? else {
        return Ok(None);
    };
    let selector: SettingsSelector = serde_json::from_slice(&bytes)
        .map_err(|_| "Stored extension settings selector is invalid.".to_owned())?;
    if selector.version != 1 || !selector.bundle_key.starts_with(SETTINGS_BUNDLE_PREFIX) {
        return Err("Stored extension settings selector is invalid.".to_owned());
    }
    if !matches!(selector.backend.as_str(), "keychain" | "encrypted_file") {
        return Err("Stored extension settings selector is invalid.".to_owned());
    }
    Ok(Some(selector))
}

fn read_env_settings(path: &Path) -> Result<IndexMap<String, String>, String> {
    let Some(bytes) = read_bounded_regular_file(path, MAX_SETTINGS_FILE_BYTES)? else {
        return Ok(IndexMap::new());
    };
    let content = std::str::from_utf8(&bytes).map_err(|_| {
        format!(
            "Extension settings file is not valid UTF-8: {}",
            sanitize(&path.display().to_string())
        )
    })?;
    Ok(parse_env_settings(content))
}

fn parse_env_settings(content: &str) -> IndexMap<String, String> {
    static LINE_PATTERN: OnceLock<Regex> = OnceLock::new();
    let line_pattern = LINE_PATTERN.get_or_init(|| {
        // Match dotenv 17's LINE grammar. Rust's regex syntax has no backrefs,
        // so the quoted alternatives enforce their own matching delimiters.
        let whitespace = r"[\t\n\x0B\x0C\r \x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}\x{FEFF}]";
        let pattern = format!(
            r#"(?m)^{whitespace}*(?:export{whitespace}+)?(?P<key>[A-Za-z0-9_.-]+)(?:{whitespace}*={whitespace}*?|:{whitespace}+?)(?P<value>{whitespace}*'(?:\\'|[^'])*'|{whitespace}*"(?:\\"|[^"])*"|{whitespace}*`(?:\\`|[^`])*`|[^#\r\n]+)?{whitespace}*(?:#.*)?$"#
        );
        Regex::new(&pattern).expect("dotenv parser expression is valid")
    });
    let normalized = content.replace("\r\n", "\n").replace('\r', "\n");
    let mut values = IndexMap::new();
    for captures in line_pattern.captures_iter(&normalized) {
        let Some(key) = captures.name("key") else {
            continue;
        };
        let raw_value = captures.name("value").map_or("", |value| value.as_str());
        let mut value = raw_value.trim_matches(is_dotenv_whitespace);
        let opening_quote = value.chars().next();
        if let Some(quote @ ('\'' | '"' | '`')) = opening_quote
            && value.chars().last() == Some(quote)
            && value.len() >= 2
        {
            value = &value[quote.len_utf8()..value.len() - quote.len_utf8()];
        }
        let value = if opening_quote == Some('"') {
            value.replace("\\n", "\n").replace("\\r", "\r")
        } else {
            value.to_owned()
        };
        values.insert(key.as_str().to_owned(), value);
    }
    values
}

fn is_dotenv_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000A}'
            | '\u{000B}'
            | '\u{000C}'
            | '\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200A}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202F}'
                | '\u{205F}'
                | '\u{3000}'
                | '\u{FEFF}'
    )
}

fn read_bounded_regular_file(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, String> {
    let display_path = sanitize(&path.display().to_string());
    let path_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Could not inspect {display_path}: {}",
                sanitize(&error.to_string())
            ));
        }
    };
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(format!(
            "Settings file is not a regular file: {display_path}"
        ));
    }
    if path_metadata.len() > limit {
        return Err(format!(
            "Settings file exceeds the {limit}-byte limit: {display_path}"
        ));
    }
    let file = File::open(path).map_err(|error| {
        format!(
            "Could not read {display_path}: {}",
            sanitize(&error.to_string())
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        format!(
            "Could not inspect {display_path}: {}",
            sanitize(&error.to_string())
        )
    })?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(format!(
            "Settings file changed or exceeds the {limit}-byte limit: {display_path}"
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
            return Err(format!(
                "Settings file changed while being read: {display_path}"
            ));
        }
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            format!(
                "Could not read {display_path}: {}",
                sanitize(&error.to_string())
            )
        })?;
    if bytes.len() as u64 > limit {
        return Err(format!(
            "Settings file exceeds the {limit}-byte limit: {display_path}"
        ));
    }
    Ok(Some(bytes))
}

fn sanitize(value: &str) -> String {
    strip_terminal_control_sequences(value)
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

fn print_help() {
    println!("{USAGE}");
    println!("The default scope for `set` is `user`.");
}
