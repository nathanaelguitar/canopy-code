// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Native extension activation and uninstall commands.

use std::path::PathBuf;

use canopy_core::extension_activation::{ExtensionActivation, ExtensionIdentity};
use canopy_core::extension_activation_store::{
    ExtensionArtifactOperation, commit_extension_artifact, set_installed_extension_activation,
};
use canopy_core::extension_inventory::{
    InstalledExtensionListOptions, load_installed_local_extensions,
};
use canopy_core::extension_preferences::{ExtensionPreferencesStore, ExtensionScope};
use canopy_core::extension_source_projection::sanitize_display;
use canopy_core::storage::Storage;

const USAGE: &str = "Usage: canopy extensions <install|uninstall|enable|disable> ...\n  enable <name> [--scope user|workspace]\n  disable <name> [--scope user|workspace]\n  install <source>\n  uninstall <name-or-source>";

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

fn parse_activation_args(args: &[String]) -> Result<(String, ExtensionScope), String> {
    let mut name = None;
    let mut scope = ExtensionScope::User;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--scope" => {
                index += 1;
                let value = args.get(index).ok_or_else(|| {
                    format!("{USAGE}\n--scope requires either user or workspace.")
                })?;
                scope = parse_scope(value)?;
            }
            value if value.starts_with("--scope=") => {
                scope = parse_scope(&value[8..])?;
            }
            value if value.starts_with('-') => {
                return Err(format!("{USAGE}\nUnknown option: {value}"));
            }
            value => {
                if name.replace(value.to_owned()).is_some() {
                    return Err(format!("{USAGE}\nExpected one extension name."));
                }
            }
        }
        index += 1;
    }
    let name = name.ok_or_else(|| format!("{USAGE}\nAn extension name is required."))?;
    Ok((name, scope))
}

fn parse_scope(value: &str) -> Result<ExtensionScope, String> {
    match value.to_ascii_lowercase().as_str() {
        "user" => Ok(ExtensionScope::User),
        "workspace" => Ok(ExtensionScope::Project),
        _ => Err(format!(
            "Invalid scope: {value}. Please use one of user, workspace."
        )),
    }
}

fn change_activation(args: &[String], enabled: bool) -> Result<(), String> {
    let (name, scope) = parse_activation_args(args)?;
    let workspace_root = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let user_home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let extensions_dir = Storage::get_user_extensions_dir();
    let store_dir = Storage::get_global_canopy_dir().join("extension-store");
    let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &workspace_root,
        user_home: &user_home,
        user_extensions_dir: &extensions_dir,
        extension_store_dir: &store_dir,
    });
    if inventory.diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("More than 128 installed extensions")
            || diagnostic.starts_with("Extension directory contains more than 256 entries")
    }) {
        return Err(
            "The installed-extension scan was truncated; refusing to mutate activation state from an incomplete inventory."
                .to_owned(),
        );
    }
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {diagnostic}");
    }
    let extension = inventory
        .extensions
        .iter()
        .find(|extension| extension.name == name)
        .ok_or_else(|| format!("Extension with name {name} does not exist."))?;
    let identity = ExtensionIdentity {
        id: extension.id.clone(),
        name: extension.name.clone(),
    };
    let identities = inventory
        .extensions
        .iter()
        .map(|extension| ExtensionIdentity {
            id: extension.id.clone(),
            name: extension.name.clone(),
        })
        .collect::<Vec<_>>();
    set_installed_extension_activation(
        &extensions_dir,
        &store_dir,
        &identities,
        &identity,
        scope,
        &workspace_root,
        &user_home,
        if enabled {
            ExtensionActivation::Enabled
        } else {
            ExtensionActivation::Disabled
        },
    )?;
    let scope_label = match scope {
        ExtensionScope::User => "user",
        ExtensionScope::Project => "workspace",
    };
    println!(
        "Extension \"{}\" successfully {} for scope \"{scope_label}\".",
        sanitize_display(&name),
        if enabled { "enabled" } else { "disabled" }
    );
    Ok(())
}

fn uninstall(args: &[String]) -> Result<(), String> {
    let name_or_source = match args.get(1) {
        Some(value) if args.len() == 2 => value,
        _ => return Err(format!("{USAGE}\nExpected one extension name or source.")),
    };
    let workspace_root = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let user_home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let extensions_dir = Storage::get_user_extensions_dir();
    let store_dir = Storage::get_global_canopy_dir().join("extension-store");
    let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &workspace_root,
        user_home: &user_home,
        user_extensions_dir: &extensions_dir,
        extension_store_dir: &store_dir,
    });
    if inventory.diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("More than 128 installed extensions")
            || diagnostic.starts_with("Extension directory contains more than 256 entries")
    }) {
        return Err(
            "The installed-extension scan was truncated; refusing to uninstall from an incomplete inventory."
                .to_owned(),
        );
    }
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {diagnostic}");
    }
    let extension = inventory
        .extensions
        .iter()
        .find(|extension| {
            extension.name.eq_ignore_ascii_case(name_or_source)
                || extension
                    .source
                    .as_deref()
                    .is_some_and(|source| source.to_lowercase() == name_or_source.to_lowercase())
        })
        .ok_or_else(|| "Extension not found.".to_owned())?;
    let identity = ExtensionIdentity {
        id: extension.id.clone(),
        name: extension.name.clone(),
    };
    let identities = inventory
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
        ExtensionArtifactOperation::Uninstall,
        &identity,
        &extension.install_slot,
        None,
        None,
        None,
        &identities,
    )?;
    let preferences =
        ExtensionPreferencesStore::new(extensions_dir.join("extension-preferences.json"));
    if let Err(error) = preferences.clear(&identity.name) {
        eprintln!(
            "[CANOPY] Extension \"{}\" was uninstalled, but preference cleanup failed: {error}",
            identity.name
        );
    }
    println!(
        "Extension \"{}\" successfully uninstalled.",
        sanitize_display(name_or_source)
    );
    Ok(())
}

fn print_help() {
    println!("{USAGE}");
    println!("The Rust runtime supports enable, disable, and local uninstall mutations.");
}

pub fn handles(command: &str) -> bool {
    matches!(command, "uninstall" | "enable" | "disable")
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        print_help();
        return Ok(());
    }
    match args.first().map(String::as_str) {
        Some("enable") => change_activation(args, true),
        Some("disable") => change_activation(args, false),
        Some("uninstall") => uninstall(args),
        _ => Err(format!("{USAGE}\nUnknown extension command.")),
    }
}
