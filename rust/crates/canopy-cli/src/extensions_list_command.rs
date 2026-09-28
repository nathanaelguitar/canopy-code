// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Read-only listing of installed local extensions.

use std::path::PathBuf;

use canopy_core::extension_activation::{
    ActivationSource, ExtensionActivation, ExtensionActivationResult,
};
use canopy_core::extension_inventory::{
    InstalledExtensionListOptions, InstalledLocalExtension, load_installed_local_extensions,
};
use canopy_core::extension_source_projection::sanitize_display;
use canopy_core::extensions::redact_url_credentials;
use canopy_core::storage::Storage;

const USAGE: &str = "Usage: canopy extensions list";

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

fn activation_enabled(activation: Option<&ExtensionActivationResult>) -> Option<bool> {
    activation.map(|activation| activation.effective == ExtensionActivation::Enabled)
}

fn activation_source(activation: &ExtensionActivationResult) -> &'static str {
    match activation.source {
        ActivationSource::CliOverride => "CLI override",
        ActivationSource::WorkspaceOverride => "workspace override",
        ActivationSource::LegacyPathRule => "path rule",
        ActivationSource::Default => "default",
    }
}

fn activation_label(activation: Option<&ExtensionActivationResult>) -> String {
    match activation {
        Some(activation) => format!(
            "{} ({})",
            activation.effective == ExtensionActivation::Enabled,
            activation_source(activation)
        ),
        None => "unknown".to_owned(),
    }
}

fn display_extension(extension: &InstalledLocalExtension) -> String {
    let status = match activation_enabled(extension.workspace_activation.as_ref()) {
        Some(true) => "✓",
        Some(false) => "✗",
        None => "?",
    };
    let display_name = extension
        .display_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(&extension.name);
    let mut output = format!(
        "{status} {} ({})",
        sanitize_display(display_name),
        sanitize_display(&extension.version)
    );
    if let Some(description) = extension.description.as_deref() {
        if !description.is_empty() {
            output.push_str(&format!(
                "\n Description: {}",
                sanitize_display(description)
            ));
        }
    }
    output.push_str(&format!(
        "\n Path: {}",
        sanitize_display(&extension.path.to_string_lossy())
    ));
    if let (Some(source), Some(source_type)) = (
        extension.source.as_deref(),
        extension.source_type.as_deref(),
    ) {
        output.push_str(&format!(
            "\n Source: {} (Type: {})",
            sanitize_display(&redact_url_credentials(source)),
            sanitize_display(source_type)
        ));
    }
    if let Some(origin_source) = extension.origin_source.as_deref() {
        output.push_str(&format!("\n Origin: {}", sanitize_display(origin_source)));
    }
    if let Some(source_ref) = extension.source_ref.as_deref() {
        output.push_str(&format!("\n Ref: {}", sanitize_display(source_ref)));
    }
    if let Some(release_tag) = extension.release_tag.as_deref() {
        output.push_str(&format!(
            "\n Release tag: {}",
            sanitize_display(release_tag)
        ));
    }
    output.push_str(&format!(
        "\n Enabled (User): {}\n Enabled (Workspace): {}",
        activation_label(extension.user_activation.as_ref()),
        activation_label(extension.workspace_activation.as_ref())
    ));
    append_section(
        &mut output,
        "Context files",
        extension
            .context_files
            .iter()
            .map(|path| sanitize_display(&path.to_string_lossy())),
    );
    append_section(
        &mut output,
        "Commands",
        extension
            .commands
            .iter()
            .map(|command| format!("/{command}")),
    );
    append_section(&mut output, "Skills", extension.skills.iter().cloned());
    append_section(&mut output, "Agents", extension.agents.iter().cloned());
    append_section(
        &mut output,
        "MCP servers",
        extension.mcp_servers.iter().cloned(),
    );
    output
}

fn append_section(output: &mut String, heading: &str, values: impl IntoIterator<Item = String>) {
    let values = values.into_iter().collect::<Vec<_>>();
    if values.is_empty() {
        return;
    }
    output.push_str(&format!("\n {heading}:"));
    for value in values {
        output.push_str(&format!("\n  {}", sanitize_display(&value)));
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
    if !args.is_empty() {
        return Err(format!("{USAGE}\nlist accepts no arguments."));
    }

    let workspace_root = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let user_home =
        home_directory().ok_or_else(|| "could not determine the user home directory".to_owned())?;
    let user_extensions_dir = Storage::get_user_extensions_dir();
    let extension_store_dir = Storage::get_global_canopy_dir().join("extension-store");
    let inventory = load_installed_local_extensions(InstalledExtensionListOptions {
        workspace_root: &workspace_root,
        user_home: &user_home,
        user_extensions_dir: &user_extensions_dir,
        extension_store_dir: &extension_store_dir,
    });
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {diagnostic}");
    }
    if inventory.extensions.is_empty() {
        println!("No extensions installed.");
        return Ok(());
    }
    println!(
        "{}",
        inventory
            .extensions
            .iter()
            .map(display_extension)
            .collect::<Vec<_>>()
            .join("\n\n")
    );
    Ok(())
}
