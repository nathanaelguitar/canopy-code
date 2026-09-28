// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Explicit approval and rejection of workspace-scoped MCP server configs.

use std::fs;
use std::path::Path;

use canopy_core::config::{LoadSettingsOptions, get_settings_warnings, load_settings};
use canopy_core::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;

use crate::mcp_host::{
    MCP_APPROVALS_MAX_BYTES, McpApprovalStatus, McpCliSettings, McpListApprovalSnapshot,
};

const APPROVE_USAGE: &str = "Usage: canopy mcp approve [options] [name]";
const REJECT_USAGE: &str = "Usage: canopy mcp reject [options] [name]";

pub fn handles(subcommand: &str) -> bool {
    matches!(subcommand, "approve" | "reject")
}

/// Run one explicit `approve` or `reject` operation. `args` includes the
/// subcommand as its first element so the parent CLI can dispatch the module.
pub fn run(args: &[String]) -> Result<(), String> {
    let subcommand = args
        .first()
        .map(String::as_str)
        .ok_or_else(|| "mcp approval command requires `approve` or `reject`".to_owned())?;
    let (status, verb, usage) = match subcommand {
        "approve" => (McpApprovalStatus::Approved, "Approved", APPROVE_USAGE),
        "reject" => (McpApprovalStatus::Rejected, "Rejected", REJECT_USAGE),
        _ => return Err("mcp approval command requires `approve` or `reject`".to_owned()),
    };
    let Some((name, all)) = parse_args(&args[1..], usage)? else {
        return Ok(());
    };
    update_approval(name.as_deref(), all, status, verb)
}

fn parse_args(args: &[String], usage: &str) -> Result<Option<(Option<String>, bool)>, String> {
    let mut name = None;
    let mut all = false;
    for argument in args {
        match argument.as_str() {
            "--help" | "-h" => {
                println!("{usage}");
                println!("Set a hash-bound approval decision for gated MCP servers.");
                return Ok(None);
            }
            "--all" | "--all=true" => all = true,
            "--all=false" => all = false,
            value if value.starts_with('-') => {
                return Err(format!("{usage}\nUnknown option: {value}"));
            }
            value => {
                if name.replace(value.to_owned()).is_some() {
                    return Err(format!("{usage}\nSpecify at most one server name."));
                }
            }
        }
    }
    Ok(Some((name, all)))
}

fn update_approval(
    name: Option<&str>,
    all: bool,
    status: McpApprovalStatus,
    verb: &str,
) -> Result<(), String> {
    let workspace_root = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let loaded = load_settings(workspace_root.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    for warning in get_settings_warnings(&loaded) {
        eprintln!("[CANOPY] {warning}");
    }

    // `load_settings` omits workspace settings for an untrusted workspace.
    // Project `.mcp.json` remains visible so the user can make an explicit,
    // hash-bound decision without starting its server.
    let settings = McpCliSettings::from_loaded_settings(&loaded).with_sources(
        &workspace_root,
        None,
        None,
        &[],
    )?;
    let approval_path_override = settings.approval_path_override.clone();
    for warning in &settings.source_warnings {
        eprintln!("[CANOPY] {warning}");
    }
    let servers = settings
        .servers
        .into_iter()
        .filter(|(_, config)| {
            matches!(
                config.get("scope").and_then(serde_json::Value::as_str),
                Some("project" | "workspace")
            )
        })
        .collect::<Vec<_>>();
    let names = servers
        .iter()
        .map(|(server_name, _)| server_name.clone())
        .collect::<Vec<_>>();
    if names.is_empty() {
        println!(
            "No approval-requiring MCP servers found (looked in .mcp.json and .canopy/settings.json)."
        );
        return Ok(());
    }

    let targets = if all {
        names.clone()
    } else if let Some(name) = name {
        vec![name.to_owned()]
    } else {
        println!("Specify a server name or pass --all.");
        return Ok(());
    };
    let target_configs = targets
        .iter()
        .filter_map(|target| {
            servers
                .iter()
                .find(|(server_name, _)| server_name == target)
                .map(|(_, config)| (target.clone(), config.clone()))
        })
        .collect::<Vec<_>>();
    if target_configs.len() != targets.len() {
        let target = targets
            .iter()
            .find(|target| !names.contains(target))
            .expect("length mismatch means at least one requested name was not found");
        let available = names
            .iter()
            .map(|server_name| strip_terminal_control_sequences(server_name))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "Server \"{}\" not found. Available: {available}",
            strip_terminal_control_sequences(target)
        );
        return Ok(());
    }

    let mut approvals = McpListApprovalSnapshot::load(
        approval_path_override
            .as_deref()
            .filter(|path| !path.as_os_str().is_empty()),
    )?;
    for (target, config) in &target_configs {
        approvals.set_status(&workspace_root, target, config, status)?;
    }
    let bytes = approvals.to_bounded_json()?;
    if bytes.len() as u64 > MCP_APPROVALS_MAX_BYTES {
        return Err(format!(
            "updated MCP approvals would exceed the {MCP_APPROVALS_MAX_BYTES} byte limit"
        ));
    }
    let path = approvals.path().to_path_buf();
    persist_approvals(&path, &bytes)?;

    for (target, _) in target_configs {
        println!(
            "{verb} MCP server \"{}\" (bound to its current config).",
            strip_terminal_control_sequences(&target)
        );
    }
    if status == McpApprovalStatus::Approved {
        println!("Approved servers connect in your next interactive session.");
    }
    Ok(())
}

fn persist_approvals(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 > MCP_APPROVALS_MAX_BYTES {
        return Err(format!(
            "MCP approvals data exceeds the {MCP_APPROVALS_MAX_BYTES} byte limit"
        ));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "could not create MCP approvals directory {}: {error}",
            parent.display()
        )
    })?;
    atomic_write_file(
        path,
        bytes,
        &AtomicWriteOptions {
            mode: Some(0o600),
            force_mode: true,
            symlink_policy: SymlinkPolicy::Follow,
            ..AtomicWriteOptions::default()
        },
    )
    .map_err(|error| {
        format!(
            "could not save MCP approvals to {}: {error}",
            path.display()
        )
    })
}
