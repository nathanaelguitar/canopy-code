// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Explicit, one-shot MCP server reconnect and tool discovery command.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use canopy_core::config::{LoadSettingsOptions, get_settings_warnings, load_settings};
use canopy_core::extension_inventory::{
    ExtensionInventoryOptions, load_active_local_extension_references,
};
use canopy_core::permissions::PermissionRuleSet;
use canopy_core::storage::Storage;
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use serde_json::{Map, Value};

use crate::mcp_host::{
    ExtensionMcpSource, McpCliApprovalPrompt, McpCliSettings, McpCliWorkspace,
    McpListApprovalSnapshot, McpListApprovalState,
};

const USAGE: &str = "Usage: canopy mcp reconnect [options] [server-name]";
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RECONNECT_SERVERS: usize = 64;

struct NoReconnectPrompt;

impl McpCliApprovalPrompt for NoReconnectPrompt {
    fn is_available(&self) -> bool {
        false
    }

    fn confirm(&self, _prompt: &str) -> Result<bool, String> {
        Ok(false)
    }
}

pub fn handles(subcommand: &str) -> bool {
    subcommand == "reconnect"
}

/// Run `reconnect`; `args` includes `reconnect` as its first element.
pub fn run(args: &[String]) -> Result<(), String> {
    if args.first().map(String::as_str) != Some("reconnect") {
        return Err(format!("{USAGE}\nMissing reconnect subcommand."));
    }
    let Some(selection) = parse_args(&args[1..])? else {
        return Ok(());
    };

    let workspace_root = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let loaded = load_settings(workspace_root.clone(), &mut LoadSettingsOptions::default())
        .map_err(|error| error.to_string())?;
    for warning in get_settings_warnings(&loaded) {
        eprintln!("[CANOPY] {warning}");
    }

    let safe_mode = canopy_core::utils::safe_mode::is_safe_mode_env();
    let bare_mode = canopy_core::utils::bare_mode::is_bare_mode(None);
    let user_extensions_dir = Storage::get_user_extensions_dir();
    let extension_store_dir = Storage::get_global_canopy_dir().join("extension-store");
    let inventory = load_active_local_extension_references(ExtensionInventoryOptions {
        workspace_root: &workspace_root,
        user_extensions_dir: &user_extensions_dir,
        extension_store_dir: &extension_store_dir,
        enabled_extension_overrides: &[],
        workspace_trusted: loaded.is_trusted,
        safe_mode,
        bare_mode,
    });
    for diagnostic in &inventory.diagnostics {
        eprintln!("[CANOPY] {diagnostic}");
    }
    let extension_sources = inventory
        .active_mcp_servers
        .into_iter()
        .map(ExtensionMcpSource::from)
        .collect::<Vec<_>>();
    let settings = McpCliSettings::from_loaded_settings(&loaded).with_sources(
        &workspace_root,
        None,
        None,
        &extension_sources,
    )?;
    for warning in &settings.source_warnings {
        eprintln!("[CANOPY] {}", strip_terminal_control_sequences(warning));
    }

    let selected = if selection.all {
        if settings.servers.is_empty() {
            println!("No MCP servers configured.");
            return Ok(());
        }
        println!("Reconnecting to all MCP servers...\n");
        if settings.servers.len() > MAX_RECONNECT_SERVERS {
            eprintln!(
                "[CANOPY] MCP server inventory exceeds the {MAX_RECONNECT_SERVERS}-server reconnect limit; remaining servers were skipped."
            );
        }
        settings
            .servers
            .keys()
            .take(MAX_RECONNECT_SERVERS)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        let name = selection
            .server_name
            .expect("argument parser requires a server name unless --all is set");
        if !settings.servers.contains_key(&name) {
            return Err(format!(
                "Error: Server \"{}\" not found in configuration.",
                strip_terminal_control_sequences(&name)
            ));
        }
        println!(
            "Reconnecting to server \"{}\"...",
            strip_terminal_control_sequences(&name)
        );
        vec![name]
    };

    let effective_env = loaded.runtime_environment.effective_env.clone();
    let approval_path = settings.approval_path_override.clone();
    run_async(reconnect_selected(
        selected,
        settings,
        workspace_root,
        effective_env,
        approval_path,
        selection.all,
    ))
}

#[derive(Clone, Debug)]
struct ReconnectSelection {
    server_name: Option<String>,
    all: bool,
}

fn parse_args(args: &[String]) -> Result<Option<ReconnectSelection>, String> {
    let mut server_name = None;
    let mut all = false;
    for argument in args {
        match argument.as_str() {
            "--help" | "-h" => {
                println!("{USAGE}");
                println!("Reconnect to configured MCP server(s) and discover their tools.");
                return Ok(None);
            }
            "--all" | "--all=true" | "-a" => all = true,
            "--all=false" => all = false,
            value if value.starts_with('-') => {
                return Err(format!("{USAGE}\nUnknown option: {value}"));
            }
            value => {
                if server_name.replace(value.to_owned()).is_some() {
                    return Err(format!("{USAGE}\nSpecify at most one server name."));
                }
            }
        }
    }
    if all && server_name.is_some() {
        return Err(format!(
            "{USAGE}\nSpecify a server name or --all, not both."
        ));
    }
    if !all && server_name.is_none() {
        return Err(
            "Please specify a server name or use --all to reconnect all servers.".to_owned(),
        );
    }
    Ok(Some(ReconnectSelection { server_name, all }))
}

fn run_async<F>(future: F) -> Result<(), String>
where
    F: std::future::Future<Output = Result<(), String>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("could not start async runtime: {error}"))?
        .block_on(future)
}

async fn reconnect_selected(
    selected: Vec<String>,
    settings: McpCliSettings,
    workspace_root: PathBuf,
    effective_env: HashMap<String, String>,
    approval_path: Option<PathBuf>,
    all: bool,
) -> Result<(), String> {
    let has_gated = selected
        .iter()
        .any(|name| is_gated_config(name, &settings.servers, &settings.ungated_server_names));
    let approvals = if has_gated {
        Some(McpListApprovalSnapshot::load(
            approval_path
                .as_deref()
                .filter(|path| !path.as_os_str().is_empty()),
        ))
    } else {
        None
    };

    for server_name in selected {
        let result = reconnect_one(
            &server_name,
            &settings,
            &workspace_root,
            &effective_env,
            approvals.as_ref(),
        )
        .await;
        match result {
            Ok(()) if all => println!(
                "✓ {}: Reconnected successfully",
                strip_terminal_control_sequences(&server_name)
            ),
            Ok(()) => println!(
                "Successfully reconnected to server \"{}\".",
                strip_terminal_control_sequences(&server_name)
            ),
            Err(error) if all => println!(
                "✗ {}: Failed - {}",
                strip_terminal_control_sequences(&server_name),
                strip_terminal_control_sequences(&error)
            ),
            Err(error) => {
                return Err(format!(
                    "Failed to reconnect to server \"{}\": {}",
                    strip_terminal_control_sequences(&server_name),
                    strip_terminal_control_sequences(&error)
                ));
            }
        }
    }
    Ok(())
}

fn is_gated_config(
    server_name: &str,
    servers: &Map<String, Value>,
    ungated_server_names: &std::collections::HashSet<String>,
) -> bool {
    !ungated_server_names.contains(server_name)
        && servers
            .get(server_name)
            .and_then(|config| config.get("scope"))
            .and_then(Value::as_str)
            .is_some_and(|scope| matches!(scope, "project" | "workspace"))
}

async fn reconnect_one(
    server_name: &str,
    settings: &McpCliSettings,
    workspace_root: &std::path::Path,
    effective_env: &HashMap<String, String>,
    approvals: Option<&Result<McpListApprovalSnapshot, String>>,
) -> Result<(), String> {
    if !settings.trusted_workspace {
        return Err("MCP servers are disabled in an untrusted workspace".to_owned());
    }
    let config = settings
        .servers
        .get(server_name)
        .cloned()
        .ok_or_else(|| format!("Server \"{server_name}\" not found in configuration."))?;
    if is_gated_config(
        server_name,
        &settings.servers,
        &settings.ungated_server_names,
    ) {
        let approvals = approvals
            .ok_or_else(|| "MCP approval state was not loaded for a gated server".to_owned())?;
        let snapshot = approvals.as_ref().map_err(|error| {
            format!("could not read MCP approvals; refusing to connect: {error}")
        })?;
        match snapshot.state(workspace_root, server_name, &config) {
            McpListApprovalState::Approved => {}
            McpListApprovalState::Rejected => {
                return Err("MCP server approval was rejected; refusing to connect".to_owned());
            }
            McpListApprovalState::Pending => {
                return Err("MCP server is pending approval; refusing to connect".to_owned());
            }
        }
    }

    // One isolated native workspace and session per server keeps this CLI
    // command process-scoped and prevents one failing server from blocking the
    // remaining `--all` entries.
    let workspace = McpCliWorkspace::new_native(workspace_root.to_path_buf(), effective_env);
    let mut one_server_settings = settings.clone();
    one_server_settings.servers = Map::new();
    one_server_settings
        .servers
        .insert(server_name.to_owned(), config);
    one_server_settings.allowed = None;
    one_server_settings.excluded.clear();

    let connect_result = tokio::time::timeout(
        RECONNECT_TIMEOUT,
        workspace.open_session(
            "mcp-reconnect",
            &one_server_settings,
            PermissionRuleSet::default(),
            None,
            Vec::new(),
            Arc::new(NoReconnectPrompt),
        ),
    )
    .await;

    let mut outcome = match connect_result {
        Err(_) => Err(format!(
            "connection and tool discovery timed out after {}ms",
            RECONNECT_TIMEOUT.as_millis()
        )),
        Ok(Err(error)) => Err(error.to_string()),
        Ok(Ok(session)) => {
            let result = if let Some((_, reason)) = session.skipped_servers().first() {
                Err(reason.clone())
            } else if let Some(error) = session.discovery_errors().get(server_name) {
                Err(error.clone())
            } else {
                Ok(())
            };
            session.stop();
            result
        }
    };

    if tokio::time::timeout(SHUTDOWN_TIMEOUT, workspace.shutdown())
        .await
        .is_err()
        && outcome.is_ok()
    {
        outcome = Err(format!(
            "server reconnected but transport cleanup timed out after {}ms",
            SHUTDOWN_TIMEOUT.as_millis()
        ));
    }
    outcome
}
