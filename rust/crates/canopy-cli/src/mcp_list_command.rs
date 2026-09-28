// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Read-only MCP server inventory and bounded connectivity checks.

use std::path::PathBuf;
use std::time::Duration;

use canopy_core::config::{LoadSettingsOptions, get_settings_warnings, load_settings};
use canopy_core::extension_inventory::{
    ExtensionInventoryOptions, load_active_local_extension_references,
};
use canopy_core::storage::Storage;
use serde_json::Value;

use crate::mcp_host::{
    ExtensionMcpSource, McpCliSettings, McpListApprovalSnapshot, McpListApprovalState,
    McpListProbeError, probe_mcp_server,
};

const USAGE: &str = "Usage: canopy mcp list";
const CONNECT_TIMEOUT: Duration = Duration::from_millis(5_000);
const COLOR_GREEN: &str = "\u{1b}[32m";
const COLOR_YELLOW: &str = "\u{1b}[33m";
const COLOR_RED: &str = "\u{1b}[31m";
const RESET_COLOR: &str = "\u{1b}[0m";

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        println!("List configured MCP servers and test approved servers with a bounded ping.");
        return Ok(());
    }
    if !args.is_empty() {
        return Err(format!("{USAGE}\nlist accepts no arguments."));
    }

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
        eprintln!("[CANOPY] {warning}");
    }

    if settings.servers.is_empty() {
        println!("No MCP servers configured.");
        return Ok(());
    }
    println!("Configured MCP servers:\n");

    let effective_env = loaded.runtime_environment.effective_env.clone();
    let approval_path = settings.approval_path_override.clone();
    run_async(list_servers(
        settings,
        workspace_root,
        effective_env,
        approval_path,
    ))
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

async fn list_servers(
    settings: McpCliSettings,
    workspace_root: PathBuf,
    effective_env: std::collections::HashMap<String, String>,
    approval_path: Option<PathBuf>,
) -> Result<(), String> {
    // Read approvals only if a gated server is actually encountered. A corrupt
    // approval file is treated as pending for all gated servers in this run.
    let mut approvals: Option<Result<McpListApprovalSnapshot, String>> = None;
    let mut approval_error_reported = false;

    for (server_name, configured_server) in &settings.servers {
        let mut server = configured_server.clone();
        let server_info = format_server_info(server_name, &server);
        let gated = !settings.ungated_server_names.contains(server_name)
            && matches!(
                server.get("scope").and_then(Value::as_str),
                Some("project" | "workspace")
            );
        if gated {
            let approval_result = approvals
                .get_or_insert_with(|| McpListApprovalSnapshot::load(approval_path.as_deref()));
            let approval_state = match approval_result {
                Ok(snapshot) => snapshot.state(&workspace_root, server_name, &server),
                Err(error) => {
                    if !approval_error_reported {
                        eprintln!("Warning: MCP approvals file error: {error}");
                        approval_error_reported = true;
                    }
                    McpListApprovalState::Pending
                }
            };
            if approval_state != McpListApprovalState::Approved {
                let status = match approval_state {
                    McpListApprovalState::Rejected => "Rejected",
                    McpListApprovalState::Approved => unreachable!("handled above"),
                    McpListApprovalState::Pending => "Pending approval",
                };
                println!("{COLOR_YELLOW}●{RESET_COLOR} {server_info} - {status}");
                continue;
            }
        }

        let result = if settings
            .apply_extension_settings_to_server_config(
                server_name,
                &mut server,
                &workspace_root,
                |key| effective_env.contains_key(key),
            )
            .await
            .is_ok()
        {
            probe_mcp_server(
                server_name,
                &server,
                &workspace_root,
                &effective_env,
                CONNECT_TIMEOUT,
            )
            .await
        } else {
            Err(McpListProbeError::Failed)
        };
        let (indicator, status) = match result {
            Ok(()) => (
                format!("{COLOR_GREEN}✓{RESET_COLOR}"),
                "Connected".to_owned(),
            ),
            Err(McpListProbeError::TimedOut) => (
                format!("{COLOR_RED}✗{RESET_COLOR}"),
                format!(
                    "Disconnected (timed out after {}ms)",
                    CONNECT_TIMEOUT.as_millis()
                ),
            ),
            Err(McpListProbeError::Failed) => (
                format!("{COLOR_RED}✗{RESET_COLOR}"),
                "Disconnected".to_owned(),
            ),
        };
        println!("{indicator} {server_info} - {status}");
    }
    Ok(())
}

fn format_server_info(server_name: &str, server: &Value) -> String {
    let mut info = format!("{server_name}: ");
    if let Some(url) = server
        .get("httpUrl")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        info.push_str(&format!("{url} (http)"));
    } else if let Some(url) = server
        .get("url")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        info.push_str(&format!("{url} (sse)"));
    } else if let Some(command) = server
        .get("command")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        let args = server
            .get("args")
            .and_then(Value::as_array)
            .map(|args| {
                args.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        info.push_str(&format!("{command} {args} (stdio)"));
    }
    canopy_core::utils::terminal_safe::strip_terminal_control_sequences(&info)
}
