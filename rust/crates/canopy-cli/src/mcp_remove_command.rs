// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Remove a configured MCP server without connecting to it.

use std::fs;
use std::io;

use canopy_core::config::{LoadSettingsOptions, SettingScope, load_settings};
use canopy_core::jsonc::{parse_jsonc_object, update_jsonc_content};
use canopy_core::mcp::token_storage::{ConfiguredTokenStorage, TokenStorage};
use canopy_core::storage::Storage;
use canopy_core::utils::atomic_file_write::{AtomicWriteOptions, atomic_write_file};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use serde_json::{Map, Value};
use tokio::runtime::Builder;

const USAGE: &str = "Usage: canopy mcp remove [options] <name>";

#[derive(Clone, Copy)]
enum Scope {
    User,
    Project,
}

impl Scope {
    fn setting_scope(self) -> SettingScope {
        match self {
            Self::User => SettingScope::User,
            Self::Project => SettingScope::Workspace,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Project => "project",
        }
    }
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        println!("{USAGE}");
        println!("Remove a server from user or project settings.");
        return Ok(());
    }

    let (name, scope) = parse_args(args)?;
    remove_server(&name, scope)
}

fn parse_args(args: &[String]) -> Result<(String, Scope), String> {
    let mut name = None;
    let mut scope = Scope::User;
    let mut index = 0;
    let mut positional_only = false;

    while index < args.len() {
        let argument = args[index].as_str();
        if positional_only {
            set_name(&mut name, argument)?;
            index += 1;
            continue;
        }
        if argument == "--" {
            positional_only = true;
            index += 1;
            continue;
        }
        if argument == "--scope" || argument == "-s" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("{USAGE}\n--scope requires user or project."))?;
            scope = parse_scope(value)?;
            index += 2;
            continue;
        }
        if let Some(value) = argument
            .strip_prefix("--scope=")
            .or_else(|| argument.strip_prefix("-s="))
        {
            scope = parse_scope(value)?;
            index += 1;
            continue;
        }
        if argument.starts_with('-') {
            return Err(format!("{USAGE}\nUnknown option: {argument}"));
        }
        set_name(&mut name, argument)?;
        index += 1;
    }

    let Some(name) = name else {
        return Err(format!("{USAGE}\nSpecify the server name to remove."));
    };
    Ok((name, scope))
}

fn set_name(name: &mut Option<String>, value: &str) -> Result<(), String> {
    if name.replace(value.to_owned()).is_some() {
        return Err(format!("{USAGE}\nExpected one server name."));
    }
    Ok(())
}

fn parse_scope(value: &str) -> Result<Scope, String> {
    match value {
        "user" => Ok(Scope::User),
        "project" => Ok(Scope::Project),
        _ => Err(format!("{USAGE}\nScope must be user or project.")),
    }
}

fn remove_server(name: &str, scope: Scope) -> Result<(), String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let mut preflight_options = LoadSettingsOptions::default();
    preflight_options.skip_workspace_settings = true;
    preflight_options.skip_load_environment = true;
    let preflight = load_settings(workspace.clone(), &mut preflight_options)
        .map_err(|error| error.to_string())?;

    if matches!(scope, Scope::Project) {
        if preflight.workspace.path == preflight.user.path {
            return Err(
                "Please use --scope user to edit settings in the home directory.".to_owned(),
            );
        }
        if !preflight.is_trusted {
            return Err(format!(
                "Cannot remove an MCP server from project settings in untrusted folder at {}.",
                sanitize(&workspace.display().to_string())
            ));
        }
    }

    let settings_path = &preflight.for_scope(scope.setting_scope()).path;
    let original = match fs::read_to_string(settings_path) {
        Ok(original) => original,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            print_not_found(name, scope);
            return Ok(());
        }
        Err(error) => {
            return Err(format!(
                "Could not read {} settings at {}: {error}",
                scope.label(),
                sanitize(&settings_path.display().to_string())
            ));
        }
    };
    let settings = parse_jsonc_object(&original)
        .map_err(|error| format!("Could not parse settings: {error}"))?;
    let Some(existing_servers) = settings.get("mcpServers") else {
        print_not_found(name, scope);
        return Ok(());
    };
    let Some(existing_servers) = existing_servers.as_object() else {
        return Err(format!(
            "Cannot remove server: mcpServers in {} settings is not an object.",
            scope.label()
        ));
    };
    if !existing_servers.get(name).is_some_and(is_truthy) {
        print_not_found(name, scope);
        return Ok(());
    }

    let mut servers = existing_servers.clone();
    servers.remove(name);
    let updates = Map::from_iter([("mcpServers".to_owned(), Value::Object(servers))]);
    let replaced_path = ["mcpServers".to_owned()];
    let updated = update_jsonc_content(&original, &updates, false, &replaced_path)
        .map_err(|error| format!("Could not update MCP settings: {error}"))?;
    atomic_write_file(
        settings_path,
        updated.as_bytes(),
        &AtomicWriteOptions::default(),
    )
    .map_err(|error| format!("Could not write MCP settings: {error}"))?;

    cleanup_oauth_token(name);

    println!(
        "Server \"{}\" removed from {} settings.",
        sanitize(name),
        scope.label()
    );
    Ok(())
}

fn cleanup_oauth_token(name: &str) {
    let Ok(runtime) = Builder::new_current_thread().enable_all().build() else {
        return;
    };
    let storage = ConfiguredTokenStorage::new(
        Storage::get_mcp_oauth_tokens_path(),
        Storage::get_global_canopy_dir(),
    );
    let _ = runtime.block_on(storage.delete_credentials(name));
}

fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn print_not_found(name: &str, scope: Scope) {
    println!(
        "Server \"{}\" not found in {} settings.",
        sanitize(name),
        scope.label()
    );
}

fn sanitize(value: &str) -> String {
    strip_terminal_control_sequences(value)
}
