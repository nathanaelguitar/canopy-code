// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Add or replace one MCP server configuration without connecting to it.

use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use canopy_core::config::{LoadSettingsOptions, SettingScope, load_settings};
use canopy_core::jsonc::{parse_jsonc_object, update_jsonc_content};
use canopy_core::utils::atomic_file_write::{AtomicWriteOptions, SymlinkPolicy, atomic_write_file};
use canopy_core::utils::terminal_safe::strip_terminal_control_sequences;
use serde_json::{Map, Value};

const USAGE: &str = "Usage: canopy mcp add [options] <name> <commandOrUrl> [args...]";
const MAX_SETTINGS_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Transport {
    Stdio,
    Sse,
    Http,
}

impl Transport {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::Sse => "sse",
            Self::Http => "http",
        }
    }
}

#[derive(Default)]
struct AddOptions {
    scope: Option<Scope>,
    transport: Option<Transport>,
    env: Vec<String>,
    headers: Vec<String>,
    timeout: Option<f64>,
    trust: Option<bool>,
    description: Option<String>,
    include_tools: Vec<String>,
    exclude_tools: Vec<String>,
    oauth_client_id: Option<String>,
    oauth_client_secret: Option<String>,
    oauth_redirect_uri: Option<String>,
    oauth_authorization_url: Option<String>,
    oauth_token_url: Option<String>,
    oauth_scopes: Vec<String>,
}

struct AddRequest {
    name: String,
    command_or_url: String,
    args: Vec<String>,
    options: AddOptions,
}

pub fn run(args: &[String]) -> Result<(), String> {
    if args
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        print_help();
        return Ok(());
    }

    add_server(parse_args(args)?)
}

fn parse_args(args: &[String]) -> Result<AddRequest, String> {
    let mut options = AddOptions::default();
    let mut positional = Vec::new();
    let mut command_args = Vec::new();
    let mut after_separator = false;
    let mut index = 0;

    while index < args.len() {
        let argument = args[index].as_str();
        if after_separator {
            command_args.push(argument.to_owned());
            index += 1;
            continue;
        }
        if argument == "--" {
            after_separator = true;
            index += 1;
            continue;
        }

        let (option, inline_value) = argument
            .split_once('=')
            .map_or((argument, None), |(option, value)| (option, Some(value)));
        match option {
            "--scope" | "-s" => {
                let value = option_value(args, &mut index, inline_value, "scope")?;
                options.scope = Some(parse_scope(&value)?);
            }
            "--transport" | "-t" => {
                let value = option_value(args, &mut index, inline_value, "transport")?;
                options.transport = Some(parse_transport(&value)?);
            }
            "--env" | "-e" => {
                options
                    .env
                    .push(option_value(args, &mut index, inline_value, "env")?);
            }
            "--header" | "-H" => {
                options
                    .headers
                    .push(option_value(args, &mut index, inline_value, "header")?);
            }
            "--timeout" => {
                let value = option_value(args, &mut index, inline_value, "timeout")?;
                let timeout = value
                    .parse::<f64>()
                    .ok()
                    .filter(|value| value.is_finite() && *value >= 0.0)
                    .ok_or_else(|| "`--timeout` must be a non-negative number.".to_owned())?;
                options.timeout = Some(timeout);
            }
            "--trust" => {
                options.trust = Some(match inline_value {
                    None | Some("true") => true,
                    Some("false") => false,
                    Some(_) => return Err("`--trust` accepts only true or false.".to_owned()),
                });
            }
            "--no-trust" if inline_value.is_none() => options.trust = Some(false),
            "--description" => {
                options.description =
                    Some(option_value(args, &mut index, inline_value, "description")?);
            }
            "--include-tools" => options.include_tools.push(option_value(
                args,
                &mut index,
                inline_value,
                "include-tools",
            )?),
            "--exclude-tools" => options.exclude_tools.push(option_value(
                args,
                &mut index,
                inline_value,
                "exclude-tools",
            )?),
            "--oauth-client-id" => {
                options.oauth_client_id = Some(option_value(
                    args,
                    &mut index,
                    inline_value,
                    "oauth-client-id",
                )?);
            }
            "--oauth-client-secret" => {
                options.oauth_client_secret = Some(option_value(
                    args,
                    &mut index,
                    inline_value,
                    "oauth-client-secret",
                )?);
            }
            "--oauth-redirect-uri" => {
                options.oauth_redirect_uri = Some(option_value(
                    args,
                    &mut index,
                    inline_value,
                    "oauth-redirect-uri",
                )?);
            }
            "--oauth-authorization-url" => {
                options.oauth_authorization_url = Some(option_value(
                    args,
                    &mut index,
                    inline_value,
                    "oauth-authorization-url",
                )?);
            }
            "--oauth-token-url" => {
                options.oauth_token_url = Some(option_value(
                    args,
                    &mut index,
                    inline_value,
                    "oauth-token-url",
                )?);
            }
            "--oauth-scopes" => options.oauth_scopes.push(option_value(
                args,
                &mut index,
                inline_value,
                "oauth-scopes",
            )?),
            _ if argument.starts_with('-') && positional.len() >= 2 => {
                command_args.push(argument.to_owned());
            }
            // The TypeScript yargs parser preserves unknown options as server
            // arguments, including when they appear before the command.
            _ if argument.starts_with('-') => command_args.push(argument.to_owned()),
            _ if positional.len() < 2 => positional.push(argument.to_owned()),
            _ => command_args.push(argument.to_owned()),
        }
        index += 1;
    }

    if positional.len() < 2 {
        return Err(format!(
            "{USAGE}\nSpecify a server name and command or URL."
        ));
    }
    let name = positional.remove(0);
    let command_or_url = positional.remove(0);
    if name.trim().is_empty() || command_or_url.trim().is_empty() {
        return Err(format!(
            "{USAGE}\nServer name and command or URL cannot be empty."
        ));
    }
    if matches!(name.as_str(), "__proto__" | "constructor" | "prototype") {
        return Err("That server name cannot be used in settings.".to_owned());
    }

    Ok(AddRequest {
        name,
        command_or_url,
        args: command_args,
        options,
    })
}

fn option_value(
    args: &[String],
    index: &mut usize,
    inline_value: Option<&str>,
    option: &str,
) -> Result<String, String> {
    if let Some(value) = inline_value {
        return Ok(value.to_owned());
    }
    *index += 1;
    args.get(*index)
        .cloned()
        .ok_or_else(|| format!("`--{option}` requires a value."))
}

fn parse_scope(value: &str) -> Result<Scope, String> {
    match value {
        "user" => Ok(Scope::User),
        "project" => Ok(Scope::Project),
        _ => Err("`--scope` must be `user` or `project`.".to_owned()),
    }
}

fn parse_transport(value: &str) -> Result<Transport, String> {
    match value {
        "stdio" => Ok(Transport::Stdio),
        "sse" => Ok(Transport::Sse),
        "http" => Ok(Transport::Http),
        _ => Err("`--transport` must be `stdio`, `sse`, or `http`.".to_owned()),
    }
}

fn add_server(request: AddRequest) -> Result<(), String> {
    let workspace = std::env::current_dir()
        .map_err(|error| format!("could not determine current directory: {error}"))?;
    let mut preflight_options = LoadSettingsOptions::default();
    preflight_options.skip_workspace_settings = true;
    preflight_options.skip_load_environment = true;
    let preflight = load_settings(workspace.clone(), &mut preflight_options)
        .map_err(|error| error.to_string())?;

    let scope = request.options.scope.unwrap_or(Scope::User);
    if matches!(scope, Scope::Project) {
        if preflight.workspace.path == preflight.user.path {
            return Err(
                "Please use --scope user to edit settings in the home directory.".to_owned(),
            );
        }
        if !preflight.is_trusted {
            return Err(format!(
                "Cannot add an MCP server to project settings in untrusted folder at {}.",
                sanitize(&workspace.display().to_string())
            ));
        }
    }

    let transport = request.options.transport.unwrap_or_else(|| {
        if request
            .command_or_url
            .get(..7)
            .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
            || request
                .command_or_url
                .get(..8)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
        {
            Transport::Http
        } else {
            Transport::Stdio
        }
    });
    let server = build_server_config(&request, transport)?;
    let settings_path = preflight.for_scope(scope.setting_scope()).path.clone();
    let original = read_settings_file(&settings_path)?;
    let settings = parse_jsonc_object(&original)
        .map_err(|error| format!("Could not parse {} settings: {error}", scope.label()))?;
    let existing_servers = match settings.get("mcpServers") {
        None => None,
        Some(Value::Object(servers)) => Some(servers),
        Some(value) if !is_truthy(value) => None,
        Some(_) => {
            return Err(format!(
                "Cannot add a server: mcpServers in {} settings is not an object.",
                scope.label()
            ));
        }
    };
    let already_configured = existing_servers
        .and_then(|servers| servers.get(&request.name))
        .is_some_and(is_truthy);
    if already_configured {
        println!(
            "MCP server \"{}\" is already configured within {} settings.",
            sanitize(&request.name),
            scope.label()
        );
    }

    let mut server_update = Map::new();
    server_update.insert(request.name.clone(), server);
    let mut updates = Map::new();
    updates.insert("mcpServers".to_owned(), Value::Object(server_update));
    let replaced_path = ["mcpServers".to_owned(), request.name.clone()];
    let updated = update_jsonc_content(&original, &updates, false, &replaced_path)
        .map_err(|error| format!("Could not update MCP settings: {error}"))?;

    if let Some(parent) = settings_path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "Could not create the {} settings directory: {}",
                scope.label(),
                sanitize(&error.to_string())
            )
        })?;
    }
    atomic_write_file(
        &settings_path,
        updated.as_bytes(),
        &AtomicWriteOptions {
            symlink_policy: SymlinkPolicy::NoFollow,
            ..AtomicWriteOptions::default()
        },
    )
    .map_err(|error| {
        format!(
            "Could not write MCP settings: {}",
            sanitize(&error.to_string())
        )
    })?;

    if already_configured {
        println!(
            "MCP server \"{}\" updated in {} settings.",
            sanitize(&request.name),
            scope.label()
        );
    } else {
        println!(
            "MCP server \"{}\" added to {} settings. ({})",
            sanitize(&request.name),
            scope.label(),
            transport.as_str()
        );
    }
    Ok(())
}

fn build_server_config(request: &AddRequest, transport: Transport) -> Result<Value, String> {
    let options = &request.options;
    let scopes = options
        .oauth_scopes
        .iter()
        .flat_map(|scope| scope.split(','))
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let has_oauth = options
        .oauth_client_id
        .as_deref()
        .is_some_and(|value| !value.is_empty())
        || options
            .oauth_client_secret
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        || options
            .oauth_redirect_uri
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        || options
            .oauth_authorization_url
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        || options
            .oauth_token_url
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        || !scopes.is_empty();
    if has_oauth && transport == Transport::Stdio {
        return Err(
            "OAuth options (--oauth-*) are only supported with --transport sse or --transport http."
                .to_owned(),
        );
    }

    let mut server = Map::new();
    match transport {
        Transport::Stdio => {
            server.insert(
                "command".to_owned(),
                Value::String(request.command_or_url.clone()),
            );
            if !request.args.is_empty() {
                server.insert(
                    "args".to_owned(),
                    Value::Array(request.args.iter().cloned().map(Value::String).collect()),
                );
            }
            let environment = options
                .env
                .iter()
                .filter_map(|entry| {
                    let (key, value) = entry.split_once('=')?;
                    (!key.is_empty() && !value.is_empty())
                        .then(|| (key.to_owned(), Value::String(value.to_owned())))
                })
                .collect::<Map<_, _>>();
            if !environment.is_empty() {
                server.insert("env".to_owned(), Value::Object(environment));
            }
        }
        Transport::Sse => {
            server.insert(
                "url".to_owned(),
                Value::String(request.command_or_url.clone()),
            );
            insert_headers(&mut server, &options.headers);
        }
        Transport::Http => {
            server.insert(
                "httpUrl".to_owned(),
                Value::String(request.command_or_url.clone()),
            );
            insert_headers(&mut server, &options.headers);
        }
    }
    if let Some(timeout) = options.timeout {
        let number = serde_json::Number::from_f64(timeout)
            .ok_or_else(|| "`--timeout` must be a finite number.".to_owned())?;
        server.insert("timeout".to_owned(), Value::Number(number));
    }
    if let Some(trust) = options.trust {
        server.insert("trust".to_owned(), Value::Bool(trust));
    }
    if let Some(description) = &options.description {
        server.insert("description".to_owned(), Value::String(description.clone()));
    }
    if !options.include_tools.is_empty() {
        server.insert(
            "includeTools".to_owned(),
            Value::Array(
                options
                    .include_tools
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if !options.exclude_tools.is_empty() {
        server.insert(
            "excludeTools".to_owned(),
            Value::Array(
                options
                    .exclude_tools
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if has_oauth {
        let mut oauth = Map::new();
        oauth.insert("enabled".to_owned(), Value::Bool(true));
        insert_nonempty_string(&mut oauth, "clientId", &options.oauth_client_id);
        insert_nonempty_string(&mut oauth, "clientSecret", &options.oauth_client_secret);
        insert_nonempty_string(&mut oauth, "redirectUri", &options.oauth_redirect_uri);
        insert_nonempty_string(
            &mut oauth,
            "authorizationUrl",
            &options.oauth_authorization_url,
        );
        insert_nonempty_string(&mut oauth, "tokenUrl", &options.oauth_token_url);
        if !scopes.is_empty() {
            oauth.insert(
                "scopes".to_owned(),
                Value::Array(scopes.into_iter().map(Value::String).collect()),
            );
        }
        server.insert("oauth".to_owned(), Value::Object(oauth));
    }
    Ok(Value::Object(server))
}

fn insert_headers(server: &mut Map<String, Value>, entries: &[String]) {
    let headers = entries
        .iter()
        .filter_map(|entry| {
            let (key, value) = entry.split_once(':')?;
            let key = key.trim();
            let value = value.trim();
            (!key.is_empty() && !value.is_empty())
                .then(|| (key.to_owned(), Value::String(value.to_owned())))
        })
        .collect::<Map<_, _>>();
    if !headers.is_empty() {
        server.insert("headers".to_owned(), Value::Object(headers));
    }
}

fn insert_nonempty_string(object: &mut Map<String, Value>, key: &str, value: &Option<String>) {
    if let Some(value) = value.as_ref().filter(|value| !value.is_empty()) {
        object.insert(key.to_owned(), Value::String(value.clone()));
    }
}

fn read_settings_file(path: &Path) -> Result<String, String> {
    let Some(bytes) = read_bounded_regular_file(path)? else {
        return Ok("{}\n".to_owned());
    };
    String::from_utf8(bytes).map_err(|_| "Settings file is not valid UTF-8.".to_owned())
}

fn read_bounded_regular_file(path: &Path) -> Result<Option<Vec<u8>>, String> {
    let display_path = sanitize(&path.display().to_string());
    let before = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "Could not inspect settings at {display_path}: {}",
                sanitize(&error.to_string())
            ));
        }
    };
    if before.file_type().is_symlink() || !before.is_file() {
        return Err(format!(
            "Settings path is not a regular file: {display_path}"
        ));
    }
    if before.len() > MAX_SETTINGS_BYTES {
        return Err(format!(
            "Settings file exceeds the {MAX_SETTINGS_BYTES}-byte limit."
        ));
    }
    let file = File::open(path).map_err(|error| {
        format!(
            "Could not read settings at {display_path}: {}",
            sanitize(&error.to_string())
        )
    })?;
    let opened = file.metadata().map_err(|error| {
        format!(
            "Could not inspect settings at {display_path}: {}",
            sanitize(&error.to_string())
        )
    })?;
    if !opened.is_file() || opened.len() > MAX_SETTINGS_BYTES {
        return Err("Settings file changed or exceeds its size limit.".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err("Settings file changed while being read.".to_owned());
        }
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_SETTINGS_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            format!(
                "Could not read settings at {display_path}: {}",
                sanitize(&error.to_string())
            )
        })?;
    if bytes.len() as u64 > MAX_SETTINGS_BYTES {
        return Err("Settings file exceeds its size limit.".to_owned());
    }
    Ok(Some(bytes))
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

fn sanitize(value: &str) -> String {
    strip_terminal_control_sequences(value)
}

fn print_help() {
    println!("{USAGE}");
    println!("  --scope user|project       Configuration scope (default: user)");
    println!("  --transport stdio|sse|http Auto-detected from HTTP(S) URL or stdio command");
    println!("  --env KEY=value            Set a stdio environment variable");
    println!("  --header 'Key: value'      Set a remote transport header");
    println!("  --timeout milliseconds    Set connection timeout");
    println!("  --trust                   Trust this server");
    println!("  --description text        Set the server description");
    println!("  --include-tools pattern   Include matching tools");
    println!("  --exclude-tools pattern   Exclude matching tools");
    println!("  --oauth-*                 Configure OAuth for SSE or HTTP transports");
}
