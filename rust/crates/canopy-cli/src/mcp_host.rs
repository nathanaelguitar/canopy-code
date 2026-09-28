//! Interactive CLI ownership of one MCP manager per selected session.
//!
//! The core crate owns protocol and pooling behavior. This module supplies
//! settings admission, terminal permission prompts, and the process-level
//! pool/workspace budget used by `canopy run`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use canopy_core::agent_runtime::AgentToolExecutor;
use canopy_core::config::LoadedSettings;
use canopy_core::extension_inventory::ExtensionMcpServerSource;
use canopy_core::extension_setting_helpers::{
    ExtensionSetting, validate_extension_setting_env_vars,
};
use canopy_core::mcp::config_hash::hash_mcp_server_config;
use canopy_core::mcp::oauth_utils::OAuthUtils;
#[cfg(target_os = "macos")]
use canopy_core::mcp::token_storage::KeychainTokenStorage;
use canopy_core::mcp::token_storage::{
    ConfiguredTokenStorage, EncryptedFileTokenStorage, SecretStorage,
};
use canopy_core::mcp::{McpOAuthProvider, McpOAuthProviderConfig};
use canopy_core::permissions::{
    PermissionCheckContext, PermissionDecision, PermissionRule, PermissionRuleSet, parse_rules,
};
use canopy_core::resources::resource_registry::ResourceRegistry;
use canopy_core::storage::Storage;
use canopy_core::tool_utils::is_tool_enabled;
use canopy_core::tools::mcp::agent_tool_adapter::{
    McpAgentToolAdapter, McpAgentToolAdapterError, McpAgentToolAuthorizer, McpAuthorizationFuture,
    McpAuthorizationOperation, mcp_function_name,
};
use canopy_core::tools::mcp::client_manager::{McpClientManager, McpManagerDiscoveryReport};
use canopy_core::tools::mcp::client_runtime::{
    McpClientError, McpClientRuntime, McpOAuthRecoveryState, McpOAuthTokenConfig,
    McpOAuthTokenResolver, McpRequestOptions, McpTransportBuildOptions, McpTransportFactory,
    get_valid_mcp_oauth_token,
};
use canopy_core::tools::mcp::native_transports::NativeMcpTransportFactory;
use canopy_core::tools::mcp::prompt_registry::PromptRegistry;
use canopy_core::tools::mcp::transport_pool::{McpTransportPool, McpTransportPoolOptions};
use canopy_core::tools::mcp::workspace_budget::{McpBudgetMode, WorkspaceMcpBudget};
use canopy_core::turn::ToolCallRequestInfo;
use futures_util::future::BoxFuture;
use serde::Deserialize;
use serde_json::{Map, Value, json};

const APPROVALS_FILENAME: &str = "mcpApprovals.json";
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const MCP_APPROVALS_MAX_BYTES: u64 = 1024 * 1024;
const MCP_LIST_PROBE_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const OAUTH_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);
static MCP_APPROVAL_PROMPT_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

struct CliMcpOAuthTokenResolver {
    storage: ConfiguredTokenStorage,
    http: reqwest::Client,
}

impl CliMcpOAuthTokenResolver {
    fn new() -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()
            .expect("building bounded MCP OAuth HTTP client should succeed");
        Self {
            storage: ConfiguredTokenStorage::new(
                Storage::get_mcp_oauth_tokens_path(),
                Storage::get_global_canopy_dir(),
            ),
            http,
        }
    }
}

impl McpOAuthTokenResolver for CliMcpOAuthTokenResolver {
    fn resolve_token<'a>(
        &'a self,
        server_name: &'a str,
        oauth_config: &'a Value,
    ) -> BoxFuture<'a, Result<Option<String>, String>> {
        Box::pin(async move {
            let string = |name: &str| {
                oauth_config
                    .get(name)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            };
            let strings = |name: &str| {
                oauth_config
                    .get(name)
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            };
            let config = McpOAuthTokenConfig {
                client_id: string("clientId"),
                client_secret: string("clientSecret"),
                scopes: strings("scopes"),
                audiences: strings("audiences"),
            };
            get_valid_mcp_oauth_token(
                &self.storage,
                &self.http,
                server_name,
                &config,
                unix_now_millis(),
            )
            .await
            .map_err(|error| error.to_string())
        })
    }
}

fn unix_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// MCP-specific settings taken from the same merged settings object as the
/// rest of the interactive runtime.
#[derive(Clone, Debug, Default)]
pub struct McpCliSettings {
    pub servers: Map<String, Value>,
    /// Optional `mcp.serverCommand`, admitted only for a trusted workspace
    /// outside safe and bare modes. It is parsed into the synthetic `mcp`
    /// stdio server after the configured source map has been assembled.
    mcp_server_command: Option<String>,
    /// `None` allows every configured server; an explicit empty list denies
    /// every server, matching `mcp.allowed` semantics.
    pub allowed: Option<Vec<String>>,
    pub excluded: Vec<String>,
    pub trusted_workspace: bool,
    pub approval_path_override: Option<PathBuf>,
    /// CLI-supplied servers are top-tier and are not approval-gated.
    pub ungated_server_names: HashSet<String>,
    /// Non-fatal project-file problems, reported during run startup.
    pub source_warnings: Vec<String>,
    extension_settings_by_server: HashMap<String, ExtensionSettingsSource>,
}

/// Bounded server config contributed by an active extension. The inventory
/// validates activation and trust policy but never connects to the server or
/// creates plugin data directories; each host passes these sources through
/// the normal MCP settings and connection lifecycle.
#[derive(Clone, Debug, Default)]
pub struct ExtensionMcpSource {
    pub extension_id: String,
    pub extension_name: String,
    pub install_slot: PathBuf,
    pub settings: Vec<ExtensionSetting>,
    pub servers: Map<String, Value>,
}

#[derive(Clone, Debug)]
struct ExtensionSettingsSource {
    extension_id: String,
    extension_name: String,
    install_slot: PathBuf,
    settings: Vec<ExtensionSetting>,
}

impl From<ExtensionMcpServerSource> for ExtensionMcpSource {
    fn from(source: ExtensionMcpServerSource) -> Self {
        Self {
            extension_id: source.extension_id,
            extension_name: source.extension_name,
            install_slot: source.install_slot,
            settings: source.settings,
            servers: source.servers,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtensionSettingsSelector {
    version: u32,
    backend: String,
    bundle_key: String,
}

enum ExtensionSettingsSecretBackend {
    Encrypted(EncryptedFileTokenStorage),
    #[cfg(target_os = "macos")]
    Keychain(KeychainTokenStorage),
}

impl ExtensionSettingsSecretBackend {
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
                    let _ = (global_config_dir, service_name);
                    Err("Stored extension settings require the macOS keychain.".to_owned())
                }
            }
            _ => Err("Stored extension settings selector is invalid.".to_owned()),
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
                .map_err(|_| "Could not read stored extension settings.".to_owned()),
            #[cfg(target_os = "macos")]
            Self::Keychain(storage) => storage
                .get_secret(key)
                .await
                .map_err(|_| "Could not read stored extension settings.".to_owned()),
        }
    }
}

const EXTENSION_SETTINGS_SELECTOR_FILE: &str = ".canopy-extension-settings.json";
const EXTENSION_SETTINGS_BUNDLE_PREFIX: &str = "$canopy:extension-settings:v2:";
const MAX_EXTENSION_SETTINGS_FILE_BYTES: u64 = 1024 * 1024;
const MAX_EXTENSION_SETTINGS_SELECTOR_BYTES: u64 = 64 * 1024;
const MAX_EXTENSION_SETTINGS_BUNDLE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum McpListApprovalState {
    Approved,
    Rejected,
    Pending,
}

/// Bounded, read-only approval snapshot used by `canopy mcp list`. Corrupt or
/// unreadable approval data must never make a gated server eligible to connect.
pub(crate) struct McpListApprovalSnapshot {
    path: PathBuf,
    config: Value,
}

impl McpListApprovalSnapshot {
    pub(crate) fn load(path_override: Option<&Path>) -> Result<Self, String> {
        let path = path_override
            .map(Path::to_path_buf)
            .unwrap_or_else(|| Storage::get_global_canopy_dir().join(APPROVALS_FILENAME));
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Self {
                    path,
                    config: Value::Object(Map::new()),
                });
            }
            Err(error) => {
                return Err(format!("could not read {}: {error}", path.display()));
            }
        };
        let mut contents = Vec::new();
        file.take(MCP_APPROVALS_MAX_BYTES + 1)
            .read_to_end(&mut contents)
            .map_err(|error| format!("could not read {}: {error}", path.display()))?;
        if contents.len() as u64 > MCP_APPROVALS_MAX_BYTES {
            return Err(format!(
                "{} exceeds the {} byte limit",
                path.display(),
                MCP_APPROVALS_MAX_BYTES
            ));
        }
        let contents = std::str::from_utf8(&contents)
            .map_err(|error| format!("{} is not valid UTF-8: {error}", path.display()))?;
        let parsed =
            serde_json::from_str::<Value>(&canopy_core::jsonc::strip_json_comments(contents))
                .map_err(|error| format!("{} is not valid JSON: {error}", path.display()))?;
        if !parsed.is_object() {
            return Err(format!("{} is not a JSON object", path.display()));
        }
        Ok(Self {
            path,
            config: parsed,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn state(
        &self,
        workspace_root: &Path,
        server_name: &str,
        server_config: &Value,
    ) -> McpListApprovalState {
        let project = resolve_project_root(workspace_root)
            .to_string_lossy()
            .into_owned();
        let Some(record) = self
            .config
            .get(project.as_str())
            .and_then(|project| project.get(server_name))
        else {
            return McpListApprovalState::Pending;
        };
        let Ok(current_hash) = hash_mcp_server_config(server_config) else {
            return McpListApprovalState::Pending;
        };
        if record.get("hash").and_then(Value::as_str) != Some(current_hash.as_str()) {
            return McpListApprovalState::Pending;
        }
        match record.get("status").and_then(Value::as_str) {
            Some("approved") => McpListApprovalState::Approved,
            Some("rejected") => McpListApprovalState::Rejected,
            _ => McpListApprovalState::Pending,
        }
    }

    pub(crate) fn set_status(
        &mut self,
        workspace_root: &Path,
        server_name: &str,
        server_config: &Value,
        status: McpApprovalStatus,
    ) -> Result<(), String> {
        let project = resolve_project_root(workspace_root)
            .to_string_lossy()
            .into_owned();
        let hash = hash_mcp_server_config(server_config)
            .map_err(|error| format!("could not hash MCP server configuration: {error}"))?;
        let root = self
            .config
            .as_object_mut()
            .ok_or_else(|| "MCP approvals data is not a JSON object".to_owned())?;
        let project_records = root
            .entry(project.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        let project_records = project_records.as_object_mut().ok_or_else(|| {
            format!("MCP approvals entry for workspace {project} is not an object")
        })?;
        project_records.insert(
            server_name.to_owned(),
            json!({
                "hash": hash,
                "status": status.as_str(),
            }),
        );
        Ok(())
    }

    pub(crate) fn to_bounded_json(&self) -> Result<Vec<u8>, String> {
        let bytes = serde_json::to_vec_pretty(&self.config)
            .map_err(|error| format!("could not serialize MCP approvals: {error}"))?;
        if bytes.len() as u64 > MCP_APPROVALS_MAX_BYTES {
            return Err(format!(
                "updated MCP approvals would exceed the {} byte limit",
                MCP_APPROVALS_MAX_BYTES
            ));
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum McpApprovalStatus {
    Approved,
    Rejected,
}

impl McpApprovalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum McpListProbeError {
    TimedOut,
    Failed,
}

/// Connect to one approved MCP server, complete initialization, ping it, and
/// always close the transport. The outer timeout also covers transport startup
/// and OAuth credential lookup, which do not all use the MCP request timeout.
pub(crate) async fn probe_mcp_server(
    server_name: &str,
    config: &Value,
    workspace_root: &Path,
    effective_env: &HashMap<String, String>,
    timeout: Duration,
) -> Result<(), McpListProbeError> {
    use canopy_core::utils::cancellation::CancellationToken;

    let recovery = Arc::new(McpOAuthRecoveryState::default());
    let factory = NativeMcpTransportFactory::new().with_oauth_recovery(Arc::clone(&recovery));
    let runtime = McpClientRuntime::new(server_name, config.clone(), Arc::new(factory));
    let build_options = McpTransportBuildOptions {
        parent_env: effective_env
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        oauth_token_resolver: Some(Arc::new(CliMcpOAuthTokenResolver::new())),
        workspace_directories: vec![workspace_root.to_path_buf()],
        ..McpTransportBuildOptions::default()
    };
    let cancellation = CancellationToken::new();
    let probe = tokio::time::timeout(timeout, async {
        runtime
            .connect(&build_options, Some(cancellation.clone()))
            .await?;
        runtime
            .request_raw(
                "ping",
                json!({}),
                McpRequestOptions {
                    timeout_ms: Some(timeout.as_millis().min(u64::MAX as u128) as u64),
                    cancellation: Some(cancellation.clone()),
                    ..McpRequestOptions::default()
                },
            )
            .await?;
        Ok::<(), McpClientError>(())
    })
    .await;

    let result = match probe {
        Ok(Ok(())) => {
            match tokio::time::timeout(MCP_LIST_PROBE_CLOSE_TIMEOUT, runtime.disconnect()).await {
                Ok(Ok(())) => Ok(()),
                _ => Err(McpListProbeError::Failed),
            }
        }
        Ok(Err(McpClientError::Timeout { .. })) | Err(_) => {
            cancellation.cancel();
            let _ = tokio::time::timeout(MCP_LIST_PROBE_CLOSE_TIMEOUT, runtime.disconnect()).await;
            Err(McpListProbeError::TimedOut)
        }
        Ok(Err(_)) => {
            cancellation.cancel();
            let _ = tokio::time::timeout(MCP_LIST_PROBE_CLOSE_TIMEOUT, runtime.disconnect()).await;
            Err(McpListProbeError::Failed)
        }
    };
    result
}

impl McpCliSettings {
    pub fn from_loaded_settings(loaded: &LoadedSettings) -> Self {
        let servers = loaded
            .merged
            .get("mcpServers")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mcp = loaded.merged.get("mcp").and_then(Value::as_object);
        let dynamic_server_command_enabled = loaded.is_trusted
            && !canopy_core::utils::safe_mode::is_safe_mode_env()
            && !canopy_core::utils::bare_mode::is_bare_mode(None);
        let mcp_server_command = dynamic_server_command_enabled
            .then(|| mcp.and_then(|settings| settings.get("serverCommand")))
            .flatten()
            .and_then(Value::as_str)
            .filter(|command| !command.is_empty())
            .map(str::to_owned);
        let allowed = mcp
            .and_then(|settings| settings.get("allowed"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|entry| !entry.is_empty())
                    .map(str::to_owned)
                    .collect()
            });
        let excluded = mcp
            .and_then(|settings| settings.get("excluded"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect();
        Self {
            servers,
            mcp_server_command,
            allowed,
            excluded,
            trusted_workspace: loaded.is_trusted,
            approval_path_override: loaded
                .runtime_environment
                .effective_env
                .get("CANOPY_CODE_MCP_APPROVALS_PATH")
                .map(PathBuf::from),
            ungated_server_names: HashSet::new(),
            source_warnings: Vec::new(),
            extension_settings_by_server: HashMap::new(),
        }
    }

    /// An explicit CLI allowlist replaces settings-derived filters, including
    /// exclusions, matching `--allowed-mcp-server-names` precedence.
    pub fn with_cli_server_allowlist(mut self, names: Option<&[String]>) -> Self {
        if let Some(names) = names {
            self.allowed = Some(names.to_vec());
            self.excluded.clear();
        }
        self
    }

    pub(crate) async fn apply_extension_settings_to_server_config<F>(
        &self,
        server_name: &str,
        config: &mut Value,
        workspace_root: &Path,
        inherited_env_contains: F,
    ) -> Result<(), String>
    where
        F: Fn(&str) -> bool,
    {
        let Some(source) = self.extension_settings_by_server.get(server_name) else {
            return Ok(());
        };
        if !is_stdio_server_config(config) {
            return Ok(());
        }
        if source.settings.is_empty() {
            return Ok(());
        }

        let values = resolve_extension_settings(source, workspace_root).await?;
        let Some(object) = config.as_object_mut() else {
            return Err(
                "extension MCP settings could not be merged with its env object".to_owned(),
            );
        };
        let env = object
            .entry("env".to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(env) = env.as_object_mut() else {
            return Err(
                "extension MCP settings could not be merged with its env object".to_owned(),
            );
        };
        for (key, value) in values {
            if !inherited_env_contains(&key) {
                env.entry(key).or_insert(Value::String(value));
            }
        }
        Ok(())
    }

    /// Merge `.mcp.json` and optional `--mcp-config` sources over the merged
    /// settings map, preserving source precedence and top-tier approval rules.
    pub fn with_project_and_cli(
        self,
        workspace_root: &Path,
        mcp_config_argument: Option<&str>,
    ) -> Result<Self, String> {
        self.with_sources(workspace_root, mcp_config_argument, None, &[])
    }

    /// Assemble session and extension sources on top of settings and project
    /// configuration. Extensions fill only names not already provided by a
    /// stronger source; session servers then override extensions, and CLI
    /// config remains the final winner.
    pub fn with_sources(
        mut self,
        workspace_root: &Path,
        mcp_config_argument: Option<&str>,
        session_servers: Option<&Map<String, Value>>,
        extension_sources: &[ExtensionMcpSource],
    ) -> Result<Self, String> {
        let project = load_project_mcp_servers(workspace_root);
        let cli = parse_cli_mcp_config(mcp_config_argument, workspace_root)?;
        let cli_names = cli
            .as_ref()
            .map(|servers| servers.keys().cloned().collect::<HashSet<_>>())
            .unwrap_or_default();
        let mut servers = merge_server_sources(&self.servers, &project.servers, None);
        let mut source_warnings = project.warnings;
        let mut ungated_server_names = HashSet::new();
        self.extension_settings_by_server.clear();

        for extension in extension_sources {
            for (name, value) in &extension.servers {
                if servers.get(name).is_some_and(json_truthy) {
                    continue;
                }
                let Some(config) = value.as_object() else {
                    source_warnings.push(format!(
                        "extension `{}` MCP server `{name}` is not an object — skipped",
                        extension.extension_name
                    ));
                    continue;
                };
                let mut config = config.clone();
                // Active inventory owns extension activation and workspace
                // trust; manifest data cannot opt into the workspace approval
                // gate or elevate tool trust.
                config.remove("scope");
                config.remove("trust");
                config.insert(
                    "extensionName".to_owned(),
                    Value::String(extension.extension_name.clone()),
                );
                servers.insert(name.clone(), Value::Object(config));
                self.extension_settings_by_server.insert(
                    name.clone(),
                    ExtensionSettingsSource {
                        extension_id: extension.extension_id.clone(),
                        extension_name: extension.extension_name.clone(),
                        install_slot: extension.install_slot.clone(),
                        settings: extension.settings.clone(),
                    },
                );
                ungated_server_names.insert(name.clone());
            }
        }

        if let Some(session_servers) = session_servers {
            for name in session_servers.keys() {
                self.extension_settings_by_server.remove(name);
            }
            servers.extend(
                session_servers
                    .iter()
                    .map(|(name, config)| (name.clone(), config.clone())),
            );
            ungated_server_names.extend(session_servers.keys().cloned());
        }
        if let Some(cli) = cli.as_ref() {
            for name in cli.keys() {
                self.extension_settings_by_server.remove(name);
            }
            servers.extend(
                cli.iter()
                    .map(|(name, config)| (name.clone(), config.clone())),
            );
        }
        ungated_server_names.extend(cli_names);
        if let Some(command) = self.mcp_server_command.as_deref() {
            let dynamic_server = parse_mcp_server_command(command, workspace_root)?;
            self.extension_settings_by_server.remove("mcp");
            servers.insert("mcp".to_owned(), Value::Object(dynamic_server));
            // The synthetic server comes from the user-level command setting,
            // so a colliding project entry cannot impose the project approval
            // gate on this transport.
            ungated_server_names.insert("mcp".to_owned());
        }
        self.servers = servers;
        self.ungated_server_names = ungated_server_names;
        self.source_warnings = source_warnings;
        Ok(self)
    }
}

const MAX_MCP_SERVER_COMMAND_BYTES: usize = 8 * 1024;
const MAX_MCP_SERVER_COMMAND_ARGS: usize = 128;
const MAX_MCP_SERVER_COMMAND_ARGV_BYTES: usize = 64 * 1024;

/// Parse the configured command into an argv-only stdio config. This mirrors
/// the useful part of `shell-quote.parse` without invoking a shell: quoting and
/// `$NAME` expansion are supported, while operators, comments, and globs are
/// rejected because the TypeScript caller rejects their non-string parse
/// entries too.
fn parse_mcp_server_command(
    command: &str,
    workspace_root: &Path,
) -> Result<Map<String, Value>, String> {
    if command.len() > MAX_MCP_SERVER_COMMAND_BYTES {
        return Err(format!(
            "mcp.serverCommand exceeds the {MAX_MCP_SERVER_COMMAND_BYTES}-byte limit"
        ));
    }
    let environment = std::env::vars()
        .filter(|(name, _)| {
            !matches!(
                name.as_str(),
                "QWEN_SERVER_TOKEN" | "QWEN_DAEMON_TOKEN" | "QWEN_CODE_PRIVATE_ACP_CAPABILITY"
            )
        })
        .collect::<HashMap<_, _>>();
    let characters = command.chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut token_started = false;
    let mut quote = None;
    let mut index = 0;

    while index < characters.len() {
        let character = characters[index];
        index += 1;
        match quote {
            Some('\'') => {
                if character == '\'' {
                    quote = None;
                } else {
                    token.push(character);
                }
            }
            Some('"') => match character {
                '"' => quote = None,
                '$' => token.push_str(&expand_mcp_command_variable(
                    &characters,
                    &mut index,
                    &environment,
                )?),
                '\\' => {
                    if let Some(next) = characters.get(index).copied() {
                        if matches!(next, '"' | '\\' | '$') {
                            token.push(next);
                            index += 1;
                        } else {
                            token.push('\\');
                            token.push(next);
                            index += 1;
                        }
                    } else {
                        token.push('\\');
                    }
                }
                _ => token.push(character),
            },
            Some(_) => token.push(character),
            None => match character {
                '\'' | '"' => {
                    quote = Some(character);
                    token_started = true;
                }
                value if value.is_whitespace() => {
                    if token_started {
                        push_mcp_command_token(&mut tokens, &mut token)?;
                        token_started = false;
                    }
                }
                '\\' => {
                    let Some(next) = characters.get(index).copied() else {
                        return Err("mcp.serverCommand ends with an incomplete escape".to_owned());
                    };
                    if matches!(next, '*' | '?') {
                        return Err("mcp.serverCommand must not contain glob patterns".to_owned());
                    }
                    token.push(next);
                    token_started = true;
                    index += 1;
                }
                '$' => {
                    token.push_str(&expand_mcp_command_variable(
                        &characters,
                        &mut index,
                        &environment,
                    )?);
                    token_started = true;
                }
                '*' | '?' => {
                    return Err("mcp.serverCommand must not contain glob patterns".to_owned());
                }
                '#' | '|' | '&' | ';' | '(' | ')' | '<' | '>' => {
                    return Err(
                        "mcp.serverCommand must be a command and arguments, without shell operators or comments"
                            .to_owned(),
                    );
                }
                value => {
                    token.push(value);
                    token_started = true;
                }
            },
        }
    }

    if quote.is_some() {
        return Err("mcp.serverCommand contains an unterminated quote".to_owned());
    }
    if token_started {
        push_mcp_command_token(&mut tokens, &mut token)?;
    }
    if tokens.is_empty() || tokens[0].is_empty() {
        return Err("mcp.serverCommand does not specify an executable".to_owned());
    }

    let mut server = Map::new();
    server.insert("command".to_owned(), Value::String(tokens.remove(0)));
    server.insert(
        "args".to_owned(),
        Value::Array(tokens.into_iter().map(Value::String).collect()),
    );
    server.insert(
        "cwd".to_owned(),
        Value::String(workspace_root.to_string_lossy().into_owned()),
    );
    Ok(server)
}

fn push_mcp_command_token(tokens: &mut Vec<String>, token: &mut String) -> Result<(), String> {
    if tokens.len() >= MAX_MCP_SERVER_COMMAND_ARGS {
        return Err(format!(
            "mcp.serverCommand exceeds the {MAX_MCP_SERVER_COMMAND_ARGS}-argument limit"
        ));
    }
    if token.contains('\0') {
        return Err("mcp.serverCommand contains a NUL byte".to_owned());
    }
    let existing_bytes = tokens.iter().map(String::len).sum::<usize>();
    if existing_bytes.saturating_add(token.len()) > MAX_MCP_SERVER_COMMAND_ARGV_BYTES {
        return Err(format!(
            "expanded mcp.serverCommand arguments exceed the {MAX_MCP_SERVER_COMMAND_ARGV_BYTES}-byte limit"
        ));
    }
    tokens.push(std::mem::take(token));
    Ok(())
}

fn expand_mcp_command_variable(
    characters: &[char],
    index: &mut usize,
    environment: &HashMap<String, String>,
) -> Result<String, String> {
    let Some(first) = characters.get(*index).copied() else {
        return Ok("$".to_owned());
    };
    if first == '{' {
        *index += 1;
        let start = *index;
        while *index < characters.len() && characters[*index] != '}' {
            *index += 1;
        }
        if *index == characters.len() || *index == start {
            return Err("mcp.serverCommand contains an invalid ${...} expansion".to_owned());
        }
        let name = characters[start..*index].iter().collect::<String>();
        *index += 1;
        return Ok(environment.get(&name).cloned().unwrap_or_default());
    }

    if matches!(first, '*' | '@' | '#' | '?' | '$' | '!' | '_' | '-') {
        *index += 1;
        return Ok(environment
            .get(&first.to_string())
            .cloned()
            .unwrap_or_default());
    }

    let start = *index;
    while *index < characters.len()
        && (characters[*index].is_ascii_alphanumeric() || characters[*index] == '_')
    {
        *index += 1;
    }
    if *index == start {
        return Ok(String::new());
    }
    let name = characters[start..*index].iter().collect::<String>();
    Ok(environment.get(&name).cloned().unwrap_or_default())
}

fn is_stdio_server_config(config: &Value) -> bool {
    let Some(object) = config.as_object() else {
        return false;
    };
    if object.get("type").and_then(Value::as_str) == Some("sdk")
        || matches!(
            object.get("authProviderType").and_then(Value::as_str),
            Some("service_account_impersonation" | "google_credentials")
        )
    {
        return false;
    }
    let has_network_endpoint = ["httpUrl", "url", "tcp"].iter().any(|name| {
        object
            .get(*name)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
    });
    !has_network_endpoint
        && object
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|command| !command.is_empty())
}

async fn resolve_extension_settings(
    source: &ExtensionSettingsSource,
    workspace_root: &Path,
) -> Result<HashMap<String, String>, String> {
    validate_extension_setting_env_vars(Some(&source.settings))?;
    if source.settings.is_empty() {
        return Ok(HashMap::new());
    }

    let global_config_dir = Storage::get_global_canopy_dir();
    let user_service = format!(
        "Canopy Code Extensions {} {}",
        source.extension_name, source.extension_id
    );
    let user_selector = read_extension_settings_selector(&source.install_slot)?;
    let mut values = read_scoped_extension_settings(
        &source.settings,
        &source.install_slot.join(".env"),
        &user_service,
        user_selector.as_ref(),
        &global_config_dir,
    )
    .await?;
    let workspace_service = format!("{user_service} {}", workspace_root.display());
    let workspace_values = read_scoped_extension_settings(
        &source.settings,
        &workspace_root.join(".env"),
        &workspace_service,
        None,
        &global_config_dir,
    )
    .await?;
    values.extend(workspace_values);
    Ok(values)
}

async fn read_scoped_extension_settings(
    settings: &[ExtensionSetting],
    env_path: &Path,
    service_name: &str,
    selector: Option<&ExtensionSettingsSelector>,
    global_config_dir: &Path,
) -> Result<HashMap<String, String>, String> {
    let mut values = read_extension_env_file(env_path)?;
    let sensitive = settings
        .iter()
        .filter(|setting| setting.is_sensitive())
        .collect::<Vec<_>>();
    if sensitive.is_empty() {
        return Ok(values);
    }

    let storage = match selector {
        Some(selector) => ExtensionSettingsSecretBackend::selected(
            &selector.backend,
            global_config_dir,
            service_name,
        )?,
        None => ExtensionSettingsSecretBackend::default_for(global_config_dir, service_name).await,
    };
    let bundle = if let Some(selector) = selector {
        let contents = storage
            .get_secret(&selector.bundle_key)
            .await?
            .ok_or_else(|| "Stored extension settings bundle is missing.".to_owned())?;
        if contents.len() > MAX_EXTENSION_SETTINGS_BUNDLE_BYTES {
            return Err("Stored extension settings bundle exceeds the 1 MiB limit.".to_owned());
        }
        let parsed: Value = serde_json::from_str(&contents)
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

    for setting in sensitive {
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

fn read_extension_settings_selector(
    install_slot: &Path,
) -> Result<Option<ExtensionSettingsSelector>, String> {
    let path = install_slot.join(EXTENSION_SETTINGS_SELECTOR_FILE);
    let Some(bytes) = read_extension_settings_file(&path, MAX_EXTENSION_SETTINGS_SELECTOR_BYTES)?
    else {
        return Ok(None);
    };
    let selector: ExtensionSettingsSelector = serde_json::from_slice(&bytes)
        .map_err(|_| "Stored extension settings selector is invalid.".to_owned())?;
    if selector.version != 1
        || !selector
            .bundle_key
            .starts_with(EXTENSION_SETTINGS_BUNDLE_PREFIX)
        || !matches!(selector.backend.as_str(), "keychain" | "encrypted_file")
    {
        return Err("Stored extension settings selector is invalid.".to_owned());
    }
    Ok(Some(selector))
}

fn read_extension_env_file(path: &Path) -> Result<HashMap<String, String>, String> {
    let Some(bytes) = read_extension_settings_file(path, MAX_EXTENSION_SETTINGS_FILE_BYTES)? else {
        return Ok(HashMap::new());
    };
    let contents = std::str::from_utf8(&bytes)
        .map_err(|_| "Extension settings file is not valid UTF-8.".to_owned())?;
    Ok(canopy_core::utils::dotenv::parse_dotenv(contents))
}

fn read_extension_settings_file(path: &Path, limit: u64) -> Result<Option<Vec<u8>>, String> {
    let path_metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("Could not inspect extension settings file.".to_owned()),
    };
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err("Extension settings file is not a regular file.".to_owned());
    }
    if path_metadata.len() > limit {
        return Err(format!(
            "Extension settings file exceeds the {limit}-byte limit."
        ));
    }
    let file =
        File::open(path).map_err(|_| "Could not read extension settings file.".to_owned())?;
    let metadata = file
        .metadata()
        .map_err(|_| "Could not inspect extension settings file.".to_owned())?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err("Extension settings file changed or exceeds its size limit.".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != metadata.dev() || path_metadata.ino() != metadata.ino() {
            return Err("Extension settings file changed while being read.".to_owned());
        }
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Could not read extension settings file.".to_owned())?;
    if bytes.len() as u64 > limit {
        return Err(format!(
            "Extension settings file exceeds the {limit}-byte limit."
        ));
    }
    Ok(Some(bytes))
}

/// A process-level MCP pool and budget. Each selected session receives a
/// distinct manager while compatible transports and the workspace budget are
/// shared through this host.
pub struct McpCliWorkspace {
    workspace_root: PathBuf,
    effective_env: BTreeMap<String, String>,
    pool: McpTransportPool,
    budget: Arc<Mutex<WorkspaceMcpBudget>>,
    oauth_recovery: Option<Arc<McpOAuthRecoveryState>>,
    allow_automatic_oauth: bool,
}

impl McpCliWorkspace {
    pub fn new_native(
        workspace_root: impl Into<PathBuf>,
        effective_env: &HashMap<String, String>,
    ) -> Self {
        let oauth_recovery = Arc::new(McpOAuthRecoveryState::default());
        let factory =
            NativeMcpTransportFactory::new().with_oauth_recovery(Arc::clone(&oauth_recovery));
        Self::with_factory_and_recovery(
            workspace_root,
            Arc::new(factory),
            effective_env,
            Some(oauth_recovery),
        )
    }

    /// Injection point used by host tests and alternate transport hosts.
    pub fn with_factory(
        workspace_root: impl Into<PathBuf>,
        factory: Arc<dyn McpTransportFactory>,
        effective_env: &HashMap<String, String>,
    ) -> Self {
        Self::with_factory_and_recovery(workspace_root, factory, effective_env, None)
    }

    fn with_factory_and_recovery(
        workspace_root: impl Into<PathBuf>,
        factory: Arc<dyn McpTransportFactory>,
        effective_env: &HashMap<String, String>,
        oauth_recovery: Option<Arc<McpOAuthRecoveryState>>,
    ) -> Self {
        let workspace_root = workspace_root.into();
        let budget = Arc::new(Mutex::new(workspace_budget_from_env(effective_env)));
        let effective_env = effective_env
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        let build_options = McpTransportBuildOptions {
            parent_env: effective_env.clone(),
            oauth_token_resolver: Some(Arc::new(CliMcpOAuthTokenResolver::new())),
            workspace_directories: vec![workspace_root.clone()],
            ..McpTransportBuildOptions::default()
        };
        let pool =
            McpTransportPool::new(factory, build_options, McpTransportPoolOptions::default());
        Self {
            workspace_root,
            effective_env,
            pool,
            budget,
            oauth_recovery,
            allow_automatic_oauth: false,
        }
    }

    /// Enable the CLI's automatic browser login path. Hosts without a user
    /// terminal, including ACP, leave this disabled and retain stored-token
    /// resolution without opening a browser.
    pub fn with_automatic_oauth(mut self, enabled: bool) -> Self {
        self.allow_automatic_oauth = enabled;
        self
    }

    /// Start discovery only after the caller has selected the durable session
    /// ID. The exact ID is passed through to the manager/pool and is never
    /// regenerated or replaced here.
    pub async fn open_session(
        &self,
        session_id: impl Into<String>,
        settings: &McpCliSettings,
        permissions: PermissionRuleSet,
        core_tools: Option<Vec<String>>,
        excluded_tools: Vec<String>,
        prompt: Arc<dyn McpCliApprovalPrompt>,
    ) -> Result<McpCliSession, McpAgentToolAdapterError> {
        let session_id = session_id.into();
        let server_trust = settings
            .servers
            .iter()
            .filter_map(|(name, config)| {
                config
                    .get("trust")
                    .and_then(Value::as_bool)
                    .map(|trust| (name.clone(), trust))
            })
            .collect();
        let skill_grants = McpSessionSkillGrants::default();
        let authorizer = Arc::new(CliMcpAuthorizer {
            permissions: permissions.clone(),
            skill_grants: skill_grants.clone(),
            workspace_root: self.workspace_root.clone(),
            trusted_workspace: settings.trusted_workspace,
            server_trust,
            core_tools,
            excluded_tools,
            prompt,
        });
        let manager = Arc::new(
            McpClientManager::new(self.pool.clone(), session_id.clone())
                .with_workspace_budget(Arc::clone(&self.budget)),
        );
        let adapter = Arc::new(McpAgentToolAdapter::new(manager, authorizer));
        let (mut servers, mut skipped) = admitted_servers(
            &settings.servers,
            settings.allowed.as_deref(),
            &settings.excluded,
            settings.trusted_workspace,
            &settings.ungated_server_names,
            &self.workspace_root,
            settings.approval_path_override.as_deref(),
        );
        let mut disabled_extension_servers = Vec::new();
        for (name, config) in &mut servers {
            if !settings.extension_settings_by_server.contains_key(name)
                || !is_stdio_server_config(config)
            {
                continue;
            }
            if let Err(error) = settings
                .apply_extension_settings_to_server_config(
                    name,
                    config,
                    &self.workspace_root,
                    |key| self.effective_env.contains_key(key),
                )
                .await
            {
                skipped.push((
                    name.clone(),
                    format!("extension settings could not be loaded: {error}"),
                ));
                disabled_extension_servers.push(name.clone());
            }
        }
        for name in disabled_extension_servers {
            servers.remove(&name);
        }
        let mut report = adapter.discover_all(&servers).await?;
        if self.allow_automatic_oauth {
            if let Some(recovery) = self.oauth_recovery.as_ref() {
                let mut challenged = report
                    .errors
                    .iter()
                    .filter_map(|(name, error)| {
                        let config = servers.get(name)?;
                        let challenge = recovery.challenge(name, config);
                        let configured_oauth_is_missing =
                            config.pointer("/oauth/enabled").and_then(Value::as_bool) == Some(true)
                                && error.contains("requires OAuth authentication");
                        if challenge.is_none() && !configured_oauth_is_missing {
                            return None;
                        }
                        automatic_oauth_is_allowed(config)
                            .then(|| (name.clone(), config.clone(), challenge.unwrap_or_default()))
                    })
                    .collect::<Vec<_>>();
                challenged.sort_by(|left, right| left.0.cmp(&right.0));

                for (name, config, challenge) in challenged {
                    eprintln!(
                        "MCP server `{name}` requires OAuth; starting browser authorization."
                    );
                    match authenticate_mcp_oauth(&name, &config, &challenge).await {
                        Ok(()) => {
                            enable_oauth_for_retry(&mut servers, &name);
                            report = adapter.discover_all(&servers).await?;
                        }
                        Err(error) => {
                            let previous = report.errors.get(&name).cloned().unwrap_or_default();
                            report.errors.insert(
                                name.clone(),
                                format!(
                                    "{previous}; automatic OAuth authorization failed: {error}"
                                ),
                            );
                        }
                    }
                }
            }
        }
        Ok(McpCliSession {
            session_id,
            adapter,
            report,
            skipped,
            permissions,
            skill_grants,
            workspace_root: self.workspace_root.clone(),
        })
    }

    /// Bound the last transport close during normal interactive shutdown.
    /// Managers release their own references first; pooled entries still in
    /// use by another session remain accounted for until that session exits.
    pub async fn shutdown(&self) {
        let _ = self.pool.drain_all(DEFAULT_DRAIN_TIMEOUT).await;
    }

    pub fn budget(&self) -> &Arc<Mutex<WorkspaceMcpBudget>> {
        &self.budget
    }
}

fn automatic_oauth_is_allowed(config: &Value) -> bool {
    let auth_provider = config
        .get("authProviderType")
        .and_then(Value::as_str)
        .unwrap_or("dynamic_discovery");
    if auth_provider != "dynamic_discovery" {
        return false;
    }
    let has_explicit_authorization = config
        .get("headers")
        .and_then(Value::as_object)
        .is_some_and(|headers| {
            headers
                .keys()
                .any(|name| name.eq_ignore_ascii_case("authorization"))
        });
    if has_explicit_authorization {
        return false;
    }
    let has_http_url = config
        .get("httpUrl")
        .and_then(Value::as_str)
        .is_some_and(|url| !url.is_empty());
    let oauth_enabled = config.pointer("/oauth/enabled").and_then(Value::as_bool) == Some(true);
    has_http_url || oauth_enabled
}

fn enable_oauth_for_retry(servers: &mut Map<String, Value>, server_name: &str) {
    let Some(server) = servers.get_mut(server_name).and_then(Value::as_object_mut) else {
        return;
    };
    let mut oauth = server
        .get("oauth")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    oauth.insert("enabled".to_owned(), Value::Bool(true));
    server.insert("oauth".to_owned(), Value::Object(oauth));
}

async fn authenticate_mcp_oauth(
    server_name: &str,
    server_config: &Value,
    challenge: &str,
) -> Result<(), String> {
    let server_url = server_config
        .get("httpUrl")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .or_else(|| {
            server_config
                .get("url")
                .and_then(Value::as_str)
                .filter(|url| !url.is_empty())
        })
        .map(str::to_owned);
    let mut oauth_config: McpOAuthProviderConfig = server_config
        .get("oauth")
        .cloned()
        .filter(Value::is_object)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid MCP OAuth configuration: {error}"))?
        .unwrap_or_default();
    oauth_config.enabled = Some(true);

    if oauth_config.authorization_url.is_none() || oauth_config.token_url.is_none() {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(OAUTH_DISCOVERY_TIMEOUT)
            .build()
            .map_err(|error| format!("could not configure OAuth discovery HTTP client: {error}"))?;
        let mut discovered = if challenge.is_empty() {
            None
        } else {
            OAuthUtils::discover_oauth_from_www_authenticate(&http, challenge)
                .await
                .ok()
                .flatten()
        };
        if discovered.is_none()
            && let Some(url) = server_url.as_deref()
        {
            discovered = OAuthUtils::discover_oauth_config(&http, url).await;
        }
        if let Some(discovered) = discovered {
            if oauth_config.authorization_url.is_none() {
                oauth_config.authorization_url = Some(discovered.authorization_url);
            }
            if oauth_config.token_url.is_none() {
                oauth_config.token_url = Some(discovered.token_url);
            }
            if oauth_config.registration_url.is_none() {
                oauth_config.registration_url = discovered.registration_url;
            }
            if oauth_config.scopes.is_empty() {
                oauth_config.scopes = discovered.scopes;
            }
        }
    }

    let provider = McpOAuthProvider::new().map_err(|error| error.to_string())?;
    let storage = ConfiguredTokenStorage::new(
        Storage::get_mcp_oauth_tokens_path(),
        Storage::get_global_canopy_dir(),
    );
    provider
        .authenticate(&storage, server_name.to_owned(), oauth_config, server_url)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

pub struct McpCliSession {
    pub session_id: String,
    adapter: Arc<McpAgentToolAdapter>,
    report: McpManagerDiscoveryReport,
    skipped: Vec<(String, String)>,
    permissions: PermissionRuleSet,
    skill_grants: McpSessionSkillGrants,
    workspace_root: PathBuf,
}

/// Dynamic session allow rules contributed by invoked skills. Keeping these
/// separate from configured rules preserves deny/ask precedence.
#[derive(Clone, Default)]
pub struct McpSessionSkillGrants {
    rules: Arc<Mutex<Vec<PermissionRule>>>,
}

impl McpSessionSkillGrants {
    pub fn apply_allowed_tools(&self, allowed_tools: Option<&[String]>) {
        let Some(allowed_tools) = allowed_tools else {
            return;
        };
        let rules = parse_rules(allowed_tools.iter().cloned());
        self.rules
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(rules);
    }

    fn decision(&self, context: &PermissionCheckContext<'_>) -> PermissionDecision {
        let rules = self
            .rules
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if rules.is_empty() {
            return PermissionDecision::Default;
        }
        PermissionRuleSet {
            allow: rules,
            ..PermissionRuleSet::default()
        }
        .evaluate(context)
    }
}

impl McpCliSession {
    /// Build the session's discovered prompt/resource registries for native
    /// prompt-reference resolution. These are snapshots from the same
    /// successful discovery pass used to build the MCP tool declarations.
    pub fn reference_registries(&self) -> (ResourceRegistry, PromptRegistry) {
        let mut resources = ResourceRegistry::new();
        let mut prompts = PromptRegistry::new();
        for snapshot in self.report.snapshots.values() {
            for resource in &snapshot.resources {
                resources.register_resource(resource.clone());
            }
            for prompt in &snapshot.prompts {
                prompts.register_prompt(prompt.clone());
            }
        }
        (resources, prompts)
    }

    /// Access the manager whose connections were established for this session.
    pub fn manager(&self) -> &Arc<McpClientManager> {
        self.adapter.manager()
    }

    pub fn skill_grants(&self) -> McpSessionSkillGrants {
        self.skill_grants.clone()
    }

    pub fn append_function_declarations(
        &self,
        declarations: &mut Vec<Value>,
        core_tools: Option<&[String]>,
        excluded_tools: &[String],
    ) {
        let mut group = self.adapter.function_declaration_group();
        let Some(functions) = group
            .get_mut("functionDeclarations")
            .and_then(Value::as_array_mut)
        else {
            return;
        };
        functions.retain(|declaration| {
            let Some(name) = declaration.get("name").and_then(Value::as_str) else {
                return false;
            };
            self.tool_is_enabled(name, core_tools, excluded_tools)
                && self.permission_does_not_deny(name, declaration)
        });
        if !functions.is_empty() {
            declarations.push(group);
        }
    }

    fn permission_aliases(&self, name: &str) -> Vec<String> {
        if name == "read_mcp_resource" {
            return vec!["ReadMcpResource".to_owned()];
        }
        self.report
            .snapshots
            .values()
            .flat_map(|snapshot| snapshot.tools.iter())
            .find(|tool| mcp_function_name(&tool.server_name, &tool.name) == name)
            .map(|tool| legacy_mcp_name(&tool.server_name, &tool.name))
            .filter(|legacy| legacy.as_str() != name)
            .into_iter()
            .collect()
    }

    fn tool_is_enabled(
        &self,
        name: &str,
        core_tools: Option<&[String]>,
        excluded_tools: &[String],
    ) -> bool {
        mcp_tool_is_enabled(
            name,
            &self.permission_aliases(name),
            core_tools,
            excluded_tools,
        )
    }

    fn permission_does_not_deny(&self, name: &str, _declaration: &Value) -> bool {
        let aliases = self.permission_aliases(name);
        std::iter::once(name)
            .chain(aliases.iter().map(String::as_str))
            .all(|candidate| {
                self.permissions.evaluate(&PermissionCheckContext {
                    tool_name: candidate,
                    command: None,
                    file_path: None,
                    domain: None,
                    specifier: None,
                    tool_params: None,
                    project_root: &self.workspace_root,
                    cwd: &self.workspace_root,
                }) != PermissionDecision::Deny
            })
    }

    pub fn discovery_errors(&self) -> &HashMap<String, String> {
        &self.report.errors
    }

    pub fn skipped_servers(&self) -> &[(String, String)] {
        &self.skipped
    }

    pub fn compose<E: AgentToolExecutor>(
        &self,
        fallback: E,
    ) -> canopy_core::tools::mcp::agent_tool_adapter::McpComposedToolExecutor<E> {
        self.adapter.compose(fallback)
    }

    /// Release the manager's session references. The workspace host performs
    /// bounded pool draining after all active sessions are done.
    pub fn stop(&self) {
        self.adapter.manager().stop();
    }
}

/// A mockable boundary for the terminal confirmation prompt.
pub trait McpCliApprovalPrompt: Send + Sync {
    fn is_available(&self) -> bool;
    fn confirm(&self, prompt: &str) -> Result<bool, String>;
}

pub struct TerminalMcpApprovalPrompt;

impl McpCliApprovalPrompt for TerminalMcpApprovalPrompt {
    fn is_available(&self) -> bool {
        io::stdin().is_terminal() && io::stderr().is_terminal()
    }

    fn confirm(&self, prompt: &str) -> Result<bool, String> {
        if !self.is_available() {
            return Ok(false);
        }
        eprintln!("{prompt}");
        eprint!("Allow this MCP operation? [y/N] ");
        io::stderr()
            .flush()
            .map_err(|error| format!("could not display MCP approval: {error}"))?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .map_err(|error| format!("could not read MCP approval: {error}"))?;
        Ok(matches!(
            answer.trim().to_ascii_lowercase().as_str(),
            "y" | "yes"
        ))
    }
}

struct CliMcpAuthorizer {
    permissions: PermissionRuleSet,
    skill_grants: McpSessionSkillGrants,
    workspace_root: PathBuf,
    trusted_workspace: bool,
    server_trust: HashMap<String, bool>,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
    prompt: Arc<dyn McpCliApprovalPrompt>,
}

impl McpAgentToolAuthorizer for CliMcpAuthorizer {
    fn authorize<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        operation: McpAuthorizationOperation,
    ) -> McpAuthorizationFuture<'a> {
        Box::pin(async move {
            let (tool_name, legacy_name, server_name, trust, specifier) = match operation {
                McpAuthorizationOperation::ToolCall {
                    server_name,
                    tool_name,
                    trust,
                    ..
                } => {
                    let registered = mcp_function_name(&server_name, &tool_name);
                    let legacy = legacy_mcp_name(&server_name, &tool_name);
                    (registered, legacy, server_name, trust, None)
                }
                McpAuthorizationOperation::ResourceRead { server_name, .. } => {
                    let trust = self.server_trust.get(&server_name).copied();
                    let specifier = Some(server_name.clone());
                    (
                        "read_mcp_resource".to_owned(),
                        "ReadMcpResource".to_owned(),
                        server_name,
                        trust,
                        specifier,
                    )
                }
            };
            let aliases = (legacy_name != tool_name)
                .then(|| vec![legacy_name.clone()])
                .unwrap_or_default();
            if !mcp_tool_is_enabled(
                &tool_name,
                &aliases,
                self.core_tools.as_deref(),
                &self.excluded_tools,
            ) {
                return Err(format!(
                    "Tool `{tool_name}` is disabled by configured core/excluded tool settings."
                ));
            }
            let args = call.args.clone();
            let mut decision = PermissionDecision::Default;
            for candidate in [&tool_name, &legacy_name] {
                let context = PermissionCheckContext {
                    tool_name: candidate,
                    command: None,
                    file_path: None,
                    domain: None,
                    specifier: specifier.as_deref(),
                    tool_params: Some(&args),
                    project_root: &self.workspace_root,
                    cwd: &self.workspace_root,
                };
                let mut candidate_decision = self.permissions.evaluate(&context);
                if candidate_decision == PermissionDecision::Default {
                    candidate_decision = self.skill_grants.decision(&context);
                }
                if candidate_decision == PermissionDecision::Deny {
                    return Err(format!(
                        "MCP operation `{tool_name}` on server `{server_name}` blocked by a permissions.deny rule."
                    ));
                }
                if candidate_decision == PermissionDecision::Ask {
                    decision = PermissionDecision::Ask;
                } else if candidate_decision == PermissionDecision::Allow
                    && decision == PermissionDecision::Default
                {
                    decision = PermissionDecision::Allow;
                }
            }
            if decision == PermissionDecision::Default {
                decision = if trust == Some(true) && self.trusted_workspace {
                    PermissionDecision::Allow
                } else {
                    // MCP's source default is ask, including resource reads.
                    PermissionDecision::Ask
                };
            }
            match decision {
                PermissionDecision::Deny => Err(format!(
                    "MCP operation `{tool_name}` on server `{server_name}` blocked by a permissions.deny rule."
                )),
                PermissionDecision::Allow => Ok(()),
                PermissionDecision::Ask => {
                    let _prompt_guard = MCP_APPROVAL_PROMPT_LOCK
                        .get_or_init(|| tokio::sync::Mutex::new(()))
                        .lock()
                        .await;
                    if !self.prompt.is_available() {
                        return Err(format!(
                            "MCP operation `{tool_name}` requires terminal approval; no interactive TTY is available."
                        ));
                    }
                    let prompt = format!(
                        "MCP server `{server_name}` requests `{tool_name}` with arguments: {}",
                        compact_json(&args)
                    );
                    match self.prompt.confirm(&prompt)? {
                        true => Ok(()),
                        false => Err(format!(
                            "MCP operation `{tool_name}` on server `{server_name}` was not approved."
                        )),
                    }
                }
                PermissionDecision::Default => unreachable!("MCP defaults are converted to ask"),
            }
        })
    }
}

fn compact_json(value: &Value) -> String {
    let encoded = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_owned());
    const MAX_BYTES: usize = 1_000;
    if encoded.len() <= MAX_BYTES {
        encoded
    } else {
        let mut end = MAX_BYTES;
        while !encoded.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &encoded[..end])
    }
}

fn legacy_mcp_name(server_name: &str, tool_name: &str) -> String {
    let raw = format!("mcp__{server_name}__{tool_name}");
    let mut legacy = raw
        .encode_utf16()
        .map(|unit| {
            if matches!(unit, 45 | 46 | 48..=57 | 65..=90 | 95 | 97..=122) {
                char::from_u32(u32::from(unit)).unwrap_or('_')
            } else {
                '_'
            }
        })
        .collect::<String>();
    if legacy.len() > 63 {
        legacy = format!("{}___{}", &legacy[..28], &legacy[legacy.len() - 32..]);
    }
    legacy
}

fn mcp_tool_is_enabled(
    name: &str,
    aliases: &[String],
    core_tools: Option<&[String]>,
    excluded_tools: &[String],
) -> bool {
    let is_core_enabled = core_tools.is_none_or(|entries| {
        entries.iter().all(|entry| entry.trim().is_empty())
            || is_tool_enabled(name, Some(entries), None)
            || aliases
                .iter()
                .any(|alias| is_tool_enabled(alias, Some(entries), None))
    });
    let is_not_excluded = is_tool_enabled(name, None, Some(excluded_tools))
        && aliases
            .iter()
            .all(|alias| is_tool_enabled(alias, None, Some(excluded_tools)));
    is_core_enabled && is_not_excluded
}

struct ProjectMcpSource {
    servers: Map<String, Value>,
    warnings: Vec<String>,
}

fn load_project_mcp_servers(workspace_root: &Path) -> ProjectMcpSource {
    let path = workspace_root.join(".mcp.json");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        // Missing and unreadable project files are both non-fatal in the CLI.
        return ProjectMcpSource {
            servers: Map::new(),
            warnings: Vec::new(),
        };
    };
    let parsed =
        match serde_json::from_str::<Value>(&canopy_core::jsonc::strip_json_comments(&contents)) {
            Ok(parsed) => parsed,
            Err(error) => {
                return ProjectMcpSource {
                    servers: Map::new(),
                    warnings: vec![format!("Failed to parse {}: {error}", path.display())],
                };
            }
        };
    let Some(servers) = parsed.get("mcpServers").and_then(Value::as_object) else {
        return ProjectMcpSource {
            servers: Map::new(),
            warnings: vec![format!("{} has no \"mcpServers\" object", path.display())],
        };
    };

    let mut normalized = Map::new();
    let mut warnings = Vec::new();
    for (name, config) in servers {
        let Some(config_object) = config.as_object() else {
            warnings.push(format!(
                "{}: server \"{name}\" is not an object — skipped",
                path.display()
            ));
            continue;
        };
        let mut config = normalize_project_mcp_server(config_object);
        config.insert("scope".to_owned(), Value::String("project".to_owned()));
        normalized.insert(name.clone(), Value::Object(config));
    }
    ProjectMcpSource {
        servers: normalized,
        warnings,
    }
}

fn normalize_project_mcp_server(config: &Map<String, Value>) -> Map<String, Value> {
    let mut normalized = config.clone();
    let server_type = config.get("type").and_then(Value::as_str);
    let has_primary_transport = ["command", "httpUrl", "tcp"]
        .iter()
        .any(|field| config.get(*field).is_some_and(json_truthy));

    if has_primary_transport {
        let preserve_type = match config.get("type") {
            None => true,
            Some(Value::String(value)) if value == "sdk" => true,
            _ => false,
        };
        if !preserve_type {
            normalized.remove("type");
        }
    } else if let Some(url) = config.get("url").and_then(Value::as_str) {
        normalized.remove("url");
        if server_type != Some("sdk") {
            normalized.remove("type");
        }
        let field = if server_type == Some("http") {
            "httpUrl"
        } else {
            "url"
        };
        normalized.insert(field.to_owned(), Value::String(url.to_owned()));
    }
    normalized
}

fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn parse_cli_mcp_config(
    argument: Option<&str>,
    workspace_root: &Path,
) -> Result<Option<Map<String, Value>>, String> {
    let Some(argument) = argument.filter(|argument| !argument.is_empty()) else {
        return Ok(None);
    };
    let path = {
        let candidate = Path::new(argument);
        if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            workspace_root.join(candidate)
        }
    };
    let (contents, is_file) = if path.exists() {
        let contents = std::fs::read_to_string(&path).map_err(|error| {
            format!("Invalid MCP configuration provided via --mcp-config: {error}")
        })?;
        (contents, true)
    } else {
        (argument.to_owned(), false)
    };
    let source = if is_file {
        canopy_core::jsonc::strip_json_comments(&contents)
    } else {
        contents
    };
    let parsed = serde_json::from_str::<Value>(&source)
        .map_err(|error| format!("Invalid MCP configuration provided via --mcp-config: {error}"))?;
    let servers = parsed
        .as_object()
        .and_then(|object| object.get("mcpServers"))
        .filter(|servers| servers.is_object() || servers.is_array() || servers.is_null())
        .unwrap_or(&parsed);
    let Some(servers) = servers.as_object() else {
        return Err(
            "Invalid MCP configuration provided via --mcp-config: Invalid MCP server configuration format. Expected an object with server names as keys."
                .to_owned(),
        );
    };
    if servers
        .values()
        .any(|server| server.is_null() || !(server.is_object() || server.is_array()))
    {
        return Err(
            "Invalid MCP configuration provided via --mcp-config: Invalid MCP server configuration format. Expected an object with server names as keys."
                .to_owned(),
        );
    }
    Ok(Some(servers.clone()))
}

fn merge_server_sources(
    settings: &Map<String, Value>,
    project: &Map<String, Value>,
    cli: Option<&Map<String, Value>>,
) -> Map<String, Value> {
    let mut below_project = Map::new();
    let mut above_project = Map::new();
    for (name, config) in settings {
        if matches!(
            config.get("scope").and_then(Value::as_str),
            Some("workspace" | "system")
        ) {
            above_project.insert(name.clone(), config.clone());
        } else {
            below_project.insert(name.clone(), config.clone());
        }
    }
    let mut merged = below_project;
    merged.extend(
        project
            .iter()
            .map(|(name, config)| (name.clone(), config.clone())),
    );
    merged.extend(above_project);
    if let Some(cli) = cli {
        merged.extend(
            cli.iter()
                .map(|(name, config)| (name.clone(), config.clone())),
        );
    }
    merged
}

fn admitted_servers(
    configured: &Map<String, Value>,
    allowed: Option<&[String]>,
    excluded: &[String],
    trusted_workspace: bool,
    ungated_server_names: &HashSet<String>,
    workspace_root: &Path,
    approval_path_override: Option<&Path>,
) -> (Map<String, Value>, Vec<(String, String)>) {
    let mut admitted = Map::new();
    let mut skipped = Vec::new();
    if !trusted_workspace {
        skipped.extend(configured.keys().map(|name| {
            (
                name.clone(),
                "MCP servers are disabled in an untrusted workspace".to_owned(),
            )
        }));
        return (admitted, skipped);
    }
    for (name, config) in configured {
        if allowed.is_some_and(|patterns| {
            patterns.is_empty()
                || !patterns
                    .iter()
                    .any(|pattern| server_name_matches(name, pattern))
        }) {
            skipped.push((name.clone(), "not admitted by mcp.allowed".to_owned()));
            continue;
        }
        if excluded
            .iter()
            .any(|pattern| server_name_matches(name, pattern))
        {
            skipped.push((name.clone(), "excluded by mcp.excluded".to_owned()));
            continue;
        }
        let scope = config
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !ungated_server_names.contains(name)
            && matches!(scope, "project" | "workspace")
            && !gated_config_is_approved(name, config, workspace_root, approval_path_override)
        {
            skipped.push((
                name.clone(),
                "pending or rejected approval for this workspace-scoped MCP config".to_owned(),
            ));
            continue;
        }
        admitted.insert(name.clone(), config.clone());
    }
    (admitted, skipped)
}

fn server_name_matches(name: &str, pattern: &str) -> bool {
    if !pattern.contains('*') && !pattern.contains('?') {
        return name == pattern;
    }
    let (mut name_index, mut pattern_index) = (0usize, 0usize);
    let (mut star_name, mut star_pattern) = (None, None);
    while name_index < name.len() {
        let name_char = name[name_index..].chars().next().expect("valid UTF-8");
        let pattern_char = pattern
            .get(pattern_index..)
            .and_then(|tail| tail.chars().next());
        if pattern_char.is_some_and(|candidate| candidate == '?' || candidate == name_char) {
            name_index += name_char.len_utf8();
            pattern_index += pattern_char.expect("checked above").len_utf8();
        } else if pattern_char == Some('*') {
            star_pattern = Some(pattern_index + 1);
            star_name = Some(name_index);
            pattern_index += 1;
        } else if let (Some(saved_pattern), Some(saved_name)) = (star_pattern, star_name) {
            let next_name = pattern
                .get(saved_name..)
                .and_then(|tail| tail.chars().next())
                .map_or(name.len(), |character| saved_name + character.len_utf8());
            star_name = Some(next_name);
            name_index = next_name;
            pattern_index = saved_pattern;
        } else {
            return false;
        }
    }
    while pattern
        .get(pattern_index..)
        .is_some_and(|tail| tail.starts_with('*'))
    {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn gated_config_is_approved(
    name: &str,
    config: &Value,
    workspace_root: &Path,
    approval_path_override: Option<&Path>,
) -> bool {
    let approval_path = approval_path_override
        .map(Path::to_path_buf)
        .unwrap_or_else(|| Storage::get_global_canopy_dir().join(APPROVALS_FILENAME));
    let Ok(contents) = std::fs::read_to_string(approval_path) else {
        return false;
    };
    let Ok(approvals) = serde_json::from_str::<Value>(&contents) else {
        return false;
    };
    let project = resolve_project_root(workspace_root)
        .to_string_lossy()
        .into_owned();
    let Some(record) = approvals
        .get(project.as_str())
        .and_then(|project| project.get(name))
    else {
        return false;
    };
    let Ok(current_hash) = hash_mcp_server_config(config) else {
        return false;
    };
    record.get("status").and_then(Value::as_str) == Some("approved")
        && record.get("hash").and_then(Value::as_str) == Some(current_hash.as_str())
}

fn resolve_project_root(workspace_root: &Path) -> PathBuf {
    let absolute = if workspace_root.is_absolute() {
        workspace_root.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_default()
            .join(workspace_root)
    };
    let mut resolved = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            component => resolved.push(component.as_os_str()),
        }
    }
    resolved
}

fn workspace_budget_from_env(env: &HashMap<String, String>) -> WorkspaceMcpBudget {
    let budget = env
        .get("CANOPY_SERVE_MCP_CLIENT_BUDGET")
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .map(|value| value as f64);
    let requested_mode = env.get("CANOPY_SERVE_MCP_BUDGET_MODE").map(String::as_str);
    let mode = match requested_mode {
        Some("enforce") if budget.is_some() => McpBudgetMode::Enforce,
        Some("warn") if budget.is_some() => McpBudgetMode::Warn,
        Some("off") => McpBudgetMode::Off,
        Some("enforce" | "warn") => McpBudgetMode::Off,
        _ if budget.is_some() => McpBudgetMode::Warn,
        _ => McpBudgetMode::Off,
    };
    WorkspaceMcpBudget::new(budget, mode)
}

#[cfg(test)]
mod tests {
    use super::*;
    use canopy_core::tool_response_finalizer::ToolExecutionOutput;
    use canopy_core::tools::mcp::client_runtime::{
        McpTransport, McpTransportError, McpTransportSpec,
    };
    use canopy_core::utils::cancellation::CancellationToken;
    use serde_json::json;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

    static NEXT_APPROVAL_TEST_ID: AtomicUsize = AtomicUsize::new(0);

    fn temp_workspace(label: &str) -> PathBuf {
        let unique = NEXT_APPROVAL_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "canopy-cli-mcp-{label}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[derive(Default)]
    struct MockPrompt {
        available: bool,
        approved: bool,
        calls: AtomicUsize,
        last: StdMutex<String>,
    }

    impl McpCliApprovalPrompt for MockPrompt {
        fn is_available(&self) -> bool {
            self.available
        }

        fn confirm(&self, prompt: &str) -> Result<bool, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = prompt.to_owned();
            Ok(self.approved)
        }
    }

    #[derive(Default)]
    struct MockFactory {
        creates: AtomicUsize,
        closes: Arc<AtomicUsize>,
    }

    impl McpTransportFactory for MockFactory {
        fn create<'a>(
            &'a self,
            _spec: McpTransportSpec,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Arc<dyn McpTransport>, McpTransportError>> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            let closes = Arc::clone(&self.closes);
            Box::pin(async move { Ok(Arc::new(MockTransport(closes)) as Arc<dyn McpTransport>) })
        }
    }

    struct MockTransport(Arc<AtomicUsize>);

    impl McpTransport for MockTransport {
        fn request<'a>(
            &'a self,
            request: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<Value, McpTransportError>> {
            Box::pin(async move {
                let method = request.get("method").and_then(Value::as_str).unwrap_or("");
                let id = request.get("id").cloned().unwrap_or(Value::Null);
                let result = match method {
                    "initialize" => json!({
                        "protocolVersion":"2025-06-18",
                        "capabilities":{},
                        "serverInfo":{"name":"mock","version":"1"}
                    }),
                    "tools/list" => json!({"tools":[{
                        "name":"read",
                        "description":"read a fixture",
                        "inputSchema":{"type":"object","properties":{}},
                        "annotations":{"readOnlyHint":true}
                    }]}),
                    "tools/call" => json!({"content":[{
                        "type":"text",
                        "text":"mock response"
                    }]}),
                    "prompts/list" => json!({"prompts":[]}),
                    "resources/list" => json!({"resources":[]}),
                    _ => json!({}),
                };
                Ok(json!({"jsonrpc":"2.0","id":id,"result":result}))
            })
        }

        fn notify<'a>(
            &'a self,
            _notification: Value,
            _cancellation: Option<CancellationToken>,
        ) -> BoxFuture<'a, Result<(), McpTransportError>> {
            Box::pin(async { Ok(()) })
        }

        fn close<'a>(&'a self) -> BoxFuture<'a, Result<(), McpTransportError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    fn call() -> ToolCallRequestInfo {
        ToolCallRequestInfo {
            call_id: "call-1".to_owned(),
            provider_call_id: None,
            name: "mcp__fixture__read".to_owned(),
            args: json!({"path":"README.md"}),
            is_client_initiated: false,
            prompt_id: "prompt-1".to_owned(),
            response_id: None,
            was_output_truncated: None,
            goal_context: None,
        }
    }

    struct FallbackExecutor;

    impl AgentToolExecutor for FallbackExecutor {
        fn execute<'a>(
            &'a self,
            _call: &'a ToolCallRequestInfo,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ToolExecutionOutput, String>> + Send + 'a>,
        > {
            Box::pin(async { Err("unexpected fallback dispatch".to_owned()) })
        }
    }

    fn authorizer(
        permissions: PermissionRuleSet,
        trusted_workspace: bool,
        prompt: Arc<MockPrompt>,
    ) -> CliMcpAuthorizer {
        CliMcpAuthorizer {
            permissions,
            skill_grants: McpSessionSkillGrants::default(),
            workspace_root: PathBuf::from("/tmp/workspace"),
            trusted_workspace,
            server_trust: HashMap::new(),
            core_tools: None,
            excluded_tools: Vec::new(),
            prompt,
        }
    }

    async fn authorize(
        authorizer: &CliMcpAuthorizer,
        operation: McpAuthorizationOperation,
    ) -> Result<(), String> {
        let call = call();
        authorizer.authorize(&call, operation).await
    }

    fn tool_operation() -> McpAuthorizationOperation {
        McpAuthorizationOperation::ToolCall {
            server_name: "fixture".to_owned(),
            tool_name: "read".to_owned(),
            trust: None,
            annotations: None,
        }
    }

    #[tokio::test]
    async fn default_permission_requires_tty_and_confirmation() {
        let no_tty = Arc::new(MockPrompt::default());
        let host = authorizer(PermissionRuleSet::default(), false, Arc::clone(&no_tty));
        assert!(
            authorize(&host, tool_operation())
                .await
                .unwrap_err()
                .contains("TTY")
        );
        assert_eq!(no_tty.calls.load(Ordering::SeqCst), 0);

        let prompt = Arc::new(MockPrompt {
            available: true,
            approved: true,
            ..MockPrompt::default()
        });
        let host = authorizer(PermissionRuleSet::default(), false, Arc::clone(&prompt));
        assert!(authorize(&host, tool_operation()).await.is_ok());
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 1);

        let trusted_prompt = Arc::new(MockPrompt::default());
        let trusted = authorizer(
            PermissionRuleSet::default(),
            true,
            Arc::clone(&trusted_prompt),
        );
        let trusted_operation = McpAuthorizationOperation::ToolCall {
            server_name: "fixture".to_owned(),
            tool_name: "read".to_owned(),
            trust: Some(true),
            annotations: None,
        };
        assert!(authorize(&trusted, trusted_operation).await.is_ok());
        assert_eq!(trusted_prompt.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn deny_rule_wins_even_for_allowlisted_or_trusted_tools() {
        let permissions = PermissionRuleSet::from_raw(
            ["mcp__fixture__read".to_owned()],
            [],
            ["mcp__fixture__*".to_owned()],
        );
        let prompt = Arc::new(MockPrompt {
            available: true,
            approved: true,
            ..MockPrompt::default()
        });
        let host = authorizer(permissions, true, Arc::clone(&prompt));
        assert!(
            authorize(&host, tool_operation())
                .await
                .unwrap_err()
                .contains("deny")
        );
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn explicit_allow_skips_prompt_but_ask_still_prompts() {
        let allow = PermissionRuleSet::from_raw(["mcp__fixture__read".to_owned()], [], []);
        let prompt = Arc::new(MockPrompt::default());
        let host = authorizer(allow, false, Arc::clone(&prompt));
        assert!(authorize(&host, tool_operation()).await.is_ok());
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 0);

        let ask = PermissionRuleSet::from_raw([], ["mcp__fixture__read".to_owned()], []);
        let prompt = Arc::new(MockPrompt {
            available: true,
            approved: true,
            ..MockPrompt::default()
        });
        let host = authorizer(ask, true, Arc::clone(&prompt));
        assert!(authorize(&host, tool_operation()).await.is_ok());
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn resource_read_uses_server_scoped_rules_and_trust() {
        let prompt = Arc::new(MockPrompt::default());
        let mut trusted = authorizer(PermissionRuleSet::default(), true, Arc::clone(&prompt));
        trusted.server_trust.insert("fixture".to_owned(), true);
        let operation = McpAuthorizationOperation::ResourceRead {
            server_name: "fixture".to_owned(),
            uri: "fixture://item".to_owned(),
        };
        assert!(authorize(&trusted, operation.clone()).await.is_ok());
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 0);

        let permissions = PermissionRuleSet::from_raw(
            ["read_mcp_resource(fixture)".to_owned()],
            [],
            ["ReadMcpResource(fixture)".to_owned()],
        );
        let mut denied = authorizer(permissions, true, Arc::clone(&prompt));
        denied.server_trust.insert("fixture".to_owned(), true);
        assert!(
            authorize(&denied, operation)
                .await
                .unwrap_err()
                .contains("deny")
        );
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn core_and_excluded_tool_lists_match_legacy_mcp_aliases() {
        let name = mcp_function_name("fixture", "read tool");
        let legacy = legacy_mcp_name("fixture", "read tool");
        assert_ne!(name, legacy);
        assert!(mcp_tool_is_enabled(
            &name,
            std::slice::from_ref(&legacy),
            Some(std::slice::from_ref(&legacy)),
            &[]
        ));
        assert!(!mcp_tool_is_enabled(
            &name,
            std::slice::from_ref(&legacy),
            None,
            std::slice::from_ref(&legacy)
        ));
    }

    #[test]
    fn server_admission_honors_allow_exclude_and_workspace_approval_gates() {
        let configured = json!({
            "allowed": {"command":"safe"},
            "excluded": {"command":"excluded"},
            "pending": {"command":"unreviewed","scope":"workspace"}
        });
        let (servers, skipped) = admitted_servers(
            configured.as_object().unwrap(),
            Some(&["allow*".to_owned(), "pending".to_owned()]),
            &["excluded".to_owned()],
            true,
            &HashSet::new(),
            Path::new("/definitely/missing/canopy-workspace"),
            Some(Path::new("/definitely/missing/canopy-mcp-approvals.json")),
        );
        assert_eq!(
            servers.keys().map(String::as_str).collect::<Vec<_>>(),
            ["allowed"]
        );
        assert_eq!(skipped.len(), 2);
        assert!(
            skipped
                .iter()
                .any(|(name, reason)| name == "pending" && reason.contains("approval"))
        );
    }

    #[test]
    fn project_mcp_jsonc_is_normalized_and_bad_entries_are_skipped() {
        let workspace = temp_workspace("project-source");
        let project_file = workspace.join(".mcp.json");
        std::fs::write(
            &project_file,
            r#"{
              // Claude-style HTTP transport with a comment.
              "mcpServers": {
                "api": { "type": "http", "url": "https://example.invalid/mcp" },
                "stdio": { "type": "stdio", "command": "node", "args": ["server.js"] },
                "invalid": "skip me"
              }
            }"#,
        )
        .unwrap();

        let loaded = load_project_mcp_servers(&workspace);
        assert_eq!(
            loaded.servers["api"]["httpUrl"],
            "https://example.invalid/mcp"
        );
        assert_eq!(loaded.servers["api"]["scope"], "project");
        assert!(loaded.servers["api"].get("url").is_none());
        assert_eq!(loaded.servers["stdio"]["command"], "node");
        assert!(loaded.servers["stdio"].get("type").is_none());
        assert!(!loaded.servers.contains_key("invalid"));
        assert_eq!(loaded.warnings.len(), 1);
        assert!(loaded.warnings[0].contains("server \"invalid\" is not an object"));
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn project_mcp_json_malformed_and_missing_sources_are_non_fatal() {
        let workspace = temp_workspace("project-malformed");
        assert!(load_project_mcp_servers(&workspace).warnings.is_empty());

        let project_file = workspace.join(".mcp.json");
        std::fs::write(&project_file, "{ broken").unwrap();
        let malformed = load_project_mcp_servers(&workspace);
        assert!(malformed.servers.is_empty());
        assert!(malformed.warnings[0].contains("Failed to parse"));

        std::fs::write(&project_file, r#"{"other":{}}"#).unwrap();
        let missing_servers = load_project_mcp_servers(&workspace);
        assert!(missing_servers.servers.is_empty());
        assert!(missing_servers.warnings[0].contains("has no \"mcpServers\" object"));
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn mcp_server_sources_merge_in_typescript_precedence_order() {
        let workspace = temp_workspace("source-precedence");
        std::fs::write(
            workspace.join(".mcp.json"),
            r#"{"mcpServers": {
              "user": {"command":"project-user"},
              "workspace": {"command":"project-workspace"},
              "system": {"command":"project-system"},
              "cli": {"command":"project-cli"},
              "project-only": {"command":"project"}
            }}"#,
        )
        .unwrap();
        let settings = Map::from_iter([
            ("user".to_owned(), json!({"command":"settings-user"})),
            (
                "workspace".to_owned(),
                json!({"command":"settings-workspace","scope":"workspace"}),
            ),
            (
                "system".to_owned(),
                json!({"command":"settings-system","scope":"system"}),
            ),
        ]);
        let cli = parse_cli_mcp_config(
            Some(r#"{"mcpServers":{"cli":{"command":"cli-config"},"cli-only":{"command":"cli"}}}"#),
            &workspace,
        )
        .unwrap();
        let cli_names = cli
            .as_ref()
            .unwrap()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let settings = McpCliSettings {
            servers: settings,
            ..McpCliSettings::default()
        }
        .with_project_and_cli(
            &workspace,
            Some(r#"{"mcpServers":{"cli":{"command":"cli-config"},"cli-only":{"command":"cli"}}}"#),
        )
        .unwrap();
        assert_eq!(settings.servers["user"]["command"], "project-user");
        assert_eq!(
            settings.servers["workspace"]["command"],
            "settings-workspace"
        );
        assert_eq!(settings.servers["system"]["command"], "settings-system");
        assert_eq!(settings.servers["cli"]["command"], "cli-config");
        assert_eq!(settings.servers["project-only"]["command"], "project");
        assert!(settings.ungated_server_names.contains("cli"));
        assert!(settings.ungated_server_names.contains("cli-only"));
        assert_eq!(settings.ungated_server_names, cli_names);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn cli_mcp_config_reads_relative_jsonc_files_and_rejects_invalid_shapes() {
        let workspace = temp_workspace("cli-source");
        std::fs::write(
            workspace.join("servers.jsonc"),
            r#"{
              // Explicit CLI config can be a relative file.
              "mcpServers": {"remote": {"httpUrl":"https://example.invalid/mcp"}}
            }"#,
        )
        .unwrap();
        let parsed = parse_cli_mcp_config(Some("servers.jsonc"), &workspace)
            .unwrap()
            .unwrap();
        assert_eq!(parsed["remote"]["httpUrl"], "https://example.invalid/mcp");
        assert!(
            parse_cli_mcp_config(Some(r#"{"broken":true}"#), &workspace)
                .unwrap_err()
                .starts_with("Invalid MCP configuration provided via --mcp-config:")
        );
        assert!(
            parse_cli_mcp_config(Some("{broken"), &workspace)
                .unwrap_err()
                .starts_with("Invalid MCP configuration provided via --mcp-config:")
        );
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn server_filters_apply_after_source_merge_and_cli_source_skips_approval_gate() {
        let configured = Map::from_iter([
            (
                "project".to_owned(),
                json!({"command":"project","scope":"project"}),
            ),
            ("cli".to_owned(), json!({"command":"cli","scope":"project"})),
            ("excluded".to_owned(), json!({"command":"excluded"})),
        ]);
        let ungated = HashSet::from(["cli".to_owned()]);
        let (admitted, skipped) = admitted_servers(
            &configured,
            Some(&["proj*".to_owned(), "cli".to_owned(), "excluded".to_owned()]),
            &["excluded".to_owned()],
            true,
            &ungated,
            Path::new("/workspace-that-has-no-approval"),
            None,
        );
        assert_eq!(
            admitted.keys().map(String::as_str).collect::<Vec<_>>(),
            ["cli"]
        );
        assert!(
            skipped
                .iter()
                .any(|(name, reason)| { name == "project" && reason.contains("approval") })
        );
        assert!(
            skipped
                .iter()
                .any(|(name, reason)| { name == "excluded" && reason.contains("excluded") })
        );
    }

    #[test]
    fn explicit_cli_server_allowlist_replaces_settings_filters() {
        let configured = Map::from_iter([
            ("cli".to_owned(), json!({"command":"cli"})),
            ("settings".to_owned(), json!({"command":"settings"})),
        ]);
        let settings = McpCliSettings {
            servers: configured,
            allowed: Some(vec!["settings".to_owned()]),
            excluded: vec!["cli".to_owned()],
            trusted_workspace: true,
            ..McpCliSettings::default()
        }
        .with_cli_server_allowlist(Some(&["cli".to_owned()]));
        let (admitted, skipped) = admitted_servers(
            &settings.servers,
            settings.allowed.as_deref(),
            &settings.excluded,
            settings.trusted_workspace,
            &HashSet::new(),
            Path::new("/workspace"),
            None,
        );
        assert_eq!(
            admitted.keys().map(String::as_str).collect::<Vec<_>>(),
            ["cli"]
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].0, "settings");

        let empty = settings.with_cli_server_allowlist(Some(&[]));
        let (admitted, _) = admitted_servers(
            &empty.servers,
            empty.allowed.as_deref(),
            &empty.excluded,
            empty.trusted_workspace,
            &HashSet::new(),
            Path::new("/workspace"),
            None,
        );
        assert!(admitted.is_empty());
    }

    #[test]
    fn settings_expose_merged_mcp_servers_and_empty_allow_is_deny_all() {
        let merged = Map::from_iter([
            ("mcpServers".to_owned(), json!({"one":{"command":"run"}})),
            ("mcp".to_owned(), json!({"allowed":[]})),
        ]);
        let loaded = LoadedSettings {
            system: Default::default(),
            system_defaults: Default::default(),
            user: Default::default(),
            workspace: Default::default(),
            is_trusted: true,
            migrated_in_memory_scopes: Default::default(),
            migration_warnings: Vec::new(),
            corrupted_path: None,
            was_recovered: false,
            workspace_settings_active: true,
            settings_errors: Vec::new(),
            merged,
            runtime_environment: Default::default(),
        };
        let settings = McpCliSettings::from_loaded_settings(&loaded);
        let (servers, skipped) = admitted_servers(
            &settings.servers,
            settings.allowed.as_deref(),
            &settings.excluded,
            settings.trusted_workspace,
            &settings.ungated_server_names,
            Path::new("/tmp/workspace"),
            None,
        );
        assert!(servers.is_empty());
        assert_eq!(skipped[0].0, "one");
    }

    #[test]
    fn workspace_server_approval_is_bound_to_its_current_config_hash() {
        let unique = NEXT_APPROVAL_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let project = std::env::temp_dir().join(format!(
            "canopy-cli-mcp-approval-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&project).unwrap();
        let approval_path = project.join("approvals.json");
        let config = json!({"command":"fixture-server","scope":"workspace"});
        let hash = hash_mcp_server_config(&config).unwrap();
        let project_key = project
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let mut approvals = Map::new();
        approvals.insert(
            project_key,
            json!({"fixture":{"hash":hash,"status":"approved"}}),
        );
        std::fs::write(
            &approval_path,
            serde_json::to_vec(&Value::Object(approvals)).unwrap(),
        )
        .unwrap();

        let configured = Map::from_iter([("fixture".to_owned(), config)]);
        let (admitted, skipped) = admitted_servers(
            &configured,
            None,
            &[],
            true,
            &HashSet::new(),
            &project,
            Some(&approval_path),
        );
        assert!(admitted.contains_key("fixture"));
        assert!(skipped.is_empty());

        let edited = Map::from_iter([(
            "fixture".to_owned(),
            json!({"command":"changed-server","scope":"workspace"}),
        )]);
        let (admitted, skipped) = admitted_servers(
            &edited,
            None,
            &[],
            true,
            &HashSet::new(),
            &project,
            Some(&approval_path),
        );
        assert!(admitted.is_empty());
        assert_eq!(skipped[0].0, "fixture");
        let _ = std::fs::remove_dir_all(project);
    }

    #[tokio::test]
    async fn opens_the_selected_session_with_mocked_transport_and_shared_budget() {
        let factory = Arc::new(MockFactory::default());
        let env = HashMap::from([
            ("CANOPY_SERVE_MCP_CLIENT_BUDGET".to_owned(), "4".to_owned()),
            (
                "CANOPY_SERVE_MCP_BUDGET_MODE".to_owned(),
                "enforce".to_owned(),
            ),
        ]);
        let workspace = McpCliWorkspace::with_factory("/tmp/workspace", factory.clone(), &env);
        let settings = McpCliSettings {
            servers: Map::from_iter([(
                "fixture".to_owned(),
                // The injected factory records a stdio spec but never spawns
                // the configured command. Stdio is pooled across sessions.
                json!({"command":"mock-server"}),
            )]),
            trusted_workspace: true,
            ..McpCliSettings::default()
        };
        let session = workspace
            .open_session(
                "selected-session-id",
                &settings,
                PermissionRuleSet::default(),
                None,
                Vec::new(),
                Arc::new(MockPrompt::default()),
            )
            .await
            .unwrap();
        assert_eq!(session.session_id, "selected-session-id");
        assert_eq!(factory.creates.load(Ordering::SeqCst), 1);
        assert_eq!(session.discovery_errors().len(), 0);
        let mut declarations = Vec::new();
        session.append_function_declarations(&mut declarations, None, &[]);
        assert_eq!(
            declarations[0]["functionDeclarations"][0]["name"],
            "mcp__fixture__read"
        );
        assert_eq!(
            workspace.budget().lock().unwrap().get_reserved_slots(),
            ["fixture"]
        );

        let second_session = workspace
            .open_session(
                "second-selected-session-id",
                &settings,
                PermissionRuleSet::default(),
                None,
                Vec::new(),
                Arc::new(MockPrompt::default()),
            )
            .await
            .unwrap();
        assert_eq!(factory.creates.load(Ordering::SeqCst), 1);
        session.stop();
        assert_eq!(
            workspace.budget().lock().unwrap().get_reserved_slots(),
            ["fixture"]
        );
        second_session.stop();
        assert!(
            workspace
                .budget()
                .lock()
                .unwrap()
                .get_reserved_slots()
                .is_empty()
        );
        workspace.shutdown().await;
    }

    #[tokio::test]
    async fn untrusted_workspace_skips_mcp_transports_before_startup() {
        let factory = Arc::new(MockFactory::default());
        let env = HashMap::new();
        let workspace = McpCliWorkspace::with_factory("/tmp/workspace", factory.clone(), &env);
        let settings = McpCliSettings {
            servers: Map::from_iter([("fixture".to_owned(), json!({"command":"must-not-spawn"}))]),
            ..McpCliSettings::default()
        };
        let session = workspace
            .open_session(
                "untrusted-session-id",
                &settings,
                PermissionRuleSet::default(),
                None,
                Vec::new(),
                Arc::new(MockPrompt::default()),
            )
            .await
            .unwrap();
        assert_eq!(factory.creates.load(Ordering::SeqCst), 0);
        assert!(
            session
                .skipped_servers()
                .iter()
                .any(|(name, reason)| name == "fixture" && reason.contains("untrusted"))
        );
        session.stop();
        workspace.shutdown().await;
    }

    #[tokio::test]
    async fn composed_executor_authorizes_then_dispatches_to_mock_transport() {
        let factory = Arc::new(MockFactory::default());
        let env = HashMap::new();
        let workspace = McpCliWorkspace::with_factory("/tmp/workspace", factory, &env);
        let settings = McpCliSettings {
            servers: Map::from_iter([(
                "fixture".to_owned(),
                json!({"httpUrl":"https://fixture.invalid/mcp"}),
            )]),
            trusted_workspace: true,
            ..McpCliSettings::default()
        };
        let prompt = Arc::new(MockPrompt {
            available: true,
            approved: true,
            ..MockPrompt::default()
        });
        let session = workspace
            .open_session(
                "dispatch-session-id",
                &settings,
                PermissionRuleSet::default(),
                None,
                Vec::new(),
                prompt.clone(),
            )
            .await
            .unwrap();
        let executor = session.compose(FallbackExecutor);
        let call = call();
        let output = executor.execute(&call).await.unwrap();
        assert!(output.output.contains("mock response"));
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 1);
        session.stop();
        workspace.shutdown().await;
    }
}
