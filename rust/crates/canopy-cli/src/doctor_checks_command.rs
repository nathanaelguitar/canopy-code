//! Native Rust equivalents for the general interactive `/doctor` checks.
//!
//! The active TUI supplies the configuration and session snapshot. This
//! module probes Git and the operating-system release and keeps all other
//! checks read-only; it never prints credential values.

use std::io;
use std::process::Stdio;
use std::time::Duration;

use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::process::{ChildStdout, Command};
use tokio::time::timeout;

const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_VERSION_OUTPUT_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorCheckStatus {
    Pass,
    Warn,
    Fail,
    NotApplicable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DoctorCheckResult {
    pub category: String,
    pub name: String,
    pub status: DoctorCheckStatus,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct DoctorCheckInput {
    /// `None` means the caller has no active provider snapshot. An empty
    /// `auth_type` means authentication is known to be unconfigured.
    pub auth: Option<DoctorAuthInput>,
    pub api_client_initialized: Option<bool>,
    pub settings_loaded: Option<bool>,
    pub selected_model: Option<String>,
    /// `None` means MCP configuration was not supplied; an empty vector means
    /// the loaded settings contain no configured servers.
    pub mcp_servers: Option<Vec<DoctorMcpServerInput>>,
    /// The active native declaration count, when the caller can provide it.
    pub native_tool_count: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct DoctorAuthInput {
    pub auth_type: Option<String>,
    /// Whether the active configured model-provider entry has a nonempty
    /// `baseUrl`. `None` means there was no matching provider entry.
    pub model_provider_base_url_configured: Option<bool>,
    /// Whether `ANTHROPIC_BASE_URL` exists in the effective runtime
    /// environment. This is used only when no selected model-provider entry
    /// exists, matching the TypeScript Anthropic validation rule.
    pub anthropic_base_url_env_configured: bool,
    /// Environment variable name only; the associated credential value must
    /// never be copied into this structure or rendered by this module.
    pub credential_env_name: Option<String>,
    pub credential_configured: Option<bool>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DoctorMcpConnectionStatus {
    Connected,
    Connecting,
    Disconnected,
}

#[derive(Clone, Debug)]
pub struct DoctorMcpServerInput {
    pub name: String,
    /// `None` means the caller cannot determine whether policy disabled it.
    pub disabled: Option<bool>,
    /// `None` means no live status snapshot was available to the caller.
    pub connection_status: Option<DoctorMcpConnectionStatus>,
}

/// Run the general `/doctor` checks using the active native runtime snapshot.
///
/// Git and platform release probes have a five-second timeout, kill timed-out
/// children, and read at most 1 KiB of stdout. No check makes a network
/// request.
pub async fn run_doctor_checks(input: &DoctorCheckInput) -> Vec<DoctorCheckResult> {
    let git = check_git_version().await;
    let platform = check_platform().await;
    let mut checks = vec![
        result(
            "System",
            "Runtime",
            DoctorCheckStatus::Pass,
            "native Rust runtime",
            None,
        ),
        result(
            "System",
            "Node.js version",
            DoctorCheckStatus::NotApplicable,
            "not applicable to the native Rust runtime",
            None,
        ),
        result(
            "System",
            "npm version",
            DoctorCheckStatus::NotApplicable,
            "not applicable to the native Rust runtime",
            None,
        ),
        platform,
        check_auth(input.auth.as_ref()),
        check_api_client(input.api_client_initialized),
        check_settings(input.settings_loaded),
        check_model(input.settings_loaded, input.selected_model.as_deref()),
    ];

    checks.extend(check_mcp_servers(input.mcp_servers.as_deref()));
    checks.push(check_tool_registry(input.native_tool_count));
    checks.push(result(
        "Tools",
        "Ripgrep",
        DoctorCheckStatus::Pass,
        "available through native Rust search",
        Some("The native grep_search implementation is used; an external ripgrep binary is not required."),
    ));
    checks.push(git);
    checks
}

pub fn format_doctor_checks(checks: &[DoctorCheckResult]) -> String {
    let mut passed = 0usize;
    let mut warnings = 0usize;
    let mut failed = 0usize;
    let mut not_applicable = 0usize;
    let mut lines = vec!["Native environment diagnostics".to_owned()];

    for check in checks {
        let status = match check.status {
            DoctorCheckStatus::Pass => {
                passed += 1;
                "PASS"
            }
            DoctorCheckStatus::Warn => {
                warnings += 1;
                "WARN"
            }
            DoctorCheckStatus::Fail => {
                failed += 1;
                "FAIL"
            }
            DoctorCheckStatus::NotApplicable => {
                not_applicable += 1;
                "N/A"
            }
        };
        lines.push(format!(
            "{status} [{}] {}: {}",
            check.category, check.name, check.message
        ));
        if let Some(detail) = &check.detail {
            lines.extend(detail.lines().map(|line| format!("  {line}")));
        }
    }

    lines.push(format!(
        "Summary: {passed} passed, {warnings} warnings, {failed} failed, {not_applicable} not applicable"
    ));
    lines.join("\n")
}

async fn check_platform() -> DoctorCheckResult {
    let platform = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
    match platform_release().await {
        Some(release) => result(
            "System",
            "Platform",
            DoctorCheckStatus::Pass,
            format!("{platform} ({release})"),
            None,
        ),
        None => result(
            "System",
            "Platform",
            DoctorCheckStatus::Warn,
            format!("{platform} (release unavailable)"),
            Some("Could not obtain the operating-system release within the probe limits."),
        ),
    }
}

#[cfg(unix)]
async fn platform_release() -> Option<String> {
    let mut command = Command::new("uname");
    command.arg("-r");
    let output = bounded_command_stdout(&mut command).await?;
    parse_unix_release(&output)
}

#[cfg(windows)]
async fn platform_release() -> Option<String> {
    let mut command = Command::new("cmd.exe");
    command.args(["/C", "ver"]);
    let output = bounded_command_stdout(&mut command).await?;
    parse_windows_release(&output)
}

#[cfg(not(any(unix, windows)))]
async fn platform_release() -> Option<String> {
    None
}

fn check_auth(auth: Option<&DoctorAuthInput>) -> DoctorCheckResult {
    let Some(auth) = auth else {
        return result(
            "Authentication",
            "Credentials",
            DoctorCheckStatus::NotApplicable,
            "provider snapshot unavailable",
            Some("The active Rust runtime did not supply authentication state."),
        );
    };

    let Some(auth_type) = auth
        .auth_type
        .as_deref()
        .filter(|auth_type| !auth_type.trim().is_empty())
    else {
        return result(
            "Authentication",
            "Credentials",
            DoctorCheckStatus::Fail,
            "not configured",
            Some("Select an authentication provider and configure its credentials."),
        );
    };
    let auth_label = safe_identifier(auth_type).unwrap_or("configured provider");

    let method_validation = validate_auth_method(auth);
    match &method_validation {
        AuthMethodValidation::Unavailable(detail) => {
            return result(
                "Authentication",
                "Credentials",
                DoctorCheckStatus::Warn,
                format!("configured ({auth_label})"),
                Some(detail),
            );
        }
        AuthMethodValidation::Invalid(detail)
            if !matches!(auth_type, "openai" | "anthropic" | "gemini") =>
        {
            return result(
                "Authentication",
                "Credentials",
                DoctorCheckStatus::Fail,
                format!("invalid ({auth_label})"),
                Some(detail),
            );
        }
        AuthMethodValidation::Valid | AuthMethodValidation::Invalid(_) => {}
    }

    if auth.credential_configured == Some(false) {
        let detail = auth
            .credential_env_name
            .as_deref()
            .and_then(safe_environment_name)
            .map(|name| format!("Credential environment variable {name} is not set."))
            .unwrap_or_else(|| "The configured provider has no credential available.".to_owned());
        return result(
            "Authentication",
            "Credentials",
            DoctorCheckStatus::Fail,
            format!("credential missing ({auth_label})"),
            Some(&detail),
        );
    }

    if let AuthMethodValidation::Invalid(detail) = method_validation {
        return result(
            "Authentication",
            "Credentials",
            DoctorCheckStatus::Fail,
            format!("invalid ({auth_label})"),
            Some(&detail),
        );
    }

    if auth.credential_configured.is_none() {
        return result(
            "Authentication",
            "Credentials",
            DoctorCheckStatus::Warn,
            format!("configured ({auth_label})"),
            Some(
                "Native auth configuration is valid, but credential availability was not supplied.",
            ),
        );
    }

    let detail = match auth
        .credential_env_name
        .as_deref()
        .and_then(safe_environment_name)
    {
        Some(name) => format!("Credential environment variable {name} is set."),
        None => "Provider and credential are configured.".to_owned(),
    };

    result(
        "Authentication",
        "Credentials",
        DoctorCheckStatus::Pass,
        format!("configured ({auth_label})"),
        Some(&detail),
    )
}

enum AuthMethodValidation {
    Valid,
    Invalid(String),
    Unavailable(String),
}

fn validate_auth_method(auth: &DoctorAuthInput) -> AuthMethodValidation {
    match auth.auth_type.as_deref() {
        Some("openai" | "gemini") => AuthMethodValidation::Valid,
        Some("anthropic") => match auth.model_provider_base_url_configured {
            Some(true) => AuthMethodValidation::Valid,
            Some(false) => AuthMethodValidation::Invalid(
                "Anthropic provider is missing required baseUrl in modelProviders[].baseUrl."
                    .to_owned(),
            ),
            None if auth.anthropic_base_url_env_configured => AuthMethodValidation::Valid,
            None => AuthMethodValidation::Invalid(
                "ANTHROPIC_BASE_URL environment variable is not set.".to_owned(),
            ),
        },
        Some("canopy-oauth") => AuthMethodValidation::Invalid(
            "Canopy OAuth free tier was discontinued on 2026-04-15.".to_owned(),
        ),
        Some("chatgpt-oauth" | "vertex-ai") => AuthMethodValidation::Unavailable(
            "This authentication method is not supported by the active native runtime, so its credentials cannot be validated here.".to_owned(),
        ),
        Some(_) => AuthMethodValidation::Invalid("Invalid auth method selected.".to_owned()),
        None => AuthMethodValidation::Unavailable(
            "The active native runtime did not supply an authentication method.".to_owned(),
        ),
    }
}

fn check_api_client(initialized: Option<bool>) -> DoctorCheckResult {
    match initialized {
        Some(true) => result(
            "Authentication",
            "API client",
            DoctorCheckStatus::Pass,
            "client initialized",
            None,
        ),
        Some(false) => result(
            "Authentication",
            "API client",
            DoctorCheckStatus::Warn,
            "client not initialized",
            Some("The active native runtime did not initialize its provider client."),
        ),
        None => result(
            "Authentication",
            "API client",
            DoctorCheckStatus::NotApplicable,
            "initialization state unavailable",
            None,
        ),
    }
}

fn check_settings(settings_loaded: Option<bool>) -> DoctorCheckResult {
    match settings_loaded {
        Some(true) => result(
            "Configuration",
            "Settings",
            DoctorCheckStatus::Pass,
            "loaded",
            None,
        ),
        Some(false) => result(
            "Configuration",
            "Settings",
            DoctorCheckStatus::Fail,
            "not loaded",
            Some("Settings could not be loaded. Check the settings files for syntax errors."),
        ),
        None => result(
            "Configuration",
            "Settings",
            DoctorCheckStatus::NotApplicable,
            "load state unavailable",
            None,
        ),
    }
}

fn check_model(settings_loaded: Option<bool>, model: Option<&str>) -> DoctorCheckResult {
    match (
        settings_loaded,
        model.filter(|model| !model.trim().is_empty()),
    ) {
        (Some(false), _) => result(
            "Configuration",
            "Model",
            DoctorCheckStatus::NotApplicable,
            "not checked because settings did not load",
            None,
        ),
        (None, _) => result(
            "Configuration",
            "Model",
            DoctorCheckStatus::NotApplicable,
            "configuration state unavailable",
            None,
        ),
        (Some(true), Some(model)) => result(
            "Configuration",
            "Model",
            DoctorCheckStatus::Pass,
            safe_model_name(model).unwrap_or("configured"),
            None,
        ),
        (Some(true), None) => result(
            "Configuration",
            "Model",
            DoctorCheckStatus::Fail,
            "not configured",
            Some("Select a model in the native runtime configuration."),
        ),
    }
}

fn check_mcp_servers(servers: Option<&[DoctorMcpServerInput]>) -> Vec<DoctorCheckResult> {
    let Some(servers) = servers else {
        return vec![result(
            "MCP Servers",
            "MCP servers",
            DoctorCheckStatus::NotApplicable,
            "configuration snapshot unavailable",
            None,
        )];
    };
    if servers.is_empty() {
        return vec![result(
            "MCP Servers",
            "MCP servers",
            DoctorCheckStatus::Pass,
            "none configured",
            None,
        )];
    }

    servers
        .iter()
        .map(|server| {
            if server.disabled == Some(true) {
                return result(
                    "MCP Servers",
                    safe_server_name(&server.name),
                    DoctorCheckStatus::Pass,
                    "disabled",
                    None,
                );
            }
            match server.connection_status {
                Some(DoctorMcpConnectionStatus::Connected) => result(
                    "MCP Servers",
                    safe_server_name(&server.name),
                    DoctorCheckStatus::Pass,
                    "connected",
                    None,
                ),
                Some(DoctorMcpConnectionStatus::Connecting) => result(
                    "MCP Servers",
                    safe_server_name(&server.name),
                    DoctorCheckStatus::Warn,
                    "connecting",
                    Some("Server is still starting up."),
                ),
                Some(DoctorMcpConnectionStatus::Disconnected)
                    if server.disabled == Some(false) =>
                {
                    result(
                        "MCP Servers",
                        safe_server_name(&server.name),
                        DoctorCheckStatus::Fail,
                        "disconnected",
                        Some("Check that the server process and its configuration are available."),
                    )
                }
                Some(DoctorMcpConnectionStatus::Disconnected) => result(
                    "MCP Servers",
                    safe_server_name(&server.name),
                    DoctorCheckStatus::Warn,
                    "status unavailable",
                    Some("The server is disconnected, but the caller could not determine whether policy disabled it."),
                ),
                None => result(
                    "MCP Servers",
                    safe_server_name(&server.name),
                    DoctorCheckStatus::Warn,
                    "status unavailable",
                    Some("The caller did not provide a live MCP connection snapshot."),
                ),
            }
        })
        .collect()
}

fn check_tool_registry(tool_count: Option<usize>) -> DoctorCheckResult {
    match tool_count {
        Some(count) => result(
            "Tools",
            "Tool registry",
            DoctorCheckStatus::Pass,
            format!("{count} native tool declarations active"),
            Some(
                "Count reflects the active declaration snapshot supplied by the caller; the Rust CLI does not expose a shared ToolRegistry.",
            ),
        ),
        None => result(
            "Tools",
            "Tool registry",
            DoctorCheckStatus::NotApplicable,
            "native tool count unavailable",
            Some("The active runtime did not supply a tool declaration count."),
        ),
    }
}

async fn check_git_version() -> DoctorCheckResult {
    let mut child = match Command::new("git")
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return git_unavailable("not available", None),
    };
    let Some(stdout) = child.stdout.take() else {
        return git_unavailable("check failed", None);
    };

    let probe = timeout(VERSION_PROBE_TIMEOUT, async {
        let read_output = read_probe_output(stdout);
        tokio::pin!(read_output);
        let wait_child = child.wait();
        tokio::pin!(wait_child);
        let (output, exit_status) = tokio::join!(read_output, wait_child);
        Ok::<_, io::Error>((output?, exit_status?.success()))
    })
    .await;

    let (output, success) = match probe {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => return git_unavailable("check failed", None),
        Err(_) => {
            let _ = child.kill().await;
            return git_unavailable(
                "check timed out",
                Some("Git did not exit within five seconds."),
            );
        }
    };
    if !success {
        return git_unavailable("not available", None);
    }
    if output.len() > MAX_VERSION_OUTPUT_BYTES {
        return git_unavailable(
            "check failed",
            Some("Git version output exceeded the 1 KiB limit."),
        );
    }
    let version = parse_git_version(&output);
    result(
        "Git",
        "Git",
        DoctorCheckStatus::Pass,
        version.unwrap_or_else(|| "available".to_owned()),
        None,
    )
}

async fn bounded_command_stdout(command: &mut Command) -> Option<Vec<u8>> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let stdout = child.stdout.take()?;
    let probe = timeout(VERSION_PROBE_TIMEOUT, async {
        let read_output = read_probe_output(stdout);
        tokio::pin!(read_output);
        let wait_child = child.wait();
        tokio::pin!(wait_child);
        let (output, exit_status) = tokio::join!(read_output, wait_child);
        Ok::<_, io::Error>((output?, exit_status?.success()))
    })
    .await;

    match probe {
        Ok(Ok((output, true))) if output.len() <= MAX_VERSION_OUTPUT_BYTES => Some(output),
        Err(_) => {
            let _ = child.kill().await;
            None
        }
        _ => None,
    }
}

fn parse_unix_release(output: &[u8]) -> Option<String> {
    let release = std::str::from_utf8(output)
        .ok()?
        .trim()
        .split_whitespace()
        .next()?;
    safe_os_release(release)
}

#[cfg(windows)]
fn parse_windows_release(output: &[u8]) -> Option<String> {
    let version = output
        .split(|byte| !byte.is_ascii_digit() && *byte != b'.')
        .find(|part| part.contains(&b'.') && part.iter().any(u8::is_ascii_digit))?;
    safe_os_release(std::str::from_utf8(version).ok()?)
}

fn safe_os_release(release: &str) -> Option<String> {
    if release.is_empty()
        || release.len() > 128
        || !release.bytes().any(|byte| byte.is_ascii_digit())
        || !release.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+' | b'~')
        })
    {
        return None;
    }
    Some(release.to_owned())
}

async fn read_probe_output(stdout: ChildStdout) -> io::Result<Vec<u8>> {
    let mut limited = stdout.take((MAX_VERSION_OUTPUT_BYTES + 1) as u64);
    let mut output = Vec::with_capacity(MAX_VERSION_OUTPUT_BYTES + 1);
    limited.read_to_end(&mut output).await?;
    Ok(output)
}

fn parse_git_version(output: &[u8]) -> Option<String> {
    let line = std::str::from_utf8(output).ok()?.trim();
    let version = line
        .strip_prefix("git version ")?
        .split_whitespace()
        .next()?;
    if version.is_empty()
        || version.len() > 64
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return None;
    }
    Some(format!("git version {version}"))
}

fn git_unavailable(message: &str, detail: Option<&str>) -> DoctorCheckResult {
    result("Git", "Git", DoctorCheckStatus::Warn, message, detail)
}

fn safe_environment_name(value: &str) -> Option<&str> {
    let mut bytes = value.bytes();
    let first = bytes.next()?;
    if value.len() > 128
        || !(first == b'_' || first.is_ascii_alphabetic())
        || !bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
    {
        return None;
    }
    Some(value)
}

fn safe_identifier(value: &str) -> Option<&str> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return None;
    }
    Some(value)
}

fn safe_model_name(value: &str) -> Option<&str> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b':' | b'@')
        })
    {
        return None;
    }
    Some(value)
}

fn safe_server_name(value: &str) -> String {
    let safe: String = value
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ' ')
        })
        .take(80)
        .collect();
    if safe.is_empty() {
        "MCP server".to_owned()
    } else {
        safe
    }
}

fn result(
    category: impl Into<String>,
    name: impl Into<String>,
    status: DoctorCheckStatus,
    message: impl Into<String>,
    detail: Option<&str>,
) -> DoctorCheckResult {
    DoctorCheckResult {
        category: category.into(),
        name: name.into(),
        status,
        message: message.into(),
        detail: detail.map(str::to_owned),
    }
}
