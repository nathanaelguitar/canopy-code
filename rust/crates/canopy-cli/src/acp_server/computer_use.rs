//! ACP host wiring for the shared Rust Computer Use adapter.
//!
//! Every desktop action asks the connected ACP client for a one-call choice.
//! The model cannot reach the driver until that request selects `proceed_once`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use canopy_core::permissions::{PermissionCheckContext, PermissionDecision, PermissionRuleSet};
use canopy_core::tools::computer_use::{
    BootstrapFuture, BootstrapHost, BootstrapOptions, ComputerUseAdapterOptions,
    ComputerUseAgentToolAdapter, ComputerUseAuthorizationFuture, ComputerUseCallAuthorizer,
    ComputerUseClientOptions, ComputerUseProgress, InstallOptions, InstallState,
    NativeComputerUseDriverClient, PermissionKind, PermissionProbeResult, StatusDaemon,
    SystemArchiveExtractor, approval_key, binary_path, ensure_installed, is_package_spec_approved,
    save_install_state,
};
use canopy_core::turn::ToolCallRequestInfo;
use serde_json::{Map, Value, json};

use super::ProtocolOutput;

pub(super) fn build_adapter(
    output: ProtocolOutput,
    session_id: String,
    workspace_root: &Path,
    permissions: PermissionRuleSet,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
    enabled: bool,
    max_image_dimension: Option<f64>,
    idle_timeout_ms: Option<f64>,
    image_dimension_env: Option<&str>,
    approval_mode: Arc<super::AcpApprovalModeState>,
) -> Result<(Arc<ComputerUseAgentToolAdapter>, Vec<Value>), String> {
    let home = home_dir().ok_or_else(|| "could not determine the home directory".to_owned())?;
    let platform = host_platform();
    let arch = host_arch();
    let binary = match binary_path(
        &home,
        platform,
        arch,
        canopy_core::tools::computer_use::CUA_DRIVER_VERSION,
    ) {
        Ok(path) => path,
        Err(error) if enabled => return Err(error),
        Err(_) => home.join(".canopy/computer-use/disabled-driver"),
    };
    let image_dimension = canopy_core::tools::computer_use::resolve_max_image_dimension(
        max_image_dimension,
        image_dimension_env,
    )
    .filter(|value| value.is_finite() && *value >= 0.0 && value.fract() == 0.0)
    .filter(|value| *value <= u32::MAX as f64)
    .map(|value| value as u32);

    let progress: ComputerUseProgress = Arc::new(|message| {
        eprintln!("[Computer Use] {message}");
    });
    let mut client_options = ComputerUseClientOptions::new(binary);
    client_options.max_image_dimension = image_dimension;
    client_options.idle_timeout_ms = idle_timeout_ms;
    client_options.on_progress = Some(progress.clone());
    let driver = Arc::new(NativeComputerUseDriverClient::from_options(client_options));

    let authorizer = Arc::new(AcpComputerUseAuthorizer {
        output: output.clone(),
        session_id,
        home: home.clone(),
        workspace_root: workspace_root.to_path_buf(),
        permissions: permissions.clone(),
        core_tools: core_tools.clone(),
        excluded_tools: excluded_tools.clone(),
        approval_mode,
    });
    let host = Arc::new(AcpComputerUseBootstrapHost {
        home: home.clone(),
        progress,
        started_at: Instant::now(),
    });
    let mut bootstrap =
        BootstrapOptions::new(home, approval_key(env!("CARGO_PKG_VERSION")), platform);
    // The authorizer requested one-call ACP consent before the adapter reaches
    // bootstrap, so that consent also covers the first driver installation.
    bootstrap.auto_approve_install = true;
    let mut adapter_options = ComputerUseAdapterOptions::new(bootstrap);
    adapter_options.max_image_dimension = image_dimension;
    adapter_options.idle_timeout_ms = idle_timeout_ms;
    let adapter = Arc::new(
        ComputerUseAgentToolAdapter::new(driver, adapter_options)
            .with_authorizer(authorizer)
            .with_bootstrap_host(host),
    );

    let mut declarations = Vec::new();
    adapter
        .register_enabled_tools(
            enabled,
            |name| {
                canopy_core::tool_utils::is_tool_enabled(
                    name,
                    core_tools.as_deref(),
                    Some(&excluded_tools),
                ) && permission_decision(&permissions, workspace_root, name, None)
                    != PermissionDecision::Deny
            },
            &mut declarations,
        )
        .map_err(|error| error.to_string())?;
    Ok((adapter, declarations))
}

struct AcpComputerUseAuthorizer {
    output: ProtocolOutput,
    session_id: String,
    home: PathBuf,
    workspace_root: PathBuf,
    permissions: PermissionRuleSet,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
    approval_mode: Arc<super::AcpApprovalModeState>,
}

impl ComputerUseCallAuthorizer for AcpComputerUseAuthorizer {
    fn authorize<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        upstream_name: &'a str,
        params: &'a Map<String, Value>,
        high_risk: bool,
    ) -> ComputerUseAuthorizationFuture<'a> {
        Box::pin(async move {
            let tool_name = format!("computer_use__{upstream_name}");
            if !canopy_core::tool_utils::is_tool_enabled(
                &tool_name,
                self.core_tools.as_deref(),
                Some(&self.excluded_tools),
            ) {
                return Err(format!(
                    "Tool `{tool_name}` is disabled by the configured tools.core/tools.exclude settings."
                ));
            }
            if permission_decision(
                &self.permissions,
                &self.workspace_root,
                &tool_name,
                Some(&Value::Object(params.clone())),
            ) == PermissionDecision::Deny
            {
                return Err(format!("{tool_name} blocked by a permissions.deny rule."));
            }
            match self.approval_mode.current() {
                super::AcpApprovalMode::Plan => {
                    return Err(format!(
                        "Plan mode is active. The tool \"{tool_name}\" cannot be executed because it controls the desktop."
                    ));
                }
                super::AcpApprovalMode::Yolo => return Ok(()),
                super::AcpApprovalMode::Default
                | super::AcpApprovalMode::AutoEdit
                | super::AcpApprovalMode::Auto => {}
            }

            let package_spec = approval_key(env!("CARGO_PKG_VERSION"));
            let binary_exists = binary_path(
                &self.home,
                host_platform(),
                host_arch(),
                canopy_core::tools::computer_use::CUA_DRIVER_VERSION,
            )
            .is_ok_and(|path| path.exists());
            let install_note = if is_package_spec_approved(&self.home, &package_spec)
                && binary_exists
            {
                String::new()
            } else {
                "\n\nFirst use also downloads the signed and notarized Computer Use driver (~20 MB) into ~/.canopy/computer-use/. On macOS, Accessibility and Screen Recording permissions may be requested by System Settings."
                    .to_owned()
            };
            let args = serde_json::to_string_pretty(params)
                .map_err(|error| format!("could not display Computer Use arguments: {error}"))?;
            let risk = if high_risk {
                "HIGH-RISK desktop action. "
            } else {
                ""
            };
            let detail = format!(
                "Tool: {tool_name}\n\n{risk}Args:\n{args}\n\nThis action can operate desktop applications through the Computer Use driver.{install_note}"
            );
            let request = json!({
                "sessionId":self.session_id,
                "options":[
                    {"optionId":"proceed_once","name":"Allow","kind":"allow_once"},
                    {"optionId":"cancel","name":"Reject","kind":"reject_once"}
                ],
                "toolCall":{
                    "toolCallId":call.call_id,
                    "status":"pending",
                    "title":format!("Allow Computer Use ({upstream_name})"),
                    "kind":"other",
                    "content":[{"type":"content","content":{"type":"text","text":detail}}],
                    "rawInput":params,
                    "_meta":{"toolName":tool_name,"computerUse":true,"highRisk":high_risk}
                }
            });
            let response = self
                .output
                .request_client("session/request_permission", request)
                .await?;
            match response.pointer("/outcome/outcome").and_then(Value::as_str) {
                Some("selected")
                    if response
                        .pointer("/outcome/optionId")
                        .and_then(Value::as_str)
                        == Some("proceed_once") =>
                {
                    Ok(())
                }
                Some("cancelled") => Err(format!("{tool_name} declined by ACP client.")),
                Some("selected") => Err(format!(
                    "{tool_name} was not approved with the offered one-call option."
                )),
                _ => Err(
                    "ACP client returned an invalid Computer Use permission outcome.".to_owned(),
                ),
            }
        })
    }

    fn report_uncertain_cancellation(&self, upstream_name: &str) {
        let _ = self.output.notification(
            "session/update",
            json!({
                "sessionId":self.session_id,
                "update":{
                    "sessionUpdate":"agent_message_chunk",
                    "content":{
                        "type":"text",
                        "text":format!(
                            "Computer Use action `{upstream_name}` was interrupted after its MCP request began dispatching toward the desktop driver. The driver may have received it and may have completed all or part of the action. Inspect the desktop and application state before retrying."
                        )
                    }
                }
            }),
        );
    }
}

fn permission_decision(
    permissions: &PermissionRuleSet,
    workspace_root: &Path,
    tool_name: &str,
    params: Option<&Value>,
) -> PermissionDecision {
    permissions.evaluate(&PermissionCheckContext {
        tool_name,
        command: None,
        file_path: None,
        domain: None,
        specifier: None,
        tool_params: params,
        project_root: workspace_root,
        cwd: workspace_root,
    })
}

struct AcpComputerUseBootstrapHost {
    home: PathBuf,
    progress: ComputerUseProgress,
    started_at: Instant,
}

impl BootstrapHost for AcpComputerUseBootstrapHost {
    fn approval_is_granted(
        &self,
        home_dir: PathBuf,
        package_spec: String,
    ) -> BootstrapFuture<'_, bool> {
        Box::pin(async move { is_package_spec_approved(home_dir, &package_spec) })
    }

    fn prompt_install_approval(&self, _package_spec: String) -> BootstrapFuture<'_, bool> {
        // The per-call ACP permission request is the install consent surface.
        Box::pin(async { false })
    }

    fn save_install_approval(
        &self,
        state: InstallState,
    ) -> BootstrapFuture<'_, Result<(), String>> {
        Box::pin(async move {
            save_install_state(&self.home, &state)
                .map_err(|error| format!("could not save Computer Use install approval: {error}"))
        })
    }

    fn install<'a>(
        &'a self,
        _on_progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BootstrapFuture<'a, Result<(), String>> {
        let home = self.home.clone();
        let progress = self.progress.clone();
        Box::pin(async move {
            let options = InstallOptions::for_host(home)
                .map_err(|error| format!("Computer Use install setup failed: {error}"))?
                .with_extractor(Arc::new(SystemArchiveExtractor))
                .with_progress(progress);
            ensure_installed(&options).await.map(|_| ())
        })
    }

    fn start_status_daemon(&self) -> Box<dyn StatusDaemon> {
        AcpComputerUseStatusDaemon::start(self.home.clone())
    }

    fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult> {
        let binary = binary_path(
            &self.home,
            host_platform(),
            host_arch(),
            canopy_core::tools::computer_use::CUA_DRIVER_VERSION,
        );
        match binary {
            Ok(binary) => Box::pin(async move {
                let command = tokio::process::Command::new(binary)
                    .args(["permissions", "status", "--json"])
                    .output();
                match tokio::time::timeout(Duration::from_secs(10), command).await {
                    Ok(Ok(output)) if output.status.success() => {
                        let text = String::from_utf8_lossy(&output.stdout);
                        canopy_core::tools::computer_use::parse_permissions_status(&text)
                    }
                    _ => PermissionProbeResult::Unknown,
                }
            }),
            Err(_) => Box::pin(async { PermissionProbeResult::Unknown }),
        }
    }

    fn open_permission_pane(&self, kind: PermissionKind) {
        #[cfg(target_os = "macos")]
        {
            let anchor = match kind {
                PermissionKind::Accessibility => "Privacy_Accessibility",
                PermissionKind::ScreenRecording => "Privacy_ScreenCapture",
            };
            let url = format!("x-apple.systempreferences:com.apple.preference.security?{anchor}");
            let _ = std::process::Command::new("open")
                .arg(url)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        #[cfg(not(target_os = "macos"))]
        let _ = kind;
    }

    fn is_cancelled(&self) -> bool {
        false
    }

    fn update_output(&self, output: &str) {
        (self.progress)(output);
    }

    fn now_millis(&self) -> u64 {
        self.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    fn now_iso(&self) -> String {
        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    }

    fn sleep(&self, duration: Duration) -> BootstrapFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

struct AcpComputerUseStatusDaemon {
    home: PathBuf,
    killed: AtomicBool,
}

impl AcpComputerUseStatusDaemon {
    fn start(home: PathBuf) -> Box<dyn StatusDaemon> {
        #[cfg(target_os = "macos")]
        {
            kill_status_daemons(&home);
            let _ = std::process::Command::new("open")
                .args([
                    "-n",
                    "-g",
                    "-a",
                    "CuaDriver",
                    "--args",
                    "serve",
                    "--no-permissions-gate",
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        Box::new(Self {
            home,
            killed: AtomicBool::new(false),
        })
    }

    fn kill(&self) {
        if self.killed.swap(true, Ordering::AcqRel) {
            return;
        }
        #[cfg(target_os = "macos")]
        kill_status_daemons(&self.home);
        #[cfg(not(target_os = "macos"))]
        let _ = &self.home;
    }
}

impl StatusDaemon for AcpComputerUseStatusDaemon {
    fn kill(&self) {
        AcpComputerUseStatusDaemon::kill(self);
    }
}

impl Drop for AcpComputerUseStatusDaemon {
    fn drop(&mut self) {
        self.kill();
    }
}

#[cfg(target_os = "macos")]
fn kill_status_daemons(home: &Path) {
    let _ = std::process::Command::new("pkill")
        .args(["-f", "CuaDriver.app/Contents/MacOS/cua-driver serve"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    let socket = home
        .join("Library")
        .join("Caches")
        .join("cua-driver")
        .join("cua-driver.sock");
    let _ = std::fs::remove_file(socket);
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

fn host_platform() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        "linux" => "linux",
        other => other,
    }
}

fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    }
}
