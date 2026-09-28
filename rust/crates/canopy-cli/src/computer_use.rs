//! Interactive CLI host for the Rust Computer Use adapter.
//!
//! `canopy run` has a terminal approval surface, so it can authorize one CUA
//! action at a time and use the shared bootstrap/client implementation. ACP
//! deliberately does not compose this adapter because it has no permission
//! request UI yet.

use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
use serde_json::{Map, Value};

pub(crate) fn build_adapter(
    home: PathBuf,
    workspace_root: &Path,
    permissions: PermissionRuleSet,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
    enabled: bool,
    max_image_dimension: Option<f64>,
    idle_timeout_ms: Option<f64>,
) -> Result<(Arc<ComputerUseAgentToolAdapter>, Vec<Value>), String> {
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
    let max_image_dimension = max_image_dimension
        .filter(|value| value.is_finite() && *value >= 0.0 && value.fract() == 0.0)
        .filter(|value| *value <= u32::MAX as f64)
        .map(|value| value as u32);
    let progress: ComputerUseProgress = Arc::new(|message| {
        eprintln!("[Computer Use] {message}");
    });
    let mut client_options = ComputerUseClientOptions::new(binary);
    client_options.max_image_dimension = max_image_dimension;
    client_options.idle_timeout_ms = idle_timeout_ms;
    client_options.on_progress = Some(progress.clone());
    let driver = Arc::new(NativeComputerUseDriverClient::from_options(client_options));

    let prompt: Arc<dyn ComputerUseApprovalPrompt> = Arc::new(TerminalComputerUsePrompt);
    let authorizer = Arc::new(CliComputerUseAuthorizer {
        permissions: permissions.clone(),
        workspace_root: workspace_root.to_path_buf(),
        core_tools: core_tools.clone(),
        excluded_tools: excluded_tools.clone(),
        home: home.clone(),
        prompt,
    });
    let host = Arc::new(CliComputerUseBootstrapHost::new(
        home.clone(),
        Arc::new(SystemComputerUseBackend),
        progress,
    ));

    let mut bootstrap =
        BootstrapOptions::new(home, approval_key(env!("CARGO_PKG_VERSION")), platform);
    // The per-action authorizer runs first. Reaching bootstrap therefore
    // already represents the user's approval for this install plus action.
    bootstrap.auto_approve_install = true;
    let mut options = ComputerUseAdapterOptions::new(bootstrap);
    options.max_image_dimension = max_image_dimension;
    options.idle_timeout_ms = idle_timeout_ms;
    let adapter = Arc::new(
        ComputerUseAgentToolAdapter::new(driver, options)
            .with_authorizer(authorizer)
            .with_bootstrap_host(host),
    );

    let mut declarations = Vec::new();
    adapter
        .register_enabled_tools(
            enabled,
            |name| {
                if !canopy_core::tool_utils::is_tool_enabled(
                    name,
                    core_tools.as_deref(),
                    Some(&excluded_tools),
                ) {
                    return false;
                }
                permission_decision(&permissions, workspace_root, name, None)
                    != PermissionDecision::Deny
            },
            &mut declarations,
        )
        .map_err(|error| error.to_string())?;
    Ok((adapter, declarations))
}

trait ComputerUseApprovalPrompt: Send + Sync {
    fn is_available(&self) -> bool;
    fn confirm(&self, prompt: &str) -> Result<bool, String>;
}

struct TerminalComputerUsePrompt;

impl ComputerUseApprovalPrompt for TerminalComputerUsePrompt {
    fn is_available(&self) -> bool {
        io::stdin().is_terminal() && io::stderr().is_terminal()
    }

    fn confirm(&self, prompt: &str) -> Result<bool, String> {
        if !self.is_available() {
            return Ok(false);
        }
        eprintln!("{prompt}");
        eprint!("Allow this Computer Use action? [y/N] ");
        io::stderr()
            .flush()
            .map_err(|error| format!("could not display Computer Use approval: {error}"))?;
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .map_err(|error| format!("could not read Computer Use approval: {error}"))?;
        Ok(matches!(
            answer.trim().to_ascii_lowercase().as_str(),
            "y" | "yes"
        ))
    }
}

struct CliComputerUseAuthorizer {
    permissions: PermissionRuleSet,
    workspace_root: PathBuf,
    core_tools: Option<Vec<String>>,
    excluded_tools: Vec<String>,
    home: PathBuf,
    prompt: Arc<dyn ComputerUseApprovalPrompt>,
}

impl ComputerUseCallAuthorizer for CliComputerUseAuthorizer {
    fn authorize<'a>(
        &'a self,
        call: &'a ToolCallRequestInfo,
        upstream_name: &'a str,
        params: &'a Map<String, Value>,
        high_risk: bool,
    ) -> ComputerUseAuthorizationFuture<'a> {
        Box::pin(async move {
            let name = format!("computer_use__{upstream_name}");
            if !canopy_core::tool_utils::is_tool_enabled(
                &name,
                self.core_tools.as_deref(),
                Some(&self.excluded_tools),
            ) {
                return Err(format!(
                    "Tool `{name}` is disabled by the configured tools.core/tools.exclude settings."
                ));
            }
            let params_value = Value::Object(params.clone());
            match permission_decision(
                &self.permissions,
                &self.workspace_root,
                &name,
                Some(&params_value),
            ) {
                PermissionDecision::Deny => {
                    return Err(format!("{name} blocked by a permissions.deny rule."));
                }
                PermissionDecision::Allow => return Ok(()),
                PermissionDecision::Ask | PermissionDecision::Default => {}
            }
            if !self.prompt.is_available() {
                return Err(format!(
                    "{name} requires approval; no interactive approval surface is available. Add a matching permissions.allow rule to enable it in headless runs."
                ));
            }

            let install_approved =
                is_package_spec_approved(&self.home, &approval_key(env!("CARGO_PKG_VERSION")));
            let args = serde_json::to_string_pretty(params)
                .map_err(|error| format!("could not display Computer Use arguments: {error}"))?;
            let risk = if high_risk {
                "HIGH-RISK desktop action. "
            } else {
                ""
            };
            let install = if install_approved {
                String::new()
            } else {
                "This first use also downloads the signed and notarized driver (~20 MB) into ~/.canopy/computer-use/. On macOS, you'll be guided through Accessibility and Screen Recording permissions after the download.\n\n".to_owned()
            };
            let prompt = format!(
                "Tool: {name}\nCall ID: {}\n\n{risk}Args:\n{args}\n\n{install}This can act on your desktop through the Computer Use driver.",
                call.call_id
            );
            if self.prompt.confirm(&prompt)? {
                Ok(())
            } else {
                Err(format!("{name} declined by user."))
            }
        })
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

trait ComputerUseBootstrapBackend: Send + Sync {
    fn install(
        &self,
        home: PathBuf,
        progress: ComputerUseProgress,
    ) -> BootstrapFuture<'_, Result<(), String>>;
    fn start_status_daemon(&self, home: PathBuf) -> Box<dyn StatusDaemon>;
    fn probe_permissions(&self, binary: PathBuf) -> BootstrapFuture<'_, PermissionProbeResult>;
    fn open_permission_pane(&self, kind: PermissionKind);
}

struct CliComputerUseBootstrapHost {
    home: PathBuf,
    backend: Arc<dyn ComputerUseBootstrapBackend>,
    progress: ComputerUseProgress,
    started_at: Instant,
}

impl CliComputerUseBootstrapHost {
    fn new(
        home: PathBuf,
        backend: Arc<dyn ComputerUseBootstrapBackend>,
        progress: ComputerUseProgress,
    ) -> Self {
        Self {
            home,
            backend,
            progress,
            started_at: Instant::now(),
        }
    }
}

impl BootstrapHost for CliComputerUseBootstrapHost {
    fn approval_is_granted(
        &self,
        home_dir: PathBuf,
        package_spec: String,
    ) -> BootstrapFuture<'_, bool> {
        Box::pin(async move { is_package_spec_approved(home_dir, &package_spec) })
    }

    fn prompt_install_approval(&self, package_spec: String) -> BootstrapFuture<'_, bool> {
        Box::pin(async move {
            let prompt = format!(
                "Computer Use first-time setup\nDriver: {package_spec}\nThis downloads a signed driver (~20 MB) into ~/.canopy/computer-use/.\nComputer Use can click, type, and read your desktop apps."
            );
            TerminalComputerUsePrompt.confirm(&prompt).unwrap_or(false)
        })
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
        self.backend
            .install(self.home.clone(), self.progress.clone())
    }

    fn start_status_daemon(&self) -> Box<dyn StatusDaemon> {
        self.backend.start_status_daemon(self.home.clone())
    }

    fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult> {
        let binary = binary_path(
            &self.home,
            host_platform(),
            host_arch(),
            canopy_core::tools::computer_use::CUA_DRIVER_VERSION,
        );
        match binary {
            Ok(binary) => self.backend.probe_permissions(binary),
            Err(_) => Box::pin(async { PermissionProbeResult::Unknown }),
        }
    }

    fn open_permission_pane(&self, kind: PermissionKind) {
        self.backend.open_permission_pane(kind);
    }

    fn is_cancelled(&self) -> bool {
        // The CLI currently has process-level Ctrl-C handling only; the CUA
        // bootstrap does not yet receive a per-call cancellation token.
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

struct SystemComputerUseBackend;

impl ComputerUseBootstrapBackend for SystemComputerUseBackend {
    fn install(
        &self,
        home: PathBuf,
        progress: ComputerUseProgress,
    ) -> BootstrapFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let options = InstallOptions::for_host(home)
                .map_err(|error| format!("Computer Use install setup failed: {error}"))?
                .with_extractor(Arc::new(SystemArchiveExtractor))
                .with_progress(progress);
            ensure_installed(&options).await.map(|_| ())
        })
    }

    fn start_status_daemon(&self, home: PathBuf) -> Box<dyn StatusDaemon> {
        SystemStatusDaemon::start(home)
    }

    fn probe_permissions(&self, binary: PathBuf) -> BootstrapFuture<'_, PermissionProbeResult> {
        Box::pin(async move {
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
        })
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
}

struct SystemStatusDaemon {
    home: PathBuf,
}

impl SystemStatusDaemon {
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
        Box::new(Self { home })
    }
}

impl StatusDaemon for SystemStatusDaemon {
    fn kill(&self) {
        #[cfg(target_os = "macos")]
        kill_status_daemons(&self.home);
        #[cfg(not(target_os = "macos"))]
        let _ = &self.home;
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

fn host_platform() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        "linux" => "linux",
        _ => std::env::consts::OS,
    }
}

fn host_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "arm64",
        "x86_64" => "x64",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use canopy_core::permissions::{PermissionDecision, PermissionRuleSet};
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    static NEXT_HOME: AtomicU64 = AtomicU64::new(0);

    struct TempHome(PathBuf);

    impl TempHome {
        fn new() -> Self {
            let id = NEXT_HOME.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "canopy-cli-computer-use-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct MockPrompt {
        available: bool,
        answer: bool,
        calls: AtomicUsize,
        last_prompt: Mutex<String>,
    }

    impl ComputerUseApprovalPrompt for MockPrompt {
        fn is_available(&self) -> bool {
            self.available
        }

        fn confirm(&self, prompt: &str) -> Result<bool, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_prompt.lock().unwrap() = prompt.to_owned();
            Ok(self.answer)
        }
    }

    fn call() -> ToolCallRequestInfo {
        ToolCallRequestInfo {
            call_id: "call-9".to_owned(),
            provider_call_id: None,
            name: "computer_use__click".to_owned(),
            args: json!({"x": 24, "y": 81}),
            is_client_initiated: false,
            prompt_id: "prompt-1".to_owned(),
            response_id: None,
            was_output_truncated: None,
            goal_context: None,
        }
    }

    fn authorizer(
        permissions: PermissionRuleSet,
        home: &Path,
        prompt: Arc<MockPrompt>,
    ) -> CliComputerUseAuthorizer {
        CliComputerUseAuthorizer {
            permissions,
            workspace_root: PathBuf::from("/workspace"),
            core_tools: None,
            excluded_tools: Vec::new(),
            home: home.to_path_buf(),
            prompt,
        }
    }

    #[tokio::test]
    async fn default_policy_prompts_with_action_arguments_and_install_notice() {
        let home = TempHome::new();
        let prompt = Arc::new(MockPrompt {
            available: true,
            answer: true,
            ..MockPrompt::default()
        });
        let authorizer = authorizer(PermissionRuleSet::default(), &home.0, prompt.clone());
        let call = call();
        authorizer
            .authorize(&call, "click", call.args.as_object().unwrap(), false)
            .await
            .unwrap();
        let prompt_text = prompt.last_prompt.lock().unwrap().clone();
        assert!(prompt_text.contains("computer_use__click"));
        assert!(prompt_text.contains("\"x\": 24"));
        assert!(prompt_text.contains("first use also downloads"));
        assert_eq!(prompt.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn headless_default_and_declined_calls_fail_closed() {
        let home = TempHome::new();
        let no_surface = Arc::new(MockPrompt::default());
        let headless_authorizer =
            authorizer(PermissionRuleSet::default(), &home.0, no_surface.clone());
        let call = call();
        let denied = headless_authorizer
            .authorize(&call, "click", call.args.as_object().unwrap(), false)
            .await
            .unwrap_err();
        assert!(denied.contains("no interactive approval surface"));
        assert_eq!(no_surface.calls.load(Ordering::SeqCst), 0);

        let decline = Arc::new(MockPrompt {
            available: true,
            answer: false,
            ..MockPrompt::default()
        });
        let declined_authorizer =
            authorizer(PermissionRuleSet::default(), &home.0, decline.clone());
        let call = call();
        assert!(
            declined_authorizer
                .authorize(&call, "click", call.args.as_object().unwrap(), true)
                .await
                .unwrap_err()
                .contains("declined by user")
        );
        assert!(decline.last_prompt.lock().unwrap().contains("HIGH-RISK"));
    }

    #[tokio::test]
    async fn explicit_allow_skips_prompt_and_deny_blocks_before_prompt() {
        let home = TempHome::new();
        let allow_prompt = Arc::new(MockPrompt::default());
        let allowed = authorizer(
            PermissionRuleSet::from_raw(
                ["computer_use__click".to_owned()],
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
            &home.0,
            allow_prompt.clone(),
        );
        let call = call();
        allowed
            .authorize(&call, "click", call.args.as_object().unwrap(), true)
            .await
            .unwrap();
        assert_eq!(allow_prompt.calls.load(Ordering::SeqCst), 0);

        let deny_prompt = Arc::new(MockPrompt {
            available: true,
            answer: true,
            ..MockPrompt::default()
        });
        let denied = authorizer(
            PermissionRuleSet::from_raw(
                Vec::<String>::new(),
                Vec::<String>::new(),
                ["computer_use__click".to_owned()],
            ),
            &home.0,
            deny_prompt.clone(),
        );
        assert!(
            denied
                .authorize(&call, "click", call.args.as_object().unwrap(), false)
                .await
                .unwrap_err()
                .contains("permissions.deny")
        );
        assert_eq!(deny_prompt.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn setting_filter_requires_feature_opt_in_and_uses_existing_policy_filters() {
        let cwd = PathBuf::from("/workspace");
        let home = TempHome::new();
        let (adapter, mut declarations) = build_adapter(
            home.0.clone(),
            &cwd,
            PermissionRuleSet::default(),
            None,
            Vec::new(),
            false,
            None,
            None,
        )
        .unwrap();
        assert!(!adapter.is_registered("computer_use__click"));
        assert!(declarations.is_empty());

        let (adapter, declarations) = build_adapter(
            home.0.clone(),
            &cwd,
            PermissionRuleSet::from_raw(
                Vec::<String>::new(),
                Vec::<String>::new(),
                ["computer_use__click".to_owned()],
            ),
            None,
            Vec::new(),
            true,
            None,
            None,
        )
        .unwrap();
        assert!(!adapter.is_registered("computer_use__click"));
        assert!(declarations.is_empty());

        let (_adapter, declarations) = build_adapter(
            home.0,
            &cwd,
            PermissionRuleSet::default(),
            Some(vec!["computer_use__click".to_owned()]),
            Vec::new(),
            true,
            None,
            None,
        )
        .unwrap();
        let names = declarations
            .iter()
            .filter_map(|group| group["functionDeclarations"].as_array())
            .flatten()
            .filter_map(|declaration| declaration["name"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["computer_use__click"]);
    }

    #[tokio::test]
    async fn bootstrap_host_uses_exact_install_approval_store_without_external_calls() {
        let home = TempHome::new();
        let host = CliComputerUseBootstrapHost::new(
            home.0.clone(),
            Arc::new(MockBackend::default()),
            Arc::new(|_| {}),
        );
        assert!(
            !host
                .approval_is_granted(home.0.clone(), approval_key())
                .await
        );
        let state = InstallState {
            approved_package_spec: approval_key(),
            approved_at_iso: "2026-09-25T00:00:00.000Z".to_owned(),
        };
        host.save_install_approval(state.clone()).await.unwrap();
        assert!(
            host.approval_is_granted(home.0.clone(), approval_key())
                .await
        );
        assert!(!is_package_spec_approved(&home.0, "another-driver@1"));
    }

    #[tokio::test]
    async fn bootstrap_host_forwards_download_progress_through_mock_backend() {
        let updates = Arc::new(Mutex::new(Vec::<String>::new()));
        let output = Arc::clone(&updates);
        let host = CliComputerUseBootstrapHost::new(
            PathBuf::from("/not-used"),
            Arc::new(MockBackend),
            Arc::new(move |message| output.lock().unwrap().push(message.to_owned())),
        );
        let on_progress = |_message: &str| {};
        host.install(&on_progress).await.unwrap();
        assert_eq!(
            *updates.lock().unwrap(),
            vec!["mock download progress".to_owned()]
        );
    }

    #[tokio::test]
    async fn bootstrap_host_forwards_permission_operations_to_mock_backend() {
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let backend = Arc::new(MockBackend::with_events(events.clone()));
        let host = CliComputerUseBootstrapHost::new(
            PathBuf::from("/mock-home"),
            backend,
            Arc::new(|_| {}),
        );
        let daemon = host.start_status_daemon();
        daemon.kill();
        assert_eq!(
            host.probe_permissions().await,
            PermissionProbeResult::Accessibility
        );
        host.open_permission_pane(PermissionKind::Accessibility);
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "status:start".to_owned(),
                "status:kill".to_owned(),
                "permissions:probe".to_owned(),
                "settings:accessibility".to_owned(),
            ]
        );
    }

    #[derive(Default)]
    struct MockBackend {
        events: Arc<Mutex<Vec<String>>>,
    }

    impl MockBackend {
        fn with_events(events: Arc<Mutex<Vec<String>>>) -> Self {
            Self { events }
        }
    }

    impl ComputerUseBootstrapBackend for MockBackend {
        fn install(
            &self,
            _home: PathBuf,
            progress: ComputerUseProgress,
        ) -> BootstrapFuture<'_, Result<(), String>> {
            Box::pin(async move {
                self.events.lock().unwrap().push("install".to_owned());
                progress("mock download progress");
                Ok(())
            })
        }

        fn start_status_daemon(&self, _home: PathBuf) -> Box<dyn StatusDaemon> {
            self.events.lock().unwrap().push("status:start".to_owned());
            Box::new(MockDaemon(Arc::clone(&self.events)))
        }

        fn probe_permissions(
            &self,
            _binary: PathBuf,
        ) -> BootstrapFuture<'_, PermissionProbeResult> {
            Box::pin(async move {
                self.events
                    .lock()
                    .unwrap()
                    .push("permissions:probe".to_owned());
                PermissionProbeResult::Accessibility
            })
        }

        fn open_permission_pane(&self, kind: PermissionKind) {
            self.events.lock().unwrap().push(match kind {
                PermissionKind::Accessibility => "settings:accessibility".to_owned(),
                PermissionKind::ScreenRecording => "settings:screen-recording".to_owned(),
            });
        }
    }

    struct MockDaemon(Arc<Mutex<Vec<String>>>);

    impl StatusDaemon for MockDaemon {
        fn kill(&self) {
            self.0.lock().unwrap().push("status:kill".to_owned());
        }
    }

    #[test]
    fn policy_context_preserves_cua_parameter_scoped_asks() {
        let rules = PermissionRuleSet::from_raw(
            Vec::<String>::new(),
            ["computer_use__click(x:24)".to_owned()],
            Vec::<String>::new(),
        );
        let args = json!({"x":24,"y":81});
        assert_eq!(
            permission_decision(
                &rules,
                Path::new("/workspace"),
                "computer_use__click",
                Some(&args)
            ),
            PermissionDecision::Ask
        );
        assert_eq!(
            permission_decision(
                &rules,
                Path::new("/workspace"),
                "computer_use__click",
                Some(&json!({"x":25}))
            ),
            PermissionDecision::Default
        );
    }
}
