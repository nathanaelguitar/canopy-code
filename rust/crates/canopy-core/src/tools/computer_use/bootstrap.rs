//! Host-independent port of the computer-use install and permission bootstrap.
//!
//! This module contains the state machine from `packages/core/src/tools/computer-use/bootstrap.ts`.
//! Approval storage, prompting, installation, daemon lifecycle, macOS settings,
//! cancellation, progress reporting, and timing are injected through
//! [`BootstrapHost`] so the flow can be tested without the driver or macOS APIs.

use super::InstallState;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

/// Boxed async result used by the injectable bootstrap interfaces.
pub type BootstrapFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Result of a per-permission TCC status probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionProbeResult {
    /// Accessibility and Screen Recording are both granted.
    Ok,
    /// Accessibility is missing.
    Accessibility,
    /// Accessibility is present, but Screen Recording is missing.
    ScreenRecording,
    /// The status daemon is absent, starting, or restarting.
    Unknown,
}

/// Parse `cua-driver permissions status --json` like the TypeScript bootstrap.
///
/// Accessibility must be a JSON boolean. Once it is true, JavaScript
/// truthiness is used for `screen_recording`, matching the source's
/// `if (!o.screen_recording)` check.
pub fn parse_permissions_status(json: &str) -> PermissionProbeResult {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(json) else {
        return PermissionProbeResult::Unknown;
    };
    let Some(accessibility) = value.get("accessibility").and_then(|v| v.as_bool()) else {
        return PermissionProbeResult::Unknown;
    };
    if !accessibility {
        return PermissionProbeResult::Accessibility;
    }
    if !value
        .get("screen_recording")
        .is_some_and(json_javascript_truthy)
    {
        return PermissionProbeResult::ScreenRecording;
    }
    PermissionProbeResult::Ok
}

fn json_javascript_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::Number(value) => value.as_f64().is_some_and(|n| n != 0.0),
        serde_json::Value::String(value) => !value.is_empty(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => true,
    }
}

/// A status-only daemon that must be stopped on every exit from the
/// permission flow, including cancellation, probe failure, and timeout.
pub trait StatusDaemon: Send + Sync {
    fn kill(&self);
}

/// Injected host operations used by the bootstrap state machine.
///
/// Implementations should make `probe_permissions` return `Unknown` when the
/// underlying command fails, as `probePermissionsViaStatus` does in Node.
pub trait BootstrapHost: Send + Sync {
    fn approval_is_granted(
        &self,
        home_dir: PathBuf,
        approval_key: String,
    ) -> BootstrapFuture<'_, bool>;

    fn prompt_install_approval(&self, approval_key: String) -> BootstrapFuture<'_, bool>;

    fn save_install_approval(&self, state: InstallState)
    -> BootstrapFuture<'_, Result<(), String>>;

    fn install<'a>(
        &'a self,
        on_progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BootstrapFuture<'a, Result<(), String>>;

    fn start_status_daemon(&self) -> Box<dyn StatusDaemon>;

    fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult>;

    fn open_permission_pane(&self, kind: PermissionKind);

    fn is_cancelled(&self) -> bool;

    fn update_output(&self, output: &str);

    /// Monotonic milliseconds, used for timeout and elapsed-time reporting.
    fn now_millis(&self) -> u64;

    /// ISO timestamp used in the persisted approval record.
    fn now_iso(&self) -> String;

    fn sleep(&self, duration: Duration) -> BootstrapFuture<'_, ()>;
}

/// The macOS System Settings pane to open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionKind {
    Accessibility,
    ScreenRecording,
}

/// Client operations required by the bootstrap flow.
pub trait ComputerUseClient: Send + Sync {
    fn is_started(&self) -> bool;

    fn start<'a>(
        &'a self,
        on_progress: &'a (dyn Fn(&str) + Send + Sync),
    ) -> BootstrapFuture<'a, Result<(), String>>;
}

/// Inputs corresponding to `BootstrapContext` and the per-call dependencies.
#[derive(Clone, Debug)]
pub struct BootstrapOptions {
    pub home_dir: PathBuf,
    pub approval_key: String,
    /// Node's `process.platform`; permission prompts run only for `darwin`.
    pub platform: String,
    pub auto_approve_install: bool,
    pub poll_interval: Duration,
    pub poll_timeout: Duration,
}

impl BootstrapOptions {
    pub fn new(
        home_dir: impl Into<PathBuf>,
        approval_key: impl Into<String>,
        platform: impl Into<String>,
    ) -> Self {
        Self {
            home_dir: home_dir.into(),
            approval_key: approval_key.into(),
            platform: platform.into(),
            auto_approve_install: false,
            poll_interval: Duration::from_secs(5),
            poll_timeout: Duration::from_secs(10 * 60),
        }
    }
}

/// Run first-use approval, install, permission, and client startup in order.
///
/// A started client is checked before any host calls, matching the warm-client
/// short-circuit in the TypeScript implementation.
pub async fn run_bootstrap<C: ComputerUseClient, H: BootstrapHost>(
    client: &C,
    host: &H,
    options: &BootstrapOptions,
) -> Result<(), String> {
    if client.is_started() {
        return Ok(());
    }

    if !host
        .approval_is_granted(options.home_dir.clone(), options.approval_key.clone())
        .await
    {
        if options.auto_approve_install {
            host.update_output("Computer Use install auto-approved (approval mode).");
        } else {
            host.update_output("Computer Use needs a one-time driver download (first use).");
            if !host
                .prompt_install_approval(options.approval_key.clone())
                .await
            {
                return Err("Computer Use install declined by user. Re-invoke the tool to be prompted again.".to_owned());
            }
        }

        host.save_install_approval(InstallState {
            approved_package_spec: options.approval_key.clone(),
            approved_at_iso: host.now_iso(),
        })
        .await?;
    }

    let progress = |message: &str| host.update_output(message);
    host.install(&progress).await?;

    if options.platform == "darwin" {
        ensure_permissions(host, options).await?;
    }

    client.start(&progress).await
}

async fn ensure_permissions<H: BootstrapHost>(
    host: &H,
    options: &BootstrapOptions,
) -> Result<(), String> {
    let mut daemon = host.start_status_daemon();
    let result = poll_permissions(host, options, &mut daemon).await;
    daemon.kill();
    result
}

async fn poll_permissions<H: BootstrapHost>(
    host: &H,
    options: &BootstrapOptions,
    daemon: &mut Box<dyn StatusDaemon>,
) -> Result<(), String> {
    let started_at = host.now_millis();
    let mut opened_accessibility = false;
    let mut opened_screen_recording = false;

    loop {
        if host.is_cancelled() {
            return Err("Computer Use bootstrap aborted.".to_owned());
        }
        let elapsed_ms = host.now_millis().saturating_sub(started_at);
        if elapsed_ms > options.poll_timeout.as_millis().min(u64::MAX as u128) as u64 {
            let seconds = rounded_seconds(options.poll_timeout);
            return Err(format!(
                "Computer Use permission grant timed out after {seconds}s. Re-invoke the tool to retry."
            ));
        }

        host.sleep(options.poll_interval).await;
        let probe = host.probe_permissions().await;

        if probe == PermissionProbeResult::Ok {
            return Ok(());
        }

        if probe == PermissionProbeResult::Unknown {
            // A screen-recording grant may restart the app. Relaunch the
            // status-only daemon and continue polling until it reports again.
            daemon.kill();
            *daemon = host.start_status_daemon();
            let elapsed = rounded_millis_to_seconds(host.now_millis().saturating_sub(started_at));
            host.update_output(&format!(
                "Bringing up Computer Use permissions check… ({elapsed}s)"
            ));
            continue;
        }

        if probe == PermissionProbeResult::Accessibility {
            if !opened_accessibility {
                opened_accessibility = true;
                host.open_permission_pane(PermissionKind::Accessibility);
                host.update_output(
                    "Step 1/2 — In the System Settings window that opened (Privacy & Security → Accessibility), turn ON CuaDriver. This continues automatically.",
                );
            } else {
                let elapsed =
                    rounded_millis_to_seconds(host.now_millis().saturating_sub(started_at));
                host.update_output(&format!(
                    "Waiting for Accessibility… ({elapsed}s) — enable CuaDriver in System Settings."
                ));
            }
            continue;
        }

        // Remaining probe is ScreenRecording.
        if !opened_screen_recording {
            opened_screen_recording = true;
            host.open_permission_pane(PermissionKind::ScreenRecording);
            host.update_output(
                "Step 2/2 — Accessibility granted. Now in System Settings (Privacy & Security → Screen & System Audio Recording), turn ON CuaDriver. macOS will ask to restart CuaDriver — allow it; that is expected. This continues automatically.",
            );
        } else {
            let elapsed = rounded_millis_to_seconds(host.now_millis().saturating_sub(started_at));
            host.update_output(&format!(
                "Waiting for Screen Recording… ({elapsed}s) — enable CuaDriver in System Settings."
            ));
        }
    }
}

fn rounded_seconds(duration: Duration) -> u64 {
    rounded_millis_to_seconds(duration.as_millis().min(u64::MAX as u128) as u64)
}

fn rounded_millis_to_seconds(milliseconds: u64) -> u64 {
    milliseconds.saturating_add(500) / 1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Events {
        log: Mutex<Vec<String>>,
        probes: Mutex<VecDeque<PermissionProbeResult>>,
        now: AtomicU64,
        cancelled: AtomicBool,
        cancel_on_sleep: AtomicBool,
        approved: AtomicBool,
        prompt_result: AtomicBool,
        fail_install: AtomicBool,
        fail_start: AtomicBool,
        daemon_starts: AtomicUsize,
        daemon_kills: AtomicUsize,
        install_calls: AtomicUsize,
        client_start_calls: AtomicUsize,
    }

    impl Events {
        fn record(&self, event: impl Into<String>) {
            self.log.lock().unwrap().push(event.into());
        }

        fn events(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    struct MockHost(Arc<Events>);

    struct MockDaemon(Arc<Events>);

    impl StatusDaemon for MockDaemon {
        fn kill(&self) {
            self.0.daemon_kills.fetch_add(1, Ordering::SeqCst);
            self.0.record("daemon:kill");
        }
    }

    impl BootstrapHost for MockHost {
        fn approval_is_granted(
            &self,
            _home_dir: PathBuf,
            _approval_key: String,
        ) -> BootstrapFuture<'_, bool> {
            Box::pin(async move {
                self.0.record("approval:read");
                self.0.approved.load(Ordering::SeqCst)
            })
        }

        fn prompt_install_approval(&self, _approval_key: String) -> BootstrapFuture<'_, bool> {
            Box::pin(async move {
                self.0.record("approval:prompt");
                self.0.prompt_result.load(Ordering::SeqCst)
            })
        }

        fn save_install_approval(
            &self,
            state: InstallState,
        ) -> BootstrapFuture<'_, Result<(), String>> {
            Box::pin(async move {
                self.0
                    .record(format!("approval:save:{}", state.approved_at_iso));
                Ok(())
            })
        }

        fn install<'a>(
            &'a self,
            on_progress: &'a (dyn Fn(&str) + Send + Sync),
        ) -> BootstrapFuture<'a, Result<(), String>> {
            Box::pin(async move {
                self.0.install_calls.fetch_add(1, Ordering::SeqCst);
                self.0.record("install");
                on_progress("download progress");
                if self.0.fail_install.load(Ordering::SeqCst) {
                    Err("install failed".to_owned())
                } else {
                    Ok(())
                }
            })
        }

        fn start_status_daemon(&self) -> Box<dyn StatusDaemon> {
            self.0.daemon_starts.fetch_add(1, Ordering::SeqCst);
            self.0.record("daemon:start");
            Box::new(MockDaemon(self.0.clone()))
        }

        fn probe_permissions(&self) -> BootstrapFuture<'_, PermissionProbeResult> {
            Box::pin(async move {
                self.0.record("permissions:probe");
                self.0
                    .probes
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(PermissionProbeResult::Unknown)
            })
        }

        fn open_permission_pane(&self, kind: PermissionKind) {
            self.0.record(match kind {
                PermissionKind::Accessibility => "settings:accessibility",
                PermissionKind::ScreenRecording => "settings:screen-recording",
            });
        }

        fn is_cancelled(&self) -> bool {
            self.0.cancelled.load(Ordering::SeqCst)
        }

        fn update_output(&self, output: &str) {
            self.0.record(format!("output:{output}"));
        }

        fn now_millis(&self) -> u64 {
            self.0.now.load(Ordering::SeqCst)
        }

        fn now_iso(&self) -> String {
            "2026-09-25T12:00:00.000Z".to_owned()
        }

        fn sleep(&self, duration: Duration) -> BootstrapFuture<'_, ()> {
            Box::pin(async move {
                self.0.now.fetch_add(
                    duration.as_millis().min(u64::MAX as u128) as u64,
                    Ordering::SeqCst,
                );
                self.0.record("sleep");
                if self.0.cancel_on_sleep.load(Ordering::SeqCst) {
                    self.0.cancelled.store(true, Ordering::SeqCst);
                }
            })
        }
    }

    struct MockClient {
        events: Arc<Events>,
        started: bool,
    }

    impl ComputerUseClient for MockClient {
        fn is_started(&self) -> bool {
            self.started
        }

        fn start<'a>(
            &'a self,
            on_progress: &'a (dyn Fn(&str) + Send + Sync),
        ) -> BootstrapFuture<'a, Result<(), String>> {
            Box::pin(async move {
                self.events
                    .client_start_calls
                    .fetch_add(1, Ordering::SeqCst);
                self.events.record("client:start");
                on_progress("client progress");
                if self.events.fail_start.load(Ordering::SeqCst) {
                    Err("client start failed".to_owned())
                } else {
                    Ok(())
                }
            })
        }
    }

    fn setup(platform: &str) -> (Arc<Events>, MockHost, MockClient, BootstrapOptions) {
        let events = Arc::new(Events::default());
        events.prompt_result.store(true, Ordering::SeqCst);
        let host = MockHost(events.clone());
        let client = MockClient {
            events: events.clone(),
            started: false,
        };
        let mut options = BootstrapOptions::new("/home/test", "cua-driver-rs@0.5.2", platform);
        options.poll_interval = Duration::from_millis(1);
        options.poll_timeout = Duration::from_millis(100);
        (events, host, client, options)
    }

    #[test]
    fn parses_permission_status_with_source_truthiness() {
        assert_eq!(
            parse_permissions_status("bad"),
            PermissionProbeResult::Unknown
        );
        assert_eq!(
            parse_permissions_status(r#"{"accessibility":"yes","screen_recording":true}"#),
            PermissionProbeResult::Unknown
        );
        assert_eq!(
            parse_permissions_status(r#"{"accessibility":false,"screen_recording":true}"#),
            PermissionProbeResult::Accessibility
        );
        assert_eq!(
            parse_permissions_status(r#"{"accessibility":true}"#),
            PermissionProbeResult::ScreenRecording
        );
        assert_eq!(
            parse_permissions_status(r#"{"accessibility":true,"screen_recording":{}}"#),
            PermissionProbeResult::Ok
        );
        assert_eq!(
            parse_permissions_status(r#"{"accessibility":true,"screen_recording":false}"#),
            PermissionProbeResult::ScreenRecording
        );
    }

    #[tokio::test]
    async fn warm_client_short_circuits_before_every_host_effect() {
        let (events, host, mut client, options) = setup("darwin");
        client.started = true;
        run_bootstrap(&client, &host, &options).await.unwrap();
        assert!(events.events().is_empty());
    }

    #[tokio::test]
    async fn approval_prompt_save_install_and_non_macos_start_keep_order() {
        let (events, host, client, options) = setup("linux");
        run_bootstrap(&client, &host, &options).await.unwrap();
        let log = events.events();
        assert_eq!(
            &log[..5],
            [
                "approval:read",
                "output:Computer Use needs a one-time driver download (first use).",
                "approval:prompt",
                "approval:save:2026-09-25T12:00:00.000Z",
                "install",
            ]
        );
        assert!(log.contains(&"client:start".to_owned()));
        assert!(!log.iter().any(|event| event.starts_with("daemon:")));
    }

    #[tokio::test]
    async fn declined_approval_stops_before_saving_or_installing() {
        let (events, host, client, options) = setup("darwin");
        events.prompt_result.store(false, Ordering::SeqCst);
        let error = run_bootstrap(&client, &host, &options).await.unwrap_err();
        assert_eq!(
            error,
            "Computer Use install declined by user. Re-invoke the tool to be prompted again."
        );
        let log = events.events();
        assert!(log.contains(&"approval:prompt".to_owned()));
        assert!(!log.iter().any(|event| event.starts_with("approval:save")));
        assert_eq!(events.install_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn auto_approval_saves_without_prompting() {
        let (events, host, client, mut options) = setup("linux");
        options.auto_approve_install = true;
        run_bootstrap(&client, &host, &options).await.unwrap();
        let log = events.events();
        assert!(
            log.iter()
                .any(|entry| entry == "approval:save:2026-09-25T12:00:00.000Z")
        );
        assert!(
            log.iter()
                .any(|entry| entry == "output:Computer Use install auto-approved (approval mode).")
        );
        assert!(!log.contains(&"approval:prompt".to_owned()));
    }

    #[tokio::test]
    async fn macos_requests_permissions_one_at_a_time_and_cleans_up() {
        let (events, host, client, options) = setup("darwin");
        events.approved.store(true, Ordering::SeqCst);
        events.probes.lock().unwrap().extend([
            PermissionProbeResult::Accessibility,
            PermissionProbeResult::Accessibility,
            PermissionProbeResult::ScreenRecording,
            PermissionProbeResult::Ok,
        ]);
        run_bootstrap(&client, &host, &options).await.unwrap();
        let log = events.events();
        let accessibility = log
            .iter()
            .position(|entry| entry == "settings:accessibility")
            .unwrap();
        let screen = log
            .iter()
            .position(|entry| entry == "settings:screen-recording")
            .unwrap();
        assert!(accessibility < screen);
        assert_eq!(events.daemon_starts.load(Ordering::SeqCst), 1);
        assert_eq!(events.daemon_kills.load(Ordering::SeqCst), 1);
        assert!(
            log.iter()
                .any(|entry| entry.contains("Waiting for Accessibility"))
        );
        assert!(log.iter().any(|entry| entry.starts_with("output:Step 2/2")));
        assert!(log.contains(&"client:start".to_owned()));
    }

    #[tokio::test]
    async fn unknown_status_relaunches_daemon_and_releases_final_daemon() {
        let (events, host, client, options) = setup("darwin");
        events.approved.store(true, Ordering::SeqCst);
        events
            .probes
            .lock()
            .unwrap()
            .extend([PermissionProbeResult::Unknown, PermissionProbeResult::Ok]);
        run_bootstrap(&client, &host, &options).await.unwrap();
        assert_eq!(events.daemon_starts.load(Ordering::SeqCst), 2);
        assert_eq!(events.daemon_kills.load(Ordering::SeqCst), 2);
        let log = events.events();
        let first_kill = log.iter().position(|entry| entry == "daemon:kill").unwrap();
        let second_start = log
            .iter()
            .enumerate()
            .filter(|(_, entry)| *entry == "daemon:start")
            .nth(1)
            .unwrap()
            .0;
        assert!(first_kill < second_start);
        assert!(
            log.iter()
                .any(|entry| entry.contains("Bringing up Computer Use permissions check"))
        );
    }

    #[tokio::test]
    async fn cancellation_and_timeout_both_kill_the_status_daemon() {
        let (events, host, client, options) = setup("darwin");
        events.approved.store(true, Ordering::SeqCst);
        events.cancel_on_sleep.store(true, Ordering::SeqCst);
        let error = run_bootstrap(&client, &host, &options).await.unwrap_err();
        assert_eq!(error, "Computer Use bootstrap aborted.");
        assert_eq!(
            events.daemon_starts.load(Ordering::SeqCst),
            events.daemon_kills.load(Ordering::SeqCst)
        );

        let (events, host, client, mut options) = setup("darwin");
        events.approved.store(true, Ordering::SeqCst);
        options.poll_interval = Duration::from_millis(1);
        options.poll_timeout = Duration::ZERO;
        let error = run_bootstrap(&client, &host, &options).await.unwrap_err();
        assert_eq!(
            error,
            "Computer Use permission grant timed out after 0s. Re-invoke the tool to retry."
        );
        assert_eq!(
            events.daemon_starts.load(Ordering::SeqCst),
            events.daemon_kills.load(Ordering::SeqCst)
        );
        assert_eq!(events.install_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn install_and_client_errors_propagate_without_losing_cleanup() {
        let (events, host, client, options) = setup("linux");
        events.fail_install.store(true, Ordering::SeqCst);
        assert_eq!(
            run_bootstrap(&client, &host, &options).await.unwrap_err(),
            "install failed"
        );
        assert!(!events.events().contains(&"client:start".to_owned()));

        let (events, host, client, options) = setup("darwin");
        events.approved.store(true, Ordering::SeqCst);
        events
            .probes
            .lock()
            .unwrap()
            .push_back(PermissionProbeResult::Ok);
        events.fail_start.store(true, Ordering::SeqCst);
        assert_eq!(
            run_bootstrap(&client, &host, &options).await.unwrap_err(),
            "client start failed"
        );
        assert_eq!(events.daemon_kills.load(Ordering::SeqCst), 1);
    }
}
