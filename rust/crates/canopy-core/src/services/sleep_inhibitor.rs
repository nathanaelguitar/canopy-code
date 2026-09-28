//! Best-effort operating-system sleep inhibition while the agent is working.
//!
//! This follows `packages/core/src/services/sleepInhibitor.ts`. Callers hold a
//! reference-counted handle; the shared child is stopped when the last handle
//! is dropped. A missing platform command never fails an agent turn.

use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};

const MAX_REASON_UTF16_UNITS: usize = 120;
const ENV_ALLOW_LIST: [&str; 7] = [
    "PATH",
    "DBUS_SESSION_BUS_ADDRESS",
    "XDG_RUNTIME_DIR",
    "SYSTEMROOT",
    "WINDIR",
    "TEMP",
    "TMP",
];

#[derive(Default)]
struct State {
    active_count: usize,
    child: Option<Child>,
    spawn_failed_for_current_run: bool,
}

static STATE: OnceLock<Mutex<State>> = OnceLock::new();
static SYSTEMD_NO_ASK_PASSWORD: OnceLock<bool> = OnceLock::new();

/// A reference to the shared inhibitor process. Dropping the final handle
/// stops the process, including on cancellation and error returns.
pub struct SleepInhibitorHandle {
    active: bool,
}

impl Drop for SleepInhibitorHandle {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = state_lock();
        state.active_count = state.active_count.saturating_sub(1);
        if state.active_count == 0 {
            stop_child(&mut state);
            state.spawn_failed_for_current_run = false;
        }
    }
}

/// Acquire a best-effort sleep inhibitor for the current operation.
pub fn acquire(reason: &str) -> SleepInhibitorHandle {
    let mut state = state_lock();
    refresh_child(&mut state);
    state.active_count = state.active_count.saturating_add(1);
    if state.active_count == 1 {
        state.spawn_failed_for_current_run = false;
    }
    if state.child.is_none() && !state.spawn_failed_for_current_run {
        match spawn_inhibitor(reason) {
            Some(Ok(child)) => state.child = Some(child),
            Some(Err(_)) | None => state.spawn_failed_for_current_run = true,
        }
    }
    SleepInhibitorHandle { active: true }
}

fn state_lock() -> MutexGuard<'static, State> {
    STATE
        .get_or_init(|| Mutex::new(State::default()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn refresh_child(state: &mut State) {
    let Some(child) = state.child.as_mut() else {
        return;
    };
    match child.try_wait() {
        Ok(Some(_)) => state.child = None,
        Ok(None) => {}
        Err(_) => {
            state.child = None;
            state.spawn_failed_for_current_run = true;
        }
    }
}

fn stop_child(state: &mut State) {
    let Some(mut child) = state.child.take() else {
        return;
    };
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn spawn_inhibitor(reason: &str) -> Option<std::io::Result<Child>> {
    let os = std::env::consts::OS;
    let (program, args) = match os {
        "macos" => ("caffeinate", vec!["-is".to_owned()]),
        "linux" if is_headless_ssh_session() => return None,
        "linux" => {
            let mut args = Vec::new();
            if *SYSTEMD_NO_ASK_PASSWORD.get_or_init(systemd_supports_no_ask_password) {
                args.push("--no-ask-password".to_owned());
            }
            args.extend([
                "--what=sleep".to_owned(),
                "--who=Canopy Code".to_owned(),
                format!("--why={}", sanitize_reason(reason)),
                "--mode=block".to_owned(),
                "sleep".to_owned(),
                "infinity".to_owned(),
            ]);
            ("systemd-inhibit", args)
        }
        "windows" => (
            "powershell.exe",
            vec![
                "-NoProfile".to_owned(),
                "-NonInteractive".to_owned(),
                "-ExecutionPolicy".to_owned(),
                "Bypass".to_owned(),
                "-Command".to_owned(),
                WINDOWS_INHIBIT_SCRIPT.to_owned(),
            ],
        ),
        _ => return None,
    };

    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env_clear();
    for key in ENV_ALLOW_LIST {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    Some(command.spawn())
}

fn is_headless_ssh_session() -> bool {
    let has_ssh = ["SSH_CONNECTION", "SSH_TTY", "SSH_CLIENT"]
        .iter()
        .any(|key| std::env::var_os(key).is_some_and(|value| !value.is_empty()));
    let has_display = ["DISPLAY", "WAYLAND_DISPLAY", "MIR_SOCKET"]
        .iter()
        .any(|key| std::env::var_os(key).is_some_and(|value| !value.is_empty()));
    has_ssh && !has_display
}

fn sanitize_reason(reason: &str) -> String {
    let cleaned = reason
        .chars()
        .map(|ch| {
            if (ch as u32) <= 0x1f || ch == '\u{7f}' {
                ' '
            } else {
                ch
            }
        })
        .collect::<String>();
    let utf16_prefix = cleaned
        .encode_utf16()
        .take(MAX_REASON_UTF16_UNITS)
        .collect::<Vec<_>>();
    String::from_utf16_lossy(&utf16_prefix).trim().to_owned()
}

fn systemd_supports_no_ask_password() -> bool {
    Command::new("systemd-inhibit")
        .arg("--help")
        .stdin(Stdio::null())
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .ok()
        .is_some_and(|output| {
            String::from_utf8_lossy(&output.stdout).contains("--no-ask-password")
                || String::from_utf8_lossy(&output.stderr).contains("--no-ask-password")
        })
}

const WINDOWS_INHIBIT_SCRIPT: &str = r#"
Add-Type -Namespace CanopyCode -Name SleepUtil -MemberDefinition '[DllImport("kernel32.dll")] public static extern uint SetThreadExecutionState(uint esFlags);';
[CanopyCode.SleepUtil]::SetThreadExecutionState(0x80000001) | Out-Null;
try {
  while ($true) { Start-Sleep -Seconds 3600 }
} finally {
  [CanopyCode.SleepUtil]::SetThreadExecutionState(0x80000000) | Out-Null;
}
"#;
