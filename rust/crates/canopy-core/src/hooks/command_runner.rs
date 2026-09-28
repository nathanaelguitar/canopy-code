//! Execution of command-based hooks.
//!
//! Port of the command path in `packages/core/src/hooks/hookRunner.ts`. This
//! module deliberately owns only one command invocation; hook planning,
//! asynchronous-hook registration, and HTTP/function/prompt hooks stay with
//! their respective host modules.

use std::collections::BTreeMap;
use std::future::pending;
use std::io;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::planner::HookEventName;
use crate::utils::cancellation::CancellationToken;
use crate::utils::sanitize_child_env::sanitize_child_env;

/// Hook command timeout in milliseconds when no override is configured.
pub const DEFAULT_HOOK_TIMEOUT_MS: f64 = 60_000.0;
/// Maximum number of captured bytes for each output stream.
pub const MAX_COMMAND_OUTPUT_BYTES: usize = 1024 * 1024;
const TERMINATION_GRACE_PERIOD: Duration = Duration::from_secs(2);

/// Shell used to interpret a hook command string.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HookShell {
    Bash,
    Powershell,
}

/// Shell executable and argument prefix, corresponding to `shell-utils.ts`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShellConfiguration {
    pub shell: ShellType,
    pub executable: String,
    pub args_prefix: Vec<String>,
}

/// Shell kind needed for command expansion and shell selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellType {
    Bash,
    Powershell,
    Cmd,
}

/// The command-hook fields used by this runner. Unknown config fields are
/// retained when the config is parsed from JSON and returned in the result.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandHookConfig {
    #[serde(rename = "type", default = "command_type")]
    pub hook_type: String,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub timeout: Option<f64>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default, rename = "async")]
    pub is_async: bool,
    #[serde(default)]
    pub shell: Option<HookShell>,
    #[serde(default)]
    pub status_message: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
    #[serde(skip)]
    original_config: Option<Value>,
}

fn command_type() -> String {
    "command".to_owned()
}

impl CommandHookConfig {
    /// Parse a command-hook config while retaining its exact source JSON.
    pub fn from_value(value: Value) -> Result<Self, serde_json::Error> {
        let mut config: Self = serde_json::from_value(value.clone())?;
        config.original_config = Some(value);
        Ok(config)
    }

    fn source_value(&self) -> Value {
        self.original_config.clone().unwrap_or_else(|| {
            serde_json::to_value(self).unwrap_or_else(|_| json!({ "type": "command" }))
        })
    }
}

impl Default for CommandHookConfig {
    fn default() -> Self {
        Self {
            hook_type: command_type(),
            command: None,
            name: None,
            description: None,
            timeout: None,
            env: BTreeMap::new(),
            is_async: false,
            shell: None,
            status_message: None,
            extra: BTreeMap::new(),
            original_config: None,
        }
    }
}

/// Result of one synchronous command-hook invocation.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandHookExecutionResult {
    pub hook_config: Value,
    pub event_name: HookEventName,
    pub success: bool,
    pub output: Option<Value>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Injectable process context for command hooks.
///
/// `process_environment` defaults to the current process environment.
/// `shell_context_environment` represents the source runtime's
/// AsyncLocalStorage-backed `getShellContextEnvVars()` values; callers supply
/// those values explicitly because Rust has no equivalent ambient JS context.
#[derive(Clone, Debug, Default)]
pub struct CommandHookRunner {
    pub process_environment: Option<BTreeMap<String, String>>,
    pub shell_context_environment: BTreeMap<String, String>,
    pub global_shell_configuration: Option<ShellConfiguration>,
}

impl CommandHookRunner {
    /// Execute one command hook. `input` is serialized to the child process's
    /// stdin and must normally contain the hook input's `cwd` property.
    pub async fn execute(
        &self,
        hook_config: &CommandHookConfig,
        event_name: HookEventName,
        input: &Value,
        cancellation: Option<&CancellationToken>,
    ) -> CommandHookExecutionResult {
        let started = Instant::now();
        let config_value = hook_config.source_value();

        if cancellation.is_some_and(CancellationToken::is_cancelled) {
            return result(
                config_value,
                event_name,
                false,
                None,
                None,
                None,
                None,
                started,
                Some("Hook execution cancelled (aborted)".to_owned()),
            );
        }

        let Some(raw_command) = hook_config.command.as_deref().filter(|s| !s.is_empty()) else {
            return result(
                config_value,
                event_name,
                false,
                None,
                None,
                None,
                None,
                started,
                Some("Command hook missing command".to_owned()),
            );
        };

        let cwd = input.get("cwd").and_then(Value::as_str).unwrap_or_default();
        let mut process_environment = self
            .process_environment
            .clone()
            .unwrap_or_else(|| std::env::vars().collect());
        process_environment = sanitize_child_env(&process_environment);

        let mut environment = process_environment;
        environment.insert("GEMINI_PROJECT_DIR".to_owned(), cwd.to_owned());
        environment.insert("CLAUDE_PROJECT_DIR".to_owned(), cwd.to_owned());
        environment.insert("CANOPY_PROJECT_DIR".to_owned(), cwd.to_owned());
        environment.extend(self.shell_context_environment.clone());
        environment.extend(hook_config.env.clone());

        let global_shell = self
            .global_shell_configuration
            .clone()
            .unwrap_or_else(|| default_shell_configuration(&environment));
        let shell = shell_configuration_for_hook(hook_config.shell, global_shell);
        let command = expand_command(raw_command, cwd, shell.shell);
        let input_bytes = serde_json::to_vec(input).unwrap_or_default();

        let mut child_command = Command::new(&shell.executable);
        child_command
            .args(&shell.args_prefix)
            .arg(command)
            .env_clear()
            .envs(&environment)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = input.get("cwd").and_then(Value::as_str) {
            child_command.current_dir(cwd);
        }

        let mut child = match child_command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return result(
                    config_value,
                    event_name,
                    false,
                    None,
                    Some(String::new()),
                    Some(String::new()),
                    None,
                    started,
                    Some(error.to_string()),
                );
            }
        };

        let stdout_capture = Arc::new(Mutex::new(Vec::new()));
        let stderr_capture = Arc::new(Mutex::new(Vec::new()));
        let stdout_task = child
            .stdout
            .take()
            .map(|pipe| spawn_capped_reader(pipe, Arc::clone(&stdout_capture)));
        let stderr_task = child
            .stderr
            .take()
            .map(|pipe| spawn_capped_reader(pipe, Arc::clone(&stderr_capture)));
        let stdin_task = child.stdin.take().map(|mut pipe| {
            tokio::spawn(async move {
                // EPIPE is expected when a hook exits before consuming all input.
                let _ = pipe.write_all(&input_bytes).await;
                let _ = pipe.shutdown().await;
            })
        });

        let timeout_ms = hook_config.timeout.unwrap_or(DEFAULT_HOOK_TIMEOUT_MS);
        let timeout_duration = Duration::from_millis(normalize_timeout_ms(timeout_ms));
        let cancellation = cancellation.cloned();
        let wait_result = tokio::select! {
            biased;
            _ = wait_for_cancellation(cancellation.clone()) => WaitResult::Aborted,
            _ = tokio::time::sleep(timeout_duration) => WaitResult::TimedOut,
            status = child.wait() => WaitResult::Exited(status),
        };

        let (status, wait_error, timeout_or_abort) = match wait_result {
            WaitResult::Exited(Ok(status)) => (Some(status), None, None),
            WaitResult::Exited(Err(error)) => (None, Some(error.to_string()), None),
            WaitResult::Aborted => (terminate_child(&mut child).await, None, Some("aborted")),
            WaitResult::TimedOut => (terminate_child(&mut child).await, None, Some("timed_out")),
        };

        if let Some(task) = stdin_task {
            let _ = task.await;
        }
        if let Some(task) = stdout_task {
            let _ = task.await;
        }
        if let Some(task) = stderr_task {
            let _ = task.await;
        }

        let stdout = captured_text(&stdout_capture);
        let stderr = captured_text(&stderr_capture);
        if let Some(reason) = timeout_or_abort {
            let error = if reason == "aborted" {
                "Hook execution cancelled (aborted)".to_owned()
            } else {
                format!("Hook timed out after {}ms", js_number_string(timeout_ms))
            };
            return result(
                config_value,
                event_name,
                false,
                None,
                Some(stdout),
                Some(stderr),
                None,
                started,
                Some(error),
            );
        }

        if let Some(error) = wait_error {
            return result(
                config_value,
                event_name,
                false,
                None,
                Some(stdout),
                Some(stderr),
                None,
                started,
                Some(error),
            );
        }

        let Some(status) = status else {
            return result(
                config_value,
                event_name,
                false,
                None,
                Some(stdout),
                Some(stderr),
                Some(-1),
                started,
                Some("Hook killed by signal".to_owned()),
            );
        };

        let exit_code = status.code();
        let blocking_error = exit_code == Some(2);
        let text_to_parse = if blocking_error {
            stderr.trim()
        } else if !stdout.trim().is_empty() {
            stdout.trim()
        } else {
            stderr.trim()
        };
        let output = parse_hook_output(text_to_parse, exit_code);
        result(
            config_value,
            event_name,
            exit_code == Some(0),
            output,
            Some(stdout),
            Some(stderr),
            Some(exit_code.unwrap_or(-1)),
            started,
            exit_code
                .is_none()
                .then(|| "Hook killed by signal".to_owned()),
        )
    }
}

enum WaitResult {
    Exited(io::Result<ExitStatus>),
    TimedOut,
    Aborted,
}

async fn wait_for_cancellation(token: Option<CancellationToken>) {
    if let Some(token) = token {
        token.cancelled().await;
    } else {
        pending::<()>().await;
    }
}

fn spawn_capped_reader<R>(reader: R, output: Arc<Mutex<Vec<u8>>>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut reader = reader;
        let mut scratch = [0_u8; 8192];
        loop {
            let count = match reader.read(&mut scratch).await {
                Ok(0) | Err(_) => break,
                Ok(count) => count,
            };
            let mut captured = output.lock().unwrap_or_else(|error| error.into_inner());
            let remaining = MAX_COMMAND_OUTPUT_BYTES.saturating_sub(captured.len());
            captured.extend_from_slice(&scratch[..count.min(remaining)]);
        }
    })
}

fn captured_text(output: &Arc<Mutex<Vec<u8>>>) -> String {
    let bytes = output.lock().unwrap_or_else(|error| error.into_inner());
    String::from_utf8_lossy(&bytes).into_owned()
}

async fn terminate_child(child: &mut Child) -> Option<ExitStatus> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;

        if let Some(pid) = child.id() {
            let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
        return child.wait().await.ok();
    }

    match timeout(TERMINATION_GRACE_PERIOD, child.wait()).await {
        Ok(Ok(status)) => Some(status),
        Ok(Err(_)) => None,
        Err(_) => {
            let _ = child.start_kill();
            child.wait().await.ok()
        }
    }
}

fn result(
    hook_config: Value,
    event_name: HookEventName,
    success: bool,
    output: Option<Value>,
    stdout: Option<String>,
    stderr: Option<String>,
    exit_code: Option<i32>,
    started: Instant,
    error: Option<String>,
) -> CommandHookExecutionResult {
    CommandHookExecutionResult {
        hook_config,
        event_name,
        success,
        output,
        stdout,
        stderr,
        exit_code,
        duration_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        error,
    }
}

fn parse_hook_output(text: &str, exit_code: Option<i32>) -> Option<Value> {
    if text.is_empty() {
        return None;
    }

    let parsed = serde_json::from_str::<Value>(text).and_then(|value| match value {
        Value::String(inner) => serde_json::from_str::<Value>(&inner),
        other => Ok(other),
    });
    match parsed {
        Ok(value @ (Value::Object(_) | Value::Array(_))) => Some(value),
        Ok(_) => None,
        Err(_) => Some(plain_text_hook_output(text, exit_code)),
    }
}

fn plain_text_hook_output(text: &str, exit_code: Option<i32>) -> Value {
    match exit_code {
        Some(0) => json!({
            "decision": "allow",
            "reason": "Hook executed successfully",
            "systemMessage": text,
        }),
        Some(2) => json!({ "decision": "deny", "reason": text }),
        _ => json!({
            "decision": "allow",
            "reason": format!("Non-blocking error: {text}"),
            "systemMessage": format!("Warning: {text}"),
        }),
    }
}

fn expand_command(command: &str, cwd: &str, shell: ShellType) -> String {
    let escaped_cwd = escape_shell_argument(cwd, shell);
    command
        .replace("$GEMINI_PROJECT_DIR", &escaped_cwd)
        .replace("$CLAUDE_PROJECT_DIR", &escaped_cwd)
}

/// Escape one project-directory value for insertion into a shell command.
pub fn escape_shell_argument(value: &str, shell: ShellType) -> String {
    if value.is_empty() {
        return String::new();
    }
    match shell {
        ShellType::Bash => {
            if value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || "_@%+=:,./-".contains(ch))
            {
                value.to_owned()
            } else {
                format!("'{}'", value.replace('\'', "'\"'\"'"))
            }
        }
        ShellType::Powershell => format!("'{}'", value.replace('\'', "''")),
        ShellType::Cmd => format!("\"{}\"", value.replace('"', "\"\"")),
    }
}

fn shell_configuration_for_hook(
    override_shell: Option<HookShell>,
    global: ShellConfiguration,
) -> ShellConfiguration {
    match override_shell {
        Some(HookShell::Powershell) => ShellConfiguration {
            shell: ShellType::Powershell,
            executable: "powershell".to_owned(),
            args_prefix: vec!["-Command".to_owned()],
        },
        Some(HookShell::Bash) => ShellConfiguration {
            shell: ShellType::Bash,
            executable: if global.shell == ShellType::Bash {
                global.executable
            } else {
                "bash".to_owned()
            },
            args_prefix: vec!["-c".to_owned()],
        },
        None => global,
    }
}

fn default_shell_configuration(environment: &BTreeMap<String, String>) -> ShellConfiguration {
    if cfg!(windows) {
        let msystem = environment
            .get("MSYSTEM")
            .map(String::as_str)
            .unwrap_or_default();
        let term = environment
            .get("TERM")
            .map(String::as_str)
            .unwrap_or_default();
        if msystem.starts_with("MINGW")
            || msystem.starts_with("MSYS")
            || term.contains("msys")
            || term.contains("cygwin")
        {
            return ShellConfiguration {
                shell: ShellType::Bash,
                executable: find_bash_in_path(environment).unwrap_or_else(|| "bash".to_owned()),
                args_prefix: vec!["-c".to_owned()],
            };
        }

        let com_spec = environment
            .get("ComSpec")
            .cloned()
            .unwrap_or_else(|| "cmd.exe".to_owned());
        if com_spec.to_ascii_lowercase().ends_with("powershell.exe")
            || com_spec.to_ascii_lowercase().ends_with("pwsh.exe")
        {
            ShellConfiguration {
                shell: ShellType::Powershell,
                executable: com_spec,
                args_prefix: vec!["-NoProfile".to_owned(), "-Command".to_owned()],
            }
        } else {
            ShellConfiguration {
                shell: ShellType::Cmd,
                executable: com_spec,
                args_prefix: vec!["/d".to_owned(), "/s".to_owned(), "/c".to_owned()],
            }
        }
    } else {
        ShellConfiguration {
            shell: ShellType::Bash,
            executable: "bash".to_owned(),
            args_prefix: vec!["-c".to_owned()],
        }
    }
}

#[cfg(windows)]
fn find_bash_in_path(environment: &BTreeMap<String, String>) -> Option<String> {
    use std::path::Path;

    environment
        .get("PATH")
        .into_iter()
        .flat_map(|paths| std::env::split_paths(paths))
        .map(|path| path.join("bash.exe"))
        .find(|path| Path::new(path).is_file())
        .map(|path| path.to_string_lossy().into_owned())
}

#[cfg(not(windows))]
fn find_bash_in_path(_environment: &BTreeMap<String, String>) -> Option<String> {
    None
}

fn normalize_timeout_ms(timeout_ms: f64) -> u64 {
    if !timeout_ms.is_finite() || timeout_ms < 1.0 || timeout_ms > 2_147_483_647.0 {
        1
    } else {
        timeout_ms.trunc() as u64
    }
}

fn js_number_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else if value == 0.0 {
        "0".to_owned()
    } else {
        value.to_string()
    }
}
