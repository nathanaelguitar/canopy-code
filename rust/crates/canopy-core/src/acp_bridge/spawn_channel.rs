//! Child-process transport used by daemon ACP sessions.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use super::channel::{AcpChannelExitInfo, ChannelFailure};
use super::log_redaction::redact_log_credentials;
use super::process_registry::{ProcessRegistry, TrackedChildProcess};

pub const DAEMON_ACP_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub const DAEMON_ACP_MAX_QUEUED_MESSAGES: usize = 256;
pub const DAEMON_ACP_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;
const MAX_CHILD_STDERR_LINE_BYTES: usize = 256 * 1024;
const MAX_RETAINED_CHILD_STDERR_LINE_CAPACITY: usize = 16 * 1024;
pub const CHILD_TERM_GRACE_MS: u64 = 5_000;
pub const CHILD_EXIT_DEADLINE_MS: u64 = 10_000;
pub const SCRUBBED_CHILD_ENV_KEYS: [&str; 3] = [
    "QWEN_SERVER_TOKEN",
    "QWEN_CODE_SIMPLE",
    "QWEN_CODE_EXTERNAL_TOOL_GUARD_TOKEN",
];

#[derive(Clone, Debug)]
pub struct SpawnChannelOptions {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub workspace_cwd: PathBuf,
    pub source_env: HashMap<String, String>,
    pub env_overrides: HashMap<String, Option<String>>,
    pub extra_args: Vec<String>,
    pub stderr_prefix: Option<String>,
}

impl SpawnChannelOptions {
    pub fn qwen_cli(cli_entry: impl Into<String>, workspace_cwd: impl Into<PathBuf>) -> Self {
        let args = vec![cli_entry.into(), "--acp".into()];
        Self {
            program: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("qwen")),
            args,
            workspace_cwd: workspace_cwd.into(),
            source_env: std::env::vars().collect(),
            env_overrides: HashMap::new(),
            extra_args: Vec::new(),
            stderr_prefix: None,
        }
    }
}

pub struct SpawnedAcpChannel {
    pub incoming: mpsc::Receiver<Result<Bytes, ChannelFailure>>,
    pub outgoing: BoundedByteSender,
    pub transport_failed: watch::Receiver<Option<ChannelFailure>>,
    pub exited: watch::Receiver<Option<AcpChannelExitInfo>>,
    pub process: TrackedChildProcess,
}

struct OutgoingFrame {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub struct BoundedByteSender {
    sender: mpsc::Sender<OutgoingFrame>,
    permits: std::sync::Arc<Semaphore>,
    max_bytes: usize,
}
pub struct BoundedByteReceiver {
    receiver: mpsc::Receiver<OutgoingFrame>,
}
pub struct BoundedByteFrame {
    pub bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}
impl BoundedByteSender {
    pub async fn send(&self, bytes: Bytes) -> Result<(), String> {
        let size = bytes.len().max(1);
        if size > self.max_bytes {
            return Err(format!(
                "ACP outbound queue frame exceeds {} bytes",
                self.max_bytes
            ));
        }
        let permit = self
            .permits
            .clone()
            .acquire_many_owned(size as u32)
            .await
            .map_err(|_| "ACP outbound queue is closed".to_owned())?;
        self.sender
            .send(OutgoingFrame {
                bytes,
                _permit: permit,
            })
            .await
            .map_err(|_| "ACP outbound queue is closed".to_owned())
    }
}
impl BoundedByteReceiver {
    pub async fn recv(&mut self) -> Option<BoundedByteFrame> {
        self.receiver.recv().await.map(|frame| BoundedByteFrame {
            bytes: frame.bytes,
            _permit: frame._permit,
        })
    }
}

/// Pure child-environment construction. Per-child overrides cannot restore
/// daemon credentials that are on the source denylist.
pub fn scrub_child_env(
    source: &HashMap<String, String>,
    overrides: &HashMap<String, Option<String>>,
) -> HashMap<String, String> {
    let scrubbed: std::collections::HashSet<&str> = SCRUBBED_CHILD_ENV_KEYS.into_iter().collect();
    let mut child: HashMap<String, String> = source
        .iter()
        .filter(|(key, _)| !scrubbed.contains(key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    for (key, value) in overrides {
        if scrubbed.contains(key.as_str()) {
            continue;
        }
        match value {
            Some(value) => {
                child.insert(key.clone(), value.clone());
            }
            None => {
                child.remove(key);
            }
        }
    }
    child.insert("QWEN_CODE_NO_RELAUNCH".into(), "true".into());
    child.insert("QWEN_CODE_SERVE".into(), "1".into());
    child
}

pub async fn spawn_acp_channel(
    options: SpawnChannelOptions,
    registry: &ProcessRegistry,
) -> io::Result<SpawnedAcpChannel> {
    let reservation = registry.reserve().map_err(io::Error::other)?;
    let env = scrub_child_env(&options.source_env, &options.env_overrides);
    let mut command = Command::new(&options.program);
    command
        .args(&options.args)
        .args(&options.extra_args)
        .current_dir(&options.workspace_cwd)
        .env_clear()
        .envs(env)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            drop(reservation);
            return Err(error);
        }
    };
    let pid = child
        .id()
        .ok_or_else(|| io::Error::other("spawned ACP child has no process id"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("spawned ACP child has no stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("spawned ACP child has no stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("spawned ACP child has no stderr"))?;
    let (exit_tx, exit_rx) = watch::channel(None);
    let (failed_tx, failed_rx) = watch::channel(None);
    let termination_exit = exit_rx.clone();
    let process = reservation
        .attach(TrackedChildProcess::new(
            exit_rx.clone(),
            move || terminate_child(pid, termination_exit.clone()),
            move || kill_child_sync(pid),
        ))
        .map_err(io::Error::other)?;

    tokio::spawn(async move {
        let status = child.wait().await;
        let info = status.ok().map(exit_info);
        let _ = exit_tx.send(info);
    });

    let (incoming_tx, incoming) = mpsc::channel(DAEMON_ACP_MAX_QUEUED_MESSAGES);
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<OutgoingFrame>(DAEMON_ACP_MAX_QUEUED_MESSAGES);
    let outgoing = BoundedByteSender {
        sender: outgoing_tx,
        permits: std::sync::Arc::new(Semaphore::new(DAEMON_ACP_MAX_QUEUED_BYTES)),
        max_bytes: DAEMON_ACP_MAX_QUEUED_BYTES,
    };
    let read_failure = failed_tx.clone();
    let read_incoming = incoming_tx.clone();
    let read_process = process.clone();
    tokio::spawn(async move {
        let mut stdout = stdout;
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            match stdout.read(&mut buffer).await {
                Ok(0) => break,
                Ok(size) => {
                    if read_incoming
                        .send(Ok(Bytes::copy_from_slice(&buffer[..size])))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    let failure = ChannelFailure::Transport(error.to_string());
                    let _ = read_failure.send(Some(failure.clone()));
                    let _ = read_incoming.send(Err(failure)).await;
                    let _ = read_process.terminate().await;
                    break;
                }
            }
        }
    });
    let write_failure = failed_tx.clone();
    let write_incoming = incoming_tx.clone();
    let write_process = process.clone();
    tokio::spawn(async move {
        let mut stdin = stdin;
        let mut outgoing_rx = BoundedByteReceiver {
            receiver: outgoing_rx,
        };
        while let Some(frame) = outgoing_rx.recv().await {
            if let Err(error) = stdin.write_all(&frame.bytes).await {
                let failure = ChannelFailure::Transport(error.to_string());
                let _ = write_failure.send(Some(failure.clone()));
                let _ = write_incoming.send(Err(failure)).await;
                let _ = write_process.terminate().await;
                break;
            }
        }
        let _ = stdin.shutdown().await;
    });
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr);
        let mut line = Vec::new();
        loop {
            if line.capacity() > MAX_RETAINED_CHILD_STDERR_LINE_CAPACITY {
                line = Vec::new();
            } else {
                line.clear();
            }
            match read_bounded_child_stderr_line(&mut reader, &mut line).await {
                Ok(None) => break,
                Ok(Some(BoundedChildStderrLine::Complete)) => {
                    let prefix = options.stderr_prefix.as_deref().unwrap_or("[acp] ");
                    let text = String::from_utf8_lossy(&line);
                    eprint!("{prefix}{}", redact_log_credentials(&text));
                }
                Ok(Some(BoundedChildStderrLine::TooLarge)) => {
                    let prefix = options.stderr_prefix.as_deref().unwrap_or("[acp] ");
                    eprintln!(
                        "{prefix}[child stderr line exceeded {MAX_CHILD_STDERR_LINE_BYTES} bytes; content omitted]"
                    );
                }
                Err(_) => break,
            }
        }
    });
    Ok(SpawnedAcpChannel {
        incoming,
        outgoing,
        transport_failed: failed_rx,
        exited: exit_rx,
        process,
    })
}

enum BoundedChildStderrLine {
    Complete,
    TooLarge,
}

/// Read a child stderr line with a fixed memory ceiling. Oversized lines are
/// drained but not logged, which also avoids printing a truncated credential
/// fragment that could evade whole-line redaction.
async fn read_bounded_child_stderr_line<R>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<Option<BoundedChildStderrLine>>
where
    R: AsyncBufRead + Unpin,
{
    let mut too_large = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if too_large {
                return Ok(Some(BoundedChildStderrLine::TooLarge));
            }
            return if line.is_empty() {
                Ok(None)
            } else {
                Ok(Some(BoundedChildStderrLine::Complete))
            };
        }

        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        if !too_large {
            if line.len().saturating_add(consumed) > MAX_CHILD_STDERR_LINE_BYTES {
                line.clear();
                too_large = true;
            } else {
                line.extend_from_slice(&available[..consumed]);
            }
        }
        reader.consume(consumed);

        if newline.is_some() {
            return Ok(Some(if too_large {
                BoundedChildStderrLine::TooLarge
            } else {
                BoundedChildStderrLine::Complete
            }));
        }
    }
}

fn exit_info(status: std::process::ExitStatus) -> AcpChannelExitInfo {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        AcpChannelExitInfo {
            exit_code: status.code(),
            signal_code: status.signal().map(|signal| signal.to_string()),
        }
    }
    #[cfg(not(unix))]
    {
        AcpChannelExitInfo {
            exit_code: status.code(),
            signal_code: None,
        }
    }
}

fn terminate_child(
    pid: u32,
    mut exited: watch::Receiver<Option<AcpChannelExitInfo>>,
) -> super::channel::ChannelFuture<'static, Result<(), String>> {
    Box::pin(async move {
        if exited.borrow().is_some() {
            return Ok(());
        }
        signal_child(pid, false).await?;
        if tokio::time::timeout(
            Duration::from_millis(CHILD_TERM_GRACE_MS),
            wait_for_exit(&mut exited),
        )
        .await
        .is_err()
        {
            kill_child_sync(pid);
            if tokio::time::timeout(
                Duration::from_millis(CHILD_EXIT_DEADLINE_MS - CHILD_TERM_GRACE_MS),
                wait_for_exit(&mut exited),
            )
            .await
            .is_err()
            {
                return Err(format!(
                    "ACP child pid={pid} did not exit within {CHILD_EXIT_DEADLINE_MS}ms"
                ));
            }
        }
        match exited.borrow().clone() {
            Some(info) if info.exit_code != Some(0) || info.signal_code.is_some() => Err(format!(
                "ACP child pid={pid} exited uncleanly during shutdown (code={:?}, signal={:?})",
                info.exit_code, info.signal_code
            )),
            _ => Ok(()),
        }
    })
}
async fn wait_for_exit(receiver: &mut watch::Receiver<Option<AcpChannelExitInfo>>) {
    loop {
        if receiver.borrow().is_some() {
            break;
        }
        if receiver.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(unix)]
async fn signal_child(pid: u32, force: bool) -> Result<(), String> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    let signal = if force {
        Signal::SIGKILL
    } else {
        Signal::SIGTERM
    };
    match kill(Pid::from_raw(pid as i32), signal) {
        Ok(()) => Ok(()),
        Err(nix::errno::Errno::ESRCH) => Ok(()),
        Err(error) => Err(format!("failed to signal ACP child pid={pid}: {error}")),
    }
}
#[cfg(not(unix))]
async fn signal_child(pid: u32, force: bool) -> Result<(), String> {
    let mut command = Command::new("taskkill");
    command.args(["/PID", &pid.to_string(), "/T"]);
    if force {
        command.arg("/F");
    }
    let status = command.status().await.map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to terminate ACP child pid={pid}"))
    }
}
#[cfg(unix)]
fn kill_child_sync(pid: u32) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;
    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
}
#[cfg(not(unix))]
fn kill_child_sync(pid: u32) {
    let _ = std::process::Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .spawn();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrubbed_environment_keys_cannot_be_reintroduced_by_overrides() {
        let source = [
            ("QWEN_SERVER_TOKEN".into(), "secret".into()),
            ("A".into(), "old".into()),
        ]
        .into_iter()
        .collect();
        let overrides = [
            ("QWEN_SERVER_TOKEN".into(), Some("again".into())),
            ("A".into(), Some("new".into())),
            ("B".into(), None),
        ]
        .into_iter()
        .collect();
        let env = scrub_child_env(&source, &overrides);
        assert!(!env.contains_key("QWEN_SERVER_TOKEN"));
        assert_eq!(env["A"], "new");
        assert_eq!(env["QWEN_CODE_SERVE"], "1");
    }
}
