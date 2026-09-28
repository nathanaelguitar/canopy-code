//! Bounded external process execution shared by mobile device adapters.

use std::io::{self, Read};
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use wait_timeout::ChildExt;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_OUTPUT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct CommandOutput {
    pub status: ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug)]
pub struct CommandError {
    pub program: String,
    pub message: String,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.program, self.message)
    }
}

impl std::error::Error for CommandError {}

#[derive(Clone, Debug)]
pub struct CommandRunner {
    pub timeout: Duration,
    pub max_output_bytes: usize,
}

impl Default for CommandRunner {
    fn default() -> Self {
        Self {
            timeout: COMMAND_TIMEOUT,
            max_output_bytes: MAX_OUTPUT_BYTES,
        }
    }
}

impl CommandRunner {
    pub fn run<I, S>(
        &self,
        program: impl Into<PathBuf>,
        args: I,
    ) -> Result<CommandOutput, CommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let program = program.into();
        let display = program.display().to_string();
        let mut command = Command::new(&program);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|e| CommandError {
            program: display.clone(),
            message: e.to_string(),
            stdout: vec![],
            stderr: vec![],
        })?;

        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let output_limit = self.max_output_bytes;
        let stdout_reader = thread::spawn(move || drain_limited(stdout, output_limit));
        let stderr_reader = thread::spawn(move || drain_limited(stderr, output_limit));

        let deadline = Instant::now() + self.timeout;
        let status = loop {
            let now = Instant::now();
            if now >= deadline {
                kill_process_tree(&mut child);
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(CommandError {
                    program: display,
                    message: format!("timed out after {:.2} seconds", self.timeout.as_secs_f64()),
                    stdout: vec![],
                    stderr: vec![],
                });
            }
            match child.wait_timeout((deadline - now).min(Duration::from_millis(100))) {
                Ok(Some(status)) => break status,
                Ok(None) => continue,
                Err(error) => {
                    kill_process_tree(&mut child);
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(CommandError {
                        program: display,
                        message: error.to_string(),
                        stdout: vec![],
                        stderr: vec![],
                    });
                }
            }
        };

        // A command can exit while a helper it spawned still holds inherited
        // stdout/stderr pipes. Treat pipe draining as part of the same deadline
        // and stop any such helper before joining the readers.
        while (!stdout_reader.is_finished() || !stderr_reader.is_finished())
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(5));
        }
        let drain_timed_out = !stdout_reader.is_finished() || !stderr_reader.is_finished();
        if drain_timed_out {
            kill_process_group(child.id());
        }

        let stdout_result = stdout_reader.join().map_err(|_| CommandError {
            program: display.clone(),
            message: "stdout reader failed".to_owned(),
            stdout: vec![],
            stderr: vec![],
        })?;
        let stderr_result = stderr_reader.join().map_err(|_| CommandError {
            program: display.clone(),
            message: "stderr reader failed".to_owned(),
            stdout: vec![],
            stderr: vec![],
        })?;
        let (stdout, stdout_truncated) = stdout_result.map_err(|error| CommandError {
            program: display.clone(),
            message: format!("failed reading stdout: {error}"),
            stdout: vec![],
            stderr: vec![],
        })?;
        let (stderr, stderr_truncated) = stderr_result.map_err(|error| CommandError {
            program: display.clone(),
            message: format!("failed reading stderr: {error}"),
            stdout: vec![],
            stderr: vec![],
        })?;
        if drain_timed_out {
            return Err(CommandError {
                program: display,
                message: format!("timed out after {:.2} seconds", self.timeout.as_secs_f64()),
                stdout,
                stderr,
            });
        }
        if stdout_truncated || stderr_truncated {
            return Err(CommandError {
                program: display,
                message: format!("output exceeded the {} byte limit", self.max_output_bytes),
                stdout,
                stderr,
            });
        }
        Ok(CommandOutput {
            status,
            stdout,
            stderr,
        })
    }

    pub fn run_checked<I, S>(
        &self,
        program: impl Into<PathBuf>,
        args: I,
    ) -> Result<CommandOutput, CommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let program = program.into();
        let display = program.display().to_string();
        let output = self.run(program, args)?;
        if output.status.success() {
            Ok(output)
        } else {
            let message = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            Err(CommandError {
                program: display,
                message: if message.is_empty() {
                    format!("exited with {}", output.status)
                } else {
                    message
                },
                stdout: output.stdout,
                stderr: output.stderr,
            })
        }
    }

    pub fn spawn<I, S>(&self, program: impl Into<PathBuf>, args: I) -> Result<Child, CommandError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let program = program.into();
        let mut command = Command::new(&program);
        command
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        command.spawn().map_err(|e| CommandError {
            program: program.display().to_string(),
            message: e.to_string(),
            stdout: vec![],
            stderr: vec![],
        })
    }
}

#[cfg(unix)]
fn kill_process_tree(child: &mut Child) {
    kill_process_group(child.id());
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn kill_process_group(pid: u32) {
    let group = -(pid as i32);
    // Each bounded invocation gets a private process group above so helpers
    // that outlive their direct parent do not keep its output pipes open.
    let _ = unsafe { libc::kill(group, libc::SIGKILL) };
}

#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

#[cfg(not(unix))]
fn kill_process_tree(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn drain_limited(mut reader: impl Read, limit: usize) -> io::Result<(Vec<u8>, bool)> {
    let mut retained = Vec::with_capacity(limit.min(64 * 1024));
    let mut buf = [0_u8; 8192];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buf)?;
        if count == 0 {
            break;
        }
        let room = limit.saturating_sub(retained.len());
        truncated |= count > room;
        retained.extend_from_slice(&buf[..count.min(room)]);
    }
    Ok((retained, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn bounds_output_and_kills_timed_out_children() {
        let bounded = CommandRunner {
            timeout: Duration::from_secs(2),
            max_output_bytes: 1024,
        };
        let overflow = bounded
            .run("/usr/bin/head", ["-c", "2048", "/dev/zero"])
            .unwrap_err();
        assert!(overflow.message.contains("output exceeded"));
        assert_eq!(overflow.stdout.len(), 1024);

        let timed = CommandRunner {
            timeout: Duration::from_millis(30),
            max_output_bytes: 1024,
        };
        let timeout = timed.run("/bin/sleep", ["5"]).unwrap_err();
        assert!(timeout.message.contains("timed out"));

        let started = Instant::now();
        let timeout = timed.run("/bin/sh", ["-c", "sleep 5 & wait"]).unwrap_err();
        assert!(timeout.message.contains("timed out"));
        assert!(started.elapsed() < Duration::from_secs(1));

        let started = Instant::now();
        let completed = timed.run("/bin/sh", ["-c", "sleep 5 & exit 0"]);
        assert!(completed.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
