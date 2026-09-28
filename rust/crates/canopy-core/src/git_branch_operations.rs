// Copyright 2026 Canopy Team
// SPDX-License-Identifier: Apache-2.0
//! Bounded Git branch mutations shared by native hosts.

use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_COMMAND_OUTPUT: usize = 10 * 1024 * 1024;
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const ENV_VARS_TO_CLEAR: &[&str] = &[
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_INDEX_FILE",
    "GIT_CONFIG_GLOBAL",
    "GIT_CONFIG_SYSTEM",
    "GIT_CONFIG_NOSYSTEM",
    "GH_REPO",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_PARAMETERS",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
];
const ENV_PREFIXES_TO_CLEAR: &[&str] = &["GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitCheckoutResult {
    pub branch: String,
    pub detached: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitMutationResult {
    pub success: bool,
    pub output: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitCommitResult {
    pub sha: String,
    pub subject: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GitBranchOperationError {
    InvalidInput(String),
    CommandFailed { command: String, stderr: String },
    TimedOut { command: String },
    OutputLimitExceeded { command: String },
}

impl fmt::Display for GitBranchOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) => f.write_str(message),
            Self::CommandFailed { command, stderr } if stderr.trim().is_empty() => {
                write!(f, "{command} failed")
            }
            Self::CommandFailed { command, stderr } => {
                write!(f, "{command} failed: {}", stderr.trim())
            }
            Self::TimedOut { command } => write!(f, "{command} timed out"),
            Self::OutputLimitExceeded { command } => {
                write!(f, "{command} exceeded the output limit")
            }
        }
    }
}

impl Error for GitBranchOperationError {}

pub fn is_valid_ref_name(name: &str) -> bool {
    if name.is_empty()
        || name == "HEAD"
        || name.starts_with('/')
        || name.ends_with('/')
        || name.starts_with('.')
        || name.ends_with('.')
        || name.ends_with(".lock")
        || name.contains("..")
        || name.contains("//")
        || name.contains("/.")
        || name.contains("./")
        || name.contains(".lock/")
        || name.contains("@{")
    {
        return false;
    }
    if name
        .split('/')
        .any(|component| component.encode_utf16().count() > 255)
    {
        return false;
    }
    !name.chars().any(|character| {
        character.is_control()
            || matches!(
                character,
                ' ' | '~' | '^' | ':' | '?' | '*' | '[' | '\\'
                    | '\u{200b}'..='\u{200d}'
                    | '\u{2028}'..='\u{2029}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
                    | '\u{feff}'
            )
    })
}

pub fn is_valid_git_sha(value: &str) -> bool {
    (value.len() == 40 || value.len() == 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn is_valid_checkout_ref(value: &str) -> bool {
    let value = value.trim();
    value == "HEAD" || is_valid_ref_name(value) || is_valid_git_sha(value)
}

pub fn git_checkout(
    cwd: &Path,
    reference: &str,
) -> Result<GitCheckoutResult, GitBranchOperationError> {
    if !is_valid_checkout_ref(reference) {
        return Err(GitBranchOperationError::InvalidInput(format!(
            "invalid checkout ref: {reference}"
        )));
    }

    let remote_ref = format!("refs/remotes/{reference}");
    if run_git(cwd, &["show-ref", "--verify", "--quiet", &remote_ref]).is_ok() {
        let local_name = reference
            .split_once('/')
            .map(|(_, name)| name)
            .unwrap_or_default();
        if !is_valid_checkout_ref(local_name) {
            return Err(GitBranchOperationError::InvalidInput(format!(
                "invalid local branch name derived from remote ref: {local_name}"
            )));
        }
        let local_ref = format!("refs/heads/{local_name}");
        if run_git(cwd, &["show-ref", "--verify", "--quiet", &local_ref]).is_ok() {
            run_git(cwd, &["checkout", local_name, "--"])?;
        } else {
            run_git(cwd, &["checkout", "--track", reference])?;
        }
        let branch = run_git(cwd, &["symbolic-ref", "--short", "HEAD"])?;
        return Ok(GitCheckoutResult {
            branch: branch.trim().to_owned(),
            detached: false,
        });
    }

    run_git(cwd, &["checkout", reference, "--"])?;
    let symbolic_head = run_git(cwd, &["symbolic-ref", "--short", "HEAD"]).unwrap_or_default();
    if !symbolic_head.trim().is_empty() {
        return Ok(GitCheckoutResult {
            branch: symbolic_head.trim().to_owned(),
            detached: false,
        });
    }
    let sha = run_git(cwd, &["rev-parse", "--short", "HEAD"])?;
    Ok(GitCheckoutResult {
        branch: sha.trim().to_owned(),
        detached: true,
    })
}

pub fn git_create_branch(
    cwd: &Path,
    name: &str,
    start_point: Option<&str>,
) -> Result<GitCheckoutResult, GitBranchOperationError> {
    if !is_valid_ref_name(name) || name.starts_with('-') {
        return Err(GitBranchOperationError::InvalidInput(format!(
            "invalid branch name: {name}"
        )));
    }
    if let Some(start_point) = start_point
        && !is_valid_checkout_ref(start_point)
    {
        return Err(GitBranchOperationError::InvalidInput(format!(
            "invalid start point: {start_point}"
        )));
    }

    let mut args = vec!["checkout", "-b", name];
    if let Some(start_point) = start_point {
        args.push(start_point);
    }
    args.push("--");

    // `git checkout -b <existing-name>` fails before switching branches. If
    // that name is also the current branch, checking only HEAD after failure
    // would mistake the pre-existing branch for one created by this call and
    // could delete it during rollback.
    let branch_exists = !run_git(
        cwd,
        &["branch", "--list", "--format=%(refname:short)", name],
    )?
    .trim()
    .is_empty();

    let original_ref =
        run_git(cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"]).unwrap_or_default();
    let original_ref = original_ref.trim().to_owned();
    let original_commit = if original_ref.is_empty() {
        run_git(cwd, &["rev-parse", "HEAD"])
            .unwrap_or_default()
            .trim()
            .to_owned()
    } else {
        String::new()
    };

    if let Err(error) = run_git(cwd, &args) {
        let current_ref =
            run_git(cwd, &["symbolic-ref", "--quiet", "--short", "HEAD"]).unwrap_or_default();
        if !branch_exists && current_ref.trim() == name {
            if !original_ref.is_empty() {
                let _ = run_git(cwd, &["checkout", &original_ref, "--"]);
            } else if !original_commit.is_empty() {
                let _ = run_git(cwd, &["checkout", "--detach", &original_commit, "--"]);
            }
            let _ = run_git(cwd, &["branch", "-D", name]);
        }
        return Err(error);
    }
    Ok(GitCheckoutResult {
        branch: name.to_owned(),
        detached: false,
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GitPushOptions {
    pub set_upstream: bool,
    pub force: bool,
}

pub fn git_push(
    cwd: &Path,
    options: GitPushOptions,
) -> Result<GitMutationResult, GitBranchOperationError> {
    let mut args = vec!["push"];
    if options.force {
        args.push("--force-with-lease");
    }
    let mut owned_args = Vec::<String>::new();
    if options.set_upstream {
        let branch = run_git(cwd, &["symbolic-ref", "--short", "HEAD"]).map_err(|_| {
            GitBranchOperationError::InvalidInput(
                "cannot push with --set-upstream in detached HEAD state; check out a branch first"
                    .to_owned(),
            )
        })?;
        let branch = branch.trim();
        let upstream = format!("{branch}@{{u}}");
        if run_git(cwd, &["rev-parse", "--abbrev-ref", &upstream])
            .is_ok_and(|s| !s.trim().is_empty())
        {
            let output = run_git(cwd, &args)?;
            return Ok(GitMutationResult {
                success: true,
                output: output.trim().to_owned(),
            });
        }

        let push_remote_key = format!("branch.{branch}.pushRemote");
        let branch_remote_key = format!("branch.{branch}.remote");
        let mut remote = run_git(cwd, &["config", &push_remote_key])
            .unwrap_or_default()
            .trim()
            .to_owned();
        if remote.is_empty() {
            remote = run_git(cwd, &["config", "remote.pushDefault"])
                .unwrap_or_default()
                .trim()
                .to_owned();
        }
        if remote.is_empty() {
            remote = run_git(cwd, &["config", &branch_remote_key])
                .unwrap_or_default()
                .trim()
                .to_owned();
        }
        if remote.is_empty() {
            let remotes = run_git(cwd, &["remote"]).unwrap_or_default();
            let mut remotes = remotes.lines().map(str::trim).filter(|s| !s.is_empty());
            let first = remotes.next().unwrap_or("origin");
            remote = if remotes.next().is_none() {
                first.to_owned()
            } else {
                "origin".to_owned()
            };
        }
        owned_args.extend(["--set-upstream".to_owned(), remote, branch.to_owned()]);
        args.extend(owned_args.iter().map(String::as_str));
    }
    let output = run_git(cwd, &args)?;
    Ok(GitMutationResult {
        success: true,
        output: output.trim().to_owned(),
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GitPullOptions {
    pub rebase: bool,
    pub fetch_only: bool,
}

pub fn git_pull(
    cwd: &Path,
    options: GitPullOptions,
) -> Result<GitMutationResult, GitBranchOperationError> {
    let args = if options.fetch_only {
        vec!["fetch", "--all", "--prune"]
    } else if options.rebase {
        vec!["pull", "--rebase"]
    } else {
        vec!["pull"]
    };
    let output = run_git(cwd, &args)?;
    Ok(GitMutationResult {
        success: true,
        output: output.trim().to_owned(),
    })
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GitCommitOptions {
    pub all: bool,
}

pub fn git_commit(
    cwd: &Path,
    message: &str,
    options: GitCommitOptions,
) -> Result<GitCommitResult, GitBranchOperationError> {
    let saved_tree = if options.all {
        match run_git(cwd, &["write-tree"]) {
            Ok(tree) if !tree.trim().is_empty() => Some(tree.trim().to_owned()),
            _ => {
                let unmerged = run_git(cwd, &["ls-files", "--unmerged"])?;
                if !unmerged.trim().is_empty() {
                    return Err(GitBranchOperationError::InvalidInput(
                        "cannot stage all changes: unresolved merge conflicts in the index"
                            .to_owned(),
                    ));
                }
                return Err(GitBranchOperationError::InvalidInput(
                    "cannot stage all changes: failed to snapshot index (write-tree failed)"
                        .to_owned(),
                ));
            }
        }
    } else {
        None
    };

    let commit_operation = (|| {
        if options.all {
            run_git(cwd, &["add", "-A"])?;
        }
        run_git(cwd, &["commit", "-m", message])?;
        Ok::<(), GitBranchOperationError>(())
    })();

    if let Err(error) = commit_operation {
        if let Some(tree) = saved_tree
            && let Err(rollback_error) = run_git(cwd, &["read-tree", &tree])
        {
            eprintln!("git index rollback failed: {rollback_error}");
        }
        return Err(error);
    }
    let sha = run_git(cwd, &["rev-parse", "--short", "HEAD"])?;
    let subject = run_git(cwd, &["log", "-1", "--format=%s"])?;
    Ok(GitCommitResult {
        sha: sha.trim().to_owned(),
        subject: subject.trim().to_owned(),
    })
}

struct BoundedOutput {
    bytes: Vec<u8>,
    exceeded: bool,
}

fn read_bounded(mut reader: impl Read) -> io::Result<BoundedOutput> {
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = MAX_COMMAND_OUTPUT.saturating_sub(bytes.len());
        let keep = remaining.min(count);
        bytes.extend_from_slice(&buffer[..keep]);
        exceeded |= keep != count;
    }
    Ok(BoundedOutput { bytes, exceeded })
}

fn run_git(cwd: &Path, args: &[&str]) -> Result<String, GitBranchOperationError> {
    let command_label = format!("git {}", args.join(" "));
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LC_ALL", "C")
        .env("LANG", "C");
    for key in ENV_VARS_TO_CLEAR {
        command.env_remove(key);
    }
    for (key, _) in std::env::vars_os() {
        if starts_with_any(&key, ENV_PREFIXES_TO_CLEAR) {
            command.env_remove(key);
        }
    }

    let mut child = command
        .spawn()
        .map_err(|error| GitBranchOperationError::CommandFailed {
            command: command_label.clone(),
            stderr: error.to_string(),
        })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitBranchOperationError::CommandFailed {
            command: command_label.clone(),
            stderr: "could not capture stdout".to_owned(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GitBranchOperationError::CommandFailed {
            command: command_label.clone(),
            stderr: "could not capture stderr".to_owned(),
        })?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout));
    let stderr_reader = thread::spawn(move || read_bounded(stderr));
    let started = Instant::now();
    let status = wait_for_exit(&mut child, started, &command_label);
    let stdout = join_output(stdout_reader);
    let stderr = join_output(stderr_reader);
    let status = status?;
    if stdout.exceeded || stderr.exceeded {
        return Err(GitBranchOperationError::OutputLimitExceeded {
            command: command_label,
        });
    }
    let output = String::from_utf8_lossy(&stdout.bytes).into_owned();
    if !status.success() {
        return Err(GitBranchOperationError::CommandFailed {
            command: command_label,
            stderr: String::from_utf8_lossy(&stderr.bytes).into_owned(),
        });
    }
    Ok(output)
}

fn wait_for_exit(
    child: &mut std::process::Child,
    started: Instant,
    command: &str,
) -> Result<ExitStatus, GitBranchOperationError> {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if started.elapsed() < GIT_TIMEOUT => thread::sleep(POLL_INTERVAL),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(GitBranchOperationError::TimedOut {
                    command: command.to_owned(),
                });
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(GitBranchOperationError::CommandFailed {
                    command: command.to_owned(),
                    stderr: error.to_string(),
                });
            }
        }
    }
}

fn join_output(handle: thread::JoinHandle<io::Result<BoundedOutput>>) -> BoundedOutput {
    handle
        .join()
        .ok()
        .and_then(Result::ok)
        .unwrap_or(BoundedOutput {
            bytes: Vec::new(),
            exceeded: false,
        })
}

fn starts_with_any(value: &OsStr, prefixes: &[&str]) -> bool {
    let value = value.to_string_lossy();
    prefixes.iter().any(|prefix| value.starts_with(prefix))
}
