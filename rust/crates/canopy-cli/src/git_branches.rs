//! Read-only Git branch, tag, and checkout-history queries for serve routes.
//!
//! The helper keeps repository selection tied to `cwd`, removes inherited Git
//! environment redirects, and bounds every Git child by time and output size.

use serde::Serialize;
use std::error::Error;
use std::ffi::OsStr;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const GIT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_COMMAND_OUTPUT: usize = 10 * 1024 * 1024;
const MAX_RECENT_BRANCHES: usize = 20;
const MAX_REFLOG_ENTRIES: usize = 200;
const STDERR_CAPTURE_LIMIT: usize = MAX_COMMAND_OUTPUT;

const GIT_ENV_VARS_TO_CLEAR: &[&str] = &[
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

const GIT_ENV_PREFIXES_TO_CLEAR: &[&str] = &["GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"];

/// Branch, tag, and recent checkout information for a Git worktree.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranches {
    pub local: Vec<GitBranchInfo>,
    pub remote: Vec<GitBranchInfo>,
    pub tags: Vec<GitTagInfo>,
    pub recent: Vec<String>,
    pub head: String,
    pub detached: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitBranchInfo {
    pub name: String,
    pub is_head: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    pub ahead: u64,
    pub behind: u64,
    /// Unix epoch seconds of the branch tip commit.
    pub commit_date: i64,
    pub commit_subject: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitTagInfo {
    pub name: String,
    /// Unix epoch seconds of the tag or tagged commit.
    pub date: i64,
    pub subject: String,
}

/// Failure from the repository probe or a bounded Git subprocess.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GitBranchesError {
    NotRepository,
    CommandFailed { command: String },
    TimedOut { command: String },
    OutputLimitExceeded { command: String },
}

impl fmt::Display for GitBranchesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRepository => f.write_str("workspace is not a Git repository"),
            Self::CommandFailed { command } => write!(f, "{command} failed"),
            Self::TimedOut { command } => write!(f, "{command} timed out"),
            Self::OutputLimitExceeded { command } => {
                write!(f, "{command} exceeded the output limit")
            }
        }
    }
}

impl Error for GitBranchesError {}

/// Fetch branch, tag, and recent checkout data for the repository at `cwd`.
///
/// As in the TypeScript implementation, only the initial repository probe is
/// required to succeed. Failures in optional ref, reflog, and head queries
/// produce empty values, so partial repository metadata remains available.
pub fn fetch_git_branches(cwd: &Path) -> Result<GitBranches, GitBranchesError> {
    let probe_args = ["rev-parse", "--git-dir"];
    if let Err(error) = run_git(cwd, &probe_args) {
        return Err(match error {
            GitCommandError::Failed { stderr }
                if String::from_utf8_lossy(&stderr)
                    .to_ascii_lowercase()
                    .contains("not a git repository") =>
            {
                GitBranchesError::NotRepository
            }
            GitCommandError::Failed { .. } => GitBranchesError::CommandFailed {
                command: command_label(&probe_args),
            },
            GitCommandError::TimedOut => GitBranchesError::TimedOut {
                command: command_label(&probe_args),
            },
            GitCommandError::OutputLimitExceeded => GitBranchesError::OutputLimitExceeded {
                command: command_label(&probe_args),
            },
        });
    }

    let branch_format = "--format=%(refname:short)%00%(HEAD)%00%(upstream:short)%00%(upstream:track,nobracket)%00%(committerdate:unix)%00%(subject)%00%(symref)";
    let local_args = ["for-each-ref", branch_format, "refs/heads/"];
    let remote_args = ["for-each-ref", branch_format, "refs/remotes/"];
    let tag_args = [
        "for-each-ref",
        "--format=%(refname:short)%00%(creatordate:unix)%00%(subject)",
        "--sort=-creatordate",
        "refs/tags/",
    ];
    let head_args = ["symbolic-ref", "--short", "HEAD"];
    let reflog_limit = format!("-{MAX_REFLOG_ENTRIES}");
    let reflog_args = ["reflog", "show", "--format=%gs", reflog_limit.as_str()];

    let (local_raw, remote_raw, tags_raw, head_raw, reflog_raw) = thread::scope(|scope| {
        let local = scope.spawn(|| run_git(cwd, &local_args));
        let remote = scope.spawn(|| run_git(cwd, &remote_args));
        let tags = scope.spawn(|| run_git(cwd, &tag_args));
        let head = scope.spawn(|| run_git(cwd, &head_args));
        let reflog = scope.spawn(|| run_git(cwd, &reflog_args));

        (
            best_effort_join(local),
            best_effort_join(remote),
            best_effort_join(tags),
            best_effort_join(head),
            best_effort_join(reflog),
        )
    });

    let local = parse_branch_lines(&local_raw);
    let remote = parse_branch_lines(&remote_raw);
    let tags = parse_tag_lines(&tags_raw);
    let symbolic_head = String::from_utf8_lossy(&head_raw).trim().to_owned();
    let detached = symbolic_head.is_empty();
    let head = if detached {
        run_git(cwd, &["rev-parse", "--short", "HEAD"])
            .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_owned())
            .unwrap_or_default()
    } else {
        symbolic_head
    };
    let recent = parse_recent_branches(&reflog_raw, if detached { "" } else { &head });

    Ok(GitBranches {
        local,
        remote,
        tags,
        recent,
        head,
        detached,
    })
}

fn best_effort_join(
    handle: thread::ScopedJoinHandle<'_, Result<Vec<u8>, GitCommandError>>,
) -> Vec<u8> {
    handle.join().ok().and_then(Result::ok).unwrap_or_default()
}

fn parse_branch_lines(raw: &[u8]) -> Vec<GitBranchInfo> {
    let mut branches = Vec::new();
    for line in raw
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let fields = split_nul_fields(line);
        if fields.len() < 7 || !fields[6].is_empty() {
            // Ignore malformed rows and symbolic refs (for example
            // refs/remotes/origin/HEAD -> refs/remotes/origin/main).
            continue;
        }
        let track = String::from_utf8_lossy(fields[3]);
        let (ahead, behind) = parse_tracking_counts(&track);
        branches.push(GitBranchInfo {
            name: decode_field(fields[0]),
            is_head: fields[1] == b"*",
            upstream: nonempty_field(fields[2]),
            ahead,
            behind,
            commit_date: parse_i64(fields[4]),
            commit_subject: decode_field(fields[5]),
        });
    }
    branches
}

fn parse_tag_lines(raw: &[u8]) -> Vec<GitTagInfo> {
    raw.split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            let fields = split_nul_fields(line);
            (fields.len() >= 3).then(|| GitTagInfo {
                name: decode_field(fields[0]),
                date: parse_i64(fields[1]),
                subject: decode_field(fields[2]),
            })
        })
        .collect()
}

fn split_nul_fields(line: &[u8]) -> Vec<&[u8]> {
    line.split(|byte| *byte == 0).collect()
}

fn decode_field(field: &[u8]) -> String {
    String::from_utf8_lossy(field).into_owned()
}

fn nonempty_field(field: &[u8]) -> Option<String> {
    (!field.is_empty()).then(|| decode_field(field))
}

fn parse_i64(raw: &[u8]) -> i64 {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn parse_tracking_counts(track: &str) -> (u64, u64) {
    let mut ahead = 0;
    let mut behind = 0;
    for part in track.split(", ") {
        if let Some(count) = part.strip_prefix("ahead ") {
            ahead = count.parse().unwrap_or(0);
        } else if let Some(count) = part.strip_prefix("behind ") {
            behind = count.parse().unwrap_or(0);
        }
    }
    (ahead, behind)
}

fn parse_recent_branches(raw: &[u8], current_head: &str) -> Vec<String> {
    let mut seen = Vec::<String>::new();
    for line in raw
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let message = String::from_utf8_lossy(line);
        let Some(rest) = message.strip_prefix("checkout: moving from ") else {
            continue;
        };
        let Some((_, branch)) = rest.split_once(" to ") else {
            continue;
        };
        if branch.is_empty()
            || branch == current_head
            || is_object_id(branch)
            || seen.iter().any(|item| item.as_str() == branch)
        {
            continue;
        }
        seen.push(branch.to_owned());
        if seen.len() == MAX_RECENT_BRANCHES {
            break;
        }
    }
    seen
}

fn is_object_id(value: &str) -> bool {
    (7..=40).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn command_label(args: &[&str]) -> String {
    format!("git {}", args.join(" "))
}

#[derive(Debug)]
enum GitCommandError {
    Failed { stderr: Vec<u8> },
    TimedOut,
    OutputLimitExceeded,
}

struct BoundedRead {
    bytes: Vec<u8>,
    exceeded: bool,
}

fn read_bounded(mut reader: impl Read, limit: usize) -> io::Result<BoundedRead> {
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    let mut exceeded = false;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        let keep = remaining.min(count);
        bytes.extend_from_slice(&buffer[..keep]);
        exceeded |= keep != count;
    }
    Ok(BoundedRead { bytes, exceeded })
}

fn run_git(cwd: &Path, args: &[&str]) -> Result<Vec<u8>, GitCommandError> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LC_ALL", "C")
        .env("LANG", "C");
    for key in GIT_ENV_VARS_TO_CLEAR {
        command.env_remove(key);
    }
    for (key, _) in std::env::vars_os() {
        if starts_with_any(&key, GIT_ENV_PREFIXES_TO_CLEAR) {
            command.env_remove(key);
        }
    }

    let mut child = command
        .spawn()
        .map_err(|_| GitCommandError::Failed { stderr: Vec::new() })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitCommandError::Failed { stderr: Vec::new() })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GitCommandError::Failed { stderr: Vec::new() })?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, MAX_COMMAND_OUTPUT));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, STDERR_CAPTURE_LIMIT));

    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if started.elapsed() < GIT_TIMEOUT => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(GitCommandError::TimedOut);
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(GitCommandError::Failed { stderr: Vec::new() });
            }
        }
    };

    let stdout = stdout_reader
        .join()
        .ok()
        .and_then(Result::ok)
        .unwrap_or(BoundedRead {
            bytes: Vec::new(),
            exceeded: false,
        });
    let stderr = stderr_reader
        .join()
        .ok()
        .and_then(Result::ok)
        .unwrap_or(BoundedRead {
            bytes: Vec::new(),
            exceeded: false,
        });

    let status = status?;
    if stdout.exceeded || stderr.exceeded {
        return Err(GitCommandError::OutputLimitExceeded);
    }
    if !status.success() {
        return Err(GitCommandError::Failed {
            stderr: stderr.bytes,
        });
    }
    Ok(stdout.bytes)
}

fn starts_with_any(value: &OsStr, prefixes: &[&str]) -> bool {
    let value = value.to_string_lossy();
    prefixes.iter().any(|prefix| value.starts_with(prefix))
}
