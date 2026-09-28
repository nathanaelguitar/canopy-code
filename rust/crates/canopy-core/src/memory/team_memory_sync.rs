//! Best-effort synchronization of shared team memory through Git.
//!
//! Port of `packages/core/src/memory/team-memory-sync.ts`. Git execution is
//! injectable so the ordering and safety gates can be checked without a real
//! remote. The process adapter uses argument vectors (never a shell), bounded
//! child lifetimes, and noninteractive credential settings.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::time::Instant;

use super::paths::AutoMemoryPaths;

const GIT_TIMEOUT: Duration = Duration::from_secs(15);
const MUTATION_TERM_GRACE: Duration = Duration::from_millis(250);
const CHILD_REAP_GRACE: Duration = Duration::from_millis(500);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
// Node's execFile default maxBuffer is 1 MiB. Keep output memory bounded to the
// same size while continuing to drain the pipe so Git cannot block on stdout.
const MAX_GIT_STDOUT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TeamMemorySyncOptions {
    pub message: String,
    /// Cooperative per-user commit attribution. When omitted, Git uses the
    /// repository's configured author, just as the TypeScript implementation.
    pub author: Option<TeamMemoryGitAuthor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeamMemoryGitAuthor {
    pub name: String,
    pub email: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TeamMemorySyncSkippedReason {
    NotAGitRepo,
    NoUpstream,
    DetachedHead,
    PullFailed,
    PushFailed,
    LocalAhead,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TeamMemorySyncResult {
    pub committed: bool,
    pub pulled: bool,
    pub pushed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<TeamMemorySyncSkippedReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GitTerminationBehavior {
    /// The command is read-only or network-bound. Kill immediately on timeout.
    Force,
    /// Give Git time to release index/lock state before forcing termination.
    Graceful,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitCommandRequest {
    pub cwd: PathBuf,
    pub args: Vec<OsString>,
    /// Environment overrides. The process environment is otherwise inherited.
    pub env: BTreeMap<OsString, OsString>,
    pub timeout: Duration,
    pub termination: GitTerminationBehavior,
}

/// Process environment inputs used by the Git adapter. Supplying this value in
/// tests avoids mutating global environment variables.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TeamMemorySyncEnvironment {
    pub git_ssh_command: Option<OsString>,
}

impl TeamMemorySyncEnvironment {
    pub fn from_process() -> Self {
        Self {
            git_ssh_command: std::env::var_os("GIT_SSH_COMMAND"),
        }
    }

    fn git_overrides(&self) -> BTreeMap<OsString, OsString> {
        let ssh_command = self
            .git_ssh_command
            .as_deref()
            .map(|value| {
                format!(
                    "{} -oBatchMode=yes -oConnectTimeout=5",
                    value.to_string_lossy()
                )
            })
            .unwrap_or_else(|| "ssh -oBatchMode=yes -oConnectTimeout=5".to_owned());

        BTreeMap::from([
            (OsString::from("GIT_TERMINAL_PROMPT"), OsString::from("0")),
            (
                OsString::from("GIT_SSH_COMMAND"),
                OsString::from(ssh_command),
            ),
        ])
    }
}

/// Async seam for deterministic tests. A command returns stdout only when it
/// exits successfully; spawn errors, nonzero status, output overflow, and
/// timeout are all represented as `None`, matching the source's best-effort
/// `tryGit` helper.
pub trait GitCommandRunner: Send + Sync {
    fn run(&self, request: GitCommandRequest) -> impl Future<Output = Option<String>> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessGitCommandRunner;

impl GitCommandRunner for ProcessGitCommandRunner {
    fn run(&self, request: GitCommandRequest) -> impl Future<Output = Option<String>> + Send {
        run_git_process(request)
    }
}

/// Synchronize `.canopy/team-memory` using the process environment and the
/// real Git process adapter. All errors are swallowed into a best-effort result
/// so session startup cannot fail because Git or the remote is unavailable.
pub async fn sync_team_memory(
    project_root: impl AsRef<Path>,
    options: TeamMemorySyncOptions,
) -> TeamMemorySyncResult {
    let environment = TeamMemorySyncEnvironment::from_process();
    sync_team_memory_with(
        project_root,
        options,
        &ProcessGitCommandRunner,
        &environment,
    )
    .await
}

/// Injectable variant of [`sync_team_memory`] for tests and alternate process
/// adapters. The operation remains best-effort and never propagates an error.
pub async fn sync_team_memory_with<R: GitCommandRunner>(
    project_root: impl AsRef<Path>,
    options: TeamMemorySyncOptions,
    runner: &R,
    environment: &TeamMemorySyncEnvironment,
) -> TeamMemorySyncResult {
    let mut result = TeamMemorySyncResult::default();
    let team_root = AutoMemoryPaths::from_process(project_root.as_ref()).team_auto_memory_root();
    let Some(git_root) = find_git_root(&team_root) else {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::NotAGitRepo);
        return result;
    };
    let rel_path = team_root
        .strip_prefix(&git_root)
        .ok()
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    // Detached HEAD has no branch to advance. A commit here would be orphaned,
    // so stop before reading upstream or touching the index.
    let branch = run_git(
        runner,
        &git_root,
        ["symbolic-ref", "--quiet", "--short", "HEAD"],
        environment,
        GitTerminationBehavior::Force,
    )
    .await
    .map(|value| value.trim().to_owned())
    .filter(|value| !value.is_empty());
    let Some(branch) = branch else {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::DetachedHead);
        return result;
    };

    // This presence check gates both reconcile and push. No upstream means the
    // local branch can still receive a team-memory commit, but it cannot push.
    let upstream = run_git(
        runner,
        &git_root,
        ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
        environment,
        GitTerminationBehavior::Force,
    )
    .await;

    // Record preexisting ahead state before pull/commit. If it was already
    // ahead, pushing this branch would publish unrelated commits too.
    let was_ahead_before_sync = if upstream.is_some() {
        run_git(
            runner,
            &git_root,
            ["rev-list", "@{u}..HEAD"],
            environment,
            GitTerminationBehavior::Force,
        )
        .await
        .is_some_and(|ahead| !ahead.trim().is_empty())
    } else {
        false
    };

    // Reconcile before making our commit so a two-writer branch remains
    // fast-forwardable. `--ff-only` refuses divergence without creating a
    // merge commit; on failure leave the working tree and index untouched.
    if upstream.is_some() {
        result.pulled = run_git(
            runner,
            &git_root,
            ["pull", "--ff-only"],
            environment,
            GitTerminationBehavior::Force,
        )
        .await
        .is_some();
        if !result.pulled {
            result.skipped_reason = Some(TeamMemorySyncSkippedReason::PullFailed);
            return result;
        }
    }

    let status_args = vec![
        OsString::from("status"),
        OsString::from("--porcelain"),
        OsString::from("--"),
        rel_path.as_os_str().to_owned(),
    ];
    let status = run_git_args(
        runner,
        &git_root,
        status_args,
        environment,
        GitTerminationBehavior::Force,
    )
    .await;
    if status.is_some_and(|value| !value.trim().is_empty()) {
        let staged = run_git_args(
            runner,
            &git_root,
            vec![
                OsString::from("add"),
                OsString::from("--"),
                rel_path.as_os_str().to_owned(),
            ],
            environment,
            GitTerminationBehavior::Graceful,
        )
        .await
        .is_some();

        let mut commit_args = vec![OsString::from("commit"), OsString::from("-m")];
        commit_args.push(OsString::from(options.message));
        if let Some(author) = options.author {
            let email = author
                .email
                .unwrap_or_else(|| format!("{}@users.noreply", author.name));
            commit_args.push(OsString::from("--author"));
            commit_args.push(OsString::from(format!("{} <{}>", author.name, email)));
        }
        commit_args.push(OsString::from("--"));
        commit_args.push(rel_path.as_os_str().to_owned());
        result.committed = run_git_args(
            runner,
            &git_root,
            commit_args,
            environment,
            GitTerminationBehavior::Graceful,
        )
        .await
        .is_some();

        if !result.committed && staged {
            // Do not leave this sync's files staged for a later unrelated
            // manual commit if a hook, signing step, or author config fails.
            let _ = run_git_args(
                runner,
                &git_root,
                vec![
                    OsString::from("reset"),
                    OsString::from("--quiet"),
                    OsString::from("--"),
                    rel_path.as_os_str().to_owned(),
                ],
                environment,
                GitTerminationBehavior::Graceful,
            )
            .await;
        }
    }

    if upstream.is_none() {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::NoUpstream);
        return result;
    }
    if !result.committed {
        return result;
    }
    if was_ahead_before_sync {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::LocalAhead);
        return result;
    }

    // Use the current branch's configured remote and merge ref, then push only
    // HEAD to that one destination. Never use an unqualified `git push`.
    let remote = run_git(
        runner,
        &git_root,
        ["config", "--get", &format!("branch.{branch}.remote")],
        environment,
        GitTerminationBehavior::Force,
    )
    .await
    .map(|value| value.trim().to_owned())
    .filter(|value| !value.is_empty());
    let merge_ref = run_git(
        runner,
        &git_root,
        ["config", "--get", &format!("branch.{branch}.merge")],
        environment,
        GitTerminationBehavior::Force,
    )
    .await
    .map(|value| value.trim().to_owned())
    .filter(|value| !value.is_empty());
    let (Some(remote), Some(merge_ref)) = (remote, merge_ref) else {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::PushFailed);
        return result;
    };

    result.pushed = run_git_args(
        runner,
        &git_root,
        vec![
            OsString::from("push"),
            OsString::from("--"),
            OsString::from(remote),
            OsString::from(format!("HEAD:{merge_ref}")),
        ],
        environment,
        GitTerminationBehavior::Force,
    )
    .await
    .is_some();
    if !result.pushed {
        result.skipped_reason = Some(TeamMemorySyncSkippedReason::PushFailed);
    }
    result
}

async fn run_git<const N: usize, R: GitCommandRunner>(
    runner: &R,
    cwd: &Path,
    args: [&str; N],
    environment: &TeamMemorySyncEnvironment,
    termination: GitTerminationBehavior,
) -> Option<String> {
    run_git_args(
        runner,
        cwd,
        args.into_iter().map(OsString::from).collect(),
        environment,
        termination,
    )
    .await
}

async fn run_git_args<R: GitCommandRunner>(
    runner: &R,
    cwd: &Path,
    args: Vec<OsString>,
    environment: &TeamMemorySyncEnvironment,
    termination: GitTerminationBehavior,
) -> Option<String> {
    runner
        .run(GitCommandRequest {
            cwd: cwd.to_path_buf(),
            args,
            env: environment.git_overrides(),
            timeout: GIT_TIMEOUT,
            termination,
        })
        .await
}

async fn run_git_process(request: GitCommandRequest) -> Option<String> {
    let mut command = Command::new("git");
    command
        .args(&request.args)
        .current_dir(&request.cwd)
        .envs(&request.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command.spawn().ok()?;
    let stdout = child.stdout.take()?;
    let output_reader = tokio::spawn(read_bounded_stdout(stdout));
    let started = Instant::now();
    let timeout_at = started + request.timeout;
    let graceful_at = if request.termination == GitTerminationBehavior::Graceful {
        timeout_at
            .checked_sub(MUTATION_TERM_GRACE)
            .unwrap_or(started)
    } else {
        timeout_at
    };

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {}
            Err(_) => break None,
        }

        let now = Instant::now();
        if now >= timeout_at {
            force_kill_child_group(&mut child).await;
            let _ = tokio::time::timeout(CHILD_REAP_GRACE, child.wait()).await;
            output_reader.abort();
            return None;
        }
        if request.termination == GitTerminationBehavior::Graceful && now >= graceful_at {
            signal_child_group(&child, GitTerminationBehavior::Graceful);
            match tokio::time::timeout_at(timeout_at, child.wait()).await {
                Ok(Ok(status)) => break Some(status),
                Ok(Err(_)) => break None,
                Err(_) => {
                    force_kill_child_group(&mut child).await;
                    let _ = tokio::time::timeout(CHILD_REAP_GRACE, child.wait()).await;
                    output_reader.abort();
                    return None;
                }
            }
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    };
    let Some(status) = status else {
        let _ = child.start_kill();
        output_reader.abort();
        return None;
    };
    if !status.success() {
        output_reader.abort();
        return None;
    }

    let reader_remaining = timeout_at.saturating_duration_since(Instant::now());
    let mut output_reader = output_reader;
    let Ok(Ok((bytes, overflow))) =
        tokio::time::timeout(reader_remaining, &mut output_reader).await
    else {
        output_reader.abort();
        return None;
    };
    if overflow {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

async fn read_bounded_stdout<R: AsyncRead + Unpin>(mut stdout: R) -> (Vec<u8>, bool) {
    let mut output = Vec::with_capacity(MAX_GIT_STDOUT_BYTES.min(8192));
    let mut overflow = false;
    let mut buffer = [0u8; 8192];
    loop {
        let Ok(size) = stdout.read(&mut buffer).await else {
            return (output, true);
        };
        if size == 0 {
            break;
        }
        let remaining = MAX_GIT_STDOUT_BYTES.saturating_sub(output.len());
        let to_copy = remaining.min(size);
        output.extend_from_slice(&buffer[..to_copy]);
        overflow |= to_copy < size;
    }
    (output, overflow)
}

#[cfg(unix)]
fn signal_child_group(child: &Child, behavior: GitTerminationBehavior) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;

    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        return;
    };
    let signal = match behavior {
        GitTerminationBehavior::Force => Signal::SIGKILL,
        GitTerminationBehavior::Graceful => Signal::SIGTERM,
    };
    let _ = killpg(Pid::from_raw(pid), signal);
}

#[cfg(not(unix))]
fn signal_child_group(child: &Child, _behavior: GitTerminationBehavior) {
    let _ = child.id();
}

async fn force_kill_child_group(child: &mut Child) {
    #[cfg(unix)]
    signal_child_group(child, GitTerminationBehavior::Force);
    let _ = child.start_kill();
}

fn find_git_root(start_path: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut current = if start_path.is_absolute() {
        start_path.to_path_buf()
    } else {
        cwd.join(start_path)
    };
    loop {
        // A `.git` directory or worktree pointer file is accepted, mirroring
        // `findGitRoot`/`isGitRepository` in the Node implementation.
        if current.join(".git").exists() {
            return Some(current);
        }
        let parent = current.parent()?;
        if parent == current {
            return None;
        }
        current = parent.to_path_buf();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::ffi::OsStr;
    use std::sync::Mutex;

    #[derive(Default)]
    struct ScriptedRunner {
        responses: Mutex<VecDeque<Option<String>>>,
        requests: Mutex<Vec<GitCommandRequest>>,
    }

    impl ScriptedRunner {
        fn new(responses: impl IntoIterator<Item = Option<&'static str>>) -> Self {
            Self {
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|value| value.map(str::to_owned))
                        .collect(),
                ),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn requests(&self) -> Vec<GitCommandRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl GitCommandRunner for ScriptedRunner {
        async fn run(&self, request: GitCommandRequest) -> Option<String> {
            self.requests.lock().unwrap().push(request);
            self.responses.lock().unwrap().pop_front().flatten()
        }
    }

    fn temporary_repo(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "canopy-team-sync-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join(".git")).unwrap();
        root
    }

    fn response(value: &'static str) -> Option<&'static str> {
        Some(value)
    }

    fn arguments(request: &GitCommandRequest) -> Vec<String> {
        request
            .args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn sync_opts() -> TeamMemorySyncOptions {
        TeamMemorySyncOptions {
            message: "sync team memory".to_owned(),
            author: None,
        }
    }

    #[tokio::test]
    async fn missing_repository_skips_before_running_git() {
        let root = std::env::temp_dir().join(format!("canopy-no-git-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let runner = ScriptedRunner::new([None]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert_eq!(
            result.skipped_reason,
            Some(TeamMemorySyncSkippedReason::NotAGitRepo)
        );
        assert!(runner.requests().is_empty());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn detached_head_stops_before_upstream_and_mutations() {
        let root = temporary_repo("detached");
        let runner = ScriptedRunner::new([None]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert_eq!(
            result.skipped_reason,
            Some(TeamMemorySyncSkippedReason::DetachedHead)
        );
        assert_eq!(runner.requests().len(), 1);
        assert_eq!(
            arguments(&runner.requests()[0]),
            ["symbolic-ref", "--quiet", "--short", "HEAD"]
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn pull_failure_happens_before_status_or_staging() {
        let root = temporary_repo("pull-fails");
        let runner = ScriptedRunner::new([
            response("main\n"),
            response("origin/main\n"),
            response(""),
            None,
        ]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert_eq!(
            result.skipped_reason,
            Some(TeamMemorySyncSkippedReason::PullFailed)
        );
        assert!(!result.committed);
        assert_eq!(runner.requests().len(), 4);
        assert_eq!(arguments(&runner.requests()[3]), ["pull", "--ff-only"]);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn successful_flow_commits_and_pushes_only_team_root_explicitly() {
        let root = temporary_repo("happy");
        let runner = ScriptedRunner::new([
            response("main\n"),
            response("origin/main\n"),
            response(""),
            response(""), // ff-only pull
            response(" M .canopy/team-memory/feedback.md\n"),
            response(""), // add
            response(""), // commit
            response("origin\n"),
            response("refs/heads/main\n"),
            response(""), // push
        ]);
        let environment = TeamMemorySyncEnvironment {
            git_ssh_command: Some(OsString::from("ssh -i /custom/key")),
        };

        let result = sync_team_memory_with(&root, sync_opts(), &runner, &environment).await;

        assert_eq!(
            result,
            TeamMemorySyncResult {
                committed: true,
                pulled: true,
                pushed: true,
                skipped_reason: None,
            }
        );
        let requests = runner.requests();
        let args = requests.iter().map(arguments).collect::<Vec<_>>();
        assert_eq!(args[2], ["rev-list", "@{u}..HEAD"]);
        assert_eq!(args[3], ["pull", "--ff-only"]);
        assert_eq!(
            args[4],
            ["status", "--porcelain", "--", ".canopy/team-memory"]
        );
        assert_eq!(args[5], ["add", "--", ".canopy/team-memory"]);
        assert_eq!(
            args[6],
            [
                "commit",
                "-m",
                "sync team memory",
                "--",
                ".canopy/team-memory"
            ]
        );
        assert_eq!(args[9], ["push", "--", "origin", "HEAD:refs/heads/main"]);
        assert!(args.iter().all(
            |command| command.first().map(String::as_str) != Some("push") || command.len() == 4
        ));
        for request in requests {
            assert_eq!(request.timeout, GIT_TIMEOUT);
            assert_eq!(
                request.env.get(OsStr::new("GIT_TERMINAL_PROMPT")),
                Some(&OsString::from("0"))
            );
            assert_eq!(
                request.env.get(OsStr::new("GIT_SSH_COMMAND")),
                Some(&OsString::from(
                    "ssh -i /custom/key -oBatchMode=yes -oConnectTimeout=5"
                ))
            );
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn no_upstream_still_commits_locally_and_skips_push() {
        let root = temporary_repo("no-upstream");
        let runner = ScriptedRunner::new([
            response("main\n"),
            None,
            response(" M .canopy/team-memory/a.md\n"),
            response(""),
            response(""),
        ]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert!(result.committed);
        assert!(!result.pulled && !result.pushed);
        assert_eq!(
            result.skipped_reason,
            Some(TeamMemorySyncSkippedReason::NoUpstream)
        );
        assert!(
            runner
                .requests()
                .iter()
                .all(|request| arguments(request).first().map(String::as_str) != Some("push"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn preexisting_ahead_commit_is_never_pushed() {
        let root = temporary_repo("ahead");
        let runner = ScriptedRunner::new([
            response("main\n"),
            response("origin/main\n"),
            response("unrelated-commit\n"),
            response(""),
            response(" M .canopy/team-memory/a.md\n"),
            response(""),
            response(""),
        ]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert!(result.committed);
        assert!(result.pulled);
        assert!(!result.pushed);
        assert_eq!(
            result.skipped_reason,
            Some(TeamMemorySyncSkippedReason::LocalAhead)
        );
        assert!(
            runner
                .requests()
                .iter()
                .all(|request| arguments(request).first().map(String::as_str) != Some("push"))
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn failed_commit_unstages_only_the_team_path() {
        let root = temporary_repo("commit-fails");
        let runner = ScriptedRunner::new([
            response("main\n"),
            response("origin/main\n"),
            response(""),
            response(""),
            response(" M .canopy/team-memory/a.md\n"),
            response(""), // add succeeded
            None,         // commit failed
            response(""), // reset
        ]);

        let result = sync_team_memory_with(
            &root,
            sync_opts(),
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert!(!result.committed);
        assert_eq!(
            arguments(&runner.requests()[7]),
            ["reset", "--quiet", "--", ".canopy/team-memory"]
        );
        assert!(
            runner
                .requests()
                .iter()
                .all(|request| arguments(request).first().map(String::as_str) != Some("push"))
        );
        assert_eq!(
            runner.requests()[5].termination,
            GitTerminationBehavior::Graceful
        );
        assert_eq!(
            runner.requests()[6].termination,
            GitTerminationBehavior::Graceful
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn author_falls_back_to_noreply_email_and_remote_ref_are_separate_args() {
        let root = temporary_repo("author");
        let runner = ScriptedRunner::new([
            response("main\n"),
            response("origin/main\n"),
            response(""),
            response(""),
            response(" M .canopy/team-memory/a.md\n"),
            response(""),
            response(""),
            response("origin\n"),
            response("refs/heads/main\n"),
            response(""),
        ]);
        let options = TeamMemorySyncOptions {
            message: "sync".into(),
            author: Some(TeamMemoryGitAuthor {
                name: "Memory Writer".into(),
                email: None,
            }),
        };

        let result = sync_team_memory_with(
            &root,
            options,
            &runner,
            &TeamMemorySyncEnvironment::default(),
        )
        .await;

        assert!(result.pushed);
        assert_eq!(
            arguments(&runner.requests()[6]),
            [
                "commit",
                "-m",
                "sync",
                "--author",
                "Memory Writer <Memory Writer@users.noreply>",
                "--",
                ".canopy/team-memory"
            ]
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
