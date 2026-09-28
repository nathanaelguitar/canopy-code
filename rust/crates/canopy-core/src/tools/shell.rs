use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::services::attribution_trailer::build_git_notes_command;
use crate::services::background_shell_registry::{
    BackgroundShellRegistry, BackgroundShellStatus, ShellTask, ShellTaskRegistration,
};
use crate::services::commit_attribution::CommitAttributionService;
use crate::services::commit_attribution_git::{
    CommitAttributionGitConfig, CommittedFileInfoProjection, project_captured_commit_file_info,
};
use crate::utils::cancellation::{CancellationReason, CancellationToken};
use crate::utils::sanitize_child_env::{INTERNAL_SECRET_ENV_VARS, sanitize_child_env};

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 600_000;
const MAX_CAPTURE_BYTES_PER_STREAM: usize = 32 * 1024;
const TERMINATION_GRACE: Duration = Duration::from_millis(250);
const PIPE_DRAIN_GRACE: Duration = Duration::from_millis(500);
const LEGACY_PRIVATE_ACP_CAPABILITY_ENV: &str = "CANOPY_PRIVATE_ACP_CAPABILITY";
const GIT_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const GIT_NOTES_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_GIT_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

type CaptureTask = JoinHandle<io::Result<()>>;
type CaptureTaskPair = (CaptureTask, CaptureTask);

struct ForegroundProcessContext<'a> {
    command_text: &'a str,
    cwd: &'a Path,
    process_id: Option<u32>,
}

#[derive(Clone, Debug)]
struct CommitHookContext {
    pre_head: Option<String>,
    is_amend: bool,
}

#[derive(Debug)]
struct GitCapture {
    status: ExitStatus,
    stdout: Vec<u8>,
    overflowed: bool,
}

#[derive(Clone, Debug, Default)]
struct GitCommitContext {
    has_commit: bool,
    backgrounded_commit: bool,
    attributable_segment: Option<std::ops::Range<usize>>,
    is_amend: bool,
}

#[derive(Clone, Debug)]
struct ShellCommandSegment {
    range: std::ops::Range<usize>,
    backgrounded: bool,
}

fn split_shell_command(command: &str) -> Option<Vec<ShellCommandSegment>> {
    let characters = command.char_indices().collect::<Vec<_>>();
    let mut segments = Vec::new();
    let mut segment_start = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut in_comment = false;
    let mut index = 0;

    while index < characters.len() {
        let (byte_index, character) = characters[index];
        if in_comment {
            if character == '\n' {
                segment_start = byte_index + character.len_utf8();
                in_comment = false;
            }
            index += 1;
            continue;
        }
        if escaped {
            escaped = false;
            index += 1;
            continue;
        }
        if let Some(active_quote) = quote {
            if character == active_quote {
                quote = None;
            } else if character == '\\' && active_quote == '"' {
                escaped = true;
            }
            index += 1;
            continue;
        }
        if character == '\\' {
            escaped = true;
            index += 1;
            continue;
        }
        if matches!(character, '\'' | '"') {
            quote = Some(character);
            index += 1;
            continue;
        }
        if character == '#'
            && (byte_index == segment_start
                || command[..byte_index]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_whitespace))
        {
            push_shell_segment(&mut segments, command, segment_start..byte_index, false);
            in_comment = true;
            index += 1;
            continue;
        }
        if matches!(character, ';' | '|' | '&' | '\n') {
            let repeated = characters
                .get(index + 1)
                .is_some_and(|(_, next)| *next == character)
                && matches!(character, '|' | '&');
            push_shell_segment(
                &mut segments,
                command,
                segment_start..byte_index,
                character == '&' && !repeated,
            );
            index += if repeated { 2 } else { 1 };
            segment_start = characters
                .get(index)
                .map_or(command.len(), |(next, _)| *next);
            continue;
        }
        index += 1;
    }
    if escaped || quote.is_some() {
        return None;
    }
    push_shell_segment(&mut segments, command, segment_start..command.len(), false);
    Some(segments)
}

fn push_shell_segment(
    segments: &mut Vec<ShellCommandSegment>,
    command: &str,
    range: std::ops::Range<usize>,
    backgrounded: bool,
) {
    let segment = &command[range.clone()];
    let leading = segment.len() - segment.trim_start().len();
    let trimmed_len = segment.trim().len();
    if trimmed_len > 0 {
        segments.push(ShellCommandSegment {
            range: range.start + leading..range.start + leading + trimmed_len,
            backgrounded,
        });
    }
}

fn shell_words(command: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in command.chars() {
        if escaped {
            word.push(character);
            escaped = false;
            started = true;
            continue;
        }
        match (quote, character) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), _) => word.push(character),
            (Some('"'), '\\') => escaped = true,
            (Some(_), _) => word.push(character),
            (None, '\\') => {
                escaped = true;
                started = true;
            }
            (None, '\'' | '"') => {
                quote = Some(character);
                started = true;
            }
            (None, character) if character.is_whitespace() => {
                if started {
                    words.push(std::mem::take(&mut word));
                    started = false;
                }
            }
            (None, _) => {
                word.push(character);
                started = true;
            }
        }
    }
    if escaped || quote.is_some() {
        return None;
    }
    if started {
        words.push(word);
    }
    Some(words)
}

fn analyze_git_commit_command(command: &str) -> GitCommitContext {
    let Some(segments) = split_shell_command(command) else {
        return GitCommitContext::default();
    };
    let mut context = GitCommitContext::default();
    let mut cwd_shifted = false;
    for segment in segments {
        let text = &command[segment.range.clone()];
        let Some(words) = shell_words(text) else {
            continue;
        };
        let Some(program) = words.first().map(String::as_str) else {
            continue;
        };
        match program {
            "cd" | "pushd" => {
                if !context.has_commit && cd_may_change_repo(&words) {
                    cwd_shifted = true;
                }
                continue;
            }
            "popd" => {
                if !context.has_commit {
                    cwd_shifted = true;
                }
                continue;
            }
            _ => {}
        }
        if Path::new(program)
            .file_name()
            .and_then(|name| name.to_str())
            != Some("git")
        {
            continue;
        }

        let (subcommand, changes_repo, subcommand_index) = parse_git_subcommand(&words);
        if subcommand.as_deref() == Some("commit") {
            context.has_commit = true;
            context.backgrounded_commit |= segment.backgrounded;
            if context.attributable_segment.is_none()
                && !cwd_shifted
                && !changes_repo
                && !segment.backgrounded
            {
                context.is_amend = commit_has_amend(&words, subcommand_index + 1);
                context.attributable_segment = Some(segment.range);
            }
        } else if changes_repo && !context.has_commit {
            cwd_shifted = true;
        }
    }
    context
}

fn parse_git_subcommand(words: &[String]) -> (Option<String>, bool, usize) {
    let mut index = 1;
    let mut changes_repo = false;
    while let Some(word) = words.get(index) {
        if matches!(word.as_str(), "-C" | "--git-dir" | "--work-tree") {
            changes_repo = true;
            index += 2;
            continue;
        }
        if word.starts_with("--git-dir=") || word.starts_with("--work-tree=") {
            changes_repo = true;
            index += 1;
            continue;
        }
        if word.starts_with("-C") && word.len() > 2 {
            changes_repo = true;
            index += 1;
            continue;
        }
        if matches!(
            word.as_str(),
            "-c" | "--config-env" | "--exec-path" | "--namespace"
        ) {
            index += 2;
            continue;
        }
        if word.starts_with("--config-env=")
            || word.starts_with("--exec-path=")
            || word.starts_with("--namespace=")
        {
            index += 1;
            continue;
        }
        if word.starts_with('-') {
            index += 1;
            continue;
        }
        return (Some(word.clone()), changes_repo, index);
    }
    (None, changes_repo, words.len())
}

fn cd_may_change_repo(words: &[String]) -> bool {
    let target = words.iter().skip(1).find(|word| !word.starts_with('-'));
    let Some(target) = target else {
        return true;
    };
    target != "." && target != "./"
}

fn segment_has_redirection(segment: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    for character in segment.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if let Some(active_quote) = quote {
            if character == active_quote {
                quote = None;
            } else if character == '\\' && active_quote == '"' {
                escaped = true;
            }
            continue;
        }
        match character {
            '\\' => escaped = true,
            '\'' | '"' => quote = Some(character),
            '<' | '>' => return true,
            _ => {}
        }
    }
    false
}

fn has_inline_commit_message(words: &[String], mut index: usize) -> bool {
    while let Some(word) = words.get(index) {
        if word == "--" {
            return false;
        }
        if matches!(word.as_str(), "-m" | "--message") {
            return words.get(index + 1).is_some();
        }
        if let Some(message) = word.strip_prefix("--message=") {
            return !message.is_empty();
        }
        if word.starts_with('-') && !word.starts_with("--") && word[1..].contains('m') {
            return word.len() > 2 || words.get(index + 1).is_some();
        }
        if commit_option_takes_value(word) {
            index += 2;
            continue;
        }
        index += 1;
    }
    false
}

fn already_has_coauthor_trailer(
    words: &[String],
    mut index: usize,
    name: &str,
    email: &str,
) -> bool {
    let configured = format!("Co-authored-by: {name} <{email}>");
    let lower_configured = configured.to_ascii_lowercase();
    while let Some(word) = words.get(index) {
        if word == "--" {
            break;
        }
        if matches!(word.as_str(), "-m" | "--message") {
            if words
                .get(index + 1)
                .is_some_and(|message| message.to_ascii_lowercase().contains(&lower_configured))
            {
                return true;
            }
            index += 2;
            continue;
        }
        if let Some(message) = word.strip_prefix("--message=")
            && message.to_ascii_lowercase().contains(&lower_configured)
        {
            return true;
        }
        if word.starts_with('-') && !word.starts_with("--") && word[1..].contains('m') {
            let message_flag = word[1..].find('m').unwrap_or_default();
            let message_start = message_flag + 2;
            let attached = word.get(message_start..).unwrap_or_default();
            if attached.to_ascii_lowercase().contains(&lower_configured)
                || attached.is_empty()
                    && words.get(index + 1).is_some_and(|message| {
                        message.to_ascii_lowercase().contains(&lower_configured)
                    })
            {
                return true;
            }
        }
        if commit_option_takes_value(word) {
            index += 2;
            continue;
        }
        index += 1;
    }
    false
}

fn commit_option_takes_value(word: &str) -> bool {
    matches!(
        word,
        "-F" | "--file"
            | "--author"
            | "--date"
            | "--cleanup"
            | "--trailer"
            | "--reuse-message"
            | "-C"
            | "--reedit-message"
            | "-c"
    )
}

fn commit_has_amend(words: &[String], mut index: usize) -> bool {
    while let Some(word) = words.get(index) {
        if word == "--" {
            return false;
        }
        if matches!(word.as_str(), "-m" | "--message" | "-F" | "--file") {
            index += 2;
            continue;
        }
        if word == "--amend" || word.starts_with("--amend=") {
            return true;
        }
        if word.starts_with('-') && !word.starts_with("--") {
            let short_flags = &word[1..];
            if short_flags.contains('a') {
                return true;
            }
            if let Some(message_flag) = short_flags.find('m') {
                let message_value_is_attached = message_flag + 1 < short_flags.len();
                index += if message_value_is_attached { 1 } else { 2 };
                continue;
            }
        }
        index += 1;
    }
    false
}

fn environment_redirects_git_repo(environment: &HashMap<String, String>) -> bool {
    ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR"]
        .iter()
        .any(|key| environment.get(*key).is_some_and(|value| !value.is_empty()))
}

fn append_coauthor_trailer(
    command: &str,
    context: &GitCommitContext,
    config: Option<&CommitAttributionGitConfig>,
) -> Option<String> {
    let config = config?;
    let segment_range = context.attributable_segment.as_ref()?;
    let segment = command.get(segment_range.clone())?;
    if segment_has_redirection(segment) {
        return None;
    }
    let words = shell_words(segment)?;
    let (_, _, subcommand_index) = parse_git_subcommand(&words);
    let commit_args_index = subcommand_index.checked_add(1)?;
    if !has_inline_commit_message(&words, commit_args_index)
        || already_has_coauthor_trailer(
            &words,
            commit_args_index,
            &config.coauthor_name,
            &config.coauthor_email,
        )
    {
        return None;
    }
    let trailer = format!(
        "Co-authored-by: {} <{}>",
        config.coauthor_name, config.coauthor_email
    );
    let quoted = format!("'{}'", trailer.replace('\'', "'\"'\"'"));
    let insertion = find_unquoted_separator(segment)
        .map(|offset| segment_range.start + offset)
        .unwrap_or(segment_range.end);
    let mut result = String::with_capacity(command.len() + quoted.len() + 4);
    result.push_str(&command[..insertion]);
    result.push_str(" -m ");
    result.push_str(&quoted);
    result.push_str(&command[insertion..]);
    Some(result)
}

fn find_unquoted_separator(segment: &str) -> Option<usize> {
    let characters = segment.char_indices().collect::<Vec<_>>();
    let mut quote = None;
    let mut escaped = false;
    for (index, (byte_index, character)) in characters.iter().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        if let Some(active_quote) = quote {
            if *character == active_quote {
                quote = None;
            } else if *character == '\\' && active_quote == '"' {
                escaped = true;
            }
            continue;
        }
        match *character {
            '\\' => escaped = true,
            '\'' | '"' => quote = Some(*character),
            '-' if characters
                .get(index + 1)
                .is_some_and(|(_, next)| *next == '-') =>
            {
                let is_token_start = *byte_index == 0
                    || segment[..*byte_index]
                        .chars()
                        .next_back()
                        .is_some_and(char::is_whitespace);
                let is_token_end = characters
                    .get(index + 2)
                    .is_none_or(|(_, next)| next.is_whitespace());
                if is_token_start && is_token_end {
                    return Some(*byte_index);
                }
                // Do not scan the second dash as the start of another token.
                if is_token_start {
                    continue;
                }
            }
            _ => {}
        }
    }
    None
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackgroundShellTaskSummary {
    pub id: String,
    pub command: String,
    pub cwd: String,
    pub pid: Option<u32>,
    pub status: BackgroundShellStatus,
    pub start_time: i64,
    pub end_time: Option<i64>,
    pub exit_code: Option<i32>,
    pub error: Option<String>,
    pub output_file: String,
}

impl From<&ShellTask> for BackgroundShellTaskSummary {
    fn from(task: &ShellTask) -> Self {
        Self {
            id: task.id.clone(),
            command: task.command.clone(),
            cwd: task.cwd.clone(),
            pid: task.pid,
            status: task.status,
            start_time: task.start_time,
            end_time: task.end_time,
            exit_code: task.exit_code,
            error: task.error.clone(),
            output_file: task.output_file.clone(),
        }
    }
}

pub struct ShellTool {
    workspace_root: PathBuf,
    environment: HashMap<String, String>,
    project_temp_dir: PathBuf,
    session_id: String,
    background_registry: Arc<Mutex<BackgroundShellRegistry>>,
    commit_attribution: Option<Arc<Mutex<CommitAttributionService>>>,
    commit_attribution_config: Option<CommitAttributionGitConfig>,
}

impl ShellTool {
    pub fn new(workspace_root: impl AsRef<Path>) -> Result<Self, String> {
        let environment = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
            .collect();
        Self::new_with_env(workspace_root, environment)
    }

    /// Construct a shell tool with a stable environment snapshot. Settings
    /// loaders can pass `.env` overlays here without mutating process-global
    /// state from a multithreaded Rust runtime.
    pub fn new_with_env(
        workspace_root: impl AsRef<Path>,
        environment: HashMap<String, String>,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }

        let mut digest = Sha256::new();
        digest.update(workspace_root.to_string_lossy().as_bytes());
        let project_key = format!("{:x}", digest.finalize());
        let project_temp_dir = std::env::temp_dir()
            .join("canopy")
            .join("projects")
            .join(&project_key[..24])
            .join("tmp");
        let session_id = Uuid::new_v4().simple().to_string();
        Self::new_with_env_and_session(workspace_root, environment, project_temp_dir, session_id)
    }

    /// Construct a shell tool with the same project-temp/session layout used
    /// by the TypeScript shell tool. `project_temp_dir` is the storage
    /// provider's project temp directory, and `session_id` scopes output and
    /// sidecars to one session.
    pub fn new_with_env_and_session(
        workspace_root: impl AsRef<Path>,
        environment: HashMap<String, String>,
        project_temp_dir: impl AsRef<Path>,
        session_id: impl Into<String>,
    ) -> Result<Self, String> {
        let workspace_root = std::fs::canonicalize(workspace_root.as_ref())
            .map_err(|error| format!("could not resolve workspace root: {error}"))?;
        if !workspace_root.is_dir() {
            return Err("workspace root is not a directory".to_owned());
        }
        let session_id = session_id.into();
        if session_id.is_empty()
            || !session_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("session id contains unsupported path characters".to_owned());
        }

        Ok(Self {
            workspace_root,
            environment,
            project_temp_dir: project_temp_dir.as_ref().to_path_buf(),
            session_id,
            background_registry: Arc::new(Mutex::new(BackgroundShellRegistry::new())),
            commit_attribution: None,
            commit_attribution_config: None,
        })
    }

    /// Attach the session-owned attribution service and merged Git settings.
    /// The host passes this same service to file tools so shell commits update
    /// the state that the transcript snapshot hook persists.
    pub fn with_commit_attribution(
        mut self,
        service: Arc<Mutex<CommitAttributionService>>,
        config: CommitAttributionGitConfig,
    ) -> Self {
        self.commit_attribution = Some(service);
        self.commit_attribution_config = Some(config);
        self
    }

    /// Run a shell command. The default API is foreground-only with respect
    /// to cancellation; callers that expose a promote control can use
    /// [`execute_with_cancellation`](Self::execute_with_cancellation) and
    /// cancel its token with reason `"background"`.
    pub async fn execute(&self, args: &Value) -> Result<String, String> {
        self.execute_with_cancellation(args, CancellationToken::new())
            .await
    }

    /// Run a command with a caller-owned cancellation token. Cancelling with
    /// reason `"background"` transfers a live foreground process into the
    /// managed background registry and returns immediately. Any other
    /// cancellation kills the process tree.
    pub async fn execute_with_cancellation(
        &self,
        args: &Value,
        cancellation: CancellationToken,
    ) -> Result<String, String> {
        let command_text = args
            .get("command")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|command| !command.is_empty())
            .ok_or_else(|| "Command cannot be empty.".to_owned())?;
        if command_text.len() > 64 * 1024 {
            return Err("Shell command exceeds the 64 KiB input limit.".to_owned());
        }
        if cancellation.is_cancelled() {
            return Ok("Command was cancelled by user before it could start.".to_owned());
        }

        let timeout_ms = match args.get("timeout") {
            None => DEFAULT_TIMEOUT_MS,
            Some(value) => {
                let timeout = value.as_u64().ok_or_else(|| {
                    "Timeout must be a positive integer number of milliseconds.".to_owned()
                })?;
                if timeout == 0 {
                    return Err("Timeout must be a positive number.".to_owned());
                }
                if timeout > MAX_TIMEOUT_MS {
                    return Err(format!(
                        "Timeout cannot exceed {MAX_TIMEOUT_MS}ms (10 minutes)."
                    ));
                }
                timeout
            }
        };
        let cwd = self.resolve_directory(args.get("directory"))?;
        let is_background = args
            .get("is_background")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if is_background && strip_trailing_background_amp(command_text) != command_text {
            return Err("Background shell commands must not end with a bare '&'. Remove the trailing '&' and rely on is_background: true instead.".to_owned());
        }
        let command_text = if is_background {
            strip_trailing_background_amp(command_text)
        } else {
            command_text
        };

        let git_context = analyze_git_commit_command(command_text);
        if git_context.backgrounded_commit {
            return Err(
                "Refusing to run a backgrounded `git commit`: commit attribution requires foreground completion."
                    .to_owned(),
            );
        }
        if is_background && git_context.has_commit {
            return Err(
                "Refusing to run `git commit` in background mode: commit attribution requires foreground completion."
                    .to_owned(),
            );
        }

        if is_background {
            self.run_background_command(command_text, &cwd).await
        } else {
            self.run_foreground_command(
                command_text,
                &cwd,
                Duration::from_millis(timeout_ms),
                cancellation,
            )
            .await
        }
    }

    /// Access the per-tool background registry for task listings and UI
    /// subscriptions. Task handles remain synchronized and may outlive this
    /// borrow, but the registry itself is shared only with this tool instance.
    pub fn background_registry(&self) -> Arc<Mutex<BackgroundShellRegistry>> {
        Arc::clone(&self.background_registry)
    }

    /// Snapshot the retained shell tasks in registry insertion order.
    pub fn background_shell_tasks(&self) -> Vec<BackgroundShellTaskSummary> {
        let entries = lock(&self.background_registry).get_all();
        entries
            .iter()
            .map(|entry| {
                let task = lock(entry);
                BackgroundShellTaskSummary::from(&*task)
            })
            .collect()
    }

    pub fn background_shell_task(&self, shell_id: &str) -> Option<BackgroundShellTaskSummary> {
        let entry = lock(&self.background_registry).get(shell_id)?;
        let task = lock(&entry);
        Some(BackgroundShellTaskSummary::from(&*task))
    }

    /// Request cancellation and let the supervisor publish the real terminal
    /// status after it has killed the process tree and drained its output.
    /// Returns `None` when the id is unknown and `Some(false)` if it is
    /// already terminal.
    pub fn request_cancel_background_shell(&self, shell_id: &str) -> Option<bool> {
        let registry = lock(&self.background_registry);
        let entry = registry.get(shell_id)?;
        if lock(&entry).status != BackgroundShellStatus::Running {
            return Some(false);
        }
        registry.request_cancel(shell_id);
        Some(true)
    }

    /// Cancel a managed process tree and mark its task cancelled immediately.
    /// Returns false if the task does not exist or is already terminal.
    pub fn cancel_background_shell(&self, shell_id: &str) -> bool {
        let mut registry = lock(&self.background_registry);
        let Some(entry) = registry.get(shell_id) else {
            return false;
        };
        if lock(&entry).status != BackgroundShellStatus::Running {
            return false;
        }
        registry.cancel(shell_id, current_time_millis());
        true
    }

    /// Cancel all active background tasks. Used when the owning session is
    /// shutting down; natural command completion remains independent of turns.
    pub fn abort_all_background_shells(&self) {
        lock(&self.background_registry).abort_all();
    }

    fn resolve_directory(&self, value: Option<&Value>) -> Result<PathBuf, String> {
        let Some(value) = value else {
            return Ok(self.workspace_root.clone());
        };
        let raw = value
            .as_str()
            .filter(|path| !path.trim().is_empty())
            .ok_or_else(|| "Directory must be a non-empty absolute path.".to_owned())?;
        let requested = Path::new(raw);
        if !requested.is_absolute() {
            return Err("Directory must be an absolute path.".to_owned());
        }
        let canonical = std::fs::canonicalize(requested)
            .map_err(|error| format!("could not resolve command directory: {error}"))?;
        if !canonical.starts_with(&self.workspace_root) {
            return Err(
                "Shell commands are restricted to directories inside the workspace.".to_owned(),
            );
        }
        if !canonical.is_dir() {
            return Err(format!(
                "Command directory is not a directory: {}",
                canonical.display()
            ));
        }
        Ok(canonical)
    }

    async fn git_capture(
        &self,
        cwd: &Path,
        args: &[String],
        timeout: Duration,
    ) -> Option<GitCapture> {
        let environment = shell_child_environment(&self.environment);
        let mut child = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .envs(&environment)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .ok()?;
        let stdout = child.stdout.take()?;
        let mut capture_task = tokio::spawn(read_git_output_bounded(stdout, MAX_GIT_CAPTURE_BYTES));

        let status = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(Ok(status)) => status,
            _ => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = tokio::time::timeout(PIPE_DRAIN_GRACE, &mut capture_task).await;
                capture_task.abort();
                return None;
            }
        };
        let captured = tokio::time::timeout(PIPE_DRAIN_GRACE, &mut capture_task).await;
        let (stdout, overflowed) = match captured {
            Ok(Ok(Ok(capture))) => capture,
            _ => {
                capture_task.abort();
                return None;
            }
        };
        Some(GitCapture {
            status,
            stdout,
            overflowed,
        })
    }

    async fn git_stdout(&self, cwd: &Path, args: &[String], timeout: Duration) -> Option<Vec<u8>> {
        let capture = self.git_capture(cwd, args, timeout).await?;
        (capture.status.success() && !capture.overflowed).then_some(capture.stdout)
    }

    async fn git_head(&self, cwd: &Path) -> Option<String> {
        let output = self
            .git_stdout(
                cwd,
                &["rev-parse".to_owned(), "HEAD".to_owned()],
                GIT_PROBE_TIMEOUT,
            )
            .await?;
        let head = String::from_utf8_lossy(&output).trim().to_owned();
        (!head.is_empty()).then_some(head)
    }

    async fn committed_file_info(
        &self,
        cwd: &Path,
        post_head: &str,
        pre_head: Option<&str>,
        is_amend: bool,
    ) -> Result<CommittedFileInfoProjection, String> {
        let root_output = self
            .git_stdout(
                cwd,
                &["rev-parse".to_owned(), "--show-toplevel".to_owned()],
                GIT_PROBE_TIMEOUT,
            )
            .await
            .ok_or_else(|| "could not resolve the git repository root".to_owned())?;
        let repo_root = String::from_utf8_lossy(&root_output).trim().to_owned();
        if repo_root.is_empty() {
            return Err("git returned an empty repository root".to_owned());
        }

        let (name_args, status_args, numstat_args) = if is_amend {
            let pre_head = pre_head
                .filter(|head| !head.is_empty())
                .ok_or_else(|| "cannot analyze --amend without the pre-amend HEAD".to_owned())?;
            let pre_head_args = vec![
                "rev-parse".to_owned(),
                "--verify".to_owned(),
                pre_head.to_owned(),
            ];
            if self
                .git_stdout(cwd, &pre_head_args, GIT_PROBE_TIMEOUT)
                .await
                .is_none()
            {
                return Err("the pre-amend commit is no longer available".to_owned());
            }
            (
                vec![
                    "diff".to_owned(),
                    "--find-renames".to_owned(),
                    "--name-only".to_owned(),
                    pre_head.to_owned(),
                    post_head.to_owned(),
                ],
                vec![
                    "diff".to_owned(),
                    "--find-renames".to_owned(),
                    "--name-status".to_owned(),
                    pre_head.to_owned(),
                    post_head.to_owned(),
                ],
                vec![
                    "diff".to_owned(),
                    "--find-renames".to_owned(),
                    "--numstat".to_owned(),
                    pre_head.to_owned(),
                    post_head.to_owned(),
                ],
            )
        } else {
            let parent_probe = format!("{post_head}~1");
            let parent_probe_args = vec![
                "rev-parse".to_owned(),
                "--verify".to_owned(),
                parent_probe.clone(),
            ];
            let parent_output = self
                .git_stdout(
                    cwd,
                    &[
                        "log".to_owned(),
                        "-1".to_owned(),
                        "--pretty=%P".to_owned(),
                        post_head.to_owned(),
                    ],
                    GIT_PROBE_TIMEOUT,
                )
                .await
                .ok_or_else(|| "could not inspect the committed revision metadata".to_owned())?;
            let has_parent = self
                .git_stdout(cwd, &parent_probe_args, GIT_PROBE_TIMEOUT)
                .await
                .is_some();
            let parent_text = String::from_utf8_lossy(&parent_output);
            let is_true_root_commit = parent_text.trim().is_empty();
            if !has_parent && !is_true_root_commit {
                return Err("the commit parent is unavailable (shallow repository)".to_owned());
            }

            if has_parent {
                (
                    vec![
                        "diff".to_owned(),
                        "--find-renames".to_owned(),
                        "--name-only".to_owned(),
                        parent_probe.clone(),
                        post_head.to_owned(),
                    ],
                    vec![
                        "diff".to_owned(),
                        "--find-renames".to_owned(),
                        "--name-status".to_owned(),
                        parent_probe.clone(),
                        post_head.to_owned(),
                    ],
                    vec![
                        "diff".to_owned(),
                        "--find-renames".to_owned(),
                        "--numstat".to_owned(),
                        parent_probe,
                        post_head.to_owned(),
                    ],
                )
            } else {
                (
                    vec![
                        "diff-tree".to_owned(),
                        "--root".to_owned(),
                        "--find-renames".to_owned(),
                        "--no-commit-id".to_owned(),
                        "-r".to_owned(),
                        "--name-only".to_owned(),
                        post_head.to_owned(),
                    ],
                    vec![
                        "diff-tree".to_owned(),
                        "--root".to_owned(),
                        "--find-renames".to_owned(),
                        "--no-commit-id".to_owned(),
                        "-r".to_owned(),
                        "--name-status".to_owned(),
                        post_head.to_owned(),
                    ],
                    vec![
                        "diff-tree".to_owned(),
                        "--root".to_owned(),
                        "--find-renames".to_owned(),
                        "--no-commit-id".to_owned(),
                        "-r".to_owned(),
                        "--numstat".to_owned(),
                        post_head.to_owned(),
                    ],
                )
            }
        };

        let (name_output, status_output, numstat_output) = tokio::join!(
            self.git_stdout(cwd, &name_args, GIT_PROBE_TIMEOUT),
            self.git_stdout(cwd, &status_args, GIT_PROBE_TIMEOUT),
            self.git_stdout(cwd, &numstat_args, GIT_PROBE_TIMEOUT),
        );
        let name_output = name_output.map(|output| String::from_utf8_lossy(&output).into_owned());
        let status_output =
            status_output.map(|output| String::from_utf8_lossy(&output).into_owned());
        let numstat_output =
            numstat_output.map(|output| String::from_utf8_lossy(&output).into_owned());
        project_captured_commit_file_info(
            name_output.as_deref(),
            status_output.as_deref(),
            numstat_output.as_deref(),
            Some(&repo_root),
        )
        .map_err(|error| error.to_string())
    }

    async fn attach_commit_attribution(
        &self,
        cwd: &Path,
        hook: &CommitHookContext,
    ) -> Option<String> {
        let service = self.commit_attribution.as_ref()?;
        let config = self.commit_attribution_config.as_ref()?;
        let post_head = self.git_head(cwd).await?;
        if hook.pre_head.as_deref() == Some(post_head.as_str()) {
            return None;
        }

        let count_args = if let Some(pre_head) = hook.pre_head.as_deref() {
            vec![
                "rev-list".to_owned(),
                "--count".to_owned(),
                format!("{pre_head}..{post_head}"),
            ]
        } else {
            vec![
                "rev-list".to_owned(),
                "--count".to_owned(),
                post_head.clone(),
            ]
        };
        let commit_count = self
            .git_stdout(cwd, &count_args, GIT_PROBE_TIMEOUT)
            .await
            .and_then(|output| String::from_utf8_lossy(&output).trim().parse::<u64>().ok());
        if commit_count != Some(1) {
            lock(service).note_commit_without_clearing();
            return None;
        }
        if !lock(service).has_attributions() {
            lock(service).note_commit_without_clearing();
            return None;
        }

        let staged_info = match self
            .committed_file_info(cwd, &post_head, hook.pre_head.as_deref(), hook.is_amend)
            .await
        {
            Ok(CommittedFileInfoProjection::Files(info)) => info,
            Ok(CommittedFileInfoProjection::EmptyCommit) => {
                lock(service).note_commit_without_clearing();
                return None;
            }
            Err(error) => {
                lock(service).note_commit_without_clearing();
                return Some(format!(
                    "AI attribution note skipped: could not analyze the commit diff ({error})."
                ));
            }
        };
        let base_dir = staged_info
            .repo_root
            .clone()
            .unwrap_or_else(|| cwd.to_path_buf());
        let canonical_base = std::fs::canonicalize(&base_dir).unwrap_or(base_dir);
        {
            lock(service).apply_committed_renames(&staged_info.renamed_files, &canonical_base);
        }
        let committed_scope =
            lock(service).match_committed_files(&staged_info.files, &canonical_base);

        let mut committed_blobs = HashMap::new();
        for absolute_path in &committed_scope {
            let Ok(relative_path) = absolute_path.strip_prefix(&canonical_base) else {
                continue;
            };
            let relative_path = relative_path.to_string_lossy().replace('\\', "/");
            if relative_path.is_empty() || relative_path.starts_with("../") {
                continue;
            }
            let Some(blob) = self
                .git_stdout(
                    cwd,
                    &["show".to_owned(), format!("{post_head}:{relative_path}")],
                    GIT_PROBE_TIMEOUT,
                )
                .await
            else {
                continue;
            };
            committed_blobs.insert(
                absolute_path.clone(),
                String::from_utf8_lossy(&blob).into_owned(),
            );
        }
        lock(service).validate_against(|path| committed_blobs.get(path).cloned());

        let committed_paths =
            lock(service).match_committed_files(&staged_info.files, &canonical_base);
        if committed_paths.is_empty() {
            lock(service).note_commit_without_clearing();
            return None;
        }
        if !config.commit_enabled {
            lock(service).clear_attributed_files(&committed_paths);
            return None;
        }

        let note = lock(service).generate_note_payload(
            &staged_info,
            &canonical_base,
            config.generator_name.as_deref(),
        );
        let note = match serde_json::to_value(note) {
            Ok(note) => note,
            Err(error) => {
                lock(service).note_commit_without_clearing();
                return Some(format!(
                    "AI attribution note skipped: could not encode note ({error})."
                ));
            }
        };
        let Some(notes_command) = build_git_notes_command(&note, &post_head) else {
            lock(service).note_commit_without_clearing();
            return Some(
                "AI attribution note skipped: payload exceeded the 30 KiB size cap.".to_owned(),
            );
        };
        let notes_args = notes_command.args;
        let notes_result = self.git_capture(cwd, &notes_args, GIT_NOTES_TIMEOUT).await;
        if !notes_result.is_some_and(|capture| capture.status.success() && !capture.overflowed) {
            lock(service).note_commit_without_clearing();
            return Some(
                "AI attribution note skipped: git notes add failed or timed out.".to_owned(),
            );
        }
        lock(service).clear_attributed_files(&committed_paths);
        None
    }

    fn supports_commit_trailer_rewrite(&self) -> bool {
        #[cfg(unix)]
        {
            let shell = self
                .environment
                .get("SHELL")
                .map(String::as_str)
                .unwrap_or("/bin/zsh");
            return matches!(
                Path::new(shell).file_name().and_then(|name| name.to_str()),
                Some("bash" | "zsh" | "sh")
            );
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    async fn run_foreground_command(
        &self,
        command_text: &str,
        cwd: &Path,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<String, String> {
        let git_context = analyze_git_commit_command(command_text);
        let can_attribute = git_context.attributable_segment.is_some()
            && self.commit_attribution.is_some()
            && !environment_redirects_git_repo(&self.environment);
        let hook = if can_attribute {
            Some(CommitHookContext {
                pre_head: self.git_head(cwd).await,
                is_amend: git_context.is_amend,
            })
        } else {
            None
        };
        let command_to_execute = if can_attribute
            && self
                .commit_attribution_config
                .as_ref()
                .is_some_and(|config| config.commit_enabled)
            && self.supports_commit_trailer_rewrite()
        {
            append_coauthor_trailer(
                command_text,
                &git_context,
                self.commit_attribution_config.as_ref(),
            )
            .unwrap_or_else(|| command_text.to_owned())
        } else {
            command_text.to_owned()
        };
        let mut child = self.spawn_command(&command_to_execute, cwd)?;
        let process_id = child.id();
        let (stdout_task, stderr_task, capture) = start_capture_tasks(&mut child, None).await?;

        enum ForegroundEvent {
            Exited(io::Result<ExitStatus>),
            Cancelled(Option<CancellationReason>),
            TimedOut,
        }
        let event = tokio::select! {
            result = child.wait() => ForegroundEvent::Exited(result),
            reason = cancellation.cancelled() => ForegroundEvent::Cancelled(reason),
            _ = tokio::time::sleep(timeout) => ForegroundEvent::TimedOut,
        };

        let execution_result = match event {
            ForegroundEvent::Exited(Ok(status)) => {
                let (stdout_capture, stderr_capture) =
                    join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await?;
                finish_output_file(&capture).await;
                Ok(format_foreground_output(
                    &stdout_capture,
                    &stderr_capture,
                    Some(status),
                    None,
                    false,
                ))
            }
            ForegroundEvent::Exited(Err(error)) => {
                let _ = terminate_and_wait(&mut child, process_id).await;
                let _ = join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await;
                finish_output_file(&capture).await;
                Err(format!("could not wait for shell command: {error}"))
            }
            ForegroundEvent::TimedOut => {
                let status = terminate_and_wait(&mut child, process_id)
                    .await
                    .map_err(|error| format!("could not reap timed-out shell command: {error}"))?;
                let (stdout_capture, stderr_capture) =
                    join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await?;
                finish_output_file(&capture).await;
                Ok(format_foreground_output(
                    &stdout_capture,
                    &stderr_capture,
                    Some(status),
                    Some(timeout),
                    false,
                ))
            }
            ForegroundEvent::Cancelled(reason) => {
                let requested_background = matches!(
                    reason,
                    Some(CancellationReason::Explicit(ref value)) if value.as_ref() == "background"
                );
                if requested_background && !git_context.has_commit {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            let (stdout_capture, stderr_capture) =
                                join_capture_tasks(stdout_task, stderr_task, process_id, &capture)
                                    .await?;
                            finish_output_file(&capture).await;
                            Ok(format_foreground_output(
                                &stdout_capture,
                                &stderr_capture,
                                Some(status),
                                None,
                                false,
                            ))
                        }
                        Ok(None) => {
                            return self
                                .promote_foreground_process(
                                    child,
                                    stdout_task,
                                    stderr_task,
                                    capture,
                                    ForegroundProcessContext {
                                        command_text: &command_to_execute,
                                        cwd,
                                        process_id,
                                    },
                                )
                                .await;
                        }
                        Err(error) => {
                            let _ = terminate_and_wait(&mut child, process_id).await;
                            let _ =
                                join_capture_tasks(stdout_task, stderr_task, process_id, &capture)
                                    .await;
                            Err(format!(
                                "could not check shell process before promotion: {error}"
                            ))
                        }
                    }
                } else {
                    let status =
                        terminate_and_wait(&mut child, process_id)
                            .await
                            .map_err(|error| {
                                format!("could not reap cancelled shell command: {error}")
                            })?;
                    let (stdout_capture, stderr_capture) =
                        join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await?;
                    finish_output_file(&capture).await;
                    Ok(format_foreground_output(
                        &stdout_capture,
                        &stderr_capture,
                        Some(status),
                        None,
                        true,
                    ))
                }
            }
        };

        let attribution_warning = if let Some(hook) = hook.as_ref() {
            self.attach_commit_attribution(cwd, hook).await
        } else {
            None
        };
        match execution_result {
            Ok(mut output) => {
                if let Some(warning) = attribution_warning {
                    output.push_str("\n\n");
                    output.push_str(&warning);
                }
                Ok(output)
            }
            Err(error) => Err(error),
        }
    }

    async fn run_background_command(
        &self,
        command_text: &str,
        cwd: &Path,
    ) -> Result<String, String> {
        let (shell_id, output_path) = self.create_background_output_path()?;
        let output_file = match create_private_output_file(&output_path) {
            Ok(file) => tokio::fs::File::from_std(file),
            Err(error) => return Err(format!("could not create shell output file: {error}")),
        };
        let mut child = match self.spawn_command(command_text, cwd) {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_file(&output_path);
                return Err(error);
            }
        };
        let process_id = child.id();
        let cancellation = CancellationToken::new();
        let capture = Arc::new(AsyncMutex::new(OutputCapture::with_file(output_file)));
        let (stdout_task, stderr_task) =
            match start_capture_tasks_with_capture(&mut child, &capture) {
                Ok(tasks) => tasks,
                Err(error) => {
                    let _ = terminate_and_wait(&mut child, process_id).await;
                    let _ = fs::remove_file(&output_path);
                    return Err(error);
                }
            };

        let registration = ShellTaskRegistration {
            shell_id: shell_id.clone(),
            command: command_text.to_owned(),
            cwd: cwd.to_string_lossy().into_owned(),
            pid: process_id,
            status: BackgroundShellStatus::Running,
            start_time: current_time_millis(),
            end_time: None,
            exit_code: None,
            error: None,
            output_path: output_path.to_string_lossy().into_owned(),
            todo_work_chain_id: None,
            abort_controller: cancellation.clone(),
        };
        lock(&self.background_registry).register(registration);

        tokio::spawn(supervise_background_process(
            child,
            stdout_task,
            stderr_task,
            capture,
            Arc::clone(&self.background_registry),
            shell_id.clone(),
            cancellation,
        ));

        Ok(background_started_message(
            &shell_id,
            process_id,
            &output_path,
        ))
    }

    async fn promote_foreground_process(
        &self,
        mut child: Child,
        stdout_task: CaptureTask,
        stderr_task: CaptureTask,
        capture: Arc<AsyncMutex<OutputCapture>>,
        context: ForegroundProcessContext<'_>,
    ) -> Result<String, String> {
        let ForegroundProcessContext {
            command_text,
            cwd,
            process_id,
        } = context;
        let (shell_id, output_path) = match self.create_background_output_path() {
            Ok(paths) => paths,
            Err(error) => {
                let _ = terminate_and_wait(&mut child, process_id).await;
                let _ = join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await;
                return Err(error);
            }
        };
        let output_file = match create_private_output_file(&output_path) {
            Ok(file) => tokio::fs::File::from_std(file),
            Err(error) => {
                let _ = terminate_and_wait(&mut child, process_id).await;
                let _ = join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await;
                return Err(format!(
                    "could not create promoted shell output file: {error}"
                ));
            }
        };
        let Some(process_id) = process_id else {
            let _ = terminate_and_wait(&mut child, None).await;
            let _ = join_capture_tasks(stdout_task, stderr_task, None, &capture).await;
            let _ = fs::remove_file(&output_path);
            return Err("could not promote a shell process without a pid".to_owned());
        };

        let cancellation = CancellationToken::new();
        let promotion_write = {
            // Capture readers take this lock for each chunk. Writing the
            // snapshot and switching the sink under the same lock prevents
            // bytes from being lost or reordered at the handoff boundary.
            let mut state = capture.lock().await;
            let snapshot = state.snapshot_for_file();
            let mut output_file = output_file;
            match output_file.write_all(snapshot.as_bytes()).await {
                Err(error) => Err(format!("could not write promoted shell snapshot: {error}")),
                Ok(()) => match output_file.flush().await {
                    Err(error) => Err(format!("could not flush promoted shell snapshot: {error}")),
                    Ok(()) => {
                        state.output_file = Some(output_file);
                        Ok(())
                    }
                },
            }
        };
        if let Err(error) = promotion_write {
            let _ = terminate_and_wait(&mut child, Some(process_id)).await;
            let _ = join_capture_tasks(stdout_task, stderr_task, Some(process_id), &capture).await;
            let _ = fs::remove_file(&output_path);
            return Err(error);
        }

        let registration = ShellTaskRegistration {
            shell_id: shell_id.clone(),
            command: command_text.to_owned(),
            cwd: cwd.to_string_lossy().into_owned(),
            pid: Some(process_id),
            status: BackgroundShellStatus::Running,
            start_time: current_time_millis(),
            end_time: None,
            exit_code: None,
            error: None,
            output_path: output_path.to_string_lossy().into_owned(),
            todo_work_chain_id: None,
            // The promotion token has already fired. Background ownership
            // needs a new token so task-stop can kill the transferred child.
            abort_controller: cancellation.clone(),
        };
        lock(&self.background_registry).register(registration);

        tokio::spawn(supervise_background_process(
            child,
            stdout_task,
            stderr_task,
            capture,
            Arc::clone(&self.background_registry),
            shell_id.clone(),
            cancellation,
        ));

        Ok(format!(
            "Foreground shell promoted to background.\n{}\nOutput snapshot saved at promotion time.",
            background_started_message(&shell_id, Some(process_id), &output_path)
        ))
    }

    fn create_background_output_path(&self) -> Result<(String, PathBuf), String> {
        let output_dir = self
            .project_temp_dir
            .join("background-shells")
            .join(&self.session_id);
        fs::create_dir_all(&output_dir)
            .map_err(|error| format!("could not create background shell directory: {error}"))?;
        let directory_metadata = fs::symlink_metadata(&output_dir)
            .map_err(|error| format!("could not inspect background shell directory: {error}"))?;
        if directory_metadata.file_type().is_symlink() || !directory_metadata.is_dir() {
            return Err("background shell directory must be a real directory".to_owned());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&output_dir, fs::Permissions::from_mode(0o700))
                .map_err(|error| format!("could not secure background shell directory: {error}"))?;
        }
        let shell_id = format!("bg_{}", &Uuid::new_v4().simple().to_string()[..8]);
        let output_path = output_dir.join(format!("shell-{shell_id}.output"));
        Ok((shell_id, output_path))
    }

    fn spawn_command(&self, command_text: &str, cwd: &Path) -> Result<Child, String> {
        let environment = shell_child_environment(&self.environment);
        let mut command = shell_command(command_text, &environment);
        command
            .current_dir(cwd)
            .envs(&environment)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for key in INTERNAL_SECRET_ENV_VARS
            .iter()
            .copied()
            .chain(std::iter::once(LEGACY_PRIVATE_ACP_CAPABILITY_ENV))
        {
            command.env_remove(key);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        command
            .spawn()
            .map_err(|error| format!("could not start shell command: {error}"))
    }
}

impl Drop for ShellTool {
    fn drop(&mut self) {
        let entries = {
            let registry = lock(&self.background_registry);
            registry.get_all()
        };
        let pids = entries
            .iter()
            .filter_map(|entry| {
                let task = lock(entry);
                (task.status == BackgroundShellStatus::Running)
                    .then_some(task.pid)
                    .flatten()
            })
            .collect::<Vec<_>>();
        lock(&self.background_registry).abort_all();
        for process_id in pids {
            terminate_process_group(Some(process_id), false);
        }
    }
}

#[derive(Clone, Copy)]
enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Default)]
struct AnsiStripper {
    state: AnsiState,
}

#[derive(Default, Clone, Copy)]
enum AnsiState {
    #[default]
    Text,
    Escape,
    Csi,
    Osc,
    OscEscape,
    String,
    StringEscape,
}

impl AnsiStripper {
    fn push(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut visible = Vec::with_capacity(bytes.len());
        for byte in bytes.iter().copied() {
            self.state = match self.state {
                AnsiState::Text if byte == 0x1b => AnsiState::Escape,
                AnsiState::Text => {
                    visible.push(byte);
                    AnsiState::Text
                }
                AnsiState::Escape => match byte {
                    b'[' => AnsiState::Csi,
                    b']' => AnsiState::Osc,
                    b'P' | b'X' | b'^' | b'_' => AnsiState::String,
                    0x40..=0x5f => AnsiState::Text,
                    _ => AnsiState::Text,
                },
                AnsiState::Csi if (0x40..=0x7e).contains(&byte) => AnsiState::Text,
                AnsiState::Csi => AnsiState::Csi,
                AnsiState::Osc if byte == 0x07 => AnsiState::Text,
                AnsiState::Osc if byte == 0x1b => AnsiState::OscEscape,
                AnsiState::Osc => AnsiState::Osc,
                AnsiState::OscEscape if byte == b'\\' => AnsiState::Text,
                AnsiState::OscEscape if byte == 0x1b => AnsiState::OscEscape,
                AnsiState::OscEscape => AnsiState::Osc,
                AnsiState::String if byte == 0x1b => AnsiState::StringEscape,
                AnsiState::String => AnsiState::String,
                AnsiState::StringEscape if byte == b'\\' => AnsiState::Text,
                AnsiState::StringEscape if byte == 0x1b => AnsiState::StringEscape,
                AnsiState::StringEscape => AnsiState::String,
            };
        }
        visible
    }
}

struct OutputCapture {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    output_file: Option<tokio::fs::File>,
    output_file_error: Option<String>,
    stdout_ansi: AnsiStripper,
    stderr_ansi: AnsiStripper,
}

impl OutputCapture {
    fn new(output_file: Option<tokio::fs::File>) -> Self {
        Self {
            stdout: Vec::with_capacity(MAX_CAPTURE_BYTES_PER_STREAM),
            stderr: Vec::with_capacity(MAX_CAPTURE_BYTES_PER_STREAM),
            stdout_truncated: false,
            stderr_truncated: false,
            output_file,
            output_file_error: None,
            stdout_ansi: AnsiStripper::default(),
            stderr_ansi: AnsiStripper::default(),
        }
    }

    fn with_file(output_file: tokio::fs::File) -> Self {
        Self::new(Some(output_file))
    }

    fn capture_memory(&mut self, stream: OutputStream, bytes: &[u8]) {
        let (target, truncated) = match stream {
            OutputStream::Stdout => (&mut self.stdout, &mut self.stdout_truncated),
            OutputStream::Stderr => (&mut self.stderr, &mut self.stderr_truncated),
        };
        let remaining = MAX_CAPTURE_BYTES_PER_STREAM.saturating_sub(target.len());
        let captured = bytes.len().min(remaining);
        target.extend_from_slice(&bytes[..captured]);
        *truncated |= captured < bytes.len();
    }

    fn snapshot_for_file(&self) -> String {
        let mut stdout_ansi = AnsiStripper::default();
        let mut stderr_ansi = AnsiStripper::default();
        let stdout = String::from_utf8_lossy(&stdout_ansi.push(&self.stdout)).into_owned();
        let stderr = String::from_utf8_lossy(&stderr_ansi.push(&self.stderr)).into_owned();
        combine_stream_text(&stdout, &stderr)
    }
}

async fn start_capture_tasks(
    child: &mut Child,
    output_file: Option<tokio::fs::File>,
) -> Result<
    (
        JoinHandle<io::Result<()>>,
        JoinHandle<io::Result<()>>,
        Arc<AsyncMutex<OutputCapture>>,
    ),
    String,
> {
    let capture = Arc::new(AsyncMutex::new(OutputCapture::new(output_file)));
    let (stdout_task, stderr_task) = start_capture_tasks_with_capture(child, &capture)?;
    Ok((stdout_task, stderr_task, capture))
}

fn start_capture_tasks_with_capture(
    child: &mut Child,
    capture: &Arc<AsyncMutex<OutputCapture>>,
) -> Result<CaptureTaskPair, String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "shell stdout pipe was unavailable".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "shell stderr pipe was unavailable".to_owned())?;
    Ok((
        tokio::spawn(capture_output(
            stdout,
            OutputStream::Stdout,
            Arc::clone(capture),
        )),
        tokio::spawn(capture_output(
            stderr,
            OutputStream::Stderr,
            Arc::clone(capture),
        )),
    ))
}

async fn capture_output<R: AsyncRead + Unpin>(
    mut reader: R,
    stream: OutputStream,
    capture: Arc<AsyncMutex<OutputCapture>>,
) -> io::Result<()> {
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        let mut state = capture.lock().await;
        state.capture_memory(stream, &buffer[..read]);
        let visible = match stream {
            OutputStream::Stdout => state.stdout_ansi.push(&buffer[..read]),
            OutputStream::Stderr => state.stderr_ansi.push(&buffer[..read]),
        };
        if state.output_file_error.is_some() {
            continue;
        }
        let write_result = if let Some(file) = state.output_file.as_mut() {
            file.write_all(&visible).await
        } else {
            Ok(())
        };
        if let Err(error) = write_result {
            state.output_file_error = Some(error.to_string());
            state.output_file.take();
        }
    }
}

async fn read_git_output_bounded<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
) -> io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::with_capacity(limit.min(64 * 1024));
    let mut overflowed = false;
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            return Ok((output, overflowed));
        }
        let remaining = limit.saturating_sub(output.len());
        let retained = read.min(remaining);
        output.extend_from_slice(&buffer[..retained]);
        overflowed |= retained < read;
    }
}

async fn join_capture_tasks(
    stdout_task: JoinHandle<io::Result<()>>,
    stderr_task: JoinHandle<io::Result<()>>,
    process_id: Option<u32>,
    capture: &Arc<AsyncMutex<OutputCapture>>,
) -> Result<(CapturedOutput, CapturedOutput), String> {
    let stdout = join_capture(stdout_task, process_id).await;
    let stderr = join_capture(stderr_task, process_id).await;
    match (stdout, stderr) {
        (Ok(()), Ok(())) => {
            let state = capture.lock().await;
            let mut stdout_ansi = AnsiStripper::default();
            let mut stderr_ansi = AnsiStripper::default();
            let stdout = CapturedOutput {
                text: String::from_utf8_lossy(&stdout_ansi.push(&state.stdout)).into_owned(),
                truncated: state.stdout_truncated,
            };
            let stderr = CapturedOutput {
                text: String::from_utf8_lossy(&stderr_ansi.push(&state.stderr)).into_owned(),
                truncated: state.stderr_truncated,
            };
            Ok((stdout, stderr))
        }
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

struct CapturedOutput {
    text: String,
    truncated: bool,
}

async fn join_capture(
    mut task: JoinHandle<io::Result<()>>,
    process_id: Option<u32>,
) -> Result<(), String> {
    let result = match tokio::time::timeout(PIPE_DRAIN_GRACE, &mut task).await {
        Ok(joined) => joined,
        Err(_) => {
            terminate_process_group_async(process_id, false).await;
            match tokio::time::timeout(TERMINATION_GRACE, &mut task).await {
                Ok(joined) => joined,
                Err(_) => {
                    terminate_process_group_async(process_id, true).await;
                    match tokio::time::timeout(PIPE_DRAIN_GRACE, &mut task).await {
                        Ok(joined) => joined,
                        Err(_) => {
                            task.abort();
                            return Err("shell command left a child process holding its output pipe open; its process group was terminated".to_owned());
                        }
                    }
                }
            }
        }
    };
    result
        .map_err(|error| format!("shell output reader failed: {error}"))?
        .map_err(|error| format!("could not read shell output: {error}"))?;
    Ok(())
}

async fn finish_output_file(capture: &Arc<AsyncMutex<OutputCapture>>) {
    let mut state = capture.lock().await;
    if let Some(file) = state.output_file.as_mut() {
        let _ = file.flush().await;
    }
    state.output_file.take();
}

async fn supervise_background_process(
    mut child: Child,
    stdout_task: JoinHandle<io::Result<()>>,
    stderr_task: JoinHandle<io::Result<()>>,
    capture: Arc<AsyncMutex<OutputCapture>>,
    registry: Arc<Mutex<BackgroundShellRegistry>>,
    shell_id: String,
    cancellation: CancellationToken,
) {
    let process_id = child.id();
    enum BackgroundEvent {
        Exited(io::Result<ExitStatus>),
        Cancelled,
    }
    let event = tokio::select! {
        result = child.wait() => BackgroundEvent::Exited(result),
        _ = cancellation.cancelled() => BackgroundEvent::Cancelled,
    };
    let exit = match event {
        BackgroundEvent::Exited(result) => (result, false),
        BackgroundEvent::Cancelled => (terminate_and_wait(&mut child, process_id).await, true),
    };
    let capture_result = join_capture_tasks(stdout_task, stderr_task, process_id, &capture).await;
    finish_output_file(&capture).await;
    let end_time = current_time_millis();
    let mut registry = lock(&registry);
    if exit.1 {
        registry.cancel(&shell_id, end_time);
        return;
    }
    match (exit.0, capture_result) {
        (Err(error), _) => registry.fail(&shell_id, error.to_string(), end_time),
        (Ok(_status), Err(error)) => registry.fail(&shell_id, error, end_time),
        (Ok(status), Ok(_)) if status.success() => {
            registry.complete(&shell_id, status.code().unwrap_or(0), end_time)
        }
        (Ok(status), Ok(_)) => registry.fail(&shell_id, describe_exit_status(status), end_time),
    }
}

async fn terminate_and_wait(child: &mut Child, process_id: Option<u32>) -> io::Result<ExitStatus> {
    terminate_process_group_async(process_id, false).await;
    match tokio::time::timeout(TERMINATION_GRACE, child.wait()).await {
        Ok(result) => result,
        Err(_) => {
            terminate_process_group_async(process_id, true).await;
            #[cfg(windows)]
            let _ = child.start_kill();
            child.wait().await
        }
    }
}

fn format_foreground_output(
    stdout: &CapturedOutput,
    stderr: &CapturedOutput,
    status: Option<ExitStatus>,
    timeout: Option<Duration>,
    cancelled: bool,
) -> String {
    let output = combine_stream_text(&stdout.text, &stderr.text);
    let mut output = output;
    if cancelled {
        output.push_str("\n[command cancelled]");
    } else if let Some(timeout) = timeout {
        output.push_str(&format!(
            "\n[command timed out after {} ms]",
            timeout.as_millis()
        ));
    } else if let Some(status) = status {
        output.push_str(&format!(
            "\n[exit code: {}]",
            status
                .code()
                .map_or_else(|| "signal".to_owned(), |code| code.to_string())
        ));
    }
    if stdout.truncated || stderr.truncated {
        output.push_str("\n[output truncated at the Rust shell tool limit]");
    }
    output
}

fn combine_stream_text(stdout: &str, stderr: &str) -> String {
    let mut output = stdout.to_owned();
    if !stderr.is_empty() {
        if !output.is_empty() && !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str("[stderr]\n");
        output.push_str(stderr);
    }
    output
}

fn background_started_message(
    shell_id: &str,
    process_id: Option<u32>,
    output_path: &Path,
) -> String {
    let status_path = crate::services::background_shell_registry::status_file_path_for(
        &output_path.to_string_lossy(),
    );
    let pid_line = process_id.map_or_else(String::new, |pid| format!("pid: {pid}\n"));
    format!(
        "Background shell started.\nid: {shell_id}\n{pid_line}output file: {}\nstatus file: {status_path}\nRead the status file for liveness and the output file for captured text. Stop it with ShellTool::cancel_background_shell(\"{shell_id}\").",
        output_path.display()
    )
}

fn strip_trailing_background_amp(command: &str) -> &str {
    let trimmed = command.trim_end();
    let Some(without_amp) = trimmed.strip_suffix('&') else {
        return command;
    };
    if without_amp.ends_with('&') || without_amp.ends_with('\\') {
        return command;
    }
    without_amp.trim_end()
}

fn create_private_output_file(path: &Path) -> io::Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn terminate_process_group(process_id: Option<u32>, force: bool) {
    let Some(process_id) = process_id else {
        return;
    };
    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;
        let signal = if force {
            Signal::SIGKILL
        } else {
            Signal::SIGTERM
        };
        let _ = killpg(Pid::from_raw(process_id as i32), signal);
    }
    #[cfg(windows)]
    {
        let _ = force;
        let _ = process_id;
    }
    #[cfg(not(any(unix, windows)))]
    let _ = (process_id, force);
}

async fn terminate_process_group_async(process_id: Option<u32>, force: bool) {
    let Some(process_id) = process_id else {
        return;
    };
    #[cfg(unix)]
    terminate_process_group(Some(process_id), force);
    #[cfg(windows)]
    if force {
        let _ = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await;
    }
    #[cfg(not(any(unix, windows)))]
    let _ = (process_id, force);
}

fn describe_exit_status(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        format!("exited with code {code}")
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return format!("terminated by signal {signal}");
            }
        }
        "terminated by signal".to_owned()
    }
}

fn shell_command(command: &str, environment: &HashMap<String, String>) -> Command {
    #[cfg(unix)]
    {
        let configured = environment.get("SHELL").map(PathBuf::from);
        let executable = configured
            .filter(|path| path.is_absolute() && path.is_file())
            .unwrap_or_else(|| PathBuf::from("/bin/zsh"));
        let mut process = Command::new(executable);
        process.arg("-lc").arg(command);
        process
    }
    #[cfg(windows)]
    {
        let executable = environment
            .get("COMSPEC")
            .map(PathBuf::from)
            .unwrap_or_else(|| "cmd.exe".into());
        let mut process = Command::new(executable);
        process.arg("/C").arg(command);
        process
    }
    #[cfg(not(any(unix, windows)))]
    {
        let mut process = Command::new("sh");
        process.arg("-c").arg(command);
        process
    }
}

fn shell_child_environment(environment: &HashMap<String, String>) -> HashMap<String, String> {
    let mut sanitized = sanitize_child_env(environment);
    // Preserve the shell's pre-existing compatibility scrub for older
    // Canopy ACP launchers; this key is deliberately outside the shared list.
    sanitized.remove(LEGACY_PRIVATE_ACP_CAPABILITY_ENV);
    sanitized
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn function_declaration() -> Value {
    json!({
        "name":"run_shell_command",
        "description":"Run a shell command in the current workspace. The CLI shows the exact command and requires approval first. Output is bounded; foreground timeout defaults to 120 seconds and cannot exceed 10 minutes. Set is_background to true to start a managed background process.",
        "parameters":{
            "type":"OBJECT",
            "properties":{
                "command":{"type":"STRING","description":"Exact shell command to execute."},
                "timeout":{"type":"INTEGER","description":"Optional foreground timeout in milliseconds, at most 600000."},
                "directory":{"type":"STRING","description":"Optional absolute working directory inside the current workspace."},
                "description":{"type":"STRING","description":"Short explanation of the command."},
                "is_background":{"type":"BOOLEAN","description":"Start the process as a managed background shell and return its id and output/status paths."}
            },
            "required":["command"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TempWorkspace(PathBuf);

    impl TempWorkspace {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("canopy-shell-{}", Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn task_status(tool: &ShellTool, id: &str) -> Option<BackgroundShellStatus> {
        let entry = lock(&tool.background_registry).get(id)?;
        let status = lock(&entry).status;
        Some(status)
    }

    fn shell_id(output: &str) -> String {
        output
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap()
            .to_owned()
    }

    #[test]
    fn background_mode_strips_only_a_bare_trailing_ampersand() {
        assert_eq!(strip_trailing_background_amp("sleep 1 &  "), "sleep 1");
        assert_eq!(strip_trailing_background_amp("echo &&"), "echo &&");
        assert_eq!(strip_trailing_background_amp(r"printf \&"), r"printf \&");
    }

    #[test]
    fn shell_child_environment_removes_internal_and_legacy_secrets_only() {
        let source = HashMap::from([
            ("QWEN_SERVER_TOKEN".to_owned(), "serve-secret".to_owned()),
            ("QWEN_DAEMON_TOKEN".to_owned(), "daemon-secret".to_owned()),
            (
                "QWEN_CODE_PRIVATE_ACP_CAPABILITY".to_owned(),
                "private-capability".to_owned(),
            ),
            (
                LEGACY_PRIVATE_ACP_CAPABILITY_ENV.to_owned(),
                "legacy-capability".to_owned(),
            ),
            ("GH_TOKEN".to_owned(), "gh-token".to_owned()),
            ("AWS_ACCESS_KEY_ID".to_owned(), "aws-key".to_owned()),
            ("NPM_TOKEN".to_owned(), "npm-token".to_owned()),
        ]);
        let child_environment = shell_child_environment(&source);

        for key in INTERNAL_SECRET_ENV_VARS
            .iter()
            .copied()
            .chain(std::iter::once(LEGACY_PRIVATE_ACP_CAPABILITY_ENV))
        {
            assert!(!child_environment.contains_key(key));
            assert!(source.contains_key(key));
        }
        assert_eq!(child_environment["GH_TOKEN"], "gh-token");
        assert_eq!(child_environment["AWS_ACCESS_KEY_ID"], "aws-key");
        assert_eq!(child_environment["NPM_TOKEN"], "npm-token");
    }

    #[tokio::test]
    async fn background_commands_reject_a_bare_trailing_ampersand() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new(&workspace.0).unwrap();
        let error = tool
            .execute(&json!({"command":"sleep 1 &","is_background":true}))
            .await
            .unwrap_err();
        assert!(error.contains("must not end with a bare '&'"));
    }

    #[tokio::test]
    async fn runs_foreground_commands_in_the_workspace_and_reports_status() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new(&workspace.0).unwrap();
        let output = tool
            .execute(&json!({"command":"printf canopy-shell-ok"}))
            .await
            .unwrap();
        assert!(output.contains("canopy-shell-ok"));
        assert!(output.contains("[exit code: 0]"));
    }

    #[tokio::test]
    async fn caps_output_rejects_external_directories_and_kills_timed_out_groups() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new(&workspace.0).unwrap();
        let output = tool
            .execute(&json!({"command":"printf '%0100000d' 0"}))
            .await
            .unwrap();
        assert!(output.len() < 40 * 1024);
        assert!(output.contains("output truncated"));

        assert!(
            tool.execute(&json!({"command":"pwd","directory":"/"}))
                .await
                .unwrap_err()
                .contains("inside the workspace")
        );
        let timeout = tool
            .execute(&json!({"command":"sleep 2","timeout":10}))
            .await
            .unwrap();
        assert!(timeout.contains("timed out"));
    }

    #[tokio::test]
    async fn starts_background_shell_and_persists_output_and_terminal_status() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new_with_env_and_session(
            &workspace.0,
            HashMap::new(),
            workspace.0.join("temp"),
            "session-1",
        )
        .unwrap();
        let result = tool
            .execute(&json!({
                "command":"printf '\\033[31mfirst\\033[0m'; sleep 0.05; printf second",
                "is_background":true
            }))
            .await
            .unwrap();
        let id = shell_id(&result);
        assert!(result.contains("status file:"));
        assert!(result.contains("output file:"));

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if task_status(&tool, &id)
                    .is_some_and(|status| status != BackgroundShellStatus::Running)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let entry = lock(&tool.background_registry).get(&id).unwrap();
        let task = lock(&entry);
        assert_eq!(task.status, BackgroundShellStatus::Completed);
        let output = fs::read_to_string(&task.output_file).unwrap();
        assert_eq!(output, "firstsecond");
        let status_path =
            crate::services::background_shell_registry::status_file_path_for(&task.output_file);
        let status: Value = serde_json::from_slice(&fs::read(status_path).unwrap()).unwrap();
        assert_eq!(status["status"], "completed");
    }

    #[tokio::test]
    async fn nonzero_background_exit_transitions_to_failed() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new(&workspace.0).unwrap();
        let result = tool
            .execute(&json!({"command":"exit 9","is_background":true}))
            .await
            .unwrap();
        let id = shell_id(&result);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if task_status(&tool, &id) == Some(BackgroundShellStatus::Failed) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let entry = lock(&tool.background_registry).get(&id).unwrap();
        let task = lock(&entry);
        assert_eq!(task.error.as_deref(), Some("exited with code 9"));
    }

    #[tokio::test]
    async fn cancellation_stops_background_process_tree_and_marks_task_cancelled() {
        let workspace = TempWorkspace::new();
        let tool = ShellTool::new(&workspace.0).unwrap();
        let result = tool
            .execute(&json!({"command":"sleep 30","is_background":true}))
            .await
            .unwrap();
        let id = shell_id(&result);
        assert!(tool.cancel_background_shell(&id));
        assert_eq!(
            task_status(&tool, &id),
            Some(BackgroundShellStatus::Cancelled)
        );
        tokio::time::sleep(Duration::from_millis(350)).await;
        let entry = lock(&tool.background_registry).get(&id).unwrap();
        let task = lock(&entry);
        #[cfg(unix)]
        if let Some(pid) = task.pid {
            use nix::sys::signal::killpg;
            use nix::unistd::Pid;
            assert!(killpg(Pid::from_raw(pid as i32), None).is_err());
        }
    }

    #[tokio::test]
    async fn foreground_cancellation_with_background_reason_promotes_and_keeps_streaming() {
        let workspace = TempWorkspace::new();
        let tool = Arc::new(
            ShellTool::new_with_env_and_session(
                &workspace.0,
                HashMap::new(),
                workspace.0.join("temp"),
                "session-2",
            )
            .unwrap(),
        );
        let promotion = CancellationToken::new();
        let run_tool = Arc::clone(&tool);
        let run_promotion = promotion.clone();
        let run = tokio::spawn(async move {
            run_tool
                .execute_with_cancellation(
                    &json!({"command":"printf before; sleep 0.2; printf after"}),
                    run_promotion,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        promotion.cancel_with_reason("background");
        let result = tokio::time::timeout(Duration::from_secs(2), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let id = shell_id(&result);
        assert!(result.contains("promoted to background"));
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if task_status(&tool, &id) == Some(BackgroundShellStatus::Completed) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let entry = lock(&tool.background_registry).get(&id).unwrap();
        let task = lock(&entry);
        assert_eq!(
            fs::read_to_string(&task.output_file).unwrap(),
            "beforeafter"
        );
    }
}
