//! Runtime request construction for managed-memory consolidation dreams.
//!
//! Port of `packages/core/src/memory/dreamAgentPlanner.ts`. The native agent
//! adapter is injected and is responsible for applying the scoped paths and
//! read-only pinned-memory policy described in each request.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use tokio::sync::watch;

use super::paths::{AUTO_MEMORY_INDEX_FILENAME, AUTO_MEMORY_PINNED_DIRNAME, AutoMemoryPaths};
use crate::storage::Storage;

pub const MANAGED_AUTO_MEMORY_DREAM_AGENT_NAME: &str = "managed-auto-memory-dreamer";
pub const DEFAULT_DREAM_AGENT_MAX_TURNS: u32 = 8;
pub const DEFAULT_DREAM_AGENT_TIMEOUT_MINUTES: f64 = 5.0;
pub const DREAM_AGENT_TOOLS: [&str; 7] = [
    "read_file",
    "grep_search",
    "glob",
    "list_directory",
    "run_shell_command",
    "write_file",
    "edit",
];

pub const DREAM_AGENT_SYSTEM_PROMPT: &str = concat!(
    "You are performing a managed memory dream — a reflective pass over durable memory files.\n\n",
    "Synthesize what you've learned recently into durable, well-organized memories so that future sessions can orient quickly.\n\n",
    "Rules:\n",
    "- Treat files under the top-level `pinned/` directory as protected read-only records. Never modify, overwrite, rename, merge into, or delete them.\n",
    "- Leave `pinned/` out of consolidation analysis; do not list, read, or compare its files during Dream.\n",
    "- Merge semantically duplicate entries among writable topic files — if the same fact appears in multiple writable files, consolidate into one file and delete the rest.\n",
    "- Preserve all durable information; do not delete content that is still accurate.\n",
    "- Fix contradicted or stale facts only when the evidence is clear from the existing memory content or recent transcript signal.\n",
    "- Update the MEMORY.md index to accurately reflect surviving files.\n",
    "- Keep the MEMORY.md index concise: one line per file in the format `- [Title](relative/path.md) — one-line hook`.\n",
    "- If nothing needs consolidation, do nothing and say so."
);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DreamShellFlavor {
    #[default]
    Posix,
    PowerShell,
    Cmd,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DreamAgentScopedPaths {
    pub project_root: PathBuf,
    pub memory_root: PathBuf,
    pub transcript_dir: PathBuf,
    /// The memory agent may search recent session transcripts for narrow
    /// evidence, while its writable scope remains project auto-memory only.
    pub allow_transcript_reads: bool,
    pub allow_shell: bool,
    pub include_user_memory: bool,
    pub protect_pinned_memory: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DreamAgentRequest {
    pub name: &'static str,
    pub task_prompt: String,
    pub system_prompt: &'static str,
    pub max_turns: u32,
    pub max_time_minutes: f64,
    pub tools: &'static [&'static str],
    pub suppress_chat_recording: bool,
    pub scoped_paths: DreamAgentScopedPaths,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DreamAgentStatus {
    #[default]
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DreamAgentRunResult {
    pub status: DreamAgentStatus,
    pub final_text: Option<String>,
    pub files_touched: Vec<PathBuf>,
    pub terminate_reason: Option<String>,
}

pub type DreamAgentFuture<'a> =
    Pin<Box<dyn Future<Output = Result<DreamAgentRunResult, String>> + Send + 'a>>;

/// Native agent integration boundary. Implementations must enforce project
/// memory writes, project/transcript reads, pinned-file protections, and the
/// tool allowlist in the request.
pub trait DreamAgentRuntime: Send + Sync {
    fn max_turns(&self) -> Option<u32> {
        None
    }

    fn timeout_minutes(&self) -> Option<f64> {
        None
    }

    fn execute_dream_agent<'a>(
        &'a self,
        request: DreamAgentRequest,
        abort_signal: Option<watch::Receiver<bool>>,
    ) -> DreamAgentFuture<'a>;
}

pub fn get_transcript_dir(project_root: impl AsRef<Path>) -> PathBuf {
    Storage::new(project_root.as_ref().to_path_buf())
        .get_project_dir()
        .join("chats")
}

pub fn build_consolidation_task_prompt(
    memory_root: impl AsRef<Path>,
    transcript_dir: impl AsRef<Path>,
    shell: DreamShellFlavor,
) -> String {
    let memory_root = memory_root.as_ref().to_string_lossy();
    let transcript_dir = transcript_dir.as_ref();
    let display_transcript_dir = transcript_dir.to_string_lossy();
    let mut grep_dir = transcript_dir.to_string_lossy().into_owned();
    while grep_dir.ends_with('/') || grep_dir.ends_with('\\') {
        grep_dir.pop();
    }
    grep_dir.push(std::path::MAIN_SEPARATOR);
    let quoted_transcript_dir = escape_shell_arg(&grep_dir, shell);
    [
        format!("Memory directory: `{memory_root}`"),
        "This directory already exists — write to it directly with the write_file tool (do not run mkdir or check for its existence).".to_owned(),
        format!("Session transcripts: `{display_transcript_dir}` (large JSONL files — grep narrowly, don't read whole files)"),
        String::new(),
        "## Phase 1 — Orient".to_owned(),
        String::new(),
        "- List the memory directory to see what files exist".to_owned(),
        format!("- Read `{memory_root}/{AUTO_MEMORY_INDEX_FILENAME}` to understand the current index"),
        "- Skim topic subdirectories (`user/`, `project/`, `feedback/`, `reference/`)".to_owned(),
        format!("- Skip `{AUTO_MEMORY_PINNED_DIRNAME}/` during Dream; do not list or read files there"),
        "- If `logs/` or `sessions/` subdirectories exist, review recent entries there".to_owned(),
        String::new(),
        "## Phase 2 — Gather recent signal".to_owned(),
        String::new(),
        "Look for new information worth persisting. Sources in rough priority order:".to_owned(),
        String::new(),
        "1. Existing memories that drifted — facts that contradict something you now know from current memory files".to_owned(),
        "2. Transcript search — if you need specific context, grep session transcripts for narrow terms:".to_owned(),
        format!("   `grep -rn \"<narrow term>\" {quoted_transcript_dir} --include=\"*.jsonl\" | tail -50`"),
        String::new(),
        "Don't exhaustively read transcripts. Look only for things you already suspect matter.".to_owned(),
        String::new(),
        "## Phase 3 — Consolidate".to_owned(),
        String::new(),
        "For each topic directory:".to_owned(),
        "- Identify duplicate or near-duplicate `.md` files (same fact expressed differently)".to_owned(),
        "- Merge duplicates: write the canonical version into one file, delete the redundant files".to_owned(),
        format!("- Exclude `{AUTO_MEMORY_PINNED_DIRNAME}/` from duplicate, stale, and contradiction analysis; never use a pinned file as a merge target or deletion candidate"),
        "- Fix stale or contradicted facts when clear from the existing content".to_owned(),
        "- Convert relative dates (for example: \"yesterday\", \"last week\") to absolute dates when preserving them".to_owned(),
        String::new(),
        "## Phase 4 — Prune and index".to_owned(),
        String::new(),
        format!("Update `{memory_root}/{AUTO_MEMORY_INDEX_FILENAME}` to reflect surviving files."),
        "Each entry: `- [Title](relative/path.md) — one-line hook`".to_owned(),
        "Keep the index under roughly 200 lines and ~25KB.".to_owned(),
        format!("Do not intentionally remove existing index entries for valid `{AUTO_MEMORY_PINNED_DIRNAME}/` files during consolidation; normal index limits still apply."),
        "Remove pointers to deleted, stale, wrong, or superseded files. Add pointers to any newly created files.".to_owned(),
        "If an index line is too verbose, shorten it and move the detail back into the memory file itself.".to_owned(),
        String::new(),
        "---".to_owned(),
        String::new(),
        "Return a brief summary of what you consolidated, updated, or pruned. If nothing needed consolidation, say so briefly.".to_owned(),
    ]
    .join("\n")
}

pub fn build_dream_agent_request(
    paths: &AutoMemoryPaths,
    runtime: &dyn DreamAgentRuntime,
    options: DreamPlannerOptions,
) -> DreamAgentRequest {
    let project_root = paths.project_root().to_path_buf();
    let transcript_dir = get_transcript_dir(&project_root);
    DreamAgentRequest {
        name: MANAGED_AUTO_MEMORY_DREAM_AGENT_NAME,
        task_prompt: build_consolidation_task_prompt(
            paths.auto_memory_root(),
            &transcript_dir,
            options.shell,
        ),
        system_prompt: DREAM_AGENT_SYSTEM_PROMPT,
        max_turns: runtime.max_turns().unwrap_or(DEFAULT_DREAM_AGENT_MAX_TURNS),
        max_time_minutes: runtime
            .timeout_minutes()
            .unwrap_or(DEFAULT_DREAM_AGENT_TIMEOUT_MINUTES),
        tools: &DREAM_AGENT_TOOLS,
        suppress_chat_recording: options.suppress_chat_recording,
        scoped_paths: DreamAgentScopedPaths {
            project_root,
            memory_root: paths.auto_memory_root(),
            transcript_dir,
            allow_transcript_reads: true,
            allow_shell: true,
            include_user_memory: false,
            protect_pinned_memory: true,
        },
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DreamPlannerOptions {
    pub suppress_chat_recording: bool,
    pub shell: DreamShellFlavor,
}

#[derive(Debug, thiserror::Error)]
pub enum DreamPlannerError {
    #[error("Dream agent execution failed: {0}")]
    Execution(String),
    #[error("{0}")]
    Failed(String),
    #[error("{0}")]
    Cancelled(String),
}

pub async fn plan_managed_auto_memory_dream_by_agent<R: DreamAgentRuntime>(
    paths: &AutoMemoryPaths,
    runtime: &R,
    abort_signal: Option<watch::Receiver<bool>>,
    options: DreamPlannerOptions,
) -> Result<DreamAgentRunResult, DreamPlannerError> {
    let request = build_dream_agent_request(paths, runtime, options);
    let result = runtime
        .execute_dream_agent(request, abort_signal)
        .await
        .map_err(DreamPlannerError::Execution)?;
    match result.status {
        DreamAgentStatus::Failed => Err(DreamPlannerError::Failed(
            result
                .terminate_reason
                .clone()
                .unwrap_or_else(|| "Dream agent failed".to_owned()),
        )),
        DreamAgentStatus::Cancelled => Err(DreamPlannerError::Cancelled(
            result
                .terminate_reason
                .clone()
                .unwrap_or_else(|| "Dream agent cancelled before completion".to_owned()),
        )),
        DreamAgentStatus::Completed => Ok(result),
    }
}

fn escape_shell_arg(argument: &str, shell: DreamShellFlavor) -> String {
    if argument.is_empty() {
        return String::new();
    }
    match shell {
        DreamShellFlavor::PowerShell => format!("'{}'", argument.replace('\'', "''")),
        DreamShellFlavor::Cmd => format!("\"{}\"", argument.replace('"', "\"\"")),
        DreamShellFlavor::Posix => format!("'{}'", argument.replace('\'', "'\\''")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_shell_quotes_transcript_path_and_excludes_pinned() {
        let prompt = build_consolidation_task_prompt(
            "/tmp/project/memory",
            "/tmp/transcripts with 'quote'",
            DreamShellFlavor::Posix,
        );
        assert!(prompt.contains("'/tmp/transcripts with '\\''quote'\\''/' --include=\"*.jsonl\""));
        assert!(prompt.contains("Skip `pinned/` during Dream"));
        assert!(prompt.contains("normal index limits still apply"));
    }

    #[test]
    fn configured_agent_limits_are_preserved_including_zero_turn_sentinel() {
        struct Runtime;
        impl DreamAgentRuntime for Runtime {
            fn max_turns(&self) -> Option<u32> {
                Some(0)
            }
            fn timeout_minutes(&self) -> Option<f64> {
                Some(30.0)
            }
            fn execute_dream_agent<'a>(
                &'a self,
                _request: DreamAgentRequest,
                _abort_signal: Option<watch::Receiver<bool>>,
            ) -> DreamAgentFuture<'a> {
                Box::pin(async { Ok(DreamAgentRunResult::default()) })
            }
        }
        let root = std::env::temp_dir().join(format!("dream-plan-{}", uuid::Uuid::new_v4()));
        let paths = AutoMemoryPaths::new(
            &root,
            root.join("memory-base"),
            false,
            super::super::paths::MemoryProjectScope::GitRoot,
        );
        let request = build_dream_agent_request(&paths, &Runtime, DreamPlannerOptions::default());
        assert_eq!(request.max_turns, 0);
        assert_eq!(request.max_time_minutes, 30.0);
        assert!(request.scoped_paths.protect_pinned_memory);
        assert!(!request.scoped_paths.include_user_memory);
    }
}
