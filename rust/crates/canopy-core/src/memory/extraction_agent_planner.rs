//! Managed auto-memory extraction agent request construction.
//!
//! Port of `packages/core/src/memory/extractionAgentPlanner.ts`. The native
//! runtime executes the forked agent and enforces the scoped permissions
//! described by [`ExtractionAgentRequest`].

use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;

use serde_json::{Map, Value, json};
use thiserror::Error;

use super::paths::{AUTO_MEMORY_INDEX_FILENAME, AUTO_MEMORY_PINNED_DIRNAME, AutoMemoryPaths};
use super::prompt::{
    MEMORY_FRONTMATTER_EXAMPLE, TYPES_SECTION_INDIVIDUAL, WHAT_NOT_TO_SAVE_SECTION,
};
use super::scan::{
    ScannedAutoMemoryDocument, scan_auto_memory_topic_documents,
    scan_user_auto_memory_topic_documents,
};
use super::store::AutoMemoryType;

pub const MANAGED_AUTO_MEMORY_EXTRACTOR_AGENT_NAME: &str = "managed-auto-memory-extractor";
pub const DEFAULT_EXTRACTION_MAX_TURNS: u32 = 5;
pub const DEFAULT_EXTRACTION_TIMEOUT_MINUTES: u32 = 2;
pub const MAX_EXTRACTION_TOPIC_SUMMARY_UTF16_UNITS: usize = 280;
pub const EXTRACTION_AGENT_TOOLS: [&str; 7] = [
    "read_file",
    "grep_search",
    "glob",
    "list_directory",
    "run_shell_command",
    "write_file",
    "edit",
];

const EXTRACTION_AGENT_INTRO: &[&str] = &[
    "You are now acting as the managed memory extraction subagent for an AI coding assistant.",
    "",
    "The recent conversation history is already in your context. Analyze only that recent conversation and use it to update persistent managed memory.",
    "",
    "Rules:",
    "- Read existing memory files first to avoid creating duplicates.",
    "- Extract only durable facts stated by the user.",
    "- Ignore temporary, session-specific, speculative, or question content.",
    "- If the user explicitly asks the assistant to remember something durable, preserve it.",
    "- Use one of the allowed topics: user, feedback, project, reference.",
    "- Keep entries concise and suitable for bullet points. No leading bullet markers.",
    "- Do not investigate repository code, git history, or unrelated files.",
    "- Work only from the conversation history in your context and the existing memory files.",
    "- If nothing durable should be saved, make no file changes.",
    "",
];

/// Conversation content is retained as JSON so the host runtime can pass
/// provider-specific parts through without lossy conversion.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExtractionMessage {
    pub role: String,
    pub parts: Vec<Value>,
    /// Provider-specific Content fields are preserved when history is rebuilt.
    pub extra_fields: Map<String, Value>,
}

impl ExtractionMessage {
    pub fn new(role: impl Into<String>, parts: Vec<Value>) -> Self {
        Self {
            role: role.into(),
            parts,
            extra_fields: Map::new(),
        }
    }

    fn into_value(self) -> Value {
        let mut message = self.extra_fields;
        message.insert("role".to_owned(), Value::String(self.role));
        message.insert("parts".to_owned(), Value::Array(self.parts));
        Value::Object(message)
    }
}

/// Build valid fork history, dropping a trailing user turn and closing any
/// open function calls on the final model message with placeholder results.
pub fn build_extraction_agent_history(history: &[Value]) -> Vec<Value> {
    let Some(last) = history.last() else {
        return Vec::new();
    };
    if last.get("role").and_then(Value::as_str) != Some("model") {
        return history[..history.len() - 1].to_vec();
    }

    let open_calls = last
        .get("parts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| part.get("functionCall"))
        .filter(|call| !call.is_null())
        .collect::<Vec<_>>();
    if open_calls.is_empty() {
        return history.to_vec();
    }

    let responses = open_calls
        .into_iter()
        .map(|call| {
            let mut function_response = Map::new();
            if let Some(id) = call.get("id") {
                function_response.insert("id".to_owned(), id.clone());
            }
            if let Some(name) = call.get("name") {
                function_response.insert("name".to_owned(), name.clone());
            }
            function_response.insert(
                "response".to_owned(),
                json!({ "output": "Background extraction started." }),
            );
            json!({ "functionResponse": Value::Object(function_response) })
        })
        .collect::<Vec<_>>();
    let mut result = history.to_vec();
    result.push(ExtractionMessage::new("user", responses).into_value());
    result.push(
        ExtractionMessage::new("model", vec![json!({ "text": "Acknowledged." })]).into_value(),
    );
    result
}

/// Summary and current roots passed to the agent's restricted runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtractionScopedPaths {
    pub project_root: PathBuf,
    pub trusted_project_anchor: PathBuf,
    pub project_memory_root: PathBuf,
    pub user_memory_root: PathBuf,
    pub allow_shell: bool,
    pub shell_read_only: bool,
    pub shell_working_directory: PathBuf,
    pub protect_pinned_memory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtractionAgentRequest {
    pub name: &'static str,
    pub task_prompt: String,
    pub system_prompt: String,
    pub max_turns: u32,
    pub max_time_minutes: u32,
    pub tools: &'static [&'static str],
    pub extra_history: Vec<Value>,
    pub scoped_paths: ExtractionScopedPaths,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtractionAgentStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtractionAgentRunResult {
    pub status: ExtractionAgentStatus,
    pub terminate_reason: Option<String>,
    pub files_touched: Vec<String>,
    pub files_written: Vec<String>,
}

pub type ExtractionAgentFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExtractionAgentRunResult, String>> + Send + 'a>>;
pub type ExtractionRefreshFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Runtime integration points for forked model execution and live instruction
/// refresh. The runner must enforce every permission/path/tool constraint in
/// the request; this core module only describes those constraints.
pub trait AutoMemoryExtractionRuntime: Send + Sync {
    fn execute_extraction_agent<'a>(
        &'a self,
        request: ExtractionAgentRequest,
    ) -> ExtractionAgentFuture<'a>;

    fn refresh_memory_instruction<'a>(
        &'a self,
        log_context: &'static str,
    ) -> ExtractionRefreshFuture<'a>;

    fn max_memory_agent_turns(&self) -> Option<u32> {
        None
    }

    fn memory_agent_timeout_minutes(&self) -> Option<u32> {
        None
    }

    fn warn(&self, message: &str) {
        eprintln!("[AUTO_MEMORY_EXTRACTION_AGENT] {message}");
    }
}

#[derive(Debug, Error)]
pub enum ExtractionAgentPlannerError {
    #[error("Failed to scan project managed memory: {0}")]
    ProjectScan(#[source] io::Error),
    #[error("Memory extraction agent execution failed: {0}")]
    AgentExecution(String),
    #[error("{0}")]
    AgentFailed(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AutoMemoryExtractionExecutionResult {
    pub touched_topics: Vec<AutoMemoryType>,
    pub touched_project_scope: bool,
    pub touched_user_scope: bool,
    pub system_message: Option<String>,
    pub has_tool_activity: bool,
}

/// Execute the managed extraction fork and infer touched topics from its
/// successful write-path report.
pub async fn run_auto_memory_extraction_by_agent(
    paths: &AutoMemoryPaths,
    history: &[Value],
    runtime: &dyn AutoMemoryExtractionRuntime,
) -> Result<AutoMemoryExtractionExecutionResult, ExtractionAgentPlannerError> {
    let project_docs = scan_auto_memory_topic_documents(paths)
        .await
        .map_err(ExtractionAgentPlannerError::ProjectScan)?;
    let user_docs = match scan_user_auto_memory_topic_documents(paths).await {
        Ok(docs) => docs,
        Err(error) => {
            runtime.warn(&format!(
                "User-level auto-memory scan failed; extraction agent will see project-level summaries only: {error}"
            ));
            Vec::new()
        }
    };
    let topic_summaries = build_topic_summary_block(&user_docs, &project_docs);
    let project_memory_root = paths.auto_memory_root();
    let user_memory_root = paths.user_auto_memory_root();
    let request = ExtractionAgentRequest {
        name: MANAGED_AUTO_MEMORY_EXTRACTOR_AGENT_NAME,
        task_prompt: build_extraction_task_prompt(
            &project_memory_root,
            &user_memory_root,
            &topic_summaries,
        ),
        system_prompt: extraction_agent_system_prompt(),
        max_turns: runtime
            .max_memory_agent_turns()
            .unwrap_or(DEFAULT_EXTRACTION_MAX_TURNS),
        max_time_minutes: runtime
            .memory_agent_timeout_minutes()
            .unwrap_or(DEFAULT_EXTRACTION_TIMEOUT_MINUTES),
        tools: &EXTRACTION_AGENT_TOOLS,
        extra_history: build_extraction_agent_history(history),
        scoped_paths: ExtractionScopedPaths {
            project_root: paths.project_root().to_path_buf(),
            trusted_project_anchor: paths.auto_memory_trusted_anchor().to_path_buf(),
            project_memory_root: project_memory_root.clone(),
            user_memory_root: user_memory_root.clone(),
            allow_shell: true,
            shell_read_only: true,
            shell_working_directory: paths.project_root().to_path_buf(),
            protect_pinned_memory: true,
        },
    };
    let result = runtime
        .execute_extraction_agent(request)
        .await
        .map_err(ExtractionAgentPlannerError::AgentExecution)?;
    if result.status != ExtractionAgentStatus::Completed {
        return Err(ExtractionAgentPlannerError::AgentFailed(
            result
                .terminate_reason
                .unwrap_or_else(|| "Extraction agent did not complete successfully".to_owned()),
        ));
    }

    let touched = touched_topics_from_file_paths(
        &result.files_written,
        &project_memory_root,
        &user_memory_root,
    );
    let system_message = (!touched.topics.is_empty()).then(|| {
        format!(
            "Managed auto-memory updated: {}",
            touched
                .topics
                .iter()
                .map(|topic| format!("{}.md", topic.as_str()))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });

    Ok(AutoMemoryExtractionExecutionResult {
        touched_topics: touched.topics,
        touched_project_scope: touched.touched_project_scope,
        touched_user_scope: touched.touched_user_scope,
        system_message,
        has_tool_activity: !result.files_touched.is_empty(),
    })
}

pub fn extraction_agent_system_prompt() -> String {
    EXTRACTION_AGENT_INTRO
        .iter()
        .copied()
        .chain(TYPES_SECTION_INDIVIDUAL.iter().copied())
        .chain(WHAT_NOT_TO_SAVE_SECTION.iter().copied())
        .chain([""])
        .chain(MEMORY_FRONTMATTER_EXAMPLE.iter().copied())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn build_extraction_task_prompt(
    project_memory_root: &std::path::Path,
    user_memory_root: &std::path::Path,
    topic_summaries: &str,
) -> String {
    [
        "Managed memory has TWO directories. Choose which one to write each memory into using the per-type `<scope>` guidance in your system instructions:",
        &format!("- USER memory (cross-project, durable knowledge about who the user is): `{}`", user_memory_root.display()),
        &format!("- PROJECT memory (this project only): `{}`", project_memory_root.display()),
        "",
        "Scan the recent conversation history in your context and update durable managed memory in whichever directory each memory belongs.",
        "",
        "Available tools in this run: `read_file`, `grep_search`, `glob`, `list_directory`, read-only `run_shell_command`, and `write_file`/`edit` for paths inside EITHER managed memory directory above.",
        "- Do not use any other tools.",
        "- You have a limited turn budget. `edit` requires a prior `read_file` of the same file, so the efficient strategy is: first issue all reads in parallel for every file you might update; then issue all `write_file`/`edit` calls in parallel. Do not interleave reads and writes across multiple turns.",
        "- You MUST only use content from the recent conversation history in your context plus the current managed memory files.",
        "- Do not inspect repository code, git history, or unrelated files.",
        &format!("- Treat files under the top-level `{AUTO_MEMORY_PINNED_DIRNAME}/` directory in either managed memory root as protected read-only records. You may read them to avoid duplicates, but never modify, overwrite, rename, merge into, or delete them, and do not intentionally remove their valid entries from `{AUTO_MEMORY_INDEX_FILENAME}`."),
        "- Prefer updating an existing writable memory file over creating a duplicate. Check both directories for an existing entry before creating a new one.",
        "- Keep one durable memory per file under `user/`, `feedback/`, `project/`, or `reference/` inside the chosen directory.",
        "",
        "## How to save memories",
        "",
        "**Step 1** — write or update the memory file itself, in the directory chosen by the type `<scope>`, using the required frontmatter format.",
        &format!("**Step 2** — update the `{AUTO_MEMORY_INDEX_FILENAME}` in the SAME directory where you wrote the file (`{}/{AUTO_MEMORY_INDEX_FILENAME}` for USER memory, `{}/{AUTO_MEMORY_INDEX_FILENAME}` for PROJECT memory). The index is one line per entry: `- [Title](relative/path.md) — one-line hook`. Never write memory content directly into the index.", user_memory_root.display(), project_memory_root.display()),
        "- If you create or delete a memory file, also update the managed memory index in the SAME directory.",
        "- If nothing durable should be saved, make no file changes.",
        "",
        "## Existing memory files (across both directories)",
        "",
        if topic_summaries.is_empty() { "(none yet)" } else { topic_summaries },
    ]
    .join("\n")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TouchedExtractionTopics {
    pub topics: Vec<AutoMemoryType>,
    pub touched_project_scope: bool,
    pub touched_user_scope: bool,
}

/// Derive the topic and private scope touched by an agent's reported write
/// paths. Root containment is separator-neutral and rejects sibling-prefix
/// collisions exactly like the source implementation.
pub fn touched_topics_from_file_paths(
    file_paths: &[String],
    project_memory_root: &std::path::Path,
    user_memory_root: &std::path::Path,
) -> TouchedExtractionTopics {
    let project_root = canonicalize_separators(&project_memory_root.to_string_lossy());
    let user_root = canonicalize_separators(&user_memory_root.to_string_lossy());
    let mut output = TouchedExtractionTopics {
        topics: Vec::new(),
        touched_project_scope: false,
        touched_user_scope: false,
    };
    for file_path in file_paths {
        let candidate = canonicalize_separators(file_path);
        let relative = if is_under_root(&candidate, &project_root) {
            output.touched_project_scope = true;
            candidate[project_root.len() + 1..].to_owned()
        } else if is_under_root(&candidate, &user_root) {
            output.touched_user_scope = true;
            candidate[user_root.len() + 1..].to_owned()
        } else {
            continue;
        };
        let Some(topic_segment) = relative.split('/').next() else {
            continue;
        };
        let Some(topic) = AutoMemoryType::ALL
            .into_iter()
            .find(|topic| topic.as_str() == topic_segment)
        else {
            continue;
        };
        if !output.topics.contains(&topic) {
            output.topics.push(topic);
        }
    }
    output
}

pub fn build_topic_summary_block(
    user_docs: &[ScannedAutoMemoryDocument],
    project_docs: &[ScannedAutoMemoryDocument],
) -> String {
    user_docs
        .iter()
        .map(|doc| render_topic_summary(doc, "user"))
        .chain(
            project_docs
                .iter()
                .map(|doc| render_topic_summary(doc, "project")),
        )
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn render_topic_summary(doc: &ScannedAutoMemoryDocument, scope: &str) -> String {
    let body = if doc.body == "_No entries yet._" {
        String::new()
    } else {
        truncate_summary(&doc.body, MAX_EXTRACTION_TOPIC_SUMMARY_UTF16_UNITS)
    };
    format!(
        "- [{}]({}) — {}\n  scope={}\n  topic={}\n  path={}\n  current={}",
        doc.title,
        doc.relative_path,
        if doc.description.is_empty() {
            "(no description)"
        } else {
            &doc.description
        },
        scope,
        doc.memory_type.as_str(),
        doc.file_path.display(),
        if body.is_empty() { "(empty)" } else { &body },
    )
}

fn truncate_summary(text: &str, max_utf16_units: usize) -> String {
    let normalized = normalize_ecmascript_whitespace(text);
    if normalized.encode_utf16().count() <= max_utf16_units {
        return normalized;
    }
    let mut used_units = 0;
    let mut truncated = String::new();
    for character in normalized.chars() {
        let units = character.len_utf16();
        if used_units + units > max_utf16_units {
            break;
        }
        truncated.push(character);
        used_units += units;
    }
    format!("{}…", truncated.trim_end())
}

fn normalize_ecmascript_whitespace(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut pending_space = false;
    for character in text.chars() {
        if is_ecmascript_whitespace(character) {
            pending_space = !output.is_empty();
        } else {
            if pending_space {
                output.push(' ');
                pending_space = false;
            }
            output.push(character);
        }
    }
    output
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn is_under_root(candidate: &str, root: &str) -> bool {
    candidate.starts_with(root) && candidate.as_bytes().get(root.len()) == Some(&b'/')
}

fn canonicalize_separators(path: &str) -> String {
    path.replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(memory_type: AutoMemoryType, path: &str, body: &str) -> ScannedAutoMemoryDocument {
        ScannedAutoMemoryDocument {
            memory_type,
            file_path: PathBuf::from(path),
            relative_path: path.rsplit('/').next().unwrap_or(path).to_owned(),
            filename: path.rsplit('/').next().unwrap_or(path).to_owned(),
            title: "Memory title".to_owned(),
            description: String::new(),
            body: body.to_owned(),
            mtime_ms: 0.0,
        }
    }

    #[test]
    fn history_drops_trailing_user_message() {
        let history = vec![
            json!({"role":"user", "parts":[{"text":"question"}]}),
            json!({"role":"model", "parts":[{"text":"answer"}]}),
            json!({"role":"user", "parts":[{"text":"unfinished"}]}),
        ];
        let result = build_extraction_agent_history(&history);
        assert_eq!(result, history[..2]);
    }

    #[test]
    fn history_closes_every_open_model_function_call() {
        let history = vec![json!({
            "role": "model",
            "parts": [
                {"functionCall": {"id": "a", "name": "read_file"}},
                {"functionCall": {"id": "b", "name": "glob"}}
            ]
        })];
        let result = build_extraction_agent_history(&history);
        assert_eq!(result.len(), 3);
        assert_eq!(result[1]["parts"].as_array().unwrap().len(), 2);
        assert_eq!(
            result[1]["parts"][0]["functionResponse"]["response"]["output"],
            "Background extraction started."
        );
        assert_eq!(result[2]["parts"][0]["text"], "Acknowledged.");
    }

    #[test]
    fn history_with_closed_model_message_is_copied_unchanged() {
        let history = vec![json!({"role":"model", "parts":[{"text":"ok"}]})];
        assert_eq!(build_extraction_agent_history(&history), history);
    }

    #[test]
    fn touched_paths_keep_insertion_order_and_reject_siblings() {
        let touched = touched_topics_from_file_paths(
            &[
                "/project-memory/project/a.md".into(),
                "/user-memory/user/a.md".into(),
                "/project-memory/feedback/a.md".into(),
                "/project-memory-other/user/no.md".into(),
            ],
            std::path::Path::new("/project-memory"),
            std::path::Path::new("/user-memory"),
        );
        assert_eq!(
            touched.topics,
            vec![
                AutoMemoryType::Project,
                AutoMemoryType::User,
                AutoMemoryType::Feedback
            ]
        );
        assert!(touched.touched_project_scope);
        assert!(touched.touched_user_scope);
    }

    #[test]
    fn touched_paths_match_slash_variants_and_only_allowed_first_segments() {
        let touched = touched_topics_from_file_paths(
            &[
                r"C:\memory\reference\api.md".into(),
                r"C:\memory\pinned\locked.md".into(),
            ],
            std::path::Path::new(r"C:\memory"),
            std::path::Path::new(r"C:\user-memory"),
        );
        assert_eq!(touched.topics, vec![AutoMemoryType::Reference]);
        assert!(touched.touched_project_scope);
    }

    #[test]
    fn summary_block_orders_user_before_project_and_truncates_by_utf16_units() {
        let user = doc(AutoMemoryType::User, "/user/user.md", &"😀".repeat(200));
        let project = doc(
            AutoMemoryType::Project,
            "/project/project.md",
            "_No entries yet._",
        );
        let summaries = build_topic_summary_block(&[user], &[project]);
        assert!(summaries.starts_with("- [Memory title](user.md)"));
        assert!(summaries.contains("current=(empty)"));
        let current = summaries.split("current=").nth(1).unwrap();
        let body = current.split("\n\n").next().unwrap();
        assert_eq!(
            body.encode_utf16().count(),
            MAX_EXTRACTION_TOPIC_SUMMARY_UTF16_UNITS + 1
        );
        assert!(body.ends_with('…'));
    }

    #[test]
    fn task_prompt_mentions_both_roots_pinned_protection_and_same_scope_indexes() {
        let prompt = build_extraction_task_prompt(
            std::path::Path::new("/project/memory"),
            std::path::Path::new("/user/memories"),
            "summaries",
        );
        assert!(prompt.contains("/project/memory/MEMORY.md"));
        assert!(prompt.contains("/user/memories/MEMORY.md"));
        assert!(prompt.contains("pinned/"));
        assert!(prompt.contains("summaries"));
    }

    #[test]
    fn system_prompt_includes_memory_rules_and_frontmatter() {
        let prompt = extraction_agent_system_prompt();
        assert!(prompt.contains("Extract only durable facts stated by the user."));
        assert!(prompt.contains("<name>feedback</name>"));
        assert!(prompt.contains("{{memory name}}"));
    }
}
