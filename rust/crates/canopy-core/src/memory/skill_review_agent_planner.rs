//! Managed skill-review agent prompt, history, and permission policy.
//!
//! Port of `packages/core/src/memory/skillReviewAgentPlanner.ts`. Agent
//! execution is injected; adapters must enforce `SkillScopedPermissionPolicy`
//! for every requested tool operation.

use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};

use crate::permissions::PermissionDecision;
use crate::skills::{
    SKILL_FILE_NAME, assert_real_project_skill_path, get_archived_skills_root,
    get_project_skills_root, is_project_skill_path,
};

pub const SKILL_REVIEW_AGENT_NAME: &str = "managed-skill-extractor";
pub const DEFAULT_AUTO_SKILL_MAX_TURNS: u32 = 8;
pub const DEFAULT_AUTO_SKILL_TIMEOUT_MS: u64 = 120_000;
pub const AUTO_SKILL_DIR_PREFIX: &str = "auto-skill-";
pub const SKILL_REVIEW_AGENT_TOOLS: [&str; 4] =
    ["read_file", "list_directory", "write_file", "edit"];

pub const SKILL_REVIEW_SYSTEM_PROMPT: &str = concat!(
    "You are reviewing this conversation to extract reusable skills.\n\n",
    "Review the conversation above and consider saving or updating a skill if appropriate.\n\n",
    "Focus on: was a non-trivial approach used to complete a task that required trial and error, or changing course due to experiential findings along the way, or did the user expect or desire a different method or outcome? If a relevant skill already exists and has 'source: auto-skill' in its frontmatter, update it with what you learned. Otherwise, create a new skill if the approach is reusable.\n\n",
    "IMPORTANT constraints:\n",
    "- You may ONLY modify skill files that contain 'source: auto-skill' in their YAML frontmatter. Always read a skill file before editing it.\n",
    "- Do NOT touch skills that lack this marker — they were created by the user.\n",
    "- When creating a new skill, you MUST include 'source: auto-skill' in the frontmatter so future review agents can safely update it.\n",
    "- When creating a new skill, its directory MUST use the `auto-skill-` prefix (e.g. `.canopy/skills/auto-skill-<name>/SKILL.md`) so the project's .gitignore keeps auto-generated skills out of version control. Keep the frontmatter `name:` as the natural `<name>` without the prefix.\n",
    "- Do NOT delete any skill. Only create or update.\n\n",
    "If nothing is worth saving, just say 'Nothing to save.' and stop."
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillReviewPermissionContext {
    pub tool_name: String,
    pub file_path: Option<PathBuf>,
    pub command: Option<String>,
    pub domain: Option<String>,
    pub specifier: Option<String>,
    pub tool_params: Option<Value>,
}

/// Narrow adapter over existing configured permissions. The policy below
/// adds skill-review restrictions and combines the result with this base.
pub trait SkillReviewBasePermissionManager: Send + Sync {
    fn has_relevant_rules(&self, context: &SkillReviewPermissionContext) -> bool;
    fn has_matching_ask_rule(&self, context: &SkillReviewPermissionContext) -> bool;
    fn find_matching_deny_rule(&self, context: &SkillReviewPermissionContext) -> Option<String>;
    fn evaluate(&self, context: &SkillReviewPermissionContext) -> PermissionDecision;
    fn is_tool_enabled(&self, tool_name: &str) -> bool;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillScopedPermissionPolicy {
    pub project_root: PathBuf,
}

impl SkillScopedPermissionPolicy {
    pub fn has_relevant_rules(
        &self,
        context: &SkillReviewPermissionContext,
        base: Option<&dyn SkillReviewBasePermissionManager>,
    ) -> bool {
        is_scoped_tool(&context.tool_name)
            || base.is_some_and(|base| base.has_relevant_rules(context))
    }

    pub fn has_matching_ask_rule(
        &self,
        context: &SkillReviewPermissionContext,
        base: Option<&dyn SkillReviewBasePermissionManager>,
    ) -> bool {
        base.is_some_and(|base| base.has_matching_ask_rule(context))
    }

    pub fn find_matching_deny_rule(
        &self,
        context: &SkillReviewPermissionContext,
        base: Option<&dyn SkillReviewBasePermissionManager>,
    ) -> Option<String> {
        get_scoped_deny_rule(context, &self.project_root)
            .or_else(|| base.and_then(|base| base.find_matching_deny_rule(context)))
    }

    pub async fn evaluate(
        &self,
        context: &SkillReviewPermissionContext,
        base: Option<&dyn SkillReviewBasePermissionManager>,
    ) -> PermissionDecision {
        let scoped = evaluate_scoped_decision(context, &self.project_root).await;
        let Some(base) = base else {
            return scoped;
        };
        let base_decision = if base.has_relevant_rules(context) {
            base.evaluate(context)
        } else {
            PermissionDecision::Default
        };
        merge_permission_decision(scoped, base_decision)
    }

    pub fn is_tool_enabled(
        &self,
        tool_name: &str,
        base: Option<&dyn SkillReviewBasePermissionManager>,
    ) -> bool {
        if is_scoped_tool(tool_name) {
            return true;
        }
        base.is_none_or(|base| base.is_tool_enabled(tool_name))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SkillReviewAgentRequest {
    pub name: &'static str,
    pub task_prompt: String,
    pub system_prompt: &'static str,
    pub history: Vec<Value>,
    pub max_turns: u32,
    pub max_time_minutes: f64,
    pub tools: &'static [&'static str],
    pub permission_policy: SkillScopedPermissionPolicy,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SkillReviewAgentStatus {
    #[default]
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SkillReviewAgentRunResult {
    pub status: SkillReviewAgentStatus,
    pub terminate_reason: Option<String>,
    pub files_touched: Vec<PathBuf>,
}

pub type SkillReviewAgentFuture<'a> =
    Pin<Box<dyn Future<Output = Result<SkillReviewAgentRunResult, String>> + Send + 'a>>;

/// The session/config layer supplies the active memory-agent limits and
/// permission rules, then delegates model/tool execution to its agent runtime.
pub trait SkillReviewAgentRuntime: Send + Sync {
    fn max_turns(&self) -> Option<u32> {
        None
    }

    fn timeout_minutes(&self) -> Option<f64> {
        None
    }

    fn base_permissions(&self) -> Option<&dyn SkillReviewBasePermissionManager> {
        None
    }

    fn execute_skill_review<'a>(
        &'a self,
        request: SkillReviewAgentRequest,
    ) -> SkillReviewAgentFuture<'a>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillReviewExecutionResult {
    pub touched_skill_files: Vec<PathBuf>,
    pub system_message: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SkillReviewOptions {
    /// Per-call turn override, if set.
    pub max_turns: Option<u32>,
    /// Per-call timeout override in milliseconds, if set.
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum SkillReviewError {
    #[error("Skill review agent execution failed: {0}")]
    Execution(String),
    #[error("{0}")]
    AgentFailed(String),
    #[error("{0}")]
    AgentCancelled(String),
}

/// Read an existing auto-generated skill marker. `None` means the file does
/// not exist; all other read failures return `Some(false)` so the permission
/// policy denies conservatively.
pub async fn has_auto_skill_source(file_path: impl AsRef<Path>) -> Option<bool> {
    let content = match tokio::fs::read(file_path).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(_) => return Some(false),
    };
    let Ok(frontmatter_regex) =
        regex::Regex::new(r"(?s)^---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|$)")
    else {
        return Some(false);
    };
    let Some(captures) = frontmatter_regex.captures(&content) else {
        return Some(false);
    };
    let Some(frontmatter) = captures.get(1) else {
        return Some(false);
    };
    let Ok(source_regex) = regex::Regex::new(r"(?m)^source:\s*auto-skill\s*$") else {
        return Some(false);
    };
    Some(source_regex.is_match(frontmatter.as_str()))
}

pub async fn is_archived_skill_directory_reserved(
    file_path: impl AsRef<Path>,
    project_root: impl AsRef<Path>,
) -> bool {
    let Some(directory_name) = file_path.as_ref().parent().and_then(Path::file_name) else {
        return true;
    };
    let archive_path = get_archived_skills_root(project_root).join(directory_name);
    match tokio::fs::symlink_metadata(archive_path).await {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

pub async fn evaluate_scoped_decision(
    context: &SkillReviewPermissionContext,
    project_root: impl AsRef<Path>,
) -> PermissionDecision {
    let project_root = project_root.as_ref();
    match context.tool_name.as_str() {
        "read_file" | "list_directory" => {
            let Some(file_path) = context.file_path.as_deref() else {
                return PermissionDecision::Allow;
            };
            if is_within_project(file_path, project_root) {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            }
        }
        "edit" => {
            let Some(file_path) = context.file_path.as_deref() else {
                return PermissionDecision::Deny;
            };
            if !is_project_skill_path(file_path, project_root)
                || assert_real_project_skill_path(file_path, project_root).is_err()
            {
                return PermissionDecision::Deny;
            }
            match has_auto_skill_source(file_path).await {
                None => {
                    if is_archived_skill_directory_reserved(file_path, project_root).await {
                        PermissionDecision::Deny
                    } else {
                        PermissionDecision::Allow
                    }
                }
                Some(true) => PermissionDecision::Allow,
                Some(false) => PermissionDecision::Deny,
            }
        }
        "write_file" => {
            let Some(file_path) = context.file_path.as_deref() else {
                return PermissionDecision::Deny;
            };
            if !is_project_skill_path(file_path, project_root)
                || file_path.file_name().and_then(|name| name.to_str()) != Some(SKILL_FILE_NAME)
                || assert_real_project_skill_path(file_path, project_root).is_err()
                || is_archived_skill_directory_reserved(file_path, project_root).await
            {
                return PermissionDecision::Deny;
            }
            match tokio::fs::metadata(file_path).await {
                Ok(_) => PermissionDecision::Deny,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    PermissionDecision::Allow
                }
                Err(_) => PermissionDecision::Deny,
            }
        }
        _ => PermissionDecision::Default,
    }
}

pub fn get_scoped_deny_rule(
    context: &SkillReviewPermissionContext,
    project_root: impl AsRef<Path>,
) -> Option<String> {
    match context.tool_name.as_str() {
        "read_file" | "list_directory" => None,
        "edit" => Some(format!(
            "ManagedSkillReview(edit: only within {} and only on skills with 'source: auto-skill' in frontmatter)",
            get_project_skills_root(project_root).display()
        )),
        "write_file" => Some(format!(
            "ManagedSkillReview(write_file: only within {} and only to a path that does not yet exist — use a different skill name like `<name>-2`, or use `edit` to update an existing auto-skill)",
            get_project_skills_root(project_root).display()
        )),
        _ => None,
    }
}

pub async fn list_existing_skill_dir_names(project_root: impl AsRef<Path>) -> Vec<String> {
    let skills_root = get_project_skills_root(project_root);
    let Ok(mut entries) = tokio::fs::read_dir(&skills_root).await else {
        return Vec::new();
    };
    let mut names = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) | Err(_) => break,
        };
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if !file_type.is_dir() && !file_type.is_symlink() {
            continue;
        }
        if tokio::fs::metadata(entry.path().join(SKILL_FILE_NAME))
            .await
            .is_ok()
        {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    names.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    names
}

pub async fn list_archived_skill_dir_names(project_root: impl AsRef<Path>) -> Vec<String> {
    let Ok(mut entries) = tokio::fs::read_dir(get_archived_skills_root(project_root)).await else {
        return Vec::new();
    };
    let mut names = Vec::new();
    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) | Err(_) => break,
        };
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        if (file_type.is_dir() || file_type.is_symlink()) && is_valid_skill_name(&name) {
            names.push(name);
        }
    }
    names.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    names
}

pub async fn build_task_prompt(project_root: impl AsRef<Path>, now: DateTime<Utc>) -> String {
    let project_root = project_root.as_ref();
    let skills_root = get_project_skills_root(project_root);
    let (active, archived) = tokio::join!(
        list_existing_skill_dir_names(project_root),
        list_archived_skill_dir_names(project_root),
    );
    let existing_line = if active.is_empty() && archived.is_empty() {
        "(no skills exist yet — any name is available)".to_owned()
    } else {
        let mut lines = Vec::new();
        if !active.is_empty() {
            lines.push(format!(
                "Active skill directory names (use `edit` to update): {}",
                active.join(", ")
            ));
        }
        if !archived.is_empty() {
            lines.push(format!(
                "Archived skill directory names (do NOT reuse for write_file): {}",
                archived.join(", ")
            ));
        }
        lines.join("\n")
    };
    [
        format!("Project skills directory: `{}`", skills_root.display()),
        String::new(),
        existing_line,
        String::new(),
        "Use `ls` and `read_file` to inspect existing skills before writing.".to_owned(),
        "Use `write_file` to create a new skill, `edit` to update an existing auto-skill.".to_owned(),
        format!("New skills you create MUST live at `.canopy/skills/{AUTO_SKILL_DIR_PREFIX}<name>/SKILL.md` — the `{AUTO_SKILL_DIR_PREFIX}` directory prefix is mandatory so the project's .gitignore keeps auto-generated skills out of version control. Keep the frontmatter `name:` as the natural `<name>` (no prefix). The frontmatter MUST include 'source: auto-skill':"),
        String::new(),
        "---".to_owned(),
        "name: <skill-name>".to_owned(),
        "description: <one-line description>".to_owned(),
        "source: auto-skill".to_owned(),
        format!("extracted_at: '{}'", now.to_rfc3339_opts(SecondsFormat::Millis, true)),
        "---".to_owned(),
        String::new(),
        "<markdown body with the procedure/approach>".to_owned(),
    ]
    .join("\n")
}

/// Close unfinished model tool calls and omit a trailing unanswered user turn
/// before passing a conversation snapshot to the skill-review agent.
pub fn build_agent_history(history: &[Value]) -> Vec<Value> {
    if history.is_empty() {
        return Vec::new();
    }
    let last = &history[history.len() - 1];
    if last.get("role").and_then(Value::as_str) != Some("model") {
        return history[..history.len() - 1].to_vec();
    }
    let calls = last
        .get("parts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| part.get("functionCall").filter(|call| js_truthy(call)))
        .collect::<Vec<_>>();
    if calls.is_empty() {
        return history.to_vec();
    }
    let responses = calls
        .into_iter()
        .map(|call| {
            let mut response = serde_json::Map::new();
            if let Some(id) = call.get("id") {
                response.insert("id".to_owned(), id.clone());
            }
            if let Some(name) = call.get("name") {
                response.insert("name".to_owned(), name.clone());
            }
            response.insert(
                "response".to_owned(),
                json!({"output":"Background skill review started."}),
            );
            json!({"functionResponse": response})
        })
        .collect::<Vec<_>>();
    let mut result = history.to_vec();
    result.push(json!({"role":"user", "parts":responses}));
    result.push(json!({"role":"model", "parts":[{"text":"Acknowledged."}]}));
    result
}

pub async fn run_skill_review_by_agent<R: SkillReviewAgentRuntime>(
    runtime: &R,
    project_root: impl AsRef<Path>,
    history: &[Value],
    now: DateTime<Utc>,
    options: SkillReviewOptions,
) -> Result<SkillReviewExecutionResult, SkillReviewError> {
    let project_root = project_root.as_ref();
    let configured_timeout = runtime
        .timeout_minutes()
        .unwrap_or(DEFAULT_AUTO_SKILL_TIMEOUT_MS as f64 / 60_000.0);
    let max_time_minutes = options
        .timeout_ms
        .map(|timeout_ms| timeout_ms as f64 / 60_000.0)
        .unwrap_or(configured_timeout);
    let request = SkillReviewAgentRequest {
        name: SKILL_REVIEW_AGENT_NAME,
        task_prompt: build_task_prompt(project_root, now).await,
        system_prompt: SKILL_REVIEW_SYSTEM_PROMPT,
        history: build_agent_history(history),
        max_turns: options
            .max_turns
            .or_else(|| runtime.max_turns())
            .unwrap_or(DEFAULT_AUTO_SKILL_MAX_TURNS),
        max_time_minutes,
        tools: &SKILL_REVIEW_AGENT_TOOLS,
        permission_policy: SkillScopedPermissionPolicy {
            project_root: project_root.to_path_buf(),
        },
    };
    let result = runtime
        .execute_skill_review(request)
        .await
        .map_err(SkillReviewError::Execution)?;
    match result.status {
        SkillReviewAgentStatus::Failed => {
            return Err(SkillReviewError::AgentFailed(
                result.terminate_reason.unwrap_or_else(|| {
                    "Skill review agent did not complete successfully".to_owned()
                }),
            ));
        }
        SkillReviewAgentStatus::Cancelled => {
            return Err(SkillReviewError::AgentCancelled(
                result.terminate_reason.unwrap_or_else(|| {
                    "Skill review agent did not complete successfully".to_owned()
                }),
            ));
        }
        SkillReviewAgentStatus::Completed => {}
    }
    let touched_skill_files = result
        .files_touched
        .into_iter()
        .filter(|path| is_project_skill_path(path, project_root))
        .collect::<Vec<_>>();
    let system_message = (!touched_skill_files.is_empty()).then(|| {
        format!(
            "Skill review updated {} file(s).",
            touched_skill_files.len()
        )
    });
    Ok(SkillReviewExecutionResult {
        touched_skill_files,
        system_message,
    })
}

fn is_scoped_tool(tool_name: &str) -> bool {
    matches!(
        tool_name,
        "read_file" | "list_directory" | "edit" | "write_file"
    )
}

fn merge_permission_decision(
    scoped: PermissionDecision,
    base: PermissionDecision,
) -> PermissionDecision {
    fn priority(decision: PermissionDecision) -> u8 {
        match decision {
            PermissionDecision::Deny => 4,
            PermissionDecision::Ask => 3,
            PermissionDecision::Allow => 2,
            PermissionDecision::Default => 1,
        }
    }
    if priority(base) > priority(scoped) {
        base
    } else {
        scoped
    }
}

fn is_within_project(candidate: &Path, project_root: &Path) -> bool {
    let root = absolute_lexical(project_root);
    let resolved = if candidate.is_absolute() {
        absolute_lexical(candidate)
    } else {
        absolute_lexical(&project_root.join(candidate))
    };
    resolved == root || resolved.starts_with(root)
}

fn absolute_lexical(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut result = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => result.push(prefix.as_os_str()),
            Component::RootDir => result.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !result.pop() && !result.has_root() {
                    result.push(component.as_os_str());
                }
            }
            Component::Normal(part) => result.push(part),
        }
    }
    result
}

fn is_valid_skill_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|character| {
            character.is_alphanumeric() || matches!(character, '_' | ':' | '.' | '-')
        })
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trailing_unanswered_user_turn_is_removed() {
        let history = vec![
            json!({"role":"user", "parts":[{"text":"old"}]}),
            json!({"role":"model", "parts":[{"text":"answer"}]}),
            json!({"role":"user", "parts":[{"text":"new question"}]}),
        ];
        assert_eq!(build_agent_history(&history), history[..2]);
    }

    #[test]
    fn open_tool_calls_get_placeholder_responses() {
        let history = vec![json!({
            "role":"model",
            "parts":[
                {"functionCall":{"id":"call-1","name":"read_file","args":{}}},
                {"functionCall":{"name":"list_directory","args":{}}}
            ]
        })];
        let closed = build_agent_history(&history);
        assert_eq!(closed.len(), 3);
        assert_eq!(closed[1]["parts"][0]["functionResponse"]["id"], "call-1");
        assert_eq!(
            closed[1]["parts"][1]["functionResponse"]["name"],
            "list_directory"
        );
        assert_eq!(closed[2]["parts"][0]["text"], "Acknowledged.");
    }

    #[tokio::test]
    async fn new_skill_writes_are_limited_to_fresh_canonical_manifest_slots() {
        let root = std::env::temp_dir().join(format!("skill-policy-{}", uuid::Uuid::new_v4()));
        let target = get_project_skills_root(&root).join("auto-skill-new/SKILL.md");
        let policy = SkillScopedPermissionPolicy {
            project_root: root.clone(),
        };
        let context = SkillReviewPermissionContext {
            tool_name: "write_file".to_owned(),
            file_path: Some(target.clone()),
            command: None,
            domain: None,
            specifier: None,
            tool_params: None,
        };
        assert_eq!(
            policy.evaluate(&context, None).await,
            PermissionDecision::Allow
        );
        let non_manifest = SkillReviewPermissionContext {
            file_path: Some(target.with_file_name("NOTES.md")),
            ..context
        };
        assert_eq!(
            policy.evaluate(&non_manifest, None).await,
            PermissionDecision::Deny
        );
    }

    #[test]
    fn prompt_requires_prefix_marker_and_forbids_deletion() {
        assert!(SKILL_REVIEW_SYSTEM_PROMPT.contains("auto-skill-<name>"));
        assert!(SKILL_REVIEW_SYSTEM_PROMPT.contains("source: auto-skill"));
        assert!(SKILL_REVIEW_SYSTEM_PROMPT.contains("Do NOT delete any skill"));
    }
}
