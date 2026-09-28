//! Static skill-tool contract and skill invocation parameter helpers.
//!
//! Port of the declaration and pure helpers in
//! `packages/core/src/tools/skill.ts` and `skill-utils.ts`. Loading skills,
//! refreshing the available set, and executing a selected skill remain host
//! responsibilities.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::skills::SkillConfig;

pub const NAME: &str = "skill";
pub const DISPLAY_NAME: &str = "Skill";
pub const KIND: &str = "read";

/// Static prompt text. The changing skill list is supplied through system
/// reminders so refreshing skills does not mutate the model tool declaration.
pub const DESCRIPTION: &str = r#"Execute a skill within the main conversation

<skills_instructions>
When users ask you to perform tasks, check if any of the available skills can help complete the task more effectively. Skills provide specialized capabilities and domain knowledge.

How to invoke:
- Use this tool with the skill name only (no arguments)
- Examples:
  - `skill: "pdf"` - invoke the pdf skill
  - `skill: "xlsx"` - invoke the xlsx skill
  - `skill: "ms-office-suite:pdf"` - invoke using fully qualified name
  - `skill: "mcp-prompt", args: "topic"` - invoke a model-invocable command with arguments

Important:
- Available skills are listed in <system-reminder> messages in the conversation; only use skills listed there.
- When a skill is relevant, you must invoke this tool IMMEDIATELY as your first action
- NEVER just announce or mention a skill in your text response without actually calling this tool
- This is a BLOCKING REQUIREMENT: invoke the relevant Skill tool BEFORE generating any other response about the task
- Do not invoke a skill that is already running
- Do not use this tool for built-in CLI commands (like /help, /clear, etc.)
- When executing scripts or loading referenced files, ALWAYS resolve absolute paths from skill's base directory. Examples:
  - `bash scripts/init.sh` -> `bash /path/to/skill/scripts/init.sh`
  - `python scripts/helper.py` -> `python /path/to/skill/scripts/helper.py`
  - `reference.md` -> `/path/to/skill/reference.md`
</skills_instructions>"#;

/// Arguments accepted by the static Skill tool declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SkillParams {
    pub skill: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,
}

/// Native holder for the static, read-only skill tool declaration.
pub struct SkillTool;

impl SkillTool {
    pub const NAME: &'static str = NAME;
    pub const DISPLAY_NAME: &'static str = DISPLAY_NAME;
    pub const KIND: &'static str = KIND;

    pub fn function_declaration() -> Value {
        function_declaration()
    }

    pub fn parse_params(args: &Value) -> Result<SkillParams, String> {
        parse_params(args)
    }

    pub fn validate_tool_params(
        params: &SkillParams,
        available_skills: &[SkillConfig],
    ) -> Option<String> {
        validate_tool_params(params, available_skills)
    }
}

/// Return the static JSON-schema declaration used by `SkillTool`.
pub fn function_declaration() -> Value {
    json!({
        "name": NAME,
        "description": DESCRIPTION,
        "parameters": {
            "type": "object",
            "properties": {
                "skill": {
                    "type": "string",
                    "description": "The skill or command name. E.g., \"pdf\" or \"xlsx\""
                },
                "args": {
                    "type": "string",
                    "description": "Optional arguments for model-invocable slash commands."
                }
            },
            "required": ["skill"],
            "additionalProperties": false,
            "$schema": "http://json-schema.org/draft-07/schema#"
        }
    })
}

/// Parse the tool's two declared fields and reject undeclared fields.
pub fn parse_params(args: &Value) -> Result<SkillParams, String> {
    let object = args
        .as_object()
        .ok_or_else(|| "skill arguments must be an object".to_owned())?;
    if object.keys().any(|key| key != "skill" && key != "args") {
        return Err("skill arguments may contain only \"skill\" and \"args\"".to_owned());
    }

    let skill = object
        .get("skill")
        .and_then(Value::as_str)
        .ok_or_else(|| "Parameter \"skill\" must be a non-empty string.".to_owned())?;
    let args = match object.get("args") {
        None => None,
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => {
            return Err("Parameter \"args\" must be a string when provided.".to_owned());
        }
    };

    let params = SkillParams {
        skill: skill.to_owned(),
        args,
    };
    if params.skill.trim().is_empty() {
        return Err("Parameter \"skill\" must be a non-empty string.".to_owned());
    }
    Ok(params)
}

/// Validate the skill name against the host's current model-available skills.
/// The host owns disabled/path-gated skill filtering and command fallbacks, so
/// callers should pass the same active `SkillConfig` slice shown in reminders.
pub fn validate_tool_params(
    params: &SkillParams,
    available_skills: &[SkillConfig],
) -> Option<String> {
    if params.skill.trim().is_empty() {
        return Some("Parameter \"skill\" must be a non-empty string.".to_owned());
    }

    if available_skills
        .iter()
        .any(|skill| skill.name == params.skill)
    {
        return None;
    }

    if available_skills.is_empty() {
        return Some(format!(
            "Skill \"{}\" not found. No skills are currently available.",
            params.skill
        ));
    }

    let available_names = available_skills
        .iter()
        .map(|skill| skill.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "Skill \"{}\" not found. Available skills: {available_names}",
        params.skill
    ))
}

/// Build the exact model-facing body injected when a skill is loaded.
pub fn build_skill_llm_content(base_dir: &str, body: &str) -> String {
    format!(
        "Base directory for this skill: {base_dir}\nImportant: ALWAYS resolve absolute paths from this base directory when working with skills.\n\n{body}\n"
    )
}
