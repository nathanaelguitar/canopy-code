//! Tool-name aliases and matching rules corresponding to
//! `packages/core/src/utils/tool-utils.ts` and `tools/tool-names.ts`.

use std::collections::BTreeSet;

use serde_json::Value;

#[derive(Clone, Copy)]
struct ToolNameSpec {
    canonical: &'static str,
    display: &'static str,
}

const TOOL_NAMES: &[ToolNameSpec] = &[
    ToolNameSpec {
        canonical: "edit",
        display: "Edit",
    },
    ToolNameSpec {
        canonical: "write_file",
        display: "WriteFile",
    },
    ToolNameSpec {
        canonical: "read_file",
        display: "ReadFile",
    },
    ToolNameSpec {
        canonical: "zoom_image",
        display: "ZoomImage",
    },
    ToolNameSpec {
        canonical: "grep_search",
        display: "Grep",
    },
    ToolNameSpec {
        canonical: "glob",
        display: "Glob",
    },
    ToolNameSpec {
        canonical: "super_search",
        display: "SuperSearch",
    },
    ToolNameSpec {
        canonical: "run_shell_command",
        display: "Shell",
    },
    ToolNameSpec {
        canonical: "todo_write",
        display: "TodoList",
    },
    ToolNameSpec {
        canonical: "save_memory",
        display: "SaveMemory",
    },
    ToolNameSpec {
        canonical: "agent",
        display: "Agent",
    },
    ToolNameSpec {
        canonical: "skill",
        display: "Skill",
    },
    ToolNameSpec {
        canonical: "exit_plan_mode",
        display: "ExitPlanMode",
    },
    ToolNameSpec {
        canonical: "enter_plan_mode",
        display: "EnterPlanMode",
    },
    ToolNameSpec {
        canonical: "web_fetch",
        display: "WebFetch",
    },
    ToolNameSpec {
        canonical: "web_search",
        display: "WebSearch",
    },
    ToolNameSpec {
        canonical: "image_gen",
        display: "ImageGen",
    },
    ToolNameSpec {
        canonical: "list_directory",
        display: "ListFiles",
    },
    ToolNameSpec {
        canonical: "lsp",
        display: "Lsp",
    },
    ToolNameSpec {
        canonical: "ask_user_question",
        display: "AskUserQuestion",
    },
    ToolNameSpec {
        canonical: "cron_create",
        display: "CronCreate",
    },
    ToolNameSpec {
        canonical: "cron_list",
        display: "CronList",
    },
    ToolNameSpec {
        canonical: "cron_delete",
        display: "CronDelete",
    },
    ToolNameSpec {
        canonical: "loop_wakeup",
        display: "LoopWakeup",
    },
    ToolNameSpec {
        canonical: "create_sub_session",
        display: "CreateSubSession",
    },
    ToolNameSpec {
        canonical: "list_agents",
        display: "ListAgents",
    },
    ToolNameSpec {
        canonical: "task_stop",
        display: "TaskStop",
    },
    ToolNameSpec {
        canonical: "task_create",
        display: "TaskCreate",
    },
    ToolNameSpec {
        canonical: "task_update",
        display: "TaskUpdate",
    },
    ToolNameSpec {
        canonical: "task_list",
        display: "TaskList",
    },
    ToolNameSpec {
        canonical: "team_create",
        display: "TeamCreate",
    },
    ToolNameSpec {
        canonical: "team_delete",
        display: "TeamDelete",
    },
    ToolNameSpec {
        canonical: "team_plan_approval",
        display: "TeamPlanApproval",
    },
    ToolNameSpec {
        canonical: "send_message",
        display: "SendMessage",
    },
    ToolNameSpec {
        canonical: "structured_output",
        display: "StructuredOutput",
    },
    ToolNameSpec {
        canonical: "monitor",
        display: "Monitor",
    },
    ToolNameSpec {
        canonical: "notebook_edit",
        display: "NotebookEdit",
    },
    ToolNameSpec {
        canonical: "tool_search",
        display: "ToolSearch",
    },
    ToolNameSpec {
        canonical: "read_mcp_resource",
        display: "ReadMcpResource",
    },
    ToolNameSpec {
        canonical: "enter_worktree",
        display: "EnterWorktree",
    },
    ToolNameSpec {
        canonical: "exit_worktree",
        display: "ExitWorktree",
    },
    ToolNameSpec {
        canonical: "workflow",
        display: "Workflow",
    },
    ToolNameSpec {
        canonical: "artifact",
        display: "Artifact",
    },
    ToolNameSpec {
        canonical: "record_artifact",
        display: "RecordArtifact",
    },
    ToolNameSpec {
        canonical: "get_goal",
        display: "Goal",
    },
    ToolNameSpec {
        canonical: "update_goal",
        display: "UpdateGoal",
    },
    ToolNameSpec {
        canonical: "display_image",
        display: "DisplayImage",
    },
];

const LEGACY_TOOL_NAMES: &[(&str, &str)] = &[
    ("search_file_content", "grep_search"),
    ("replace", "edit"),
    ("task", "agent"),
];

const LEGACY_DISPLAY_NAMES: &[(&str, &str)] = &[
    ("SearchFiles", "Grep"),
    ("FindFiles", "Glob"),
    ("ReadFolder", "ListFiles"),
    ("Task", "Agent"),
    ("TodoWrite", "TodoList"),
];

/// Trim surrounding whitespace and remove leading underscores from an identifier.
pub fn normalize_identifier(identifier: &str) -> String {
    identifier.trim().trim_start_matches('_').to_owned()
}

/// Resolve the legacy tool-name migrations used by Canopy.
///
/// Display names and unknown names are returned unchanged, matching
/// `canonicalToolName` in `tools/tool-names.ts`.
pub fn canonical_tool_name(tool_name: &str) -> String {
    LEGACY_TOOL_NAMES
        .iter()
        .find_map(|(legacy, canonical)| (*legacy == tool_name).then_some(*canonical))
        .unwrap_or(tool_name)
        .to_owned()
}

/// Return canonical, display, class-style, and legacy aliases for a tool.
///
/// Unknown names get only their normalized spelling, as in the TypeScript
/// fallback. Lookup of a known canonical name itself remains exact.
pub fn get_alias_set_for_tool(tool_name: &str) -> BTreeSet<String> {
    let Some(spec) = TOOL_NAMES.iter().find(|spec| spec.canonical == tool_name) else {
        return BTreeSet::from([normalize_identifier(tool_name)]);
    };

    let mut aliases = BTreeSet::new();
    add_alias(&mut aliases, spec.canonical);
    add_alias(&mut aliases, spec.display);
    add_alias(&mut aliases, &format!("{}Tool", spec.display));

    for (legacy_name, mapped_name) in LEGACY_TOOL_NAMES {
        if *mapped_name == spec.canonical {
            add_alias(&mut aliases, legacy_name);
        }
    }
    for (legacy_display, mapped_display) in LEGACY_DISPLAY_NAMES {
        if *mapped_display == spec.display {
            add_alias(&mut aliases, legacy_display);
        }
    }
    aliases
}

fn add_alias(aliases: &mut BTreeSet<String>, alias: &str) {
    if !alias.is_empty() {
        aliases.insert(normalize_identifier(alias));
    }
}

fn sanitize_pattern_identifier(value: &str) -> String {
    normalize_identifier(value.split_once('(').map_or(value, |(name, _)| name))
}

fn filtered_entries(list: Option<&[String]>) -> impl Iterator<Item = &str> {
    list.into_iter()
        .flatten()
        .map(String::as_str)
        .filter(|entry| !entry.is_empty() && !entry.trim().is_empty())
}

/// Check whether a tool is enabled by core and exclusion configuration.
///
/// Core entries accept canonical, display, legacy, and class-style aliases;
/// only core entries may include an argument suffix. Exclusions compare the
/// full identifier, so an argument-specific exclusion does not exclude the
/// whole tool. Empty or absent core lists allow every tool unless excluded.
pub fn is_tool_enabled(
    tool_name: &str,
    core_tools: Option<&[String]>,
    exclude_tools: Option<&[String]>,
) -> bool {
    let aliases = get_alias_set_for_tool(tool_name);
    let core: Vec<_> = filtered_entries(core_tools).collect();
    let excluded =
        filtered_entries(exclude_tools).any(|entry| aliases.contains(&normalize_identifier(entry)));

    if core.is_empty() {
        return !excluded;
    }

    let explicitly_enabled = core.iter().any(|entry| {
        aliases.contains(&normalize_identifier(entry))
            || aliases.contains(&sanitize_pattern_identifier(entry))
    });
    explicitly_enabled && !excluded
}

const SHELL_TOOL_NAMES: &[&str] = &["run_shell_command", "ShellTool"];

/// Match a tool invocation against bare-name or shell-command patterns.
///
/// `tool_names` contains the declared tool name and, when available, its class
/// name. Names and patterns are exact and case-sensitive, matching the source
/// implementation. Shell tools recognize both `run_shell_command` and
/// `ShellTool`; command patterns match the full command or a prefix ending at
/// a space boundary.
pub fn does_tool_invocation_match(
    tool_names: &[&str],
    invocation_params: &Value,
    patterns: &[String],
) -> bool {
    let mut names = tool_names.to_vec();
    if names.iter().any(|name| SHELL_TOOL_NAMES.contains(name)) {
        for shell_name in SHELL_TOOL_NAMES {
            if !names.contains(shell_name) {
                names.push(shell_name);
            }
        }
    }

    for pattern in patterns {
        let Some((pattern_tool_name, _)) = pattern.split_once('(') else {
            if names.contains(&pattern.as_str()) {
                return true;
            }
            continue;
        };
        if !names.contains(&pattern_tool_name) {
            continue;
        }
        let Some(arg_pattern) = pattern.strip_prefix(pattern_tool_name).and_then(|tail| {
            tail.strip_prefix('(')
                .and_then(|tail| tail.strip_suffix(')'))
        }) else {
            continue;
        };
        if !names.contains(&"run_shell_command") {
            continue;
        }
        let Some(command) = invocation_params
            .as_object()
            .and_then(|params| params.get("command"))
        else {
            continue;
        };
        let command = js_string(command);
        if command == arg_pattern || command.starts_with(&format!("{arg_pattern} ")) {
            return true;
        }
    }
    false
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                value => js_string(value),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        TOOL_NAMES, canonical_tool_name, does_tool_invocation_match, get_alias_set_for_tool,
        is_tool_enabled, normalize_identifier,
    };

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn identifier_normalization_trims_whitespace_and_leading_underscores() {
        assert_eq!(normalize_identifier("  ___ShellTool \n"), "ShellTool");
        assert_eq!(normalize_identifier("___"), "");
        assert_eq!(normalize_identifier(" Shell_Tool "), "Shell_Tool");
    }

    #[test]
    fn every_canonical_and_display_name_has_its_expected_aliases() {
        for spec in TOOL_NAMES {
            let aliases = get_alias_set_for_tool(spec.canonical);
            assert!(aliases.contains(spec.canonical), "{}", spec.canonical);
            assert!(aliases.contains(spec.display), "{}", spec.canonical);
            assert!(
                aliases.contains(&format!("{}Tool", spec.display)),
                "{}",
                spec.canonical
            );
        }
    }

    #[test]
    fn legacy_tool_and_display_aliases_map_to_their_canonical_tools() {
        assert_eq!(canonical_tool_name("search_file_content"), "grep_search");
        assert_eq!(canonical_tool_name("replace"), "edit");
        assert_eq!(canonical_tool_name("task"), "agent");
        assert_eq!(canonical_tool_name("SearchFiles"), "SearchFiles");

        assert!(get_alias_set_for_tool("grep_search").contains("search_file_content"));
        assert!(get_alias_set_for_tool("edit").contains("replace"));
        assert!(get_alias_set_for_tool("agent").contains("task"));
        assert!(get_alias_set_for_tool("grep_search").contains("SearchFiles"));
        assert!(get_alias_set_for_tool("glob").contains("FindFiles"));
        assert!(get_alias_set_for_tool("list_directory").contains("ReadFolder"));
        assert!(get_alias_set_for_tool("agent").contains("Task"));
        assert!(get_alias_set_for_tool("todo_write").contains("TodoWrite"));
    }

    #[test]
    fn unknown_tool_alias_set_contains_its_normalized_name_only() {
        assert_eq!(
            get_alias_set_for_tool(" __custom_tool "),
            ["custom_tool".to_owned()].into()
        );
    }

    #[test]
    fn empty_core_list_allows_tools_except_exact_exclusions() {
        assert!(is_tool_enabled("run_shell_command", None, None));
        assert!(is_tool_enabled("run_shell_command", Some(&[]), None));
        assert!(is_tool_enabled(
            "run_shell_command",
            Some(&[]),
            Some(&strings(&["Shell(git status)"]))
        ));
        assert!(!is_tool_enabled(
            "run_shell_command",
            None,
            Some(&strings(&["__ShellTool"]))
        ));
    }

    #[test]
    fn core_tool_aliases_include_legacy_names_and_argument_suffixed_entries() {
        assert!(is_tool_enabled(
            "grep_search",
            Some(&strings(&["search_file_content"])),
            None
        ));
        assert!(is_tool_enabled(
            "glob",
            Some(&strings(&["FindFiles"])),
            None
        ));
        assert!(is_tool_enabled(
            "run_shell_command",
            Some(&strings(&["Shell(git status)"])),
            None
        ));
        assert!(is_tool_enabled(
            "run_shell_command",
            Some(&strings(&["___ShellTool(git status)"])),
            None
        ));
        assert!(!is_tool_enabled(
            "run_shell_command",
            Some(&strings(&["Edit"])),
            None
        ));
    }

    #[test]
    fn exclusions_win_after_explicit_enablement_and_only_match_full_aliases() {
        assert!(!is_tool_enabled(
            "run_shell_command",
            Some(&strings(&["Shell"])),
            Some(&strings(&["ShellTool"]))
        ));
        assert!(is_tool_enabled(
            "run_shell_command",
            None,
            Some(&strings(&["Shell(git status)"]))
        ));
        assert!(!is_tool_enabled(
            "todo_write",
            None,
            Some(&strings(&["TodoWrite"]))
        ));
    }

    #[test]
    fn invocation_matches_bare_declared_and_class_names() {
        let params = json!({ "file": "test.txt" });
        assert!(does_tool_invocation_match(
            &["read_file", "ReadFileTool"],
            &params,
            &strings(&["read_file"])
        ));
        assert!(does_tool_invocation_match(
            &["read_file", "ReadFileTool"],
            &params,
            &strings(&["ReadFileTool"])
        ));
        assert!(!does_tool_invocation_match(
            &["read_file", "ReadFileTool"],
            &params,
            &strings(&["another_tool"])
        ));
    }

    #[test]
    fn shell_command_patterns_match_exact_and_space_delimited_prefixes() {
        assert!(does_tool_invocation_match(
            &["run_shell_command"],
            &json!({ "command": "git status" }),
            &strings(&["ShellTool(git status)"])
        ));
        assert!(does_tool_invocation_match(
            &["run_shell_command"],
            &json!({ "command": "git status -v" }),
            &strings(&["ShellTool(git status)"])
        ));
        assert!(does_tool_invocation_match(
            &["ShellTool"],
            &json!({ "command": "git status -v" }),
            &strings(&["run_shell_command(git status)"])
        ));
        assert!(!does_tool_invocation_match(
            &["run_shell_command"],
            &json!({ "command": "git commitsomething" }),
            &strings(&["ShellTool(git commit)"])
        ));
    }

    #[test]
    fn malformed_patterns_and_non_shell_argument_patterns_do_not_match() {
        let shell_params = json!({ "command": "git status" });
        assert!(!does_tool_invocation_match(
            &["run_shell_command"],
            &shell_params,
            &strings(&["ShellTool(git status"])
        ));
        assert!(!does_tool_invocation_match(
            &["run_shell_command"],
            &shell_params,
            &strings(&["ShellTool(git status))"])
        ));
        assert!(!does_tool_invocation_match(
            &["read_file", "ReadFileTool"],
            &json!({ "command": "git status", "file": "x" }),
            &strings(&["ReadFileTool(x)"])
        ));
    }

    #[test]
    fn invocation_params_are_read_from_json_command_field() {
        assert!(does_tool_invocation_match(
            &["run_shell_command"],
            &json!({ "command": "42" }),
            &strings(&["run_shell_command(42)"])
        ));
        assert!(!does_tool_invocation_match(
            &["run_shell_command"],
            &json!({ "other": "git status" }),
            &strings(&["run_shell_command(git status)"])
        ));
        assert!(!does_tool_invocation_match(
            &["run_shell_command"],
            &json!("git status"),
            &strings(&["run_shell_command(git status)"])
        ));
    }
}
