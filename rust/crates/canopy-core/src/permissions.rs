/*
 * @license
 * Copyright 2025 Google LLC
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fs;
use std::path::{Path, PathBuf};

use globset::{GlobBuilder, GlobMatcher};
use regex::Regex;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PermissionDecision {
    Allow,
    Ask,
    Deny,
    Default,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuleType {
    Allow,
    Ask,
    Deny,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SpecifierKind {
    Command,
    Path,
    Domain,
    Literal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParamMatcher {
    key: String,
    value_pattern: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PermissionRule {
    pub raw: String,
    pub tool_name: String,
    specifier: Option<String>,
    specifier_kind: Option<SpecifierKind>,
    param_matchers: Vec<ParamMatcher>,
    invalid: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PermissionRuleSet {
    pub allow: Vec<PermissionRule>,
    pub ask: Vec<PermissionRule>,
    pub deny: Vec<PermissionRule>,
}

#[derive(Clone, Debug)]
pub struct PermissionCheckContext<'a> {
    pub tool_name: &'a str,
    pub command: Option<&'a str>,
    pub file_path: Option<&'a Path>,
    pub domain: Option<&'a str>,
    pub specifier: Option<&'a str>,
    pub tool_params: Option<&'a Value>,
    pub project_root: &'a Path,
    pub cwd: &'a Path,
}

impl PermissionRuleSet {
    pub fn from_raw(
        allow: impl IntoIterator<Item = String>,
        ask: impl IntoIterator<Item = String>,
        deny: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            allow: parse_rules(allow),
            ask: parse_rules(ask),
            deny: parse_rules(deny),
        }
    }

    pub fn evaluate(&self, context: &PermissionCheckContext<'_>) -> PermissionDecision {
        let commands = context
            .command
            .map(split_compound_command)
            .unwrap_or_default();
        let contexts = if commands.len() > 1 {
            commands
                .iter()
                .map(|command| PermissionCheckContext {
                    command: Some(command),
                    ..context.clone()
                })
                .collect::<Vec<_>>()
        } else {
            vec![context.clone()]
        };

        let mut combined = PermissionDecision::Default;
        for item in &contexts {
            let decision = self.evaluate_single(item);
            combined = more_restrictive(combined, decision);
            if combined == PermissionDecision::Deny {
                return combined;
            }
        }
        combined
    }

    pub fn has_path_or_domain_rule(&self, rule_type: RuleType) -> bool {
        let rules = match rule_type {
            RuleType::Allow => &self.allow,
            RuleType::Ask => &self.ask,
            RuleType::Deny => &self.deny,
        };
        rules.iter().any(|rule| {
            !rule.invalid
                && matches!(
                    rule.specifier_kind
                        .unwrap_or_else(|| specifier_kind(&rule.tool_name)),
                    SpecifierKind::Path | SpecifierKind::Domain
                )
        })
    }

    pub fn has_command_rule(&self, rule_type: RuleType) -> bool {
        let rules = match rule_type {
            RuleType::Allow => &self.allow,
            RuleType::Ask => &self.ask,
            RuleType::Deny => &self.deny,
        };
        rules.iter().any(|rule| {
            !rule.invalid
                && matches!(
                    rule.specifier_kind
                        .unwrap_or_else(|| specifier_kind(&rule.tool_name)),
                    SpecifierKind::Command
                )
        })
    }

    fn evaluate_single(&self, context: &PermissionCheckContext<'_>) -> PermissionDecision {
        if self
            .deny
            .iter()
            .any(|rule| matches_rule(rule, context, true))
        {
            return PermissionDecision::Deny;
        }
        if self
            .ask
            .iter()
            .any(|rule| matches_rule(rule, context, true))
        {
            return PermissionDecision::Ask;
        }
        if self
            .allow
            .iter()
            .any(|rule| matches_rule(rule, context, false))
        {
            return PermissionDecision::Allow;
        }
        PermissionDecision::Default
    }
}

pub fn parse_rules(rules: impl IntoIterator<Item = String>) -> Vec<PermissionRule> {
    rules
        .into_iter()
        .filter(|raw| !raw.trim().is_empty())
        .map(|raw| parse_rule(&raw))
        .collect()
}

pub fn parse_rule(raw: &str) -> PermissionRule {
    let trimmed = raw.trim();
    let Some(open_paren) = trimmed.find('(') else {
        return PermissionRule {
            raw: trimmed.to_owned(),
            tool_name: resolve_tool_name(trimmed),
            specifier: None,
            specifier_kind: None,
            param_matchers: Vec::new(),
            invalid: false,
        };
    };

    let tool_part = trimmed[..open_paren].trim();
    if !trimmed.ends_with(')') {
        return PermissionRule {
            raw: trimmed.to_owned(),
            tool_name: resolve_tool_name(tool_part),
            specifier: None,
            specifier_kind: None,
            param_matchers: Vec::new(),
            invalid: true,
        };
    }

    let tool_name = resolve_tool_name(tool_part);
    let kind = specifier_kind(&tool_name);
    let mut specifier = trimmed[open_paren + 1..trimmed.len() - 1].to_owned();
    if kind == SpecifierKind::Command {
        specifier = specifier.replace(":*", " *");
    }

    let mut matchers = Vec::new();
    if kind == SpecifierKind::Literal && !tool_name.starts_with("mcp__") {
        let mut plain = Vec::new();
        for part in specifier.split(',').map(str::trim) {
            let Some((key, value)) = part.split_once(':') else {
                plain.push(part.to_owned());
                continue;
            };
            if is_identifier(key.trim()) {
                matchers.push(ParamMatcher {
                    key: key.trim().to_owned(),
                    value_pattern: value.trim().to_owned(),
                });
            } else {
                plain.push(part.to_owned());
            }
        }
        if !matchers.is_empty() {
            let joined = plain.join(",").trim().to_owned();
            specifier = joined;
        }
    }

    PermissionRule {
        raw: trimmed.to_owned(),
        tool_name,
        specifier: (!specifier.is_empty()).then_some(specifier),
        specifier_kind: Some(kind),
        param_matchers: matchers,
        invalid: false,
    }
}

fn resolve_tool_name(raw: &str) -> String {
    match raw {
        "run_shell_command" | "Shell" | "ShellTool" | "Bash" => "run_shell_command".to_owned(),
        "edit" | "Edit" | "EditTool" | "replace" => "edit".to_owned(),
        "notebook_edit" | "NotebookEdit" | "NotebookEditTool" => "notebook_edit".to_owned(),
        "write_file" | "WriteFile" | "WriteFileTool" | "Write" => "write_file".to_owned(),
        "read_file" | "ReadFile" | "ReadFileTool" | "Read" => "read_file".to_owned(),
        "grep_search" | "Grep" | "GrepTool" | "search_file_content" | "SearchFiles" => {
            "grep_search".to_owned()
        }
        "glob" | "Glob" | "GlobTool" | "FindFiles" => "glob".to_owned(),
        "list_directory" | "ListFiles" | "ListFilesTool" | "ReadFolder" => {
            "list_directory".to_owned()
        }
        "todo_write" | "TodoList" | "TodoWrite" | "TodoWriteTool" => "todo_write".to_owned(),
        "web_fetch" | "WebFetch" | "WebFetchTool" => "web_fetch".to_owned(),
        "web_search" | "WebSearch" | "WebSearchTool" => "web_search".to_owned(),
        "agent" | "Agent" | "AgentTool" | "task" | "Task" | "TaskTool" => "agent".to_owned(),
        "skill" | "Skill" | "SkillTool" => "skill".to_owned(),
        "monitor" | "Monitor" | "MonitorTool" => "monitor".to_owned(),
        other => other.to_owned(),
    }
}

fn specifier_kind(tool_name: &str) -> SpecifierKind {
    match tool_name {
        "run_shell_command" | "monitor" => SpecifierKind::Command,
        "read_file" | "zoom_image" | "grep_search" | "glob" | "list_directory" | "edit"
        | "write_file" | "notebook_edit" => SpecifierKind::Path,
        "web_fetch" => SpecifierKind::Domain,
        _ => SpecifierKind::Literal,
    }
}

fn matches_rule(
    rule: &PermissionRule,
    context: &PermissionCheckContext<'_>,
    canonical: bool,
) -> bool {
    if rule.invalid {
        return false;
    }
    let context_name = resolve_tool_name(context.tool_name);
    if rule.tool_name.starts_with("mcp__") || context_name.starts_with("mcp__") {
        return rule.specifier.is_none()
            && rule.param_matchers.is_empty()
            && matches_mcp_name(&rule.tool_name, &context_name);
    }
    if !tool_names_match(&rule.tool_name, &context_name) {
        return false;
    }
    if let Some(specifier) = rule.specifier.as_deref() {
        let kind = rule
            .specifier_kind
            .unwrap_or_else(|| specifier_kind(&rule.tool_name));
        let specifier_matches = match kind {
            SpecifierKind::Command => context
                .command
                .is_some_and(|command| matches_command_pattern(specifier, command)),
            SpecifierKind::Path => context.file_path.is_some_and(|file_path| {
                matches_path_pattern(
                    specifier,
                    file_path,
                    context.project_root,
                    context.cwd,
                    canonical,
                )
            }),
            SpecifierKind::Domain => context
                .domain
                .is_some_and(|domain| matches_domain_pattern(specifier, domain)),
            SpecifierKind::Literal => context
                .command
                .or(context.specifier)
                .is_some_and(|value| value == specifier),
        };
        if !specifier_matches {
            return false;
        }
    }
    rule.param_matchers
        .iter()
        .all(|matcher| matches_param(matcher, context.tool_params))
}

fn tool_names_match(rule: &str, context: &str) -> bool {
    if rule == context {
        return true;
    }
    match rule {
        "read_file" => matches!(
            context,
            "read_file" | "zoom_image" | "grep_search" | "glob" | "list_directory"
        ),
        "edit" => matches!(
            context,
            "edit" | "write_file" | "notebook_edit" | "edit_file"
        ),
        "run_shell_command" => context == "monitor",
        _ => false,
    }
}

fn matches_mcp_name(pattern: &str, tool_name: &str) -> bool {
    if pattern == tool_name {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return tool_name.starts_with(prefix);
    }
    let pattern_parts = pattern.split("__").collect::<Vec<_>>();
    let tool_parts = tool_name.split("__").collect::<Vec<_>>();
    pattern_parts.len() == 2
        && tool_parts.len() >= 3
        && pattern_parts[0] == tool_parts[0]
        && pattern_parts[1] == tool_parts[1]
}

fn matches_command_pattern(pattern: &str, command: &str) -> bool {
    let command = strip_leading_assignments(command);
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return command == pattern || command.starts_with(&format!("{pattern} "));
    }
    let mut regex = String::from("(?s)^");
    let mut offset = 0;
    while let Some(relative_star) = pattern[offset..].find('*') {
        let star = offset + relative_star;
        let before = &pattern[offset..star];
        if star > 0 && pattern.as_bytes()[star - 1] == b' ' {
            regex.push_str(&regex::escape(before.strip_suffix(' ').unwrap_or(before)));
            regex.push_str("( .*)?");
        } else {
            regex.push_str(&regex::escape(before));
            regex.push_str(".*");
        }
        offset = star + 1;
    }
    regex.push_str(&regex::escape(&pattern[offset..]));
    regex.push('$');
    Regex::new(&regex).is_ok_and(|matcher| matcher.is_match(&command))
}

fn strip_leading_assignments(command: &str) -> String {
    let trimmed = command.trim();
    let tokens = shell_words(trimmed);
    let Some(tokens) = tokens else {
        return trimmed.to_owned();
    };
    let first_command = tokens
        .iter()
        .take_while(|token| is_assignment(token))
        .count();
    tokens[first_command..].join(" ")
}

fn shell_words(input: &str) -> Option<Vec<String>> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in input.chars() {
        if escaped {
            token.push(character);
            escaped = false;
            started = true;
            continue;
        }
        match (quote, character) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), _) => token.push(character),
            (Some('"'), '\\') => escaped = true,
            (Some('"'), _) => token.push(character),
            (Some(_), _) => token.push(character),
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
                    tokens.push(std::mem::take(&mut token));
                    started = false;
                }
            }
            (None, _) => {
                token.push(character);
                started = true;
            }
        }
    }
    if escaped || quote.is_some() {
        return None;
    }
    if started {
        tokens.push(token);
    }
    Some(tokens)
}

fn is_assignment(token: &str) -> bool {
    let Some((name, _)) = token.split_once('=') else {
        return false;
    };
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

fn split_compound_command(command: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let characters = command.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        if escaped {
            current.push(character);
            escaped = false;
            index += 1;
            continue;
        }
        if quote == Some('\'') {
            current.push(character);
            if character == '\'' {
                quote = None;
            }
            index += 1;
            continue;
        }
        if quote == Some('"') {
            current.push(character);
            if character == '"' {
                quote = None;
            } else if character == '\\' {
                escaped = true;
            }
            index += 1;
            continue;
        }
        if character == '\\' {
            current.push(character);
            escaped = true;
            index += 1;
            continue;
        }
        if character == '\'' || character == '"' {
            quote = Some(character);
            current.push(character);
            index += 1;
            continue;
        }
        let is_operator = matches!(character, ';' | '|' | '&' | '\n');
        if is_operator {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                segments.push(trimmed.to_owned());
            }
            current.clear();
            if matches!(character, '|' | '&')
                && characters
                    .get(index + 1)
                    .is_some_and(|next| *next == character)
            {
                index += 1;
            }
            index += 1;
            continue;
        }
        current.push(character);
        index += 1;
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        segments.push(trimmed.to_owned());
    }
    if segments.is_empty() {
        vec![command.trim().to_owned()]
    } else {
        segments
    }
}

pub fn shell_command_uses_indirection(command: &str) -> bool {
    if ["$(", "`", "<(", ">(", "${"]
        .iter()
        .any(|marker| command.contains(marker))
    {
        return true;
    }
    let Some(tokens) = shell_words(command) else {
        return true;
    };
    let program_index = tokens
        .iter()
        .take_while(|token| is_assignment(token))
        .count();
    let Some(program) = tokens.get(program_index) else {
        return false;
    };
    let program = Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(program)
        .to_ascii_lowercase();
    matches!(
        program.as_str(),
        "sh" | "bash"
            | "dash"
            | "zsh"
            | "fish"
            | "env"
            | "command"
            | "exec"
            | "sudo"
            | "doas"
            | "xargs"
            | "find"
            | "make"
            | "gmake"
            | "python"
            | "python2"
            | "python3"
            | "node"
            | "ruby"
            | "perl"
            | "awk"
            | "busybox"
            | "timeout"
            | "time"
            | "nohup"
            | "setsid"
    )
}

fn matches_path_pattern(
    specifier: &str,
    file_path: &Path,
    project_root: &Path,
    cwd: &Path,
    canonical: bool,
) -> bool {
    let Some(pattern) = resolve_path_pattern(specifier, project_root, cwd) else {
        return false;
    };
    let mut patterns = vec![pattern.clone()];
    if canonical {
        if let Some(real_pattern) = canonicalize_with_missing_tail(Path::new(&pattern)) {
            patterns.push(path_string(&real_pattern));
        }
    }
    let mut paths = vec![path_string(file_path)];
    if canonical {
        if let Some(real_path) = canonicalize_with_missing_tail(file_path) {
            paths.push(path_string(&real_path));
        }
    }
    let matchers = patterns
        .iter()
        .filter_map(|pattern| {
            GlobBuilder::new(pattern)
                .literal_separator(true)
                .backslash_escape(false)
                .build()
                .ok()
                .map(|glob| glob.compile_matcher())
        })
        .collect::<Vec<GlobMatcher>>();
    paths
        .iter()
        .any(|path| matchers.iter().any(|matcher| matcher.is_match(path)))
}

fn resolve_path_pattern(specifier: &str, project_root: &Path, cwd: &Path) -> Option<String> {
    let resolved = if let Some(absolute) = specifier.strip_prefix("//") {
        PathBuf::from(format!("/{absolute}"))
    } else if let Some(relative) = specifier.strip_prefix("~/") {
        let home = std::env::var_os("HOME")?;
        PathBuf::from(home).join(relative)
    } else if let Some(relative) = specifier.strip_prefix('/') {
        project_root.join(relative)
    } else if let Some(relative) = specifier.strip_prefix("./") {
        cwd.join(relative)
    } else {
        cwd.join(specifier)
    };
    Some(path_string(&resolved))
}

fn canonicalize_with_missing_tail(path: &Path) -> Option<PathBuf> {
    let mut current = path.to_path_buf();
    let mut tail = Vec::new();
    loop {
        match fs::canonicalize(&current) {
            Ok(mut canonical_parent) => {
                for item in tail.iter().rev() {
                    canonical_parent.push(item);
                }
                return Some(canonical_parent);
            }
            Err(_) => {
                let name = current.file_name()?.to_os_string();
                tail.push(name);
                if !current.pop() {
                    return None;
                }
            }
        }
    }
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn matches_domain_pattern(specifier: &str, domain: &str) -> bool {
    let pattern = specifier
        .strip_prefix("domain:")
        .unwrap_or(specifier)
        .trim()
        .to_ascii_lowercase();
    let domain = domain.to_ascii_lowercase();
    !pattern.is_empty()
        && !domain.is_empty()
        && (domain == pattern || domain.ends_with(&format!(".{pattern}")))
}

fn matches_param(matcher: &ParamMatcher, params: Option<&Value>) -> bool {
    let Some(value) = params
        .and_then(Value::as_object)
        .and_then(|object| object.get(&matcher.key))
    else {
        return false;
    };
    let actual = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return false,
    };
    wildcard_match(&matcher.value_pattern, &actual)
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.to_lowercase();
    let value = value.to_lowercase();
    if pattern == "*" {
        return true;
    }
    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.len() == 1 {
        return value == pattern;
    }
    let mut position = 0;
    if !parts[0].is_empty() {
        if !value.starts_with(parts[0]) {
            return false;
        }
        position = parts[0].len();
    }
    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        let Some(found) = value[position..].find(part) else {
            return false;
        };
        position += found + part.len();
    }
    let Some(last) = parts.last() else {
        return false;
    };
    last.is_empty() || (value.len().saturating_sub(position) >= last.len() && value.ends_with(last))
}

fn more_restrictive(left: PermissionDecision, right: PermissionDecision) -> PermissionDecision {
    fn priority(decision: PermissionDecision) -> u8 {
        match decision {
            PermissionDecision::Deny => 3,
            PermissionDecision::Ask => 2,
            PermissionDecision::Default => 1,
            PermissionDecision::Allow => 0,
        }
    }
    if priority(right) > priority(left) {
        right
    } else {
        left
    }
}

fn is_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}
