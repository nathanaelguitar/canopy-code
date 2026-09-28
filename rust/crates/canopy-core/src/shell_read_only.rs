//! Bash AST classifier used by scoped managed-memory agents.
//!
//! Source: `packages/core/src/utils/shellAstParser.ts` and its safety helpers.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use regex::Regex;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;
use tree_sitter::{Node, Parser};

use crate::memory::{MemoryShellReadOnlyChecker, ShellReadOnlyFuture};

const READ_ONLY_ROOT_COMMANDS: &[&str] = &[
    "awk", "basename", "cat", "cd", "column", "cut", "df", "dirname", "du", "echo", "find", "git",
    "grep", "head", "less", "ls", "more", "printenv", "printf", "ps", "pwd", "rg", "ripgrep",
    "sed", "sort", "stat", "tail", "tree", "uniq", "wc", "which", "where", "whoami",
];
const READ_ONLY_GIT_SUBCOMMANDS: &[&str] = &[
    "blame",
    "branch",
    "cat-file",
    "diff",
    "grep",
    "log",
    "ls-files",
    "remote",
    "rev-parse",
    "show",
    "status",
    "describe",
];
const WRITE_GIT_SUBCOMMANDS: &[&str] = &[
    "add",
    "am",
    "checkout",
    "cherry-pick",
    "clean",
    "clone",
    "commit",
    "fetch",
    "gc",
    "init",
    "merge",
    "mv",
    "pull",
    "push",
    "rebase",
    "reset",
    "restore",
    "revert",
    "rm",
    "stash",
    "switch",
];
const WRITE_GIT_REMOTE_ACTIONS: &[&str] = &[
    "add",
    "remove",
    "rm",
    "rename",
    "set-branches",
    "set-head",
    "set-url",
    "update",
];
const GIT_COMMIT_VALUE_OPTIONS: &[&str] = &[
    "-C",
    "-c",
    "-F",
    "-m",
    "-t",
    "--author",
    "--cleanup",
    "--date",
    "--file",
    "--fixup",
    "--message",
    "--pathspec-from-file",
    "--reedit-message",
    "--reuse-message",
    "--squash",
    "--template",
    "--trailer",
];
const UNIQ_VALUE_OPTIONS: &[&str] = &[
    "-f",
    "--skip-fields",
    "-s",
    "--skip-chars",
    "-w",
    "--check-chars",
];
const WRITE_REDIRECT_OPERATORS: &[&str] = &[">", ">>", "&>", "&>>", ">|"];
const BLOCKED_FIND_PREFIXES: &[&str] = &["-fls", "-fprint", "-fprintf"];
const SED_VALUE_OPTIONS: &[&str] = &["-f", "--file", "-e", "--expression", "-l", "--line-length"];
const SAFE_SED_COMMAND: &str = "^[dDgGhHlnNpPqQxz=]$";
const SAFE_SUBSTITUTION_FLAGS: &str = "^[0-9gIpM]*$";
const SAFE_SED_OPTION: &str = "^(?:-[nElrsuz]|--(?:quiet|silent|line-length(?:=.*)?))$";
const AWK_UNKNOWN_OPERATION: &str = r"(?:system|close)\s*\(|getline\b";
const AWK_PRINT: &str = r"\b(?:print|printf)\b";
const MAX_GIT_CONFIG_OUTPUT: usize = 64 * 1024;
const GIT_CONFIG_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellCommandSafety {
    ReadOnly,
    Write,
    Unknown,
}

impl ShellCommandSafety {
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::Write, _) | (_, Self::Write) => Self::Write,
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            _ => Self::ReadOnly,
        }
    }
}

/// Remove a leading shell wrapper using the same token rules as the source.
pub fn strip_shell_wrapper(command: &str) -> String {
    let trimmed = js_trim(command);
    let mut rest = trimmed.clone();
    loop {
        let Some(token) = take_leading_token(&rest) else {
            break;
        };
        if !is_env_assignment_token(&token.token) {
            break;
        }
        rest = token.rest;
    }

    let Some(wrapper) = take_leading_token(&rest) else {
        return trimmed;
    };
    if !is_known_monitor_wrapper_token(&wrapper.token) {
        return trimmed;
    }
    let wrapper_token = wrapper.token;
    rest = wrapper.rest;

    loop {
        let Some(token) = take_leading_token(&rest) else {
            return trimmed;
        };
        if is_monitor_command_marker(&wrapper_token, &token.token) {
            let Some(command_token) = take_leading_token(&token.rest) else {
                return trimmed;
            };
            let (inner_command, quote) = strip_symmetric_quotes(&command_token.token);
            if quote.is_none() && shell_wrapper_command_consumes_rest(&wrapper_token) {
                let rest_after_marker = js_trim_start(&token.rest);
                return if rest_after_marker.is_empty() {
                    trimmed
                } else {
                    rest_after_marker.to_owned()
                };
            }
            return if inner_command.is_empty() {
                trimmed
            } else {
                inner_command
            };
        }

        let normalized = get_normalized_shell_token(&token.token);
        if !is_shell_wrapper_flag_token(&normalized) {
            return trimmed;
        }
        rest = token.rest;
        if shell_wrapper_flag_consumes_operand(&token.token) {
            let Some(operand) = take_leading_token(&rest) else {
                return trimmed;
            };
            rest = operand.rest;
        }
    }
}

/// Classify a command with Bash syntax and the active working directory.
/// Empty input, syntax errors, and parser failures are unknown and therefore
/// are denied by the memory-scoped permission policy.
pub async fn classify_shell_command_safety_in_directory(
    command: &str,
    cwd: &Path,
) -> ShellCommandSafety {
    if js_trim(command).is_empty() {
        return ShellCommandSafety::Unknown;
    }
    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .is_err()
    {
        return ShellCommandSafety::Unknown;
    }
    let Some(tree) = parser.parse(command, None) else {
        return ShellCommandSafety::Unknown;
    };
    let root = tree.root_node();
    if root.named_child_count() == 0 || root.has_error() {
        return ShellCommandSafety::Unknown;
    }

    let mut safety = ShellCommandSafety::ReadOnly;
    for child in named_children(root) {
        safety = safety.merge(evaluate_statement_safety(child, command));
    }
    if safety == ShellCommandSafety::ReadOnly && command.contains("\\\n") {
        let normalized = command.replace("\\\n", "");
        if classify_without_git_config(&normalized) != ShellCommandSafety::ReadOnly {
            return ShellCommandSafety::Unknown;
        }
    }
    if safety != ShellCommandSafety::ReadOnly {
        return safety;
    }
    if local_git_config_makes_command_unsafe(root, command, cwd).await {
        ShellCommandSafety::Unknown
    } else {
        ShellCommandSafety::ReadOnly
    }
}

/// Memory-scoped shell checks permit only commands classified read-only.
#[derive(Clone, Copy, Debug, Default)]
pub struct AstMemoryShellReadOnlyChecker;

impl MemoryShellReadOnlyChecker for AstMemoryShellReadOnlyChecker {
    fn strip_shell_wrapper(&self, command: &str) -> String {
        strip_shell_wrapper(command)
    }

    fn is_read_only_ast_in_directory<'a>(
        &'a self,
        command: &'a str,
        directory: &'a Path,
    ) -> ShellReadOnlyFuture<'a> {
        Box::pin(async move {
            classify_shell_command_safety_in_directory(command, directory).await
                == ShellCommandSafety::ReadOnly
        })
    }
}

#[derive(Debug)]
struct LeadingToken {
    token: String,
    rest: String,
}

fn take_leading_token(input: &str) -> Option<LeadingToken> {
    let trimmed = js_trim_start(input);
    if trimmed.is_empty() {
        return None;
    }
    let chars = trimmed.chars().collect::<Vec<_>>();
    let mut quote = None;
    let mut escaped = false;
    let mut in_backticks = false;
    let mut command_substitution_depth = 0usize;
    let mut idx = 0usize;
    while idx < chars.len() {
        let ch = chars[idx];
        if quote == Some('\'') {
            if ch == '\'' {
                quote = None;
            }
            idx += 1;
            continue;
        }
        if quote == Some('"') {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                quote = None;
            }
            idx += 1;
            continue;
        }
        if in_backticks {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '`' {
                in_backticks = false;
            }
            idx += 1;
            continue;
        }
        if escaped {
            escaped = false;
            idx += 1;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            idx += 1;
            continue;
        }
        if ch == '"' || ch == '\'' {
            quote = Some(ch);
            idx += 1;
            continue;
        }
        if ch == '`' {
            in_backticks = true;
            idx += 1;
            continue;
        }
        if matches!(ch, '$' | '<' | '>') && chars.get(idx + 1) == Some(&'(') {
            command_substitution_depth += 1;
            idx += 2;
            continue;
        }
        if ch == ')' && command_substitution_depth > 0 {
            command_substitution_depth -= 1;
            idx += 1;
            continue;
        }
        if js_whitespace(ch) && command_substitution_depth == 0 {
            break;
        }
        idx += 1;
    }
    if idx == 0 || quote.is_some() || escaped || in_backticks || command_substitution_depth > 0 {
        return None;
    }
    Some(LeadingToken {
        token: chars[..idx].iter().collect(),
        rest: chars[idx..].iter().collect(),
    })
}

fn strip_symmetric_quotes(value: &str) -> (String, Option<char>) {
    let trimmed = js_trim(value);
    let chars = trimmed.chars().collect::<Vec<_>>();
    if chars.len() >= 2
        && ((chars[0] == '"' && chars[chars.len() - 1] == '"')
            || (chars[0] == '\'' && chars[chars.len() - 1] == '\''))
    {
        return (chars[1..chars.len() - 1].iter().collect(), Some(chars[0]));
    }
    (trimmed, None)
}

fn get_normalized_shell_token(token: &str) -> String {
    let (value, _) = strip_symmetric_quotes(token);
    value.replace('\\', "/").to_lowercase()
}

fn is_env_assignment_token(token: &str) -> bool {
    let (value, _) = strip_symmetric_quotes(token);
    let Some((name, _)) = value.split_once('=') else {
        return false;
    };
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn get_shell_wrapper_base(token: &str) -> String {
    get_normalized_shell_token(token)
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_owned()
}

fn is_known_monitor_wrapper_token(token: &str) -> bool {
    matches!(
        get_shell_wrapper_base(token).as_str(),
        "sh" | "sh.exe"
            | "bash"
            | "bash.exe"
            | "zsh"
            | "zsh.exe"
            | "cmd"
            | "cmd.exe"
            | "powershell"
            | "powershell.exe"
            | "pwsh"
            | "pwsh.exe"
    )
}

fn is_shell_wrapper_flag_token(token: &str) -> bool {
    token.starts_with('-') || token.starts_with('/') || token == "+o"
}

fn option_has_inline_value(token: &str) -> bool {
    token.contains('=') || token.contains(':')
}

fn shell_wrapper_flag_consumes_operand(token: &str) -> bool {
    if option_has_inline_value(token) {
        return false;
    }
    matches!(
        get_normalized_shell_token(token).as_str(),
        "-o" | "+o" | "-executionpolicy" | "-file" | "-encodedcommand"
    )
}

fn shell_wrapper_command_consumes_rest(wrapper_token: &str) -> bool {
    matches!(
        get_shell_wrapper_base(wrapper_token).as_str(),
        "cmd" | "cmd.exe" | "powershell" | "powershell.exe" | "pwsh" | "pwsh.exe"
    )
}

fn is_monitor_command_marker(wrapper_token: &str, token: &str) -> bool {
    let base = get_shell_wrapper_base(wrapper_token);
    let normalized = get_normalized_shell_token(token);
    match base.as_str() {
        "cmd" | "cmd.exe" => normalized == "/c",
        "powershell" | "powershell.exe" | "pwsh" | "pwsh.exe" => {
            normalized == "-command" || normalized == "-c"
        }
        _ => {
            normalized == "-c"
                || Regex::new(r"^-[a-z]*c[a-z]*$")
                    .expect("valid shell option regex")
                    .is_match(&normalized)
        }
    }
}

fn js_whitespace(ch: char) -> bool {
    ch.is_whitespace() || ch == '\u{feff}'
}

fn js_trim(value: &str) -> String {
    value.trim_matches(js_whitespace).to_owned()
}

fn js_trim_start(value: &str) -> &str {
    value.trim_start_matches(js_whitespace)
}

fn is_match(pattern: &str, value: &str) -> bool {
    Regex::new(pattern)
        .expect("valid shell safety regex")
        .is_match(value)
}

fn node_text<'source>(node: Node<'_>, source: &'source str) -> &'source str {
    &source[node.byte_range()]
}

fn named_children(node: Node<'_>) -> impl Iterator<Item = Node<'_>> {
    (0..node.named_child_count()).filter_map(move |idx| node.named_child(idx))
}

fn children(node: Node<'_>) -> impl Iterator<Item = Node<'_>> {
    (0..node.child_count()).filter_map(move |idx| node.child(idx))
}

fn collect_descendants<'tree>(
    node: Node<'tree>,
    types: &[&str],
    outermost_only: bool,
) -> Vec<Node<'tree>> {
    let mut result = Vec::new();
    let mut stack = vec![node];
    while let Some(current) = stack.pop() {
        if types.contains(&current.kind()) {
            result.push(current);
            if outermost_only {
                continue;
            }
        }
        let child_nodes = children(current).collect::<Vec<_>>();
        stack.extend(child_nodes.into_iter().rev());
    }
    result
}

fn command_name<'source>(node: Node<'_>, source: &'source str) -> Option<&'source str> {
    node.child_by_field_name("name")
        .map(|name| node_text(name, source))
}

fn argument_nodes(node: Node<'_>) -> Vec<Node<'_>> {
    (0..node.child_count())
        .filter(|idx| node.field_name_for_child(*idx as u32) == Some("argument"))
        .filter_map(|idx| node.child(idx))
        .collect()
}

fn strip_outer_quotes(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() >= 2
        && ((chars[0] == '\'' && chars[chars.len() - 1] == '\'')
            || (chars[0] == '"' && chars[chars.len() - 1] == '"'))
    {
        chars[1..chars.len() - 1].iter().collect()
    } else {
        value.to_owned()
    }
}

fn has_shell_pattern_expansion(value: &str) -> bool {
    if value.chars().any(|ch| matches!(ch, '[' | '*' | '?')) {
        return true;
    }
    let mut brace_depth = 0usize;
    let mut previous_dot = false;
    for ch in value.chars() {
        if ch == '{' {
            brace_depth += 1;
            previous_dot = false;
        } else if ch == '}' {
            brace_depth = brace_depth.saturating_sub(1);
            previous_dot = false;
        } else if brace_depth > 0 {
            if ch == ',' || (ch == '.' && previous_dot) {
                return true;
            }
            previous_dot = ch == '.';
        }
    }
    false
}

fn has_shell_expansion(node: Node<'_>, source: &str) -> bool {
    !collect_descendants(
        node,
        &["simple_expansion", "expansion", "arithmetic_expansion"],
        false,
    )
    .is_empty()
        || (matches!(node.kind(), "word" | "concatenation")
            && has_shell_pattern_expansion(node_text(node, source)))
}

fn before_terminator(args: &[String]) -> &[String] {
    args.split(|arg| arg == "--").next().unwrap_or(args)
}

fn has_help(args: &[String], value_options: &[&str]) -> bool {
    let args = before_terminator(args);
    args.iter().enumerate().any(|(index, arg)| {
        is_match(r"(?i)^(?:--help|--version)$", arg)
            && !index
                .checked_sub(1)
                .and_then(|previous| args.get(previous))
                .is_some_and(|previous| value_options.contains(&previous.as_str()))
    })
}

fn without_option_values(args: &[String], value_option: impl Fn(&str) -> bool) -> Vec<String> {
    let mut result = Vec::new();
    let mut index = 0;
    while index < args.len() {
        result.push(args[index].clone());
        if value_option(&args[index]) {
            index += 1;
        }
        index += 1;
    }
    result
}

fn evaluate_output_option(args: &[String], long: bool, short: bool) -> Option<ShellCommandSafety> {
    for (index, arg) in args.iter().enumerate() {
        if arg == "--" {
            break;
        }
        if (short && arg == "-o") || (long && arg == "--output") {
            return Some(
                if args.get(index + 1).is_some_and(|value| !value.is_empty()) {
                    ShellCommandSafety::Write
                } else {
                    ShellCommandSafety::Unknown
                },
            );
        }
        if short && arg.starts_with("-o") && arg.len() > 2 {
            return Some(ShellCommandSafety::Write);
        }
        if long && arg.starts_with("--output=") {
            return Some(if arg.len() > 9 {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            });
        }
    }
    None
}

fn evaluate_git_safety(args: &[String]) -> ShellCommandSafety {
    let Some(first) = args.first() else {
        return ShellCommandSafety::ReadOnly;
    };
    if first == "--version" {
        return ShellCommandSafety::ReadOnly;
    }
    if first == "--help" {
        return if args.len() == 1 {
            ShellCommandSafety::ReadOnly
        } else {
            ShellCommandSafety::Unknown
        };
    }
    if first.starts_with('-') {
        return ShellCommandSafety::Unknown;
    }
    let subcommand = first.to_lowercase();
    let rest = &args[1..];
    let options = before_terminator(rest);
    let invokes_helper = options.iter().any(|arg| {
        is_match(
            r"^--(?:ext-diff|filters|show-signature|textconv|open-files-in-pager)(?:=|$)",
            arg,
        )
    }) || (subcommand == "grep"
        && options.iter().any(|arg| arg.starts_with("-O")))
        || (["log", "show"].contains(&subcommand.as_str())
            && options.iter().any(|arg| is_match(r"%G[?GKFPST]", arg)));

    if WRITE_GIT_SUBCOMMANDS.contains(&subcommand.as_str()) {
        let effective_args = if subcommand == "commit" {
            without_option_values(rest, |arg| GIT_COMMIT_VALUE_OPTIONS.contains(&arg))
        } else {
            rest.to_vec()
        };
        let effective_options = before_terminator(&effective_args);
        let help = has_help(&effective_args, &[]);
        let dry_run = effective_options.iter().any(|arg| arg == "--dry-run")
            || (effective_options.iter().any(|arg| arg == "-n")
                && ["add", "clean", "mv", "push", "rm"].contains(&subcommand.as_str()));
        return if help || dry_run {
            ShellCommandSafety::Unknown
        } else {
            ShellCommandSafety::Write
        };
    }
    if !READ_ONLY_GIT_SUBCOMMANDS.contains(&subcommand.as_str()) {
        return ShellCommandSafety::Unknown;
    }
    if ["diff", "log", "show"].contains(&subcommand.as_str()) {
        if let Some(output) = evaluate_output_option(rest, true, false) {
            return output;
        }
    }
    if subcommand == "blame"
        && before_terminator(rest)
            .iter()
            .any(|arg| is_match(r"^--output(?:=|$)", arg))
    {
        return ShellCommandSafety::Unknown;
    }
    if subcommand != "branch" && has_help(rest, &[]) {
        return ShellCommandSafety::Unknown;
    }
    if subcommand == "remote" {
        let action = rest
            .iter()
            .find(|arg| !arg.starts_with('-'))
            .map(|arg| arg.to_lowercase());
        let Some(action) = action else {
            return if invokes_helper {
                ShellCommandSafety::Unknown
            } else {
                ShellCommandSafety::ReadOnly
            };
        };
        if action == "show" || action == "get-url" {
            return if rest.iter().any(|arg| {
                is_match(
                    r"(?i)^(?:add|remove|rm|rename|set-branches|set-head|set-url|update|prune)$",
                    arg,
                )
            }) || invokes_helper
            {
                ShellCommandSafety::Unknown
            } else {
                ShellCommandSafety::ReadOnly
            };
        }
        if WRITE_GIT_REMOTE_ACTIONS.contains(&action.as_str()) {
            return ShellCommandSafety::Write;
        }
        if action == "prune" {
            return if rest.iter().any(|arg| arg == "-n" || arg == "--dry-run") {
                ShellCommandSafety::Unknown
            } else {
                ShellCommandSafety::Write
            };
        }
        return ShellCommandSafety::Unknown;
    }
    if subcommand == "branch" {
        let actions = without_option_values(rest, |arg| is_match(r"^--(?:format|sort)$", arg));
        let action_options = before_terminator(&actions);
        if has_help(&actions, &[]) {
            return ShellCommandSafety::Unknown;
        }
        let is_write_flag = |arg: &str| {
            is_match(
                r"^(?:-[cCdDmMu](?:.|$)|--(?:delete|move|copy|set-upstream(?:-to)?|unset-upstream|create-reflog|edit-description)(?:=|$))",
                arg,
            )
        };
        if actions.iter().any(|arg| is_write_flag(arg)) {
            return if action_options.iter().any(|arg| is_write_flag(arg)) {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            };
        }
        if actions.len() != rest.len() {
            return ShellCommandSafety::Unknown;
        }
        if action_options.iter().any(|arg| {
            is_match(
                r"^(?:-[alr]|--(?:all|list|remotes|show-current|contains|no-contains|merged|no-merged|points-at))(?:=|$)",
                arg,
            )
        }) {
            return ShellCommandSafety::ReadOnly;
        }
        if rest.iter().any(|arg| !arg.starts_with('-')) {
            return ShellCommandSafety::Write;
        }
        if rest.iter().any(|arg| arg == "--") || invokes_helper {
            return ShellCommandSafety::Unknown;
        }
        return if rest.is_empty() {
            ShellCommandSafety::ReadOnly
        } else {
            ShellCommandSafety::Unknown
        };
    }
    if invokes_helper {
        ShellCommandSafety::Unknown
    } else {
        ShellCommandSafety::ReadOnly
    }
}

fn evaluate_find_safety(args: &[String]) -> ShellCommandSafety {
    let mut result = ShellCommandSafety::ReadOnly;
    let mut index = 0;
    while index < args.len() {
        let lower = args[index].to_lowercase();
        if lower == "--" {
            return result.merge(ShellCommandSafety::Unknown);
        }
        if is_match(r"^--(?:help|version)$", &lower) {
            return ShellCommandSafety::Unknown;
        }
        if is_match(
            r"^-(?:[ac]?newer|newer[a-z]{2}|[acm](?:min|time)|context|fstype|gid|group|i?(?:lname|name|path|regex)|inum|links|maxdepth|mindepth|path|perm|printf|regextype|samefile|size|type|uid|used|user|wholename|xtype)$",
            &lower,
        ) {
            index += 1;
            if args
                .get(index)
                .is_none_or(|value| !is_match(r"^[^-]", value))
            {
                result = result.merge(ShellCommandSafety::Unknown);
            }
            index += 1;
            continue;
        }
        if lower == "-delete" {
            result = ShellCommandSafety::Write;
            index += 1;
            continue;
        }
        if let Some(prefix) = BLOCKED_FIND_PREFIXES
            .iter()
            .find(|prefix| lower.starts_with(**prefix))
        {
            result = ShellCommandSafety::Write;
            index += if *prefix == "-fprintf" { 3 } else { 2 };
            continue;
        }
        if ["-exec", "-execdir", "-ok", "-okdir"].contains(&lower.as_str()) {
            let invoked = args.get(index + 1).map(|arg| arg.to_lowercase());
            let end = (index + 2..args.len())
                .find(|candidate| [";", "\\;", "+"].contains(&args[*candidate].as_str()));
            let invoked_args = args[index + 2..end.unwrap_or(args.len())].to_vec();
            let mut nested = ShellCommandSafety::Unknown;
            if let Some(invoked) = invoked.as_deref() {
                if is_write_root_command(invoked) {
                    nested = if has_help(&invoked_args, &[]) {
                        ShellCommandSafety::Unknown
                    } else {
                        ShellCommandSafety::Write
                    };
                } else if ["kill", "killall", "pkill"].contains(&invoked) {
                    nested = process_safety(invoked, &invoked_args);
                }
            }
            result = result.merge(nested);
            index = end.map_or(args.len(), |value| value + 1);
            continue;
        }
        index += 1;
    }
    result
}

fn is_write_root_command(root: &str) -> bool {
    matches!(
        root,
        "chgrp"
            | "chmod"
            | "chown"
            | "cp"
            | "install"
            | "ln"
            | "mkdir"
            | "mkfifo"
            | "mknod"
            | "mv"
            | "rename"
            | "rm"
            | "rmdir"
            | "shred"
            | "touch"
            | "truncate"
            | "unlink"
    )
}

fn process_safety(root: &str, args: &[String]) -> ShellCommandSafety {
    let options = before_terminator(args);
    let signal_value_options = match root {
        "pkill" => vec!["--signal"],
        "kill" => vec!["--signal", "-s", "-n"],
        _ => vec!["--signal", "-s"],
    };
    if args.is_empty()
        || has_help(args, &[])
        || options
            .iter()
            .any(|arg| ["-h", "-V", "-help", "-version"].contains(&arg.as_str()))
    {
        return ShellCommandSafety::Unknown;
    }
    let zero = |arg: &str| is_match(r"(?i)^(?:SIG)?0+$", arg);
    if options.iter().enumerate().any(|(index, arg)| {
        (is_match(r"[$`*?()\[\]{}]", arg)
            && (arg.starts_with('-')
                || index
                    .checked_sub(1)
                    .and_then(|previous| options.get(previous))
                    .is_some_and(|previous| signal_value_options.contains(&previous.as_str()))))
            || is_match(r"^-(?:[lL0]|-(?:.*list|table)(?:=|$))", arg)
            || (arg.starts_with("--signal=") && zero(&arg[9..]))
            || is_match(r"(?i)^-(?:SIG)?0+$", arg)
            || (root == "kill" && is_match(r"(?i)^-[sn](?:SIG)?0+$", arg))
            || (root == "killall" && is_match(r"(?i)^-s(?:SIG)?0+$", arg))
            || (index > 0
                && signal_value_options.contains(&options[index - 1].as_str())
                && zero(arg))
    }) {
        ShellCommandSafety::Unknown
    } else {
        ShellCommandSafety::Write
    }
}

fn evaluate_substitutions(node: Node<'_>, source: &str) -> ShellCommandSafety {
    let substitutions = collect_descendants(
        node,
        &["command_substitution", "process_substitution"],
        true,
    );
    if substitutions.is_empty() {
        for expansion in collect_descendants(node, &["expansion"], false) {
            for index in 0..expansion.child_count().saturating_sub(1) {
                if expansion
                    .child(index)
                    .is_some_and(|child| child.kind() == "@")
                    && expansion
                        .child(index + 1)
                        .is_some_and(|child| child.kind() == "P")
                {
                    return ShellCommandSafety::Unknown;
                }
            }
        }
        return ShellCommandSafety::ReadOnly;
    }
    let mut result = ShellCommandSafety::Unknown;
    for substitution in substitutions {
        for child in named_children(substitution) {
            result = result.merge(evaluate_statement_safety(child, source));
        }
    }
    result
}

fn evaluate_command_safety(node: Node<'_>, source: &str) -> ShellCommandSafety {
    let raw_root = command_name(node, source);
    let root = raw_root.map(str::to_lowercase);
    let args = argument_nodes(node)
        .into_iter()
        .map(|arg| strip_outer_quotes(node_text(arg, source)))
        .collect::<Vec<_>>();
    let mut result = match (raw_root, root.as_deref()) {
        (None, _) => ShellCommandSafety::ReadOnly,
        (Some(raw), Some(root)) if raw != root => ShellCommandSafety::Unknown,
        (_, Some(root)) if is_write_root_command(root) => {
            if has_help(&args, &[]) {
                ShellCommandSafety::Unknown
            } else {
                ShellCommandSafety::Write
            }
        }
        (_, Some(root @ ("kill" | "killall" | "pkill"))) => process_safety(root, &args),
        (_, Some("git")) => evaluate_git_safety(&args),
        (_, Some("find")) => evaluate_find_safety(&args),
        (_, Some("sed")) => classify_sed_command_safety(&args),
        (_, Some("awk")) => classify_awk_command_safety(&args),
        (_, Some(root @ ("sort" | "tree"))) => {
            let mut safety = evaluate_output_option(&args, root == "sort", true)
                .unwrap_or(ShellCommandSafety::ReadOnly);
            if has_help(&args, &["-o", "--output"]) {
                safety = ShellCommandSafety::Unknown;
            }
            if before_terminator(&args).iter().any(|arg| {
                is_match(r"^(?:--o|-[^-]+o)", arg) || (root == "sort" && arg.starts_with("--co"))
            }) {
                safety = safety.merge(ShellCommandSafety::Unknown);
            }
            safety
        }
        (_, Some("uniq")) => {
            if has_help(&args, &[]) {
                ShellCommandSafety::Unknown
            } else {
                evaluate_uniq_safety(&args)
            }
        }
        (_, Some("tee")) => {
            if args.iter().enumerate().any(|(index, arg)| {
                !arg.starts_with('-')
                    || index
                        .checked_sub(1)
                        .and_then(|i| args.get(i))
                        .is_some_and(|previous| previous == "--")
            }) {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            }
        }
        (_, Some("dd")) => {
            if args.iter().any(|arg| arg.starts_with("of=")) {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            }
        }
        (_, Some("printf"))
            if before_terminator(&args)
                .iter()
                .any(|arg| is_match(r"^-[^-]*v", arg)) =>
        {
            ShellCommandSafety::Unknown
        }
        (_, Some("less" | "more")) => ShellCommandSafety::Unknown,
        (_, Some("rg" | "ripgrep"))
            if before_terminator(&args).iter().any(|arg| {
                is_match(
                    r"^(?:--(?:hostname-bin|pre)(?:=|$)|--search-zip$|-[^-]*z)",
                    arg,
                )
            }) =>
        {
            ShellCommandSafety::Unknown
        }
        (_, Some(root)) if READ_ONLY_ROOT_COMMANDS.contains(&root) => ShellCommandSafety::ReadOnly,
        _ => ShellCommandSafety::Unknown,
    };

    let root = root.as_deref();
    if result == ShellCommandSafety::ReadOnly
        && root.is_some_and(|root| {
            [
                "awk", "find", "git", "printf", "rg", "ripgrep", "sed", "sort", "tree", "uniq",
            ]
            .contains(&root)
        })
        && argument_nodes(node)
            .iter()
            .any(|arg| has_shell_expansion(*arg, source))
    {
        result = ShellCommandSafety::Unknown;
    }
    if result == ShellCommandSafety::Write
        && !root.is_some_and(|root| ["find", "git", "sed", "sort", "tree"].contains(&root))
        && has_help(&args, &[])
    {
        result = ShellCommandSafety::Unknown;
    }
    let has_environment = named_children(node).any(|child| child.kind() == "variable_assignment");
    if root.is_some() && has_environment {
        result = result.merge(ShellCommandSafety::Unknown);
    }
    result = result.merge(evaluate_redirection_safety(node, source));
    for child in named_children(node).filter(|child| !child.kind().ends_with("_redirect")) {
        result = result.merge(evaluate_substitutions(child, source));
    }
    result
}

fn evaluate_uniq_safety(args: &[String]) -> ShellCommandSafety {
    let mut positional = 0usize;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            return if args.len() - index + positional > 2 {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::ReadOnly
            };
        }
        if UNIQ_VALUE_OPTIONS.contains(&arg.as_str()) {
            if args.get(index + 1).is_none_or(|next| next.is_empty()) {
                return ShellCommandSafety::Unknown;
            }
            index += 2;
            continue;
        } else if arg == "-" || !arg.starts_with('-') {
            positional += 1;
        }
        index += 1;
    }
    if positional >= 2 {
        ShellCommandSafety::Write
    } else {
        ShellCommandSafety::ReadOnly
    }
}

fn evaluate_redirection_safety(node: Node<'_>, source: &str) -> ShellCommandSafety {
    let mut result = ShellCommandSafety::ReadOnly;
    for redirect in named_children(node).filter(|child| child.kind().ends_with("_redirect")) {
        result = result.merge(evaluate_substitutions(redirect, source));
        if redirect.kind() != "file_redirect" {
            continue;
        }
        let operator = children(redirect).find(|child| child.kind() != "file_descriptor");
        let Some(operator) = operator else {
            return ShellCommandSafety::Unknown;
        };
        if WRITE_REDIRECT_OPERATORS.contains(&operator.kind()) {
            return ShellCommandSafety::Write;
        }
        if operator.kind() == ">&" {
            let Some(destination) = redirect.child_by_field_name("destination") else {
                return ShellCommandSafety::Unknown;
            };
            let target = strip_outer_quotes(node_text(destination, source));
            if is_match(r"^(?:\d+|-)$", &target) {
                continue;
            }
            result = result.merge(if is_match(r"[$`*?()[\]{}]", &target) {
                ShellCommandSafety::Unknown
            } else {
                ShellCommandSafety::Write
            });
        }
    }
    result
}

fn children_safety(node: Node<'_>, source: &str, floor: ShellCommandSafety) -> ShellCommandSafety {
    named_children(node).fold(floor, |safety, child| {
        safety.merge(evaluate_statement_safety(child, source))
    })
}

fn evaluate_statement_safety(node: Node<'_>, source: &str) -> ShellCommandSafety {
    match node.kind() {
        "command" => evaluate_command_safety(node, source),
        "pipeline" | "list" | "subshell" | "compound_statement" | "negated_command" => {
            children_safety(node, source, ShellCommandSafety::ReadOnly)
        }
        "redirected_statement" => {
            let mut result = ShellCommandSafety::ReadOnly;
            for child in named_children(node).filter(|child| !child.kind().ends_with("_redirect")) {
                result = result.merge(evaluate_statement_safety(child, source));
            }
            result.merge(evaluate_redirection_safety(node, source))
        }
        "variable_assignment" | "variable_assignments" => {
            let floor = if node
                .parent()
                .is_some_and(|parent| parent.named_child_count() == 1)
            {
                ShellCommandSafety::ReadOnly
            } else {
                ShellCommandSafety::Unknown
            };
            floor.merge(evaluate_substitutions(node, source))
        }
        "function_definition" => ShellCommandSafety::Unknown,
        _ => children_safety(node, source, ShellCommandSafety::Unknown),
    }
}

fn classify_without_git_config(command: &str) -> ShellCommandSafety {
    if js_trim(command).is_empty() {
        return ShellCommandSafety::Unknown;
    }
    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .is_err()
    {
        return ShellCommandSafety::Unknown;
    }
    let Some(tree) = parser.parse(command, None) else {
        return ShellCommandSafety::Unknown;
    };
    let root = tree.root_node();
    if root.named_child_count() == 0 || root.has_error() {
        return ShellCommandSafety::Unknown;
    }
    named_children(root).fold(ShellCommandSafety::ReadOnly, |safety, child| {
        safety.merge(evaluate_statement_safety(child, command))
    })
}

#[derive(Default)]
struct GitConfigRisk {
    diff_external: bool,
    fsmonitor: bool,
}

fn probe_failed_risk() -> GitConfigRisk {
    GitConfigRisk {
        diff_external: true,
        fsmonitor: true,
    }
}

async fn local_git_config_risk(cwd: &Path) -> GitConfigRisk {
    if !tokio::fs::metadata(cwd)
        .await
        .is_ok_and(|metadata| metadata.is_dir())
    {
        return GitConfigRisk::default();
    }
    let mut child = match Command::new("git")
        .arg("-C")
        .arg(cwd)
        .args([
            "config",
            "--includes",
            "--show-scope",
            "--null",
            "--get-regexp",
            r"^diff\.external$|^core\.fsmonitor$",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return probe_failed_risk(),
    };
    let Some(stdout) = child.stdout.take() else {
        return probe_failed_risk();
    };
    let probe = timeout(GIT_CONFIG_TIMEOUT, async {
        let mut output = Vec::new();
        stdout
            .take((MAX_GIT_CONFIG_OUTPUT + 1) as u64)
            .read_to_end(&mut output)
            .await?;
        if output.len() > MAX_GIT_CONFIG_OUTPUT {
            let _ = child.kill().await;
            return Err(ErrorKind::InvalidData.into());
        }
        let status = child.wait().await?;
        Ok::<_, std::io::Error>((output, status))
    })
    .await;
    let (output, status) = match probe {
        Ok(Ok(value)) => value,
        _ => return probe_failed_risk(),
    };
    if status.code() == Some(1) {
        return GitConfigRisk::default();
    }
    if !status.success() {
        return probe_failed_risk();
    }
    let output = String::from_utf8_lossy(&output);
    let fields = output.split('\0').collect::<Vec<_>>();
    let mut effective = std::collections::HashMap::<&str, (&str, &str)>::new();
    let mut index = 0;
    while index + 1 < fields.len() {
        let entry = fields[index + 1];
        let Some(newline) = entry.find('\n') else {
            return probe_failed_risk();
        };
        effective.insert(&entry[..newline], (fields[index], &entry[newline + 1..]));
        index += 2;
    }
    let local_value = |key: &str| {
        effective.get(key).and_then(|(scope, value)| {
            (*scope == "local" || *scope == "worktree").then_some(js_trim(value))
        })
    };
    let diff_external = local_value("diff.external");
    let fsmonitor = local_value("core.fsmonitor");
    GitConfigRisk {
        diff_external: diff_external.is_some_and(|value| !value.is_empty()),
        fsmonitor: fsmonitor.is_some_and(|value| {
            !value.is_empty()
                && !is_match(r"^(?:true|false|yes|no|on|off|0|1)$", &value.to_lowercase())
        }),
    }
}

async fn local_git_config_makes_command_unsafe(root: Node<'_>, source: &str, cwd: &Path) -> bool {
    let mut changed_directory = false;
    let mut uses_diff = false;
    let mut uses_status = false;
    for command in collect_descendants(root, &["command"], false) {
        let name = command_name(command, source)
            .unwrap_or_default()
            .to_lowercase();
        if name == "cd" || name == "pushd" {
            changed_directory = true;
            continue;
        }
        if name != "git" {
            continue;
        }
        let subcommand = argument_nodes(command)
            .first()
            .map(|arg| strip_outer_quotes(node_text(*arg, source)).to_lowercase())
            .unwrap_or_default();
        if subcommand != "diff" && subcommand != "status" {
            continue;
        }
        if changed_directory {
            return true;
        }
        uses_diff |= subcommand == "diff";
        uses_status |= subcommand == "status";
    }
    if !uses_diff && !uses_status {
        return false;
    }
    let risk = local_git_config_risk(cwd).await;
    (uses_diff && risk.diff_external) || (uses_status && risk.fsmonitor)
}

fn scan_delimited_section(script: &str, start: usize, delimiter: u8) -> Option<usize> {
    let bytes = script.as_bytes();
    let mut escaped = false;
    for (index, byte) in bytes.iter().enumerate().skip(start) {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == delimiter {
            return Some(index + 1);
        }
    }
    None
}

fn classify_single_sed_command_safety(script: &str) -> ShellCommandSafety {
    let compatibility_unknown = is_match(r"(?:^|[^\\])[ewr]\s", script);
    let command_offset = is_match_prefix_len(
        r"^\s*(?:(?:\d+|\$)(?:\s*,\s*(?:\d+|\$))?|/(?:\\[\s\S]|[^/\\])*/)?\s*",
        script,
    )
    .unwrap_or(0);
    if command_offset == script.len() {
        return ShellCommandSafety::ReadOnly;
    }
    let Some(command) = script.as_bytes().get(command_offset).copied() else {
        return ShellCommandSafety::Unknown;
    };
    if command == b'w' || command == b'W' {
        return if !js_trim(&script[command_offset + 1..]).is_empty() {
            ShellCommandSafety::Write
        } else {
            ShellCommandSafety::Unknown
        };
    }
    if matches!(command, b'e' | b'E' | b'r' | b'R') {
        return ShellCommandSafety::Unknown;
    }
    if command == b's' {
        let Some(delimiter) = script.as_bytes().get(command_offset + 1).copied() else {
            return ShellCommandSafety::Unknown;
        };
        if delimiter == b'\\' || delimiter.is_ascii_whitespace() {
            return ShellCommandSafety::Unknown;
        }
        let Some(replacement_start) = scan_delimited_section(script, command_offset + 2, delimiter)
        else {
            return ShellCommandSafety::Unknown;
        };
        let Some(flags_start) = scan_delimited_section(script, replacement_start, delimiter) else {
            return ShellCommandSafety::Unknown;
        };
        let flags = js_trim(&script[flags_start..]);
        if flags
            .bytes()
            .any(|byte| matches!(byte, b';' | b'\n' | b'{' | b'}'))
        {
            return ShellCommandSafety::Unknown;
        }
        if let Some(write_flag) = flags.find('w') {
            return if !js_trim(&flags[write_flag + 1..]).is_empty() {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            };
        }
        if is_match(r"[eErRwW]", &flags) || !is_match(SAFE_SUBSTITUTION_FLAGS, &flags) {
            return ShellCommandSafety::Unknown;
        }
        return if compatibility_unknown {
            ShellCommandSafety::Unknown
        } else {
            ShellCommandSafety::ReadOnly
        };
    }
    if script.as_bytes()[command_offset + 1..]
        .iter()
        .any(|byte| matches!(byte, b';' | b'\n' | b'{' | b'}'))
        || !is_match(SAFE_SED_COMMAND, &(command as char).to_string())
    {
        return ShellCommandSafety::Unknown;
    }
    if compatibility_unknown {
        ShellCommandSafety::Unknown
    } else {
        ShellCommandSafety::ReadOnly
    }
}

fn next_sed_separator(script: &str, start: usize) -> usize {
    script
        .as_bytes()
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, byte)| (*byte == b';' || *byte == b'\n').then_some(index))
        .unwrap_or(script.len())
}

fn classify_sed_script_safety(script: &str) -> ShellCommandSafety {
    let mut result = ShellCommandSafety::ReadOnly;
    let mut start = 0usize;
    while start < script.len() {
        let Some(address_len) = is_match_prefix_len(
            r"^\s*(?:(?:\d+|\$)(?:\s*,\s*(?:\d+|\$))?|/(?:\\[\s\S]|[^/\\])*/)?\s*",
            &script[start..],
        ) else {
            return ShellCommandSafety::Unknown;
        };
        let command_offset = start + address_len;
        if command_offset == script.len() {
            return result;
        }
        let command = script.as_bytes()[command_offset];
        if command == b'w' || command == b'W' {
            return if classify_single_sed_command_safety(&script[start..])
                == ShellCommandSafety::Write
            {
                ShellCommandSafety::Write
            } else {
                ShellCommandSafety::Unknown
            };
        }
        if matches!(command, b'e' | b'E' | b'r' | b'R') {
            return ShellCommandSafety::Unknown;
        }
        if command != b's' && !is_match(SAFE_SED_COMMAND, &(command as char).to_string()) {
            return ShellCommandSafety::Unknown;
        }
        let separator = if command == b's' {
            let Some(delimiter) = script.as_bytes().get(command_offset + 1).copied() else {
                return ShellCommandSafety::Unknown;
            };
            if delimiter == b'\\' || delimiter.is_ascii_whitespace() {
                return ShellCommandSafety::Unknown;
            }
            let Some(replacement_start) =
                scan_delimited_section(script, command_offset + 2, delimiter)
            else {
                return ShellCommandSafety::Unknown;
            };
            let Some(flags_start) = scan_delimited_section(script, replacement_start, delimiter)
            else {
                return ShellCommandSafety::Unknown;
            };
            next_sed_separator(script, flags_start)
        } else {
            next_sed_separator(script, command_offset + 1)
        };
        let current = classify_single_sed_command_safety(&script[start..separator]);
        if current == ShellCommandSafety::Write {
            return ShellCommandSafety::Write;
        }
        if current == ShellCommandSafety::Unknown {
            result = ShellCommandSafety::Unknown;
        }
        if separator == script.len() {
            return result;
        }
        start = separator + 1;
    }
    result
}

fn classify_sed_command_safety(args: &[String]) -> ShellCommandSafety {
    let options = before_terminator(args);
    if options.iter().enumerate().any(|(index, arg)| {
        is_match(r"(?i)^(?:--help|--version)$", arg)
            && !index
                .checked_sub(1)
                .and_then(|previous| options.get(previous))
                .is_some_and(|previous| SED_VALUE_OPTIONS.contains(&previous.as_str()))
    }) {
        return ShellCommandSafety::Unknown;
    }
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            break;
        }
        if SED_VALUE_OPTIONS.contains(&arg.as_str()) {
            index += 2;
            continue;
        }
        if is_match(r"^-[nErsuz]*e.+", arg) {
            index += 1;
            continue;
        }
        if is_match(r"^(?:-[nErsuz]*[iI]|--in-place(?:=|$))", arg) {
            return ShellCommandSafety::Write;
        }
        index += 1;
    }

    let mut scripts = Vec::<String>::new();
    let mut script_arguments = HashSet::<usize>::new();
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            if scripts.is_empty() {
                let Some(script) = args.get(index + 1) else {
                    return ShellCommandSafety::Unknown;
                };
                if script.starts_with('-') {
                    return ShellCommandSafety::Unknown;
                }
                scripts.push(script.clone());
                script_arguments.insert(index + 1);
            }
            break;
        }
        if is_match(r"^(?:-l|--line-length)$", arg) {
            index += 1;
            if args.get(index).is_none_or(|value| value.is_empty()) {
                return ShellCommandSafety::Unknown;
            }
        } else if is_match(r"^(?:-f|--file(?:=|$))", arg) {
            return ShellCommandSafety::Unknown;
        } else if arg == "-e" || arg == "--expression" {
            index += 1;
            let Some(script) = args.get(index) else {
                return ShellCommandSafety::Unknown;
            };
            if script.is_empty() || script.starts_with('-') {
                return ShellCommandSafety::Unknown;
            }
            scripts.push(script.clone());
            script_arguments.insert(index);
        } else if is_match(r"^(?:-e.+|--expression=)", arg) {
            let script = if arg.starts_with("-e") {
                &arg[2..]
            } else {
                &arg[13..]
            };
            if script.starts_with('-') {
                return ShellCommandSafety::Unknown;
            }
            scripts.push(script.to_owned());
            script_arguments.insert(index);
        } else if arg.starts_with("--") && !is_match(r"^--line-length(?:=|$)", arg) {
            return ShellCommandSafety::Unknown;
        } else if arg.starts_with('-') && !is_match(SAFE_SED_OPTION, arg) {
            return ShellCommandSafety::Unknown;
        } else if !arg.starts_with('-') && scripts.is_empty() {
            scripts.push(arg.clone());
            script_arguments.insert(index);
        }
        index += 1;
    }

    let mut result = ShellCommandSafety::ReadOnly;
    for script in &scripts {
        let current = classify_sed_script_safety(script);
        if current == ShellCommandSafety::Write {
            return ShellCommandSafety::Write;
        }
        if current == ShellCommandSafety::Unknown {
            result = ShellCommandSafety::Unknown;
        }
    }
    let remaining_args = args
        .iter()
        .enumerate()
        .filter(|(index, _)| !script_arguments.contains(index))
        .map(|(_, arg)| arg.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    if is_match(r"(?:^|[^\\])[ewr]\s", &remaining_args) {
        ShellCommandSafety::Unknown
    } else {
        result
    }
}

fn is_match_prefix_len(pattern: &str, value: &str) -> Option<usize> {
    Regex::new(pattern)
        .expect("valid shell safety regex")
        .find(value)
        .filter(|matched| matched.start() == 0)
        .map(|matched| matched.end())
}

fn split_awk_statements(script: &str) -> (Vec<&str>, bool, bool) {
    let mut statements = Vec::new();
    let mut ambiguous_slash = false;
    let mut unsupported_at = false;
    let mut start = 0usize;
    let mut escaped = false;
    let mut in_string = false;
    let mut in_regex = false;
    let mut previous_significant = None;
    let mut index = 0usize;
    while index < script.len() {
        let Some(ch) = script[index..].chars().next() else {
            break;
        };
        let char_len = ch.len_utf8();
        if escaped {
            escaped = false;
            index += char_len;
            continue;
        }
        if (in_string || in_regex) && ch == '\\' {
            escaped = true;
            index += char_len;
            continue;
        }
        if in_string {
            if ch == '"' {
                in_string = false;
            }
            index += char_len;
            continue;
        }
        if in_regex {
            if ch == '/' {
                in_regex = false;
            }
            index += char_len;
            continue;
        }
        if ch == '"' {
            in_string = true;
            index += char_len;
            continue;
        }
        if ch == '/'
            && previous_significant.is_none_or(|previous| "({[=,:;!~?&|".contains(previous))
        {
            in_regex = true;
            index += char_len;
            continue;
        }
        if ch == '/' {
            ambiguous_slash = true;
        }
        if ch == '@' {
            unsupported_at = true;
        }
        if ch == '#' {
            statements.push(&script[start..index]);
            let newline = script[index + char_len..]
                .find('\n')
                .map(|relative| index + char_len + relative);
            let Some(newline) = newline else {
                return (statements, ambiguous_slash, unsupported_at);
            };
            start = newline + 1;
            index = newline + 1;
            previous_significant = Some('\n');
            continue;
        }
        if matches!(ch, ';' | '{' | '}' | '\n') {
            statements.push(&script[start..index]);
            start = index + char_len;
            previous_significant = Some(ch);
            index += char_len;
            continue;
        }
        if !js_whitespace(ch) {
            previous_significant = Some(ch);
        }
        index += char_len;
    }
    statements.push(&script[start..]);
    (statements, ambiguous_slash, unsupported_at)
}

fn classify_awk_script_safety(script: &str) -> ShellCommandSafety {
    let (statements, ambiguous_slash, unsupported_at) = split_awk_statements(script);
    if !ambiguous_slash
        && statements
            .iter()
            .any(|statement| awk_static_write_statement(statement))
    {
        return ShellCommandSafety::Write;
    }
    if unsupported_at || is_match(AWK_UNKNOWN_OPERATION, script) {
        return ShellCommandSafety::Unknown;
    }
    if is_match(AWK_PRINT, script) && script.chars().any(|ch| matches!(ch, '>' | '|')) {
        ShellCommandSafety::Unknown
    } else {
        ShellCommandSafety::ReadOnly
    }
}

fn awk_static_write_statement(statement: &str) -> bool {
    let statement = js_trim_start(statement);
    let keyword_len = if statement.starts_with("print")
        && statement[5..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_ascii_alphanumeric() && next != '_')
    {
        5
    } else if statement.starts_with("printf")
        && statement[6..]
            .chars()
            .next()
            .is_none_or(|next| !next.is_ascii_alphanumeric() && next != '_')
    {
        6
    } else {
        return false;
    };
    let after_keyword = js_trim_start(&statement[keyword_len..]);
    if after_keyword.starts_with('(') {
        return false;
    }
    is_match(
        r#"^(?:(?:"(?:\\[\s\S]|[^"\\])*")|[^">|])*(?:>>?)\s*"[^"]*"\s*$"#,
        after_keyword,
    )
}

fn classify_awk_command_safety(args: &[String]) -> ShellCommandSafety {
    let mut program_index = None;
    let mut index = 0;
    while index < args.len() {
        let arg = &args[index];
        if arg == "--" {
            program_index = Some(index + 1);
            break;
        }
        if arg == "-F" || arg == "-v" {
            index += 1;
            if args.get(index).is_none_or(|value| value.is_empty()) {
                return ShellCommandSafety::Unknown;
            }
            index += 1;
            continue;
        }
        if is_match(r"^-[Fv].+", arg) {
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return ShellCommandSafety::Unknown;
        }
        program_index = Some(index);
        break;
    }
    let Some(program_index) = program_index else {
        return ShellCommandSafety::ReadOnly;
    };
    let Some(program) = args.get(program_index) else {
        return ShellCommandSafety::Unknown;
    };
    let result = classify_awk_script_safety(program);
    if result != ShellCommandSafety::ReadOnly {
        return result;
    }
    if classify_awk_script_safety(&args.join(" ")) == ShellCommandSafety::ReadOnly {
        ShellCommandSafety::ReadOnly
    } else {
        ShellCommandSafety::Unknown
    }
}
