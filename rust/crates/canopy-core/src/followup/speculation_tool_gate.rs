//! Tool safety gate for speculative execution.
//!
//! The gate mirrors `packages/core/src/followup/speculationToolGate.ts`.
//! Shell parsing and the overlay filesystem are supplied through narrow traits
//! because they are independent runtime services; an absent or failed shell
//! classification must be represented as [`ShellCommandSafety::Unknown`].

use serde_json::{Map, Value};

/// Internal tool names used by the speculation gate. Keep these in sync with
/// `packages/core/src/tools/tool-names.ts`.
pub mod tool_names {
    pub const EDIT: &str = "edit";
    pub const WRITE_FILE: &str = "write_file";
    pub const READ_FILE: &str = "read_file";
    pub const GREP: &str = "grep_search";
    pub const GLOB: &str = "glob";
    pub const SUPER_SEARCH: &str = "super_search";
    pub const SHELL: &str = "run_shell_command";
    pub const TODO_WRITE: &str = "todo_write";
    pub const MEMORY: &str = "save_memory";
    pub const AGENT: &str = "agent";
    pub const SKILL: &str = "skill";
    pub const EXIT_PLAN_MODE: &str = "exit_plan_mode";
    pub const ENTER_PLAN_MODE: &str = "enter_plan_mode";
    pub const WEB_FETCH: &str = "web_fetch";
    pub const WEB_SEARCH: &str = "web_search";
    pub const LS: &str = "list_directory";
    pub const LSP: &str = "lsp";
    pub const ASK_USER_QUESTION: &str = "ask_user_question";
    pub const TEAM_PLAN_APPROVAL: &str = "team_plan_approval";
}

/// Approval modes accepted by Canopy's configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalMode {
    Plan,
    Default,
    AutoEdit,
    Auto,
    Yolo,
}

impl ApprovalMode {
    /// Parse the exact serialized config value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "plan" => Some(Self::Plan),
            "default" => Some(Self::Default),
            "auto-edit" => Some(Self::AutoEdit),
            "auto" => Some(Self::Auto),
            "yolo" => Some(Self::Yolo),
            _ => None,
        }
    }

    fn permits_speculative_writes(self) -> bool {
        matches!(self, Self::AutoEdit | Self::Auto | Self::Yolo)
    }
}

/// Gate decision, equivalent to the TypeScript `ToolGateResult`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolGateResult {
    pub action: GateAction,
    pub reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateAction {
    Allow,
    Redirect,
    Boundary,
}

impl ToolGateResult {
    fn allow() -> Self {
        Self {
            action: GateAction::Allow,
            reason: None,
        }
    }

    fn redirect(reason: String) -> Self {
        Self {
            action: GateAction::Redirect,
            reason: Some(reason),
        }
    }

    fn boundary(reason: String) -> Self {
        Self {
            action: GateAction::Boundary,
            reason: Some(reason),
        }
    }
}

/// Safety result from the shell AST classifier used by the TypeScript gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellCommandSafety {
    ReadOnly,
    Write,
    Unknown,
}

/// Supplies the same classification performed by `shellAstParser.ts`.
/// Implementations should return `Unknown` on parse errors or uncertainty.
pub trait ShellSafetyClassifier {
    fn classify(&self, command: &str, directory: Option<&str>) -> ShellCommandSafety;
}

/// Minimal overlay operations needed by this gate.
pub trait SpeculationOverlay {
    type Error;

    /// Resolve a path to its overlay copy when the file was already written.
    fn resolve_read_path(&self, real_path: &str) -> String;

    /// Redirect a write path into the overlay, preserving overlay errors.
    fn redirect_write(&mut self, real_path: &str) -> Result<String, Self::Error>;
}

impl SpeculationOverlay for super::overlay_fs::OverlayFs {
    type Error = super::overlay_fs::OverlayFsError;

    fn resolve_read_path(&self, real_path: &str) -> String {
        self.resolve_read_path(real_path)
            .to_string_lossy()
            .into_owned()
    }

    fn redirect_write(&mut self, real_path: &str) -> Result<String, Self::Error> {
        self.redirect_write(real_path)
            .map(|path| path.to_string_lossy().into_owned())
    }
}

/// Path-bearing argument names shared by the file tools.
pub const PATH_ARG_KEYS: [&str; 4] = ["file_path", "path", "filePath", "notebook_path"];

const SAFE_READ_ONLY_TOOLS: [&str; 6] = [
    tool_names::READ_FILE,
    tool_names::GREP,
    tool_names::GLOB,
    tool_names::SUPER_SEARCH,
    tool_names::LS,
    tool_names::LSP,
];

const WRITE_TOOLS: [&str; 2] = [tool_names::EDIT, tool_names::WRITE_FILE];

const BOUNDARY_TOOLS: [&str; 10] = [
    tool_names::AGENT,
    tool_names::SKILL,
    tool_names::TODO_WRITE,
    tool_names::MEMORY,
    tool_names::ASK_USER_QUESTION,
    tool_names::EXIT_PLAN_MODE,
    tool_names::ENTER_PLAN_MODE,
    tool_names::TEAM_PLAN_APPROVAL,
    tool_names::WEB_FETCH,
    tool_names::WEB_SEARCH,
];

/// Evaluate whether a tool call is safe during speculative execution.
///
/// Safe read tools have their first recognized path argument resolved through
/// the overlay. Writes are only redirected when approval mode already permits
/// automatic edits. Shell commands are allowed only when the injected source
/// classifier says `ReadOnly`; every other classification stops speculation.
pub fn evaluate_tool_call<O, C>(
    tool_name: &str,
    args: &mut Map<String, Value>,
    overlay_fs: &O,
    approval_mode: ApprovalMode,
    cwd: Option<&str>,
    shell_classifier: &C,
) -> ToolGateResult
where
    O: SpeculationOverlay,
    C: ShellSafetyClassifier,
{
    if SAFE_READ_ONLY_TOOLS.contains(&tool_name) {
        resolve_read_paths(args, overlay_fs);
        return ToolGateResult::allow();
    }

    if WRITE_TOOLS.contains(&tool_name) {
        if approval_mode.permits_speculative_writes() {
            return ToolGateResult::redirect(format!("write_tool:{tool_name}"));
        }
        return ToolGateResult::boundary(format!("write_tool_no_auto:{tool_name}"));
    }

    if tool_name == tool_names::SHELL {
        let command = args.get("command").and_then(Value::as_str).unwrap_or("");
        let directory = args
            .get("directory")
            .and_then(Value::as_str)
            .filter(|directory| !directory.is_empty())
            .or(cwd);
        if !command.is_empty()
            && shell_classifier.classify(command, directory) == ShellCommandSafety::ReadOnly
        {
            return ToolGateResult::allow();
        }
        let snippet = if command.is_empty() {
            "empty".to_owned()
        } else {
            command.chars().take(50).collect()
        };
        return ToolGateResult::boundary(format!("shell:{snippet}"));
    }

    if BOUNDARY_TOOLS.contains(&tool_name) {
        return ToolGateResult::boundary(format!("denied_tool:{tool_name}"));
    }

    ToolGateResult::boundary(format!("unknown_tool:{tool_name}"))
}

/// Resolve the first recognized string path argument through the overlay.
/// This mutates the argument object, like the TypeScript helper.
pub fn resolve_read_paths<O: SpeculationOverlay>(args: &mut Map<String, Value>, overlay_fs: &O) {
    for key in PATH_ARG_KEYS {
        if let Some(Value::String(path)) = args.get(key) {
            let path = unescape_path(path.trim());
            args.insert(
                key.to_owned(),
                Value::String(overlay_fs.resolve_read_path(&path)),
            );
            return;
        }
    }
}

/// Rewrite the first recognized string path argument to its overlay write path.
/// This mutates the argument object, like the TypeScript helper.
pub fn rewrite_path_args<O: SpeculationOverlay>(
    args: &mut Map<String, Value>,
    overlay_fs: &mut O,
) -> Result<(), O::Error> {
    for key in PATH_ARG_KEYS {
        if let Some(Value::String(path)) = args.get(key) {
            let path = unescape_path(path.trim());
            let redirected = overlay_fs.redirect_write(&path)?;
            args.insert(key.to_owned(), Value::String(redirected));
            return Ok(());
        }
    }
    Ok(())
}

/// Remove shell escaping from the path punctuation used by Canopy's shared
/// `PATH_ARG_KEYS` helpers. Windows paths are left untouched, matching the
/// TypeScript implementation where backslashes are path separators.
pub fn unescape_path(path: &str) -> String {
    if cfg!(windows) {
        return path.to_owned();
    }

    let mut output = String::with_capacity(path.len());
    let mut chars = path.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' && chars.peek().is_some_and(|next| is_shell_special(*next)) {
            // The next character is one escaped shell metacharacter.
            output.push(chars.next().expect("peeked character exists"));
        } else {
            output.push(ch);
        }
    }
    output
}

fn is_shell_special(ch: char) -> bool {
    matches!(
        ch,
        ' ' | '\t'
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | ';'
            | '|'
            | '*'
            | '?'
            | '$'
            | '`'
            | '\''
            | '"'
            | '#'
            | '&'
            | '<'
            | '>'
            | '!'
            | '~'
            | ','
    )
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fs;

    use super::super::overlay_fs::OverlayFs;

    use serde_json::{Map, json};

    use super::{
        ApprovalMode, GateAction, ShellCommandSafety, ShellSafetyClassifier, SpeculationOverlay,
        ToolGateResult, evaluate_tool_call, rewrite_path_args, tool_names, unescape_path,
    };

    #[derive(Default)]
    struct TestOverlay {
        resolved: RefCell<Vec<String>>,
        redirected: Vec<String>,
        written: HashMap<String, String>,
    }

    impl SpeculationOverlay for TestOverlay {
        type Error = &'static str;

        fn resolve_read_path(&self, path: &str) -> String {
            self.resolved.borrow_mut().push(path.to_owned());
            self.written
                .get(path)
                .cloned()
                .unwrap_or_else(|| path.to_owned())
        }

        fn redirect_write(&mut self, path: &str) -> Result<String, Self::Error> {
            self.redirected.push(path.to_owned());
            let redirected = path.replace("/real/", "/tmp/canopy-speculation/");
            self.written.insert(path.to_owned(), redirected.clone());
            Ok(redirected)
        }
    }

    struct TestShellClassifier {
        safety: ShellCommandSafety,
        seen: RefCell<Vec<(String, Option<String>)>>,
    }

    impl TestShellClassifier {
        fn with_safety(safety: ShellCommandSafety) -> Self {
            Self {
                safety,
                seen: RefCell::new(Vec::new()),
            }
        }
    }

    impl ShellSafetyClassifier for TestShellClassifier {
        fn classify(&self, command: &str, directory: Option<&str>) -> ShellCommandSafety {
            self.seen
                .borrow_mut()
                .push((command.to_owned(), directory.map(str::to_owned)));
            self.safety
        }
    }

    fn run_gate(
        tool_name: &str,
        args: &mut Map<String, serde_json::Value>,
        overlay: &TestOverlay,
        mode: ApprovalMode,
        shell_safety: ShellCommandSafety,
    ) -> ToolGateResult {
        evaluate_tool_call(
            tool_name,
            args,
            overlay,
            mode,
            None,
            &TestShellClassifier::with_safety(shell_safety),
        )
    }

    #[test]
    fn allows_all_safe_read_only_tool_names_and_resolves_paths() {
        let mut overlay = TestOverlay::default();
        overlay
            .redirect_write("/real/file name.txt")
            .expect("fake overlay redirect succeeds");
        for tool_name in [
            tool_names::READ_FILE,
            tool_names::GREP,
            tool_names::GLOB,
            tool_names::SUPER_SEARCH,
            tool_names::LS,
            tool_names::LSP,
        ] {
            let mut args = Map::new();
            args.insert("file_path".to_owned(), json!("/real/file\\ name.txt"));
            let result = run_gate(
                tool_name,
                &mut args,
                &overlay,
                ApprovalMode::Default,
                ShellCommandSafety::Unknown,
            );
            assert_eq!(result.action, GateAction::Allow, "{tool_name}");
            assert_eq!(
                args["file_path"], "/tmp/canopy-speculation/file name.txt",
                "{tool_name}"
            );
        }
    }

    #[test]
    fn redirects_writes_only_for_auto_edit_auto_and_yolo() {
        let overlay = TestOverlay::default();
        for mode in [
            ApprovalMode::AutoEdit,
            ApprovalMode::Auto,
            ApprovalMode::Yolo,
        ] {
            for tool_name in [tool_names::EDIT, tool_names::WRITE_FILE] {
                let result = run_gate(
                    tool_name,
                    &mut Map::new(),
                    &overlay,
                    mode,
                    ShellCommandSafety::Unknown,
                );
                assert_eq!(result.action, GateAction::Redirect);
                assert_eq!(
                    result.reason.as_deref(),
                    Some(format!("write_tool:{tool_name}").as_str())
                );
            }
        }
    }

    #[test]
    fn default_and_plan_write_calls_are_boundaries_with_source_reasons() {
        let overlay = TestOverlay::default();
        for mode in [ApprovalMode::Default, ApprovalMode::Plan] {
            for tool_name in [tool_names::EDIT, tool_names::WRITE_FILE] {
                let result = run_gate(
                    tool_name,
                    &mut Map::new(),
                    &overlay,
                    mode,
                    ShellCommandSafety::Unknown,
                );
                assert_eq!(result.action, GateAction::Boundary);
                assert_eq!(
                    result.reason.as_deref(),
                    Some(format!("write_tool_no_auto:{tool_name}").as_str())
                );
            }
        }
    }

    #[test]
    fn shell_is_allowed_only_for_nonempty_read_only_commands() {
        let overlay = TestOverlay::default();
        let classifier = TestShellClassifier::with_safety(ShellCommandSafety::ReadOnly);
        let mut args = Map::new();
        args.insert("command".to_owned(), json!("ls -la"));
        args.insert("directory".to_owned(), json!("/work/project"));
        let result = evaluate_tool_call(
            tool_names::SHELL,
            &mut args,
            &overlay,
            ApprovalMode::Default,
            Some("/fallback"),
            &classifier,
        );
        assert_eq!(result.action, GateAction::Allow);
        assert_eq!(
            classifier.seen.borrow().as_slice(),
            &[("ls -la".to_owned(), Some("/work/project".to_owned()))]
        );

        for safety in [ShellCommandSafety::Write, ShellCommandSafety::Unknown] {
            let result = run_gate(
                tool_names::SHELL,
                &mut json_args("command", "python -c 'print(1)'"),
                &overlay,
                ApprovalMode::Default,
                safety,
            );
            assert_eq!(result.action, GateAction::Boundary);
            assert_eq!(result.reason.as_deref(), Some("shell:python -c 'print(1)'"));
        }

        let result = run_gate(
            tool_names::SHELL,
            &mut json_args("command", ""),
            &overlay,
            ApprovalMode::Default,
            ShellCommandSafety::ReadOnly,
        );
        assert_eq!(result.action, GateAction::Boundary);
        assert_eq!(result.reason.as_deref(), Some("shell:empty"));
    }

    #[test]
    fn shell_directory_uses_nonempty_argument_then_cwd() {
        let overlay = TestOverlay::default();
        let classifier = TestShellClassifier::with_safety(ShellCommandSafety::Unknown);
        let mut args = json_args("command", "git status");
        args.insert("directory".to_owned(), json!(""));
        let _ = evaluate_tool_call(
            tool_names::SHELL,
            &mut args,
            &overlay,
            ApprovalMode::Default,
            Some("/cwd"),
            &classifier,
        );
        assert_eq!(
            classifier.seen.borrow().as_slice(),
            &[("git status".to_owned(), Some("/cwd".to_owned()))]
        );
    }

    #[test]
    fn shell_boundary_reason_truncates_command_at_fifty_characters() {
        let overlay = TestOverlay::default();
        let command = "x".repeat(55);
        let result = run_gate(
            tool_names::SHELL,
            &mut json_args("command", &command),
            &overlay,
            ApprovalMode::Default,
            ShellCommandSafety::Unknown,
        );
        assert_eq!(
            result.reason.as_deref(),
            Some(format!("shell:{}", "x".repeat(50)).as_str())
        );
    }

    #[test]
    fn known_boundary_and_unknown_names_remain_denied() {
        let overlay = TestOverlay::default();
        for tool_name in [
            tool_names::AGENT,
            tool_names::SKILL,
            tool_names::TODO_WRITE,
            tool_names::MEMORY,
            tool_names::ASK_USER_QUESTION,
            tool_names::EXIT_PLAN_MODE,
            tool_names::ENTER_PLAN_MODE,
            tool_names::TEAM_PLAN_APPROVAL,
            tool_names::WEB_FETCH,
            tool_names::WEB_SEARCH,
        ] {
            let result = run_gate(
                tool_name,
                &mut Map::new(),
                &overlay,
                ApprovalMode::Default,
                ShellCommandSafety::Unknown,
            );
            assert_eq!(result.action, GateAction::Boundary, "{tool_name}");
            assert_eq!(
                result.reason.as_deref(),
                Some(format!("denied_tool:{tool_name}").as_str())
            );
        }
        let unknown = run_gate(
            "mcp_custom_tool",
            &mut Map::new(),
            &overlay,
            ApprovalMode::Default,
            ShellCommandSafety::Unknown,
        );
        assert_eq!(unknown.action, GateAction::Boundary);
        assert_eq!(
            unknown.reason.as_deref(),
            Some("unknown_tool:mcp_custom_tool")
        );
    }

    #[test]
    fn rewriting_uses_first_path_key_trims_and_unescapes() {
        let mut overlay = TestOverlay::default();
        let mut args = Map::new();
        args.insert("file_path".to_owned(), json!(" /real/a\\ b "));
        args.insert("path".to_owned(), json!("/real/ignored"));
        rewrite_path_args(&mut args, &mut overlay).unwrap();
        assert_eq!(args["file_path"], "/tmp/canopy-speculation/a b");
        assert_eq!(args["path"], "/real/ignored");
        assert_eq!(overlay.redirected.as_slice(), &["/real/a b"]);
        assert_eq!(
            overlay.resolve_read_path("/real/a b"),
            "/tmp/canopy-speculation/a b"
        );
    }

    #[test]
    fn absent_or_non_string_path_arguments_are_untouched() {
        let mut overlay = TestOverlay::default();
        let mut args = Map::new();
        args.insert("command".to_owned(), json!("ls"));
        args.insert("file_path".to_owned(), json!(7));
        rewrite_path_args(&mut args, &mut overlay).unwrap();
        assert_eq!(args["command"], "ls");
        assert_eq!(args["file_path"], 7);
        assert!(overlay.redirected.is_empty());
    }

    #[test]
    fn read_path_resolution_rewrites_only_when_overlay_knows_path() {
        let mut overlay = TestOverlay::default();
        overlay
            .redirect_write("/real/readme.md")
            .expect("fake overlay redirect succeeds");
        let mut args = json_args("file_path", "/real/readme.md");
        let result = run_gate(
            tool_names::READ_FILE,
            &mut args,
            &overlay,
            ApprovalMode::Default,
            ShellCommandSafety::Unknown,
        );
        assert_eq!(result.action, GateAction::Allow);
        assert_eq!(args["file_path"], "/tmp/canopy-speculation/readme.md");

        let mut args = json_args("file_path", "/outside/readme.md");
        let _ = run_gate(
            tool_names::READ_FILE,
            &mut args,
            &overlay,
            ApprovalMode::Default,
            ShellCommandSafety::Unknown,
        );
        assert_eq!(args["file_path"], "/outside/readme.md");
    }

    #[test]
    fn composes_with_overlay_fs_for_write_redirect_and_later_read_resolution() {
        let root = std::env::temp_dir().join(format!(
            "canopy-speculation-gate-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&root).expect("create test root");
        let source_path = root.join("note.txt");
        fs::write(&source_path, "before").expect("create source file");
        let source_path = source_path.to_string_lossy().into_owned();
        let mut overlay = OverlayFs::new(&root);

        let mut write_args = json_args("file_path", &source_path);
        rewrite_path_args(&mut write_args, &mut overlay).expect("redirect write");
        let overlay_path = write_args["file_path"]
            .as_str()
            .expect("rewritten path is a string")
            .to_owned();
        assert_ne!(overlay_path, source_path);

        let mut read_args = json_args("file_path", &source_path);
        let result = evaluate_tool_call(
            tool_names::READ_FILE,
            &mut read_args,
            &overlay,
            ApprovalMode::Default,
            None,
            &TestShellClassifier::with_safety(ShellCommandSafety::Unknown),
        );
        assert_eq!(result.action, GateAction::Allow);
        assert_eq!(read_args["file_path"], overlay_path);

        overlay.cleanup();
        fs::remove_dir_all(root).expect("remove test root");
    }

    #[test]
    fn approval_mode_parser_matches_config_values() {
        assert_eq!(ApprovalMode::parse("plan"), Some(ApprovalMode::Plan));
        assert_eq!(ApprovalMode::parse("default"), Some(ApprovalMode::Default));
        assert_eq!(
            ApprovalMode::parse("auto-edit"),
            Some(ApprovalMode::AutoEdit)
        );
        assert_eq!(ApprovalMode::parse("auto"), Some(ApprovalMode::Auto));
        assert_eq!(ApprovalMode::parse("yolo"), Some(ApprovalMode::Yolo));
        assert_eq!(ApprovalMode::parse("AUTO"), None);
        assert_eq!(ApprovalMode::parse("unsafe"), None);
    }

    #[test]
    fn unescape_path_removes_only_shell_metacharacter_escapes() {
        assert_eq!(unescape_path(r"a\ b\(c\)\!"), "a b(c)!");
        assert_eq!(unescape_path(r"a\zb"), r"a\zb");
    }

    fn json_args(key: &str, value: &str) -> Map<String, serde_json::Value> {
        let mut args = Map::new();
        args.insert(key.to_owned(), json!(value));
        args
    }
}
